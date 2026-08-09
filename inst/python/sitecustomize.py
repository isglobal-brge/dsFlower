"""dsFlower SuperNode code integrity hook (default-DENY).

Injected via PYTHONPATH by dsFlower when launching flower-supernode.
Python loads sitecustomize.py automatically before any application code.

Threat model: the ClientApp (FAB) is delivered to the node at run time and
runs IN-PROCESS with read access to the staged data. A malicious client must
not be able to run unverified code. So we DENY by default: any "foreign" code
module -- one loaded from outside the interpreter's stdlib / site-packages
(i.e. delivered application code) -- must be explicitly pinned by the server
for this run, verified by a recursive SHA-256 of its package contents.
Anything else kills the process immediately.

NOTE: an import hook is a defence-in-depth layer, not an absolute boundary.
The SuperNode explicitly uses subprocess isolation and only admits the
node-hash-pinned trusted ClientApp. Arbitrary HookApp code is never imported by
that parent; it runs behind the separate mandatory OS sandbox.
"""

import hashlib
import os
import site
import sys
import sysconfig


MANIFEST_DIR = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
MANIFEST_FILE = os.path.join(MANIFEST_DIR, "manifest.json") if MANIFEST_DIR else ""
# Every run uses one package-pin map: {package_name: sha256}. Every foreign
# package must be listed and hash-match (default-deny). Hook runs add their
# separately verified uploaded package to the canonical runner pin.
PINNED_PACKAGES_FILE = os.path.join(MANIFEST_DIR, "pinned_packages.json") if MANIFEST_DIR else ""


def _load_pinned_map():
    if not PINNED_PACKAGES_FILE or not os.path.exists(PINNED_PACKAGES_FILE):
        return None
    try:
        import json
        with open(PINNED_PACKAGES_FILE) as f:
            m = json.load(f)
        return m if isinstance(m, dict) and m else None
    except Exception:
        return {}  # present-but-unreadable -> deny everything (fail closed)


def _load_user_module_name():
    """The uploaded HookApp package name, pinned by the node in manifest.json."""
    if not MANIFEST_FILE or not os.path.exists(MANIFEST_FILE):
        return ""
    try:
        import json
        with open(MANIFEST_FILE) as f:
            manifest = json.load(f)
        value = manifest.get("user-module", "")
        return value if isinstance(value, str) else ""
    except Exception:
        return ""

# Top-level package names that are part of the trusted runtime and never
# treated as "foreign" application code even if their path looks unusual.
_RUNTIME_PKGS = {"flwr", "flwr_serverapp", "flwr_clientapp"}

# The node may execute exactly the node-installed, hash-pinned ClientApp selected
# by its own manifest.  The FAB controls pyproject.toml, so package hashing alone
# is insufficient: without this gate it could point Flower at another callable.
_UNIFIED_CLIENTAPP_REF = "dsflower_runner.client_app:app"
_NATIVE_TREE_CLIENTAPP_REF = "dsflower_runner.native_tree_client_app:app"


def _load_canonical_clientapp_ref():
    if not MANIFEST_FILE or not os.path.exists(MANIFEST_FILE):
        return ""
    try:
        import json
        with open(MANIFEST_FILE, encoding="utf-8") as handle:
            manifest = json.load(handle)
        track = manifest.get("dp-track") if isinstance(manifest, dict) else None
    except Exception:
        return ""
    if track == "native_tree":
        return _NATIVE_TREE_CLIENTAPP_REF
    if track in ("neural", "egress", "validation"):
        return _UNIFIED_CLIENTAPP_REF
    return ""


_CANONICAL_CLIENTAPP_REF = _load_canonical_clientapp_ref()

_verified_packages = set()


def _abort(msg):
    print("\nDSFLOWER SECURITY: " + msg + "\nAborting process.\n",
          file=sys.stderr, flush=True)
    os._exit(99)


def _safe_prefixes():
    """Directories whose code is part of the interpreter/install (trusted)."""
    prefixes = set()
    try:
        paths = sysconfig.get_paths()
        for key in ("stdlib", "platstdlib", "purelib", "platlib"):
            p = paths.get(key)
            if p:
                prefixes.add(os.path.realpath(p))
    except Exception:
        pass
    try:
        for p in site.getsitepackages():
            prefixes.add(os.path.realpath(p))
    except Exception:
        pass
    try:
        usp = site.getusersitepackages()
        if usp:
            prefixes.add(os.path.realpath(usp))
    except Exception:
        pass
    for base in (sys.prefix, sys.base_prefix, sys.exec_prefix):
        if base:
            prefixes.add(os.path.realpath(os.path.join(base, "lib")))
    return tuple(p for p in prefixes if p)


_SAFE_PREFIXES = _safe_prefixes()


def _is_foreign(path):
    """True if path is NOT inside a trusted interpreter/install directory."""
    rp = os.path.realpath(path)
    for pref in _SAFE_PREFIXES:
        if rp == pref or rp.startswith(pref + os.sep):
            return False
    return True


def _hash_package(pkg_dir):
    """Recursive SHA-256 of all files under pkg_dir (excludes compiled
    artifacts that differ per environment). Mirrors .compute_harness_hash and
    .hash_pkg_dir byte-for-byte: sorted by forward-slash relative path, each as
    relpath + "\\n" + content + "\\x00"."""
    hasher = hashlib.sha256()
    entries = []
    for root, dirs, files in os.walk(pkg_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for fname in files:
            if fname.endswith(".pyc") or fname.endswith(".pyo"):
                continue
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, pkg_dir).replace(os.sep, "/")
            entries.append((rel, full))
    for rel, full in sorted(entries, key=lambda e: e[0]):
        try:
            with open(full, "rb") as f:
                content = f.read()
        except OSError:
            return ""
        hasher.update(rel.encode("utf-8"))
        hasher.update(b"\n")
        hasher.update(content)
        hasher.update(b"\x00")
    return hasher.hexdigest()


_PINNED_MAP = _load_pinned_map()
_USER_MODULE = _load_user_module_name()


def _verify_foreign(top_name, pkg_dir):
    """Default-deny: a foreign code package must be pinned + hash-matched, else
    the process is killed -- BEFORE the module body runs."""
    actual = _hash_package(pkg_dir)

    if _PINNED_MAP is None:
        _abort("foreign code package '%s' about to load without a package pin map."
               % top_name)
    expected = _PINNED_MAP.get(top_name)
    if expected is None:
        _abort("package '%s' is not in pinned_packages.json (default-deny)."
               % top_name)
    if actual != expected:
        _abort("code hash mismatch for '%s'\n  expected: %s\n  actual:   %s\n"
               "  package:  %s" % (top_name, expected, actual, pkg_dir))


def _install_clientapp_load_guard(module):
    """Pin Flower's resolved ClientApp object reference before it is imported."""
    original = getattr(module, "load_app", None)
    if not callable(original):
        _abort("Flower ClientApp loader is unavailable; refusing an unpinned app.")
    if getattr(original, "_dsflower_entrypoint_guard", False):
        return

    def guarded_load_app(module_attribute_str, *args, **kwargs):
        if (not _CANONICAL_CLIENTAPP_REF or
                not isinstance(module_attribute_str, str) or
                module_attribute_str != _CANONICAL_CLIENTAPP_REF):
            _abort("unexpected ClientApp entrypoint '%s' (node pinned '%s')."
                   % (module_attribute_str, _CANONICAL_CLIENTAPP_REF))
        app = original(module_attribute_str, *args, **kwargs)
        # Importing the canonical reference must have crossed _IntegrityFinder,
        # which verifies the package before its module body runs.  A cached or
        # future loader path that skipped that pin is denied as well.
        if "dsflower_runner" not in _verified_packages:
            _abort("canonical ClientApp loaded without activating its code hash pin.")
        return app

    guarded_load_app._dsflower_entrypoint_guard = True
    module.load_app = guarded_load_app


class _PostExecClientAppLoader(object):
    """Delegate Flower's real loader, then replace its imported load_app alias."""

    def __init__(self, wrapped):
        self._wrapped = wrapped

    def create_module(self, spec):
        create = getattr(self._wrapped, "create_module", None)
        return create(spec) if create is not None else None

    def exec_module(self, module):
        self._wrapped.exec_module(module)
        _install_clientapp_load_guard(module)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _IntegrityFinder(object):
    """A sys.meta_path finder that verifies foreign (delivered) code BEFORE
    the import machinery executes it. find_spec runs prior to exec_module, so
    aborting here means malicious code never runs even once."""

    def find_spec(self, fullname, path=None, target=None):
        top = fullname.split(".")[0]
        # The uploaded HookApp is authorized ONLY in egress_child.py, which loads
        # its exact re-hashed __init__.py inside the OS sandbox.  It must never run
        # in this trusted parent.  Check before runtime/safe-prefix exemptions so a
        # package named ``flwr``, ``torch`` or ``numpy`` cannot shadow that runtime.
        if _USER_MODULE and top == _USER_MODULE:
            _abort("uploaded HookApp package '%s' may not load in the trusted parent."
                   % top)
        if top in _verified_packages:
            return None
        try:
            spec = _PathFinder.find_spec(fullname, path)
        except Exception:
            return None
        if spec is None:
            return None
        origin = getattr(spec, "origin", None)
        if top == "dsflower_runner":
            # The canonical runner is application code, not generic runtime code.
            # Verify it against the node pin even when its installation path sits
            # below a normally trusted site-packages prefix.
            if not origin or origin in ("built-in", "frozen", "namespace"):
                _abort("canonical dsflower_runner is not a regular pinned package.")
            locs = getattr(spec, "submodule_search_locations", None)
            pkg_dir = locs[0] if locs else os.path.dirname(os.path.abspath(origin))
            _verify_foreign(top, pkg_dir)
            _verified_packages.add(top)
            return None
        if not origin or origin in ("built-in", "frozen", "namespace"):
            return None
        if top in _RUNTIME_PKGS:
            # Runtime names are exempt only when they actually resolve inside the
            # trusted interpreter installation.  An unpinned top-level ``flwr.py``
            # in an uploaded FAB must not inherit the name-based exemption.
            if _is_foreign(origin):
                _abort("runtime package '%s' resolved to foreign code: %s"
                       % (top, origin))
            if fullname == "flwr.clientapp.utils":
                loader = getattr(spec, "loader", None)
                if loader is None or not hasattr(loader, "exec_module"):
                    _abort("Flower ClientApp loader cannot be pinned safely.")
                spec.loader = _PostExecClientAppLoader(loader)
                return spec
            return None
        if not _is_foreign(origin):
            return None  # trusted runtime: stdlib / site-packages

        locs = getattr(spec, "submodule_search_locations", None)
        pkg_dir = locs[0] if locs else os.path.dirname(os.path.abspath(origin))
        _verify_foreign(top, pkg_dir)   # aborts on any failure
        _verified_packages.add(top)
        return None  # verified -> let the normal machinery load it


if MANIFEST_DIR and os.path.isdir(MANIFEST_DIR):
    from importlib.machinery import PathFinder as _PathFinder
    sys.meta_path.insert(0, _IntegrityFinder())
