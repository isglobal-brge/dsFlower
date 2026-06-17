"""dsFlower SuperNode code integrity hook (default-DENY).

Injected via PYTHONPATH by dsFlower when launching flower-supernode.
Python loads sitecustomize.py automatically before any application code.

Threat model: the ClientApp (FAB) is delivered to the node at run time and
runs IN-PROCESS with read access to the staged data. A malicious client must
not be able to run unverified code. So we DENY by default: any "foreign" code
module -- one loaded from outside the interpreter's stdlib / site-packages
(i.e. delivered application code) -- must be the exact template the server
pinned for this run, verified by a recursive SHA-256 of its package contents.
Anything else kills the process immediately.

NOTE: an in-process import hook is a defence-in-depth layer, not an absolute
boundary (in-process code can tamper with it). The authoritative control is
running the ClientApp under `flower-supernode --isolation process` inside an
OS sandbox; this hook closes the trivial "name your FAB anything" bypass.
"""

import hashlib
import os
import site
import sys
import sysconfig


MANIFEST_DIR = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
EXPECTED_HASH_FILE = os.path.join(MANIFEST_DIR, "expected_hash.txt") if MANIFEST_DIR else ""
EXPECTED_TEMPLATE_FILE = os.path.join(MANIFEST_DIR, "expected_template.txt") if MANIFEST_DIR else ""

# Top-level package names that are part of the trusted runtime and never
# treated as "foreign" application code even if their path looks unusual.
_RUNTIME_PKGS = {"flwr", "flwr_serverapp", "flwr_clientapp"}

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
    artifacts that differ per environment). Mirrors .compute_template_hash in
    interface.R byte-for-byte: sorted by forward-slash relative path, each as
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


def _read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def _verify_foreign(top_name, pkg_dir):
    """Default-deny: a foreign code package must be the pinned, hash-matched
    template, else the process is killed -- BEFORE the module body runs."""
    if not EXPECTED_HASH_FILE or not os.path.exists(EXPECTED_HASH_FILE):
        _abort("foreign code package '%s' about to load but no "
               "expected_hash.txt to verify against." % top_name)

    expected = _read(EXPECTED_HASH_FILE)
    if not expected:
        _abort("expected_hash.txt is empty.")

    pinned = _read(EXPECTED_TEMPLATE_FILE) if EXPECTED_TEMPLATE_FILE else ""
    if pinned and top_name != pinned:
        _abort("unexpected application package '%s' (server pinned '%s'). "
               "Only the pinned template may run." % (top_name, pinned))

    actual = _hash_package(pkg_dir)
    if actual != expected:
        _abort("code hash mismatch for '%s'\n  expected: %s\n  actual:   %s\n"
               "  package:  %s" % (top_name, expected, actual, pkg_dir))


class _IntegrityFinder(object):
    """A sys.meta_path finder that verifies foreign (delivered) code BEFORE
    the import machinery executes it. find_spec runs prior to exec_module, so
    aborting here means malicious code never runs even once."""

    def find_spec(self, fullname, path=None, target=None):
        top = fullname.split(".")[0]
        if top in _RUNTIME_PKGS or top in _verified_packages:
            return None
        try:
            spec = _PathFinder.find_spec(fullname, path)
        except Exception:
            return None
        if spec is None:
            return None
        origin = getattr(spec, "origin", None)
        if not origin or origin in ("built-in", "frozen", "namespace"):
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
