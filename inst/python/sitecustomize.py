"""dsFlower SuperNode code integrity hook.

Injected via PYTHONPATH by dsFlower when launching flower-supernode.
Python loads sitecustomize.py automatically before any application code.

Registers a post-import hook that verifies the SHA-256 hash of any
dsFlower template package against the expected hash written by the server.
If the hash does not match, the process is killed immediately.
"""

import hashlib
import importlib
import os
import sys


MANIFEST_DIR = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
EXPECTED_HASH_FILE = os.path.join(MANIFEST_DIR, "expected_hash.txt") if MANIFEST_DIR else ""

_KNOWN_TEMPLATES = {
    "sklearn_logreg", "sklearn_sgd", "sklearn_ridge", "pytorch_mlp",
}

_verified_packages = set()


def _hash_package(pkg_dir):
    """Compute SHA-256 of all .py files in pkg_dir."""
    hasher = hashlib.sha256()
    try:
        py_files = sorted(f for f in os.listdir(pkg_dir) if f.endswith(".py"))
    except OSError:
        return ""
    for fname in py_files:
        fpath = os.path.join(pkg_dir, fname)
        try:
            with open(fpath, "rb") as f:
                content = f.read()
        except OSError:
            return ""
        hasher.update(fname.encode("utf-8"))
        hasher.update(b"\n")
        hasher.update(content)
        hasher.update(b"\x00")
    return hasher.hexdigest()


def _check_module(module):
    """If module belongs to a known template, verify its package hash."""
    if module is None or not hasattr(module, "__file__") or module.__file__ is None:
        return

    top_name = module.__name__.split(".")[0]
    if top_name not in _KNOWN_TEMPLATES:
        return
    if top_name in _verified_packages:
        return

    if not EXPECTED_HASH_FILE or not os.path.exists(EXPECTED_HASH_FILE):
        return

    with open(EXPECTED_HASH_FILE) as f:
        expected = f.read().strip()

    if not expected:
        return

    pkg_dir = os.path.dirname(os.path.abspath(module.__file__))
    # If this is a submodule, go up to the package root
    if "." in module.__name__:
        pkg_dir = os.path.dirname(pkg_dir)
        # But the .py files are in the inner package dir
        inner = os.path.join(pkg_dir, top_name)
        if os.path.isdir(inner):
            pkg_dir = inner

    actual = _hash_package(pkg_dir)

    if actual != expected:
        print(
            f"\nDSFLOWER SECURITY VIOLATION\n"
            f"  Expected: {expected}\n"
            f"  Actual:   {actual}\n"
            f"  Package:  {pkg_dir}\n"
            f"  Module:   {module.__name__}\n"
            f"Aborting process.\n",
            file=sys.stderr, flush=True,
        )
        os._exit(99)

    _verified_packages.add(top_name)


# Monkey-patch the import system to check after every import
_original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__


def _hooked_import(name, *args, **kwargs):
    module = _original_import(name, *args, **kwargs)
    top = name.split(".")[0]
    if top in _KNOWN_TEMPLATES and top not in _verified_packages:
        _check_module(module)
    return module


if MANIFEST_DIR and os.path.isdir(MANIFEST_DIR):
    if hasattr(__builtins__, '__import__'):
        __builtins__.__import__ = _hooked_import
    else:
        import builtins
        builtins.__import__ = _hooked_import
