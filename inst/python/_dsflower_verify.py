"""dsFlower code integrity verification.

This module lives on the server filesystem (manifest directory) and is
loaded by task.py to verify that the Python code received via Flower
matches the expected template hash computed by the server.
"""
import hashlib
import os
import sys


def verify(manifest_dir, package_dir):
    """Verify code integrity.

    Args:
        manifest_dir: Path to the manifest directory (server filesystem).
        package_dir: Path to the package directory (from Flower FAB).

    Raises:
        RuntimeError: If hash verification fails.
    """
    # Check verification status from server-side pre-flight check
    status_path = os.path.join(manifest_dir, "verification_status.txt")
    if os.path.exists(status_path):
        with open(status_path) as f:
            status = f.read().strip()
        if status == "POISONED":
            raise RuntimeError(
                "DSFLOWER SECURITY: Server rejected the app hash. "
                "The code may have been tampered with."
            )
        if status != "VERIFIED":
            raise RuntimeError(
                "DSFLOWER SECURITY: App not verified by server. "
                "Expected 'VERIFIED', got: " + status
            )

    # Also verify the hash directly
    hash_path = os.path.join(manifest_dir, "expected_hash.txt")
    if not os.path.exists(hash_path):
        raise RuntimeError(
            "DSFLOWER SECURITY: expected_hash.txt not found in manifest."
        )

    with open(hash_path) as f:
        expected = f.read().strip()

    actual = _hash_package(package_dir)

    if actual != expected:
        print(
            f"DSFLOWER SECURITY VIOLATION:\n"
            f"  Expected hash: {expected}\n"
            f"  Actual hash:   {actual}\n"
            f"  Package dir:   {package_dir}",
            file=sys.stderr,
        )
        raise RuntimeError(
            "DSFLOWER SECURITY: Code hash mismatch. "
            "Expected " + expected + ", got " + actual
        )


def _hash_package(pkg_dir):
    """Compute SHA-256 of all .py files in pkg_dir."""
    hasher = hashlib.sha256()
    py_files = sorted(f for f in os.listdir(pkg_dir) if f.endswith(".py"))
    for fname in py_files:
        fpath = os.path.join(pkg_dir, fname)
        with open(fpath, "rb") as f:
            content = f.read()
        hasher.update(fname.encode("utf-8"))
        hasher.update(b"\n")
        hasher.update(content)
        hasher.update(b"\x00")
    return hasher.hexdigest()
