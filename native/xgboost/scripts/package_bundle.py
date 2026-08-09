#!/usr/bin/env python3
"""Package a verified native XGBoost core without installing it."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "dsflower-xgboost-bundle-v1"
STATUS = "bundle-core:fixed-point-discrete-v1:internal-only"
XGBOOST_MECHANISM = "xgboost/fixed-point-discrete/v1"
DP_MECHANISM = "cks20-discrete-gaussian-i64-hmac-sha256-v1"


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value
    return values


def target() -> tuple[str, str, str, str]:
    system_name = platform.system().lower()
    if system_name == "darwin":
        system, xgboost_name, primitive_name = (
            "macos",
            "libxgboost.dylib",
            "libdsflower_dp_primitives.dylib",
        )
    elif system_name == "linux":
        system, xgboost_name, primitive_name = (
            "linux",
            "libxgboost.so",
            "libdsflower_dp_primitives.so",
        )
    elif system_name == "windows":
        system, xgboost_name, primitive_name = (
            "windows",
            "xgboost.dll",
            "dsflower_dp_primitives.dll",
        )
    else:
        raise ValueError(f"unsupported bundle system: {system_name}")

    machine_name = platform.machine().lower()
    if machine_name in {"amd64", "x86_64"}:
        machine = "x86_64"
    elif machine_name in {"arm64", "aarch64"}:
        machine = "aarch64"
    else:
        raise ValueError(f"unsupported bundle machine: {machine_name}")
    return system, machine, xgboost_name, primitive_name


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def main(source_arg: str, xgboost_arg: str, primitive_arg: str, output_arg: str) -> None:
    source = Path(source_arg).resolve(strict=True)
    xgboost = Path(xgboost_arg).resolve(strict=True)
    primitive = Path(primitive_arg).resolve(strict=True)
    output = Path(output_arg).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError("bundle output must not already exist")
    if not output.parent.is_dir():
        raise ValueError("bundle output parent must already exist")
    if not xgboost.is_file() or not primitive.is_file():
        raise ValueError("bundle inputs must be regular library files")

    subprocess.run(
        ["sh", str(ROOT / "scripts" / "verify_patched.sh"), str(source)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    metadata = parse_env(ROOT / "UPSTREAM.env")
    patched_tree = metadata["DSFLOWER_XGB_PATCHED_TREE"]
    system, machine, xgboost_name, primitive_name = target()

    temporary = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.tmp-", dir=str(output.parent))
    )
    try:
        library_dir = temporary / "lib"
        library_dir.mkdir(mode=0o755)
        packaged_xgboost = library_dir / xgboost_name
        packaged_primitive = library_dir / primitive_name
        shutil.copyfile(xgboost, packaged_xgboost)
        shutil.copyfile(primitive, packaged_primitive)
        os.chmod(packaged_xgboost, 0o755)
        os.chmod(packaged_primitive, 0o755)

        manifest = {
            "bundle_version": 1,
            "dp_primitives": {
                "abi": 2,
                "mechanism": DP_MECHANISM,
                "path": f"lib/{primitive_name}",
                "sha256": digest(packaged_primitive),
            },
            "platform": {"machine": machine, "system": system},
            "provenance": {
                "patched_tree": patched_tree,
                "patchset_version": int(metadata["DSFLOWER_XGB_PATCHSET_VERSION"]),
                "upstream_commit": metadata["DSFLOWER_XGB_UPSTREAM_COMMIT"],
                "upstream_tree": metadata["DSFLOWER_XGB_UPSTREAM_TREE"],
            },
            "schema": SCHEMA,
            "xgboost": {
                "mechanism": XGBOOST_MECHANISM,
                "path": f"lib/{xgboost_name}",
                "privacy_context_abi": 3,
                "sha256": digest(packaged_xgboost),
                "status": STATUS,
            },
        }
        canonical = json.dumps(
            manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii") + b"\n"
        (temporary / "manifest.json").write_bytes(canonical)
        os.replace(temporary, output)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise

    print(f"packaged curated bundle at {output}")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(
            f"usage: {sys.argv[0]} PATCHED_SOURCE LIBXGBOOST DP_RUNTIME_LIBRARY OUTPUT"
        )
    main(*sys.argv[1:])
