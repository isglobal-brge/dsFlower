#!/usr/bin/env python3
"""Strictly verify a local curated XGBoost bundle and its native guards."""

from __future__ import annotations

import ctypes as ct
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sys

from package_bundle import (
    DP_MECHANISM,
    ROOT,
    SCHEMA,
    STATUS,
    XGBOOST_MECHANISM,
    digest,
    parse_env,
    target,
)


def require_object(value: object, keys: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"{label} has unexpected fields")
    return value


def require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be non-empty text")
    return value


def checked_relative_file(bundle: Path, relative: object, expected: str) -> Path:
    text = require_text(relative, "library path")
    path = PurePosixPath(text)
    if text != expected or path.is_absolute() or ".." in path.parts:
        raise ValueError("bundle library path is not canonical")
    resolved = bundle.joinpath(*path.parts)
    if resolved.is_symlink() or not resolved.is_file():
        raise ValueError("bundle library must be a regular non-symlink file")
    return resolved


def load_and_check(xgboost: Path, primitive: Path) -> None:
    dll_directory = None
    try:
        if os.name == "nt":
            dll_directory = os.add_dll_directory(str(primitive.parent))
        mode = getattr(ct, "RTLD_GLOBAL", 0)
        dp = ct.CDLL(str(primitive), mode=mode)
        dp.dsflower_dp_primitives_abi_version.argtypes = []
        dp.dsflower_dp_primitives_abi_version.restype = ct.c_uint32
        dp.dsflower_dp_primitives_mechanism_id.argtypes = []
        dp.dsflower_dp_primitives_mechanism_id.restype = ct.c_char_p
        if dp.dsflower_dp_primitives_abi_version() != 2:
            raise ValueError("DP primitive runtime ABI mismatch")
        if dp.dsflower_dp_primitives_mechanism_id() != DP_MECHANISM.encode("ascii"):
            raise ValueError("DP primitive runtime mechanism mismatch")

        xgb = ct.CDLL(str(xgboost))
        xgb.XGBDsFlowerPrivacyScaffoldStatus.argtypes = [ct.POINTER(ct.c_char_p)]
        xgb.XGBDsFlowerPrivacyScaffoldStatus.restype = ct.c_int
        status = ct.c_char_p()
        if xgb.XGBDsFlowerPrivacyScaffoldStatus(ct.byref(status)) != 0:
            raise ValueError("XGBoost bundle status call failed")
        if status.value != STATUS.encode("ascii"):
            raise ValueError("XGBoost bundle runtime status mismatch")
    finally:
        if dll_directory is not None:
            dll_directory.close()


def main(bundle_arg: str) -> None:
    bundle = Path(bundle_arg).resolve(strict=True)
    if bundle.is_symlink() or not bundle.is_dir():
        raise ValueError("bundle root must be a regular directory")

    actual_entries: set[str] = set()
    for root, directories, files in os.walk(bundle, followlinks=False):
        root_path = Path(root)
        for name in directories + files:
            path = root_path / name
            if path.is_symlink():
                raise ValueError("bundle must not contain symlinks")
        for name in files:
            actual_entries.add((root_path / name).relative_to(bundle).as_posix())

    system, machine, xgboost_name, primitive_name = target()
    expected_entries = {
        "manifest.json",
        f"lib/{xgboost_name}",
        f"lib/{primitive_name}",
    }
    if actual_entries != expected_entries:
        raise ValueError("bundle contains missing or unexpected files")

    raw = (bundle / "manifest.json").read_bytes()
    if len(raw) > 65536:
        raise ValueError("bundle manifest is too large")
    manifest = require_object(
        json.loads(raw.decode("ascii")),
        {"bundle_version", "dp_primitives", "platform", "provenance", "schema", "xgboost"},
        "manifest",
    )
    canonical = json.dumps(
        manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii") + b"\n"
    if raw != canonical:
        raise ValueError("bundle manifest is not canonical JSON")
    if manifest["schema"] != SCHEMA or manifest["bundle_version"] != 1:
        raise ValueError("unsupported bundle schema")

    platform_value = require_object(manifest["platform"], {"machine", "system"}, "platform")
    if platform_value != {"machine": machine, "system": system}:
        raise ValueError("bundle platform does not match this runtime")

    metadata = parse_env(ROOT / "UPSTREAM.env")
    provenance = require_object(
        manifest["provenance"],
        {"patched_tree", "patchset_version", "upstream_commit", "upstream_tree"},
        "provenance",
    )
    expected_provenance: dict[str, object] = {
        "patched_tree": metadata["DSFLOWER_XGB_PATCHED_TREE"],
        "patchset_version": int(metadata["DSFLOWER_XGB_PATCHSET_VERSION"]),
        "upstream_commit": metadata["DSFLOWER_XGB_UPSTREAM_COMMIT"],
        "upstream_tree": metadata["DSFLOWER_XGB_UPSTREAM_TREE"],
    }
    if provenance != expected_provenance:
        raise ValueError("bundle source provenance mismatch")

    xgboost = require_object(
        manifest["xgboost"],
        {"mechanism", "path", "privacy_context_abi", "sha256", "status"},
        "xgboost",
    )
    primitive = require_object(
        manifest["dp_primitives"],
        {"abi", "mechanism", "path", "sha256"},
        "dp_primitives",
    )
    if (
        xgboost["privacy_context_abi"] != 3
        or xgboost["status"] != STATUS
        or xgboost["mechanism"] != XGBOOST_MECHANISM
        or primitive["abi"] != 2
        or primitive["mechanism"] != DP_MECHANISM
    ):
        raise ValueError("bundle native contract mismatch")

    xgboost_path = checked_relative_file(
        bundle, xgboost["path"], f"lib/{xgboost_name}"
    )
    primitive_path = checked_relative_file(
        bundle, primitive["path"], f"lib/{primitive_name}"
    )
    if digest(xgboost_path) != require_text(xgboost["sha256"], "XGBoost SHA-256"):
        raise ValueError("XGBoost library SHA-256 mismatch")
    if digest(primitive_path) != require_text(primitive["sha256"], "primitive SHA-256"):
        raise ValueError("DP primitive library SHA-256 mismatch")
    load_and_check(xgboost_path, primitive_path)

    print(f"verified curated bundle {hashlib.sha256(raw).hexdigest()}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} BUNDLE_DIRECTORY")
    main(sys.argv[1])
