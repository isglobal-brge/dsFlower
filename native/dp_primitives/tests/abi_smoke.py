#!/usr/bin/env python3
"""Minimal cross-platform smoke test for the native DP primitive C ABI."""

from __future__ import annotations

import ctypes
import os
import pathlib
import platform
import sys
from concurrent.futures import ThreadPoolExecutor


ROOT = pathlib.Path(__file__).resolve().parents[1]


def _library_path() -> pathlib.Path:
    system = platform.system()
    if system == "Windows":
        names = ("dsflower_dp_primitives.dll",)
    elif system == "Darwin":
        names = ("libdsflower_dp_primitives.dylib",)
    else:
        names = ("libdsflower_dp_primitives.so",)
    target_dir = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target_dir.is_absolute():
        target_dir = ROOT / target_dir
    for profile in ("release", "debug"):
        for name in names:
            candidate = target_dir / profile / name
            if candidate.is_file():
                return candidate
    raise FileNotFoundError("native DP primitive library has not been built")


def _load() -> ctypes.CDLL:
    library = ctypes.CDLL(str(_library_path()))
    library.dsflower_dp_primitives_abi_version.argtypes = []
    library.dsflower_dp_primitives_abi_version.restype = ctypes.c_uint32
    library.dsflower_dp_primitives_mechanism_id.argtypes = []
    library.dsflower_dp_primitives_mechanism_id.restype = ctypes.c_char_p
    library.dsflower_dp_add_discrete_gaussian_i64.argtypes = [
        ctypes.POINTER(ctypes.c_int64),
        ctypes.c_size_t,
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_int64),
    ]
    library.dsflower_dp_add_discrete_gaussian_i64.restype = ctypes.c_int32
    return library


def _sample(library: ctypes.CDLL, length: int = 256) -> tuple[int, ...]:
    array_type = ctypes.c_int64 * length
    source = array_type(*([0] * length))
    output = array_type()
    status = library.dsflower_dp_add_discrete_gaussian_i64(
        source, length, 32, output
    )
    if status != 0:
        raise AssertionError(f"sampler returned status {status}")
    return tuple(output)


def main() -> int:
    library = _load()
    assert library.dsflower_dp_primitives_abi_version() == 1
    assert (
        library.dsflower_dp_primitives_mechanism_id().decode("ascii")
        == "cks20-discrete-gaussian-i64-system-random-v1"
    )
    assert len(_sample(library)) == 256
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: _sample(library), range(8)))
    assert all(len(result) == 256 for result in results)
    print("native DP primitive ABI smoke: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
