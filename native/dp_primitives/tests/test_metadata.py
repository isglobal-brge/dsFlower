#!/usr/bin/env python3
"""Source-pin and ABI metadata checks that do not build native code."""

from __future__ import annotations

import pathlib
import re
import tomllib


ROOT = pathlib.Path(__file__).resolve().parents[1]
OPENDP_COMMIT = "c34d3d04a8872a51af523d9a2244be6171173b7d"
OPENDP_TREE = "90bae3eea427da0a6a408242998b5baefca09a4c"
OPENDP_ARCHIVE_SHA256 = (
    "465cb44a9c4e0caea422ef9650f8cf52fc9ce3c43b6679f97096c965237f82ee"
)
SOURCE_HASHES = {
    "OPENDP_CKS20_SHA256": "9b547a0dfc47d95f7b8f5b1456ffae1938b27d31fa942a96890f78e9c33b9855",
    "OPENDP_BERNOULLI_SHA256": "fa530023e7b5e1ddca0da470831724278de7e12ec15167d53f96e3622967bbee",
    "OPENDP_UNIFORM_SHA256": "a4eb108181f43a3b52a6570b0e3f5fbde25a4f0ccb53998d83cc4ce559696fe3",
    "OPENDP_SAMPLERS_MOD_SHA256": "9b3b73a7a255aba380dd78025c9254995b15864fd2e1de6b33ebc23bf22bafa4",
}


def main() -> int:
    upstream = dict(
        line.split("=", 1)
        for line in (ROOT / "UPSTREAM.env").read_text(encoding="utf-8").splitlines()
        if line
    )
    assert upstream == {
        "OPENDP_REPOSITORY": "https://github.com/opendp/opendp.git",
        "OPENDP_TAG": "v0.15.1",
        "OPENDP_COMMIT": OPENDP_COMMIT,
        "OPENDP_TREE": OPENDP_TREE,
        "OPENDP_ARCHIVE_URL": (
            "https://github.com/opendp/opendp/archive/"
            f"{OPENDP_COMMIT}.tar.gz"
        ),
        "OPENDP_ARCHIVE_SHA256": OPENDP_ARCHIVE_SHA256,
        **SOURCE_HASHES,
    }
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    assert manifest["lib"]["crate-type"] == ["cdylib"]
    dependencies = manifest["dependencies"]
    assert dependencies == {
        "dashu-base": {"version": "=0.4.3", "default-features": False},
        "dashu-int": {
            "version": "=0.4.3",
            "default-features": False,
            "features": ["std"],
        },
        "dashu-ratio": {
            "version": "=0.4.4",
            "default-features": False,
            "features": ["std"],
        },
        "getrandom": {"version": "=0.4.3", "default-features": False},
        "zeroize": {"version": "=1.8.2", "default-features": False},
    }

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    assert 'name = "opendp"' not in lock
    assert 'name = "openssl"' not in lock
    assert 'name = "openssl-src"' not in lock
    for name in dependencies:
        assert f'name = "{name}"' in lock

    notices = (ROOT / "THIRD_PARTY_LICENSES.html").read_text(encoding="utf-8")
    licensed_packages = {
        "cfg-if": "1.0.4",
        "dashu-base": "0.4.3",
        "dashu-int": "0.4.3",
        "dashu-ratio": "0.4.4",
        "getrandom": "0.4.3",
        "libc": "0.2.189",
        "num-modular": "0.6.5",
        "rustversion": "1.0.23",
        "static_assertions": "1.1.0",
        "zeroize": "1.8.2",
    }
    for name, version in licensed_packages.items():
        assert f"{name} {version}" in notices
    assert "Apache License 2.0" in notices

    bundled_notices = (ROOT / "LICENSES.md").read_text(encoding="utf-8")
    assert (
        "Copyright (c) 2022 President and Fellows of Harvard College"
        in bundled_notices
    )
    assert "Permission is hereby granted, free of charge" in bundled_notices
    assert "Copyright 2020 Thomas Steinke" in bundled_notices
    assert "http://www.apache.org/licenses/LICENSE-2.0" in bundled_notices

    header = (ROOT / "include" / "dsflower_dp_primitives.h").read_text(
        encoding="utf-8"
    )
    rust = (ROOT / "src" / "lib.rs").read_text(encoding="utf-8")
    sampler = (ROOT / "src" / "discrete_gaussian.rs").read_text(encoding="utf-8")
    assert re.search(r"DSFLOWER_DP_PRIMITIVES_ABI_VERSION\s+1U", header)
    assert "const ABI_VERSION: u32 = 1;" in rust
    mechanism_id = "cks20-discrete-gaussian-i64-system-random-v1"
    assert mechanism_id in header
    assert mechanism_id in rust
    assert OPENDP_COMMIT in sampler
    assert "getrandom::fill" in sampler
    assert "use openssl" not in sampler.lower()
    print("native DP primitive metadata: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
