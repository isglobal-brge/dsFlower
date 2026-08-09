#!/usr/bin/env python3
"""Offline integrity and fail-closed-shape tests for the XGBoost patchset."""

from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PINNED_COMMIT = "06335b125dccb859aacef142675506bfb84401b3"


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def main() -> None:
    metadata = parse_env(ROOT / "UPSTREAM.env")
    assert metadata["DSFLOWER_XGB_UPSTREAM_TAG"] == "v3.4.0"
    assert metadata["DSFLOWER_XGB_UPSTREAM_COMMIT"] == PINNED_COMMIT
    assert len(metadata["DSFLOWER_XGB_UPSTREAM_SOURCE_SHA256"]) == 64
    assert len(metadata["DSFLOWER_DMLC_CORE_COMMIT"]) == 40
    assert metadata["DSFLOWER_XGB_PATCHSET_VERSION"] == "2"

    checksums: dict[str, str] = {}
    for line in (ROOT / "PATCHES.sha256").read_text(encoding="utf-8").splitlines():
        digest, name = line.split()
        checksums[name] = digest
    patches = sorted((ROOT / "patches").glob("*.patch"))
    assert patches, "the patchset must not be empty"
    assert set(checksums) == {f"patches/{path.name}" for path in patches}
    for path in patches:
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == checksums[f"patches/{path.name}"]

    patch_text = "\n".join(path.read_text(encoding="utf-8") for path in patches)
    required_guards = (
        "option(PLUGIN_DSFLOWER_DP",
        "grow_dsflower_dp_hist",
        "XGBDsFlowerSetPrivacyContext",
        "XGBDsFlowerClearPrivacyContext",
        "privacy_unit",
        "replace_one",
        "trim-utf8-v2",
        "one-record-per-unit-v1",
        "max_rows_per_unit",
        "XGB_DSFLOWER_PRIVACY_CONTEXT_ABI_VERSION 2U",
        "DMatrixHandle dmatrix",
        "data::SimpleDMatrix",
        "binary:logistic",
        "reg:squarederror",
        "target_lower_bound",
        "base_score",
        "max_trees",
        "max_depth",
        "cut_ptrs",
        "cut_values",
        "SnapshotPrivacyContextForDMatrix",
        "ObjectiveSource::kBinaryLogistic",
        "ObjectiveSource::kSquaredError",
        "rejects custom, external, or mismatched objective gradients",
        "feature_weights.Empty",
        "labels changed after privacy context binding",
        "changed since updater configuration",
        "std::atomic<std::uint64_t> g_context_generation",
        "boost_from_average must remain disabled",
        "requires the exclusive trusted updater",
        "requires CPU execution",
        "requires one tree per boosting round",
        "fail-closed scaffold",
        "privatization has not been implemented or proven",
    )
    for guard in required_guards:
        assert guard in patch_text, f"missing fail-closed guard: {guard}"

    forbidden_claims = ("production-ready DP", "DP guarantee is complete")
    for claim in forbidden_claims:
        assert claim not in patch_text

    allowed_top_level = {
        ".gitattributes",
        "LICENSES.md",
        "PATCHES.sha256",
        "PRIVACY.md",
        "PROVENANCE.md",
        "README.md",
        "UPSTREAM.env",
        "patches",
        "reference",
        "scripts",
        "tests",
    }
    canonical_hasher = (ROOT / "scripts" / "canonical_tree_sha256.py").read_text(
        encoding="utf-8"
    )
    assert "dsflower-git-source-sha256-v1" in canonical_hasher
    assert '"cat-file", "--batch"' in canonical_hasher
    assert {path.name for path in ROOT.iterdir()} <= allowed_top_level
    print("XGBoost patch metadata: ok")


if __name__ == "__main__":
    main()
