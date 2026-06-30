"""Data loading + privacy config for the dsFlower Tier-1 harness.

CRITICAL TRUST INVARIANT: all privacy enforcement (epsilon/delta/clip) and the
data location are read from the SERVER-WRITTEN manifest.json, never from the
client-controlled run config (pyproject). The server owns the staging directory,
so the manifest is tamper-proof; the researcher cannot weaken DP through it.
"""

import json
import os

import numpy as np
import pandas as pd

_privacy_config = None


def _get_manifest_dir(context=None):
    manifest_dir = None
    if context is not None:
        manifest_dir = context.node_config.get("manifest-dir")
    if manifest_dir is None:
        manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR")
    if manifest_dir is None:
        raise ValueError(
            "No manifest directory found. Set 'manifest-dir' in node_config or "
            "the DSFLOWER_MANIFEST_DIR environment variable."
        )
    return manifest_dir


def _load_manifest(context=None):
    manifest_path = os.path.join(_get_manifest_dir(context), "manifest.json")
    with open(manifest_path) as f:
        return json.load(f)


def load_data(context=None):
    """Load (X, y) for standard supervised training from the staged manifest."""
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])

    if manifest.get("data_format", "csv") == "parquet":
        import pyarrow.parquet as pq
        df = pq.read_table(data_file).to_pandas()
    else:
        df = pd.read_csv(data_file)

    target_col = manifest["target_column"]
    feat_cols = manifest.get("feature_columns")
    feat = df[feat_cols] if feat_cols else df.drop(columns=[target_col])
    X = feat.to_numpy(dtype=np.float32)

    # Coerce the target to numeric {0,1,...}. Uses is_numeric_dtype (so pandas
    # arrow-backed string columns are handled too, not just object) and stable
    # categorical encoding for factor/string/bool labels; numeric passes through.
    y_series = df[target_col]
    if pd.api.types.is_numeric_dtype(y_series):
        y = y_series.to_numpy()
    else:
        y = y_series.astype("category").cat.codes.to_numpy()
    return X, np.asarray(y, dtype=np.float32)


def load_privacy_config(context=None):
    """Read the tamper-proof DP parameters from the server-written manifest.

    DP is ALWAYS enforced — there is no 'off' path and no profiles. Counts and
    per-node metrics are suppressed/bucketed by default (disclosure backstop).
    """
    global _privacy_config
    if _privacy_config is not None:
        return _privacy_config

    manifest = _load_manifest(context)
    epsilon = float(manifest.get("privacy-epsilon", 3.0))
    delta = float(manifest.get("privacy-delta", 1e-5))
    clipping_norm = float(manifest.get("privacy-clipping_norm", 1.0))
    # Legal ranges (a corrupted value must not silently produce zero/infinite noise that
    # voids the guarantee) + HARDCODED node ceilings (NOT manifest-derived -- a client-
    # influenced manifest cannot raise its own ceiling). Mirrors the unified runner.
    if not (epsilon > 0 and 0.0 < delta < 1.0 and clipping_norm > 0):
        raise ValueError(
            "invalid DP parameters in manifest (need epsilon>0, 0<delta<1, clipping_norm>0)")
    if epsilon > 10.0:
        raise ValueError("privacy epsilon %.4g exceeds the node ceiling (10)" % epsilon)
    if delta > 1e-3:
        raise ValueError("privacy delta %.4g exceeds the node ceiling (1e-3)" % delta)
    if clipping_norm > 100.0:
        raise ValueError("privacy clipping_norm %.4g exceeds the node ceiling (100)" % clipping_norm)
    _privacy_config = {
        "epsilon": epsilon,
        "delta": delta,
        "clipping_norm": clipping_norm,
        "sample_aggregate": bool(float(manifest.get("privacy-sample_aggregate", 0))),
        "sa_min_block": max(1, int(float(manifest.get("privacy-sa_min_block", 64)))),
        "sa_max_blocks": max(1, int(float(manifest.get("privacy-sa_max_blocks", 8)))),
        "allow_per_node_metrics": False,
        "allow_exact_num_examples": False,
        "n_samples": int(manifest.get("n_samples", 0)),
    }
    return _privacy_config
