"""Data loading for survival (Cox PH) analysis via manifest-based staging."""

import json
import os

import numpy as np
import pandas as pd


_privacy_config = None


def load_data(context=None):
    """Load survival data from manifest directory.

    Expects target_column to be a list of two column names:
    [time_column, event_column]. Returns (X, time, event).
    """
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)

    data_file = os.path.join(manifest_dir, manifest["data_file"])
    data_format = manifest.get("data_format", "csv")

    if data_format == "parquet":
        import pyarrow.parquet as pq
        df = pq.read_table(data_file).to_pandas()
    else:
        df = pd.read_csv(data_file)

    target_col = manifest["target_column"]
    feat_cols = manifest.get("feature_columns")

    if isinstance(target_col, list) and len(target_col) == 2:
        time_col, event_col = target_col
    else:
        raise ValueError(
            "Cox PH requires target_column to be [time_col, event_col], "
            f"got: {target_col}"
        )

    time = df[time_col].values.astype(np.float32)
    event = df[event_col].values.astype(np.float32)

    drop_cols = [time_col, event_col]
    if feat_cols:
        X = df[feat_cols].values
    else:
        X = df.drop(columns=drop_cols).values

    return X.astype(np.float32), time, event


def load_privacy_config(context=None):
    """Load privacy configuration from manifest.json."""
    global _privacy_config
    if _privacy_config is not None:
        return _privacy_config

    manifest = _load_manifest(context)

    config = {
        "privacy_mode": manifest.get("privacy-mode", "research"),
        "allow_per_node_metrics": manifest.get("allow_per_node_metrics", True),
        "allow_exact_num_examples": manifest.get("allow_exact_num_examples", True),
        "require_secure_aggregation": manifest.get("require_secure_aggregation", False),
        "dp_required": manifest.get("dp_required", False),
        "epsilon": float(manifest.get("privacy-epsilon", 1.0)),
        "delta": float(manifest.get("privacy-delta", 1e-5)),
        "clipping_norm": float(manifest.get("privacy-clipping_norm", 1.0)),
        "n_samples": int(manifest.get("n_samples", 0)),
    }

    _privacy_config = config
    return config


def _get_manifest_dir(context=None):
    """Resolve manifest directory from context or environment."""
    manifest_dir = None
    if context is not None:
        manifest_dir = context.node_config.get("manifest-dir")
    if manifest_dir is None:
        manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR")
    if manifest_dir is None:
        raise ValueError(
            "No manifest directory found. Set 'manifest-dir' in node_config "
            "or DSFLOWER_MANIFEST_DIR environment variable."
        )
    return manifest_dir


def _load_manifest(context=None):
    """Load and return the manifest.json dict."""
    manifest_dir = _get_manifest_dir(context)
    manifest_path = os.path.join(manifest_dir, "manifest.json")
    with open(manifest_path) as f:
        return json.load(f)
