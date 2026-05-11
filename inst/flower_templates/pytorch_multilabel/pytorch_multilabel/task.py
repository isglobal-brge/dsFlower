"""Data loading via manifest-based staging."""

import json
import os

import numpy as np
import pandas as pd


# Module-level privacy config cache
_privacy_config = None


def load_data(context=None):
    """Load training data from manifest directory.

    Reads the manifest.json from either context.node_config["manifest-dir"]
    or the DSFLOWER_MANIFEST_DIR environment variable, then loads the
    data file specified in the manifest.
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

    # Multi-label: target_column is a list of label columns.
    if isinstance(target_col, list) and len(target_col) >= 1:
        drop_cols = target_col
        if feat_cols:
            X = df[feat_cols].values
        else:
            X = df.drop(columns=[c for c in drop_cols if c in df.columns]
                        ).select_dtypes(include=[np.number]).values
        y = df[target_col].values
        return X.astype(np.float32), y.astype(np.float32)

    # Standard: single target column
    y = df[target_col].values
    if feat_cols:
        X = df[feat_cols].values
    else:
        X = df.drop(columns=[target_col]).values

    return X.astype(np.float32), y.astype(np.float32)


def load_privacy_config(context=None):
    """Load privacy configuration from manifest.json.

    Privacy config is written by the server into manifest.json and is
    tamper-proof (the server controls the staging directory). This must
    NOT be read from pyproject.toml which is client-controlled.

    Returns:
        dict with keys: privacy_mode, allow_per_node_metrics,
        allow_exact_num_examples, require_secure_aggregation, dp_required,
        plus DP params (epsilon, delta, clipping_norm) when applicable.
    """
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
