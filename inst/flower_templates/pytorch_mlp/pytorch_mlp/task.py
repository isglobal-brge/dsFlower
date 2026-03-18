"""Data loading via manifest-based staging."""

import json
import os

import numpy as np
import pandas as pd


def load_data(context=None):
    """Load training data from manifest directory.

    Reads the manifest.json from either context.node_config["manifest-dir"]
    or the DSFLOWER_MANIFEST_DIR environment variable, then loads the
    data file specified in the manifest.
    """
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

    manifest_path = os.path.join(manifest_dir, "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    data_file = os.path.join(manifest_dir, manifest["data_file"])
    df = pd.read_csv(data_file)

    target_col = manifest["target_column"]
    feat_cols = manifest.get("feature_columns")

    y = df[target_col].values
    if feat_cols:
        X = df[feat_cols].values
    else:
        X = df.drop(columns=[target_col]).values

    return X.astype(np.float32), y.astype(np.float32)
