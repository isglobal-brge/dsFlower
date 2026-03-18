"""Data loading for image classification via manifest-based staging."""

import json
import os

import numpy as np
import pandas as pd
from PIL import Image

import torch
from torch.utils.data import Dataset
from torchvision import transforms


_privacy_config = None

_DEFAULT_TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std=[0.229, 0.224, 0.225]),
])


class ImageDataset(Dataset):
    """Dataset that loads images from a data root directory."""

    def __init__(self, data_root, samples_df, target_col="label",
                 transform=None):
        self.data_root = data_root
        self.samples = samples_df
        self.target_col = target_col
        self.transform = transform or _DEFAULT_TRANSFORM

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        row = self.samples.iloc[idx]
        rel_path = row["relative_path"]

        # Path traversal rejection
        if ".." in rel_path or os.path.isabs(rel_path):
            raise ValueError(f"Invalid relative path: {rel_path}")

        img_path = os.path.join(self.data_root, rel_path)
        image = Image.open(img_path).convert("RGB")
        image = self.transform(image)

        label = row[self.target_col]
        return image, torch.tensor(label, dtype=torch.long)


def load_image_data(context=None):
    """Load image manifest and return (data_root, samples_df, target_col)."""
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)

    data_root = manifest["data_root"]
    target_col = manifest.get("target_column", "label")

    samples_file = os.path.join(manifest_dir, manifest["samples_file"])
    if samples_file.endswith(".parquet"):
        import pyarrow.parquet as pq
        samples_df = pq.read_table(samples_file).to_pandas()
    else:
        samples_df = pd.read_csv(samples_file)

    return data_root, samples_df, target_col


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
