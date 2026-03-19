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
    """Dataset that loads images from a data root directory.

    Supports configurable path column via manifest assets.images.path_col.
    Handles 2D single-frame formats: PNG, JPEG, TIFF, and single-frame DICOM.
    """

    def __init__(self, data_root, samples_df, target_col="label",
                 path_col="relative_path", transform=None):
        self.data_root = data_root
        self.samples = samples_df
        self.target_col = target_col
        self.path_col = path_col
        self.transform = transform or _DEFAULT_TRANSFORM

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        row = self.samples.iloc[idx]
        rel_path = row[self.path_col]

        # Path traversal rejection
        if ".." in rel_path or os.path.isabs(rel_path):
            raise ValueError(f"Invalid relative path: {rel_path}")

        img_path = os.path.join(self.data_root, rel_path)

        # DICOM single-frame support
        if img_path.lower().endswith(".dcm"):
            image = _load_dicom_as_pil(img_path)
        else:
            image = Image.open(img_path).convert("RGB")

        image = self.transform(image)

        label = row[self.target_col]
        return image, torch.tensor(label, dtype=torch.long)


def _load_dicom_as_pil(path):
    """Load a single-frame DICOM file as a PIL RGB image."""
    try:
        import pydicom
        ds = pydicom.dcmread(path, force=True)
        arr = ds.pixel_array.astype(np.float32)
        if arr.max() > 0:
            arr = (arr - arr.min()) / (arr.max() - arr.min()) * 255.0
        arr = arr.astype(np.uint8)
        if arr.ndim == 2:
            return Image.fromarray(arr).convert("RGB")
        return Image.fromarray(arr[:, :, :3])
    except ImportError:
        raise ImportError(
            "pydicom is required for DICOM support. "
            "Install it: pip install pydicom"
        )


def get_image_path_col(manifest):
    """Get the path column name from manifest assets or default."""
    assets = manifest.get("assets", {})
    if "images" in assets:
        return assets["images"].get("path_col", "relative_path")
    return "relative_path"


def load_image_data(context=None):
    """Load image manifest and return (data_root, samples_df, target_col).

    Supports both old format (data_root at top level) and new format
    (assets.images.root from dsImaging manifests). Joins feature tables
    from assets if present.
    """
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)

    assets = manifest.get("assets", {})

    # New format: assets.images.root (dsImaging descriptor)
    if "images" in assets:
        data_root = assets["images"]["root"]
    elif "data_root" in manifest:
        data_root = manifest["data_root"]
    else:
        raise ValueError("No data_root or assets.images.root in manifest")

    target_col = manifest.get("target_column", "label")

    samples_file = os.path.join(manifest_dir, manifest["samples_file"])
    if samples_file.endswith(".parquet"):
        import pyarrow.parquet as pq
        samples_df = pq.read_table(samples_file).to_pandas()
    else:
        samples_df = pd.read_csv(samples_file)

    # Join feature tables if present in assets
    for asset_name, asset_def in assets.items():
        if asset_def.get("type") == "feature_table":
            feat_file = asset_def["file"]
            join_key = asset_def.get("join_key")
            if join_key and os.path.exists(feat_file):
                if feat_file.endswith(".parquet"):
                    import pyarrow.parquet as pq
                    feat_df = pq.read_table(feat_file).to_pandas()
                else:
                    feat_df = pd.read_csv(feat_file)
                samples_df = samples_df.merge(feat_df, on=join_key, how="left")

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
