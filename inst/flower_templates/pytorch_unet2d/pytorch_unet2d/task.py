"""Data loading for image segmentation via manifest-based staging.

Supports multi-root assets from dsImaging manifests:
- assets.images.root: directory for input images
- assets.masks.root: directory for segmentation masks (can differ from images)
- Configurable path columns via assets.*.path_col
- Feature table joining via assets with type=feature_table

Supported image formats: PNG, JPEG, TIFF, single-frame DICOM.
Supported mask formats: PNG (grayscale), NIfTI (.nii/.nii.gz), NRRD.
"""

import json
import os

import numpy as np
import pandas as pd
from PIL import Image

import torch
from torch.utils.data import Dataset
from torchvision import transforms


_privacy_config = None

_DEFAULT_IMAGE_TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std=[0.229, 0.224, 0.225]),
])

_DEFAULT_MASK_TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224), interpolation=transforms.InterpolationMode.NEAREST),
    transforms.ToTensor(),
])


class SegmentationDataset(Dataset):
    """Dataset that loads images and segmentation masks.

    Supports separate root directories for images and masks (multi-root),
    configurable path columns, and multiple mask formats including PNG,
    NIfTI (.nii/.nii.gz), and NRRD.
    """

    def __init__(self, image_root, samples_df, target_col="mask_path",
                 image_path_col="relative_path", mask_root=None,
                 mask_path_col=None,
                 image_transform=None, mask_transform=None):
        self.image_root = image_root
        self.mask_root = mask_root or image_root
        self.samples = samples_df
        self.target_col = target_col
        self.image_path_col = image_path_col
        self.mask_path_col = mask_path_col or target_col
        self.image_transform = image_transform or _DEFAULT_IMAGE_TRANSFORM
        self.mask_transform = mask_transform or _DEFAULT_MASK_TRANSFORM

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        row = self.samples.iloc[idx]
        img_rel = row[self.image_path_col]
        mask_rel = row[self.mask_path_col]

        # Path traversal rejection
        for p in (img_rel, mask_rel):
            if ".." in str(p) or os.path.isabs(str(p)):
                raise ValueError(f"Invalid relative path: {p}")

        img_path = os.path.join(self.image_root, img_rel)
        mask_path = os.path.join(self.mask_root, mask_rel)

        # Load image (supports PNG, JPEG, TIFF, single-frame DICOM)
        if img_path.lower().endswith(".dcm"):
            image = _load_dicom_as_pil(img_path)
        else:
            image = Image.open(img_path).convert("RGB")

        # Load mask (supports PNG, NIfTI, NRRD)
        mask = _load_mask(mask_path)

        image = self.image_transform(image)
        mask = self.mask_transform(mask)
        mask = (mask > 0.5).float()

        return image, mask


def _load_mask(path):
    """Load a segmentation mask from various formats.

    Supports:
    - PNG/JPEG/TIFF: loaded as grayscale PIL image
    - NIfTI (.nii, .nii.gz): loaded via nibabel, takes first 2D slice
    - NRRD: loaded via pynrrd, takes first 2D slice
    """
    lower = path.lower()

    if lower.endswith((".nii", ".nii.gz")):
        try:
            import nibabel as nib
            img = nib.load(path)
            data = np.asarray(img.dataobj, dtype=np.float32)
            # For 3D volumes, take the middle axial slice
            if data.ndim == 3:
                data = data[:, :, data.shape[2] // 2]
            if data.max() > 0:
                data = (data / data.max() * 255).astype(np.uint8)
            else:
                data = data.astype(np.uint8)
            return Image.fromarray(data, mode="L")
        except ImportError:
            raise ImportError(
                "nibabel is required for NIfTI mask support. "
                "Install it: pip install nibabel"
            )

    if lower.endswith(".nrrd"):
        try:
            import nrrd
            data, _ = nrrd.read(path)
            data = data.astype(np.float32)
            if data.ndim == 3:
                data = data[:, :, data.shape[2] // 2]
            if data.max() > 0:
                data = (data / data.max() * 255).astype(np.uint8)
            else:
                data = data.astype(np.uint8)
            return Image.fromarray(data, mode="L")
        except ImportError:
            raise ImportError(
                "pynrrd is required for NRRD mask support. "
                "Install it: pip install pynrrd"
            )

    # Default: PIL (PNG, JPEG, TIFF, BMP, etc.)
    return Image.open(path).convert("L")


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


def load_image_data(context=None):
    """Load image manifest and return (data_root, samples_df, target_col).

    Supports both old format (data_root at top level) and new format
    (assets.images.root from dsImaging manifests). For segmentation,
    mask root is available via assets.masks.root. Joins feature tables
    from assets if present.

    The returned data_root is the IMAGE root. To get the mask root,
    call get_mask_root() separately.
    """
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)

    assets = manifest.get("assets", {})

    # New format: assets.images.root
    if "images" in assets:
        data_root = assets["images"]["root"]
    elif "data_root" in manifest:
        data_root = manifest["data_root"]
    else:
        raise ValueError("No data_root or assets.images.root in manifest")

    target_col = manifest.get("target_column", "mask_path")

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


def get_mask_root(context=None):
    """Get the mask root directory from manifest assets.

    Returns assets.masks.root if present, otherwise falls back to
    the image data_root (same directory for images and masks).
    """
    manifest = _load_manifest(context)
    assets = manifest.get("assets", {})
    if "masks" in assets:
        return assets["masks"]["root"]
    # Fallback: masks in same dir as images
    if "images" in assets:
        return assets["images"]["root"]
    return manifest.get("data_root")


def get_path_cols(context=None):
    """Get the image and mask path column names from manifest assets.

    Returns (image_path_col, mask_path_col) tuple.
    """
    manifest = _load_manifest(context)
    assets = manifest.get("assets", {})

    image_path_col = "relative_path"
    if "images" in assets:
        image_path_col = assets["images"].get("path_col", "relative_path")

    mask_path_col = manifest.get("target_column", "mask_path")
    if "masks" in assets:
        mask_path_col = assets["masks"].get("path_col", mask_path_col)

    return image_path_col, mask_path_col


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
