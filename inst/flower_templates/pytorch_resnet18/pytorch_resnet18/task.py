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

        # Medical image format support
        lower = img_path.lower()
        if lower.endswith(".dcm"):
            image = _load_dicom_as_pil(img_path)
        elif lower.endswith((".nii", ".nii.gz", ".nrrd")):
            image = _load_volume_as_pil(img_path)
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
        # Normalize to 0-255
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


def _load_volume_as_pil(path):
    """Load a NIfTI or NRRD volume and extract the middle axial slice as PIL RGB."""
    if path.lower().endswith((".nii", ".nii.gz")):
        import nibabel as nib
        vol = nib.load(path).get_fdata()
    elif path.lower().endswith(".nrrd"):
        import nrrd
        vol, _ = nrrd.read(path)
    else:
        raise ValueError(f"Unsupported volume format: {path}")

    # Extract middle axial slice (3D -> 2D)
    if vol.ndim == 3:
        mid = vol.shape[2] // 2
        arr = vol[:, :, mid]
    elif vol.ndim == 4:
        mid = vol.shape[2] // 2
        arr = vol[:, :, mid, 0]
    else:
        arr = vol

    arr = arr.astype(np.float32)
    if arr.max() > arr.min():
        arr = (arr - arr.min()) / (arr.max() - arr.min()) * 255.0
    arr = arr.astype(np.uint8)
    return Image.fromarray(arr).convert("RGB")


class WSIDataset(Dataset):
    """Dataset that extracts tiles from Whole Slide Images.

    Requires openslide-python. Each sample in the metadata table points to
    a WSI file (.svs, .ndpi, .mrxs, .tiff). Tiles are extracted at the
    specified magnification level and tile size.

    Manifest asset config:
        assets.wsi.type: wsi_root
        assets.wsi.root: /path/to/wsi_files
        assets.wsi.path_col: wsi_relpath
        assets.wsi.tile_size: 256
        assets.wsi.magnification: 20
        assets.wsi.overlap: 0
    """

    def __init__(self, wsi_root, samples_df, target_col="label",
                 path_col="wsi_relpath", tile_size=256, magnification=20,
                 overlap=0, transform=None):
        self.wsi_root = wsi_root
        self.samples = samples_df
        self.target_col = target_col
        self.path_col = path_col
        self.tile_size = tile_size
        self.magnification = magnification
        self.overlap = overlap
        self.transform = transform or _DEFAULT_TRANSFORM

        # Pre-compute tile coordinates for all slides
        self._tile_index = self._build_tile_index()

    def _build_tile_index(self):
        """Build a flat index of (sample_idx, x, y) for all tiles."""
        try:
            import openslide
        except ImportError:
            raise ImportError(
                "openslide-python is required for WSI support. "
                "Install it: pip install openslide-python"
            )

        tiles = []
        stride = self.tile_size - self.overlap
        for idx in range(len(self.samples)):
            row = self.samples.iloc[idx]
            rel_path = row[self.path_col]
            wsi_path = os.path.join(self.wsi_root, rel_path)

            slide = openslide.OpenSlide(wsi_path)
            # Find best level for target magnification
            level = self._best_level(slide)
            dims = slide.level_dimensions[level]
            slide.close()

            for y in range(0, dims[1] - self.tile_size + 1, stride):
                for x in range(0, dims[0] - self.tile_size + 1, stride):
                    tiles.append((idx, x, y, level))

        return tiles

    def _best_level(self, slide):
        """Find the level closest to the target magnification."""
        try:
            slide_mag = float(slide.properties.get(
                "openslide.objective-power",
                slide.properties.get("aperio.AppMag", 40)))
        except (ValueError, KeyError):
            slide_mag = 40.0

        target_downsample = slide_mag / self.magnification
        diffs = [abs(d - target_downsample)
                 for d in slide.level_downsamples]
        return diffs.index(min(diffs))

    def __len__(self):
        return len(self._tile_index)

    def __getitem__(self, idx):
        import openslide

        sample_idx, x, y, level = self._tile_index[idx]
        row = self.samples.iloc[sample_idx]

        rel_path = row[self.path_col]
        if ".." in rel_path or os.path.isabs(rel_path):
            raise ValueError(f"Invalid relative path: {rel_path}")

        wsi_path = os.path.join(self.wsi_root, rel_path)
        slide = openslide.OpenSlide(wsi_path)

        # Convert level coords to level-0 coords for read_region
        downsample = slide.level_downsamples[level]
        location = (int(x * downsample), int(y * downsample))
        tile = slide.read_region(location, level,
                                 (self.tile_size, self.tile_size))
        slide.close()

        image = tile.convert("RGB")
        image = self.transform(image)

        label = row[self.target_col]
        return image, torch.tensor(label, dtype=torch.long)


class DICOMSeriesDataset(Dataset):
    """Dataset for DICOM series (multiple .dcm files per study).

    Each sample in the metadata table points to a directory containing
    the .dcm slices of one series. The slices are stacked into a 3D volume,
    and a central slice or MIP is extracted for 2D classification.

    Manifest asset config:
        assets.series.type: dicom_series_root
        assets.series.root: /path/to/series_dirs
        assets.series.path_col: series_relpath
    """

    def __init__(self, series_root, samples_df, target_col="label",
                 path_col="series_relpath", slice_mode="central",
                 transform=None):
        self.series_root = series_root
        self.samples = samples_df
        self.target_col = target_col
        self.path_col = path_col
        self.slice_mode = slice_mode  # "central", "mip", or int index
        self.transform = transform or _DEFAULT_TRANSFORM

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        import pydicom

        row = self.samples.iloc[idx]
        rel_path = row[self.path_col]

        if ".." in str(rel_path) or os.path.isabs(str(rel_path)):
            raise ValueError(f"Invalid relative path: {rel_path}")

        series_dir = os.path.join(self.series_root, rel_path)
        dcm_files = sorted([
            os.path.join(series_dir, f)
            for f in os.listdir(series_dir)
            if f.lower().endswith(".dcm")
        ])

        if not dcm_files:
            raise ValueError(f"No .dcm files in series directory: {series_dir}")

        # Stack slices into volume
        slices = []
        for dcm_path in dcm_files:
            ds = pydicom.dcmread(dcm_path, force=True)
            arr = ds.pixel_array.astype(np.float32)
            slices.append(arr)
        volume = np.stack(slices, axis=-1)

        # Extract 2D slice
        if self.slice_mode == "central":
            slice_idx = volume.shape[-1] // 2
            image_2d = volume[:, :, slice_idx]
        elif self.slice_mode == "mip":
            image_2d = np.max(volume, axis=-1)
        elif isinstance(self.slice_mode, int):
            image_2d = volume[:, :, self.slice_mode]
        else:
            image_2d = volume[:, :, volume.shape[-1] // 2]

        # Normalize to 0-255
        if image_2d.max() > 0:
            image_2d = ((image_2d - image_2d.min()) /
                        (image_2d.max() - image_2d.min()) * 255.0)
        image_2d = image_2d.astype(np.uint8)
        image = Image.fromarray(image_2d).convert("RGB")
        image = self.transform(image)

        label = row[self.target_col]
        return image, torch.tensor(label, dtype=torch.long)


def get_image_path_col(manifest):
    """Get the path column name from manifest assets or default."""
    assets = manifest.get("assets", {})
    if "images" in assets:
        return assets["images"].get("path_col", "relative_path")
    return "relative_path"


def load_image_data(context=None):
    """Load image manifest and return (data_root, samples_df, target_col, assets).

    Supports both old format (data_root at top level) and new format
    (assets.images.root from dsImaging manifests). Returns the full assets
    dict for templates that need multi-root support (e.g., separate mask dirs).
    """
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)

    assets = manifest.get("assets", {})

    # New format: assets.images.root (dsImaging descriptor)
    if "images" in assets:
        data_root = assets["images"]["root"]
    elif "data_root" in manifest:
        # Backward compat: data_root at top level
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
