"""Vision support for the dsFlower Tier-1 harness — DP linear-probing on frozen features.

Why this shape (see ARCHITECTURE.md + the DP/image research):
  * DP-SGD noise scales ~ sigma*C*sqrt(d); a full vision net has huge d, so naive
    DP-SGD is poor. We therefore FREEZE a pretrained backbone (no gradients) and
    DP-train only a small HEAD on the extracted features -> tiny effective d, small
    noise (last-layer / linear-probe DP, the dominant practical recipe).
  * The frozen backbone's BatchNorm never enters the trainable graph, so Opacus'
    per-sample-gradient requirement is satisfied without touching the backbone.
  * The COMMUNICATED update is a low-dim head gradient in FEATURE space. dsFlower
    has no Secure Aggregation, so this update is the disclosure vector — but a
    feature-space head gradient is far harder to invert into pixels than a
    full-network gradient, and Opacus noises it. The raw pixels never leave.

Plug-and-play 2D/3D: a 2D backbone (default) handles 2D images directly AND 3D
volumes via a representative slice; a 3D backbone (MONAI, opt-in) handles true
volumes. `feature_dim` is fixed per backbone so the ServerApp can build the head
without the images, and is identical on every node (deterministic weights) so
FedAvg over heads is valid.
"""

import os
import re
import stat
import warnings

import numpy as np

# backbone name -> (feature_dim, is_3d). Fixed so the ServerApp builds the head
# without loading the backbone, and identical across nodes.
_BACKBONES = {
    "resnet18": (512, False),
    "resnet50": (2048, False),
    "densenet121": (1024, False),
    "resnet18_3d": (512, True),
    "densenet121_3d": (1024, True),
}
DEFAULT_BACKBONE = "resnet18"

_VOLUME_EXTS = (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm")
_INVALID_IMAGE = "__dsflower_invalid_image__"
_MAX_IMAGE_SIZE = 512
_MAX_IMAGE_AXIS = 16_384
_MAX_IMAGE_ELEMENTS = 32 * 1024 * 1024
_MAX_IMAGE_SOURCE_BYTES = 256 * 1024 * 1024
_MAX_IMAGE_DECODED_BYTES = 256 * 1024 * 1024
_MAX_MEDICAL_HEADER_BYTES = 1024 * 1024
_MAX_IMAGE_BATCH_BYTES = 128 * 1024 * 1024
_MAX_IMAGE_BATCH_RECORDS = 32

_NRRD_DTYPES = {
    "signed char": "i1", "int8": "i1", "int8_t": "i1",
    "uchar": "u1", "unsigned char": "u1", "uint8": "u1", "uint8_t": "u1",
    "short": "i2", "short int": "i2", "signed short": "i2",
    "signed short int": "i2", "int16": "i2", "int16_t": "i2",
    "ushort": "u2", "unsigned short": "u2", "unsigned short int": "u2",
    "uint16": "u2", "uint16_t": "u2",
    "int": "i4", "signed int": "i4", "int32": "i4", "int32_t": "i4",
    "uint": "u4", "unsigned int": "u4", "uint32": "u4", "uint32_t": "u4",
    "longlong": "i8", "long long": "i8", "long long int": "i8",
    "signed long long": "i8", "signed long long int": "i8",
    "int64": "i8", "int64_t": "i8",
    "ulonglong": "u8", "unsigned long long": "u8",
    "unsigned long long int": "u8", "uint64": "u8", "uint64_t": "u8",
    "float": "f4", "double": "f8",
}


def normalize_backbone(name):
    n = str(name or DEFAULT_BACKBONE).lower()
    aliases = {
        "vision": "resnet18", "pytorch_resnet18": "resnet18",
        "pytorch_densenet121": "densenet121", "densenet": "densenet121",
        "resnet": "resnet18", "vision3d": "densenet121_3d",
        "pytorch_resnet18_3d": "resnet18_3d",
    }
    n = aliases.get(n, n)
    if n not in _BACKBONES:
        raise ValueError(
            f"Unsupported vision backbone '{name}'. Supported: {sorted(_BACKBONES)}")
    return n


def feature_dim_for(backbone):
    return _BACKBONES[normalize_backbone(backbone)][0]


def is_3d_backbone(backbone):
    return _BACKBONES[normalize_backbone(backbone)][1]


# --------------------------------------------------------------------------- #
# Image reading (format-dispatched) + 3D->2D for 2D backbones
# --------------------------------------------------------------------------- #

def _validate_image_size(image_size):
    if (isinstance(image_size, (bool, np.bool_))
            or not isinstance(image_size, (int, np.integer))):
        raise ValueError("image-size must be an integer")
    size = int(image_size)
    if not 1 <= size <= _MAX_IMAGE_SIZE:
        raise ValueError(
            "image-size must be in [1, %d]" % _MAX_IMAGE_SIZE)
    return size


def _validate_source_file(path):
    path = os.fsdecode(os.fspath(path))
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("image source must be a regular file")
    if info.st_size > _MAX_IMAGE_SOURCE_BYTES:
        raise ValueError("image source exceeds the node decode limit")
    return path


def _validate_decoded_shape(shape, dtype, components=1):
    dims = tuple(shape)
    if not 2 <= len(dims) <= 4:
        raise ValueError("image header has an unsupported dimension count")
    if (isinstance(components, (bool, np.bool_))
            or not isinstance(components, (int, np.integer))
            or not 1 <= int(components) <= 1024):
        raise ValueError("image header has an invalid component count")
    total = int(components)
    for raw in dims:
        if isinstance(raw, (bool, np.bool_)):
            raise ValueError("image header has an invalid shape")
        try:
            dim = int(raw)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("image header has an invalid shape") from exc
        if dim != raw or not 1 <= dim <= _MAX_IMAGE_AXIS:
            raise ValueError("image header has an invalid shape")
        if total > _MAX_IMAGE_ELEMENTS // dim:
            raise ValueError("image header exceeds the node element limit")
        total *= dim
    dtype = np.dtype(dtype)
    if dtype.hasobject or dtype.kind not in "buif" or dtype.itemsize < 1:
        raise ValueError("image header has an unsupported dtype")
    if total > _MAX_IMAGE_DECODED_BYTES // int(dtype.itemsize):
        raise ValueError("image header exceeds the node decode limit")
    return total


def _require_bounded_nrrd_header(path):
    with open(path, "rb") as handle:
        prefix = handle.read(_MAX_MEDICAL_HEADER_BYTES + 1)
    header_end = prefix.find(b"\n\n")
    windows_end = prefix.find(b"\r\n\r\n")
    if header_end < 0 and windows_end < 0:
        raise ValueError("NRRD header exceeds the node header limit")


def _require_inline_mha(path):
    with open(path, "rb") as handle:
        prefix = handle.read(_MAX_MEDICAL_HEADER_BYTES + 1)
    for line in prefix.splitlines():
        key, separator, value = line.partition(b"=")
        if separator and key.strip().lower() == b"elementdatafile":
            if value.strip().lower() != b"local":
                raise ValueError("detached MetaImage payloads are not supported")
            return
    raise ValueError("MHA header exceeds the node header limit")


def _simpleitk_dtype(pixel_name):
    name = str(pixel_name).lower()
    match = re.search(r"(8|16|32|64)-bit", name)
    if match is None:
        raise ValueError("medical image header has an unsupported pixel type")
    itemsize = int(match.group(1)) // 8
    return np.dtype("u%d" % itemsize), (2 if "complex" in name else 1)


def _read_array(path):
    """Read any supported image/volume into a numpy array (no resize)."""
    path = _validate_source_file(path)
    p = path.lower()
    if p.endswith((".nii", ".nii.gz")):
        import nibabel as nib
        image = nib.load(path)
        _validate_decoded_shape(image.shape, image.get_data_dtype())
        return np.asarray(image.get_fdata(dtype=np.float32), dtype=np.float32)
    if p.endswith(".nrrd"):
        import nrrd
        _require_bounded_nrrd_header(path)
        header = nrrd.read_header(path)
        if any(str(key).strip().lower() in ("data file", "datafile")
               for key in header):
            raise ValueError("detached NRRD payloads are not supported")
        dtype = _NRRD_DTYPES.get(str(header.get("type", "")).strip().lower())
        if dtype is None:
            raise ValueError("NRRD header has an unsupported dtype")
        _validate_decoded_shape(header.get("sizes", ()), np.dtype(dtype))
        data, _ = nrrd.read(path)
        return np.asarray(data, dtype=np.float32)
    if p.endswith(".mhd"):
        raise ValueError("detached MHD payloads are not supported")
    if p.endswith((".mha", ".dcm")):
        import SimpleITK as sitk
        if p.endswith(".mha"):
            _require_inline_mha(path)
        reader = sitk.ImageFileReader()
        reader.SetFileName(path)
        reader.ReadImageInformation()
        dtype, component_multiplier = _simpleitk_dtype(
            sitk.GetPixelIDValueAsString(reader.GetPixelID()))
        _validate_decoded_shape(
            reader.GetSize(), dtype,
            int(reader.GetNumberOfComponents()) * component_multiplier)
        return np.asarray(sitk.GetArrayFromImage(reader.Execute()), dtype=np.float32)
    # 2D raster
    from PIL import Image
    with Image.open(path) as image:
        bands = max(3, len(image.getbands()))
        _validate_decoded_shape(
            (image.height, image.width), np.dtype("u1"), bands)
        return np.asarray(image.convert("RGB"), dtype=np.float32)


def _to_2d_slice(arr):
    """Reduce a 3D volume to a representative 2D slice (middle of the slice axis,
    taken as the smallest-extent axis, which is the slice direction for typical
    HxWxD volumes). 2D input passes through."""
    if arr.ndim <= 2:
        return arr
    # HxWxC RGB(A): a trailing 3/4 channel axis. (No min-height guard: a small
    # e.g. 4x4x3 image must stay RGB, not be read as a 4-slice volume.) Remaining
    # ambiguity (which axis is the slice direction for a true volume) is documented
    # as "smallest-extent axis"; metadata-driven orientation is future work.
    if arr.ndim == 3 and arr.shape[-1] in (3, 4):
        return arr[..., :3]
    axis = int(np.argmin(arr.shape))
    return np.take(arr, arr.shape[axis] // 2, axis=axis)


def _normalize01(arr):
    with np.errstate(over="ignore", invalid="ignore"):
        arr = np.asarray(arr, dtype=np.float32)
    if not arr.size:
        raise ValueError("empty image array")
    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    lo, hi = float(np.min(arr)), float(np.max(arr))
    if hi - lo < 1e-8:
        return np.zeros_like(arr, dtype=np.float32)
    return ((arr.astype(np.float64) - lo) / (hi - lo)).astype(np.float32)


def read_image_2d(path, image_size):
    """Return one totalized 3xHxW float32 record for a 2D backbone."""
    size = _validate_image_size(image_size)
    import torch.nn.functional as F
    import torch

    output_shape = (3, size, size)
    if path is None or (isinstance(path, str) and path == _INVALID_IMAGE):
        return np.zeros(output_shape, dtype=np.float32)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            raw = _read_array(path)
        arr = _normalize01(_to_2d_slice(raw))
        if arr.ndim == 2:
            arr = np.stack([arr, arr, arr], axis=0)        # gray -> 3ch
        elif arr.ndim == 3:
            channels = int(arr.shape[-1])
            if channels == 1:
                arr = np.repeat(arr, 3, axis=-1)
            elif channels == 2:
                arr = np.concatenate(
                    [arr, np.zeros_like(arr[..., :1])], axis=-1)
            elif channels >= 3:
                arr = arr[..., :3]
            else:
                return np.zeros(output_shape, dtype=np.float32)
            arr = np.transpose(arr, (2, 0, 1))             # HWC -> CHW
        else:
            return np.zeros(output_shape, dtype=np.float32)
        t = torch.from_numpy(np.ascontiguousarray(arr)).unsqueeze(0)
        t = F.interpolate(t, size=(size, size),
                          mode="bilinear", align_corners=False)
        result = t.squeeze(0).numpy().astype(np.float32, copy=False)
        if result.shape != output_shape or not bool(np.all(np.isfinite(result))):
            return np.zeros(output_shape, dtype=np.float32)
        return result
    except Exception:
        # A corrupt private image is one fixed zero record, never a release-level
        # success/failure predicate. Public import/config errors occur above.
        return np.zeros(output_shape, dtype=np.float32)


def read_image_3d(path, image_size):
    """Return one totalized 1xDxHxW float32 record for a 3D backbone."""
    size = _validate_image_size(image_size)
    import torch.nn.functional as F
    import torch

    depth = max(16, size // 4)
    output_shape = (1, depth, size, size)
    if path is None or (isinstance(path, str) and path == _INVALID_IMAGE):
        return np.zeros(output_shape, dtype=np.float32)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            raw = _read_array(path)
        arr = _normalize01(raw)
        if arr.ndim == 2:
            arr = arr[np.newaxis, ...]
        elif arr.ndim == 4:
            arr = arr[..., 0]
        if arr.ndim != 3:
            return np.zeros(output_shape, dtype=np.float32)
        t = torch.from_numpy(np.ascontiguousarray(arr))[None, None]
        t = F.interpolate(t, size=(depth, size, size),
                          mode="trilinear", align_corners=False)
        result = t.squeeze(0).numpy().astype(np.float32, copy=False)
        if result.shape != output_shape or not bool(np.all(np.isfinite(result))):
            return np.zeros(output_shape, dtype=np.float32)
        return result
    except Exception:
        return np.zeros(output_shape, dtype=np.float32)


# --------------------------------------------------------------------------- #
# Frozen backbone + trainable head
# --------------------------------------------------------------------------- #

def build_backbone(backbone):
    """Build a FROZEN (eval, no-grad) feature extractor. Deterministic weights so
    every node shares the same feature space (FedAvg over heads is then valid)."""
    import torch
    import torch.nn as nn

    name = normalize_backbone(backbone)
    feat_dim, is3d = _BACKBONES[name]
    torch.manual_seed(0)  # determinism for any non-pretrained fallback

    if is3d:
        # MONAI is a required dep for volumetric runs. If it is missing we must NOT
        # silently substitute a different (random Conv3d) extractor: that would give
        # this node a DIFFERENT feature space than MONAI nodes and make FedAvg over
        # heads invalid. Fail closed with a clear, actionable message instead.
        try:
            import monai.networks.nets as nets
        except Exception as e:
            raise RuntimeError(
                f"MONAI is required for the 3D backbone '{name}' (volumetric runs) "
                "but is not importable. Install monai on every node so all share the "
                "same frozen feature space (a random-conv fallback would make FedAvg "
                f"over heads invalid). Original error: {e}") from e
        base = name.split("_3d")[0]
        if "densenet" in base:
            net = nets.DenseNet121(spatial_dims=3, in_channels=1, out_channels=feat_dim)
            net.class_layers = nn.Identity()
        else:
            net = nets.resnet18(spatial_dims=3, n_input_channels=1, num_classes=feat_dim)
            net.fc = nn.Identity()
        model = net
    else:
        # Pin an explicit, version-stable weights enum (not "DEFAULT", which can
        # change across torchvision releases) so every node extracts in the SAME
        # feature space. On an air-gapped node without a pre-seeded weights cache,
        # fail closed: a silent random init would diverge per node and break FedAvg.
        import torchvision.models as tvm
        try:
            if name == "resnet18":
                net = tvm.resnet18(weights=tvm.ResNet18_Weights.IMAGENET1K_V1)
                net.fc = nn.Identity()
            elif name == "resnet50":
                net = tvm.resnet50(weights=tvm.ResNet50_Weights.IMAGENET1K_V1)
                net.fc = nn.Identity()
            else:
                net = tvm.densenet121(weights=tvm.DenseNet121_Weights.IMAGENET1K_V1)
                net.classifier = nn.Identity()
        except Exception as e:
            raise RuntimeError(
                f"Could not load pretrained weights for 2D backbone '{name}'. On an "
                "air-gapped node, pre-seed the torchvision weights cache (TORCH_HOME) "
                "so every node shares the SAME frozen feature space; a silent random "
                f"fallback would make FedAvg over heads invalid. Original error: {e}"
            ) from e
        model = net

    model.eval()
    for p in model.parameters():
        p.requires_grad_(False)
    return model, feat_dim


def build_head(feature_dim, n_classes):
    """The only trainable, DP-noised, communicated module: a linear probe on
    frozen features. Binary -> 1 logit (BCE); multiclass -> n_classes (CE)."""
    import torch.nn as nn
    out = 1 if int(n_classes) <= 2 else int(n_classes)
    return nn.Linear(int(feature_dim), out)


def pick_device():
    """cuda when a usable GPU is present (and torch is the CUDA build), else cpu —
    so the SAME harness runs on CPU-only DataSHIELD nodes and transparently uses a
    GPU when one exists. The backbone conv pass is the part that benefits most."""
    import torch
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _image_record_shape(image_size, is_3d):
    size = _validate_image_size(image_size)
    if is_3d:
        return (1, max(16, size // 4), size, size)
    return (3, size, size)


def extract_features_from_paths(backbone, paths, image_size, is_3d,
                                device=None):
    """Decode and embed a path stream in node-bounded image batches.

    Only the compact feature matrix spans the whole cohort. At most one bounded
    float32 image batch plus the record currently being decoded is resident.
    """
    import torch
    shape = _image_record_shape(image_size, is_3d)
    record_bytes = int(np.prod(shape, dtype=np.int64)) * 4
    batch_records = min(
        _MAX_IMAGE_BATCH_RECORDS, _MAX_IMAGE_BATCH_BYTES // record_bytes)
    if batch_records < 1:
        raise ValueError("one resized image exceeds the node batch limit")
    if not paths:
        raise ValueError("image collection is empty")
    if device is None:
        device = pick_device()
    backbone = backbone.to(device)
    reader = read_image_3d if is_3d else read_image_2d
    feats = []
    with torch.no_grad():
        for start in range(0, len(paths), batch_records):
            count = min(batch_records, len(paths) - start)
            images = np.empty((count, *shape), dtype=np.float32)
            for offset, path in enumerate(paths[start:start + count]):
                images[offset] = reader(path, image_size)
            xb = torch.from_numpy(images).to(device)
            f = backbone(xb)
            feats.append(f.reshape(f.shape[0], -1).cpu().numpy())
            del f, xb, images
    return np.concatenate(feats, axis=0).astype(np.float32)
