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

def _read_array(path):
    """Read any supported image/volume into a numpy array (no resize)."""
    p = path.lower()
    if p.endswith((".nii", ".nii.gz")):
        import nibabel as nib
        return np.asarray(nib.load(path).get_fdata(), dtype=np.float32)
    if p.endswith(".nrrd"):
        import nrrd
        data, _ = nrrd.read(path)
        return np.asarray(data, dtype=np.float32)
    if p.endswith((".mha", ".mhd", ".dcm")):
        import SimpleITK as sitk
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(path)),
                          dtype=np.float32)
    # 2D raster
    from PIL import Image
    return np.asarray(Image.open(path).convert("RGB"), dtype=np.float32)


def _to_2d_slice(arr):
    """Reduce a 3D volume to a representative 2D slice (middle of the slice axis,
    taken as the smallest-extent axis, which is the slice direction for typical
    HxWxD volumes). 2D input passes through."""
    if arr.ndim <= 2:
        return arr
    if arr.ndim == 3 and arr.shape[-1] in (3, 4) and arr.shape[0] > 4:
        return arr[..., :3]  # already HxWxC RGB(A)
    axis = int(np.argmin(arr.shape))
    return np.take(arr, arr.shape[axis] // 2, axis=axis)


def _normalize01(arr):
    lo, hi = float(np.min(arr)), float(np.max(arr))
    if hi - lo < 1e-8:
        return np.zeros_like(arr, dtype=np.float32)
    return ((arr - lo) / (hi - lo)).astype(np.float32)


def read_image_2d(path, image_size):
    """Return a 3xHxW float32 tensor-ready array for a 2D backbone."""
    import torch.nn.functional as F
    import torch

    arr = _to_2d_slice(_read_array(path))
    arr = _normalize01(arr)
    if arr.ndim == 2:
        arr = np.stack([arr, arr, arr], axis=0)            # gray -> 3ch
    elif arr.ndim == 3:
        arr = np.transpose(arr[..., :3], (2, 0, 1))        # HWC -> CHW
    t = torch.from_numpy(np.ascontiguousarray(arr)).unsqueeze(0)
    t = F.interpolate(t, size=(image_size, image_size),
                      mode="bilinear", align_corners=False)
    return t.squeeze(0).numpy()


def read_image_3d(path, image_size):
    """Return a 1xDxHxW float32 array for a 3D backbone (single-channel volume)."""
    import torch.nn.functional as F
    import torch

    arr = _normalize01(_read_array(path))
    if arr.ndim == 4:
        arr = arr[..., 0]
    t = torch.from_numpy(np.ascontiguousarray(arr))[None, None]
    d = max(16, image_size // 4)
    t = F.interpolate(t, size=(d, image_size, image_size),
                      mode="trilinear", align_corners=False)
    return t.squeeze(0).numpy()


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


def extract_features(backbone, images_np, device="cpu", batch=32):
    """Run the frozen backbone over a stack of images (no grad) -> feature matrix."""
    import torch
    feats = []
    with torch.no_grad():
        for i in range(0, len(images_np), batch):
            xb = torch.from_numpy(np.stack(images_np[i:i + batch])).float().to(device)
            f = backbone(xb)
            feats.append(f.reshape(f.shape[0], -1).cpu().numpy())
    return np.concatenate(feats, axis=0).astype(np.float32)
