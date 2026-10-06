"""Pinned binary image/subject contract. Only decoder parameters are released."""

import io
import os
import unicodedata
import warnings

import numpy as np
import torch
import torch.nn.functional as F

PROFILE = "resnet18_layer2_128_v1"
BACKBONE = "resnet18_layer2"
FEATURE_DIM = 128 * 16 * 16
OUTPUT_SHAPE = (1, 128, 128)
CHECKPOINT_SHA256 = "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec"
SELECTION = "canonical-image-id-lexicographic-v1"
PREPROCESSING = "rgb_bilinear_imagenet_128_v1"
PIN_KEYS = (
    "segmentation-alpha", "segmentation-smooth", "mask-vocabulary",
    "segmentation-selection", "segmentation-preprocessing",
    "segmentation-checkpoint-sha256", "segmentation-output-shape",
)


def configure_runtime():
    """This profile uses full float32 convolution and matrix arithmetic."""
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")


def validate_config(cfg):
    """Public-only admission; called before resolving any private path."""
    from .segmentation_checkpoints import checkpoint_id
    checkpoint_id(cfg)
    if "data_type" in cfg:
        if "data-kind" in cfg and cfg["data-kind"] != cfg["data_type"]:
            raise ValueError("segmentation data kind conflicts with manifest")
        cfg = dict(cfg, **{"data-kind": cfg["data_type"]})
    expected = {
        "task-type": "segmentation", "loss-name": "segmentation_bce_dice",
        "data-kind": "image", "backbone": BACKBONE,
        "vision-extractor-profile": PROFILE, "num-features": FEATURE_DIM,
        "image-size": 128, "num-classes": 2,
        "segmentation-selection": SELECTION,
        "segmentation-preprocessing": PREPROCESSING,
        "segmentation-checkpoint-sha256": CHECKPOINT_SHA256,
        "segmentation-output-shape": "1,128,128",
    }
    for key, value in expected.items():
        if type(cfg.get(key)) is not type(value) or cfg[key] != value:
            raise ValueError("segmentation requires pinned %s" % key)
    if (type(cfg.get("segmentation-alpha")) not in (int, float)
            or cfg["segmentation-alpha"] not in (0.5, 1.0)
            or type(cfg.get("segmentation-smooth")) not in (int, float)
            or cfg["segmentation-smooth"] != 1.0):
        raise ValueError("segmentation requires alpha 0.5 or 1 and smoothing 1")
    if cfg.get("mask-vocabulary") not in ("0,1", "0,255"):
        raise ValueError("segmentation requires a declared binary mask vocabulary")
    from . import resampling
    partition_cfg = dict({"dp-unit": "patient", "patient-id-canonicalization": "trim-utf8-v2"}, **cfg)
    if any(str(key).startswith("cv-") for key in cfg):
        resampling.cross_validation_contract_from_manifest(partition_cfg)
    if any(str(key).startswith(("holdout-", "resampling-")) for key in cfg):
        resampling.contract_from_manifest(partition_cfg)
    if any(str(key).startswith("validation-") for key in cfg) and cfg.get("validation-task") != "segmentation":
        raise ValueError("segmentation validation requires its fixed task layout")
    if any(str(key).startswith("hpo-") for key in cfg):
        raise ValueError("segmentation private HPO is unsupported")


def decoder_spec(variant="current"):
    if variant == "pointwise":
        return {"kind": "sequential", "layers": [
            {"op": "reshape", "shape": [128, 16, 16]},
            {"op": "conv2d", "out_channels": 1, "kernel_size": 1},
            {"op": "upsample", "scale_factor": 8},
        ]}
    if variant not in ("current", "narrow"):
        raise ValueError("unknown pinned segmentation decoder")
    first, second = (32, 16) if variant == "current" else (8, 4)
    return {"kind": "sequential", "layers": [
        {"op": "reshape", "shape": [128, 16, 16]},
        {"op": "conv2d", "out_channels": first, "kernel_size": 3, "padding": 1},
        {"op": "relu"}, {"op": "upsample", "scale_factor": 2},
        {"op": "conv2d", "out_channels": second, "kernel_size": 3, "padding": 1},
        {"op": "relu"}, {"op": "upsample", "scale_factor": 4},
        {"op": "conv2d", "out_channels": 1, "kernel_size": 1},
    ]}


def validate_decoder_spec(spec):
    # Normalize without calling the builder (which itself enforces this gate).
    # This keeps the exact decoder allowlist while admitting explicit defaults.
    if __package__:
        from .model_spec import canonicalize_spec
    else:
        from model_spec import canonicalize_spec
    def normalize(value):
        return canonicalize_spec(value, FEATURE_DIM, 1, output_shape=OUTPUT_SHAPE)
    clean = normalize(spec)
    if clean not in [normalize(decoder_spec(name)) for name in ("current", "narrow", "pointwise")]:
        raise ValueError("segmentation requires the pinned convolutional decoder")


def loss_factory(cfg):
    alpha = cfg.get("segmentation-alpha", 0.5)
    smooth = cfg.get("segmentation-smooth", 1.0)
    if (type(alpha) not in (int, float) or alpha not in (0.5, 1.0)
            or type(smooth) not in (int, float) or smooth != 1.0):
        raise ValueError("segmentation requires alpha 0.5 or 1 and smoothing 1")

    def loss(logits, target):
        # Packed target: binary mask plane plus a constant validity plane.
        # All reductions below exclude the subject/batch dimension.
        if (logits.ndim != 4 or tuple(logits.shape[1:]) != OUTPUT_SHAPE
                or tuple(target.shape) != (len(logits), 2, 128, 128)):
            raise ValueError("segmentation loss requires pinned mask/validity shape")
        if len(logits) == 0:
            return logits.sum() * 0.0
        mask, valid = target[:, :1], target[:, 1, 0, 0]
        bce = F.binary_cross_entropy_with_logits(logits, mask, reduction="none")
        bce = bce.flatten(1).mean(1)
        prob = logits.sigmoid().flatten(1)
        pixels = mask.flatten(1)
        dice = (2.0 * (prob * pixels).sum(1) + smooth) / (
            prob.sum(1) + pixels.sum(1) + smooth)
        return (valid * (alpha * bce + (1.0 - alpha) * (1.0 - dice))).mean()
    return loss


def verified_encoder_bytes(cfg=None):
    """Consume admitted encoder bytes, or the fixed custodian-preseeded default."""
    from . import segmentation_checkpoints as checkpoints
    if cfg and checkpoints.checkpoint_id(cfg) is not None:
        path = os.path.join(cfg[checkpoints.DIRECTORY_KEY], "encoder.pth")
        return checkpoints._read(path, 46_830_571, CHECKPOINT_SHA256, 46_830_571)
    # Random decoder runs retain the custodian-preseeded pinned encoder route.
    # This is not a fallback for a selected public initialisation bundle.
    from .vision import verified_backbone_bytes
    return verified_backbone_bytes("resnet18")


def prepare_encoder(cfg):
    """Verify checkpoint and frozen spatial geometry before private reads."""
    from torchvision.models import resnet18
    from .vision import pick_device

    validate_config(cfg)
    configure_runtime()
    checkpoint = verified_encoder_bytes(cfg)
    # Consume exactly the verified bytes. Asking torchvision to load weights
    # here would reopen its cache (or fetch again) after the hash check.
    state = torch.load(io.BytesIO(checkpoint), map_location="cpu", weights_only=True)
    with torch.random.fork_rng(devices=[]):
        net = resnet18(weights=None)
        net.load_state_dict(state, strict=True)
    encoder = torch.nn.Sequential(*list(net.children())[:6])
    encoder.eval()
    encoder.requires_grad_(False)
    device = pick_device()
    encoder.to(device)
    with torch.no_grad():
        probe = encoder(torch.zeros((1, 3, 128, 128), device=device))
    if tuple(probe.shape) != (1, 128, 16, 16) or not torch.isfinite(probe).all():
        raise ValueError("segmentation spatial encoder geometry mismatch")
    return encoder, device


def _open_raster(path, mask=False):
    from PIL import Image
    from .vision import _validate_source_file, _validate_decoded_shape
    path = _validate_source_file(path)
    suffix = os.path.splitext(path)[1].lower()
    if suffix not in ((".png",) if mask else (".png", ".jpg", ".jpeg")):
        raise ValueError("unsupported raster format")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with Image.open(path) as source:
            if source.format not in (("PNG",) if mask else ("PNG", "JPEG")):
                raise ValueError("unsupported raster encoding")
            _validate_decoded_shape((source.height, source.width), np.uint8,
                                    components=len(source.getbands()))
            # Pixel coordinates are used as stored, with no EXIF orientation.
            source.load()
            if mask and source.mode not in ("1", "L", "I", "I;16"):
                raise ValueError("mask must have one declared integer channel")
            return source.copy() if mask else source.convert("RGB")


def _image_tensor(image):
    from PIL import Image
    array = np.asarray(image.resize((128, 128), Image.Resampling.BILINEAR),
                       dtype=np.float32).transpose(2, 0, 1) / 255.0
    return (array - np.asarray([.485, .456, .406], dtype=np.float32)[:, None, None]) / (
        np.asarray([.229, .224, .225], dtype=np.float32)[:, None, None])


def read_pair(image_path, mask_paths, empty_flags, vocabulary):
    """Private record failures map to fixed safe tensors, never deletion."""
    from PIL import Image
    safe = (np.zeros((3, 128, 128), np.float32),
            np.zeros(OUTPUT_SHAPE, np.float32), 0.0)
    try:
        image = _open_raster(image_path)
        union = np.zeros((image.height, image.width), dtype=bool)
        if not mask_paths or len(mask_paths) != len(empty_flags):
            return safe
        foreground = 255 if vocabulary == "0,255" else 1
        for path, empty in zip(mask_paths, empty_flags):
            if isinstance(empty, str):
                empty = {"0": 0, "1": 1, "FALSE": 0, "TRUE": 1}.get(empty, -1)
            if empty not in (0, 1, False, True):
                return safe
            if empty == 1:
                if path is not None:
                    # An explicit empty annotation cannot conceal a nonempty mask.
                    raster = _open_raster(path, mask=True)
                    if raster.size != image.size or np.any(np.asarray(raster) != 0):
                        return safe
                continue
            raster = _open_raster(path, mask=True)
            pixels = np.asarray(raster)
            if raster.size != image.size or not np.isin(pixels, [0, foreground]).all():
                return safe
            union |= pixels == foreground
        mask = np.asarray(Image.fromarray(union.astype(np.uint8)).resize(
            (128, 128), Image.Resampling.NEAREST), dtype=np.float32)[None]
        return _image_tensor(image), mask, 1.0
    except Exception:
        return safe


def extract_prediction_features(encoder, paths, device=None):
    """Analyst-local canonical-grid inference; fixed zero features on invalid input."""
    if device is None:
        device = next(encoder.parameters()).device
    encoder.eval()
    result = np.zeros((len(paths), FEATURE_DIM), np.float32)
    with torch.no_grad():
        for i, path in enumerate(paths):
            try:
                x = _image_tensor(_open_raster(path))
            except Exception:
                continue
            features = encoder(torch.from_numpy(x)[None].to(device)).flatten(1)
            if torch.isfinite(features).all():
                result[i] = features.cpu().numpy()[0]
    return result


def _canonical_image_id(value):
    from .task import _canonical_patient_id, _MISSING_PATIENT_UNIT
    text = _canonical_patient_id(value)
    return "" if text == _MISSING_PATIENT_UNIT else unicodedata.normalize("NFC", text)


def load_subject_tensors(context, cfg, encoder, device, *, include_canonical_units=False):
    """Read paired assets and select once per subject without changing its census."""
    from . import task
    validate_config(cfg)
    manifest = task._load_manifest(context)
    if manifest.get("dp-unit") != "patient":
        raise ValueError("segmentation requires custodian patient privacy")
    assets = manifest.get("assets", {})
    images = assets.get("images", {})
    masks = assets.get(manifest.get("mask_asset", "masks"), {})
    for asset in (images, masks):
        if not isinstance(asset.get("root"), str) or not os.path.isdir(asset["root"]):
            raise ValueError("segmentation paired asset root is unavailable")
    frame = task._read_staged_frame(os.path.join(
        task._get_manifest_dir(context), manifest["samples_file"]), manifest)
    groups = task._load_patient_ids(frame, manifest)
    task.assert_pinned_unit_count(context, len(frame), groups, manifest=manifest)
    sample_col = manifest.get("sample_id_col", "image_id")
    image_col = images.get("path_col", "relative_path")
    mask_col = masks.get("path_col", manifest.get("target_column"))
    empty_col = manifest.get("mask_empty_col")
    columns = [sample_col, image_col, mask_col] + ([empty_col] if empty_col else [])
    if any(column not in frame.columns for column in columns):
        raise ValueError("segmentation samples schema does not match its manifest")

    def resolve(root, value):
        try:
            return task._resolve_image_path(root, value)
        except (OSError, TypeError, ValueError):
            return None

    from .canonical_units import (array_record, canonicalize_units, encode_row,
                                  encode_numeric, frame as frame_bytes, attach_units)
    image_records, mask_records, paths_all, masks_all = [], [], [], []
    cache = {}

    def content(path, is_mask):
        key = (path, is_mask)
        if key not in cache:
            try:
                cache[key] = frame_bytes("raster-decoded-v1", array_record(
                    np.asarray(_open_raster(path, mask=is_mask))))
            except Exception:
                cache[key] = frame_bytes("raster-invalid-v1")
        return cache[key]

    from . import source_projection
    source_rows = source_projection.records(
        context, manifest, frame, [empty_col] if empty_col else [],
        normalizers=({empty_col: lambda value: {"FALSE": 0, "TRUE": 1}.get(value, value)}
                     if empty_col else None))
    ids = [_canonical_image_id(value) for value in frame[sample_col]]
    records = []
    for j in range(len(frame)):
        image_path = resolve(images["root"], frame.iloc[j][image_col])
        mask_value = frame.iloc[j][mask_col]
        mask_path = resolve(masks["root"], mask_value)
        paths_all.append(image_path)
        masks_all.append(mask_path)
        image_records.append(content(image_path, False))
        mask_records.append(content(mask_path, True))
        empty_value = frame.iloc[j][empty_col] if empty_col else 0
        if isinstance(empty_value, str):
            empty_value = {"FALSE": 0, "TRUE": 1}.get(empty_value, empty_value)
        records.append(encode_row((ids[j], image_records[-1], mask_records[-1],
                                   mask_value == "__dsflower_empty_mask__",
                                   source_rows[j] if source_rows is not None else encode_numeric(empty_value))))
    units = canonicalize_units(records, unit_ids=groups)
    order = units.row_permutation
    frame = frame.iloc[order].reset_index(drop=True)
    groups = groups[order]
    ids = [ids[j] for j in order]
    paths_all = [paths_all[j] for j in order]
    masks_all = [masks_all[j] for j in order]
    image_records = [image_records[j] for j in order]
    subjects = units.unit_ids
    X = np.zeros((len(subjects), FEATURE_DIM), np.float32)
    y = np.zeros((len(subjects), 2, 128, 128), np.float32)
    encoder.eval()
    with torch.no_grad():
        for i, subject in enumerate(subjects):
            rows = np.flatnonzero(groups == subject)
            chosen = min(ids[j] for j in rows)
            if not chosen:
                continue
            selected = [j for j in rows if ids[j] == chosen]
            paths = [paths_all[j] for j in selected]
            # Repeated selected IDs may name relocated/repacked aliases of one
            # image; decoded contents, rather than local path strings, decide.
            if paths[0] is None or any(image_records[j] != image_records[selected[0]]
                                       for j in selected):
                continue
            mask_values = [frame.iloc[j][mask_col] for j in selected]
            mask_paths = [masks_all[j] for j in selected]
            empty = [frame.iloc[j][empty_col] if empty_col else 0 for j in selected]
            if any(path is None and value != "__dsflower_empty_mask__"
                   for path, value in zip(mask_paths, mask_values)):
                continue
            x, mask, valid = read_pair(paths[0], mask_paths, empty, cfg["mask-vocabulary"])
            if valid:
                features = encoder(torch.from_numpy(x)[None].to(device)).flatten(1)
                if tuple(features.shape) != (1, FEATURE_DIM) or not torch.isfinite(features).all():
                    continue
                X[i] = features.cpu().numpy()[0]
                y[i, :1] = mask
                y[i, 1] = 1.0
    result = (attach_units(X, units), attach_units(y, units), np.asarray(subjects), len(frame))
    return result + (units,) if include_canonical_units else result


def channel_b_metrics(probabilities, masks):
    """Exact public/authorized-local subject means, never a private release route."""
    probabilities = np.asarray(probabilities)
    masks = np.asarray(masks)
    if probabilities.shape != masks.shape or probabilities.ndim != 4:
        raise ValueError("channel B expects matching [subjects,1,height,width] arrays")
    if (not np.isfinite(probabilities).all() or not np.isin(masks, [0, 1]).all()
            or not len(masks)):
        raise ValueError("channel B requires finite predictions and binary masks")
    pred = (probabilities >= .5).reshape(len(masks), -1)
    true = masks.astype(bool).reshape(len(masks), -1)
    intersection = (pred & true).sum(1)
    denominator = pred.sum(1) + true.sum(1)
    union = (pred | true).sum(1)
    dice = np.divide(2 * intersection, denominator,
                     out=np.ones(len(masks), dtype=float), where=denominator != 0)
    iou = np.divide(intersection, union,
                    out=np.ones(len(masks), dtype=float), where=union != 0)
    foreground = true.any(1)
    return {"mean_dice": float(dice.mean()), "mean_iou": float(iou.mean()),
            "foreground_dice": float(dice[foreground].mean()) if foreground.any() else None,
            "empty_dice": float(dice[~foreground].mean()) if (~foreground).any() else None,
            "foreground_iou": float(iou[foreground].mean()) if foreground.any() else None,
            "empty_iou": float(iou[~foreground].mean()) if (~foreground).any() else None}
