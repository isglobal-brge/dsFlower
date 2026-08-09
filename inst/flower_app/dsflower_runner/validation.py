"""Trusted differentially-private model validation and inference.

The node emits one fixed-layout Gaussian release.  Metrics are computed only
from that release on the researcher-side; exact labels, predictions, counts and
per-site metrics never leave the node.
"""

import base64
import hashlib
import json
import math
import os

import numpy as np

_MAX_BINS = 512
_MAX_CLASSES = 1024
_MAX_VECTOR = 1 << 22
_MAX_MODEL_ABS = 1.0e6
_MAX_PUBLIC_ARRAYS = 256
_MAX_PUBLIC_ELEMENTS = 8_000_000
_MAX_PUBLIC_BYTES = 64 * 1024 * 1024
_INFERENCE_BATCH_ROWS = 1024
_VALIDATION_MECHANISM = "validation-gaussian/v2"
_VALIDATION_FINGERPRINT = "validation-sufficient-v2"


def _integer(value, name, lower, upper):
    if isinstance(value, bool):
        raise ValueError("%s must be an integer" % name)
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("%s must be an integer" % name) from exc
    if (not math.isfinite(number) or number != math.floor(number)
            or not lower <= number <= upper):
        raise ValueError("%s must be in [%d, %d]" % (name, lower, upper))
    return int(number)


def validation_layout(task, *, n_classes=2, n_labels=2, bins=32):
    task = str(task).lower()
    bins = _integer(bins, "validation bins", 4, _MAX_BINS)
    if task == "classification":
        classes = _integer(n_classes, "validation classes", 2, _MAX_CLASSES)
        if classes == 2:
            return {"task": "binary", "bins": bins, "size": 2 * bins,
                    "sensitivity": math.sqrt(2.0)}
        size = classes * classes + classes * 2 * bins
        return {"task": "multiclass", "bins": bins, "classes": classes,
                "size": size, "sensitivity": math.sqrt(2.0 * (classes + 1))}
    if task == "ordinal":
        classes = _integer(n_classes, "validation classes", 2, _MAX_CLASSES)
        size = classes * classes + classes * 2 * bins
        return {"task": "ordinal", "bins": bins, "classes": classes,
                "size": size, "sensitivity": math.sqrt(2.0 * (classes + 1))}
    if task == "multilabel":
        labels = _integer(n_labels, "validation labels", 2, _MAX_CLASSES)
        return {"task": "multilabel", "bins": bins, "labels": labels,
                "size": labels * 2 * bins,
                "sensitivity": math.sqrt(2.0 * labels)}
    if task == "regression":
        return {"task": task, "size": 5, "sensitivity": 2.0}
    if task == "count":
        return {"task": task, "size": 6, "sensitivity": math.sqrt(5.0)}
    raise ValueError("unsupported validation task %r" % task)


def _effective_validation_layout(layout):
    """Return the implemented layout, ignoring non-semantic representation."""
    if not isinstance(layout, dict):
        raise ValueError("invalid validation layout")
    task = str(layout.get("task", "")).lower()
    if task == "binary":
        effective = validation_layout(
            "classification", n_classes=2, bins=layout.get("bins"))
    elif task == "multiclass":
        effective = validation_layout(
            "classification", n_classes=layout.get("classes"),
            bins=layout.get("bins"))
    elif task == "ordinal":
        effective = validation_layout(
            "ordinal", n_classes=layout.get("classes"),
            bins=layout.get("bins"))
    elif task == "multilabel":
        effective = validation_layout(
            "multilabel", n_labels=layout.get("labels"),
            bins=layout.get("bins"))
    elif task in ("regression", "count"):
        effective = validation_layout(task)
    else:
        raise ValueError("invalid validation layout")
    try:
        size = _integer(layout.get("size"), "validation layout size", 1,
                        _MAX_VECTOR)
        sensitivity = float(layout.get("sensitivity"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid validation layout") from exc
    if (size != effective["size"] or not math.isfinite(sensitivity)
            or sensitivity != effective["sensitivity"]):
        raise ValueError("invalid validation layout")
    return effective


def _canonical_sufficient_vector(value, layout):
    effective = _effective_validation_layout(layout)
    vector = np.asarray(value, dtype=np.float64).reshape(-1)
    if (vector.shape != (effective["size"],)
            or not bool(np.all(np.isfinite(vector)))):
        raise ValueError("invalid validation sufficient vector")
    canonical = np.ascontiguousarray(vector.astype("<f8", copy=False))
    if bool(np.any(canonical == 0.0)):
        canonical = canonical.copy()
        canonical[canonical == 0.0] = 0.0
    return canonical


def _validation_noise_key(raw, layout, sigma):
    """Bind sticky validation noise only to the effective DP release."""
    from . import seeding

    effective = _effective_validation_layout(layout)
    canonical = _canonical_sufficient_vector(raw, effective)
    scale = float(sigma)
    if not math.isfinite(scale) or scale <= 0.0:
        raise ValueError("validation noise scale must be finite and positive")
    semantics = {
        "layout": effective,
        "mechanism": _VALIDATION_MECHANISM,
        "sigma": scale,
    }
    encoded = json.dumps(
        semantics, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    privacy = dict(semantics)
    privacy["policy_hash"] = hashlib.sha256(encoded).hexdigest()
    master = seeding.master_seed(
        _VALIDATION_MECHANISM, {"layout": effective}, privacy, 1,
        private_arrays=(canonical,),
        execution_fingerprint=_VALIDATION_FINGERPRINT)
    return seeding.sub_seed(master, "validation-noise/v2")


def layout_from_config(cfg):
    """Validate the public validation contract without reading private data."""
    if not isinstance(cfg, dict):
        raise ValueError("validation configuration must be an object")
    task = str(cfg.get("validation-task", "")).lower()
    loss = str(cfg.get("loss-name", "")).lower()
    bins = cfg.get("validation-bins", 32)
    if loss == "bce_logits" and task != "binary":
        raise ValueError("bce_logits validation is binary only")
    if task == "multilabel" and _integer(
            cfg.get("num-classes", 2), "validation classes", 2,
            _MAX_CLASSES) != 2:
        raise ValueError("multilabel validation requires binary target levels")
    if task == "binary":
        return validation_layout("classification", n_classes=2, bins=bins)
    if task == "multiclass":
        return validation_layout(
            "classification", n_classes=cfg.get("num-classes", 2), bins=bins)
    if task == "ordinal":
        return validation_layout(
            "ordinal", n_classes=cfg.get("num-classes", 2), bins=bins)
    if task == "multilabel":
        return validation_layout(
            "multilabel", n_labels=cfg.get("num-labels", 2), bins=bins)
    if task in ("regression", "count"):
        bounds = target_bounds_from_config(cfg)
        if task == "count" and bounds["lower"] < 0.0:
            raise ValueError("count validation requires non-negative target bounds")
        if loss == "gamma_nll" and bounds["lower"] <= 0.0:
            raise ValueError("gamma validation requires strictly positive target bounds")
        return validation_layout(task, bins=bins)
    raise ValueError("unsupported validation task %r" % task)


def _target_bounds(value):
    if not isinstance(value, dict):
        raise ValueError("numeric validation requires pinned target bounds")
    bounds = (value.get("lower"), value.get("upper"))
    try:
        lower, upper = (float(bounds[0]), float(bounds[1]))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid public target bounds") from exc
    if (not math.isfinite(lower) or not math.isfinite(upper)
            or lower >= upper or abs(lower) > _MAX_MODEL_ABS
            or abs(upper) > _MAX_MODEL_ABS):
        raise ValueError("invalid public target bounds")
    return lower, upper


def target_bounds_from_config(cfg):
    """Read the pinned node form or the scalar TOML ServerApp form."""
    bounds = cfg.get("target-bounds")
    if isinstance(bounds, dict):
        lower, upper = _target_bounds(bounds)
    else:
        lower, upper = _target_bounds({
            "lower": cfg.get("validation-target-lower"),
            "upper": cfg.get("validation-target-upper"),
        })
    return {"lower": lower, "upper": upper}


def neural_predictions(model, X, loss_name):
    """Trusted, total inference for a node-built declarative ``nn.Module``."""
    import torch

    values = np.asarray(X, dtype=np.float32)
    if values.ndim != 2 or not bool(np.all(np.isfinite(values))):
        raise ValueError("validation features must be one finite matrix")
    loss = str(loss_name).lower()
    output_limit = (_MAX_MODEL_ABS
                    if loss in ("mse", "huber", "quantile") else 30.0)

    def interpret(logits):
        logits = torch.clamp(
            torch.nan_to_num(logits, nan=0.0, posinf=output_limit,
                             neginf=-output_limit),
            -output_limit, output_limit)
        if loss in ("mse", "huber", "quantile"):
            return logits.squeeze(-1)
        if loss in ("poisson_nll", "negbin_nll", "gamma_nll"):
            return torch.exp(logits.squeeze(-1))
        if loss == "multilabel_bce":
            return torch.sigmoid(logits)
        if loss == "ordinal":
            cumulative = torch.cummin(torch.sigmoid(logits), dim=-1).values
            parts = [1.0 - cumulative[:, :1]]
            if cumulative.shape[1] > 1:
                parts.append(cumulative[:, :-1] - cumulative[:, 1:])
            parts.append(cumulative[:, -1:])
            probs = torch.clamp(torch.cat(parts, dim=-1), 0.0, 1.0)
            return probs / torch.clamp(
                probs.sum(dim=-1, keepdim=True), min=1.0e-12)
        if loss in ("cross_entropy", "hinge") or (
                logits.ndim > 1 and logits.shape[-1] > 1):
            return torch.softmax(logits, dim=-1)
        return torch.sigmoid(logits.squeeze(-1))

    model.eval()
    predictions = []
    with torch.no_grad():
        if values.shape[0] == 0:
            probe = interpret(model(torch.zeros(
                (1, values.shape[1]), dtype=torch.float32)))
            return probe.cpu().numpy()[:0]
        for start in range(0, values.shape[0], _INFERENCE_BATCH_ROWS):
            logits = model(torch.as_tensor(
                values[start:start + _INFERENCE_BATCH_ROWS],
                dtype=torch.float32))
            predictions.append(interpret(logits).cpu().numpy())
    return np.concatenate(predictions, axis=0)


def _model_track(cfg):
    track = str(cfg.get("validation-model-track", "")).lower()
    if track != "neural":
        raise ValueError("validation model track must be neural")
    return track


def _public_model_path(cfg):
    encoded = cfg.get("validation-model-path-b64")
    if not isinstance(encoded, str) or not encoded or len(encoded) > 16384:
        raise ValueError("validation model path is missing or oversized")
    try:
        raw = base64.b64decode(encoded, validate=True)
        path = raw.decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("validation model path is invalid") from exc
    if not path or "\x00" in path or not os.path.isabs(path):
        raise ValueError("validation model path must be absolute")
    try:
        info = os.stat(path)
    except OSError as exc:
        raise ValueError("validation model artifact is unavailable") from exc
    if not os.path.isfile(path) or info.st_size <= 0 or info.st_size > (1 << 30):
        raise ValueError("validation model artifact is not a bounded regular file")
    return path


def _bounded_public_arrays(arrays, *, max_elements=None):
    element_cap = (_MAX_PUBLIC_ELEMENTS if max_elements is None
                   else int(max_elements))
    if element_cap < 1:
        raise ValueError("validation public model element cap is invalid")
    if not isinstance(arrays, (list, tuple)) or not (
            1 <= len(arrays) <= _MAX_PUBLIC_ARRAYS):
        raise ValueError("validation public model has an invalid array count")
    elements = 0
    size_bytes = 0
    checked = []
    for value in arrays:
        array = np.asarray(value)
        if (array.dtype.kind not in "biuf" or array.ndim > 8 or array.size < 1
                or not bool(np.all(np.isfinite(array)))
                or bool(np.any(np.abs(array) > _MAX_MODEL_ABS))):
            raise ValueError("validation public model contains an invalid array")
        elements += int(array.size)
        size_bytes += int(array.nbytes)
        if (elements > element_cap or size_bytes > _MAX_PUBLIC_BYTES):
            raise ValueError("validation public model exceeds the transport cap")
        checked.append(array)
    return checked


def public_model_arrays(cfg):
    """Load the researcher-side public model which the ServerApp will send."""
    _model_track(cfg)
    path = _public_model_path(cfg)
    import torch
    from . import model_spec
    from .params import get_torch_params

    input_dim = _integer(
        cfg.get("num-features"), "validation feature count", 1, 65536)
    loss = str(cfg.get("loss-name", "")).lower()
    spec = model_spec.read_spec(cfg)
    output_dim = model_spec.output_width(loss, cfg)
    labels = int(cfg.get("num-labels", 2))
    model = model_spec.build_from_spec(
        spec, in_dim=input_dim, out_dim=output_dim, num_labels=labels,
        output_limit=model_spec.output_limit_for_loss(loss))
    state = torch.load(path, map_location="cpu", weights_only=True)
    if isinstance(state, dict) and "state_dict" in state:
        state = state["state_dict"]
    model.load_state_dict(state, strict=True)
    arrays = get_torch_params(model)
    return _bounded_public_arrays(arrays)


def _node_input_dim(context):
    from . import task as task_module
    manifest = task_module._load_manifest(context)
    if manifest.get("data_type") == "image":
        raise ValueError("standalone private validation currently requires tabular data")
    columns = manifest.get("feature_columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError("validation manifest must pin non-empty feature columns")
    patient = manifest.get("patient_column")
    columns = [column for column in columns if column != patient]
    if (not columns or len(set(columns)) != len(columns)
            or any(not isinstance(column, str) or not column for column in columns)):
        raise ValueError("validation feature contract is invalid")
    return len(columns)


def _apply_feature_bounds(X, cfg):
    """Repeat the neural track's public clipped affine transform exactly."""
    values = np.asarray(X, dtype=np.float64)
    bounds = cfg.get("feature-bounds")
    if bounds is None:
        safe = np.where(np.isfinite(values), values, 0.0)
        return np.clip(
            safe, -_MAX_MODEL_ABS, _MAX_MODEL_ABS).astype(np.float32)
    if not isinstance(bounds, dict):
        raise ValueError("validation feature bounds are invalid")
    lower = np.asarray(bounds.get("lower"), dtype=np.float64)
    upper = np.asarray(bounds.get("upper"), dtype=np.float64)
    if (lower.shape != (values.shape[1],) or upper.shape != (values.shape[1],)
            or not bool(np.all(np.isfinite(lower)))
            or not bool(np.all(np.isfinite(upper)))
            or not bool(np.all(lower < upper))
            or bool(np.any(np.abs(lower) > _MAX_MODEL_ABS))
            or bool(np.any(np.abs(upper) > _MAX_MODEL_ABS))):
        raise ValueError("validation feature bounds are invalid")
    center = (lower + upper) / 2.0
    scale = (upper - lower) / 2.0
    safe = np.where(np.isfinite(values), values, center)
    transformed = (np.clip(safe, lower, upper) - center) / scale
    return np.clip(np.nan_to_num(
        transformed, nan=0.0, posinf=_MAX_MODEL_ABS,
        neginf=-_MAX_MODEL_ABS), -_MAX_MODEL_ABS,
        _MAX_MODEL_ABS).astype(np.float32)


def private_model_validation(context, cfg, pcfg, round_index, public_arrays,
                             on_private_start=None):
    """Validate public inputs, then read private data and emit one DP vector."""
    from . import task as task_module
    from .params import load_user_model, set_torch_params

    del round_index  # Operational coordinates are not validation reroll axes.

    layout = layout_from_config(cfg)
    _model_track(cfg)
    input_dim = _node_input_dim(context)
    if not isinstance(public_arrays, (list, tuple)) or not public_arrays:
        raise ValueError("validation needs public model arrays")

    # Everything through model construction/decoding is public and occurs
    # before load_data opens the staged private frame.
    loss = str(cfg.get("loss-name", "")).lower()
    model = load_user_model(cfg, input_dim, loss)
    set_torch_params(model, list(public_arrays))

    empty_public_shape = np.empty((0, input_dim), dtype=np.float64)
    _apply_feature_bounds(empty_public_shape, cfg)

    if on_private_start is not None:
        on_private_start()
    X, y = task_module.load_data(context)
    unit_ids = task_module.load_tabular_patient_ids(context)
    task_module.assert_pinned_unit_count(context, len(y), unit_ids)
    X_model = _apply_feature_bounds(X, cfg)
    predictions = neural_predictions(model, X_model, cfg.get("loss-name"))
    target_bounds = (target_bounds_from_config(cfg)
                     if layout["task"] in ("regression", "count") else None)
    released, _sigma = private_validation_vector(
        y, predictions, layout, epsilon=pcfg["epsilon"],
        delta=pcfg["delta"], target_bounds=target_bounds, num_releases=1,
        unit_ids=unit_ids)
    return [released.astype(np.float64)]


def _finite_array(value, name):
    out = np.asarray(value, dtype=np.float64)
    if not bool(np.all(np.isfinite(out))):
        raise ValueError("%s must be finite" % name)
    return out


def _score_bins(scores, bins):
    return np.minimum((np.clip(scores, 0.0, 1.0) * bins).astype(np.int64),
                      bins - 1)


def _classification_contributions(y, scores, layout):
    n = int(y.shape[0])
    bins = int(layout["bins"])
    task = layout["task"]
    if task == "binary":
        if scores.ndim == 2:
            if scores.shape[1] != 2:
                raise ValueError("binary validation scores must have width 1 or 2")
            scores = scores[:, 1]
        scores = scores.reshape(-1)
        labels = np.clip(np.rint(y.reshape(-1)), 0, 1).astype(np.int64)
        if scores.shape[0] != n:
            raise ValueError("validation label/prediction length mismatch")
        out = np.zeros((n, layout["size"]), dtype=np.float64)
        out[np.arange(n), labels * bins + _score_bins(scores, bins)] = 1.0
        return out

    classes = int(layout["classes"])
    if scores.shape != (n, classes):
        raise ValueError("multiclass validation scores have the wrong shape")
    labels = np.clip(np.rint(y.reshape(-1)), 0, classes - 1).astype(np.int64)
    probs = np.clip(scores, 0.0, 1.0)
    denom = probs.sum(axis=1, keepdims=True)
    probs = np.divide(probs, denom, out=np.full_like(probs, 1.0 / classes),
                      where=denom > 0)
    pred = np.argmax(probs, axis=1)
    out = np.zeros((n, layout["size"]), dtype=np.float64)
    out[np.arange(n), labels * classes + pred] = 1.0
    offset = classes * classes
    for cls in range(classes):
        positive = (labels == cls).astype(np.int64)
        index = offset + (cls * 2 + positive) * bins + _score_bins(probs[:, cls], bins)
        out[np.arange(n), index] = 1.0
    return out


def _multilabel_contributions(y, scores, layout):
    labels = int(layout["labels"])
    if y.ndim != 2 or scores.ndim != 2 or y.shape != scores.shape:
        raise ValueError("multilabel labels and scores must have equal matrices")
    if y.shape[1] != labels:
        raise ValueError("multilabel validation width mismatch")
    n = y.shape[0]
    out = np.zeros((n, layout["size"]), dtype=np.float64)
    truth = np.clip(np.rint(y), 0, 1).astype(np.int64)
    probs = np.clip(scores, 0.0, 1.0)
    for label in range(labels):
        index = ((label * 2 + truth[:, label]) * layout["bins"]
                 + _score_bins(probs[:, label], layout["bins"]))
        out[np.arange(n), index] = 1.0
    return out


def _numeric_contributions(y, predictions, layout, target_bounds):
    if isinstance(target_bounds, dict):
        lower, upper = _target_bounds(target_bounds)
    elif isinstance(target_bounds, (list, tuple)) and len(target_bounds) == 2:
        lower, upper = _target_bounds(
            {"lower": target_bounds[0], "upper": target_bounds[1]})
    else:
        raise ValueError("numeric validation needs public target bounds")
    target = np.clip(y.reshape(-1), lower, upper)
    pred = np.clip(predictions.reshape(-1), lower, upper)
    if target.shape != pred.shape:
        raise ValueError("validation label/prediction length mismatch")
    yn = (target - lower) / (upper - lower)
    pn = (pred - lower) / (upper - lower)
    err = np.abs(yn - pn)
    cols = [np.ones_like(yn), err, err * err, yn, yn * yn]
    if layout["task"] == "count":
        floor = max(1.0e-8, lower)
        safe_pred = np.clip(pred, floor, upper)
        term = np.where(target > 0.0,
                        target * np.log(np.maximum(target, 1.0e-300) / safe_pred)
                        - (target - safe_pred),
                        safe_pred)
        dev = np.maximum(0.0, 2.0 * term)
        candidates = [2.0 * floor, 2.0 * upper]
        if upper > 0.0:
            candidates.append(2.0 * (upper * math.log(upper / floor)
                                     - (upper - floor)))
        cap = max(1.0e-12, *candidates)
        cols.append(np.clip(dev / cap, 0.0, 1.0))
    return np.column_stack(cols)


def validation_contributions(y, predictions, layout, *, target_bounds=None):
    if not isinstance(layout, dict) or int(layout.get("size", 0)) > _MAX_VECTOR:
        raise ValueError("invalid validation layout")
    target = _finite_array(y, "validation targets")
    scores = _finite_array(predictions, "validation predictions")
    if target.shape[0] != scores.shape[0]:
        raise ValueError("validation label/prediction length mismatch")
    task = layout.get("task")
    if task in ("binary", "multiclass", "ordinal"):
        out = _classification_contributions(target, scores, layout)
    elif task == "multilabel":
        out = _multilabel_contributions(target, scores, layout)
    else:
        out = _numeric_contributions(target, scores, layout, target_bounds)
    if out.shape != (target.shape[0], int(layout["size"])):
        raise RuntimeError("validation contribution geometry changed")
    if not bool(np.all(np.isfinite(out))):
        raise RuntimeError("validation contributions became non-finite")
    return out


def _unit_contributions(contributions, unit_ids=None):
    """Return one bounded average contribution per protected unit.

    Row adjacency needs no grouping.  Under patient adjacency, averaging all
    records for one canonical identifier keeps every coordinate in the same
    public [0, 1] domain as a single record.  Consequently the declared
    replace-one sensitivity remains valid regardless of visits per patient.
    """
    values = np.asarray(contributions, dtype=np.float64)
    if unit_ids is None:
        return values
    ids = np.asarray(unit_ids)
    if ids.ndim != 1 or ids.shape[0] != values.shape[0]:
        raise ValueError("validation unit identifiers must match the row count")
    try:
        from . import task as task_module
        canonical = np.asarray(
            [task_module._canonical_patient_id(value) for value in ids],
            dtype=str)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("validation unit identifiers are invalid") from exc
    unique, inverse = np.unique(canonical, return_inverse=True)
    if unique.size == 0:
        return np.empty((0, values.shape[1]), dtype=np.float64)
    sums = np.zeros((unique.size, values.shape[1]), dtype=np.float64)
    counts = np.zeros(unique.size, dtype=np.float64)
    np.add.at(sums, inverse, values)
    np.add.at(counts, inverse, 1.0)
    grouped = sums / counts[:, None]
    if not bool(np.all(np.isfinite(grouped))):
        raise RuntimeError("validation unit contributions became non-finite")
    return grouped


def _stable_index_sum(indices, values, size):
    """Sum sparse non-negative values in a representation-independent order."""
    out = np.zeros(int(size), dtype=np.float64)
    if len(indices) == 0:
        return out
    indices = np.asarray(indices, dtype=np.int64)
    values = np.asarray(values, dtype=np.float64)
    order = np.lexsort((values, indices))
    sorted_indices = indices[order]
    sorted_values = values[order]
    starts = np.r_[0, np.flatnonzero(
        sorted_indices[1:] != sorted_indices[:-1]) + 1]
    out[sorted_indices[starts]] = np.add.reduceat(sorted_values, starts)
    return out


def _patient_histogram_sum(inverse, counts, indices, size):
    """Aggregate one sparse histogram family with deterministic unit weights."""
    if len(indices) == 0:
        return np.zeros(int(size), dtype=np.float64)
    indices = np.asarray(indices, dtype=np.int64)
    order = np.lexsort((indices, inverse))
    sorted_units = inverse[order]
    sorted_indices = indices[order]
    pair_starts = np.r_[0, np.flatnonzero(
        (sorted_units[1:] != sorted_units[:-1])
        | (sorted_indices[1:] != sorted_indices[:-1])) + 1]
    pair_ends = np.r_[pair_starts[1:], len(indices)]
    pair_counts = pair_ends - pair_starts
    pair_units = sorted_units[pair_starts]
    pair_indices = sorted_indices[pair_starts]

    # Use a common dyadic denominator. Integer apportionment by histogram index
    # gives every patient exactly one unit of mass without depending on row
    # order or on the spelling/sort order of the patient identifier.
    denominator = 1 << 52
    quotient = denominator // counts
    remainder = denominator - quotient * counts
    unit_starts = np.cumsum(np.r_[0, counts[:-1]], dtype=np.int64)
    rank = pair_starts - unit_starts[pair_units]
    extra = np.minimum(
        np.maximum(remainder[pair_units] - rank, 0), pair_counts)
    numerators = quotient[pair_units] * pair_counts + extra
    return _stable_index_sum(
        pair_indices, numerators.astype(np.float64) / float(denominator),
        size)


def _stable_numeric_sum(contributions, inverse, counts):
    """Sum numeric sufficient statistics independent of row and ID order."""
    values = np.asarray(contributions, dtype=np.float64)
    total = np.zeros(values.shape[1], dtype=np.float64)
    if values.shape[0] == 0:
        return total
    for column in range(values.shape[1]):
        coordinate = values[:, column]
        if inverse is None:
            total[column] = np.sum(
                np.sort(coordinate), dtype=np.float64)
            continue
        order = np.lexsort((coordinate, inverse))
        starts = np.cumsum(np.r_[0, counts[:-1]], dtype=np.int64)
        unit_sums = np.add.reduceat(coordinate[order], starts)
        unit_means = np.clip(unit_sums / counts, 0.0, 1.0)
        total[column] = np.sum(np.sort(unit_means), dtype=np.float64)
    return total


def _summed_validation_contributions(y, predictions, layout, *,
                                     target_bounds=None, unit_ids=None):
    """Sum unit-bounded contributions without a dense row-by-layout matrix.

    For patient adjacency, each visit receives weight ``1 / visits(patient)``.
    Summing those weighted rows is exactly the sum of per-patient averages, but
    the histogram indices are accumulated directly. Peak workspace therefore
    depends on the released public layout, not ``rows * layout_size``.
    """
    if (not isinstance(layout, dict) or int(layout.get("size", 0)) < 1
            or int(layout.get("size", 0)) > _MAX_VECTOR):
        raise ValueError("invalid validation layout")
    target = _finite_array(y, "validation targets")
    scores = _finite_array(predictions, "validation predictions")
    if target.ndim == 0 or scores.ndim == 0 or target.shape[0] != scores.shape[0]:
        raise ValueError("validation label/prediction length mismatch")
    n_rows = int(target.shape[0])
    inverse = None
    counts = None
    if unit_ids is not None:
        ids = np.asarray(unit_ids)
        if ids.ndim != 1 or ids.shape[0] != n_rows:
            raise ValueError(
                "validation unit identifiers must match the row count")
        try:
            from . import task as task_module
            canonical = np.asarray([
                task_module._canonical_patient_id(value) for value in ids
            ], dtype=str)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ValueError(
                "validation unit identifiers are invalid") from exc
        _unique, inverse, counts = np.unique(
            canonical, return_inverse=True, return_counts=True)

    size = int(layout["size"])
    task = layout.get("task")
    bins = int(layout.get("bins", 0))
    total = np.zeros(size, dtype=np.float64)

    if task == "binary":
        if scores.ndim == 2:
            if scores.shape[1] != 2:
                raise ValueError(
                    "binary validation scores must have width 1 or 2")
            scores = scores[:, 1]
        scores = scores.reshape(-1)
        labels = np.clip(
            np.rint(target.reshape(-1)), 0, 1).astype(np.int64)
        if scores.shape[0] != n_rows:
            raise ValueError("validation label/prediction length mismatch")
        index = labels * bins + _score_bins(scores, bins)
        total[:] = (np.bincount(index, minlength=size)[:size]
                    if inverse is None else _patient_histogram_sum(
                        inverse, counts, index, size))
    elif task in ("multiclass", "ordinal"):
        classes = int(layout["classes"])
        if scores.shape != (n_rows, classes):
            raise ValueError("multiclass validation scores have the wrong shape")
        labels = np.clip(
            np.rint(target.reshape(-1)), 0, classes - 1).astype(np.int64)
        if labels.shape[0] != n_rows:
            raise ValueError("validation label/prediction length mismatch")
        probs = np.clip(scores, 0.0, 1.0)
        denom = probs.sum(axis=1, keepdims=True)
        probs = np.divide(
            probs, denom, out=np.full_like(probs, 1.0 / classes),
            where=denom > 0)
        pred = np.argmax(probs, axis=1)
        confusion_size = classes * classes
        confusion_index = labels * classes + pred
        total[:confusion_size] = (
            np.bincount(
                confusion_index, minlength=confusion_size)[:confusion_size]
            if inverse is None else _patient_histogram_sum(
                inverse, counts, confusion_index, confusion_size))
        stride = 2 * bins
        for cls in range(classes):
            positive = (labels == cls).astype(np.int64)
            index = positive * bins + _score_bins(probs[:, cls], bins)
            start = confusion_size + cls * stride
            total[start:start + stride] = (
                np.bincount(index, minlength=stride)[:stride]
                if inverse is None else _patient_histogram_sum(
                    inverse, counts, index, stride))
    elif task == "multilabel":
        labels = int(layout["labels"])
        if (target.ndim != 2 or scores.ndim != 2
                or target.shape != scores.shape
                or target.shape[1] != labels):
            raise ValueError(
                "multilabel labels and scores must have equal matrices")
        truth = np.clip(np.rint(target), 0, 1).astype(np.int64)
        probs = np.clip(scores, 0.0, 1.0)
        stride = 2 * bins
        for label in range(labels):
            index = (truth[:, label] * bins
                     + _score_bins(probs[:, label], bins))
            start = label * stride
            total[start:start + stride] = (
                np.bincount(index, minlength=stride)[:stride]
                if inverse is None else _patient_histogram_sum(
                    inverse, counts, index, stride))
    elif task in ("regression", "count"):
        contribution = _numeric_contributions(
            target, scores, layout, target_bounds)
        total[:] = _stable_numeric_sum(contribution, inverse, counts)
    else:
        raise ValueError("unsupported validation task %r" % task)
    if not bool(np.all(np.isfinite(total))):
        raise RuntimeError("validation sufficient statistics overflowed")
    return total


def private_validation_vector(y, predictions, layout, *, epsilon, delta,
                              target_bounds=None, num_releases=1,
                              unit_ids=None):
    """Release one semantic-sticky Gaussian sum with replace-one sensitivity."""
    from . import dp_harness, seeding

    effective = _effective_validation_layout(layout)
    raw = _summed_validation_contributions(
        y, predictions, effective, target_bounds=target_bounds,
        unit_ids=unit_ids)
    raw = _canonical_sufficient_vector(raw, effective)
    sigma = dp_harness.compute_output_sigma(
        epsilon, delta, float(effective["sensitivity"]),
        num_releases=num_releases)
    rng = seeding.np_rng(_validation_noise_key(raw, effective, sigma))
    noise = np.asarray(rng.normal(0.0, sigma, size=raw.shape), dtype=np.float64)
    released = raw + noise
    if released.shape != raw.shape or not bool(np.all(np.isfinite(released))):
        raise RuntimeError("private validation release is non-finite")
    return released, float(sigma)


def _safe_ratio(a, b):
    if not (b > 0.0 and math.isfinite(a) and math.isfinite(b)):
        return None
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        value = float(np.divide(a, b))
    return value if math.isfinite(value) else None


def _finite_metric_tree(value):
    """Map overflowed post-processing values to JSON-safe missing metrics."""
    if isinstance(value, dict):
        return {key: _finite_metric_tree(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_metric_tree(item) for item in value]
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return number if math.isfinite(number) else None
    return value


def _area(y, x):
    integrate = getattr(np, "trapezoid", None)
    if integrate is None:  # NumPy < 2.0, still supported by the package.
        integrate = np.trapz
    return float(integrate(y, x))


def _binary_metrics(hist):
    hist = np.maximum(np.asarray(hist, dtype=np.float64), 0.0)
    neg, pos = hist[0], hist[1]
    bins = neg.size
    centers = (np.arange(bins, dtype=np.float64) + 0.5) / bins
    cut = centers >= 0.5
    tn, fp = neg[~cut].sum(), neg[cut].sum()
    fn, tp = pos[~cut].sum(), pos[cut].sum()
    total = tn + fp + fn + tp
    recall = _safe_ratio(tp, tp + fn)
    specificity = _safe_ratio(tn, tn + fp)
    precision = _safe_ratio(tp, tp + fp)
    npv = _safe_ratio(tn, tn + fn)
    f1 = (None if precision is None or recall is None or precision + recall <= 0
          else 2.0 * precision * recall / (precision + recall))
    tpr = np.cumsum(pos[::-1])
    fpr = np.cumsum(neg[::-1])
    if pos.sum() > 0:
        tpr = tpr / pos.sum()
    if neg.sum() > 0:
        fpr = fpr / neg.sum()
    auc = (_area(np.r_[0.0, tpr, 1.0], np.r_[0.0, fpr, 1.0])
           if pos.sum() > 0 and neg.sum() > 0 else None)
    brier = _safe_ratio(
        np.sum(pos * (1.0 - centers) ** 2 + neg * centers ** 2), total)
    bin_total = pos + neg
    observed = np.divide(pos, bin_total, out=np.zeros_like(pos),
                         where=bin_total > 0)
    ece = _safe_ratio(np.sum(bin_total * np.abs(observed - centers)), total)
    tp_curve = np.cumsum(pos[::-1])
    fp_curve = np.cumsum(neg[::-1])
    pr_precision = np.divide(
        tp_curve, tp_curve + fp_curve, out=np.ones_like(tp_curve),
        where=(tp_curve + fp_curve) > 0)
    pr_recall = tp_curve / pos.sum() if pos.sum() > 0 else np.zeros_like(tp_curve)
    pr_auc = (_area(np.r_[1.0, pr_precision], np.r_[0.0, pr_recall])
              if pos.sum() > 0 else None)
    threshold = centers
    threshold_odds = threshold / (1.0 - threshold)
    tp_at = np.cumsum(pos[::-1])[::-1]
    fp_at = np.cumsum(neg[::-1])[::-1]
    prevalence = _safe_ratio(pos.sum(), total)
    net_benefit = ((tp_at - fp_at * threshold_odds) / total
                   if total > 0 else np.zeros_like(threshold))
    treat_all = ((float(prevalence) - (1.0 - float(prevalence)) * threshold_odds)
                 if prevalence is not None else np.zeros_like(threshold))
    return {
        "n": float(total), "accuracy": _safe_ratio(tp + tn, total),
        "sensitivity": recall, "specificity": specificity,
        "precision": precision, "negative_predictive_value": npv,
        "f1": f1, "balanced_accuracy": (
            None if recall is None or specificity is None
            else 0.5 * (recall + specificity)),
        "roc_auc": auc, "pr_auc": pr_auc, "brier": brier,
        "expected_calibration_error": ece,
        "roc": {"fpr": np.r_[0.0, fpr, 1.0].tolist(),
                "tpr": np.r_[0.0, tpr, 1.0].tolist()},
        "precision_recall": {
            "recall": np.r_[0.0, pr_recall].tolist(),
            "precision": np.r_[1.0, pr_precision].tolist()},
        "calibration": {"predicted": centers.tolist(),
                        "observed": observed.tolist(),
                        "weight": bin_total.tolist()},
        "decision_curve": {
            "threshold": threshold.tolist(),
            "net_benefit": net_benefit.tolist(),
            "treat_all": treat_all.tolist(),
            "treat_none": np.zeros_like(threshold).tolist()},
    }


def validation_metrics(released, layout, *, target_bounds=None):
    """Post-process pooled private statistics into researcher-facing metrics."""
    value = np.asarray(released, dtype=np.float64).reshape(-1)
    if value.shape != (int(layout["size"]),) or not bool(np.all(np.isfinite(value))):
        raise ValueError("private validation vector has invalid geometry")
    task = layout["task"]
    if task == "binary":
        return _finite_metric_tree(
            _binary_metrics(value.reshape(2, layout["bins"])))
    if task in ("multiclass", "ordinal"):
        classes, bins = layout["classes"], layout["bins"]
        cm_size = classes * classes
        cm = np.maximum(value[:cm_size].reshape(classes, classes), 0.0)
        total = cm.sum()
        recall = np.divide(np.diag(cm), cm.sum(axis=1),
                           out=np.zeros(classes), where=cm.sum(axis=1) > 0)
        precision = np.divide(np.diag(cm), cm.sum(axis=0),
                              out=np.zeros(classes), where=cm.sum(axis=0) > 0)
        f1 = np.divide(2 * precision * recall, precision + recall,
                       out=np.zeros(classes), where=(precision + recall) > 0)
        hists = value[cm_size:].reshape(classes, 2, bins)
        aucs = [_binary_metrics(hists[i])["roc_auc"] for i in range(classes)]
        out = {
            "n": float(total), "accuracy": _safe_ratio(np.trace(cm), total),
            "balanced_accuracy": float(np.mean(recall)),
            "macro_precision": float(np.mean(precision)),
            "macro_recall": float(np.mean(recall)),
            "macro_f1": float(np.mean(f1)),
            "macro_roc_auc": (float(np.mean([x for x in aucs if x is not None]))
                              if any(x is not None for x in aucs) else None),
            "confusion_matrix": cm.tolist(),
        }
        if task == "ordinal":
            distance = np.abs(np.arange(classes)[:, None]
                              - np.arange(classes)[None, :])
            out["ordinal_mae"] = _safe_ratio(np.sum(cm * distance), total)
        return _finite_metric_tree(out)
    if task == "multilabel":
        hists = value.reshape(layout["labels"], 2, layout["bins"])
        per_label = [_binary_metrics(hists[i]) for i in range(layout["labels"])]
        valid_auc = [m["roc_auc"] for m in per_label if m["roc_auc"] is not None]
        valid_f1 = [m["f1"] for m in per_label if m["f1"] is not None]
        return _finite_metric_tree({
            "labels": per_label,
            "macro_roc_auc": float(np.mean(valid_auc)) if valid_auc else None,
            "macro_f1": float(np.mean(valid_f1)) if valid_f1 else None})
    stats = np.maximum(value, 0.0)
    n, abs_error, squared_error, sum_y, sum_y2 = stats[:5]
    if isinstance(target_bounds, dict):
        lower, upper = _target_bounds(target_bounds)
    elif isinstance(target_bounds, (list, tuple)) and len(target_bounds) == 2:
        lower, upper = _target_bounds(
            {"lower": target_bounds[0], "upper": target_bounds[1]})
    else:
        raise ValueError("numeric validation post-processing needs target bounds")
    scale = upper - lower
    mse_normalized = _safe_ratio(squared_error, n)
    denominator = sum_y2 - (sum_y * sum_y / n) if n > 0 else 0.0
    out = {
        "n": float(n), "mae": (
            None if n <= 0 else float(abs_error / n * scale)),
        "mse": (None if mse_normalized is None
                else float(mse_normalized * scale * scale)),
        "rmse": (None if mse_normalized is None
                 else float(math.sqrt(max(0.0, mse_normalized)) * scale)),
        "r_squared": (None if denominator <= 0
                      else float(1.0 - squared_error / denominator)),
    }
    if task == "count":
        out["mean_poisson_deviance_normalized"] = _safe_ratio(stats[5], n)
    return _finite_metric_tree(out)
