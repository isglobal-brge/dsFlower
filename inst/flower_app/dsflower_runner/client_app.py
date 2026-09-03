"""dsFlower unified trusted ClientApp (node side) — always-on enforced DP.

The researcher ships only a declarative nn.Module model SPEC + hyperparameters
-- DATA, never code, so nothing the
researcher submits executes in this interpreter; this trusted, node-resident
harness owns every DP-critical step, so the guarantee cannot be bypassed. The
node-written, tamper-proof manifest pins the enforced-DP TRACK and all privacy +
sampling parameters; the client run config can only request, never weaken them.

Tracks, dispatched on the manifest's pinned ``dp-track``:
  * neural — Opacus DP-SGD over a client nn.Module (tabular, or an image sub-mode
    that trains only a head on FROZEN-backbone features). Per-sample clip + noise;
    the loss is harness-owned; the released state_dict is stash-gated.
  * egress — the labelled-weaker fallback: the client's own local_update, wrapped
    in output-perturbation DP (whole-update clip + Gaussian noise). Admission-gated.
  * validation — trusted inference for a public neural artifact followed
    by one patient-bounded Gaussian release of sufficient statistics.
"""

import hashlib
import io
import json
import math
import os
import time

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from flwr.clientapp import ClientApp
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from .task import (load_data, load_image_collection, is_image_run,
                   load_privacy_config, load_dp_track, load_run_pins,
                   load_pinned_run_config)
from .params import get_torch_params, set_torch_params, load_user_model

# RELATIVE imports only: resolve within this trusted package, so an uploaded module on
# sys.path / PYTHONPATH cannot shadow dp_harness and execute in the parent at
# ClientApp import time. (The ClientApp is always loaded as a package -- see the relative
# .task / .params imports above.)
from . import (dp_harness, release_guard, resampling, seeding,
               task as task_module, validation)


app = ClientApp()

_MAX_EGRESS_ARRAYS = 256
_MAX_EGRESS_NDIM = 8
_MAX_EGRESS_ELEMENTS = 8_000_000
_MAX_EGRESS_BYTES = 64 * 1024 * 1024
_MAX_PUBLIC_ARRAY_ABS = dp_harness.MAX_RELEASE_ABS
_NEURAL_SEED_CONFIG_KEYS = frozenset({
    "model-spec-b64", "loss-name", "num-classes", "num-labels",
    "num-features", "feature-bounds", "feature-bounds-b64",
    "backbone", "model", "image-size", "data-kind",
    "target-bounds", "nb-dispersion", "gamma-shape", "huber-delta",
    "quantile-level",
})
_NEURAL_PUBLIC_INIT_POLICY_HASH = hashlib.sha256(
    b"dsflower/neural-public-init-policy/v1").hexdigest()

# Flower 1.31 carries Context.state between isolated ClientApp tasks through its
# in-memory NodeState. These two namespaced records are the only CV accumulator;
# no file, database, process global, or reply transcript is used.
_CV_OOF_META_KEY = "dsflower-cv-oof-meta-v1"
_CV_OOF_TOTAL_KEY = "dsflower-cv-oof-total-v1"


def _reply_cache_allowed(claim):
    """CV replies are deterministic and must not become persisted fold state."""
    return not str(claim.get("operation", "train")).startswith("cv-")


def _neural_seed_contract(cfg, pins, _pcfg, geometry_n_units=None):
    """Exact public inputs which can affect trusted neural execution."""
    run = seeding.select_config(cfg, _NEURAL_SEED_CONFIG_KEYS)
    bounds = _effective_feature_bounds(cfg)
    run.pop("feature-bounds-b64", None)
    if bounds is not None:
        run["feature-bounds"] = bounds
    if cfg.get("backbone") is not None:
        run.pop("model", None)
    config = {
        "run": run,
        "pins": dict(pins),
    }
    if geometry_n_units is not None:
        config["resampling-geometry-n-units"] = int(geometry_n_units)
    return config, {}


def _reply(msg, arrays, hook_executed=None,
           public_preflight_unavailable=False,
           execution_unavailable=False):
    """Return arrays with one constant, data-independent aggregation weight."""
    metrics = {"num-examples": 1}
    if hook_executed is not None:
        metrics["hook-executed"] = int(bool(hook_executed))
    if public_preflight_unavailable:
        metrics["public-preflight-unavailable"] = 1
    if execution_unavailable:
        metrics["execution-unavailable"] = 1
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[np.asarray(a) for a in arrays]),
        "metrics": MetricRecord(metrics),
    }), reply_to=msg)


def _cache_reply(context, claim, arrays, hook_executed=None,
                 public_preflight_unavailable=False,
                 execution_unavailable=False):
    context.state["dsflower-last-release"] = ArrayRecord(
        numpy_ndarrays=[np.asarray(a) for a in arrays])
    meta = {
        "message-id": claim["message_id"],
        "release-index": int(claim["release_index"]),
        "operation": str(claim.get("operation", "train")),
        "fold": int(claim.get("fold", 0)),
    }
    if claim.get("request_id"):
        meta["request-id"] = str(claim["request_id"])
    if hook_executed is not None:
        meta["hook-executed"] = int(bool(hook_executed))
    if public_preflight_unavailable:
        meta["public-preflight-unavailable"] = 1
    if execution_unavailable:
        meta["execution-unavailable"] = 1
    context.state["dsflower-last-release-meta"] = ConfigRecord(meta)


def _replay_reply(context, claim, msg):
    meta = context.state.get("dsflower-last-release-meta")
    arrays = context.state.get("dsflower-last-release")
    cached_request = (meta.get("request-id") if meta is not None else None)
    cached_operation = (meta.get("operation", "train")
                        if meta is not None else None)
    cached_fold = (meta.get("fold", 0) if meta is not None else None)
    exact_request = (
        bool(claim.get("request_id"))
        and cached_request == claim.get("request_id")
        and cached_operation == str(claim.get("operation", "train"))
        and cached_fold == int(claim.get("fold", 0))
        and meta.get("release-index") == int(claim["release_index"])
    ) if meta is not None else False
    if (meta is not None and arrays is not None
            and exact_request):
        hook_status = meta.get("hook-executed")
        return _reply(
            msg, arrays.to_numpy_ndarrays(),
            hook_executed=(bool(hook_status) if hook_status is not None else None),
            public_preflight_unavailable=bool(meta.get(
                "public-preflight-unavailable", 0)),
            execution_unavailable=bool(meta.get(
                "execution-unavailable", 0)))
    # Defensive fallback for inconsistent or externally-mutated Context state.
    # The sticky guard returns ``replay`` only while these exact bytes are cached.
    return _safe_fallback_reply(
        msg, context, claim=claim, execution_unavailable=True)


def _validate_public_egress_arrays(arrays, label="HookApp", max_elements=None):
    """Bound an analyst-supplied public model before any private-data read."""
    element_cap = (_MAX_EGRESS_ELEMENTS if max_elements is None
                   else int(max_elements))
    if element_cap < 1:
        raise RuntimeError("public model element cap must be positive")
    if hasattr(arrays, "to_numpy_ndarrays") and hasattr(arrays, "values"):
        encoded = list(arrays.values())
        if not (1 <= len(encoded) <= _MAX_EGRESS_ARRAYS):
            raise RuntimeError("%s initial arrays exceed the public array-count cap" % label)
        serialized_bytes = 0
        metadata_elements = 0
        metadata_bytes = 0
        for item in encoded:
            data = getattr(item, "data", None)
            if getattr(item, "stype", None) != "numpy.ndarray" or not isinstance(data, bytes):
                raise RuntimeError("%s initial arrays need NumPy tensor encoding" % label)
            serialized_bytes += len(data)
            if serialized_bytes > _MAX_EGRESS_BYTES + _MAX_EGRESS_ARRAYS * 4096:
                raise RuntimeError("%s initial arrays exceed the public model-size cap" % label)
            try:
                stream = io.BytesIO(data)
                version = np.lib.format.read_magic(stream)
                if version == (1, 0):
                    shape, _, dtype = np.lib.format.read_array_header_1_0(
                        stream, max_header_size=4096)
                elif version == (2, 0):
                    shape, _, dtype = np.lib.format.read_array_header_2_0(
                        stream, max_header_size=4096)
                else:
                    raise ValueError("unsupported NPY version")
                if (tuple(shape) != tuple(item.shape)
                        or np.dtype(dtype) != np.dtype(item.dtype)
                        or np.dtype(dtype).kind not in "biuf"
                        or len(shape) > _MAX_EGRESS_NDIM
                        or any(isinstance(dim, bool) or int(dim) < 1 for dim in shape)):
                    raise ValueError("invalid array metadata")
                elements = int(np.prod(shape, dtype=object))
                expected_payload = elements * int(np.dtype(dtype).itemsize)
                if expected_payload != len(data) - stream.tell():
                    raise ValueError("array payload length mismatch")
                metadata_elements += elements
                metadata_bytes += expected_payload
                if (metadata_elements > element_cap
                        or metadata_bytes > _MAX_EGRESS_BYTES):
                    raise ValueError("array cap exceeded")
            except Exception as exc:
                raise RuntimeError(
                    "%s initial array encoding is invalid or oversized" % label) from exc
        arrays = arrays.to_numpy_ndarrays()
    if not isinstance(arrays, (list, tuple)) or not (1 <= len(arrays) <= _MAX_EGRESS_ARRAYS):
        raise RuntimeError("%s initial arrays exceed the public array-count cap" % label)
    total_elements = 0
    total_bytes = 0
    validated = []
    for value in arrays:
        array = np.asarray(value)
        if array.dtype.kind not in "biuf" or array.ndim > _MAX_EGRESS_NDIM:
            raise RuntimeError(
                "%s initial arrays need bounded real numeric tensors" % label)
        if array.size < 1 or not bool(np.all(np.isfinite(array))):
            raise RuntimeError(
                "%s initial arrays must be non-empty and finite" % label)
        if bool(np.any(array > _MAX_PUBLIC_ARRAY_ABS)) or bool(
                np.any(array < -_MAX_PUBLIC_ARRAY_ABS)):
            raise RuntimeError(
                "%s initial arrays exceed the public magnitude cap" % label)
        total_elements += int(array.size)
        total_bytes += int(array.nbytes)
        if (total_elements > element_cap
                or total_bytes > _MAX_EGRESS_BYTES):
            raise RuntimeError("%s initial arrays exceed the public model-size cap" % label)
        validated.append(array)
    return validated


# --------------------------------------------------------------------------- #
# Neural track (Opacus DP-SGD)
# --------------------------------------------------------------------------- #

def _neural_input_dim(context, cfg, manifest_image):
    """Resolve @in from public server-pinned metadata, without opening data."""
    if manifest_image:
        from . import vision
        _backbone, _image_size, feature_dim = vision.require_extractor_config(
            cfg.get("backbone", cfg.get("model", "resnet18")),
            cfg.get("vision-extractor-profile"), cfg.get("num-features"),
            cfg.get("image-size"))
        return feature_dim

    manifest = task_module._load_manifest(context)
    feature_columns = manifest.get("feature_columns")
    if not isinstance(feature_columns, list) or not feature_columns:
        raise RuntimeError(
            "tabular neural manifest must pin non-empty feature_columns")
    if any(not isinstance(column, str) or not column for column in feature_columns):
        raise RuntimeError("manifest feature_columns must be non-empty strings")
    patient_column = manifest.get("patient_column")
    feature_columns = [column for column in feature_columns
                       if column != patient_column]
    if not feature_columns or len(set(feature_columns)) != len(feature_columns):
        raise RuntimeError("manifest feature_columns are empty or duplicated")
    return len(feature_columns)


def _validate_public_neural_arrays(arrays, model):
    """Validate and load the public global model before any private-data read."""
    released = getattr(model, "_module", model)
    expected_count = sum(
        1 for _ in torch.nn.Module.named_parameters(released))
    if hasattr(arrays, "values"):
        received_count = len(list(arrays.values()))
    elif isinstance(arrays, (list, tuple)):
        received_count = len(arrays)
    else:
        received_count = -1
    if received_count != expected_count:
        raise ValueError(
            "neural parameter count mismatch: expected %d, received %d"
            % (expected_count, received_count))
    arrays = _validate_public_egress_arrays(
        arrays, label="neural global model")
    set_torch_params(model, arrays)
    return arrays


def _prepare_neural_model(msg, context, cfg, pcfg, pins):
    """Build and initialize the node-owned model using public inputs only."""
    manifest_image = is_image_run(context)
    cfg_image = str(cfg.get("data-kind", "")).lower() == "image"
    if manifest_image != cfg_image:
        raise RuntimeError(
            "data-kind mismatch: run config says "
            + ("image" if cfg_image else "tabular")
            + " but this node's data is "
            + ("an image collection" if manifest_image else "tabular")
            + ". Use a vision model for imaging collections, a tabular model otherwise.")
    input_dim = _neural_input_dim(context, cfg, manifest_image)
    seed_config, _ = _neural_seed_contract(cfg, pins, {})
    public_master = seeding.master_seed(
        "neural-public-init/v1", seed_config,
        {"policy_hash": _NEURAL_PUBLIC_INIT_POLICY_HASH},
        int(pins["round_index"]))
    seeding.seed_torch(seeding.sub_seed(public_master, "init"))
    model = load_user_model(cfg, input_dim, pins["loss_name"])
    _validate_public_neural_arrays(msg.content["arrays"], model)
    return model, input_dim, manifest_image


def _prepare_neural_evaluation_model(msg, context, cfg, pins):
    """Load a public fold model without consulting the custodial PRF."""
    manifest_image = is_image_run(context)
    cfg_image = str(cfg.get("data-kind", "")).lower() == "image"
    if manifest_image != cfg_image:
        raise RuntimeError("cross-validation data-kind does not match its manifest")
    input_dim = _neural_input_dim(context, cfg, manifest_image)
    # Initial values are overwritten by the complete public ArrayRecord. A fixed
    # public seed keeps construction deterministic without making accumulation a
    # privacy-randomness operation.
    with torch.random.fork_rng(devices=[]):
        torch.manual_seed(0)
        model = load_user_model(cfg, input_dim, pins["loss_name"])
    _validate_public_neural_arrays(msg.content["arrays"], model)
    return model, input_dim, manifest_image


def _assert_finite_private_inputs(X, y):
    """Fail closed before non-finite values can invalidate gradient clipping."""
    if not np.all(np.isfinite(X)) or not np.all(np.isfinite(y)):
        raise RuntimeError("DP-SGD inputs must contain only finite values")


def _totalize_private_features(X):
    """Map every feature to the fixed finite domain before patient pooling."""
    try:
        values = np.asarray(X, dtype=np.float64)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError("model features must be numeric") from exc
    values = np.nan_to_num(
        values, nan=0.0, posinf=dp_harness.MAX_PARAMETER_ABS,
        neginf=-dp_harness.MAX_PARAMETER_ABS)
    return np.clip(
        values, -dp_harness.MAX_PARAMETER_ABS,
        dp_harness.MAX_PARAMETER_ABS).astype(np.float32)


def _assert_finite_release(model):
    """Never release a parameter for which the finite-sensitivity path failed."""
    released = getattr(model, "_module", model)
    if any(not bool(torch.isfinite(p).all())
           for _, p in torch.nn.Module.named_parameters(released)):
        raise RuntimeError("DP-SGD produced a non-finite parameter; refusing release")


def _prep_target(y, loss_name, n_classes):
    """Target tensor shaped for the harness-owned loss. n_classes is node-pinned and
    only used by encodings that need the class/level count (ordinal)."""
    if loss_name in ("cross_entropy", "hinge"):
        return torch.from_numpy(y).long()              # [N], multi-logit output (CE / hinge-SVM)
    if loss_name == "multilabel_bce":
        return torch.from_numpy(y).float()             # [N, L]
    if loss_name == "ordinal":
        # CORN cumulative encoding: K-1 binary tasks, column j = 1{level > j}. The
        # head emits K-1 logits; the node builds the target so the client sends only
        # the ordinal level (0..K-1) and cannot mis-shape it.
        levels = torch.from_numpy(y).long().reshape(-1)              # [N] in 0..K-1
        thresholds = torch.arange(max(2, int(n_classes)) - 1)       # [K-1]
        return (levels.unsqueeze(1) > thresholds.unsqueeze(0)).float()   # [N, K-1]
    return torch.from_numpy(y).float().unsqueeze(1)    # [N, 1] (bce/mse/poisson/count/gamma)


def _build_optimizer(model, pins):
    """Construct only a node-trusted optimizer from the pinned public config."""
    lr = float(pins["learning_rate"])
    cfg = dict(pins["optimizer"])
    name = cfg["name"]
    common = {"lr": lr, "weight_decay": float(cfg["weight_decay"])}
    if name == "sgd":
        return torch.optim.SGD(
            model.parameters(), momentum=float(cfg["momentum"]),
            nesterov=bool(cfg["nesterov"]), **common)
    if name in ("adam", "adamw"):
        cls = torch.optim.Adam if name == "adam" else torch.optim.AdamW
        return cls(
            model.parameters(), betas=(float(cfg["beta1"]), float(cfg["beta2"])),
            eps=float(cfg["eps"]), amsgrad=bool(cfg["amsgrad"]), **common)
    if name == "rmsprop":
        return torch.optim.RMSprop(
            model.parameters(), alpha=float(cfg["rmsprop_alpha"]),
            eps=float(cfg["eps"]), momentum=float(cfg["momentum"]), **common)
    raise RuntimeError("optimizer is not on the trusted allowlist")


def _scheduled_learning_rate(pins, global_epoch):
    """Return the pinned global-epoch LR; schedules persist across rounds."""
    cfg = dict(pins["scheduler"])
    name = cfg["name"]
    base = float(pins["learning_rate"])
    local_epochs = int(pins["local_epochs"])
    num_rounds = int(pins["num_rounds"])
    total_epochs = local_epochs * num_rounds
    epoch = int(global_epoch)
    if epoch < 0 or epoch >= total_epochs:
        raise RuntimeError("scheduler global epoch is outside the pinned horizon")
    if name == "none":
        value = base
    elif name == "step":
        value = base * math.pow(
            float(cfg["gamma"]), epoch // int(cfg["step_size"]))
    elif name == "exponential":
        value = base * math.pow(float(cfg["gamma"]), epoch)
    elif name == "cosine":
        minimum = float(cfg["min_lr"])
        progress = epoch / max(1, total_epochs - 1)
        value = minimum + 0.5 * (base - minimum) * (
            1.0 + math.cos(math.pi * progress))
    else:
        raise RuntimeError("scheduler is not on the trusted allowlist")
    if not math.isfinite(value) or not 0.0 <= value <= task_module._MAX_LEARNING_RATE:
        raise RuntimeError("scheduled learning rate is outside the trusted range")
    return float(value)


def _dp_fit(model, X, y, pcfg, pins, n_staged, cfg, master, noise_multiplier,
            geometry_n_units=None, public_zero_gradient=False):
    """Opacus DP-SGD with the harness-owned loss + manifest-pinned sampling/horizon.
    Every input to the noise calibration (clip C, epsilon, delta, batch size, local
    epochs, rounds, sample count) is authoritative from the manifest, never the
    client run config -- so the client cannot stretch the composition horizon
    against the fixed per-training calibration."""
    if type(public_zero_gradient) is not bool:
        raise RuntimeError("zero-gradient mode must be a boolean")
    if public_zero_gradient and (
            geometry_n_units is None or len(X) != 1
            or bool(np.any(np.asarray(X) != 0.0))
            or bool(np.any(np.asarray(y) != 0.0))):
        raise RuntimeError(
            "zero-gradient mode requires one public zero dummy and pinned geometry")
    _assert_finite_private_inputs(X, y)
    X = np.clip(X, -dp_harness.MAX_PARAMETER_ABS,
                dp_harness.MAX_PARAMETER_ABS).astype(np.float32)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    loss_name = pins["loss_name"]
    batch_size = int(pins["batch_size"])
    local_epochs = int(pins["local_epochs"])
    num_rounds = int(pins["num_rounds"])

    # Penalized regression (ridge / lasso / elastic-net): L2 (weight_decay) is applied
    # INSIDE the optimizer to the NOISED grad + PUBLIC weights; L1 as a proximal
    # soft-threshold on the PUBLIC weights after the step. Both are post-processing of
    # already-DP quantities -> no privacy lever (DP is immune to post-processing).
    # Validated non-negative; both 0 -> identical to the plain path.
    l1_penalty = float(pins["optimizer"]["l1_penalty"])
    model = model.to(device)
    optimizer = _build_optimizer(model, pins)
    dataset = TensorDataset(torch.from_numpy(X).float(),
                            _prep_target(y, loss_name, int(pins["n_classes"])))
    # Anti-shrink: the manifest n_samples is the server-recorded count of the STAGED
    # (pre-pool) frame; assert it matches so the client can't shrink the accountant
    # denominator. n_staged is pre-pool and equals the manifest, so legitimate
    # per-patient pooling does not trip this check. Resampling separately pins the
    # mechanism geometry to the manifest's total privacy-unit census.
    if pcfg.get("n_samples") and int(pcfg["n_samples"]) != int(n_staged):
        raise RuntimeError("staged sample count != manifest n_samples (fail closed)")
    # This loader pins the horizon only. The harness replaces it with independent
    # Poisson draws driven directly by the domain-separated ChaCha20 stream.
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
    model, optimizer, trainloader, _engine = dp_harness.make_private_dpsgd(
        model, optimizer, trainloader,
        clipping_norm=pcfg["clipping_norm"], epsilon=pcfg["epsilon"],
        delta=pcfg["delta"], local_epochs=local_epochs, num_rounds=num_rounds,
        noise_multiplier=noise_multiplier,
        n_samples=(len(dataset) if geometry_n_units is None
                   else int(geometry_n_units)), batch_size=batch_size,
        secure_noise_rng=seeding.np_rng(seeding.sub_seed(master, "noise")),
        secure_sampling_rng=seeding.np_rng(seeding.sub_seed(master, "sample")))
    round_index = int(pins.get("round_index", 0))
    if round_index < 1 or round_index > num_rounds:
        raise RuntimeError("neural round index is outside the pinned horizon")

    criterion = dp_harness.loss_from_allowlist(loss_name, cfg)   # node allowlist, mean reduction
    # Deterministic dropout masks; sampling remains on its separate ChaCha stream.
    seeding.seed_torch(seeding.sub_seed(master, "train"))
    model.train()
    for local_epoch in range(local_epochs):
        global_epoch = (round_index - 1) * local_epochs + local_epoch
        scheduled_lr = _scheduled_learning_rate(pins, global_epoch)
        for group in optimizer.param_groups:
            group["lr"] = scheduled_lr
        for xb, yb in trainloader:
            xb, yb = xb.to(device), yb.to(device)
            clean = [p.detach().clone() for p in model.parameters()]
            optimizer.zero_grad()
            output = model(xb)
            loss = (output.sum() * 0.0 if public_zero_gradient
                    else criterion(output, yb))
            loss.backward()
            # Defense-in-depth for the param-stash (A1): undo ANY in-place write the
            # forward made to the leaf params BEFORE the optimizer steps, so weights
            # evolve ONLY via the (noised) DP-SGD update, never a forward side effect.
            # (assert_stock_architecture already forbids custom forwards; this guards
            # a missed vector -- the grad_sample Opacus uses is untouched by .data.)
            with torch.no_grad():
                for p, c in zip(model.parameters(), clean):
                    p.copy_(c)
            optimizer.step()
            with torch.no_grad():
                for p in model.parameters():
                    # Saturating parameters is post-processing of the noised
                    # optimizer step and keeps every subsequent forward inside
                    # the declared finite numeric domain.
                    p.nan_to_num_(nan=0.0,
                                  posinf=dp_harness.MAX_PARAMETER_ABS,
                                  neginf=-dp_harness.MAX_PARAMETER_ABS)
                    p.clamp_(-dp_harness.MAX_PARAMETER_ABS,
                             dp_harness.MAX_PARAMETER_ABS)
                if l1_penalty > 0.0:    # proximal on already-DP public weights
                    current_lr = float(optimizer.param_groups[0]["lr"])
                    thr = l1_penalty * current_lr
                    for p in model.parameters():
                        p.copy_(torch.sign(p) * torch.clamp(p.abs() - thr, min=0.0))
    # RELEASE-TIME gate (the load-time assert_releasable is NOT enough on its own:
    # a buffer / frozen param / new parameter registered lazily inside the
    # researcher's forward appears only AFTER load, and Opacus noises ONLY the
    # gradients of the parameters that existed when make_private was called -- so a
    # lazily-stashed tensor would ship raw + un-noised in the state_dict). Re-assert
    # releasability on the ACTUAL post-training module AND require its released
    # key-set to be EXACTLY the set validated at load. Fail closed on any drift.
    released = getattr(model, "_module", model)
    _assert_finite_release(model)
    dp_harness.assert_releasable(released)
    expected = getattr(released, "_dsflower_release_keys", None)
    current = tuple(n for n, _ in torch.nn.Module.named_parameters(released))
    if expected is None or current != tuple(expected):
        raise RuntimeError(
            "released tensor set changed during training (weight-stash channel: a "
            "lazily-added buffer or parameter); refusing to release un-noised data.")
    return get_torch_params(model), len(dataset)


# Only classification losses constrain labels to [0, n_classes); regression (mse) and
# count (poisson_nll) targets are continuous, and multilabel targets are 2D -- none of
# them carry a class-range invariant, so the label-range check must not run for them.
_CLASSIFICATION_LOSSES = ("bce_logits", "cross_entropy", "hinge", "ordinal")


def _assert_label_range(y, n_classes):
    """The head width is fixed by num-classes; verify the labels fit it. Generic
    message (no exact counts -> no disclosure)."""
    if not len(y):
        raise RuntimeError("no labelled samples to train on")
    max_label = int(np.nanmax(y))
    n_distinct = int(np.unique(y[np.isfinite(y)]).size)
    if int(n_classes) <= 2:
        if max_label > 1 or n_distinct > 2:
            raise RuntimeError(
                "label/num-classes mismatch: a binary model but the labels are not "
                "in {0,1}. Set the model's n_classes to your class count.")
    elif max_label >= int(n_classes):
        raise RuntimeError(
            "label/num-classes mismatch: a label exceeds num-classes. Set the "
            "model's n_classes to at least your class count.")


def _pool_by_patient(X, y, groups, loss_name):
    """Collapse every patient to exactly one DP unit.

    A detected patient identifier must never silently fall back to row-level
    privacy. Features and continuous outcomes are averaged; categorical
    outcomes use a deterministic mode, and multilabel outcomes use majority per
    label. These are local preprocessing operations on one privacy unit.
    """
    g = np.asarray(
        [task_module._canonical_patient_id(gv) for gv in groups],
        dtype=object)
    # Assign compact group indices in first-appearance order.  Aggregation then
    # becomes O(rows) rather than scanning all rows once per patient.
    group_index = {}
    inverse = np.empty(len(g), dtype=np.intp)
    for row, key in enumerate(g.tolist()):
        if key not in group_index:
            group_index[key] = len(group_index)
        inverse[row] = group_index[key]
    n_groups = len(group_index)
    if n_groups == 0:
        raise ValueError("cannot pool an empty patient collection")
    counts = np.bincount(inverse, minlength=n_groups).astype(np.float64)

    x_values = np.asarray(X, dtype=np.float64)
    Xp = np.zeros((n_groups,) + x_values.shape[1:], dtype=np.float64)
    np.add.at(Xp, inverse, x_values)
    Xp /= counts.reshape((n_groups,) + (1,) * (x_values.ndim - 1))

    values = np.asarray(y)
    categorical = (loss_name in _CLASSIFICATION_LOSSES
                   or loss_name == "multilabel_bce")
    if values.ndim > 1:
        yp = np.zeros((n_groups,) + values.shape[1:], dtype=np.float64)
        np.add.at(yp, inverse, np.asarray(values, dtype=np.float64))
        yp /= counts.reshape((n_groups,) + (1,) * (values.ndim - 1))
        if categorical:
            yp = (yp >= 0.5).astype(values.dtype)
    elif categorical:
        # Sparse group/label counts avoid a potentially huge patients x classes
        # matrix. np.unique sorts labels, so selecting the first maximum keeps
        # the previous deterministic lowest-label tie break.
        labels, label_inverse = np.unique(values, return_inverse=True)
        pair_ids, pair_counts = np.unique(
            inverse * len(labels) + label_inverse, return_counts=True)
        pair_groups = pair_ids // len(labels)
        pair_labels = pair_ids % len(labels)
        max_counts = np.zeros(n_groups, dtype=pair_counts.dtype)
        np.maximum.at(max_counts, pair_groups, pair_counts)
        candidates = np.flatnonzero(pair_counts == max_counts[pair_groups])
        candidate_groups = pair_groups[candidates]
        first = np.r_[True, candidate_groups[1:] != candidate_groups[:-1]]
        yp = labels[pair_labels[candidates[first]]]
    else:
        yp = np.zeros(n_groups, dtype=np.float64)
        np.add.at(yp, inverse, np.asarray(values, dtype=np.float64))
        yp /= counts
    return Xp, np.asarray(yp, dtype=y.dtype)


def _effective_feature_bounds(cfg):
    """Return the one canonical public bounds object used by execution."""
    bounds = cfg.get("feature-bounds")
    raw_bounds = cfg.get("feature-bounds-b64")
    if bounds is None and raw_bounds:
        import base64
        try:
            bounds = json.loads(base64.b64decode(
                str(raw_bounds), validate=True).decode("utf-8"))
        except Exception as e:
            raise RuntimeError("could not decode feature-bounds-b64: %s" % e)
    return bounds


def _apply_feature_bounds(X, cfg):
    """Apply the public clipped affine transform pinned for this training."""
    bounds = _effective_feature_bounds(cfg)
    if bounds is not None:
        lower = np.asarray(bounds.get("lower", []), dtype=np.float64)
        upper = np.asarray(bounds.get("upper", []), dtype=np.float64)
        if lower.shape != (X.shape[1],) or upper.shape != (X.shape[1],):
            raise RuntimeError("public feature bounds do not match num features")
        if (not np.all(np.isfinite(lower)) or not np.all(np.isfinite(upper))
                or not np.all(lower < upper)
                or np.any(np.abs(lower) > task_module._MAX_NUMERIC_ABS)
                or np.any(np.abs(upper) > task_module._MAX_NUMERIC_ABS)):
            raise RuntimeError("public feature bounds must be finite with lower < upper")
        center = (lower + upper) / 2.0
        scale = (upper - lower) / 2.0
        safe = _totalize_private_features(X).astype(np.float64)
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            transformed = (np.clip(safe, lower, upper) - center) / scale
        return _totalize_private_features(transformed)

    return _totalize_private_features(X)


def _train_neural(context, cfg, pcfg, pins, model, input_dim, manifest_image,
                  cv_fold=None, on_private_start=None):
    n_classes = int(pins["n_classes"])
    has_holdout = cfg.get("resampling-contract-sha256") is not None
    has_cv = cfg.get("cv-contract-sha256") is not None
    if has_holdout and has_cv:
        raise RuntimeError("holdout and cross-validation cannot be combined")
    if has_cv and manifest_image:
        raise RuntimeError(
            "cross-validation currently supports tabular neural data only")
    if has_cv != (cv_fold is not None):
        raise RuntimeError("cross-validation fold coordinate is unavailable")
    geometry_n_units = None
    resampling_manifest = None
    if has_holdout or has_cv:
        resampling_manifest = task_module._load_manifest(context)
        geometry_n_units = task_module.pinned_unit_count_from_manifest(
            resampling_manifest)
        if geometry_n_units < 1:
            raise ValueError(
                "resampling requires a positive pinned privacy-unit count")

    if manifest_image:
        from . import vision
        encoder, image_size, is_3d, device = vision.prepare_backbone(
            cfg.get("backbone", cfg.get("model", "resnet18")),
            cfg.get("vision-extractor-profile"), cfg.get("num-features"),
            cfg.get("image-size"))
        if on_private_start is not None:
            on_private_start()
        values, y, groups = load_image_collection(context)
    else:
        values, y, groups = load_data(context, include_unit_ids=True)
        if values.ndim != 2 or int(values.shape[1]) != int(input_dim):
            raise RuntimeError("staged feature width changed after model validation")
    n_staged = len(y)                          # pre-pool staged count (== manifest n_samples)

    task_module.assert_pinned_unit_count(
        context, len(y), patient_ids=groups, manifest=resampling_manifest)
    if has_holdout:
        values, y, groups = _holdout_partition(
            context, values, y, groups, subset="train")
    elif has_cv:
        values, y, groups = _cross_validation_partition(
            context, values, y, groups, fold=int(cv_fold), subset="train")
    empty_training = len(y) == 0
    if manifest_image:
        X = (np.empty((0, int(input_dim)), dtype=np.float32)
             if empty_training else vision.extract_features_from_paths(
                 encoder, list(values), image_size, is_3d, device=device))
    else:
        X = values
        X = _apply_feature_bounds(X, cfg)
    X = _totalize_private_features(X)
    if not empty_training and pins["loss_name"] in _CLASSIFICATION_LOSSES:
        _assert_label_range(y, n_classes)
    if not empty_training and groups is not None:
        X, y = _pool_by_patient(X, y, groups, pins["loss_name"])
        X = _totalize_private_features(X)
    # Bind every stochastic DP-SGD axis only after preprocessing has produced
    # the exact tensors consumed by training.  Parameter arrays are read back
    # from the model so dtype coercions performed by set_torch_params are part
    # of the effective, rather than merely transported, public model.
    if pins["loss_name"] in ("cross_entropy", "hinge", "ordinal"):
        seed_target = np.asarray(y, dtype=np.int64)
    else:
        seed_target = np.asarray(y, dtype=np.float32)
    seed_config, _ = _neural_seed_contract(
        cfg, pins, pcfg, geometry_n_units=geometry_n_units)
    accounting_population = (len(y) if geometry_n_units is None
                             else int(geometry_n_units))
    effective_privacy = dp_harness.effective_dpsgd_mechanism(
        epsilon=pcfg["epsilon"], delta=pcfg["delta"],
        clipping_norm=pcfg["clipping_norm"],
        n_samples=accounting_population, batch_size=int(pins["batch_size"]),
        local_epochs=int(pins["local_epochs"]),
        num_rounds=int(pins["num_rounds"]))
    effective_privacy["privacy_unit"] = (
        "patient" if groups is not None else "row")
    master = seeding.master_seed(
        "neural-dpsgd/v1", seed_config, effective_privacy,
        int(pins["round_index"]),
        public_arrays=get_torch_params(model),
        private_arrays=(np.asarray(X, dtype=np.float32), seed_target))

    fit_X, fit_y = X, y
    if empty_training:
        # Keep the exact calibrated q/steps/sigma schedule without creating an
        # output atom at the public input model. The sole public dummy has a
        # forced zero per-sample gradient; every optimizer step still receives
        # the normal DP noise under the pinned total-unit geometry.
        fit_X = np.zeros((1, int(input_dim)), dtype=np.float32)
        fit_y = np.zeros(
            (1, *np.asarray(y).shape[1:]), dtype=np.asarray(y).dtype)

    fit_options = ({"public_zero_gradient": True}
                   if empty_training else {})
    return _dp_fit(
        model, fit_X, fit_y, pcfg, pins, n_staged, cfg, master=master,
        noise_multiplier=effective_privacy["noise_multiplier"],
        geometry_n_units=geometry_n_units, **fit_options)


def _holdout_partition(context, X, y, unit_ids, *, subset):
    """Select one pre-training holdout side without exposing its roster."""
    if subset not in ("train", "test"):
        raise ValueError("holdout subset must be train or test")
    values = np.asarray(X)
    target = np.asarray(y)
    if values.ndim < 1 or target.ndim < 1 or values.shape[0] != target.shape[0]:
        raise RuntimeError("holdout inputs must share one row axis")
    mask = resampling.holdout_mask_from_context(
        context, n_rows=int(target.shape[0]), unit_ids=unit_ids)
    selected = mask if subset == "test" else ~mask
    selected_units = (None if unit_ids is None
                      else np.asarray(unit_ids)[selected])
    return values[selected], target[selected], selected_units


def _cross_validation_partition(context, X, y, unit_ids, *, fold, subset):
    """Select one HMAC-assigned fold side before any model computation."""
    if subset not in ("train", "test"):
        raise ValueError("cross-validation subset must be train or test")
    values = np.asarray(X)
    target = np.asarray(y)
    if values.ndim < 1 or target.ndim < 1 or values.shape[0] != target.shape[0]:
        raise RuntimeError("cross-validation inputs must share one row axis")
    assigned = resampling.cross_validation_folds_from_context(
        context, n_rows=int(target.shape[0]), unit_ids=unit_ids)
    fold = int(fold)
    selected = assigned == fold
    if subset == "train":
        selected = ~selected
    selected_units = (None if unit_ids is None
                      else np.asarray(unit_ids)[selected])
    return values[selected], target[selected], selected_units


def _cv_state_binding(context, layout):
    manifest = task_module._load_manifest(context)
    contract = resampling.cross_validation_contract_from_manifest(manifest)
    job_hash = manifest.get("cv-job-sha256")
    if (not isinstance(job_hash, str) or len(job_hash) != 64
            or any(value not in "0123456789abcdef" for value in job_hash)):
        raise RuntimeError("cross-validation manifest has no public job pin")
    effective = validation._effective_validation_layout(layout)
    layout_wire = json.dumps(
        effective, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    return {
        "version": "dsflower-cv-oof-state-v1",
        "cv-job-sha256": job_hash,
        "cv-contract-sha256": contract["sha256"],
        "folds": int(contract["folds"]),
        "layout-sha256": hashlib.sha256(layout_wire).hexdigest(),
        "size": int(effective["size"]),
    }


def _cv_vector_sha256(value):
    return hashlib.sha256(
        np.ascontiguousarray(value, dtype="<f8").tobytes(order="C")).hexdigest()


def _read_cv_state(context, binding, layout):
    meta = context.state.get(_CV_OOF_META_KEY)
    arrays = context.state.get(_CV_OOF_TOTAL_KEY)
    if meta is None and arrays is None:
        return None
    if not isinstance(meta, ConfigRecord) or not isinstance(arrays, ArrayRecord):
        raise RuntimeError("cross-validation OOF state is incomplete")
    expected_fields = set(binding) | {"fold-digests", "total-sha256"}
    if set(meta.keys()) != expected_fields or any(
            meta.get(key) != value for key, value in binding.items()):
        raise RuntimeError("cross-validation OOF state binding changed")
    digests = meta.get("fold-digests")
    total_digest = meta.get("total-sha256")
    if (not isinstance(digests, list) or not digests
            or len(digests) > binding["folds"]
            or any(not isinstance(value, str) or len(value) != 64
                   or any(char not in "0123456789abcdef" for char in value)
                   for value in digests)
            or not isinstance(total_digest, str) or len(total_digest) != 64):
        raise RuntimeError("cross-validation OOF state metadata is invalid")
    values = arrays.to_numpy_ndarrays()
    if len(values) != 1:
        raise RuntimeError("cross-validation OOF state geometry changed")
    total = validation._canonical_sufficient_vector(values[0], layout)
    if _cv_vector_sha256(total) != total_digest:
        raise RuntimeError("cross-validation OOF state digest changed")
    return list(digests), total


def _store_cv_sufficient(context, fold, raw, layout):
    binding = _cv_state_binding(context, layout)
    folds = binding["folds"]
    fold = int(fold)
    canonical = validation._canonical_sufficient_vector(raw, layout)
    digest = _cv_vector_sha256(canonical)
    state = _read_cv_state(context, binding, layout)
    if state is None:
        if fold != 1:
            raise RuntimeError("cross-validation folds must accumulate in order")
        digests = []
        total = np.zeros_like(canonical)
    else:
        digests, total = state
        if fold <= len(digests):
            if digests[fold - 1] != digest:
                raise RuntimeError("cross-validation fold replay changed")
            return
        if fold != len(digests) + 1:
            raise RuntimeError("cross-validation folds must accumulate in order")
    if not 1 <= fold <= folds:
        raise RuntimeError("cross-validation fold is outside its contract")
    total = np.asarray(total, dtype=np.float64) + canonical
    if not bool(np.all(np.isfinite(total))):
        raise RuntimeError("cross-validation sufficient statistics overflowed")
    total = validation._canonical_sufficient_vector(total, layout)
    digests.append(digest)
    context.state[_CV_OOF_TOTAL_KEY] = ArrayRecord(
        numpy_ndarrays=[total])
    context.state[_CV_OOF_META_KEY] = ConfigRecord({
        **binding,
        "fold-digests": digests,
        "total-sha256": _cv_vector_sha256(total),
    })


def _load_complete_cv_sufficient(context, layout):
    binding = _cv_state_binding(context, layout)
    state = _read_cv_state(context, binding, layout)
    if state is None or len(state[0]) != binding["folds"]:
        raise RuntimeError("cross-validation OOF accumulation is incomplete")
    return validation._canonical_sufficient_vector(state[1].copy(), layout)


def _forget_cv_sufficient(context):
    context.state.pop(_CV_OOF_META_KEY, None)
    context.state.pop(_CV_OOF_TOTAL_KEY, None)


def _cross_validation_neural_accumulate(context, cfg, pins, model,
                                        input_dim, fold):
    """Evaluate one held-out fold and retain only its raw vector in node RAM."""
    if is_image_run(context):
        raise RuntimeError("cross-validation supports tabular neural data only")
    X, y, unit_ids = load_data(context, include_unit_ids=True)
    if X.ndim != 2 or int(X.shape[1]) != int(input_dim):
        raise RuntimeError("staged feature width changed before CV evaluation")
    task_module.assert_pinned_unit_count(
        context, len(y), patient_ids=unit_ids)
    X, y, unit_ids = _cross_validation_partition(
        context, X, y, unit_ids, fold=int(fold), subset="test")
    X = _apply_feature_bounds(X, cfg)
    layout = validation.cross_validation_layout_from_config(cfg)
    predictions = validation.neural_predictions(model, X, pins["loss_name"])
    bounds = (validation.cross_validation_target_bounds_from_config(cfg)
              if layout["task"] in ("regression", "count") else None)
    raw = validation.validation_sufficient_vector(
        y, predictions, layout, target_bounds=bounds, unit_ids=unit_ids)
    _store_cv_sufficient(context, int(fold), raw, layout)
    return [np.zeros(1, dtype=np.float64)]


def _cross_validation_release(context, cfg, pcfg):
    """Apply the sole OOF Gaussian release and consume its in-memory state."""
    layout = validation.cross_validation_layout_from_config(cfg)
    try:
        raw = _load_complete_cv_sufficient(context, layout)
        released, _sigma = validation.private_sufficient_vector(
            raw, layout, epsilon=pcfg["epsilon"], delta=pcfg["delta"],
            num_releases=1)
        return [released.astype(np.float64)]
    finally:
        _forget_cv_sufficient(context)


def _holdout_neural_release(context, cfg, pcfg, pins, model, input_dim,
                            on_private_start=None):
    """Evaluate the final public aggregate on test units and release one vector."""
    manifest_image = is_image_run(context)
    if manifest_image:
        from . import vision
        encoder, image_size, is_3d, device = vision.prepare_backbone(
            cfg.get("backbone", cfg.get("model", "resnet18")),
            cfg.get("vision-extractor-profile"), cfg.get("num-features"),
            cfg.get("image-size"))
        if on_private_start is not None:
            on_private_start()
        values, y, unit_ids = load_image_collection(context)
    else:
        if on_private_start is not None:
            on_private_start()
        values, y, unit_ids = load_data(context, include_unit_ids=True)
        if values.ndim != 2 or int(values.shape[1]) != int(input_dim):
            raise RuntimeError(
                "staged feature width changed before holdout evaluation")
    task_module.assert_pinned_unit_count(
        context, len(y), patient_ids=unit_ids)
    values, y, unit_ids = _holdout_partition(
        context, values, y, unit_ids, subset="test")
    if manifest_image:
        X = (np.empty((0, int(input_dim)), dtype=np.float32)
             if len(y) == 0 else vision.extract_features_from_paths(
                 encoder, list(values), image_size, is_3d, device=device))
    else:
        X = _apply_feature_bounds(values, cfg)
    layout = validation.holdout_layout_from_config(cfg)
    predictions = validation.neural_predictions(model, X, pins["loss_name"])
    bounds = (validation.holdout_target_bounds_from_config(cfg)
              if layout["task"] in ("regression", "count") else None)
    privacy_unit = cfg.get("resampling-privacy-unit")
    if privacy_unit not in ("row", "patient"):
        raise RuntimeError("holdout privacy unit is not pinned")
    released, _sigma = validation.private_validation_vector(
        y, predictions, layout, epsilon=pcfg["epsilon"], delta=pcfg["delta"],
        target_bounds=bounds, num_releases=1, unit_ids=unit_ids,
        include_zero_neighbor=privacy_unit == "patient")
    return [released.astype(np.float64)]


def _public_fallback_arrays(msg, context, track):
    """Bounded response independent of private rows when no release is available."""
    if track == "validation":
        # A standalone validator must never turn a failed/no-release node into
        # plausible pooled metrics.  The fixed public wrong geometry makes the
        # ServerApp publish only ``available=false``, with no exception detail,
        # fake metric or replacement private release.
        return [np.zeros(1, dtype=np.float64)]
    return _validate_public_egress_arrays(
        msg.content["arrays"], label="public fallback model")


def _safe_public_fallback_arrays(msg, context, track):
    """Construct a bounded fallback using public inputs/configuration only."""
    if track is None:
        try:
            track = load_dp_track(context)
        except Exception:
            pass
    try:
        return _public_fallback_arrays(msg, context, track)
    except Exception:
        try:
            return _validate_public_egress_arrays(
                msg.content["arrays"], label="public fallback model")
        except Exception:
            return [np.zeros(1, dtype=np.float32)]


def _safe_fallback_reply(msg, context, claim=None, track=None,
                     hook_executed=None, public_preflight_unavailable=False,
                     execution_unavailable=False):
    """Never expose a ClientApp exception through Flower's Error response."""
    if track is None:
        try:
            track = load_dp_track(context)
        except Exception:
            pass
    if (claim is not None
            and str(claim.get("operation", "")).startswith("cv-")):
        arrays = [np.zeros(1, dtype=np.float64)]
    else:
        arrays = _safe_public_fallback_arrays(msg, context, track)
    # Once a private release path has been entered, keep success/failure
    # data-independent: a public unchanged model is still a valid no-release
    # Hook round.  Only the explicit pre-private readiness gate reports FALSE.
    hook_status = ((True if hook_executed is None else bool(hook_executed))
                   if track == "egress" else None)
    if (claim is not None and _reply_cache_allowed(claim)
            and claim.get("status") == "new"
            and claim.get("release_index") is not None):
        try:
            _cache_reply(
                context, claim, arrays, hook_executed=hook_status,
                public_preflight_unavailable=public_preflight_unavailable,
                execution_unavailable=execution_unavailable)
        except Exception:
            pass
    try:
        return _reply(
            msg, arrays, hook_executed=hook_status,
            public_preflight_unavailable=public_preflight_unavailable,
            execution_unavailable=execution_unavailable)
    except Exception:
        # Arrays above are public, but keep a constant minimal last resort in case
        # their Flower encoding itself is rejected.
        return Message(content=RecordDict({
            "arrays": ArrayRecord(
                numpy_ndarrays=[np.zeros(1, dtype=np.float32)]),
            "metrics": MetricRecord({
                "num-examples": 1,
                **({"public-preflight-unavailable": 1}
                   if public_preflight_unavailable else {}),
                **({"execution-unavailable": 1}
                   if execution_unavailable else {}),
            }),
        }), reply_to=msg)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

@app.train()
def train(msg: Message, context: Context) -> Message:
    claim = None
    track = None
    hook_public_ready = None
    private_started = False

    def mark_private_started():
        nonlocal private_started
        private_started = True

    try:
        claim = release_guard.claim_release(context, msg)  # before any private read
        if claim["status"] == "replay":
            return _replay_reply(context, claim, msg)

        cfg = load_pinned_run_config(context)
        # Server-DERIVED routing (single source of truth: dp_harness.resolve_dp_track):
        # an uploaded user-module is forced to the output-perturbation floor; node-built
        # artifacts use the node-pinned track. The client cannot route its own code to a
        # tighter track, and the neural track only ever runs the hash-verified harness.
        track = dp_harness.resolve_dp_track(cfg, load_dp_track(context))
        pcfg = load_privacy_config(context)
        # The node-written manifest is authoritative. The sticky guard validates
        # its fixed policy and returns those exact per-training values.
        pcfg["epsilon"] = float(claim["epsilon"])
        pcfg["delta"] = float(claim["delta"])
        operation = claim.get("operation", "train")
        if operation == "cv-abort":
            if track != "neural":
                raise RuntimeError("cross-validation abort requires the neural track")
            _forget_cv_sufficient(context)
            new_arrays = [np.zeros(1, dtype=np.float64)]
        elif operation == "cv-release":
            if track != "neural":
                raise RuntimeError("cross-validation release requires the neural track")
            mark_private_started()
            new_arrays = _cross_validation_release(context, cfg, pcfg)
        elif operation == "cv-accumulate":
            if track != "neural":
                raise RuntimeError(
                    "cross-validation accumulation requires the neural track")
            pins = dict(load_run_pins(context))
            if int(pins["num_rounds"]) != int(claim["num_rounds"]):
                raise RuntimeError(
                    "neural calibration horizon does not match release guard")
            pins["round_index"] = int(pins["num_rounds"])
            pins["fold_index"] = int(claim["fold"])
            pins["operation"] = "cv-accumulate"
            model, input_dim, manifest_image = _prepare_neural_evaluation_model(
                msg, context, cfg, pins)
            if manifest_image:
                raise RuntimeError(
                    "cross-validation supports tabular neural data only")
            mark_private_started()
            new_arrays = _cross_validation_neural_accumulate(
                context, cfg, pins, model, input_dim, int(claim["fold"]))
        elif operation == "holdout-evaluate":
            if track != "neural":
                raise RuntimeError("holdout evaluation requires the neural track")
            pins = dict(load_run_pins(context))
            if int(pins["num_rounds"]) != int(claim["num_rounds"]):
                raise RuntimeError(
                    "neural calibration horizon does not match release guard")
            pins["round_index"] = int(pins["num_rounds"])
            model, input_dim, manifest_image = _prepare_neural_model(
                msg, context, cfg, pcfg, pins)
            new_arrays = _holdout_neural_release(
                context, cfg, pcfg, pins, model, input_dim,
                on_private_start=mark_private_started)
        elif track == "validation":
            if int(claim["num_rounds"]) != 1:
                raise RuntimeError("validation release horizon must be exactly one")
            public_arrays = _validate_public_egress_arrays(
                msg.content["arrays"], label="public validation model",
                max_elements=_MAX_EGRESS_ELEMENTS)
            new_arrays = validation.private_model_validation(
                context, cfg, pcfg, int(claim["release_index"]), public_arrays,
                on_private_start=mark_private_started)
        elif track == "egress":
            from . import tier2_lib
            hook_public_ready = False
            old = _validate_public_egress_arrays(msg.content["arrays"])
            public_hook_cfg = tier2_lib.public_hook_config(
                cfg, round_index=int(claim["release_index"]))
            # Compose all Gaussian Hook releases jointly through the closed RDP
            # bound. Per-release sigma scales with sqrt(R), which is much tighter
            # than splitting epsilon and delta linearly while preserving the same
            # run-level (epsilon, delta) guarantee.
            num_rounds = int(claim["num_rounds"])
            pcfg_round = dict(pcfg)
            pcfg_round["composition_releases"] = num_rounds
            hook_caps = tier2_lib.hook_execution_caps(pcfg_round)
            if hook_caps is None:
                # Policy/sandbox no-op: no private file is opened and no private
                # randomness is needed.  The incoming arrays are already public.
                new_arrays = [np.asarray(a, dtype=np.float32) for a in old]
                _cache_reply(
                    context, claim, new_arrays, hook_executed=False,
                    public_preflight_unavailable=True)
                return _reply(
                    msg, new_arrays, hook_executed=False,
                    public_preflight_unavailable=True)

            hook_public_ready = True
            module_name = str(cfg["user-module"])
            hook_started = time.monotonic()
            try:
                mark_private_started()
                X, y, unit_ids = load_data(
                    context, include_unit_ids=True)
                task_module.assert_pinned_unit_count(context, len(y), unit_ids)
                master = tier2_lib.hook_master_seed(
                    module_name, old, X, y, public_hook_cfg, pcfg_round,
                    unit_ids=unit_ids)
                execution_seed = tier2_lib.hook_execution_seed(
                    module_name, old, public_hook_cfg, pcfg_round)
                new_arrays = tier2_lib.gated_local_update(
                    module_name, old, X, y, public_hook_cfg, pcfg_round,
                    seed=seeding.sub_seed(master, "egress"),
                    execution_seed=seeding.sub_seed(
                        execution_seed, "egress-execution"),
                    hook_caps=hook_caps, unit_ids=unit_ids,
                    release_started=hook_started, pad_release=False)
            finally:
                tier2_lib.pad_hook_release(hook_started, pcfg_round)
        else:  # neural (tabular or image)
            if operation not in ("train", "cv-train"):
                raise RuntimeError("unsupported neural release operation")
            pins = load_run_pins(context)
            if int(pins["num_rounds"]) != int(claim["num_rounds"]):
                raise RuntimeError(
                    "neural calibration horizon does not match release guard")
            pins = dict(pins)
            pins["round_index"] = int(claim["release_index"])
            if operation == "cv-train":
                pins["fold_index"] = int(claim["fold"])
                pins["operation"] = "cv-train"
            model, input_dim, manifest_image = _prepare_neural_model(
                msg, context, cfg, pcfg, pins)
            if not manifest_image:
                mark_private_started()
            new_arrays, _n = _train_neural(
                context, cfg, pcfg, pins, model, input_dim, manifest_image,
                cv_fold=(int(claim["fold"])
                         if operation == "cv-train" else None),
                on_private_start=(mark_private_started
                                  if manifest_image else None))

        hook_status = True if track == "egress" else None
        if _reply_cache_allowed(claim):
            _cache_reply(context, claim, new_arrays, hook_executed=hook_status)
        return _reply(msg, new_arrays, hook_executed=hook_status)
    except Exception:
        # Flower serializes uncaught exception strings into Error.reason. Those
        # strings can contain private values (for example pandas conversion errors),
        # so the trusted boundary must return content and never propagate/log them.
        return _safe_fallback_reply(
            msg, context, claim=claim, track=track,
            hook_executed=hook_public_ready,
            public_preflight_unavailable=not private_started,
            execution_unavailable=private_started)
