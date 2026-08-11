"""Dedicated trusted ClientApp for one complete native-tree training."""

import hashlib
import hmac
import io
import os
import re
import sys


_FORBIDDEN_MODULES = (
    "torch", "opacus", "dsflower_runner.client_app",
    "dsflower_runner.egress_child", "dsflower_runner.tier2_lib",
)


def _assert_native_process_isolated():
    """Fail if the dedicated native process has admitted a wider runner."""
    if os.environ.get("DSFLOWER_PINNED_APP_DIR"):
        raise RuntimeError("native-tree process cannot admit uploaded code")
    for name in tuple(sys.modules):
        if any(name == forbidden or name.startswith(forbidden + ".")
               for forbidden in _FORBIDDEN_MODULES):
            raise RuntimeError("native-tree process is not isolated")


# Production SuperNodes always carry the node-owned manifest directory.  Keep
# ordinary test discovery importable, while enforcing isolation before any
# third-party/native module enters a real ClientApp process.
if os.environ.get("DSFLOWER_MANIFEST_DIR"):
    _assert_native_process_isolated()


import numpy as np
from flwr.clientapp import ClientApp
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from . import (native_tree_engine, native_tree_request, release_guard,
               resampling, task, validation, xgboost_adapter, xgboost_bundle)


app = ClientApp()
_BUNDLE_ENV = "DSFLOWER_XGBOOST_BUNDLE_ROOT"
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_UNAVAILABLE = b'{"status":"unavailable","version":1}'
_TRAIN_MESSAGE_FIELDS = frozenset((
    "dsflower-operation", "native-tree-request-b64",
    "native-tree-request-sha256", "server-round",
))
_HOLDOUT_TRAIN_MESSAGE_FIELDS = _TRAIN_MESSAGE_FIELDS | frozenset((
    "resampling-contract-sha256",
))
_EVALUATE_MESSAGE_FIELDS = _TRAIN_MESSAGE_FIELDS | frozenset((
    "holdout-validation-bins", "native-tree-artifact-format",
    "native-tree-artifact-sha256", "native-tree-artifact-size-bytes",
    "native-tree-n-nodes", "native-tree-public-schema-sha256",
    "resampling-contract-sha256",
))
_CV_TRAIN_MESSAGE_FIELDS = frozenset((
    "cv-contract-sha256", "cv-job-sha256", "dsflower-fold",
    "dsflower-operation", "native-tree-request-b64",
    "native-tree-request-sha256", "server-round",
))
_CV_CONTROL_MESSAGE_FIELDS = frozenset((
    "cv-contract-sha256", "cv-job-sha256", "cv-validation-bins",
    "dsflower-fold", "dsflower-operation", "native-tree-n-nodes",
    "native-tree-public-schema-sha256", "native-tree-request-b64",
    "native-tree-request-sha256", "server-round",
))
_CV_ACCUMULATE_MESSAGE_FIELDS = _CV_CONTROL_MESSAGE_FIELDS | frozenset((
    "native-tree-artifact-format", "native-tree-artifact-sha256",
    "native-tree-artifact-size-bytes",
))
_HOLDOUT_TRAIN_STATE = "dsflower-native-tree-holdout-train-v1"
_CV_OOF_META = "dsflower-native-tree-cv-oof-meta-v1"
_CV_OOF_TOTAL = "dsflower-native-tree-cv-oof-total-v1"
_CV_TRAIN_META = "dsflower-native-tree-cv-train-meta-v1"
_REPLY_CACHE = "dsflower-last-release"
_REPLY_CACHE_META = "dsflower-last-release-meta"
_NPY_HEADER_ALLOWANCE = 4096
_CV_ACK = np.asarray([0], dtype=np.uint8)


def _load_node_bundle_once():
    root = os.environ.get(_BUNDLE_ENV, "")
    if not root or not os.path.isabs(root):
        return None
    try:
        return xgboost_bundle.load_verified_xgboost_bundle(root)
    except Exception:
        return None


_NATIVE_BUNDLE = _load_node_bundle_once()


def _reply(msg, artifact, available):
    encoded = np.frombuffer(bytes(artifact), dtype=np.uint8).copy()
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[encoded]),
        "metrics": MetricRecord({
            "available": int(bool(available)),
            "num-examples": 1,
        }),
    }), reply_to=msg)


def _vector_reply(msg, vector, available=True):
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[
            np.asarray(vector, dtype=np.float64)]),
        "metrics": MetricRecord({
            "available": int(bool(available)),
            "num-examples": 1,
        }),
    }), reply_to=msg)


def _unavailable_reply(msg):
    return _reply(msg, _UNAVAILABLE, False)


def _exact_message_config(msg):
    if frozenset(msg.content.keys()) not in (
            frozenset(("config",)), frozenset(("arrays", "config"))):
        raise ValueError("native-tree request message has an invalid shape")
    config = msg.content["config"]
    operation = config.get("dsflower-operation") if isinstance(
        config, ConfigRecord) else None
    if operation == "train":
        expected = (_HOLDOUT_TRAIN_MESSAGE_FIELDS
                    if "resampling-contract-sha256" in config
                    else _TRAIN_MESSAGE_FIELDS)
        expected_content = frozenset(("config",))
    elif operation == "holdout-evaluate":
        expected = _EVALUATE_MESSAGE_FIELDS
        expected_content = frozenset(("arrays", "config"))
    elif operation == "cv-train":
        expected = _CV_TRAIN_MESSAGE_FIELDS
        expected_content = frozenset(("arrays", "config"))
    elif operation == "cv-accumulate":
        expected = _CV_ACCUMULATE_MESSAGE_FIELDS
        expected_content = frozenset(("arrays", "config"))
    elif operation in ("cv-release", "cv-abort"):
        expected = _CV_CONTROL_MESSAGE_FIELDS
        expected_content = frozenset(("arrays", "config"))
    else:
        expected = frozenset()
        expected_content = frozenset()
    if not isinstance(config, ConfigRecord) or \
            frozenset(config.keys()) != expected or \
            frozenset(msg.content.keys()) != expected_content or \
            type(config["server-round"]) is not int or \
            config["server-round"] != 1 or \
            operation not in (
                "train", "holdout-evaluate", "cv-train", "cv-accumulate",
                "cv-release", "cv-abort"):
        raise ValueError("native-tree request message has an invalid config")
    return config


def _validate_public_manifest(request, manifest):
    schema = request["public_schema"]
    if manifest.get("data_type") != "tabular" or \
            manifest.get("dp-track") != "native_tree" or \
            type(manifest.get("num-server-rounds")) is not int or \
            manifest["num-server-rounds"] != 1 or \
            manifest.get("target-preencoded") is not True or \
            manifest.get("user-module") not in (None, "") or \
            manifest.get("feature_columns") != schema["features"] or \
            manifest.get("target_column") != schema["target"]["name"]:
        raise ValueError("node manifest differs from the native-tree request")
    bounds = manifest.get("feature-bounds")
    if not isinstance(bounds, dict) or \
            bounds.get("lower") != schema["lower"] or \
            bounds.get("upper") != schema["upper"]:
        raise ValueError("node feature bounds differ from the request")

    target = schema["target"]
    if request["task"] == "binary":
        levels = manifest.get("target-levels")
        level_types = {
            "character": "string", "logical": "boolean", "numeric": "number",
        }
        expected_type = (level_types.get(levels.get("type"))
                         if isinstance(levels, dict) else None)
        expected_values = levels.get("values") if isinstance(levels, dict) else None
        request_levels = target.get("levels")
        if manifest.get("task-type") != "classification" or \
                not isinstance(expected_values, list) or \
                not isinstance(request_levels, list) or \
                [item.get("type") for item in request_levels] != \
                [expected_type] * len(request_levels) or \
                [item.get("value") for item in request_levels] != expected_values:
            raise ValueError("node target levels differ from the request")
    else:
        bounds = manifest.get("target-bounds")
        if manifest.get("task-type") != "regression" or \
                not isinstance(bounds, dict) or \
                bounds.get("lower") != target.get("lower") or \
                bounds.get("upper") != target.get("upper"):
            raise ValueError("node target bounds differ from the request")


def _node_privacy(manifest, context, operation):
    policy_hash = manifest.get("privacy-policy-sha256")
    if not isinstance(policy_hash, str) or not _HEX_64_RE.fullmatch(policy_hash):
        raise ValueError("node privacy policy is not pinned")
    if manifest.get("privacy-adjacency") != "replace_one" or \
            manifest.get("dp-unit") not in ("row", "patient") or \
            manifest.get("patient-id-canonicalization") != "trim-utf8-v2":
        raise ValueError("node privacy unit is not pinned")
    values = []
    for key in ("privacy-epsilon", "privacy-delta",
                "privacy-clipping_norm"):
        value = manifest.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("node privacy parameters are invalid")
        values.append(float(value))
    epsilon, delta, gradient_clip = values
    has_holdout = manifest.get("resampling-contract-sha256") is not None
    has_cv = manifest.get("cv-contract-sha256") is not None
    if has_holdout and has_cv:
        raise ValueError("node resampling contracts cannot combine")
    if has_cv:
        fixed = release_guard._fixed_manifest(context)
        if not fixed["cross_validation"] or operation not in (
                "cv-train", "cv-accumulate", "cv-release", "cv-abort"):
            raise ValueError("node cross-validation operation is invalid")
        epsilon, delta = fixed["budgets"][operation]
    elif has_holdout:
        fixed = release_guard._fixed_manifest(context)
        if not fixed["holdout"] or operation not in (
                "train", "holdout-evaluate"):
            raise ValueError("node holdout privacy operation is invalid")
        epsilon, delta = fixed["budgets"][operation]
    elif operation != "train":
        raise ValueError("resampling operation has no pinned contract")
    return {
        "epsilon": epsilon,
        "delta": delta,
        "unit": manifest["dp-unit"],
        "unit_canonicalization": "trim-utf8-v2",
        "gradient_clip": gradient_clip,
    }


def _pinned_request(msg, context):
    message_config = _exact_message_config(msg)
    manifest = task._load_manifest(context)
    run_config = (task.load_pinned_run_config(context)
                  if (manifest.get("resampling-contract-sha256") is not None or
                      manifest.get("cv-contract-sha256") is not None)
                  else dict(context.run_config))
    request_b64 = manifest.get("native-tree-request-b64")
    request_sha256 = manifest.get("native-tree-request-sha256")
    operation = message_config["dsflower-operation"]
    has_holdout = manifest.get("resampling-contract-sha256") is not None
    has_cv = manifest.get("cv-contract-sha256") is not None
    if run_config.get("dp-track") != "native_tree" or \
            type(run_config.get("num-server-rounds")) is not int or \
            run_config["num-server-rounds"] != 1 or \
            run_config.get("native-tree-request-b64") != request_b64 or \
            run_config.get("native-tree-request-sha256") != request_sha256 or \
            message_config["native-tree-request-b64"] != request_b64 or \
            message_config["native-tree-request-sha256"] != request_sha256:
        raise ValueError("Flower request differs from the node manifest pin")
    if has_holdout != (operation == "holdout-evaluate" or
                       "resampling-contract-sha256" in message_config):
        raise ValueError("Flower holdout phase differs from the node manifest")
    if has_holdout and message_config.get(
            "resampling-contract-sha256") != manifest[
                "resampling-contract-sha256"]:
        raise ValueError("Flower holdout contract differs from the node manifest")
    if has_cv != operation.startswith("cv-"):
        raise ValueError(
            "Flower cross-validation phase differs from the node manifest")
    if has_cv:
        contract = resampling.cross_validation_contract_from_manifest(manifest)
        folds = int(contract["folds"])
        fold = message_config.get("dsflower-fold")
        final = folds + 1
        if message_config.get("cv-contract-sha256") != contract["sha256"] or \
                message_config.get("cv-job-sha256") != manifest.get(
                    "cv-job-sha256") or \
                type(fold) is not int or \
                (operation in ("cv-train", "cv-accumulate") and
                 not 1 <= fold <= folds) or \
                (operation in ("cv-release", "cv-abort") and fold != final):
            raise ValueError(
                "Flower cross-validation coordinate differs from manifest")
        if operation != "cv-train":
            expected_nodes = getattr(context, "run_config", {}).get(
                "min-train-nodes")
            supplied_nodes = message_config.get("native-tree-n-nodes")
            supplied_bins = message_config.get("cv-validation-bins")
            manifest_bins = manifest.get("cv-validation-bins")
            if type(expected_nodes) is not int or expected_nodes < 1 or \
                    type(supplied_nodes) is not int or \
                    supplied_nodes != expected_nodes or \
                    type(manifest_bins) is not int or \
                    type(supplied_bins) is not int or \
                    supplied_bins != manifest_bins:
                raise ValueError(
                    "Flower cross-validation public profile differs from manifest")
    request = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    _validate_public_manifest(request, manifest)
    privacy = _node_privacy(manifest, context, operation)
    # Validate the complete public/accounting profile before opening private data.
    # Control and OOF-release phases do not train and therefore carry zero/OOF
    # budgets of their own.  The public ensemble parser remains bound to the
    # exact fold-training mechanism that produced every member.
    profile_privacy = privacy
    if has_cv and operation != "cv-train":
        fixed = release_guard._fixed_manifest(context)
        train_epsilon, train_delta = fixed["budgets"]["cv-train"]
        profile_privacy = dict(
            privacy, epsilon=train_epsilon, delta=train_delta)
    schema_hash = request["public_schema"]["sha256"]
    preflight = native_tree_request.backend_manifest(
        request, snapshot_hash=schema_hash, cohort_hash=schema_hash,
        **{key: profile_privacy[key] for key in (
            "epsilon", "delta", "unit", "unit_canonicalization",
            "gradient_clip")})
    native_tree_engine.canonical_profile(preflight)
    return request, manifest, privacy, operation


def _partition(context, features, target, unit_ids, *, test):
    mask = resampling.holdout_mask_from_context(
        context, n_rows=int(target.shape[0]), unit_ids=unit_ids)
    selected = mask if test else ~mask
    selected_units = (None if unit_ids is None
                      else np.asarray(unit_ids)[selected])
    return features[selected], target[selected], selected_units


def _cv_partition(context, features, target, unit_ids, *, fold, test):
    assigned = resampling.cross_validation_folds_from_context(
        context, n_rows=int(target.shape[0]), unit_ids=unit_ids)
    selected = assigned == int(fold)
    if not test:
        selected = ~selected
    selected_units = (None if unit_ids is None
                      else np.asarray(unit_ids)[selected])
    return features[selected], target[selected], selected_units


def _holdout_state(context):
    state = getattr(context, "state", None)
    if state is None or not hasattr(state, "get"):
        raise RuntimeError("native-tree holdout has no in-memory run state")
    return state


def _mark_training_complete(context, request, manifest, artifact):
    state = _holdout_state(context)
    record = {
        "artifact-sha256": hashlib.sha256(artifact).hexdigest(),
        "native-tree-request-sha256": manifest["native-tree-request-sha256"],
        "resampling-contract-sha256": manifest[
            "resampling-contract-sha256"],
        "task": request["task"],
    }
    existing = state.get(_HOLDOUT_TRAIN_STATE)
    if existing is not None and dict(existing) != record:
        raise RuntimeError("native-tree holdout training state changed")
    state[_HOLDOUT_TRAIN_STATE] = ConfigRecord(record)


def _require_training_complete(context, request, manifest):
    value = _holdout_state(context).get(_HOLDOUT_TRAIN_STATE)
    expected = {
        "native-tree-request-sha256": manifest["native-tree-request-sha256"],
        "resampling-contract-sha256": manifest[
            "resampling-contract-sha256"],
        "task": request["task"],
    }
    if not isinstance(value, ConfigRecord) or any(
            value.get(key) != item for key, item in expected.items()) or \
            not isinstance(value.get("artifact-sha256"), str) or \
            not _HEX_64_RE.fullmatch(value["artifact-sha256"]):
        raise RuntimeError("native-tree holdout training phase is incomplete")


def _artifact_from_message(msg, config, byte_cap):
    size = config["native-tree-artifact-size-bytes"]
    digest = config["native-tree-artifact-sha256"]
    if type(size) is not int or not 1 <= size <= byte_cap or \
            not isinstance(digest, str) or not _HEX_64_RE.fullmatch(digest):
        raise ValueError("native-tree holdout artifact pin is invalid")
    arrays = msg.content["arrays"]
    if not isinstance(arrays, ArrayRecord):
        raise ValueError("native-tree holdout artifact record is invalid")
    encoded = list(arrays.values())
    if len(encoded) != 1:
        raise ValueError("native-tree holdout requires one public artifact")
    item = encoded[0]
    if item.stype != "numpy.ndarray" or item.dtype != "uint8" or \
            tuple(item.shape) != (size,) or \
            len(item.data) > byte_cap + _NPY_HEADER_ALLOWANCE:
        raise ValueError("native-tree holdout artifact geometry is invalid")
    try:
        stream = io.BytesIO(item.data)
        version = np.lib.format.read_magic(stream)
        if version == (1, 0):
            shape, fortran, dtype = np.lib.format.read_array_header_1_0(
                stream, max_header_size=_NPY_HEADER_ALLOWANCE)
        elif version == (2, 0):
            shape, fortran, dtype = np.lib.format.read_array_header_2_0(
                stream, max_header_size=_NPY_HEADER_ALLOWANCE)
        else:
            raise ValueError("unsupported NPY version")
        if fortran or tuple(shape) != (size,) or \
                np.dtype(dtype) != np.dtype("uint8") or \
                len(item.data) - stream.tell() != size:
            raise ValueError("invalid NPY geometry")
    except Exception as exc:
        raise ValueError(
            "native-tree holdout artifact encoding is invalid") from exc
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise ValueError("native-tree holdout requires one public artifact")
    value = np.asarray(decoded[0])
    if value.dtype != np.uint8 or value.shape != (size,):
        raise ValueError("native-tree holdout artifact payload is invalid")
    artifact = value.tobytes()
    if not hmac.compare_digest(hashlib.sha256(artifact).hexdigest(), digest):
        raise ValueError("native-tree holdout artifact digest is invalid")
    return artifact


def _holdout_layout(request, config):
    bins = config["holdout-validation-bins"]
    if type(bins) is not int or not 4 <= bins <= 512:
        raise ValueError("native-tree holdout bins are invalid")
    if request["task"] == "binary":
        return validation.validation_layout(
            "classification", n_classes=2, bins=bins)
    return validation.validation_layout("regression", bins=bins)


def _cache_vector(context, claim, vector):
    state = _holdout_state(context)
    state[_REPLY_CACHE] = ArrayRecord(numpy_ndarrays=[
        np.asarray(vector, dtype=np.float64)])
    state[_REPLY_CACHE_META] = ConfigRecord({
        "message-id": claim["message_id"],
        "request-id": claim["request_id"],
        "release-index": int(claim["release_index"]),
        "operation": "holdout-evaluate",
        "fold": 0,
    })


def _replay_vector(context, claim, msg, layout):
    arrays = _holdout_state(context).get(_REPLY_CACHE)
    if not isinstance(arrays, ArrayRecord):
        raise RuntimeError("native-tree holdout replay state is incomplete")
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise RuntimeError("native-tree holdout replay state is incomplete")
    vector = np.asarray(decoded[0], dtype=np.float64)
    if vector.shape != (int(layout["size"]),) or \
            not bool(np.all(np.isfinite(vector))):
        raise RuntimeError("native-tree holdout replay state is invalid")
    return _vector_reply(msg, vector)


def _cv_vector_sha256(value):
    canonical = np.asarray(value, dtype="<f8", order="C")
    return hashlib.sha256(canonical.tobytes(order="C")).hexdigest()


def _cv_binding(context, request, layout):
    manifest = task._load_manifest(context)
    contract = resampling.cross_validation_contract_from_manifest(manifest)
    job = manifest.get("cv-job-sha256")
    if not isinstance(job, str) or not _HEX_64_RE.fullmatch(job):
        raise RuntimeError("native-tree CV state has no public job pin")
    return {
        "contract-sha256": contract["sha256"],
        "folds": int(contract["folds"]),
        "job-sha256": job,
        "layout-size": int(layout["size"]),
        "request-sha256": manifest["native-tree-request-sha256"],
        "task": request["task"],
        "version": "dsflower-native-tree-cv-state-v1",
    }


def _read_cv_total(context, binding):
    state = _holdout_state(context)
    meta = state.get(_CV_OOF_META)
    arrays = state.get(_CV_OOF_TOTAL)
    if meta is None and arrays is None:
        return None
    if not isinstance(meta, ConfigRecord) or not isinstance(arrays, ArrayRecord):
        raise RuntimeError("native-tree CV OOF state is incomplete")
    for key, value in binding.items():
        if meta.get(key) != value:
            raise RuntimeError("native-tree CV OOF state binding changed")
    values = arrays.to_numpy_ndarrays()
    if len(values) != 1:
        raise RuntimeError("native-tree CV OOF state geometry changed")
    total = np.asarray(values[0], dtype=np.float64)
    if total.shape != (binding["layout-size"],) or \
            not bool(np.all(np.isfinite(total))) or \
            meta.get("total-sha256") != _cv_vector_sha256(total):
        raise RuntimeError("native-tree CV OOF state digest changed")
    completed = meta.get("completed-folds")
    if type(completed) is not int or not 1 <= completed <= binding["folds"]:
        raise RuntimeError("native-tree CV OOF state metadata is invalid")
    return dict(meta), total


def _cv_accumulation_status(context, request, layout, fold, artifact_sha256):
    binding = _cv_binding(context, request, layout)
    state = _read_cv_total(context, binding)
    completed = 0 if state is None else int(state[0]["completed-folds"])
    if fold <= completed:
        expected = state[0].get("fold-%d-artifact-sha256" % fold)
        if expected != artifact_sha256:
            raise RuntimeError("native-tree CV fold artifact changed")
        return "replay"
    if fold != completed + 1:
        raise RuntimeError("native-tree CV folds must accumulate in order")
    return "new"


def _store_cv_sufficient(context, request, layout, fold, artifact_sha256, raw):
    binding = _cv_binding(context, request, layout)
    canonical = np.asarray(raw, dtype=np.float64)
    if canonical.shape != (binding["layout-size"],) or \
            not bool(np.all(np.isfinite(canonical))):
        raise RuntimeError("native-tree CV sufficient vector is invalid")
    state = _read_cv_total(context, binding)
    if state is None:
        meta = dict(binding)
        total = np.zeros(binding["layout-size"], dtype=np.float64)
        completed = 0
    else:
        meta, total = state
        completed = int(meta["completed-folds"])
    if fold != completed + 1:
        raise RuntimeError("native-tree CV folds must accumulate in order")
    total = total + canonical
    if not bool(np.all(np.isfinite(total))):
        raise RuntimeError("native-tree CV sufficient statistics overflowed")
    meta.update({
        "completed-folds": int(fold),
        "fold-%d-artifact-sha256" % fold: artifact_sha256,
        "fold-%d-raw-sha256" % fold: _cv_vector_sha256(canonical),
        "total-sha256": _cv_vector_sha256(total),
    })
    state_dict = _holdout_state(context)
    state_dict[_CV_OOF_TOTAL] = ArrayRecord(numpy_ndarrays=[total])
    state_dict[_CV_OOF_META] = ConfigRecord(meta)


def _complete_cv_total(context, request, layout):
    binding = _cv_binding(context, request, layout)
    state = _read_cv_total(context, binding)
    if state is None or int(state[0]["completed-folds"]) != binding["folds"]:
        raise RuntimeError("native-tree CV OOF accumulation is incomplete")
    return state[1]


def _mark_cv_training(context, request, manifest, fold, artifact):
    state = _holdout_state(context)
    binding = {
        "contract-sha256": manifest["cv-contract-sha256"],
        "job-sha256": manifest["cv-job-sha256"],
        "request-sha256": manifest["native-tree-request-sha256"],
        "version": "dsflower-native-tree-cv-train-state-v1",
    }
    meta = state.get(_CV_TRAIN_META)
    record = dict(binding) if meta is None else dict(meta)
    if any(record.get(key) != value for key, value in binding.items()):
        raise RuntimeError("native-tree CV training state binding changed")
    digest = hashlib.sha256(artifact).hexdigest()
    key = "fold-%d-artifact-sha256" % fold
    if key in record and record[key] != digest:
        raise RuntimeError("native-tree CV fold training artifact changed")
    record[key] = digest
    state[_CV_TRAIN_META] = ConfigRecord(record)


def _require_cv_training(context, manifest, fold):
    meta = _holdout_state(context).get(_CV_TRAIN_META)
    expected = {
        "contract-sha256": manifest["cv-contract-sha256"],
        "job-sha256": manifest["cv-job-sha256"],
        "request-sha256": manifest["native-tree-request-sha256"],
        "version": "dsflower-native-tree-cv-train-state-v1",
    }
    key = "fold-%d-artifact-sha256" % fold
    if not isinstance(meta, ConfigRecord) or any(
            meta.get(name) != value for name, value in expected.items()) or \
            not isinstance(meta.get(key), str) or \
            not _HEX_64_RE.fullmatch(meta[key]):
        raise RuntimeError("native-tree CV fold training is incomplete")


def _forget_cv_state(context):
    state = _holdout_state(context)
    for key in (_CV_OOF_META, _CV_OOF_TOTAL, _CV_TRAIN_META):
        state.pop(key, None)


def _cache_cv_reply(context, claim, value):
    array = np.asarray(value).copy()
    state = _holdout_state(context)
    state[_REPLY_CACHE] = ArrayRecord(numpy_ndarrays=[array])
    state[_REPLY_CACHE_META] = ConfigRecord({
        "fold": int(claim["fold"]),
        "message-id": claim["message_id"],
        "operation": claim["operation"],
        "release-index": int(claim["release_index"]),
        "request-id": claim["request_id"],
    })


def _replay_cv_reply(context, claim, msg):
    state = _holdout_state(context)
    meta = state.get(_REPLY_CACHE_META)
    arrays = state.get(_REPLY_CACHE)
    if not isinstance(meta, ConfigRecord) or not isinstance(arrays, ArrayRecord) or \
            meta.get("request-id") != claim["request_id"] or \
            meta.get("operation") != claim["operation"] or \
            meta.get("fold") != claim["fold"] or \
            meta.get("release-index") != claim["release_index"]:
        raise RuntimeError("native-tree CV replay state is incomplete")
    values = arrays.to_numpy_ndarrays()
    if len(values) != 1:
        raise RuntimeError("native-tree CV replay state is incomplete")
    value = np.asarray(values[0])
    if claim["operation"] == "cv-release":
        if value.dtype != np.float64 or value.ndim != 1 or \
                not bool(np.all(np.isfinite(value))):
            raise RuntimeError("native-tree CV release replay is invalid")
        return _vector_reply(msg, value)
    if value.dtype != np.uint8 or value.ndim != 1:
        raise RuntimeError("native-tree CV replay is invalid")
    return _reply(msg, value.tobytes(), True)


def _evaluate_holdout(msg, context, request, node_manifest, privacy):
    config = _exact_message_config(msg)
    layout = _holdout_layout(request, config)
    _require_training_complete(context, request, node_manifest)
    spec = native_tree_engine.release_spec(request["engine"])
    expected_nodes = getattr(context, "run_config", {}).get(
        "min-train-nodes")
    if config["native-tree-artifact-format"] != spec["ensemble_format"] or \
            config["native-tree-public-schema-sha256"] != request[
                "public_schema"]["sha256"] or \
            type(config["native-tree-n-nodes"]) is not int or \
            type(expected_nodes) is not int or expected_nodes < 1 or \
            config["native-tree-n-nodes"] != expected_nodes or \
            config["holdout-validation-bins"] != node_manifest.get(
                "holdout-validation-bins"):
        raise ValueError("native-tree holdout artifact contract is invalid")
    public_manifest = native_tree_request.public_backend_manifest(request)
    artifact = _artifact_from_message(
        msg, config, public_manifest["resources"]["max_artifact_bytes"])
    model = native_tree_engine.parse_ensemble(public_manifest, artifact)
    expected_task = "binary" if request["task"] == "binary" else "regression"
    if model.task != expected_task or \
            model.num_features != len(request["public_schema"]["features"]):
        raise ValueError("native-tree holdout predictor geometry is invalid")
    claim = release_guard.claim_release(context, msg)
    if claim["status"] == "replay":
        return _replay_vector(context, claim, msg, layout)
    features, target, unit_ids = task.load_native_tree_data(
        context, manifest=node_manifest)
    features, target, unit_ids = _partition(
        context, features, target, unit_ids, test=True)
    bounds = (None if request["task"] == "binary" else {
        "lower": request["public_schema"]["target"]["lower"],
        "upper": request["public_schema"]["target"]["upper"],
    })
    if target.shape[0] == 0:
        released, _sigma = validation.private_sufficient_vector(
            np.zeros(int(layout["size"]), dtype=np.float64), layout,
            epsilon=privacy["epsilon"], delta=privacy["delta"],
            num_releases=1,
            include_zero_neighbor=privacy["unit"] == "patient")
    else:
        predictions = np.asarray(model.predict(features), dtype=np.float64)
        released, _sigma = validation.private_validation_vector(
            target, predictions, layout, epsilon=privacy["epsilon"],
            delta=privacy["delta"], target_bounds=bounds,
            num_releases=1, unit_ids=unit_ids,
            include_zero_neighbor=privacy["unit"] == "patient")
    _cache_vector(context, claim, released)
    return _vector_reply(msg, released)


def _cv_layout(request, config):
    bins = config["cv-validation-bins"]
    if type(bins) is not int or not 4 <= bins <= 512:
        raise ValueError("native-tree CV bins are invalid")
    return _holdout_layout(request, {"holdout-validation-bins": bins})


def _validate_cv_artifact_profile(request, node_manifest, config):
    spec = native_tree_engine.release_spec(request["engine"])
    expected_nodes = getattr(node_manifest, "get", lambda *_: None)(
        "cv-n-nodes")
    schema_hash = config["native-tree-public-schema-sha256"]
    artifact_format = config["native-tree-artifact-format"]
    artifact_hash = config["native-tree-artifact-sha256"]
    supplied_nodes = config["native-tree-n-nodes"]
    if not isinstance(schema_hash, str) or \
            not _HEX_64_RE.fullmatch(schema_hash) or \
            schema_hash != request["public_schema"]["sha256"] or \
            type(expected_nodes) is not int or expected_nodes < 1 or \
            type(supplied_nodes) is not int or \
            supplied_nodes != expected_nodes or \
            not isinstance(artifact_format, str) or \
            artifact_format != spec["ensemble_format"] or \
            not isinstance(artifact_hash, str) or \
            not _HEX_64_RE.fullmatch(artifact_hash):
        raise ValueError("native-tree CV artifact profile is invalid")


def _cross_validation_accumulate(msg, context, request, node_manifest,
                                 claim):
    config = _exact_message_config(msg)
    layout = _cv_layout(request, config)
    _validate_cv_artifact_profile(request, node_manifest, config)
    fold = int(claim["fold"])
    _require_cv_training(context, node_manifest, fold)
    public_manifest = native_tree_request.public_backend_manifest(request)
    artifact_sha256 = config["native-tree-artifact-sha256"]
    artifact = _artifact_from_message(
        msg, config, public_manifest["resources"]["max_artifact_bytes"])
    status = _cv_accumulation_status(
        context, request, layout, fold, artifact_sha256)
    if status == "replay":
        _cache_cv_reply(context, claim, _CV_ACK)
        return _reply(msg, _CV_ACK.tobytes(), True)
    model = native_tree_engine.parse_ensemble(public_manifest, artifact)
    expected_task = "binary" if request["task"] == "binary" else "regression"
    if model.task != expected_task or \
            model.num_features != len(request["public_schema"]["features"]):
        raise ValueError("native-tree CV predictor geometry is invalid")
    features, target, unit_ids = task.load_native_tree_data(
        context, manifest=node_manifest)
    features, target, unit_ids = _cv_partition(
        context, features, target, unit_ids, fold=fold, test=True)
    bounds = (None if request["task"] == "binary" else {
        "lower": request["public_schema"]["target"]["lower"],
        "upper": request["public_schema"]["target"]["upper"],
    })
    if target.shape[0] == 0:
        raw = np.zeros(int(layout["size"]), dtype=np.float64)
    else:
        predictions = np.asarray(model.predict(features), dtype=np.float64)
        raw = validation.validation_sufficient_vector(
            target, predictions, layout, target_bounds=bounds,
            unit_ids=unit_ids)
    _store_cv_sufficient(
        context, request, layout, fold, artifact_sha256, raw)
    _cache_cv_reply(context, claim, _CV_ACK)
    return _reply(msg, _CV_ACK.tobytes(), True)


def _cross_validation_release(msg, context, request, privacy, claim):
    config = _exact_message_config(msg)
    layout = _cv_layout(request, config)
    raw = _complete_cv_total(context, request, layout)
    released, _sigma = validation.private_sufficient_vector(
        raw, layout, epsilon=privacy["epsilon"], delta=privacy["delta"],
        num_releases=1, include_zero_neighbor=False)
    _forget_cv_state(context)
    _cache_cv_reply(context, claim, released)
    return _vector_reply(msg, released)


def _cross_validation_abort(msg, context, claim):
    _forget_cv_state(context)
    _cache_cv_reply(context, claim, _CV_ACK)
    return _reply(msg, _CV_ACK.tobytes(), True)


@app.train()
def train(msg: Message, context: Context) -> Message:
    try:
        request, node_manifest, privacy, operation = _pinned_request(
            msg, context)
        if operation == "holdout-evaluate":
            return _evaluate_holdout(
                msg, context, request, node_manifest, privacy)
        claim = None
        if operation.startswith("cv-"):
            claim = release_guard.claim_release(context, msg)
            if claim["status"] == "replay":
                return _replay_cv_reply(context, claim, msg)
            if operation == "cv-abort":
                return _cross_validation_abort(msg, context, claim)
            if operation == "cv-release":
                return _cross_validation_release(
                    msg, context, request, privacy, claim)
            if operation == "cv-accumulate":
                return _cross_validation_accumulate(
                    msg, context, request, node_manifest, claim)
        engine = request["engine"]
        if native_tree_engine.requires_xgboost_bundle(engine) and not \
                xgboost_bundle.is_verified_bundle(_NATIVE_BUNDLE):
            return _unavailable_reply(msg)
        features, target, unit_ids = task.load_native_tree_data(
            context, manifest=node_manifest)
        if node_manifest.get("resampling-contract-sha256") is not None:
            features, target, unit_ids = _partition(
                context, features, target, unit_ids, test=False)
        elif operation == "cv-train":
            features, target, unit_ids = _cv_partition(
                context, features, target, unit_ids,
                fold=int(claim["fold"]), test=False)
        # The generic ABI requires opaque scope fields, but the native adapter
        # deliberately excludes them from sticky randomness and binds the
        # canonical effective tensors itself.  Reuse the public schema pin here:
        # no redundant O(N) pass and no second private identity channel.
        scope_hash = request["public_schema"]["sha256"]
        manifest = native_tree_request.backend_manifest(
            request, snapshot_hash=scope_hash, cohort_hash=scope_hash,
            **{key: privacy[key] for key in (
                "epsilon", "delta", "unit", "unit_canonicalization",
                "gradient_clip")})
        if engine == "xgboost":
            # Preserve the verified native-bundle path byte-for-byte in scope.
            prepared = xgboost_adapter.prepare_xgboost_training(
                manifest, features, target, native_bundle=_NATIVE_BUNDLE,
                unit_ids=unit_ids)
            native_artifact = xgboost_adapter.train_xgboost_native(prepared)
            artifact, _digest = xgboost_adapter.sanitize_xgboost_artifact(
                manifest, native_artifact)
        else:
            artifact = native_tree_engine.train_model(
                manifest, features, target, unit_ids=unit_ids)
        if node_manifest.get("resampling-contract-sha256") is not None:
            _mark_training_complete(
                context, request, node_manifest, artifact)
        elif operation == "cv-train":
            _mark_cv_training(
                context, request, node_manifest, int(claim["fold"]), artifact)
            _cache_cv_reply(
                context, claim,
                np.frombuffer(artifact, dtype=np.uint8).copy())
        return _reply(msg, artifact, True)
    except Exception:
        # Never expose paths, native diagnostics, private counts or exceptions.
        return _unavailable_reply(msg)


__all__ = ["app", "train"]
