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
_HOLDOUT_TRAIN_STATE = "dsflower-native-tree-holdout-train-v1"
_REPLY_CACHE = "dsflower-last-release"
_REPLY_CACHE_META = "dsflower-last-release-meta"
_NPY_HEADER_ALLOWANCE = 4096


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
    expected = ((_HOLDOUT_TRAIN_MESSAGE_FIELDS
                 if "resampling-contract-sha256" in config
                 else _TRAIN_MESSAGE_FIELDS)
                if operation == "train" else _EVALUATE_MESSAGE_FIELDS)
    expected_content = (frozenset(("config",)) if operation == "train"
                        else frozenset(("arrays", "config")))
    if not isinstance(config, ConfigRecord) or \
            frozenset(config.keys()) != expected or \
            frozenset(msg.content.keys()) != expected_content or \
            type(config["server-round"]) is not int or \
            config["server-round"] != 1 or \
            operation not in ("train", "holdout-evaluate"):
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
    if has_holdout:
        fixed = release_guard._fixed_manifest(context)
        if not fixed["holdout"] or operation not in (
                "train", "holdout-evaluate"):
            raise ValueError("node holdout privacy operation is invalid")
        epsilon, delta = fixed["budgets"][operation]
    elif operation != "train":
        raise ValueError("holdout evaluation has no pinned contract")
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
                  if manifest.get("resampling-contract-sha256") is not None
                  else dict(context.run_config))
    request_b64 = manifest.get("native-tree-request-b64")
    request_sha256 = manifest.get("native-tree-request-sha256")
    operation = message_config["dsflower-operation"]
    has_holdout = manifest.get("resampling-contract-sha256") is not None
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
    request = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    _validate_public_manifest(request, manifest)
    privacy = _node_privacy(manifest, context, operation)
    # Validate the complete public/accounting profile before opening private data.
    schema_hash = request["public_schema"]["sha256"]
    preflight = native_tree_request.backend_manifest(
        request, snapshot_hash=schema_hash, cohort_hash=schema_hash,
        **{key: privacy[key] for key in (
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


@app.train()
def train(msg: Message, context: Context) -> Message:
    try:
        request, node_manifest, privacy, operation = _pinned_request(
            msg, context)
        if operation == "holdout-evaluate":
            return _evaluate_holdout(
                msg, context, request, node_manifest, privacy)
        engine = request["engine"]
        if native_tree_engine.requires_xgboost_bundle(engine) and not \
                xgboost_bundle.is_verified_bundle(_NATIVE_BUNDLE):
            return _unavailable_reply(msg)
        features, target, unit_ids = task.load_native_tree_data(
            context, manifest=node_manifest)
        if node_manifest.get("resampling-contract-sha256") is not None:
            features, target, unit_ids = _partition(
                context, features, target, unit_ids, test=False)
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
        return _reply(msg, artifact, True)
    except Exception:
        # Never expose paths, native diagnostics, private counts or exceptions.
        return _unavailable_reply(msg)


__all__ = ["app", "train"]
