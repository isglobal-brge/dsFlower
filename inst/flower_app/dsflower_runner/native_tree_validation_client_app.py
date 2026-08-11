"""Dedicated trusted ClientApp for private native-tree model validation."""

import hashlib
import hmac
import io
import os
import re
import sys


_FORBIDDEN_MODULES = (
    "torch", "opacus", "dsflower_runner.client_app",
    "dsflower_runner.egress_child", "dsflower_runner.tier2_lib",
    "dsflower_runner.native_tree_client_app",
    "dsflower_runner.native_tree_server_app",
    "dsflower_runner.xgboost_adapter", "dsflower_runner.xgboost_bundle",
    "dsflower_runner.xgboost_native",
)


def _assert_native_process_isolated():
    """Fail if the dependency-light validator admitted a wider runner."""
    if os.environ.get("DSFLOWER_PINNED_APP_DIR"):
        raise RuntimeError("native validation cannot admit uploaded code")
    for name in tuple(sys.modules):
        if any(name == forbidden or name.startswith(forbidden + ".")
               for forbidden in _FORBIDDEN_MODULES):
            raise RuntimeError("native validation process is not isolated")


if os.environ.get("DSFLOWER_MANIFEST_DIR"):
    _assert_native_process_isolated()


import numpy as np
from flwr.clientapp import ClientApp
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from . import native_tree_engine, native_tree_request, task, validation


app = ClientApp()
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_NPY_HEADER_ALLOWANCE = 4096
_MESSAGE_CONFIG_FIELDS = frozenset((
    "server-round",
    "validation-artifact-format",
    "validation-artifact-sha256",
    "validation-artifact-size-bytes",
    "validation-bins",
    "validation-contract-sha256",
    "validation-model-track",
    "validation-native-tree-request-b64",
    "validation-native-tree-request-sha256",
    "validation-profile-sha256",
    "validation-profile-size-bytes",
    "validation-public-schema-sha256",
    "validation-task",
))
_PIN_FIELDS = _MESSAGE_CONFIG_FIELDS - frozenset(("server-round",))


def _reply(msg, vector, available):
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[np.asarray(vector)]),
        "metrics": MetricRecord({
            "available": int(bool(available)),
            "num-examples": 1,
        }),
    }), reply_to=msg)


def _unavailable_reply(msg):
    return _reply(msg, np.zeros(1, dtype=np.float64), False)


def _exact_message_config(msg):
    if frozenset(msg.content.keys()) != frozenset(("arrays", "config")):
        raise ValueError("native validation message has an invalid shape")
    config = msg.content["config"]
    if not isinstance(config, ConfigRecord) or \
            frozenset(config.keys()) != _MESSAGE_CONFIG_FIELDS or \
            type(config["server-round"]) is not int or \
            config["server-round"] != 1:
        raise ValueError("native validation message has an invalid config")
    return config


def _positive_integer(value, where, upper):
    if type(value) is not int or not 1 <= value <= upper:
        raise ValueError("%s is outside its public bound" % where)
    return value


def _artifact_from_message(msg, byte_cap, expected_size, expected_sha256):
    arrays = msg.content["arrays"]
    if not isinstance(arrays, ArrayRecord):
        raise ValueError("native validation artifact record is invalid")
    encoded = list(arrays.values())
    if len(encoded) != 1:
        raise ValueError("native validation requires one public artifact")
    item = encoded[0]
    if item.stype != "numpy.ndarray" or item.dtype != "uint8" or \
            len(item.shape) != 1 or item.shape[0] != expected_size or \
            item.shape[0] > byte_cap or \
            len(item.data) > byte_cap + _NPY_HEADER_ALLOWANCE:
        raise ValueError("native validation artifact exceeds its public profile")
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
        if fortran or tuple(shape) != (expected_size,) or \
                np.dtype(dtype) != np.dtype("uint8") or \
                len(item.data) - stream.tell() != expected_size:
            raise ValueError("invalid NPY geometry")
    except Exception as exc:
        raise ValueError("native validation artifact encoding is invalid") from exc
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise ValueError("native validation requires one public artifact")
    value = np.asarray(decoded[0])
    if value.dtype != np.uint8 or value.ndim != 1 or value.size != expected_size:
        raise ValueError("native validation artifact payload is invalid")
    artifact = value.tobytes()
    if not hmac.compare_digest(
            hashlib.sha256(artifact).hexdigest(), expected_sha256):
        raise ValueError("native validation artifact digest is invalid")
    return artifact


def _validate_node_manifest(request, manifest, message_config):
    schema = request["public_schema"]
    task_name = message_config["validation-task"]
    if manifest.get("data_type") != "tabular" or \
            manifest.get("dp-track") != "validation" or \
            manifest.get("validation-model-track") != "native_tree" or \
            type(manifest.get("num-server-rounds")) is not int or \
            manifest["num-server-rounds"] != 1 or \
            manifest.get("target-preencoded") is not True or \
            manifest.get("user-module") not in (None, "") or \
            manifest.get("feature_columns") != schema["features"] or \
            manifest.get("target_column") != schema["target"]["name"] or \
            manifest.get("validation-task") != task_name or \
            manifest.get("validation-bins") != message_config["validation-bins"] or \
            manifest.get("num-features") != len(schema["features"]) or \
            manifest.get("num-classes") != 2 or \
            manifest.get("num-labels") != 2 or \
            manifest.get("loss-name") != ("bce_logits" if task_name == "binary"
                                           else "mse"):
        raise ValueError("node manifest differs from native validation pins")
    bounds = manifest.get("feature-bounds")
    if not isinstance(bounds, dict) or bounds.get("lower") != schema["lower"] or \
            bounds.get("upper") != schema["upper"]:
        raise ValueError("node feature bounds differ from native validation")

    target = schema["target"]
    if task_name == "binary":
        levels = manifest.get("target-levels")
        type_map = {
            "character": "string", "logical": "boolean", "numeric": "number",
        }
        expected_type = (type_map.get(levels.get("type"))
                         if isinstance(levels, dict) else None)
        expected_values = levels.get("values") if isinstance(levels, dict) else None
        request_levels = target.get("levels")
        if manifest.get("task-type") != "classification" or \
                not isinstance(request_levels, list) or \
                [item.get("type") for item in request_levels] != \
                [expected_type] * len(request_levels) or \
                [item.get("value") for item in request_levels] != expected_values:
            raise ValueError("node target levels differ from native validation")
    else:
        target_bounds = manifest.get("target-bounds")
        if manifest.get("task-type") != "regression" or \
                not isinstance(target_bounds, dict) or \
                target_bounds.get("lower") != target.get("lower") or \
                target_bounds.get("upper") != target.get("upper"):
            raise ValueError("node target bounds differ from native validation")


def _pinned_public_model(msg, context):
    message_config = _exact_message_config(msg)
    manifest = task._load_manifest(context)
    raw_config = dict(context.run_config)
    if raw_config.get("dp-track") != "validation" or \
            type(raw_config.get("num-server-rounds")) is not int or \
            raw_config["num-server-rounds"] != 1:
        raise ValueError("Flower validation config is not one exact release")
    run_config = task.load_pinned_run_config(context)
    for key in _PIN_FIELDS:
        if key not in manifest or run_config.get(key) != manifest[key] or \
                message_config[key] != manifest[key]:
            raise ValueError("Flower validation config differs from manifest pin")
    artifact_size = _positive_integer(
        manifest.get("validation-artifact-size-bytes"),
        "native validation artifact size", 64 * 1024 * 1024)
    profile_size = _positive_integer(
        manifest.get("validation-profile-size-bytes"),
        "native validation profile size", 128 * 1024)
    del profile_size
    for key in (
            "validation-artifact-sha256", "validation-contract-sha256",
            "validation-native-tree-request-sha256",
            "validation-profile-sha256", "validation-public-schema-sha256"):
        if not isinstance(manifest.get(key), str) or not _HEX_64_RE.fullmatch(
                manifest[key]):
            raise ValueError("native validation digest pin is invalid")
    request = native_tree_request.parse_request_wire(
        manifest["validation-native-tree-request-b64"],
        manifest["validation-native-tree-request-sha256"])
    release_spec = native_tree_engine.release_spec(request["engine"])
    if manifest.get("validation-artifact-format") != \
            release_spec["ensemble_format"]:
        raise ValueError("native validation artifact format is unsupported")
    expected_task = "binary" if request["task"] == "binary" else "regression"
    if manifest["validation-task"] != expected_task or \
            request["public_schema"]["sha256"] != \
            manifest["validation-public-schema-sha256"]:
        raise ValueError("native validation request differs from manifest pins")
    _validate_node_manifest(request, manifest, message_config)
    public_manifest = native_tree_request.public_backend_manifest(request)
    byte_cap = public_manifest["resources"]["max_artifact_bytes"]
    artifact = _artifact_from_message(
        msg, byte_cap, artifact_size,
        manifest["validation-artifact-sha256"])
    # This parse re-sanitizes every model member and validates the complete public
    # prediction profile. It is deliberately the final step before private I/O.
    model = native_tree_engine.parse_ensemble(public_manifest, artifact)
    layout = validation.layout_from_config(run_config)
    if model.task != expected_task or \
            model.num_features != len(request["public_schema"]["features"]):
        raise ValueError("native validation predictor geometry is invalid")
    return model, layout, manifest, run_config


@app.train()
def train(msg: Message, context: Context) -> Message:
    try:
        model, layout, node_manifest, cfg = _pinned_public_model(msg, context)
        pcfg = task.load_privacy_config(context)
        features, target, unit_ids = task.load_native_tree_data(
            context, manifest=node_manifest)
        predictions = np.asarray(model.predict(features), dtype=np.float64)
        target_bounds = (validation.target_bounds_from_config(cfg)
                         if layout["task"] == "regression" else None)
        released, _sigma = validation.private_validation_vector(
            target, predictions, layout, epsilon=pcfg["epsilon"],
            delta=pcfg["delta"], target_bounds=target_bounds,
            num_releases=1, unit_ids=unit_ids)
        return _reply(msg, released.astype(np.float64), True)
    except Exception:
        # Never expose paths, parser diagnostics, private counts or exceptions.
        return _unavailable_reply(msg)


__all__ = ["app", "train"]
