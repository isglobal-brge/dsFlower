"""Dedicated coordinator ServerApp for private native-tree validation."""

import base64
import hashlib
import hmac
import json
import math
import os
import re
import time

import numpy as np
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)
from flwr.serverapp import Grid, ServerApp

from . import native_tree_engine, native_tree_request, validation


app = ServerApp()
HISTORY_FILE = "history.json"
RESULT_FILE = "validation.json"
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_REPLY_FIELDS = frozenset(("arrays", "metrics"))
_METRIC_FIELDS = frozenset(("available", "num-examples"))
_MESSAGE_PIN_FIELDS = (
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
)


def _positive_integer(value, where, upper=None):
    if type(value) is not int or value < 1 or \
            (upper is not None and value > upper):
        raise ValueError("%s must be a bounded positive integer" % where)
    return value


def _absolute_path(encoded, where):
    if not isinstance(encoded, str) or not encoded or len(encoded) > 16384:
        raise ValueError("%s path is missing or oversized" % where)
    try:
        raw = base64.b64decode(encoded, validate=True)
        path = raw.decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("%s path is invalid" % where) from exc
    if not path or "\x00" in path or not os.path.isabs(path):
        raise ValueError("%s path must be absolute" % where)
    return path


def _read_exact_file(path, size, digest, byte_cap, where):
    if not isinstance(digest, str) or not _HEX_64_RE.fullmatch(digest):
        raise ValueError("%s digest is invalid" % where)
    _positive_integer(size, "%s size" % where, byte_cap)
    try:
        info = os.stat(path)
    except OSError as exc:
        raise ValueError("%s is unavailable" % where) from exc
    if not os.path.isfile(path) or info.st_size != size:
        raise ValueError("%s size differs from its pin" % where)
    with open(path, "rb") as handle:
        value = handle.read(size + 1)
    if len(value) != size or not hmac.compare_digest(
            hashlib.sha256(value).hexdigest(), digest):
        raise ValueError("%s differs from its digest pin" % where)
    return value


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _validate_profile(profile_bytes, cfg, request, artifact):
    spec = native_tree_engine.validate_prediction_profile(
        profile_bytes, request,
        cfg["validation-native-tree-request-b64"],
        cfg["validation-native-tree-request-sha256"], artifact)
    if cfg["validation-artifact-format"] != spec["ensemble_format"] or \
            cfg["validation-artifact-sha256"] != \
            hashlib.sha256(artifact).hexdigest() or \
            cfg["validation-artifact-size-bytes"] != len(artifact):
        raise ValueError("native validation profile bindings are invalid")


def _run_contract(cfg):
    if cfg.get("dp-track") != "validation" or \
            cfg.get("validation-model-track") != "native_tree" or \
            type(cfg.get("num-server-rounds")) is not int or \
            cfg["num-server-rounds"] != 1:
        raise ValueError("native validation ServerApp requires one exact release")
    expected = _positive_integer(cfg.get("min-train-nodes"), "node roster")
    bins = _positive_integer(cfg.get("validation-bins"), "validation bins", 512)
    if bins < 4:
        raise ValueError("validation bins are outside their public bound")
    task_name = cfg.get("validation-task")
    loss = cfg.get("loss-name")
    if task_name not in ("binary", "regression") or \
            loss != ("bce_logits" if task_name == "binary" else "mse"):
        raise ValueError("native validation task profile is invalid")
    for key in (
            "validation-artifact-sha256", "validation-contract-sha256",
            "validation-native-tree-request-sha256",
            "validation-profile-sha256", "validation-public-schema-sha256"):
        if not isinstance(cfg.get(key), str) or not _HEX_64_RE.fullmatch(cfg[key]):
            raise ValueError("native validation digest pin is invalid")
    request = native_tree_request.parse_request_wire(
        cfg.get("validation-native-tree-request-b64"),
        cfg.get("validation-native-tree-request-sha256"))
    release_spec = native_tree_engine.release_spec(request["engine"])
    if cfg.get("validation-artifact-format") != \
            release_spec["ensemble_format"]:
        raise ValueError("native validation artifact format is unsupported")
    public_manifest = native_tree_request.public_backend_manifest(request)
    expected_task = "binary" if request["task"] == "binary" else "regression"
    features = _positive_integer(
        cfg.get("num-features"), "native validation feature count", 65536)
    if task_name != expected_task or request["public_schema"]["sha256"] != \
            cfg["validation-public-schema-sha256"] or \
            features != len(request["public_schema"]["features"]) or \
            type(cfg.get("num-classes")) is not int or \
            cfg["num-classes"] != 2 or \
            type(cfg.get("num-labels")) is not int or cfg["num-labels"] != 2:
        raise ValueError("native validation request differs from run pins")
    target = request["public_schema"]["target"]
    if task_name == "regression":
        lower = cfg.get("validation-target-lower")
        upper = cfg.get("validation-target-upper")
        if isinstance(lower, bool) or isinstance(upper, bool) or \
                not isinstance(lower, (int, float)) or \
                not isinstance(upper, (int, float)) or \
                not math.isfinite(float(lower)) or \
                not math.isfinite(float(upper)) or \
                float(lower) != float(target["lower"]) or \
                float(upper) != float(target["upper"]):
            raise ValueError("native validation target bounds differ from request")
    elif "validation-target-lower" in cfg or "validation-target-upper" in cfg:
        raise ValueError("binary native validation does not accept target bounds")
    layout = validation.layout_from_config(dict(cfg))
    artifact_size = _positive_integer(
        cfg.get("validation-artifact-size-bytes"),
        "native validation artifact size",
        public_manifest["resources"]["max_artifact_bytes"])
    profile_size = _positive_integer(
        cfg.get("validation-profile-size-bytes"),
        "native validation profile size", 128 * 1024)
    artifact = _read_exact_file(
        _absolute_path(cfg.get("validation-model-path-b64"), "model"),
        artifact_size, cfg["validation-artifact-sha256"],
        public_manifest["resources"]["max_artifact_bytes"], "model")
    profile = _read_exact_file(
        _absolute_path(cfg.get("validation-profile-path-b64"), "profile"),
        profile_size, cfg["validation-profile-sha256"], 128 * 1024,
        "profile")
    _validate_profile(profile, cfg, request, artifact)
    parsed = native_tree_engine.parse_ensemble(public_manifest, artifact)
    if parsed.task != task_name or parsed.num_features != features:
        raise ValueError("native validation predictor geometry is invalid")
    timeout = cfg.get("round-timeout", request["resources"]["timeout_seconds"])
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or \
            not math.isfinite(float(timeout)) or not 0.0 < float(timeout) <= \
            request["resources"]["timeout_seconds"]:
        raise ValueError("native validation timeout is invalid")
    results_dir = cfg.get("results-dir")
    if not isinstance(results_dir, str) or not results_dir:
        raise ValueError("native validation results directory is missing")
    return (request, public_manifest, layout, artifact, expected,
            float(timeout), results_dir)


def _exact_roster(grid, expected, timeout):
    deadline = time.monotonic() + timeout
    while True:
        node_ids = list(grid.get_node_ids())
        if any(type(node_id) is not int for node_id in node_ids) or \
                len(set(node_ids)) != len(node_ids):
            raise RuntimeError("native validation roster is invalid")
        if len(node_ids) > expected:
            raise RuntimeError("native validation has an extra node")
        if len(node_ids) == expected:
            return tuple(sorted(node_ids))
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise RuntimeError("native validation roster is incomplete")
        time.sleep(min(0.25, remaining))


def _request_messages(node_ids, cfg, artifact):
    messages = []
    for node_id in node_ids:
        config = {key: cfg[key] for key in _MESSAGE_PIN_FIELDS}
        config["server-round"] = 1
        content = RecordDict({
            "arrays": ArrayRecord(numpy_ndarrays=[
                np.frombuffer(artifact, dtype=np.uint8).copy()]),
            "config": ConfigRecord(config),
        })
        messages.append(Message(
            content=content, message_type="train", dst_node_id=node_id,
            group_id="dsflower-native-validation-v1"))
    return messages


def _vector_from_reply(reply, layout):
    if reply.has_error() or frozenset(reply.content.keys()) != _REPLY_FIELDS:
        raise RuntimeError("native validation node release is unavailable")
    metrics = reply.content["metrics"]
    if not isinstance(metrics, MetricRecord) or \
            frozenset(metrics.keys()) != _METRIC_FIELDS or \
            type(metrics["available"]) is not int or metrics["available"] != 1 or \
            type(metrics["num-examples"]) is not int or \
            metrics["num-examples"] != 1:
        raise RuntimeError("native validation node release is unavailable")
    arrays = reply.content["arrays"]
    if not isinstance(arrays, ArrayRecord) or len(list(arrays.values())) != 1:
        raise RuntimeError("native validation release has invalid geometry")
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise RuntimeError("native validation release has invalid geometry")
    vector = np.asarray(decoded[0], dtype=np.float64)
    if vector.shape != (int(layout["size"]),) or \
            not bool(np.all(np.isfinite(vector))):
        raise RuntimeError("native validation release has invalid geometry")
    return vector


def _collect_vectors(grid, node_ids, cfg, artifact, layout, timeout):
    messages = _request_messages(node_ids, cfg, artifact)
    replies = list(grid.send_and_receive(messages, timeout=timeout))
    try:
        sources = [reply.metadata.src_node_id for reply in replies]
    except (AttributeError, TypeError) as exc:
        raise RuntimeError("native validation reply roster is invalid") from exc
    if any(type(source) is not int for source in sources) or \
            len(replies) != len(node_ids) or len(set(sources)) != len(node_ids) or \
            set(sources) != set(node_ids):
        raise RuntimeError("native validation reply roster is incomplete or duplicated")
    pairs = [
        (source, _vector_from_reply(reply, layout))
        for source, reply in zip(sources, replies)
    ]
    return [vector for _source, vector in sorted(pairs, key=lambda item: item[0])]


def _stable_pool(vectors):
    stacked = np.stack(vectors, axis=0).astype(np.float64, copy=False)
    scale = np.max(np.abs(stacked), axis=0)
    normalized = np.divide(
        stacked, scale, out=np.zeros_like(stacked), where=scale > 0.0)
    wide = (np.sum(normalized.astype(np.longdouble), axis=0,
                   dtype=np.longdouble) * scale.astype(np.longdouble))
    limit = np.longdouble(np.finfo(np.float64).max)
    pooled = np.asarray(np.clip(wide, -limit, limit), dtype=np.float64)
    if not bool(np.all(np.isfinite(pooled))):
        raise RuntimeError("native validation pooled vector overflowed")
    return pooled


def _atomic_write(path, payload):
    temporary = path + ".tmp"
    try:
        with open(temporary, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _save_validation(results_dir, task_name, metrics, n_nodes, available):
    os.makedirs(results_dir, exist_ok=True)
    history_path = os.path.join(results_dir, HISTORY_FILE)
    result_path = os.path.join(results_dir, RESULT_FILE)
    if os.path.exists(history_path) or os.path.exists(result_path):
        raise RuntimeError("native validation output already exists")
    history = _canonical_json([{"available": bool(available), "round": 1}])
    payload = {
        "available": bool(available),
        "n_nodes": int(n_nodes),
        "pooled_only": True,
        "privacy": "node-dp-pooled-postprocessing",
        "task": task_name,
    }
    if available:
        payload["metrics"] = metrics
    _atomic_write(history_path, history)
    try:
        _atomic_write(result_path, validation.private_metric_result_wire(payload))
    except Exception:
        try:
            os.unlink(history_path)
        except OSError:
            pass
        raise


def _run_native_validation(grid, cfg):
    (_request, _manifest, layout, artifact, expected, timeout,
     results_dir) = _run_contract(cfg)
    node_ids = _exact_roster(grid, expected, timeout)
    vectors = _collect_vectors(
        grid, node_ids, cfg, artifact, layout, timeout)
    pooled = _stable_pool(vectors)
    bounds = (validation.target_bounds_from_config(dict(cfg))
              if layout["task"] == "regression" else None)
    metrics = validation.validation_metrics(
        pooled, layout, target_bounds=bounds)
    _save_validation(
        results_dir, cfg["validation-task"], metrics, expected, True)


@app.main()
def main(grid: Grid, context: Context) -> None:
    cfg = context.run_config
    results_dir = cfg.get("results-dir")
    try:
        _run_native_validation(grid, cfg)
    except Exception:
        if isinstance(results_dir, str) and results_dir:
            try:
                expected = _positive_integer(
                    cfg.get("min-train-nodes"), "node roster")
                task_name = cfg.get("validation-task")
                if task_name not in ("binary", "regression"):
                    raise ValueError("invalid task")
                _save_validation(
                    results_dir, task_name, None, expected, False)
            except Exception:
                pass


__all__ = ["HISTORY_FILE", "RESULT_FILE", "app", "main"]
