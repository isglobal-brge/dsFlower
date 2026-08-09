"""Dedicated coordinator ServerApp for atomic native XGBoost federation."""

import hashlib
import json
import math
import os
import time

import numpy as np
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)
from flwr.serverapp import Grid, ServerApp

from . import native_tree_request, xgboost_adapter


app = ServerApp()
MODEL_FILE = "model.xgboost-ensemble.json"
PROFILE_FILE = "model.xgboost-ensemble.profile.json"
HISTORY_FILE = "history.json"
PREDICTION_PROFILE = "dsflower-xgboost-prediction-profile-v1"
_REPLY_FIELDS = frozenset(("arrays", "metrics"))
_METRIC_FIELDS = frozenset(("available", "num-examples"))
_NPY_HEADER_ALLOWANCE = 4096


def _positive_integer(value, where):
    if type(value) is not int or value < 1:
        raise ValueError("%s must be a positive integer" % where)
    return value


def _run_contract(cfg):
    if cfg.get("dp-track") != "native_tree" or \
            type(cfg.get("num-server-rounds")) is not int or \
            cfg["num-server-rounds"] != 1:
        raise ValueError("native-tree ServerApp requires one exact round")
    expected = _positive_integer(cfg.get("min-train-nodes"), "node roster")
    request_b64 = cfg.get("native-tree-request-b64")
    request_sha256 = cfg.get("native-tree-request-sha256")
    request = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    manifest = native_tree_request.public_backend_manifest(request)
    xgboost_adapter.canonical_xgboost_profile(manifest)
    timeout = cfg.get(
        "round-timeout", request["resources"]["timeout_seconds"])
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or \
            not math.isfinite(float(timeout)) or not 0.0 < float(timeout) <= \
            request["resources"]["timeout_seconds"]:
        raise ValueError("native-tree round timeout is invalid")
    results_dir = cfg.get("results-dir")
    if not isinstance(results_dir, str) or not results_dir:
        raise ValueError("native-tree results directory is missing")
    return (request, request_b64, request_sha256, manifest, expected,
            float(timeout), results_dir)


def _exact_roster(grid, expected, timeout):
    deadline = time.monotonic() + timeout
    while True:
        node_ids = list(grid.get_node_ids())
        if any(type(node_id) is not int for node_id in node_ids) or \
                len(set(node_ids)) != len(node_ids):
            raise RuntimeError("native-tree federation roster is invalid")
        if len(node_ids) > expected:
            raise RuntimeError("native-tree federation has an extra node")
        if len(node_ids) == expected:
            return tuple(sorted(node_ids))
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise RuntimeError("native-tree federation roster is incomplete")
        time.sleep(min(0.25, remaining))


def _request_messages(node_ids, request_b64, request_sha256):
    messages = []
    for node_id in node_ids:
        content = RecordDict({
            "config": ConfigRecord({
                "native-tree-request-b64": request_b64,
                "native-tree-request-sha256": request_sha256,
                "server-round": 1,
            }),
        })
        messages.append(Message(
            content=content, message_type="train", dst_node_id=node_id,
            group_id="dsflower-native-tree-v1"))
    return messages


def _artifact_from_reply(reply, byte_cap):
    if reply.has_error() or frozenset(reply.content.keys()) != _REPLY_FIELDS:
        raise RuntimeError("native-tree node release is unavailable")
    metrics = reply.content["metrics"]
    if not isinstance(metrics, MetricRecord) or \
            frozenset(metrics.keys()) != _METRIC_FIELDS or \
            type(metrics["available"]) is not int or \
            metrics["available"] != 1 or \
            type(metrics["num-examples"]) is not int or \
            metrics["num-examples"] != 1:
        raise RuntimeError("native-tree node release is unavailable")
    arrays = reply.content["arrays"]
    if not isinstance(arrays, ArrayRecord):
        raise RuntimeError("native-tree node release has an invalid array record")
    encoded = list(arrays.values())
    if len(encoded) != 1:
        raise RuntimeError("native-tree node release must contain one array")
    item = encoded[0]
    if item.stype != "numpy.ndarray" or item.dtype != "uint8" or \
            len(item.shape) != 1 or item.shape[0] < 1 or \
            item.shape[0] > byte_cap or \
            len(item.data) > byte_cap + _NPY_HEADER_ALLOWANCE:
        raise RuntimeError("native-tree node release exceeds its byte profile")
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise RuntimeError("native-tree node release must contain one array")
    array = np.asarray(decoded[0])
    if array.dtype != np.uint8 or array.ndim != 1 or \
            not 1 <= int(array.size) <= byte_cap:
        raise RuntimeError("native-tree node release has an invalid payload")
    return array.tobytes()


def _collect_artifacts(grid, node_ids, request_b64, request_sha256,
                       timeout, byte_cap):
    messages = _request_messages(node_ids, request_b64, request_sha256)
    replies = list(grid.send_and_receive(messages, timeout=timeout))
    try:
        sources = [reply.metadata.src_node_id for reply in replies]
    except (AttributeError, TypeError) as exc:
        raise RuntimeError("native-tree reply roster is invalid") from exc
    if any(type(source) is not int for source in sources):
        raise RuntimeError("native-tree reply roster is invalid")
    if len(replies) != len(node_ids) or len(set(sources)) != len(node_ids) or \
            set(sources) != set(node_ids):
        raise RuntimeError("native-tree reply roster is incomplete or duplicated")
    return [_artifact_from_reply(reply, byte_cap) for reply in replies]


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _prediction_profile(request, request_b64, request_sha256,
                        artifact, artifact_sha256):
    return _canonical_json({
        "artifact": {
            "format": xgboost_adapter.ENSEMBLE_FORMAT,
            "sha256": artifact_sha256,
            "size_bytes": len(artifact),
        },
        "contract": PREDICTION_PROFILE,
        "native_tree_request_b64": request_b64,
        "native_tree_request_sha256": request_sha256,
        "public_schema_sha256": request["public_schema"]["sha256"],
        "task": request["task"],
        "version": 1,
    })


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


def _save_unavailable(results_dir):
    os.makedirs(results_dir, exist_ok=True)
    _atomic_write(
        os.path.join(results_dir, HISTORY_FILE),
        _canonical_json([{"available": False, "round": 1}]),
    )


def _save_release(results_dir, artifact, profile):
    os.makedirs(results_dir, exist_ok=True)
    paths = [os.path.join(results_dir, name) for name in (
        MODEL_FILE, PROFILE_FILE, HISTORY_FILE)]
    if any(os.path.exists(path) for path in paths):
        raise RuntimeError("native-tree output already exists")
    payloads = [
        artifact,
        profile,
        _canonical_json([{"available": True, "round": 1}]),
    ]
    created = []
    try:
        for path, payload in zip(paths, payloads):
            _atomic_write(path, payload)
            created.append(path)
    except Exception:
        for path in created:
            try:
                os.unlink(path)
            except OSError:
                pass
        raise


def _run_native_tree(grid, cfg):
    (request, request_b64, request_sha256, manifest, expected, timeout,
     results_dir) = _run_contract(cfg)
    node_ids = _exact_roster(grid, expected, timeout)
    artifacts = _collect_artifacts(
        grid, node_ids, request_b64, request_sha256, timeout,
        manifest["resources"]["max_artifact_bytes"])
    ensemble, artifact_sha256 = xgboost_adapter.build_xgboost_ensemble(
        manifest, artifacts)
    if not hashlib.sha256(ensemble).hexdigest() == artifact_sha256:
        raise RuntimeError("native-tree ensemble digest is inconsistent")
    profile = _prediction_profile(
        request, request_b64, request_sha256, ensemble, artifact_sha256)
    _save_release(results_dir, ensemble, profile)


@app.main()
def main(grid: Grid, context: Context) -> None:
    results_dir = context.run_config.get("results-dir")
    try:
        _run_native_tree(grid, context.run_config)
    except Exception:
        if isinstance(results_dir, str) and results_dir:
            try:
                _save_unavailable(results_dir)
            except Exception:
                pass


__all__ = [
    "HISTORY_FILE", "MODEL_FILE", "PREDICTION_PROFILE", "PROFILE_FILE",
    "app", "main",
]
