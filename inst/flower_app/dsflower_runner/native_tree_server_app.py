"""Dedicated coordinator ServerApp for one atomic native-tree federation."""

import hashlib
import json
import math
import os
import re
import time

import numpy as np
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)
from flwr.serverapp import Grid, ServerApp

from . import native_tree_engine, native_tree_request, resampling, validation


app = ServerApp()
HISTORY_FILE = "history.json"
_REPLY_FIELDS = frozenset(("arrays", "metrics"))
_METRIC_FIELDS = frozenset(("available", "num-examples"))
_NPY_HEADER_ALLOWANCE = 4096
_HOLDOUT_CONFIG_FIELDS = frozenset((
    "holdout-validation-bins", "resampling-assignment",
    "resampling-contract-sha256", "resampling-method",
    "resampling-privacy-unit", "resampling-test-denominator",
    "resampling-test-numerator", "resampling-unit-canonicalization",
    "resampling-version",
))
_HOLDOUT_BOUND_FIELDS = frozenset((
    "holdout-target-lower", "holdout-target-upper",
))
_CV_CONFIG_FIELDS = frozenset((
    "cv-assignment", "cv-contract-sha256", "cv-folds", "cv-job-sha256",
    "cv-method", "cv-n-nodes", "cv-privacy-unit",
    "cv-unit-canonicalization", "cv-validation-bins", "cv-version",
))
_CV_BOUND_FIELDS = frozenset(("cv-target-lower", "cv-target-upper"))
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_CV_ACK = np.asarray([0], dtype=np.uint8)


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
    native_tree_engine.canonical_profile(manifest)
    supplied_holdout = {
        key for key in cfg if str(key).lower().startswith((
            "resampling-", "resampling_", "holdout-", "holdout_"))}
    supplied_cv = {
        key for key in cfg if str(key).lower().startswith(("cv-", "cv_"))}
    if supplied_holdout and supplied_cv:
        raise ValueError("native-tree holdout and cross-validation cannot combine")
    holdout = None
    if supplied_holdout:
        expected_holdout = (_HOLDOUT_CONFIG_FIELDS if request["task"] == "binary"
                            else _HOLDOUT_CONFIG_FIELDS |
                            _HOLDOUT_BOUND_FIELDS)
        if supplied_holdout != expected_holdout:
            raise ValueError("native-tree holdout config is incomplete")
        holdout = resampling.validate_holdout_contract({
            "assignment": cfg.get("resampling-assignment"),
            "method": cfg.get("resampling-method"),
            "privacy_unit": cfg.get("resampling-privacy-unit"),
            "sha256": cfg.get("resampling-contract-sha256"),
            "test_denominator": cfg.get("resampling-test-denominator"),
            "test_numerator": cfg.get("resampling-test-numerator"),
            "unit_canonicalization": cfg.get(
                "resampling-unit-canonicalization"),
            "version": cfg.get("resampling-version"),
        })
        bins = cfg.get("holdout-validation-bins")
        if type(bins) is not int or not 4 <= bins <= 512:
            raise ValueError("native-tree holdout bins are invalid")
        if request["task"] == "regression" and (
                cfg.get("holdout-target-lower") != request[
                    "public_schema"]["target"]["lower"] or
                cfg.get("holdout-target-upper") != request[
                    "public_schema"]["target"]["upper"]):
            raise ValueError("native-tree holdout bounds differ from request")
    cross_validation = None
    if supplied_cv:
        expected_cv = (_CV_CONFIG_FIELDS if request["task"] == "binary"
                       else _CV_CONFIG_FIELDS | _CV_BOUND_FIELDS)
        if supplied_cv != expected_cv:
            raise ValueError("native-tree cross-validation config is incomplete")
        cross_validation = resampling.validate_cross_validation_contract({
            "assignment": cfg.get("cv-assignment"),
            "folds": cfg.get("cv-folds"),
            "method": cfg.get("cv-method"),
            "privacy_unit": cfg.get("cv-privacy-unit"),
            "sha256": cfg.get("cv-contract-sha256"),
            "unit_canonicalization": cfg.get("cv-unit-canonicalization"),
            "version": cfg.get("cv-version"),
        })
        bins = cfg.get("cv-validation-bins")
        job_hash = cfg.get("cv-job-sha256")
        if type(bins) is not int or not 4 <= bins <= 512:
            raise ValueError("native-tree cross-validation bins are invalid")
        cv_nodes = cfg.get("cv-n-nodes")
        if type(cv_nodes) is not int or cv_nodes < 1 or cv_nodes != expected:
            raise ValueError(
                "native-tree cross-validation roster differs from its job pin")
        if not isinstance(job_hash, str) or not _HEX_64_RE.fullmatch(job_hash):
            raise ValueError("native-tree cross-validation job pin is invalid")
        if request["task"] == "regression" and (
                cfg.get("cv-target-lower") != request[
                    "public_schema"]["target"]["lower"] or
                cfg.get("cv-target-upper") != request[
                    "public_schema"]["target"]["upper"]):
            raise ValueError(
                "native-tree cross-validation bounds differ from request")
        cross_validation = dict(
            cross_validation, bins=bins, job_sha256=job_hash)
    timeout = cfg.get(
        "round-timeout", request["resources"]["timeout_seconds"])
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or \
            not math.isfinite(float(timeout)) or not 0.0 < float(timeout) <= \
            request["resources"]["timeout_seconds"]:
        raise ValueError("native-tree round timeout is invalid")
    results_dir = cfg.get("results-dir")
    if not isinstance(results_dir, str) or not results_dir:
        raise ValueError("native-tree results directory is missing")
    return (request, request_b64, request_sha256, manifest, holdout,
            expected, float(timeout), results_dir, cross_validation)


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


def _cv_control_array(operation, fold, contract_hash, job_hash):
    payload = _canonical_json({
        "contract": contract_hash, "fold": int(fold),
        "job": job_hash, "operation": operation,
        "version": "dsflower-native-tree-cv-control-v1",
    })
    return np.frombuffer(hashlib.sha256(payload).digest(), dtype=np.uint8).copy()


def _request_messages(node_ids, request_b64, request_sha256, holdout=None,
                      cross_validation=None, fold=None):
    if holdout is not None and cross_validation is not None:
        raise ValueError("native-tree resampling phases cannot combine")
    messages = []
    for node_id in node_ids:
        if cross_validation is None:
            operation = "train"
            config = {
                "dsflower-operation": operation,
                "native-tree-request-b64": request_b64,
                "native-tree-request-sha256": request_sha256,
                "server-round": 1,
                **({"resampling-contract-sha256": holdout["sha256"]}
                   if holdout is not None else {}),
            }
            content = RecordDict({"config": ConfigRecord(config)})
            group = "dsflower-native-tree-v1"
        else:
            if type(fold) is not int or not 1 <= fold <= cross_validation["folds"]:
                raise ValueError("native-tree CV training fold is invalid")
            operation = "cv-train"
            config = {
                "cv-contract-sha256": cross_validation["sha256"],
                "cv-job-sha256": cross_validation["job_sha256"],
                "dsflower-fold": fold,
                "dsflower-operation": operation,
                "native-tree-request-b64": request_b64,
                "native-tree-request-sha256": request_sha256,
                "server-round": 1,
            }
            content = RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[_cv_control_array(
                    operation, fold, cross_validation["sha256"],
                    cross_validation["job_sha256"])]),
                "config": ConfigRecord(config),
            })
            group = "dsflower-native-tree-cv-train-f%d-v1" % fold
        messages.append(Message(
            content=content, message_type="train", dst_node_id=node_id,
            group_id=group))
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
                       timeout, byte_cap, holdout=None,
                       cross_validation=None, fold=None):
    messages = _request_messages(
        node_ids, request_b64, request_sha256, holdout=holdout,
        cross_validation=cross_validation, fold=fold)
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
    pairs = [(source, _artifact_from_reply(reply, byte_cap))
             for source, reply in zip(sources, replies)]
    return [artifact for _source, artifact in sorted(pairs)]


def _holdout_layout(request, bins):
    if request["task"] == "binary":
        return validation.validation_layout(
            "classification", n_classes=2, bins=bins)
    return validation.validation_layout("regression", bins=bins)


def _evaluation_messages(node_ids, request, request_b64, request_sha256,
                         holdout, ensemble, artifact_sha256):
    spec = native_tree_engine.release_spec(request["engine"])
    messages = []
    for node_id in node_ids:
        content = RecordDict({
            "arrays": ArrayRecord(numpy_ndarrays=[
                np.frombuffer(ensemble, dtype=np.uint8).copy()]),
            "config": ConfigRecord({
                "dsflower-operation": "holdout-evaluate",
                "holdout-validation-bins": holdout["bins"],
                "native-tree-artifact-format": spec["ensemble_format"],
                "native-tree-artifact-sha256": artifact_sha256,
                "native-tree-artifact-size-bytes": len(ensemble),
                "native-tree-n-nodes": len(node_ids),
                "native-tree-public-schema-sha256": request[
                    "public_schema"]["sha256"],
                "native-tree-request-b64": request_b64,
                "native-tree-request-sha256": request_sha256,
                "resampling-contract-sha256": holdout["sha256"],
                "server-round": 1,
            }),
        })
        messages.append(Message(
            content=content, message_type="train", dst_node_id=node_id,
            group_id="dsflower-native-tree-holdout-v1"))
    return messages


def _cv_messages(node_ids, request, request_b64, request_sha256,
                 cross_validation, operation, fold, artifact=None,
                 artifact_sha256=None):
    if operation not in ("cv-accumulate", "cv-release", "cv-abort"):
        raise ValueError("native-tree CV operation is invalid")
    final = int(cross_validation["folds"]) + 1
    if (operation == "cv-accumulate" and not 1 <= fold < final) or \
            (operation != "cv-accumulate" and fold != final):
        raise ValueError("native-tree CV operation fold is invalid")
    if operation == "cv-accumulate":
        if not isinstance(artifact, (bytes, bytearray, memoryview)) or \
                not isinstance(artifact_sha256, str):
            raise ValueError("native-tree CV accumulation artifact is invalid")
        raw_artifact = bytes(artifact)
        array_payload = np.frombuffer(raw_artifact, dtype=np.uint8).copy()
    else:
        raw_artifact = None
        array_payload = _cv_control_array(
            operation, fold, cross_validation["sha256"],
            cross_validation["job_sha256"])
    spec = native_tree_engine.release_spec(request["engine"])
    messages = []
    for node_id in node_ids:
        config = {
            "cv-contract-sha256": cross_validation["sha256"],
            "cv-job-sha256": cross_validation["job_sha256"],
            "cv-validation-bins": cross_validation["bins"],
            "dsflower-fold": int(fold),
            "dsflower-operation": operation,
            "native-tree-n-nodes": len(node_ids),
            "native-tree-public-schema-sha256": request[
                "public_schema"]["sha256"],
            "native-tree-request-b64": request_b64,
            "native-tree-request-sha256": request_sha256,
            "server-round": 1,
        }
        if operation == "cv-accumulate":
            config.update({
                "native-tree-artifact-format": spec["ensemble_format"],
                "native-tree-artifact-sha256": artifact_sha256,
                "native-tree-artifact-size-bytes": len(raw_artifact),
            })
        messages.append(Message(
            content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[array_payload.copy()]),
                "config": ConfigRecord(config),
            }),
            message_type="train", dst_node_id=node_id,
            group_id="dsflower-native-tree-%s-f%d-v1" % (operation, fold)))
    return messages


def _reply_pairs(grid, messages, node_ids, timeout, where):
    replies = list(grid.send_and_receive(messages, timeout=timeout))
    try:
        sources = [reply.metadata.src_node_id for reply in replies]
    except (AttributeError, TypeError) as exc:
        raise RuntimeError("%s reply roster is invalid" % where) from exc
    if any(type(source) is not int for source in sources) or \
            len(replies) != len(node_ids) or \
            len(set(sources)) != len(node_ids) or set(sources) != set(node_ids):
        raise RuntimeError("%s reply roster is incomplete or duplicated" % where)
    return sorted(zip(sources, replies), key=lambda pair: pair[0])


def _ack_from_reply(reply):
    if reply.has_error() or frozenset(reply.content.keys()) != _REPLY_FIELDS:
        raise RuntimeError("native-tree CV acknowledgement is unavailable")
    metrics = reply.content["metrics"]
    arrays = reply.content["arrays"]
    if not isinstance(metrics, MetricRecord) or \
            frozenset(metrics.keys()) != _METRIC_FIELDS or \
            dict(metrics) != {"available": 1, "num-examples": 1} or \
            not isinstance(arrays, ArrayRecord):
        raise RuntimeError("native-tree CV acknowledgement is unavailable")
    values = arrays.to_numpy_ndarrays()
    if len(values) != 1 or values[0].dtype != np.uint8 or \
            values[0].shape != (1,) or values[0].tobytes() != _CV_ACK.tobytes():
        raise RuntimeError("native-tree CV acknowledgement is invalid")


def _require_exact_roster(grid, roster):
    current = list(grid.get_node_ids())
    if any(type(node_id) is not int for node_id in current) or \
            len(set(current)) != len(current) or \
            tuple(sorted(current)) != tuple(roster):
        raise RuntimeError("native-tree cross-validation roster changed")


def _cv_accumulate(grid, roster, request, request_b64, request_sha256,
                   cross_validation, fold, ensemble, artifact_sha256,
                   timeout):
    _require_exact_roster(grid, roster)
    messages = _cv_messages(
        roster, request, request_b64, request_sha256, cross_validation,
        "cv-accumulate", fold, ensemble, artifact_sha256)
    for _source, reply in _reply_pairs(
            grid, messages, roster, timeout, "native-tree CV accumulation"):
        _ack_from_reply(reply)


def _cv_release(grid, roster, request, request_b64, request_sha256,
                cross_validation, timeout, layout):
    _require_exact_roster(grid, roster)
    fold = int(cross_validation["folds"]) + 1
    messages = _cv_messages(
        roster, request, request_b64, request_sha256, cross_validation,
        "cv-release", fold)
    return [_vector_from_reply(reply, layout) for _source, reply in _reply_pairs(
        grid, messages, roster, timeout, "native-tree CV release")]


def _cv_abort(grid, roster, request, request_b64, request_sha256,
              cross_validation, timeout):
    try:
        messages = _cv_messages(
            roster, request, request_b64, request_sha256, cross_validation,
            "cv-abort", int(cross_validation["folds"]) + 1)
        for _source, reply in _reply_pairs(
                grid, messages, roster, timeout, "native-tree CV abort"):
            _ack_from_reply(reply)
    except Exception:
        pass


def _vector_from_reply(reply, layout):
    if reply.has_error() or frozenset(reply.content.keys()) != _REPLY_FIELDS:
        raise RuntimeError("native-tree holdout node release is unavailable")
    metrics = reply.content["metrics"]
    if not isinstance(metrics, MetricRecord) or \
            frozenset(metrics.keys()) != _METRIC_FIELDS or \
            type(metrics["available"]) is not int or \
            metrics["available"] != 1 or \
            type(metrics["num-examples"]) is not int or \
            metrics["num-examples"] != 1:
        raise RuntimeError("native-tree holdout node release is unavailable")
    arrays = reply.content["arrays"]
    if not isinstance(arrays, ArrayRecord):
        raise RuntimeError("native-tree holdout release has invalid geometry")
    encoded = list(arrays.values())
    size = int(layout["size"])
    if len(encoded) != 1 or encoded[0].stype != "numpy.ndarray" or \
            encoded[0].dtype != "float64" or \
            tuple(encoded[0].shape) != (size,) or \
            len(encoded[0].data) > size * 8 + _NPY_HEADER_ALLOWANCE:
        raise RuntimeError("native-tree holdout release has invalid geometry")
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise RuntimeError("native-tree holdout release has invalid geometry")
    vector = np.asarray(decoded[0])
    if vector.dtype != np.float64 or vector.shape != (size,) or \
            not bool(np.all(np.isfinite(vector))):
        raise RuntimeError("native-tree holdout release has invalid geometry")
    return vector


def _collect_holdout_vectors(grid, node_ids, request, request_b64,
                             request_sha256, holdout, ensemble,
                             artifact_sha256, layout, timeout):
    messages = _evaluation_messages(
        node_ids, request, request_b64, request_sha256, holdout, ensemble,
        artifact_sha256)
    replies = list(grid.send_and_receive(messages, timeout=timeout))
    try:
        sources = [reply.metadata.src_node_id for reply in replies]
    except (AttributeError, TypeError) as exc:
        raise RuntimeError("native-tree holdout reply roster is invalid") from exc
    if any(type(source) is not int for source in sources) or \
            len(replies) != len(node_ids) or \
            len(set(sources)) != len(node_ids) or set(sources) != set(node_ids):
        raise RuntimeError(
            "native-tree holdout reply roster is incomplete or duplicated")
    pairs = [(source, _vector_from_reply(reply, layout))
             for source, reply in zip(sources, replies)]
    return [value for _source, value in sorted(pairs)]


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
        raise RuntimeError("native-tree holdout pooled vector overflowed")
    return pooled


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _prediction_profile(request, request_b64, request_sha256,
                        artifact, artifact_sha256):
    return native_tree_engine.build_prediction_profile(
        request, request_b64, request_sha256, artifact, artifact_sha256)


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


def _save_release(results_dir, artifact, profile, *, model_file,
                  profile_file, holdout=None):
    os.makedirs(results_dir, exist_ok=True)
    names = [model_file, profile_file]
    payloads = [artifact, profile]
    if holdout is not None:
        names.append("holdout.json")
        payloads.append(validation.private_metric_result_wire(holdout))
    names.append(HISTORY_FILE)
    payloads.append(_canonical_json([{"available": True, "round": 1}]))
    paths = [os.path.join(results_dir, name) for name in names]
    if any(os.path.exists(path) for path in paths):
        raise RuntimeError("native-tree output already exists")
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


def _contains_forbidden_cv_key(value):
    if isinstance(value, dict):
        if any(str(key) in {
                "per_node", "per_site", "predictions", "oof_predictions",
                "fold", "per_fold", "fold_metrics", "models"
        } for key in value):
            return True
        return any(_contains_forbidden_cv_key(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_forbidden_cv_key(item) for item in value)
    return False


def _save_cross_validation(results_dir, request, cross_validation,
                           n_nodes, metrics):
    task_name = "binary" if request["task"] == "binary" else "regression"
    required = "accuracy" if task_name == "binary" else "mae"
    primary = metrics.get(required) if isinstance(metrics, dict) else None
    if not isinstance(metrics, dict) or required not in metrics or \
            isinstance(primary, (bool, np.bool_)) or \
            not isinstance(primary, (int, float, np.number)) or \
            not math.isfinite(float(primary)) or float(primary) < 0.0 or \
            (task_name == "binary" and float(primary) > 1.0) or \
            _contains_forbidden_cv_key(metrics):
        raise RuntimeError(
            "native-tree cross-validation metrics violate pooled-only output")
    payload = {
        "cv_contract_sha256": cross_validation["sha256"],
        "cv_job_sha256": cross_validation["job_sha256"],
        "folds": int(cross_validation["folds"]),
        "method": "cross_validation",
        "metrics": metrics,
        "n_nodes": int(n_nodes),
        "pooled_only": True,
        "privacy": "node-dp-pooled-postprocessing",
        "task": task_name,
    }
    os.makedirs(results_dir, exist_ok=True)
    if os.listdir(results_dir):
        raise RuntimeError(
            "native-tree cross-validation results directory is not empty")
    _atomic_write(
        os.path.join(results_dir, "cv.json"),
        validation.private_metric_result_wire(payload))


def _run_cross_validation(grid, request, request_b64, request_sha256,
                          manifest, cross_validation, expected, timeout,
                          results_dir):
    roster = _exact_roster(grid, expected, timeout)
    layout = _holdout_layout(request, cross_validation["bins"])
    completed = False
    try:
        for fold in range(1, int(cross_validation["folds"]) + 1):
            _require_exact_roster(grid, roster)
            artifacts = _collect_artifacts(
                grid, roster, request_b64, request_sha256, timeout,
                manifest["resources"]["max_artifact_bytes"],
                cross_validation=cross_validation, fold=fold)
            ensemble, artifact_sha256 = native_tree_engine.build_ensemble(
                manifest, artifacts)
            if hashlib.sha256(ensemble).hexdigest() != artifact_sha256:
                raise RuntimeError(
                    "native-tree CV ensemble digest is inconsistent")
            _cv_accumulate(
                grid, roster, request, request_b64, request_sha256,
                cross_validation, fold, ensemble, artifact_sha256, timeout)
        vectors = _cv_release(
            grid, roster, request, request_b64, request_sha256,
            cross_validation, timeout, layout)
        pooled = _stable_pool(vectors)
        bounds = (None if request["task"] == "binary" else {
            "lower": request["public_schema"]["target"]["lower"],
            "upper": request["public_schema"]["target"]["upper"],
        })
        metrics = validation.validation_metrics(
            pooled, layout, target_bounds=bounds)
        _save_cross_validation(
            results_dir, request, cross_validation, len(roster), metrics)
        completed = True
    finally:
        if not completed:
            _cv_abort(
                grid, roster, request, request_b64, request_sha256,
                cross_validation, timeout)


def _run_native_tree(grid, cfg):
    (request, request_b64, request_sha256, manifest, holdout, expected,
     timeout, results_dir, cross_validation) = _run_contract(cfg)
    if cross_validation is not None:
        _run_cross_validation(
            grid, request, request_b64, request_sha256, manifest,
            cross_validation, expected, timeout, results_dir)
        return
    if holdout is not None:
        holdout = dict(holdout, bins=cfg["holdout-validation-bins"])
    node_ids = _exact_roster(grid, expected, timeout)
    artifacts = _collect_artifacts(
        grid, node_ids, request_b64, request_sha256, timeout,
        manifest["resources"]["max_artifact_bytes"], holdout=holdout)
    ensemble, artifact_sha256 = native_tree_engine.build_ensemble(
        manifest, artifacts)
    if not hashlib.sha256(ensemble).hexdigest() == artifact_sha256:
        raise RuntimeError("native-tree ensemble digest is inconsistent")
    profile = _prediction_profile(
        request, request_b64, request_sha256, ensemble, artifact_sha256)
    spec = native_tree_engine.release_spec(request["engine"])
    holdout_payload = None
    if holdout is not None:
        layout = _holdout_layout(request, holdout["bins"])
        vectors = _collect_holdout_vectors(
            grid, node_ids, request, request_b64, request_sha256, holdout,
            ensemble, artifact_sha256, layout, timeout)
        pooled = _stable_pool(vectors)
        bounds = (None if request["task"] == "binary" else {
            "lower": request["public_schema"]["target"]["lower"],
            "upper": request["public_schema"]["target"]["upper"],
        })
        metrics = validation.validation_metrics(
            pooled, layout, target_bounds=bounds)
        holdout_payload = {
            "method": "holdout",
            "metrics": metrics,
            "n_nodes": len(node_ids),
            "pooled_only": True,
            "privacy": "node-dp-pooled-postprocessing",
            "provenance": {
                "artifact_sha256": artifact_sha256,
                "native_tree_request_sha256": request_sha256,
                "public_schema_sha256": request["public_schema"]["sha256"],
                "resampling_contract_sha256": holdout["sha256"],
            },
            "task": "binary" if request["task"] == "binary" else "regression",
        }
    _save_release(
        results_dir, ensemble, profile, model_file=spec["model_file"],
        profile_file=spec["profile_file"], holdout=holdout_payload)


@app.main()
def main(grid: Grid, context: Context) -> None:
    results_dir = context.run_config.get("results-dir")
    cross_validation = context.run_config.get("cv-contract-sha256") is not None
    try:
        _run_native_tree(grid, context.run_config)
    except Exception:
        if not cross_validation and isinstance(results_dir, str) and results_dir:
            try:
                _save_unavailable(results_dir)
            except Exception:
                pass


__all__ = ["HISTORY_FILE", "app", "main"]
