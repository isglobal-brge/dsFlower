"""Dedicated coordinator ServerApp for one pooled private association."""

import json
import math
import os
import re
import time

import numpy as np
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)
from flwr.serverapp import Grid, ServerApp

from . import epi_association


app = ServerApp()
HISTORY_FILE = "history.json"
RESULT_FILE = "association.json"
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_PIN_FIELDS = (
    "association-contract", "association-contract-sha256",
    "association-job-sha256", "association-n-nodes",
    "association-privacy-unit", "association-unit-semantics",
)
_REPLY_FIELDS = frozenset(("arrays", "metrics"))
_METRIC_FIELDS = frozenset(("available", "noise-sd", "num-examples"))
_MAX_ARRAY_BYTES = 8192


def _positive_integer(value, where, upper):
    if type(value) is not int or not 1 <= value <= upper:
        raise ValueError("%s is outside its public bound" % where)
    return value


def _run_contract(cfg):
    if cfg.get("dp-track") != "association" or \
            type(cfg.get("num-server-rounds")) is not int or \
            cfg["num-server-rounds"] != 1:
        raise ValueError("association ServerApp requires one exact release")
    supplied = {
        key for key in cfg
        if str(key).lower().startswith(("association-", "association_"))}
    if supplied != set(_PIN_FIELDS):
        raise ValueError("association config has an unexpected public field")
    expected = _positive_integer(
        cfg.get("association-n-nodes"), "association node count", 65_536)
    if _positive_integer(
            cfg.get("min-train-nodes"), "association roster", 65_536) != expected:
        raise ValueError("association roster differs from its job pin")
    if cfg.get("association-contract") != epi_association.ASSOCIATION_CONTRACT:
        raise ValueError("association contract literal is invalid")
    for key in ("association-contract-sha256", "association-job-sha256"):
        if not isinstance(cfg.get(key), str) or not _HEX_64_RE.fullmatch(cfg[key]):
            raise ValueError("association digest pin is invalid")
    unit = cfg.get("association-privacy-unit")
    layout = epi_association.association_layout(unit)
    if cfg.get("association-unit-semantics") != layout["unit_semantics"]:
        raise ValueError("association unit semantics differ from its privacy unit")
    timeout = cfg.get("round-timeout", 300)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or \
            not math.isfinite(float(timeout)) or \
            not 0.0 < float(timeout) <= 3_600.0:
        raise ValueError("association timeout is invalid")
    results_dir = cfg.get("results-dir")
    if not isinstance(results_dir, str) or not results_dir or \
            len(results_dir) > 16_384 or "\x00" in results_dir or \
            not os.path.isabs(results_dir):
        raise ValueError("association results directory is missing")
    return {
        "association_contract": cfg["association-contract"],
        "association_contract_sha256": cfg["association-contract-sha256"],
        "association_job_sha256": cfg["association-job-sha256"],
        "expected": expected,
        "privacy_unit": unit,
        "results_dir": results_dir,
        "timeout": float(timeout),
    }


def _exact_roster(grid, expected, timeout):
    deadline = time.monotonic() + timeout
    while True:
        node_ids = list(grid.get_node_ids())
        if any(type(node_id) is not int for node_id in node_ids) or \
                len(set(node_ids)) != len(node_ids):
            raise RuntimeError("association roster is invalid")
        if len(node_ids) > expected:
            raise RuntimeError("association roster has an extra node")
        if len(node_ids) == expected:
            return tuple(sorted(node_ids))
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise RuntimeError("association roster is incomplete")
        time.sleep(min(0.25, remaining))


def _request_messages(node_ids, cfg):
    messages = []
    for node_id in node_ids:
        config = {key: cfg[key] for key in _PIN_FIELDS}
        config["server-round"] = 1
        messages.append(Message(
            content=RecordDict({"config": ConfigRecord(config)}),
            message_type="train", dst_node_id=node_id,
            group_id="dsflower-association-v1"))
    return messages


def _release_from_reply(reply):
    if reply.has_error() or frozenset(reply.content.keys()) != _REPLY_FIELDS:
        raise RuntimeError("association node release is unavailable")
    metrics = reply.content["metrics"]
    if not isinstance(metrics, MetricRecord) or \
            frozenset(metrics.keys()) != _METRIC_FIELDS or \
            type(metrics["available"]) is not int or metrics["available"] != 1 or \
            type(metrics["num-examples"]) is not int or \
            metrics["num-examples"] != 1 or \
            type(metrics["noise-sd"]) is not float or \
            not math.isfinite(metrics["noise-sd"]) or metrics["noise-sd"] <= 0.0:
        raise RuntimeError("association node release is unavailable")
    arrays = reply.content["arrays"]
    if not isinstance(arrays, ArrayRecord):
        raise RuntimeError("association release has invalid geometry")
    encoded = list(arrays.values())
    if len(encoded) != 1:
        raise RuntimeError("association release has invalid geometry")
    item = encoded[0]
    if item.stype != "numpy.ndarray" or item.dtype != "float64" or \
            tuple(item.shape) != (9,) or len(item.data) > _MAX_ARRAY_BYTES:
        raise RuntimeError("association release has invalid geometry")
    decoded = arrays.to_numpy_ndarrays()
    if len(decoded) != 1:
        raise RuntimeError("association release has invalid geometry")
    vector = np.asarray(decoded[0])
    if vector.dtype != np.dtype(np.float64) or vector.shape != (9,) or \
            not bool(np.all(np.isfinite(vector))):
        raise RuntimeError("association release has invalid geometry")
    return np.array(vector, dtype=np.float64, order="C", copy=True), \
        float(metrics["noise-sd"])


def _collect_releases(grid, node_ids, cfg, timeout):
    replies = list(grid.send_and_receive(
        _request_messages(node_ids, cfg), timeout=timeout))
    try:
        sources = [reply.metadata.src_node_id for reply in replies]
    except (AttributeError, TypeError) as exc:
        raise RuntimeError("association reply roster is invalid") from exc
    if any(type(source) is not int for source in sources) or \
            len(replies) != len(node_ids) or len(set(sources)) != len(node_ids) or \
            set(sources) != set(node_ids):
        raise RuntimeError("association reply roster is incomplete or duplicated")
    checked = [
        (source, *_release_from_reply(reply))
        for source, reply in zip(sources, replies)
    ]
    checked.sort(key=lambda item: item[0])
    return ([item[1] for item in checked], [item[2] for item in checked])


def _result(contract, vectors, sigmas):
    result = epi_association.build_pooled_association_result(
        vectors, sigmas, expected_nodes=contract["expected"],
        privacy_unit=contract["privacy_unit"])
    result.update({
        "association_contract": contract["association_contract"],
        "association_contract_sha256":
            contract["association_contract_sha256"],
        "association_job_sha256": contract["association_job_sha256"],
    })
    return result


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _atomic_write(path, payload):
    temporary = path + ".tmp"
    descriptor = None
    try:
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if os.path.exists(temporary):
            os.unlink(temporary)


def _save_result(results_dir, result):
    os.makedirs(results_dir, mode=0o700, exist_ok=True)
    result_path = os.path.join(results_dir, RESULT_FILE)
    history_path = os.path.join(results_dir, HISTORY_FILE)
    if os.path.exists(result_path) or os.path.exists(history_path):
        raise RuntimeError("association output already exists")
    _atomic_write(result_path, _canonical_json(result))
    try:
        _atomic_write(history_path, _canonical_json([
            {"available": bool(result["available"]), "round": 1}]))
    except Exception:
        try:
            os.unlink(result_path)
        except OSError:
            pass
        raise


def _run_association(grid, cfg, contract):
    node_ids = _exact_roster(
        grid, contract["expected"], contract["timeout"])
    vectors, sigmas = _collect_releases(
        grid, node_ids, cfg, contract["timeout"])
    result = _result(contract, vectors, sigmas)
    _save_result(contract["results_dir"], result)


@app.main()
def main(grid: Grid, context: Context) -> None:
    contract = None
    try:
        contract = _run_contract(context.run_config)
        _run_association(grid, context.run_config, contract)
    except Exception:
        if contract is not None:
            try:
                _save_result(contract["results_dir"], _result(contract, [], []))
            except Exception:
                pass


__all__ = ["HISTORY_FILE", "RESULT_FILE", "app", "main"]
