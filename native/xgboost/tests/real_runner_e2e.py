#!/usr/bin/env python3
"""Exercise the packaged native bundle through the real Flower runner path."""

import base64
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace


def _canonical(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _request_wire(task):
    target = ({
        "name": "outcome", "kind": "binary",
        "levels": [
            {"type": "string", "value": "control"},
            {"type": "string", "value": "case"},
        ],
        "lower": 0.0, "upper": 1.0,
    } if task == "binary" else {
        "name": "outcome", "kind": "continuous", "levels": None,
        "lower": -10.0, "upper": 10.0,
    })
    core = {
        "version": 1,
        "features": ["age", "marker"],
        "lower": [0.0, -5.0],
        "upper": [100.0, 5.0],
        "cuts": [[18.0, 40.0, 65.0], [-1.0, 0.0, 1.0]],
        "target": target,
    }
    schema = dict(
        core, sha256=hashlib.sha256(_canonical(core)).hexdigest())
    request = {
        "contract": "dsflower-native-tree-request-v1",
        "engine": "xgboost", "mode": "native-tight", "task": task,
        "public_schema": schema,
        "parameters": [
            {"name": "learning_rate", "type": "number", "value": 0.25},
            {"name": "max_delta_step", "type": "number", "value": 1.0},
            {"name": "max_depth", "type": "integer", "value": 1},
            {"name": "min_child_weight", "type": "number", "value": 1.0},
            {"name": "min_split_loss", "type": "number", "value": 0.0},
            {"name": "num_boost_round", "type": "integer", "value": 1},
            {"name": "reg_alpha", "type": "number", "value": 0.0},
            {"name": "reg_lambda", "type": "number", "value": 1.0},
        ],
        "resources": {
            "max_features": 2, "max_trees": 1, "max_depth": 1,
            "max_bins": 4, "max_threads": 4, "memory_mb": 4096,
            "timeout_seconds": 900,
        },
    }
    raw = _canonical(request)
    return (request, base64.b64encode(raw).decode("ascii"),
            hashlib.sha256(raw).hexdigest())


def _node_manifest(task, request_b64, request_sha256, rows):
    result = {
        "data_type": "tabular", "data_file": "train.csv",
        "data_format": "csv", "dp-track": "native_tree",
        "num-server-rounds": 1, "target-preencoded": True,
        "target_column": "outcome",
        "feature_columns": ["age", "marker"],
        "feature-bounds": {
            "lower": [0.0, -5.0], "upper": [100.0, 5.0]},
        "task-type": ("classification" if task == "binary"
                      else "regression"),
        "num-classes": 2, "dp-unit": "row", "patient_column": None,
        "patient-id-canonicalization": "trim-utf8-v2",
        "n_units": len(rows), "privacy-adjacency": "replace_one",
        "privacy-epsilon": 1.0, "privacy-delta": 1.0e-6,
        "privacy-clipping_norm": 1.0,
        "privacy-policy-sha256": "a" * 64,
        "native-tree-request-b64": request_b64,
        "native-tree-request-sha256": request_sha256,
    }
    if task == "binary":
        result["target-levels"] = {
            "type": "character", "values": ["control", "case"]}
    else:
        result["target-bounds"] = {"lower": -10.0, "upper": 10.0}
    return result


def _write_node(root, task, request_b64, request_sha256, rows):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "train.csv").open(
            "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("age", "marker", "outcome"))
        writer.writerows(rows)
    with (root / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(
            _node_manifest(task, request_b64, request_sha256, rows),
            handle, allow_nan=False, separators=(",", ":"))


def _run_config(request_b64, request_sha256, *, results_dir=None, nodes=1):
    result = {
        "dp-track": "native_tree", "num-server-rounds": 1,
        "min-train-nodes": nodes,
        "native-tree-request-b64": request_b64,
        "native-tree-request-sha256": request_sha256,
    }
    if results_dir is not None:
        result["results-dir"] = str(results_dir)
    return result


def _context(root, request_b64, request_sha256):
    return SimpleNamespace(
        node_config={"manifest-dir": str(root)},
        run_config=_run_config(request_b64, request_sha256))


def _artifact(reply):
    metrics = dict(reply.content["metrics"])
    if metrics != {"available": 1, "num-examples": 1}:
        raise AssertionError("native runner returned an unavailable release")
    arrays = reply.content["arrays"].to_numpy_ndarrays()
    if len(arrays) != 1 or str(arrays[0].dtype) != "uint8":
        raise AssertionError("native runner returned a malformed release")
    return arrays[0].tobytes()


def _message(server_app, node_id, request_b64, request_sha256):
    return server_app._request_messages(
        (node_id,), request_b64, request_sha256)[0]


def _single_release(client_app, server_app, context, request_b64,
                    request_sha256, node_id=11):
    return _artifact(client_app.train(
        _message(server_app, node_id, request_b64, request_sha256),
        context))


class _Grid:
    def __init__(self, contexts, client_app):
        self.contexts = contexts
        self.client_app = client_app

    def get_node_ids(self):
        return list(self.contexts)

    def send_and_receive(self, messages, timeout):
        del timeout
        return [self.client_app.train(
            message, self.contexts[message.metadata.dst_node_id])
                for message in messages]


def _exercise_task(work, task, client_app, server_app, native_tree_engine,
                   native_tree_request, xgboost_predictor):
    request, request_b64, request_sha256 = _request_wire(task)
    if task == "binary":
        first_rows = [
            (20.0, -0.5, 0.0), (60.0, 0.5, 1.0),
            ("NaN", 1.5, 1.0), (1000.0, "-Inf", 0.0),
        ]
        equivalent_rows = [
            ("NaN", 1.25, 1.0), (64.0, 0.75, 1.0),
            (25.0, -0.25, 0.0), (100.0, 0.0, 0.0),
        ]
    else:
        first_rows = [
            (20.0, -0.5, -4.0), (60.0, 0.5, 3.0),
            ("NaN", 1.5, "NaN"), (1000.0, "Inf", 20.0),
        ]
        equivalent_rows = None

    node_one = work / (task + "-node-one")
    node_two = work / (task + "-node-two")
    _write_node(node_one, task, request_b64, request_sha256, first_rows)
    _write_node(node_two, task, request_b64, request_sha256,
                list(reversed(first_rows)))
    context_one = _context(node_one, request_b64, request_sha256)
    context_two = _context(node_two, request_b64, request_sha256)

    first = _single_release(
        client_app, server_app, context_one, request_b64, request_sha256)
    replay = _single_release(
        client_app, server_app, context_one, request_b64, request_sha256)
    if first != replay:
        raise AssertionError("native semantic replay was not byte-identical")

    if equivalent_rows is not None:
        _write_node(
            node_one, task, request_b64, request_sha256, equivalent_rows)
        equivalent = _single_release(
            client_app, server_app, context_one,
            request_b64, request_sha256)
        if first != equivalent:
            raise AssertionError(
                "permutation and same-bin values changed native output")
        _write_node(node_one, task, request_b64, request_sha256, first_rows)

    results = work / (task + "-results")
    config = _run_config(
        request_b64, request_sha256, results_dir=results, nodes=2)
    grid = _Grid({11: context_one, 22: context_two}, client_app)
    server_app.main(grid, SimpleNamespace(run_config=config))
    spec = native_tree_engine.release_spec(request["engine"])
    model_path = results / spec["model_file"]
    profile_path = results / spec["profile_file"]
    history_path = results / server_app.HISTORY_FILE
    artifact = model_path.read_bytes()
    ensemble = json.loads(artifact.decode("ascii"))
    profile = json.loads(profile_path.read_text(encoding="ascii"))
    history = json.loads(history_path.read_text(encoding="utf-8"))
    if ensemble.get("aggregation") != "mean_prediction" or \
            len(ensemble.get("models", ())) != 2 or \
            history != [{"available": True, "round": 1}] or \
            profile["artifact"]["sha256"] != hashlib.sha256(
                artifact).hexdigest() or \
            profile["artifact"]["size_bytes"] != len(artifact):
        raise AssertionError("native coordinator output is not atomically bound")
    manifest = native_tree_request.public_backend_manifest(request)
    predictor = xgboost_predictor.parse_xgboost_ensemble(artifact, manifest)
    predictions = predictor.predict([
        [20.0, -0.5], [60.0, 0.5], [math.nan, 1.5]])
    if len(predictions) != 3 or not all(math.isfinite(x) for x in predictions):
        raise AssertionError("native ensemble prediction is invalid")
    if task == "binary" and not all(0.0 <= x <= 1.0 for x in predictions):
        raise AssertionError("binary native predictions are not probabilities")

    bad = _message(server_app, 11, request_b64, "0" * 64)
    unavailable = client_app.train(bad, context_one)
    if dict(unavailable.content["metrics"]) != {
            "available": 0, "num-examples": 1}:
        raise AssertionError("invalid request did not fail with constant release")


def _child(bundle_root, secret_file, work):
    for name in tuple(os.environ):
        if name.upper().startswith(("LD_", "DYLD_")):
            os.environ.pop(name, None)
    os.environ["DSFLOWER_XGBOOST_BUNDLE_ROOT"] = str(bundle_root)
    os.environ["DSFLOWER_NODE_SECRET_FILE"] = str(secret_file)
    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root / "inst" / "flower_app"))

    from dsflower_runner import native_tree_client_app as client_app
    from dsflower_runner import native_tree_engine
    from dsflower_runner import native_tree_request
    from dsflower_runner import native_tree_server_app as server_app
    from dsflower_runner import xgboost_bundle, xgboost_predictor

    if not xgboost_bundle.is_verified_bundle(client_app._NATIVE_BUNDLE):
        probe = xgboost_bundle.probe_xgboost_bundle(bundle_root)
        raise AssertionError(
            "real curated bundle was not loaded and verified: %s" %
            probe.error_code)
    _exercise_task(
        work, "binary", client_app, server_app,
        native_tree_engine, native_tree_request, xgboost_predictor)
    _exercise_task(
        work, "regression", client_app, server_app,
        native_tree_engine, native_tree_request, xgboost_predictor)


def _harden_windows(path):
    identity = subprocess.run(
        ["whoami"], check=True, capture_output=True,
        text=True).stdout.strip()
    if not identity:
        raise RuntimeError("Windows test identity is unavailable")
    targets = [path]
    for current, directories, files in os.walk(path):
        root = Path(current)
        targets.extend(root / name for name in directories)
        targets.extend(root / name for name in files)
    for target in targets:
        grant = "%s:%s" % (
            identity, "(OI)(CI)F" if target.is_dir() else "F")
        subprocess.run([
            "icacls", str(target), "/inheritance:r", "/grant:r", grant,
        ], check=True, capture_output=True, text=True)


def _parent(bundle_source):
    root = Path(tempfile.mkdtemp(
        prefix="dsflower-real-xgb-e2e-", dir=Path.home())).resolve()
    try:
        if os.name != "nt":
            root.chmod(0o700)
        bundle = root / "bundle"
        shutil.copytree(bundle_source, bundle)
        secret = root / "noise_root"
        secret.write_text("42" * 32, encoding="ascii")
        if os.name != "nt":
            secret.chmod(0o600)
        work = root / "work"
        work.mkdir()
        if os.name == "nt":
            # Model a provisioned custodial root exactly.  Copying prebuilt
            # DLLs may retain protected source ACLs, so secure every existing
            # entry instead of assuming inheritance repaired the full tree.
            _harden_windows(root)
        environment = {
            name: value for name, value in os.environ.items()
            if not name.upper().startswith(("LD_", "DYLD_"))
        }
        result = subprocess.run([
            sys.executable, "-I", str(Path(__file__).resolve()),
            "--child", str(bundle), str(secret), str(work),
        ], env=environment, check=False)
        if result.returncode != 0:
            raise RuntimeError("real native runner subprocess failed")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main():
    if len(sys.argv) == 5 and sys.argv[1] == "--child":
        _child(
            Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve(),
            Path(sys.argv[4]).resolve())
    elif len(sys.argv) == 2:
        _parent(Path(sys.argv[1]).resolve())
        print("real native runner e2e: ok")
    else:
        raise SystemExit("usage: real_runner_e2e.py BUNDLE")


if __name__ == "__main__":
    main()
