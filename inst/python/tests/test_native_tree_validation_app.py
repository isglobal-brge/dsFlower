"""End-to-end and fail-closed tests for native XGBoost validation apps."""

import base64
import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
from flwr.common import ArrayRecord, Message, MetricRecord, RecordDict


TESTS = os.path.dirname(os.path.abspath(__file__))
FLOWER_APP = os.path.join(TESTS, "..", "..", "flower_app")
sys.path.insert(0, TESTS)
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import native_tree_engine, native_tree_request, task
from dsflower_runner import native_tree_validation_client_app as client_app
from dsflower_runner import native_tree_validation_server_app as server_app

from test_xgboost_predictor import _ensemble, _manifest, _member, _schema


def _canonical(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _request_wire(task_name="binary"):
    engine_task = ("binary_classification" if task_name == "binary"
                   else "regression")
    schema = _schema(engine_task)
    parameters = [
        {"name": "learning_rate", "type": "number", "value": 0.1},
        {"name": "max_delta_step", "type": "number", "value": 10.0},
        {"name": "max_depth", "type": "integer", "value": 1},
        {"name": "min_child_weight", "type": "number", "value": 1.0},
        {"name": "min_split_loss", "type": "number", "value": 0.0},
        {"name": "num_boost_round", "type": "integer", "value": 1},
        {"name": "reg_alpha", "type": "number", "value": 0.0},
        {"name": "reg_lambda", "type": "number", "value": 1.0},
    ]
    request = {
        "contract": "dsflower-native-tree-request-v1",
        "engine": "xgboost",
        "mode": "native-tight",
        "parameters": parameters,
        "public_schema": schema,
        "resources": {
            "max_features": 8, "max_trees": 20, "max_depth": 8,
            "max_bins": 16, "max_threads": 4, "memory_mb": 4096,
            "timeout_seconds": 60,
        },
        "task": task_name,
    }
    raw = json.dumps(
        request, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    return request, base64.b64encode(raw).decode("ascii"), \
        hashlib.sha256(raw).hexdigest()


def _artifact(task_name="binary"):
    engine_task = ("binary_classification" if task_name == "binary"
                   else "regression")
    manifest = _manifest(engine_task)
    return _ensemble(manifest, [_member(task=engine_task)])


def _profile(request, request_b64, request_sha256, artifact):
    return _canonical({
        "artifact": {
            "format": "dsflower-xgboost-ensemble-json-v1",
            "sha256": hashlib.sha256(artifact).hexdigest(),
            "size_bytes": len(artifact),
        },
        "contract": "dsflower-xgboost-prediction-profile-v1",
        "native_tree_request_b64": request_b64,
        "native_tree_request_sha256": request_sha256,
        "public_schema_sha256": request["public_schema"]["sha256"],
        "task": request["task"],
        "version": 1,
    })


def _pins(task_name, request, request_b64, request_sha256, artifact, profile):
    return {
        "validation-artifact-format": "dsflower-xgboost-ensemble-json-v1",
        "validation-artifact-sha256": hashlib.sha256(artifact).hexdigest(),
        "validation-artifact-size-bytes": len(artifact),
        "validation-bins": 8,
        "validation-contract-sha256": "c" * 64,
        "validation-model-track": "native_tree",
        "validation-native-tree-request-b64": request_b64,
        "validation-native-tree-request-sha256": request_sha256,
        "validation-profile-sha256": hashlib.sha256(profile).hexdigest(),
        "validation-profile-size-bytes": len(profile),
        "validation-public-schema-sha256": request["public_schema"]["sha256"],
        "validation-task": task_name,
    }


def _node_manifest(task_name, pins):
    request = native_tree_request.parse_request_wire(
        pins["validation-native-tree-request-b64"],
        pins["validation-native-tree-request-sha256"])
    schema = request["public_schema"]
    target = schema["target"]
    manifest = {
        "data_type": "tabular", "data_file": "train.csv",
        "data_format": "csv", "dp-track": "validation",
        "num-server-rounds": 1, "target-preencoded": True,
        "target_column": target["name"],
        "feature_columns": schema["features"],
        "feature-bounds": {"lower": schema["lower"], "upper": schema["upper"]},
        "task-type": "classification" if task_name == "binary" else "regression",
        "loss-name": "bce_logits" if task_name == "binary" else "mse",
        "num-features": len(schema["features"]), "num-classes": 2,
        "num-labels": 2, "dp-unit": "row", "patient_column": None,
        "patient-id-canonicalization": "trim-utf8-v2", "n_units": 4,
        "n_samples": 4, "privacy-adjacency": "replace_one",
        "privacy-epsilon": 1.0, "privacy-delta": 1.0e-6,
        "privacy-clipping_norm": 1.0,
        "privacy-policy-sha256": "a" * 64,
        **pins,
    }
    if task_name == "binary":
        manifest["target-levels"] = {
            "type": "numeric", "values": [0.0, 1.0]}
    else:
        manifest["target-bounds"] = {
            "lower": target["lower"], "upper": target["upper"]}
    return manifest


def _run_config(root, results_dir, task_name, pins):
    model_path = os.path.join(root, "model.json")
    profile_path = os.path.join(root, "profile.json")
    return {
        "dp-track": "validation", "num-server-rounds": 1,
        "min-train-nodes": 2, "round-timeout": 10,
        "num-features": 2, "num-classes": 2, "num-labels": 2,
        "loss-name": "bce_logits" if task_name == "binary" else "mse",
        "results-dir": results_dir,
        "validation-model-path-b64": base64.b64encode(
            model_path.encode()).decode(),
        "validation-profile-path-b64": base64.b64encode(
            profile_path.encode()).decode(),
        **pins,
        **({"validation-target-lower": 0.0, "validation-target-upper": 4.0}
           if task_name == "regression" else {}),
    }


def _write_contract(root, task_name, rows=None):
    request, request_b64, request_sha256 = _request_wire(task_name)
    artifact = _artifact(task_name)
    profile = _profile(
        request, request_b64, request_sha256, artifact)
    pins = _pins(
        task_name, request, request_b64, request_sha256, artifact, profile)
    if rows is None:
        rows = pd.DataFrame({
            "marker": [-2.0, -0.5, 0.5, 2.0],
            "age": [20.0, 30.0, 60.0, 70.0],
            "outcome": ([0, 0, 1, 1] if task_name == "binary"
                        else [0.5, 1.5, 2.5, 3.5]),
        })
    rows.to_csv(os.path.join(root, "train.csv"), index=False)
    with open(os.path.join(root, "model.json"), "wb") as handle:
        handle.write(artifact)
    with open(os.path.join(root, "profile.json"), "wb") as handle:
        handle.write(profile)
    with open(os.path.join(root, "manifest.json"), "w",
              encoding="utf-8") as handle:
        json.dump(_node_manifest(task_name, pins), handle)
    return request, artifact, profile, pins


def _secret(root):
    path = os.path.join(root, "node-secret")
    with open(path, "w", encoding="ascii") as handle:
        handle.write("1" * 64)
    os.chmod(path, 0o600)
    return path


def _vector_reply(request, vector):
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[
            np.asarray(vector, dtype=np.float64)]),
        "metrics": MetricRecord({"available": 1, "num-examples": 1}),
    }), reply_to=request)


class _Grid:
    def __init__(self, context):
        self.context = context
        self.messages = []

    @staticmethod
    def get_node_ids():
        return [11, 22]

    def send_and_receive(self, messages, timeout):
        self.messages = list(messages)
        return [client_app.train(message, self.context) for message in messages]


class NativeTreeValidationClientTests(unittest.TestCase):
    def test_isolation_guard_rejects_training_or_uploaded_code(self):
        with mock.patch.dict(sys.modules, {
                "dsflower_runner.xgboost_adapter": object()}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "not isolated"):
                client_app._assert_native_process_isolated()
        with mock.patch.dict(os.environ, {
                "DSFLOWER_PINNED_APP_DIR": "/uploaded/app"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "uploaded code"):
                client_app._assert_native_process_isolated()

    def test_public_tamper_fails_before_private_read(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            _request, artifact, _profile_bytes, pins = _write_contract(
                root, "binary")
            cfg = _run_config(root, results_dir, "binary", pins)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)
            message = server_app._request_messages((1,), cfg, artifact)[0]
            context.run_config = dict(cfg)
            context.run_config["validation-artifact-sha256"] = "0" * 64
            with mock.patch.object(
                    task, "load_native_tree_data",
                    side_effect=AssertionError("private data was read")) as load:
                reply = client_app.train(message, context)
            load.assert_not_called()
            self.assertEqual(dict(reply.content["metrics"]), {
                "available": 0, "num-examples": 1})

    def test_resanitize_finishes_before_the_single_private_read(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            _request, artifact, _profile_bytes, pins = _write_contract(
                root, "binary")
            cfg = _run_config(root, results_dir, "binary", pins)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)
            message = server_app._request_messages((1,), cfg, artifact)[0]
            events = []
            original_parse = native_tree_engine.parse_ensemble
            original_load = task.load_native_tree_data

            def parsed(*args, **kwargs):
                events.append("sanitize")
                return original_parse(*args, **kwargs)

            def loaded(*args, **kwargs):
                events.append("private")
                return original_load(*args, **kwargs)

            with (mock.patch.object(
                    client_app.native_tree_engine, "parse_ensemble",
                    side_effect=parsed),
                  mock.patch.object(
                    task, "load_native_tree_data", side_effect=loaded),
                  mock.patch.dict(os.environ, {
                    "DSFLOWER_NODE_SECRET_FILE": _secret(root),
                    "DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET": "1",
                  }, clear=False)):
                reply = client_app.train(message, context)
            self.assertEqual(events, ["sanitize", "private"])
            self.assertEqual(dict(reply.content["metrics"]), {
                "available": 1, "num-examples": 1})

    def test_contract_spelling_and_row_order_are_not_noise_reroll_axes(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            _request, artifact, _profile_bytes, pins = _write_contract(
                root, "binary")
            cfg = _run_config(root, results_dir, "binary", pins)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)

            def release():
                message = server_app._request_messages((1,), cfg, artifact)[0]
                return client_app.train(
                    message, context).content["arrays"].to_numpy_ndarrays()[0]

            with mock.patch.dict(os.environ, {
                    "DSFLOWER_NODE_SECRET_FILE": _secret(root),
                    "DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET": "1",
                  }, clear=False):
                first = release()
                frame = pd.read_csv(os.path.join(root, "train.csv"))
                frame.iloc[::-1].to_csv(
                    os.path.join(root, "train.csv"), index=False)
                manifest_path = os.path.join(root, "manifest.json")
                with open(manifest_path, encoding="utf-8") as handle:
                    manifest = json.load(handle)
                manifest["validation-contract-sha256"] = "d" * 64
                with open(manifest_path, "w", encoding="utf-8") as handle:
                    json.dump(manifest, handle)
                cfg["validation-contract-sha256"] = "d" * 64
                context.run_config = cfg
                second = release()
            np.testing.assert_array_equal(first, second)

    def test_regression_target_bounds_come_from_the_node_manifest(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            _request, artifact, _profile_bytes, pins = _write_contract(
                root, "regression")
            cfg = _run_config(root, results_dir, "regression", pins)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)

            def release():
                message = server_app._request_messages((1,), cfg, artifact)[0]
                return client_app.train(
                    message, context).content["arrays"].to_numpy_ndarrays()[0]

            with mock.patch.dict(os.environ, {
                    "DSFLOWER_NODE_SECRET_FILE": _secret(root),
                    "DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET": "1",
                  }, clear=False):
                first = release()
                cfg["validation-target-lower"] = -1000.0
                cfg["validation-target-upper"] = 1000.0
                context.run_config = cfg
                second = release()
            np.testing.assert_array_equal(first, second)


class NativeTreeValidationServerTests(unittest.TestCase):
    def test_permuted_replies_produce_identical_result_bytes(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as first_dir, \
                tempfile.TemporaryDirectory() as second_dir:
            _request, artifact, _profile_bytes, pins = _write_contract(
                root, "binary")
            values = {11: 1.0e308, 22: -1.0e308, 33: 1.0}

            class PermutedGrid:
                def __init__(self, reverse):
                    self.reverse = reverse

                @staticmethod
                def get_node_ids():
                    return [11, 22, 33]

                def send_and_receive(self, messages, timeout):
                    replies = []
                    for message in messages:
                        vector = np.zeros(16, dtype=np.float64)
                        vector[0] = values[message.metadata.dst_node_id]
                        replies.append(_vector_reply(message, vector))
                    return list(reversed(replies)) if self.reverse else replies

            outputs = []
            for reverse, results_dir in ((False, first_dir), (True, second_dir)):
                cfg = _run_config(root, results_dir, "binary", pins)
                cfg["min-train-nodes"] = 3
                server_app.main(
                    PermutedGrid(reverse), SimpleNamespace(run_config=cfg))
                with open(os.path.join(results_dir, server_app.RESULT_FILE),
                          "rb") as handle:
                    outputs.append(handle.read())
            self.assertEqual(outputs[0], outputs[1])
            self.assertTrue(json.loads(outputs[0])["available"])

    def test_binary_and_regression_are_one_atomic_pooled_release(self):
        for task_name in ("binary", "regression"):
            with self.subTest(task=task_name), \
                    tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results_dir:
                _request, _artifact_bytes, _profile_bytes, pins = \
                    _write_contract(root, task_name)
                cfg = _run_config(root, results_dir, task_name, pins)
                context = SimpleNamespace(
                    node_config={"manifest-dir": root}, run_config=cfg)
                grid = _Grid(context)
                with mock.patch.dict(os.environ, {
                        "DSFLOWER_NODE_SECRET_FILE": _secret(root),
                        "DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET": "1",
                      }, clear=False):
                    server_app.main(grid, SimpleNamespace(run_config=cfg))
                with open(os.path.join(results_dir, server_app.RESULT_FILE),
                          encoding="ascii") as handle:
                    released = json.load(handle)
                self.assertTrue(released["available"])
                self.assertTrue(released["pooled_only"])
                self.assertEqual(released["n_nodes"], 2)
                self.assertEqual(released["task"], task_name)
                self.assertIsInstance(released["metrics"], dict)
                self.assertEqual(len(grid.messages), 2)
                self.assertFalse(any(os.path.exists(os.path.join(
                    results_dir, name)) for name in (
                        "model.json", "predictions.json", "per_node.json")))

    def test_incomplete_roster_publishes_unavailable_without_metrics(self):
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            _request, _artifact_bytes, _profile_bytes, pins = _write_contract(
                root, "binary")
            cfg = _run_config(root, results_dir, "binary", pins)

            class ExtraGrid:
                @staticmethod
                def get_node_ids():
                    return [1, 2, 3]

            server_app.main(ExtraGrid(), SimpleNamespace(run_config=cfg))
            with open(os.path.join(results_dir, server_app.RESULT_FILE),
                      encoding="ascii") as handle:
                released = json.load(handle)
            self.assertFalse(released["available"])
            self.assertNotIn("metrics", released)

    def test_profile_or_artifact_tampering_yields_no_partial_metrics(self):
        for filename in ("model.json", "profile.json"):
            with self.subTest(filename=filename), \
                    tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results_dir:
                _request, _artifact_bytes, _profile_bytes, pins = _write_contract(
                    root, "binary")
                with open(os.path.join(root, filename), "ab") as handle:
                    handle.write(b"\n")
                cfg = _run_config(root, results_dir, "binary", pins)
                server_app.main(
                    _Grid(SimpleNamespace(
                        node_config={"manifest-dir": root}, run_config=cfg)),
                    SimpleNamespace(run_config=cfg))
                with open(os.path.join(results_dir, server_app.RESULT_FILE),
                          encoding="ascii") as handle:
                    released = json.load(handle)
                self.assertFalse(released["available"])
                self.assertNotIn("metrics", released)


if __name__ == "__main__":
    unittest.main()
