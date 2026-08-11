"""Adversarial and end-to-end tests for the dedicated native-tree apps."""

import base64
import hashlib
import json
import os
import struct
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
from flwr.common import ArrayRecord, ConfigRecord, Message, MetricRecord, RecordDict


FLOWER_APP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import native_tree_client_app as client_app  # noqa: E402
from dsflower_runner import native_tree_engine  # noqa: E402
from dsflower_runner import native_tree_request  # noqa: E402
from dsflower_runner import native_tree_server_app as server_app  # noqa: E402
from dsflower_runner import (resampling, seeding, task, validation,
                             xgboost_predictor)  # noqa: E402


def _canonical(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _request_wire():
    core = {
        "version": 1,
        "features": ["age", "marker"],
        "lower": [0.0, -5.0],
        "upper": [100.0, 5.0],
        "cuts": [[18.0, 40.0, 65.0], [-1.0, 0.0, 1.0]],
        "target": {
            "name": "outcome", "kind": "binary",
            "levels": [
                {"type": "string", "value": "control"},
                {"type": "string", "value": "case"},
            ],
            "lower": 0.0, "upper": 1.0,
        },
    }
    schema = dict(core, sha256=hashlib.sha256(_canonical(core)).hexdigest())
    parameters = [
        {"name": "learning_rate", "type": "number", "value": 0.25},
        {"name": "max_delta_step", "type": "number", "value": 1.0},
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
        "task": "binary",
        "public_schema": schema,
        "parameters": parameters,
        "resources": {
            "max_features": 2, "max_trees": 1, "max_depth": 1,
            "max_bins": 4, "max_threads": 4, "memory_mb": 4096,
            "timeout_seconds": 900,
        },
    }
    raw = _canonical(request)
    return (request, base64.b64encode(raw).decode("ascii"),
            hashlib.sha256(raw).hexdigest())


def _regression_request_wire():
    request, _request_b64, _request_sha256 = _request_wire()
    request = json.loads(json.dumps(request))
    core = {
        key: value for key, value in request["public_schema"].items()
        if key != "sha256"
    }
    core["target"] = {
        "name": "outcome", "kind": "continuous", "levels": None,
        "lower": -10.0, "upper": 10.0,
    }
    request["task"] = "regression"
    request["public_schema"] = dict(
        core, sha256=hashlib.sha256(_canonical(core)).hexdigest())
    raw = _canonical(request)
    return (request, base64.b64encode(raw).decode("ascii"),
            hashlib.sha256(raw).hexdigest())


def _next_float32(value):
    value = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    bits = bits + 1 if value >= 0.0 else bits - 1
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def _native_artifact(leaf=0.1):
    tree = {
        "base_weights": [0.0, 0.0, 0.0],
        "categories": [], "categories_nodes": [],
        "categories_segments": [], "categories_sizes": [],
        "default_left": [0, 0, 0], "id": 0,
        "left_children": [1, -1, -1],
        "loss_changes": [0.0, 0.0, 0.0],
        "parents": [2_147_483_647, 0, 0],
        "right_children": [2, -1, -1],
        "split_conditions": [
            _next_float32(18.0), -abs(float(leaf)), abs(float(leaf))],
        "split_indices": [0, 0, 0], "split_type": [0, 0, 0],
        "sum_hessian": [0.0, 0.0, 0.0],
        "tree_param": {
            "num_deleted": "0", "num_feature": "2", "num_nodes": "3",
            "size_leaf_vector": "1",
        },
    }
    model = {
        "learner": {
            "attributes": {}, "feature_names": [], "feature_types": [],
            "gradient_booster": {
                "model": {
                    "cats": {"enc": [], "feature_segments": [], "sorted_idx": []},
                    "gbtree_model_param": {
                        "num_parallel_tree": "1", "num_trees": "1",
                    },
                    "iteration_indptr": [0, 1], "tree_info": [0],
                    "trees": [tree],
                },
                "name": "gbtree",
            },
            "learner_model_param": {
                "base_score": "[5E-1]", "boost_from_average": "0",
                "num_class": "0", "num_feature": "2", "num_target": "1",
            },
            "objective": {
                "name": "binary:logistic",
                "reg_loss_param": {"scale_pos_weight": "1"},
            },
        },
        "version": [3, 4, 0],
    }
    return _canonical(model)


def _node_manifest(request_b64, request_sha256, holdout=None):
    value = {
        "data_type": "tabular", "data_file": "train.csv",
        "data_format": "csv", "dp-track": "native_tree",
        "num-server-rounds": 1, "target-preencoded": True,
        "target_column": "outcome", "feature_columns": ["age", "marker"],
        "feature-bounds": {"lower": [0.0, -5.0], "upper": [100.0, 5.0]},
        "target-levels": {"type": "character", "values": ["control", "case"]},
        "task-type": "classification", "num-classes": 2,
        "dp-unit": "row", "patient_column": None,
        "patient-id-canonicalization": "trim-utf8-v2", "n_units": 2,
        "privacy-adjacency": "replace_one", "privacy-epsilon": 1.0,
        "privacy-delta": 1.0e-6, "privacy-clipping_norm": 1.0,
        "privacy-policy-sha256": "a" * 64,
        "native-tree-request-b64": request_b64,
        "native-tree-request-sha256": request_sha256,
    }
    if holdout is not None:
        value.update(resampling.manifest_fields(holdout))
        value.update({
            "holdout-validation-bins": 4,
            "privacy-training-epsilon": 0.8,
            "privacy-training-delta": 8.0e-7,
            "privacy-holdout-epsilon": 0.2,
            "privacy-holdout-delta": 2.0e-7,
            "run_token": "run_" + "1" * 32,
        })
    return value


def _run_config(request_b64, request_sha256, results_dir=None, nodes=2,
                holdout=None):
    value = {
        "dp-track": "native_tree", "num-server-rounds": 1,
        "min-train-nodes": nodes,
        "native-tree-request-b64": request_b64,
        "native-tree-request-sha256": request_sha256,
        **({"results-dir": results_dir} if results_dir is not None else {}),
    }
    if holdout is not None:
        value.update({
            **resampling.manifest_fields(holdout),
            "holdout-validation-bins": 4,
        })
    return value


def _request_message(node_id, request_b64, request_sha256, holdout=None):
    return Message(content=RecordDict({
        "config": ConfigRecord({
            "dsflower-operation": "train",
            "native-tree-request-b64": request_b64,
            "native-tree-request-sha256": request_sha256,
            "server-round": 1,
            **({"resampling-contract-sha256": holdout["sha256"]}
               if holdout is not None else {}),
        }),
    }), message_type="train", dst_node_id=node_id)


def _release_reply(request, artifact, available=1):
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[
            np.frombuffer(artifact, dtype=np.uint8).copy()]),
        "metrics": MetricRecord({
            "available": available, "num-examples": 1,
        }),
    }), reply_to=request)


class NativeTreeRequestTests(unittest.TestCase):
    def test_exact_public_wire_builds_the_closed_backend_profile(self):
        _request, encoded, digest = _request_wire()
        parsed = native_tree_request.parse_request_wire(encoded, digest)
        manifest = native_tree_request.public_backend_manifest(parsed)
        self.assertEqual(manifest["task"], "binary_classification")
        self.assertEqual(manifest["engine_params"]["max_bin"]["value"], 4)
        self.assertNotIn("contract", manifest)

        with self.assertRaises(ValueError):
            native_tree_request.parse_request_wire(encoded + "=", digest)
        with self.assertRaises(ValueError):
            native_tree_request.parse_request_wire(encoded, "0" * 64)
        with self.assertRaises(ValueError):
            native_tree_request.parse_request_wire("á", digest)


class NativeTreeClientTests(unittest.TestCase):
    def setUp(self):
        self.request, self.request_b64, self.request_sha256 = _request_wire()
        self.root = tempfile.TemporaryDirectory()
        pd.DataFrame({
            "age": [20.0, 60.0], "marker": [-0.5, 0.5],
            "outcome": [0, 1],
        }).to_csv(os.path.join(self.root.name, "train.csv"), index=False)
        with open(os.path.join(self.root.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(_node_manifest(self.request_b64, self.request_sha256), handle)
        self.context = SimpleNamespace(
            node_config={"manifest-dir": self.root.name},
            run_config=_run_config(
                self.request_b64, self.request_sha256, nodes=2),
            state=RecordDict(),
        )

    def tearDown(self):
        self.root.cleanup()

    def test_manifest_mismatch_fails_before_private_data_and_is_constant(self):
        message = _request_message(1, self.request_b64, "0" * 64)
        with (mock.patch.object(
                  task, "load_native_tree_data",
                  side_effect=AssertionError("private data was read")) as load,
              mock.patch.object(
                  seeding, "master_seed",
                  side_effect=AssertionError(
                      "seed derived before materialization")) as master_seed):
            reply = client_app.train(message, self.context)
        load.assert_not_called()
        master_seed.assert_not_called()
        self.assertEqual(set(reply.content.keys()), {"arrays", "metrics"})
        self.assertEqual(dict(reply.content["metrics"]), {
            "available": 0, "num-examples": 1})
        arrays = reply.content["arrays"].to_numpy_ndarrays()
        self.assertEqual(len(arrays), 1)
        self.assertEqual(arrays[0].dtype, np.uint8)
        self.assertEqual(arrays[0].tobytes(), client_app._UNAVAILABLE)

    def test_unverified_bundle_fails_before_private_data_or_seed(self):
        message = _request_message(
            1, self.request_b64, self.request_sha256)
        with (mock.patch.object(client_app, "_NATIVE_BUNDLE", None),
              mock.patch.object(client_app.xgboost_bundle,
                                "is_verified_bundle", return_value=False),
              mock.patch.object(
                  task, "load_native_tree_data",
                  side_effect=AssertionError("private data was read")) as load,
              mock.patch.object(
                  seeding, "master_seed",
                  side_effect=AssertionError("seed was derived")) as master_seed):
            reply = client_app.train(message, self.context)
        load.assert_not_called()
        master_seed.assert_not_called()
        self.assertEqual(dict(reply.content["metrics"]), {
            "available": 0, "num-examples": 1})

    def test_one_private_read_then_prepare_ffi_and_sanitize(self):
        message = _request_message(
            1, self.request_b64, self.request_sha256)
        original_load = task.load_native_tree_data
        original_manifest_load = task._load_manifest
        original_frame_read = task._read_staged_frame
        original_sanitize = client_app.xgboost_adapter.sanitize_xgboost_artifact
        with (mock.patch.object(client_app, "_NATIVE_BUNDLE", object()),
              mock.patch.object(client_app.xgboost_bundle, "is_verified_bundle",
                                return_value=True),
              mock.patch.object(task, "load_native_tree_data",
                                wraps=original_load) as load,
              mock.patch.object(task, "_load_manifest",
                                wraps=original_manifest_load) as manifest_load,
              mock.patch.object(task, "_read_staged_frame",
                                wraps=original_frame_read) as frame_read,
              mock.patch.object(client_app.xgboost_adapter,
                                "prepare_xgboost_training",
                                return_value=object()) as prepare,
              mock.patch.object(client_app.xgboost_adapter,
                                "train_xgboost_native",
                                return_value=_native_artifact()) as native,
              mock.patch.object(client_app.xgboost_adapter,
                                "sanitize_xgboost_artifact",
                                wraps=original_sanitize) as sanitize):
            reply = client_app.train(message, self.context)
        load.assert_called_once()
        self.assertEqual(load.call_args.args, (self.context,))
        self.assertEqual(load.call_args.kwargs["manifest"],
                         _node_manifest(
                             self.request_b64, self.request_sha256))
        manifest_load.assert_called_once_with(self.context)
        frame_read.assert_called_once()
        prepare.assert_called_once()
        native.assert_called_once()
        sanitize.assert_called_once()
        self.assertEqual(dict(reply.content["metrics"]), {
            "available": 1, "num-examples": 1})
        artifact = reply.content["arrays"].to_numpy_ndarrays()[0].tobytes()
        self.assertEqual(_canonical(json.loads(artifact)), artifact)
        self.assertNotIn(b"patient", artifact.lower())

    def test_isolation_guard_rejects_wider_runner_or_uploaded_code(self):
        with mock.patch.dict(sys.modules, {"torch": object()}):
            with self.assertRaisesRegex(RuntimeError, "not isolated"):
                client_app._assert_native_process_isolated()
        with mock.patch.dict(
                os.environ, {"DSFLOWER_PINNED_APP_DIR": "/uploaded"}):
            with self.assertRaisesRegex(RuntimeError, "uploaded code"):
                client_app._assert_native_process_isolated()

    def test_privacy_numbers_require_exact_numeric_manifest_types(self):
        manifest = _node_manifest(self.request_b64, self.request_sha256)
        manifest["privacy-epsilon"] = True
        with self.assertRaisesRegex(ValueError, "privacy parameters"):
            client_app._node_privacy(manifest, self.context, "train")

    def test_holdout_replays_exact_ensemble_and_rejects_a_second_identity(self):
        holdout = resampling.holdout_contract(500_000, "row")
        manifest = _node_manifest(
            self.request_b64, self.request_sha256, holdout=holdout)
        with open(os.path.join(self.root.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(manifest, handle)
        self.context.run_config = _run_config(
            self.request_b64, self.request_sha256, nodes=1,
            holdout=holdout)
        holdout_profile = dict(holdout, bins=4)
        train_message = server_app._request_messages(
            (1,), self.request_b64, self.request_sha256,
            holdout=holdout_profile)[0]
        original_load = task.load_native_tree_data

        with (mock.patch.object(client_app, "_NATIVE_BUNDLE", object()),
              mock.patch.object(client_app.xgboost_bundle,
                                "is_verified_bundle", return_value=True),
              mock.patch.object(
                  resampling, "holdout_mask_from_context",
                  return_value=np.array([False, True])),
              mock.patch.object(task, "load_native_tree_data",
                                wraps=original_load) as load,
              mock.patch.object(client_app.xgboost_adapter,
                                "prepare_xgboost_training",
                                return_value=object()),
              mock.patch.object(client_app.xgboost_adapter,
                                "train_xgboost_native",
                                return_value=_native_artifact())):
            train_reply = client_app.train(train_message, self.context)
            member = train_reply.content["arrays"].to_numpy_ndarrays()[
                0].tobytes()
            public_manifest = native_tree_request.public_backend_manifest(
                self.request)
            ensemble, digest = native_tree_engine.build_ensemble(
                public_manifest, [member])
            evaluate = server_app._evaluation_messages(
                (1,), self.request, self.request_b64, self.request_sha256,
                holdout_profile, ensemble, digest)[0]

            def deterministic_release(y, predictions, layout, **kwargs):
                return (validation.validation_sufficient_vector(
                    y, predictions, layout,
                    target_bounds=kwargs.get("target_bounds"),
                    unit_ids=kwargs.get("unit_ids")), 0.0)

            with mock.patch.object(
                    validation, "private_validation_vector",
                    side_effect=deterministic_release) as release:
                first = client_app.train(evaluate, self.context)
                replay = client_app.train(evaluate, self.context)
                changed, changed_digest = native_tree_engine.build_ensemble(
                    public_manifest, [_native_artifact(0.2)])
                changed_message = server_app._evaluation_messages(
                    (1,), self.request, self.request_b64,
                    self.request_sha256, holdout_profile, changed,
                    changed_digest)[0]
                rejected = client_app.train(changed_message, self.context)
                bad_nodes = server_app._evaluation_messages(
                    (1,), self.request, self.request_b64,
                    self.request_sha256, holdout_profile, ensemble, digest)[0]
                bad_nodes.content["config"]["native-tree-n-nodes"] = 2
                rejected_nodes = client_app.train(bad_nodes, self.context)
                bad_bins = server_app._evaluation_messages(
                    (1,), self.request, self.request_b64,
                    self.request_sha256, dict(holdout, bins=5),
                    ensemble, digest)[0]
                rejected_bins = client_app.train(bad_bins, self.context)

        self.assertEqual(release.call_count, 1)
        self.assertEqual(load.call_count, 2)
        self.assertEqual(dict(first.content["metrics"])["available"], 1)
        self.assertEqual(
            first.content["arrays"].to_numpy_ndarrays()[0].tobytes(),
            replay.content["arrays"].to_numpy_ndarrays()[0].tobytes())
        self.assertEqual(dict(rejected.content["metrics"]), {
            "available": 0, "num-examples": 1})
        self.assertEqual(dict(rejected_nodes.content["metrics"]), {
            "available": 0, "num-examples": 1})
        self.assertEqual(dict(rejected_bins.content["metrics"]), {
            "available": 0, "num-examples": 1})

    def test_empty_holdout_is_one_fixed_noise_only_vector(self):
        holdout = resampling.holdout_contract(500_000, "patient")
        manifest = _node_manifest(
            self.request_b64, self.request_sha256, holdout=holdout)
        manifest["dp-unit"] = "patient"
        with open(os.path.join(self.root.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(manifest, handle)
        self.context.run_config = _run_config(
            self.request_b64, self.request_sha256, nodes=1,
            holdout=holdout)
        member = _native_artifact()
        client_app._mark_training_complete(
            self.context, self.request, manifest, member)
        public_manifest = native_tree_request.public_backend_manifest(
            self.request)
        ensemble, digest = native_tree_engine.build_ensemble(
            public_manifest, [member])
        evaluate = server_app._evaluation_messages(
            (1,), self.request, self.request_b64, self.request_sha256,
            dict(holdout, bins=4), ensemble, digest)[0]
        layout = validation.validation_layout(
            "classification", n_classes=2, bins=4)
        expected = np.arange(layout["size"], dtype=np.float64)
        with (mock.patch.object(
                  task, "load_native_tree_data", return_value=(
                      np.asarray([[20.0, -0.5], [60.0, 0.5]]),
                      np.asarray([0, 1]), np.asarray(["p0", "p1"]))),
              mock.patch.object(
                  resampling, "holdout_mask_from_context",
                  return_value=np.array([False, False])),
              mock.patch.object(
                  validation, "private_sufficient_vector",
                  return_value=(expected, 1.0)) as noise_only,
              mock.patch.object(
                  validation, "private_validation_vector",
                  side_effect=AssertionError("non-empty path used"))):
            reply = client_app.train(evaluate, self.context)
        self.assertEqual(dict(reply.content["metrics"]), {
            "available": 1, "num-examples": 1})
        np.testing.assert_array_equal(
            reply.content["arrays"].to_numpy_ndarrays()[0], expected)
        noise_only.assert_called_once()
        np.testing.assert_array_equal(
            noise_only.call_args.args[0], np.zeros(layout["size"]))
        self.assertAlmostEqual(noise_only.call_args.kwargs["epsilon"], 0.2)
        self.assertAlmostEqual(noise_only.call_args.kwargs["delta"], 2.0e-7)
        self.assertIs(
            noise_only.call_args.kwargs["include_zero_neighbor"], True)


class _EndToEndGrid:
    def __init__(self, context):
        self.context = context
        self.send_calls = 0

    @staticmethod
    def get_node_ids():
        return [11, 22]

    def send_and_receive(self, messages, timeout):
        self.send_calls += 1
        return [client_app.train(message, self.context) for message in messages]


class _HoldoutGrid:
    def __init__(self, contexts):
        self.contexts = contexts
        self.send_calls = 0

    @staticmethod
    def get_node_ids():
        return [11, 22]

    def send_and_receive(self, messages, timeout):
        self.send_calls += 1
        return [client_app.train(
            message, self.contexts[message.metadata.dst_node_id])
            for message in messages]


class NativeTreeServerTests(unittest.TestCase):
    def test_holdout_vector_wire_is_exact_float64_and_finite(self):
        _request, request_b64, request_sha256 = _request_wire()
        message = _request_message(1, request_b64, request_sha256)
        layout = validation.validation_layout(
            "classification", n_classes=2, bins=4)

        def reply(value):
            return Message(content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[value]),
                "metrics": MetricRecord({
                    "available": 1, "num-examples": 1,
                }),
            }), reply_to=message)

        vector = np.arange(layout["size"], dtype=np.float64)
        np.testing.assert_array_equal(
            server_app._vector_from_reply(reply(vector), layout), vector)
        with self.assertRaisesRegex(RuntimeError, "invalid geometry"):
            server_app._vector_from_reply(
                reply(vector.astype(np.float32)), layout)
        invalid = vector.copy()
        invalid[0] = np.nan
        with self.assertRaisesRegex(RuntimeError, "invalid geometry"):
            server_app._vector_from_reply(reply(invalid), layout)

    def test_regression_holdout_requires_exact_public_target_bounds(self):
        request, request_b64, request_sha256 = _regression_request_wire()
        holdout = resampling.holdout_contract(500_000, "row")
        config = _run_config(
            request_b64, request_sha256, "/tmp/results", nodes=2,
            holdout=holdout)
        config.update({
            "holdout-target-lower": -10.0,
            "holdout-target-upper": 10.0,
        })
        parsed = server_app._run_contract(config)
        self.assertEqual(parsed[0], request)
        self.assertEqual(parsed[4]["sha256"], holdout["sha256"])
        self.assertEqual(
            server_app._holdout_layout(request, 4)["task"], "regression")
        changed = dict(config, **{"holdout-target-lower": -9.0})
        with self.assertRaisesRegex(ValueError, "bounds differ"):
            server_app._run_contract(changed)

    def test_exact_roster_rejects_extra_and_duplicate_replies(self):
        class ExtraGrid:
            @staticmethod
            def get_node_ids():
                return [1, 2, 3]

        with self.assertRaisesRegex(RuntimeError, "extra node"):
            server_app._exact_roster(ExtraGrid(), 2, 1.0)

        _request, request_b64, request_sha256 = _request_wire()
        messages = server_app._request_messages(
            (1, 2), request_b64, request_sha256)

        class DuplicateGrid:
            @staticmethod
            def send_and_receive(_messages, timeout):
                return [
                    _release_reply(messages[0], _native_artifact()),
                    _release_reply(messages[0], _native_artifact()),
                ]

        with self.assertRaisesRegex(RuntimeError, "duplicated"):
            server_app._collect_artifacts(
                DuplicateGrid(), (1, 2), request_b64, request_sha256,
                1.0, 1_000_000)

        class MissingGrid:
            @staticmethod
            def send_and_receive(_messages, timeout):
                return [_release_reply(messages[0], _native_artifact())]

        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            server_app._collect_artifacts(
                MissingGrid(), (1, 2), request_b64, request_sha256,
                1.0, 1_000_000)

    def test_permuted_reply_order_produces_identical_ensemble_bytes(self):
        request, request_b64, request_sha256 = _request_wire()
        messages = server_app._request_messages(
            (1, 2), request_b64, request_sha256)
        replies = [
            _release_reply(messages[0], _native_artifact(0.1)),
            _release_reply(messages[1], _native_artifact(0.2)),
        ]

        class Grid:
            def __init__(self, reverse):
                self.reverse = reverse

            def send_and_receive(self, _messages, timeout):
                return list(reversed(replies)) if self.reverse else list(replies)

        forward = server_app._collect_artifacts(
            Grid(False), (1, 2), request_b64, request_sha256,
            1.0, 1_000_000)
        reverse = server_app._collect_artifacts(
            Grid(True), (1, 2), request_b64, request_sha256,
            1.0, 1_000_000)
        manifest = native_tree_request.public_backend_manifest(request)
        first = native_tree_engine.build_ensemble(manifest, forward)[0]
        second = native_tree_engine.build_ensemble(manifest, reverse)[0]
        self.assertEqual(first, second)
        self.assertNotIn(b"node_id", first.lower())

    def test_malformed_member_yields_no_partial_model(self):
        _request, request_b64, request_sha256 = _request_wire()
        with tempfile.TemporaryDirectory() as results_dir:
            cfg = _run_config(request_b64, request_sha256, results_dir, nodes=2)
            messages = server_app._request_messages(
                (1, 2), request_b64, request_sha256)

            class Grid:
                @staticmethod
                def get_node_ids():
                    return [1, 2]

                @staticmethod
                def send_and_receive(_messages, timeout):
                    return [
                        _release_reply(messages[0], _native_artifact()),
                        _release_reply(messages[1], b'{"private":"channel"}'),
                    ]

            server_app.main(Grid(), SimpleNamespace(run_config=cfg))
            spec = native_tree_engine.release_spec("xgboost")
            self.assertFalse(os.path.exists(os.path.join(
                results_dir, spec["model_file"])))
            self.assertFalse(os.path.exists(os.path.join(
                results_dir, spec["profile_file"])))
            with open(os.path.join(results_dir, server_app.HISTORY_FILE),
                      encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), [{
                    "available": False, "round": 1}])

    def test_doubled_client_server_e2e_is_atomic_and_predictable(self):
        request, request_b64, request_sha256 = _request_wire()
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            pd.DataFrame({
                "age": [20.0, 60.0], "marker": [-0.5, 0.5],
                "outcome": [0, 1],
            }).to_csv(os.path.join(root, "train.csv"), index=False)
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(_node_manifest(request_b64, request_sha256), handle)
            cfg = _run_config(request_b64, request_sha256, results_dir, nodes=2)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)
            grid = _EndToEndGrid(context)
            original_load = task.load_native_tree_data
            with (mock.patch.object(client_app, "_NATIVE_BUNDLE", object()),
                  mock.patch.object(client_app.xgboost_bundle,
                                    "is_verified_bundle", return_value=True),
                  mock.patch.object(task, "load_native_tree_data",
                                    wraps=original_load) as load,
                  mock.patch.object(client_app.xgboost_adapter,
                                    "prepare_xgboost_training",
                                    return_value=object()) as prepare,
                  mock.patch.object(client_app.xgboost_adapter,
                                    "train_xgboost_native",
                                    return_value=_native_artifact()) as native):
                server_app.main(grid, SimpleNamespace(run_config=cfg))

            self.assertEqual(grid.send_calls, 1)
            self.assertEqual(load.call_count, 2)
            self.assertEqual(prepare.call_count, 2)
            self.assertEqual(native.call_count, 2)
            spec = native_tree_engine.release_spec("xgboost")
            model_path = os.path.join(results_dir, spec["model_file"])
            profile_path = os.path.join(results_dir, spec["profile_file"])
            with open(model_path, "rb") as handle:
                artifact = handle.read()
            with open(profile_path, "rb") as handle:
                profile_bytes = handle.read()
            profile = json.loads(profile_bytes.decode("ascii"))
            with open(os.path.join(results_dir, server_app.HISTORY_FILE),
                      encoding="utf-8") as handle:
                history = json.load(handle)
            self.assertEqual(history, [{"available": True, "round": 1}])
            self.assertEqual(profile["contract"], spec["profile_contract"])
            self.assertEqual(profile_bytes, server_app._canonical_json(profile))
            self.assertEqual(
                profile["artifact"]["sha256"],
                hashlib.sha256(artifact).hexdigest())
            self.assertEqual(profile["artifact"]["size_bytes"], len(artifact))
            self.assertEqual(profile["artifact"]["format"],
                             spec["ensemble_format"])
            self.assertEqual(profile["native_tree_request_b64"], request_b64)
            self.assertEqual(profile["native_tree_request_sha256"],
                             request_sha256)
            self.assertEqual(profile["public_schema_sha256"],
                             request["public_schema"]["sha256"])
            self.assertEqual(profile["task"], "binary")
            parsed = json.loads(artifact)
            self.assertEqual(len(parsed["models"]), 2)
            public_manifest = native_tree_request.public_backend_manifest(request)
            predictor = xgboost_predictor.parse_xgboost_ensemble(
                artifact, public_manifest)
            self.assertEqual(predictor.num_models, 2)
            self.assertEqual(len(predictor.predict([[20.0, 0.0]])), 1)

    def test_atomic_holdout_trains_only_complement_then_releases_test(self):
        request, request_b64, request_sha256 = _request_wire()
        holdout = resampling.holdout_contract(500_000, "row")
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            pd.DataFrame({
                "age": [10.0, 20.0, 60.0, 70.0],
                "marker": [-1.0, -0.5, 0.5, 1.0],
                "outcome": [0, 1, 0, 1],
            }).to_csv(os.path.join(root, "train.csv"), index=False)
            manifest = _node_manifest(
                request_b64, request_sha256, holdout=holdout)
            manifest["n_units"] = 4
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            cfg = _run_config(
                request_b64, request_sha256, results_dir, nodes=2,
                holdout=holdout)
            contexts = {
                node_id: SimpleNamespace(
                    node_config={"manifest-dir": root},
                    run_config=cfg, state=RecordDict())
                for node_id in (11, 22)
            }
            grid = _HoldoutGrid(contexts)
            original_load = task.load_native_tree_data

            def deterministic_release(y, predictions, layout, **kwargs):
                raw = validation.validation_sufficient_vector(
                    y, predictions, layout,
                    target_bounds=kwargs.get("target_bounds"),
                    unit_ids=kwargs.get("unit_ids"))
                return raw, 0.0

            with (mock.patch.object(client_app, "_NATIVE_BUNDLE", object()),
                  mock.patch.object(client_app.xgboost_bundle,
                                    "is_verified_bundle", return_value=True),
                  mock.patch.object(
                      resampling, "holdout_mask_from_context",
                      return_value=np.array([False, False, True, True])),
                  mock.patch.object(task, "load_native_tree_data",
                                    wraps=original_load) as load,
                  mock.patch.object(client_app.xgboost_adapter,
                                    "prepare_xgboost_training",
                                    return_value=object()) as prepare,
                  mock.patch.object(client_app.xgboost_adapter,
                                    "train_xgboost_native",
                                    return_value=_native_artifact()),
                  mock.patch.object(
                      validation, "private_validation_vector",
                      side_effect=deterministic_release) as release):
                server_app.main(grid, SimpleNamespace(run_config=cfg))

            self.assertEqual(grid.send_calls, 2)
            self.assertEqual(load.call_count, 4)
            self.assertEqual(prepare.call_count, 2)
            for call in prepare.call_args_list:
                self.assertEqual(call.args[1].shape, (2, 2))
                self.assertEqual(call.args[2].shape, (2,))
            self.assertEqual(release.call_count, 2)
            for call in release.call_args_list:
                self.assertEqual(call.args[0].shape, (2,))
                self.assertAlmostEqual(call.kwargs["epsilon"], 0.2)
                self.assertAlmostEqual(call.kwargs["delta"], 2.0e-7)

            spec = native_tree_engine.release_spec("xgboost")
            model_path = os.path.join(results_dir, spec["model_file"])
            with open(model_path, "rb") as handle:
                ensemble = handle.read()
            with open(os.path.join(results_dir, "holdout.json"),
                      encoding="ascii") as handle:
                released = json.load(handle)
            with open(os.path.join(results_dir, server_app.HISTORY_FILE),
                      encoding="ascii") as handle:
                history = json.load(handle)
            self.assertEqual(history, [{"available": True, "round": 1}])
            self.assertEqual(released["method"], "holdout")
            self.assertEqual(released["n_nodes"], 2)
            self.assertTrue(released["pooled_only"])
            self.assertEqual(set(released["provenance"]), {
                "artifact_sha256", "native_tree_request_sha256",
                "public_schema_sha256", "resampling_contract_sha256",
            })
            self.assertEqual(
                released["provenance"]["artifact_sha256"],
                hashlib.sha256(ensemble).hexdigest())
            self.assertEqual(
                released["provenance"]["resampling_contract_sha256"],
                holdout["sha256"])

    def test_holdout_phase_failure_publishes_no_partial_model(self):
        request, request_b64, request_sha256 = _request_wire()
        holdout = resampling.holdout_contract(500_000, "row")
        with tempfile.TemporaryDirectory() as results_dir:
            cfg = _run_config(
                request_b64, request_sha256, results_dir, nodes=2,
                holdout=holdout)

            class Grid:
                calls = 0

                @staticmethod
                def get_node_ids():
                    return [1, 2]

                @classmethod
                def send_and_receive(cls, messages, timeout):
                    cls.calls += 1
                    if cls.calls == 1:
                        return [
                            _release_reply(messages[0], _native_artifact()),
                            _release_reply(messages[1], _native_artifact(0.2)),
                        ]
                    return [_release_reply(messages[0], b"invalid-vector")]

            server_app.main(Grid(), SimpleNamespace(run_config=cfg))
            spec = native_tree_engine.release_spec("xgboost")
            for name in (spec["model_file"], spec["profile_file"],
                         "holdout.json"):
                self.assertFalse(os.path.exists(os.path.join(
                    results_dir, name)))
            with open(os.path.join(results_dir, server_app.HISTORY_FILE),
                      encoding="ascii") as handle:
                self.assertEqual(json.load(handle), [{
                    "available": False, "round": 1}])


if __name__ == "__main__":
    unittest.main()
