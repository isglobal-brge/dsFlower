"""Cross-language runtime contract regressions."""

import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
from flwr.common import (
    ArrayRecord, ConfigRecord, Error, Message, MetricRecord, RecordDict)


FLOWER_APP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import client_app, server_app, task  # noqa: E402


class PublicPrivacyTailTests(unittest.TestCase):
    def test_tree_noop_uses_only_public_manifest_and_returns_valid_booster(self):
        token = "run_" + "a" * 32
        manifest = {
            "run_token": token,
            "data_type": "tabular",
            "data_format": "none",
            "n_samples": 0,
            "n_units": 0,
            "target_column": "target",
            "feature_columns": ["f1", "f2"],
            "dp-unit": "row",
            "patient_column": None,
            "patient-id-canonicalization": "trim-utf8-v2",
            "source_kind": "privacy_noop",
            "dp-track": "trees",
            "num-features": 2,
            "gbdt-spec": {
                "objective": "binary:logistic",
                "max_depth": 2,
                "n_trees": 3,
                "learning_rate": 0.1,
                "reg_lambda": 1.0,
                "n_bins": 8,
                "feature_ranges": [[-1.0, 1.0], [-2.0, 2.0]],
            },
            "privacy-reserved": True,
            "privacy-release-enabled": False,
            "privacy-domain": "node",
            "privacy-allocation-index": 30,
            "privacy-max-releases": 1,
            "privacy-epsilon": 1.0e-9,
            "privacy-delta": 1.0e-15,
            "privacy-adjacency": "replace_one",
        }
        claim = {
            "status": "noop", "message_id": "tail", "release_index": None,
            "max_releases": 1, "run_token": token, "allocation_index": 30,
            "epsilon": 1.0e-9, "delta": 1.0e-15,
        }
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.zeros(1, dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )

        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config={}, state=RecordDict())

            private_read = AssertionError("private data was read")
            with (mock.patch.object(client_app.release_guard, "claim_release",
                                    return_value=claim),
                  mock.patch.object(client_app, "load_data",
                                    side_effect=private_read) as load_data,
                  mock.patch.object(client_app, "load_image_collection",
                                    side_effect=private_read) as load_images,
                  mock.patch.object(client_app, "load_tabular_patient_ids",
                                    side_effect=private_read) as load_ids,
                  mock.patch.object(task, "_read_staged_frame",
                                    side_effect=private_read) as read_frame):
                replies = [client_app.train(msg, context) for _ in range(2)]

            load_data.assert_not_called()
            load_images.assert_not_called()
            load_ids.assert_not_called()
            read_frame.assert_not_called()
            self.assertEqual(os.listdir(manifest_dir), ["manifest.json"])
            self.assertTrue(all(not reply.has_error() for reply in replies))
            arrays = [reply.content["arrays"].to_numpy_ndarrays()[0]
                      for reply in replies]
            np.testing.assert_array_equal(arrays[0], arrays[1])
            booster = json.loads(bytes(np.asarray(
                arrays[0], dtype=np.uint8)).decode("utf-8"))
            self.assertEqual(len(booster["trees"]), 3)
            self.assertTrue(all(
                weight == 0.0
                for tree in booster["trees"] for weight in tree["w"]))


class MultilabelRuntimeTests(unittest.TestCase):
    def test_tabular_loader_returns_n_by_l_targets(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            pd.DataFrame({
                "x": [1.0, 2.0], "a": [0, 1], "b": [1, 0],
            }).to_csv(os.path.join(manifest_dir, "data.csv"), index=False)
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_file": "data.csv", "data_format": "csv",
                    "target_column": ["a", "b"], "feature_columns": ["x"],
                    "task-type": "classification", "loss-name": "multilabel_bce",
                    "num-classes": 2, "num-labels": 2,
                }, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir})

            X, y = task.load_data(context)
            np.testing.assert_array_equal(X, np.asarray([[1], [2]], np.float32))
            np.testing.assert_array_equal(
                y, np.asarray([[0, 1], [1, 0]], np.float32))

    def test_patient_pooling_uses_majority_per_label(self):
        X, y = client_app._pool_by_patient(
            np.asarray([[1.0], [3.0], [5.0]], np.float32),
            np.asarray([[0, 1], [1, 1], [0, 0]], np.float32),
            np.asarray(["p1", "p1", "p2"]), "multilabel_bce")
        np.testing.assert_array_equal(X, np.asarray([[2], [5]], np.float32))
        np.testing.assert_array_equal(y, np.asarray([[1, 1], [0, 0]], np.float32))


class StrategyRuntimeTests(unittest.TestCase):
    def test_adaptive_strategy_receives_public_hyperparameters(self):
        strategy = server_app._build_strategy({
            "strategy": "fedadam", "strategy-eta": 0.2,
            "strategy-eta-l": 0.05, "strategy-beta-1": 0.8,
            "strategy-beta-2": 0.95, "strategy-tau": 1e-4,
        }, min_nodes=3)

        self.assertEqual(strategy.min_train_nodes, 3)
        self.assertEqual(strategy.min_available_nodes, 3)
        self.assertEqual(strategy.fraction_train, 1.0)
        self.assertEqual(strategy.fraction_evaluate, 0.0)
        self.assertEqual(strategy.weighted_by_key, "num-examples")
        self.assertEqual(strategy.eta, 0.2)
        self.assertEqual(strategy.eta_l, 0.05)
        self.assertEqual(strategy.beta_1, 0.8)
        self.assertEqual(strategy.beta_2, 0.95)
        self.assertEqual(strategy.tau, 1e-4)

    @staticmethod
    def _train_reply(value, node_id):
        request = Message(
            content=RecordDict(), dst_node_id=node_id, message_type="train")
        return Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.asarray([value], dtype=np.float64)]),
                "metrics": MetricRecord({"num-examples": 1}),
            }),
            reply_to=request,
        )

    @staticmethod
    def _error_reply(node_id):
        request = Message(
            content=RecordDict(), dst_node_id=node_id, message_type="train")
        return Message(error=Error(1, "client failed"), reply_to=request)

    def test_strategy_requires_every_configured_node_in_every_round(self):
        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        valid = [self._train_reply(1.0, 1), self._train_reply(3.0, 2)]
        arrays, _ = strategy.aggregate_train(1, valid)
        np.testing.assert_array_equal(
            arrays.to_numpy_ndarrays()[0], np.asarray([2.0]))

        for name in server_app._STRATEGIES:
            with self.subTest(strategy=name):
                candidate = server_app._build_strategy(
                    {"strategy": name}, min_nodes=2)
                with self.assertRaisesRegex(
                        RuntimeError, "1 of 2.*degraded federation"):
                    candidate.aggregate_train(1, [
                        self._train_reply(1.0, 1), self._error_reply(2)])
        with self.assertRaisesRegex(RuntimeError, "1 of 2.*degraded federation"):
            strategy.aggregate_train(1, valid[:1])
        with self.assertRaisesRegex(RuntimeError, "3 of 2.*degraded federation"):
            strategy.aggregate_train(
                1, valid + [self._train_reply(5.0, 3)])

    def test_strategy_rejects_an_extra_connected_node_before_sending(self):
        class ChangingGrid:
            def __init__(self):
                self.calls = 0

            def get_node_ids(self):
                self.calls += 1
                return [] if self.calls == 1 else [11, 22, 33]

        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        with self.assertRaisesRegex(
                RuntimeError, "3 node.*expected exactly 2"):
            strategy.configure_train(
                1,
                ArrayRecord(numpy_ndarrays=[np.asarray([0.0])]),
                ConfigRecord(),
                ChangingGrid(),
            )

    def test_tree_aggregation_rejects_any_missing_booster(self):
        booster = {
            "model_type": "dp_gbdt", "trees": [], "n_trees": 0,
        }
        raw = np.frombuffer(
            json.dumps(booster).encode("utf-8"), dtype=np.uint8)
        valid = [self._train_reply(raw, 1), self._train_reply(raw, 2)]
        with self.assertRaisesRegex(RuntimeError, "2 of 2.*degraded federation"):
            server_app._collect_trees(
                valid + [self._error_reply(3)], n_connected=2, min_nodes=2)


class ArtifactRuntimeTests(unittest.TestCase):
    def test_save_results_writes_reader_compatible_portable_weights(self):
        arrays = [
            np.asarray([[1.25, -2.5]], dtype=np.float32),
            np.asarray([0.75], dtype=np.float32),
        ]
        result = SimpleNamespace(
            arrays=SimpleNamespace(to_numpy_ndarrays=lambda: arrays),
            train_metrics_clientapp={},
        )
        with tempfile.TemporaryDirectory() as results_dir:
            server_app._save_results({
                "results-dir": results_dir,
                "num-server-rounds": 2,
            }, None, result)

            with open(os.path.join(results_dir, "global_model.json"),
                      encoding="utf-8") as handle:
                portable = json.load(handle)
            self.assertEqual(portable["0"], [[1.25, -2.5]])
            self.assertEqual(portable["1"], [0.75])
            self.assertEqual(portable["__shapes__"], [[1, 2], [1]])
            self.assertEqual(portable["__round__"], 2)
            self.assertTrue(os.path.isfile(os.path.join(results_dir, "model.npz")))

    def test_portable_weights_are_bounded_and_reject_non_finite_values(self):
        with tempfile.TemporaryDirectory() as results_dir:
            original = server_app._PORTABLE_JSON_MAX_BYTES
            try:
                server_app._PORTABLE_JSON_MAX_BYTES = 1
                server_app._save_portable_arrays(
                    results_dir, [np.asarray([1.0])], 1)
            finally:
                server_app._PORTABLE_JSON_MAX_BYTES = original
            self.assertFalse(os.path.exists(
                os.path.join(results_dir, "global_model.json")))
            with open(os.path.join(results_dir, "global_model.skipped.json"),
                      encoding="utf-8") as handle:
                marker = json.load(handle)
            self.assertEqual(marker["reason"], "weights_exceed_json_limit")

            with self.assertRaisesRegex(RuntimeError, "non-finite"):
                server_app._save_portable_arrays(
                    results_dir, [np.asarray([np.inf])], 1)


if __name__ == "__main__":
    unittest.main()
