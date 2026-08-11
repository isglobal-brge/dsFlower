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


class ReplaySafetyTests(unittest.TestCase):
    def test_replay_never_reads_private_data_or_new_seed(self):
        claim = {
            "status": "replay", "message_id": "m-replay", "release_index": 1,
            "num_rounds": 2, "run_token": "run_" + "a" * 32,
            "request_id": "1" * 64, "epsilon": 1.0, "delta": 1e-5,
        }
        incoming = np.asarray([1.0, 2.0], dtype=np.float32)
        cached = np.asarray([9.0, 8.0], dtype=np.float32)
        private_access = AssertionError("replay touched private release state")

        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[incoming])
            }), dst_node_id=1, message_type="train")
        context = SimpleNamespace(state=RecordDict())
        client_app._cache_reply(context, claim, [cached])

        with (mock.patch.object(
                  client_app.release_guard, "claim_release",
                  return_value=claim),
              mock.patch.object(
                  client_app, "load_pinned_run_config",
                  side_effect=private_access) as load_config,
              mock.patch.object(
                  client_app, "load_data",
                  side_effect=private_access) as load_data,
              mock.patch.object(
                  client_app, "load_image_collection",
                  side_effect=private_access) as load_images,
              mock.patch.object(
                  client_app.seeding, "master_seed",
                  side_effect=private_access) as master_seed,
              mock.patch.object(
                  client_app, "_train_neural",
                  side_effect=private_access) as train_neural):
            reply = client_app.train(msg, context)

        for private_call in (
                load_config, load_data, load_images, master_seed, train_neural):
            private_call.assert_not_called()
        self.assertFalse(reply.has_error())
        actual = reply.content["arrays"].to_numpy_ndarrays()[0]
        np.testing.assert_array_equal(actual, cached)

    def test_public_preflight_failure_and_replay_remain_unavailable(self):
        new_claim = {
            "status": "new", "message_id": "m-public-failure",
            "release_index": 1, "num_rounds": 1,
            "request_id": "2" * 64,
        }
        replay_claim = dict(new_claim, status="replay")
        incoming = np.asarray([4.0, 5.0], dtype=np.float32)
        message = Message(
            content=RecordDict({"arrays": ArrayRecord(
                numpy_ndarrays=[incoming])}),
            dst_node_id=1, message_type="train")
        context = SimpleNamespace(state=RecordDict())
        private_access = AssertionError("public preflight touched private data")

        with (mock.patch.object(
                  client_app.release_guard, "claim_release",
                  side_effect=[new_claim, replay_claim]),
              mock.patch.object(
                  client_app, "load_pinned_run_config",
                  side_effect=RuntimeError("public config invalid")),
              mock.patch.object(
                  client_app, "load_data", side_effect=private_access) as load_data):
            first = client_app.train(message, context)
            replay = client_app.train(message, context)

        load_data.assert_not_called()
        for reply in (first, replay):
            self.assertFalse(reply.has_error())
            self.assertEqual(int(reply.content["metrics"].get(
                "public-preflight-unavailable", 0)), 1)
            np.testing.assert_array_equal(
                reply.content["arrays"].to_numpy_ndarrays()[0], incoming)

    def test_execution_failure_marker_survives_an_exact_replay(self):
        claim = {
            "status": "new", "message_id": "m-execution-failure",
            "release_index": 1, "num_rounds": 1,
            "request_id": "3" * 64,
        }
        replay_claim = dict(claim, status="replay")
        context = SimpleNamespace(state=RecordDict())
        incoming = np.asarray([2.0, 3.0], dtype=np.float32)
        message = Message(
            content=RecordDict({"arrays": ArrayRecord(
                numpy_ndarrays=[incoming])}),
            dst_node_id=1, message_type="train")

        client_app._cache_reply(
            context, claim, [incoming], execution_unavailable=True)
        reply = client_app._replay_reply(context, replay_claim, message)

        self.assertFalse(reply.has_error())
        self.assertEqual(int(reply.content["metrics"].get(
            "execution-unavailable", 0)), 1)
        np.testing.assert_array_equal(
            reply.content["arrays"].to_numpy_ndarrays()[0], incoming)


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
    def _train_reply(value, node_id, **metrics):
        request = Message(
            content=RecordDict(), dst_node_id=node_id, message_type="train")
        return Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.asarray([value], dtype=np.float64)]),
                "metrics": MetricRecord({"num-examples": 1, **metrics}),
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

    def test_strategy_pins_a_fresh_exact_round_config_for_every_message(self):
        class Grid:
            @staticmethod
            def get_node_ids():
                return [11, 22]

        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        shared = ConfigRecord({"public-option": 7})
        messages = strategy.configure_train(
            2,
            ArrayRecord(numpy_ndarrays=[np.asarray([0.0])]),
            shared,
            Grid(),
        )
        self.assertNotIn("server-round", shared)
        self.assertEqual(len(messages), 2)
        for message in messages:
            config = message.content["config"]
            self.assertIsInstance(config, ConfigRecord)
            self.assertEqual(config["server-round"], 2)
            self.assertEqual(config["public-option"], 7)

    def test_aggregation_rejects_duplicate_node_replies(self):
        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        duplicate = [self._train_reply(1.0, 1), self._train_reply(3.0, 1)]
        with self.assertRaisesRegex(RuntimeError, "federation roster"):
            strategy.aggregate_train(1, duplicate)

    def test_strategy_marks_public_preflight_unavailable(self):
        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        strategy._round_input_arrays[1] = ArrayRecord(
            numpy_ndarrays=[np.asarray([0.0])])
        replies = [
            self._train_reply(1.0, 1),
            self._train_reply(
                3.0, 2, **{"public-preflight-unavailable": 1}),
        ]
        strategy.aggregate_train(1, replies)
        self.assertEqual(strategy.available_rounds, set())
        self.assertEqual(strategy.unavailable_rounds, {1})

    def test_unavailable_rounds_never_replace_trained_arrays(self):
        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2)
        trained, _ = strategy.aggregate_train(1, [
            self._train_reply(1.0, 1), self._train_reply(3.0, 2)])
        public_failure, _ = strategy.aggregate_train(2, [
            self._train_reply(
                300.0, 1, **{"public-preflight-unavailable": 1}),
            self._train_reply(
                400.0, 2, **{"public-preflight-unavailable": 1}),
        ])
        execution_failure, _ = strategy.aggregate_train(3, [
            self._train_reply(
                500.0, 1, **{"execution-unavailable": 1}),
            self._train_reply(
                600.0, 2, **{"execution-unavailable": 1}),
        ])
        np.testing.assert_array_equal(
            trained.to_numpy_ndarrays()[0], np.asarray([2.0]))
        np.testing.assert_array_equal(
            public_failure.to_numpy_ndarrays()[0], np.asarray([2.0]))
        np.testing.assert_array_equal(
            execution_failure.to_numpy_ndarrays()[0], np.asarray([2.0]))
        self.assertEqual(strategy.available_rounds, {1})
        self.assertEqual(strategy.unavailable_rounds, {2, 3})

class UnsupportedTrackTests(unittest.TestCase):
    def test_tree_track_is_rejected_and_runtime_entry_points_are_absent(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"dp-track": "trees"}, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            with self.assertRaisesRegex(ValueError, "invalid dp-track 'trees'"):
                task.load_dp_track(context)

        for module, names in (
                (client_app, ("_train_trees",)),
                (server_app, ("_collect_trees", "_bag_boosters", "_save_trees"))):
            for name in names:
                with self.subTest(module=module.__name__, name=name):
                    self.assertFalse(hasattr(module, name))


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

    def test_unavailable_privacy_tail_writes_status_but_no_model(self):
        arrays = [np.asarray([0.0], dtype=np.float32)]
        result = SimpleNamespace(
            arrays=SimpleNamespace(to_numpy_ndarrays=lambda: arrays),
            train_metrics_clientapp={},
        )
        with tempfile.TemporaryDirectory() as results_dir:
            server_app._save_results({
                "results-dir": results_dir,
                "num-server-rounds": 1,
            }, None, result, available_rounds=set())
            self.assertFalse(os.path.exists(
                os.path.join(results_dir, "global_model.json")))
            self.assertFalse(os.path.exists(
                os.path.join(results_dir, "model.npz")))
            with open(os.path.join(results_dir, "history.json"),
                      encoding="utf-8") as handle:
                history = json.load(handle)
            self.assertEqual(history, [{
                "round": 1, "n_failures": 0, "available": False}])

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
