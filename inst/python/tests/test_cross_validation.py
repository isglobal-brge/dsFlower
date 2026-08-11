"""Dedicated neural cross-validation privacy and orchestration regressions."""

import json
import os
import stat
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)


FLOWER_APP = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "flower_app"))
if FLOWER_APP not in sys.path:
    sys.path.insert(0, FLOWER_APP)

from dsflower_runner import (client_app, release_guard, resampling, server_app,
                             task, validation)  # noqa: E402


class CvPartitionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.secret = os.path.join(self.tmp.name, "node-secret")
        with open(self.secret, "w", encoding="ascii") as handle:
            handle.write("24" * 32)
        os.chmod(self.secret, stat.S_IRUSR | stat.S_IWUSR)
        self.secret_env = mock.patch.dict(
            os.environ, {"DSFLOWER_NODE_SECRET_FILE": self.secret})
        self.secret_env.start()

    def tearDown(self):
        self.secret_env.stop()
        self.tmp.cleanup()

    def test_contract_is_seed_free_and_k_is_strict(self):
        contract = resampling.cross_validation_contract(5, "patient")
        self.assertEqual(contract["folds"], 5)
        self.assertEqual(contract["method"], "cross_validation")
        self.assertFalse(any(
            token in key.lower() for key in contract
            for token in ("seed", "salt", "nonce")))
        with self.assertRaisesRegex(ValueError, "fold"):
            resampling.cross_validation_contract(1, "patient")
        with self.assertRaisesRegex(ValueError, "fold"):
            resampling.cross_validation_contract(11, "patient")
        with self.assertRaisesRegex(ValueError, "field|seed"):
            resampling.validate_cross_validation_contract(
                {**contract, "seed": 7})

    def test_patient_has_one_fold_and_assignment_replays(self):
        contract = resampling.cross_validation_contract(5, "patient")
        ids = np.asarray(["p3", "p1", "p2", "p1", "p3", "p4"])
        first = resampling.cross_validation_folds(
            contract, n_rows=len(ids), unit_ids=ids)
        replay = resampling.cross_validation_folds(
            contract, n_rows=len(ids), unit_ids=ids)
        np.testing.assert_array_equal(first, replay)
        self.assertTrue(bool(np.all((first >= 1) & (first <= 5))))
        for unit in np.unique(ids):
            self.assertEqual(len(set(first[ids == unit].tolist())), 1)

    def test_k_only_maps_the_same_secret_score(self):
        ids = np.asarray(["p%d" % index for index in range(1000)])
        folds_5 = resampling.cross_validation_folds(
            resampling.cross_validation_contract(5, "patient"),
            n_rows=len(ids), unit_ids=ids)
        folds_10 = resampling.cross_validation_folds(
            resampling.cross_validation_contract(10, "patient"),
            n_rows=len(ids), unit_ids=ids)
        np.testing.assert_array_equal(folds_5, (folds_10 + 1) // 2)


class CvReleaseGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        contract = resampling.cross_validation_contract(4, "row")
        self.manifest = {
            "run_token": "run_" + "b" * 32,
            "privacy-adjacency": "replace_one",
            "privacy-policy-sha256": "1" * 64,
            "privacy-epsilon": 2.0,
            "privacy-delta": 1e-5,
            "privacy-cv-training-epsilon": 1.6,
            "privacy-cv-training-delta": 8e-6,
            "privacy-cv-fold-epsilon": 0.4,
            "privacy-cv-fold-delta": 2e-6,
            "privacy-cv-oof-epsilon": 0.4,
            "privacy-cv-oof-delta": 2e-6,
            "num-server-rounds": 2,
            "dp-unit": "row",
            "patient_column": None,
            **resampling.cross_validation_manifest_fields(contract),
        }
        with open(os.path.join(self.tmp.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(self.manifest, handle)
        self.context = SimpleNamespace(
            node_config={"manifest-dir": self.tmp.name}, state=RecordDict())

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def message(operation, fold, server_round, value=1.0):
        return SimpleNamespace(
            metadata=SimpleNamespace(message_id="%s-%s-%s" % (
                operation, fold, server_round), group_id="g"),
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.asarray([value])]),
                "config": ConfigRecord({
                    "server-round": server_round,
                    "dsflower-operation": operation,
                    "dsflower-fold": fold,
                }),
            }))

    def test_budget_and_identity_are_bound_to_operation_fold_and_round(self):
        first = release_guard.claim_release(
            self.context, self.message("cv-train", 1, 1))
        other_fold = release_guard.claim_release(
            self.context, self.message("cv-train", 2, 1))
        accumulate = release_guard.claim_release(
            self.context, self.message("cv-accumulate", 1, 2))
        release = release_guard.claim_release(
            self.context, self.message("cv-release", 5, 2))
        self.assertEqual((first["epsilon"], first["delta"]), (0.4, 2e-6))
        self.assertEqual((accumulate["epsilon"], accumulate["delta"]), (0.0, 0.0))
        self.assertEqual((release["epsilon"], release["delta"]), (0.4, 2e-6))
        self.assertNotEqual(first["request_id"], other_fold["request_id"])
        self.assertEqual(first["fold"], 1)

    def test_control_coordinates_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError, "final training round"):
            release_guard.claim_release(
                self.context, self.message("cv-accumulate", 1, 1))
        with self.assertRaisesRegex(RuntimeError, "final release fold"):
            release_guard.claim_release(
                self.context, self.message("cv-release", 4, 2))
        with self.assertRaisesRegex(RuntimeError, "fold"):
            release_guard.claim_release(
                self.context, self.message("cv-train", 0, 1))

    def test_abort_is_zero_budget_and_has_one_final_coordinate(self):
        claim = release_guard.claim_release(
            self.context, self.message("cv-abort", 5, 2))
        self.assertEqual((claim["epsilon"], claim["delta"]), (0.0, 0.0))
        with self.assertRaisesRegex(RuntimeError, "abort coordinate"):
            release_guard.claim_release(
                self.context, self.message("cv-abort", 4, 2))


class CvClientTests(unittest.TestCase):
    @staticmethod
    def _execution_fixture(contract):
        manifest = {
            "dp-track": "neural", "data_type": "tabular",
            "dp-unit": "row", "patient_column": None,
            "n_units": 9,
            "task-type": "classification", "model-spec-b64": "e30=",
            "loss-name": "bce_logits", "num-classes": 2,
            "num-labels": 2, "num-features": 2, "local-epochs": 1,
            "batch-size": 32, "num-server-rounds": 2,
            "learning-rate": 0.01, "weight-decay": 0.0,
            "l1-penalty": 0.0, "optimizer-name": "adamw",
            "optimizer-beta1": 0.9, "optimizer-beta2": 0.999,
            "optimizer-eps": 1e-8, "optimizer-amsgrad": False,
            "scheduler-name": "step", "scheduler-step-size": 1,
            "scheduler-gamma": 0.5, "cv-validation-bins": 4,
            "cv-n-nodes": 2, "cv-job-sha256": "b" * 64,
            "strategy": "fedavg",
            **resampling.cross_validation_manifest_fields(contract),
        }
        flower_cfg = {
            key: value for key, value in manifest.items()
            if key not in ("data_type", "dp-unit", "patient_column", "n_units")
        }
        flower_cfg["data-kind"] = "tabular"
        flower_cfg["min-train-nodes"] = 2
        return manifest, flower_cfg

    def test_job_pin_mismatch_fails_before_any_staged_frame_read(self):
        contract = resampling.cross_validation_contract(3, "row")
        manifest, flower_cfg = self._execution_fixture(contract)
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            flower_cfg["cv-job-sha256"] = "c" * 64
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=flower_cfg)
            with (mock.patch.object(
                      task, "_read_staged_frame",
                      side_effect=AssertionError("private read reached")),
                  self.assertRaisesRegex(ValueError, "manifest pin")):
                task.load_pinned_run_config(context)

    def test_every_cv_execution_pin_is_exact_before_private_read(self):
        contract = resampling.cross_validation_contract(3, "row")
        manifest, flower_cfg = self._execution_fixture(contract)
        mutations = {
            "model": ("model-spec-b64", "W10="),
            # BCE and MSE both build a one-output head here; geometry equality
            # must not let a different loss pass under the same CV-job pin.
            "loss-same-geometry": ("loss-name", "mse"),
            "classes": ("num-classes", 3),
            "labels": ("num-labels", 3),
            "features": ("num-features", 3),
            "epochs": ("local-epochs", 2),
            "batch": ("batch-size", 16),
            "task": ("task-type", "regression"),
            "data-kind": ("data-kind", "image"),
            "rounds": ("num-server-rounds", 3),
            "learning-rate": ("learning-rate", 0.02),
            "regularization": ("weight-decay", 0.1),
            "optimizer": ("optimizer-name", "adam"),
            "optimizer-param": ("optimizer-beta1", 0.8),
            "scheduler": ("scheduler-name", "exponential"),
            "scheduler-param": ("scheduler-gamma", 0.25),
        }
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": root},
                run_config=dict(flower_cfg))
            pinned = task.load_pinned_run_config(context)
            self.assertEqual(pinned["loss-name"], "bce_logits")

            with mock.patch.object(
                    task, "_read_staged_frame",
                    side_effect=AssertionError("private read reached")):
                for group, (key, value) in mutations.items():
                    with self.subTest(group=group):
                        context.run_config = dict(flower_cfg)
                        context.run_config[key] = value
                        with self.assertRaisesRegex(ValueError, "manifest pin"):
                            task.load_pinned_run_config(context)

                for key in task._cv_execution_contract(manifest):
                    with self.subTest(missing=key):
                        context.run_config = dict(flower_cfg)
                        context.run_config.pop(key)
                        with self.assertRaisesRegex(ValueError, "manifest pin"):
                            task.load_pinned_run_config(context)

                context.run_config = dict(flower_cfg)
                context.run_config["huber-delta"] = 1.0
                with self.assertRaisesRegex(ValueError, "manifest pin"):
                    task.load_pinned_run_config(context)

    def test_train_partition_is_the_exact_complement_of_oof(self):
        X = np.arange(24, dtype=np.float32).reshape(12, 2)
        y = np.arange(12, dtype=np.float32)
        assigned = np.asarray([1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3])
        with mock.patch.object(
                client_app.resampling, "cross_validation_folds_from_context",
                return_value=assigned):
            train = client_app._cross_validation_partition(
                None, X, y, None, fold=2, subset="train")
            test = client_app._cross_validation_partition(
                None, X, y, None, fold=2, subset="test")
        np.testing.assert_array_equal(train[0], X[assigned != 2])
        np.testing.assert_array_equal(test[0], X[assigned == 2])
        self.assertEqual(len(train[1]) + len(test[1]), len(y))
        self.assertEqual(set(train[1]).intersection(set(test[1])), set())

    def test_cv_training_keeps_one_fold_horizon_and_excludes_oof(self):
        import torch

        X = np.arange(18, dtype=np.float32).reshape(9, 2)
        y = np.asarray([0, 1, 0, 1, 0, 1, 0, 1, 0], dtype=np.float32)
        assigned = np.asarray([1, 2, 3, 1, 2, 3, 1, 2, 3])
        pins = {
            "loss_name": "bce_logits", "n_classes": 2,
            "num_rounds": 2, "round_index": 1, "fold_index": 2,
            "batch_size": 2, "local_epochs": 1,
        }
        pcfg = {
            "epsilon": 0.2, "delta": 1e-6, "clipping_norm": 1.0,
        }
        captured = {}

        def fake_fit(_model, values, target, pcfg, inner_pins, n_staged,
                     _cfg, master, noise_multiplier, geometry_n_units=None):
            captured.update(X=values.copy(), y=target.copy(), pcfg=dict(pcfg),
                            pins=dict(inner_pins), n_staged=n_staged,
                            master=master, noise_multiplier=noise_multiplier,
                            geometry_n_units=geometry_n_units)
            return [np.asarray([1.0])], len(target)

        with (mock.patch.object(
                  client_app, "load_data", return_value=(X, y, None)),
              mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": 9}),
              mock.patch.object(client_app.task_module,
                                "assert_pinned_unit_count"),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(
                  client_app.resampling, "cross_validation_folds_from_context",
                  return_value=assigned),
              mock.patch.object(client_app, "_neural_seed_contract",
                                return_value=({}, {})) as seed_contract,
              mock.patch.object(client_app.dp_harness,
                                "effective_dpsgd_mechanism",
                                return_value={"noise_multiplier": 1.0}),
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"fold-master"),
              mock.patch.object(client_app, "_dp_fit", side_effect=fake_fit)):
            client_app._train_neural(
                None, {"cv-contract-sha256": "a" * 64},
                pcfg, pins,
                torch.nn.Linear(2, 1), input_dim=2, manifest_image=False,
                cv_fold=2)

        np.testing.assert_array_equal(captured["X"], X[assigned != 2])
        self.assertEqual(captured["pins"]["num_rounds"], 2)
        self.assertEqual(captured["pins"]["fold_index"], 2)
        self.assertEqual(captured["pcfg"]["epsilon"], 0.2)
        self.assertEqual(captured["n_staged"], len(y))
        self.assertEqual(captured["geometry_n_units"], len(y))
        seed_contract.assert_called_once_with(
            {"cv-contract-sha256": "a" * 64},
            pins, pcfg, geometry_n_units=len(y))

    def test_patient_replacement_changes_fold_size_not_training_geometry(self):
        import torch

        X = np.arange(16, dtype=np.float32).reshape(8, 2)
        y = np.asarray([0, 0, 1, 1, 0, 0, 1, 1], dtype=np.float32)
        rosters = (
            np.asarray(["a", "a", "b", "b", "c", "c", "d", "d"]),
            np.asarray(["a", "a", "b", "b", "c", "c", "e", "e"]),
        )
        captured = []

        def fake_fit(_model, values, target, _pcfg, _pins, _n_staged,
                     _cfg, master, noise_multiplier, geometry_n_units=None):
            captured.append((
                len(np.unique(target)), len(target), geometry_n_units, master,
                noise_multiplier))
            return [np.asarray([1.0])], len(target)

        with (mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": 4}),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(
                  client_app.resampling,
                  "cross_validation_folds_from_context",
                  side_effect=lambda context, n_rows, unit_ids:
                  np.where(np.isin(unit_ids, ["c", "e"]), 2, 1)),
              mock.patch.object(client_app, "_neural_seed_contract",
                                return_value=({}, {})),
              mock.patch.object(client_app.dp_harness,
                                "effective_dpsgd_mechanism",
                                return_value={"noise_multiplier": 1.0}),
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"\x41" * 32),
              mock.patch.object(client_app, "_dp_fit", side_effect=fake_fit)):
            for roster in rosters:
                with mock.patch.object(
                        client_app, "load_data", return_value=(X, y, roster)):
                    client_app._train_neural(
                        None, {"cv-contract-sha256": "a" * 64},
                        {"epsilon": 1.0, "delta": 1e-5,
                         "clipping_norm": 1.0},
                        {"loss_name": "bce_logits", "n_classes": 2,
                         "round_index": 1, "batch_size": 2,
                         "local_epochs": 1, "num_rounds": 1},
                        torch.nn.Linear(2, 1), input_dim=2,
                        manifest_image=False, cv_fold=2)

        self.assertEqual([item[1] for item in captured], [3, 2])
        self.assertEqual([item[2] for item in captured], [4, 4])

    def test_accumulate_has_no_release_or_custodial_prf(self):
        X = np.arange(12, dtype=np.float32).reshape(6, 2)
        y = np.asarray([0, 1, 0, 1, 0, 1], dtype=np.float32)
        assigned = np.asarray([1, 2, 3, 1, 2, 3])
        captured = {}
        cfg = {"loss-name": "bce_logits", "task-type": "classification",
               "num-classes": 2, "cv-validation-bins": 4}
        with (mock.patch.object(client_app, "is_image_run", return_value=False),
              mock.patch.object(
                  client_app, "load_data", return_value=(X, y, None)),
              mock.patch.object(client_app.task_module,
                                "assert_pinned_unit_count"),
              mock.patch.object(
                  client_app.resampling, "cross_validation_folds_from_context",
                  return_value=assigned),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(validation, "neural_predictions",
                                side_effect=lambda model, values, loss:
                                np.full(len(values), 0.75)),
              mock.patch.object(client_app, "_store_cv_sufficient",
                                side_effect=lambda context, fold, raw, layout:
                                captured.update(fold=fold, raw=raw.copy())),
              mock.patch.object(validation, "private_sufficient_vector",
                                side_effect=AssertionError("release called")),
              mock.patch.object(client_app.seeding, "master_seed",
                                side_effect=AssertionError("PRF called"))):
            ack = client_app._cross_validation_neural_accumulate(
                None, cfg, {"loss_name": "bce_logits"}, object(), 2, 2)
        self.assertEqual(captured["fold"], 2)
        self.assertEqual(captured["raw"].shape, (8,))
        np.testing.assert_array_equal(ack[0], np.zeros(1))

    def test_context_state_round_trips_between_tasks_and_release_consumes_it(self):
        with tempfile.TemporaryDirectory() as root:
            contract = resampling.cross_validation_contract(3, "row")
            manifest = {
                "run_token": "run_" + "c" * 32,
                "dp-unit": "row", "patient_column": None,
                "cv-job-sha256": "d" * 64,
                **resampling.cross_validation_manifest_fields(contract),
            }
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = Context(
                run_id=1, node_id=2,
                node_config={"manifest-dir": root}, state=RecordDict(),
                run_config={})
            layout = validation.validation_layout(
                "classification", n_classes=2, bins=4)
            vectors = [np.full(layout["size"], value, dtype=np.float64)
                       for value in (1.0, 2.0, 3.0)]

            def node_state_round_trip(state):
                rebuilt = RecordDict()
                for key, record in state.items():
                    if isinstance(record, ConfigRecord):
                        rebuilt[key] = ConfigRecord(dict(record))
                    elif isinstance(record, ArrayRecord):
                        rebuilt[key] = ArrayRecord(numpy_ndarrays=[
                            value.copy()
                            for value in record.to_numpy_ndarrays()])
                    else:
                        self.fail("unexpected record in CV node state")
                return rebuilt

            for fold, raw in enumerate(vectors, 1):
                client_app._store_cv_sufficient(context, fold, raw, layout)
                # Flower hands a fresh Context to each isolated task while its
                # in-memory NodeState carries the RecordDict forward.
                context = Context(
                    run_id=1, node_id=2,
                    node_config={"manifest-dir": root},
                    state=node_state_round_trip(context.state), run_config={})
            # Exact retry is idempotent, not another contribution.
            client_app._store_cv_sufficient(context, 2, vectors[1], layout)
            total = client_app._load_complete_cv_sufficient(context, layout)
            np.testing.assert_array_equal(total, np.full(layout["size"], 6.0))
            with self.assertRaisesRegex(RuntimeError, "replay changed"):
                client_app._store_cv_sufficient(
                    context, 2, np.full(layout["size"], 9.0), layout)
            with mock.patch.object(
                    validation, "private_sufficient_vector",
                    side_effect=lambda raw, *args, **kwargs: (raw, 1.0)):
                released = client_app._cross_validation_release(
                    context,
                    {"loss-name": "bce_logits", "task-type": "classification",
                     "num-classes": 2, "cv-validation-bins": 4},
                    {"epsilon": 1.0, "delta": 1e-6})
            np.testing.assert_array_equal(released[0], total)
            self.assertNotIn(client_app._CV_OOF_META_KEY, context.state)
            self.assertNotIn(client_app._CV_OOF_TOTAL_KEY, context.state)
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                client_app._load_complete_cv_sufficient(context, layout)

    def test_context_state_rejects_order_and_tampering_and_abort_purges(self):
        with tempfile.TemporaryDirectory() as root:
            contract = resampling.cross_validation_contract(3, "row")
            manifest = {
                "dp-unit": "row", "patient_column": None,
                "cv-job-sha256": "e" * 64,
                **resampling.cross_validation_manifest_fields(contract),
            }
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            layout = validation.validation_layout(
                "classification", n_classes=2, bins=4)
            raw = np.ones(layout["size"], dtype=np.float64)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, state=RecordDict())
            with self.assertRaisesRegex(RuntimeError, "in order"):
                client_app._store_cv_sufficient(context, 2, raw, layout)
            client_app._store_cv_sufficient(context, 1, raw, layout)

            tampered_total = context.state.copy()
            tampered_total[client_app._CV_OOF_TOTAL_KEY] = ArrayRecord(
                numpy_ndarrays=[np.full(layout["size"], 9.0)])
            with self.assertRaisesRegex(RuntimeError, "digest changed"):
                client_app._store_cv_sufficient(SimpleNamespace(
                    node_config={"manifest-dir": root},
                    state=tampered_total), 2, raw, layout)

            tampered_meta = context.state.copy()
            meta = dict(tampered_meta[client_app._CV_OOF_META_KEY])
            meta["cv-job-sha256"] = "f" * 64
            tampered_meta[client_app._CV_OOF_META_KEY] = ConfigRecord(meta)
            with self.assertRaisesRegex(RuntimeError, "binding changed"):
                client_app._store_cv_sufficient(SimpleNamespace(
                    node_config={"manifest-dir": root},
                    state=tampered_meta), 2, raw, layout)

            client_app._forget_cv_sufficient(context)
            self.assertEqual(list(context.state.keys()), [])

            context.state[client_app._CV_OOF_META_KEY] = ConfigRecord(
                {"placeholder": "public"})
            context.state[client_app._CV_OOF_TOTAL_KEY] = ArrayRecord(
                numpy_ndarrays=[np.zeros(1)])
            with (mock.patch.object(
                      release_guard, "claim_release", return_value={
                          "status": "new", "operation": "cv-abort",
                          "epsilon": 0.0, "delta": 0.0}),
                  mock.patch.object(client_app, "load_pinned_run_config",
                                    return_value={}),
                  mock.patch.object(client_app, "load_dp_track",
                                    return_value="neural"),
                  mock.patch.object(client_app, "load_privacy_config",
                                    return_value={}),
                  mock.patch.object(client_app.dp_harness, "resolve_dp_track",
                                    return_value="neural"),
                  mock.patch.object(client_app, "_reply", return_value="reply")):
                self.assertEqual(client_app.train(object(), context), "reply")
            self.assertEqual(list(context.state.keys()), [])

    def test_cv_replies_are_not_persisted_but_cache_identity_includes_fold(self):
        self.assertFalse(client_app._reply_cache_allowed({
            "operation": "cv-train"}))
        self.assertFalse(client_app._reply_cache_allowed({
            "operation": "cv-accumulate"}))
        self.assertFalse(client_app._reply_cache_allowed({
            "operation": "cv-release"}))
        self.assertTrue(client_app._reply_cache_allowed({"operation": "train"}))
        context = SimpleNamespace(state=RecordDict())
        claim = {"message_id": "m", "release_index": 1,
                 "operation": "cv-train", "fold": 3,
                 "request_id": "r"}
        client_app._cache_reply(context, claim, [np.asarray([1.0])])
        meta = context.state["dsflower-last-release-meta"]
        self.assertEqual(meta["fold"], 3)


class CvServerTests(unittest.TestCase):
    @staticmethod
    def _cfg(results_dir=None):
        return {
            "cv-contract-sha256": "a" * 64,
            "cv-job-sha256": "b" * 64,
            "cv-folds": 3,
            "cv-n-nodes": 2,
            "cv-validation-bins": 4,
            "data-kind": "tabular",
            "loss-name": "bce_logits",
            "task-type": "classification",
            "num-classes": 2,
            "num-labels": 2,
            "num-features": 2,
            "num-server-rounds": 2,
            "min-train-nodes": 2,
            **({"results-dir": results_dir} if results_dir else {}),
        }

    def test_public_fold_initialization_is_deterministic_and_clean(self):
        import torch

        cfg = self._cfg()

        def random_model(_cfg):
            return torch.nn.Linear(2, 1)

        with mock.patch.object(server_app, "_build_initial_model",
                               side_effect=random_model):
            _m1, first = server_app._cross_validation_initial_arrays(cfg, 2)
            torch.manual_seed(999)
            _m2, replay = server_app._cross_validation_initial_arrays(cfg, 2)
            _m3, other = server_app._cross_validation_initial_arrays(cfg, 3)
        for left, right in zip(first.to_numpy_ndarrays(),
                               replay.to_numpy_ndarrays()):
            np.testing.assert_array_equal(left, right)
        self.assertTrue(any(
            not np.array_equal(left, right)
            for left, right in zip(first.to_numpy_ndarrays(),
                                   other.to_numpy_ndarrays())))

    def test_orchestration_runs_k_clean_trainings_and_persists_no_model(self):
        cfg = self._cfg()
        starts = []
        accumulated = []

        class Grid:
            @staticmethod
            def get_node_ids():
                return [11, 22]

        def strategy_factory(_cfg, _nodes, **kwargs):
            fold = kwargs["fold"]
            result = SimpleNamespace(arrays=ArrayRecord(
                numpy_ndarrays=[np.asarray([float(fold)])]))
            strategy = SimpleNamespace(
                available_rounds={1, 2},
                start=lambda **call: (starts.append((fold, call["num_rounds"]))
                                      or result))
            return strategy

        layout = validation.cross_validation_layout_from_config(cfg)
        metrics = validation.validation_metrics(
            np.ones(layout["size"]), layout)
        with (mock.patch.object(
                  server_app, "_cross_validation_initial_arrays",
                  side_effect=lambda _cfg, fold: (
                      object(), ArrayRecord(numpy_ndarrays=[np.asarray([fold])]))),
              mock.patch.object(server_app, "_build_strategy",
                                side_effect=strategy_factory),
              mock.patch.object(
                  server_app, "_cross_validation_accumulate",
                  side_effect=lambda grid, inner_cfg, roster, fold, arrays:
                  accumulated.append(fold)),
              mock.patch.object(
                  server_app, "_cross_validation_release",
                  return_value=(layout, metrics)),
              mock.patch.object(server_app, "_save_cross_validation") as save,
              mock.patch.object(server_app, "_save_results") as save_model):
            server_app._run_cross_validation(Grid(), cfg, "neural")
        self.assertEqual(starts, [(1, 2), (2, 2), (3, 2)])
        self.assertEqual(accumulated, [1, 2, 3])
        save.assert_called_once()
        save_model.assert_not_called()

    def test_failed_fold_aborts_and_publishes_nothing(self):
        cfg = self._cfg()

        class Grid:
            @staticmethod
            def get_node_ids():
                return [11, 22]

        strategy = SimpleNamespace(
            available_rounds=set(),
            start=lambda **call: SimpleNamespace(arrays=ArrayRecord(
                numpy_ndarrays=[np.asarray([0.0])])) )
        with (mock.patch.object(
                  server_app, "_cross_validation_initial_arrays",
                  return_value=(object(), ArrayRecord(
                      numpy_ndarrays=[np.asarray([0.0])]))),
              mock.patch.object(server_app, "_build_strategy",
                                return_value=strategy),
              mock.patch.object(server_app, "_abort_cross_validation") as abort,
              mock.patch.object(server_app, "_save_cross_validation") as save,
              self.assertRaisesRegex(RuntimeError, "every round")):
            server_app._run_cross_validation(Grid(), cfg, "neural")
        abort.assert_called_once()
        save.assert_not_called()

    def test_atomic_output_is_cv_json_only_with_finite_plausible_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            cfg = self._cfg(root)
            layout = validation.cross_validation_layout_from_config(cfg)
            released = np.ones(layout["size"], dtype=np.float64)
            metrics = validation.validation_metrics(released, layout)
            server_app._save_cross_validation(cfg, layout, metrics, folds=3)
            self.assertEqual(os.listdir(root), ["cv.json"])
            with open(os.path.join(root, "cv.json"), encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["cv_contract_sha256"], "a" * 64)
            self.assertEqual(payload["cv_job_sha256"], "b" * 64)
            self.assertNotIn("per_node", payload)
            self.assertNotIn("predictions", payload)
            self.assertTrue(np.isfinite(payload["metrics"]["accuracy"]))
            self.assertGreaterEqual(payload["metrics"]["accuracy"], 0.0)
            self.assertLessEqual(payload["metrics"]["accuracy"], 1.0)

    def test_noise_only_nullable_primary_is_persisted_for_valid_tasks(self):
        cases = (
            ("cross_entropy", "classification", 3, "accuracy"),
            ("ordinal", "ordinal", 3, "accuracy"),
            ("multilabel_bce", "multilabel", 2, "macro_f1"),
        )
        for loss, task, width, primary in cases:
            with self.subTest(task=task), tempfile.TemporaryDirectory() as root:
                cfg = self._cfg(root)
                cfg.update({
                    "loss-name": loss, "task-type": task,
                    "num-classes": width, "num-labels": width,
                })
                layout = validation.cross_validation_layout_from_config(cfg)
                metrics = validation.validation_metrics(
                    np.zeros(layout["size"], dtype=np.float64), layout)
                self.assertIsNone(metrics[primary])

                server_app._save_cross_validation(
                    cfg, layout, metrics, folds=3)
                with open(os.path.join(root, "cv.json"),
                          encoding="utf-8") as handle:
                    payload = json.load(handle)
                self.assertIsNone(payload["metrics"][primary])

    def test_output_rejects_nonplausible_primary_or_fold_transcript(self):
        layout = validation.cross_validation_layout_from_config(self._cfg())
        for metrics in (
                {"accuracy": None}, {"accuracy": float("inf")},
                {"accuracy": -0.1}, {"accuracy": 1.1},
                {"accuracy": 0.5, "per_fold": [0.4, 0.6]}):
            with tempfile.TemporaryDirectory() as root:
                with self.assertRaisesRegex(RuntimeError, "pooled-only"):
                    server_app._save_cross_validation(
                        self._cfg(root), layout, metrics, folds=3)
                self.assertEqual(os.listdir(root), [])

    def test_atomic_save_failure_leaves_no_partial_transcript(self):
        with tempfile.TemporaryDirectory() as root:
            cfg = self._cfg(root)
            layout = validation.cross_validation_layout_from_config(cfg)
            with (mock.patch.object(server_app.os, "replace",
                                    side_effect=OSError("forced save crash")),
                  self.assertRaisesRegex(OSError, "save crash")):
                server_app._save_cross_validation(
                    cfg, layout, {"accuracy": 0.5}, folds=3)
            self.assertEqual(os.listdir(root), [])


if __name__ == "__main__":
    unittest.main()
