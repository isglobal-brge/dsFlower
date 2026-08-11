"""Atomic neural holdout and engine-agnostic partition regressions."""

import json
import os
import stat
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
from flwr.common import ArrayRecord, ConfigRecord, Message, MetricRecord, RecordDict


FLOWER_APP = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "flower_app"))
if FLOWER_APP not in sys.path:
    sys.path.insert(0, FLOWER_APP)

from dsflower_runner import (client_app, dp_harness, release_guard, resampling,
                             server_app, validation, vision)  # noqa: E402


class PartitionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.secret = os.path.join(self.tmp.name, "node-secret")
        with open(self.secret, "w", encoding="ascii") as handle:
            handle.write("42" * 32)
        os.chmod(self.secret, stat.S_IRUSR | stat.S_IWUSR)
        self.secret_env = mock.patch.dict(
            os.environ, {"DSFLOWER_NODE_SECRET_FILE": self.secret})
        self.secret_env.start()

    def tearDown(self):
        self.secret_env.stop()
        self.tmp.cleanup()

    @staticmethod
    def contract(unit="patient", numerator=200000):
        return resampling.holdout_contract(numerator, unit)

    def test_patient_assignment_is_sticky_per_unit_and_order_independent(self):
        contract = self.contract()
        ids = np.asarray(["p3", "p1", "p2", "p1", "p3"])
        first = resampling.holdout_mask(
            contract, n_rows=len(ids), unit_ids=ids)
        order = np.asarray([3, 4, 2, 0, 1])
        permuted = resampling.holdout_mask(
            contract, n_rows=len(ids), unit_ids=ids[order])

        by_id = {unit: bool(first[index]) for index, unit in enumerate(ids)}
        self.assertTrue(all(bool(permuted[index]) == by_id[unit]
                            for index, unit in enumerate(ids[order])))
        self.assertEqual(bool(first[0]), bool(first[4]))
        self.assertEqual(bool(first[1]), bool(first[3]))
        for unit in np.unique(ids):
            self.assertEqual(len(set(first[ids == unit].tolist())), 1)
        self.assertTrue(set(ids[first]).isdisjoint(set(ids[~first])))
        np.testing.assert_array_equal(
            first, resampling.holdout_mask(
                contract, n_rows=len(ids), unit_ids=ids))

    def test_row_assignment_depends_only_on_ordinal_and_contract(self):
        contract = self.contract(unit="row", numerator=500000)
        first = resampling.holdout_mask(contract, n_rows=128)
        replay = resampling.holdout_mask(contract, n_rows=128)
        changed_values = np.linspace(-1e9, 1e9, 128)  # never an input axis
        del changed_values
        np.testing.assert_array_equal(first, replay)
        self.assertTrue(bool(np.any(first)))
        self.assertTrue(bool(np.any(~first)))

    def test_fraction_changes_are_nested_not_partition_rerolls(self):
        small = resampling.holdout_mask(
            self.contract(unit="row", numerator=200000), n_rows=4096)
        large = resampling.holdout_mask(
            self.contract(unit="row", numerator=300000), n_rows=4096)
        self.assertTrue(bool(np.all(~small | large)))
        self.assertGreater(int(np.sum(large)), int(np.sum(small)))

    def test_partition_opens_the_custodial_secret_once_not_per_row(self):
        original = resampling.seeding._node_secret
        with mock.patch.object(
                resampling.seeding, "_node_secret", wraps=original) as read_secret:
            resampling.holdout_mask(
                self.contract(unit="row", numerator=200000), n_rows=4096)
        self.assertEqual(read_secret.call_count, 1)

    def test_contract_rejects_seed_fields_and_hash_drift(self):
        contract = self.contract()
        self.assertEqual(
            contract["sha256"],
            "00b0a490eb3d92fec7ce532e452523a32cbf73d19953372194faffc21eb4c75b")
        with self.assertRaisesRegex(ValueError, "field|seed"):
            resampling.validate_holdout_contract({**contract, "seed": 7})
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            resampling.validate_holdout_contract({**contract, "sha256": "0" * 64})


class ReleaseGuardHoldoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        contract = resampling.holdout_contract(200000, "row")
        self.manifest = {
            "run_token": "run_" + "a" * 32,
            "privacy-adjacency": "replace_one",
            "privacy-policy-sha256": "1" * 64,
            "privacy-epsilon": 2.0,
            "privacy-delta": 1e-5,
            "privacy-training-epsilon": 1.6,
            "privacy-training-delta": 8e-6,
            "privacy-holdout-epsilon": 2.0 - 2.0 * 0.8,
            "privacy-holdout-delta": 1e-5 - 1e-5 * 0.8,
            "num-server-rounds": 2,
            "dp-unit": "row",
            "patient_column": None,
            **resampling.manifest_fields(contract),
        }
        with open(os.path.join(self.tmp.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(self.manifest, handle)
        self.context = SimpleNamespace(
            node_config={"manifest-dir": self.tmp.name}, state=RecordDict())

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def message(operation="train", server_round=1, message_id="m"):
        return SimpleNamespace(
            metadata=SimpleNamespace(message_id=message_id, group_id="g"),
            content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
                "config": ConfigRecord({
                    "server-round": server_round,
                    "dsflower-operation": operation,
                }),
            }))

    def test_job_budget_is_split_by_fixed_operation_not_history(self):
        train = release_guard.claim_release(
            self.context, self.message("train", 1))
        evaluate = release_guard.claim_release(
            self.context, self.message("holdout-evaluate", 2))
        self.assertEqual((train["epsilon"], train["delta"]), (1.6, 8e-6))
        self.assertEqual(
            (evaluate["epsilon"], evaluate["delta"]),
            (2.0 - 2.0 * 0.8, 1e-5 - 1e-5 * 0.8))
        self.assertEqual(evaluate["operation"], "holdout-evaluate")

    def test_train_and_holdout_cache_identities_do_not_collide(self):
        train_msg = self.message("train", 2, message_id="train-message")
        train = release_guard.claim_release(self.context, train_msg)
        client_app._cache_reply(
            self.context, train, [np.asarray([1.0])])

        evaluate_msg = self.message(
            "holdout-evaluate", 2, message_id="holdout-message")
        evaluate = release_guard.claim_release(self.context, evaluate_msg)
        self.assertEqual(evaluate["status"], "new")
        self.assertNotEqual(evaluate["request_id"], train["request_id"])
        client_app._cache_reply(
            self.context, evaluate, [np.asarray([2.0])])
        replay = release_guard.claim_release(self.context, evaluate_msg)
        self.assertEqual(replay["status"], "replay")

    def test_holdout_operation_requires_manifest_contract(self):
        self.manifest.pop("resampling-contract-sha256")
        with open(os.path.join(self.tmp.name, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(self.manifest, handle)
        with self.assertRaisesRegex(RuntimeError, "holdout contract"):
            release_guard.claim_release(
                self.context, self.message("holdout-evaluate", 2))


class NeuralHoldoutTests(unittest.TestCase):
    def test_training_receives_only_complement_and_evaluation_only_test(self):
        X = np.arange(12, dtype=np.float32).reshape(6, 2)
        y = np.asarray([0, 1, 0, 1, 0, 1], dtype=np.float32)
        mask = np.asarray([False, True, False, True, False, True])
        pins = {"loss_name": "bce_logits", "n_classes": 2}
        cfg = {"loss-name": "bce_logits", "task-type": "classification",
               "num-classes": 2, "holdout-validation-bins": 8,
               "resampling-privacy-unit": "patient"}
        patient_ids = np.asarray(["a", "b", "c", "d", "e", "f"])

        with mock.patch.object(
                client_app.resampling, "holdout_mask_from_context",
                return_value=mask):
            train = client_app._holdout_partition(
                None, X, y, None, subset="train")
            test = client_app._holdout_partition(
                None, X, y, None, subset="test")

        np.testing.assert_array_equal(train[0], X[~mask])
        np.testing.assert_array_equal(train[1], y[~mask])
        np.testing.assert_array_equal(test[0], X[mask])
        np.testing.assert_array_equal(test[1], y[mask])
        self.assertIsNone(train[2])

        model = object()
        captured = {}
        with (mock.patch.object(
                  client_app, "load_data", return_value=(X, y, patient_ids)),
              mock.patch.object(client_app.task_module, "assert_pinned_unit_count"),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(client_app, "is_image_run", return_value=False),
              mock.patch.object(
                  client_app.resampling, "holdout_mask_from_context",
                  return_value=mask),
              mock.patch.object(validation, "neural_predictions",
                                side_effect=lambda ignored_model, values, loss:
                                np.full(len(values), 0.75)),
              mock.patch.object(
                  validation, "private_validation_vector",
                  side_effect=lambda yy, predictions, layout, **kwargs:
                  (captured.update(y=yy.copy(), predictions=predictions.copy(),
                                   include_zero_neighbor=kwargs.get(
                                       "include_zero_neighbor"))
                   or (np.ones(layout["size"]), 1.0)))):
            released = client_app._holdout_neural_release(
                None, cfg, {"epsilon": 0.4, "delta": 2e-6}, pins,
                model, input_dim=2)

        np.testing.assert_array_equal(captured["y"], y[mask])
        self.assertEqual(len(captured["predictions"]), int(mask.sum()))
        self.assertIs(captured["include_zero_neighbor"], True)
        self.assertEqual(len(released), 1)

    def test_empty_train_and_test_sides_are_shape_preserving(self):
        X = np.arange(8, dtype=np.float32).reshape(4, 2)
        y = np.asarray([0, 1, 0, 1], dtype=np.float32)
        unit_ids = np.asarray(["a", "b", "c", "d"])
        cases = (("train", np.ones(4, dtype=bool)),
                 ("test", np.zeros(4, dtype=bool)))
        for subset, mask in cases:
            with (mock.patch.object(
                    client_app.resampling, "holdout_mask_from_context",
                    return_value=mask)):
                empty = client_app._holdout_partition(
                    None, X, y, unit_ids, subset=subset)
            self.assertEqual(empty[0].shape, (0, 2))
            self.assertEqual(empty[1].shape, (0,))
            self.assertEqual(empty[2].shape, (0,))

    def test_empty_train_side_runs_the_same_noise_schedule_without_decode(self):
        import torch

        X = np.arange(8, dtype=np.float32).reshape(4, 2)
        y = np.asarray([0, 1, 0, 1], dtype=np.int64)
        paths = np.asarray(["p0", "p1", "p2", "p3"])
        pins = {
            "loss_name": "cross_entropy", "n_classes": 2,
            "round_index": 1, "batch_size": 2, "local_epochs": 1,
            "num_rounds": 1,
        }
        pcfg = {
            "epsilon": 1.0, "delta": 1e-5, "clipping_norm": 1.0,
            "n_samples": len(y),
        }
        cases = (
            (False, None),
            (True, np.asarray(["a", "a", "b", "b"])),
        )
        for manifest_image, groups in cases:
            with self.subTest(
                    manifest_image=manifest_image,
                    unit="patient" if groups is not None else "row"):
                cfg = {"resampling-contract-sha256": "a" * 64}
                if manifest_image:
                    cfg.update({
                        "backbone": "resnet18",
                        "vision-extractor-profile": "profile",
                        "num-features": 2, "image-size": 32,
                    })
                model = torch.nn.Linear(2, 2)
                expected = [value.copy() for value in
                            client_app.get_torch_params(model)]
                callback = mock.Mock()
                captured = {}

                def noise_only_fit(
                        _model, values, target, _pcfg, _pins, _n_staged,
                        _cfg, master, noise_multiplier,
                        geometry_n_units=None, public_zero_gradient=False):
                    captured.update(
                        X=values.copy(), y=target.copy(), master=master,
                        noise_multiplier=noise_multiplier,
                        geometry_n_units=geometry_n_units,
                        public_zero_gradient=public_zero_gradient)
                    return [np.full_like(value, 7.0) for value in expected], 1

                n_units = len(y) if groups is None else len(np.unique(groups))
                with (mock.patch.object(
                          client_app.task_module, "_load_manifest",
                          return_value={"n_units": n_units}),
                      mock.patch.object(
                          client_app.task_module, "assert_pinned_unit_count"),
                      mock.patch.object(
                          client_app.resampling, "holdout_mask_from_context",
                          return_value=np.ones(len(y), dtype=bool)),
                      mock.patch.object(
                          client_app, "load_data", return_value=(X, y, groups)),
                      mock.patch.object(
                          client_app, "load_image_collection",
                          return_value=(paths, y, groups)),
                      mock.patch.object(
                          vision, "prepare_backbone",
                          return_value=(object(), 32, False, "cpu")),
                      mock.patch.object(
                          vision, "extract_features_from_paths",
                          side_effect=AssertionError("empty side decoded")) as extract,
                      mock.patch.object(
                          client_app, "_neural_seed_contract",
                          return_value=({}, {})),
                      mock.patch.object(
                          client_app.dp_harness, "effective_dpsgd_mechanism",
                          return_value={"noise_multiplier": 1.75}) as mechanism,
                      mock.patch.object(
                          client_app.seeding, "master_seed",
                          return_value=b"\x3d" * 32) as seed,
                      mock.patch.object(client_app, "_dp_fit",
                                        side_effect=noise_only_fit) as fit):
                    arrays, n_examples = client_app._train_neural(
                        None, cfg, pcfg, pins, model, input_dim=2,
                        manifest_image=manifest_image,
                        on_private_start=callback)

                self.assertEqual(n_examples, 1)
                self.assertTrue(all(bool(np.all(value == 7.0))
                                    for value in arrays))
                fit.assert_called_once()
                seed.assert_called_once()
                mechanism.assert_called_once()
                self.assertEqual(captured["X"].shape, (1, 2))
                self.assertEqual(captured["y"].shape, (1,))
                np.testing.assert_array_equal(captured["X"], np.zeros((1, 2)))
                np.testing.assert_array_equal(captured["y"], np.zeros(1))
                self.assertEqual(captured["noise_multiplier"], 1.75)
                self.assertEqual(captured["geometry_n_units"], n_units)
                self.assertTrue(captured["public_zero_gradient"])
                extract.assert_not_called()
                self.assertEqual(callback.call_count, int(manifest_image))

    def test_empty_test_side_gets_one_noise_only_release_without_decode(self):
        import torch

        X = np.arange(8, dtype=np.float32).reshape(4, 2)
        y = np.asarray([0, 1, 0, 1], dtype=np.int64)
        paths = np.asarray(["p0", "p1", "p2", "p3"])
        for manifest_image, groups in (
                (False, None),
                (True, np.asarray(["a", "a", "b", "b"]))):
            with self.subTest(manifest_image=manifest_image):
                cfg = {
                    "loss-name": "cross_entropy",
                    "task-type": "classification", "num-classes": 2,
                    "holdout-validation-bins": 8,
                    "resampling-privacy-unit": (
                        "patient" if groups is not None else "row"),
                    "backbone": "resnet18",
                    "vision-extractor-profile": "profile",
                    "num-features": 2, "image-size": 32,
                }
                callback = mock.Mock()
                with (mock.patch.object(
                          client_app, "is_image_run",
                          return_value=manifest_image),
                      mock.patch.object(
                          client_app, "load_data", return_value=(X, y, groups)),
                      mock.patch.object(
                          client_app, "load_image_collection",
                          return_value=(paths, y, groups)),
                      mock.patch.object(
                          client_app.task_module, "assert_pinned_unit_count"),
                      mock.patch.object(
                          client_app.resampling, "holdout_mask_from_context",
                          return_value=np.zeros(len(y), dtype=bool)),
                      mock.patch.object(
                          vision, "prepare_backbone",
                          return_value=(object(), 32, False, "cpu")),
                      mock.patch.object(
                          vision, "extract_features_from_paths",
                          side_effect=AssertionError("empty side decoded")) as extract,
                      mock.patch.object(
                          dp_harness, "compute_output_sigma", return_value=1.0),
                      mock.patch.object(
                          client_app.seeding, "master_seed",
                          return_value=b"\x5a" * 32),
                      mock.patch.object(
                          validation, "private_validation_vector",
                          wraps=validation.private_validation_vector) as release):
                    arrays = client_app._holdout_neural_release(
                        None, cfg, {"epsilon": 0.4, "delta": 2e-6},
                        {"loss_name": "cross_entropy"},
                        torch.nn.Linear(2, 2), input_dim=2,
                        on_private_start=callback)

                self.assertEqual(len(arrays), 1)
                self.assertTrue(bool(np.all(np.isfinite(arrays[0]))))
                self.assertFalse(bool(np.all(arrays[0] == 0.0)))
                self.assertEqual(release.call_count, 1)
                self.assertEqual(release.call_args.args[0].shape, (0,))
                self.assertEqual(release.call_args.args[1].shape, (0, 2))
                extract.assert_not_called()
                callback.assert_called_once_with()

    def test_empty_and_one_unit_train_neighbors_share_noise_geometry(self):
        import torch

        X = np.arange(8, dtype=np.float32).reshape(4, 2)
        y = np.asarray([0, 1, 0, 1], dtype=np.float32)
        cfg = {"resampling-contract-sha256": "a" * 64}
        pins = {
            "loss_name": "bce_logits", "n_classes": 2,
            "round_index": 1, "batch_size": 2, "local_epochs": 1,
            "num_rounds": 1,
        }
        pcfg = {"epsilon": 1.0, "delta": 1e-5, "clipping_norm": 1.0}
        captured = []

        def fit(_model, values, _target, _pcfg, _pins, _n_staged,
                _cfg, master, noise_multiplier, geometry_n_units=None,
                public_zero_gradient=False):
            captured.append({
                "rows": len(values), "master": master,
                "noise_multiplier": noise_multiplier,
                "geometry_n_units": geometry_n_units,
                "public_zero_gradient": public_zero_gradient,
            })
            return [np.asarray([1.0])], len(values)

        with (mock.patch.object(client_app, "load_data",
                                return_value=(X, y, None)),
              mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": len(y)}),
              mock.patch.object(client_app.task_module,
                                "assert_pinned_unit_count"),
              mock.patch.object(client_app, "_neural_seed_contract",
                                return_value=({}, {})),
              mock.patch.object(
                  client_app.dp_harness, "effective_dpsgd_mechanism",
                  return_value={"noise_multiplier": 1.5}) as mechanism,
              mock.patch.object(client_app.seeding, "master_seed",
                                side_effect=(b"\x51" * 32, b"\x52" * 32)),
              mock.patch.object(client_app, "_dp_fit", side_effect=fit)):
            for mask in (
                    np.ones(len(y), dtype=bool),
                    np.asarray([True, True, True, False])):
                with mock.patch.object(
                        client_app.resampling, "holdout_mask_from_context",
                        return_value=mask):
                    client_app._train_neural(
                        None, cfg, pcfg, pins, torch.nn.Linear(2, 1),
                        input_dim=2, manifest_image=False)

        self.assertEqual(mechanism.call_count, 2)
        self.assertEqual(
            [call.kwargs for call in mechanism.call_args_list],
            [mechanism.call_args_list[0].kwargs] * 2)
        self.assertEqual(
            [(item["noise_multiplier"], item["geometry_n_units"])
             for item in captured],
            [(1.5, len(y)), (1.5, len(y))])
        self.assertEqual(
            [(item["rows"], item["public_zero_gradient"])
             for item in captured],
            [(1, True), (1, False)])

    def test_neural_training_partitions_before_the_dp_fit(self):
        import torch

        X = np.arange(12, dtype=np.float32).reshape(6, 2)
        y = np.asarray([0, 1, 0, 1, 0, 1], dtype=np.float32)
        mask = np.asarray([False, True, False, True, False, True])
        model = torch.nn.Linear(2, 1)
        cfg = {"resampling-contract-sha256": "a" * 64}
        pins = {
            "loss_name": "bce_logits", "n_classes": 2, "round_index": 1,
            "batch_size": 2, "local_epochs": 1, "num_rounds": 1,
        }
        pcfg = {"epsilon": 1.0, "delta": 1e-5, "clipping_norm": 1.0}
        captured = {}

        def fake_fit(_model, values, target, _pcfg, _pins, n_staged,
                     _cfg, master, noise_multiplier, geometry_n_units=None):
            captured.update(
                X=values.copy(), y=target.copy(), n_staged=n_staged,
                master=master, noise_multiplier=noise_multiplier,
                geometry_n_units=geometry_n_units)
            return [np.asarray([1.0])], len(target)

        with (mock.patch.object(
                  client_app, "load_data", return_value=(X, y, None)),
              mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": 6}),
              mock.patch.object(client_app.task_module, "assert_pinned_unit_count"),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(client_app.resampling,
                                "holdout_mask_from_context",
                                return_value=mask),
              mock.patch.object(client_app, "_neural_seed_contract",
                                return_value=({}, {})) as seed_contract,
              mock.patch.object(client_app.dp_harness,
                                "effective_dpsgd_mechanism",
                                return_value={"noise_multiplier": 1.0}),
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"semantic-master"),
              mock.patch.object(client_app, "_dp_fit", side_effect=fake_fit)):
            client_app._train_neural(
                None, cfg, pcfg, pins, model, input_dim=2,
                manifest_image=False)

        np.testing.assert_array_equal(captured["X"], X[~mask])
        np.testing.assert_array_equal(captured["y"], y[~mask])
        self.assertEqual(captured["n_staged"], len(y))
        self.assertEqual(captured["geometry_n_units"], len(y))
        seed_contract.assert_called_once_with(
            cfg, pins, pcfg, geometry_n_units=len(y))

    def test_patient_replacement_keeps_fixed_dp_sampling_geometry(self):
        import torch

        X = np.arange(16, dtype=np.float32).reshape(8, 2)
        y = np.asarray([0, 0, 1, 1, 0, 0, 1, 1], dtype=np.float32)
        rosters = (
            np.asarray(["a", "a", "b", "b", "c", "c", "d", "d"]),
            np.asarray(["a", "a", "b", "b", "c", "c", "e", "e"]),
        )
        pcfg = {
            "n_samples": 8, "clipping_norm": 1.0,
            "epsilon": 1.6, "delta": 8e-6,
        }
        pins = {
            "loss_name": "bce_logits", "batch_size": 2,
            "local_epochs": 1, "num_rounds": 1, "n_classes": 2,
            "learning_rate": 0.1, "round_index": 1,
            "optimizer": {
                "name": "sgd", "weight_decay": 0.0, "momentum": 0.0,
                "nesterov": False, "l1_penalty": 0.0,
            },
            "scheduler": {"name": "none"},
        }
        cfg = {"resampling-contract-sha256": "a" * 64}
        captured = []
        original = dp_harness.make_private_dpsgd

        def spy_make_private(*args, **kwargs):
            wrapped, optimizer, loader, engine = original(*args, **kwargs)
            captured.append({
                "subset-units": len(loader.dataset),
                "steps": len(loader),
                "q": loader.batch_sampler.sample_rate,
                "noise": optimizer.noise_multiplier,
                "normalizer": optimizer.expected_batch_size,
            })
            return wrapped, optimizer, [], engine

        with (mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": 4}),
              mock.patch.object(client_app, "_apply_feature_bounds",
                                side_effect=lambda values, ignored: values),
              mock.patch.object(
                  client_app.resampling, "holdout_mask_from_context",
                  side_effect=lambda context, n_rows, unit_ids:
                  np.isin(unit_ids, ["c", "e"])),
              mock.patch.object(client_app, "_neural_seed_contract",
                                return_value=({}, {})),
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"\x39" * 32),
              mock.patch.object(dp_harness, "_cached_noise_multiplier",
                                return_value=1.75),
              mock.patch.object(dp_harness, "make_private_dpsgd",
                                side_effect=spy_make_private)):
            for roster in rosters:
                model = torch.nn.Linear(2, 1)
                model._dsflower_release_keys = tuple(
                    name for name, _ in torch.nn.Module.named_parameters(model))
                with mock.patch.object(
                        client_app, "load_data", return_value=(X, y, roster)):
                    client_app._train_neural(
                        None, cfg, pcfg, pins, model, input_dim=2,
                        manifest_image=False)

        self.assertEqual([item["subset-units"] for item in captured], [3, 2])
        self.assertEqual(
            [(item["steps"], item["q"], item["noise"], item["normalizer"])
             for item in captured],
            [(2, 0.5, 1.75, 2), (2, 0.5, 1.75, 2)],
        )

    def test_resampling_unit_pin_fails_before_private_read(self):
        with (mock.patch.object(client_app.task_module, "_load_manifest",
                                return_value={"n_units": "invalid"}),
              mock.patch.object(client_app, "load_data") as load,
              self.assertRaisesRegex(ValueError, "privacy-unit count")):
            client_app._train_neural(
                None, {"resampling-contract-sha256": "a" * 64}, {},
                {"loss_name": "bce_logits", "n_classes": 2}, object(),
                input_dim=2, manifest_image=False)
        load.assert_not_called()

    def test_image_training_partitions_paths_before_2d_or_3d_decode(self):
        import torch

        paths = np.asarray(["p0", "p1", "p2", "p3", "p4", "p5"])
        y = np.asarray([0, 0, 1, 0, 1, 1], dtype=np.int64)
        cases = (
            (False, None,
             np.asarray([False, True, False, True, False, True])),
            (True, np.asarray(["a", "a", "b", "c", "d", "d"]),
             np.asarray([True, True, False, True, False, False])),
        )
        cfg = {
            "resampling-contract-sha256": "a" * 64,
            "backbone": "resnet18", "vision-extractor-profile": "profile",
            "num-features": 2, "image-size": 32,
        }
        pins = {
            "loss_name": "cross_entropy", "n_classes": 2,
            "round_index": 1, "batch_size": 2, "local_epochs": 1,
            "num_rounds": 1,
        }
        pcfg = {"epsilon": 1.0, "delta": 1e-5, "clipping_norm": 1.0}

        for is_3d, groups, mask in cases:
            with self.subTest(is_3d=is_3d, unit="patient" if groups is not None else "row"):
                events = []
                captured = {}

                def prepare(*_args):
                    events.append("preflight")
                    return object(), 32, is_3d, "cpu"

                def load(_context):
                    events.append("paths")
                    return paths, y, groups

                def extract(_encoder, selected_paths, _size, got_3d, **_kwargs):
                    events.append("decode")
                    captured["paths"] = np.asarray(selected_paths).copy()
                    captured["is_3d"] = got_3d
                    return np.arange(
                        len(selected_paths) * 2, dtype=np.float32).reshape(-1, 2)

                def fit(_model, values, target, *_args, **_kwargs):
                    captured["features"] = values.copy()
                    captured["target"] = target.copy()
                    return [np.asarray([1.0])], len(target)

                n_units = len(y) if groups is None else len(np.unique(groups))
                model = torch.nn.Linear(2, 2)
                with (mock.patch.object(vision, "prepare_backbone",
                                        side_effect=prepare),
                      mock.patch.object(client_app, "load_image_collection",
                                        side_effect=load),
                      mock.patch.object(vision, "extract_features_from_paths",
                                        side_effect=extract),
                      mock.patch.object(client_app.task_module, "_load_manifest",
                                        return_value={"n_units": n_units}),
                      mock.patch.object(
                          client_app.task_module, "assert_pinned_unit_count"),
                      mock.patch.object(
                          client_app.resampling, "holdout_mask_from_context",
                          return_value=mask),
                      mock.patch.object(client_app, "_neural_seed_contract",
                                        return_value=({}, {})),
                      mock.patch.object(
                          client_app.dp_harness, "effective_dpsgd_mechanism",
                          return_value={"noise_multiplier": 1.0}),
                      mock.patch.object(client_app.seeding, "master_seed",
                                        return_value=b"vision-master"),
                      mock.patch.object(client_app, "_dp_fit", side_effect=fit)):
                    client_app._train_neural(
                        None, cfg, pcfg, pins, model, input_dim=2,
                        manifest_image=True,
                        on_private_start=lambda: events.append("private"))

                self.assertEqual(events, ["preflight", "private", "paths", "decode"])
                np.testing.assert_array_equal(captured["paths"], paths[~mask])
                self.assertEqual(captured["is_3d"], is_3d)
                self.assertEqual(len(captured["features"]), len(np.unique(
                    groups[~mask])) if groups is not None else int((~mask).sum()))

    def test_image_evaluation_decodes_only_held_out_paths(self):
        paths = np.asarray(["p0", "p1", "p2", "p3", "p4", "p5"])
        y = np.asarray([0, 0, 1, 0, 1, 1], dtype=np.int64)
        cases = (
            (False, None,
             np.asarray([False, True, False, True, False, True])),
            (True, np.asarray(["a", "a", "b", "c", "d", "d"]),
             np.asarray([True, True, False, True, False, False])),
        )
        cfg = {
            "loss-name": "cross_entropy", "task-type": "classification",
            "num-classes": 2, "holdout-validation-bins": 8,
            "resampling-privacy-unit": "patient",
            "backbone": "resnet18", "vision-extractor-profile": "profile",
            "num-features": 2, "image-size": 32,
        }
        pins = {"loss_name": "cross_entropy"}

        for is_3d, groups, mask in cases:
            with self.subTest(is_3d=is_3d, unit="patient" if groups is not None else "row"):
                events = []
                captured = {}

                def prepare(*_args):
                    events.append("preflight")
                    return object(), 32, is_3d, "cpu"

                def load(_context):
                    events.append("paths")
                    return paths, y, groups

                def extract(_encoder, selected_paths, _size, got_3d, **_kwargs):
                    events.append("decode")
                    captured["paths"] = np.asarray(selected_paths).copy()
                    captured["is_3d"] = got_3d
                    return np.arange(
                        len(selected_paths) * 2, dtype=np.float32).reshape(-1, 2)

                def release(target, predictions, layout, **kwargs):
                    captured["target"] = target.copy()
                    captured["predictions"] = predictions.copy()
                    captured["unit_ids"] = kwargs.get("unit_ids")
                    return np.ones(layout["size"]), 1.0

                with (mock.patch.object(client_app, "is_image_run",
                                        return_value=True),
                      mock.patch.object(vision, "prepare_backbone",
                                        side_effect=prepare),
                      mock.patch.object(client_app, "load_image_collection",
                                        side_effect=load),
                      mock.patch.object(vision, "extract_features_from_paths",
                                        side_effect=extract),
                      mock.patch.object(
                          client_app.task_module, "assert_pinned_unit_count"),
                      mock.patch.object(
                          client_app.resampling, "holdout_mask_from_context",
                          return_value=mask),
                      mock.patch.object(
                          validation, "neural_predictions",
                          side_effect=lambda _model, values, _loss:
                          np.full(len(values), 0.75)),
                      mock.patch.object(
                          validation, "private_validation_vector",
                          side_effect=release)):
                    released = client_app._holdout_neural_release(
                        None, cfg, {"epsilon": 0.4, "delta": 2e-6}, pins,
                        object(), input_dim=2,
                        on_private_start=lambda: events.append("private"))

                self.assertEqual(
                    events, ["preflight", "private", "paths", "decode"])
                np.testing.assert_array_equal(captured["paths"], paths[mask])
                np.testing.assert_array_equal(captured["target"], y[mask])
                self.assertEqual(captured["is_3d"], is_3d)
                if groups is None:
                    self.assertIsNone(captured["unit_ids"])
                else:
                    np.testing.assert_array_equal(
                        captured["unit_ids"], groups[mask])
                self.assertEqual(len(released), 1)

    def test_dp_fit_calibrates_to_post_partition_dataset(self):
        import torch

        model = torch.nn.Linear(2, 1)
        model._dsflower_release_keys = tuple(
            name for name, _ in torch.nn.Module.named_parameters(model))
        X = np.arange(6, dtype=np.float32).reshape(3, 2)
        y = np.asarray([0, 1, 0], dtype=np.float32)
        pcfg = {
            "n_samples": 6, "clipping_norm": 1.0,
            "epsilon": 1.6, "delta": 8e-6,
        }
        pins = {
            "loss_name": "bce_logits", "batch_size": 2,
            "local_epochs": 1, "num_rounds": 1, "n_classes": 2,
            "learning_rate": 0.1, "round_index": 1,
            "optimizer": {
                "name": "sgd", "weight_decay": 0.0, "momentum": 0.0,
                "nesterov": False, "l1_penalty": 0.0,
            },
            "scheduler": {"name": "none"},
        }
        captured = {}

        def make_private(inner_model, optimizer, trainloader, **kwargs):
            captured.update(kwargs)
            return inner_model, optimizer, [], None

        with (mock.patch.object(
                  client_app.dp_harness, "make_private_dpsgd",
                  side_effect=make_private),
              mock.patch.object(
                  client_app.dp_harness, "assert_releasable")):
            client_app._dp_fit(
                model, X, y, pcfg, pins, n_staged=6, cfg={},
                master=b"\x01" * 32, noise_multiplier=1.0)

        self.assertEqual(captured["n_samples"], len(X))
        self.assertNotEqual(captured["n_samples"], pcfg["n_samples"])

        fresh = torch.nn.Linear(2, 1)
        fresh._dsflower_release_keys = tuple(
            name for name, _ in torch.nn.Module.named_parameters(fresh))
        with (mock.patch.object(
                  client_app.dp_harness, "make_private_dpsgd") as make_private,
              self.assertRaisesRegex(RuntimeError, "staged sample count")):
            client_app._dp_fit(
                fresh, X, y, pcfg, pins, n_staged=5, cfg={},
                master=b"\x01" * 32, noise_multiplier=1.0)
        make_private.assert_not_called()

    def test_zero_gradient_dummy_executes_the_pinned_opacus_geometry(self):
        import torch

        model = torch.nn.Linear(2, 1)
        model._dsflower_release_keys = tuple(
            name for name, _ in torch.nn.Module.named_parameters(model))
        before = [value.copy() for value in client_app.get_torch_params(model)]
        pins = {
            "loss_name": "bce_logits", "batch_size": 2,
            "local_epochs": 1, "num_rounds": 1, "n_classes": 2,
            "learning_rate": 0.1, "round_index": 1,
            "optimizer": {
                "name": "sgd", "weight_decay": 0.0, "momentum": 0.0,
                "nesterov": False, "l1_penalty": 0.0,
            },
            "scheduler": {"name": "none"},
        }
        captured = {}
        original = dp_harness.make_private_dpsgd

        def wrap(*args, **kwargs):
            wrapped, optimizer, loader, engine = original(*args, **kwargs)
            captured.update(
                steps=len(loader), q=loader.batch_sampler.sample_rate,
                expected_batch_size=optimizer.expected_batch_size,
                noise_multiplier=optimizer.noise_multiplier,
                n_samples=kwargs.get("n_samples"))
            return wrapped, optimizer, loader, engine

        with mock.patch.object(
                dp_harness, "make_private_dpsgd", side_effect=wrap):
            arrays, n_examples = client_app._dp_fit(
                model, np.zeros((1, 2), dtype=np.float32),
                np.zeros(1, dtype=np.float32),
                {"n_samples": 4, "clipping_norm": 1.0,
                 "epsilon": 1.0, "delta": 1e-5},
                pins, n_staged=4, cfg={}, master=b"\x6a" * 32,
                noise_multiplier=1.25, geometry_n_units=4,
                public_zero_gradient=True)

        self.assertEqual(n_examples, 1)
        self.assertEqual(captured, {
            "steps": 2, "q": 0.5, "expected_batch_size": 2,
            "noise_multiplier": 1.25, "n_samples": 4,
        })
        self.assertTrue(any(bool(np.any(actual != public))
                            for actual, public in zip(arrays, before)))
        self.assertTrue(all(bool(np.all(np.isfinite(value)))
                            for value in arrays))


class ServerHoldoutTests(unittest.TestCase):
    @staticmethod
    def reply(request, vector):
        return Message(content=RecordDict({
            "arrays": ArrayRecord(numpy_ndarrays=[vector]),
            "metrics": MetricRecord({"num-examples": 1}),
        }), reply_to=request)

    def test_server_pools_one_vector_and_never_returns_fold_or_node_values(self):
        cfg = {
            "num-server-rounds": 2, "min-train-nodes": 2,
            "loss-name": "bce_logits", "task-type": "classification",
            "num-classes": 2, "num-labels": 2,
            "holdout-validation-bins": 4,
        }
        layout = validation.holdout_layout_from_config(cfg)

        class Grid:
            @staticmethod
            def get_node_ids():
                return [11, 22]

            def send_and_receive(self, messages, timeout):
                del timeout
                vectors = [np.ones(layout["size"]), np.full(layout["size"], 2.0)]
                return [ServerHoldoutTests.reply(message, vector)
                        for message, vector in zip(messages, vectors)]

        metrics = server_app._run_holdout(
            Grid(), cfg, ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
            {11, 22})
        self.assertIsInstance(metrics, dict)
        self.assertIn("accuracy", metrics)
        self.assertNotIn("per_node", metrics)
        self.assertNotIn("folds", metrics)

    def test_atomic_training_rejects_roster_replacement_between_rounds(self):
        class Grid:
            node_ids = [11, 22]

            @classmethod
            def get_node_ids(cls):
                return list(cls.node_ids)

        strategy = server_app._build_strategy(
            {"strategy": "fedavg"}, min_nodes=2, stable_roster=True)
        arrays = ArrayRecord(numpy_ndarrays=[np.asarray([0.0])])
        strategy.configure_train(1, arrays, ConfigRecord(), Grid())
        self.assertEqual(strategy.training_roster, frozenset({11, 22}))
        Grid.node_ids = [11, 33]
        with self.assertRaisesRegex(RuntimeError, "roster changed"):
            strategy.configure_train(2, arrays, ConfigRecord(), Grid())

    def test_empty_partition_outcomes_never_save_an_acceptable_model(self):
        cfg = {
            "resampling-contract-sha256": "a" * 64,
            "data-kind": "tabular", "num-server-rounds": 1,
            "min-train-nodes": 2,
        }
        result = SimpleNamespace(
            arrays=ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
            train_metrics_clientapp={})
        initial = ArrayRecord(numpy_ndarrays=[np.asarray([0.0])])

        train_empty = SimpleNamespace(
            available_rounds=set(), training_roster=frozenset({11, 22}),
            start=mock.Mock(return_value=result))
        with (mock.patch.object(
                  server_app, "_initial_arrays", return_value=(None, initial)),
              mock.patch.object(
                  server_app, "_build_strategy", return_value=train_empty),
              mock.patch.object(server_app, "_run_holdout") as evaluate,
              mock.patch.object(server_app, "_save_results") as save,
              self.assertRaisesRegex(RuntimeError, "every training round")):
            server_app._run_fedavg(None, cfg, "neural")
        evaluate.assert_not_called()
        save.assert_not_called()

    def test_image_holdout_reuses_the_neural_atomic_orchestration(self):
        cfg = {
            "resampling-contract-sha256": "a" * 64,
            "data-kind": "image", "num-server-rounds": 1,
            "min-train-nodes": 2,
        }
        result = SimpleNamespace(
            arrays=ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
            train_metrics_clientapp={})
        initial = ArrayRecord(numpy_ndarrays=[np.asarray([0.0])])
        strategy = SimpleNamespace(
            available_rounds={1}, training_roster=frozenset({11, 22}),
            start=mock.Mock(return_value=result))
        metrics = {"accuracy": 0.75}

        with (mock.patch.object(
                  server_app, "_initial_arrays", return_value=(None, initial)),
              mock.patch.object(
                  server_app, "_build_strategy", return_value=strategy),
              mock.patch.object(
                  server_app, "_run_holdout", return_value=metrics) as evaluate,
              mock.patch.object(server_app, "_save_results") as save):
            server_app._run_fedavg(None, cfg, "neural")

        evaluate.assert_called_once_with(
            None, cfg, result.arrays, strategy.training_roster)
        save.assert_called_once_with(
            cfg, None, result, available_rounds={1},
            holdout_metrics=metrics)

        test_empty = SimpleNamespace(
            available_rounds={1}, training_roster=frozenset({11, 22}),
            start=mock.Mock(return_value=result))
        with (mock.patch.object(
                  server_app, "_initial_arrays", return_value=(None, initial)),
              mock.patch.object(
                  server_app, "_build_strategy", return_value=test_empty),
              mock.patch.object(
                  server_app, "_run_holdout",
                  side_effect=RuntimeError(
                      "one or more holdout releases are unavailable")),
              mock.patch.object(server_app, "_save_results") as save,
              self.assertRaisesRegex(RuntimeError, "releases are unavailable")):
            server_app._run_fedavg(None, cfg, "neural")
        save.assert_not_called()

    def test_model_and_holdout_use_history_as_atomic_commit_marker(self):
        with tempfile.TemporaryDirectory() as results_dir:
            cfg = {
                "results-dir": results_dir,
                "num-server-rounds": 1,
                "min-train-nodes": 2,
                "loss-name": "bce_logits",
                "task-type": "classification",
                "num-classes": 2,
                "holdout-validation-bins": 4,
            }
            result = SimpleNamespace(
                arrays=ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
                train_metrics_clientapp={},
            )
            metrics = {"accuracy": 0.75}

            server_app._save_results(
                cfg, None, result, available_rounds={1},
                holdout_metrics=metrics)

            self.assertTrue(os.path.isfile(os.path.join(results_dir, "model.npz")))
            self.assertTrue(os.path.isfile(os.path.join(results_dir, "holdout.json")))
            self.assertTrue(os.path.isfile(os.path.join(results_dir, "history.json")))
            self.assertFalse(any(
                name.startswith(".holdout-") for name in os.listdir(results_dir)))

    def test_failed_holdout_transaction_publishes_no_model_or_commit_marker(self):
        with tempfile.TemporaryDirectory() as results_dir:
            cfg = {
                "results-dir": results_dir,
                "num-server-rounds": 1,
                "min-train-nodes": 2,
                "loss-name": "bce_logits",
                "task-type": "classification",
                "num-classes": 2,
                "holdout-validation-bins": 4,
            }
            result = SimpleNamespace(
                arrays=ArrayRecord(numpy_ndarrays=[np.asarray([1.0])]),
                train_metrics_clientapp={},
            )
            with (mock.patch.object(
                    server_app, "_save_holdout",
                    side_effect=RuntimeError("forced holdout failure")),
                  self.assertRaisesRegex(RuntimeError, "forced holdout failure")):
                server_app._save_results(
                    cfg, None, result, available_rounds={1},
                    holdout_metrics={"accuracy": 0.75})

            self.assertEqual(os.listdir(results_dir), [])

    def test_transaction_rolls_back_after_partial_publication(self):
        for fail_at in (2, 4):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as results_dir:
                cfg = {
                    "results-dir": results_dir,
                    "num-server-rounds": 1,
                    "min-train-nodes": 2,
                    "loss-name": "bce_logits",
                    "task-type": "classification",
                    "num-classes": 2,
                    "holdout-validation-bins": 4,
                }
                result = SimpleNamespace(
                    arrays=ArrayRecord(
                        numpy_ndarrays=[np.asarray([1.0])]),
                    train_metrics_clientapp={})
                real_replace = server_app.os.replace
                publications = 0

                def crashing_replace(source, destination):
                    nonlocal publications
                    if (os.path.dirname(destination) == results_dir
                            and os.path.dirname(source) != results_dir):
                        publications += 1
                        if publications == fail_at:
                            raise OSError("forced publication crash")
                    return real_replace(source, destination)

                with (mock.patch.object(
                          server_app.os, "replace",
                          side_effect=crashing_replace),
                      self.assertRaisesRegex(OSError, "publication crash")):
                    server_app._save_results(
                        cfg, None, result, available_rounds={1},
                        holdout_metrics={"accuracy": 0.75})
                self.assertEqual(publications, fail_at)
                self.assertEqual(os.listdir(results_dir), [])


if __name__ == "__main__":
    unittest.main()
