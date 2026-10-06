"""The versioned numeric holdout release: geometry, recovery and replay."""

import itertools
import json
import math
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
from flwr.common import RecordDict

FLOWER_APP = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "flower_app"))
if FLOWER_APP not in sys.path:
    sys.path.insert(0, FLOWER_APP)

from dsflower_runner import (canonical_units, client_app, dp_harness,
                             resampling, seeding, task, validation)
from v3_test_support import tagged_arrays


class NumericHoldoutGeometryTests(unittest.TestCase):
    def test_exhaustive_small_grid_covers_every_split_neighbor_case(self):
        grid = np.linspace(0.0, 1.0, 5)
        pairs = list(itertools.product(grid, repeat=2))
        y, prediction = np.asarray(pairs).T
        for task_name, sensitivity, radius in (
                ("regression", 2.0, math.sqrt(13.0) / 2.0),
                ("count", math.sqrt(5.0), math.sqrt(61.0) / 4.0)):
            with self.subTest(task=task_name):
                layout = validation.numeric_holdout_layout(task_name)
                rows = validation.validation_contributions(
                    y, prediction, layout, target_bounds=(0.0, 1.0))
                self.assertTrue(np.all(rows[:, 0] == 1.0))
                self.assertTrue(np.all(rows[:, 1:] >= -0.25))
                self.assertTrue(np.all(rows[:, 1:] <= 0.75))
                self.assertAlmostEqual(max(np.linalg.norm(row) for row in rows), radius)
                # Exhaust each adjacency case independently, so a test-only
                # replacement check cannot miss content-driven split movement.
                for old_in_test, new_in_test in itertools.product((0, 1), repeat=2):
                    observed = max(
                        np.linalg.norm(new_in_test * new - old_in_test * old)
                        for old in rows for new in rows)
                    self.assertLessEqual(observed, sensitivity + 1e-12)
                    expected = (sensitivity if old_in_test and new_in_test
                                else radius if old_in_test or new_in_test else 0.0)
                    self.assertAlmostEqual(observed, expected, places=7)
                # Patient means stay in the convex hull, including arbitrary
                # visit changes and presence/absence from the test partition.
                means = np.asarray([(a + b) / 2.0 for a in rows[::4] for b in rows[::4]])
                units = np.vstack((rows, means, np.zeros(layout["size"])))
                observed = np.max(np.linalg.norm(units[:, None] - units[None, :], axis=2))
                self.assertLessEqual(observed, sensitivity + 1e-12)
                for include_zero in (False, True):
                    self.assertEqual(validation._validation_release_sensitivity(
                        layout, include_zero_neighbor=include_zero), sensitivity)

    def test_actual_content_split_grid_bounds_enter_leave_and_replacement(self):
        with mock.patch.object(seeding, "_node_secret", return_value=b"c" * 32):
            grid = np.linspace(0.0, 1.0, 5)
            candidates = np.asarray(list(itertools.product(grid, repeat=2)))
            units = canonical_units.canonicalize_arrays(candidates[:, :1], candidates[:, 1])
            order = units.row_permutation
            candidates = candidates[order]
            contract = resampling.holdout_contract(500_000, "row")
            mask = resampling.holdout_mask(contract, n_rows=len(candidates),
                                           assignment_tokens=units.row_tokens)
            self.assertEqual(set(mask.tolist()), {False, True})
            for task_name in ("regression", "count"):
                layout = validation.numeric_holdout_layout(task_name)
                rows = validation.validation_contributions(
                    candidates[:, 1], candidates[:, 0], layout, target_bounds=(0.0, 1.0))
                selected = rows * mask[:, None]
                observed = max(np.linalg.norm(a - b) for a in selected for b in selected)
                self.assertLessEqual(observed, layout["sensitivity"] + 1e-12)

    def test_count_replacement_bound_has_an_exact_public_domain_witness(self):
        layout = validation.numeric_holdout_layout("count")
        rows = validation.validation_contributions(
            np.asarray([1.0, 10.0]), np.asarray([1.0, 1.0]), layout,
            target_bounds=(1.0, 10.0))
        self.assertEqual(np.linalg.norm(rows[1] - rows[0]), math.sqrt(5.0))
        self.assertEqual(np.linalg.norm(rows[1]), math.sqrt(61.0) / 4.0)

    def test_patient_accumulator_and_affine_recovery_preserve_exact_statistics(self):
        y = np.asarray([0.0, 0.25, 1.0, 0.75, 0.5])
        prediction = np.asarray([0.0, 0.5, 0.0, 0.5, 0.75])
        ids = np.asarray(["a", "a", "b", "c", "c"])
        for task_name, unit_ids in itertools.product(("regression", "count"), (None, ids)):
            with self.subTest(task=task_name, patient=unit_ids is not None):
                legacy = validation.validation_layout(task_name)
                layout = validation.numeric_holdout_layout(task_name)
                before = validation.validation_sufficient_vector(
                    y, prediction, legacy, target_bounds=(0.0, 1.0), unit_ids=unit_ids)
                centered = validation.validation_sufficient_vector(
                    y, prediction, layout, target_bounds=(0.0, 1.0), unit_ids=unit_ids)
                rows = validation.validation_contributions(
                    y, prediction, layout, target_bounds=(0.0, 1.0))
                dense = validation._unit_contributions(rows, unit_ids).sum(axis=0)
                np.testing.assert_allclose(centered, dense, atol=1e-15)
                recovered = centered.copy()
                recovered[1:] += 0.25 * recovered[0]
                np.testing.assert_allclose(recovered, before, atol=1e-15)
                permutation = np.asarray([4, 0, 3, 2, 1])
                replay = validation.validation_sufficient_vector(
                    y[permutation], prediction[permutation], layout,
                    target_bounds=(0.0, 1.0),
                    unit_ids=None if unit_ids is None else unit_ids[permutation])
                self.assertEqual(centered.tobytes(), replay.tobytes())
                self.assertEqual(validation.validation_metrics(centered, layout,
                    target_bounds=(0.0, 1.0)), validation.validation_metrics(before, legacy,
                    target_bounds=(0.0, 1.0)))

    def test_noisy_recovery_is_affine_and_uses_the_released_count(self):
        for task_name in ("regression", "count"):
            layout = validation.numeric_holdout_layout(task_name)
            legacy = validation.validation_layout(task_name)
            raw = validation.validation_sufficient_vector(
                np.asarray([0.0, 0.25, 0.5, 1.0]),
                np.asarray([0.25, 0.5, 0.5, 0.75]), layout,
                target_bounds=(0.0, 1.0))
            perturbation = np.linspace(0.2, -0.2, layout["size"])
            restored = []
            for noise in (perturbation, -perturbation):
                released = raw + noise
                unshifted = released.copy()
                unshifted[1:] += 0.25 * released[0]
                restored.append(unshifted)
                self.assertEqual(validation.validation_metrics(released, layout,
                    target_bounds=(0.0, 1.0)), validation.validation_metrics(unshifted, legacy,
                    target_bounds=(0.0, 1.0)))
            expected = raw.copy()
            expected[1:] += 0.25 * raw[0]
            np.testing.assert_allclose(np.mean(restored, axis=0), expected, atol=1e-15)

    def test_layout_version_is_explicit_and_legacy_vectors_keep_their_meaning(self):
        for task_name in ("regression", "count"):
            legacy = validation.validation_layout(task_name)
            layout = validation.numeric_holdout_layout(task_name)
            self.assertNotIn("version", legacy)
            self.assertEqual(layout["version"], "validation-vector-v4")
            self.assertEqual({key: value for key, value in layout.items() if key != "version"}, legacy)
            y, p = np.asarray([0.0, 1.0]), np.asarray([0.0, 0.0])
            raw = validation.validation_sufficient_vector(y, p, legacy, target_bounds=(0.0, 1.0))
            self.assertEqual(raw.tolist()[:5], [2.0, 1.0, 1.0, 1.0, 1.0])
            self.assertEqual(validation.validation_metrics(raw, legacy,
                target_bounds=(0.0, 1.0))["mae"], 0.5)
            for version in ("validation-vector-v3", "validation-vector-v5", 4, None):
                forged = dict(layout, version=version)
                with self.assertRaises(ValueError):
                    validation.validation_sufficient_vector(y, p, forged, target_bounds=(0.0, 1.0))
                with self.assertRaises(ValueError):
                    validation.validation_metrics(raw, forged, target_bounds=(0.0, 1.0))

    def test_numeric_holdout_changes_only_the_layouts_with_lower_calibrated_noise(self):
        for task_name, loss, lower in (("regression", "mse", 0.0),
                                      ("regression", "gamma_nll", 0.1),
                                      ("count", "poisson_nll", 0.0)):
            cfg = {"task-type": task_name, "validation-task": task_name, "loss-name": loss,
                   "holdout-target-lower": lower, "holdout-target-upper": 1.0,
                   "cv-target-lower": lower, "cv-target-upper": 1.0,
                   "validation-target-lower": lower, "validation-target-upper": 1.0}
            self.assertEqual(validation.holdout_layout_from_config(cfg),
                             validation.numeric_holdout_layout(task_name))
            self.assertEqual(validation.cross_validation_layout_from_config(cfg),
                             validation.validation_layout(task_name))
            self.assertEqual(validation.layout_from_config(cfg),
                             validation.validation_layout(task_name))
        binary = {"task-type": "classification", "loss-name": "bce_logits"}
        self.assertEqual(validation.holdout_layout_from_config(binary),
                         validation.validation_layout("classification"))

    def test_v4_layout_changes_identity_and_rejects_profile_version_drift(self):
        legacy = validation.build_validation_request(validation.validation_layout("regression"),
            1.0, 1e-6, operation="holdout-evaluate")
        shifted = validation.build_validation_request(validation.numeric_holdout_layout("regression"),
            1.0, 1e-6, operation="holdout-evaluate")
        self.assertNotEqual(legacy.canonical_json, shifted.canonical_json)
        value = json.loads(shifted.canonical_json)
        self.assertEqual(value.pop("contract"), seeding.SEMANTIC_CONTRACT)
        self.assertEqual(value["evaluation"]["layout_version"], "validation-vector-v4")
        self.assertEqual(seeding.build_request_identity(**value), shifted)
        for field, changed in (("version", "validation-vector-v5"), ("sensitivity", 0.1)):
            corrupted = json.loads(shifted.canonical_json)
            corrupted.pop("contract")
            corrupted["mechanism"]["profile"]["parameters"][field] = changed
            with self.assertRaises(ValueError):
                seeding.build_request_identity(**corrupted)
        value["evaluation"]["layout_version"] = "validation-vector-v3"
        with self.assertRaises(ValueError):
            seeding.build_request_identity(**value)


class NumericHoldoutReleaseTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.object(seeding, "_node_secret", return_value=b"h" * 32)
        patch.start()
        self.addCleanup(patch.stop)

    def test_empty_numeric_holdout_releases_one_noise_only_vector_at_new_sensitivity(self):
        for task_name in ("regression", "count"):
            layout = validation.numeric_holdout_layout(task_name)
            with mock.patch.object(dp_harness, "compute_output_sigma",
                                   wraps=dp_harness.compute_output_sigma) as calibrate:
                released, sigma = validation.private_validation_vector(
                    np.asarray([], dtype=np.float64), np.asarray([], dtype=np.float64),
                    layout, epsilon=1.0, delta=1e-6, target_bounds=(0.0, 1.0),
                    include_zero_neighbor=True)
            calibrate.assert_called_once()
            self.assertEqual(calibrate.call_args.args[2], layout["sensitivity"])
            self.assertGreater(sigma, 0.0)
            self.assertEqual(released.shape, (layout["size"],))
            self.assertTrue(np.all(np.isfinite(released)))
            self.assertFalse(np.array_equal(released, np.zeros(layout["size"])))

    def test_neural_regression_gamma_count_holdout_release_and_shuffle_replay(self):
        for task_name, loss, lower in (("regression", "mse", 0.0),
                                      ("regression", "gamma_nll", 0.1),
                                      ("count", "poisson_nll", 0.0)):
            for privacy_unit in ("row", "patient"):
                with self.subTest(task=task_name, loss=loss, privacy_unit=privacy_unit):
                    X = np.arange(48, dtype=np.float64).reshape(24, 2) / 48.0
                    y = np.linspace(lower, 1.0, len(X))
                    ids = (None if privacy_unit == "row" else
                           np.asarray(["patient-%02d" % (i // 2) for i in range(len(X))]))
                    contract = resampling.holdout_contract(500_000, privacy_unit)
                    manifest = {"target_column": "outcome", "feature_columns": ["a", "b"],
                                "patient_column": None if ids is None else "patient",
                                "patient-id-canonicalization": "trim-utf8-v2",
                                "dp-unit": privacy_unit, **resampling.manifest_fields(contract)}
                    cfg = {"task-type": task_name, "loss-name": loss,
                           "resampling-privacy-unit": privacy_unit,
                           "holdout-target-lower": lower, "holdout-target-upper": 1.0}
                    layout = validation.holdout_layout_from_config(cfg)
                    outputs = []
                    for order in (np.arange(len(y)), np.arange(len(y))[::-1]):
                        with tempfile.TemporaryDirectory() as directory:
                            context = SimpleNamespace(state=RecordDict(), run_config={},
                                node_config={"manifest-dir": directory})
                            loaded = tagged_arrays(X[order], y[order],
                                None if ids is None else ids[order])
                            with (mock.patch.object(task, "_load_manifest", return_value=manifest),
                                  mock.patch.object(client_app, "load_data", return_value=loaded),
                                  mock.patch.object(task, "assert_pinned_unit_count"),
                                  mock.patch.object(client_app, "is_image_run", return_value=False),
                                  mock.patch.object(client_app, "get_torch_params", return_value=[np.zeros(1)]),
                                  mock.patch.object(validation, "metric_predictions",
                                                    side_effect=lambda model, features, labels, config, layout:
                                                    np.clip(features[:, 0], lower, 1.0)),
                                  mock.patch.object(validation, "private_sufficient_vector",
                                                    wraps=validation.private_sufficient_vector) as release,
                                  mock.patch.object(dp_harness, "compute_output_sigma",
                                                    wraps=dp_harness.compute_output_sigma) as calibrate):
                                vector = client_app._holdout_neural_release(context, cfg,
                                    {"epsilon": 1.0, "delta": 1e-6}, {"loss_name": loss},
                                    object(), input_dim=2)[0]
                            release.assert_called_once()
                            self.assertIs(release.call_args.kwargs["include_zero_neighbor"], True)
                            self.assertEqual(calibrate.call_args.args[2], layout["sensitivity"])
                            calibrate.assert_called_once()
                            self.assertEqual(vector.shape, (layout["size"],))
                            self.assertTrue(np.all(np.isfinite(vector)))
                            metrics = validation.validation_metrics(vector, layout, target_bounds=(lower, 1.0))
                            self.assertTrue(math.isfinite(metrics["mae"]))
                            outputs.append(vector)
                    self.assertEqual(outputs[0].tobytes(), outputs[1].tobytes())

    def test_numeric_cv_keeps_legacy_layout_and_shuffled_oof_release(self):
        X = np.arange(48, dtype=np.float64).reshape(24, 2) / 48.0
        y = np.linspace(0.0, 1.0, len(X))
        for task_name in ("regression", "count"):
            contract = resampling.cross_validation_contract(3, "row")
            manifest = {"target_column": "outcome", "feature_columns": ["a", "b"],
                        "patient_column": None, "dp-unit": "row", "cv-job-sha256": "e" * 64,
                        **resampling.cross_validation_manifest_fields(contract)}
            cfg = {"task-type": task_name, "loss-name": "mse" if task_name == "regression" else "poisson_nll",
                   "cv-target-lower": 0.0, "cv-target-upper": 1.0}
            layout = validation.cross_validation_layout_from_config(cfg)
            outputs = []
            for order in (np.arange(len(y)), np.arange(len(y))[::-1]):
                with tempfile.TemporaryDirectory() as directory:
                    context = SimpleNamespace(state=RecordDict(), run_config={},
                        node_config={"manifest-dir": directory})
                    loaded = tagged_arrays(X[order], y[order])
                    with (mock.patch.object(task, "_load_manifest", return_value=manifest),
                          mock.patch.object(client_app, "load_data", return_value=loaded),
                          mock.patch.object(task, "assert_pinned_unit_count"),
                          mock.patch.object(client_app, "is_image_run", return_value=False),
                          mock.patch.object(client_app, "get_torch_params",
                                            side_effect=lambda model: [np.asarray([model], dtype=np.float64)]),
                          mock.patch.object(validation, "metric_predictions",
                                            side_effect=lambda model, features, labels, config, layout:
                                            np.clip(features[:, 0] + model / 10., 0., 1.)),
                          mock.patch.object(dp_harness, "compute_output_sigma",
                                            wraps=dp_harness.compute_output_sigma) as calibrate):
                        for fold in range(1, 4):
                            client_app._cross_validation_neural_accumulate(context, cfg,
                                {"loss_name": cfg["loss-name"]}, fold, input_dim=2, fold=fold)
                        raw = client_app._load_complete_cv_sufficient(context, layout)
                        self.assertEqual(raw[0], len(y))
                        self.assertTrue(np.all(raw >= 0.0))
                        outputs.append(client_app._cross_validation_release(context, cfg,
                            {"epsilon": 1.0, "delta": 1e-6})[0])
                        self.assertEqual(calibrate.call_args.args[2], layout["sensitivity"])
                        self.assertNotIn(client_app._CV_OOF_TOTAL_KEY, context.state)
            self.assertNotIn("version", layout)
            self.assertEqual(outputs[0].tobytes(), outputs[1].tobytes())


if __name__ == "__main__":
    unittest.main()
