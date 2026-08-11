"""Formal regressions for the trusted DP validation sufficient statistics."""

import base64
import hashlib
import json
import math
import os
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np

RUNNER_ROOT = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "flower_app"))
if RUNNER_ROOT not in sys.path:
    sys.path.insert(0, RUNNER_ROOT)

from dsflower_runner import (client_app, dp_harness, params, server_app, task,
                             validation, vision)  # noqa: E402
from flwr.common import (ArrayRecord, ConfigRecord, Message, MetricRecord,
                         RecordDict)  # noqa: E402


class ValidationSensitivityTests(unittest.TestCase):
    def test_binary_histogram_replace_one_sensitivity(self):
        layout = validation.validation_layout("classification", n_classes=2, bins=8)
        records = []
        for label in (0, 1):
            for score in np.linspace(0.0, 1.0, 17):
                records.append(validation.validation_contributions(
                    np.asarray([label]), np.asarray([score]), layout)[0])
        observed = max(np.linalg.norm(a - b) for a in records for b in records)
        self.assertLessEqual(observed, layout["sensitivity"] + 1e-12)
        self.assertAlmostEqual(observed, math.sqrt(2.0))

    def test_multiclass_declared_sensitivity_bounds_every_record_pair(self):
        layout = validation.validation_layout("classification", n_classes=3, bins=4)
        records = []
        scores = (np.asarray([0.8, 0.1, 0.1]),
                  np.asarray([0.1, 0.8, 0.1]),
                  np.asarray([0.1, 0.1, 0.8]),
                  np.asarray([1 / 3, 1 / 3, 1 / 3]))
        for label in range(3):
            for score in scores:
                records.append(validation.validation_contributions(
                    np.asarray([label]), score.reshape(1, -1), layout)[0])
        observed = max(np.linalg.norm(a - b) for a in records for b in records)
        self.assertLessEqual(observed, layout["sensitivity"] + 1e-12)

    def test_multilabel_declared_sensitivity_bounds_record_pairs(self):
        layout = validation.validation_layout("multilabel", n_labels=3, bins=4)
        a = validation.validation_contributions(
            np.asarray([[0, 0, 0]]), np.asarray([[0.1, 0.2, 0.3]]), layout)[0]
        b = validation.validation_contributions(
            np.asarray([[1, 1, 1]]), np.asarray([[0.9, 0.8, 0.7]]), layout)[0]
        self.assertLessEqual(
            np.linalg.norm(a - b), layout["sensitivity"] + 1e-12)

    def test_numeric_declared_sensitivities_bound_endpoints(self):
        for task in ("regression", "count"):
            layout = validation.validation_layout(task)
            records = []
            for target in (0.0, 10.0):
                for pred in (0.0, 10.0):
                    records.append(validation.validation_contributions(
                        np.asarray([target]), np.asarray([pred]), layout,
                        target_bounds=(0.0, 10.0))[0])
            observed = max(np.linalg.norm(a - b) for a in records for b in records)
            self.assertLessEqual(observed, layout["sensitivity"] + 1e-12)

    def test_patient_holdout_sensitivity_also_bounds_an_absent_unit(self):
        binary = validation.validation_layout("classification", bins=4)
        self.assertEqual(
            validation._validation_release_sensitivity(
                binary, include_zero_neighbor=True),
            binary["sensitivity"])
        expected = {"regression": math.sqrt(5.0), "count": math.sqrt(6.0)}
        for task, bound in expected.items():
            with self.subTest(task=task):
                layout = validation.validation_layout(task)
                record = validation.validation_contributions(
                    np.asarray([10.0]), np.asarray([0.0]), layout,
                    target_bounds=(0.0, 10.0))[0]
                self.assertGreater(np.linalg.norm(record), layout["sensitivity"])
                self.assertEqual(
                    validation._validation_release_sensitivity(
                        layout, include_zero_neighbor=True),
                    bound)
                self.assertLessEqual(np.linalg.norm(record), bound)

    def test_patient_holdout_release_calibrates_to_the_absent_unit_bound(self):
        layout = validation.validation_layout("regression")
        with (mock.patch.object(
                  dp_harness, "compute_output_sigma", return_value=1.0) as sigma,
              mock.patch.object(
                  validation, "_validation_noise_key", return_value=b"\x00" * 32)):
            validation.private_sufficient_vector(
                np.zeros(layout["size"]), layout, epsilon=1.0, delta=1e-6,
                include_zero_neighbor=True)
        self.assertEqual(sigma.call_args.args[2], math.sqrt(5.0))

    def test_layout_caps_dimension(self):
        thirty_three = validation.validation_layout(
            "classification", n_classes=33, bins=512)
        maximum = validation.validation_layout(
            "classification", n_classes=1024, bins=512)
        multilabel = validation.validation_layout(
            "multilabel", n_labels=1024, bins=512)
        self.assertEqual(thirty_three["classes"], 33)
        self.assertEqual(maximum["size"], 2_097_152)
        self.assertEqual(multilabel["size"], 1_048_576)
        with self.assertRaises(ValueError):
            validation.validation_layout("classification", n_classes=1025)
        with self.assertRaises(ValueError):
            validation.validation_layout("multilabel", n_labels=1025)
        with self.assertRaises(ValueError):
            validation.validation_layout("classification", bins=513)


class ValidationReleaseTests(unittest.TestCase):
    def setUp(self):
        self._secret_dir = tempfile.TemporaryDirectory()
        secret = os.path.join(self._secret_dir.name, "node-secret")
        with open(secret, "w", encoding="ascii") as handle:
            handle.write("31" * 32)
        os.chmod(secret, 0o600)
        self._secret_env = mock.patch.dict(
            os.environ, {"DSFLOWER_NODE_SECRET_FILE": secret})
        self._secret_env.start()

    def tearDown(self):
        self._secret_env.stop()
        self._secret_dir.cleanup()

    @staticmethod
    def _raw_and_key(y, predictions, layout, *, unit_ids=None):
        raw = validation._summed_validation_contributions(
            y, predictions, layout, unit_ids=unit_ids)
        sigma = dp_harness.compute_output_sigma(
            1.0, 1e-5, layout["sensitivity"], num_releases=1)
        return raw, validation._validation_noise_key(raw, layout, sigma)

    def test_private_release_requires_custodial_secret(self):
        layout = validation.validation_layout("classification", bins=8)
        with mock.patch.dict(
                os.environ, {"DSFLOWER_NODE_SECRET_FILE": ""}):
            with self.assertRaises(RuntimeError):
                validation.private_validation_vector(
                    np.asarray([0, 1]), np.asarray([0.2, 0.8]), layout,
                    epsilon=1.0, delta=1e-5)

    def test_private_release_rejects_forged_layout_sensitivity(self):
        layout = validation.validation_layout("classification", bins=8)
        with self.assertRaises(ValueError):
            validation.private_validation_vector(
                np.asarray([0, 1]), np.asarray([0.2, 0.8]),
                {**layout, "sensitivity": 0.01},
                epsilon=1.0, delta=1e-5)

    def test_same_release_key_is_byte_deterministic(self):
        layout = validation.validation_layout("classification", bins=8)
        kwargs = dict(y=np.asarray([0, 1, 1]),
                      predictions=np.asarray([0.1, 0.7, 0.9]),
                      layout=layout, epsilon=1.0, delta=1e-5)
        one, sigma_one = validation.private_validation_vector(**kwargs)
        two, sigma_two = validation.private_validation_vector(**kwargs)
        self.assertEqual(one.tobytes(), two.tobytes())
        self.assertEqual(sigma_one, sigma_two)
        self.assertFalse(np.array_equal(
            one, validation.validation_contributions(
                kwargs["y"], kwargs["predictions"], layout).sum(axis=0)))

    def test_row_permutation_and_patient_relabel_keep_release_and_key(self):
        layout = validation.validation_layout("classification", bins=8)
        y = np.asarray([0, 1, 0, 1, 1, 0])
        predictions = np.asarray([0.1, 0.8, 0.3, 0.7, 0.9, 0.2])
        patient_ids = np.asarray(["a", "a", "b", "c", "c", "b"])
        relabeled = np.asarray(["z", "z", "x", "q", "q", "x"])
        permutation = np.asarray([5, 2, 4, 0, 3, 1])

        raw, key = self._raw_and_key(
            y, predictions, layout, unit_ids=patient_ids)
        permuted_raw, permuted_key = self._raw_and_key(
            y[permutation], predictions[permutation], layout,
            unit_ids=relabeled[permutation])
        np.testing.assert_array_equal(raw, permuted_raw)
        self.assertEqual(key, permuted_key)

        released, _ = validation.private_validation_vector(
            y, predictions, layout, epsilon=1.0, delta=1e-5,
            unit_ids=patient_ids)
        permuted, _ = validation.private_validation_vector(
            y[permutation], predictions[permutation], layout,
            epsilon=1.0, delta=1e-5,
            unit_ids=relabeled[permutation])
        np.testing.assert_array_equal(released, permuted)

    def test_distinct_scores_in_same_bins_keep_release_and_key(self):
        layout = validation.validation_layout("classification", bins=8)
        y = np.asarray([0, 0, 1, 1], dtype=np.int32)
        first_scores = np.asarray([0.02, 0.24, 0.76, 0.99])
        second_scores = np.asarray([0.10, 0.20, 0.80, 0.90],
                                   dtype=np.float32)
        first_raw, first_key = self._raw_and_key(
            y, first_scores, layout)
        represented_layout = dict(reversed(list(layout.items())))
        represented_layout["non-semantic-note"] = "different artifact"
        second_raw, second_key = self._raw_and_key(
            y.astype(np.float64), second_scores, represented_layout)
        np.testing.assert_array_equal(first_raw, second_raw)
        self.assertEqual(first_key, second_key)
        first, _ = validation.private_validation_vector(
            y, first_scores, layout, epsilon=1.0, delta=1e-5)
        second, _ = validation.private_validation_vector(
            y.astype(np.float64), second_scores, layout,
            epsilon=1.0, delta=1e-5)
        np.testing.assert_array_equal(first, second)

    def test_changed_sufficient_statistic_changes_release_key(self):
        layout = validation.validation_layout("classification", bins=8)
        y = np.asarray([0, 1, 1])
        first_scores = np.asarray([0.1, 0.7, 0.9])
        second_scores = np.asarray([0.1, 0.2, 0.9])
        first_raw, first_key = self._raw_and_key(y, first_scores, layout)
        second_raw, second_key = self._raw_and_key(y, second_scores, layout)
        self.assertFalse(np.array_equal(first_raw, second_raw))
        self.assertNotEqual(first_key, second_key)

    def test_changed_effective_sigma_changes_release_key(self):
        layout = validation.validation_layout("classification", bins=8)
        raw = validation._summed_validation_contributions(
            np.asarray([0, 1]), np.asarray([0.1, 0.9]), layout)
        first = validation._validation_noise_key(raw, layout, 1.0)
        second = validation._validation_noise_key(raw, layout, 2.0)
        self.assertNotEqual(first, second)

    def test_metric_subset_is_postprocessing_of_one_release(self):
        layout = validation.validation_layout("classification", bins=16)
        y = np.asarray([0, 0, 1, 1])
        predictions = np.asarray([0.05, 0.2, 0.8, 0.95])
        released, _ = validation.private_validation_vector(
            y, predictions, layout, epsilon=1.0, delta=1e-5)
        complete = validation.validation_metrics(released, layout)
        requested = {name: complete[name]
                     for name in ("accuracy", "roc_auc")}
        replayed, _ = validation.private_validation_vector(
            y, predictions, layout, epsilon=1.0, delta=1e-5)
        np.testing.assert_array_equal(released, replayed)
        self.assertEqual(requested, {
            "accuracy": complete["accuracy"],
            "roc_auc": complete["roc_auc"],
        })

    def test_empty_cohort_reaches_the_dp_mechanism(self):
        layout = validation.validation_layout("classification", bins=8)
        released, sigma = validation.private_validation_vector(
            np.asarray([], dtype=np.float64),
            np.asarray([], dtype=np.float64), layout,
            epsilon=1.0, delta=1e-5)
        self.assertEqual(released.shape, (layout["size"],))
        self.assertTrue(np.all(np.isfinite(released)))
        self.assertGreater(sigma, 0.0)
        self.assertFalse(np.array_equal(released, np.zeros(layout["size"])))

    def test_shard_statistics_are_additive_before_dp(self):
        layout = validation.validation_layout("classification", bins=8)
        y = np.asarray([0, 0, 1, 1])
        p = np.asarray([0.1, 0.3, 0.7, 0.9])
        full = validation.validation_contributions(y, p, layout).sum(axis=0)
        shards = sum(
            (validation.validation_contributions(y[s], p[s], layout).sum(axis=0)
             for s in (slice(0, 2), slice(2, 4))),
            np.zeros(layout["size"]))
        np.testing.assert_array_equal(full, shards)

    def test_patient_unit_contribution_is_visit_count_independent(self):
        layout = validation.validation_layout("classification", bins=8)
        y = np.asarray([0, 0, 1])
        p = np.asarray([0.1, 0.3, 0.9])
        rows = validation.validation_contributions(y, p, layout)
        grouped = validation._unit_contributions(
            rows, unit_ids=np.asarray(["patient-a", "patient-a", "patient-b"]))
        self.assertEqual(grouped.shape, (2, layout["size"]))
        np.testing.assert_allclose(grouped[0], 0.5 * (rows[0] + rows[1]))
        np.testing.assert_array_equal(grouped[1], rows[2])
        self.assertLessEqual(
            np.linalg.norm(grouped[0] - grouped[1]),
            layout["sensitivity"] + 1e-12)

    def test_direct_patient_sum_matches_dense_grouping(self):
        layout = validation.validation_layout(
            "classification", n_classes=3, bins=8)
        y = np.asarray([0, 1, 2, 1, 0, 2, 2])
        p = np.asarray([
            [0.8, 0.1, 0.1], [0.2, 0.7, 0.1], [0.1, 0.2, 0.7],
            [0.2, 0.6, 0.2], [0.9, 0.05, 0.05], [0.1, 0.1, 0.8],
            [0.2, 0.2, 0.6]])
        ids = np.asarray(["a", "a", "b", "c", "c", "c", "d"])
        dense = validation._unit_contributions(
            validation.validation_contributions(y, p, layout), ids).sum(axis=0)
        direct = validation._summed_validation_contributions(
            y, p, layout, unit_ids=ids)
        np.testing.assert_allclose(direct, dense, atol=1e-12)

    def test_direct_accumulator_supports_1024_classes(self):
        layout = validation.validation_layout(
            "classification", n_classes=1024, bins=512)
        scores = np.zeros((2, 1024), dtype=np.float64)
        scores[0, 0] = 1.0
        scores[1, -1] = 1.0
        total = validation._summed_validation_contributions(
            np.asarray([0, 1023]), scores, layout)
        self.assertEqual(total.shape, (2_097_152,))
        self.assertEqual(float(total.sum()), 2.0 * 1025.0)

    def test_direct_accumulator_matches_dense_for_every_layout(self):
        cases = [
            (validation.validation_layout("classification", bins=8),
             np.asarray([0, 1, 1]), np.asarray([0.1, 0.8, 0.6]), None),
            (validation.validation_layout(
                "classification", n_classes=3, bins=8),
             np.asarray([0, 1, 2]), np.eye(3), None),
            (validation.validation_layout("ordinal", n_classes=3, bins=8),
             np.asarray([0, 1, 2]), np.eye(3), None),
            (validation.validation_layout("multilabel", n_labels=2, bins=8),
             np.asarray([[0, 1], [1, 0], [1, 1]]),
             np.asarray([[0.1, 0.9], [0.8, 0.2], [0.7, 0.6]]), None),
            (validation.validation_layout("regression"),
             np.asarray([0.0, 4.0, 10.0]), np.asarray([1.0, 3.0, 8.0]),
             (0.0, 10.0)),
            (validation.validation_layout("count"),
             np.asarray([0.0, 4.0, 10.0]), np.asarray([1.0, 3.0, 8.0]),
             (0.0, 10.0)),
        ]
        ids = np.asarray(["a", "a", "b"])
        for layout, y, scores, bounds in cases:
            with self.subTest(task=layout["task"]):
                rows = validation.validation_contributions(
                    y, scores, layout, target_bounds=bounds)
                expected = validation._unit_contributions(rows, ids).sum(axis=0)
                actual = validation._summed_validation_contributions(
                    y, scores, layout, target_bounds=bounds, unit_ids=ids)
                np.testing.assert_allclose(actual, expected, atol=1e-12)

    def test_high_visit_patient_cannot_exceed_unit_mass(self):
        rows = 1_000_000
        layout = validation.validation_layout("classification", bins=8)
        ids = np.full(rows, "p", dtype="U1")
        left = validation._summed_validation_contributions(
            np.zeros(rows), np.zeros(rows), layout, unit_ids=ids)
        right = validation._summed_validation_contributions(
            np.ones(rows), np.ones(rows), layout, unit_ids=ids)
        self.assertEqual(left.sum(), 1.0)
        self.assertEqual(right.sum(), 1.0)
        self.assertLessEqual(
            np.linalg.norm(left - right), layout["sensitivity"])

    def test_private_release_rejects_misaligned_unit_ids(self):
        layout = validation.validation_layout("classification", bins=8)
        with self.assertRaises(ValueError):
            validation.private_validation_vector(
                np.asarray([0, 1]), np.asarray([0.2, 0.8]), layout,
                epsilon=1.0, delta=1e-5,
                unit_ids=np.asarray(["only-one"]))

    def test_binary_metrics_are_postprocessing_of_histogram(self):
        layout = validation.validation_layout("classification", bins=16)
        y = np.asarray([0, 0, 1, 1])
        p = np.asarray([0.05, 0.2, 0.8, 0.95])
        raw = validation.validation_contributions(y, p, layout).sum(axis=0)
        metrics = validation.validation_metrics(raw, layout)
        self.assertEqual(metrics["accuracy"], 1.0)
        self.assertEqual(metrics["roc_auc"], 1.0)
        self.assertLess(metrics["brier"], 0.05)

    def test_noise_only_primary_metrics_are_finite_when_count_projects_to_zero(self):
        binary = validation.validation_layout("classification", bins=4)
        binary_metrics = validation.validation_metrics(
            -np.ones(binary["size"], dtype=np.float64), binary)
        self.assertTrue(math.isfinite(binary_metrics["accuracy"]))

        regression = validation.validation_layout("regression")
        regression_metrics = validation.validation_metrics(
            -np.ones(regression["size"], dtype=np.float64), regression,
            target_bounds=(0.0, 1.0))
        for name in ("mae", "mse", "rmse"):
            with self.subTest(name=name):
                self.assertTrue(math.isfinite(regression_metrics[name]))

    def test_regression_metrics_rescale_to_public_target_domain(self):
        layout = validation.validation_layout("regression")
        y = np.asarray([0.0, 5.0, 10.0])
        p = np.asarray([1.0, 5.0, 9.0])
        raw = validation.validation_contributions(
            y, p, layout, target_bounds=(0.0, 10.0)).sum(axis=0)
        metrics = validation.validation_metrics(
            raw, layout, target_bounds=(0.0, 10.0))
        self.assertAlmostEqual(metrics["mae"], 2.0 / 3.0)
        self.assertAlmostEqual(metrics["rmse"], math.sqrt(2.0 / 3.0))

    def test_extreme_finite_release_stays_json_serializable(self):
        layout = validation.validation_layout("regression")
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            metrics = validation.validation_metrics(
                np.asarray([1e-320, 1e308, 1e308, 0.0, 1e-320]),
                layout, target_bounds=(0.0, 1.0))
        json.dumps(metrics, allow_nan=False)
        self.assertGreaterEqual(metrics["mae"], 0.0)
        self.assertLessEqual(metrics["mae"], 1.0)
        self.assertGreaterEqual(metrics["mse"], 0.0)
        self.assertLessEqual(metrics["mse"], 1.0)

    def test_numeric_metrics_project_huge_noise_to_public_ranges(self):
        regression = validation.validation_layout("regression")
        metrics = validation.validation_metrics(
            np.asarray([2.0, 1e300, 1e300, 1e300, -1e300]),
            regression, target_bounds=(10.0, 20.0))
        self.assertEqual(metrics["n"], 2.0)
        self.assertGreaterEqual(metrics["mae"], 0.0)
        self.assertLessEqual(metrics["mae"], 10.0)
        self.assertGreaterEqual(metrics["mse"], 0.0)
        self.assertLessEqual(metrics["mse"], 100.0)
        self.assertGreaterEqual(metrics["rmse"], 0.0)
        self.assertLessEqual(metrics["rmse"], 10.0)
        if metrics["r_squared"] is not None:
            self.assertLessEqual(metrics["r_squared"], 1.0)

        count = validation.validation_layout("count")
        count_metrics = validation.validation_metrics(
            np.asarray([3.0, 1e300, 1e300, 1e300, 1e300, 1e300]),
            count, target_bounds=(0.0, 5.0))
        self.assertGreaterEqual(
            count_metrics["mean_poisson_deviance_normalized"], 0.0)
        self.assertLessEqual(
            count_metrics["mean_poisson_deviance_normalized"], 1.0)

    def test_multiclass_ordinal_multilabel_and_count_metrics(self):
        labels = np.asarray([0, 1, 2])
        probabilities = np.eye(3, dtype=np.float64)
        multiclass = validation.validation_layout(
            "classification", n_classes=3, bins=8)
        raw = validation.validation_contributions(
            labels, probabilities, multiclass).sum(axis=0)
        self.assertEqual(
            validation.validation_metrics(raw, multiclass)["accuracy"], 1.0)

        ordinal = validation.validation_layout("ordinal", n_classes=3, bins=8)
        raw = validation.validation_contributions(
            labels, probabilities, ordinal).sum(axis=0)
        self.assertEqual(
            validation.validation_metrics(raw, ordinal)["ordinal_mae"], 0.0)

        multilabel = validation.validation_layout(
            "multilabel", n_labels=2, bins=8)
        truth = np.asarray([[0, 1], [1, 0]])
        raw = validation.validation_contributions(
            truth, truth.astype(np.float64), multilabel).sum(axis=0)
        self.assertEqual(
            validation.validation_metrics(raw, multilabel)["macro_f1"], 1.0)

        count = validation.validation_layout("count")
        values = np.asarray([1.0, 2.0, 5.0])
        raw = validation.validation_contributions(
            values, values, count, target_bounds=(0.0, 5.0)).sum(axis=0)
        metrics = validation.validation_metrics(
            raw, count, target_bounds=(0.0, 5.0))
        self.assertEqual(metrics["rmse"], 0.0)
        self.assertEqual(metrics["mean_poisson_deviance_normalized"], 0.0)


class ValidationInferenceTests(unittest.TestCase):
    @staticmethod
    def _vision_config(backbone, n_classes=2):
        feature_dim = vision.feature_dim_for(backbone)
        spec = {"kind": "sequential", "layers": [
            {"op": "linear", "out": "@out"}]}
        return {
            "validation-model-track": "neural",
            "validation-task": "binary" if n_classes == 2 else "multiclass",
            "validation-bins": 8,
            "validation-contract-sha256": "a" * 64,
            "data-kind": "image", "backbone": backbone,
            "image-size": vision._MIN_BACKBONE_IMAGE_SIZE[backbone],
            "vision-extractor-profile": vision.extractor_profile_for(backbone),
            "num-features": feature_dim, "num-classes": n_classes,
            "num-labels": 2, "loss-name": "cross_entropy",
            "model-spec-b64": base64.b64encode(
                json.dumps(spec).encode("utf-8")).decode("ascii"),
            "validation-artifact-format": "pytorch-state-dict-v1",
            "validation-artifact-sha256": "0" * 64,
            "validation-artifact-size-bytes": 1,
        }

    def test_neural_inference_is_batched_and_accepts_empty_cohorts(self):
        import torch

        model = torch.nn.Linear(2, 3)
        batch_sizes = []
        hook = model.register_forward_hook(
            lambda _module, inputs, _output: batch_sizes.append(inputs[0].shape[0]))
        try:
            with mock.patch.object(validation, "_INFERENCE_BATCH_ROWS", 3):
                scores = validation.neural_predictions(
                    model, np.ones((8, 2), dtype=np.float32), "CROSS_ENTROPY")
                empty = validation.neural_predictions(
                    model, np.empty((0, 2), dtype=np.float32), "cross_entropy")
        finally:
            hook.remove()
        self.assertEqual(scores.shape, (8, 3))
        self.assertEqual(empty.shape, (0, 3))
        self.assertLessEqual(max(batch_sizes), 3)

    def test_public_preprocessing_replays_neural_contract(self):
        values = np.asarray([
            [np.nan, np.inf], [-100.0, 100.0], [2.0, 14.0]])
        cfg = {"feature-bounds": {
            "lower": [0.0, 10.0], "upper": [4.0, 18.0]}}
        np.testing.assert_array_equal(
            validation._apply_feature_bounds(values, cfg),
            np.asarray([[0.0, 0.0], [-1.0, 1.0], [0.0, 0.0]],
                       dtype=np.float32))

    def test_server_loads_saved_neural_artifact_as_public_arrays(self):
        import torch

        spec = {"kind": "sequential", "layers": [
            {"op": "linear", "out": 4}, {"op": "relu"},
            {"op": "linear", "out": "@out"}]}
        cfg = {
            "validation-model-track": "neural", "num-features": 2,
            "num-classes": 2, "num-labels": 2,
            "loss-name": "bce_logits",
            "model-spec-b64": base64.b64encode(
                json.dumps(spec).encode("utf-8")).decode("ascii"),
        }
        model = params.load_user_model(cfg, 2, "bce_logits")
        expected = params.get_torch_params(model)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "model.pt")
            torch.save(model.state_dict(), path)
            cfg["validation-model-path-b64"] = base64.b64encode(
                path.encode("utf-8")).decode("ascii")
            actual = validation.public_model_arrays(cfg)
        self.assertEqual(len(actual), len(expected))
        for got, want in zip(actual, expected):
            np.testing.assert_array_equal(got, want)

    def test_server_loads_canonical_2d_and_3d_vision_heads(self):
        import torch

        for backbone in (
                "resnet18", "resnet18_3d",
                "densenet121", "densenet121_3d"):
            with self.subTest(backbone=backbone), \
                    tempfile.TemporaryDirectory() as directory:
                cfg = self._vision_config(
                    backbone, n_classes=3 if "dense" in backbone else 2)
                model = params.load_user_model(
                    cfg, cfg["num-features"], "cross_entropy")
                expected = params.get_torch_params(model)
                path = os.path.join(directory, "model.pt")
                torch.save(model.state_dict(), path)
                with open(path, "rb") as handle:
                    digest = hashlib.sha256(handle.read()).hexdigest()
                cfg.update({
                    "validation-model-path-b64": base64.b64encode(
                        path.encode("utf-8")).decode("ascii"),
                    "validation-artifact-sha256": digest,
                    "validation-artifact-size-bytes": os.path.getsize(path),
                })
                actual = validation.public_model_arrays(cfg)
                self.assertEqual(len(actual), len(expected))
                for got, want in zip(actual, expected):
                    np.testing.assert_array_equal(got, want)

    def test_vision_artifact_tamper_fails_before_torch_decoder(self):
        import torch

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "model.pt")
            with open(path, "wb") as handle:
                handle.write(b"public but tampered checkpoint")
            cfg = self._vision_config("resnet18")
            cfg.update({
                "validation-model-path-b64": base64.b64encode(
                    path.encode("utf-8")).decode("ascii"),
                "validation-artifact-size-bytes": os.path.getsize(path),
            })
            with mock.patch.object(
                    torch, "load",
                    side_effect=AssertionError("decoder must not run")) as load:
                with self.assertRaisesRegex(ValueError, "SHA-256 pin"):
                    validation.public_model_arrays(cfg)
            load.assert_not_called()

    def test_server_rejects_removed_tree_validation_track(self):
        with self.assertRaisesRegex(ValueError, "must be neural"):
            validation.public_model_arrays({
                "validation-model-track": "trees", "num-features": 2,
            })

    def test_public_model_arrays_are_transport_bounded(self):
        with mock.patch.object(validation, "_MAX_PUBLIC_ELEMENTS", 1):
            with self.assertRaises(ValueError):
                validation._bounded_public_arrays([
                    np.zeros(2, dtype=np.float32)])
            accepted = validation._bounded_public_arrays(
                [np.zeros(2, dtype=np.uint8)], max_elements=2)
            self.assertEqual(accepted[0].size, 2)
        with self.assertRaises(ValueError):
            validation._bounded_public_arrays([
                np.asarray([np.inf], dtype=np.float32)])

    def test_client_public_transport_honors_an_explicit_element_cap(self):
        with mock.patch.object(client_app, "_MAX_EGRESS_ELEMENTS", 1):
            with self.assertRaises(RuntimeError):
                client_app._validate_public_egress_arrays([
                    np.zeros(2, dtype=np.uint8)])
            accepted = client_app._validate_public_egress_arrays(
                [np.zeros(2, dtype=np.uint8)], max_elements=2)
        self.assertEqual(accepted[0].size, 2)

    def test_neural_binary_and_ordinal_probabilities_are_total(self):
        import torch

        binary = torch.nn.Linear(2, 1)
        with torch.no_grad():
            binary.weight.zero_()
            binary.bias.zero_()
        score = validation.neural_predictions(
            binary, np.asarray([[1.0, 2.0], [3.0, 4.0]]), "bce_logits")
        np.testing.assert_allclose(score, np.asarray([0.5, 0.5]))

        ordinal = torch.nn.Linear(2, 2)
        with torch.no_grad():
            ordinal.weight.zero_()
            ordinal.bias.zero_()
        probs = validation.neural_predictions(
            ordinal, np.asarray([[1.0, 2.0]]), "ordinal")
        self.assertEqual(probs.shape, (1, 3))
        np.testing.assert_allclose(probs.sum(axis=1), np.ones(1))

    def test_layout_from_config_accepts_server_scalar_target_bounds(self):
        layout = validation.layout_from_config({
            "validation-task": "regression", "validation-bins": 16,
            "validation-target-lower": -10.0,
            "validation-target-upper": 10.0,
        })
        self.assertEqual(layout["task"], "regression")
        self.assertEqual(layout["size"], 5)

    def test_layout_rejects_loss_incompatible_numeric_bounds(self):
        with self.assertRaises(ValueError):
            validation.layout_from_config({
                "validation-task": "count", "validation-bins": 16,
                "validation-target-lower": -1.0,
                "validation-target-upper": 10.0,
            })
        with self.assertRaises(ValueError):
            validation.layout_from_config({
                "validation-task": "regression", "validation-bins": 16,
                "validation-target-lower": 0.0,
                "validation-target-upper": 10.0,
                "loss-name": "gamma_nll",
            })
        with self.assertRaises(ValueError):
            validation.layout_from_config({
                "validation-task": "multiclass", "validation-bins": 16,
                "num-classes": 3, "loss-name": "bce_logits",
            })
        with self.assertRaises(ValueError):
            validation.layout_from_config({
                "validation-task": "multilabel", "validation-bins": 16,
                "num-classes": 3, "num-labels": 2,
                "loss-name": "multilabel_bce",
            })

    def test_node_neural_validation_releases_only_private_statistics(self):
        with tempfile.TemporaryDirectory() as directory:
            data_path = os.path.join(directory, "data.csv")
            with open(data_path, "w", encoding="utf-8") as handle:
                handle.write("x1,x2,y\n0,1,0\n1,0,1\n1,1,1\n")
            manifest = {
                "data_file": "data.csv", "data_format": "csv",
                "data_type": "tabular", "target_column": "y",
                "feature_columns": ["x1", "x2"], "dp-unit": "row",
                "patient_column": None, "n_units": 3,
                "task-type": "classification", "loss-name": "bce_logits",
                "num-classes": 2, "num-labels": 2,
            }
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            secret = os.path.join(directory, "node-secret")
            with open(secret, "w", encoding="ascii") as handle:
                handle.write("42" * 32)
            os.chmod(secret, 0o600)
            spec = {"kind": "sequential", "layers": [
                {"op": "linear", "out": "@out"}]}
            cfg = {
                "validation-model-track": "neural",
                "validation-task": "binary", "validation-bins": 8,
                "validation-contract-sha256": "a" * 64,
                "validation-metrics": ["accuracy", "roc_auc", "brier"],
                "validation-thresholds": [0.25, 0.5, 0.75],
                "num-features": 2, "num-classes": 2, "num-labels": 2,
                "loss-name": "bce_logits",
                "model-spec-b64": base64.b64encode(
                    json.dumps(spec).encode("utf-8")).decode("ascii"),
            }
            model = params.load_user_model(cfg, 2, "bce_logits")
            arrays = params.get_torch_params(model)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            with mock.patch.dict(os.environ, {
                    "DSFLOWER_NODE_SECRET_FILE": secret}):
                released = validation.private_model_validation(
                    context, cfg, {
                        "epsilon": 1.0, "delta": 1e-5,
                        "policy_hash": "1" * 64,
                    }, 1, arrays)
                replayed = validation.private_model_validation(
                    context, {
                        **cfg, "run-token": "another-run",
                        "message-id": "another-message",
                        "validation-contract-sha256": "b" * 64,
                        "validation-metrics": ["accuracy"],
                        "validation-thresholds": [0.42],
                        "validation-model-path-b64": base64.b64encode(
                            b"/another/public/path").decode("ascii"),
                    }, {
                        "epsilon": 1.0, "delta": 1e-5,
                        "policy_hash": "1" * 64,
                    }, 99, arrays)
            self.assertEqual(len(released), 1)
            self.assertEqual(released[0].shape, (16,))
            self.assertTrue(np.all(np.isfinite(released[0])))
            np.testing.assert_array_equal(released[0], replayed[0])
            raw = validation.validation_contributions(
                np.asarray([0, 1, 1]),
                validation.neural_predictions(
                    model, np.asarray([[0, 1], [1, 0], [1, 1]],
                                      dtype=np.float32), "bce_logits"),
                validation.validation_layout("classification", bins=8)).sum(axis=0)
            self.assertFalse(np.array_equal(released[0], raw))

    def test_node_vision_validation_preflights_then_releases_2d_and_3d(self):
        import torch

        class FixedEncoder(torch.nn.Module):
            def __init__(self, width):
                super().__init__()
                self.width = width

            def forward(self, batch):
                axes = tuple(range(1, batch.ndim))
                return batch.mean(dim=axes)[:, None].expand(-1, self.width)

        cases = (
            ("resnet18", 2, "row"),
            ("densenet121_3d", 3, "patient"),
        )
        for backbone, n_classes, privacy_unit in cases:
            with self.subTest(backbone=backbone, unit=privacy_unit), \
                    tempfile.TemporaryDirectory() as directory:
                corrupt = os.path.join(directory, "corrupt.png")
                with open(corrupt, "wb") as handle:
                    handle.write(b"not an image")
                patient_header = ",patient" if privacy_unit == "patient" else ""
                patient_values = ", p1" if privacy_unit == "patient" else ""
                with open(os.path.join(directory, "samples.csv"), "w",
                          encoding="utf-8") as handle:
                    handle.write("relative_path,label%s\n" % patient_header)
                    handle.write("corrupt.png,0%s\n" % patient_values)
                    handle.write("corrupt.png,1%s\n" % patient_values)

                cfg = self._vision_config(backbone, n_classes=n_classes)
                manifest = {
                    **{key: value for key, value in cfg.items()
                       if key != "data-kind"},
                    "dp-track": "validation", "data_type": "image",
                    "samples_file": "samples.csv", "target_column": "label",
                    "assets": {"images": {
                        "type": "image_root", "root": directory,
                        "path_col": "relative_path"}},
                    "dp-unit": privacy_unit,
                    "patient_column": (
                        "patient" if privacy_unit == "patient" else None),
                    "patient-id-canonicalization": "trim-utf8-v2",
                    "n_units": 1 if privacy_unit == "patient" else 2,
                    "task-type": "classification",
                    "target-levels": {"type": "character", "values": [
                        "class-%d" % value for value in range(n_classes)]},
                }
                with open(os.path.join(directory, "manifest.json"), "w",
                          encoding="utf-8") as handle:
                    json.dump(manifest, handle)
                secret = os.path.join(directory, "node-secret")
                with open(secret, "w", encoding="ascii") as handle:
                    handle.write("72" * 32)
                os.chmod(secret, 0o600)
                context = type("Context", (), {
                    "node_config": {"manifest-dir": directory}})()
                model = params.load_user_model(
                    cfg, cfg["num-features"], "cross_entropy")
                private_started = [False]

                def build(name):
                    self.assertFalse(private_started[0])
                    self.assertEqual(name, backbone)
                    return FixedEncoder(cfg["num-features"]), cfg["num-features"]

                original_load = task.load_image_collection

                def load(*args, **kwargs):
                    self.assertTrue(private_started[0])
                    self.assertTrue(kwargs.get("allow_empty"))
                    return original_load(*args, **kwargs)

                with (mock.patch.dict(os.environ, {
                        "DSFLOWER_NODE_SECRET_FILE": secret}),
                      mock.patch.object(vision, "build_backbone", side_effect=build),
                      mock.patch.object(vision, "pick_device",
                                        return_value=torch.device("cpu")),
                      mock.patch.object(task, "load_image_collection",
                                        side_effect=load),
                      mock.patch.object(
                          validation, "private_validation_vector",
                          wraps=validation.private_validation_vector) as release):
                    result = validation.private_model_validation(
                        context, cfg, {"epsilon": 1.0, "delta": 1e-5}, 1,
                        params.get_torch_params(model),
                        on_private_start=lambda: private_started.__setitem__(0, True))
                self.assertTrue(private_started[0])
                self.assertEqual(release.call_count, 1)
                unit_ids = release.call_args.kwargs["unit_ids"]
                if privacy_unit == "patient":
                    np.testing.assert_array_equal(unit_ids, np.asarray(["p1", "p1"]))
                else:
                    self.assertIsNone(unit_ids)
                self.assertEqual(result[0].shape, (
                    validation.validation_layout(
                        "classification", n_classes=n_classes, bins=8)["size"],))
                self.assertTrue(bool(np.all(np.isfinite(result[0]))))

    def test_empty_vision_cohort_emits_one_noise_only_release(self):
        import torch

        class FixedEncoder(torch.nn.Module):
            def forward(self, batch):
                return torch.zeros((batch.shape[0], 512), device=batch.device)

        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "samples.csv"), "w",
                      encoding="utf-8") as handle:
                handle.write("relative_path,label\n")
            cfg = self._vision_config("resnet18")
            manifest = {
                **{key: value for key, value in cfg.items()
                   if key != "data-kind"},
                "dp-track": "validation", "data_type": "image",
                "samples_file": "samples.csv", "target_column": "label",
                "assets": {"images": {
                    "type": "image_root", "root": directory,
                    "path_col": "relative_path"}},
                "dp-unit": "row", "patient_column": None, "n_units": 0,
                "task-type": "classification",
                "target-levels": {
                    "type": "character", "values": ["no", "yes"]},
            }
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            secret = os.path.join(directory, "node-secret")
            with open(secret, "w", encoding="ascii") as handle:
                handle.write("73" * 32)
            os.chmod(secret, 0o600)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            model = params.load_user_model(cfg, 512, "cross_entropy")
            with (mock.patch.dict(os.environ, {
                    "DSFLOWER_NODE_SECRET_FILE": secret}),
                  mock.patch.object(
                      vision, "build_backbone",
                      return_value=(FixedEncoder(), 512)),
                  mock.patch.object(vision, "pick_device",
                                    return_value=torch.device("cpu")),
                  mock.patch.object(
                      vision, "extract_features_from_paths",
                      side_effect=AssertionError("empty cohort has no images")) as extract,
                  mock.patch.object(
                      validation, "private_validation_vector",
                      wraps=validation.private_validation_vector) as release):
                result = validation.private_model_validation(
                    context, cfg, {"epsilon": 1.0, "delta": 1e-5}, 1,
                    params.get_torch_params(model))
            extract.assert_not_called()
            self.assertEqual(release.call_count, 1)
            self.assertEqual(release.call_args.args[0].size, 0)
            self.assertTrue(bool(np.all(np.isfinite(result[0]))))
            self.assertFalse(bool(np.all(result[0] == 0.0)))

    def test_invalid_vision_target_levels_fail_before_private_read(self):
        cfg = self._vision_config("resnet18")
        invalid_levels = (
            {"type": "character", "values": ["only-one"]},
            {"type": "numeric", "values": [1, 1.0]},
            {"type": "logical", "values": [True, 1]},
        )
        for levels in invalid_levels:
            with self.subTest(levels=levels), \
                    tempfile.TemporaryDirectory() as directory:
                with open(os.path.join(directory, "manifest.json"), "w",
                          encoding="utf-8") as handle:
                    json.dump({
                        "data_type": "image", "target-levels": levels,
                    }, handle)
                context = type("Context", (), {
                    "node_config": {"manifest-dir": directory}})()
                with mock.patch.object(
                        task, "load_image_collection",
                        side_effect=AssertionError("private read")) as private:
                    with self.assertRaisesRegex(ValueError, "target levels"):
                        validation.private_model_validation(
                            context, cfg,
                            {"epsilon": 1.0, "delta": 1e-5}, 1,
                            [np.zeros(1, dtype=np.float32)])
                private.assert_not_called()

    def test_invalid_vision_artifact_pins_fail_before_private_read(self):
        base = self._vision_config("resnet18")
        cases = (
            ("validation-artifact-format", "other"),
            ("validation-artifact-sha256", "A" * 64),
            ("validation-artifact-size-bytes", True),
        )
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "image",
                    "target-levels": {
                        "type": "character", "values": ["no", "yes"]},
                }, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            for key, value in cases:
                callback = mock.Mock()
                with self.subTest(key=key), mock.patch.object(
                        task, "load_image_collection",
                        side_effect=AssertionError("private read")) as private:
                    with self.assertRaisesRegex(ValueError, "artifact"):
                        validation.private_model_validation(
                            context, {**base, key: value},
                            {"epsilon": 1.0, "delta": 1e-5}, 1,
                            [np.zeros(1, dtype=np.float32)],
                            on_private_start=callback)
                callback.assert_not_called()
                private.assert_not_called()

    def test_vision_model_and_backbone_tamper_fail_before_private_read(self):
        cfg = self._vision_config("resnet18")
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "image",
                    "target-levels": {
                        "type": "character", "values": ["no", "yes"]},
                }, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            model = params.load_user_model(cfg, 512, "cross_entropy")
            arrays = params.get_torch_params(model)
            cases = (
                ("arrays", [np.zeros(1, dtype=np.float32)], None),
                ("geometry", arrays, (object(), 1024)),
                ("dependency", arrays, RuntimeError("missing backbone")),
            )
            for label, supplied, build_result in cases:
                callback = mock.Mock()
                build = (mock.Mock(side_effect=build_result)
                         if isinstance(build_result, Exception)
                         else mock.Mock(return_value=build_result))
                with self.subTest(label=label), \
                        mock.patch.object(vision, "build_backbone", build), \
                        mock.patch.object(
                            task, "load_image_collection",
                            side_effect=AssertionError("private read")) as private:
                    with self.assertRaises((ValueError, RuntimeError)):
                        validation.private_model_validation(
                            context, cfg, {"epsilon": 1.0, "delta": 1e-5}, 1,
                            supplied, on_private_start=callback)
                callback.assert_not_called()
                private.assert_not_called()

    def test_node_rejects_removed_tree_validation_before_private_read(self):
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular", "feature_columns": ["x1", "x2"],
                    "patient_column": None,
                }, handle)
            cfg = {
                "validation-model-track": "trees",
                "validation-task": "binary", "validation-bins": 8,
                "num-features": 2, "num-classes": 2, "num-labels": 2,
                "loss-name": "bce_logits",
            }
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            with mock.patch(
                    "dsflower_runner.task.load_data",
                    side_effect=AssertionError("private read")) as load_data:
                with self.assertRaisesRegex(ValueError, "must be neural"):
                    validation.private_model_validation(
                        context, cfg, {"epsilon": 1.0, "delta": 1e-5},
                        1, [np.zeros(1, dtype=np.float32)])
            load_data.assert_not_called()

    def test_invalid_public_model_fails_before_private_frame_read(self):
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular", "feature_columns": ["x"],
                    "patient_column": None}, handle)
            spec = {"kind": "sequential", "layers": [
                {"op": "linear", "out": "@out"}]}
            cfg = {
                "validation-model-track": "neural",
                "validation-task": "binary", "validation-bins": 8,
                "num-features": 1, "num-classes": 2, "num-labels": 2,
                "loss-name": "bce_logits",
                "model-spec-b64": base64.b64encode(
                    json.dumps(spec).encode("utf-8")).decode("ascii"),
            }
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            with mock.patch("dsflower_runner.task.load_data",
                            side_effect=AssertionError("private read")):
                with self.assertRaises(ValueError):
                    validation.private_model_validation(
                        context, cfg, {"epsilon": 1.0, "delta": 1e-5},
                        1, [np.zeros(3, dtype=np.float32)])

    def test_invalid_public_preprocessing_fails_before_private_frame_read(self):
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular", "feature_columns": ["x"],
                    "patient_column": None}, handle)
            spec = {"kind": "sequential", "layers": [
                {"op": "linear", "out": "@out"}]}
            cfg = {
                "validation-model-track": "neural",
                "validation-task": "binary", "validation-bins": 8,
                "num-features": 1, "num-classes": 2, "num-labels": 2,
                "loss-name": "bce_logits",
                "feature-bounds": {"lower": [0.0, 1.0], "upper": [1.0, 2.0]},
                "model-spec-b64": base64.b64encode(
                    json.dumps(spec).encode("utf-8")).decode("ascii"),
            }
            model = params.load_user_model(cfg, 1, "bce_logits")
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            with mock.patch("dsflower_runner.task.load_data",
                            side_effect=AssertionError("private read")):
                with self.assertRaises(ValueError):
                    validation.private_model_validation(
                        context, cfg, {"epsilon": 1.0, "delta": 1e-5},
                        1, params.get_torch_params(model))

    def test_server_sums_all_node_releases_and_saves_no_per_node_payload(self):
        layout = validation.validation_layout("classification", bins=8)
        first = validation.validation_contributions(
            np.asarray([0, 1]), np.asarray([0.1, 0.9]), layout).sum(axis=0)
        second = validation.validation_contributions(
            np.asarray([0, 1]), np.asarray([0.2, 0.8]), layout).sum(axis=0)

        class Reply:
            def __init__(self, vector, node_id, **metrics):
                self.content = {
                    "arrays": ArrayRecord(numpy_ndarrays=[vector]),
                    "metrics": MetricRecord({
                        "num-examples": 1, **metrics}),
                }
                self.metadata = type(
                    "Metadata", (), {"src_node_id": node_id})()

            @staticmethod
            def has_error():
                return False

        class Grid:
            @staticmethod
            def get_node_ids():
                return [1, 2]

            @staticmethod
            def send_and_receive(messages, timeout):
                for message in messages:
                    config = message.content["config"]
                    if (not isinstance(config, ConfigRecord)
                            or config.get("server-round") != 1):
                        raise AssertionError(
                            "validation message has no canonical round")
                del timeout
                return [Reply(first, 1), Reply(second, 2)]

        cfg = {
            "validation-task": "binary", "validation-bins": 8,
            "validation-model-track": "neural", "num-features": 2,
            "min-train-nodes": 2,
        }
        with mock.patch(
                "dsflower_runner.validation.public_model_arrays",
                return_value=[np.zeros(1, dtype=np.float32)]):
            metrics, count, available = server_app._run_validation(Grid(), cfg)
        self.assertEqual(count, 2)
        self.assertTrue(available)
        self.assertEqual(metrics["accuracy"], 1.0)

        class DuplicateGrid(Grid):
            @staticmethod
            def send_and_receive(messages, timeout):
                del messages, timeout
                return [Reply(first, 1), Reply(second, 1)]

        with mock.patch(
                "dsflower_runner.validation.public_model_arrays",
                return_value=[np.zeros(1, dtype=np.float32)]):
            duplicate_metrics, _, duplicate_available = (
                server_app._run_validation(DuplicateGrid(), cfg))
        self.assertIsNone(duplicate_metrics)
        self.assertFalse(duplicate_available)

        class ExecutionUnavailableGrid(Grid):
            @staticmethod
            def send_and_receive(messages, timeout):
                del messages, timeout
                return [
                    Reply(first, 1),
                    Reply(second, 2, **{"execution-unavailable": 1}),
                ]

        with mock.patch(
                "dsflower_runner.validation.public_model_arrays",
                return_value=[np.zeros(1, dtype=np.float32)]):
            unavailable_metrics, _, unavailable = server_app._run_validation(
                ExecutionUnavailableGrid(), cfg)
        self.assertIsNone(unavailable_metrics)
        self.assertFalse(unavailable)

        with tempfile.TemporaryDirectory() as directory:
            server_app._save_validation(
                {**cfg, "results-dir": directory}, metrics, count, available)
            with open(os.path.join(directory, "validation.json"),
                      encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertTrue(payload["pooled_only"])
            self.assertTrue(payload["available"])
            self.assertNotIn("per_node", payload)
            self.assertNotIn("statistics", payload)

    def test_validation_noop_has_invalid_release_geometry(self):
        message = Message(
            content=RecordDict({"arrays": ArrayRecord(
                numpy_ndarrays=[np.zeros(16, dtype=np.float64)])}),
            dst_node_id=1, message_type="train")
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"dp-track": "validation"}, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory}})()
            arrays = client_app._safe_public_fallback_arrays(
                message, context, track=None)
        self.assertEqual(len(arrays), 1)
        self.assertEqual(arrays[0].shape, (1,))

    def test_server_marks_a_validation_noop_unavailable_without_metrics(self):
        class Reply:
            content = {"arrays": ArrayRecord(
                numpy_ndarrays=[np.zeros(1, dtype=np.float64)])}
            metadata = type("Metadata", (), {"src_node_id": 1})()

            @staticmethod
            def has_error():
                return False

        class Grid:
            @staticmethod
            def get_node_ids():
                return [1]

            @staticmethod
            def send_and_receive(messages, timeout):
                del messages, timeout
                return [Reply()]

        cfg = {
            "validation-task": "binary", "validation-bins": 8,
            "validation-model-track": "neural", "num-features": 2,
            "min-train-nodes": 1,
        }
        with mock.patch(
                "dsflower_runner.validation.public_model_arrays",
                return_value=[np.zeros(1, dtype=np.float32)]):
            metrics, count, available = server_app._run_validation(Grid(), cfg)
        self.assertIsNone(metrics)
        self.assertEqual(count, 1)
        self.assertFalse(available)
        with tempfile.TemporaryDirectory() as directory:
            server_app._save_validation(
                {**cfg, "results-dir": directory}, metrics, count, available)
            with open(os.path.join(directory, "validation.json"),
                      encoding="utf-8") as handle:
                payload = json.load(handle)
        self.assertFalse(payload["available"])
        self.assertNotIn("metrics", payload)

    def test_server_pooling_totalizes_extreme_finite_node_releases(self):
        layout = validation.validation_layout("classification", bins=8)

        class Reply:
            def __init__(self, node_id):
                self.content = {
                    "arrays": ArrayRecord(numpy_ndarrays=[
                        np.full(layout["size"], 1e308, dtype=np.float64)]),
                    "metrics": MetricRecord({"num-examples": 1}),
                }
                self.metadata = type(
                    "Metadata", (), {"src_node_id": node_id})()

            @staticmethod
            def has_error():
                return False

        class Grid:
            @staticmethod
            def get_node_ids():
                return [1, 2]

            @staticmethod
            def send_and_receive(messages, timeout):
                del messages, timeout
                return [Reply(1), Reply(2)]

        cfg = {
            "validation-task": "binary", "validation-bins": 8,
            "validation-model-track": "neural", "num-features": 2,
            "min-train-nodes": 2,
        }
        with (mock.patch(
                "dsflower_runner.validation.public_model_arrays",
                return_value=[np.zeros(1, dtype=np.float32)]),
              np.errstate(over="ignore", invalid="ignore", divide="ignore")):
            metrics, count, available = server_app._run_validation(Grid(), cfg)
        self.assertEqual(count, 2)
        self.assertTrue(available)
        json.dumps(metrics, allow_nan=False)

    def test_client_dispatches_validation_through_the_release_guard(self):
        message = Message(
            content=RecordDict({"arrays": ArrayRecord(
                numpy_ndarrays=[np.zeros((1, 2), dtype=np.float32),
                                np.zeros(1, dtype=np.float32)])}),
            dst_node_id=1, message_type="train")
        context = type("Context", (), {"state": RecordDict()})()
        claim = {
            "status": "new", "message_id": "validation-message",
            "release_index": 1, "num_rounds": 1,
            "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        cfg = {
            "validation-model-track": "neural",
            "validation-task": "binary", "validation-bins": 8,
        }
        private = np.arange(16, dtype=np.float64)
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value=cfg),
              mock.patch.object(client_app, "load_dp_track",
                                return_value="validation"),
              mock.patch.object(client_app, "load_privacy_config",
                                return_value={}),
              mock.patch.object(
                  client_app.validation, "private_model_validation",
                  return_value=[private]) as release):
            reply = client_app.train(message, context)
        release.assert_called_once()
        self.assertFalse(reply.has_error())
        np.testing.assert_array_equal(
            reply.content["arrays"].to_numpy_ndarrays()[0], private)

    def test_node_requires_exact_manifest_pinned_validation_config(self):
        cfg = {
            "dp-track": "validation", "validation-model-track": "neural",
            "validation-task": "binary", "validation-bins": 8,
            "validation-contract-sha256": "a" * 64,
            "num-server-rounds": 1, "num-features": 2,
            "num-classes": 2, "num-labels": 2,
            "loss-name": "bce_logits", "model-spec-b64": "e30=",
        }
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({**cfg, "data_type": "tabular"}, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory},
                "run_config": dict(cfg),
            })()
            pinned = task.load_pinned_run_config(context)
            self.assertEqual(pinned["validation-bins"], 8)
            context.run_config["validation-bins"] = 9
            with self.assertRaises(ValueError):
                task.load_pinned_run_config(context)

    def test_image_validation_pins_are_exact_before_private_read(self):
        cfg = self._vision_config("resnet18")
        cfg["dp-track"] = "validation"
        cfg["num-server-rounds"] = 1
        manifest = {
            **{key: value for key, value in cfg.items()
               if key != "data-kind"},
            "data_type": "image",
        }
        mutations = {
            "data-kind": "tabular",
            "backbone": "densenet121",
            "image-size": 17,
            "vision-extractor-profile": "wrong-profile",
            "validation-artifact-format": "other",
            "validation-artifact-sha256": "1" * 64,
            "validation-artifact-size-bytes": 2,
            "num-features": 512.0,
        }
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory},
                "run_config": dict(cfg),
            })()
            pinned = task.load_pinned_run_config(context)
            self.assertEqual(pinned["data-kind"], "image")
            for key, value in mutations.items():
                with self.subTest(key=key), mock.patch.object(
                        task, "load_image_collection",
                        side_effect=AssertionError("private read")) as private:
                    context.run_config = {**cfg, key: value}
                    with self.assertRaises(ValueError):
                        task.load_pinned_run_config(context)
                    private.assert_not_called()

    def test_tabular_neural_validation_rejects_image_only_fields(self):
        base = {
            "dp-track": "validation", "validation-model-track": "neural",
            "validation-task": "binary", "validation-bins": 8,
            "validation-contract-sha256": "a" * 64,
            "num-server-rounds": 1, "num-features": 2,
            "num-classes": 2, "num-labels": 2,
            "loss-name": "bce_logits", "model-spec-b64": "e30=",
        }
        fields = {
            "data-kind": "tabular", "backbone": "resnet18",
            "image-size": 16,
            "vision-extractor-profile":
                "dsflower-resnet18-imagenet1k-v1-extractor-v1",
            "validation-artifact-format": "pytorch-state-dict-v1",
            "validation-artifact-sha256": "0" * 64,
            "validation-artifact-size-bytes": 1,
        }
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({**base, "data_type": "tabular"}, handle)
            context = type("Context", (), {
                "node_config": {"manifest-dir": directory},
                "run_config": dict(base),
            })()
            for key, value in fields.items():
                with self.subTest(key=key):
                    context.run_config = {**base, key: value}
                    with self.assertRaisesRegex(ValueError, "image-only"):
                        task.load_pinned_run_config(context)


if __name__ == "__main__":
    unittest.main()
