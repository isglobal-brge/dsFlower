"""Accounting, training and sticky tests for public-bin boosting styles."""

import copy
import importlib.util
import math
import os
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dsflower_runner import boosting_accounting  # noqa: E402
from dsflower_runner import boosting_adapter  # noqa: E402
from dsflower_runner import boosting_profile  # noqa: E402
from dsflower_runner import catboost_artifact  # noqa: E402
from dsflower_runner import lightgbm_artifact  # noqa: E402
from test_boosting_artifacts import _manifest  # noqa: E402


def _training_manifest(engine, trees=3, task="binary_classification"):
    manifest = _manifest(engine, task)
    if engine == "lightgbm":
        manifest["engine_params"]["num_iterations"]["value"] = trees
        manifest["engine_params"]["num_leaves"]["value"] = 2
    else:
        manifest["engine_params"]["iterations"]["value"] = trees
        manifest["engine_params"]["depth"]["value"] = 1
    return manifest


def _data():
    features = np.asarray([
        [-4.0, 10.0], [-3.0, 15.0], [-2.0, 20.0], [-1.0, 25.0],
        [0.5, 60.0], [1.0, 65.0], [2.0, 70.0], [4.0, 90.0],
    ], dtype=np.float64)
    target = np.asarray([0, 0, 0, 0, 1, 1, 1, 1], dtype=np.float64)
    return features, target


class AccountingTests(unittest.TestCase):
    def test_replace_one_histogram_sensitivity_and_fixed_transcript(self):
        expected = math.sqrt(2.0 * max(
            4.0 * 1.0 ** 2 + 1.0 ** 2,
            2.0 * (1.0 ** 2 + 1.0 ** 2 + 1.0),
        ))
        self.assertAlmostEqual(
            boosting_accounting.histogram_sensitivity(2, 1.0, 1.0),
            expected,
        )
        self.assertEqual(
            boosting_accounting.fixed_release_count(
                "lightgbm", trees=3, num_leaves=4),
            9,
        )
        self.assertEqual(
            boosting_accounting.fixed_release_count(
                "catboost", trees=3, depth=2),
            6,
        )
        for invalid in (0, True, -1):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                boosting_accounting.histogram_sensitivity(
                    invalid, 1.0, 1.0)


class TrainingTests(unittest.TestCase):
    def test_over_cap_shape_is_rejected_before_numeric_copy(self):
        manifest = _training_manifest("lightgbm", trees=1)
        manifest["resources"]["max_rows"] = 1
        features, target = _data()
        with mock.patch.object(
                boosting_adapter, "_numeric_array",
                side_effect=AssertionError("numeric copy must not run")) as copy:
            with self.assertRaisesRegex(ValueError, "row or feature ceiling"):
                boosting_adapter.materialize_boosting_units(
                    manifest, features[:2], target[:2])
        copy.assert_not_called()

    def test_fixed_point_accumulator_fits_int64_at_the_hard_row_cap(self):
        hard_rows = boosting_profile.tree_contract.RESOURCE_HARD_CAPS["max_rows"]
        self.assertLess(
            hard_rows * boosting_adapter._FIXED_POINT_SCALE,
            np.iinfo(np.int64).max,
        )

    def _train_without_noise(self, engine, task="binary_classification"):
        manifest = _training_manifest(engine, task=task)
        features, target = _data()
        if task == "regression":
            target = np.clip(
                0.8 * features[:, 0] + 0.02 * features[:, 1], -4.0, 6.0)
        calls = []

        def exact_release(value, **kwargs):
            calls.append(copy.deepcopy(kwargs))
            return np.asarray(value, dtype=np.float64).copy(), 0.0

        with mock.patch.object(
                boosting_adapter.tree_release, "joint_gaussian_release",
                side_effect=exact_release):
            prepared = boosting_adapter.prepare_boosting_training(
                manifest, features, target)
            artifact = boosting_adapter.train_boosting(prepared)
            with self.assertRaisesRegex(ValueError, "prepared"):
                boosting_adapter.train_boosting(prepared)
        return manifest, artifact, calls

    def test_lightgbm_style_trains_asymmetric_safe_projection(self):
        manifest, artifact, calls = self._train_without_noise("lightgbm")
        sanitized, _ = lightgbm_artifact.sanitize_model(artifact, manifest)
        self.assertEqual(sanitized, artifact)
        predictor = lightgbm_artifact.parse_ensemble(
            lightgbm_artifact.build_ensemble([artifact], manifest), manifest)
        features, target = _data()
        predictions = predictor.predict(features)
        self.assertLess(
            np.mean(np.asarray(predictions)[target == 0]),
            np.mean(np.asarray(predictions)[target == 1]),
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            [call["layout"]["release_index"] for call in calls],
            [0, 1, 2],
        )
        self.assertTrue(all(call["num_releases"] == 3 for call in calls))

    def test_catboost_style_trains_oblivious_safe_projection(self):
        manifest, artifact, calls = self._train_without_noise("catboost")
        sanitized, _ = catboost_artifact.sanitize_model(artifact, manifest)
        self.assertEqual(sanitized, artifact)
        predictor = catboost_artifact.parse_ensemble(
            catboost_artifact.build_ensemble([artifact], manifest), manifest)
        features, target = _data()
        predictions = predictor.predict(features)
        self.assertLess(
            np.mean(np.asarray(predictions)[target == 0]),
            np.mean(np.asarray(predictions)[target == 1]),
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            [call["layout"]["release_index"] for call in calls],
            [0, 1, 2],
        )

    def test_both_styles_train_finite_bounded_regression(self):
        for engine, module in (
                ("lightgbm", lightgbm_artifact),
                ("catboost", catboost_artifact)):
            manifest, artifact, calls = self._train_without_noise(
                engine, "regression")
            predictor = module.parse_ensemble(
                module.build_ensemble([artifact], manifest), manifest)
            features, _target = _data()
            target = np.clip(
                0.8 * features[:, 0] + 0.02 * features[:, 1], -4.0, 6.0)
            predictions = np.asarray(predictor.predict(features))
            with self.subTest(engine=engine):
                self.assertTrue(np.all(np.isfinite(predictions)))
                self.assertGreater(np.corrcoef(predictions, target)[0, 1], 0.5)
                self.assertEqual(len(calls), 3)

    def test_private_values_are_totalized_and_patient_units_are_bounded(self):
        manifest = _training_manifest("lightgbm", trees=1)
        manifest["privacy"]["unit"] = "patient"
        features, target = _data()
        features[0] = [float("nan"), float("inf")]
        features[1] = [float("-inf"), float("nan")]
        target[0] = float("nan")
        target[1] = float("inf")
        units = ["a", "a", "b", "b", "c", "c", "d", "d"]
        with mock.patch.object(
                boosting_adapter.tree_release, "joint_gaussian_release",
                side_effect=lambda value, **_kwargs: (
                    np.asarray(value, dtype=np.float64).copy(), 0.0)):
            first = boosting_adapter.train_boosting(
                boosting_adapter.prepare_boosting_training(
                    manifest, features, target, unit_ids=units))
            order = np.asarray([7, 0, 3, 1, 6, 4, 2, 5])
            second = boosting_adapter.train_boosting(
                boosting_adapter.prepare_boosting_training(
                    manifest, features[order], target[order],
                    unit_ids=[units[index] for index in order]))
        self.assertEqual(first, second)

    def test_real_prf_noise_is_sticky_without_a_database(self):
        if importlib.util.find_spec("cryptography") is None:
            self.skipTest("cryptography is not installed in this interpreter")
        features, target = _data()
        order = np.asarray([4, 0, 7, 3, 1, 6, 2, 5])
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            secret = os.path.join(directory, "node.key")
            with open(secret, "w", encoding="ascii") as handle:
                handle.write("42" * 32)
            os.chmod(secret, 0o600)
            with mock.patch.dict(
                    os.environ, {"DSFLOWER_NODE_SECRET_FILE": secret},
                    clear=False):
                for engine in ("lightgbm", "catboost"):
                    manifest = _training_manifest(engine, trees=2)
                    first = boosting_adapter.train_boosting(
                        boosting_adapter.prepare_boosting_training(
                            manifest, features, target))
                    second = boosting_adapter.train_boosting(
                        boosting_adapter.prepare_boosting_training(
                            manifest, features[order], target[order]))
                    with self.subTest(engine=engine):
                        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
