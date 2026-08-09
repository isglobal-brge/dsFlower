"""Cross-check the runtime XGBoost fixed-point pins against the proof oracle."""

import os
import sys
import unittest
from fractions import Fraction


HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.path.join(HERE, "..", "..", "flower_app", "dsflower_runner")
REFERENCE = os.path.join(HERE, "..", "..", "..", "native", "xgboost",
                         "reference")
sys.path.insert(0, RUNNER)
sys.path.insert(0, REFERENCE)

import mechanism_v1 as oracle
import xgboost_accounting as accounting


class FixedPointAccountingTests(unittest.TestCase):
    def test_task_factors_match_exhaustive_replace_one_geometry(self):
        for task, factor in (("binary", 5), ("regression", 4)):
            for features in (1, 2):
                self.assertEqual(
                    oracle.exhaustive_max_squared_distance(
                        task, features, root=False),
                    factor * features)
                self.assertEqual(
                    oracle.exhaustive_max_squared_distance(
                        task, features, root=True),
                    factor * (features + 1))

    def test_runtime_matches_independent_oracle(self):
        for task, features, trees, depth, epsilon, delta in (
                ("binary_classification", 2, 4, 3, 1.0, 1e-6),
                ("regression", 17, 20, 5, 2.5, 1e-8),
                ("binary_classification", 1, 1, 1, 0.1, 0.01)):
            cuts = [[float(index + 1)] for index in range(features)]
            actual = accounting.fixed_point_training_pins(
                task=task, features=features, public_cuts=cuts,
                trees=trees, depth=depth, epsilon=epsilon, delta=delta,
                gradient_clip=1.5, hessian_clip=0.25)
            oracle_task = "binary" if task == "binary_classification" else "regression"
            allocation = oracle.discrete_zcdp_allocation(
                Fraction(str(epsilon)), Fraction(str(delta)), trees, depth)
            root_sensitivity = oracle.fixed_point_sensitivity(
                oracle_task, features + 1, accounting.FIXED_POINT_BITS)
            level_sensitivity = oracle.fixed_point_sensitivity(
                oracle_task, features, accounting.FIXED_POINT_BITS)
            self.assertEqual(actual["releases"], allocation.releases)
            self.assertEqual(actual["log2_delta_ceiling"],
                             allocation.log2_delta_ceiling)
            self.assertEqual(actual["fixed_point_scale"],
                             1 << accounting.FIXED_POINT_BITS)
            self.assertEqual(actual["root_sensitivity"], root_sensitivity)
            self.assertEqual(actual["level_sensitivity"], level_sensitivity)
            total_bins = sum(len(feature) + 2 for feature in cuts)
            expected_coordinates = 2 * (
                (1 << (depth - 1)) * total_bins + 1)
            self.assertEqual(actual["maximum_release_coordinates"],
                             expected_coordinates)
            self.assertEqual(
                actual["root_noise_scale"],
                oracle.discrete_gaussian_sigma_int(
                    root_sensitivity, allocation.rho_per_release))
            self.assertEqual(
                actual["level_noise_scale"],
                oracle.discrete_gaussian_sigma_int(
                    level_sensitivity, allocation.rho_per_release))

    def test_decimal_text_semantics_are_stable(self):
        as_float = accounting.fixed_point_training_pins(
            task="binary_classification", features=2,
            public_cuts=[[0.0], [1.0]], trees=4, depth=3,
            epsilon=0.1, delta=1e-6, gradient_clip=1.0,
            hessian_clip=1.0)
        as_text = accounting.fixed_point_training_pins(
            task="binary_classification", features=2,
            public_cuts=[[0.0], [1.0]], trees=4, depth=3,
            epsilon="0.1", delta="0.000001", gradient_clip="1.0",
            hessian_clip="1.0")
        self.assertEqual(as_float, as_text)

    def test_sampler_scale_and_public_geometry_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "exact sampler"):
            accounting.fixed_point_training_pins(
                task="binary_classification", features=8192,
                public_cuts=[[0.0]] * 8192, trees=10_000, depth=32,
                epsilon="1e-100", delta="1e-100", gradient_clip=1.0,
                hessian_clip=1.0)

        with self.assertRaisesRegex(ValueError, "vector"):
            accounting.fixed_point_training_pins(
                task="regression", features=1024,
                public_cuts=[[0.0]] * 1024, trees=1, depth=16,
                epsilon=1.0, delta=1e-6, gradient_clip=1.0,
                hessian_clip=1.0)

        with self.assertRaises(ValueError):
            accounting.fixed_point_training_pins(
                task="regression", features=2, public_cuts=[[0.0]],
                trees=1, depth=1, epsilon=1.0, delta=1e-6,
                gradient_clip=1.0, hessian_clip=1.0)

        with self.assertRaisesRegex(ValueError, "materialized units"):
            accounting.validate_fixed_point_unit_geometry(
                accounting.MAX_PROTOCOL_UNITS + 1, 1 << accounting.FIXED_POINT_BITS)


if __name__ == "__main__":
    unittest.main()
