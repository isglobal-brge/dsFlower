from decimal import Decimal, localcontext
from fractions import Fraction
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mechanism_v1 import (
    CONTINUOUS_PRACTICAL_V1,
    FIXED_POINT_DISCRETE_V1,
    continuous_epsilon_bound,
    continuous_gaussian_sigma,
    continuous_zcdp_allocation,
    discrete_gaussian_sigma_int,
    discrete_zcdp_allocation,
    exhaustive_max_squared_distance,
    fixed_point_sensitivity,
    histogram_groups,
    log2_delta_ceiling,
    normalized_sensitivity,
    project_hessian_bins,
)


class ContinuousAccountantTests(unittest.TestCase):
    def test_composed_rho_stays_within_requested_epsilon(self):
        for epsilon, delta, trees, depths in (
            (0.1, 1e-12, 1, 1),
            (1.0, 1e-6, 20, 4),
            (10.0, 0.1, 7, 3),
        ):
            with self.subTest(epsilon=epsilon, delta=delta):
                allocation = continuous_zcdp_allocation(
                    epsilon, delta, trees, depths
                )
                self.assertEqual(allocation.mechanism_id, CONTINUOUS_PRACTICAL_V1)
                self.assertEqual(allocation.releases, trees * depths)
                self.assertLessEqual(
                    continuous_epsilon_bound(
                        allocation.rho_per_release * allocation.releases, delta
                    ),
                    epsilon,
                )

    def test_sigma_is_conservative_and_monotone(self):
        allocation = continuous_zcdp_allocation(1.0, 1e-6, 10, 3)
        sensitivity = normalized_sensitivity("binary", 6)
        sigma = continuous_gaussian_sigma(
            sensitivity, allocation.rho_per_release
        )
        self.assertLessEqual(
            sensitivity * sensitivity / (2.0 * sigma * sigma),
            allocation.rho_per_release,
        )
        self.assertGreater(
            continuous_gaussian_sigma(
                sensitivity * 2.0, allocation.rho_per_release
            ),
            sigma,
        )

    def test_invalid_inputs_fail_closed(self):
        for args in ((0, 1e-6, 1, 1), (1, 0, 1, 1), (1, 1, 1, 1)):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    continuous_zcdp_allocation(*args)
        with self.assertRaises(ValueError):
            continuous_zcdp_allocation(1, 1e-6, 0, 1)
        with self.assertRaises(ValueError):
            continuous_gaussian_sigma(0, 1)


class DiscreteAccountantTests(unittest.TestCase):
    def test_log2_delta_ceiling_is_exact(self):
        self.assertEqual(log2_delta_ceiling(Fraction(1, 8)), 3)
        self.assertEqual(log2_delta_ceiling(Fraction(1, 10)), 4)
        for delta in (Fraction(3, 10), Fraction(1, 100_000), Fraction(7, 9)):
            bound = log2_delta_ceiling(delta)
            self.assertGreaterEqual(delta * (1 << bound), 1)
            self.assertLess(delta * (1 << (bound - 1)), 1)

    def test_allocation_is_exact_rational_and_conservative(self):
        epsilon = Fraction(3, 2)
        delta = Fraction(1, 100_000)
        allocation = discrete_zcdp_allocation(epsilon, delta, 12, 5)
        expected = epsilon**2 / (
            4 * (allocation.log2_delta_ceiling + epsilon)
        )
        self.assertEqual(allocation.mechanism_id, FIXED_POINT_DISCRETE_V1)
        self.assertEqual(allocation.rho_total, expected)
        self.assertEqual(allocation.rho_per_release * 60, expected)

        with localcontext() as context:
            context.prec = 100
            rho = Decimal(allocation.rho_total.numerator) / Decimal(
                allocation.rho_total.denominator
            )
            log_delta = -(Decimal(delta.numerator) / delta.denominator).ln()
            epsilon_bound = rho + 2 * (rho * log_delta).sqrt()
            requested = Decimal(epsilon.numerator) / epsilon.denominator
        self.assertLessEqual(epsilon_bound, requested)

    def test_integer_sigma_is_minimal(self):
        allocation = discrete_zcdp_allocation("1.0", "0.000001", 10, 3)
        sensitivity = fixed_point_sensitivity("binary", 5, 12)
        sigma = discrete_gaussian_sigma_int(
            sensitivity, allocation.rho_per_release
        )
        achieved = Fraction(sensitivity * sensitivity, 2 * sigma * sigma)
        self.assertLessEqual(achieved, allocation.rho_per_release)
        self.assertGreater(
            Fraction(sensitivity * sensitivity, 2 * (sigma - 1) ** 2),
            allocation.rho_per_release,
        )

    def test_invalid_inputs_fail_closed(self):
        for delta in (0, 1, -1):
            with self.subTest(delta=delta):
                with self.assertRaises(ValueError):
                    discrete_zcdp_allocation(1, delta, 1, 1)
        with self.assertRaises(ValueError):
            discrete_gaussian_sigma_int(1, Fraction(0))


class SensitivityOracleTests(unittest.TestCase):
    def test_root_and_later_group_geometry(self):
        self.assertEqual(histogram_groups(7, root=True), 8)
        self.assertEqual(histogram_groups(7, root=False), 7)

    def test_exhaustive_replace_one_bounds_are_tight(self):
        for task, factor in (("binary", 5), ("regression", 4)):
            for features in (1, 2, 3):
                for root in (False, True):
                    with self.subTest(task=task, features=features, root=root):
                        groups = histogram_groups(features, root)
                        maximum = exhaustive_max_squared_distance(
                            task, features, root, locations=2
                        )
                        self.assertEqual(maximum, factor * groups)
                        sensitivity = normalized_sensitivity(task, groups)
                        self.assertGreaterEqual(
                            sensitivity * sensitivity, maximum
                        )

    def test_integer_sensitivity_is_minimal_ceiling(self):
        for task, factor in (("binary", 5), ("regression", 4)):
            for groups in (1, 2, 7):
                for fractional_bits in (0, 8, 16):
                    with self.subTest(
                        task=task, groups=groups, fractional_bits=fractional_bits
                    ):
                        scale = 1 << fractional_bits
                        squared_bound = factor * groups * scale * scale
                        sensitivity = fixed_point_sensitivity(
                            task, groups, fractional_bits
                        )
                        self.assertGreaterEqual(
                            sensitivity * sensitivity, squared_bound
                        )
                        self.assertLess(
                            (sensitivity - 1) ** 2, squared_bound
                        )


class HessianProjectionTests(unittest.TestCase):
    def test_projects_onto_nonnegative_bounded_simplex(self):
        projected, missing = project_hessian_bins([3.0, -1.0, 2.0], 2.0)
        self.assertEqual(projected, [1.5, 0.0, 0.5])
        self.assertEqual(missing, 0.0)

    def test_preserves_feasible_bins_and_returns_missing_residual(self):
        projected, missing = project_hessian_bins([0.2, -0.1], 1.0)
        self.assertEqual(projected, [0.2, 0.0])
        self.assertAlmostEqual(missing, 0.8)

    def test_negative_total_maps_to_zero(self):
        projected, missing = project_hessian_bins([1.0, 2.0], -1.0)
        self.assertEqual(projected, [0.0, 0.0])
        self.assertEqual(missing, 0.0)


if __name__ == "__main__":
    unittest.main()
