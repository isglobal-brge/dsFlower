"""Independent accounting and sensitivity oracle for dsFlower XGBoost v1.

This module is deliberately not imported by the runtime.  It specifies two
different mechanism profiles and must not be used to make the native updater
available:

* ``binary64-box-muller-practical-v1`` calibrates the ideal continuous
  Gaussian mechanism.  A keyed finite-precision Box--Muller implementation is
  only a computational/practical approximation to that ideal mechanism.
* ``fixed-point-discrete-v1`` uses exact rational accounting and integer
  sensitivity for a mathematical discrete Gaussian mechanism.  No sampler is
  implemented here.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from fractions import Fraction
import itertools
import math
from typing import Sequence


CONTINUOUS_PRACTICAL_V1 = "binary64-box-muller-practical-v1"
FIXED_POINT_DISCRETE_V1 = "fixed-point-discrete-v1"

_DECIMAL_PRECISION = 160
_TASKS = frozenset(("binary", "regression"))


@dataclass(frozen=True)
class ContinuousAllocation:
    mechanism_id: str
    releases: int
    rho_total: float
    rho_per_release: float


@dataclass(frozen=True)
class DiscreteAllocation:
    mechanism_id: str
    releases: int
    log2_delta_ceiling: int
    rho_total: Fraction
    rho_per_release: Fraction


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _task(value: str) -> str:
    if value not in _TASKS:
        raise ValueError("task must be 'binary' or 'regression'")
    return value


def _finite_float(value: float, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _fraction(value: object, name: str) -> Fraction:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite rational value")
    try:
        result = Fraction(value)
    except (TypeError, ValueError, ZeroDivisionError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite rational value") from exc
    return result


def _floor_binary64(value: Decimal) -> float:
    """Round a positive high-precision value outward toward zero."""
    candidate = float(value)
    if not math.isfinite(candidate) or candidate <= 0.0:
        raise OverflowError("positive value is not representable in binary64")
    while Decimal.from_float(candidate) > value:
        candidate = math.nextafter(candidate, 0.0)
    # One additional ulp is an outward guard for the finite Decimal evaluation.
    candidate = math.nextafter(candidate, 0.0)
    if candidate <= 0.0:
        raise OverflowError("positive value is not conservatively representable")
    return candidate


def _ceil_binary64(value: Decimal) -> float:
    """Round a positive high-precision value outward toward +infinity."""
    candidate = float(value)
    if not math.isfinite(candidate) or candidate <= 0.0:
        raise OverflowError("positive value is not representable in binary64")
    while Decimal.from_float(candidate) < value:
        candidate = math.nextafter(candidate, math.inf)
    candidate = math.nextafter(candidate, math.inf)
    if not math.isfinite(candidate):
        raise OverflowError("positive value is not conservatively representable")
    return candidate


def _continuous_rho_decimal(epsilon: float, delta: float) -> Decimal:
    epsilon = _finite_float(epsilon, "epsilon")
    delta = _finite_float(delta, "delta")
    if epsilon <= 0.0 or not 0.0 < delta < 1.0:
        raise ValueError("require epsilon > 0 and 0 < delta < 1")
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        eps = Decimal.from_float(epsilon)
        probability = Decimal.from_float(delta)
        log_delta = -probability.ln()
        root_rho = eps / ((log_delta + eps).sqrt() + log_delta.sqrt())
        return +(root_rho * root_rho)


def continuous_zcdp_allocation(
    epsilon: float, delta: float, trees: int, depths: int
) -> ContinuousAllocation:
    """Allocate the ideal continuous-Gaussian zCDP budget over ``T * D``.

    ``rho_total`` and ``rho_per_release`` are rounded down.  This oracle treats
    one joint histogram vector at each tree/depth pair as one release.
    """
    releases = _positive_int(trees, "trees") * _positive_int(depths, "depths")
    rho_exact = _continuous_rho_decimal(epsilon, delta)
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        rho_per_release = rho_exact / Decimal(releases)
    rho_total = _floor_binary64(rho_exact)
    rho_per_release = _floor_binary64(rho_per_release)
    # Ensure even the upward-rounded verifier remains within the public target.
    while continuous_epsilon_bound(rho_total, delta) > float(epsilon):
        rho_total = math.nextafter(rho_total, 0.0)
    while (
        continuous_epsilon_bound(rho_per_release * releases, delta)
        > float(epsilon)
    ):
        rho_per_release = math.nextafter(rho_per_release, 0.0)
    return ContinuousAllocation(
        mechanism_id=CONTINUOUS_PRACTICAL_V1,
        releases=releases,
        rho_total=rho_total,
        rho_per_release=rho_per_release,
    )


def continuous_epsilon_bound(rho: float, delta: float) -> float:
    """Return an upward-rounded ``rho + 2*sqrt(rho*ln(1/delta))``."""
    rho = _finite_float(rho, "rho")
    delta = _finite_float(delta, "delta")
    if rho <= 0.0 or not 0.0 < delta < 1.0:
        raise ValueError("require rho > 0 and 0 < delta < 1")
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        rho_decimal = Decimal.from_float(rho)
        log_delta = -Decimal.from_float(delta).ln()
        bound = rho_decimal + 2 * (rho_decimal * log_delta).sqrt()
    return _ceil_binary64(bound)


def normalized_sensitivity(task: str, groups: int) -> float:
    """Upward-rounded L2 sensitivity for ``groups`` normalized histograms."""
    task = _task(task)
    groups = _positive_int(groups, "groups")
    squared = (5 if task == "binary" else 4) * groups
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        value = Decimal(squared).sqrt()
    return _ceil_binary64(value)


def histogram_groups(features: int, root: bool) -> int:
    """Return ``F + 1`` at the root and ``F`` at subsequent levels."""
    features = _positive_int(features, "features")
    if not isinstance(root, bool):
        raise ValueError("root must be boolean")
    return features + int(root)


def continuous_gaussian_sigma(sensitivity: float, rho_per_release: float) -> float:
    """Return an upward-rounded ``sensitivity / sqrt(2*rho)``."""
    sensitivity = _finite_float(sensitivity, "sensitivity")
    rho_per_release = _finite_float(rho_per_release, "rho_per_release")
    if sensitivity <= 0.0 or rho_per_release <= 0.0:
        raise ValueError("sensitivity and rho_per_release must be positive")
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        sensitivity_decimal = Decimal.from_float(sensitivity)
        rho_decimal = Decimal.from_float(rho_per_release)
        sigma = sensitivity_decimal / (2 * rho_decimal).sqrt()
    return _ceil_binary64(sigma)


def log2_delta_ceiling(delta: object) -> int:
    """Compute ``ceil(log2(1/delta))`` exactly for a rational ``delta``."""
    probability = _fraction(delta, "delta")
    if not 0 < probability < 1:
        raise ValueError("require 0 < delta < 1")
    numerator, denominator = probability.numerator, probability.denominator
    result = max(0, denominator.bit_length() - numerator.bit_length())
    while (numerator << result) < denominator:
        result += 1
    while result > 0 and (numerator << (result - 1)) >= denominator:
        result -= 1
    return result


def discrete_zcdp_allocation(
    epsilon: object, delta: object, trees: int, depths: int
) -> DiscreteAllocation:
    """Return the exact rational allocation for ``fixed-point-discrete-v1``.

    With ``B = ceil(log2(1/delta))``, the conservative total allocation is
    ``epsilon**2 / (4 * (B + epsilon))``.
    """
    epsilon_fraction = _fraction(epsilon, "epsilon")
    delta_fraction = _fraction(delta, "delta")
    if epsilon_fraction <= 0 or not 0 < delta_fraction < 1:
        raise ValueError("require epsilon > 0 and 0 < delta < 1")
    releases = _positive_int(trees, "trees") * _positive_int(depths, "depths")
    log_bound = log2_delta_ceiling(delta_fraction)
    rho_total = epsilon_fraction**2 / (4 * (log_bound + epsilon_fraction))
    return DiscreteAllocation(
        mechanism_id=FIXED_POINT_DISCRETE_V1,
        releases=releases,
        log2_delta_ceiling=log_bound,
        rho_total=rho_total,
        rho_per_release=rho_total / releases,
    )


def _ceil_sqrt(value: int) -> int:
    root = math.isqrt(value)
    return root if root * root == value else root + 1


def fixed_point_sensitivity(task: str, groups: int, fractional_bits: int) -> int:
    """Return the exact integer ceiling of the fixed-point L2 bound."""
    task = _task(task)
    groups = _positive_int(groups, "groups")
    if (
        isinstance(fractional_bits, bool)
        or not isinstance(fractional_bits, int)
        or fractional_bits < 0
    ):
        raise ValueError("fractional_bits must be a non-negative integer")
    scale = 1 << fractional_bits
    squared = (5 if task == "binary" else 4) * groups * scale * scale
    return _ceil_sqrt(squared)


def discrete_gaussian_sigma_int(sensitivity: int, rho_per_release: Fraction) -> int:
    """Smallest integer sigma satisfying ``Delta^2/(2*sigma^2) <= rho``."""
    sensitivity = _positive_int(sensitivity, "sensitivity")
    rho = _fraction(rho_per_release, "rho_per_release")
    if rho <= 0:
        raise ValueError("rho_per_release must be positive")
    numerator = sensitivity * sensitivity * rho.denominator
    denominator = 2 * rho.numerator
    quotient_ceiling = (numerator + denominator - 1) // denominator
    sigma = _ceil_sqrt(quotient_ceiling)
    assert Fraction(sensitivity * sensitivity, 2 * sigma * sigma) <= rho
    assert sigma == 1 or Fraction(
        sensitivity * sensitivity, 2 * (sigma - 1) ** 2
    ) > rho
    return sigma


def exhaustive_max_squared_distance(
    task: str, features: int, root: bool, locations: int = 2
) -> int:
    """Enumerate a small extremal domain under replace-one adjacency.

    A location represents a node/bin coordinate.  Binary enumeration treats
    clipped normalized gradient and Hessian as the conservative independent
    rectangle ``[-1, 1] x [0, 1]``.  Regression fixes normalized Hessian to 1.
    """
    task = _task(task)
    features = _positive_int(features, "features")
    locations = _positive_int(locations, "locations")
    if not isinstance(root, bool):
        raise ValueError("root must be boolean")
    gradients = (-1, 0, 1)
    hessians = (0, 1) if task == "binary" else (1,)
    assignments = itertools.product(range(locations), repeat=features)
    records = tuple(itertools.product(gradients, hessians, assignments))
    maximum = 0
    for old_gradient, old_hessian, old_locations in records:
        for new_gradient, new_hessian, new_locations in records:
            squared = 0
            if root:
                squared += (new_gradient - old_gradient) ** 2
                squared += (new_hessian - old_hessian) ** 2
            for old_location, new_location in zip(old_locations, new_locations):
                if old_location == new_location:
                    squared += (new_gradient - old_gradient) ** 2
                    squared += (new_hessian - old_hessian) ** 2
                else:
                    squared += old_gradient**2 + old_hessian**2
                    squared += new_gradient**2 + new_hessian**2
            maximum = max(maximum, squared)
    return maximum


def project_hessian_bins(
    values: Sequence[float], noisy_total: float
) -> tuple[list[float], float]:
    """Project Hessian bins onto ``x >= 0, sum(x) <= max(0, total)``.

    The returned second value is the non-negative missing-bin residual.  This
    deterministic helper is post-processing only; it does not access raw data.
    """
    total = _finite_float(noisy_total, "noisy_total")
    parsed = [_finite_float(value, "Hessian bin") for value in values]
    total = max(0.0, total)
    positive = [max(0.0, value) for value in parsed]
    if not positive or sum(positive) <= total:
        projected = positive
    elif total == 0.0:
        projected = [0.0] * len(positive)
    else:
        ordered = sorted(positive, reverse=True)
        cumulative = 0.0
        rho = 0
        for index, value in enumerate(ordered, 1):
            cumulative += value
            if value > (cumulative - total) / index:
                rho = index
        theta = (sum(ordered[:rho]) - total) / rho
        projected = [max(value - theta, 0.0) for value in positive]
    projected_sum = sum(projected)
    if projected_sum > total:
        # Absorb a possible binary64 summation overshoot without touching data.
        largest = max(range(len(projected)), key=projected.__getitem__)
        projected[largest] = max(0.0, projected[largest] - (projected_sum - total))
        projected_sum = sum(projected)
    return projected, max(0.0, total - projected_sum)
