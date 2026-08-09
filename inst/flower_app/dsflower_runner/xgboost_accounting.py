"""Minimal exact runtime calibration for the fixed-point XGBoost profile."""

import math
from fractions import Fraction


FIXED_POINT_BITS = 20
MAX_EXACT_INTEGER = 1 << 53
MAX_VECTOR_LENGTH = 4_194_304
MAX_PROTOCOL_UNITS = 10_000_000
_INT64_MAX = (1 << 63) - 1
_TASKS = {
    # Replace-one changes a normalized gradient by at most 2Q.  Binary
    # Hessians add at most Q (4Q^2 + Q^2); regression Hessians are fixed at Q
    # and cancel (4Q^2).  The native core normalizes the server-owned gradient
    # and Hessian clips to Q before accumulating, so their magnitudes do not
    # remain in the integer sensitivity after this positive-finite check.
    "binary_classification": 5,
    "regression": 4,
}


def _positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("%s must be a positive integer" % name)
    return int(value)


def _fraction(value, name):
    if isinstance(value, bool):
        raise ValueError("%s must be a finite decimal rational" % name)
    try:
        result = Fraction(str(value))
    except (TypeError, ValueError, ZeroDivisionError) as exc:
        raise ValueError("%s must be a finite decimal rational" % name) from exc
    return result


def _ceil_sqrt(value):
    root = math.isqrt(value)
    return root if root * root == value else root + 1


def _log2_delta_ceiling(delta):
    numerator, denominator = delta.numerator, delta.denominator
    result = max(0, denominator.bit_length() - numerator.bit_length())
    while (numerator << result) < denominator:
        result += 1
    while result > 0 and (numerator << (result - 1)) >= denominator:
        result -= 1
    return result


def _sensitivity(task_factor, groups, scale):
    return _ceil_sqrt(task_factor * groups * scale * scale)


def _noise_scale(sensitivity, rho):
    numerator = sensitivity * sensitivity * rho.denominator
    denominator = 2 * rho.numerator
    return _ceil_sqrt((numerator + denominator - 1) // denominator)


def validate_fixed_point_unit_geometry(units, scale):
    """Validate the effective materialized unit count before native entry."""
    units = _positive_integer(units, "units")
    scale = _positive_integer(scale, "fixed_point_scale")
    if units > MAX_PROTOCOL_UNITS:
        raise ValueError("materialized units exceed the fixed-point profile")
    accumulator = units * scale
    if accumulator > MAX_EXACT_INTEGER or accumulator > _INT64_MAX:
        raise ValueError("fixed-point accumulator geometry is not exact")
    return accumulator


def fixed_point_training_pins(*, task, features, public_cuts, trees, depth,
                              epsilon, delta, gradient_clip, hessian_clip):
    """Derive exact public C-ABI pins for one complete T-tree training."""
    if task not in _TASKS:
        raise ValueError("fixed-point XGBoost task is unsupported")
    features = _positive_integer(features, "features")
    trees = _positive_integer(trees, "trees")
    depth = _positive_integer(depth, "depth")
    if not isinstance(public_cuts, (list, tuple)) or \
            len(public_cuts) != features or any(
                not isinstance(cuts, (list, tuple)) or not cuts
                for cuts in public_cuts):
        raise ValueError("public cuts must contain one non-empty array per feature")

    epsilon_fraction = _fraction(epsilon, "epsilon")
    delta_fraction = _fraction(delta, "delta")
    if epsilon_fraction <= 0 or not 0 < delta_fraction < 1:
        raise ValueError("require epsilon > 0 and 0 < delta < 1")
    if _fraction(gradient_clip, "gradient_clip") <= 0 or \
            _fraction(hessian_clip, "hessian_clip") <= 0:
        raise ValueError("fixed-point clips must be positive")

    scale = 1 << FIXED_POINT_BITS
    if scale > (1 << 31):
        raise ValueError("fixed-point scale exceeds the native profile")
    validate_fixed_point_unit_geometry(MAX_PROTOCOL_UNITS, scale)

    releases = trees * depth
    log_bound = _log2_delta_ceiling(delta_fraction)
    rho_total = epsilon_fraction * epsilon_fraction / (
        4 * (log_bound + epsilon_fraction))
    rho_per_release = rho_total / releases
    root_sensitivity = _sensitivity(_TASKS[task], features + 1, scale)
    level_sensitivity = _sensitivity(_TASKS[task], features, scale)
    root_noise_scale = _noise_scale(root_sensitivity, rho_per_release)
    level_noise_scale = _noise_scale(level_sensitivity, rho_per_release)
    if root_noise_scale > MAX_EXACT_INTEGER or \
            level_noise_scale > MAX_EXACT_INTEGER:
        raise ValueError("fixed-point exact sampler scale exceeds 2^53")

    total_bins = sum(len(cuts) + 2 for cuts in public_cuts)
    deepest_active_nodes = 1 << (depth - 1)
    # Match the native preflight exactly.  The runtime vector omits the extra
    # root-total pair below depth zero, but ValidatePublicGeometry deliberately
    # keeps that pair in its worst-case bound at every depth.
    maximum_coordinates = 2 * (deepest_active_nodes * total_bins + 1)
    root_coordinates = 2 * (total_bins + 1)
    if max(root_coordinates, maximum_coordinates) > MAX_VECTOR_LENGTH:
        raise ValueError("fixed-point public histogram vector exceeds its ABI bound")

    return {
        "fixed_point_bits": FIXED_POINT_BITS,
        "fixed_point_scale": scale,
        "level_noise_scale": level_noise_scale,
        "level_sensitivity": level_sensitivity,
        "log2_delta_ceiling": log_bound,
        "maximum_release_coordinates": max(root_coordinates,
                                           maximum_coordinates),
        "releases": releases,
        "root_noise_scale": root_noise_scale,
        "root_sensitivity": root_sensitivity,
    }


__all__ = [
    "FIXED_POINT_BITS",
    "MAX_EXACT_INTEGER",
    "MAX_PROTOCOL_UNITS",
    "MAX_VECTOR_LENGTH",
    "fixed_point_training_pins",
    "validate_fixed_point_unit_geometry",
]
