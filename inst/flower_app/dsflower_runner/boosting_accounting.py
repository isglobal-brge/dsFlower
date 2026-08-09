"""Sensitivity and fixed-transcript accounting for public-bin boosting."""

import math


MECHANISM_PROFILE = "boosting/public-bin-joint-histogram/v1"
HISTOGRAM_RELEASE_PROFILE = "joint-gradient-hessian-count/v1"


def _positive_integer(value, where):
    if type(value) is not int or value < 1:
        raise ValueError("%s must be a positive integer" % where)
    return value


def _positive_float(value, where):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s must be finite and positive" % where)
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise ValueError("%s must be finite and positive" % where)
    return result


def histogram_sensitivity(features, gradient_clip, hessian_clip):
    """Exact conservative replace-one L2 bound for one histogram release.

    One effective unit contributes ``(g, h, 1)`` to one bin for every public
    feature.  For a replacement in different bins the squared difference is at
    most ``2*(G^2+H^2+1)`` per feature.  In the same bin the count cancels while
    signed gradients can differ by ``2G``, giving ``4G^2+H^2``.  The larger of
    the two cases is used for every feature.
    """
    features = _positive_integer(features, "feature count")
    gradient_clip = _positive_float(gradient_clip, "gradient clip")
    hessian_clip = _positive_float(hessian_clip, "hessian clip")
    per_feature = max(
        2.0 * (gradient_clip ** 2 + hessian_clip ** 2 + 1.0),
        4.0 * gradient_clip ** 2 + hessian_clip ** 2,
    )
    result = math.sqrt(float(features) * per_feature)
    if not math.isfinite(result):
        raise ValueError("histogram sensitivity is not finite")
    return result


def fixed_release_count(engine, *, trees, num_leaves=None, depth=None):
    """Return the data-independent adaptive transcript length."""
    trees = _positive_integer(trees, "tree count")
    if engine == "lightgbm":
        num_leaves = _positive_integer(num_leaves, "leaf count")
        if num_leaves < 2:
            raise ValueError("LightGBM-style leaf count must be at least two")
        releases = trees * (num_leaves - 1)
    elif engine == "catboost":
        depth = _positive_integer(depth, "tree depth")
        releases = trees * depth
    else:
        raise ValueError("boosting accounting engine is unsupported")
    if releases > 1_000_000:
        raise ValueError("boosting transcript exceeds its release ceiling")
    return releases


__all__ = [
    "HISTOGRAM_RELEASE_PROFILE",
    "MECHANISM_PROFILE",
    "fixed_release_count",
    "histogram_sensitivity",
]
