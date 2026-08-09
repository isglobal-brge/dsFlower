"""Closed-form accounting for the data-independent DP forest profile."""

import math

from . import dp_harness


MECHANISM_PROFILE = "extra-trees/joint-gaussian-leaves/v1"
TOPOLOGY_PROFILE = "data-independent-complete-binary/v1"
LEAF_RELEASE_PROFILE = "joint-gaussian-sufficient-vector/v1"


def leaf_vector_sensitivity(task, trees):
    """Return the replace-one L2 sensitivity of the joint forest vector.

    Every effective unit reaches one leaf in every public-random tree.  Binary
    classification contributes one unit class count per tree, so replacing a
    record changes at most two coordinates per tree.  Regression contributes a
    count and a target normalized to [0, 1], giving the tight conservative
    bound two per square-root tree count.
    """
    if isinstance(trees, bool) or not isinstance(trees, int) or trees < 1:
        raise ValueError("forest tree count must be a positive integer")
    if task == "binary_classification":
        return math.sqrt(2.0 * trees)
    if task == "regression":
        return 2.0 * math.sqrt(float(trees))
    raise ValueError("forest accounting task is unsupported")


def joint_leaf_release(task, trees, epsilon, delta):
    """Return the sole Gaussian release pins for one complete forest."""
    sensitivity = leaf_vector_sensitivity(task, trees)
    sigma = dp_harness.compute_output_sigma(
        epsilon, delta, sensitivity, num_releases=1)
    return {
        "leaf_release": LEAF_RELEASE_PROFILE,
        "mechanism": MECHANISM_PROFILE,
        "num_releases": 1,
        "sensitivity": sensitivity,
        "sigma": sigma,
        "topology": TOPOLOGY_PROFILE,
    }


__all__ = [
    "LEAF_RELEASE_PROFILE",
    "MECHANISM_PROFILE",
    "TOPOLOGY_PROFILE",
    "joint_leaf_release",
    "leaf_vector_sensitivity",
]
