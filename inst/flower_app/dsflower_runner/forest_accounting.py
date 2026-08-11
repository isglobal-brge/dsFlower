"""Closed-form accounting for the trusted DP forest profiles."""

import math


MECHANISM_PROFILE = "extra-trees/joint-gaussian-leaves/v1"
TOPOLOGY_PROFILE = "data-independent-complete-binary/v1"
LEAF_RELEASE_PROFILE = "joint-gaussian-sufficient-vector/v1"
RANDOM_FOREST_MECHANISM_PROFILE = \
    "random-forest/adaptive-level-gaussian/v1"
RANDOM_FOREST_HISTOGRAM_PROFILE = \
    "joint-public-bin-node-histogram/v1"
RANDOM_FOREST_LEAF_PROFILE = "joint-terminal-sufficient-vector/v1"
RANDOM_FOREST_PARTITION_PROFILE = "record-prf-disjoint-tree/v1"
RANDOM_FOREST_CANDIDATE_PROFILE = \
    "public-prf-mtry-without-replacement/v1"
RANDOM_FOREST_TRANSCRIPT_PROFILE = \
    "complete-depth-levels-plus-leaves/v1"


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
    from . import dp_harness

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


def random_forest_sensitivities(task, max_features):
    """Return replace-one L2 bounds for one RF level and its leaves.

    A fixed PRF assigns each effective record to exactly one tree.  At a split
    level it contributes to one public bin for each of ``max_features``
    data-independent candidates.  Replacing it can remove those coordinates
    from one tree/node and add them to another.  Binary records have unit norm
    per candidate; normalized regression records contribute ``(count, sum)``
    with norm at most ``sqrt(2)``.  The terminal release has one candidate.
    """
    if isinstance(max_features, bool) or not isinstance(max_features, int) or \
            max_features < 1:
        raise ValueError("random forest max_features must be positive")
    if task == "binary_classification":
        return math.sqrt(2.0 * max_features), math.sqrt(2.0)
    if task == "regression":
        return 2.0 * math.sqrt(float(max_features)), 2.0
    raise ValueError("random forest accounting task is unsupported")


def random_forest_release(task, max_features, depth, epsilon, delta):
    """Return pins for the fixed adaptive RF transcript.

    There are exactly ``depth`` joint level-histogram releases followed by one
    joint leaf release.  Each Gaussian is calibrated with the full transcript
    count, so adaptive RDP composition remains within the requested policy.
    """
    from . import dp_harness

    if isinstance(depth, bool) or not isinstance(depth, int) or depth < 1:
        raise ValueError("random forest depth must be positive")
    split_sensitivity, leaf_sensitivity = random_forest_sensitivities(
        task, max_features)
    releases = depth + 1
    return {
        "candidate_schedule": RANDOM_FOREST_CANDIDATE_PROFILE,
        "histogram_release": RANDOM_FOREST_HISTOGRAM_PROFILE,
        "leaf_release": RANDOM_FOREST_LEAF_PROFILE,
        "leaf_sensitivity": leaf_sensitivity,
        "leaf_sigma": dp_harness.compute_output_sigma(
            epsilon, delta, leaf_sensitivity, num_releases=releases),
        "mechanism": RANDOM_FOREST_MECHANISM_PROFILE,
        "num_releases": releases,
        "partition": RANDOM_FOREST_PARTITION_PROFILE,
        "split_sensitivity": split_sensitivity,
        "split_sigma": dp_harness.compute_output_sigma(
            epsilon, delta, split_sensitivity, num_releases=releases),
        "transcript": RANDOM_FOREST_TRANSCRIPT_PROFILE,
    }


__all__ = [
    "LEAF_RELEASE_PROFILE",
    "MECHANISM_PROFILE",
    "RANDOM_FOREST_CANDIDATE_PROFILE",
    "RANDOM_FOREST_HISTOGRAM_PROFILE",
    "RANDOM_FOREST_LEAF_PROFILE",
    "RANDOM_FOREST_MECHANISM_PROFILE",
    "RANDOM_FOREST_PARTITION_PROFILE",
    "RANDOM_FOREST_TRANSCRIPT_PROFILE",
    "TOPOLOGY_PROFILE",
    "joint_leaf_release",
    "leaf_vector_sensitivity",
    "random_forest_release",
    "random_forest_sensitivities",
]
