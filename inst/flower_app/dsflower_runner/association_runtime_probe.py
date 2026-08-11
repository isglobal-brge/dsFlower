"""Fresh dependency-light executable probe for private association apps."""

import sys

import numpy as np

from . import association_parquet, epi_association, seeding


_FORBIDDEN_MODULES = (
    "torch", "opacus", "dsflower_runner.client_app",
    "dsflower_runner.native_tree_client_app",
    "dsflower_runner.native_tree_validation_client_app",
    "dsflower_runner.xgboost_adapter", "dsflower_runner.xgboost_bundle",
    "dsflower_runner.xgboost_native",
)


def _dependency_light():
    return not any(
        name == forbidden or name.startswith(forbidden + ".")
        for name in tuple(sys.modules) for forbidden in _FORBIDDEN_MODULES)


def probe_association_runtime():
    """Exercise synthetic sufficient-vector, Gaussian and pooling paths."""
    if not _dependency_light() or not association_parquet.runtime_ready():
        return False
    sufficient = epi_association.association_sufficient_vector(
        np.asarray([0, 1, 1, 2], dtype=np.uint8),
        np.asarray([0, 0, 1, 2], dtype=np.uint8),
        outcome_levels=(0, 1), exposure_levels=(0, 1), privacy_unit="row")
    original = seeding._node_secret
    seeding._node_secret = lambda: b"\x00" * 32
    try:
        released, sigma = epi_association.private_association_vector(
            sufficient, privacy_unit="row", epsilon=1.0, delta=1.0e-6)
    finally:
        seeding._node_secret = original
    result = epi_association.build_pooled_association_result(
        [released], [sigma], expected_nodes=1, privacy_unit="row")
    return bool(
        _dependency_light() and result.get("available") is True and
        result.get("contract") == epi_association.RESULT_CONTRACT and
        np.asarray(result.get("table_dp")).shape == (3, 3))


__all__ = ["probe_association_runtime"]
