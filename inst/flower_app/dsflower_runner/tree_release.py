"""Common sticky Gaussian release primitive for trusted tree adapters."""

import hashlib
import json
import math
import platform
import sys

import numpy as np

from . import dp_harness, seeding


_MAX_COORDINATES = 8_000_000
_RELEASE_DOMAIN = "tree-joint-gaussian/v1"
_NUMERIC_CONTRACT = "dsflower-tree-gaussian-numeric-v1"


def request_selection(canonical, selection=None, *, parameters=None):
    """Bind native schema and unit selections, plus the node request pins.

    The canonical backend manifest is trusted and validated by each adapter.
    Scope labels and operational resource ceilings do not select computation.
    """
    return {
        "node": {} if selection is None else selection,
        "native-tree": {
            "engine": canonical["engine"],
            "mode": canonical["mode"],
            "task": canonical["task"],
            "public-schema-sha256": canonical["public_schema"]["sha256"],
            "parameters-sha256": _policy_hash(
                canonical["engine_params"] if parameters is None else parameters),
            "unit-policy": seeding.select_config(canonical["privacy"], (
                "unit", "adjacency", "unit_canonicalization",
                "contribution_strategy", "max_rows_per_unit")),
        },
    }


def native_request_identity(canonical, selection=None, *, execution_fingerprint=None,
                            manifest=None, operation="train", fold=None):
    """Project a validated public engine contract before private materialization."""
    engine_contract = {name: canonical[name] for name in (
        "contract_version", "mode", "engine", "task", "public_schema", "engine_params")}
    if canonical["engine"] == "xgboost":
        from .xgboost_adapter import canonical_xgboost_profile
        profile = canonical_xgboost_profile(canonical)
        engine_contract["engine_params"] = {
            name: {"type": pin["type"], "value": profile.get(name, pin["value"])}
            for name, pin in canonical["engine_params"].items()}
    if selection is None and manifest is None:
        schema = canonical["public_schema"]
        target = schema["target"]
        manifest = {"feature_columns": schema["features"], "target_column": target["name"],
            "task-type": canonical["task"], "dp-unit": canonical["privacy"]["unit"],
            "patient-id-canonicalization": canonical["privacy"]["unit_canonicalization"],
            "feature-bounds": {"lower": schema["lower"], "upper": schema["upper"]},
            "target-levels": target["levels"],
            "target-bounds": None if target["lower"] is None else [target["lower"], target["upper"]]}
    config = {
        "engine_contract": engine_contract,
        "mechanism-profile": canonical["privacy"]["mechanism_params"],
        "request-selection": selection,
        "operation": operation, "fold": fold,
    }
    return seeding.request_identity(
        "native-" + canonical["engine"].replace("_", "-"), config,
        canonical["privacy"], 1, execution_fingerprint=execution_fingerprint,
        manifest=manifest)


def canonical_native_inputs(features, target, unit_ids=None):
    """Canonical source order precedes casts, pooling, binning and summation."""
    from . import canonical_units
    retained = canonical_units.source_units(features, target)
    if retained is not None:
        return np.asarray(features), np.asarray(target), unit_ids, retained
    units = canonical_units.canonicalize_arrays(
        features, target, unit_ids=unit_ids)
    order = units.row_permutation
    return (canonical_units.attach_units(np.asarray(features)[order], units),
            canonical_units.attach_units(np.asarray(target)[order], units),
            None if unit_ids is None else np.asarray(unit_ids)[order], units)


def native_binding(request, units, materialized, *, subset=None, sigma=None,
                   fixed_point_scale=None):
    targets = (materialized.target if hasattr(materialized, "target")
               else materialized._target_units)
    tensors = (materialized._binned_features, targets)
    if hasattr(materialized, "features"):
        # Native DMatrix consumes float features as well as the public bins.
        # Bind its missingness separately so the finite-array hash preserves
        # exact effective values without admitting NaNs to the release ABI.
        values = np.asarray(materialized.features)
        tensors += (np.nan_to_num(values, nan=0.0), np.isnan(values))
    return seeding.bind_private_data(
        request, units, effective_tensors=tensors,
        geometry={"n_staged_rows": len(units.row_permutation),
                  "n_privacy_units": len(units.records),
                  "accounting_n_units": len(units.records),
                  "output_sigma": sigma, "fixed_point_scale": fixed_point_scale},
        subset=subset)


def _canonical_vector(value):
    array = np.asarray(value)
    if array.dtype.hasobject or array.dtype.kind not in "iuf" or \
            array.size < 1 or array.size > _MAX_COORDINATES:
        raise ValueError("tree sufficient vector has an unsupported shape")
    canonical = np.ascontiguousarray(array, dtype="<f8")
    if not bool(np.all(np.isfinite(canonical))):
        raise ValueError("tree sufficient vector must be finite")
    if bool(np.any(canonical == 0.0)):
        canonical = canonical.copy()
        canonical[canonical == 0.0] = 0.0
    return canonical


def _policy_hash(value):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def numeric_execution_profile():
    """Public facts that domain-separate non-identical numeric runtimes."""
    return {
        "byteorder": sys.byteorder,
        "contract": _NUMERIC_CONTRACT,
        "machine": platform.machine().lower(),
        "numpy": np.__version__,
        "rng": "chacha20-box-muller-four-sample/v2",
        "system": platform.system().lower(),
    }


def joint_gaussian_release(
        value, *, mechanism, layout, epsilon, delta, sensitivity,
        num_releases, execution_fingerprint, request_selection=None,
        request_identity=None, data_binding=None):
    """Release one fixed-layout sufficient vector with semantic sticky noise.

    ``num_releases`` is the fixed transcript count accounted by the caller's
    profile.  Adaptive multi-stage tree mechanisms put a canonical
    ``release_index`` in ``layout`` and call this function exactly that many
    times.  This primitive owns canonicalization, RDP sigma calibration and PRF
    identity; adapters own sensitivity proofs and transcript geometry.  Raw
    epsilon and delta remain in the parent public request; this stage also binds
    its exact calibrated sigma and fixed release count.
    """
    if not isinstance(mechanism, str) or not mechanism or \
            not isinstance(layout, dict) or \
            not isinstance(execution_fingerprint, str) or \
            not execution_fingerprint:
        raise ValueError("tree release semantics are invalid")
    if type(num_releases) is not int or not 1 <= num_releases <= 1_000_000:
        raise ValueError("tree release count is invalid")
    try:
        sensitivity = float(sensitivity)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("tree release sensitivity is invalid") from exc
    if not math.isfinite(sensitivity) or sensitivity <= 0.0:
        raise ValueError("tree release sensitivity is invalid")
    canonical = _canonical_vector(value)
    sigma = float(dp_harness.compute_output_sigma(
        epsilon, delta, sensitivity, num_releases=num_releases))
    if not isinstance(request_identity, seeding.RequestIdentity) or not isinstance(
            data_binding, seeding.DataBinding):
        raise ValueError("tree releases require public request and private source binding")
    master = bytearray(seeding.release_key(request_identity, data_binding))
    # Adaptive stage state is bound below the parent R/B key: no future private
    # histogram or noised topology participates in the parent key derivation.
    stage = json.dumps({"mechanism": mechanism, "layout": layout,
                        "sigma": sigma, "num_releases": num_releases},
                       sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    subkey = None
    try:
        subkey = bytearray(seeding.bind_seed(
            seeding.sub_seed(master, _RELEASE_DOMAIN),
            "tree-stage/v3:" + hashlib.sha256(stage).hexdigest(), (canonical,)))
        rng = seeding.np_rng(subkey)
        subkey[:] = b"\x00" * len(subkey)
        noise = np.asarray(
            rng.normal(0.0, sigma, size=canonical.shape), dtype=np.float64)
    finally:
        if isinstance(subkey, bytearray):
            subkey[:] = b"\x00" * len(subkey)
        master[:] = b"\x00" * len(master)
    released = canonical + noise
    if released.shape != canonical.shape or not bool(np.all(np.isfinite(released))):
        raise RuntimeError("tree private release is non-finite")
    return released, float(sigma)


__all__ = ["joint_gaussian_release", "numeric_execution_profile",
           "request_selection"]
