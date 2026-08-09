"""Server-owned boundary for the pinned native XGBoost DP profile.

The adapter validates and materializes the complete effective training input,
then derives sticky randomness with the runner's existing stateless PRF.  The
committed native fork is still a fail-closed scaffold, so this module does not
load a library or invoke training.  ``train_xgboost_native`` is the single
explicit activation boundary and rejects every current or unknown status.

Sanitization is deliberately separate from provenance: parsing native-looking
JSON cannot prove that its topology and leaves came from privatized histograms.
Only a future verified implementation of ``train_xgboost_native`` may connect
native output to the sanitizer and result attestation.
"""

import copy
import hashlib
import json
import math
import re

import numpy as np

from . import native_tree_contract as tree_contract
from . import seeding, xgboost_accounting
from .xgboost_sanitizer import sanitize_xgboost_json


MECHANISM_PROFILE = "xgboost/fixed-point-discrete/v1"
EXECUTION_PROFILE = "dsflower-xgboost-execution-v1"
ENSEMBLE_FORMAT = "dsflower-xgboost-ensemble-json-v1"
ENSEMBLE_CONTRACT = "dsflower-xgboost-ensemble-v1"
_MISSING_PATIENT_UNIT = "__dsflower_missing_patient_unit__"
_ASCII_ID_TRIM = " \t\r\n"
_MAX_PATIENT_ID_BYTES = 4096
_NUMERIC_ABS_CAP = tree_contract.MAX_FLOAT_ABS
_MAX_NATIVE_DEPTH = 30
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")

_REQUIRED_ENGINE_PARAMETERS = {
    "base_score": "float",
    "learning_rate": "float",
    "max_bin": "int",
    "max_delta_step": "float",
    "max_depth": "int",
    "min_child_weight": "float",
    "min_split_loss": "float",
    "num_boost_round": "int",
    "reg_alpha": "float",
    "reg_lambda": "float",
}
_MECHANISM_PARAMETERS = {
    "gradient_clip": "float",
    "hessian_clip": "float",
}


class NativeXGBoostUnavailable(RuntimeError):
    """Raised while the verified production native capability is absent."""


class MaterializedXGBoostData:
    """Effective one-record-per-unit arrays that may cross the native ABI."""

    __slots__ = ("_binned_features", "features", "privacy_unit", "target")

    def __init__(self, features, target, privacy_unit, binned_features):
        self._binned_features = binned_features
        self.features = features
        self.target = target
        self.privacy_unit = privacy_unit

    def __repr__(self):
        return "MaterializedXGBoostData(rows=%d, features=%d, unit=%s)" % (
            self.features.shape[0], self.features.shape[1],
            self.privacy_unit,
        )


class PreparedXGBoostTraining:
    """Validated internal request; repr intentionally excludes data and key."""

    __slots__ = (
        "_manifest", "_native_parameters", "_noise_key", "features",
        "target", "invocation_id",
        "num_boost_round", "profile",
    )

    def __init__(self, *, manifest, native_parameters, noise_key, materialized,
                 profile):
        self._manifest = manifest
        self._native_parameters = native_parameters
        self._noise_key = noise_key
        self.features = materialized.features
        self.target = materialized.target
        self.invocation_id = tree_contract.invocation_identity(manifest)
        self.num_boost_round = profile["num_boost_round"]
        self.profile = copy.deepcopy(profile)

    @property
    def manifest(self):
        return copy.deepcopy(self._manifest)

    @property
    def native_parameters(self):
        return copy.deepcopy(self._native_parameters)

    def __repr__(self):
        return (
            "PreparedXGBoostTraining(invocation_id=%r, "
            "rows=%d, features=%d, trees=%d)"
        ) % (
            self.invocation_id, self.features.shape[0], self.features.shape[1],
            self.num_boost_round,
        )


def _typed_value(parameters, name, expected_type):
    record = parameters[name]
    if record["type"] != expected_type:
        raise ValueError("XGBoost parameter has the wrong declared type")
    return record["value"]


def _finite_float(value, name, *, lower, upper, lower_open=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s must be a finite number" % name)
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("%s must be a finite number" % name)
    if (result <= lower if lower_open else result < lower) or result > upper:
        raise ValueError("%s is outside the supported range" % name)
    return 0.0 if result == 0.0 else result


def _positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("%s must be a positive integer" % name)
    return int(value)


def _float32(value, name):
    with np.errstate(over="ignore", invalid="ignore"):
        result = np.float32(value)
    if not bool(np.isfinite(result)):
        raise ValueError("%s must remain finite as float32" % name)
    return float(result)


def _float32_schema(schema):
    lower = tuple(_float32(value, "public feature lower bound")
                  for value in schema["lower"])
    upper = tuple(_float32(value, "public feature upper bound")
                  for value in schema["upper"])
    cuts = []
    for index, raw_cuts in enumerate(schema["cuts"]):
        feature_cuts = tuple(
            _float32(value, "public feature cut") for value in raw_cuts)
        derived_thresholds = tuple(float(np.nextafter(
            np.float32(value), np.float32(np.inf)))
            for value in feature_cuts)
        if not lower[index] < upper[index] or any(
                not lower[index] < value < upper[index]
                for value in feature_cuts) or any(
                    right <= left for left, right in
                    zip(feature_cuts, feature_cuts[1:])) or any(
                        not math.isfinite(value) or abs(value) > _NUMERIC_ABS_CAP
                        for value in derived_thresholds):
            raise ValueError(
                "public cuts and bounds must remain strict as float32")
        cuts.append(feature_cuts)
    return lower, upper, tuple(cuts)


def canonical_xgboost_profile(manifest):
    """Return the exact, narrower server-owned XGBoost V1 profile."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != "xgboost" or canonical["mode"] != "native-tight":
        raise ValueError("XGBoost adapter requires native-tight xgboost")

    parameters = canonical["engine_params"]
    required = frozenset(_REQUIRED_ENGINE_PARAMETERS)
    if frozenset(parameters) != required:
        raise ValueError("XGBoost parameter profile has unknown or missing fields")
    values = {
        name: _typed_value(parameters, name, kind)
        for name, kind in _REQUIRED_ENGINE_PARAMETERS.items()
    }
    resources = canonical["resources"]
    for name, ceiling in (
            ("num_boost_round", "max_trees"),
            ("max_depth", "max_depth"),
            ("max_bin", "max_bins")):
        values[name] = _positive_integer(values[name], name)
        if values[name] > resources[ceiling]:
            raise ValueError("XGBoost training shape exceeds a resource ceiling")
    if values["max_depth"] > _MAX_NATIVE_DEPTH:
        raise ValueError("max_depth exceeds the native XGBoost profile")

    schema = canonical["public_schema"]
    lower, upper, cuts = _float32_schema(schema)
    expected_max_bin = max(len(feature) + 1 for feature in cuts)
    if values["max_bin"] != expected_max_bin:
        raise ValueError("max_bin must be derived exactly from the public cuts")

    learning_rate = _float32(_finite_float(
        values["learning_rate"], "learning_rate",
        lower=0.0, upper=1.0, lower_open=True), "learning_rate")
    if learning_rate <= 0.0 or learning_rate > 1.0:
        raise ValueError("learning_rate is outside its float32 range")
    nonnegative = {}
    for name in ("min_child_weight", "min_split_loss", "reg_alpha"):
        nonnegative[name] = _float32(_finite_float(
            values[name], name, lower=0.0, upper=_NUMERIC_ABS_CAP), name)
    # Positive L2 regularization keeps the noisy-Hessian denominator bounded
    # away from zero.  max_delta_step is the public egress/stability leaf clip;
    # privacy itself comes from the already-private histogram.
    for name in ("max_delta_step", "reg_lambda"):
        nonnegative[name] = _float32(_finite_float(
            values[name], name, lower=0.0, upper=_NUMERIC_ABS_CAP,
            lower_open=True), name)
        if nonnegative[name] <= 0.0:
            raise ValueError("%s must remain positive as float32" % name)
    leaf_abs_cap = float(np.nextafter(
        np.float32(learning_rate) * np.float32(nonnegative["max_delta_step"]),
        np.float32(np.inf)))
    if not math.isfinite(leaf_abs_cap) or leaf_abs_cap > _NUMERIC_ABS_CAP:
        raise ValueError("derived XGBoost leaf clip is outside the numeric profile")

    target = schema["target"]
    target_lower = _float32(target["lower"], "public target lower bound")
    target_upper = _float32(target["upper"], "public target upper bound")
    if target_lower >= target_upper:
        raise ValueError("public target bounds must remain strict as float32")
    expected_base = (0.5 if canonical["task"] == "binary_classification"
                     else _float32(
                         target_lower + (target_upper - target_lower) / 2.0,
                         "derived base_score"))
    supplied_base = _finite_float(
        values["base_score"], "base_score", lower=-_NUMERIC_ABS_CAP,
        upper=_NUMERIC_ABS_CAP)
    if _float32(supplied_base, "base_score") != expected_base:
        raise ValueError("base_score differs from the server-derived public value")

    mechanism = canonical["privacy"]["mechanism_params"]
    if canonical["privacy"]["delta"] <= 0.0:
        raise ValueError("XGBoost fixed-point profile requires positive delta")
    if frozenset(mechanism) != frozenset(_MECHANISM_PARAMETERS):
        raise ValueError("XGBoost mechanism parameter profile must be exact")
    clips = {}
    for name, kind in _MECHANISM_PARAMETERS.items():
        clips[name] = _float32(_finite_float(
            _typed_value(mechanism, name, kind), name,
            lower=0.0, upper=_NUMERIC_ABS_CAP, lower_open=True), name)
        if clips[name] <= 0.0:
            raise ValueError("%s must remain positive as float32" % name)
    if canonical["task"] == "regression" and clips["hessian_clip"] != 1.0:
        raise ValueError("regression hessian_clip must equal its fixed public value")

    accounting = xgboost_accounting.fixed_point_training_pins(
        task=canonical["task"],
        features=len(schema["features"]),
        public_cuts=[list(feature) for feature in cuts],
        trees=values["num_boost_round"],
        depth=values["max_depth"],
        epsilon=canonical["privacy"]["epsilon"],
        delta=canonical["privacy"]["delta"],
        gradient_clip=clips["gradient_clip"],
        hessian_clip=clips["hessian_clip"],
    )

    return dict({
        "objective": ("binary:logistic"
                      if canonical["task"] == "binary_classification"
                      else "reg:squarederror"),
        "base_score": expected_base,
        "learning_rate": learning_rate,
        "leaf_abs_cap": leaf_abs_cap,
        "max_bin": values["max_bin"],
        "max_delta_step": nonnegative["max_delta_step"],
        "max_depth": values["max_depth"],
        "min_child_weight": nonnegative["min_child_weight"],
        "min_split_loss": nonnegative["min_split_loss"],
        "num_boost_round": values["num_boost_round"],
        "reg_alpha": nonnegative["reg_alpha"],
        "reg_lambda": nonnegative["reg_lambda"],
        "gradient_clip": clips["gradient_clip"],
        "hessian_clip": clips["hessian_clip"],
        "feature_lower": list(lower),
        "feature_upper": list(upper),
        "target_lower": target_lower,
        "target_upper": target_upper,
        "public_cuts": [list(feature) for feature in cuts],
    }, **accounting)


def _numeric_array_shape(value, name, ndim):
    if not isinstance(value, np.ndarray) or value.ndim != ndim or \
            value.dtype.hasobject or value.dtype.kind not in "iuf":
        raise ValueError("%s must be a real numeric rank-%d array" % (name, ndim))
    return value.shape


def _numeric_array(value, name, ndim, *, allow_nan=False):
    _numeric_array_shape(value, name, ndim)
    with np.errstate(over="ignore", invalid="ignore"):
        result = np.array(value, dtype=np.float32, order="C", copy=True)
    valid = ~np.isinf(result) if allow_nan else np.isfinite(result)
    if not bool(np.all(valid)):
        raise ValueError("%s must remain finite as float32" % name)
    return result


def _canonical_patient_id(value):
    if not isinstance(value, (str, np.str_)):
        return _MISSING_PATIENT_UNIT
    try:
        text = str(value).strip(_ASCII_ID_TRIM)
    except (UnicodeError, ValueError):
        return _MISSING_PATIENT_UNIT
    if not text or (len(text) <= 4 and text.lower() in (
            "na", "nan", "null", "<na>", "nat")):
        return _MISSING_PATIENT_UNIT
    if text.isascii():
        if len(text) > _MAX_PATIENT_ID_BYTES:
            return _MISSING_PATIENT_UNIT
    else:
        try:
            if len(text.encode("utf-8", errors="strict")) > \
                    _MAX_PATIENT_ID_BYTES:
                return _MISSING_PATIENT_UNIT
        except UnicodeError:
            return _MISSING_PATIENT_UNIT
    return text


def _public_bin_indices(features, public_cuts):
    """Return the exact lower-bound bins consumed by the native updater."""
    rows, columns = features.shape
    binned = np.empty((rows, columns), dtype=np.uint32, order="C")
    missing_value = np.iinfo(np.uint32).max
    for index, raw_cuts in enumerate(public_cuts):
        cuts = np.asarray(raw_cuts, dtype=np.float32)
        values = features[:, index]
        missing = np.isnan(values)
        binned[:, index] = np.searchsorted(
            cuts, values, side="left").astype(np.uint32, copy=False)
        binned[missing, index] = missing_value
    return binned


def _canonical_row_order(binned_features, target):
    """Sort by effective public-bin bytes followed by float32 target bytes."""
    rows, columns = binned_features.shape
    little_features = np.asarray(binned_features, dtype="<u4", order="C")
    little_target = np.asarray(target, dtype="<f4", order="C")
    width = 4 * columns + 4
    records = np.empty((rows, width), dtype=np.uint8)
    records[:, :4 * columns] = little_features.view(
        np.uint8).reshape(rows, 4 * columns)
    records[:, 4 * columns:] = little_target.view(
        np.uint8).reshape(rows, 4)
    keys = records.view(np.dtype((np.void, width))).reshape(rows)
    return np.argsort(keys, kind="mergesort")


def _materialization_peak_bytes(rows, columns, privacy_unit):
    """Conservative peak for canonical copies, masks, sort keys and indices."""
    per_row = 18 * int(columns) + 20
    if privacy_unit == "patient":
        # A fixed SHA-256 token plus Python set/object-table overhead.  Keeping
        # digests instead of arbitrary-length identifiers bounds this adapter
        # working set independently of identifier length.
        per_row += 192
    return int(rows) * per_row


def materialize_xgboost_units(manifest, features, target, *, unit_ids=None):
    """Copy, validate and freeze exactly one effective row per privacy unit."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_xgboost_profile(canonical)
    rows, columns = _numeric_array_shape(features, "features", 2)
    target_rows, = _numeric_array_shape(target, "target", 1)
    if rows < 1 or target_rows != rows:
        raise ValueError("target row count must match a non-empty feature matrix")
    if columns != len(canonical["public_schema"]["features"]):
        raise ValueError("feature count differs from the public schema")
    if rows > canonical["resources"]["max_rows"]:
        raise ValueError("materialized units exceed the row ceiling")
    xgboost_accounting.validate_fixed_point_unit_geometry(
        rows, profile["fixed_point_scale"])
    if columns > canonical["resources"]["max_features"]:
        raise ValueError("materialized features exceed the feature ceiling")
    privacy_unit = canonical["privacy"]["unit"]
    if _materialization_peak_bytes(rows, columns, privacy_unit) > \
            canonical["resources"]["memory_mib"] * 1024 * 1024:
        raise ValueError("materialized arrays exceed the memory ceiling")

    X = _numeric_array(features, "features", 2, allow_nan=True)
    y = _numeric_array(target, "target", 1)

    lower = np.asarray(profile["feature_lower"], dtype=np.float32)
    upper = np.asarray(profile["feature_upper"], dtype=np.float32)
    missing = np.isnan(X)
    if bool(np.any((X < lower) | (X > upper))):
        raise ValueError("features exceed their public bounds")

    target_lower = np.float32(profile["target_lower"])
    target_upper = np.float32(profile["target_upper"])
    if bool(np.any(y < target_lower)) or bool(np.any(y > target_upper)):
        raise ValueError("target exceeds its public bounds")
    if canonical["task"] == "binary_classification" and not bool(
            np.all((y == np.float32(0.0)) | (y == np.float32(1.0)))):
        raise ValueError("binary target must contain exactly zero or one")

    X[X == np.float32(0.0)] = np.float32(0.0)
    X[missing] = np.float32(np.nan)
    y[y == np.float32(0.0)] = np.float32(0.0)

    if privacy_unit == "row":
        if unit_ids is not None:
            raise ValueError("row-level materialization must not carry unit identifiers")
    else:
        if unit_ids is None or not isinstance(unit_ids, (list, tuple, np.ndarray)):
            raise ValueError("patient unit identifiers must align one-to-one with rows")
        if isinstance(unit_ids, np.ndarray) and unit_ids.ndim != 1:
            raise ValueError("patient unit identifiers must align one-to-one with rows")
        if len(unit_ids) != rows:
            raise ValueError("patient unit identifiers must align one-to-one with rows")
        seen = set()
        for value in unit_ids:
            unit = _canonical_patient_id(value)
            if unit == _MISSING_PATIENT_UNIT:
                raise ValueError("patient unit identifier is missing or invalid")
            token = hashlib.sha256(
                b"dsflower/xgboost/patient-unit/v1\x00" +
                unit.encode("utf-8", errors="strict")).digest()
            if token in seen:
                raise ValueError(
                    "duplicated patient units violate one-record-per-unit")
            seen.add(token)

    # Unit labels select contribution groups but are not a model input once the
    # one-record-per-unit contract is validated.  Canonicalise both row- and
    # patient-level data by the effective numeric records, so harmless row
    # permutations or patient relabelling cannot create a fresh noise stream.
    binned = _public_bin_indices(X, profile["public_cuts"])
    order = _canonical_row_order(binned, y)

    X = np.ascontiguousarray(X[order], dtype=np.float32)
    y = np.ascontiguousarray(y[order], dtype=np.float32)
    binned = np.ascontiguousarray(binned[order], dtype=np.uint32)

    X.setflags(write=False)
    y.setflags(write=False)
    binned.setflags(write=False)
    return MaterializedXGBoostData(
        X, y, canonical["privacy"]["unit"], binned)


def _native_parameters(canonical, profile):
    parameters = {
        "base_score": profile["base_score"],
        "booster": "gbtree",
        "boost_from_average": 0,
        "colsample_bylevel": 1.0,
        "colsample_bynode": 1.0,
        "colsample_bytree": 1.0,
        "device": "cpu",
        "disable_default_eval_metric": 1,
        "grow_policy": "depthwise",
        "learning_rate": profile["learning_rate"],
        "max_bin": profile["max_bin"],
        "max_delta_step": profile["max_delta_step"],
        "max_depth": profile["max_depth"],
        "max_leaves": 0,
        "min_child_weight": profile["min_child_weight"],
        "min_split_loss": profile["min_split_loss"],
        "multi_strategy": "one_output_per_tree",
        "nthread": canonical["resources"]["threads"],
        "num_class": 0,
        "num_parallel_tree": 1,
        "num_target": 1,
        "objective": profile["objective"],
        "process_type": "default",
        "reg_alpha": profile["reg_alpha"],
        "reg_lambda": profile["reg_lambda"],
        "sampling_method": "uniform",
        "subsample": 1.0,
        "tree_method": "hist",
        "updater": "grow_dsflower_dp_hist",
        "validate_parameters": True,
        "verbosity": 0,
    }
    return parameters


def prepare_xgboost_training(manifest, features, target, *, unit_ids=None,
                             native_bundle_sha256):
    """Prepare one complete T-tree training and its private-bound sticky key."""
    if not isinstance(native_bundle_sha256, str) or \
            _SHA256_RE.fullmatch(native_bundle_sha256) is None:
        raise ValueError("verified native bundle SHA-256 is missing or invalid")
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_xgboost_profile(canonical)
    materialized = materialize_xgboost_units(
        canonical, features, target, unit_ids=unit_ids)

    # Resources are rejection/operational ceilings and cannot create a reroll.
    # Opaque snapshot/cohort labels are likewise excluded: the effective
    # canonical private tensors below bind the complete model input directly.
    # One native invocation trains all T trees, so the sole outer runner round
    # coordinate is fixed to one; T and depth remain inside the semantic config.
    # Admission bounds are deliberately absent: once the effective records pass
    # them, widening a bound without changing bins, targets or base_score must
    # not create a fresh noise stream.
    semantic_config = {
        "contract_version": canonical["contract_version"],
        "engine": "xgboost",
        "mode": "native-tight",
        "task": canonical["task"],
        "native_profile": {
            name: copy.deepcopy(profile[name]) for name in (
                "base_score", "learning_rate", "max_bin",
                "max_delta_step", "max_depth", "min_child_weight",
                "min_split_loss", "num_boost_round", "objective",
                "public_cuts", "reg_alpha", "reg_lambda")
        },
    }
    source_privacy = canonical["privacy"]
    privacy_policy = {
        "mechanism": MECHANISM_PROFILE,
        "unit": source_privacy["unit"],
        "adjacency": source_privacy["adjacency"],
        "unit_canonicalization": source_privacy["unit_canonicalization"],
        "contribution_strategy": source_privacy["contribution_strategy"],
        "max_rows_per_unit": source_privacy["max_rows_per_unit"],
        "mechanism_params": {
            "fixed_point_scale": profile["fixed_point_scale"],
            "gradient_clip": profile["gradient_clip"],
            "hessian_clip": profile["hessian_clip"],
            "level_noise_scale": profile["level_noise_scale"],
            "releases": profile["releases"],
            "root_noise_scale": profile["root_noise_scale"],
        },
    }
    privacy_wire = json.dumps(
        privacy_policy, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    privacy_policy["policy_hash"] = hashlib.sha256(privacy_wire).hexdigest()
    private_arrays = (materialized._binned_features, materialized.target)
    master = seeding.master_seed(
        MECHANISM_PROFILE,
        semantic_config,
        privacy_policy,
        1,
        private_arrays=private_arrays,
        execution_fingerprint={
            "contract": EXECUTION_PROFILE,
            "native_bundle_sha256": native_bundle_sha256,
        },
    )
    noise_key = seeding.sub_seed(master, "xgboost/native-fixed-point-noise/v1")
    return PreparedXGBoostTraining(
        manifest=canonical,
        native_parameters=_native_parameters(canonical, profile),
        noise_key=noise_key,
        materialized=materialized,
        profile=profile,
    )


def train_xgboost_native(prepared, *, native_status=None):
    """Fail closed until a reviewed production C ABI replaces the scaffold.

    ``native_status`` is accepted only so deployment probes can feed the status
    they observed into this fixed boundary.  It is never reflected in the
    exception because native diagnostics are node-admin-only output.
    """
    if not isinstance(prepared, PreparedXGBoostTraining):
        raise ValueError("native XGBoost request was not prepared by dsFlower")
    del native_status
    raise NativeXGBoostUnavailable(
        "native XGBoost DP training is not a verified production capability")


def _sanitizer_arguments(canonical, profile):
    theoretical_nodes = profile["num_boost_round"] * (
        (1 << (profile["max_depth"] + 1)) - 1)
    # Every JSON node consumes multiple bytes.  This secondary bound prevents a
    # nonsensical public depth from turning the parser cap into a giant integer;
    # it never truncates a model and only rejects output above the byte ceiling.
    byte_bound_nodes = canonical["resources"]["max_artifact_bytes"]
    return {
        "expected_task": canonical["task"],
        "expected_features": len(canonical["public_schema"]["features"]),
        "expected_trees": profile["num_boost_round"],
        "expected_max_depth": profile["max_depth"],
        "public_cuts": profile["public_cuts"],
        "expected_base_score": profile["base_score"],
        "max_total_nodes": min(theoretical_nodes, byte_bound_nodes),
        "max_artifact_bytes": canonical["resources"]["max_artifact_bytes"],
        "numeric_abs_cap": _NUMERIC_ABS_CAP,
        "leaf_abs_cap": profile["leaf_abs_cap"],
    }


def sanitize_xgboost_artifact(manifest, artifact):
    """Sanitize bytes under the exact public profile, without attesting origin."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_xgboost_profile(canonical)
    return sanitize_xgboost_json(
        artifact, **_sanitizer_arguments(canonical, profile))


def build_xgboost_ensemble(manifest, artifacts):
    """Return a canonical mean-prediction ensemble of sanitized node models."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_xgboost_profile(canonical)
    if not isinstance(artifacts, (list, tuple)) or not artifacts:
        raise ValueError("XGBoost ensemble requires at least one model")
    byte_ceiling = canonical["resources"]["max_artifact_bytes"]
    input_bytes = 0
    for artifact in artifacts:
        if not isinstance(artifact, (bytes, bytearray, memoryview)):
            raise ValueError("XGBoost ensemble members must be JSON bytes")
        input_bytes += artifact.nbytes if isinstance(
            artifact, memoryview) else len(artifact)
        if input_bytes > byte_ceiling:
            raise ValueError("XGBoost ensemble exceeds its artifact byte ceiling")
    sanitized = []
    arguments = _sanitizer_arguments(canonical, profile)
    for artifact in artifacts:
        encoded, digest = sanitize_xgboost_json(artifact, **arguments)
        sanitized.append((digest, encoded))
    sanitized.sort(key=lambda item: (item[0], item[1]))
    container = {
        "aggregation": "mean_prediction",
        "contract": ENSEMBLE_CONTRACT,
        "engine": "xgboost",
        "models": [json.loads(encoded.decode("ascii"))
                   for _digest, encoded in sanitized],
        "public_schema_sha256": canonical["public_schema"]["sha256"],
        "task": ("binary"
                 if canonical["task"] == "binary_classification"
                 else "regression"),
        "version": 1,
    }
    encoded = json.dumps(
        container, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if len(encoded) > byte_ceiling:
        raise ValueError("XGBoost ensemble exceeds its artifact byte ceiling")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ENSEMBLE_CONTRACT",
    "ENSEMBLE_FORMAT",
    "EXECUTION_PROFILE",
    "MECHANISM_PROFILE",
    "MaterializedXGBoostData",
    "NativeXGBoostUnavailable",
    "PreparedXGBoostTraining",
    "build_xgboost_ensemble",
    "canonical_xgboost_profile",
    "materialize_xgboost_units",
    "prepare_xgboost_training",
    "sanitize_xgboost_artifact",
    "train_xgboost_native",
]
