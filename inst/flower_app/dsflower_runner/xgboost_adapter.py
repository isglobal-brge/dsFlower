"""Server-owned boundary for the pinned native XGBoost DP profile.

The adapter validates and materializes the complete effective training input,
then derives sticky randomness with the runner's existing stateless PRF.  The
verified node-owned bundle is mandatory.  Public R/Flower routing remains
disabled until its own release gates pass; this module is only the internal
execution boundary.

Sanitization is deliberately separate from provenance: parsing native-looking
JSON cannot prove that its topology and leaves came from privatized histograms.
Only ``train_xgboost_native`` connects native output to the mandatory sanitizer.
"""

import copy
import hashlib
import json
import math

import numpy as np

from . import native_tree_contract as tree_contract
from . import seeding, xgboost_accounting, xgboost_bundle, xgboost_native
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


class MaterializedXGBoostData:
    """Effective one-record-per-unit arrays that may cross the native ABI."""

    __slots__ = ("_binned_features", "features", "privacy_unit", "target")

    def __init__(self, features, target, privacy_unit, binned_features):
        self._binned_features = binned_features
        self.features = features
        self.target = target
        self.privacy_unit = privacy_unit

    def __repr__(self):
        return "MaterializedXGBoostData(unit=%s)" % self.privacy_unit


class PreparedXGBoostTraining:
    """Validated internal request; repr intentionally excludes data and key."""

    __slots__ = (
        "_features", "_manifest", "_native_bundle", "_native_bundle_sha256",
        "_native_parameters", "_noise_key", "_num_boost_round", "_profile",
        "_request_sha256", "_sealed", "_target", "invocation_id",
    )

    def __init__(self, *, manifest, native_parameters, noise_key, materialized,
                 profile, native_bundle):
        object.__setattr__(self, "_sealed", False)
        self._manifest = copy.deepcopy(manifest)
        self._native_bundle = native_bundle
        self._native_bundle_sha256 = native_bundle.bundle_sha256
        self._native_parameters = copy.deepcopy(native_parameters)
        self._noise_key = noise_key
        self._features = materialized.features
        self._target = materialized.target
        self.invocation_id = tree_contract.invocation_identity(self._manifest)
        self._num_boost_round = profile["num_boost_round"]
        self._profile = copy.deepcopy(profile)
        self._request_sha256 = xgboost_native.request_sha256(
            self._manifest, self._profile, self._native_parameters)
        self._sealed = True

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False):
            raise AttributeError("prepared native request is frozen")
        object.__setattr__(self, name, value)

    @property
    def features(self):
        result = self._features.view()
        result.setflags(write=False)
        return result

    @property
    def target(self):
        result = self._target.view()
        result.setflags(write=False)
        return result

    @property
    def num_boost_round(self):
        return self._num_boost_round

    @property
    def profile(self):
        return copy.deepcopy(self._profile)

    @property
    def manifest(self):
        return copy.deepcopy(self._manifest)

    @property
    def native_parameters(self):
        return copy.deepcopy(self._native_parameters)

    def __repr__(self):
        return "PreparedXGBoostTraining(invocation_id=%r, trees=%d)" % (
            self.invocation_id, self._num_boost_round)


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


def _numeric_array(value, name, ndim):
    _numeric_array_shape(value, name, ndim)
    try:
        with np.errstate(over="ignore", invalid="ignore"):
            return np.array(value, dtype=np.float32, order="C", copy=True)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("%s cannot be represented as float32" % name) from exc


def _canonical_patient_id(value):
    try:
        text = ("" if value is None else str(value)).strip(_ASCII_ID_TRIM)
        encoded = text.encode("utf-8", errors="strict")
    except Exception:
        return _MISSING_PATIENT_UNIT
    if not text or text.lower() in ("na", "nan", "null", "<na>", "nat"):
        return _MISSING_PATIENT_UNIT
    if len(encoded) > _MAX_PATIENT_ID_BYTES:
        return _MISSING_PATIENT_UNIT
    return text


def _totalize_features(features, lower, upper):
    """Map every private feature value into its public float32 domain."""
    missing = np.isnan(features)
    np.copyto(features, lower, where=np.isneginf(features))
    np.copyto(features, upper, where=np.isposinf(features))
    np.maximum(features, lower, out=features, where=~missing)
    np.minimum(features, upper, out=features, where=~missing)
    features[missing] = np.float32(np.nan)
    features[features == np.float32(0.0)] = np.float32(0.0)
    return features


def _totalize_target(target, task, lower, upper):
    """Apply the public task mapping without a private-value rejection bit."""
    if task == "binary_classification":
        finite = np.isfinite(target)
        return np.where(
            finite & (target >= np.float32(0.5)),
            np.float32(1.0), np.float32(0.0),
        ).astype(np.float32, copy=False)
    midpoint = np.float32(
        float(lower) + (float(upper) - float(lower)) / 2.0)
    target = np.nan_to_num(
        target, copy=False, nan=midpoint, posinf=upper, neginf=lower)
    np.maximum(target, lower, out=target)
    np.minimum(target, upper, out=target)
    target[target == np.float32(0.0)] = np.float32(0.0)
    return target


def _patient_unit_tokens(unit_ids, rows):
    """Return only fixed-size canonical unit tokens; never retain raw IDs."""
    if unit_ids is None or not isinstance(unit_ids, (list, tuple, np.ndarray)):
        raise ValueError("patient unit identifiers must align one-to-one with rows")
    if isinstance(unit_ids, np.ndarray) and unit_ids.ndim != 1:
        raise ValueError("patient unit identifiers must align one-to-one with rows")
    if len(unit_ids) != rows:
        raise ValueError("patient unit identifiers must align one-to-one with rows")
    tokens = np.empty(rows, dtype="|S32")
    domain = b"dsflower/xgboost/patient-unit/v1\x00"
    for index, value in enumerate(unit_ids):
        unit = _canonical_patient_id(value)
        tokens[index] = hashlib.sha256(
            domain + unit.encode("utf-8", errors="strict")).digest()
    return tokens


def _canonical_group_order(tokens, features, target):
    """Order raw rows by fixed unit token and totalized float32 contents."""
    rows, columns = features.shape
    width = 32 + 4 * columns + 4
    records = np.empty((rows, width), dtype=np.uint8)
    records[:, :32] = tokens.view(np.uint8).reshape(rows, 32)
    records[:, 32:32 + 4 * columns] = np.asarray(
        features, dtype="<f4", order="C").view(np.uint8).reshape(
            rows, 4 * columns)
    records[:, 32 + 4 * columns:] = np.asarray(
        target, dtype="<f4", order="C").view(np.uint8).reshape(rows, 4)
    keys = records.view(np.dtype((np.void, width))).reshape(rows)
    return np.argsort(keys, kind="mergesort")


def _aggregate_patient_units(features, target, tokens, task, feature_lower,
                             feature_upper, target_lower, target_upper):
    """Materialize one deterministic mean/majority record per fixed token."""
    order = _canonical_group_order(tokens, features, target)
    features = np.ascontiguousarray(features[order], dtype=np.float32)
    target = np.ascontiguousarray(target[order], dtype=np.float32)
    tokens = np.ascontiguousarray(tokens[order])

    starts = np.flatnonzero(np.concatenate((
        np.asarray([True]), tokens[1:] != tokens[:-1])))
    group_counts = np.diff(np.append(starts, features.shape[0])).astype(
        np.int64, copy=False)

    observed = ~np.isnan(features)
    values = np.where(observed, features, np.float32(0.0))
    sums = np.add.reduceat(values, starts, axis=0, dtype=np.float64)
    counts = np.add.reduceat(observed, starts, axis=0, dtype=np.int64)
    means = np.full(sums.shape, np.nan, dtype=np.float64)
    np.divide(sums, counts, out=means, where=counts > 0)
    effective_features = np.asarray(means, dtype=np.float32, order="C")
    np.maximum(effective_features, feature_lower, out=effective_features,
               where=~np.isnan(effective_features))
    np.minimum(effective_features, feature_upper, out=effective_features,
               where=~np.isnan(effective_features))
    effective_features[effective_features == np.float32(0.0)] = \
        np.float32(0.0)
    effective_features[np.isnan(effective_features)] = np.float32(np.nan)

    target_sums = np.add.reduceat(target, starts, dtype=np.float64)
    if task == "binary_classification":
        effective_target = np.where(
            2.0 * target_sums > group_counts,
            np.float32(1.0), np.float32(0.0),
        ).astype(np.float32, copy=False)
    else:
        effective_target = np.asarray(
            target_sums / group_counts, dtype=np.float32)
        np.maximum(effective_target, target_lower, out=effective_target)
        np.minimum(effective_target, target_upper, out=effective_target)
        effective_target[effective_target == np.float32(0.0)] = \
            np.float32(0.0)
    return effective_features, effective_target


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


def _canonical_row_order(binned_features, target, features):
    """Sort by effective public-bin bytes followed by float32 target bytes."""
    rows, columns = binned_features.shape
    little_features = np.asarray(binned_features, dtype="<u4", order="C")
    little_target = np.asarray(target, dtype="<f4", order="C")
    little_raw_features = np.asarray(features, dtype="<f4", order="C")
    width = 8 * columns + 4
    records = np.empty((rows, width), dtype=np.uint8)
    records[:, :4 * columns] = little_features.view(
        np.uint8).reshape(rows, 4 * columns)
    records[:, 4 * columns:4 * columns + 4] = little_target.view(
        np.uint8).reshape(rows, 4)
    records[:, 4 * columns + 4:] = little_raw_features.view(
        np.uint8).reshape(rows, 4 * columns)
    keys = records.view(np.dtype((np.void, width))).reshape(rows)
    return np.argsort(keys, kind="mergesort")


def _materialization_peak_bytes(rows, columns, privacy_unit):
    """Conservative peak for canonical copies, masks, sort keys and indices."""
    per_row = 32 * int(columns) + 64
    if privacy_unit == "patient":
        # Fixed tokens, canonical group records and worst-case one-row groups.
        # Raw identifiers are never retained by the materialized object.
        per_row += 40 * int(columns) + 320
    return int(rows) * per_row


def _native_training_peak_bytes(rows, columns, privacy_unit, profile, resources):
    """Conservative co-resident peak before any private array is copied."""
    effective_rows = max(1, int(rows))
    features = int(columns)
    depth_frontier = 1 << (int(profile["max_depth"]) - 1)
    cut_count = sum(len(feature) for feature in profile["public_cuts"])
    bins_per_frontier = sum(
        len(feature) + 2 for feature in profile["public_cuts"])
    histogram_cells = depth_frontier * bins_per_frontier
    private_coordinates = 2 * (histogram_cells + 1)
    core = (
        16 * histogram_cells +
        32 * private_coordinates +
        64 * depth_frontier +
        4 * effective_rows +
        16 * (features + 1) +
        4 * cut_count +
        1024 * 1024
    )
    materialization = _materialization_peak_bytes(
        effective_rows, features, privacy_unit)
    # Retained canonical tensors plus a dense SimpleDMatrix, row offsets,
    # gradients and predictions.  This deliberately assumes no sparse saving.
    dataset = effective_rows * (24 * features + 96) + 8 * (
        effective_rows + 1)
    # Native buffer, Python copy, parsed sanitizer objects and canonical output.
    artifact = 8 * int(resources["max_artifact_bytes"])
    subtotal = core + materialization + dataset + artifact
    return subtotal + subtotal // 4 + 16 * 1024 * 1024


def materialize_xgboost_units(manifest, features, target, *, unit_ids=None):
    """Totalize raw values and freeze one effective row per privacy unit."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_xgboost_profile(canonical)
    rows, columns = _numeric_array_shape(features, "features", 2)
    target_rows, = _numeric_array_shape(target, "target", 1)
    if target_rows != rows:
        raise ValueError("target row count must match the feature matrix")
    if columns != len(canonical["public_schema"]["features"]):
        raise ValueError("feature count differs from the public schema")
    if rows > canonical["resources"]["max_rows"]:
        raise ValueError("materialized units exceed the row ceiling")
    if columns > canonical["resources"]["max_features"]:
        raise ValueError("materialized features exceed the feature ceiling")
    privacy_unit = canonical["privacy"]["unit"]
    if _native_training_peak_bytes(
            rows, columns, privacy_unit, profile, canonical["resources"]) > \
            canonical["resources"]["memory_mib"] * 1024 * 1024:
        raise ValueError("complete native training exceeds the memory ceiling")

    X = _numeric_array(features, "features", 2)
    y = _numeric_array(target, "target", 1)

    lower = np.asarray(profile["feature_lower"], dtype=np.float32)
    upper = np.asarray(profile["feature_upper"], dtype=np.float32)
    target_lower = np.float32(profile["target_lower"])
    target_upper = np.float32(profile["target_upper"])
    X = _totalize_features(X, lower, upper)
    y = _totalize_target(
        y, canonical["task"], target_lower, target_upper)

    if rows == 0:
        X = np.full((1, columns), np.nan, dtype=np.float32)
        y = np.asarray([
            np.float32(0.0) if canonical["task"] == "binary_classification"
            else np.float32(profile["base_score"])
        ], dtype=np.float32)

    if privacy_unit == "row":
        if unit_ids is not None:
            raise ValueError("row-level materialization must not carry unit identifiers")
    else:
        if rows == 0:
            if unit_ids is not None and (
                    not isinstance(unit_ids, (list, tuple, np.ndarray)) or
                    (isinstance(unit_ids, np.ndarray) and unit_ids.ndim != 1) or
                    len(unit_ids) != 0):
                raise ValueError(
                    "patient unit identifiers must align one-to-one with rows")
        else:
            tokens = _patient_unit_tokens(unit_ids, rows)
            X, y = _aggregate_patient_units(
                X, y, tokens, canonical["task"], lower, upper,
                target_lower, target_upper)

    xgboost_accounting.validate_fixed_point_unit_geometry(
        X.shape[0], profile["fixed_point_scale"])

    # Unit labels only select contribution groups.  The native matrix and sticky
    # identity contain the effective bounded records, never identifiers or raw
    # visit multiplicities.
    binned = _public_bin_indices(X, profile["public_cuts"])
    order = _canonical_row_order(binned, y, X)

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


def prepare_xgboost_training(manifest, features, target, *, native_bundle,
                             unit_ids=None):
    """Prepare one complete T-tree training and its private-bound sticky key."""
    if not xgboost_bundle.is_verified_bundle(native_bundle):
        raise ValueError("verified native XGBoost bundle is required")
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
    master = bytearray(seeding.master_seed(
        MECHANISM_PROFILE,
        semantic_config,
        privacy_policy,
        1,
        private_arrays=private_arrays,
        execution_fingerprint={
            "contract": EXECUTION_PROFILE,
            "native_bundle_sha256": native_bundle.bundle_sha256,
        },
    ))
    try:
        noise_key = bytearray(seeding.sub_seed(
            master, "xgboost/native-fixed-point-noise/v1"))
    finally:
        master[:] = b"\x00" * len(master)
    return PreparedXGBoostTraining(
        manifest=canonical,
        native_parameters=_native_parameters(canonical, profile),
        noise_key=noise_key,
        materialized=materialized,
        profile=profile,
        native_bundle=native_bundle,
    )


def train_xgboost_native(prepared):
    """Train once through the verified ABI and return canonical safe bytes."""
    if type(prepared) is not PreparedXGBoostTraining:
        raise ValueError("native XGBoost request was not prepared by dsFlower")
    return xgboost_native.train(prepared)


def _sanitizer_arguments(canonical, profile):
    return xgboost_native.fixed_sanitizer_arguments(canonical, profile)


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
    "PreparedXGBoostTraining",
    "build_xgboost_ensemble",
    "canonical_xgboost_profile",
    "materialize_xgboost_units",
    "prepare_xgboost_training",
    "sanitize_xgboost_artifact",
    "train_xgboost_native",
]
