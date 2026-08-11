"""Trusted adapter for the data-independent DP ExtraTrees profile.

All split topology is drawn from the custodial PRF using only the canonical
public request.  Private data therefore affects one fixed-layout vector only:
class counts, or count plus normalized-target sum, at every terminal leaf.  A
single jointly calibrated Gaussian release privatizes that vector before any
model bytes exist. Adaptive Random Forest is implemented by the separate
random_forest_adapter and shares only the sanitized forest artifact contract.
"""

import copy
import hashlib
import hmac
import json
import math

import numpy as np

from . import (forest_accounting, native_tree_contract as tree_contract,
               seeding, tree_release, xgboost_adapter as tree_data)
from .forest_sanitizer import MODEL_CONTRACT, sanitize_forest_json


EXECUTION_PROFILE = "dsflower-extra-trees-execution-v1"
ENSEMBLE_CONTRACT = "dsflower-forest-ensemble-v1"
ENSEMBLE_FORMAT = "dsflower-forest-ensemble-json-v1"
_MAX_DEPTH = 12
_MAX_TREES = 512
_MAX_RELEASE_COORDINATES = 8_000_000
_REGRESSION_SCALE = 1 << 20
_REQUIRED_ENGINE_PARAMETERS = {
    "max_depth": "int",
    "n_estimators": "int",
}
_REQUIRED_MECHANISM_PARAMETERS = {
    "leaf_release": "string",
    "topology": "string",
}
_PREPARED_TOKEN = object()


def _typed_value(parameters, name, expected_type):
    record = parameters[name]
    if record["type"] != expected_type:
        raise ValueError("ExtraTrees parameter has the wrong declared type")
    return record["value"]


def _positive_integer(value, name, upper):
    if isinstance(value, bool) or not isinstance(value, int) or not (
            1 <= value <= upper):
        raise ValueError("%s is outside the ExtraTrees profile" % name)
    return int(value)


def canonical_extra_trees_profile(manifest):
    """Return the exact server-owned ExtraTrees V1 execution profile."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != "extra_trees" or \
            canonical["mode"] != "native-tight":
        raise ValueError("ExtraTrees adapter requires native-tight extra_trees")
    parameters = canonical["engine_params"]
    if frozenset(parameters) != frozenset(_REQUIRED_ENGINE_PARAMETERS):
        raise ValueError("ExtraTrees parameter profile has unknown or missing fields")
    values = {
        name: _typed_value(parameters, name, kind)
        for name, kind in _REQUIRED_ENGINE_PARAMETERS.items()
    }
    trees = _positive_integer(values["n_estimators"],
                              "n_estimators", _MAX_TREES)
    depth = _positive_integer(values["max_depth"], "max_depth", _MAX_DEPTH)
    resources = canonical["resources"]
    if trees > resources["max_trees"] or depth > resources["max_depth"]:
        raise ValueError("ExtraTrees training shape exceeds a resource ceiling")

    schema = canonical["public_schema"]
    lower, upper, cuts = tree_data._float32_schema(schema)
    if not cuts or any(not feature for feature in cuts):
        raise ValueError("ExtraTrees requires complete non-empty public cuts")
    target = schema["target"]
    target_lower = tree_data._float32(
        target["lower"], "public target lower bound")
    target_upper = tree_data._float32(
        target["upper"], "public target upper bound")
    if target_lower >= target_upper:
        raise ValueError("ExtraTrees target bounds must remain strict as float32")

    mechanism = canonical["privacy"]["mechanism_params"]
    if frozenset(mechanism) != frozenset(_REQUIRED_MECHANISM_PARAMETERS):
        raise ValueError("ExtraTrees mechanism parameter profile must be exact")
    topology = _typed_value(mechanism, "topology", "string")
    release = _typed_value(mechanism, "leaf_release", "string")
    if topology != forest_accounting.TOPOLOGY_PROFILE or \
            release != forest_accounting.LEAF_RELEASE_PROFILE:
        raise ValueError("ExtraTrees mechanism profile differs from its fixed pins")

    leaves = 1 << depth
    coordinates = 2 * trees * leaves
    if coordinates > _MAX_RELEASE_COORDINATES:
        raise ValueError("ExtraTrees leaf vector exceeds its execution ceiling")
    accounting = forest_accounting.joint_leaf_release(
        canonical["task"], trees, canonical["privacy"]["epsilon"],
        canonical["privacy"]["delta"])
    return dict({
        "engine": "extra_trees",
        "feature_lower": list(lower),
        "feature_upper": list(upper),
        "max_depth": depth,
        "n_estimators": trees,
        "public_cuts": [list(feature) for feature in cuts],
        "release_coordinates": coordinates,
        "target_lower": target_lower,
        "target_upper": target_upper,
    }, **accounting)


class MaterializedForestData:
    """One bounded record per privacy unit, represented only by public bins."""

    __slots__ = ("_binned_features", "privacy_unit", "target")

    def __init__(self, binned_features, target, privacy_unit):
        self._binned_features = binned_features
        self.target = target
        self.privacy_unit = privacy_unit

    def __repr__(self):
        return "MaterializedForestData(unit=%s)" % self.privacy_unit


def _memory_peak_bytes(rows, columns, profile):
    topology = profile["n_estimators"] * (
        (1 << profile["max_depth"]) - 1)
    release = profile["release_coordinates"]
    private = rows * (20 * columns + 96)
    working = rows * 40 + topology * 32 + release * 32
    subtotal = private + working + 8 * 1024 * 1024
    return subtotal + subtotal // 4


def materialize_forest_units(manifest, features, target, *, unit_ids=None):
    """Totalize raw values and materialize one deterministic bounded unit."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_extra_trees_profile(canonical)
    rows, columns = tree_data._numeric_array_shape(features, "features", 2)
    target_rows, = tree_data._numeric_array_shape(target, "target", 1)
    if target_rows != rows:
        raise ValueError("target row count must match the feature matrix")
    if columns != len(canonical["public_schema"]["features"]):
        raise ValueError("feature count differs from the public schema")
    resources = canonical["resources"]
    if rows > resources["max_rows"] or columns > resources["max_features"]:
        raise ValueError("forest data exceeds a row or feature ceiling")
    if _memory_peak_bytes(rows, columns, profile) > \
            resources["memory_mib"] * 1024 * 1024:
        raise ValueError("complete ExtraTrees training exceeds the memory ceiling")

    X = tree_data._numeric_array(features, "features", 2)
    y = tree_data._numeric_array(target, "target", 1)
    lower = np.asarray(profile["feature_lower"], dtype=np.float32)
    upper = np.asarray(profile["feature_upper"], dtype=np.float32)
    target_lower = np.float32(profile["target_lower"])
    target_upper = np.float32(profile["target_upper"])
    X = tree_data._totalize_features(X, lower, upper)
    y = tree_data._totalize_target(
        y, canonical["task"], target_lower, target_upper)

    privacy_unit = canonical["privacy"]["unit"]
    if privacy_unit == "row":
        if unit_ids is not None:
            raise ValueError("row-level materialization must not carry unit identifiers")
    elif rows:
        tokens = tree_data._patient_unit_tokens(unit_ids, rows)
        X, y = tree_data._aggregate_patient_units(
            X, y, tokens, canonical["task"], lower, upper,
            target_lower, target_upper)
    elif unit_ids is not None and (
            not isinstance(unit_ids, (list, tuple, np.ndarray)) or
            (isinstance(unit_ids, np.ndarray) and unit_ids.ndim != 1) or
            len(unit_ids) != 0):
        raise ValueError("patient unit identifiers must align one-to-one with rows")

    binned = tree_data._public_bin_indices(X, profile["public_cuts"])
    binned = np.ascontiguousarray(binned, dtype=np.uint32)
    y = np.ascontiguousarray(y, dtype=np.float32)
    binned.setflags(write=False)
    y.setflags(write=False)
    return MaterializedForestData(binned, y, privacy_unit)


def _policy_hash(value):
    encoded = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _topology(profile, features):
    config = {
        "cut_counts": [len(value) for value in profile["public_cuts"]],
        "depth": profile["max_depth"],
        "engine": "extra_trees",
        "features": features,
        "trees": profile["n_estimators"],
    }
    policy = {
        "mechanism": "public-random-topology",
        "profile": forest_accounting.TOPOLOGY_PROFILE,
    }
    policy["policy_hash"] = _policy_hash(policy)
    master = bytearray(seeding.master_seed(
        "extra-trees/public-topology/v1", config, policy, 1,
        execution_fingerprint="dsflower-extra-trees-topology-v1"))
    subkey = None
    try:
        subkey = bytearray(seeding.sub_seed(master, "complete-topology/v1"))
        rng = seeding.np_rng(subkey)
        subkey[:] = b"\x00" * len(subkey)
    finally:
        if isinstance(subkey, bytearray):
            subkey[:] = b"\x00" * len(subkey)
        master[:] = b"\x00" * len(master)
    internal = (1 << profile["max_depth"]) - 1
    cut_counts = [len(value) for value in profile["public_cuts"]]
    trees = []
    for _tree in range(profile["n_estimators"]):
        split_features = []
        split_cuts = []
        default_left = []
        for _node in range(internal):
            feature = rng._randbelow(features)
            split_features.append(feature)
            split_cuts.append(rng._randbelow(cut_counts[feature]))
            default_left.append(bool(rng._randbelow(2)))
        trees.append({
            "cut_indices": split_cuts,
            "default_left": default_left,
            "features": split_features,
        })
    return trees


def _leaf_indices(binned, topology, depth):
    rows = binned.shape[0]
    nodes = np.zeros(rows, dtype=np.int64)
    row_index = np.arange(rows, dtype=np.int64)
    missing = np.iinfo(np.uint32).max
    features = np.asarray(topology["features"], dtype=np.int64)
    cuts = np.asarray(topology["cut_indices"], dtype=np.uint32)
    defaults = np.asarray(topology["default_left"], dtype=np.bool_)
    for _level in range(depth):
        node_features = features[nodes]
        values = binned[row_index, node_features]
        go_left = np.where(
            values == missing, defaults[nodes], values <= cuts[nodes])
        nodes = 2 * nodes + 1 + np.logical_not(go_left).astype(np.int64)
    return nodes - ((1 << depth) - 1)


def _sufficient_vector(materialized, topology, canonical, profile):
    trees = profile["n_estimators"]
    leaves = 1 << profile["max_depth"]
    stats = np.zeros((trees, leaves, 2), dtype=np.float64)
    if canonical["task"] == "binary_classification":
        labels = np.asarray(materialized.target >= np.float32(0.5),
                            dtype=np.int64)
        for index, tree in enumerate(topology):
            leaf = _leaf_indices(
                materialized._binned_features, tree, profile["max_depth"])
            for label in (0, 1):
                stats[index, :, label] = np.bincount(
                    leaf[labels == label], minlength=leaves)[:leaves]
    else:
        width = profile["target_upper"] - profile["target_lower"]
        normalized = np.clip(
            (materialized.target.astype(np.float64) - profile["target_lower"])
            / width, 0.0, 1.0)
        quantized = np.rint(normalized * _REGRESSION_SCALE).astype(np.int64)
        for index, tree in enumerate(topology):
            leaf = _leaf_indices(
                materialized._binned_features, tree, profile["max_depth"])
            stats[index, :, 0] = np.bincount(
                leaf, minlength=leaves)[:leaves]
            stats[index, :, 1] = np.bincount(
                leaf, weights=quantized.astype(np.float64, copy=False),
                minlength=leaves)[:leaves] / _REGRESSION_SCALE
    return np.ascontiguousarray(stats, dtype=np.float64)


def _topology_hash(topology):
    encoded = json.dumps(
        topology, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _release_layout(topology, canonical, profile):
    layout = {
        "depth": profile["max_depth"],
        "engine": "extra_trees",
        "release_coordinates": profile["release_coordinates"],
        "task": canonical["task"],
        "topology_sha256": _topology_hash(topology),
        "trees": profile["n_estimators"],
    }
    return layout


class PreparedExtraTreesTraining:
    """Frozen one-shot forest release; repr excludes private statistics."""

    __slots__ = ("_canonical", "_digest", "_profile", "_sealed", "_stats",
                 "_topology", "_used")

    def __init__(self, token, canonical, profile, topology, stats):
        if token is not _PREPARED_TOKEN:
            raise TypeError("use prepare_extra_trees_training")
        object.__setattr__(self, "_sealed", False)
        self._canonical = copy.deepcopy(canonical)
        self._profile = copy.deepcopy(profile)
        self._topology = copy.deepcopy(topology)
        self._stats = stats
        self._used = False
        self._digest = _prepared_digest(
            self._canonical, self._profile, self._topology, self._stats)
        self._sealed = True

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False):
            raise AttributeError("prepared ExtraTrees request is frozen")
        object.__setattr__(self, name, value)

    def __repr__(self):
        return "PreparedExtraTreesTraining(trees=%d, depth=%d)" % (
            self._profile["n_estimators"], self._profile["max_depth"])


def _prepared_digest(canonical, profile, topology, stats):
    digest = hashlib.sha256()
    for label, value in (
            (b"canonical", canonical), (b"profile", profile),
            (b"topology", topology)):
        payload = json.dumps(
            value, ensure_ascii=True, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        digest.update(len(label).to_bytes(4, "big"))
        digest.update(label)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    array = np.asarray(stats)
    if array.dtype != np.float64 or not array.flags.c_contiguous or \
            not bool(np.all(np.isfinite(array))):
        raise ValueError("prepared ExtraTrees statistics are invalid")
    canonical_stats = np.ascontiguousarray(array, dtype="<f8")
    shape = json.dumps(list(array.shape), separators=(",", ":")).encode("ascii")
    digest.update(len(shape).to_bytes(8, "big"))
    digest.update(shape)
    raw = memoryview(canonical_stats).cast("B")
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)
    return digest.hexdigest()


def _validate_prepared(prepared):
    if type(prepared) is not PreparedExtraTreesTraining or \
            getattr(prepared, "_sealed", False) is not True or \
            getattr(prepared, "_used", None) is not False:
        raise ValueError("ExtraTrees request was not prepared by dsFlower")
    try:
        canonical = tree_contract.canonical_engine_manifest(prepared._canonical)
        profile = canonical_extra_trees_profile(canonical)
        expected_shape = (
            profile["n_estimators"], 1 << profile["max_depth"], 2)
        if canonical != prepared._canonical or profile != prepared._profile or \
                type(prepared._stats) is not np.ndarray or \
                prepared._stats.shape != expected_shape or \
                prepared._stats.dtype != np.float64 or \
                prepared._stats.flags.writeable or \
                not prepared._stats.flags.c_contiguous:
            raise ValueError("prepared ExtraTrees request changed")
        actual = _prepared_digest(
            canonical, profile, prepared._topology, prepared._stats)
        if not isinstance(prepared._digest, str) or not hmac.compare_digest(
                prepared._digest, actual):
            raise ValueError("prepared ExtraTrees request changed")
    except Exception as exc:
        raise ValueError("ExtraTrees request was not prepared by dsFlower") from exc
    return canonical, profile


def prepare_extra_trees_training(manifest, features, target, *, unit_ids=None):
    """Prepare the sole private sufficient vector and sticky release key."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_extra_trees_profile(canonical)
    materialized = materialize_forest_units(
        canonical, features, target, unit_ids=unit_ids)
    topology = _topology(
        profile, len(canonical["public_schema"]["features"]))
    stats = _sufficient_vector(materialized, topology, canonical, profile)
    stats.setflags(write=False)
    return PreparedExtraTreesTraining(
        _PREPARED_TOKEN, canonical, profile, topology, stats)


def _sanitizer_arguments(canonical, profile):
    return {
        "expected_engine": "extra_trees",
        "expected_task": canonical["task"],
        "expected_features": len(canonical["public_schema"]["features"]),
        "expected_trees": profile["n_estimators"],
        "expected_depth": profile["max_depth"],
        "public_cut_counts": [len(value) for value in profile["public_cuts"]],
        "public_schema_sha256": canonical["public_schema"]["sha256"],
        "target_lower": profile["target_lower"],
        "target_upper": profile["target_upper"],
        "max_artifact_bytes": canonical["resources"]["max_artifact_bytes"],
    }


def train_extra_trees(prepared):
    """Consume one prepared request and return only sanitized model bytes."""
    canonical, profile = _validate_prepared(prepared)
    object.__setattr__(prepared, "_used", True)
    try:
        released, sigma = tree_release.joint_gaussian_release(
            prepared._stats,
            mechanism=forest_accounting.MECHANISM_PROFILE,
            layout=_release_layout(prepared._topology, canonical, profile),
            epsilon=canonical["privacy"]["epsilon"],
            delta=canonical["privacy"]["delta"],
            sensitivity=profile["sensitivity"], num_releases=1,
            execution_fingerprint=EXECUTION_PROFILE)
        if sigma != profile["sigma"]:
            raise RuntimeError("ExtraTrees accountant and release sigma differ")
        trees = []
        if canonical["task"] == "binary_classification":
            negative = np.maximum(released[:, :, 0], 0.0)
            positive = np.maximum(released[:, :, 1], 0.0)
            leaves = (positive + 1.0) / (negative + positive + 2.0)
        else:
            counts = np.maximum(released[:, :, 0], 0.0)
            sums = np.minimum(np.maximum(released[:, :, 1], 0.0), counts)
            normalized = (sums + 0.5) / (counts + 1.0)
            leaves = profile["target_lower"] + normalized * (
                profile["target_upper"] - profile["target_lower"])
        for topology, values in zip(prepared._topology, leaves):
            trees.append(dict(topology, leaf_values=[
                float(value) for value in values]))
        model = {
            "contract": MODEL_CONTRACT,
            "depth": profile["max_depth"],
            "engine": "extra_trees",
            "num_features": len(canonical["public_schema"]["features"]),
            "public_schema_sha256": canonical["public_schema"]["sha256"],
            "task": ("binary" if canonical["task"] == "binary_classification"
                     else "regression"),
            "trees": trees,
            "version": 1,
        }
        raw = json.dumps(
            model, ensure_ascii=True, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        return sanitize_forest_json(
            raw, **_sanitizer_arguments(canonical, profile))[0]
    finally:
        object.__setattr__(prepared, "_stats", np.empty((0,), dtype=np.float64))


def sanitize_extra_trees_artifact(manifest, artifact):
    """Re-sanitize bytes under their exact public profile."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_extra_trees_profile(canonical)
    return sanitize_forest_json(
        artifact, **_sanitizer_arguments(canonical, profile))


def build_extra_trees_ensemble(manifest, artifacts):
    """Build a canonical mean-prediction ensemble of private node models."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_extra_trees_profile(canonical)
    if not isinstance(artifacts, (list, tuple)) or not artifacts:
        raise ValueError("ExtraTrees ensemble requires at least one model")
    byte_cap = canonical["resources"]["max_artifact_bytes"]
    total = 0
    sanitized = []
    arguments = _sanitizer_arguments(canonical, profile)
    for artifact in artifacts:
        if not isinstance(artifact, (bytes, bytearray, memoryview)):
            raise ValueError("ExtraTrees ensemble members must be JSON bytes")
        total += artifact.nbytes if isinstance(artifact, memoryview) else len(artifact)
        if total > byte_cap:
            raise ValueError("ExtraTrees ensemble exceeds its byte ceiling")
        encoded, digest = sanitize_forest_json(artifact, **arguments)
        sanitized.append((digest, encoded))
    sanitized.sort(key=lambda item: (item[0], item[1]))
    container = {
        "aggregation": "mean_prediction",
        "contract": ENSEMBLE_CONTRACT,
        "engine": "extra_trees",
        "models": [json.loads(value.decode("ascii"))
                   for _digest, value in sanitized],
        "public_schema_sha256": canonical["public_schema"]["sha256"],
        "task": ("binary" if canonical["task"] == "binary_classification"
                 else "regression"),
        "version": 1,
    }
    encoded = json.dumps(
        container, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if len(encoded) > byte_cap:
        raise ValueError("ExtraTrees ensemble exceeds its byte ceiling")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ENSEMBLE_CONTRACT",
    "ENSEMBLE_FORMAT",
    "EXECUTION_PROFILE",
    "MaterializedForestData",
    "PreparedExtraTreesTraining",
    "build_extra_trees_ensemble",
    "canonical_extra_trees_profile",
    "materialize_forest_units",
    "prepare_extra_trees_training",
    "sanitize_extra_trees_artifact",
    "train_extra_trees",
]
