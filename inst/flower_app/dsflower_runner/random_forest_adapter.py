"""Trusted differentially private Random Forest adapter.

Each effective bounded record is assigned to exactly one tree by a custodial
PRF over its public-bin representation and quantized target.  Candidate
features are also drawn by a data-independent PRF.  Training then follows a
fixed transcript: one joint Gaussian public-bin histogram per complete depth
level, followed by one joint Gaussian terminal-leaf vector.  Split decisions
and missing-value directions use released histograms only.
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


EXECUTION_PROFILE = "dsflower-random-forest-execution-v1"
ENSEMBLE_CONTRACT = "dsflower-forest-ensemble-v1"
ENSEMBLE_FORMAT = "dsflower-forest-ensemble-json-v1"
_MAX_DEPTH = 12
_MAX_TREES = 512
_MAX_RELEASE_COORDINATES = 8_000_000
_REGRESSION_SCALE = 1 << 20
_MAX_NOISY_STAT = 1.0e100
_MISSING_BIN = np.iinfo(np.uint32).max
_ASSIGNMENT_DOMAIN = b"dsflower/random-forest/tree-assignment/v1\x00"
_REQUIRED_ENGINE_PARAMETERS = {
    "max_depth": "int",
    "max_features": "int",
    "n_estimators": "int",
}
_REQUIRED_MECHANISM_PARAMETERS = {
    "candidate_schedule": "string",
    "histogram_release": "string",
    "leaf_release": "string",
    "partition": "string",
    "transcript": "string",
}
_PREPARED_TOKEN = object()


def _typed_value(parameters, name, expected_type):
    record = parameters[name]
    if record["type"] != expected_type:
        raise ValueError("Random Forest parameter has the wrong declared type")
    return record["value"]


def _positive_integer(value, name, upper):
    if isinstance(value, bool) or not isinstance(value, int) or not (
            1 <= value <= upper):
        raise ValueError("%s is outside the Random Forest profile" % name)
    return int(value)


def canonical_random_forest_profile(manifest):
    """Return the exact server-owned adaptive Random Forest profile."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != "random_forest" or \
            canonical["mode"] != "native-tight":
        raise ValueError("Random Forest adapter requires native-tight random_forest")
    parameters = canonical["engine_params"]
    if frozenset(parameters) != frozenset(_REQUIRED_ENGINE_PARAMETERS):
        raise ValueError("Random Forest parameter profile has unknown or missing fields")
    values = {
        name: _typed_value(parameters, name, kind)
        for name, kind in _REQUIRED_ENGINE_PARAMETERS.items()
    }
    trees = _positive_integer(values["n_estimators"],
                              "n_estimators", _MAX_TREES)
    depth = _positive_integer(values["max_depth"], "max_depth", _MAX_DEPTH)
    resources = canonical["resources"]
    if trees > resources["max_trees"] or depth > resources["max_depth"]:
        raise ValueError("Random Forest training shape exceeds a resource ceiling")

    schema = canonical["public_schema"]
    lower, upper, cuts = tree_data._float32_schema(schema)
    if not cuts or any(not feature for feature in cuts):
        raise ValueError("Random Forest requires complete non-empty public cuts")
    features = len(cuts)
    max_features = _positive_integer(
        values["max_features"], "max_features", features)
    target = schema["target"]
    target_lower = tree_data._float32(
        target["lower"], "public target lower bound")
    target_upper = tree_data._float32(
        target["upper"], "public target upper bound")
    if target_lower >= target_upper:
        raise ValueError("Random Forest target bounds must remain strict as float32")

    mechanism = canonical["privacy"]["mechanism_params"]
    if frozenset(mechanism) != frozenset(_REQUIRED_MECHANISM_PARAMETERS):
        raise ValueError("Random Forest mechanism parameter profile must be exact")
    pinned = {
        name: _typed_value(mechanism, name, kind)
        for name, kind in _REQUIRED_MECHANISM_PARAMETERS.items()
    }
    expected = {
        "candidate_schedule": forest_accounting.RANDOM_FOREST_CANDIDATE_PROFILE,
        "histogram_release": forest_accounting.RANDOM_FOREST_HISTOGRAM_PROFILE,
        "leaf_release": forest_accounting.RANDOM_FOREST_LEAF_PROFILE,
        "partition": forest_accounting.RANDOM_FOREST_PARTITION_PROFILE,
        "transcript": forest_accounting.RANDOM_FOREST_TRANSCRIPT_PROFILE,
    }
    if pinned != expected:
        raise ValueError("Random Forest mechanism profile differs from its fixed pins")

    bin_slots = max(len(feature) + 2 for feature in cuts)
    split_coordinates = (2 * trees * (1 << (depth - 1)) *
                         max_features * bin_slots)
    leaf_coordinates = 2 * trees * (1 << depth)
    if max(split_coordinates, leaf_coordinates) > _MAX_RELEASE_COORDINATES:
        raise ValueError("Random Forest transcript exceeds its execution ceiling")
    accounting = forest_accounting.random_forest_release(
        canonical["task"], max_features, depth,
        canonical["privacy"]["epsilon"], canonical["privacy"]["delta"])
    return dict({
        "bin_slots": bin_slots,
        "engine": "random_forest",
        "feature_lower": list(lower),
        "feature_upper": list(upper),
        "leaf_release_coordinates": leaf_coordinates,
        "max_depth": depth,
        "max_features": max_features,
        "n_estimators": trees,
        "public_cuts": [list(feature) for feature in cuts],
        "split_release_coordinates": split_coordinates,
        "target_lower": target_lower,
        "target_upper": target_upper,
    }, **accounting)


class MaterializedRandomForestData:
    """One bounded effective unit represented only by bins and fixed targets."""

    __slots__ = ("_binned_features", "_target_units", "privacy_unit")

    def __init__(self, binned_features, target_units, privacy_unit):
        self._binned_features = binned_features
        self._target_units = target_units
        self.privacy_unit = privacy_unit

    def __repr__(self):
        return "MaterializedRandomForestData(unit=%s)" % self.privacy_unit


def _memory_peak_bytes(rows, columns, profile, artifact_bytes, privacy_unit):
    internal = profile["n_estimators"] * (
        (1 << profile["max_depth"]) - 1)
    candidates = 4 * internal * profile["max_features"]
    private = rows * (
        20 * columns + 24 * profile["max_features"] + 192)
    if privacy_unit == "patient":
        private += rows * (40 * columns + 320)
    release = 40 * max(
        profile["split_release_coordinates"],
        profile["leaf_release_coordinates"])
    artifacts = 4 * artifact_bytes
    subtotal = private + candidates + release + artifacts + 8 * 1024 * 1024
    return subtotal + subtotal // 4


def materialize_random_forest_units(
        manifest, features, target, *, unit_ids=None):
    """Totalize private input into one immutable binned record per unit."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_random_forest_profile(canonical)
    rows, columns = tree_data._numeric_array_shape(features, "features", 2)
    target_rows, = tree_data._numeric_array_shape(target, "target", 1)
    if target_rows != rows:
        raise ValueError("target row count must match the feature matrix")
    if columns != len(canonical["public_schema"]["features"]):
        raise ValueError("feature count differs from the public schema")
    resources = canonical["resources"]
    if rows > resources["max_rows"] or columns > resources["max_features"]:
        raise ValueError("Random Forest data exceeds a row or feature ceiling")
    if _memory_peak_bytes(
            rows, columns, profile, resources["max_artifact_bytes"],
            canonical["privacy"]["unit"]) > \
            resources["memory_mib"] * 1024 * 1024:
        raise ValueError("complete Random Forest training exceeds the memory ceiling")

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

    binned = np.ascontiguousarray(
        tree_data._public_bin_indices(X, profile["public_cuts"]),
        dtype=np.uint32)
    if canonical["task"] == "binary_classification":
        targets = np.asarray(y >= np.float32(0.5), dtype=np.uint32)
    else:
        normalized = np.clip(
            (y.astype(np.float64) - profile["target_lower"]) /
            (profile["target_upper"] - profile["target_lower"]),
            0.0, 1.0)
        targets = np.rint(normalized * _REGRESSION_SCALE).astype(
            np.uint32)
    targets = np.ascontiguousarray(targets, dtype=np.uint32)
    binned.setflags(write=False)
    targets.setflags(write=False)
    return MaterializedRandomForestData(binned, targets, privacy_unit)


def _policy_hash(value):
    encoded = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sample_without_replacement(rng, population, count):
    swaps = {}
    result = []
    for slot in range(count):
        remaining = population - slot
        draw = rng._randbelow(remaining)
        value = swaps.get(draw, draw)
        last = remaining - 1
        swaps[draw] = swaps.get(last, last)
        result.append(value)
    return result


def _public_schedule(profile, features):
    config = {
        "cut_counts": [len(value) for value in profile["public_cuts"]],
        "depth": profile["max_depth"],
        "features": features,
        "max_features": profile["max_features"],
        "trees": profile["n_estimators"],
    }
    policy = {
        "candidate_schedule": forest_accounting.RANDOM_FOREST_CANDIDATE_PROFILE,
        "partition": forest_accounting.RANDOM_FOREST_PARTITION_PROFILE,
    }
    policy["policy_hash"] = _policy_hash(policy)
    master = bytearray(seeding.master_seed(
        "random-forest/public-prf/v1", config, policy, 1,
        execution_fingerprint="dsflower-random-forest-public-prf-v1"))
    candidate_key = None
    assignment_key = None
    try:
        candidate_key = bytearray(seeding.sub_seed(
            master, "candidate-schedule/v1"))
        assignment_key = bytearray(seeding.sub_seed(
            master, "disjoint-tree-assignment/v1"))
        rng = seeding.np_rng(candidate_key)
        candidate_key[:] = b"\x00" * len(candidate_key)
        internal = (1 << profile["max_depth"]) - 1
        candidates = np.empty(
            (profile["n_estimators"], internal, profile["max_features"]),
            dtype=np.uint32)
        for tree in range(profile["n_estimators"]):
            for node in range(internal):
                candidates[tree, node, :] = _sample_without_replacement(
                    rng, features, profile["max_features"])
        candidates.setflags(write=False)
        return candidates, assignment_key
    except Exception:
        if isinstance(assignment_key, bytearray):
            assignment_key[:] = b"\x00" * len(assignment_key)
        raise
    finally:
        if isinstance(candidate_key, bytearray):
            candidate_key[:] = b"\x00" * len(candidate_key)
        master[:] = b"\x00" * len(master)


def _tree_assignments(binned, target_units, trees, assignment_key):
    rows = binned.shape[0]
    result = np.empty(rows, dtype=np.uint32)
    key = bytes(assignment_key)
    limit = (1 << 64) - ((1 << 64) % trees)
    little_bins = np.ascontiguousarray(binned, dtype="<u4")
    for row in range(rows):
        counter = 0
        while True:
            digest = hmac.new(key, _ASSIGNMENT_DOMAIN, hashlib.sha256)
            digest.update(memoryview(little_bins[row]).cast("B"))
            digest.update(int(target_units[row]).to_bytes(4, "little"))
            digest.update(counter.to_bytes(4, "little"))
            value = int.from_bytes(digest.digest()[:8], "little")
            if value < limit:
                result[row] = value % trees
                break
            counter += 1
    result.setflags(write=False)
    return result


def _update_digest_array(digest, label, value):
    encoded = label.encode("ascii")
    digest.update(len(encoded).to_bytes(4, "big"))
    digest.update(encoded)
    shape = json.dumps(list(value.shape), separators=(",", ":")).encode("ascii")
    digest.update(len(shape).to_bytes(8, "big"))
    digest.update(shape)
    canonical = np.ascontiguousarray(value, dtype="<u4")
    raw = (b"" if canonical.size == 0
           else memoryview(canonical).cast("B"))
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def _prepared_digest(canonical, profile, candidates, binned, targets,
                     assignments):
    digest = hashlib.sha256()
    for label, value in (("canonical", canonical), ("profile", profile)):
        payload = json.dumps(
            value, ensure_ascii=True, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        encoded = label.encode("ascii")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    for label, value in (
            ("candidates", candidates), ("binned", binned),
            ("targets", targets), ("assignments", assignments)):
        if type(value) is not np.ndarray or value.dtype != np.uint32 or \
                not value.flags.c_contiguous:
            raise ValueError("prepared Random Forest array is invalid")
        _update_digest_array(digest, label, value)
    return digest.hexdigest()


class PreparedRandomForestTraining:
    """Frozen one-shot adaptive transcript; repr excludes private contents."""

    __slots__ = ("_assignments", "_binned", "_candidates", "_canonical",
                 "_digest", "_profile", "_sealed", "_targets", "_used")

    def __init__(self, token, canonical, profile, candidates, binned,
                 targets, assignments):
        if token is not _PREPARED_TOKEN:
            raise TypeError("use prepare_random_forest_training")
        object.__setattr__(self, "_sealed", False)
        self._canonical = copy.deepcopy(canonical)
        self._profile = copy.deepcopy(profile)
        self._candidates = candidates
        self._binned = binned
        self._targets = targets
        self._assignments = assignments
        self._used = False
        self._digest = _prepared_digest(
            self._canonical, self._profile, self._candidates,
            self._binned, self._targets, self._assignments)
        self._sealed = True

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False):
            raise AttributeError("prepared Random Forest request is frozen")
        object.__setattr__(self, name, value)

    def __repr__(self):
        return "PreparedRandomForestTraining(trees=%d, depth=%d)" % (
            self._profile["n_estimators"], self._profile["max_depth"])


def _validate_prepared(prepared):
    if type(prepared) is not PreparedRandomForestTraining or \
            getattr(prepared, "_sealed", False) is not True or \
            getattr(prepared, "_used", None) is not False:
        raise ValueError("Random Forest request was not prepared by dsFlower")
    try:
        canonical = tree_contract.canonical_engine_manifest(prepared._canonical)
        profile = canonical_random_forest_profile(canonical)
        rows = prepared._binned.shape[0]
        expected_candidates = (
            profile["n_estimators"],
            (1 << profile["max_depth"]) - 1,
            profile["max_features"],
        )
        arrays = (
            (prepared._candidates, expected_candidates),
            (prepared._binned, (rows, len(profile["public_cuts"]))),
            (prepared._targets, (rows,)),
            (prepared._assignments, (rows,)),
        )
        if canonical != prepared._canonical or profile != prepared._profile or \
                any(type(value) is not np.ndarray or
                    value.shape != shape or value.dtype != np.uint32 or
                    value.flags.writeable or not value.flags.c_contiguous
                    for value, shape in arrays) or \
                bool(np.any(prepared._candidates >= len(profile["public_cuts"]))) or \
                bool(np.any(prepared._assignments >= profile["n_estimators"])) or \
                bool(np.any(prepared._targets > _REGRESSION_SCALE)):
            raise ValueError("prepared Random Forest request changed")
        actual = _prepared_digest(
            canonical, profile, prepared._candidates, prepared._binned,
            prepared._targets, prepared._assignments)
        if not isinstance(prepared._digest, str) or not hmac.compare_digest(
                prepared._digest, actual):
            raise ValueError("prepared Random Forest request changed")
    except Exception as exc:
        raise ValueError("Random Forest request was not prepared by dsFlower") from exc
    return canonical, profile


def prepare_random_forest_training(
        manifest, features, target, *, unit_ids=None):
    """Freeze the bounded units and public-PRF schedule for one transcript."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_random_forest_profile(canonical)
    materialized = materialize_random_forest_units(
        canonical, features, target, unit_ids=unit_ids)
    candidates, assignment_key = _public_schedule(
        profile, len(canonical["public_schema"]["features"]))
    try:
        assignments = _tree_assignments(
            materialized._binned_features, materialized._target_units,
            profile["n_estimators"], assignment_key)
    finally:
        assignment_key[:] = b"\x00" * len(assignment_key)
    return PreparedRandomForestTraining(
        _PREPARED_TOKEN, canonical, profile, candidates,
        materialized._binned_features, materialized._target_units,
        assignments)


def _schedule_hash(candidates):
    digest = hashlib.sha256()
    _update_digest_array(digest, "candidate-schedule", candidates)
    return digest.hexdigest()


def _release_layout(profile, canonical, candidates_hash, *, level, coordinates):
    return {
        "bin_slots": profile["bin_slots"],
        "candidate_schedule_sha256": candidates_hash,
        "cut_counts": [len(value) for value in profile["public_cuts"]],
        "depth": profile["max_depth"],
        "engine": "random_forest",
        "level": level,
        "max_features": profile["max_features"],
        "partition": forest_accounting.RANDOM_FOREST_PARTITION_PROFILE,
        "release_coordinates": coordinates,
        "release_index": level,
        "task": canonical["task"],
        "transcript": forest_accounting.RANDOM_FOREST_TRANSCRIPT_PROFILE,
        "trees": profile["n_estimators"],
    }


def _level_histograms(binned, targets, assignments, nodes, candidates,
                      canonical, profile):
    trees, level_nodes, max_features = candidates.shape
    slots = profile["bin_slots"]
    cells = trees * level_nodes * max_features * slots
    stats = np.zeros((cells, 2), dtype=np.float64)
    rows = binned.shape[0]
    if rows:
        groups = assignments.astype(np.int64) * level_nodes + \
            nodes.astype(np.int64)
        candidate_rows = candidates.reshape(
            trees * level_nodes, max_features)[groups]
        row_index = np.arange(rows, dtype=np.int64)[:, None]
        raw_bins = binned[row_index, candidate_rows]
        cut_counts = np.asarray(
            [len(value) for value in profile["public_cuts"]],
            dtype=np.uint32)
        bins = np.where(
            raw_bins == _MISSING_BIN,
            cut_counts[candidate_rows] + np.uint32(1), raw_bins)
        encoded = (((groups[:, None] * max_features +
                     np.arange(max_features, dtype=np.int64)[None, :]) *
                    slots) + bins.astype(np.int64)).reshape(-1)
        counts = np.bincount(encoded, minlength=cells).astype(
            np.float64, copy=False)
        if canonical["task"] == "binary_classification":
            positive_weights = np.broadcast_to(
                targets[:, None], (rows, max_features)).reshape(-1)
            positive = np.bincount(
                encoded, weights=positive_weights, minlength=cells)
            stats[:, 0] = counts - positive
            stats[:, 1] = positive
        else:
            target_weights = np.broadcast_to(
                targets[:, None], (rows, max_features)).reshape(-1)
            sums = np.bincount(
                encoded, weights=target_weights, minlength=cells)
            stats[:, 0] = counts
            stats[:, 1] = sums / _REGRESSION_SCALE
    return stats.reshape(trees, level_nodes, max_features, slots, 2)


def _leaf_histograms(assignments, nodes, targets, canonical, profile):
    trees = profile["n_estimators"]
    leaves = 1 << profile["max_depth"]
    cells = trees * leaves
    stats = np.zeros((cells, 2), dtype=np.float64)
    if targets.size:
        encoded = assignments.astype(np.int64) * leaves + nodes.astype(np.int64)
        counts = np.bincount(encoded, minlength=cells).astype(
            np.float64, copy=False)
        if canonical["task"] == "binary_classification":
            positive = np.bincount(
                encoded, weights=targets, minlength=cells)
            stats[:, 0] = counts - positive
            stats[:, 1] = positive
        else:
            sums = np.bincount(
                encoded, weights=targets, minlength=cells)
            stats[:, 0] = counts
            stats[:, 1] = sums / _REGRESSION_SCALE
    return stats.reshape(trees, leaves, 2)


def _score_binary(left, right):
    left_total = left[:, 0] + left[:, 1]
    right_total = right[:, 0] + right[:, 1]
    return ((left[:, 0] * left[:, 0] + left[:, 1] * left[:, 1]) /
            np.maximum(left_total, np.finfo(np.float64).tiny) +
            (right[:, 0] * right[:, 0] + right[:, 1] * right[:, 1]) /
            np.maximum(right_total, np.finfo(np.float64).tiny))


def _score_regression(left, right):
    return (left[:, 1] * left[:, 1] /
            np.maximum(left[:, 0], np.finfo(np.float64).tiny) +
            right[:, 1] * right[:, 1] /
            np.maximum(right[:, 0], np.finfo(np.float64).tiny))


def _choose_splits(released, candidates, canonical, profile):
    trees, level_nodes, max_features, slots, _channels = released.shape
    groups = trees * level_nodes
    values = np.clip(
        released.reshape(groups, max_features, slots, 2),
        0.0, _MAX_NOISY_STAT)
    if canonical["task"] == "regression":
        values[:, :, :, 1] = np.minimum(
            values[:, :, :, 1], values[:, :, :, 0])
    candidate_rows = candidates.reshape(groups, max_features)
    public_cut_counts = np.asarray(
        [len(value) for value in profile["public_cuts"]], dtype=np.int64)
    best_score = np.full(groups, -1.0, dtype=np.float64)
    best_feature = np.full(groups, len(public_cut_counts), dtype=np.int64)
    best_cut = np.full(groups, slots, dtype=np.int64)
    best_default = np.ones(groups, dtype=np.bool_)
    row_index = np.arange(groups, dtype=np.int64)

    for slot in range(max_features):
        feature = candidate_rows[:, slot].astype(np.int64, copy=False)
        cut_count = public_cut_counts[feature]
        hist = values[:, slot, :, :]
        prefix = np.cumsum(hist, axis=1)
        total = prefix[row_index, cut_count, :]
        missing = hist[row_index, cut_count + 1, :]
        for cut in range(int(np.max(public_cut_counts))):
            valid = cut < cut_count
            base_left = prefix[:, cut, :]
            base_right = total - base_left
            for default_left in (False, True):
                left = base_left + (missing if default_left else 0.0)
                right = base_right + (0.0 if default_left else missing)
                score = (_score_binary(left, right)
                         if canonical["task"] == "binary_classification"
                         else _score_regression(left, right))
                tie = score == best_score
                lexical = ((feature < best_feature) |
                           ((feature == best_feature) &
                            ((cut < best_cut) |
                             ((cut == best_cut) &
                              (np.logical_not(default_left) & best_default)))))
                better = valid & ((score > best_score) | (tie & lexical))
                best_score[better] = score[better]
                best_feature[better] = feature[better]
                best_cut[better] = cut
                best_default[better] = default_left
    return (
        best_feature.reshape(trees, level_nodes).astype(np.uint32),
        best_cut.reshape(trees, level_nodes).astype(np.uint32),
        best_default.reshape(trees, level_nodes),
    )


def _route_level(binned, assignments, nodes, features, cuts, defaults):
    if not nodes.size:
        return nodes
    row_index = np.arange(nodes.size, dtype=np.int64)
    feature = features[assignments, nodes]
    cut = cuts[assignments, nodes]
    default = defaults[assignments, nodes]
    value = binned[row_index, feature]
    go_left = np.where(value == _MISSING_BIN, default, value <= cut)
    return (2 * nodes + np.logical_not(go_left).astype(np.uint32)).astype(
        np.uint32, copy=False)


def _sanitizer_arguments(canonical, profile):
    return {
        "expected_engine": "random_forest",
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


def train_random_forest(prepared):
    """Consume one prepared request and return only canonical model bytes."""
    canonical, profile = _validate_prepared(prepared)
    object.__setattr__(prepared, "_used", True)
    try:
        trees = profile["n_estimators"]
        depth = profile["max_depth"]
        internal = (1 << depth) - 1
        split_features = np.zeros((trees, internal), dtype=np.uint32)
        split_cuts = np.zeros((trees, internal), dtype=np.uint32)
        split_defaults = np.zeros((trees, internal), dtype=np.bool_)
        nodes = np.zeros(prepared._targets.size, dtype=np.uint32)
        schedule_hash = _schedule_hash(prepared._candidates)

        for level in range(depth):
            start = (1 << level) - 1
            level_nodes = 1 << level
            candidates = prepared._candidates[:, start:start + level_nodes, :]
            stats = _level_histograms(
                prepared._binned, prepared._targets, prepared._assignments,
                nodes, candidates, canonical, profile)
            released, sigma = tree_release.joint_gaussian_release(
                stats,
                mechanism=forest_accounting.RANDOM_FOREST_MECHANISM_PROFILE,
                layout=_release_layout(
                    profile, canonical, schedule_hash, level=level,
                    coordinates=stats.size),
                epsilon=canonical["privacy"]["epsilon"],
                delta=canonical["privacy"]["delta"],
                sensitivity=profile["split_sensitivity"],
                num_releases=profile["num_releases"],
                execution_fingerprint=EXECUTION_PROFILE)
            if sigma != profile["split_sigma"]:
                raise RuntimeError("Random Forest accountant and split sigma differ")
            features, cuts, defaults = _choose_splits(
                released, candidates, canonical, profile)
            split_features[:, start:start + level_nodes] = features
            split_cuts[:, start:start + level_nodes] = cuts
            split_defaults[:, start:start + level_nodes] = defaults
            nodes = _route_level(
                prepared._binned, prepared._assignments, nodes,
                features, cuts, defaults)

        leaf_stats = _leaf_histograms(
            prepared._assignments, nodes, prepared._targets,
            canonical, profile)
        released, sigma = tree_release.joint_gaussian_release(
            leaf_stats,
            mechanism=forest_accounting.RANDOM_FOREST_MECHANISM_PROFILE,
            layout=_release_layout(
                profile, canonical, schedule_hash, level=depth,
                coordinates=leaf_stats.size),
            epsilon=canonical["privacy"]["epsilon"],
            delta=canonical["privacy"]["delta"],
            sensitivity=profile["leaf_sensitivity"],
            num_releases=profile["num_releases"],
            execution_fingerprint=EXECUTION_PROFILE)
        if sigma != profile["leaf_sigma"]:
            raise RuntimeError("Random Forest accountant and leaf sigma differ")
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

        model_trees = []
        for tree in range(trees):
            model_trees.append({
                "cut_indices": [int(value) for value in split_cuts[tree]],
                "default_left": [bool(value) for value in split_defaults[tree]],
                "features": [int(value) for value in split_features[tree]],
                "leaf_values": [float(value) for value in leaves[tree]],
            })
        model = {
            "contract": MODEL_CONTRACT,
            "depth": depth,
            "engine": "random_forest",
            "num_features": len(canonical["public_schema"]["features"]),
            "public_schema_sha256": canonical["public_schema"]["sha256"],
            "task": ("binary" if canonical["task"] == "binary_classification"
                     else "regression"),
            "trees": model_trees,
            "version": 1,
        }
        raw = json.dumps(
            model, ensure_ascii=True, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        return sanitize_forest_json(
            raw, **_sanitizer_arguments(canonical, profile))[0]
    finally:
        object.__setattr__(prepared, "_binned", np.empty(
            (0, 0), dtype=np.uint32))
        object.__setattr__(prepared, "_targets", np.empty(
            (0,), dtype=np.uint32))
        object.__setattr__(prepared, "_assignments", np.empty(
            (0,), dtype=np.uint32))


def sanitize_random_forest_artifact(manifest, artifact):
    """Re-sanitize Random Forest bytes under their exact public profile."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_random_forest_profile(canonical)
    return sanitize_forest_json(
        artifact, **_sanitizer_arguments(canonical, profile))


def build_random_forest_ensemble(manifest, artifacts):
    """Build a canonical mean-prediction ensemble of private node models."""
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_random_forest_profile(canonical)
    if not isinstance(artifacts, (list, tuple)) or not artifacts:
        raise ValueError("Random Forest ensemble requires at least one model")
    byte_cap = canonical["resources"]["max_artifact_bytes"]
    total = 0
    sanitized = []
    arguments = _sanitizer_arguments(canonical, profile)
    for artifact in artifacts:
        if not isinstance(artifact, (bytes, bytearray, memoryview)):
            raise ValueError("Random Forest ensemble members must be JSON bytes")
        total += artifact.nbytes if isinstance(artifact, memoryview) else len(artifact)
        if total > byte_cap:
            raise ValueError("Random Forest ensemble exceeds its byte ceiling")
        encoded, digest = sanitize_forest_json(artifact, **arguments)
        sanitized.append((digest, encoded))
    sanitized.sort(key=lambda item: (item[0], item[1]))
    container = {
        "aggregation": "mean_prediction",
        "contract": ENSEMBLE_CONTRACT,
        "engine": "random_forest",
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
        raise ValueError("Random Forest ensemble exceeds its byte ceiling")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ENSEMBLE_CONTRACT",
    "ENSEMBLE_FORMAT",
    "EXECUTION_PROFILE",
    "MaterializedRandomForestData",
    "PreparedRandomForestTraining",
    "build_random_forest_ensemble",
    "canonical_random_forest_profile",
    "materialize_random_forest_units",
    "prepare_random_forest_training",
    "sanitize_random_forest_artifact",
    "train_random_forest",
]
