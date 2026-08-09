"""Trusted public-bin trainers for LightGBM-style and CatBoost-style models.

These are dsFlower algorithms, not wrappers around upstream LightGBM/CatBoost
binaries.  The asymmetric and oblivious growth rules mirror their useful tree
shapes, while all private influence passes through fixed-layout jointly noised
gradient/Hessian/count histograms over analyst-declared public cuts.
"""

import copy
import hashlib
import json
import math

import numpy as np

from . import boosting_accounting
from . import boosting_profile
from . import catboost_artifact
from . import lightgbm_artifact
from . import tree_release
from . import xgboost_adapter as tree_data


EXECUTION_PROFILES = {
    "lightgbm": "dsflower-asymmetric-public-bin-boosting-v1",
    "catboost": "dsflower-oblivious-public-bin-boosting-v1",
}
_FIXED_POINT_SCALE = 1 << 20
# At the protocol hard cap, one cell can accumulate at most
# 10_000_000 * 2^20 = 10_485_760_000_000, safely below signed int64.
_MAX_RELEASE_COORDINATES = 8_000_000


def canonical_boosting_profile(manifest):
    """Return one exact training profile plus its fixed transcript proof."""
    if not isinstance(manifest, dict):
        raise ValueError("boosting manifest must be an object")
    engine = manifest.get("engine")
    if engine == "lightgbm":
        profile = boosting_profile.lightgbm_profile(manifest)
        releases = boosting_accounting.fixed_release_count(
            engine, trees=profile["trees"],
            num_leaves=profile["num_leaves"])
        leaf_capacity = profile["num_leaves"]
        max_bin = profile["max_bin"]
    elif engine == "catboost":
        profile = boosting_profile.catboost_profile(manifest)
        releases = boosting_accounting.fixed_release_count(
            engine, trees=profile["trees"], depth=profile["max_depth"])
        leaf_capacity = 1 << profile["max_depth"]
        max_bin = profile["border_count"] + 1
    else:
        raise ValueError("boosting adapter engine is unsupported")
    features_per_release = min(
        profile["features"], max(1, int(math.ceil(
            math.sqrt(profile["features"])))))
    coordinates = leaf_capacity * features_per_release * (max_bin + 1) * 3
    if not 1 <= coordinates <= _MAX_RELEASE_COORDINATES:
        raise ValueError("boosting histogram exceeds its coordinate ceiling")
    sensitivity = boosting_accounting.histogram_sensitivity(
        features_per_release, profile["gradient_clip"],
        profile["hessian_clip"])
    return dict(
        profile,
        execution_profile=EXECUTION_PROFILES[engine],
        features_per_release=features_per_release,
        histogram_shape=(leaf_capacity, features_per_release, max_bin + 1, 3),
        leaf_capacity=leaf_capacity,
        max_bin=max_bin,
        num_releases=releases,
        release_coordinates=coordinates,
        sensitivity=sensitivity,
    )


def _numeric_array(value, where, ndim, dtype):
    array = np.asarray(value)
    if array.ndim != ndim or array.dtype.hasobject or array.dtype.kind not in "iuf":
        raise ValueError("%s must be a real numeric rank-%d array" % (where, ndim))
    try:
        return np.array(array, dtype=dtype, order="C", copy=True)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("%s cannot be represented in its numeric profile" % where) from exc


def _numeric_view(value, where, ndim):
    """Inspect numeric shape and dtype without a second cast/copy buffer."""
    array = np.asarray(value)
    if array.ndim != ndim or array.dtype.hasobject or array.dtype.kind not in "iuf":
        raise ValueError("%s must be a real numeric rank-%d array" % (where, ndim))
    return array


def _totalize_features(features, bounds, dtype):
    lower = np.asarray([item[0] for item in bounds], dtype=dtype)
    upper = np.asarray([item[1] for item in bounds], dtype=dtype)
    missing = np.isnan(features)
    np.copyto(features, lower, where=np.isneginf(features))
    np.copyto(features, upper, where=np.isposinf(features))
    np.maximum(features, lower, out=features, where=~missing)
    np.minimum(features, upper, out=features, where=~missing)
    features[missing] = np.asarray(np.nan, dtype=dtype)
    features[features == dtype(0.0)] = dtype(0.0)
    return features


def _totalize_target(target, canonical):
    task = canonical["task"]
    lower = float(canonical["public_schema"]["target"]["lower"])
    upper = float(canonical["public_schema"]["target"]["upper"])
    if task == "binary_classification":
        finite = np.isfinite(target)
        return np.where(finite & (target >= 0.5), 1.0, 0.0)
    midpoint = lower + (upper - lower) / 2.0
    target = np.nan_to_num(
        target, copy=False, nan=midpoint, posinf=upper, neginf=lower)
    np.maximum(target, lower, out=target)
    np.minimum(target, upper, out=target)
    target[target == 0.0] = 0.0
    return target


def _canonical_row_order(tokens, features, target):
    rows, columns = features.shape
    feature_bytes = features.dtype.itemsize * columns
    width = 32 + feature_bytes + 8
    records = np.empty((rows, width), dtype=np.uint8)
    records[:, :32] = tokens.view(np.uint8).reshape(rows, 32)
    records[:, 32:32 + feature_bytes] = np.asarray(
        features, dtype=features.dtype.newbyteorder("<"), order="C").view(
            np.uint8).reshape(rows, feature_bytes)
    records[:, 32 + feature_bytes:] = np.asarray(
        target, dtype="<f8", order="C").view(np.uint8).reshape(rows, 8)
    keys = records.view(np.dtype((np.void, width))).reshape(rows)
    return np.argsort(keys, kind="mergesort")


def _aggregate_patient_units(features, target, unit_ids, canonical, profile):
    rows = features.shape[0]
    tokens = tree_data._patient_unit_tokens(unit_ids, rows)
    if not rows:
        return features, target
    order = _canonical_row_order(tokens, features, target)
    features = np.ascontiguousarray(features[order], dtype=features.dtype)
    target = np.ascontiguousarray(target[order], dtype=np.float64)
    tokens = np.ascontiguousarray(tokens[order])
    starts = np.flatnonzero(np.concatenate((
        np.asarray([True]), tokens[1:] != tokens[:-1])))
    counts = np.diff(np.append(starts, rows)).astype(np.int64, copy=False)

    observed = ~np.isnan(features)
    values = np.where(observed, features, features.dtype.type(0.0))
    sums = np.add.reduceat(values.astype(np.float64), starts, axis=0)
    observed_counts = np.add.reduceat(observed, starts, axis=0, dtype=np.int64)
    means = np.full(sums.shape, np.nan, dtype=np.float64)
    np.divide(sums, observed_counts, out=means, where=observed_counts > 0)
    effective_features = np.asarray(means, dtype=features.dtype, order="C")
    effective_features = _totalize_features(
        effective_features, profile["feature_bounds"], features.dtype.type)

    target_sums = np.add.reduceat(target, starts, dtype=np.float64)
    if canonical["task"] == "binary_classification":
        effective_target = np.where(
            2.0 * target_sums > counts, 1.0, 0.0)
    else:
        effective_target = target_sums / counts
        lower = float(canonical["public_schema"]["target"]["lower"])
        upper = float(canonical["public_schema"]["target"]["upper"])
        np.maximum(effective_target, lower, out=effective_target)
        np.minimum(effective_target, upper, out=effective_target)
    return np.ascontiguousarray(effective_features), np.ascontiguousarray(
        effective_target, dtype=np.float64)


class MaterializedBoostingData:
    """One bounded record per unit, with public bins only."""

    __slots__ = ("_binned_features", "privacy_unit", "target")

    def __init__(self, binned_features, target, privacy_unit):
        self._binned_features = binned_features
        self.target = target
        self.privacy_unit = privacy_unit

    def __repr__(self):
        return "MaterializedBoostingData(unit=%s)" % self.privacy_unit


def materialize_boosting_units(manifest, features, target, *, unit_ids=None):
    """Totalize and bin one row or deterministic patient aggregate per unit."""
    from . import native_tree_contract as tree_contract
    manifest_value = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_boosting_profile(manifest_value)
    dtype = np.float32 if profile["feature_cast"] == "float32" else np.float64
    X_view = _numeric_view(features, "features", 2)
    y_view = _numeric_view(target, "target", 1)
    rows, columns = X_view.shape
    if y_view.shape[0] != rows or columns != profile["features"]:
        raise ValueError("boosting data shape differs from its public schema")
    resources = manifest_value["resources"]
    if rows > resources["max_rows"] or columns > resources["max_features"]:
        raise ValueError("boosting data exceeds a row or feature ceiling")
    # Account conservatively for the input views, cast arrays, bins, patient
    # aggregation workspace, gradients, assignments and release workspace.
    memory = X_view.nbytes + y_view.nbytes + rows * (64 * columns + 512) + \
        profile["release_coordinates"] * 40 + 16 * 1024 * 1024
    if memory > resources["memory_mib"] * 1024 * 1024:
        raise ValueError("boosting training exceeds its memory ceiling")
    X = _numeric_array(X_view, "features", 2, dtype)
    y = _numeric_array(y_view, "target", 1, np.float64)
    X = _totalize_features(X, profile["feature_bounds"], dtype)
    y = _totalize_target(y, manifest_value)
    privacy_unit = manifest_value["privacy"]["unit"]
    if privacy_unit == "row":
        if unit_ids is not None:
            raise ValueError("row-level training must not carry patient identifiers")
    else:
        X, y = _aggregate_patient_units(
            X, y, unit_ids, manifest_value, profile)

    binned = np.empty(X.shape, dtype=np.uint32, order="C")
    missing_value = np.iinfo(np.uint32).max
    for feature, public_cuts in enumerate(profile["cuts"]):
        values = X[:, feature]
        missing = np.isnan(values)
        binned[:, feature] = np.searchsorted(
            np.asarray(public_cuts, dtype=dtype), values,
            side="left").astype(np.uint32, copy=False)
        binned[missing, feature] = missing_value
    binned.setflags(write=False)
    y = np.ascontiguousarray(y, dtype=np.float64)
    y.setflags(write=False)
    return MaterializedBoostingData(binned, y, privacy_unit)


def _gradients(prediction, target, task, gradient_clip, hessian_clip):
    if task == "binary_classification":
        bounded = np.clip(prediction, -700.0, 700.0)
        probability = 1.0 / (1.0 + np.exp(-bounded))
        gradient = probability - target
        hessian = probability * (1.0 - probability)
    else:
        gradient = prediction - target
        hessian = np.ones_like(gradient)
    np.clip(gradient, -gradient_clip, gradient_clip, out=gradient)
    np.clip(hessian, 0.0, hessian_clip, out=hessian)
    quantized_gradient = np.rint(
        gradient / gradient_clip * _FIXED_POINT_SCALE).astype(np.int64)
    quantized_hessian = np.rint(
        hessian / hessian_clip * _FIXED_POINT_SCALE).astype(np.int64)
    return quantized_gradient, quantized_hessian


def _feature_schedule(profile, release_index):
    """Return a balanced public subset; no private value or caller seed enters."""
    ranked = []
    domain = (
        "dsflower/boosting/public-feature-schedule/v1\x00%s\x00%s\x00%d\x00"
        % (profile["engine"], profile["public_schema_sha256"], release_index)
    ).encode("ascii")
    for feature in range(profile["features"]):
        digest = hashlib.sha256(
            domain + feature.to_bytes(8, "big")).digest()
        ranked.append((digest, feature))
    ranked.sort()
    return tuple(sorted(
        feature for _digest, feature in ranked[:profile["features_per_release"]]))


def _sufficient_histogram(materialized, leaf_slots, profile, q_gradient,
                          q_hessian, selected_features):
    rows = materialized._binned_features.shape[0]
    leaf_capacity, local_features, bin_slots, _ = profile["histogram_shape"]
    if len(selected_features) != local_features:
        raise RuntimeError("boosting public feature schedule differs from its layout")
    if leaf_slots.shape != (rows,) or leaf_slots.dtype.kind not in "iu" or \
            bool(np.any(leaf_slots < 0)) or \
            bool(np.any(leaf_slots >= leaf_capacity)):
        raise RuntimeError("boosting leaf assignment is outside its fixed layout")
    cells = leaf_capacity * local_features * bin_slots
    gradient = np.zeros(cells, dtype=np.int64)
    hessian = np.zeros(cells, dtype=np.int64)
    count = np.zeros(cells, dtype=np.int64)
    missing = np.iinfo(np.uint32).max
    for local_feature, feature in enumerate(selected_features):
        bins = materialized._binned_features[:, feature].astype(
            np.int64, copy=True)
        bins[bins == missing] = profile["max_bin"]
        index = ((leaf_slots * local_features + local_feature) * bin_slots + bins)
        np.add.at(gradient, index, q_gradient)
        np.add.at(hessian, index, q_hessian)
        np.add.at(count, index, 1)
    shape = profile["histogram_shape"][:-1]
    stats = np.empty(profile["histogram_shape"], dtype=np.float64)
    stats[..., 0] = gradient.reshape(shape) * (
        profile["gradient_clip"] / _FIXED_POINT_SCALE)
    stats[..., 1] = hessian.reshape(shape) * (
        profile["hessian_clip"] / _FIXED_POINT_SCALE)
    stats[..., 2] = count.reshape(shape)
    return np.ascontiguousarray(stats, dtype=np.float64)


def _topology_hash(value):
    encoded = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _release_histogram(raw, canonical, profile, *, release_index,
                       tree_index, stage_index, topology, selected_features):
    layout = {
        "engine": profile["engine"],
        "feature_indices": list(selected_features),
        "histogram_shape": list(profile["histogram_shape"]),
        "profile": boosting_accounting.HISTOGRAM_RELEASE_PROFILE,
        "public_schema_sha256": profile["public_schema_sha256"],
        "release_index": release_index,
        "stage_index": stage_index,
        "topology_sha256": _topology_hash(topology),
        "tree_index": tree_index,
    }
    return tree_release.joint_gaussian_release(
        raw,
        mechanism=boosting_accounting.MECHANISM_PROFILE,
        layout=layout,
        epsilon=canonical["privacy"]["epsilon"],
        delta=canonical["privacy"]["delta"],
        sensitivity=profile["sensitivity"],
        num_releases=profile["num_releases"],
        execution_fingerprint=profile["execution_profile"],
    )[0]


def _clean_stats(value):
    return (float(value[0]), max(float(value[1]), 0.0),
            max(float(value[2]), 0.0))


def _leaf_score(stats, l1, l2):
    gradient, hessian, _count = _clean_stats(stats)
    magnitude = max(abs(gradient) - l1, 0.0)
    return magnitude * magnitude / (hessian + l2)


def _split_stats(histogram, profile, leaf_slot, local_feature, feature, cut,
                 default_left):
    real_bins = len(profile["cuts"][feature]) + 1
    values = histogram[leaf_slot, local_feature]
    left = np.sum(values[:cut + 1], axis=0, dtype=np.float64)
    right = np.sum(values[cut + 1:real_bins], axis=0, dtype=np.float64)
    missing = values[profile["max_bin"]]
    if default_left:
        left = left + missing
    else:
        right = right + missing
    return _clean_stats(left), _clean_stats(right)


def _parent_stats(histogram, profile, leaf_slot, local_feature, feature):
    real_bins = len(profile["cuts"][feature]) + 1
    values = histogram[leaf_slot, local_feature]
    total = np.sum(values[:real_bins], axis=0, dtype=np.float64)
    return _clean_stats(total + values[profile["max_bin"]])


def _better(candidate, best):
    if best is None or candidate[0] > best[0]:
        return True
    return candidate[0] == best[0] and candidate[1:] < best[1:]


def _leaf_weight(stats, profile, *, l1, l2):
    gradient, hessian, _count = _clean_stats(stats)
    adjusted = math.copysign(max(abs(gradient) - l1, 0.0), gradient)
    step = -adjusted / (hessian + l2)
    step = min(max(step, -profile["max_delta_step"]),
               profile["max_delta_step"])
    # leaf_abs_cap already equals nextafter(learning_rate*max_delta_step).
    learning_rate = profile["learning_rate"]
    value = learning_rate * step
    return min(max(value, -profile["leaf_abs_cap"]), profile["leaf_abs_cap"])


def _go_right(bins, feature, cut, default_left):
    values = bins[:, feature]
    missing = values == np.iinfo(np.uint32).max
    return np.where(missing, not default_left, values > cut)


def _train_lightgbm_tree(materialized, canonical, profile, prediction,
                         tree_index, release_offset):
    q_gradient, q_hessian = _gradients(
        prediction, materialized.target, canonical["task"],
        profile["gradient_clip"], profile["hessian_clip"])
    rows = materialized.target.shape[0]
    assignments = np.zeros(rows, dtype=np.int64)
    left = [-1]
    right = [-1]
    defaults = [False]
    split_features = [0]
    split_cuts = [0]
    depths = [0]
    leaf_stats = {0: (0.0, 0.0, 0.0)}

    for stage in range(profile["num_leaves"] - 1):
        release_index = release_offset + stage
        selected_features = _feature_schedule(profile, release_index)
        # Include empty topological leaves.  Whether a private branch happens
        # to contain a unit must influence growth only through its noised count.
        active = [node for node, child in enumerate(left) if child == -1]
        slot_by_node = {node: slot for slot, node in enumerate(active)}
        slots = np.asarray(
            [slot_by_node[int(node)] for node in assignments], dtype=np.int64)
        raw = _sufficient_histogram(
            materialized, slots, profile, q_gradient, q_hessian,
            selected_features)
        topology = {
            "default_left": defaults,
            "left": left,
            "right": right,
            "split_cuts": split_cuts,
            "split_features": split_features,
        }
        released = _release_histogram(
            raw, canonical, profile,
            release_index=release_index,
            tree_index=tree_index, stage_index=stage, topology=topology,
            selected_features=selected_features)
        best = None
        best_stats = None
        for node in active:
            slot = slot_by_node[node]
            leaf_stats[node] = _parent_stats(
                released, profile, slot, 0, selected_features[0])
            if depths[node] >= profile["max_depth"]:
                continue
            for local_feature, feature in enumerate(selected_features):
                cuts = profile["cuts"][feature]
                for cut in range(len(cuts)):
                    for default_left in (False, True):
                        child_stats = _split_stats(
                            released, profile, slot, local_feature, feature,
                            cut, default_left)
                        if child_stats[0][2] < profile["min_data_in_leaf"] or \
                                child_stats[1][2] < profile["min_data_in_leaf"]:
                            continue
                        parent = tuple(
                            child_stats[0][i] + child_stats[1][i]
                            for i in range(3))
                        gain = (
                            _leaf_score(child_stats[0], profile["lambda_l1"],
                                        profile["lambda_l2"])
                            + _leaf_score(child_stats[1], profile["lambda_l1"],
                                          profile["lambda_l2"])
                            - _leaf_score(parent, profile["lambda_l1"],
                                          profile["lambda_l2"])
                        )
                        candidate = (gain, node, feature, cut, default_left)
                        if _better(candidate, best):
                            best = candidate
                            best_stats = child_stats
        if best is None or best[0] < profile["min_gain_to_split"]:
            continue
        _gain, node, feature, cut, default_left = best
        left_node = len(left)
        right_node = left_node + 1
        left[node] = left_node
        right[node] = right_node
        defaults[node] = default_left
        split_features[node] = feature
        split_cuts[node] = cut
        left.extend([-1, -1])
        right.extend([-1, -1])
        defaults.extend([False, False])
        split_features.extend([0, 0])
        split_cuts.extend([0, 0])
        depths.extend([depths[node] + 1, depths[node] + 1])
        leaf_stats.pop(node, None)
        leaf_stats[left_node], leaf_stats[right_node] = best_stats
        selected = assignments == node
        branch = _go_right(
            materialized._binned_features, feature, cut, default_left)
        assignments[selected & ~branch] = left_node
        assignments[selected & branch] = right_node

    leaves = [0.0] * len(left)
    for node in leaf_stats:
        leaves[node] = _leaf_weight(
            leaf_stats[node], profile, l1=profile["lambda_l1"],
            l2=profile["lambda_l2"])
    tree = {
        "default_left": defaults,
        "leaf_values": leaves,
        "left_children": left,
        "right_children": right,
        "split_cut_indices": split_cuts,
        "split_indices": split_features,
    }
    update = np.asarray([leaves[int(node)] for node in assignments],
                        dtype=np.float64)
    return tree, update


def _train_catboost_tree(materialized, canonical, profile, prediction,
                         tree_index, release_offset):
    q_gradient, q_hessian = _gradients(
        prediction, materialized.target, canonical["task"],
        profile["gradient_clip"], profile["hessian_clip"])
    rows = materialized.target.shape[0]
    assignments = np.zeros(rows, dtype=np.int64)
    splits = []
    final_stats = None
    for level in range(profile["max_depth"]):
        release_index = release_offset + level
        selected_features = _feature_schedule(profile, release_index)
        raw = _sufficient_histogram(
            materialized, assignments, profile, q_gradient, q_hessian,
            selected_features)
        released = _release_histogram(
            raw, canonical, profile,
            release_index=release_index,
            tree_index=tree_index, stage_index=level,
            topology={"splits": splits},
            selected_features=selected_features)
        active_leaves = 1 << level
        best = None
        best_stats = None
        for local_feature, feature in enumerate(selected_features):
            cuts = profile["cuts"][feature]
            for cut in range(len(cuts)):
                for default_left in (False, True):
                    gain = 0.0
                    children = [None] * (2 * active_leaves)
                    for leaf in range(active_leaves):
                        child_stats = _split_stats(
                            released, profile, leaf, local_feature, feature,
                            cut, default_left)
                        parent = tuple(
                            child_stats[0][i] + child_stats[1][i]
                            for i in range(3))
                        gain += (
                            _leaf_score(child_stats[0], 0.0,
                                        profile["l2_leaf_reg"])
                            + _leaf_score(child_stats[1], 0.0,
                                          profile["l2_leaf_reg"])
                            - _leaf_score(parent, 0.0,
                                          profile["l2_leaf_reg"])
                        )
                        children[leaf] = child_stats[0]
                        children[leaf + active_leaves] = child_stats[1]
                    candidate = (gain, feature, cut, default_left)
                    if _better(candidate, best):
                        best = candidate
                        best_stats = children
        if best is None:
            raise RuntimeError("CatBoost-style split geometry is empty")
        _gain, feature, cut, default_left = best
        splits.append({
            "cut_index": cut,
            "default_left": default_left,
            "feature_index": feature,
        })
        branch = _go_right(
            materialized._binned_features, feature, cut, default_left)
        assignments |= branch.astype(np.int64) << level
        final_stats = best_stats
    leaves = [
        _leaf_weight(stats, profile, l1=0.0, l2=profile["l2_leaf_reg"])
        for stats in final_stats
    ]
    update = np.asarray([leaves[int(leaf)] for leaf in assignments],
                        dtype=np.float64)
    return {"leaf_values": leaves, "splits": splits}, update


class PreparedBoostingTraining:
    """One immutable, one-shot adaptive DP transcript."""

    __slots__ = ("_canonical", "_materialized", "_profile", "_used")

    def __init__(self, canonical, profile, materialized):
        self._canonical = copy.deepcopy(canonical)
        self._profile = copy.deepcopy(profile)
        self._materialized = materialized
        self._used = False

    def __repr__(self):
        return "PreparedBoostingTraining(engine=%s, releases=%d)" % (
            self._profile["engine"], self._profile["num_releases"])


def prepare_boosting_training(manifest, features, target, *, unit_ids=None):
    """Prepare bounded units; no model bytes or private statistic leave here."""
    from . import native_tree_contract as tree_contract
    canonical = tree_contract.canonical_engine_manifest(manifest)
    profile = canonical_boosting_profile(canonical)
    materialized = materialize_boosting_units(
        canonical, features, target, unit_ids=unit_ids)
    return PreparedBoostingTraining(canonical, profile, materialized)


def train_boosting(prepared):
    """Consume a fixed transcript and return only a safe canonical projection."""
    if type(prepared) is not PreparedBoostingTraining or prepared._used:
        raise ValueError("boosting request was not prepared by dsFlower")
    prepared._used = True
    canonical = prepared._canonical
    profile = prepared._profile
    materialized = prepared._materialized
    prediction = np.full(
        materialized.target.shape[0], profile["base_score"], dtype=np.float64)
    trees = []
    release_offset = 0
    for tree_index in range(profile["trees"]):
        if profile["engine"] == "lightgbm":
            tree, update = _train_lightgbm_tree(
                materialized, canonical, profile, prediction, tree_index,
                release_offset)
            release_offset += profile["num_leaves"] - 1
        else:
            tree, update = _train_catboost_tree(
                materialized, canonical, profile, prediction, tree_index,
                release_offset)
            release_offset += profile["max_depth"]
        trees.append(tree)
        prediction += update
    if release_offset != profile["num_releases"] or not bool(
            np.all(np.isfinite(prediction))):
        raise RuntimeError("boosting transcript differs from its fixed profile")
    contract = (lightgbm_artifact.MODEL_CONTRACT
                if profile["engine"] == "lightgbm"
                else catboost_artifact.MODEL_CONTRACT)
    model = {
        "base_score": profile["base_score"],
        "contract": contract,
        "engine": profile["engine"],
        "public_schema_sha256": profile["public_schema_sha256"],
        "task": profile["task"],
        "trees": trees,
        "version": 1,
    }
    raw = json.dumps(
        model, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    sanitizer = (lightgbm_artifact.sanitize_model
                 if profile["engine"] == "lightgbm"
                 else catboost_artifact.sanitize_model)
    return sanitizer(raw, canonical)[0]


def sanitize_boosting_artifact(manifest, artifact):
    """Re-sanitize one node artifact under its exact public profile."""
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "lightgbm":
        return lightgbm_artifact.sanitize_model(artifact, manifest)
    if engine == "catboost":
        return catboost_artifact.sanitize_model(artifact, manifest)
    raise ValueError("boosting artifact engine is unsupported")


def build_boosting_ensemble(manifest, artifacts):
    """Build one canonical mean-prediction ensemble and its digest."""
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "lightgbm":
        encoded = lightgbm_artifact.build_ensemble(artifacts, manifest)
    elif engine == "catboost":
        encoded = catboost_artifact.build_ensemble(artifacts, manifest)
    else:
        raise ValueError("boosting ensemble engine is unsupported")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = [
    "EXECUTION_PROFILES",
    "MaterializedBoostingData",
    "PreparedBoostingTraining",
    "build_boosting_ensemble",
    "canonical_boosting_profile",
    "materialize_boosting_units",
    "prepare_boosting_training",
    "sanitize_boosting_artifact",
    "train_boosting",
]
