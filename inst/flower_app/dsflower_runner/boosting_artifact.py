"""Pure, dependency-light sanitizer and predictor for numeric tree projections.

This module never loads LightGBM, CatBoost, pickle, shared libraries, callbacks
or native model files.  Splits reference the already public cut geometry by
integer index, so a model cannot smuggle arbitrary strings, paths or metadata.
"""

import hashlib
import json
import math
import numbers
import struct


_MODEL_FIELDS = frozenset((
    "base_score", "contract", "engine", "public_schema_sha256", "task",
    "trees", "version",
))
_ENSEMBLE_FIELDS = frozenset((
    "aggregation", "contract", "engine", "models",
    "public_schema_sha256", "task", "version",
))
_LIGHTGBM_TREE_FIELDS = frozenset((
    "default_left", "leaf_values", "left_children", "right_children",
    "split_cut_indices", "split_indices",
))
_CATBOOST_TREE_FIELDS = frozenset(("leaf_values", "splits"))
_CATBOOST_SPLIT_FIELDS = frozenset((
    "cut_index", "default_left", "feature_index",
))
_MAX_ENSEMBLE_MODELS = 4096
_MAX_TOTAL_NODES = 1_000_000
_CONSTRUCTION_TOKEN = object()


def _float32(value):
    try:
        result = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    except (OverflowError, TypeError, ValueError, struct.error) as exc:
        raise ValueError("prediction feature is not representable as float32") from exc
    if not math.isfinite(result):
        raise ValueError("prediction feature is not representable as float32")
    return 0.0 if result == 0.0 else result


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _exact(value, fields, where):
    if not isinstance(value, dict) or frozenset(value) != fields:
        raise ValueError("%s has an unsupported shape" % where)
    return value


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _parse_json(artifact, byte_cap, where, *, canonical=False):
    if not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("%s must be bounded JSON bytes" % where)
    raw = bytes(artifact)
    if not 1 <= len(raw) <= byte_cap:
        raise ValueError("%s exceeds its artifact byte cap" % where)
    try:
        value = json.loads(
            raw.decode("ascii"), object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("%s JSON is invalid" % where) from exc
    if canonical and _canonical_json(value) != raw:
        raise ValueError("%s JSON is not canonical" % where)
    return value


def _integer(value, where, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError("%s is outside its integer domain" % where)
    return value


def _finite(value, where, lower, upper):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s is not a finite number" % where)
    result = float(value)
    if not math.isfinite(result) or not lower <= result <= upper:
        raise ValueError("%s is outside its finite domain" % where)
    return 0.0 if result == 0.0 else result


def _bounded_leaf(value, profile):
    try:
        return _finite(
            value, "leaf value", -profile["leaf_abs_cap"],
            profile["leaf_abs_cap"])
    except ValueError as exc:
        raise ValueError("leaf value exceeds its public bound") from exc


def _sanitize_lightgbm_tree(value, profile, node_budget):
    tree = _exact(value, _LIGHTGBM_TREE_FIELDS, "LightGBM tree")
    arrays = [tree[name] for name in sorted(_LIGHTGBM_TREE_FIELDS)]
    if any(not isinstance(array, list) for array in arrays):
        raise ValueError("LightGBM tree arrays are malformed")
    length = len(tree["left_children"])
    if not 1 <= length <= node_budget or any(
            len(array) != length for array in arrays):
        raise ValueError("LightGBM tree arrays differ or exceed their cap")

    left = [_integer(item, "left child", -1, length - 1)
            for item in tree["left_children"]]
    right = [_integer(item, "right child", -1, length - 1)
             for item in tree["right_children"]]
    if any(type(item) is not bool for item in tree["default_left"]):
        raise ValueError("LightGBM default direction is malformed")
    default_left = list(tree["default_left"])
    split_indices = [_integer(
        item, "split feature", 0, profile["features"] - 1)
        for item in tree["split_indices"]]
    split_cuts = []
    for node, item in enumerate(tree["split_cut_indices"]):
        feature = split_indices[node]
        split_cuts.append(_integer(
            item, "split cut", 0, len(profile["cuts"][feature]) - 1))
    leaves = [_bounded_leaf(item, profile) for item in tree["leaf_values"]]

    depths = [0] * length
    children = set()
    for node in range(length):
        is_leaf = left[node] == -1 and right[node] == -1
        if (left[node] == -1) != (right[node] == -1):
            raise ValueError("LightGBM node has only one child")
        if is_leaf:
            if default_left[node] or split_indices[node] != 0 or \
                    split_cuts[node] != 0:
                raise ValueError("LightGBM leaf carries split state")
            continue
        if leaves[node] != 0.0 or left[node] <= node or right[node] <= node or \
                left[node] == right[node]:
            raise ValueError("LightGBM topology or internal value is invalid")
        for child in (left[node], right[node]):
            if child in children:
                raise ValueError("LightGBM child has multiple parents")
            children.add(child)
            depths[child] = depths[node] + 1
            if depths[child] > profile["max_depth"]:
                raise ValueError("LightGBM tree exceeds its public depth")
    if children != set(range(1, length)):
        raise ValueError("LightGBM tree contains unreachable nodes")
    return {
        "default_left": default_left,
        "leaf_values": leaves,
        "left_children": left,
        "right_children": right,
        "split_cut_indices": split_cuts,
        "split_indices": split_indices,
    }, length


def _sanitize_catboost_tree(value, profile, node_budget):
    tree = _exact(value, _CATBOOST_TREE_FIELDS, "CatBoost tree")
    splits = tree["splits"]
    if not isinstance(splits, list) or not 1 <= len(splits) <= min(
            16, profile["max_depth"]):
        raise ValueError("CatBoost tree depth exceeds its public cap")
    if (1 << (len(splits) + 1)) - 1 > node_budget:
        raise ValueError("CatBoost tree exceeds its node cap")
    canonical_splits = []
    for split in splits:
        split = _exact(split, _CATBOOST_SPLIT_FIELDS, "CatBoost split")
        feature = _integer(
            split["feature_index"], "split feature", 0,
            profile["features"] - 1)
        cut = _integer(
            split["cut_index"], "split cut", 0,
            len(profile["cuts"][feature]) - 1)
        if type(split["default_left"]) is not bool:
            raise ValueError("CatBoost default direction is malformed")
        canonical_splits.append({
            "cut_index": cut,
            "default_left": split["default_left"],
            "feature_index": feature,
        })
    leaves = tree["leaf_values"]
    if not isinstance(leaves, list) or len(leaves) != 1 << len(splits):
        raise ValueError("CatBoost leaf array differs from its oblivious depth")
    return {
        "leaf_values": [_bounded_leaf(item, profile) for item in leaves],
        "splits": canonical_splits,
    }, (1 << (len(splits) + 1)) - 1


def sanitize_model(artifact, profile, *, engine, contract):
    """Return a canonical, metadata-free numeric model projection."""
    root = _exact(
        _parse_json(artifact, profile["max_artifact_bytes"], "boosting model"),
        _MODEL_FIELDS,
        "boosting model",
    )
    if root["contract"] != contract or root["engine"] != engine or \
            type(root["version"]) is not int or root["version"] != 1 or \
            root["task"] != profile["task"] or \
            root["public_schema_sha256"] != profile["public_schema_sha256"]:
        raise ValueError("boosting model identity differs from its profile")
    base_score = _finite(
        root["base_score"], "base score", -1.0e12, 1.0e12)
    if base_score != profile["base_score"]:
        raise ValueError("boosting model base score differs from its profile")
    trees = root["trees"]
    if not isinstance(trees, list) or len(trees) != profile["trees"]:
        raise ValueError("boosting tree count differs from its profile")
    node_budget = min(
        _MAX_TOTAL_NODES, max(1, profile["max_artifact_bytes"] // 24))
    sanitizer = (_sanitize_lightgbm_tree if engine == "lightgbm"
                 else _sanitize_catboost_tree)
    canonical_trees = []
    used_nodes = 0
    for tree in trees:
        canonical, nodes = sanitizer(tree, profile, node_budget - used_nodes)
        canonical_trees.append(canonical)
        used_nodes += nodes
    result = {
        "base_score": base_score,
        "contract": contract,
        "engine": engine,
        "public_schema_sha256": profile["public_schema_sha256"],
        "task": profile["task"],
        "trees": canonical_trees,
        "version": 1,
    }
    encoded = _canonical_json(result)
    if len(encoded) > profile["max_artifact_bytes"]:
        raise ValueError("sanitized boosting model exceeds its artifact byte cap")
    return encoded, hashlib.sha256(encoded).hexdigest()


def build_ensemble(model_artifacts, profile, *, engine, model_contract,
                   ensemble_contract):
    """Build one canonically ordered mean-prediction ensemble."""
    if not isinstance(model_artifacts, (list, tuple)) or not (
            1 <= len(model_artifacts) <= _MAX_ENSEMBLE_MODELS):
        raise ValueError("boosting ensemble model count is invalid")
    members = []
    for artifact in model_artifacts:
        encoded, digest = sanitize_model(
            artifact, profile, engine=engine, contract=model_contract)
        members.append((digest, encoded, json.loads(encoded.decode("ascii"))))
    members.sort(key=lambda item: (item[0], item[1]))
    result = {
        "aggregation": "mean_prediction",
        "contract": ensemble_contract,
        "engine": engine,
        "models": [item[2] for item in members],
        "public_schema_sha256": profile["public_schema_sha256"],
        "task": profile["task"],
        "version": 1,
    }
    encoded = _canonical_json(result)
    if len(encoded) > profile["max_artifact_bytes"]:
        raise ValueError("boosting ensemble exceeds its artifact byte cap")
    return encoded


def _extract_lightgbm(model):
    return tuple((
        tuple(tree["left_children"]), tuple(tree["right_children"]),
        tuple(tree["default_left"]), tuple(tree["split_indices"]),
        tuple(tree["split_cut_indices"]), tuple(tree["leaf_values"]),
    ) for tree in model["trees"])


def _extract_catboost(model):
    return tuple((
        tuple((split["feature_index"], split["cut_index"],
               split["default_left"]) for split in tree["splits"]),
        tuple(tree["leaf_values"]),
    ) for tree in model["trees"])


class BoostingEnsemble:
    """Opaque predictor constructed only from re-sanitized projections."""

    __slots__ = ("_bounds", "_cuts", "_engine", "_models", "_task")

    def __init__(self, token, profile, models):
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("use parse_ensemble")
        self._engine = profile["engine"]
        self._task = profile["task"]
        self._bounds = profile["feature_bounds"]
        self._cuts = profile["cuts"]
        self._models = models

    @property
    def task(self):
        return self._task

    @property
    def num_features(self):
        return len(self._bounds)

    @property
    def num_models(self):
        return len(self._models)

    def _row(self, row):
        if isinstance(row, (str, bytes, bytearray, memoryview)):
            raise ValueError("prediction row is malformed")
        try:
            values = list(row)
        except TypeError as exc:
            raise ValueError("prediction row is malformed") from exc
        if len(values) != len(self._bounds):
            raise ValueError("prediction row differs from the public schema")
        result = []
        for value, bounds in zip(values, self._bounds):
            if isinstance(value, bool) or not isinstance(value, numbers.Real):
                raise ValueError("prediction feature is not numeric")
            try:
                number = float(value)
            except (OverflowError, TypeError, ValueError) as exc:
                raise ValueError("prediction feature is not numeric") from exc
            if math.isnan(number):
                result.append(number)
            else:
                number = min(max(number, bounds[0]), bounds[1])
                result.append(
                    _float32(number) if self._engine == "catboost" else number)
        return tuple(result)

    def _lightgbm_margin(self, model, row):
        base_score, trees = model
        margin = base_score
        for left, right, default_left, features, cut_indices, leaves in trees:
            node = 0
            while left[node] != -1:
                value = row[features[node]]
                if math.isnan(value):
                    node = left[node] if default_left[node] else right[node]
                elif value <= self._cuts[features[node]][cut_indices[node]]:
                    node = left[node]
                else:
                    node = right[node]
            margin += leaves[node]
        return margin

    def _catboost_margin(self, model, row):
        base_score, trees = model
        margin = base_score
        for splits, leaves in trees:
            leaf = 0
            for level, (feature, cut_index, default_left) in enumerate(splits):
                value = row[feature]
                right = (not default_left if math.isnan(value)
                         else value > self._cuts[feature][cut_index])
                if right:
                    leaf |= 1 << level
            margin += leaves[leaf]
        return margin

    def predict_one(self, row):
        values = self._row(row)
        total = 0.0
        margin_function = (self._lightgbm_margin
                           if self._engine == "lightgbm"
                           else self._catboost_margin)
        for model in self._models:
            margin = margin_function(model, values)
            if not math.isfinite(margin):
                raise ValueError("boosting prediction is non-finite")
            if self._task == "binary":
                if margin >= 0.0:
                    prediction = 1.0 / (1.0 + math.exp(-margin))
                else:
                    exp_margin = math.exp(margin)
                    prediction = exp_margin / (1.0 + exp_margin)
            else:
                prediction = margin
            total += prediction
        result = total / len(self._models)
        if not math.isfinite(result):
            raise ValueError("boosting prediction is non-finite")
        return result

    def predict(self, rows):
        if isinstance(rows, (str, bytes, bytearray, memoryview)):
            raise ValueError("prediction rows must be a rank-two iterable")
        try:
            return [self.predict_one(row) for row in rows]
        except TypeError as exc:
            raise ValueError("prediction rows must be a rank-two iterable") from exc


def parse_ensemble(artifact, profile, *, engine, model_contract,
                   ensemble_contract):
    """Re-sanitize a canonical ensemble and return its pure predictor."""
    root = _exact(
        _parse_json(
            artifact, profile["max_artifact_bytes"], "boosting ensemble",
            canonical=True),
        _ENSEMBLE_FIELDS,
        "boosting ensemble",
    )
    if root["contract"] != ensemble_contract or root["engine"] != engine or \
            root["aggregation"] != "mean_prediction" or \
            type(root["version"]) is not int or root["version"] != 1 or \
            root["task"] != profile["task"] or \
            root["public_schema_sha256"] != profile["public_schema_sha256"]:
        raise ValueError("boosting ensemble identity differs from its profile")
    models = root["models"]
    if not isinstance(models, list) or not 1 <= len(models) <= \
            _MAX_ENSEMBLE_MODELS:
        raise ValueError("boosting ensemble model count is invalid")
    pairs = []
    extracted = []
    extractor = (_extract_lightgbm if engine == "lightgbm"
                 else _extract_catboost)
    for model in models:
        member = _canonical_json(model)
        sanitized, digest = sanitize_model(
            member, profile, engine=engine, contract=model_contract)
        if sanitized != member:
            raise ValueError("boosting ensemble member is not sanitized")
        pairs.append((digest, member))
        extracted.append((model["base_score"], extractor(model)))
    if pairs != sorted(pairs, key=lambda item: (item[0], item[1])):
        raise ValueError("boosting ensemble members are not canonically ordered")
    return BoostingEnsemble(_CONSTRUCTION_TOKEN, profile, tuple(extracted))


__all__ = [
    "BoostingEnsemble",
    "build_ensemble",
    "parse_ensemble",
    "sanitize_model",
]
