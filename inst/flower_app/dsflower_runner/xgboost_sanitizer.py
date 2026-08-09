"""Strict sanitizer for the pinned dsFlower XGBoost JSON profile.

This module removes accidental model-metadata channels.  It does not establish
the DP provenance of split and leaf values; the trusted native updater must
ensure that they are computed exclusively from privatized histograms.
"""

import hashlib
import json
import math
import re
import struct
from decimal import Decimal, InvalidOperation


XGBOOST_MODEL_VERSION = (3, 4, 0)
_ROOT_PARENT = 2_147_483_647
_FLOAT32_MAX = 3.4028234663852886e38
_FLOAT_TEXT = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?$"
)
_TREE_FIELDS = frozenset((
    "base_weights", "categories", "categories_nodes", "categories_segments",
    "categories_sizes", "default_left", "id", "left_children",
    "loss_changes", "parents", "right_children", "split_conditions",
    "split_indices", "split_type", "sum_hessian", "tree_param",
))


def _object_without_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _exact(value, fields, where):
    if not isinstance(value, dict) or frozenset(value) != frozenset(fields):
        raise ValueError("%s has an unsupported shape" % where)
    return value


def _integer(value, where, lower=0, upper=(1 << 63) - 1):
    if isinstance(value, bool) or not isinstance(value, int) or not (
            lower <= value <= upper):
        raise ValueError("%s is outside its integer domain" % where)
    return int(value)


def _finite(value, where, *, lower, upper):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s is not numeric" % where)
    result = float(value)
    if not math.isfinite(result) or result < lower or result > upper:
        raise ValueError("%s is outside its public numeric domain" % where)
    return 0.0 if result == 0.0 else result


def _finite_float32(value, where, *, lower, upper):
    result = _finite(value, where, lower=lower, upper=upper)
    try:
        result = struct.unpack(">f", struct.pack(">f", result))[0]
    except (OverflowError, struct.error) as exc:
        raise ValueError("%s is not representable as float32" % where) from exc
    if not math.isfinite(result) or result < lower or result > upper:
        raise ValueError("%s is outside its public float32 domain" % where)
    return 0.0 if result == 0.0 else result


def _next_float32(value, where):
    """Return the exact next float32 toward +infinity."""
    value = _finite_float32(
        value, where, lower=-_FLOAT32_MAX, upper=_FLOAT32_MAX)
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    if value >= 0.0:
        bits += 1
    else:
        bits -= 1
    result = struct.unpack(">f", struct.pack(">I", bits))[0]
    if not math.isfinite(result):
        raise ValueError("%s has no finite derived split threshold" % where)
    return result


def _decimal_text(value, where):
    if not isinstance(value, str):
        raise ValueError("%s is not a numeric string" % where)
    bracketed = value.startswith("[") and value.endswith("]")
    if value.startswith("[") != value.endswith("]"):
        raise ValueError("%s is not a numeric string" % where)
    text = value[1:-1] if bracketed else value
    if not _FLOAT_TEXT.fullmatch(text):
        raise ValueError("%s is not a numeric string" % where)
    try:
        result = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("%s is invalid" % where) from exc
    if not result.is_finite():
        raise ValueError("%s must be finite" % where)
    return result


def _array(value, length, where):
    if not isinstance(value, list) or len(value) != length:
        raise ValueError("%s length differs from num_nodes" % where)
    return value


def _fixed_integer_array(value, expected, where):
    if not isinstance(value, list) or len(value) != len(expected) or any(
            type(item) is not int or item != pin
            for item, pin in zip(value, expected)):
        raise ValueError("%s differs from the fixed profile" % where)
    return list(expected)


def _public_cuts(value, expected_features, numeric_abs_cap):
    if not isinstance(value, (list, tuple)) or len(value) != expected_features:
        raise ValueError("public cut geometry is invalid")
    cuts = []
    for feature in value:
        if not isinstance(feature, (list, tuple)) or not feature:
            raise ValueError("public cut geometry is invalid")
        canonical = tuple(
            _finite_float32(item, "public cut", lower=-numeric_abs_cap,
                            upper=numeric_abs_cap)
            for item in feature
        )
        if any(right <= left for left, right in zip(canonical, canonical[1:])):
            raise ValueError("public cuts must be strictly increasing")
        # Native bins use lower_bound (value <= cut is the left bin), whereas
        # RegTree traverses with value < threshold.  The sole allowed serialized
        # threshold is therefore the next float32 above the copied public cut.
        cuts.append(frozenset(
            _next_float32(item, "public cut") for item in canonical))
    return tuple(cuts)


def _sanitize_tree(tree, *, tree_id, expected_features, expected_max_depth,
                   public_cuts, max_total_nodes, numeric_abs_cap,
                   leaf_abs_cap):
    tree = _exact(tree, _TREE_FIELDS, "tree")
    if type(tree["id"]) is not int or tree["id"] != tree_id:
        raise ValueError("tree id is not canonical")
    param = _exact(
        tree["tree_param"],
        ("num_deleted", "num_feature", "num_nodes", "size_leaf_vector"),
        "tree parameters",
    )
    if param["num_deleted"] != "0" or \
            param["num_feature"] != str(expected_features) or \
            param["size_leaf_vector"] != "1":
        raise ValueError("tree parameters violate the single-output profile")
    try:
        num_nodes = int(param["num_nodes"], 10)
    except (TypeError, ValueError) as exc:
        raise ValueError("num_nodes is invalid") from exc
    if str(num_nodes) != param["num_nodes"] or not (
            1 <= num_nodes <= max_total_nodes):
        raise ValueError("num_nodes exceeds its public cap")

    for field in (
            "categories", "categories_nodes", "categories_segments",
            "categories_sizes"):
        if tree[field] != []:
            raise ValueError("categorical model state is forbidden")

    left = [
        _integer(item, "left child", -1, num_nodes - 1)
        for item in _array(tree["left_children"], num_nodes, "left children")
    ]
    right = [
        _integer(item, "right child", -1, num_nodes - 1)
        for item in _array(tree["right_children"], num_nodes, "right children")
    ]
    parents = [
        _integer(item, "parent", 0, _ROOT_PARENT)
        for item in _array(tree["parents"], num_nodes, "parents")
    ]
    default_left = [
        _integer(item, "default direction", 0, 1)
        for item in _array(tree["default_left"], num_nodes,
                           "default directions")
    ]
    split_type = [
        _integer(item, "split type", 0, 0)
        for item in _array(tree["split_type"], num_nodes, "split types")
    ]
    split_indices = [
        _integer(item, "split feature", 0, expected_features - 1)
        for item in _array(tree["split_indices"], num_nodes, "split indices")
    ]
    split_conditions = [
        _finite_float32(item, "split condition", lower=-numeric_abs_cap,
                        upper=numeric_abs_cap)
        for item in _array(tree["split_conditions"], num_nodes,
                           "split conditions")
    ]
    # Validate every native numeric array even though the three auxiliary
    # arrays are replaced with zeros below.  This keeps malformed native output
    # fail-closed while removing fields that prediction does not consume.
    for field, lower in (
            ("base_weights", -_FLOAT32_MAX), ("loss_changes", 0.0),
            ("sum_hessian", 0.0)):
        for item in _array(tree[field], num_nodes, field):
            _finite_float32(item, field, lower=lower, upper=_FLOAT32_MAX)

    if parents[0] != _ROOT_PARENT:
        raise ValueError("tree root parent sentinel is invalid")
    depths = [0] * num_nodes
    seen_children = set()
    for node in range(num_nodes):
        is_leaf = left[node] == -1 and right[node] == -1
        if (left[node] == -1) != (right[node] == -1):
            raise ValueError("tree node has only one child")
        if is_leaf:
            if default_left[node] != 0 or split_indices[node] != 0 or abs(
                    split_conditions[node]) > leaf_abs_cap:
                raise ValueError("leaf carries a forbidden channel")
            continue
        if left[node] <= node or right[node] <= node or left[node] == right[node]:
            raise ValueError("tree topology is not forward, binary and acyclic")
        for child in (left[node], right[node]):
            if child in seen_children or parents[child] != node:
                raise ValueError("tree parent/child topology is inconsistent")
            depths[child] = depths[node] + 1
            if depths[child] > expected_max_depth:
                raise ValueError("tree exceeds its public depth")
            seen_children.add(child)
        feature = split_indices[node]
        if split_conditions[node] not in public_cuts[feature]:
            raise ValueError("split threshold is not a complete public cut")
    if seen_children != set(range(1, num_nodes)):
        raise ValueError("tree contains unreachable nodes")

    zeros = [0.0] * num_nodes
    return {
        "base_weights": list(zeros),
        "categories": [],
        "categories_nodes": [],
        "categories_segments": [],
        "categories_sizes": [],
        "default_left": default_left,
        "id": tree_id,
        "left_children": left,
        "loss_changes": list(zeros),
        "parents": parents,
        "right_children": right,
        "split_conditions": split_conditions,
        "split_indices": split_indices,
        "split_type": split_type,
        "sum_hessian": list(zeros),
        "tree_param": {
            "num_deleted": "0",
            "num_feature": str(expected_features),
            "num_nodes": str(num_nodes),
            "size_leaf_vector": "1",
        },
    }, num_nodes


def sanitize_xgboost_json(
        artifact, *, expected_task, expected_features, expected_trees,
        expected_max_depth, public_cuts, expected_base_score,
        max_total_nodes, max_artifact_bytes, numeric_abs_cap=1.0e30,
        leaf_abs_cap=1.0e6, expected_version=XGBOOST_MODEL_VERSION):
    """Return canonical prediction-only XGBoost JSON bytes and SHA-256."""
    for value, name in (
            (expected_features, "expected_features"),
            (expected_trees, "expected_trees"),
            (expected_max_depth, "expected_max_depth"),
            (max_total_nodes, "max_total_nodes"),
            (max_artifact_bytes, "max_artifact_bytes")):
        _integer(value, name, 1, (1 << 53) - 1)
    numeric_abs_cap = _finite(
        numeric_abs_cap, "numeric_abs_cap", lower=1.0, upper=1.0e100)
    leaf_abs_cap = _finite(
        leaf_abs_cap, "leaf_abs_cap", lower=0.0, upper=numeric_abs_cap)
    base_score = _finite_float32(
        expected_base_score, "expected_base_score", lower=-numeric_abs_cap,
        upper=numeric_abs_cap)
    cuts = _public_cuts(public_cuts, expected_features, numeric_abs_cap)
    if not isinstance(expected_version, (list, tuple)) or any(
            type(item) is not int for item in expected_version) or \
            tuple(expected_version) != XGBOOST_MODEL_VERSION:
        raise ValueError("XGBoost model version is not the pinned profile")
    if not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("artifact exceeds its public byte cap")
    artifact = bytes(artifact)
    if not 1 <= len(artifact) <= max_artifact_bytes:
        raise ValueError("artifact exceeds its public byte cap")
    try:
        root = json.loads(
            artifact.decode("utf-8"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("artifact JSON is invalid") from exc
    root = _exact(root, ("learner", "version"), "model root")
    _fixed_integer_array(
        root["version"], list(XGBOOST_MODEL_VERSION), "model version")

    learner = _exact(
        root["learner"],
        ("attributes", "feature_names", "feature_types", "gradient_booster",
         "learner_model_param", "objective"),
        "learner",
    )
    if learner["attributes"] != {} or learner["feature_names"] != [] or \
            learner["feature_types"] != []:
        raise ValueError("learner metadata is a forbidden egress channel")
    model_param = _exact(
        learner["learner_model_param"],
        ("base_score", "boost_from_average", "num_class", "num_feature",
         "num_target"),
        "learner model parameters",
    )
    if model_param["boost_from_average"] != "0" or \
            model_param["num_class"] != "0" or \
            model_param["num_feature"] != str(expected_features) or \
            model_param["num_target"] != "1":
        raise ValueError("learner model parameters violate the fixed profile")
    parsed_base_score = _decimal_text(model_param["base_score"], "base score")
    try:
        model_base_score = float(parsed_base_score)
    except (OverflowError, ValueError) as exc:
        raise ValueError("model base score is not representable") from exc
    if _finite_float32(
            model_base_score, "model base score", lower=-numeric_abs_cap,
            upper=numeric_abs_cap) != base_score:
        raise ValueError("model base score differs from its public pin")

    objective_name = {
        "binary_classification": "binary:logistic",
        "regression": "reg:squarederror",
    }.get(expected_task)
    if objective_name is None:
        raise ValueError("unsupported task")
    objective = _exact(
        learner["objective"], ("name", "reg_loss_param"), "objective")
    if objective != {
            "name": objective_name,
            "reg_loss_param": {"scale_pos_weight": "1"}}:
        raise ValueError("objective contains a non-server-owned setting")

    booster = _exact(
        learner["gradient_booster"], ("model", "name"), "booster")
    if booster["name"] != "gbtree":
        raise ValueError("only the gbtree booster is allowed")
    model = _exact(
        booster["model"],
        ("cats", "gbtree_model_param", "iteration_indptr", "tree_info",
         "trees"),
        "gbtree model",
    )
    cats = _exact(
        model["cats"], ("enc", "feature_segments", "sorted_idx"), "cats")
    if cats != {"enc": [], "feature_segments": [], "sorted_idx": []}:
        raise ValueError("categorical state is forbidden")
    gbtree_param = _exact(
        model["gbtree_model_param"], ("num_parallel_tree", "num_trees"),
        "gbtree parameters",
    )
    if gbtree_param != {
            "num_parallel_tree": "1", "num_trees": str(expected_trees)}:
        raise ValueError("gbtree geometry differs from the public schedule")
    iteration_indptr = list(range(expected_trees + 1))
    tree_info = [0] * expected_trees
    _fixed_integer_array(
        model["iteration_indptr"], iteration_indptr, "iteration pointers")
    _fixed_integer_array(model["tree_info"], tree_info, "tree info")
    if not isinstance(model["trees"], list) or \
            len(model["trees"]) != expected_trees:
        raise ValueError("model iteration/output geometry is invalid")

    sanitized_trees = []
    total_nodes = 0
    for tree_id, tree in enumerate(model["trees"]):
        sanitized, num_nodes = _sanitize_tree(
            tree, tree_id=tree_id, expected_features=expected_features,
            expected_max_depth=expected_max_depth, public_cuts=cuts,
            max_total_nodes=max_total_nodes, numeric_abs_cap=numeric_abs_cap,
            leaf_abs_cap=leaf_abs_cap,
        )
        total_nodes += num_nodes
        if total_nodes > max_total_nodes:
            raise ValueError("model exceeds its public total-node cap")
        sanitized_trees.append(sanitized)

    canonical_model_param = {
        "base_score": "[" + format(base_score, ".17g") + "]",
        "boost_from_average": "0",
        "num_class": "0",
        "num_feature": str(expected_features),
        "num_target": "1",
    }
    safe = {
        "learner": {
            "attributes": {},
            "feature_names": [],
            "feature_types": [],
            "gradient_booster": {
                "model": {
                    "cats": {"enc": [], "feature_segments": [],
                             "sorted_idx": []},
                    "gbtree_model_param": dict(gbtree_param),
                    "iteration_indptr": iteration_indptr,
                    "tree_info": tree_info,
                    "trees": sanitized_trees,
                },
                "name": "gbtree",
            },
            "learner_model_param": canonical_model_param,
            "objective": dict(objective),
        },
        "version": list(XGBOOST_MODEL_VERSION),
    }
    encoded = json.dumps(
        safe, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if len(encoded) > max_artifact_bytes:
        raise ValueError("sanitized artifact exceeds its public byte cap")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = ["XGBOOST_MODEL_VERSION", "sanitize_xgboost_json"]
