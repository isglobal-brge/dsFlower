"""Exact fail-closed profiles for private boosting-style runners.

The profiles bind dsFlower's reviewed reference trainers and safe numeric
prediction projections.  They do not advertise an upstream LightGBM/CatBoost
runtime or enable a public Flower capability; that requires a separate E2E
release gate.
"""

import math
import struct

from . import native_tree_contract as tree_contract


_MECHANISM_PARAMETERS = {
    "gradient_clip": "float",
    "hessian_clip": "float",
}
_LIGHTGBM_PARAMETERS = {
    "base_score": "float",
    "lambda_l1": "float",
    "lambda_l2": "float",
    "learning_rate": "float",
    "max_bin": "int",
    "max_delta_step": "float",
    "max_depth": "int",
    "min_data_in_leaf": "int",
    "min_gain_to_split": "float",
    "num_leaves": "int",
    "num_iterations": "int",
}
_CATBOOST_PARAMETERS = {
    "base_score": "float",
    "border_count": "int",
    "depth": "int",
    "iterations": "int",
    "l2_leaf_reg": "float",
    "learning_rate": "float",
    "max_delta_step": "float",
}
_SUPPORTED = frozenset(("lightgbm", "catboost"))


def _float32(value, where):
    try:
        result = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    except (OverflowError, TypeError, ValueError, struct.error) as exc:
        raise ValueError("%s is not representable as float32" % where) from exc
    if not math.isfinite(result):
        raise ValueError("%s is not representable as finite float32" % where)
    return 0.0 if result == 0.0 else result


def _exact_typed(parameters, expected, where):
    if not isinstance(parameters, dict) or frozenset(parameters) != frozenset(
            expected):
        raise ValueError("%s parameter profile must be exact" % where)
    values = {}
    for name, expected_type in expected.items():
        record = parameters[name]
        if not isinstance(record, dict) or frozenset(record) != frozenset((
                "type", "value")) or record["type"] != expected_type:
            raise ValueError("%s parameter has the wrong declared type" % where)
        values[name] = record["value"]
    return values


def _range(value, where, lower, upper, *, lower_open=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s is outside its supported range" % where)
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("%s is outside its supported range" % where)
    lower_ok = result > lower if lower_open else result >= lower
    if not lower_ok or result > upper:
        raise ValueError("%s is outside its supported range" % where)
    return result


def _integer_range(value, where, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError("%s is outside its supported range" % where)
    return value


def _base_profile(manifest, engine, parameter_types):
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != engine or canonical["mode"] != "native-tight":
        raise ValueError("profile engine or mode mismatch")
    if canonical["privacy"]["mechanism"] != "dp-histogram-v1" or \
            canonical["privacy"]["delta"] <= 0.0:
        raise ValueError("boosting profile requires the fixed DP histogram mechanism")
    mechanism = _exact_typed(
        canonical["privacy"]["mechanism_params"],
        _MECHANISM_PARAMETERS,
        "mechanism",
    )
    gradient_clip = _range(
        mechanism["gradient_clip"], "gradient_clip", 0.0,
        tree_contract.MAX_FLOAT_ABS, lower_open=True)
    hessian_clip = _range(
        mechanism["hessian_clip"], "hessian_clip", 0.0,
        tree_contract.MAX_FLOAT_ABS, lower_open=True)

    parameters = _exact_typed(
        canonical["engine_params"], parameter_types, engine)
    schema = canonical["public_schema"]
    cuts = schema["cuts"]
    if not isinstance(cuts, list) or not cuts or any(
            not isinstance(feature, list) or not feature for feature in cuts):
        raise ValueError("boosting profile requires complete public cut geometry")
    expected_base = (
        0.0 if canonical["task"] == "binary_classification"
        else float(schema["target"]["lower"]) + (
            float(schema["target"]["upper"]) -
            float(schema["target"]["lower"])) / 2.0
    )
    if parameters["base_score"] != expected_base:
        raise ValueError("base_score differs from its public task derivation")
    learning_rate = _range(
        parameters["learning_rate"], "learning_rate", 0.0, 1.0,
        lower_open=True)
    max_delta_step = _range(
        parameters["max_delta_step"], "max_delta_step", 0.0,
        tree_contract.MAX_FLOAT_ABS, lower_open=True)
    leaf_product = learning_rate * max_delta_step
    if not math.isfinite(leaf_product):
        raise ValueError("derived leaf bound is outside its supported range")
    leaf_abs_cap = math.nextafter(leaf_product, math.inf)
    if not math.isfinite(leaf_abs_cap):
        raise ValueError("derived leaf bound is outside its supported range")
    return canonical, parameters, {
        "base_score": expected_base,
        "cuts": tuple(tuple(float(cut) for cut in feature) for feature in cuts),
        "feature_bounds": tuple(zip(
            (float(value) for value in schema["lower"]),
            (float(value) for value in schema["upper"]),
        )),
        "features": len(schema["features"]),
        "gradient_clip": gradient_clip,
        "hessian_clip": hessian_clip,
        "leaf_abs_cap": leaf_abs_cap,
        "learning_rate": learning_rate,
        "max_delta_step": max_delta_step,
        "public_schema_sha256": schema["sha256"],
        "task": ("binary" if canonical["task"] == "binary_classification"
                 else "regression"),
    }


def lightgbm_profile(manifest):
    """Validate the complete narrow LightGBM adapter/prediction profile."""
    canonical, parameters, result = _base_profile(
        manifest, "lightgbm", _LIGHTGBM_PARAMETERS)
    resources = canonical["resources"]
    max_depth = _integer_range(
        parameters["max_depth"], "max_depth", 1,
        min(32, resources["max_depth"]))
    trees = _integer_range(
        parameters["num_iterations"], "num_iterations", 1,
        resources["max_trees"])
    num_leaves = _integer_range(
        parameters["num_leaves"], "num_leaves", 2,
        min(256, 1 << min(max_depth, 8)))
    max_bin = _integer_range(
        parameters["max_bin"], "max_bin", 2, resources["max_bins"])
    if max_bin != max(len(feature) + 1 for feature in result["cuts"]):
        raise ValueError("max_bin differs from the public cut geometry")
    min_data = _integer_range(
        parameters["min_data_in_leaf"], "min_data_in_leaf", 1,
        resources["max_rows"])
    lambda_l1 = _range(
        parameters["lambda_l1"], "lambda_l1", 0.0,
        tree_contract.MAX_FLOAT_ABS)
    lambda_l2 = _range(
        parameters["lambda_l2"], "lambda_l2", 0.0,
        tree_contract.MAX_FLOAT_ABS, lower_open=True)
    min_gain = _range(
        parameters["min_gain_to_split"], "min_gain_to_split", 0.0,
        tree_contract.MAX_FLOAT_ABS)
    return dict(result, engine="lightgbm", trees=trees,
                max_depth=max_depth, max_bin=max_bin,
                num_leaves=num_leaves,
                min_data_in_leaf=min_data, lambda_l1=lambda_l1,
                lambda_l2=lambda_l2, min_gain_to_split=min_gain,
                feature_cast="float64",
                max_artifact_bytes=resources["max_artifact_bytes"])


def catboost_profile(manifest):
    """Validate the complete narrow numeric-only CatBoost profile."""
    canonical, parameters, result = _base_profile(
        manifest, "catboost", _CATBOOST_PARAMETERS)
    resources = canonical["resources"]
    max_depth = _integer_range(
        parameters["depth"], "depth", 1, min(16, resources["max_depth"]))
    trees = _integer_range(
        parameters["iterations"], "iterations", 1,
        resources["max_trees"])
    border_count = _integer_range(
        parameters["border_count"], "border_count", 1,
        resources["max_bins"] - 1)
    if border_count != max(len(feature) for feature in result["cuts"]):
        raise ValueError("border_count differs from the public cut geometry")
    bounds = tuple(
        (_float32(lower, "CatBoost public lower bound"),
         _float32(upper, "CatBoost public upper bound"))
        for lower, upper in result["feature_bounds"])
    cuts = tuple(
        tuple(_float32(cut, "CatBoost public cut") for cut in feature)
        for feature in result["cuts"])
    if any(lower >= upper for lower, upper in bounds) or any(
            any(right <= left for left, right in zip(feature, feature[1:])) or
            any(cut <= bounds[index][0] or cut >= bounds[index][1]
                for cut in feature)
            for index, feature in enumerate(cuts)):
        raise ValueError("CatBoost public cut geometry collapses as float32")
    l2_leaf_reg = _range(
        parameters["l2_leaf_reg"], "l2_leaf_reg", 0.0,
        tree_contract.MAX_FLOAT_ABS, lower_open=True)
    return dict(result, engine="catboost", trees=trees,
                max_depth=max_depth, border_count=border_count,
                l2_leaf_reg=l2_leaf_reg,
                cuts=cuts, feature_bounds=bounds, feature_cast="float32",
                max_artifact_bytes=resources["max_artifact_bytes"])


def training_capability(engine):
    """Describe the honest node-local gate; this is not a privacy ACL."""
    if engine not in _SUPPORTED:
        raise ValueError("unsupported boosting engine")
    return {
        "available": False,
        "engine": engine,
        "reason": "verified-native-runner-unavailable",
        "required_profile": "fixed-point-public-cuts-prf-v1",
    }


def reject_unverified_native_artifact(engine, artifact):
    """Reject native serialization until its pure sanitizer is implemented."""
    if engine not in _SUPPORTED or not isinstance(
            artifact, (bytes, bytearray, memoryview)):
        raise ValueError("verified native boosting sanitizer is unavailable")
    raise ValueError("verified native boosting sanitizer is unavailable")


__all__ = [
    "catboost_profile",
    "lightgbm_profile",
    "reject_unverified_native_artifact",
    "training_capability",
]
