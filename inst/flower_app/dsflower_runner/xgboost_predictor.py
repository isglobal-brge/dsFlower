"""Dependency-light predictor for canonical sanitized XGBoost ensembles."""

import json
import math
import numbers
import struct

from . import native_tree_contract as tree_contract
from .xgboost_sanitizer import sanitize_xgboost_json


ENSEMBLE_CONTRACT = "dsflower-xgboost-ensemble-v1"
EXTERNAL_ENSEMBLE_CONTRACT = "dsflower-external-xgboost-ensemble-v1"
_ENSEMBLE_FIELDS = frozenset((
    "aggregation", "contract", "engine", "models",
    "public_schema_sha256", "task", "version",
))
_ENGINE_PARAMETERS = {
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
_MAX_NATIVE_DEPTH = 30
_CONSTRUCTION_TOKEN = object()


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _exact(value, fields, where):
    if not isinstance(value, dict) or frozenset(value) != frozenset(fields):
        raise ValueError("%s has an unsupported shape" % where)
    return value


def _float32(value, where):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s is not numeric" % where)
    try:
        result = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    except (OverflowError, TypeError, ValueError, struct.error) as exc:
        raise ValueError("%s is not representable as float32" % where) from exc
    if not math.isfinite(result):
        raise ValueError("%s must be finite" % where)
    return 0.0 if result == 0.0 else result


def _next_float32(value, where):
    value = _float32(value, where)
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    bits = bits + 1 if value >= 0.0 else bits - 1
    result = struct.unpack(">f", struct.pack(">I", bits))[0]
    if not math.isfinite(result):
        raise ValueError("%s has no finite successor" % where)
    return result


def _parameter(parameters, name, kind):
    record = _exact(parameters[name], ("type", "value"), name)
    if record["type"] != kind:
        raise ValueError("XGBoost parameter type mismatch")
    value = record["value"]
    if kind == "int":
        if type(value) is not int:
            raise ValueError("XGBoost integer parameter is malformed")
    elif isinstance(value, bool) or not isinstance(value, (int, float)) or \
            not math.isfinite(float(value)):
        raise ValueError("XGBoost float parameter is malformed")
    return value


def _prediction_profile(manifest):
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != "xgboost" or canonical["mode"] != "native-tight":
        raise ValueError("predictor requires the native-tight XGBoost profile")
    parameters = canonical["engine_params"]
    if frozenset(parameters) != frozenset(_ENGINE_PARAMETERS):
        raise ValueError("XGBoost parameter profile has unknown or missing fields")
    values = {
        name: _parameter(parameters, name, kind)
        for name, kind in _ENGINE_PARAMETERS.items()
    }
    trees = values["num_boost_round"]
    depth = values["max_depth"]
    max_bin = values["max_bin"]
    if type(trees) is not int or type(depth) is not int or type(max_bin) is not int or \
            trees < 1 or depth < 1 or depth > _MAX_NATIVE_DEPTH or max_bin < 2:
        raise ValueError("XGBoost prediction geometry is outside its profile")

    schema = canonical["public_schema"]
    cuts = schema["cuts"]
    if not isinstance(cuts, list) or len(cuts) != len(schema["features"]) or \
            not cuts or any(not isinstance(feature, list) or not feature
                            for feature in cuts):
        raise ValueError("XGBoost public cuts are incomplete")
    if max_bin != max(len(feature) + 1 for feature in cuts):
        raise ValueError("max_bin differs from the public cut geometry")
    bounds = []
    for index, (lower_value, upper_value) in enumerate(zip(
            schema["lower"], schema["upper"])):
        lower = _float32(lower_value, "feature %d lower bound" % index)
        upper = _float32(upper_value, "feature %d upper bound" % index)
        if lower >= upper:
            raise ValueError("feature bounds are not strict float32 values")
        if any(not lower < _float32(
                cut, "feature %d cut" % index) < upper
                for cut in cuts[index]):
            raise ValueError("public cuts are outside the float32 bounds")
        bounds.append((lower, upper))

    learning_rate = _float32(values["learning_rate"], "learning_rate")
    max_delta_step = _float32(values["max_delta_step"], "max_delta_step")
    if not 0.0 < learning_rate <= 1.0 or max_delta_step <= 0.0:
        raise ValueError("XGBoost leaf profile is invalid")
    leaf_product = _float32(
        learning_rate * max_delta_step, "derived leaf bound")
    leaf_abs_cap = _next_float32(leaf_product, "derived leaf bound")

    target = schema["target"]
    if canonical["task"] == "binary_classification":
        expected_base = 0.5
        ensemble_task = "binary"
    else:
        lower = _float32(target["lower"], "target lower bound")
        upper = _float32(target["upper"], "target upper bound")
        if lower >= upper:
            raise ValueError("target bounds are not strict float32 values")
        expected_base = _float32(
            lower + (upper - lower) / 2.0, "derived base score")
        ensemble_task = "regression"
    if _float32(values["base_score"], "base score") != expected_base:
        raise ValueError("base score differs from the public profile")

    artifact_cap = canonical["resources"]["max_artifact_bytes"]
    theoretical_nodes = trees * ((1 << (depth + 1)) - 1)
    arguments = {
        "expected_task": canonical["task"],
        "expected_features": len(schema["features"]),
        "expected_trees": trees,
        "expected_max_depth": depth,
        "public_cuts": cuts,
        "expected_base_score": expected_base,
        "max_total_nodes": min(theoretical_nodes, artifact_cap),
        "max_artifact_bytes": artifact_cap,
        "numeric_abs_cap": tree_contract.MAX_FLOAT_ABS,
        "leaf_abs_cap": leaf_abs_cap,
    }
    return canonical, arguments, ensemble_task, expected_base, tuple(bounds)


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _parse_artifact(artifact, byte_cap):
    if not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("XGBoost ensemble must be JSON bytes")
    raw = bytes(artifact)
    if not 1 <= len(raw) <= byte_cap:
        raise ValueError("XGBoost ensemble exceeds its artifact byte cap")
    try:
        value = json.loads(
            raw.decode("ascii"), object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("XGBoost ensemble JSON is invalid") from exc
    if _canonical_json(value) != raw:
        raise ValueError("XGBoost ensemble JSON is not canonical")
    return _exact(value, _ENSEMBLE_FIELDS, "XGBoost ensemble")


def _extract_model(model):
    learner = model["learner"]
    trees = learner["gradient_booster"]["model"]["trees"]
    extracted = []
    for tree in trees:
        extracted.append((
            tuple(tree["left_children"]), tuple(tree["right_children"]),
            tuple(tree["default_left"]), tuple(tree["split_indices"]),
            tuple(tree["split_conditions"]),
        ))
    return tuple(extracted)


class XGBoostEnsemble:
    """Opaque, already re-sanitized prediction-only ensemble."""

    __slots__ = ("_base_score", "_bounds", "_features", "_models", "_task")

    def __init__(self, token, task, features, base_score, bounds, models):
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("use parse_xgboost_ensemble")
        self._task = task
        self._features = features
        self._base_score = base_score
        self._bounds = bounds
        self._models = models

    @property
    def task(self):
        return self._task

    @property
    def num_features(self):
        return self._features

    @property
    def num_models(self):
        return len(self._models)

    def predict_one(self, row):
        values = _prediction_row(row, self._features, self._bounds)
        total = 0.0
        for model in self._models:
            margin = (math.log(self._base_score / (1.0 - self._base_score))
                      if self._task == "binary" else self._base_score)
            for left, right, default_left, split_indices, conditions in model:
                node = 0
                while left[node] != -1:
                    value = values[split_indices[node]]
                    if math.isnan(value):
                        node = left[node] if default_left[node] else right[node]
                    elif value < conditions[node]:
                        node = left[node]
                    else:
                        node = right[node]
                margin += conditions[node]
            if self._task == "binary":
                if margin >= 0.0:
                    prediction = 1.0 / (1.0 + math.exp(-margin))
                else:
                    exp_margin = math.exp(margin)
                    prediction = exp_margin / (1.0 + exp_margin)
            else:
                prediction = margin
            total += prediction
        return total / len(self._models)

    def predict(self, rows):
        if isinstance(rows, (str, bytes, bytearray, memoryview)):
            raise ValueError("prediction rows must be a rank-two iterable")
        try:
            return [self.predict_one(row) for row in rows]
        except TypeError as exc:
            raise ValueError("prediction rows must be a rank-two iterable") from exc


def _prediction_row(row, expected_features, bounds):
    if isinstance(row, (str, bytes, bytearray, memoryview)):
        raise ValueError("prediction row is malformed")
    try:
        values = list(row)
    except TypeError as exc:
        raise ValueError("prediction row is malformed") from exc
    if len(values) != expected_features:
        raise ValueError("prediction row differs from the public schema")
    result = []
    for index, value in enumerate(values):
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise ValueError("prediction feature is not numeric")
        try:
            converted = float(value)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ValueError("prediction feature is not numeric") from exc
        if math.isnan(converted):
            result.append(float("nan"))
        else:
            lower, upper = bounds[index]
            converted = min(max(converted, lower), upper)
            result.append(_float32(converted, "prediction feature"))
    return tuple(result)


def parse_xgboost_ensemble(artifact, manifest):
    """Parse and re-sanitize one canonical mean-prediction ensemble."""
    canonical, arguments, task, base_score, bounds = _prediction_profile(manifest)
    container = _parse_artifact(
        artifact, canonical["resources"]["max_artifact_bytes"])
    if container["contract"] not in (
            ENSEMBLE_CONTRACT, EXTERNAL_ENSEMBLE_CONTRACT) or \
            container["version"] != 1 or container["engine"] != "xgboost" or \
            container["aggregation"] != "mean_prediction":
        raise ValueError("unsupported XGBoost ensemble contract")
    if container["task"] != task:
        raise ValueError("XGBoost ensemble task differs from its manifest")
    if container["public_schema_sha256"] != canonical["public_schema"]["sha256"]:
        raise ValueError("XGBoost ensemble schema differs from its manifest")
    if not isinstance(container["models"], list) or not container["models"]:
        raise ValueError("XGBoost ensemble requires at least one model")

    pairs = []
    models = []
    for model in container["models"]:
        member = _canonical_json(model)
        sanitized, digest = sanitize_xgboost_json(member, **arguments)
        if member != sanitized:
            raise ValueError("XGBoost ensemble member is not canonical and sanitized")
        pairs.append((digest, sanitized))
        models.append(_extract_model(model))
    if pairs != sorted(pairs, key=lambda item: (item[0], item[1])):
        raise ValueError("XGBoost ensemble members are not canonically ordered")
    return XGBoostEnsemble(
        _CONSTRUCTION_TOKEN, task, arguments["expected_features"],
        base_score, bounds, tuple(models))


def predict_xgboost_ensemble(artifact, manifest, rows):
    """Parse a canonical ensemble and predict a rank-two public matrix."""
    return parse_xgboost_ensemble(artifact, manifest).predict(rows)


__all__ = [
    "ENSEMBLE_CONTRACT",
    "EXTERNAL_ENSEMBLE_CONTRACT",
    "XGBoostEnsemble",
    "parse_xgboost_ensemble",
    "predict_xgboost_ensemble",
]
