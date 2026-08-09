"""Dependency-light predictor for canonical private forest ensembles."""

import bisect
import hashlib
import json
import math
import numbers
import struct

from . import forest_accounting, native_tree_contract as tree_contract
from .forest_sanitizer import sanitize_forest_json


ENSEMBLE_CONTRACT = "dsflower-forest-ensemble-v1"
_ENSEMBLE_FIELDS = frozenset((
    "aggregation", "contract", "engine", "models",
    "public_schema_sha256", "task", "version",
))
_ENGINE_PARAMETERS = {"max_depth": "int", "n_estimators": "int"}
_MECHANISM_PARAMETERS = {"leaf_release": "string", "topology": "string"}
_CONSTRUCTION_TOKEN = object()


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("forest ensemble has duplicate JSON keys")
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


def _parameter(parameters, name, kind):
    record = _exact(parameters[name], ("type", "value"), name)
    if record["type"] != kind:
        raise ValueError("ExtraTrees parameter type mismatch")
    value = record["value"]
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("ExtraTrees integer parameter is malformed")
    elif not isinstance(value, str):
        raise ValueError("ExtraTrees string parameter is malformed")
    return value


def _prediction_profile(manifest):
    canonical = tree_contract.canonical_engine_manifest(manifest)
    if canonical["engine"] != "extra_trees" or \
            canonical["mode"] != "native-tight":
        raise ValueError("predictor requires native-tight ExtraTrees")
    parameters = canonical["engine_params"]
    if frozenset(parameters) != frozenset(_ENGINE_PARAMETERS):
        raise ValueError("ExtraTrees parameter profile is incomplete")
    values = {
        name: _parameter(parameters, name, kind)
        for name, kind in _ENGINE_PARAMETERS.items()
    }
    trees = values["n_estimators"]
    depth = values["max_depth"]
    if not 1 <= trees <= min(512, canonical["resources"]["max_trees"]) or \
            not 1 <= depth <= min(12, canonical["resources"]["max_depth"]):
        raise ValueError("ExtraTrees prediction geometry is outside its profile")
    if 2 * trees * (1 << depth) > 8_000_000:
        raise ValueError("ExtraTrees prediction vector exceeds its profile")

    mechanism = canonical["privacy"]["mechanism_params"]
    if frozenset(mechanism) != frozenset(_MECHANISM_PARAMETERS):
        raise ValueError("ExtraTrees mechanism profile is incomplete")
    pinned = {
        name: _parameter(mechanism, name, kind)
        for name, kind in _MECHANISM_PARAMETERS.items()
    }
    if pinned["topology"] != forest_accounting.TOPOLOGY_PROFILE or \
            pinned["leaf_release"] != forest_accounting.LEAF_RELEASE_PROFILE:
        raise ValueError("ExtraTrees mechanism profile differs from its pins")

    schema = canonical["public_schema"]
    cuts = []
    bounds = []
    for index, (raw_lower, raw_upper, raw_cuts) in enumerate(zip(
            schema["lower"], schema["upper"], schema["cuts"])):
        lower = _float32(raw_lower, "feature lower bound")
        upper = _float32(raw_upper, "feature upper bound")
        feature_cuts = tuple(_float32(value, "public cut")
                             for value in raw_cuts)
        if lower >= upper or not feature_cuts or any(
                right <= left for left, right in zip(
                    feature_cuts, feature_cuts[1:])) or any(
                        not lower < value < upper for value in feature_cuts):
            raise ValueError("ExtraTrees public bins are outside their profile")
        bounds.append((lower, upper))
        cuts.append(feature_cuts)
    target = schema["target"]
    target_lower = _float32(target["lower"], "target lower bound")
    target_upper = _float32(target["upper"], "target upper bound")
    if target_lower >= target_upper:
        raise ValueError("ExtraTrees target bounds are not strict")
    task = ("binary" if canonical["task"] == "binary_classification"
            else "regression")
    arguments = {
        "expected_engine": "extra_trees",
        "expected_task": canonical["task"],
        "expected_features": len(schema["features"]),
        "expected_trees": trees,
        "expected_depth": depth,
        "public_cut_counts": [len(value) for value in cuts],
        "public_schema_sha256": schema["sha256"],
        "target_lower": target_lower,
        "target_upper": target_upper,
        "max_artifact_bytes": canonical["resources"]["max_artifact_bytes"],
    }
    return canonical, arguments, task, tuple(bounds), tuple(cuts)


def _parse(artifact, byte_cap):
    if not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("forest ensemble must be JSON bytes")
    raw = bytes(artifact)
    if not 1 <= len(raw) <= byte_cap:
        raise ValueError("forest ensemble exceeds its byte ceiling")
    try:
        value = json.loads(
            raw.decode("ascii"), object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite forest number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("forest ensemble JSON is invalid") from exc
    if _canonical_json(value) != raw:
        raise ValueError("forest ensemble JSON is not canonical")
    return value


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _extract_model(model):
    return tuple((
        tuple(tree["features"]), tuple(tree["cut_indices"]),
        tuple(tree["default_left"]), tuple(tree["leaf_values"]),
    ) for tree in model["trees"])


class ForestEnsemble:
    """Parsed prediction-only forest; construct with parse_forest_ensemble."""

    __slots__ = ("_bounds", "_cuts", "_depth", "_features", "_models",
                 "_task")

    def __init__(self, token, task, features, depth, bounds, cuts, models):
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("use parse_forest_ensemble")
        self._task = task
        self._features = features
        self._depth = depth
        self._bounds = bounds
        self._cuts = cuts
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
        values = _prediction_row(row, self._bounds)
        binned = tuple(
            None if math.isnan(value) else bisect.bisect_left(cuts, value)
            for value, cuts in zip(values, self._cuts))
        model_total = 0.0
        internal = (1 << self._depth) - 1
        for model in self._models:
            tree_total = 0.0
            for features, cut_indices, defaults, leaves in model:
                node = 0
                for _level in range(self._depth):
                    value = binned[features[node]]
                    go_left = defaults[node] if value is None else \
                        value <= cut_indices[node]
                    node = 2 * node + 1 + int(not go_left)
                tree_total += leaves[node - internal]
            model_total += tree_total / len(model)
        return model_total / len(self._models)

    def predict(self, rows):
        if isinstance(rows, (str, bytes, bytearray, memoryview)):
            raise ValueError("prediction rows must be a rank-two iterable")
        try:
            return [self.predict_one(row) for row in rows]
        except TypeError as exc:
            raise ValueError("prediction rows must be a rank-two iterable") from exc


def _prediction_row(row, bounds):
    if isinstance(row, (str, bytes, bytearray, memoryview)):
        raise ValueError("prediction row is malformed")
    try:
        values = list(row)
    except TypeError as exc:
        raise ValueError("prediction row is malformed") from exc
    if len(values) != len(bounds):
        raise ValueError("prediction row differs from the public schema")
    result = []
    for value, (lower, upper) in zip(values, bounds):
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise ValueError("prediction feature is not numeric")
        try:
            converted = float(value)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ValueError("prediction feature is not numeric") from exc
        if math.isnan(converted):
            result.append(float("nan"))
            continue
        converted = min(max(converted, lower), upper)
        result.append(_float32(converted, "prediction feature"))
    return tuple(result)


def parse_forest_ensemble(artifact, manifest):
    """Parse and re-sanitize one canonical mean-prediction ensemble."""
    canonical, arguments, task, bounds, cuts = _prediction_profile(manifest)
    container = _exact(
        _parse(artifact, canonical["resources"]["max_artifact_bytes"]),
        _ENSEMBLE_FIELDS, "forest ensemble")
    if container["contract"] != ENSEMBLE_CONTRACT or \
            type(container["version"]) is not int or \
            container["version"] != 1 or \
            container["engine"] != "extra_trees" or \
            container["aggregation"] != "mean_prediction" or \
            container["task"] != task or \
            container["public_schema_sha256"] != canonical["public_schema"]["sha256"]:
        raise ValueError("forest ensemble differs from its pinned profile")
    models = container["models"]
    if not isinstance(models, list) or not models:
        raise ValueError("forest ensemble requires at least one model")
    pairs = []
    parsed = []
    for model in models:
        member = _canonical_json(model)
        sanitized, digest = sanitize_forest_json(member, **arguments)
        if member != sanitized:
            raise ValueError("forest ensemble member is not canonical")
        pairs.append((digest, sanitized))
        parsed.append(_extract_model(model))
    if pairs != sorted(pairs, key=lambda item: (item[0], item[1])):
        raise ValueError("forest ensemble models are not canonically ordered")
    return ForestEnsemble(
        _CONSTRUCTION_TOKEN, task, arguments["expected_features"],
        arguments["expected_depth"], bounds, cuts, tuple(parsed))


def predict_forest_ensemble(artifact, manifest, rows):
    """Parse a canonical forest ensemble and predict public rows."""
    return parse_forest_ensemble(artifact, manifest).predict(rows)


__all__ = [
    "ENSEMBLE_CONTRACT",
    "ForestEnsemble",
    "parse_forest_ensemble",
    "predict_forest_ensemble",
]
