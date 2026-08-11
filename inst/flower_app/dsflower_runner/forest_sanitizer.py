"""Strict data-only sanitizer for dsFlower forest artifacts.

The sanitizer removes no fields by convention: it accepts one tiny prediction
schema exactly and rewrites it canonically.  DP provenance still comes only
from the trusted forest adapter that creates the bytes.
"""

import hashlib
import json
import math
import re


MODEL_CONTRACT = "dsflower-forest-model-v1"
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_MODEL_FIELDS = frozenset((
    "contract", "depth", "engine", "num_features",
    "public_schema_sha256", "task", "trees", "version",
))
_TREE_FIELDS = frozenset((
    "cut_indices", "default_left", "features", "leaf_values",
))


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("forest artifact has duplicate JSON keys")
        result[key] = value
    return result


def _exact(value, fields, where):
    if not isinstance(value, dict) or frozenset(value) != fields:
        raise ValueError("%s has an unsupported shape" % where)
    return value


def _integer(value, where, lower, upper):
    if isinstance(value, bool) or not isinstance(value, int) or not (
            lower <= value <= upper):
        raise ValueError("%s is outside its integer domain" % where)
    return int(value)


def _finite(value, where, lower, upper):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s is not numeric" % where)
    result = float(value)
    if not math.isfinite(result) or not lower <= result <= upper:
        raise ValueError("%s is outside its public numeric domain" % where)
    return 0.0 if result == 0.0 else result


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _parse(artifact, byte_cap):
    if not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("forest artifact must be bounded JSON bytes")
    raw = bytes(artifact)
    if not 1 <= len(raw) <= byte_cap:
        raise ValueError("forest artifact exceeds its byte ceiling")
    try:
        return json.loads(
            raw.decode("ascii"), object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("forest artifact contains a non-finite number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("forest artifact JSON is invalid") from exc


def sanitize_forest_json(
        artifact, *, expected_engine, expected_task, expected_features,
        expected_trees, expected_depth, public_cut_counts,
        public_schema_sha256, target_lower, target_upper,
        max_artifact_bytes):
    """Validate and return canonical prediction-only forest JSON bytes."""
    if expected_engine not in ("extra_trees", "random_forest"):
        raise ValueError("forest sanitizer engine is unsupported")
    if expected_task not in ("binary_classification", "regression"):
        raise ValueError("forest sanitizer task is unsupported")
    expected_features = _integer(
        expected_features, "expected feature count", 1, 8192)
    expected_trees = _integer(expected_trees, "expected tree count", 1, 512)
    expected_depth = _integer(expected_depth, "expected tree depth", 1, 12)
    max_artifact_bytes = _integer(
        max_artifact_bytes, "artifact byte ceiling", 1, 64 * 1024 * 1024)
    if not isinstance(public_schema_sha256, str) or not _HEX_64_RE.fullmatch(
            public_schema_sha256):
        raise ValueError("forest schema digest is invalid")
    if not isinstance(public_cut_counts, (list, tuple)) or len(
            public_cut_counts) != expected_features:
        raise ValueError("forest public cut geometry is invalid")
    cut_counts = tuple(
        _integer(value, "public cut count", 1, 65535)
        for value in public_cut_counts
    )
    lower = _finite(target_lower, "target lower bound", -1.0e12, 1.0e12)
    upper = _finite(target_upper, "target upper bound", -1.0e12, 1.0e12)
    if lower >= upper:
        raise ValueError("forest target bounds are not strict")

    model = _exact(_parse(artifact, max_artifact_bytes),
                   _MODEL_FIELDS, "forest model")
    artifact_task = ("binary" if expected_task == "binary_classification"
                     else "regression")
    if model["contract"] != MODEL_CONTRACT or \
            type(model["version"]) is not int or model["version"] != 1 or \
            type(model["depth"]) is not int or \
            type(model["num_features"]) is not int or \
            model["engine"] != expected_engine or \
            model["task"] != artifact_task or \
            model["depth"] != expected_depth or \
            model["num_features"] != expected_features or \
            model["public_schema_sha256"] != public_schema_sha256:
        raise ValueError("forest artifact differs from its pinned profile")
    trees = model["trees"]
    if not isinstance(trees, list) or len(trees) != expected_trees:
        raise ValueError("forest artifact tree count differs from its profile")

    internal_count = (1 << expected_depth) - 1
    leaf_count = 1 << expected_depth
    leaf_lower, leaf_upper = ((0.0, 1.0)
                              if expected_task == "binary_classification"
                              else (lower, upper))
    canonical_trees = []
    for tree in trees:
        tree = _exact(tree, _TREE_FIELDS, "forest tree")
        features = tree["features"]
        cuts = tree["cut_indices"]
        defaults = tree["default_left"]
        leaves = tree["leaf_values"]
        if not isinstance(features, list) or len(features) != internal_count or \
                not isinstance(cuts, list) or len(cuts) != internal_count or \
                not isinstance(defaults, list) or len(defaults) != internal_count or \
                not isinstance(leaves, list) or len(leaves) != leaf_count:
            raise ValueError("forest tree geometry differs from its profile")
        canonical_features = [
            _integer(value, "forest split feature", 0, expected_features - 1)
            for value in features
        ]
        canonical_cuts = []
        for feature, value in zip(canonical_features, cuts):
            canonical_cuts.append(_integer(
                value, "forest split cut", 0, cut_counts[feature] - 1))
        if any(type(value) is not bool for value in defaults):
            raise ValueError("forest missing-value directions must be boolean")
        canonical_leaves = [
            _finite(value, "forest leaf", leaf_lower, leaf_upper)
            for value in leaves
        ]
        canonical_trees.append({
            "cut_indices": canonical_cuts,
            "default_left": list(defaults),
            "features": canonical_features,
            "leaf_values": canonical_leaves,
        })
    canonical = {
        "contract": MODEL_CONTRACT,
        "depth": expected_depth,
        "engine": expected_engine,
        "num_features": expected_features,
        "public_schema_sha256": public_schema_sha256,
        "task": artifact_task,
        "trees": canonical_trees,
        "version": 1,
    }
    encoded = _canonical_json(canonical)
    if len(encoded) > max_artifact_bytes:
        raise ValueError("canonical forest artifact exceeds its byte ceiling")
    return encoded, hashlib.sha256(encoded).hexdigest()


__all__ = ["MODEL_CONTRACT", "sanitize_forest_json"]
