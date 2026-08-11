"""Fresh executable probes for dsFlower's pure private tree engines."""

import base64
import hashlib
import json
import struct

import numpy as np

from . import native_tree_engine, native_tree_request, seeding


_PURE_ENGINES = frozenset((
    "extra_trees", "random_forest", "lightgbm", "catboost"))
_VALIDATION_ENGINES = _PURE_ENGINES | frozenset(("xgboost",))


def _request(engine):
    core = {
        "version": 1,
        "features": ["x"],
        "lower": [-1.0],
        "upper": [1.0],
        "cuts": [[0.0]],
        "target": {
            "name": "y", "kind": "binary",
            "levels": [
                {"type": "string", "value": "no"},
                {"type": "string", "value": "yes"},
            ],
            "lower": 0.0, "upper": 1.0,
        },
    }
    schema = dict(
        core,
        sha256=hashlib.sha256(json.dumps(
            core, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")).hexdigest(),
    )
    parameters = {
        "xgboost": [
            {"name": "learning_rate", "type": "number", "value": 0.25},
            {"name": "max_delta_step", "type": "number", "value": 1.0},
            {"name": "max_depth", "type": "integer", "value": 1},
            {"name": "min_child_weight", "type": "number", "value": 1.0},
            {"name": "min_split_loss", "type": "number", "value": 0.0},
            {"name": "num_boost_round", "type": "integer", "value": 1},
            {"name": "reg_alpha", "type": "number", "value": 0.0},
            {"name": "reg_lambda", "type": "number", "value": 1.0},
        ],
        "extra_trees": [
            {"name": "max_depth", "type": "integer", "value": 1},
            {"name": "n_estimators", "type": "integer", "value": 1},
        ],
        "random_forest": [
            {"name": "max_depth", "type": "integer", "value": 1},
            {"name": "max_features", "type": "integer", "value": 1},
            {"name": "n_estimators", "type": "integer", "value": 1},
        ],
        "lightgbm": [
            {"name": "lambda_l1", "type": "number", "value": 0.0},
            {"name": "lambda_l2", "type": "number", "value": 1.0},
            {"name": "learning_rate", "type": "number", "value": 0.25},
            {"name": "max_delta_step", "type": "number", "value": 1.0},
            {"name": "max_depth", "type": "integer", "value": 1},
            {"name": "min_data_in_leaf", "type": "integer", "value": 1},
            {"name": "min_gain_to_split", "type": "number", "value": 0.0},
            {"name": "num_iterations", "type": "integer", "value": 1},
            {"name": "num_leaves", "type": "integer", "value": 2},
        ],
        "catboost": [
            {"name": "depth", "type": "integer", "value": 1},
            {"name": "iterations", "type": "integer", "value": 1},
            {"name": "l2_leaf_reg", "type": "number", "value": 1.0},
            {"name": "learning_rate", "type": "number", "value": 0.25},
            {"name": "max_delta_step", "type": "number", "value": 1.0},
        ],
    }[engine]
    return {
        "contract": native_tree_request.REQUEST_CONTRACT,
        "engine": engine,
        "mode": "native-tight",
        "parameters": parameters,
        "public_schema": schema,
        "resources": {
            "max_features": 1, "max_trees": 1, "max_depth": 1,
            "max_bins": 2, "max_threads": 1, "memory_mb": 4096,
            "timeout_seconds": 30,
        },
        "task": "binary",
    }


def _next_float32(value):
    value = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    bits = bits + 1 if value >= 0.0 else bits - 1
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def _xgboost_member():
    tree = {
        "base_weights": [0.0, -0.1, 0.1],
        "categories": [], "categories_nodes": [],
        "categories_segments": [], "categories_sizes": [],
        "default_left": [1, 0, 0], "id": 0,
        "left_children": [1, -1, -1],
        "loss_changes": [0.0, 0.0, 0.0],
        "parents": [2_147_483_647, 0, 0],
        "right_children": [2, -1, -1],
        "split_conditions": [_next_float32(0.0), -0.1, 0.1],
        "split_indices": [0, 0, 0], "split_type": [0, 0, 0],
        "sum_hessian": [1.0, 0.5, 0.5],
        "tree_param": {
            "num_deleted": "0", "num_feature": "1", "num_nodes": "3",
            "size_leaf_vector": "1",
        },
    }
    model = {
        "learner": {
            "attributes": {}, "feature_names": [], "feature_types": [],
            "gradient_booster": {
                "model": {
                    "cats": {
                        "enc": [], "feature_segments": [], "sorted_idx": []},
                    "gbtree_model_param": {
                        "num_parallel_tree": "1", "num_trees": "1"},
                    "iteration_indptr": [0, 1], "tree_info": [0],
                    "trees": [tree],
                },
                "name": "gbtree",
            },
            "learner_model_param": {
                "base_score": "[5E-1]", "boost_from_average": "0",
                "num_class": "0", "num_feature": "1", "num_target": "1",
            },
            "objective": {
                "name": "binary:logistic",
                "reg_loss_param": {"scale_pos_weight": "1"},
            },
        },
        "version": [3, 4, 0],
    }
    return json.dumps(model, separators=(",", ":")).encode("ascii")


def synthetic_release(engine, *, xgboost_bundle=None):
    """Build one fixed public artifact/profile pair for executable probes."""
    if engine not in _VALIDATION_ENGINES:
        raise ValueError("native-tree probe engine is unsupported")
    request = _request(engine)
    raw = json.dumps(
        request, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    request_b64 = base64.b64encode(raw).decode("ascii")
    request_sha256 = hashlib.sha256(raw).hexdigest()
    parsed = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    manifest = native_tree_request.public_backend_manifest(parsed)
    native_tree_engine.canonical_profile(manifest)
    features = np.asarray([[-0.75], [-0.25], [0.25], [0.75]],
                          dtype=np.float64)
    target = np.asarray([0.0, 0.0, 1.0, 1.0], dtype=np.float64)
    if engine == "xgboost" and xgboost_bundle is None:
        member = _xgboost_member()
    else:
        # This is an availability probe over fixed public synthetic data. Keep
        # it independent of node bootstrap while exercising the exact sticky
        # PRF path used by real pure-engine training.
        original_node_secret = seeding._node_secret
        seeding._node_secret = lambda: b"\x00" * 32
        try:
            member = native_tree_engine.train_model(
                manifest, features, target, xgboost_bundle=xgboost_bundle)
        finally:
            seeding._node_secret = original_node_secret
    ensemble, digest = native_tree_engine.build_ensemble(manifest, [member])
    if hashlib.sha256(ensemble).hexdigest() != digest:
        raise RuntimeError("native-tree probe ensemble digest differs")
    profile = native_tree_engine.build_prediction_profile(
        parsed, request_b64, request_sha256, ensemble, digest)
    native_tree_engine.validate_prediction_profile(
        profile, parsed, request_b64, request_sha256, ensemble)
    return {
        "artifact": ensemble,
        "features": features,
        "manifest": manifest,
        "profile": profile,
        "request": parsed,
        "request_b64": request_b64,
        "request_sha256": request_sha256,
    }


def _probe_prediction(release):
    predictor = native_tree_engine.parse_ensemble(
        release["manifest"], release["artifact"])
    predictions = np.asarray(
        predictor.predict(release["features"]), dtype=np.float64)
    if predictions.shape != (4,) or not bool(np.all(np.isfinite(predictions))):
        raise RuntimeError("native-tree probe prediction is invalid")
    return True


def probe_pure_engine(engine):
    """Exercise request -> private trainer -> ensemble -> sidecar -> predictor."""
    if engine not in _PURE_ENGINES:
        raise ValueError("pure native-tree probe engine is unsupported")
    return _probe_prediction(synthetic_release(engine))


def probe_validation_engine(engine):
    """Exercise the data-only parser/predictor used before private validation."""
    if engine not in _VALIDATION_ENGINES:
        raise ValueError("native-tree validation probe engine is unsupported")
    return _probe_prediction(synthetic_release(engine))


def probe_xgboost_engine(xgboost_bundle):
    """Exercise the verified native FFI trainer and complete release path."""
    return _probe_prediction(synthetic_release(
        "xgboost", xgboost_bundle=xgboost_bundle))


def probe_pure_engines(engines=None):
    """Return independent executable readiness for requested pure engines."""
    requested = tuple(sorted(_PURE_ENGINES if engines is None else engines))
    if not requested or any(engine not in _PURE_ENGINES for engine in requested):
        raise ValueError("pure native-tree probe engine is unsupported")
    readiness = {}
    for engine in requested:
        try:
            readiness[engine] = bool(probe_pure_engine(engine))
        except Exception:
            readiness[engine] = False
    return readiness


__all__ = [
    "probe_pure_engine", "probe_pure_engines", "probe_validation_engine",
    "probe_xgboost_engine", "synthetic_release",
]
