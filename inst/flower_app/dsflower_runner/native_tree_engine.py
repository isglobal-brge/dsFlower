"""Narrow dispatch for implemented trusted native-tree engines.

Imports are deliberately lazy: public request parsing and portable prediction
must not load the XGBoost bundle boundary, and pure dsFlower engines never need
that bundle to train or predict.
"""

import hashlib
import hmac
import json


_PROFILE_V2 = "dsflower-native-tree-prediction-profile-v2"
_PROFILE_MAX_BYTES = 128 * 1024
_SPECS = {
    "xgboost": {
        "engine": "xgboost",
        "ensemble_contract": "dsflower-xgboost-ensemble-v1",
        "ensemble_format": "dsflower-xgboost-ensemble-json-v1",
        "model_file": "model.xgboost-ensemble.json",
        "profile_contract": "dsflower-xgboost-prediction-profile-v1",
        "profile_file": "model.xgboost-ensemble.profile.json",
        "profile_version": 1,
    },
    "extra_trees": {
        "engine": "extra_trees",
        "ensemble_contract": "dsflower-forest-ensemble-v1",
        "ensemble_format": "dsflower-forest-ensemble-json-v1",
        "model_file": "model.extra-trees-ensemble.json",
        "profile_contract": _PROFILE_V2,
        "profile_file": "model.extra-trees-ensemble.profile.json",
        "profile_version": 2,
    },
    "lightgbm": {
        "engine": "lightgbm",
        "ensemble_contract": "dsflower-lightgbm-safe-ensemble-v1",
        "ensemble_format": "dsflower-lightgbm-ensemble-json-v1",
        "model_file": "model.lightgbm-ensemble.json",
        "profile_contract": _PROFILE_V2,
        "profile_file": "model.lightgbm-ensemble.profile.json",
        "profile_version": 2,
    },
    "catboost": {
        "engine": "catboost",
        "ensemble_contract": "dsflower-catboost-safe-ensemble-v1",
        "ensemble_format": "dsflower-catboost-ensemble-json-v1",
        "model_file": "model.catboost-ensemble.json",
        "profile_contract": _PROFILE_V2,
        "profile_file": "model.catboost-ensemble.profile.json",
        "profile_version": 2,
    },
}
_ARTIFACT_FIELDS = frozenset(("format", "sha256", "size_bytes"))
_PROFILE_V1_FIELDS = frozenset((
    "artifact", "contract", "native_tree_request_b64",
    "native_tree_request_sha256", "public_schema_sha256", "task", "version",
))
_PROFILE_V2_FIELDS = _PROFILE_V1_FIELDS | frozenset(("engine",))


def _canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _without_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("prediction profile contains a duplicate key")
        value[key] = item
    return value


def release_spec(engine):
    """Return immutable-by-copy release metadata for one implemented engine."""
    if engine not in _SPECS:
        raise ValueError("native-tree engine is unsupported")
    return dict(_SPECS[engine])


def requires_xgboost_bundle(engine):
    release_spec(engine)
    return engine == "xgboost"


def canonical_profile(manifest):
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "xgboost":
        from . import xgboost_adapter
        return xgboost_adapter.canonical_xgboost_profile(manifest)
    if engine == "extra_trees":
        from . import forest_adapter
        return forest_adapter.canonical_extra_trees_profile(manifest)
    if engine in ("lightgbm", "catboost"):
        from . import boosting_adapter
        return boosting_adapter.canonical_boosting_profile(manifest)
    raise ValueError("native-tree engine is unsupported")


def train_model(manifest, features, target, *, unit_ids=None,
                xgboost_bundle=None):
    """Train once and return only a re-sanitized model projection."""
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "xgboost":
        from . import xgboost_adapter
        prepared = xgboost_adapter.prepare_xgboost_training(
            manifest, features, target, native_bundle=xgboost_bundle,
            unit_ids=unit_ids)
        artifact = xgboost_adapter.train_xgboost_native(prepared)
        return xgboost_adapter.sanitize_xgboost_artifact(
            manifest, artifact)[0]
    if engine == "extra_trees":
        from . import forest_adapter
        prepared = forest_adapter.prepare_extra_trees_training(
            manifest, features, target, unit_ids=unit_ids)
        artifact = forest_adapter.train_extra_trees(prepared)
        return forest_adapter.sanitize_extra_trees_artifact(
            manifest, artifact)[0]
    if engine in ("lightgbm", "catboost"):
        from . import boosting_adapter
        prepared = boosting_adapter.prepare_boosting_training(
            manifest, features, target, unit_ids=unit_ids)
        artifact = boosting_adapter.train_boosting(prepared)
        return boosting_adapter.sanitize_boosting_artifact(
            manifest, artifact)[0]
    raise ValueError("native-tree engine is unsupported")


def build_ensemble(manifest, artifacts):
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "xgboost":
        from . import xgboost_adapter
        return xgboost_adapter.build_xgboost_ensemble(manifest, artifacts)
    if engine == "extra_trees":
        from . import forest_adapter
        return forest_adapter.build_extra_trees_ensemble(manifest, artifacts)
    if engine in ("lightgbm", "catboost"):
        from . import boosting_adapter
        return boosting_adapter.build_boosting_ensemble(manifest, artifacts)
    raise ValueError("native-tree engine is unsupported")


def parse_ensemble(manifest, artifact):
    engine = manifest.get("engine") if isinstance(manifest, dict) else None
    if engine == "xgboost":
        from . import xgboost_predictor
        return xgboost_predictor.parse_xgboost_ensemble(artifact, manifest)
    if engine == "extra_trees":
        from . import forest_predictor
        return forest_predictor.parse_forest_ensemble(artifact, manifest)
    if engine == "lightgbm":
        from . import lightgbm_artifact
        return lightgbm_artifact.parse_ensemble(artifact, manifest)
    if engine == "catboost":
        from . import catboost_artifact
        return catboost_artifact.parse_ensemble(artifact, manifest)
    raise ValueError("native-tree engine is unsupported")


def build_prediction_profile(request, request_b64, request_sha256,
                             artifact, artifact_sha256):
    from . import native_tree_request
    parsed = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    if parsed != request or not isinstance(artifact, (bytes, bytearray,
                                                       memoryview)):
        raise ValueError("prediction profile inputs are inconsistent")
    raw = bytes(artifact)
    actual = hashlib.sha256(raw).hexdigest()
    if not isinstance(artifact_sha256, str) or not hmac.compare_digest(
            actual, artifact_sha256):
        raise ValueError("prediction artifact digest is inconsistent")
    spec = release_spec(request.get("engine"))
    profile = {
        "artifact": {
            "format": spec["ensemble_format"],
            "sha256": actual,
            "size_bytes": len(raw),
        },
        "contract": spec["profile_contract"],
        "native_tree_request_b64": request_b64,
        "native_tree_request_sha256": request_sha256,
        "public_schema_sha256": request["public_schema"]["sha256"],
        "task": request["task"],
        "version": spec["profile_version"],
    }
    if spec["profile_version"] == 2:
        profile["engine"] = spec["engine"]
    encoded = _canonical_json(profile)
    if len(encoded) > _PROFILE_MAX_BYTES:
        raise ValueError("prediction profile exceeds its byte ceiling")
    return encoded


def validate_prediction_profile(profile, request, request_b64,
                                request_sha256, artifact):
    if not isinstance(profile, (bytes, bytearray, memoryview)) or \
            not 1 <= len(profile) <= _PROFILE_MAX_BYTES or \
            not isinstance(artifact, (bytes, bytearray, memoryview)):
        raise ValueError("prediction profile is malformed")
    raw_profile = bytes(profile)
    try:
        value = json.loads(
            raw_profile.decode("ascii"), object_pairs_hook=_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite profile number")))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("prediction profile is malformed") from exc
    spec = release_spec(request.get("engine") if isinstance(request, dict)
                        else None)
    expected_fields = (_PROFILE_V1_FIELDS if spec["profile_version"] == 1
                       else _PROFILE_V2_FIELDS)
    binding = value.get("artifact") if isinstance(value, dict) else None
    raw_artifact = bytes(artifact)
    if frozenset(value) != expected_fields or \
            not isinstance(binding, dict) or \
            frozenset(binding) != _ARTIFACT_FIELDS or \
            _canonical_json(value) != raw_profile or \
            value["contract"] != spec["profile_contract"] or \
            type(value["version"]) is not int or \
            value["version"] != spec["profile_version"] or \
            (spec["profile_version"] == 2 and
             value["engine"] != spec["engine"]) or \
            value["native_tree_request_b64"] != request_b64 or \
            value["native_tree_request_sha256"] != request_sha256 or \
            value["public_schema_sha256"] != request["public_schema"]["sha256"] or \
            value["task"] != request["task"] or \
            binding["format"] != spec["ensemble_format"] or \
            type(binding["size_bytes"]) is not int or \
            binding["size_bytes"] != len(raw_artifact) or \
            not isinstance(binding["sha256"], str) or \
            not hmac.compare_digest(
                binding["sha256"], hashlib.sha256(raw_artifact).hexdigest()):
        raise ValueError("prediction profile bindings are invalid")
    from . import native_tree_request
    if native_tree_request.parse_request_wire(
            request_b64, request_sha256) != request:
        raise ValueError("prediction profile request is inconsistent")
    return spec


__all__ = [
    "build_ensemble", "build_prediction_profile", "canonical_profile",
    "parse_ensemble", "release_spec", "requires_xgboost_bundle",
    "train_model", "validate_prediction_profile",
]
