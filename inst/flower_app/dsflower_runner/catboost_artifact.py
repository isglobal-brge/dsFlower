"""Safe numeric CatBoost projection; never loads CBM or executable objects."""

from . import boosting_artifact
from . import boosting_profile


MODEL_CONTRACT = "dsflower-catboost-safe-model-v1"
ENSEMBLE_CONTRACT = "dsflower-catboost-safe-ensemble-v1"
ENSEMBLE_FORMAT = "dsflower-catboost-ensemble-json-v1"


def sanitize_model(artifact, manifest):
    profile = boosting_profile.catboost_profile(manifest)
    return boosting_artifact.sanitize_model(
        artifact, profile, engine="catboost", contract=MODEL_CONTRACT)


def build_ensemble(model_artifacts, manifest):
    profile = boosting_profile.catboost_profile(manifest)
    return boosting_artifact.build_ensemble(
        model_artifacts, profile, engine="catboost",
        model_contract=MODEL_CONTRACT,
        ensemble_contract=ENSEMBLE_CONTRACT)


def parse_ensemble(artifact, manifest):
    profile = boosting_profile.catboost_profile(manifest)
    return boosting_artifact.parse_ensemble(
        artifact, profile, engine="catboost",
        model_contract=MODEL_CONTRACT,
        ensemble_contract=ENSEMBLE_CONTRACT)


__all__ = [
    "ENSEMBLE_CONTRACT", "ENSEMBLE_FORMAT", "MODEL_CONTRACT",
    "build_ensemble", "parse_ensemble", "sanitize_model",
]
