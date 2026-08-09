"""Safe numeric LightGBM projection; never loads native model text."""

from . import boosting_artifact
from . import boosting_profile


MODEL_CONTRACT = "dsflower-lightgbm-safe-model-v1"
ENSEMBLE_CONTRACT = "dsflower-lightgbm-safe-ensemble-v1"
ENSEMBLE_FORMAT = "dsflower-lightgbm-ensemble-json-v1"


def sanitize_model(artifact, manifest):
    profile = boosting_profile.lightgbm_profile(manifest)
    return boosting_artifact.sanitize_model(
        artifact, profile, engine="lightgbm", contract=MODEL_CONTRACT)


def build_ensemble(model_artifacts, manifest):
    profile = boosting_profile.lightgbm_profile(manifest)
    return boosting_artifact.build_ensemble(
        model_artifacts, profile, engine="lightgbm",
        model_contract=MODEL_CONTRACT,
        ensemble_contract=ENSEMBLE_CONTRACT)


def parse_ensemble(artifact, manifest):
    profile = boosting_profile.lightgbm_profile(manifest)
    return boosting_artifact.parse_ensemble(
        artifact, profile, engine="lightgbm",
        model_contract=MODEL_CONTRACT,
        ensemble_contract=ENSEMBLE_CONTRACT)


__all__ = [
    "ENSEMBLE_CONTRACT", "ENSEMBLE_FORMAT", "MODEL_CONTRACT",
    "build_ensemble", "parse_ensemble", "sanitize_model",
]
