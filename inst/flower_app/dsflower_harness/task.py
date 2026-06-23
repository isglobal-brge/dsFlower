"""Data loading + privacy config for the dsFlower Tier-1 harness.

CRITICAL TRUST INVARIANT: all privacy enforcement (epsilon/delta/clip) and the
data location are read from the SERVER-WRITTEN manifest.json, never from the
client-controlled run config (pyproject). The server owns the staging directory,
so the manifest is tamper-proof; the researcher cannot weaken DP through it.
"""

import json
import os

import numpy as np
import pandas as pd

_privacy_config = None


def _get_manifest_dir(context=None):
    manifest_dir = None
    if context is not None:
        manifest_dir = context.node_config.get("manifest-dir")
    if manifest_dir is None:
        manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR")
    if manifest_dir is None:
        raise ValueError(
            "No manifest directory found. Set 'manifest-dir' in node_config or "
            "the DSFLOWER_MANIFEST_DIR environment variable."
        )
    return manifest_dir


def _load_manifest(context=None):
    manifest_path = os.path.join(_get_manifest_dir(context), "manifest.json")
    with open(manifest_path) as f:
        return json.load(f)


def load_data(context=None):
    """Load (X, y) for standard supervised training from the staged manifest."""
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])

    if manifest.get("data_format", "csv") == "parquet":
        import pyarrow.parquet as pq
        df = pq.read_table(data_file).to_pandas()
    else:
        df = pd.read_csv(data_file)

    target_col = manifest["target_column"]
    feat_cols = manifest.get("feature_columns")
    feat = df[feat_cols] if feat_cols else df.drop(columns=[target_col])
    X = feat.to_numpy(dtype=np.float32)

    # Coerce the target to numeric {0,1,...}. Uses is_numeric_dtype (so pandas
    # arrow-backed string columns are handled too, not just object) and stable
    # categorical encoding for factor/string/bool labels; numeric passes through.
    y_series = df[target_col]
    if pd.api.types.is_numeric_dtype(y_series):
        y = y_series.to_numpy()
    else:
        y = y_series.astype("category").cat.codes.to_numpy()
    return X, np.asarray(y, dtype=np.float32)


def is_image_run(context=None):
    """True if the staged manifest is an image collection (data_type=='image')."""
    return _load_manifest(context).get("data_type") == "image"


# Patient / subject identifier candidates — MUST mirror R .detectPatientColumn
# (policy.R) so the admission grouping and the per-patient DP unit pick the same
# column. The server-written manifest may pin one via "patient_column".
_PATIENT_CANDIDATES = ("patient_id", "patientid", "patient", "subject_id",
                       "subjectid", "subject", "participant_id", "case_id",
                       "person_id", "pid", "mrn")


def _detect_patient_column(df, manifest):
    explicit = manifest.get("patient_column")
    if explicit and explicit in df.columns:
        return explicit
    lc = {str(c).lower(): c for c in df.columns}
    for cand in _PATIENT_CANDIDATES:
        if cand in lc:
            return lc[cand]
    return None


def load_image_collection(context=None):
    """Resolve a staged dsImaging collection to (image_paths, y, patient_ids).

    The R side (.stageFromDescriptor_image) already resolved the dsImaging dataset
    to a local image root + a samples table with a per-sample path column; here we
    only join images_root + relative_path and read the label. Pixels stay on disk
    (read lazily during feature extraction); the samples table never enters the
    trainable tensors except the label, so sample_id / metadata are not features.

    patient_ids is the per-image patient/subject identifier (or None when no
    patient column exists) so the harness can train per-PATIENT (the DP unit then
    matches the admission unit) instead of per-image — see client_app._pool_by_patient.
    """
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)
    samples_file = os.path.join(manifest_dir, manifest["samples_file"])
    if not os.path.isfile(samples_file):
        raise ValueError(f"image samples file not found: {samples_file}")
    if samples_file.lower().endswith(".parquet"):
        import pyarrow.parquet as pq
        df = pq.read_table(samples_file).to_pandas()
    else:
        df = pd.read_csv(samples_file)

    assets = manifest.get("assets", {}) or {}
    images = assets.get("images", {}) or {}
    images_root = images.get("root") or manifest["data_root"]
    if not os.path.isdir(images_root):
        raise ValueError(f"image data root not found: {images_root}")
    path_col = images.get("path_col", "relative_path")
    if path_col not in df.columns:
        raise ValueError(
            f"image samples table is missing the path column '{path_col}'")

    target_col = manifest["target_column"]
    # Drop rows with a missing label: a NaN label becomes categorical code -1 ->
    # an off-range gradient that silently mislearns AND leaks the row's presence.
    if df[target_col].isna().any():
        df = df[df[target_col].notna()].reset_index(drop=True)
    if not len(df):
        raise ValueError(
            "image collection has no labelled samples after dropping missing labels")

    paths = [os.path.join(images_root, str(p)) for p in df[path_col]]
    y_series = df[target_col]
    if pd.api.types.is_numeric_dtype(y_series):
        y = y_series.to_numpy()
    else:
        y = y_series.astype("category").cat.codes.to_numpy()

    pcol = _detect_patient_column(df, manifest)
    groups = df[pcol].astype(str).to_numpy() if pcol else None
    return paths, np.asarray(y, dtype=np.float32), groups


def load_privacy_config(context=None):
    """Read the tamper-proof DP parameters from the server-written manifest.

    DP is ALWAYS enforced — there is no 'off' path and no profiles. Counts and
    per-node metrics are suppressed/bucketed by default (disclosure backstop).
    """
    global _privacy_config
    if _privacy_config is not None:
        return _privacy_config

    manifest = _load_manifest(context)
    epsilon = float(manifest.get("privacy-epsilon", 3.0))
    delta = float(manifest.get("privacy-delta", 1e-5))
    clipping_norm = float(manifest.get("privacy-clipping_norm", 1.0))
    # The manifest is server-written + tamper-proof, but validate the DP params
    # are in legal ranges so a corrupted manifest fails closed (not silently
    # producing infinite/zero noise that would void the guarantee).
    if not (epsilon > 0 and 0.0 < delta < 1.0 and clipping_norm > 0):
        raise ValueError(
            "invalid DP parameters in manifest (need epsilon>0, 0<delta<1, "
            "clipping_norm>0)")
    _privacy_config = {
        "epsilon": epsilon,
        "delta": delta,
        "clipping_norm": clipping_norm,
        "allow_per_node_metrics": False,
        "allow_exact_num_examples": False,
        "n_samples": int(manifest.get("n_samples", 0)),
    }
    return _privacy_config
