"""Data loading + privacy config for the dsFlower Tier-1 harness.

CRITICAL TRUST INVARIANT: all privacy enforcement (epsilon/delta/clip) and the
data location are read from the SERVER-WRITTEN manifest.json, never from the
client-controlled run config (pyproject). The server owns the staging directory,
so the manifest is tamper-proof; the researcher cannot weaken DP through it.
"""

import json
import math
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
    # The manifest is server-written, but during the staging transition it may
    # still carry client-PROPOSED DP params (the run_config is merged in at stage
    # time), so the node treats them as untrusted. First: legal ranges (a corrupted
    # value must not silently produce zero/infinite noise that voids the guarantee).
    if not (epsilon > 0 and 0.0 < delta < 1.0 and clipping_norm > 0):
        raise ValueError(
            "invalid DP parameters in manifest (need epsilon>0, 0<delta<1, "
            "clipping_norm>0)")
    # Then: HARDCODED node ceilings (NOT manifest-derived -- a client-influenced
    # manifest cannot raise its own ceiling). Reject anything weaker than policy; an
    # inflated epsilon or a near-1 delta would void the (epsilon, delta) guarantee
    # even with DP-SGD running. (Defaults eps=3, delta=1e-5, clip=1 pass with margin.)
    if epsilon > 10.0:
        raise ValueError("privacy epsilon %.4g exceeds the node ceiling (10)" % epsilon)
    if delta > 1e-3:
        raise ValueError("privacy delta %.4g exceeds the node ceiling (1e-3)" % delta)
    if clipping_norm > 100.0:
        raise ValueError(
            "privacy clipping_norm %.4g exceeds the node ceiling (100)" % clipping_norm)
    _privacy_config = {
        "epsilon": epsilon,
        "delta": delta,
        "clipping_norm": clipping_norm,
        # Improved Tier-2 floor (sample-and-aggregate) policy. Parsed defensively; the
        # harness clamps k = min(sa_max_blocks, n // sa_min_block) and falls back to the
        # plain 2C floor below 2 blocks, so out-of-range values cannot weaken DP.
        "sample_aggregate": bool(float(manifest.get("privacy-sample_aggregate", 0))),
        "sa_min_block": max(1, int(float(manifest.get("privacy-sa_min_block", 64)))),
        "sa_max_blocks": max(1, int(float(manifest.get("privacy-sa_max_blocks", 8)))),
        "egress_time_pad": max(0.0, float(manifest.get("privacy-egress_time_pad", 0))),
        "allow_per_node_metrics": False,
        "allow_exact_num_examples": False,
        "n_samples": int(manifest.get("n_samples", 0)),
    }
    return _privacy_config


def _run_config(context):
    return dict(context.run_config) if context is not None else {}


def load_dp_track(context=None):
    """The enforced-DP track. Read from the manifest (authoritative once the node
    pins it); fall back to the client run config during the transition, exactly
    like load_run_pins / load_gbdt_spec. Defaults to 'neural'. (Security note: the
    track only selects WHICH enforced-DP mechanism runs; every track is enforced
    node-side, so a client-chosen track cannot dodge DP -- at most pick trees vs
    neural. The manifest pin, once added, makes it fully node-authoritative.)"""
    manifest = _load_manifest(context)
    cfg = _run_config(context)
    track = str(manifest.get("dp-track", cfg.get("dp-track", "neural"))).lower()
    if track not in ("neural", "trees", "egress"):
        raise ValueError("invalid dp-track '%s'" % track)
    return track


def load_run_pins(context=None):
    """Manifest-pinned sampling / horizon / loss for the neural track.

    Every value that feeds the DP-SGD noise calibration -- the loss (a coupling
    loss breaks the per-sample bound), batch size, local epochs, and rounds (they
    set the composition horizon), and num-classes (the head width + label gate) --
    is read from the tamper-proof manifest. The client run config is a fallback
    ONLY during the node-package transition; once staging pins these, the manifest
    is authoritative and the client cannot stretch the horizon against a fixed
    ledger debit. (Learning rate is not DP-critical and may come from the config.)
    """
    manifest = _load_manifest(context)
    cfg = _run_config(context)

    def pin(key, default, cast):
        return cast(manifest[key]) if key in manifest else cast(cfg.get(key, default))

    return {
        "loss_name": str(manifest.get("loss-name", cfg.get("loss-name", "bce_logits"))),
        "batch_size": pin("batch-size", 32, int),
        "local_epochs": pin("local-epochs", 1, int),
        "num_rounds": pin("num-server-rounds", 1, int),
        "n_classes": pin("num-classes", 2, int),
        "learning_rate": float(cfg.get("learning-rate", manifest.get("learning-rate", 0.01))),
    }


def _decode_feature_norm(cfg, n_features):
    """Decode the client's GLOBAL feature mean/SD from the run config (same b64 JSON
    the neural track standardizes with). Returns (mean, sd) float arrays of length
    n_features, or None if absent / malformed / length-mismatched (caller falls back
    to the [0,1] binning prior). SDs are floored to a positive value."""
    raw = cfg.get("feature-norm-b64")
    if not raw:
        return None                       # absent -> caller uses the [0,1] prior (legit)
    # Present-but-malformed/mismatched is unexpected (the client sent it): FAIL CLOSED
    # rather than silently falling back to [0,1], which would resurrect the degenerate
    # binning bug without anyone noticing.
    import base64
    import json as _json
    try:
        norm = _json.loads(base64.b64decode(str(raw), validate=True).decode("utf-8"))
        mean = np.asarray(norm["means"], dtype=np.float64)
        sd = np.asarray(norm["sds"], dtype=np.float64)
    except Exception as e:
        raise RuntimeError("feature-norm-b64 present but undecodable: %s" % e)
    if mean.shape[0] != int(n_features) or sd.shape[0] != int(n_features):
        raise RuntimeError(
            "feature-norm length (%d/%d) != num features (%d)"
            % (mean.shape[0], sd.shape[0], int(n_features)))
    sd = np.where(np.isfinite(sd) & (sd > 1e-8), sd, 1.0)
    mean = np.where(np.isfinite(mean), mean, 0.0)
    return mean, sd


def load_gbdt_spec(context=None):
    """Validated, manifest-authoritative XGBoost spec for the trees track.

    The client's spec is a DATA dict (never imported / eval'd). The node clamps it
    to admin policy and pins the DP-critical pieces: the objective (dp_gbdt's
    bounded-gradient allowlist), the depth / tree / bin caps (so leaves are not
    singletons and compute is bounded), the run token that seeds the PUBLIC random
    splits, and the data-independent feature ranges (the binning prior -- never
    data quantiles). Returns the dict consumed by dp_gbdt.fit_dp_gbdt.
    """
    manifest = _load_manifest(context)
    spec = dict(manifest.get("gbdt-spec", {}) or {})
    cfg = _run_config(context)

    objective = str(spec.get("objective", cfg.get("objective", "binary:logistic")))
    max_depth = int(spec.get("max_depth", cfg.get("max-depth", 3)))
    n_trees = int(spec.get("n_trees", cfg.get("n-trees", 20)))
    learning_rate = float(spec.get("learning_rate", cfg.get("learning-rate", 0.3)))
    reg_lambda = float(spec.get("reg_lambda", cfg.get("reg-lambda", 1.0)))
    # Newton-step denominator is max(lambda, H+lambda); lambda<=0 (or non-finite lr/lambda)
    # can yield inf/NaN leaf weights. Clamp to safe values (fail-closed on the math).
    if not (math.isfinite(learning_rate) and learning_rate > 0):
        raise RuntimeError("DP-GBDT learning_rate must be finite and > 0")
    if not (math.isfinite(reg_lambda) and reg_lambda > 0):
        reg_lambda = 1e-6
    n_bins = int(spec.get("n_bins", cfg.get("n-bins", 32)))
    run_token = str(manifest.get("run_token", spec.get("run_token", "dsflower")))
    # Admin caps (data-independent): bound compute + keep leaves from being singletons.
    max_depth = max(1, min(max_depth, int(manifest.get("gbdt-max-depth", 6))))
    n_trees = max(1, min(n_trees, int(manifest.get("gbdt-max-trees", 200))))
    n_bins = max(2, min(n_bins, int(manifest.get("gbdt-max-bins", 64))))

    feature_ranges = spec.get("feature_ranges")
    if not feature_ranges:
        n_features = int(manifest.get("num-features", 0))
        if n_features <= 0:
            X, _ = load_data(context)            # shape only (schema), not content
            n_features = int(X.shape[1])
        # Binning prior for the random-split thresholds. The DP-GBDT draws each
        # threshold from a DATA-INDEPENDENT range; a fixed [0,1] collapses every
        # feature whose values exceed 1 (area, perimeter, ...) to a single leaf, so
        # the booster learns nothing (-> majority baseline). The client ships the
        # GLOBAL feature mean/SD (a sanctioned low-sensitivity aggregate, not data
        # quantiles), and we centre the prior at [mu-4sd, mu+4sd] -- ~all of a
        # roughly-normal feature, so the random thresholds land in the real range.
        # This depends on data ONLY through the already-released mu/sd (pure
        # post-processing -> no extra privacy cost). Absent stats -> [0,1] fallback.
        norm = _decode_feature_norm(cfg, n_features)
        if norm is not None:
            mean, sd = norm
            R = 4.0
            feature_ranges = [[float(mean[j] - R * sd[j]),
                               float(mean[j] + R * sd[j])] for j in range(n_features)]
        else:
            feature_ranges = [[0.0, 1.0]] * n_features
    return {"objective": objective, "max_depth": max_depth, "n_trees": n_trees,
            "learning_rate": learning_rate, "reg_lambda": reg_lambda,
            "n_bins": n_bins, "run_token": run_token,
            "feature_ranges": [[float(a), float(b)] for a, b in feature_ranges]}


def load_tabular_patient_ids(context=None):
    """Patient/subject ids for the tabular frame (or None) so the tabular neural +
    trees paths can make the patient the DP unit, mirroring the image path."""
    manifest = _load_manifest(context)
    if manifest.get("data_type") == "image":
        return None
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])
    if manifest.get("data_format", "csv") == "parquet":
        import pyarrow.parquet as pq
        df = pq.read_table(data_file).to_pandas()
    else:
        df = pd.read_csv(data_file)
    pcol = _detect_patient_column(df, manifest)
    return df[pcol].astype(str).to_numpy() if pcol else None
