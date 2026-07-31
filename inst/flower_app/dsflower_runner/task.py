"""Data loading + privacy config for the dsFlower Tier-1 harness.

CRITICAL TRUST INVARIANT: all privacy enforcement (epsilon/delta/clip) and the
data location are read from the SERVER-WRITTEN manifest.json, never from the
client-controlled run config (pyproject). The server owns the staging directory,
so the manifest is tamper-proof; the researcher cannot weaken DP through it.
"""

import json
import math
import ntpath
import os

import numpy as np
import pandas as pd


_MAX_BATCH_SIZE = 65_536
_MAX_LOCAL_EPOCHS = 1_000
_MAX_SERVER_ROUNDS = 500
_MAX_LEARNING_RATE = 10.0
_MAX_GBDT_REG_LAMBDA = 1.0e6
_MAX_NUMERIC_ABS = 1.0e6


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


def _load_target(y_series, manifest):
    """Load a target under its public, manifest-pinned semantics.

    New manifests stage the target as numeric codes (classification) or a
    publicly clipped scalar (regression/count).  The checks here are defense in
    depth and deliberately never infer a vocabulary from private cohort values.
    """
    task_type = str(manifest.get("task-type", "classification")).lower()
    numeric_task = task_type in ("regression", "count", "continuous")
    try:
        numeric = pd.to_numeric(y_series, errors="raise").to_numpy(dtype=np.float64)
    except Exception as exc:
        if numeric_task:
            raise ValueError("regression/count target must be numeric") from exc
        raise ValueError(
            "non-numeric classification target requires public target-levels") from exc
    if not bool(np.all(np.isfinite(numeric))):
        raise ValueError("target contains non-finite values")

    if numeric_task:
        bounds = manifest.get("target-bounds")
        if not isinstance(bounds, dict):
            raise ValueError("manifest is missing public target-bounds")
        try:
            lower = float(bounds["lower"])
            upper = float(bounds["upper"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("manifest has invalid public target-bounds") from exc
        if not (math.isfinite(lower) and math.isfinite(upper) and lower < upper
                and abs(lower) <= _MAX_NUMERIC_ABS
                and abs(upper) <= _MAX_NUMERIC_ABS):
            raise ValueError("manifest has invalid public target-bounds")
        loss_name = str(manifest.get("loss-name", "")).lower()
        if task_type == "count" and lower < 0.0:
            raise ValueError("count target-bounds require lower >= 0")
        if loss_name == "gamma_nll" and lower <= 0.0:
            raise ValueError("gamma_nll target-bounds require lower > 0")
        return np.clip(numeric, lower, upper).astype(np.float32)

    levels = manifest.get("target-levels")
    if levels is not None:
        if (not isinstance(levels, dict)
                or levels.get("type") not in ("character", "logical", "numeric")
                or not isinstance(levels.get("values"), list)):
            raise ValueError("manifest has invalid public target-levels")
        n_classes = len(levels["values"])
    else:
        n_classes = int(manifest.get(
            "num-classes", 2 if manifest.get("dp-track") == "trees" else 2))
    if n_classes < 2 or n_classes > 1024:
        raise ValueError("manifest has invalid public class count")
    if not bool(np.all(numeric == np.floor(numeric))):
        raise ValueError("classification target must contain integer class codes")
    if not bool(np.all((numeric >= 0) & (numeric < n_classes))):
        raise ValueError("classification target is outside the public class domain")
    return numeric.astype(np.float32)


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
    if not isinstance(target_col, str) or target_col not in df.columns:
        raise ValueError("manifest must pin one available target column")
    feat_cols = manifest.get("feature_columns")
    patient_col = manifest.get("patient_column")
    if feat_cols:
        feat_cols = [str(col) for col in feat_cols if str(col) != patient_col]
        if not feat_cols:
            raise ValueError("manifest has no non-identifier feature columns")
        feat = df[feat_cols]
    else:
        excluded = [target_col] + ([patient_col] if patient_col else [])
        feat = df.drop(columns=excluded)
    X = feat.to_numpy(dtype=np.float32)
    return X, _load_target(df[target_col], manifest)


def is_image_run(context=None):
    """True if the staged manifest is an image collection (data_type=='image')."""
    return _load_manifest(context).get("data_type") == "image"


def _load_patient_ids(df, manifest):
    """Return the explicit server-pinned DP unit; never auto-detect/fallback."""
    unit = manifest.get("dp-unit")
    if unit not in ("row", "patient"):
        raise ValueError("manifest is missing a valid pinned DP unit")
    explicit = manifest.get("patient_column")
    if unit == "row":
        if explicit not in (None, ""):
            raise ValueError("row-level manifest must not pin a patient column")
        return None
    if (not isinstance(explicit, str) or not explicit
            or explicit not in df.columns):
        raise ValueError("manifest-pinned patient column is missing")
    if manifest.get("patient-id-canonicalization") != "trim-utf8-v1":
        raise ValueError("manifest has an unsupported patient ID canonicalization")
    values = df[explicit]
    text = values.astype(str).str.strip()
    invalid = values.isna() | text.eq("") | text.str.lower().isin(
        ("na", "nan", "null"))
    if bool(invalid.any()):
        raise ValueError(
            "manifest-pinned patient identifiers contain missing values")
    return text.to_numpy(dtype=str)


def _resolve_image_path(images_root, value):
    """Resolve one portable relative path without allowing root escape."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("image samples contain an unsafe relative path")
    if ("\\" in value or os.path.isabs(value) or ntpath.isabs(value)
            or ntpath.splitdrive(value)[0]):
        raise ValueError("image samples contain an unsafe relative path")
    parts = value.split("/")
    if any(not part or part in (".", "..") for part in parts):
        raise ValueError("image samples contain an unsafe relative path")

    root = os.path.realpath(images_root)
    candidate = os.path.realpath(os.path.join(root, *parts))
    try:
        contained = os.path.commonpath((root, candidate)) == root
    except ValueError:
        contained = False
    if not contained or not os.path.isfile(candidate):
        raise ValueError("one or more staged images are unavailable")
    return candidate


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
        raise ValueError("image samples file is unavailable")
    if samples_file.lower().endswith(".parquet"):
        import pyarrow.parquet as pq
        df = pq.read_table(samples_file).to_pandas()
    else:
        df = pd.read_csv(samples_file)

    assets = manifest.get("assets", {}) or {}
    images = assets.get("images", {}) or {}
    images_root = images.get("root") or manifest["data_root"]
    if not os.path.isdir(images_root):
        raise ValueError("image data root is unavailable")
    path_col = images.get("path_col", "relative_path")
    if path_col not in df.columns:
        raise ValueError(
            f"image samples table is missing the path column '{path_col}'")

    target_col = manifest["target_column"]
    if not isinstance(target_col, str) or target_col not in df.columns:
        raise ValueError("manifest must pin one available target column")
    if df[target_col].isna().any() or not len(df):
        raise ValueError("staged image collection has an invalid target column")

    paths = [_resolve_image_path(images_root, p) for p in df[path_col]]
    groups = _load_patient_ids(df, manifest)
    return paths, _load_target(df[target_col], manifest), groups


def load_privacy_config(context=None):
    """Read the tamper-proof DP parameters from the server-written manifest.

    DP is ALWAYS enforced — there is no 'off' path and no profiles. Counts and
    per-node metrics are suppressed/bucketed by default (disclosure backstop).
    """
    manifest = _load_manifest(context)
    if manifest.get("privacy-reserved") is not True:
        raise ValueError("manifest has no committed privacy reservation")
    release_enabled = bool(manifest.get("privacy-release-enabled", False))
    epsilon = float(manifest.get("privacy-epsilon", 0.0))
    delta = float(manifest.get("privacy-delta", 0.0))
    clipping_norm = float(manifest.get("privacy-clipping_norm", 1.0))
    # The manifest is server-written, but during the staging transition it may
    # still carry client-PROPOSED DP params (the run_config is merged in at stage
    # time), so the node treats them as untrusted. First: legal ranges (a corrupted
    # value must not silently produce zero/infinite noise that voids the guarantee).
    if not (clipping_norm > 0 and math.isfinite(clipping_norm)):
        raise ValueError(
            "invalid DP clipping norm in manifest")
    if release_enabled and not (
        epsilon > 0 and 0.0 < delta < 1.0
        and math.isfinite(epsilon) and math.isfinite(delta)
    ):
        raise ValueError("enabled DP release has invalid epsilon/delta")
    if not release_enabled and not (
        epsilon >= 0.0 and delta >= 0.0
        and math.isfinite(epsilon) and math.isfinite(delta)
    ):
        # The accountant preserves the exact (possibly tiny) scheduled values
        # in the ledger/manifest even when numerical policy turns the operation
        # into a data-independent no-op.
        raise ValueError("disabled DP allocation has invalid epsilon/delta")
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
    try:
        sample_aggregate_raw = float(
            manifest.get("privacy-sample_aggregate", 0))
        sa_blocks_raw = float(manifest.get("privacy-sa_blocks", 8))
        egress_time_pad = float(manifest.get("privacy-egress_time_pad", 0))
        egress_timeout_raw = float(
            manifest.get("privacy-egress_timeout", 900))
        egress_memory_mb_raw = float(
            manifest.get("privacy-egress_memory_mb", 8192))
        egress_file_mb_raw = float(
            manifest.get("privacy-egress_file_mb", 1024))
        egress_processes_raw = float(
            manifest.get("privacy-egress_processes", 128))
        hook_enabled_raw = float(manifest.get("privacy-hook_enabled", 0))
        n_samples_raw = float(manifest.get("n_samples", 0))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("manifest contains an invalid server privacy option") from exc
    if sample_aggregate_raw not in (0.0, 1.0) or hook_enabled_raw not in (0.0, 1.0):
        raise ValueError("manifest privacy switches must be exactly zero or one")
    if manifest.get("privacy-adjacency") != "replace_one":
        raise ValueError("manifest must pin replace-one privacy adjacency")
    if (not math.isfinite(sa_blocks_raw) or sa_blocks_raw != math.floor(sa_blocks_raw)
            or not 2 <= sa_blocks_raw <= 64):
        raise ValueError("manifest S&A block count must be an integer in [2, 64]")
    if not math.isfinite(egress_time_pad) or egress_time_pad < 0:
        raise ValueError("manifest Hook time pad must be finite and non-negative")
    if (not math.isfinite(egress_timeout_raw)
            or egress_timeout_raw != math.floor(egress_timeout_raw)
            or not 1 <= egress_timeout_raw <= 3600):
        raise ValueError("manifest Hook timeout must be an integer in [1, 3600]")
    resource_limits = (
        ("memory", egress_memory_mb_raw, 512, 131072),
        ("file", egress_file_mb_raw, 16, 16384),
        ("process", egress_processes_raw, 1, 1024),
    )
    for label, value, lower, upper in resource_limits:
        if (not math.isfinite(value) or value != math.floor(value)
                or not lower <= value <= upper):
            raise ValueError(
                "manifest Hook %s limit must be an integer in [%d, %d]"
                % (label, lower, upper))
    if (not math.isfinite(n_samples_raw) or n_samples_raw != math.floor(n_samples_raw)
            or n_samples_raw < 0):
        raise ValueError("manifest sample count must be a non-negative integer")
    return {
        "epsilon": epsilon,
        "delta": delta,
        "clipping_norm": clipping_norm,
        "release_enabled": release_enabled,
        "adjacency": "replace_one",
        # Improved Tier-2 floor (sample-and-aggregate) policy. k is a fixed,
        # administrator-pinned value: deriving it from private n would expose n
        # through a data-dependent Gaussian variance.
        "sample_aggregate": bool(sample_aggregate_raw),
        "sa_blocks": int(sa_blocks_raw),
        "egress_time_pad": egress_time_pad,
        "egress_timeout": int(egress_timeout_raw),
        "egress_memory_mb": int(egress_memory_mb_raw),
        "egress_file_mb": int(egress_file_mb_raw),
        "egress_processes": int(egress_processes_raw),
        "hook_enabled": bool(hook_enabled_raw),
        "allow_per_node_metrics": False,
        "allow_exact_num_examples": False,
        "n_samples": int(n_samples_raw),
    }


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
    if "dp-track" not in manifest:
        raise ValueError("manifest is missing the pinned dp-track")
    track = str(manifest["dp-track"]).lower()
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

    def required(key, cast):
        if key not in manifest:
            raise ValueError("manifest is missing DP-critical pin '%s'" % key)
        return cast(manifest[key])

    def bounded_int(key, upper):
        if key not in manifest or isinstance(manifest[key], bool):
            raise ValueError("manifest is missing valid DP-critical pin '%s'" % key)
        try:
            number = float(manifest[key])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("manifest pin '%s' must be an integer" % key) from exc
        if (not math.isfinite(number) or number != math.floor(number)
                or number < 1 or number > int(upper)):
            raise ValueError(
                "manifest pin '%s' must be in [1, %d]" % (key, int(upper)))
        return int(number)

    try:
        learning_rate = float(
            cfg.get("learning-rate", manifest.get("learning-rate", 0.01)))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("learning-rate must be in (0, 10]") from exc
    if (not math.isfinite(learning_rate) or learning_rate <= 0.0
            or learning_rate > _MAX_LEARNING_RATE):
        raise ValueError("learning-rate must be in (0, 10]")

    return {
        "loss_name": required("loss-name", str),
        "batch_size": bounded_int("batch-size", _MAX_BATCH_SIZE),
        "local_epochs": bounded_int("local-epochs", _MAX_LOCAL_EPOCHS),
        "num_rounds": bounded_int("num-server-rounds", _MAX_SERVER_ROUNDS),
        "n_classes": required("num-classes", int),
        "learning_rate": learning_rate,
    }


def load_pinned_run_config(context=None):
    """Overlay every node-pinned declarative field onto Flower run_config."""
    manifest = _load_manifest(context)
    cfg = _run_config(context)
    # The analyst-facing Flower config may name a module for the researcher-side
    # ServerApp, but node execution accepts only the package name derived and
    # written by flowerTier2PinDS after installation/hash verification.
    cfg.pop("user-module", None)
    keys = (
        "model-spec-b64", "loss-name", "num-classes", "num-labels",
        "local-epochs", "batch-size", "num-server-rounds", "num-features",
        "gbdt-spec", "feature-bounds", "user-module",
    )
    for key in keys:
        if key in manifest:
            cfg[key] = manifest[key]
    return cfg


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


def _decode_feature_bounds(cfg, n_features):
    """Decode public bounds pinned in the manifest or legacy b64 run config."""
    bounds = cfg.get("feature-bounds")
    if bounds is None and cfg.get("feature-bounds-b64"):
        import base64
        try:
            bounds = json.loads(base64.b64decode(
                str(cfg["feature-bounds-b64"]), validate=True).decode("utf-8"))
        except Exception as exc:
            raise RuntimeError("feature-bounds-b64 is invalid: %s" % exc)
    if bounds is None:
        return None
    lower = np.asarray(bounds.get("lower", []), dtype=np.float64)
    upper = np.asarray(bounds.get("upper", []), dtype=np.float64)
    if lower.shape != (int(n_features),) or upper.shape != (int(n_features),):
        raise RuntimeError("public feature bounds do not match num features")
    if (not np.all(np.isfinite(lower)) or not np.all(np.isfinite(upper))
            or not np.all(lower < upper)
            or np.any(np.abs(lower) > _MAX_NUMERIC_ABS)
            or np.any(np.abs(upper) > _MAX_NUMERIC_ABS)):
        raise RuntimeError("public feature bounds must be finite with lower < upper")
    return lower, upper


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
    if "gbdt-spec" not in manifest:
        raise ValueError("manifest is missing the pinned gbdt-spec")
    spec = dict(manifest.get("gbdt-spec", {}) or {})
    cfg = load_pinned_run_config(context)

    objective = str(spec.get("objective", cfg.get("objective", "binary:logistic")))
    max_depth = int(spec.get("max_depth", cfg.get("max-depth", 3)))
    n_trees = int(spec.get("n_trees", cfg.get("n-trees", 20)))
    learning_rate = float(spec.get("learning_rate", cfg.get("learning-rate", 0.3)))
    reg_lambda = float(spec.get("reg_lambda", cfg.get("reg-lambda", 1.0)))
    # Newton-step denominator is max(lambda, H+lambda); lambda<=0 (or non-finite lr/lambda)
    # can yield inf/NaN leaf weights. Clamp to safe values (fail-closed on the math).
    if not (math.isfinite(learning_rate)
            and 0 < learning_rate <= _MAX_LEARNING_RATE):
        raise RuntimeError("DP-GBDT learning_rate must be in (0, 10]")
    if not (math.isfinite(reg_lambda) and reg_lambda > 0):
        reg_lambda = 1e-6
    reg_lambda = min(reg_lambda, _MAX_GBDT_REG_LAMBDA)
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
            raise RuntimeError(
                "manifest must pin num-features before DP-GBDT execution")
        # Threshold ranges are public constants, never exact node statistics.
        bounds = _decode_feature_bounds(cfg, n_features)
        if bounds is not None:
            lower, upper = bounds
            feature_ranges = [[float(lower[j]), float(upper[j])]
                              for j in range(n_features)]
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
    return _load_patient_ids(df, manifest)
