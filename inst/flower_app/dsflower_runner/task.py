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
import re

import numpy as np
import pandas as pd


_MAX_BATCH_SIZE = 65_536
_MAX_LOCAL_EPOCHS = 1_000
_MAX_SERVER_ROUNDS = 500
_MAX_LEARNING_RATE = 10.0
_MAX_NUMERIC_ABS = 1.0e6
_MAX_PATIENT_ID_BYTES = 4096
_MISSING_PATIENT_UNIT = "__dsflower_missing_patient_unit__"
_INVALID_IMAGE = "__dsflower_invalid_image__"
_ASCII_ID_TRIM = " \t\r\n"

_CV_EXECUTION_FIELDS = (
    "dp-track", "task-type", "model-spec-b64", "loss-name",
    "num-classes", "num-labels", "num-features", "local-epochs",
    "batch-size", "num-server-rounds",
)
_CV_TRAINING_FIELDS = (
    "learning-rate", "weight-decay", "l1-penalty",
    "optimizer-name", "scheduler-name",
)
_CV_OPTIMIZER_FIELDS = {
    "sgd": ("optimizer-momentum", "optimizer-nesterov"),
    "adam": ("optimizer-beta1", "optimizer-beta2", "optimizer-eps",
             "optimizer-amsgrad"),
    "adamw": ("optimizer-beta1", "optimizer-beta2", "optimizer-eps",
              "optimizer-amsgrad"),
    "rmsprop": ("optimizer-momentum", "optimizer-eps",
                "optimizer-rmsprop-alpha"),
}
_CV_SCHEDULER_FIELDS = {
    "none": (),
    "step": ("scheduler-step-size", "scheduler-gamma"),
    "exponential": ("scheduler-gamma",),
    "cosine": ("scheduler-min-lr",),
}
_CV_LOSS_FIELDS = {
    "negbin_nll": ("nb-dispersion",),
    "gamma_nll": ("gamma-shape",),
    "huber": ("huber-delta",),
    "quantile": ("quantile-level",),
}
_CV_KNOWN_EXECUTION_FIELDS = frozenset(
    _CV_EXECUTION_FIELDS + _CV_TRAINING_FIELDS + ("data-kind",)
    + tuple(field for fields in _CV_OPTIMIZER_FIELDS.values()
            for field in fields)
    + tuple(field for fields in _CV_SCHEDULER_FIELDS.values()
            for field in fields)
    + tuple(field for fields in _CV_LOSS_FIELDS.values()
            for field in fields)
)


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
        numeric = pd.to_numeric(y_series, errors="coerce").to_numpy(
            dtype=np.float64)
        midpoint = lower + (upper - lower) / 2.0
        numeric = np.nan_to_num(
            numeric, nan=midpoint, posinf=upper, neginf=lower)
        return np.clip(numeric, lower, upper).astype(np.float32)

    levels = manifest.get("target-levels")
    if levels is not None:
        if (not isinstance(levels, dict)
                or levels.get("type") not in ("character", "logical", "numeric")
                or not isinstance(levels.get("values"), list)):
            raise ValueError("manifest has invalid public target-levels")
        n_classes = len(levels["values"])
    else:
        n_classes = int(manifest.get("num-classes", 2))
    if n_classes < 2 or n_classes > 1024:
        raise ValueError("manifest has invalid public class count")
    numeric = pd.to_numeric(y_series, errors="coerce").to_numpy(
        dtype=np.float64)
    valid = (np.isfinite(numeric) & (numeric == np.floor(numeric))
             & (numeric >= 0) & (numeric < n_classes))
    # Code zero is a public catch-all. Invalid private values therefore change
    # one bounded record instead of selecting an exact no-op response.
    return np.where(valid, numeric, 0.0).astype(np.float32)


def _load_features(frame, manifest):
    """Map every private feature cell to the fixed finite numeric domain."""
    numeric = frame.apply(pd.to_numeric, errors="coerce").to_numpy(
        dtype=np.float64)
    defaults = np.zeros(numeric.shape[1], dtype=np.float64)
    bounds = manifest.get("feature-bounds")
    if isinstance(bounds, dict):
        lower = np.asarray(bounds.get("lower", []), dtype=np.float64)
        upper = np.asarray(bounds.get("upper", []), dtype=np.float64)
        if (lower.shape == defaults.shape and upper.shape == defaults.shape
                and np.all(np.isfinite(lower)) and np.all(np.isfinite(upper))
                and np.all(lower < upper)
                and np.all(np.abs(lower) <= _MAX_NUMERIC_ABS)
                and np.all(np.abs(upper) <= _MAX_NUMERIC_ABS)):
            defaults = lower + (upper - lower) / 2.0
    invalid = ~np.isfinite(numeric)
    if bool(np.any(invalid)):
        numeric = np.where(invalid, defaults[np.newaxis, :], numeric)
    return np.clip(
        numeric, -_MAX_NUMERIC_ABS, _MAX_NUMERIC_ABS).astype(np.float32)


def _read_staged_frame(path, manifest):
    """Read one staged table without lossy CSV inference of identity fields."""
    if (manifest.get("data_format") == "parquet"
            or path.lower().endswith(".parquet")):
        import pyarrow.parquet as pq
        return pq.read_table(path).to_pandas()

    string_columns = []
    patient_column = manifest.get("patient_column")
    if isinstance(patient_column, str) and patient_column:
        string_columns.append(patient_column)
    if manifest.get("data_type") == "image":
        assets = manifest.get("assets", {}) or {}
        images = assets.get("images", {}) or {}
        path_column = images.get("path_col", "relative_path")
        if isinstance(path_column, str) and path_column:
            string_columns.append(path_column)
    return pd.read_csv(
        path,
        dtype={column: "string" for column in set(string_columns)},
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8",
        encoding_errors="strict",
    )


def load_data(context=None):
    """Load (X, y) for standard supervised training from the staged manifest."""
    manifest = _load_manifest(context)
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])

    df = _read_staged_frame(data_file, manifest)

    target_col = manifest["target_column"]
    multilabel = str(manifest.get("loss-name", "")).lower() == "multilabel_bce"
    if multilabel:
        num_labels = int(manifest.get("num-labels", 0))
        if (not isinstance(target_col, list) or len(target_col) != num_labels
                or num_labels < 2 or num_labels > 1024
                or len(set(target_col)) != len(target_col)
                or any(not isinstance(col, str) or col not in df.columns
                       for col in target_col)):
            raise ValueError(
                "multilabel manifest must pin num-labels available target columns")
        target_cols = target_col
    else:
        if not isinstance(target_col, str) or target_col not in df.columns:
            raise ValueError("manifest must pin one available target column")
        target_cols = [target_col]
    feat_cols = manifest.get("feature_columns")
    patient_col = manifest.get("patient_column")
    if feat_cols:
        feat_cols = [str(col) for col in feat_cols if str(col) != patient_col]
        if not feat_cols:
            raise ValueError("manifest has no non-identifier feature columns")
        feat = df[feat_cols]
    else:
        excluded = target_cols + ([patient_col] if patient_col else [])
        feat = df.drop(columns=excluded)
    X = _load_features(feat, manifest)
    if multilabel:
        y = np.column_stack([
            _load_target(df[column], manifest) for column in target_cols
        ]).astype(np.float32)
    else:
        y = _load_target(df[target_col], manifest)
    return X, y


def load_native_tree_data(context=None, *, manifest=None):
    """Load one tabular native-tree input without reopening its staged table."""
    if manifest is None:
        manifest = _load_manifest(context)
    if manifest.get("data_type") != "tabular":
        raise ValueError("native-tree training requires staged tabular data")
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])
    frame = _read_staged_frame(data_file, manifest)

    target_column = manifest.get("target_column")
    feature_columns = manifest.get("feature_columns")
    patient_column = manifest.get("patient_column")
    if not isinstance(target_column, str) or target_column not in frame.columns:
        raise ValueError("native-tree manifest target is unavailable")
    if not isinstance(feature_columns, list) or not feature_columns or \
            any(not isinstance(column, str) or not column
                for column in feature_columns) or \
            len(set(feature_columns)) != len(feature_columns) or \
            patient_column in feature_columns or \
            target_column in feature_columns or \
            any(column not in frame.columns for column in feature_columns):
        raise ValueError("native-tree manifest features are invalid")

    features = _load_features(frame[feature_columns], manifest)
    target = _load_target(frame[target_column], manifest)
    unit_ids = _load_patient_ids(frame, manifest)
    assert_pinned_unit_count(
        context, int(features.shape[0]), patient_ids=unit_ids,
        manifest=manifest)
    return features, target, unit_ids


def _load_association_codes(series):
    """Totalize one staged association axis to the fixed 0/1/2 domain."""
    numeric = pd.to_numeric(series, errors="coerce").to_numpy(dtype=np.float64)
    valid = (np.isfinite(numeric) & (numeric == np.floor(numeric))
             & (numeric >= 0.0) & (numeric <= 2.0))
    return np.where(valid, numeric, 2.0).astype(np.uint8)


def load_association_data(context=None, *, manifest=None):
    """Load the two pre-encoded axes for one private association release."""
    if manifest is None:
        manifest = _load_manifest(context)
    if manifest.get("data_type") != "tabular":
        raise ValueError("association requires staged tabular data")

    target_column = manifest.get("target_column")
    feature_columns = manifest.get("feature_columns")
    patient_column = manifest.get("patient_column")
    if not isinstance(target_column, str) or not target_column or \
            not isinstance(feature_columns, list) or len(feature_columns) != 1 or \
            not isinstance(feature_columns[0], str) or not feature_columns[0] or \
            feature_columns[0] == target_column or \
            patient_column in (target_column, feature_columns[0]):
        raise ValueError("association manifest columns are invalid")

    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])
    frame = _read_staged_frame(data_file, manifest)
    exposure_column = feature_columns[0]
    if target_column not in frame.columns or exposure_column not in frame.columns:
        raise ValueError("association staged columns are unavailable")

    outcome = _load_association_codes(frame[target_column])
    exposure = _load_association_codes(frame[exposure_column])
    unit_ids = _load_patient_ids(frame, manifest)
    assert_pinned_unit_count(
        context, int(outcome.size), patient_ids=unit_ids, manifest=manifest)
    return outcome, exposure, unit_ids


def is_image_run(context=None):
    """True if the staged manifest is an image collection (data_type=='image')."""
    return _load_manifest(context).get("data_type") == "image"


def _canonical_patient_id(value):
    """Apply the exact public trim-utf8-v2 patient-ID mapping."""
    try:
        text = ("" if value is None else str(value)).strip(_ASCII_ID_TRIM)
        encoded = text.encode("utf-8", errors="strict")
    except (TypeError, UnicodeError, ValueError):
        return _MISSING_PATIENT_UNIT
    if (not text or len(encoded) > _MAX_PATIENT_ID_BYTES
            or text.lower() in ("na", "nan", "null", "<na>", "nat")):
        return _MISSING_PATIENT_UNIT
    return text


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
    if manifest.get("patient-id-canonicalization") != "trim-utf8-v2":
        raise ValueError("manifest has an unsupported patient ID canonicalization")
    values = df[explicit]
    return np.asarray(
        [_canonical_patient_id(value) for value in values], dtype=str)


def assert_pinned_unit_count(context, n_rows, patient_ids=None, *,
                             manifest=None):
    """Fail closed on a structurally changed staged privacy-unit roster."""
    if manifest is None:
        manifest = _load_manifest(context)
    if "n_units" not in manifest:
        return
    number = pinned_unit_count_from_manifest(manifest)
    observed = (int(n_rows) if patient_ids is None else int(np.unique(
        np.asarray([_canonical_patient_id(value) for value in patient_ids],
                   dtype=str)).size))
    if observed != number:
        raise RuntimeError("staged privacy-unit roster changed after manifest pinning")


def pinned_unit_count_from_manifest(manifest):
    """Return the validated server-pinned public privacy-unit census."""
    if "n_units" not in manifest:
        raise ValueError("manifest is missing its pinned privacy-unit count")
    raw = manifest["n_units"]
    if isinstance(raw, (bool, np.bool_)):
        raise ValueError("manifest has an invalid pinned privacy-unit count")
    try:
        number = float(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("manifest has an invalid pinned privacy-unit count") from exc
    if (not math.isfinite(number) or number != math.floor(number) or number < 0):
        raise ValueError("manifest has an invalid pinned privacy-unit count")
    return int(number)


def _resolve_image_path(images_root, value):
    """Resolve one portable relative path without allowing root escape."""
    if isinstance(value, str) and value == _INVALID_IMAGE:
        return None
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
    df = _read_staged_frame(samples_file, manifest)

    assets = manifest.get("assets", {}) or {}
    images = assets.get("images", {}) or {}
    images_root = images.get("root")
    if not isinstance(images_root, str) or not images_root:
        raise ValueError("image manifest must pin an image-root asset")
    if not os.path.isdir(images_root):
        raise ValueError("image data root is unavailable")
    path_col = images.get("path_col", "relative_path")
    if path_col not in df.columns:
        raise ValueError(
            f"image samples table is missing the path column '{path_col}'")

    target_col = manifest["target_column"]
    if not isinstance(target_col, str) or target_col not in df.columns:
        raise ValueError("manifest must pin one available target column")
    if not len(df):
        raise ValueError("staged image collection has an invalid target column")

    paths = []
    for value in df[path_col]:
        try:
            paths.append(_resolve_image_path(images_root, value))
        except (OSError, TypeError, ValueError):
            # A private bad/missing path is one bounded zero-image record. Never
            # pass the sentinel to a filesystem API in the vision reader.
            paths.append(None)
    groups = _load_patient_ids(df, manifest)
    return paths, _load_target(df[target_col], manifest), groups


def load_privacy_config(context=None):
    """Read the tamper-proof DP parameters from the server-written manifest.

    DP is ALWAYS enforced — there is no 'off' path and no profiles. Counts and
    per-node metrics are suppressed/bucketed by default (disclosure backstop).
    """
    manifest = _load_manifest(context)
    epsilon = float(manifest.get("privacy-epsilon", 0.0))
    delta = float(manifest.get("privacy-delta", 0.0))
    clipping_norm = float(manifest.get("privacy-clipping_norm", 1.0))
    policy_hash = manifest.get("privacy-policy-sha256")
    if (not isinstance(policy_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", policy_hash) is None):
        raise ValueError("manifest has no canonical stateless privacy policy")
    # Treat even server-written values as untrusted at the Python boundary. A
    # corrupted value must not silently produce zero/infinite noise.
    if not (clipping_norm > 0 and math.isfinite(clipping_norm)):
        raise ValueError(
            "invalid DP clipping norm in manifest")
    if not (
        epsilon > 0 and 0.0 < delta < 1.0
        and math.isfinite(epsilon) and math.isfinite(delta)
    ):
        raise ValueError("stateless DP release has invalid epsilon/delta")
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
        "policy_hash": policy_hash,
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
    pins it). The track only selects which node-enforced DP mechanism runs; the
    manifest pin makes it node-authoritative."""
    manifest = _load_manifest(context)
    if "dp-track" not in manifest:
        raise ValueError("manifest is missing the pinned dp-track")
    track = str(manifest["dp-track"]).lower()
    if track not in ("neural", "egress", "validation", "association"):
        raise ValueError("invalid dp-track '%s'" % track)
    return track


def load_run_pins(context=None):
    """Manifest-pinned sampling / horizon / loss for the neural track.

    Every value that feeds the DP-SGD noise calibration -- the loss (a coupling
    loss breaks the per-sample bound), batch size, local epochs, and rounds (they
    set the composition horizon), and num-classes (the head width + label gate) --
    is read from the tamper-proof manifest. Public defaults are applied only by
    this trusted loader; the client run config is never authoritative.
    """
    manifest = _load_manifest(context)

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

    def bounded_float(key, default, lower, upper, *, lower_open=False,
                      upper_open=False):
        try:
            value = float(manifest.get(key, default))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("manifest pin '%s' must be finite" % key) from exc
        lower_ok = value > lower if lower_open else value >= lower
        upper_ok = value < upper if upper_open else value <= upper
        if not math.isfinite(value) or not lower_ok or not upper_ok:
            raise ValueError("manifest pin '%s' is outside its allowed range" % key)
        return value

    def pinned_bool(key, default):
        value = manifest.get(key, default)
        if type(value) is not bool:
            raise ValueError("manifest pin '%s' must be boolean" % key)
        return value

    loss_name = required("loss-name", str)
    allowed_losses = {
        "bce_logits", "cross_entropy", "mse", "poisson_nll",
        "multilabel_bce", "hinge", "negbin_nll", "gamma_nll",
        "huber", "quantile", "ordinal"}
    if loss_name not in allowed_losses:
        raise ValueError("loss-name is not on the trusted allowlist")
    loss_fields = {"nb-dispersion", "gamma-shape", "huber-delta", "quantile-level"}
    selected_loss_field = {
        "negbin_nll": "nb-dispersion",
        "gamma_nll": "gamma-shape",
        "huber": "huber-delta",
        "quantile": "quantile-level",
    }.get(loss_name)
    incompatible_loss_fields = (
        loss_fields - ({selected_loss_field} if selected_loss_field else set())
    ) & set(manifest)
    if incompatible_loss_fields:
        raise ValueError("loss config contains incompatible field(s): %s"
                         % ", ".join(sorted(incompatible_loss_fields)))
    if selected_loss_field is not None and selected_loss_field not in manifest:
        raise ValueError(
            "manifest is missing selected loss parameter '%s'"
            % selected_loss_field)
    if loss_name == "negbin_nll":
        bounded_float("nb-dispersion", 1.0, 1.0e-6, 1.0e12)
    elif loss_name == "gamma_nll":
        bounded_float("gamma-shape", 1.0, 1.0e-6, 1.0e12)
    elif loss_name == "huber":
        bounded_float("huber-delta", 1.0, 1.0e-6, 1.0e6)
    elif loss_name == "quantile":
        quantile = bounded_float("quantile-level", 0.5, 0.0, 1.0,
                                 lower_open=True)
        if quantile >= 1.0:
            raise ValueError("quantile-level must be in (0, 1)")

    learning_rate = bounded_float(
        "learning-rate", 0.01, 0.0, _MAX_LEARNING_RATE, lower_open=True)
    optimizer = str(manifest.get("optimizer-name", "sgd")).lower()
    if optimizer not in ("sgd", "adam", "adamw", "rmsprop"):
        raise ValueError("optimizer-name is not on the trusted allowlist")
    scheduler = str(manifest.get("scheduler-name", "none")).lower()
    if scheduler not in ("none", "step", "exponential", "cosine"):
        raise ValueError("scheduler-name is not on the trusted allowlist")

    optimizer_config = {
        "name": optimizer,
        "weight_decay": bounded_float("weight-decay", 0.0, 0.0, 1000.0),
        "l1_penalty": bounded_float("l1-penalty", 0.0, 0.0, 1000.0),
        "momentum": bounded_float(
            "optimizer-momentum", 0.0, 0.0, 1.0, upper_open=True),
        "nesterov": pinned_bool("optimizer-nesterov", False),
        "beta1": bounded_float(
            "optimizer-beta1", 0.9, 0.0, 1.0, upper_open=True),
        "beta2": bounded_float(
            "optimizer-beta2", 0.999, 0.0, 1.0, upper_open=True),
        "eps": bounded_float(
            "optimizer-eps", 1e-8, 0.0, 1.0, lower_open=True),
        "amsgrad": pinned_bool("optimizer-amsgrad", False),
        "rmsprop_alpha": bounded_float(
            "optimizer-rmsprop-alpha", 0.99, 0.0, 1.0, upper_open=True),
    }
    if optimizer_config["nesterov"] and (
            optimizer != "sgd" or optimizer_config["momentum"] <= 0.0):
        raise ValueError("nesterov requires SGD with positive momentum")
    optimizer_fields = {
        "optimizer-momentum", "optimizer-nesterov", "optimizer-beta1",
        "optimizer-beta2", "optimizer-eps", "optimizer-amsgrad",
        "optimizer-rmsprop-alpha"}
    optimizer_allowed = {
        "sgd": {"optimizer-momentum", "optimizer-nesterov"},
        "adam": {"optimizer-beta1", "optimizer-beta2", "optimizer-eps",
                 "optimizer-amsgrad"},
        "adamw": {"optimizer-beta1", "optimizer-beta2", "optimizer-eps",
                  "optimizer-amsgrad"},
        "rmsprop": {"optimizer-momentum", "optimizer-eps",
                    "optimizer-rmsprop-alpha"},
    }[optimizer]
    incompatible = (optimizer_fields - optimizer_allowed) & set(manifest)
    if incompatible:
        raise ValueError("optimizer config contains incompatible field(s): %s"
                         % ", ".join(sorted(incompatible)))

    scheduler_config = {
        "name": scheduler,
        "step_size": bounded_int("scheduler-step-size", _MAX_LOCAL_EPOCHS)
            if "scheduler-step-size" in manifest else 1,
        "gamma": bounded_float(
            "scheduler-gamma", 0.1, 0.0, _MAX_LEARNING_RATE,
            lower_open=True),
        "min_lr": bounded_float(
            "scheduler-min-lr", 0.0, 0.0, _MAX_LEARNING_RATE),
    }
    if scheduler_config["min_lr"] > learning_rate:
        raise ValueError("scheduler-min-lr cannot exceed learning-rate")
    scheduler_fields = {
        "scheduler-step-size", "scheduler-gamma", "scheduler-min-lr"}
    scheduler_allowed = {
        "none": set(),
        "step": {"scheduler-step-size", "scheduler-gamma"},
        "exponential": {"scheduler-gamma"},
        "cosine": {"scheduler-min-lr"},
    }[scheduler]
    incompatible = (scheduler_fields - scheduler_allowed) & set(manifest)
    if incompatible:
        raise ValueError("scheduler config contains incompatible field(s): %s"
                         % ", ".join(sorted(incompatible)))

    n_classes = bounded_int("num-classes", 1024)
    if loss_name == "bce_logits" and n_classes != 2:
        raise ValueError("bce_logits is binary only; use cross_entropy")

    return {
        "loss_name": loss_name,
        "batch_size": bounded_int("batch-size", _MAX_BATCH_SIZE),
        "local_epochs": bounded_int("local-epochs", _MAX_LOCAL_EPOCHS),
        "num_rounds": bounded_int("num-server-rounds", _MAX_SERVER_ROUNDS),
        "n_classes": n_classes,
        "learning_rate": learning_rate,
        "optimizer": optimizer_config,
        "scheduler": scheduler_config,
    }


def _cv_execution_contract(manifest):
    """Return every Flower scalar that can change one pinned CV execution."""
    optimizer = str(manifest.get("optimizer-name", "")).lower()
    scheduler = str(manifest.get("scheduler-name", "")).lower()
    loss = str(manifest.get("loss-name", "")).lower()
    if (optimizer not in _CV_OPTIMIZER_FIELDS
            or scheduler not in _CV_SCHEDULER_FIELDS):
        raise ValueError(
            "manifest has an invalid cross-validation training pin")
    fields = (
        _CV_EXECUTION_FIELDS + _CV_TRAINING_FIELDS
        + _CV_OPTIMIZER_FIELDS[optimizer]
        + _CV_SCHEDULER_FIELDS[scheduler]
        + _CV_LOSS_FIELDS.get(loss, ())
    )
    missing = [field for field in fields if field not in manifest]
    if "data_type" not in manifest:
        missing.append("data_type")
    if missing:
        raise ValueError(
            "manifest is missing cross-validation execution pin '%s'"
            % sorted(missing)[0])
    expected = {field: manifest[field] for field in fields}
    expected["data-kind"] = manifest["data_type"]
    return expected


def _validate_cv_execution_config(manifest, cfg):
    """Reject a Flower CV recipe that differs from its server-authored job."""
    expected = _cv_execution_contract(manifest)
    present = _CV_KNOWN_EXECUTION_FIELDS & set(cfg)
    if present != set(expected):
        raise ValueError(
            "Flower cross-validation execution config does not match manifest pin")
    for key, value in expected.items():
        supplied = cfg[key]
        if type(supplied) is not type(value) or supplied != value:
            raise ValueError(
                "Flower cross-validation execution config does not match "
                "manifest pin")


def load_pinned_run_config(context=None):
    """Overlay every node-pinned declarative field onto Flower run_config."""
    manifest = _load_manifest(context)
    if (manifest.get("resampling-contract-sha256") is not None
            or manifest.get("cv-contract-sha256") is not None):
        if pinned_unit_count_from_manifest(manifest) < 1:
            raise ValueError(
                "resampling requires a positive pinned privacy-unit count")
    cfg = _run_config(context)
    if manifest.get("cv-contract-sha256") is not None:
        _validate_cv_execution_config(manifest, cfg)
    if str(manifest.get("dp-track", "")).lower() == "egress":
        # The R service canonicalises and hashes the public app payload before
        # staging.  Require the Flower bundle to carry that exact copy before a
        # private file can be opened; neither side may silently substitute it.
        for key in (
                "app-params-b64", "app-params-sha256",
                "num-server-rounds", "task-type", "num-classes",
                "num-features"):
            if key not in manifest:
                raise ValueError("manifest is missing HookApp pin '%s'" % key)
            if cfg.get(key) != manifest[key]:
                raise ValueError("Flower HookApp config does not match manifest pin")
    neural_public_keys = {
        "learning-rate", "nb-dispersion", "gamma-shape", "huber-delta",
        "quantile-level",
        "weight-decay",
        "l1-penalty", "optimizer-name", "optimizer-momentum",
        "optimizer-nesterov", "optimizer-beta1", "optimizer-beta2",
        "optimizer-eps", "optimizer-amsgrad", "optimizer-rmsprop-alpha",
        "scheduler-name", "scheduler-step-size", "scheduler-gamma",
        "scheduler-min-lr"}
    if str(manifest.get("dp-track", "")).lower() == "neural":
        for key in neural_public_keys:
            if key in cfg and (key not in manifest or cfg[key] != manifest[key]):
                raise ValueError("Flower neural config does not match manifest pin")
    resampling_fields = (
        "resampling-version", "resampling-method", "resampling-assignment",
        "resampling-test-numerator", "resampling-test-denominator",
        "resampling-privacy-unit", "resampling-unit-canonicalization",
        "resampling-contract-sha256", "holdout-validation-bins")
    holdout_bound_fields = {"holdout-target-lower", "holdout-target-upper"}
    supplied_resampling = {
        key for key in cfg if str(key).lower().startswith((
            "resampling-", "resampling_", "holdout-", "holdout_"))}
    if manifest.get("resampling-contract-sha256") is not None:
        try:
            from . import resampling
            resampling.contract_from_manifest(manifest)
        except Exception as exc:
            raise ValueError("manifest has no valid holdout contract") from exc
        expected_supplied = set(resampling_fields)
        supplied_bounds = supplied_resampling & holdout_bound_fields
        if supplied_bounds and supplied_bounds != holdout_bound_fields:
            raise ValueError("Flower holdout target bounds are incomplete")
        if supplied_bounds:
            bounds = manifest.get("target-bounds")
            if (not isinstance(bounds, dict)
                    or cfg.get("holdout-target-lower") != bounds.get("lower")
                    or cfg.get("holdout-target-upper") != bounds.get("upper")):
                raise ValueError("Flower holdout target bounds do not match manifest pin")
            expected_supplied |= holdout_bound_fields
        if supplied_resampling != expected_supplied:
            raise ValueError("Flower holdout config has an unexpected field or seed axis")
        for key in resampling_fields:
            if key not in manifest or cfg.get(key) != manifest[key]:
                raise ValueError("Flower holdout config does not match manifest pin")
    elif supplied_resampling:
        raise ValueError("Flower config requests holdout without a manifest contract")
    cv_fields = (
        "cv-version", "cv-method", "cv-assignment", "cv-folds",
        "cv-privacy-unit", "cv-unit-canonicalization",
        "cv-contract-sha256", "cv-validation-bins", "cv-n-nodes",
        "cv-job-sha256")
    cv_bound_fields = {"cv-target-lower", "cv-target-upper"}
    supplied_cv = {
        key for key in cfg
        if str(key).lower().startswith(("cv-", "cv_"))}
    if (manifest.get("resampling-contract-sha256") is not None
            and manifest.get("cv-contract-sha256") is not None):
        raise ValueError("manifest cannot combine holdout and cross-validation")
    if manifest.get("cv-contract-sha256") is not None:
        try:
            from . import resampling
            resampling.cross_validation_contract_from_manifest(manifest)
        except Exception as exc:
            raise ValueError(
                "manifest has no valid cross-validation contract") from exc
        expected_supplied = set(cv_fields)
        supplied_bounds = supplied_cv & cv_bound_fields
        if supplied_bounds and supplied_bounds != cv_bound_fields:
            raise ValueError("Flower cross-validation target bounds are incomplete")
        if supplied_bounds:
            bounds = manifest.get("target-bounds")
            if (not isinstance(bounds, dict)
                    or cfg.get("cv-target-lower") != bounds.get("lower")
                    or cfg.get("cv-target-upper") != bounds.get("upper")):
                raise ValueError(
                    "Flower cross-validation target bounds do not match manifest pin")
            expected_supplied |= cv_bound_fields
        if supplied_cv != expected_supplied:
            raise ValueError(
                "Flower cross-validation config has an unexpected field or seed axis")
        for key in cv_fields:
            if key not in manifest or cfg.get(key) != manifest[key]:
                raise ValueError(
                    "Flower cross-validation config does not match manifest pin")
        try:
            cv_nodes = int(manifest["cv-n-nodes"])
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "manifest has no valid cross-validation node pin") from exc
        if (isinstance(manifest["cv-n-nodes"], bool)
                or cv_nodes != manifest["cv-n-nodes"]
                or not 1 <= cv_nodes <= 65536
                or cfg.get("min-train-nodes") != cv_nodes):
            raise ValueError(
                "Flower cross-validation node count does not match manifest pin")
        strategy_fields = {
            "strategy", "strategy-eta", "strategy-eta-l",
            "strategy-beta-1", "strategy-beta-2", "strategy-tau",
            "strategy-server-learning-rate", "strategy-server-momentum",
        }
        manifest_strategy = {
            key for key in strategy_fields if key in manifest}
        flower_strategy = {key for key in strategy_fields if key in cfg}
        if (manifest_strategy != flower_strategy
                or "strategy" not in manifest_strategy
                or any(cfg[key] != manifest[key] for key in manifest_strategy)):
            raise ValueError(
                "Flower cross-validation strategy does not match manifest pin")
    elif supplied_cv:
        raise ValueError(
            "Flower config requests cross-validation without a manifest contract")
    if str(manifest.get("dp-track", "")).lower() == "validation":
        required = [
            "validation-model-track", "validation-task", "validation-bins",
            "validation-contract-sha256",
            "num-server-rounds", "num-features", "num-classes",
            "num-labels", "loss-name",
        ]
        if str(manifest.get("validation-model-track", "")) == "neural":
            required.append("model-spec-b64")
        elif str(manifest.get("validation-model-track", "")) == "native_tree":
            required.extend([
                "validation-native-tree-request-b64",
                "validation-native-tree-request-sha256",
                "validation-artifact-format", "validation-artifact-sha256",
                "validation-artifact-size-bytes",
                "validation-profile-sha256", "validation-profile-size-bytes",
                "validation-public-schema-sha256",
            ])
        for key in required:
            if key not in manifest:
                raise ValueError("manifest is missing validation pin '%s'" % key)
            if cfg.get(key) != manifest[key]:
                raise ValueError("Flower validation config does not match manifest pin")
    if str(manifest.get("dp-track", "")).lower() == "association":
        required = (
            "dp-track", "num-server-rounds", "association-contract",
            "association-contract-sha256", "association-job-sha256",
            "association-n-nodes", "association-privacy-unit",
            "association-unit-semantics",
        )
        supplied = {
            key for key in cfg
            if str(key).lower().startswith(("association-", "association_"))}
        if supplied != set(required) - {"dp-track", "num-server-rounds"}:
            raise ValueError(
                "Flower association config has an unexpected public field")
        for key in required:
            if key not in manifest:
                raise ValueError("manifest is missing association pin '%s'" % key)
            if key not in cfg or type(cfg[key]) is not type(manifest[key]) or \
                    cfg[key] != manifest[key]:
                raise ValueError("Flower association config does not match manifest pin")
    # The analyst-facing Flower config may name a module for the researcher-side
    # ServerApp, but node execution accepts only the package name derived and
    # written by flowerTier2PinDS after installation/hash verification.
    cfg.pop("user-module", None)
    keys = (
        "model-spec-b64", "loss-name", "num-classes", "num-labels",
        "local-epochs", "batch-size", "num-server-rounds", "num-features",
        "feature-bounds", "backbone", "image-size", "user-module",
        "task-type", "app-params-b64", "app-params-sha256",
        "target-bounds", "target-levels", "validation-model-track",
        "validation-task", "validation-bins", "validation-contract-sha256",
        "validation-native-tree-request-b64",
        "validation-native-tree-request-sha256",
        "validation-artifact-format", "validation-artifact-sha256",
        "validation-artifact-size-bytes",
        "validation-profile-sha256", "validation-profile-size-bytes",
        "validation-public-schema-sha256",
        "association-contract", "association-contract-sha256",
        "association-job-sha256", "association-n-nodes",
        "association-privacy-unit", "association-unit-semantics",
        "learning-rate", "weight-decay", "l1-penalty",
        "optimizer-name", "optimizer-momentum", "optimizer-nesterov",
        "optimizer-beta1", "optimizer-beta2", "optimizer-eps",
        "optimizer-amsgrad", "optimizer-rmsprop-alpha",
        "scheduler-name", "scheduler-step-size", "scheduler-gamma",
        "scheduler-min-lr",
        "nb-dispersion", "gamma-shape", "huber-delta", "quantile-level",
        "resampling-version", "resampling-method", "resampling-assignment",
        "resampling-test-numerator", "resampling-test-denominator",
        "resampling-privacy-unit", "resampling-unit-canonicalization",
        "resampling-contract-sha256", "holdout-validation-bins",
        "cv-version", "cv-method", "cv-assignment", "cv-folds",
        "cv-privacy-unit", "cv-unit-canonicalization",
        "cv-contract-sha256", "cv-validation-bins", "cv-n-nodes",
        "cv-job-sha256", "strategy", "strategy-eta", "strategy-eta-l",
        "strategy-beta-1", "strategy-beta-2", "strategy-tau",
        "strategy-server-learning-rate", "strategy-server-momentum",
    )
    for key in keys:
        if key in manifest:
            cfg[key] = manifest[key]
    return cfg


def load_tabular_patient_ids(context=None):
    """Patient/subject ids so tabular neural training uses patient-level DP."""
    manifest = _load_manifest(context)
    if manifest.get("data_type") == "image":
        return None
    manifest_dir = _get_manifest_dir(context)
    data_file = os.path.join(manifest_dir, manifest["data_file"])
    df = _read_staged_frame(data_file, manifest)
    return _load_patient_ids(df, manifest)
