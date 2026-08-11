# Module: Disclosure Controls (Policy)
# Statistical disclosure control for DataSHIELD compliance in Flower FL.
#
# All thresholds and permissions are read from server-side R options,
# following the DataSHIELD convention of double-fallback:
#   getOption("dsflower.X", getOption("default.dsflower.X", hardcoded_default))

# Public vocabulary implemented by runner ABI 3.  These are capabilities of the
# node-resident, hash-pinned executor; they are not names of executable app
# templates. Keep this list in lockstep with model_spec.py, dp_harness.py, and
# server_app.py when the runner ABI changes.
.RUNNER_PUBLIC_CAPABILITIES <- list(
  dp_tracks = c("neural", "egress", "validation"),
  declarative_model_ops = list(
    layers = c(
      "linear", "relu", "gelu", "tanh", "sigmoid", "elu", "silu",
      "leaky_relu", "dropout", "layernorm", "softmax", "reshape",
      "flatten", "conv1d", "conv2d", "maxpool2d", "adaptiveavgpool2d",
      "upsample", "lstm", "gru"
    ),
    graph = c("add", "mul", "sub", "div", "affine", "concat", "matmul",
              "transpose")
  ),
  declarative_losses = c(
    "bce_logits", "cross_entropy", "mse", "poisson_nll",
    "multilabel_bce", "hinge", "ordinal", "negbin_nll", "gamma_nll",
    "huber", "quantile"
  ),
  aggregation_strategies = c(
    "fedavg", "fedadam", "fedadagrad", "fedyogi", "fedavgm"
  ),
  resampling = list(
    holdout = list(
      available = TRUE,
      tracks = "neural",
      data_kinds = "tabular",
      assignment = "hmac-sha256-threshold-v1",
      pooled_only = TRUE),
    cross_validation = list(
      available = TRUE,
      tracks = "neural",
      data_kinds = "tabular",
      folds = c(2L, 10L),
      assignment = "hmac-sha256-score-v1",
      pooled_only = TRUE)
  )
)


#' Bucket a count to prevent exact sample sizes from leaking
#'
#' Delegates to \code{dsImaging::safe_metadata_count()} for consistent
#' profile-aware bucketing across all DS packages. Falls back to local
#' power-of-two bucketing if dsImaging is unavailable.
#'
#' @param n Integer; the exact count.
#' @return Integer; the bucketed count.
#' @keywords internal
.bucket_count <- function(n) {
  if (requireNamespace("dsImaging", quietly = TRUE)) {
    return(dsImaging::safe_metadata_count(as.integer(n)))
  }
  # Fallback: local power-of-two bucketing
  n <- as.integer(n)
  if (is.na(n) || n <= 0) return(0L)
  # Suppress small counts: returning 1/2/3 exactly discloses a near-empty
  # stratum. Counts at or below the DataSHIELD subset filter report as 0.
  thr <- as.integer(getOption("nfilter.subset",
                              getOption("default.nfilter.subset", 3)))
  if (n <= thr) return(0L)
  as.integer(2^round(log2(n)))
}

#' Read all disclosure settings from DataSHIELD server options
#'
#' Returns a named list of all disclosure thresholds and server-gated
#' permissions. Every setting follows the standard DataSHIELD option chain:
#' direct option -> \code{default.*} prefix -> hardcoded fallback.
#'
#' @return Named list of disclosure thresholds and permissions
#' @keywords internal
.flowerDisclosureSettings <- function() {
  list(
    # --- Standard DataSHIELD thresholds (inherited, not redefined) ---
    nfilter_subset = as.numeric(getOption("nfilter.subset",
                        getOption("default.nfilter.subset", 3))),
    nfilter_tab = as.numeric(getOption("nfilter.tab",
                        getOption("default.nfilter.tab", 3))),
    nfilter_levels_max = as.numeric(getOption("nfilter.levels.max",
                        getOption("default.nfilter.levels.max", 40))),
    # --- dsFlower-specific settings ---
    max_rounds = as.numeric(.dsf_option("max_rounds", 500)),
    allow_supernode_spawn = as.logical(.dsf_option("allow_supernode_spawn", TRUE)),
    max_concurrent_runs = as.numeric(.dsf_option("max_concurrent_runs", Inf))
  )
}

#' Minimum cell count for any returned class/event/stratum count.
#'
#' Small-cell rule: counts at or below this are disclosive. Inherits the
#' standard DataSHIELD table filter (\code{nfilter.tab}, default 3); a server
#' admin may raise it via \code{dsflower.min_cell_count}.
#' @keywords internal
.disclosure_min_cell <- function() {
  base <- as.integer(getOption("nfilter.tab", getOption("default.nfilter.tab", 3)))
  ov <- suppressWarnings(as.integer(.dsf_option("min_cell_count", NA)))
  max(base, if (is.na(ov)) base else ov, na.rm = TRUE)
}

#' Minimum training rows to allow a run.
#'
#' Floors at the DataSHIELD subset filter (\code{nfilter.subset}, default 3); a
#' server admin may raise it via \code{dsflower.min_train_rows} (e.g. for deep /
#' vision models that need far more data for a meaningful DP guarantee).
#' @keywords internal
.disclosure_min_rows <- function() {
  base <- as.integer(getOption("nfilter.subset",
                               getOption("default.nfilter.subset", 3)))
  ov <- suppressWarnings(as.integer(.dsf_option("min_train_rows", NA)))
  max(base, if (is.na(ov)) base else ov, na.rm = TRUE)
}

#' Assert minimum training samples
#'
#' Prevents training on datasets too small to provide meaningful privacy
#' guarantees. The error message is deliberately generic to avoid leaking
#' the actual sample count.
#'
#' @param n_samples Numeric; number of training samples
#' @return TRUE invisibly, or stops with an error
#' @keywords internal
.assertMinSamples <- function(n_samples, min_n = NULL) {
  threshold <- if (!is.null(min_n)) min_n else .disclosure_min_rows()

  n <- suppressWarnings(as.numeric(unlist(n_samples, use.names = FALSE)))
  threshold <- suppressWarnings(as.numeric(unlist(threshold, use.names = FALSE)))
  if (length(n) != 1L || length(threshold) != 1L ||
      !is.finite(n) || !is.finite(threshold) ||
      n < 0 || threshold < 0 || n < threshold) {
    stop(
      "Disclosive: operation blocked -- insufficient training samples to ",
      "meet disclosure threshold. No further details available.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Resolve the server-owned differential-privacy unit
#'
#' The adjacency relation is a server-owned policy, not an analyst heuristic.
#' \code{dsflower.dp_unit} defaults to \code{"row"}.  Selecting
#' \code{"patient"} requires one explicit, stable
#' \code{dsflower.patient_column}; no column-name auto-detection or row fallback
#' is allowed.
#'
#' @return A named list with \code{dp_unit}, \code{patient_column}, and the
#'   identifier canonicalisation version.
#' @keywords internal
.dpUnitPolicy <- function() {
  unit <- tolower(trimws(as.character(.dsf_option("dp_unit", "row"))))
  if (length(unit) != 1L || is.na(unit) || !unit %in% c("row", "patient")) {
    stop("dsflower.dp_unit must be exactly 'row' or 'patient'.", call. = FALSE)
  }

  patient_column <- NULL
  if (identical(unit, "patient")) {
    configured <- .dsf_option("patient_column", NA_character_)
    configured <- as.character(unlist(configured, use.names = FALSE))
    if (length(configured) != 1L || is.na(configured) ||
        !nzchar(trimws(configured))) {
      stop("dsflower.patient_column must name one column when ",
           "dsflower.dp_unit='patient'.", call. = FALSE)
    }
    patient_column <- trimws(configured)
  }

  list(
    dp_unit = unit,
    patient_column = patient_column,
    canonicalization = "trim-utf8-v2"
  )
}

#' Resolve a server-owned patient / subject identifier column
#'
#' This helper applies \code{.dpUnitPolicy()} to a concrete frame.  In row mode
#' it always returns \code{NULL}. In patient mode the configured column must be
#' present; missing columns fail closed rather than silently changing adjacency.
#'
#' @param df Data.frame; the samples / training metadata table.
#' @param run_config Named list; trusted server-authored run configuration.
#' @return Character column name, or NULL in row mode.
#' @keywords internal
.detectPatientColumn <- function(df, run_config = list()) {
  policy <- .dpUnitPolicy()
  pinned_unit <- run_config[["dp-unit"]] %||% policy$dp_unit
  pinned_unit <- tolower(as.character(unlist(pinned_unit, use.names = FALSE)))
  if (length(pinned_unit) != 1L || !identical(pinned_unit, policy$dp_unit)) {
    stop("Prepared DP unit disagrees with the server policy.",
         call. = FALSE)
  }
  if (identical(policy$dp_unit, "row")) return(NULL)

  explicit <- run_config[["patient_column"]] %||% policy$patient_column
  explicit <- as.character(unlist(explicit, use.names = FALSE))
  if (length(explicit) != 1L || !identical(explicit, policy$patient_column)) {
    stop("Prepared patient column disagrees with the server policy.",
         call. = FALSE)
  }
  if (is.null(df) || !is.data.frame(df) || !explicit %in% names(df)) {
    stop("The server-configured patient identifier column is unavailable.",
         call. = FALSE)
  }
  explicit
}

#' Canonicalise patient identifiers with the v2 cross-language contract
#'
#' Conversion is strict UTF-8 followed by trimming only ASCII space, tab, CR,
#' and LF.  The exact trim set is intentionally narrower than R's
#' \code{trimws()} and Python's \code{str.strip()} so both runtimes agree for
#' every Unicode string. Invalid encodings become missing record values.
#' @keywords internal
.canonicalPatientIdText <- function(values) {
  value_count <- length(values)
  text <- tryCatch(
    as.character(values),
    error = function(e) rep(NA_character_, value_count)
  )
  text <- tryCatch(
    suppressWarnings(iconv(text, from = "", to = "UTF-8", sub = NA)),
    error = function(e) rep(NA_character_, length(text))
  )
  gsub("^[ \\t\\r\\n]+|[ \\t\\r\\n]+$", "", text, perl = TRUE)
}

#' Identify values outside the v2 patient identifier domain
#' @keywords internal
.invalidPatientIds <- function(ids) {
  is.na(ids) | !nzchar(ids) |
    tolower(ids) %in% c("na", "nan", "null", "<na>", "nat")
}

#' Canonicalise and validate a complete patient roster
#' @keywords internal
.canonicalPatientIds <- function(values) {
  ids <- .canonicalPatientIdText(values)
  if (any(.invalidPatientIds(ids))) {
    stop("The configured patient identifier must be complete and non-empty.",
         call. = FALSE)
  }
  ids
}

#' Validate training rounds against maximum allowed
#'
#' @param num_rounds Integer; requested number of training rounds
#' @return The validated num_rounds, or stops with an error
#' @keywords internal
.validateMaxRounds <- function(num_rounds) {
  settings <- .flowerDisclosureSettings()
  max_rounds <- settings$max_rounds
  num_rounds <- as.integer(num_rounds)

  if (is.na(num_rounds) || num_rounds < 1) {
    stop("num_rounds must be a positive integer.", call. = FALSE)
  }

  if (num_rounds > max_rounds) {
    stop(
      "Requested rounds (", num_rounds, ") exceeds server maximum (",
      max_rounds, "). Contact your server administrator to increase ",
      "dsflower.max_rounds if needed.",
      call. = FALSE
    )
  }
  num_rounds
}
