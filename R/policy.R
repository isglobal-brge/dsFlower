# Module: Disclosure Controls (Policy)
# Statistical disclosure control for DataSHIELD compliance in Flower FL.
#
# All thresholds and permissions are read from server-side R options,
# following the DataSHIELD convention of double-fallback:
#   getOption("dsflower.X", getOption("default.dsflower.X", hardcoded_default))

# --- Template -> framework map ---
# Maps a model/template name to the Python framework whose venv runs it; used
# only to resolve the SuperNode venv when a run passes a template name (Tier-2
# and no-template runs default to the pytorch venv via .resolve_framework_runtime).
.TEMPLATE_METADATA <- list(
  sklearn_logreg            = list(framework = "sklearn",        requires_secagg = FALSE),
  sklearn_ridge             = list(framework = "sklearn",        requires_secagg = FALSE),
  sklearn_sgd               = list(framework = "sklearn",        requires_secagg = FALSE),
  pytorch_mlp               = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_logreg            = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_linear_regression = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_coxph             = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_poisson           = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_lognormal_aft         = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_cause_specific_cox    = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_multilabel        = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_multiclass        = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_resnet18          = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_densenet121       = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_unet2d            = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_tcn               = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_lstm              = list(framework = "pytorch",        requires_secagg = FALSE)
)

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
  )
)


#' Validate target distribution against trust profile thresholds
#'
#' Uses the explicit task type when provided and otherwise infers a conservative
#' classification/survival interpretation from the target column. Validates:
#' - Binary (2 unique values): min(table(target)) >= .disclosure_min_cell()
#' - Multiclass (>2 unique): all(table(target) >= .disclosure_min_cell())
#' - Survival (2-column target or event/status column): sum(events==1) >= .disclosure_min_cell()
#' - Regression/count targets: no class-count check is applied.
#'
#' Error messages are deliberately generic to avoid leaking counts.
#'
#' @param data Data.frame or Arrow Table; the training data.
#' @param target_column Character; name(s) of the target column(s).
#' @param task_type Character task type ("classification", "regression",
#'   "survival", etc.) or NULL.
#' @return TRUE invisibly, or stops with an error.
#' @keywords internal
.validateClassDistribution <- function(data, target_column,
                                       task_type = NULL) {
  if (is.null(target_column) || length(target_column) == 0) {
    return(invisible(TRUE))
  }
  task_type <- tolower(as.character(task_type %||% ""))
  if (task_type %in% c("regression", "count", "continuous")) {
    return(invisible(TRUE))
  }

  # Survival detection: 2-column target or column named "event"/"status"
  is_survival <- identical(task_type, "survival")
  if (length(target_column) == 2) {
    is_survival <- TRUE
    # Look for the event/status column (not the time column)
    event_col <- NULL
    for (tc in target_column) {
      if (tc %in% names(data) &&
          tolower(tc) %in% c("event", "status", "dead", "died",
                              "censored", "censor")) {
        event_col <- tc
        break
      }
    }
    if (is.null(event_col)) {
      # Default: second column is the event indicator
      event_col <- target_column[2]
    }
    if (event_col %in% names(data)) {
      events <- data[[event_col]]
      n_events <- sum(events == 1, na.rm = TRUE)
      if (n_events < .disclosure_min_cell()) {
        stop("Disclosive: operation blocked -- insufficient event counts to ",
             "meet disclosure threshold. No further details available.",
             call. = FALSE)
      }
    }
    return(invisible(TRUE))
  }

  # Single target column
  tc <- target_column[1]
  if (!tc %in% names(data)) return(invisible(TRUE))

  target_vals <- data[[tc]]
  unique_vals <- unique(target_vals[!is.na(target_vals)])
  n_unique <- length(unique_vals)

  if (n_unique <= 1) {
    return(invisible(TRUE))
  }

  # Check for survival-like single column (named event/status)
  if (identical(task_type, "survival") ||
      tolower(tc) %in% c("event", "status", "dead", "died")) {
    n_events <- sum(target_vals == 1, na.rm = TRUE)
    if (n_events < .disclosure_min_cell()) {
      stop("Disclosive: operation blocked -- insufficient event counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
    return(invisible(TRUE))
  }

  if (n_unique == 2) {
    # Binary classification
    counts <- table(target_vals)
    if (min(counts) < .disclosure_min_cell()) {
      stop("Disclosive: operation blocked -- insufficient class counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
  } else {
    # Multiclass
    counts <- table(target_vals)
    if (any(counts < .disclosure_min_cell())) {
      stop("Disclosive: operation blocked -- insufficient class counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
  }

  invisible(TRUE)
}

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
    allow_custom_config = FALSE,
    allow_custom_config_deprecated = TRUE,
    # Retained as an empty compatibility field. Model flexibility is expressed
    # through the declarative runner vocabulary, not legacy executable template
    # names.
    allowed_templates = character(),
    allowed_templates_deprecated = TRUE,
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
#' The adjacency relation is a lifetime policy, not a per-run heuristic.
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
    stop("Prepared DP unit disagrees with the server lifetime policy.",
         call. = FALSE)
  }
  if (identical(policy$dp_unit, "row")) return(NULL)

  explicit <- run_config[["patient_column"]] %||% policy$patient_column
  explicit <- as.character(unlist(explicit, use.names = FALSE))
  if (length(explicit) != 1L || !identical(explicit, policy$patient_column)) {
    stop("Prepared patient column disagrees with the server lifetime policy.",
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

#' Legacy reduction of a training table to distinct patients
#'
#' Retained for compatibility with older internal extensions. Current run
#' admission uses the server-authored \code{n_units} structural manifest field
#' and deliberately does not inspect labels: a class/event threshold would make
#' prepare success or failure a label-dependent transcript oracle.
#'
#' The canonical runner uses the same server-pinned patient column and collapses
#' all of a patient's rows/images to one training example before DP-SGD. The
#' formal privacy unit therefore matches this admission unit. Patient mode
#' requires a stable, complete roster and never falls back to rows or images.
#'
#' @param samples Data.frame; training rows / image samples metadata.
#' @param target_column Character; label column name(s).
#' @param run_config Named list; trusted server-authored run configuration.
#' @return list(n_patients, data) or NULL.
#' @keywords internal
.privacyDisclosureUnits <- function(samples, target_column, run_config = list()) {
  if (is.null(samples) || !is.data.frame(samples) || !nrow(samples)) return(NULL)
  pcol <- .detectPatientColumn(samples, run_config)
  if (is.null(pcol)) return(NULL)

  pid <- .canonicalPatientIds(samples[[pcol]])
  n_patients <- length(unique(pid))

  tc <- if (!is.null(target_column) && length(target_column) >= 1 &&
            target_column[[1]] %in% names(samples)) target_column[[1]] else NULL
  if (is.null(tc)) {
    keep <- !duplicated(pid)
    return(list(n_patients = n_patients, data = samples[keep, , drop = FALSE]))
  }
  # Exactly one row per patient.  A patient with inconsistent labels must not
  # count once in every class; choose the deterministic mode (lexical first on
  # ties), matching the runner's one-unit pooling contract.
  selected <- vapply(unique(pid), function(id) {
    idx <- which(pid == id)
    labels <- samples[[tc]][idx]
    usable <- !is.na(labels)
    if (!any(usable)) return(idx[[1]])
    counts <- table(labels[usable])
    winners <- names(counts)[counts == max(counts)]
    winner <- if (is.numeric(labels) || is.integer(labels)) {
      as.character(min(as.numeric(winners)))
    } else {
      sort(winners, method = "radix")[[1]]
    }
    idx[which(as.character(labels) == winner)[[1]]]
  }, integer(1))
  list(n_patients = n_patients, data = samples[selected, , drop = FALSE])
}

# Backward-compatible internal name used by older tests/extensions.
.imageDisclosureUnits <- function(samples, target_column, run_config = list()) {
  .privacyDisclosureUnits(samples, target_column, run_config)
}

#' Sanitize training metrics before returning through DataSHIELD
#'
#' Only round-level aggregate metrics (loss, accuracy, F1, precision,
#' recall, num_examples) are returned. Per-sample metrics, raw gradients,
#' and any other potentially disclosive information is stripped.
#'
#' @param metrics Named list or data.frame of metrics from Flower
#' @return Data frame with only safe columns, or empty data.frame
#' @keywords internal
.sanitizeMetrics <- function(metrics) {
  if (is.null(metrics) || length(metrics) == 0) {
    return(data.frame(
      round = integer(0), metric = character(0),
      value = numeric(0), stringsAsFactors = FALSE
    ))
  }

  # Allowlisted metric names
  safe_metrics <- c(
    "loss", "accuracy", "f1", "f1_score",
    "precision", "recall", "auc", "roc_auc",
    "mse", "mae", "rmse", "r2",
    "num_examples", "num_clients"
  )

  if (is.data.frame(metrics)) {
    # Filter to safe columns
    if ("metric" %in% names(metrics)) {
      metrics <- metrics[tolower(metrics$metric) %in% safe_metrics, , drop = FALSE]
    }
    # Strip any path or IP columns
    unsafe_cols <- grep("path|ip|host|pid|dir", names(metrics),
                        ignore.case = TRUE, value = TRUE)
    metrics <- metrics[, !names(metrics) %in% unsafe_cols, drop = FALSE]
    # Bucket count-bearing metrics (num_examples, num_clients)
    if ("metric" %in% names(metrics) && "value" %in% names(metrics)) {
      count_rows <- tolower(metrics$metric) %in% c("num_examples", "num_clients")
      if (any(count_rows)) {
        metrics$value[count_rows] <- vapply(
          metrics$value[count_rows],
          function(v) as.numeric(dsImaging::safe_metadata_count(as.integer(v))),
          numeric(1))
      }
    }
    rownames(metrics) <- NULL
    return(metrics)
  }

  # Convert named list to data.frame
  if (is.list(metrics)) {
    rows <- list()
    for (nm in names(metrics)) {
      if (tolower(nm) %in% safe_metrics) {
        val <- metrics[[nm]]
        if (is.numeric(val)) {
          # Bucket count-bearing metrics
          if (tolower(nm) %in% c("num_examples", "num_clients")) {
            val <- as.numeric(dsImaging::safe_metadata_count(as.integer(val)))
          }
          rows[[length(rows) + 1]] <- data.frame(
            metric = nm, value = val, stringsAsFactors = FALSE
          )
        }
      }
    }
    if (length(rows) > 0) {
      return(do.call(rbind, rows))
    }
  }

  data.frame(
    metric = character(0), value = numeric(0),
    stringsAsFactors = FALSE
  )
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

#' Sanitize log lines before returning through DataSHIELD
#'
#' Strips filesystem paths, IP addresses, and other potentially
#' identifying information from Flower log output.
#'
#' @param lines Character vector of log lines
#' @param last_n Integer; maximum number of lines to return (default 50)
#' @return Character vector of sanitized log lines
#' @keywords internal
.sanitizeLogs <- function(lines, last_n = 50L) {
  if (is.null(lines) || length(lines) == 0) return(character(0))

  last_n <- min(as.integer(last_n), 200L)
  if (length(lines) > last_n) {
    lines <- utils::tail(lines, last_n)
  }

  # Strip filesystem paths (Unix and Windows)
  lines <- gsub("/[a-zA-Z0-9_./-]{3,}", "<path>", lines)
  lines <- gsub("[A-Z]:\\\\[a-zA-Z0-9_.\\\\ -]{3,}", "<path>", lines)

  # Strip IP addresses (IPv4)
  lines <- gsub("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b", "<ip>", lines)

  # Strip port patterns (host:port)
  lines <- gsub("<ip>:\\d+", "<ip>:<port>", lines)

  # Strip PID references
  lines <- gsub("\\bpid[= ]+\\d+", "pid=<pid>", lines, ignore.case = TRUE)

  lines
}

#' Reject retired named-template inputs
#'
#' Models are data-only declarative specifications in the current runner. Named
#' executable templates are retained only as a legacy argument shape and are not
#' an authorization mechanism.
#'
#' @param app_template Character; the template name to validate
#' @return TRUE invisibly, or stops with an error
#' @keywords internal
.validateTemplate <- function(app_template) {
  stop("Named executable templates are retired. Submit a declarative model ",
       "specification through the hash-pinned runner instead.", call. = FALSE)
}
