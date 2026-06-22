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
  pytorch_lstm              = list(framework = "pytorch",        requires_secagg = FALSE),
  xgboost                   = list(framework = "xgboost",       requires_secagg = FALSE)
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
#' @param trust Named list; the trust profile settings.
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
    allow_custom_config = as.logical(.dsf_option("allow_custom_config", FALSE)),
    allowed_templates = c("sklearn_logreg", "sklearn_ridge",
                          "sklearn_sgd", "pytorch_mlp",
                          "pytorch_logreg", "pytorch_linear_regression",
                          "pytorch_coxph", "pytorch_multiclass",
                          "pytorch_poisson", "pytorch_multilabel",
                          "pytorch_lognormal_aft", "pytorch_cause_specific_cox",
                          "pytorch_resnet18",
                          "pytorch_densenet121", "pytorch_unet2d",
                          "pytorch_tcn", "pytorch_lstm",
                          "xgboost"),
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

  n <- as.numeric(n_samples)
  if (is.na(n) || n < threshold) {
    stop(
      "Disclosive: operation blocked -- insufficient training samples to ",
      "meet disclosure threshold. No further details available.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Auto-detect a patient / subject identifier column for imaging disclosure
#'
#' Image collections hold one row per IMAGE, but several images can belong to the
#' same patient. The right disclosure unit for medical imaging is the PATIENT, so
#' we group by a patient identifier when one is present. Detection is
#' case-insensitive over common identifiers; a server admin may pin an exact
#' column with \code{dsflower.patient_column} (or the run_config
#' \code{patient_column}/\code{group_column}).
#'
#' @param df Data.frame; the samples / training metadata table.
#' @param run_config Named list; the run configuration (optional override).
#' @return Character column name, or NULL when no patient column is found.
#' @keywords internal
.detectPatientColumn <- function(df, run_config = list()) {
  if (is.null(df) || !is.data.frame(df) || !ncol(df)) return(NULL)
  explicit <- run_config[["patient_column"]] %||% run_config[["group_column"]] %||%
    .dsf_option("patient_column", NA)
  if (!is.null(explicit) && !is.na(explicit) && nzchar(as.character(explicit)) &&
      as.character(explicit) %in% names(df)) {
    return(as.character(explicit))
  }
  candidates <- c("patient_id", "patientid", "patient", "subject_id",
                  "subjectid", "subject", "participant_id", "case_id",
                  "person_id", "pid", "mrn")
  lc <- tolower(names(df))
  for (cand in candidates) {
    hit <- which(lc == cand)
    if (length(hit)) return(names(df)[hit[1]])
  }
  NULL
}

#' Reduce an image samples table to its disclosure UNIT (distinct patients)
#'
#' When a patient column exists, the admission checks should count distinct
#' PATIENTS (a patient with many slices counts once) for both the minimum
#' collection size and the minimum per-class count. Returns the distinct-patient
#' count plus a frame with one row per (patient, label) so the existing
#' class-distribution check counts patients-per-class. Returns NULL when no
#' patient column is found, so the caller falls back to per-image counts.
#'
#' NOTE: this groups the DISCLOSURE/ADMISSION unit by patient. The DP-SGD noise
#' in the trusted harness is applied per-IMAGE example, so the formal DP unit
#' remains the image; a patient contributing k images has a per-patient guarantee
#' weaker by up to a factor k (group privacy). This is documented in ARCHITECTURE.
#'
#' @param samples Data.frame; the image samples metadata table.
#' @param target_column Character; label column name(s).
#' @param run_config Named list; run configuration (for the column override).
#' @return list(n_patients, data) or NULL.
#' @keywords internal
.imageDisclosureUnits <- function(samples, target_column, run_config = list()) {
  if (is.null(samples) || !is.data.frame(samples) || !nrow(samples)) return(NULL)
  pcol <- .detectPatientColumn(samples, run_config)
  if (is.null(pcol)) return(NULL)

  pid <- as.character(samples[[pcol]])
  # NA / empty patient ids are NOT patients: exclude them from the distinct-patient
  # count AND from the per-class frame, else `paste(NA, label)` yields a unique key
  # ("NA\rlabel") that survives dedup and inflates a class count, letting a class
  # with too few real patients pass the min-per-class check.
  valid <- !is.na(pid) & nzchar(pid)
  n_patients <- length(unique(pid[valid]))

  tc <- if (!is.null(target_column) && length(target_column) >= 1 &&
            target_column[[1]] %in% names(samples)) target_column[[1]] else NULL
  if (is.null(tc)) {
    keep <- valid & !duplicated(pid)
    return(list(n_patients = n_patients, data = samples[keep, , drop = FALSE]))
  }
  # One row per (patient, label): class-distribution then counts DISTINCT
  # patients per class (a patient with many same-label slices counts once).
  key <- paste(pid, as.character(samples[[tc]]), sep = "\r")
  keep <- valid & !duplicated(key)
  list(n_patients = n_patients, data = samples[keep, , drop = FALSE])
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

#' Validate that a template is in the allowed list
#'
#' When \code{dsflower.allow_custom_config} is FALSE (default), only
#' pre-approved app templates bundled with dsFlower can be used.
#'
#' @param app_template Character; the template name to validate
#' @return TRUE invisibly, or stops with an error
#' @keywords internal
.validateTemplate <- function(app_template) {
  settings <- .flowerDisclosureSettings()

  if (!settings$allow_custom_config &&
      !app_template %in% settings$allowed_templates) {
    stop(
      "Template '", app_template, "' is not in the approved list. ",
      "Allowed templates: ",
      paste(settings$allowed_templates, collapse = ", "), ". ",
      "Contact your server administrator to enable custom configs ",
      "(dsflower.allow_custom_config = TRUE).",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

