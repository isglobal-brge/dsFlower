# Module: Disclosure Controls (Policy)
# Statistical disclosure control for DataSHIELD compliance in Flower FL.
#
# All thresholds and permissions are read from server-side R options,
# following the DataSHIELD convention of double-fallback:
#   getOption("dsflower.X", getOption("default.dsflower.X", hardcoded_default))

# --- Template Family Matrix ---
# Maps each template to a family, and each family to per-profile minimum
# row counts. NA means the template is NOT supported under that profile.

.TEMPLATE_FAMILIES <- list(
  sklearn_logreg             = "closed_form_linear",
  sklearn_ridge              = "closed_form_linear",
  sklearn_sgd                = "iterative_linear",
  pytorch_logreg             = "iterative_linear",
  pytorch_linear_regression  = "iterative_linear",
  pytorch_multiclass         = "iterative_linear",
  pytorch_coxph              = "iterative_linear",
  pytorch_poisson            = "iterative_linear",
  pytorch_lognormal_aft          = "iterative_linear",
  pytorch_cause_specific_cox     = "iterative_linear",
  pytorch_multilabel         = "tabular_deep",
  pytorch_mlp                = "tabular_deep",
  pytorch_tcn                = "tabular_deep",
  pytorch_lstm               = "tabular_deep",
  pytorch_resnet18           = "vision",
  pytorch_densenet121        = "vision",
  pytorch_unet2d             = "segmentation",
  xgboost                    = "xgboost_histogram"
)

# --- Template Metadata ---
# Per-template metadata that does NOT vary by profile.
# SecAgg requirements are profile-driven: XGBoost can run in sandbox/trusted
# validation profiles without SecAgg, but clinical/consortium profiles still
# require server-side Secure Aggregation through their trust profile.
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

# --- Hyperparameter schemas per template ---
# Server-side validation: bounds, types, allowed values.
# Rejects bad params before staging. Does NOT replace Python-side defaults.
.TEMPLATE_PARAM_SCHEMA <- list(
  # Shared bounds for all sklearn templates
  .sklearn_common = list(
    alpha       = list(type = "numeric", min = 1e-10, max = 100),
    C           = list(type = "numeric", min = 1e-10, max = 1000),
    eta0        = list(type = "numeric", min = 1e-8,  max = 10),
    max_iter    = list(type = "integer", min = 1,     max = 100000),
    l1_ratio    = list(type = "numeric", min = 0,     max = 1)
  ),
  # Shared bounds for all pytorch templates
  .pytorch_common = list(
    learning_rate = list(type = "numeric", min = 1e-8,  max = 1.0),
    batch_size    = list(type = "integer", min = 1,     max = 100000),
    local_epochs  = list(type = "integer", min = 1,     max = 1000)
  ),
  # Template-specific overrides/additions
  pytorch_mlp = list(
    hidden_layers = list(type = "string", max_length = 100)
  ),
  pytorch_multiclass = list(
    n_classes     = list(type = "integer", min = 2,   max = 10000),
    hidden_layers = list(type = "string", max_length = 100)
  ),
  pytorch_multilabel = list(
    n_labels      = list(type = "integer", min = 1,   max = 10000),
    hidden_layers = list(type = "string", max_length = 100)
  ),
  pytorch_resnet18 = list(
    n_classes     = list(type = "integer", min = 2,   max = 10000),
    image_size    = list(type = "integer", min = 16,  max = 4096)
  ),
  pytorch_densenet121 = list(
    n_classes     = list(type = "integer", min = 2,   max = 10000),
    image_size    = list(type = "integer", min = 16,  max = 4096)
  ),
  pytorch_unet2d = list(
    n_classes     = list(type = "integer", min = 1,   max = 100),
    image_size    = list(type = "integer", min = 16,  max = 4096),
    base_channels = list(type = "integer", min = 1,   max = 512)
  ),
  pytorch_lstm = list(
    hidden_size   = list(type = "integer", min = 1,   max = 4096),
    num_layers    = list(type = "integer", min = 1,   max = 20)
  ),
  pytorch_tcn = list(
    n_channels    = list(type = "integer", min = 1,   max = 100),
    kernel_size   = list(type = "integer", min = 1,   max = 100),
    n_layers      = list(type = "integer", min = 1,   max = 50)
  ),
  pytorch_cause_specific_cox = list(
    n_causes      = list(type = "integer", min = 2,   max = 100)
  ),
  xgboost = list(
    n_trees       = list(type = "integer", min = 1,   max = 1000),
    max_depth     = list(type = "integer", min = 1,   max = 15),
    n_bins        = list(type = "integer", min = 2,   max = 1024),
    eta           = list(type = "numeric", min = 1e-6, max = 1.0),
    reg_lambda    = list(type = "numeric", min = 0,   max = 1000)
  )
)

#' Validate hyperparameters against template schema
#'
#' Checks that all hyperparameters in run_config fall within the
#' server-defined bounds for the given template. Rejects out-of-range
#' or wrong-type values before staging.
#'
#' @param template_name Character; the template name.
#' @param run_config Named list; the run configuration.
#' @return Invisible TRUE, or stops with a descriptive error.
#' @keywords internal
.validateTemplateHyperparameters <- function(template_name, run_config) {
  # Get the framework for common bounds
  meta <- .TEMPLATE_METADATA[[template_name]]
  if (is.null(meta)) return(invisible(TRUE))

  framework <- meta$framework
  common_key <- if (grepl("sklearn", framework)) ".sklearn_common"
                else if (grepl("pytorch", framework)) ".pytorch_common"
                else NULL

  # Merge common + template-specific schemas
  schema <- list()
  if (!is.null(common_key) && !is.null(.TEMPLATE_PARAM_SCHEMA[[common_key]]))
    schema <- .TEMPLATE_PARAM_SCHEMA[[common_key]]
  template_schema <- .TEMPLATE_PARAM_SCHEMA[[template_name]]
  if (!is.null(template_schema))
    schema <- c(schema, template_schema)

  if (length(schema) == 0) return(invisible(TRUE))

  # Validate each param that appears in run_config
  for (param_name in names(schema)) {
    spec <- schema[[param_name]]
    val <- run_config[[param_name]]
    if (is.null(val)) next  # not provided, use default

    # Type check
    if (identical(spec$type, "integer")) {
      val <- suppressWarnings(as.integer(val))
      if (is.na(val))
        stop("Parameter '", param_name, "' must be an integer for template '",
             template_name, "'.", call. = FALSE)
    } else if (identical(spec$type, "numeric")) {
      val <- suppressWarnings(as.numeric(val))
      if (is.na(val))
        stop("Parameter '", param_name, "' must be numeric for template '",
             template_name, "'.", call. = FALSE)
    } else if (identical(spec$type, "string")) {
      val <- as.character(val)
    }

    # Range check
    if (!is.null(spec$min) && is.numeric(val) && val < spec$min)
      stop("Parameter '", param_name, "' = ", val, " is below minimum (",
           spec$min, ") for template '", template_name, "'.", call. = FALSE)
    if (!is.null(spec$max) && is.numeric(val) && val > spec$max)
      stop("Parameter '", param_name, "' = ", val, " exceeds maximum (",
           spec$max, ") for template '", template_name, "'.", call. = FALSE)

    # String length check
    if (!is.null(spec$max_length) && is.character(val) && nchar(val) > spec$max_length)
      stop("Parameter '", param_name, "' string too long (max ",
           spec$max_length, " chars).", call. = FALSE)
  }
  invisible(TRUE)
}

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

