# Module: Disclosure Controls (Policy)
# Statistical disclosure control for DataSHIELD compliance in Flower FL.
#
# All thresholds and permissions are read from server-side R options,
# following the DataSHIELD convention of double-fallback:
#   getOption("dsflower.X", getOption("default.dsflower.X", hardcoded_default))

# --- Profile Order ---
# Canonical ordering of privacy profiles, from least to most restrictive.
.PROFILE_ORDER <- c(
  "sandbox_open",
  "trusted_internal",
  "consortium_internal",
  "clinical_default",
  "clinical_hardened",
  "clinical_dp",
  "high_sensitivity_dp"
)

# --- Trust Profiles ---
# Server-controlled profiles that determine what privacy guarantees are
# enforced. The client requests a profile and the server enforces it.
#
# IMPORTANT SECURITY NOTES:
#
# 1. model_release is "advisory_gated" (not "gated") in clinical profiles
#    because it is NOT enforceable when the researcher controls the SuperLink.
#    The final global model lives on the researcher's machine by design.
#    For truly gated model release, the SuperLink must run on a trusted
#    neutral host or TEE.
#
# 2. dp_scope has two levels:
#    - "update_noise_only": weight delta clipping + Gaussian noise on updates.
#      Provides meaningful obfuscation but NOT formal patient-level DP.
#    - "patient_level_dp_sgd": per-example gradient clipping via Opacus.
#      Provides formal patient-level differential privacy guarantees.
#
# 3. Secure aggregation (SecAgg+) protects individual weight updates from
#    the SuperLink, but the final aggregated model is still visible to the
#    researcher. SecAgg+ is implemented both server-side (SecAggPlusWorkflow)
#    and client-side (secaggplus_mod) in all templates.

.TRUST_PROFILES <- list(
  sandbox_open = list(
    min_train_rows             = 3,
    allow_per_node_metrics     = TRUE,
    allow_exact_num_examples   = TRUE,
    require_secure_aggregation = FALSE,
    dp_required                = FALSE,
    model_release              = "allowed",
    min_clients_per_round      = 1L,
    fixed_client_sampling      = FALSE,
    dp_scope                   = "none",
    min_positive_examples      = 0L,
    min_per_class              = 0L,
    min_events                 = 0L
  ),
  trusted_internal = list(
    min_train_rows             = 50,
    allow_per_node_metrics     = TRUE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = FALSE,
    dp_required                = FALSE,
    model_release              = "allowed",
    min_clients_per_round      = 1L,
    fixed_client_sampling      = FALSE,
    dp_scope                   = "none",
    min_positive_examples      = 5L,
    min_per_class              = 5L,
    min_events                 = 5L
  ),
  consortium_internal = list(
    min_train_rows             = 50,
    allow_per_node_metrics     = FALSE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = FALSE,
    dp_required                = FALSE,
    model_release              = "advisory_gated",
    min_clients_per_round      = 2L,
    fixed_client_sampling      = TRUE,
    dp_scope                   = "none",
    min_positive_examples      = 10L,
    min_per_class              = 10L,
    min_events                 = 10L
  ),
  clinical_default = list(
    min_train_rows             = 100,
    allow_per_node_metrics     = FALSE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = TRUE,
    dp_required                = FALSE,
    model_release              = "advisory_gated",
    min_clients_per_round      = 2L,
    fixed_client_sampling      = TRUE,
    dp_scope                   = "none",
    min_positive_examples      = 20L,
    min_per_class              = 20L,
    min_events                 = 20L
  ),
  clinical_hardened = list(
    min_train_rows             = 200,
    allow_per_node_metrics     = FALSE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = TRUE,
    dp_required                = FALSE,
    model_release              = "advisory_gated",
    min_clients_per_round      = 3L,
    fixed_client_sampling      = TRUE,
    dp_scope                   = "none",
    min_positive_examples      = 30L,
    min_per_class              = 30L,
    min_events                 = 30L
  ),
  clinical_dp = list(
    min_train_rows             = 200,
    allow_per_node_metrics     = FALSE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = TRUE,
    dp_required                = TRUE,
    model_release              = "advisory_gated",
    min_clients_per_round      = 2L,
    fixed_client_sampling      = TRUE,
    dp_scope                   = "update_noise_only",
    min_positive_examples      = 30L,
    min_per_class              = 30L,
    min_events                 = 30L
  ),
  high_sensitivity_dp = list(
    min_train_rows             = 500,
    allow_per_node_metrics     = FALSE,
    allow_exact_num_examples   = FALSE,
    require_secure_aggregation = TRUE,
    dp_required                = TRUE,
    model_release              = "advisory_gated",
    min_clients_per_round      = 3L,
    fixed_client_sampling      = TRUE,
    dp_scope                   = "patient_level_dp_sgd",
    min_positive_examples      = 50L,
    min_per_class              = 50L,
    min_events                 = 50L
  )
)

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
  pytorch_mlp                = "tabular_deep",
  pytorch_tcn                = "tabular_deep",
  pytorch_lstm               = "tabular_deep",
  pytorch_resnet18           = "vision",
  pytorch_densenet121        = "vision",
  pytorch_unet2d             = "segmentation",
  xgboost_secure_horizontal  = "xgboost_secure",
  xgboost_tabular            = "xgboost_research"
)

# Per-family minimum rows indexed by .PROFILE_ORDER.
# NA = template family not supported under that profile.
.FAMILY_MIN_ROWS <- list(
  closed_form_linear = c(3L, 50L, 50L, 100L, 200L, NA_integer_, NA_integer_),
  iterative_linear   = c(3L, 100L, 100L, 200L, 300L, 200L, 500L),
  tabular_deep       = c(3L, 250L, 250L, 500L, 750L, 500L, 1000L),
  vision             = c(3L, 500L, 500L, 1000L, 1500L, 2000L, 5000L),
  segmentation       = c(3L, 500L, 500L, 1000L, 1500L, 2000L, 5000L),
  xgboost_secure     = c(NA_integer_, 100L, 100L, 200L, 300L, NA_integer_, NA_integer_),
  xgboost_research   = c(3L, 100L, NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_)
)

# --- Template Metadata ---
# Per-template metadata that does NOT vary by profile (framework, SecAgg requirement).
.TEMPLATE_METADATA <- list(
  sklearn_logreg            = list(framework = "sklearn",        requires_secagg = FALSE),
  sklearn_ridge             = list(framework = "sklearn",        requires_secagg = FALSE),
  sklearn_sgd               = list(framework = "sklearn",        requires_secagg = FALSE),
  pytorch_mlp               = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_logreg            = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_linear_regression = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_coxph             = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_multiclass        = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_resnet18          = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_densenet121       = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_unet2d            = list(framework = "pytorch_vision", requires_secagg = FALSE),
  pytorch_tcn               = list(framework = "pytorch",        requires_secagg = FALSE),
  pytorch_lstm              = list(framework = "pytorch",        requires_secagg = FALSE),
  xgboost_secure_horizontal = list(framework = "xgboost",       requires_secagg = TRUE),
  xgboost_tabular           = list(framework = "xgboost",       requires_secagg = FALSE)
)

#' Validate template compatibility with the active trust profile
#'
#' Rejects templates whose family has NA for the given profile index.
#'
#' @param template_name Character; the template name.
#' @param profile_name Character; the active trust profile name.
#' @return TRUE invisibly, or stops with an error.
#' @keywords internal
.validateTemplateProfile <- function(template_name, profile_name) {
  family <- .TEMPLATE_FAMILIES[[template_name]]
  if (is.null(family)) {
    # Unknown template -- block DP profiles for safety
    if (profile_name %in% c("clinical_dp", "high_sensitivity_dp")) {
      stop("Template '", template_name, "' has no registered family. ",
           "Cannot use with '", profile_name, "' profile (requires verified ",
           "DP support).", call. = FALSE)
    }
    return(invisible(TRUE))
  }

  profile_idx <- match(profile_name, .PROFILE_ORDER)
  if (is.na(profile_idx)) {
    stop("Unknown profile '", profile_name, "' for template validation.",
         call. = FALSE)
  }

  min_rows_vec <- .FAMILY_MIN_ROWS[[family]]
  if (is.null(min_rows_vec) || is.na(min_rows_vec[profile_idx])) {
    stop("Template '", template_name, "' (family '", family,
         "') is not supported under the '", profile_name, "' profile.",
         call. = FALSE)
  }

  invisible(TRUE)
}

#' Get the minimum row count for a template under a given profile
#'
#' @param template_name Character; the template name.
#' @param profile_name Character; the active trust profile name.
#' @return Integer; the minimum row count, or NULL if unknown template.
#' @keywords internal
.templateMinRows <- function(template_name, profile_name) {
  family <- .TEMPLATE_FAMILIES[[template_name]]
  if (is.null(family)) return(NULL)

  profile_idx <- match(profile_name, .PROFILE_ORDER)
  if (is.na(profile_idx)) return(NULL)

  min_rows_vec <- .FAMILY_MIN_ROWS[[family]]
  if (is.null(min_rows_vec)) return(NULL)

  val <- min_rows_vec[profile_idx]
  if (is.na(val)) return(NULL)
  val
}

#' Enforce absolute floors on a trust profile
#'
#' These floors cannot be violated regardless of admin overrides:
#' - min_train_rows >= 3
#' - min_clients_per_round >= 1
#'
#' @param profile Named list; the trust profile settings.
#' @return The profile with floors enforced.
#' @keywords internal
.enforce_absolute_floors <- function(profile) {
  profile$min_train_rows <- max(profile$min_train_rows, 3)
  profile$min_clients_per_round <- max(profile$min_clients_per_round, 1L)
  profile
}

#' Get the effective trust profile
#'
#' Reads the \code{dsflower.privacy_profile} option (default "clinical_default")
#' and returns the effective settings. Admin overrides via Opal options are
#' authoritative (can both raise AND lower), with absolute floors enforced after.
#'
#' The "sandbox_open" profile requires explicit admin opt-in via
#' \code{dsflower.allow_sandbox = TRUE}.
#'
#' DP-locked profiles (clinical_dp, high_sensitivity_dp) cannot have
#' require_secure_aggregation or dp_required disabled via overrides.
#'
#' @return Named list of trust profile settings.
#' @keywords internal
.flowerTrustProfile <- function() {
  profile_name <- .dsf_option("privacy_profile", "clinical_default")
  if (!profile_name %in% names(.TRUST_PROFILES)) {
    stop("Unknown privacy profile: '", profile_name,
         "'. Valid profiles: ", paste(names(.TRUST_PROFILES), collapse = ", "),
         call. = FALSE)
  }

  # sandbox_open requires explicit admin opt-in
  if (identical(profile_name, "sandbox_open")) {
    allow_sandbox <- as.logical(.dsf_option("allow_sandbox", FALSE))
    if (!isTRUE(allow_sandbox)) {
      stop("The 'sandbox_open' privacy profile requires explicit admin opt-in. ",
           "It disables secure aggregation, exposes per-node metrics, and reveals ",
           "exact sample counts -- all of which are disclosive under the DataSHIELD ",
           "threat model. To enable, set: options(dsflower.allow_sandbox = TRUE). ",
           "Default profile is 'clinical_default'.",
           call. = FALSE)
    }
  }

  profile <- .TRUST_PROFILES[[profile_name]]
  profile$name <- profile_name

  dp_locked <- profile_name %in% c("clinical_dp", "high_sensitivity_dp")

  # --- Authoritative overrides (can raise AND lower) ---

  raw_min_rows <- .dsf_option("min_train_rows", NULL)
  if (!is.null(raw_min_rows)) {
    override_val <- as.numeric(raw_min_rows)
    if (!is.na(override_val)) {
      profile$min_train_rows <- override_val
    }
  }

  raw_per_node <- .dsf_option("allow_per_node_metrics", NULL)
  if (!is.null(raw_per_node)) {
    profile$allow_per_node_metrics <- as.logical(raw_per_node)
  }

  raw_exact <- .dsf_option("allow_exact_num_examples", NULL)
  if (!is.null(raw_exact)) {
    profile$allow_exact_num_examples <- as.logical(raw_exact)
  }

  raw_secagg <- .dsf_option("require_secure_aggregation", NULL)
  if (!is.null(raw_secagg)) {
    val <- as.logical(raw_secagg)
    if (dp_locked && identical(val, FALSE)) {
      warning("Cannot disable secure aggregation for DP-locked profile '",
              profile_name, "'. Forcing require_secure_aggregation = TRUE.",
              call. = FALSE)
      profile$require_secure_aggregation <- TRUE
    } else {
      profile$require_secure_aggregation <- val
    }
  }

  raw_dp <- .dsf_option("dp_required", NULL)
  if (!is.null(raw_dp)) {
    val <- as.logical(raw_dp)
    if (dp_locked && identical(val, FALSE)) {
      warning("Cannot disable DP for DP-locked profile '",
              profile_name, "'. Forcing dp_required = TRUE.",
              call. = FALSE)
      profile$dp_required <- TRUE
    } else {
      profile$dp_required <- val
    }
  }

  raw_min_clients <- .dsf_option("min_clients_per_round", NULL)
  if (!is.null(raw_min_clients)) {
    override_val <- as.integer(raw_min_clients)
    if (!is.na(override_val)) {
      profile$min_clients_per_round <- override_val
    }
  }

  raw_fixed <- .dsf_option("fixed_client_sampling", NULL)
  if (!is.null(raw_fixed)) {
    profile$fixed_client_sampling <- as.logical(raw_fixed)
  }

  raw_dp_scope <- .dsf_option("dp_scope", NULL)
  if (!is.null(raw_dp_scope)) {
    profile$dp_scope <- as.character(raw_dp_scope)
  }

  raw_min_pos <- .dsf_option("min_positive_examples", NULL)
  if (!is.null(raw_min_pos)) {
    override_val <- as.integer(raw_min_pos)
    if (!is.na(override_val)) {
      profile$min_positive_examples <- override_val
    }
  }

  raw_min_class <- .dsf_option("min_per_class", NULL)
  if (!is.null(raw_min_class)) {
    override_val <- as.integer(raw_min_class)
    if (!is.na(override_val)) {
      profile$min_per_class <- override_val
    }
  }

  raw_min_ev <- .dsf_option("min_events", NULL)
  if (!is.null(raw_min_ev)) {
    override_val <- as.integer(raw_min_ev)
    if (!is.na(override_val)) {
      profile$min_events <- override_val
    }
  }

  # evaluation_only modifier
  raw_eval_only <- .dsf_option("evaluation_only", NULL)
  if (!is.null(raw_eval_only) && isTRUE(as.logical(raw_eval_only))) {
    profile$model_release <- "blocked"
    profile$allow_per_node_metrics <- FALSE
    profile$evaluation_only <- TRUE
  } else {
    profile$evaluation_only <- FALSE
  }

  # Enforce absolute floors
  profile <- .enforce_absolute_floors(profile)

  profile
}

#' Validate class distribution against trust profile thresholds
#'
#' Infers task type from the target column and validates:
#' - Binary (2 unique values): min(table(target)) >= trust$min_positive_examples
#' - Multiclass (>2 unique): all(table(target) >= trust$min_per_class)
#' - Survival (2-column target or event/status column): sum(events==1) >= trust$min_events
#'
#' Error messages are deliberately generic to avoid leaking counts.
#'
#' @param data Data.frame or Arrow Table; the training data.
#' @param target_column Character; name(s) of the target column(s).
#' @param trust Named list; the trust profile settings.
#' @return TRUE invisibly, or stops with an error.
#' @keywords internal
.validateClassDistribution <- function(data, target_column, trust) {
  if (is.null(target_column) || length(target_column) == 0) {
    return(invisible(TRUE))
  }

  # Survival detection: 2-column target or column named "event"/"status"
  is_survival <- FALSE
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
      if (n_events < trust$min_events) {
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
  if (tolower(tc) %in% c("event", "status", "dead", "died")) {
    n_events <- sum(target_vals == 1, na.rm = TRUE)
    if (n_events < trust$min_events) {
      stop("Disclosive: operation blocked -- insufficient event counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
    return(invisible(TRUE))
  }

  if (n_unique == 2) {
    # Binary classification
    counts <- table(target_vals)
    if (min(counts) < trust$min_positive_examples) {
      stop("Disclosive: operation blocked -- insufficient class counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
  } else {
    # Multiclass
    counts <- table(target_vals)
    if (any(counts < trust$min_per_class)) {
      stop("Disclosive: operation blocked -- insufficient class counts to ",
           "meet disclosure threshold. No further details available.",
           call. = FALSE)
    }
  }

  invisible(TRUE)
}

#' Bucket a count to prevent exact sample sizes from leaking
#'
#' Uses power-of-two bucketing: \code{2^round(log2(n))}, with a minimum
#' bucket of 1 and a floor for small counts.
#'
#' @param n Integer; the exact count.
#' @return Integer; the bucketed count.
#' @keywords internal
.bucket_count <- function(n) {
  n <- as.integer(n)
  if (is.na(n) || n <= 0) return(0L)
  if (n < 4) return(as.integer(n))
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
    # --- Standard DataSHIELD thresholds ---
    nfilter_subset = as.numeric(getOption("nfilter.subset",
                        getOption("default.nfilter.subset", 3))),
    # --- dsFlower-specific settings ---
    max_rounds = as.numeric(.dsf_option("max_rounds", 500)),
    allow_custom_config = as.logical(.dsf_option("allow_custom_config", FALSE)),
    allowed_templates = c("sklearn_logreg", "sklearn_ridge",
                          "sklearn_sgd", "pytorch_mlp",
                          "pytorch_logreg", "pytorch_linear_regression",
                          "pytorch_coxph", "pytorch_multiclass",
                          "xgboost_tabular", "pytorch_resnet18",
                          "pytorch_densenet121", "pytorch_unet2d",
                          "pytorch_tcn", "pytorch_lstm",
                          "xgboost_secure_horizontal"),
    allow_supernode_spawn = as.logical(.dsf_option("allow_supernode_spawn", TRUE)),
    max_concurrent_runs = as.integer(.dsf_option("max_concurrent_runs", 5))
  )
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
  settings <- .flowerDisclosureSettings()
  threshold <- if (!is.null(min_n)) min_n else settings$nfilter_subset

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
