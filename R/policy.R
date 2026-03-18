# Module: Disclosure Controls (Policy)
# Statistical disclosure control for DataSHIELD compliance in Flower FL.
#
# All thresholds and permissions are read from server-side R options,
# following the DataSHIELD convention of double-fallback:
#   getOption("dsflower.X", getOption("default.dsflower.X", hardcoded_default))

# --- Trust Profiles ---
# Server-controlled profiles that determine what privacy guarantees are
# enforced. The client can request a profile but the server enforces the floor.

.TRUST_PROFILES <- list(
  research = list(
    min_train_rows          = 3,
    allow_per_node_metrics  = TRUE,
    allow_exact_num_examples = TRUE,
    require_secure_aggregation = FALSE,
    dp_required             = FALSE,
    model_release           = "allowed"
  ),
  secure = list(
    min_train_rows          = 100,
    allow_per_node_metrics  = FALSE,
    allow_exact_num_examples = FALSE,
    require_secure_aggregation = TRUE,
    dp_required             = FALSE,
    model_release           = "gated"
  ),
  secure_dp = list(
    min_train_rows          = 200,
    allow_per_node_metrics  = FALSE,
    allow_exact_num_examples = FALSE,
    require_secure_aggregation = TRUE,
    dp_required             = TRUE,
    model_release           = "gated"
  )
)

#' Get the effective trust profile
#'
#' Reads the \code{dsflower.privacy_profile} option (default "research")
#' and returns the effective settings. Individual option overrides
#' (e.g. \code{dsflower.min_train_rows}) can only strengthen (never weaken)
#' the profile.
#'
#' @return Named list of trust profile settings.
#' @keywords internal
.flowerTrustProfile <- function() {
  profile_name <- .dsf_option("privacy_profile", "research")
  if (!profile_name %in% names(.TRUST_PROFILES)) {
    stop("Unknown privacy profile: '", profile_name,
         "'. Valid profiles: ", paste(names(.TRUST_PROFILES), collapse = ", "),
         call. = FALSE)
  }
  profile <- .TRUST_PROFILES[[profile_name]]
  profile$name <- profile_name

  # Allow individual overrides that can only strengthen the profile
  raw_min_rows <- .dsf_option("min_train_rows", NULL)
  if (!is.null(raw_min_rows)) {
    override_min_rows <- as.numeric(raw_min_rows)
    if (!is.na(override_min_rows)) {
      profile$min_train_rows <- max(profile$min_train_rows, override_min_rows)
    }
  }

  raw_per_node <- .dsf_option("allow_per_node_metrics", NULL)
  if (!is.null(raw_per_node)) {
    # Can only disable, not enable
    if (!as.logical(raw_per_node)) {
      profile$allow_per_node_metrics <- FALSE
    }
  }

  raw_exact <- .dsf_option("allow_exact_num_examples", NULL)
  if (!is.null(raw_exact)) {
    if (!as.logical(raw_exact)) {
      profile$allow_exact_num_examples <- FALSE
    }
  }

  raw_secagg <- .dsf_option("require_secure_aggregation", NULL)
  if (!is.null(raw_secagg)) {
    # Can only require, not disable
    if (as.logical(raw_secagg)) {
      profile$require_secure_aggregation <- TRUE
    }
  }

  raw_dp <- .dsf_option("dp_required", NULL)
  if (!is.null(raw_dp)) {
    if (as.logical(raw_dp)) {
      profile$dp_required <- TRUE
    }
  }

  profile
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
                          "sklearn_sgd", "pytorch_mlp"),
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
