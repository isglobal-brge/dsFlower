# Module: Manifest-Based Data Staging
# Replaces env-var approach with per-run token directories containing
# training data and a JSON manifest.

#' Generate a unique run token
#'
#' Creates a token in the format \code{run_YYYYMMDD_HHMMSS_PID_XXXXXXXXXXXX}
#' where PID is the R process ID and X is a 12-character random hex suffix.
#' This provides sufficient uniqueness even when multiple users on the
#' same server create runs at the same second.
#'
#' @return Character; the run token string.
#' @keywords internal
.generate_run_token <- function() {
  hex <- paste(sample(c(0:9, letters[1:6]), 12, replace = TRUE), collapse = "")
  paste0("run_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", Sys.getpid(), "_", hex)
}

#' Load training data from a file path
#'
#' Reads training data from CSV, Parquet, or Feather format.
#'
#' @param data_path Character; path to the data file.
#' @param data_format Character; one of "csv", "parquet", "feather".
#' @return A data.frame of training data.
#' @keywords internal
.loadTrainingData <- function(data_path, data_format = "csv") {
  fmt <- tolower(data_format %||% "csv")

  if (is.null(data_path) || !file.exists(data_path)) {
    stop("Training data file not found: ", data_path %||% "(not configured)",
         call. = FALSE)
  }

  if (fmt == "csv") {
    return(utils::read.csv(data_path, stringsAsFactors = FALSE))
  }

  if (fmt == "parquet") {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("arrow package required for Parquet support.", call. = FALSE)
    }
    return(as.data.frame(arrow::read_parquet(data_path)))
  }

  if (fmt == "feather") {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("arrow package required for Feather support.", call. = FALSE)
    }
    return(as.data.frame(arrow::read_feather(data_path)))
  }

  stop("Unsupported data format: '", fmt, "'. Supported: csv, parquet, feather.",
       call. = FALSE)
}

#' Validate that training data has the expected schema
#'
#' Checks that the target column exists, feature columns (if specified)
#' exist, and the dataset is not empty.
#'
#' @param data Data.frame of training data.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character vector; names of feature columns, or NULL.
#' @return TRUE invisibly, or stops with an error.
#' @keywords internal
.validateDataSchema <- function(data, target_column, feature_columns = NULL) {
  if (nrow(data) == 0) {
    stop("Training data is empty.", call. = FALSE)
  }

  # Support vector target_column for survival models (e.g. c("time", "event"))
  if (!is.null(target_column)) {
    missing_targets <- setdiff(target_column, names(data))
    if (length(missing_targets) > 0) {
      stop("Target column(s) '", paste(missing_targets, collapse = "', '"),
           "' not found in training data. ",
           "Available columns: ", paste(names(data), collapse = ", "),
           call. = FALSE)
    }
  }

  if (!is.null(feature_columns) && length(feature_columns) > 0) {
    missing <- setdiff(feature_columns, names(data))
    if (length(missing) > 0) {
      stop("Feature column(s) not found in training data: ",
           paste(missing, collapse = ", "),
           call. = FALSE)
    }
  }

  invisible(TRUE)
}

#' Stage data for a training run
#'
#' Creates a run-specific directory under \code{{tempdir()}/dsflower/{run_token}/},
#' writes the training data as CSV and a JSON manifest describing the data.
#'
#' @param data Data.frame of training data.
#' @param run_token Character; the unique run token.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character vector or NULL; names of feature columns.
#' @param extra_config Named list of additional configuration to include in manifest.
#' @return Character; path to the staging directory.
#' @keywords internal
.stageData <- function(data, run_token, target_column,
                       feature_columns = NULL, extra_config = list()) {
  # Prefer tmpfs (/dev/shm) when available for ephemeral staging
  base_dir <- if (dir.exists("/dev/shm")) "/dev/shm" else tempdir()
  staging_dir <- file.path(base_dir, "dsflower", run_token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

  # Strict directory permissions (owner-only)
  Sys.chmod(staging_dir, "0700")

  # Write training data -- prefer Parquet when arrow is available
  use_parquet <- requireNamespace("arrow", quietly = TRUE)
  if (use_parquet) {
    data_file <- "train.parquet"
    data_format <- "parquet"
    arrow::write_parquet(data, file.path(staging_dir, data_file))
  } else {
    data_file <- "train.csv"
    data_format <- "csv"
    utils::write.csv(data, file.path(staging_dir, data_file), row.names = FALSE)
  }

  # Strict file permissions
  Sys.chmod(file.path(staging_dir, data_file), "0600")

  # Build manifest
  manifest <- list(
    run_token       = run_token,
    data_file       = data_file,
    data_format     = data_format,
    n_samples       = nrow(data),
    target_column   = target_column,
    feature_columns = feature_columns,
    staged_at       = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  # Merge extra config (includes privacy settings from trust profile)
  if (length(extra_config) > 0) {
    manifest <- c(manifest, extra_config)
  }

  # Write manifest
  manifest_path <- file.path(staging_dir, "manifest.json")
  jsonlite::write_json(manifest, manifest_path,
                       auto_unbox = TRUE, pretty = TRUE, null = "null")
  Sys.chmod(manifest_path, "0600")

  staging_dir
}

#' Clean up a staging directory
#'
#' Removes the staging directory for a given run token.
#'
#' @param run_token Character; the run token whose staging dir to remove.
#' @return Invisible TRUE.
#' @keywords internal
.cleanupStaging <- function(run_token) {
  if (is.null(run_token)) return(invisible(TRUE))

  # Check both possible base directories (tmpfs and tempdir)
  for (base in c("/dev/shm", tempdir())) {
    staging_dir <- file.path(base, "dsflower", run_token)
    if (dir.exists(staging_dir)) {
      unlink(staging_dir, recursive = TRUE)
    }
  }
  invisible(TRUE)
}

#' Resolve the approved image data root from server-side option
#'
#' Reads \code{dsflower.image_data_root} server option. This is NEVER
#' supplied by the researcher -- only the server admin sets it.
#'
#' @return Character; absolute path to the image data root.
#' @keywords internal
.resolve_image_data_root <- function() {
  data_root <- .dsf_option("image_data_root", NULL)
  if (is.null(data_root) || !nzchar(data_root)) {
    stop("dsflower.image_data_root server option is not configured. ",
         "Contact your server administrator.", call. = FALSE)
  }
  if (!dir.exists(data_root)) {
    stop("Image data root does not exist: ", data_root, call. = FALSE)
  }
  normalizePath(data_root, mustWork = TRUE)
}

#' Stage an image manifest for a training run
#'
#' Writes the samples metadata (data.frame or file) to staging and
#' creates a manifest pointing to the approved data root. Images are NOT
#' copied -- they stay in place (zero-copy).
#'
#' @param run_token Character; the unique run token.
#' @param target_column Character; label column name in samples data.
#' @param samples_data Data.frame of samples metadata, or character path to file.
#' @param extra_config Named list of additional manifest entries.
#' @return Character; path to the staging directory.
#' @keywords internal
.stage_image_manifest <- function(run_token, target_column,
                                   samples_data, extra_config = list()) {
  data_root <- .resolve_image_data_root()

  # Create staging directory
  base_dir <- if (dir.exists("/dev/shm")) "/dev/shm" else tempdir()
  staging_dir <- file.path(base_dir, "dsflower", run_token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)
  Sys.chmod(staging_dir, "0700")

  # Write samples metadata to staging
  if (is.data.frame(samples_data)) {
    samples_basename <- "samples.csv"
    staged_samples <- file.path(staging_dir, samples_basename)
    utils::write.csv(samples_data, staged_samples, row.names = FALSE)
    n_samples <- nrow(samples_data)
  } else if (is.character(samples_data) && file.exists(samples_data)) {
    samples_basename <- basename(samples_data)
    staged_samples <- file.path(staging_dir, samples_basename)
    file.copy(samples_data, staged_samples)
    if (grepl("\\.parquet$", samples_basename, ignore.case = TRUE)) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("arrow package required for Parquet support.", call. = FALSE)
      }
      n_samples <- nrow(arrow::read_parquet(staged_samples))
    } else {
      n_samples <- nrow(utils::read.csv(staged_samples))
    }
  } else {
    stop("samples_data must be a data.frame or a path to an existing file.",
         call. = FALSE)
  }
  Sys.chmod(staged_samples, "0600")

  # Build manifest
  manifest <- list(
    run_token    = run_token,
    data_type    = "image",
    data_root    = data_root,
    samples_file = samples_basename,
    n_samples    = n_samples,
    target_column = target_column,
    staged_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  if (length(extra_config) > 0) {
    manifest <- c(manifest, extra_config)
  }

  manifest_path <- file.path(staging_dir, "manifest.json")
  jsonlite::write_json(manifest, manifest_path,
                       auto_unbox = TRUE, pretty = TRUE, null = "null")
  Sys.chmod(manifest_path, "0600")

  staging_dir
}

#' Get a disclosure-safe summary of training data
#'
#' Returns row count, column count, and column names without exposing
#' any actual data values.
#'
#' @param data_path Character; path to the data file.
#' @param data_format Character; the format of the data file.
#' @return Named list with n_rows, n_cols, columns.
#' @keywords internal
.getDataSummary <- function(data_path, data_format = "csv") {
  data <- .loadTrainingData(data_path, data_format)
  list(
    n_rows  = nrow(data),
    n_cols  = ncol(data),
    columns = names(data)
  )
}
