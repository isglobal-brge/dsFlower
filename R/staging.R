# Module: Manifest-Based Data Staging
# Replaces env-var approach with per-run token directories containing
# training data and a JSON manifest.

#' Generate a unique run token
#'
#' Creates a token in the format \code{run_YYYYMMDD_HHMMSS_XXXX} where
#' XXXX is a random 4-character hex suffix.
#'
#' @return Character; the run token string.
#' @keywords internal
.generate_run_token <- function() {
  hex <- paste(sample(c(0:9, letters[1:6]), 4, replace = TRUE), collapse = "")
  paste0("run_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", hex)
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

  if (!is.null(target_column) && !target_column %in% names(data)) {
    stop("Target column '", target_column, "' not found in training data. ",
         "Available columns: ", paste(names(data), collapse = ", "),
         call. = FALSE)
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
  staging_dir <- file.path(tempdir(), "dsflower", run_token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

  # Write training data
  data_file <- "train.csv"
  utils::write.csv(data, file.path(staging_dir, data_file), row.names = FALSE)

  # Build manifest
  manifest <- list(
    run_token       = run_token,
    data_file       = data_file,
    data_format     = "csv",
    n_samples       = nrow(data),
    target_column   = target_column,
    feature_columns = feature_columns,
    staged_at       = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  # Merge extra config
  if (length(extra_config) > 0) {
    manifest <- c(manifest, extra_config)
  }

  # Write manifest
  jsonlite::write_json(manifest, file.path(staging_dir, "manifest.json"),
                       auto_unbox = TRUE, pretty = TRUE, null = "null")

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
  staging_dir <- file.path(tempdir(), "dsflower", run_token)
  if (dir.exists(staging_dir)) {
    unlink(staging_dir, recursive = TRUE)
  }
  invisible(TRUE)
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
