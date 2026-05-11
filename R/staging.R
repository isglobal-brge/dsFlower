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

.trainingColumns <- function(data, target_column, feature_columns = NULL) {
  target_column <- as.character(unlist(target_column, use.names = FALSE))
  feature_columns <- as.character(unlist(feature_columns, use.names = FALSE))
  feature_columns <- feature_columns[nzchar(feature_columns)]

  if (length(feature_columns) > 0L) {
    unique(c(target_column, feature_columns))
  } else {
    names(data)
  }
}

.prepareTrainingFrame <- function(data, target_column, feature_columns = NULL,
                                  drop_missing = TRUE,
                                  select_columns = TRUE) {
  .validateDataSchema(data, target_column, feature_columns)

  cols <- .trainingColumns(data, target_column, feature_columns)
  cols <- intersect(cols, names(data))
  input_n <- nrow(data)

  if (isTRUE(drop_missing) && length(cols) > 0L && nrow(data) > 0L) {
    ok <- stats::complete.cases(data[, cols, drop = FALSE])
    for (nm in cols) {
      x <- data[[nm]]
      if (is.numeric(x) || is.integer(x)) {
        ok <- ok & is.finite(x)
      }
    }
    data <- data[ok, , drop = FALSE]
  }

  if (isTRUE(select_columns) &&
      !is.null(feature_columns) && length(feature_columns) > 0L) {
    data <- data[, cols, drop = FALSE]
  }

  if (nrow(data) == 0L) {
    stop("Training data has no complete rows after applying target and ",
         "feature-column missing-value filters.", call. = FALSE)
  }

  list(
    data = data,
    n_input_samples = input_n,
    n_samples = nrow(data),
    dropped_missing = input_n - nrow(data)
  )
}

.as_nonempty_character <- function(x) {
  if (is.null(x)) return(character(0))
  x <- as.character(x)
  x[nzchar(x)]
}

.stagingBaseCandidates <- function(create = FALSE) {
  roots <- c(
    .as_nonempty_character(.dsf_option("staging_root", NULL)),
    .as_nonempty_character(.dsf_option("staging.root", NULL)),
    .as_nonempty_character(Sys.getenv("DSFLOWER_STAGING_ROOT", "")),
    .as_nonempty_character(Sys.getenv("DSFLOWER_STAGING_DIR", "")),
    if (dir.exists("/dev/shm")) "/dev/shm" else character(0),
    tempdir()
  )
  roots <- unique(normalizePath(roots, mustWork = FALSE))
  if (isTRUE(create)) {
    for (root in roots) {
      dir.create(file.path(root, "dsflower"), recursive = TRUE,
                 showWarnings = FALSE)
    }
  }
  roots
}

.filesystemFreeBytes <- function(path) {
  probe <- path
  while (!dir.exists(probe) && !identical(dirname(probe), probe)) {
    probe <- dirname(probe)
  }
  if (!dir.exists(probe)) return(NA_real_)

  out <- tryCatch(
    system2("df", c("-Pk", probe), stdout = TRUE, stderr = FALSE),
    error = function(e) character(0)
  )
  if (length(out) < 2L) return(NA_real_)
  fields <- strsplit(trimws(out[length(out)]), "\\s+")[[1]]
  if (length(fields) < 4L) return(NA_real_)
  suppressWarnings(as.numeric(fields[[4]]) * 1024)
}

.chooseStagingBase <- function(required_bytes = 0) {
  required <- suppressWarnings(as.numeric(required_bytes %||% 0))
  if (!is.finite(required) || is.na(required) || required < 0) required <- 0

  min_free <- suppressWarnings(
    as.numeric(.dsf_option("staging_min_free_bytes", 64 * 1024^2))
  )
  if (!is.finite(min_free) || is.na(min_free) || min_free < 0) {
    min_free <- 64 * 1024^2
  }
  headroom <- suppressWarnings(
    as.numeric(.dsf_option("staging_free_headroom", 0.25))
  )
  if (!is.finite(headroom) || is.na(headroom) || headroom < 0) headroom <- 0.25

  needed <- required * (1 + headroom) + min_free
  candidates <- .stagingBaseCandidates(create = TRUE)
  for (root in candidates) {
    free <- .filesystemFreeBytes(root)
    if (is.na(free) || free >= needed) return(root)
    message("  Skipping staging root ", root, " (free ",
            round(free / 1024^2, 1), " MiB; need ",
            round(needed / 1024^2, 1), " MiB)")
  }

  stop("No dsFlower staging root has enough free space. Required: ",
       round(needed / 1024^2, 1), " MiB. Configure ",
       "options(dsflower.staging_root='/path/with/space') or ",
       "DSFLOWER_STAGING_ROOT.", call. = FALSE)
}

.ensureStagingDir <- function(run_token, required_bytes = 0) {
  base_dir <- .chooseStagingBase(required_bytes)
  staging_dir <- file.path(base_dir, "dsflower", run_token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)
  Sys.chmod(staging_dir, "0700")
  staging_dir
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
  drop_missing <- !identical(extra_config[["drop_missing"]], FALSE)
  prepared <- .prepareTrainingFrame(
    data,
    target_column = target_column,
    feature_columns = feature_columns,
    drop_missing = drop_missing,
    select_columns = TRUE
  )
  data <- prepared$data

  staging_dir <- .ensureStagingDir(run_token)

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
    n_samples       = prepared$n_samples,
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
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

  # Check configured and default base directories. Older runs may exist in
  # either tmpfs or tempdir, so cleanup intentionally scans all candidates.
  for (base in .stagingBaseCandidates(create = FALSE)) {
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

  staging_dir <- .ensureStagingDir(run_token)

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

#' Stage data from a FlowerDatasetDescriptor
#'
#' Dispatches on \code{source_kind} to stage data appropriately:
#' \itemize{
#'   \item \code{in_memory_df}: wraps \code{.stageData()} with the descriptor's
#'     \code{table_data}.
#'   \item \code{staged_parquet}: exports selected columns via arrow without
#'     loading the full table into R memory.
#'   \item \code{image_bundle}: stages metadata + manifest; images stay on disk
#'     (zero-copy). Supports multiple asset roots from the descriptor.
#' }
#'
#' @param desc A \code{FlowerDatasetDescriptor}.
#' @param run_token Character; unique run token.
#' @param target_column Character; target column name.
#' @param feature_columns Character vector or NULL.
#' @param extra_config Named list of additional manifest entries.
#' @return Character; path to the staging directory.
#' @keywords internal
.stageFromDescriptor <- function(desc, run_token, target_column,
                                  feature_columns = NULL,
                                  extra_config = list()) {
  kind <- desc$source_kind

  if (identical(kind, "in_memory_df")) {
    return(.stageFromDescriptor_df(desc, run_token, target_column,
                                    feature_columns, extra_config))
  }

  if (identical(kind, "staged_parquet")) {
    return(.stageFromDescriptor_parquet(desc, run_token, target_column,
                                         feature_columns, extra_config))
  }

  if (identical(kind, "image_bundle")) {
    return(.stageFromDescriptor_image(desc, run_token, target_column,
                                       feature_columns, extra_config))
  }

  if (identical(kind, "asset_ref")) {
    return(.stageFromDescriptor_asset_ref(desc, run_token, target_column,
                                           feature_columns, extra_config))
  }

  stop("Unknown descriptor source_kind: '", kind, "'.", call. = FALSE)
}

#' Stage from an asset_ref descriptor (dsImaging feature_table asset)
#'
#' Downloads the Parquet asset to the staging directory (from S3 or local),
#' then delegates to the staged_parquet path. No data.frame materialization.
#' @keywords internal
.stageFromDescriptor_asset_ref <- function(desc, run_token, target_column,
                                            feature_columns, extra_config) {
  asset_info <- desc$asset_info
  if (is.null(asset_info))
    stop("asset_ref descriptor requires asset_info.", call. = FALSE)

  staging_dir <- .ensureStagingDir(run_token)
  local_parquet <- file.path(staging_dir, "train.parquet")

  if (identical(asset_info$storage_backend, "s3")) {
    if (!requireNamespace("dsImaging", quietly = TRUE))
      stop("dsImaging required for S3 asset staging.", call. = FALSE)
    # Build backend and download
    resolve_dataset <- utils::getFromNamespace("resolve_dataset", "dsImaging")
    resolved <- resolve_dataset(asset_info$dataset_id)
    dsImaging::backend_get_file(resolved$backend, asset_info$uri, local_parquet)
  } else {
    # Local file: copy or symlink to staging dir
    src <- asset_info$uri
    if (!file.exists(src))
      stop("Asset file not found: ", src, call. = FALSE)
    file.copy(src, local_parquet, overwrite = TRUE)
  }

  # Delegate to staged_parquet path
  desc$source_kind <- "staged_parquet"
  desc$metadata <- list(uri = local_parquet, file = local_parquet, format = "parquet")
  .stageFromDescriptor_parquet(desc, run_token, target_column,
                                feature_columns, extra_config)
}

#' Stage from an in-memory data.frame descriptor
#' @keywords internal
.stageFromDescriptor_df <- function(desc, run_token, target_column,
                                     feature_columns, extra_config) {
  df <- desc$table_data
  if (is.null(df) || !is.data.frame(df)) {
    stop("Descriptor source_kind='in_memory_df' but no table_data found.",
         call. = FALSE)
  }
  .validateDataSchema(df, target_column, feature_columns)
  .stageData(df, run_token, target_column, feature_columns, extra_config)
}

#' Stage from a staged Parquet descriptor
#'
#' Reads only the required columns from a Parquet file via arrow::open_dataset
#' or arrow::read_parquet with column selection. The full table is never loaded
#' into R memory.
#'
#' @keywords internal
.stageFromDescriptor_parquet <- function(desc, run_token, target_column,
                                          feature_columns, extra_config) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for staged_parquet descriptors.",
         call. = FALSE)
  }

  meta <- desc$metadata
  if (is.null(meta) || is.null(meta$file)) {
    stop("Descriptor source_kind='staged_parquet' requires metadata$file.",
         call. = FALSE)
  }

  src_path <- meta$file
  if (!file.exists(src_path)) {
    stop("Parquet file not found: ", src_path, call. = FALSE)
  }

  # Determine columns to select
  # Ensure character vector (JSON deserialization may produce a list)
  if (is.list(feature_columns)) feature_columns <- unlist(feature_columns)
  feature_columns <- as.character(feature_columns)
  feature_columns <- feature_columns[nzchar(feature_columns)]
  cols_needed <- NULL
  if (!is.null(feature_columns) && length(feature_columns) > 0L) {
    cols_needed <- unique(c(target_column, feature_columns))
  }

  staging_dir <- .ensureStagingDir(
    run_token,
    required_bytes = file.info(src_path)$size %||% 0
  )

  # Read with column selection, drop incomplete rows, and write to staging.
  tbl <- if (is.null(cols_needed)) {
    arrow::read_parquet(src_path)
  } else {
    arrow::read_parquet(src_path, col_select = cols_needed)
  }
  prepared <- .prepareTrainingFrame(
    as.data.frame(tbl),
    target_column = target_column,
    feature_columns = feature_columns,
    drop_missing = !identical(extra_config[["drop_missing"]], FALSE),
    select_columns = TRUE
  )
  data_file <- "train.parquet"
  arrow::write_parquet(prepared$data, file.path(staging_dir, data_file))
  Sys.chmod(file.path(staging_dir, data_file), "0600")

  # Build manifest
  manifest <- list(
    run_token       = run_token,
    data_file       = data_file,
    data_format     = "parquet",
    n_samples       = prepared$n_samples,
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column   = target_column,
    feature_columns = feature_columns,
    dataset_id      = desc$dataset_id,
    source_kind     = "staged_parquet",
    staged_at       = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
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

.readStagedSamples <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("arrow package required for Parquet image metadata.", call. = FALSE)
    }
    return(as.data.frame(arrow::read_parquet(path)))
  }
  utils::read.csv(path, stringsAsFactors = FALSE)
}

.writeStagedSamples <- function(data, path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("arrow package required for Parquet image metadata.", call. = FALSE)
    }
    arrow::write_parquet(data, path)
  } else {
    utils::write.csv(data, path, row.names = FALSE)
  }
  Sys.chmod(path, "0600")
  invisible(path)
}

.regex_escape <- function(x) {
  gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", x)
}

.s3ObjectKey <- function(uri) {
  sub("^s3://[^/]+/?", "", uri)
}

.s3RelativePath <- function(uri, prefix) {
  key <- .s3ObjectKey(uri)
  prefix_key <- .s3ObjectKey(prefix)
  prefix_key <- paste0(sub("/+$", "", prefix_key), "/")
  rel <- sub(paste0("^", .regex_escape(prefix_key)), "", key)
  if (!nzchar(rel) || identical(rel, key)) basename(uri) else rel
}

.isDirectoryLikeObject <- function(uri) {
  grepl("/$", uri)
}

.stageS3DirectoryAssetPlan <- function(backend, s3_uri) {
  files <- dsImaging::backend_list(backend, s3_uri)
  files <- files[!vapply(files, .isDirectoryLikeObject, logical(1))]
  sizes <- vapply(files, function(f) {
    h <- tryCatch(dsImaging::backend_head(backend, f), error = function(e) NULL)
    suppressWarnings(as.numeric(h$size %||% NA_real_))
  }, numeric(1))
  list(
    files = files,
    total_bytes = sum(sizes[is.finite(sizes)], na.rm = TRUE)
  )
}

.downloadS3DirectoryAsset <- function(backend, s3_uri, local_root, files) {
  dir.create(local_root, recursive = TRUE, showWarnings = FALSE)
  rel_paths <- character(0)
  for (f in files) {
    rel <- .s3RelativePath(f, s3_uri)
    if (!nzchar(rel) || .isDirectoryLikeObject(rel)) next
    local_path <- file.path(local_root, rel)
    if (!file.exists(local_path)) {
      dsImaging::backend_get_file(backend, f, local_path)
    }
    rel_paths <- c(rel_paths, rel)
  }
  unique(rel_paths)
}

.knownImageExtensions <- function() {
  c(".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm",
    ".png", ".jpg", ".jpeg", ".tif", ".tiff")
}

.stripKnownImageExtension <- function(path) {
  sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm|png|jpe?g|tiff?)$",
      "", basename(path), ignore.case = TRUE)
}

.loadSampleManifests <- function(desc, staging_dir) {
  sm <- desc$manifest$sample_manifests %||% NULL
  if (is.null(sm)) return(NULL)

  sm_file <- sm$file %||% NULL
  sm_uri <- sm$uri %||% NULL
  if ((is.null(sm_file) || !file.exists(sm_file %||% "")) &&
      !is.null(sm_uri) && grepl("^s3://", sm_uri) &&
      !is.null(desc$backend)) {
    ext <- if (grepl("\\.parquet$", sm_uri, ignore.case = TRUE)) ".parquet" else ".csv"
    sm_file <- file.path(staging_dir, paste0("sample_manifests", ext))
    dsImaging::backend_get_file(desc$backend, sm_uri, sm_file)
    Sys.chmod(sm_file, "0600")
  }

  if (is.null(sm_file) || !file.exists(sm_file)) return(NULL)
  .readStagedSamples(sm_file)
}

.primaryFromFilesJson <- function(files_json) {
  if (is.null(files_json) || is.na(files_json) || !nzchar(files_json)) {
    return(NA_character_)
  }
  parsed <- tryCatch(
    jsonlite::fromJSON(files_json, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (!is.list(parsed) || length(parsed) == 0L) return(NA_character_)
  roles <- vapply(parsed, function(x) as.character(x$role %||% ""), character(1))
  idx <- which(roles %in% c("image", "primary"))
  item <- parsed[[if (length(idx) > 0L) idx[[1]] else 1L]]
  as.character(item$path %||% item$uri %||% item$file %||% NA_character_)
}

.normalisePrimaryPath <- function(primary, image_uri) {
  primary <- as.character(primary %||% NA_character_)
  if (is.na(primary) || !nzchar(primary)) return(NA_character_)
  if (grepl("^s3://", primary)) return(.s3RelativePath(primary, image_uri %||% primary))
  primary <- sub("^/+", "", primary)
  image_key <- if (!is.null(image_uri) && grepl("^s3://", image_uri)) {
    .s3ObjectKey(image_uri)
  } else {
    ""
  }
  image_key <- paste0(sub("/+$", "", image_key), "/")
  if (nzchar(image_key)) {
    primary <- sub(paste0("^", .regex_escape(image_key)), "", primary)
  }
  sub("^source/images/", "", primary)
}

.ensureImagePathColumn <- function(samples_df, path_col = "relative_path",
                                   sample_manifests = NULL,
                                   image_root = NULL,
                                   image_uri = NULL,
                                   downloaded_rels = character(0)) {
  if (path_col %in% names(samples_df)) {
    current <- as.character(samples_df[[path_col]])
    if (all(!is.na(current) & nzchar(current))) return(samples_df)
  }

  if (!("sample_id" %in% names(samples_df))) {
    stop("Image metadata requires either '", path_col, "' or 'sample_id'.",
         call. = FALSE)
  }

  sample_ids <- as.character(samples_df$sample_id)
  rel_paths <- rep(NA_character_, length(sample_ids))

  if (!is.null(sample_manifests) && "sample_id" %in% names(sample_manifests)) {
    sm_ids <- as.character(sample_manifests$sample_id)
    primary <- rep(NA_character_, nrow(sample_manifests))
    if ("primary_uri" %in% names(sample_manifests)) {
      primary <- as.character(sample_manifests$primary_uri)
    }
    if ("files_json" %in% names(sample_manifests)) {
      missing_primary <- is.na(primary) | !nzchar(primary)
      primary[missing_primary] <- vapply(
        sample_manifests$files_json[missing_primary],
        .primaryFromFilesJson,
        character(1)
      )
    }
    primary <- vapply(primary, .normalisePrimaryPath, character(1),
                      image_uri = image_uri)
    rel_paths <- primary[match(sample_ids, sm_ids)]
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel) && length(downloaded_rels) > 0L) {
    rel_by_stem <- downloaded_rels
    names(rel_by_stem) <- .stripKnownImageExtension(downloaded_rels)
    rel_paths[missing_rel] <- rel_by_stem[sample_ids[missing_rel]]
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel) && !is.null(image_root) && dir.exists(image_root)) {
    for (i in which(missing_rel)) {
      for (ext in .knownImageExtensions()) {
        candidate <- paste0(sample_ids[[i]], ext)
        if (file.exists(file.path(image_root, candidate))) {
          rel_paths[[i]] <- candidate
          break
        }
      }
    }
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel)) {
    stop("Could not resolve image path for sample_id(s): ",
         paste(utils::head(sample_ids[missing_rel], 5L), collapse = ", "),
         if (sum(missing_rel) > 5L) "..." else "",
         ". Provide a relative path column or a dsImaging sample_manifests table.",
         call. = FALSE)
  }

  samples_df[[path_col]] <- unname(rel_paths)
  samples_df
}

.imageAssetNeedsStaging <- function(asset_name, asset_type, extra_config) {
  if (identical(asset_type, "mask_root")) {
    return(identical(extra_config[["template_name"]], "pytorch_unet2d") ||
             !is.null(extra_config[["mask_asset"]]) ||
             !is.null(extra_config[["mask_path_col"]]))
  }
  asset_name == "images" || asset_type %in% c("image_root", "wsi_root",
                                              "dicom_series_root",
                                              "rt_struct_root")
}

#' Stage from an image bundle descriptor
#'
#' Handles multi-root image assets from the descriptor. Metadata is staged
#' (CSV/Parquet), images remain on disk (zero-copy). The manifest includes
#' an \code{assets} key mapping asset names to their validated root paths.
#'
#' @keywords internal
.stageFromDescriptor_image <- function(desc, run_token, target_column,
                                        feature_columns, extra_config) {
  meta <- desc$metadata
  assets <- desc$assets
  dir_asset_types <- c("image_root", "mask_root", "wsi_root",
                       "dicom_series_root", "rt_struct_root")
  file_asset_types <- c("feature_table", "rt_dose_file", "rt_plan_file")

  s3_asset_plans <- list()
  required_bytes <- 0
  for (asset_name in names(assets)) {
    asset <- assets[[asset_name]]
    asset_type <- asset$type %||% asset$kind %||% "unknown"
    s3_uri <- asset$uri %||% NULL
    if (asset_type %in% dir_asset_types &&
        .imageAssetNeedsStaging(asset_name, asset_type, extra_config) &&
        !is.null(s3_uri) && grepl("^s3://", s3_uri) &&
        !is.null(desc$backend)) {
      plan <- .stageS3DirectoryAssetPlan(desc$backend, s3_uri)
      s3_asset_plans[[asset_name]] <- plan
      required_bytes <- required_bytes + plan$total_bytes
    }
  }

  staging_dir <- .ensureStagingDir(run_token, required_bytes = required_bytes)

  # Stage metadata table (local file, S3 URI, or in-memory table)
  meta_file <- meta$file
  meta_uri <- meta$uri

  # If local file doesn't exist but S3 URI is available, download via backend
  if ((is.null(meta_file) || !file.exists(meta_file %||% "")) &&
      !is.null(meta_uri) && grepl("^s3://", meta_uri) &&
      !is.null(desc$backend)) {
    ext <- if (grepl("\\.parquet$", meta_uri)) ".parquet" else ".csv"
    meta_file <- file.path(staging_dir, paste0("samples", ext))
    dsImaging::backend_get_file(desc$backend, meta_uri, meta_file)
  }

  if (!is.null(meta_file) && file.exists(meta_file)) {
    samples_basename <- basename(meta_file)
    staged_samples <- file.path(staging_dir, samples_basename)
    if (!identical(normalizePath(meta_file, mustWork = FALSE),
                   normalizePath(staged_samples, mustWork = FALSE))) {
      file.copy(meta_file, staged_samples)
    }
    Sys.chmod(staged_samples, "0600")
  } else if (!is.null(desc$table_data) && is.data.frame(desc$table_data)) {
    samples_basename <- "samples.csv"
    staged_samples <- file.path(staging_dir, samples_basename)
    utils::write.csv(desc$table_data, staged_samples, row.names = FALSE)
    Sys.chmod(staged_samples, "0600")
  } else {
    stop("Image bundle descriptor requires metadata (local file, S3 URI, or table_data).",
         call. = FALSE)
  }

  # Join label set if specified
  label_set_name <- extra_config[["label_set"]] %||% NULL
  if (!is.null(label_set_name) && !is.null(desc$manifest$labels) &&
      !is.null(desc$backend)) {
    get_label_uri <- utils::getFromNamespace(".get_label_uri", "dsImaging")
    label_uri <- get_label_uri(desc$manifest, label_set_name)
    if (is.null(label_uri))
      stop("Label set '", label_set_name, "' not found in manifest.", call. = FALSE)

    label_file <- file.path(staging_dir, "labels.parquet")
    dsImaging::backend_get_file(desc$backend, label_uri, label_file)

    if (requireNamespace("arrow", quietly = TRUE)) {
      samples_df <- .readStagedSamples(staged_samples)
      labels_df <- arrow::read_parquet(label_file)
      merged <- merge(samples_df, labels_df, by = "sample_id", all.x = TRUE)
      .writeStagedSamples(merged, staged_samples)
      message("  Joined label set '", label_set_name, "' (",
              ncol(labels_df) - 1, " label columns)")
    }
    unlink(label_file)
  }

  samples_df <- .readStagedSamples(staged_samples)
  prepared <- .prepareTrainingFrame(
    samples_df,
    target_column = target_column,
    feature_columns = feature_columns,
    drop_missing = !identical(extra_config[["drop_missing"]], FALSE),
    select_columns = FALSE
  )
  samples_df <- prepared$data

  sample_manifests <- .loadSampleManifests(desc, staging_dir)

  validated_assets <- list()
  downloaded_rels <- list()
  for (asset_name in names(assets)) {
    asset <- assets[[asset_name]]
    asset_type <- asset$type %||% asset$kind %||% "unknown"

    if (asset_type %in% dir_asset_types) {
      root <- asset$root %||% NULL
      s3_uri <- asset$uri %||% NULL

      # S3 asset: download required objects, preserving paths under the prefix.
      if (.imageAssetNeedsStaging(asset_name, asset_type, extra_config) &&
          !is.null(s3_uri) && grepl("^s3://", s3_uri) &&
          !is.null(desc$backend)) {
        local_root <- file.path(staging_dir, asset_name)
        message("  Downloading ", asset_name, " from S3...")
        plan <- s3_asset_plans[[asset_name]] %||%
          .stageS3DirectoryAssetPlan(desc$backend, s3_uri)
        rels <- .downloadS3DirectoryAsset(desc$backend, s3_uri, local_root,
                                          plan$files)
        downloaded_rels[[asset_name]] <- rels
        root <- local_root
        message("  Downloaded ", length(rels), " files to staging")
      }

      if (is.null(root) && !.imageAssetNeedsStaging(asset_name, asset_type,
                                                     extra_config)) {
        next
      }
      if (is.null(root) || !dir.exists(root)) {
        stop("Asset '", asset_name, "' root directory does not exist: ",
             root %||% "(NULL)", call. = FALSE)
      }
      resolved_root <- normalizePath(root, mustWork = TRUE)
      va <- list(
        type      = asset_type,
        root      = resolved_root,
        path_col  = asset$path_col %||% "relative_path"
      )
      # WSI-specific metadata
      if (identical(asset_type, "wsi_root")) {
        va$tile_size     <- asset$tile_size %||% 256L
        va$magnification <- asset$magnification %||% NULL
        va$overlap       <- asset$overlap %||% 0L
      }
      validated_assets[[asset_name]] <- va

    } else if (asset_type %in% file_asset_types) {
      feat_file <- asset$file
      if (is.null(feat_file) || !file.exists(feat_file)) {
        stop("Asset '", asset_name, "' file does not exist: ",
             feat_file %||% "(NULL)", call. = FALSE)
      }
      validated_assets[[asset_name]] <- list(
        type     = asset_type,
        file     = normalizePath(feat_file, mustWork = TRUE),
        join_key = asset$join_key %||% NULL
      )

    } else if (identical(asset_type, "multimodal_ref")) {
      mpath <- asset$manifest
      if (is.null(mpath) || !file.exists(mpath)) {
        stop("Asset '", asset_name, "' manifest does not exist: ",
             mpath %||% "(NULL)", call. = FALSE)
      }
      validated_assets[[asset_name]] <- list(
        type     = asset_type,
        manifest = normalizePath(mpath, mustWork = TRUE),
        modality = asset$modality %||% NULL
      )
    }
  }

  # For backward compat: set data_root from primary image asset
  data_root <- NULL
  if (!is.null(validated_assets$images)) {
    data_root <- validated_assets$images$root
  }

  if (!is.null(validated_assets$images)) {
    image_asset <- assets$images %||% list()
    image_path_col <- validated_assets$images$path_col %||% "relative_path"
    samples_df <- .ensureImagePathColumn(
      samples_df,
      path_col = image_path_col,
      sample_manifests = sample_manifests,
      image_root = validated_assets$images$root,
      image_uri = image_asset$uri %||% NULL,
      downloaded_rels = downloaded_rels$images %||% character(0)
    )
  }
  .writeStagedSamples(samples_df, staged_samples)
  n_samples <- nrow(samples_df)

  # Build manifest
  manifest <- list(
    run_token     = run_token,
    data_type     = "image",
    data_root     = data_root,
    samples_file  = samples_basename,
    n_samples     = n_samples,
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column = target_column,
    dataset_id    = desc$dataset_id,
    source_kind   = "image_bundle",
    assets        = validated_assets,
    staged_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  if (length(extra_config) > 0) {
    manifest <- c(manifest, extra_config)
  }

  # Write privacy config into manifest
  trust <- tryCatch(.flowerTrustProfile(), error = function(e) NULL)
  if (!is.null(trust)) {
    manifest[["privacy_profile"]]            <- trust$name
    manifest[["allow_per_node_metrics"]]     <- trust$allow_per_node_metrics
    manifest[["allow_exact_num_examples"]]   <- trust$allow_exact_num_examples
    manifest[["require_secure_aggregation"]] <- trust$require_secure_aggregation
    manifest[["dp_required"]]                <- trust$dp_required
    manifest[["min_clients_per_round"]]      <- trust$min_clients_per_round
    manifest[["fixed_client_sampling"]]      <- trust$fixed_client_sampling
    manifest[["dp_scope"]]                   <- trust$dp_scope
    manifest[["evaluation_only"]]            <- trust$evaluation_only
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
