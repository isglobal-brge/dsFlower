# Module: Manifest-Based Data Staging
# Replaces env-var approach with per-run token directories containing
# training data and a JSON manifest.

# Keep public tabular values well below the float32 representation edge so the
# runner's public center/span arithmetic does not overflow before training.
# This is representation hardening, not a proof that every user model is
# numerically total (activations/losses remain part of the valid-input contract).
.DSFLOWER_FLOAT32_SAFE_MAX <- 1e6

# Reserved record-local marker.  It is never interpreted as a real asset path:
# the trusted runner maps it directly to a fixed zero image before filesystem
# resolution.  Keeping this value public and constant avoids a success/error bit
# for missing, malformed, or inaccessible private image records.
.DSFLOWER_INVALID_IMAGE_PATH <- "__dsflower_invalid_image__"

#' Generate a unique run token
#'
#' Creates a token in the format \code{run_<32 hex digits>} from 128 bits of
#' operating-system entropy. It identifies transient staging only and is never a
#' privacy counter or an input to sticky semantic noise.
#'
#' @return Character; the run token string.
#' @keywords internal
.generate_run_token <- function() {
  entropy <- tryCatch(.read_os_entropy(16L),
                      error = function(e) raw(0))
  if (length(entropy) != 16L) {
    stop("Could not obtain 128 bits of operating-system entropy for a run token.",
         call. = FALSE)
  }
  paste0("run_", paste(sprintf("%02x", as.integer(entropy)), collapse = ""))
}

#' Validate a server-generated run token
#' @keywords internal
.validate_run_token <- function(run_token) {
  if (!is.character(run_token) || length(run_token) != 1L ||
      is.na(run_token) || !grepl("^run_[0-9a-f]{32}$", run_token)) {
    stop("Invalid dsFlower run token.", call. = FALSE)
  }
  run_token
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
    stop("The configured training data source is unavailable.", call. = FALSE)
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
#' Checks that the target column exists and feature columns (if specified)
#' exist. Empty frames are valid inputs: staging preserves their public schema
#' and the trusted runner produces its normal data-independent no-op.
#'
#' @param data Data.frame of training data.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character vector; names of feature columns, or NULL.
#' @return TRUE invisibly, or stops with an error.
#' @keywords internal
.validateDataSchema <- function(data, target_column, feature_columns = NULL) {
  # Support vector target_column for survival models (e.g. c("time", "event"))
  if (!is.null(target_column)) {
    missing_targets <- setdiff(target_column, names(data))
    if (length(missing_targets) > 0) {
      stop("One or more requested target columns were not found.",
           call. = FALSE)
    }
  }

  if (!is.null(feature_columns) && length(feature_columns) > 0) {
    missing <- setdiff(feature_columns, names(data))
    if (length(missing) > 0) {
      stop("One or more requested feature columns were not found.",
           call. = FALSE)
    }
  }

  invisible(TRUE)
}

# Validate analyst-supplied column names without inspecting a private schema.
# This runs before private staging and defines the complete public column
# contract.
.normalizePublicColumnSelection <- function(target_column, feature_columns,
                                            run_config) {
  target_column <- as.character(unlist(target_column, use.names = FALSE))
  if (!length(target_column) || anyNA(target_column) ||
      any(!nzchar(target_column)) || anyDuplicated(target_column)) {
    stop("target_column must contain unique, non-empty column names.",
         call. = FALSE)
  }

  multilabel <- identical(tolower(as.character(unlist(
    run_config[["loss-name"]] %||% "", use.names = FALSE))),
    "multilabel_bce")
  expected_targets <- if (multilabel) {
    as.integer(run_config[["num-labels"]])
  } else {
    1L
  }
  if (length(target_column) != expected_targets) {
    stop(if (multilabel) {
      "target_column length must equal the public num-labels value."
    } else {
      "The enforced-DP runner requires exactly one target column."
    }, call. = FALSE)
  }

  if (is.null(feature_columns)) {
    return(list(target_column = target_column, feature_columns = NULL))
  }
  feature_columns <- as.character(unlist(feature_columns, use.names = FALSE))
  if (!length(feature_columns) || anyNA(feature_columns) ||
      any(!nzchar(feature_columns)) || anyDuplicated(feature_columns)) {
    stop("feature_columns must contain unique, non-empty column names.",
         call. = FALSE)
  }
  if (length(intersect(target_column, feature_columns))) {
    stop("Target columns cannot also be model feature columns.", call. = FALSE)
  }
  list(target_column = target_column, feature_columns = feature_columns)
}

.trainingColumns <- function(data, target_column, feature_columns = NULL,
                             patient_column = NULL) {
  has_explicit_features <- !is.null(feature_columns)
  target_column <- as.character(unlist(target_column, use.names = FALSE))
  feature_columns <- as.character(unlist(feature_columns, use.names = FALSE))
  feature_columns <- feature_columns[nzchar(feature_columns)]
  patient_column <- as.character(unlist(patient_column, use.names = FALSE))
  patient_column <- patient_column[nzchar(patient_column)]

  if (has_explicit_features) {
    unique(c(target_column, feature_columns, patient_column))
  } else {
    names(data)
  }
}

.prepareTrainingFrame <- function(data, target_column, feature_columns = NULL,
                                  drop_missing = TRUE,
                                  select_columns = TRUE,
                                  patient_column = NULL) {
  .validateDataSchema(data, target_column, feature_columns)

  cols <- .trainingColumns(
    data, target_column, feature_columns, patient_column = patient_column)
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
    if (length(patient_column) == 1L && patient_column %in% names(data)) {
      patient_id <- trimws(as.character(data[[patient_column]]))
      ok <- ok & !is.na(patient_id) & nzchar(patient_id)
    }
    data <- data[ok, , drop = FALSE]
  }

  if (isTRUE(select_columns) &&
      !is.null(feature_columns) && length(feature_columns) > 0L) {
    data <- data[, cols, drop = FALSE]
  }

  list(
    data = data,
    n_input_samples = input_n,
    n_samples = nrow(data),
    dropped_missing = input_n - nrow(data)
  )
}

#' Totalise numeric model features with public defaults
#'
#' The runner accepts numeric tensors. Coercion failures and non-finite values
#' must not turn prepare success/failure into a private-value predicate, so each
#' selected feature is mapped independently to a fixed public value. When valid
#' public feature bounds are present their midpoint is used; otherwise zero is
#' used. This preserves row count and replace-one adjacency.
#' @keywords internal
.coerceNumericOrMissing <- function(value) {
  tryCatch(
    suppressWarnings(if (is.factor(value) || is.character(value)) {
      as.numeric(as.character(value))
    } else {
      as.numeric(value)
    }),
    error = function(e) rep(NA_real_, length(value))
  )
}

#' @keywords internal
.totalizeModelFeatures <- function(data, feature_columns, run_config = list()) {
  columns <- as.character(unlist(feature_columns, use.names = FALSE))
  columns <- unique(columns[nzchar(columns)])
  if (!length(columns)) return(data)

  defaults <- rep(0, length(columns))
  bounds <- run_config[["feature-bounds"]] %||% NULL
  if (is.list(bounds)) {
    lower <- suppressWarnings(as.numeric(unlist(bounds$lower, use.names = FALSE)))
    upper <- suppressWarnings(as.numeric(unlist(bounds$upper, use.names = FALSE)))
    if (length(lower) == length(columns) && length(upper) == length(columns) &&
        all(is.finite(lower)) && all(is.finite(upper)) && all(lower < upper)) {
      defaults <- lower / 2 + upper / 2
    }
  }

  for (i in seq_along(columns)) {
    name <- columns[[i]]
    value <- data[[name]]
    numeric_value <- .coerceNumericOrMissing(value)
    invalid <- is.na(numeric_value) | !is.finite(numeric_value)
    numeric_value[invalid] <- defaults[[i]]
    numeric_value <- pmin(.DSFLOWER_FLOAT32_SAFE_MAX,
                          pmax(-.DSFLOWER_FLOAT32_SAFE_MAX, numeric_value))
    data[[name]] <- numeric_value
  }
  data
}

#' Apply the server-owned DP unit to a concrete training frame
#' @keywords internal
.prepareDpUnitFrame <- function(data, run_config = list()) {
  policy <- .dpUnitPolicy()
  patient_column <- .detectPatientColumn(data, run_config)
  if (!is.null(patient_column)) {
    values <- data[[patient_column]]
    ids <- .canonicalPatientIdText(values)
    invalid <- .invalidPatientIds(ids)
    # A single fixed unit is conservative: every row without a usable stable
    # identifier is protected together, rather than falling back to row-level
    # DP or exposing an error bit. A real identifier equal to the sentinel is
    # simply merged into the same (larger) protected unit.
    ids[invalid] <- "__dsflower_missing_patient_unit__"
    data[[patient_column]] <- ids
  }
  list(
    data = data,
    dp_unit = policy$dp_unit,
    patient_column = patient_column,
    canonicalization = policy$canonicalization
  )
}

#' Count the privacy units in the exact frame consumed by training
#'
#' This is an internal staging invariant, not an analyst-visible statistic.
#' Row mode counts rows. Patient mode counts the complete, canonical identifiers
#' selected by the server policy and never falls back to rows.
#' @keywords internal
.countDpUnits <- function(data, dp_unit, patient_column = NULL) {
  unit <- tolower(as.character(unlist(dp_unit, use.names = FALSE)))
  if (length(unit) != 1L || is.na(unit) || !unit %in% c("row", "patient")) {
    stop("Invalid staged DP unit.", call. = FALSE)
  }
  if (!is.data.frame(data)) {
    stop("Staged training data must be a data.frame.", call. = FALSE)
  }
  if (identical(unit, "row")) return(nrow(data))

  pcol <- as.character(unlist(patient_column, use.names = FALSE))
  if (length(pcol) != 1L || is.na(pcol) || !nzchar(pcol) ||
      !pcol %in% names(data)) {
    stop("Patient-mode staging is missing its configured identifier column.",
         call. = FALSE)
  }
  length(unique(.canonicalPatientIds(data[[pcol]])))
}

#' Remove the protected patient identifier from model features
#' @keywords internal
.excludePatientFeature <- function(feature_columns, patient_column) {
  if (is.null(feature_columns) || !length(feature_columns) ||
      is.null(patient_column)) return(feature_columns)
  original <- as.character(unlist(feature_columns, use.names = FALSE))
  kept <- setdiff(original, patient_column)
  if (length(original) > 0L && length(kept) == 0L) {
    stop("The patient identifier cannot be the only model feature.", call. = FALSE)
  }
  kept
}

#' Resolve an explicit, identifier-free tabular model feature set
#' @keywords internal
.resolveModelFeatures <- function(data, target_column, feature_columns,
                                  patient_column) {
  resolved <- if (is.null(feature_columns)) {
    setdiff(names(data), c(as.character(target_column), patient_column))
  } else {
    .excludePatientFeature(feature_columns, patient_column)
  }
  resolved <- unique(as.character(unlist(resolved, use.names = FALSE)))
  resolved <- resolved[nzchar(resolved)]
  if (!length(resolved)) {
    stop("Training data must contain at least one non-identifier feature.",
         call. = FALSE)
  }
  resolved
}

#' Apply the public target contract without cohort-derived inference
#' @keywords internal
.transformPublicTarget <- function(data, target_column, run_config) {
  target_column <- as.character(unlist(target_column, use.names = FALSE))
  if (!length(target_column) || anyNA(target_column) ||
      any(!nzchar(target_column)) || anyDuplicated(target_column) ||
      any(!target_column %in% names(data))) {
    stop("Target columns must be unique, non-empty, and present in the data.",
         call. = FALSE)
  }
  loss_name <- tolower(as.character(unlist(
    run_config[["loss-name"]] %||% "", use.names = FALSE)))
  if (identical(loss_name, "multilabel_bce")) {
    num_labels <- suppressWarnings(as.integer(unlist(
      run_config[["num-labels"]] %||% NA_integer_, use.names = FALSE)))
    if (length(num_labels) != 1L || is.na(num_labels) ||
        num_labels < 2L || num_labels > 1024L ||
        length(target_column) != num_labels) {
      stop("Multilabel staging requires exactly num-labels=", num_labels,
           " target columns.", call. = FALSE)
    }
    scalar_config <- run_config
    scalar_config[["loss-name"]] <- "bce_logits"
    scalar_config[["num-classes"]] <- 2L
    scalar_config[["num-labels"]] <- NULL
    for (column in target_column) {
      data <- .transformPublicTarget(data, column, scalar_config)
    }
    return(data)
  }
  if (length(target_column) != 1L) {
    stop("The enforced-DP runner requires exactly one target column.",
         call. = FALSE)
  }
  target <- data[[target_column]]
  task_type <- tolower(as.character(unlist(
    run_config[["task-type"]] %||% "classification", use.names = FALSE)))

  if (task_type %in% c("regression", "count", "continuous")) {
    bounds <- run_config[["target-bounds"]]
    numeric_target <- .coerceNumericOrMissing(target)
    lower <- as.numeric(bounds$lower)
    upper <- as.numeric(bounds$upper)
    invalid <- is.na(numeric_target) | !is.finite(numeric_target)
    numeric_target[invalid] <- lower / 2 + upper / 2
    numeric_target <- pmin(upper, pmax(lower, numeric_target))
    data[[target_column]] <- numeric_target
    return(data)
  }

  spec <- run_config[["target-levels"]] %||% NULL
  if (!is.null(spec)) {
    level_type <- as.character(spec$type)
    levels <- unlist(spec$values, use.names = FALSE)
    if (identical(level_type, "character")) {
      observed <- enc2utf8(as.character(target))
      levels <- enc2utf8(as.character(levels))
    } else if (identical(level_type, "logical")) {
      observed <- tryCatch(suppressWarnings(as.logical(target)),
                           error = function(e) rep(NA, length(target)))
      levels <- as.logical(levels)
    } else if (identical(level_type, "numeric")) {
      observed <- .coerceNumericOrMissing(target)
      levels <- as.numeric(levels)
    } else {
      stop("Pinned target-levels have an unsupported type.", call. = FALSE)
    }
    encoded <- match(observed, levels) - 1L
    # Code zero is a public catch-all. This makes the transformation total and
    # record-local; no success/error bit reveals whether a private value was
    # missing or outside the declared vocabulary.
    encoded[is.na(encoded)] <- 0L
    data[[target_column]] <- encoded
    return(data)
  }

  # Numeric classification labels may be supplied as public integer codes.
  # Validate against the public model class count; never infer a mapping.
  encoded <- .coerceNumericOrMissing(target)
  expected <- run_config[["num-classes"]] %||% 2L
  expected <- as.integer(unlist(expected, use.names = FALSE))
  if (length(expected) != 1L || is.na(expected) || expected < 2L) {
    stop("The public model class count must be at least two.", call. = FALSE)
  }
  invalid <- is.na(encoded) | !is.finite(encoded) | encoded != floor(encoded) |
    encoded < 0 | encoded >= expected
  encoded[invalid] <- 0L
  data[[target_column]] <- encoded
  data
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
    message("  Skipping an unavailable staging root.")
  }

  stop("No configured dsFlower staging root has sufficient capacity. ",
       "Contact the node administrator.", call. = FALSE)
}

.expectedStagingDirs <- function(run_token, create_roots = FALSE) {
  token <- .validate_run_token(run_token)
  bases <- .stagingBaseCandidates(create = create_roots)
  unique(vapply(bases, function(base) {
    root <- file.path(base, "dsflower")
    if (isTRUE(create_roots)) {
      dir.create(root, recursive = TRUE, showWarnings = FALSE)
    }
    if (.privacy_path_is_link(root)) {
      stop("The dsFlower staging root must not be a link or reparse point.",
           call. = FALSE)
    }
    if (dir.exists(root)) {
      root <- normalizePath(root, winslash = "/", mustWork = TRUE)
    } else {
      root <- .canonical_state_path(root)
    }
    .canonical_state_path(file.path(root, token))
  }, character(1)))
}

#' Validate a canonical staging path against its run token and allowed roots
#' @keywords internal
.validateStagingDir <- function(staging_dir, run_token, must_exist = TRUE) {
  token <- .validate_run_token(run_token)
  if (!is.character(staging_dir) || length(staging_dir) != 1L ||
      is.na(staging_dir) || !.path_is_absolute(staging_dir)) {
    stop("Invalid dsFlower staging directory.", call. = FALSE)
  }
  if (.privacy_path_is_link(staging_dir)) {
    stop("The dsFlower staging directory must not be a link or reparse point.",
         call. = FALSE)
  }
  exists <- dir.exists(staging_dir)
  if (isTRUE(must_exist) && !exists) {
    stop("The dsFlower staging directory does not exist.", call. = FALSE)
  }
  supplied <- if (exists) {
    normalizePath(staging_dir, winslash = "/", mustWork = TRUE)
  } else {
    .canonical_state_path(staging_dir)
  }
  allowed <- .expectedStagingDirs(token, create_roots = FALSE)
  if (!supplied %in% allowed || !identical(basename(supplied), token)) {
    stop("The dsFlower staging directory is outside the permitted run roots.",
         call. = FALSE)
  }
  supplied
}

.ensureStagingDir <- function(run_token, required_bytes = 0) {
  run_token <- .validate_run_token(run_token)
  base_dir <- .chooseStagingBase(required_bytes)
  staging_dir <- file.path(base_dir, "dsflower", run_token)
  staging_dir <- .validateStagingDir(
    staging_dir, run_token, must_exist = FALSE)
  if (file.exists(staging_dir) && !dir.exists(staging_dir)) {
    stop("The dsFlower staging path is not a directory.", call. = FALSE)
  }
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)
  staging_dir <- .validateStagingDir(staging_dir, run_token, must_exist = TRUE)
  if (.Platform$OS.type == "windows") {
    .windows_set_private_acl(staging_dir, is_directory = TRUE)
  } else {
    Sys.chmod(staging_dir, "0700")
  }
  staging_dir
}

#' Force the always-on DP contract into a staging manifest
#'
#' Differential privacy is always enforced by the node-side trusted curator
#' (central DP before egress, no Secure Aggregation), and
#' disclosure is non-disclosive by default, so every manifest carries dp_enabled
#' + suppressed metrics/counts + fixed sampling.
#' @param manifest Named list containing the server-authored run manifest.
#' @return The manifest with mandatory privacy flags applied.
#' @keywords internal
.normalize_dp_manifest <- function(manifest) {
  manifest[["dp_enabled"]]               <- TRUE
  manifest[["allow_per_node_metrics"]]   <- FALSE
  manifest[["allow_exact_num_examples"]] <- FALSE
  manifest[["fixed_client_sampling"]]    <- TRUE
  manifest
}

.manifest_structural_fields <- function() {
  c(
    "run_token", "data_file", "data_format", "samples_file", "n_samples",
    "n_units",
    "n_input_samples", "dropped_missing", "target", "target_column",
    "feature_columns", "staged_at", "data_root", "dp-unit", "patient_column",
    "patient-id-canonicalization",
    "target-preencoded", "association-preencoded",
    "group_column", "dataset_id", "source_kind", "assets", "data_type",
    "drop_missing"
  )
}

#' Validate public feature bounds before private staging
#' @keywords internal
.normalizePublicFeatureBounds <- function(run_config) {
  bounds <- run_config[["feature-bounds"]] %||% NULL
  if (is.null(bounds)) return(run_config)
  if (!is.list(bounds) || is.null(bounds$lower) || is.null(bounds$upper)) {
    stop("feature-bounds must contain public lower and upper vectors.",
         call. = FALSE)
  }
  lower <- suppressWarnings(as.numeric(unlist(bounds$lower, use.names = FALSE)))
  upper <- suppressWarnings(as.numeric(unlist(bounds$upper, use.names = FALSE)))
  if (!length(lower) || length(lower) != length(upper) ||
      any(!is.finite(lower)) || any(!is.finite(upper)) || any(lower >= upper) ||
      any(abs(lower) > .DSFLOWER_FLOAT32_SAFE_MAX) ||
      any(abs(upper) > .DSFLOWER_FLOAT32_SAFE_MAX)) {
    stop("feature-bounds must be equal-length finite vectors with lower < ",
         "upper inside the declared [-1e6, 1e6] numeric domain.", call. = FALSE)
  }
  run_config[["feature-bounds"]] <- list(lower = lower, upper = upper)
  run_config
}

#' Validate and canonicalise public target semantics
#'
#' These values are supplied by the analyst as public constants and then pinned
#' into the server-written manifest. They are never inferred from a node cohort.
#' @keywords internal
.normalizePublicTargetConfig <- function(run_config) {
  task_type <- tolower(as.character(unlist(
    run_config[["task-type"]] %||% run_config[["task_type"]] %||%
      "classification", use.names = FALSE)))
  if (length(task_type) != 1L || is.na(task_type) || !nzchar(task_type)) {
    stop("task-type must be one non-empty value.", call. = FALSE)
  }

  levels <- run_config[["target-levels"]] %||% NULL
  bounds <- run_config[["target-bounds"]] %||% NULL
  numeric_task <- task_type %in% c("regression", "count", "continuous")
  loss_name <- tolower(as.character(unlist(
    run_config[["loss-name"]] %||% "", use.names = FALSE)))
  if (identical(loss_name, "multilabel_bce")) {
    num_labels <- suppressWarnings(as.integer(unlist(
      run_config[["num-labels"]] %||% NA_integer_, use.names = FALSE)))
    if (length(num_labels) != 1L || is.na(num_labels) ||
        num_labels < 2L || num_labels > 1024L) {
      stop("multilabel_bce requires public num-labels in [2, 1024].",
           call. = FALSE)
    }
    run_config[["num-labels"]] <- num_labels
  }

  if (isTRUE(numeric_task)) {
    if (!is.null(levels)) {
      stop("target-levels is only valid for classification targets.",
           call. = FALSE)
    }
    if (!is.list(bounds) || is.null(bounds$lower) || is.null(bounds$upper)) {
      stop("Public target_bounds=list(lower=..., upper=...) is required for ",
           "regression/count models.", call. = FALSE)
    }
    lower <- suppressWarnings(as.numeric(unlist(bounds$lower, use.names = FALSE)))
    upper <- suppressWarnings(as.numeric(unlist(bounds$upper, use.names = FALSE)))
    if (length(lower) != 1L || length(upper) != 1L ||
        !is.finite(lower) || !is.finite(upper) || lower >= upper ||
        abs(lower) > .DSFLOWER_FLOAT32_SAFE_MAX ||
        abs(upper) > .DSFLOWER_FLOAT32_SAFE_MAX) {
      stop("target-bounds must contain finite scalar lower < upper inside ",
           "the declared [-1e6, 1e6] numeric domain.",
           call. = FALSE)
    }
    if (identical(task_type, "count") && lower < 0) {
      stop("Count target bounds require lower >= 0.", call. = FALSE)
    }
    if (identical(loss_name, "gamma_nll") && lower <= 0) {
      stop("gamma_nll target bounds require lower > 0.", call. = FALSE)
    }
    run_config[["target-bounds"]] <- list(lower = lower, upper = upper)
    run_config[["target-levels"]] <- NULL
    return(run_config)
  }

  if (!is.null(bounds)) {
    stop("target-bounds is only valid for regression/count targets.",
         call. = FALSE)
  }
  if (is.null(levels)) return(run_config)

  if (is.factor(levels)) levels <- as.character(levels)
  if (is.list(levels)) {
    if (!is.null(names(levels)) || !length(levels) ||
        any(vapply(levels, length, integer(1)) != 1L)) {
      stop("target-levels must be a public vector, not an object.",
           call. = FALSE)
    }
    kinds <- vapply(levels, function(value) {
      if (is.character(value)) "character" else
        if (is.logical(value)) "logical" else
          if (is.numeric(value)) "numeric" else "unsupported"
    }, character(1))
    if (length(unique(kinds)) != 1L || identical(kinds[[1]], "unsupported")) {
      stop("target-levels values must all have one public scalar type.",
           call. = FALSE)
    }
  }
  values <- unlist(levels, use.names = FALSE)
  if (!is.atomic(values) || length(values) < 2L || length(values) > 1024L ||
      anyNA(values)) {
    stop("target-levels must contain 2 to 1024 public, non-missing values.",
         call. = FALSE)
  }
  if (is.character(values)) {
    values <- enc2utf8(values)
    if (any(!nzchar(values))) {
      stop("target-levels cannot contain empty strings.", call. = FALSE)
    }
    level_type <- "character"
  } else if (is.logical(values)) {
    level_type <- "logical"
  } else if (is.numeric(values)) {
    values <- as.numeric(values)
    if (any(!is.finite(values))) {
      stop("Numeric target-levels must be finite.", call. = FALSE)
    }
    level_type <- "numeric"
  } else {
    stop("target-levels must be character, logical, or numeric.", call. = FALSE)
  }
  if (anyDuplicated(values)) {
    stop("target-levels must be unique and ordered.", call. = FALSE)
  }

  expected <- run_config[["num-classes"]] %||% NULL
  if (!is.null(expected)) {
    expected <- suppressWarnings(as.integer(unlist(expected, use.names = FALSE)))
    if (length(expected) != 1L || is.na(expected) ||
        expected != length(values)) {
      stop("target-levels length must equal the public model class count.",
           call. = FALSE)
    }
  }
  run_config[["target-levels"]] <- list(type = level_type, values = values)
  run_config[["target-bounds"]] <- NULL
  run_config
}

.server_owned_run_config_fields <- function() {
  c(
    .manifest_structural_fields()[.manifest_structural_fields() != "data_type"],
    "dp_enabled", "allow_per_node_metrics", "allow_exact_num_examples",
    "fixed_client_sampling", "privacy-adjacency",
    "privacy-policy-sha256", "privacy-epsilon",
    "privacy-delta", "privacy-clipping_norm", "privacy-sample_aggregate",
    "privacy-training-epsilon", "privacy-training-delta",
    "privacy-holdout-epsilon", "privacy-holdout-delta",
    "privacy-cv-training-epsilon", "privacy-cv-training-delta",
    "privacy-cv-fold-epsilon", "privacy-cv-fold-delta",
    "privacy-cv-oof-epsilon", "privacy-cv-oof-delta",
    "privacy-sa_blocks", "privacy-egress_time_pad",
    "privacy-egress_timeout", "privacy-egress_memory_mb",
    "privacy-egress_file_mb", "privacy-egress_processes",
    "privacy-hook_enabled", "user-module", "app-params-sha256",
    "association-contract", "association-privacy-unit",
    "association-unit-semantics"
  )
}

# Exact public run-config syntax accepted from the current dsFlowerClient wire.
# This is input validation, not an authorization or privacy-permission catalogue.
.client_run_config_fields <- function() {
  c(
    "dp-track", "data_type", "task-type", "task_type", "label_set",
    "num-server-rounds", "num-features", "num-classes", "num-labels",
    "feature-bounds", "target-bounds", "target-levels",
    "model-spec-b64", "loss-name", "local-epochs", "batch-size",
    "backbone", "image-size", "vision-extractor-profile",
    "learning-rate", "weight-decay", "l1-penalty",
    "nb-dispersion", "gamma-shape", "huber-delta", "quantile-level",
    "optimizer-name", "optimizer-momentum", "optimizer-nesterov",
    "optimizer-beta1", "optimizer-beta2", "optimizer-eps",
    "optimizer-amsgrad", "optimizer-rmsprop-alpha",
    "scheduler-name", "scheduler-step-size", "scheduler-gamma",
    "scheduler-min-lr",
    "strategy", "strategy-eta", "strategy-eta-l", "strategy-beta-1",
    "strategy-beta-2", "strategy-tau", "strategy-server-learning-rate",
    "strategy-server-momentum",
    "native-tree-request-b64", "native-tree-request-sha256",
    "validation-model-track", "validation-task", "validation-bins",
    "validation-contract-sha256", "validation-native-tree-request-b64",
    "validation-native-tree-request-sha256", "validation-artifact-format",
    "validation-artifact-sha256", "validation-artifact-size-bytes",
    "validation-profile-sha256", "validation-profile-size-bytes",
    "validation-public-schema-sha256",
    "resampling-version", "resampling-method", "resampling-assignment",
    "resampling-test-numerator", "resampling-test-denominator",
    "resampling-privacy-unit", "resampling-unit-canonicalization",
    "resampling-contract-sha256", "holdout-validation-bins",
    "cv-version", "cv-method", "cv-assignment", "cv-folds",
    "cv-privacy-unit", "cv-unit-canonicalization", "cv-contract-sha256",
    "cv-validation-bins", "cv-n-nodes", "cv-job-sha256",
    "association-outcome-levels", "association-exposure-levels",
    "association-contract-sha256", "association-n-nodes",
    "association-job-sha256",
    "app-params-b64"
  )
}

# Validate the untrusted analyst run_config before server-owned privacy fields
# are injected. data_type is intentionally allowed as a routing request; the
# selected branch removes it and writes its own authoritative manifest value.
.validate_client_run_config <- function(run_config) {
  if (!is.list(run_config)) {
    stop("run_config must be a JSON object or named list.", call. = FALSE)
  }
  if (!length(run_config)) return(run_config)
  keys <- names(run_config)
  if (is.null(keys) || length(keys) != length(run_config) ||
      any(is.na(keys) | !nzchar(keys)) || anyDuplicated(keys)) {
    stop("run_config must have unique, non-empty field names.", call. = FALSE)
  }
  conflicts <- intersect(keys, .server_owned_run_config_fields())
  conflicts <- unique(c(
    conflicts,
    keys[startsWith(tolower(keys), "privacy-") |
           startsWith(tolower(keys), "privacy_")]
  ))
  if (length(conflicts)) {
    stop("run_config cannot set server-owned manifest field(s): ",
         paste(conflicts, collapse = ", "), ".", call. = FALSE)
  }
  if ("num_rounds" %in% keys) {
    stop("run_config must use the canonical 'num-server-rounds' field.",
         call. = FALSE)
  }
  unsupported <- setdiff(keys, .client_run_config_fields())
  if (length(unsupported)) {
    stop("run_config contains unsupported field(s): ",
         paste(unsupported, collapse = ", "), ".", call. = FALSE)
  }
  run_config
}

.merge_manifest_config <- function(manifest, extra_config) {
  if (!length(extra_config)) return(manifest)
  if (!is.list(extra_config)) {
    stop("Manifest configuration must be a named list.", call. = FALSE)
  }
  keys <- names(extra_config)
  if (is.null(keys) || any(is.na(keys) | !nzchar(keys)) || anyDuplicated(keys)) {
    stop("Manifest configuration must have unique, non-empty field names.",
         call. = FALSE)
  }
  conflicts <- intersect(keys, names(manifest))
  if (length(conflicts)) {
    stop("Manifest configuration cannot duplicate server-owned field(s): ",
         paste(conflicts, collapse = ", "), ".", call. = FALSE)
  }
  manifest[keys] <- extra_config
  manifest
}

.validate_manifest_extra_config <- function(extra_config) {
  extra_config <- .merge_manifest_config(list(), extra_config)
  conflicts <- intersect(names(extra_config), .manifest_structural_fields())
  if (length(conflicts)) {
    stop("Manifest configuration cannot provide server-owned field(s): ",
         paste(conflicts, collapse = ", "), ".", call. = FALSE)
  }
  extra_config
}

#' Atomically replace a JSON manifest in its own directory
#'
#' Doubles are written with 17 significant digits (\code{digits = I(17)}), not
#' the 15 of \code{digits = NA}: the manifest is the authoritative privacy
#' contract, and the trusted runner's release guard revalidates the fixed
#' cross-validation/holdout budget split against its own IEEE recomputation at
#' \code{rel_tol = 1e-15}. A 15-digit decimal loses the low bits of computed
#' allocations (for example \code{epsilon * 0.8 / 3}), which made every live
#' cross-validation round fail closed as unavailable; 17 digits round-trip the
#' exact double (worst case one ulp, ~2.2e-16 relative, inside the guard's
#' tolerance).
#' @keywords internal
.write_manifest_atomic <- function(manifest, manifest_path) {
  tmp <- tempfile(pattern = ".manifest-", tmpdir = dirname(manifest_path))
  on.exit(unlink(tmp), add = TRUE)
  jsonlite::write_json(manifest, tmp, auto_unbox = TRUE, pretty = TRUE,
                       null = "null", digits = I(17))
  Sys.chmod(tmp, "0600")
  if (!file.rename(tmp, manifest_path)) {
    stop("Could not atomically update the run manifest.", call. = FALSE)
  }
  invisible(manifest_path)
}

#' Bind the stateless privacy contract to a prepared manifest
#' @keywords internal
.apply_privacy_contract <- function(staging_dir, contract) {
  manifest_path <- file.path(staging_dir, "manifest.json")
  if (!file.exists(manifest_path)) {
    stop("Prepared run manifest is missing.", call. = FALSE)
  }
  manifest <- tryCatch(
    jsonlite::fromJSON(manifest_path, simplifyVector = FALSE),
    error = function(e) stop("Prepared run manifest is unreadable.",
                             call. = FALSE))
  if (length(manifest[["run_token"]]) != 1L ||
      !identical(as.character(manifest[["run_token"]]),
                 as.character(contract$run_token))) {
    stop("Privacy contract run token does not match the prepared manifest.",
         call. = FALSE)
  }
  expected_horizon <- as.integer(manifest[["num-server-rounds"]] %||% NA)
  if (is.na(expected_horizon) ||
      expected_horizon != as.integer(contract$num_rounds)) {
    stop("Privacy contract horizon does not match the prepared manifest.",
         call. = FALSE)
  }
  manifest_unit <- as.character(manifest[["dp-unit"]] %||% "")
  contract_unit <- as.character(contract$dp_unit %||% "")
  manifest_patient <- manifest[["patient_column"]] %||% NULL
  contract_patient <- contract$patient_column %||% NULL
  manifest_canonicalization <- as.character(
    manifest[["patient-id-canonicalization"]] %||% "")
  contract_canonicalization <- as.character(
    contract$unit_canonicalization %||% "")
  manifest_adjacency <- as.character(
    manifest[["privacy-adjacency"]] %||% "")
  contract_adjacency <- as.character(contract$adjacency %||% "")
  manifest_epsilon <- suppressWarnings(as.numeric(
    manifest[["privacy-epsilon"]] %||% NA_real_))
  manifest_delta <- suppressWarnings(as.numeric(
    manifest[["privacy-delta"]] %||% NA_real_))
  manifest_policy_hash <- as.character(
    manifest[["privacy-policy-sha256"]] %||% "")
  if (!identical(manifest_unit, contract_unit) ||
      !identical(manifest_patient, contract_patient) ||
      !identical(manifest_canonicalization, contract_canonicalization) ||
      !identical(manifest_adjacency, contract_adjacency) ||
      !identical(manifest_epsilon, as.numeric(contract$epsilon)) ||
      !identical(manifest_delta, as.numeric(contract$delta)) ||
      !identical(manifest_policy_hash, as.character(contract$policy_hash))) {
    stop("Privacy contract does not match the prepared manifest.",
         call. = FALSE)
  }
  .write_manifest_atomic(manifest, manifest_path)
}

#' Stage a validated tabular training frame
#'
#' Writes the selected model columns and server-owned manifest into an isolated
#' run directory. A resolved patient identifier is retained only for local
#' grouping and is never added to model features.
#'
#' @param data Data.frame containing the local training rows.
#' @param run_token Server-generated run identifier.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character vector or NULL; names of feature columns.
#' @param extra_config Named list of additional configuration to include in manifest.
#' @return Character; path to the staging directory.
#' @keywords internal
.stageData <- function(data, run_token, target_column,
                       feature_columns = NULL, extra_config = list()) {
  extra_config <- .validate_manifest_extra_config(extra_config)
  if (identical(extra_config[["dp-track"]], "association")) {
    return(.stageAssociationData(
      data, run_token, target_column, feature_columns, extra_config))
  }
  data <- .transformPublicTarget(data, target_column, extra_config)
  unit <- .prepareDpUnitFrame(data)
  data <- unit$data
  patient_column <- unit$patient_column
  feature_columns <- .resolveModelFeatures(
    data, target_column, feature_columns, patient_column)
  data <- .totalizeModelFeatures(data, feature_columns, extra_config)
  prepared <- .prepareTrainingFrame(
    data,
    target_column = target_column,
    feature_columns = feature_columns,
    drop_missing = FALSE,
    select_columns = TRUE,
    patient_column = patient_column
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
    data_type       = "tabular",
    data_file       = data_file,
    data_format     = data_format,
    n_samples       = prepared$n_samples,
    n_units         = .countDpUnits(
      data, unit$dp_unit, unit$patient_column),
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column   = target_column,
    feature_columns = feature_columns,
    "dp-unit"       = unit$dp_unit,
    patient_column  = patient_column,
    "patient-id-canonicalization" = unit$canonicalization,
    "target-preencoded" = TRUE,
    staged_at       = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  # Merge the server-authored mechanism and public run configuration.
  manifest <- .merge_manifest_config(manifest, extra_config)

  # Write manifest
  manifest <- .normalize_dp_manifest(manifest)
  manifest_path <- file.path(staging_dir, "manifest.json")
  .write_manifest_atomic(manifest, manifest_path)

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

  run_token <- .validate_run_token(run_token)
  # Check every permitted root. Exact token validation and canonical containment
  # happen before recursive deletion; traversal and symlink aliases fail closed.
  for (staging_dir in .expectedStagingDirs(run_token, create_roots = FALSE)) {
    if (dir.exists(staging_dir)) {
      staging_dir <- .validateStagingDir(
        staging_dir, run_token, must_exist = TRUE)
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
    stop("The configured image data root is unavailable.", call. = FALSE)
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
  extra_config <- .validate_manifest_extra_config(extra_config)
  data_root <- .resolve_image_data_root()
  if (is.data.frame(samples_data)) {
    samples_basename <- "samples.csv"
  } else if (is.character(samples_data) && file.exists(samples_data)) {
    samples_basename <- basename(samples_data)
    if (grepl("\\.parquet$", samples_basename, ignore.case = TRUE)) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("arrow package required for Parquet support.", call. = FALSE)
      }
      samples_data <- as.data.frame(arrow::read_parquet(samples_data))
    } else {
      samples_data <- utils::read.csv(samples_data, stringsAsFactors = FALSE)
    }
  } else {
    stop("samples_data must be a data.frame or a path to an existing file.",
         call. = FALSE)
  }

  samples_data <- .transformPublicTarget(
    samples_data, target_column, extra_config)
  unit <- .prepareDpUnitFrame(samples_data)
  prepared <- .prepareTrainingFrame(
    unit$data,
    target_column = target_column,
    feature_columns = character(0),
    drop_missing = FALSE,
    select_columns = FALSE,
    patient_column = unit$patient_column
  )
  samples_data <- prepared$data

  staging_dir <- .ensureStagingDir(run_token)
  staged_samples <- file.path(staging_dir, samples_basename)
  .writeStagedSamples(samples_data, staged_samples)
  Sys.chmod(staged_samples, "0600")

  manifest <- list(
    run_token    = run_token,
    data_type    = "image",
    samples_file = samples_basename,
    n_samples    = prepared$n_samples,
    n_units      = .countDpUnits(
      samples_data, unit$dp_unit, unit$patient_column),
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column = target_column,
    "dp-unit" = unit$dp_unit,
    patient_column = unit$patient_column,
    "patient-id-canonicalization" = unit$canonicalization,
    "target-preencoded" = TRUE,
    assets = list(images = list(
      type = "image_root", root = data_root, path_col = "relative_path")),
    staged_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  manifest <- .merge_manifest_config(manifest, extra_config)

  manifest <- .normalize_dp_manifest(manifest)
  manifest_path <- file.path(staging_dir, "manifest.json")
  .write_manifest_atomic(manifest, manifest_path)

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
  extra_config <- .validate_manifest_extra_config(extra_config)
  kind <- desc$source_kind

  if (identical(extra_config[["dp-track"]], "association") &&
      identical(kind, "image_bundle")) {
    stop("The association track accepts tabular descriptors only.",
         call. = FALSE)
  }

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
      stop("The configured asset file is unavailable.", call. = FALSE)
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
  extra_config <- .validate_manifest_extra_config(extra_config)
  if (identical(extra_config[["dp-track"]], "association")) {
    return(.stageAssociationDescriptorParquet(
      desc, run_token, target_column, feature_columns, extra_config))
  }
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
    stop("The configured Parquet source is unavailable.", call. = FALSE)
  }

  # Determine columns to select
  # Ensure character vector (JSON deserialization may produce a list)
  if (is.list(feature_columns)) feature_columns <- unlist(feature_columns)
  feature_columns <- as.character(feature_columns)
  feature_columns <- feature_columns[nzchar(feature_columns)]
  schema_names <- arrow::open_dataset(src_path, format = "parquet")$schema$names
  schema_proxy <- as.data.frame(
    stats::setNames(rep(list(logical(0)), length(schema_names)), schema_names),
    optional = TRUE
  )
  patient_column <- .detectPatientColumn(schema_proxy, list())
  feature_columns <- .resolveModelFeatures(
    schema_proxy, target_column, feature_columns, patient_column)
  cols_needed <- unique(c(target_column, feature_columns, patient_column))

  staging_dir <- .ensureStagingDir(
    run_token,
    required_bytes = file.info(src_path)$size %||% 0
  )

  # Read with column selection, drop incomplete rows, and write to staging.
  all_of <- utils::getFromNamespace("all_of", "tidyselect")
  tbl <- arrow::read_parquet(src_path, col_select = all_of(cols_needed))
  transformed <- .transformPublicTarget(
    as.data.frame(tbl), target_column, extra_config)
  unit <- .prepareDpUnitFrame(transformed)
  transformed <- .totalizeModelFeatures(
    unit$data, feature_columns, extra_config)
  prepared <- .prepareTrainingFrame(
    transformed,
    target_column = target_column,
    feature_columns = feature_columns,
    drop_missing = FALSE,
    select_columns = TRUE,
    patient_column = patient_column
  )
  data_file <- "train.parquet"
  arrow::write_parquet(prepared$data, file.path(staging_dir, data_file))
  Sys.chmod(file.path(staging_dir, data_file), "0600")

  # Build manifest
  manifest <- list(
    run_token       = run_token,
    data_type       = "tabular",
    data_file       = data_file,
    data_format     = "parquet",
    n_samples       = prepared$n_samples,
    n_units         = .countDpUnits(
      prepared$data, unit$dp_unit, unit$patient_column),
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column   = target_column,
    feature_columns = feature_columns,
    "dp-unit"       = unit$dp_unit,
    patient_column  = patient_column,
    "patient-id-canonicalization" = unit$canonicalization,
    "target-preencoded" = TRUE,
    dataset_id      = desc$dataset_id,
    source_kind     = "staged_parquet",
    staged_at       = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )
  manifest <- .merge_manifest_config(manifest, extra_config)

  manifest <- .normalize_dp_manifest(manifest)
  manifest_path <- file.path(staging_dir, "manifest.json")
  .write_manifest_atomic(manifest, manifest_path)

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

.s3Bucket <- function(uri) {
  sub("^s3://([^/]+).*$", "\\1", uri)
}

#' Validate one path relative to a staged image root
#'
#' Image paths can originate in object metadata.  Keep them as portable,
#' lexical relative paths so neither R nor the Python runner can interpret an
#' absolute path, a Windows separator, or a traversal component.
#' @keywords internal
.safeRelativeAssetPath <- function(path) {
  if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path) || grepl("[[:cntrl:]]", path) ||
      grepl("\\\\", path) || grepl("^/", path) ||
      grepl("^[A-Za-z]:", path)) {
    stop("Image metadata contains an unsafe relative path.", call. = FALSE)
  }
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  if (!length(parts) || any(!nzchar(parts)) || any(parts %in% c(".", ".."))) {
    stop("Image metadata contains an unsafe relative path.", call. = FALSE)
  }
  paste(parts, collapse = "/")
}

#' Map one private image path into the staged record domain
#'
#' Invalid values are record-local data, not descriptor errors.  Return a fixed
#' marker so one bad record neither changes cardinality nor aborts preparation.
#' The strict validator above remains available for public descriptor paths.
#' @keywords internal
.totalizeImageRecordPath <- function(path) {
  tryCatch(
    .safeRelativeAssetPath(path),
    error = function(e) .DSFLOWER_INVALID_IMAGE_PATH
  )
}

#' Keep a staged image path only when it resolves to a regular file in root
#' @keywords internal
.totalizeImageRecordAtRoot <- function(path, image_root) {
  path <- .totalizeImageRecordPath(path)
  if (identical(path, .DSFLOWER_INVALID_IMAGE_PATH)) return(path)
  if (is.null(image_root)) return(path)
  if (!is.character(image_root) || length(image_root) != 1L ||
      is.na(image_root) || !dir.exists(image_root)) {
    stop("The configured image asset root is unavailable.", call. = FALSE)
  }

  root <- normalizePath(image_root, winslash = "/", mustWork = TRUE)
  candidate <- file.path(root, path)
  if (.path_is_symlink(candidate)) {
    return(.DSFLOWER_INVALID_IMAGE_PATH)
  }
  resolved <- tryCatch(
    normalizePath(candidate, winslash = "/", mustWork = TRUE),
    error = function(e) NA_character_
  )
  root_prefix <- paste0(sub("/+$", "", root), "/")
  if (is.na(resolved) ||
      !startsWith(resolved, root_prefix) ||
      !utils::file_test("-f", resolved)) {
    return(.DSFLOWER_INVALID_IMAGE_PATH)
  }
  path
}

.s3RelativePath <- function(uri, prefix) {
  if (!is.character(uri) || length(uri) != 1L || is.na(uri) ||
      !grepl("^s3://[^/]+/", uri) ||
      !is.character(prefix) || length(prefix) != 1L || is.na(prefix) ||
      !grepl("^s3://[^/]+(/|$)", prefix) ||
      !identical(.s3Bucket(uri), .s3Bucket(prefix))) {
    stop("Image metadata contains an object outside its configured S3 prefix.",
         call. = FALSE)
  }
  key <- .s3ObjectKey(uri)
  prefix_key <- sub("/+$", "", .s3ObjectKey(prefix))
  rel <- if (!nzchar(prefix_key)) {
    key
  } else if (identical(key, prefix_key)) {
    basename(key)
  } else {
    marker <- paste0(prefix_key, "/")
    if (!startsWith(key, marker)) {
      stop("Image metadata contains an object outside its configured S3 prefix.",
           call. = FALSE)
    }
    substring(key, nchar(marker) + 1L)
  }
  .safeRelativeAssetPath(rel)
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
  local_root <- normalizePath(local_root, winslash = "/", mustWork = TRUE)
  rel_paths <- character(0)
  for (f in files) {
    # One malformed object key is a record-local asset failure. Skip it here;
    # its metadata row is retained later with the fixed zero-image marker.
    rel <- tryCatch(
      .s3RelativePath(f, s3_uri),
      error = function(e) NA_character_
    )
    if (is.na(rel)) next
    if (!nzchar(rel) || .isDirectoryLikeObject(rel)) next
    local_path <- file.path(local_root, rel)
    dir.create(dirname(local_path), recursive = TRUE, showWarnings = FALSE)
    parent <- normalizePath(dirname(local_path), winslash = "/", mustWork = TRUE)
    if (!(identical(parent, local_root) ||
          startsWith(parent, paste0(local_root, "/"))) ||
        .path_is_symlink(local_path)) {
      stop("Image metadata resolves outside its staged asset root.",
           call. = FALSE)
    }
    if (!file.exists(local_path)) {
      dsImaging::backend_get_file(backend, f, local_path)
    }
    if (!file.exists(local_path) || dir.exists(local_path) ||
        .path_is_symlink(local_path)) {
      stop("The image backend did not produce a safe regular file.",
           call. = FALSE)
    }
    resolved <- normalizePath(local_path, winslash = "/", mustWork = TRUE)
    if (!startsWith(resolved, paste0(local_root, "/"))) {
      stop("Image metadata resolves outside its staged asset root.",
           call. = FALSE)
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

#' Left-join private image labels without changing the sample roster
#'
#' Missing, blank, or duplicate label identifiers are record-local failures:
#' they produce missing label cells which the public target contract totalizes.
#' Missing join columns remain a descriptor-schema error.
#' @keywords internal
.leftJoinImageLabels <- function(samples_df, labels_df) {
  if (!is.data.frame(samples_df) || !is.data.frame(labels_df) ||
      !("sample_id" %in% names(samples_df)) ||
      !("sample_id" %in% names(labels_df))) {
    stop("Image sample and label metadata must contain a sample_id column.",
         call. = FALSE)
  }
  label_columns <- setdiff(names(labels_df), "sample_id")
  if (!length(label_columns)) {
    stop("Image label metadata must contain at least one label column.",
         call. = FALSE)
  }

  sample_ids <- .canonicalPatientIdText(samples_df$sample_id)
  label_ids <- .canonicalPatientIdText(labels_df$sample_id)
  sample_invalid <- .invalidPatientIds(sample_ids)
  label_invalid <- .invalidPatientIds(label_ids)
  label_duplicate <- duplicated(label_ids) | duplicated(label_ids, fromLast = TRUE)
  label_ids[label_invalid | label_duplicate] <- NA_character_

  match_index <- match(sample_ids, label_ids)
  match_index[sample_invalid] <- NA_integer_
  for (column in label_columns) {
    samples_df[[column]] <- labels_df[[column]][match_index]
  }
  samples_df
}

.primaryFromFilesJson <- function(files_json) {
  if (is.null(files_json) || length(files_json) != 1L || is.na(files_json) ||
      !is.character(files_json) || !nzchar(files_json)) {
    return(NA_character_)
  }
  parsed <- tryCatch(
    jsonlite::fromJSON(files_json, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (!is.list(parsed) || length(parsed) == 0L) return(NA_character_)
  roles <- vapply(parsed, function(x) {
    if (!is.list(x)) return("")
    role <- x$role %||% ""
    if (length(role) != 1L || is.na(role)) return("")
    as.character(role)
  }, character(1))
  idx <- which(roles %in% c("image", "primary"))
  item <- parsed[[if (length(idx) > 0L) idx[[1]] else 1L]]
  if (!is.list(item)) return(NA_character_)
  value <- item$path %||% item$uri %||% item$file %||% NA_character_
  if (length(value) != 1L || is.na(value)) return(NA_character_)
  as.character(value)
}

.normalisePrimaryPath <- function(primary, image_uri) {
  primary <- as.character(primary %||% NA_character_)
  if (is.na(primary) || !nzchar(primary)) return(NA_character_)
  if (grepl("^s3://", primary)) return(.s3RelativePath(primary, image_uri %||% primary))
  if (grepl("^/", primary) || grepl("\\\\", primary) ||
      grepl("^[A-Za-z]:", primary)) {
    stop("Image metadata contains an unsafe relative path.", call. = FALSE)
  }
  image_key <- if (!is.null(image_uri) && grepl("^s3://", image_uri)) {
    .s3ObjectKey(image_uri)
  } else {
    ""
  }
  image_key <- paste0(sub("/+$", "", image_key), "/")
  if (nzchar(image_key)) {
    primary <- sub(paste0("^", .regex_escape(image_key)), "", primary)
  }
  .safeRelativeAssetPath(sub("^source/images/", "", primary))
}

.ensureImagePathColumn <- function(samples_df, path_col = "relative_path",
                                   sample_manifests = NULL,
                                   image_root = NULL,
                                   image_uri = NULL,
                                   downloaded_rels = character(0)) {
  if (!is.character(path_col) || length(path_col) != 1L || is.na(path_col) ||
      !nzchar(path_col)) {
    stop("Image descriptors require one non-empty path column name.",
         call. = FALSE)
  }
  has_path <- path_col %in% names(samples_df)
  has_sample_id <- "sample_id" %in% names(samples_df)
  if (!has_path && !has_sample_id) {
    stop("Image metadata requires either '", path_col, "' or 'sample_id'.",
         call. = FALSE)
  }

  rel_paths <- rep(NA_character_, nrow(samples_df))
  if (has_path) {
    current <- as.character(samples_df[[path_col]])
    rel_paths <- vapply(
      current,
      function(path) {
        value <- .totalizeImageRecordPath(path)
        if (identical(value, .DSFLOWER_INVALID_IMAGE_PATH)) NA_character_ else value
      },
      character(1)
    )
  }
  sample_ids <- if (has_sample_id) {
    as.character(samples_df$sample_id)
  } else {
    rep(NA_character_, nrow(samples_df))
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel) && has_sample_id && !is.null(sample_manifests) &&
      "sample_id" %in% names(sample_manifests)) {
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
    primary <- vapply(primary, function(path) {
      tryCatch(
        .normalisePrimaryPath(path, image_uri),
        error = function(e) NA_character_
      )
    }, character(1))
    matched <- primary[match(sample_ids, sm_ids)]
    rel_paths[missing_rel] <- matched[missing_rel]
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel) && has_sample_id && length(downloaded_rels) > 0L) {
    downloaded_rels <- vapply(
      downloaded_rels, .totalizeImageRecordPath, character(1))
    downloaded_rels <- downloaded_rels[
      downloaded_rels != .DSFLOWER_INVALID_IMAGE_PATH]
    rel_by_stem <- downloaded_rels
    names(rel_by_stem) <- .stripKnownImageExtension(downloaded_rels)
    rel_paths[missing_rel] <- rel_by_stem[sample_ids[missing_rel]]
  }

  missing_rel <- is.na(rel_paths) | !nzchar(rel_paths)
  if (any(missing_rel) && !is.null(image_root) && dir.exists(image_root)) {
    for (i in which(missing_rel)) {
      stem <- tryCatch(
        .safeRelativeAssetPath(sample_ids[[i]]),
        error = function(e) NA_character_)
      if (is.na(stem) || grepl("/", stem, fixed = TRUE)) next
      for (ext in .knownImageExtensions()) {
        candidate <- paste0(stem, ext)
        if (file.exists(file.path(image_root, candidate))) {
          rel_paths[[i]] <- candidate
          break
        }
      }
    }
  }

  samples_df[[path_col]] <- unname(vapply(rel_paths, function(path) {
    .totalizeImageRecordAtRoot(path, image_root)
  }, character(1)))
  samples_df
}

.imageAssetNeedsStaging <- function(asset_name, asset_type, extra_config) {
  if (identical(asset_type, "mask_root")) {
    return(!is.null(extra_config[["mask_asset"]]) ||
             !is.null(extra_config[["mask_path_col"]]))
  }
  asset_name == "images" || asset_type %in% c("image_root", "wsi_root",
                                              "dicom_series_root",
                                              "rt_struct_root")
}

#' Stage from an image bundle descriptor
#'
#' Handles multi-root image assets from the descriptor. Local images remain in
#' place; S3-backed images are copied into private run staging. The manifest
#' includes an \code{assets} key mapping asset names to their validated roots.
#' Private record values are totalized: an invalid/unavailable path becomes a
#' fixed zero-image marker, invalid labels become their public target default,
#' and no row is removed. Descriptor shape, metadata source, backend, and asset
#' roots are global valid-input preconditions and still fail closed.
#'
#' @keywords internal
.stageFromDescriptor_image <- function(desc, run_token, target_column,
                                        feature_columns, extra_config) {
  extra_config <- .validate_manifest_extra_config(extra_config)
  meta <- desc$metadata
  assets <- desc$assets
  if (!is.list(assets) ||
      (length(assets) > 0L &&
       (is.null(names(assets)) || any(!nzchar(names(assets))) ||
        anyDuplicated(names(assets))))) {
    stop("Image descriptors require unique, named assets.", call. = FALSE)
  }
  if (length(assets) > 0L) {
    if (any(!grepl("^[A-Za-z][A-Za-z0-9_.-]{0,63}$", names(assets)))) {
      stop("Image asset names must be safe single components.",
           call. = FALSE)
    }
  }
  dir_asset_types <- c("image_root", "mask_root", "wsi_root",
                       "dicom_series_root", "rt_struct_root")
  file_asset_types <- c("feature_table", "rt_dose_file", "rt_plan_file")

  s3_metadata <- !is.null(meta$uri) && grepl("^s3://", meta$uri)
  s3_sample_manifests <- !is.null(desc$manifest$sample_manifests$uri) &&
    grepl("^s3://", desc$manifest$sample_manifests$uri)
  selected_label <- extra_config[["label_set"]] %||% NULL
  selected_label_uri <- NULL
  if (!is.null(selected_label)) {
    for (label in desc$manifest$labels %||% list()) {
      if (identical(label$name, selected_label)) {
        selected_label_uri <- label$uri %||% NULL
        break
      }
    }
  }
  s3_labels <- !is.null(selected_label_uri) &&
    grepl("^s3://", selected_label_uri)
  s3_assets <- any(vapply(names(assets), function(asset_name) {
    asset <- assets[[asset_name]]
    asset_type <- asset$type %||% asset$kind %||% "unknown"
    uri <- asset$uri %||% ""
    .imageAssetNeedsStaging(asset_name, asset_type, extra_config) &&
      grepl("^s3://", uri)
  }, logical(1)))
  if (is.null(desc$backend) &&
      (s3_metadata || s3_sample_manifests || s3_labels || s3_assets)) {
    stop("Image bundle descriptor uses S3 objects but has no storage backend. ",
         "Initialize it with dsImaging before passing it to dsFlower.",
         call. = FALSE)
  }

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
      labels_df <- as.data.frame(arrow::read_parquet(label_file))
      merged <- .leftJoinImageLabels(samples_df, labels_df)
      .writeStagedSamples(merged, staged_samples)
      message("  Joined label set '", label_set_name, "' (",
              ncol(labels_df) - 1, " label columns)")
    }
    unlink(label_file)
  }

  samples_df <- .readStagedSamples(staged_samples)
  samples_df <- .transformPublicTarget(
    samples_df, target_column, extra_config)
  unit <- .prepareDpUnitFrame(samples_df)
  patient_column <- unit$patient_column
  prepared <- .prepareTrainingFrame(
    unit$data,
    target_column = target_column,
    feature_columns = character(0),
    drop_missing = FALSE,
    select_columns = FALSE,
    patient_column = patient_column
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
        message("  Asset staging complete")
      }

      if (is.null(root) && !.imageAssetNeedsStaging(asset_name, asset_type,
                                                     extra_config)) {
        next
      }
      if (is.null(root) || !dir.exists(root)) {
        stop("A configured image asset root is unavailable.", call. = FALSE)
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
        stop("A configured feature asset is unavailable.", call. = FALSE)
      }
      validated_assets[[asset_name]] <- list(
        type     = asset_type,
        file     = normalizePath(feat_file, mustWork = TRUE),
        join_key = asset$join_key %||% NULL
      )

    } else if (identical(asset_type, "multimodal_ref")) {
      mpath <- asset$manifest
      if (is.null(mpath) || !file.exists(mpath)) {
        stop("A configured multimodal manifest is unavailable.", call. = FALSE)
      }
      validated_assets[[asset_name]] <- list(
        type     = asset_type,
        manifest = normalizePath(mpath, mustWork = TRUE),
        modality = asset$modality %||% NULL
      )
    }
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

  # Build manifest. patient_column is the column the disclosure admission grouped
  # by (.detectPatientColumn); pinning it here makes the harness train per-PATIENT
  # on the SAME column, so the DP unit matches the admission unit.
  manifest <- list(
    run_token     = run_token,
    data_type     = "image",
    samples_file  = samples_basename,
    n_samples     = n_samples,
    n_units       = .countDpUnits(
      samples_df, unit$dp_unit, unit$patient_column),
    n_input_samples = prepared$n_input_samples,
    dropped_missing = prepared$dropped_missing,
    target_column = target_column,
    "dp-unit" = unit$dp_unit,
    patient_column = patient_column,
    "patient-id-canonicalization" = unit$canonicalization,
    "target-preencoded" = TRUE,
    dataset_id    = desc$dataset_id,
    source_kind   = "image_bundle",
    assets        = validated_assets,
    staged_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  manifest <- .merge_manifest_config(manifest, extra_config)

  manifest <- .normalize_dp_manifest(manifest)
  manifest_path <- file.path(staging_dir, "manifest.json")
  .write_manifest_atomic(manifest, manifest_path)

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
