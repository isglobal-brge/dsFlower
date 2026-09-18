# Public survival semantics and node-local subject assembly. All counts and
# validity bits remain in the staging directory; they are never release fields.
.SURVIVAL_LOSSES <- c("aft_weibull_nll", "aft_lognormal_nll")

.isSurvivalConfig <- function(run_config) {
  identical(run_config[["task-type"]], "survival") ||
    isTRUE(run_config[["loss-name"]] %in% .SURVIVAL_LOSSES)
}

.normalizeSurvivalConfig <- function(run_config, track, unit_policy = NULL) {
  supplied <- run_config[["survival-config-b64"]] %||% NULL
  requested <- run_config[["task-type"]] %||% run_config[["task_type"]] %||% ""
  loss <- run_config[["loss-name"]] %||% ""
  survival <- identical(requested, "survival") ||
    isTRUE(loss %in% .SURVIVAL_LOSSES) || !is.null(supplied)
  if (!survival) return(run_config)
  if (!identical(track, "neural") || !isTRUE(loss %in% .SURVIVAL_LOSSES)) {
    stop("Survival requires the trusted neural survival loss contract; private ",
         "validation and other tracks are unsupported.", call. = FALSE)
  }
  if (any(grepl("^(validation-|resampling-|holdout-|cv-)", names(run_config)))) {
    stop("Survival private validation, holdout and CV are unsupported.",
         call. = FALSE)
  }
  if (!identical(run_config[["data_type"]] %||% "tabular", "tabular")) {
    stop("Survival requires tabular baseline covariates.", call. = FALSE)
  }
  if (!identical(.resolvePrivacyUnitPolicy(unit_policy)$dp_unit, "patient")) {
    stop("Survival requires custodian-configured patient privacy units.",
         call. = FALSE)
  }
  if (!is.character(supplied) || length(supplied) != 1L || is.na(supplied) ||
      !nzchar(supplied) || nchar(supplied, type = "bytes") > 87384L) {
    stop("survival-config-b64 must be one bounded canonical base64 string.",
         call. = FALSE)
  }
  decoded <- tryCatch(jsonlite::base64_dec(supplied), error = function(e) NULL)
  if (is.null(decoded) || length(decoded) > 65536L ||
      !identical(gsub("[\r\n]", "", jsonlite::base64_enc(decoded)), supplied)) {
    stop("survival-config-b64 must be canonical base64.", call. = FALSE)
  }
  value <- tryCatch(jsonlite::fromJSON(rawToChar(decoded), simplifyVector = FALSE),
                    error = function(e) NULL)
  common <- c("schema_version", "time_unit", "time_origin", "t_min", "horizon")
  required <- c(common, "time_scale", "distribution", "dispersion")
  if (!is.list(value) || is.null(names(value)) || anyDuplicated(names(value)) ||
      !setequal(names(value), required)) {
    stop("survival-config has missing, unknown or duplicate fields.", call. = FALSE)
  }
  scalar_number <- function(x) {
    is.numeric(x) && length(x) == 1L && is.finite(x)
  }
  n_features <- run_config[["num-features"]]
  if (!scalar_number(n_features) || n_features < 1 ||
      n_features != floor(n_features)) {
    stop("Survival requires one positive integer num-features.", call. = FALSE)
  }
  if (!scalar_number(value$schema_version) || value$schema_version != 1 ||
      !identical(value$time_unit, "days") ||
      !identical(value$time_origin, "baseline")) {
    stop("Survival schema 1 requires time_unit='days' and time_origin='baseline'.",
         call. = FALSE)
  }
  for (field in c("t_min", "horizon", "time_scale")) {
    if (!scalar_number(value[[field]]) || value[[field]] < 1e-6 ||
        value[[field]] > 1e6) {
      stop("Survival public time fields must be finite numbers in [1e-6, 1e6].",
           call. = FALSE)
    }
  }
  if (value$t_min > value$horizon) {
    stop("Survival t_min must not exceed horizon.", call. = FALSE)
  }
  expected <- if (identical(loss, "aft_weibull_nll")) "weibull" else "lognormal"
  if (!identical(value$distribution, expected) ||
      !scalar_number(value$dispersion) || !value$dispersion %in% c(0.5, 1, 2)) {
    stop("AFT distribution must match its loss and dispersion must be 0.5, 1 or 2.",
         call. = FALSE)
  }
  if (identical(expected, "weibull") &&
      value$dispersion * (log(value$horizon / value$time_scale) + 10) > 60) {
    stop("Weibull public time domain exceeds the safe exponent bound.",
         call. = FALSE)
  }
  value <- value[required]
  value$schema_version <- 1L
  run_config[["survival-config"]] <- value
  run_config[["survival-config-b64"]] <- gsub("[\r\n]", "", jsonlite::base64_enc(
    charToRaw(as.character(jsonlite::toJSON(value, auto_unbox = TRUE,
                                            digits = I(17))))))
  run_config
}

.validateSurvivalColumns <- function(run_config, target_column, feature_columns,
                                     unit_policy = NULL) {
  if (!.isSurvivalConfig(run_config)) return(invisible(TRUE))
  policy <- .resolvePrivacyUnitPolicy(unit_policy)
  if (!identical(policy$dp_unit, "patient")) {
    stop("Survival requires custodian-configured patient privacy units.",
         call. = FALSE)
  }
  if (length(target_column) != 2L || anyNA(target_column) ||
      anyDuplicated(target_column) || !length(feature_columns) ||
      anyNA(feature_columns) || anyDuplicated(feature_columns) ||
      length(run_config[["num-features"]]) != 1L ||
      length(feature_columns) != run_config[["num-features"]] ||
      length(intersect(target_column, feature_columns)) ||
      policy$patient_column %in% c(target_column, feature_columns) ||
      any(startsWith(c(target_column, feature_columns, policy$patient_column),
                     "__survival_"))) {
    stop("Survival requires two ordered targets and explicit feature columns ",
         "matching num-features; time, event and patient roles must be distinct ",
         "and the __survival_ prefix is reserved.", call. = FALSE)
  }
  bounds <- run_config[["feature-bounds"]]
  if (!is.null(bounds) && length(bounds$lower) != length(feature_columns)) {
    stop("Survival feature-bounds must match the ordered feature columns.",
         call. = FALSE)
  }
  invisible(TRUE)
}

.stageSurvivalTargets <- function(data, manifest, staging_dir) {
  config <- manifest[["survival-config"]]
  features <- as.character(manifest$feature_columns)
  pcol <- manifest$patient_column
  ids <- data[[pcol]]
  subjects <- unique(ids)
  first <- match(subjects, ids)
  single <- tabulate(match(ids, subjects), nbins = length(subjects)) == 1L
  time <- .coerceNumericOrMissing(data[[manifest$target_column[[1L]]]])[first]
  event <- .coerceNumericOrMissing(data[[manifest$target_column[[2L]]]])[first]
  valid <- single & subjects != "__dsflower_missing_patient_unit__" &
    is.finite(time) & time > 0 & is.finite(event) & event %in% c(0, 1)
  event[!valid | time > config$horizon] <- 0
  time <- pmin(config$horizon, pmax(config$t_min, time))
  time[!valid] <- config$t_min
  subject_data <- data[first, c(pcol, features), drop = FALSE]
  for (feature in features) subject_data[[feature]][!valid] <- 0
  subject_data[["__survival_time"]] <- time
  subject_data[["__survival_event"]] <- event
  subject_data[["__survival_valid"]] <- as.integer(valid)
  targets <- c("__survival_time", "__survival_event", "__survival_valid")
  file <- "survival_subjects.csv"
  utils::write.csv(subject_data, file.path(staging_dir, file), row.names = FALSE)
  Sys.chmod(file.path(staging_dir, file), "0600")
  manifest$survival_file <- file
  manifest$survival_schema <- "subject_survival_v1"
  manifest$survival_shape <- c(length(subjects), length(features), length(targets))
  manifest$survival_feature_columns <- features
  manifest$survival_target_columns <- targets
  manifest
}
