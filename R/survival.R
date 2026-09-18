# Public survival semantics and node-local subject assembly. All counts and
# validity bits remain in the staging directory; they are never release fields.
.SURVIVAL_LOSSES <- c("aft_weibull_nll", "aft_lognormal_nll", "discrete_hazard_nll")

.isSurvivalConfig <- function(run_config) {
  identical(run_config[["task-type"]], "survival") ||
    isTRUE(run_config[["loss-name"]] %in% .SURVIVAL_LOSSES)
}

.normalizeSurvivalConfig <- function(run_config, track, unit_policy = NULL) {
  supplied <- run_config[["survival-config-b64"]] %||% NULL
  requested <- tolower(as.character(unlist(
    run_config[["task-type"]] %||% run_config[["task_type"]] %||% "",
    use.names = FALSE)))
  loss <- tolower(as.character(unlist(run_config[["loss-name"]] %||% "",
                                     use.names = FALSE)))
  survival <- identical(requested, "survival") ||
    isTRUE(loss %in% .SURVIVAL_LOSSES) || !is.null(supplied)
  if (!survival) return(run_config)
  if (!identical(track, "neural") || !isTRUE(loss %in% .SURVIVAL_LOSSES)) {
    stop("Survival requires the trusted neural survival loss contract; private ",
         "validation and other tracks are unsupported.", call. = FALSE)
  }
  run_config[["loss-name"]] <- loss
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
  hazard <- identical(loss, "discrete_hazard_nll")
  required <- c(common, if (hazard) "edges" else
                  c("time_scale", "distribution", "dispersion"))
  if (!is.list(value) || is.null(names(value)) || anyDuplicated(names(value)) ||
      !setequal(names(value), required)) {
    stop("survival-config has missing, unknown or duplicate fields.", call. = FALSE)
  }
  scalar_number <- function(x) {
    is.numeric(x) && length(x) == 1L && is.finite(x)
  }
  # Survival head width comes only from its loss/grid, never class vocabulary.
  # Pin unused compatibility fields so they cannot create fresh sticky noise.
  for (field in c("num-classes", "num-labels")) {
    pin <- run_config[[field]] %||% 2L
    if (!scalar_number(pin) || pin != 2) {
      stop("Survival num-classes and num-labels compatibility pins must equal 2.",
           call. = FALSE)
    }
    run_config[[field]] <- 2L
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
  for (field in c("t_min", "horizon", if (!hazard) "time_scale")) {
    if (!scalar_number(value[[field]]) || value[[field]] < 1e-6 ||
        value[[field]] > 1e6) {
      stop("Survival public time fields must be finite numbers in [1e-6, 1e6].",
           call. = FALSE)
    }
  }
  if (value$t_min > value$horizon) {
    stop("Survival t_min must not exceed horizon.", call. = FALSE)
  }
  if (hazard) {
    if (!is.list(value$edges) || !is.null(names(value$edges)) ||
        length(value$edges) < 2L || length(value$edges) > 65L ||
        !all(vapply(value$edges, scalar_number, logical(1)))) {
      stop("Hazard edges must define between 1 and 64 finite public intervals.",
           call. = FALSE)
    }
    edges <- unlist(value$edges, use.names = FALSE)
    if (edges[[1L]] != 0 || any(diff(edges) <= 0) ||
        utils::tail(edges, 1L) != value$horizon) {
      stop("Hazard edges must increase strictly from zero to horizon.", call. = FALSE)
    }
    value$edges <- edges
  } else {
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
    is.finite(time) & time >= config$t_min & is.finite(event) & event %in% c(0, 1)
  event[!valid | time > config$horizon] <- 0
  time <- pmin(config$horizon, pmax(config$t_min, time))
  time[!valid] <- config$t_min
  subject_data <- data[first, c(pcol, features), drop = FALSE]
  for (feature in features) subject_data[[feature]][!valid] <- 0
  subject_data[["__survival_time"]] <- time
  subject_data[["__survival_event"]] <- event
  subject_data[["__survival_valid"]] <- as.integer(valid)
  targets <- c("__survival_time", "__survival_event", "__survival_valid")
  if (identical(manifest[["loss-name"]], "discrete_hazard_nll")) {
    periods <- .stageHazardTargets(time, event, valid, config$edges)
    subject_data <- cbind(subject_data, periods)
    targets <- c(targets, names(periods))
  }
  file <- "survival_subjects.csv"
  .writeSurvivalCsv(subject_data, file.path(staging_dir, file))
  Sys.chmod(file.path(staging_dir, file), "0600")
  manifest$survival_file <- file
  manifest$survival_schema <- "subject_survival_v1"
  manifest$survival_shape <- c(length(subjects), length(features), length(targets))
  # Keep the public ordered feature contract an array even for one covariate.
  manifest$feature_columns <- as.list(features)
  manifest$survival_feature_columns <- as.list(features)
  manifest$survival_target_columns <- targets
  if (!is.null(manifest[["feature-bounds"]])) {
    manifest[["feature-bounds"]]$lower <- as.list(manifest[["feature-bounds"]]$lower)
    manifest[["feature-bounds"]]$upper <- as.list(manifest[["feature-bounds"]]$upper)
  }
  manifest
}

# Preserve interval-boundary doubles on installations without Arrow. The
# default CSV writer can round across a public edge before Python revalidation.
.writeSurvivalCsv <- function(data, path) {
  for (column in names(data)) {
    if (is.numeric(data[[column]])) {
      data[[column]] <- format(data[[column]], digits = 17L, scientific = TRUE,
                               trim = TRUE)
    }
  }
  utils::write.csv(data, path, row.names = FALSE)
}

# A censor contributes only through completed interval ends. An event includes
# its terminal interval, including an event exactly on an interval end.
.stageHazardTargets <- function(time, event, valid, edges) {
  ends <- edges[-1L]
  k <- length(ends)
  d <- matrix(0, nrow = length(time), ncol = k)
  m <- matrix(0, nrow = length(time), ncol = k)
  for (i in which(valid)) {
    if (event[[i]] == 1) {
      terminal <- which(time[[i]] <= ends)[[1L]]
      d[i, terminal] <- 1
      m[i, seq_len(terminal)] <- 1
    } else {
      m[i, ] <- as.numeric(ends <= time[[i]])
    }
  }
  colnames(d) <- paste0("__survival_d_", seq_len(k))
  colnames(m) <- paste0("__survival_m_", seq_len(k))
  as.data.frame(cbind(d, m), check.names = FALSE)
}
