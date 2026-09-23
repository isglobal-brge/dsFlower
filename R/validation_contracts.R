# Public fixed metric geometry is shared by independent validation, holdout and
# pooled out-of-fold evaluation. No metric choice depends on private observations.
.normalizePrivateMetricConfig <- function(run_config) {
  fields <- c("validation-survival-horizons", "validation-survival-nll-bound")
  supplied <- intersect(fields, names(run_config))
  evaluating <- identical(run_config[["dp-track"]], "validation") ||
    !is.null(run_config[["resampling-contract-sha256"]]) ||
    !is.null(run_config[["cv-contract-sha256"]])
  if (!.isSurvivalConfig(run_config) || !evaluating) {
    if (length(supplied)) {
      stop("Survival metric fields require survival private evaluation.", call. = FALSE)
    }
    return(run_config)
  }
  config <- run_config[["survival-config"]]
  raw <- run_config[["validation-survival-horizons"]] %||%
    as.character(jsonlite::toJSON(as.numeric(config$horizon), auto_unbox = FALSE,
                                  digits = I(17)))
  horizons <- if (is.character(raw) && length(raw) == 1L && !is.na(raw) &&
                 nchar(raw, type = "bytes") <= 4096L) {
    tryCatch(jsonlite::fromJSON(raw, simplifyVector = FALSE), error = function(e) NULL)
  } else NULL
  if (!is.list(horizons) || !is.null(names(horizons)) ||
      length(horizons) < 1L || length(horizons) > 64L ||
      !all(vapply(horizons, function(x) is.numeric(x) && !is.logical(x) &&
        length(x) == 1L && is.finite(x), logical(1)))) {
    stop("Survival metric horizons must be a public JSON array of 1 to 64 numbers.",
         call. = FALSE)
  }
  horizons <- as.numeric(unlist(horizons, use.names = FALSE))
  if (any(horizons < config$t_min) || any(horizons > config$horizon) ||
      any(diff(horizons) <= 0)) {
    stop("Survival metric horizons must increase within the fitted time domain.",
         call. = FALSE)
  }
  bound <- run_config[["validation-survival-nll-bound"]] %||% 20
  if (!is.numeric(bound) || is.logical(bound) || length(bound) != 1L ||
      !is.finite(bound) || bound <= 0 || bound > 1000) {
    stop("Survival NLL bound must be one public number in (0, 1000].", call. = FALSE)
  }
  run_config[["validation-survival-horizons"]] <- as.character(jsonlite::toJSON(
    horizons, auto_unbox = FALSE, digits = I(17)))
  run_config[["validation-survival-nll-bound"]] <- as.numeric(bound)
  run_config
}

# This function is mirrored by the client. Append only new contract fields so
# existing tabular/classification identities retain their established encoding.
.validationCvContractExtension <- function(run_config) {
  out <- list()
  if (identical(run_config[["task-type"]], "segmentation") ||
      identical(run_config[["validation-task"]], "segmentation")) {
    keys <- c("backbone", "image-size", "vision-extractor-profile",
      "image_asset", "image_path_col", "mask_asset", "mask_path_col",
      "sample_id_col", "mask_empty_col", "mask-vocabulary",
      "segmentation-alpha", "segmentation-smooth", "segmentation-selection",
      "segmentation-checkpoint-sha256", "segmentation-output-shape",
      "segmentation-preprocessing")
    out$segmentation <- run_config[intersect(keys, names(run_config))]
    for (key in intersect(c("segmentation-alpha", "segmentation-smooth"),
                          names(out$segmentation))) {
      out$segmentation[[key]] <- as.numeric(out$segmentation[[key]])
    }
    if (!is.null(out$segmentation[["image-size"]])) {
      out$segmentation[["image-size"]] <- as.integer(out$segmentation[["image-size"]])
    }
  } else if (!is.null(run_config[["cv-contract-sha256"]]) &&
             !is.null(run_config[["backbone"]])) {
    out$vision <- run_config[c("backbone", "image-size", "vision-extractor-profile")]
  }
  if (!is.null(run_config[["survival-config-b64"]])) {
    out$survival <- run_config[intersect(c("survival-config-b64",
      "validation-survival-horizons", "validation-survival-nll-bound"), names(run_config))]
  }
  if (!is.null(run_config[["public-initialisation-manifest-sha256"]])) {
    out$public_initialisation <- run_config[c("public-initialisation-origin",
      "public-initialisation-manifest-sha256", "public-initialisation-checkpoint-sha256",
      "public-initialisation-encoder-sha256", "public-initialisation-identity-version")]
  }
  out
}

.validatePreparedPublicCheckpoint <- function(run_config, feature_columns, target_column) {
  if (is.null(run_config[["public-initialisation-provenance"]]) ||
      .segmentationRequested(run_config)) return(invisible(TRUE))
  manifest <- run_config[["public-initialisation-provenance"]]$provenance$manifest
  if (!identical(manifest$role, "tabular_model") ||
      !identical(manifest$model_id, "declarative_neural")) {
    stop("Public checkpoint does not match the trusted tabular model contract.", call. = FALSE)
  }
  same <- function(a, b) identical(as.character(jsonlite::toJSON(a,
    auto_unbox = FALSE, null = "null", digits = I(17))),
    as.character(jsonlite::toJSON(b, auto_unbox = FALSE, null = "null", digits = I(17))))
  for (key in c("loss-name", "num-features", "num-classes", "num-labels")) {
    expected <- run_config[[key]] %||% if (key %in% c("num-classes", "num-labels")) 2L else NULL
    if (!same(unlist(manifest$model_config[[key]], use.names = FALSE), expected)) {
      stop("Public checkpoint model geometry disagrees with the prepared contract.", call. = FALSE)
    }
  }
  loss_parameter <- switch(run_config[["loss-name"]],
    negbin_nll = "nb-dispersion", gamma_nll = "gamma-shape",
    huber = "huber-delta", quantile = "quantile-level", NULL)
  if (!is.null(loss_parameter)) {
    default <- if (identical(loss_parameter, "quantile-level")) 0.5 else 1
    declared <- as.numeric(manifest$model_config[[loss_parameter]] %||% default)
    effective <- as.numeric(run_config[[loss_parameter]] %||% default)
    if (!same(declared, effective)) {
      stop("Public checkpoint loss parameter disagrees with the prepared contract.",
           call. = FALSE)
    }
  }
  if (!is.null(manifest$model_config[["survival-config-b64"]])) {
    decode <- function(b64) {
      value <- jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(b64)),
                                  simplifyVector = TRUE)
      value[sort(names(value))]
    }
    matching <- tryCatch(same(decode(manifest$model_config[["survival-config-b64"]]),
                             decode(run_config[["survival-config-b64"]])),
                         error = function(e) FALSE)
    if (!matching) {
      stop("Public checkpoint survival parametrisation disagrees with the prepared contract.",
           call. = FALSE)
    }
  }
  schema <- manifest$feature_contract
  bounds <- run_config[["feature-bounds"]]
  levels <- run_config[["target-levels"]]
  if (is.list(levels) && !is.null(levels$values)) levels <- levels$values
  checks <- list(features = as.character(feature_columns),
    feature_lower = if (is.null(bounds)) NULL else as.numeric(bounds$lower),
    feature_upper = if (is.null(bounds)) NULL else as.numeric(bounds$upper),
    target_levels = if (is.null(levels)) NULL else unlist(levels, use.names = FALSE),
    target_bounds = run_config[["target-bounds"]])
  for (key in names(checks)) {
    actual <- schema[[key]]
    if (key != "target_bounds" && !is.null(actual)) actual <- unlist(actual, use.names = FALSE)
    if (!same(actual, checks[[key]])) {
      stop("Public checkpoint feature/target geometry disagrees with the prepared contract.",
           call. = FALSE)
    }
  }
  invisible(TRUE)
}
