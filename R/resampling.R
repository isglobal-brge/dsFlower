# Module: Stateless resampling contracts
#
# Resampling is an ephemeral execution contract.  The only secret input is the
# existing custodial node root; there is no analyst-selectable seed, database,
# counter, or historical admission state.

.HOLDOUT_DENOMINATOR <- 1000000L
.HOLDOUT_TRAINING_BUDGET_FRACTION <- 0.8
.CV_TRAINING_BUDGET_FRACTION <- 0.8

.holdoutContract <- function(test_millionths, privacy_unit) {
  numerator <- suppressWarnings(as.numeric(test_millionths))
  if (length(numerator) != 1L || !is.finite(numerator) ||
      numerator != floor(numerator) || numerator < 1L ||
      numerator >= .HOLDOUT_DENOMINATOR) {
    stop("Holdout test fraction must be an integer number of millionths in ",
         "[1, 999999].", call. = FALSE)
  }
  unit <- tolower(as.character(unlist(privacy_unit, use.names = FALSE)))
  if (length(unit) != 1L || is.na(unit) || !unit %in% c("row", "patient")) {
    stop("Holdout privacy unit must be exactly row or patient.", call. = FALSE)
  }
  canonicalization <- if (identical(unit, "patient")) {
    "trim-utf8-v2"
  } else {
    "row-ordinal-v1"
  }
  payload <- list(
    assignment = "hmac-sha256-threshold-v1",
    method = "holdout",
    privacy_unit = unit,
    test_denominator = .HOLDOUT_DENOMINATOR,
    test_numerator = as.integer(numerator),
    unit_canonicalization = canonicalization,
    version = "dsflower-resampling-v1"
  )
  wire <- jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", digits = NA,
    pretty = FALSE)
  c(payload, list(sha256 = digest::digest(
    charToRaw(enc2utf8(wire)), algo = "sha256", serialize = FALSE)))
}
.resamplingConfigFields <- function() {
  c(
    "resampling-version", "resampling-method", "resampling-assignment",
    "resampling-test-numerator", "resampling-test-denominator",
    "resampling-privacy-unit", "resampling-unit-canonicalization",
    "resampling-contract-sha256", "holdout-validation-bins"
  )
}

.normalizeResamplingConfig <- function(run_config, track) {
  names_lower <- tolower(names(run_config) %||% character())
  present <- names(run_config)[
    startsWith(names_lower, "resampling-") |
      startsWith(names_lower, "resampling_") |
      startsWith(names_lower, "holdout-") |
      startsWith(names_lower, "holdout_")]
  if (!length(present)) return(run_config)

  required <- .resamplingConfigFields()
  if (!setequal(present, required) || length(present) != length(required)) {
    unexpected <- setdiff(present, required)
    missing <- setdiff(required, present)
    detail <- c(
      if (length(unexpected)) paste0("unexpected: ", paste(unexpected, collapse = ", ")),
      if (length(missing)) paste0("missing: ", paste(missing, collapse = ", ")))
    stop("Holdout resampling fields must match the exact seed-free contract",
         if (length(detail)) paste0(" (", paste(detail, collapse = "; "), ")") else "",
         ".", call. = FALSE)
  }
  if (!identical(track, "neural")) {
    stop("Atomic holdout is currently implemented only for the neural track.",
         call. = FALSE)
  }
  requested_type <- tolower(as.character(unlist(
    run_config[["data_type"]] %||% "tabular", use.names = FALSE)))
  if (length(requested_type) != 1L || !identical(requested_type, "tabular")) {
    stop("Atomic neural holdout currently supports tabular data only.",
         call. = FALSE)
  }

  bins <- suppressWarnings(as.numeric(unlist(
    run_config[["holdout-validation-bins"]], use.names = FALSE)))
  if (length(bins) != 1L || !is.finite(bins) || bins != floor(bins) ||
      bins < 4L || bins > 512L) {
    stop("holdout-validation-bins must be an integer in [4, 512].",
         call. = FALSE)
  }
  numerator <- run_config[["resampling-test-numerator"]]
  denominator <- suppressWarnings(as.numeric(unlist(
    run_config[["resampling-test-denominator"]], use.names = FALSE)))
  if (length(denominator) != 1L ||
      !identical(denominator, as.numeric(.HOLDOUT_DENOMINATOR))) {
    stop("Holdout resampling uses the fixed millionths denominator.",
         call. = FALSE)
  }
  policy <- .dpUnitPolicy()
  contract <- .holdoutContract(numerator, policy$dp_unit)
  supplied <- list(
    version = run_config[["resampling-version"]],
    method = run_config[["resampling-method"]],
    assignment = run_config[["resampling-assignment"]],
    test_denominator = denominator,
    privacy_unit = run_config[["resampling-privacy-unit"]],
    unit_canonicalization = run_config[["resampling-unit-canonicalization"]])
  expected <- contract[c(
    "version", "method", "assignment", "test_denominator", "privacy_unit",
    "unit_canonicalization")]
  supplied <- lapply(supplied, function(value) {
    if (is.numeric(value)) as.numeric(value) else
      as.character(unlist(value, use.names = FALSE))
  })
  expected <- lapply(expected, function(value) {
    if (is.numeric(value)) as.numeric(value) else as.character(value)
  })
  if (!identical(supplied, expected)) {
    stop("Holdout resampling contract disagrees with the node-owned unit policy.",
         call. = FALSE)
  }
  supplied_hash <- tolower(as.character(unlist(
    run_config[["resampling-contract-sha256"]], use.names = FALSE)))
  if (length(supplied_hash) != 1L || is.na(supplied_hash) ||
      !identical(supplied_hash, contract$sha256)) {
    stop("Holdout resampling contract SHA-256 does not match its canonical fields.",
         call. = FALSE)
  }

  run_config[["resampling-version"]] <- contract$version
  run_config[["resampling-method"]] <- contract$method
  run_config[["resampling-assignment"]] <- contract$assignment
  run_config[["resampling-test-numerator"]] <- contract$test_numerator
  run_config[["resampling-test-denominator"]] <- contract$test_denominator
  run_config[["resampling-privacy-unit"]] <- contract$privacy_unit
  run_config[["resampling-unit-canonicalization"]] <-
    contract$unit_canonicalization
  run_config[["resampling-contract-sha256"]] <- contract$sha256
  run_config[["holdout-validation-bins"]] <- as.integer(bins)
  run_config
}

.applyHoldoutPrivacyAllocation <- function(run_config) {
  if (is.null(run_config[["resampling-contract-sha256"]])) return(run_config)
  epsilon <- as.numeric(run_config[["privacy-epsilon"]])
  delta <- as.numeric(run_config[["privacy-delta"]])
  train_epsilon <- epsilon * .HOLDOUT_TRAINING_BUDGET_FRACTION
  train_delta <- delta * .HOLDOUT_TRAINING_BUDGET_FRACTION
  run_config[["privacy-training-epsilon"]] <- train_epsilon
  run_config[["privacy-training-delta"]] <- train_delta
  run_config[["privacy-holdout-epsilon"]] <- epsilon - train_epsilon
  run_config[["privacy-holdout-delta"]] <- delta - train_delta
  run_config
}

.crossValidationContract <- function(folds, privacy_unit) {
  k <- suppressWarnings(as.numeric(folds))
  if (length(k) != 1L || !is.finite(k) || k != floor(k) ||
      k < 2L || k > 10L) {
    stop("Cross-validation folds must be one integer in [2, 10].",
         call. = FALSE)
  }
  unit <- tolower(as.character(unlist(privacy_unit, use.names = FALSE)))
  if (length(unit) != 1L || is.na(unit) || !unit %in% c("row", "patient")) {
    stop("Cross-validation privacy unit must be exactly row or patient.",
         call. = FALSE)
  }
  payload <- list(
    assignment = "hmac-sha256-score-v1",
    folds = as.integer(k),
    method = "cross_validation",
    privacy_unit = unit,
    unit_canonicalization = if (identical(unit, "patient"))
      "trim-utf8-v2" else "row-ordinal-v1",
    version = "dsflower-cross-validation-v1"
  )
  wire <- jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", digits = NA,
    pretty = FALSE)
  c(payload, list(sha256 = digest::digest(
    charToRaw(enc2utf8(wire)), algo = "sha256", serialize = FALSE)))
}

.crossValidationConfigFields <- function() {
  c(
    "cv-version", "cv-method", "cv-assignment", "cv-folds",
    "cv-privacy-unit", "cv-unit-canonicalization",
    "cv-contract-sha256", "cv-validation-bins", "cv-n-nodes",
    "cv-job-sha256"
  )
}

.normalizeCrossValidationConfig <- function(run_config, track) {
  names_lower <- tolower(names(run_config) %||% character())
  present <- names(run_config)[
    startsWith(names_lower, "cv-") | startsWith(names_lower, "cv_")]
  if (!length(present)) return(run_config)
  if (!is.null(run_config[["resampling-contract-sha256"]])) {
    stop("Holdout and cross-validation cannot be combined.", call. = FALSE)
  }
  required <- .crossValidationConfigFields()
  if (!setequal(present, required) || length(present) != length(required)) {
    stop("Cross-validation fields must match the exact seed-free contract.",
         call. = FALSE)
  }
  if (!identical(track, "neural")) {
    stop("Cross-validation is currently implemented only for the neural track.",
         call. = FALSE)
  }
  requested_type <- tolower(as.character(unlist(
    run_config[["data_type"]] %||% "tabular", use.names = FALSE)))
  if (length(requested_type) != 1L || !identical(requested_type, "tabular")) {
    stop("Neural cross-validation currently supports tabular data only.",
         call. = FALSE)
  }
  bins <- suppressWarnings(as.numeric(unlist(
    run_config[["cv-validation-bins"]], use.names = FALSE)))
  if (length(bins) != 1L || !is.finite(bins) || bins != floor(bins) ||
      bins < 4L || bins > 512L) {
    stop("cv-validation-bins must be an integer in [4, 512].",
         call. = FALSE)
  }
  policy <- .dpUnitPolicy()
  contract <- .crossValidationContract(
    run_config[["cv-folds"]], policy$dp_unit)
  supplied <- list(
    version = run_config[["cv-version"]],
    method = run_config[["cv-method"]],
    assignment = run_config[["cv-assignment"]],
    folds = run_config[["cv-folds"]],
    privacy_unit = run_config[["cv-privacy-unit"]],
    unit_canonicalization = run_config[["cv-unit-canonicalization"]])
  expected <- contract[c(
    "version", "method", "assignment", "folds", "privacy_unit",
    "unit_canonicalization")]
  supplied <- lapply(supplied, function(value) {
    if (is.numeric(value)) as.numeric(value) else
      as.character(unlist(value, use.names = FALSE))
  })
  expected <- lapply(expected, function(value) {
    if (is.numeric(value)) as.numeric(value) else as.character(value)
  })
  if (!identical(supplied, expected)) {
    stop("Cross-validation contract disagrees with the node-owned unit policy.",
         call. = FALSE)
  }
  supplied_hash <- tolower(as.character(unlist(
    run_config[["cv-contract-sha256"]], use.names = FALSE)))
  if (length(supplied_hash) != 1L || is.na(supplied_hash) ||
      !identical(supplied_hash, contract$sha256)) {
    stop("Cross-validation contract SHA-256 does not match its canonical fields.",
         call. = FALSE)
  }
  n_nodes <- suppressWarnings(as.numeric(unlist(
    run_config[["cv-n-nodes"]], use.names = FALSE)))
  job_hash <- tolower(as.character(unlist(
    run_config[["cv-job-sha256"]], use.names = FALSE)))
  if (length(n_nodes) != 1L || !is.finite(n_nodes) ||
      n_nodes != floor(n_nodes) || n_nodes < 1L || n_nodes > 65536L) {
    stop("cv-n-nodes must be an integer in [1, 65536].", call. = FALSE)
  }
  if (length(job_hash) != 1L || is.na(job_hash) ||
      !grepl("^[0-9a-f]{64}$", job_hash)) {
    stop("cv-job-sha256 must be one lowercase SHA-256 digest.",
         call. = FALSE)
  }
  run_config[["cv-version"]] <- contract$version
  run_config[["cv-method"]] <- contract$method
  run_config[["cv-assignment"]] <- contract$assignment
  run_config[["cv-folds"]] <- contract$folds
  run_config[["cv-privacy-unit"]] <- contract$privacy_unit
  run_config[["cv-unit-canonicalization"]] <- contract$unit_canonicalization
  run_config[["cv-contract-sha256"]] <- contract$sha256
  run_config[["cv-validation-bins"]] <- as.integer(bins)
  run_config[["cv-n-nodes"]] <- as.integer(n_nodes)
  run_config[["cv-job-sha256"]] <- job_hash
  run_config
}

.applyCrossValidationPrivacyAllocation <- function(run_config) {
  if (is.null(run_config[["cv-contract-sha256"]])) return(run_config)
  epsilon <- as.numeric(run_config[["privacy-epsilon"]])
  delta <- as.numeric(run_config[["privacy-delta"]])
  folds <- as.integer(run_config[["cv-folds"]])
  training_epsilon <- epsilon * .CV_TRAINING_BUDGET_FRACTION
  training_delta <- delta * .CV_TRAINING_BUDGET_FRACTION
  run_config[["privacy-cv-training-epsilon"]] <- training_epsilon
  run_config[["privacy-cv-training-delta"]] <- training_delta
  run_config[["privacy-cv-fold-epsilon"]] <- training_epsilon / folds
  run_config[["privacy-cv-fold-delta"]] <- training_delta / folds
  run_config[["privacy-cv-oof-epsilon"]] <- epsilon - training_epsilon
  run_config[["privacy-cv-oof-delta"]] <- delta - training_delta
  run_config
}

# Public provenance for one complete cross-validation recipe. This identifier
# binds effective public execution semantics only: it deliberately excludes
# paths, run tokens, clocks, private counts/data, and randomness.
.cv_job_scalar <- function(value, name, type = c("character", "number", "integer"),
                           lower = -Inf, upper = Inf) {
  type <- match.arg(type)
  value <- unlist(value, recursive = TRUE, use.names = FALSE)
  if (length(value) != 1L || is.na(value)) {
    stop(name, " must be one public scalar.", call. = FALSE)
  }
  if (identical(type, "character")) {
    value <- enc2utf8(as.character(value))
    if (!nzchar(value)) stop(name, " must be non-empty.", call. = FALSE)
    return(value)
  }
  value <- suppressWarnings(as.numeric(value))
  if (!is.finite(value) || value < lower || value > upper ||
      (identical(type, "integer") && value != floor(value))) {
    stop(name, " is outside its public contract.", call. = FALSE)
  }
  if (identical(type, "integer")) as.integer(value) else value
}

.cv_job_sha <- function(value, name) {
  value <- tolower(.cv_job_scalar(value, name, "character"))
  if (!grepl("^[0-9a-f]{64}$", value)) {
    stop(name, " must be one lowercase SHA-256 digest.", call. = FALSE)
  }
  value
}

.cv_job_bool <- function(value, name) {
  value <- unlist(value, recursive = TRUE, use.names = FALSE)
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    stop(name, " must be one public boolean.", call. = FALSE)
  }
  value
}

.cv_job_strategy <- function(run_config) {
  name <- tolower(.cv_job_scalar(
    run_config[["strategy"]] %||% "fedavg", "strategy", "character"))
  expected <- switch(name,
    fedavg = character(),
    fedadam = c("strategy-eta", "strategy-eta-l", "strategy-beta-1",
               "strategy-beta-2", "strategy-tau"),
    fedadagrad = c("strategy-eta", "strategy-eta-l", "strategy-tau"),
    fedyogi = c("strategy-eta", "strategy-eta-l", "strategy-beta-1",
               "strategy-beta-2", "strategy-tau"),
    fedavgm = c("strategy-server-learning-rate",
                "strategy-server-momentum"),
    stop("Cross-validation aggregation strategy is unsupported.",
         call. = FALSE))
  present <- names(run_config)[startsWith(
    tolower(names(run_config) %||% character()), "strategy-")]
  if (!setequal(present, expected) || length(present) != length(expected)) {
    stop("Cross-validation strategy fields do not match the effective strategy.",
         call. = FALSE)
  }
  number <- function(key, default = NULL, lower = 0, upper = Inf) {
    value <- run_config[[key]] %||% default
    if (is.null(value)) return(NULL)
    .cv_job_scalar(value, key, "number", lower, upper)
  }
  params <- list(
    eta = number("strategy-eta"),
    eta_l = number("strategy-eta-l"),
    beta_1 = number("strategy-beta-1", lower = 0, upper = 1),
    beta_2 = number("strategy-beta-2", lower = 0, upper = 1),
    tau = number("strategy-tau"),
    server_learning_rate = number("strategy-server-learning-rate"),
    server_momentum = number(
      "strategy-server-momentum", lower = 0, upper = 1))
  if (any(vapply(params[c("eta", "eta_l", "tau",
                          "server_learning_rate")], function(value) {
    !is.null(value) && value <= 0
  }, logical(1))) ||
      any(vapply(params[c("beta_1", "beta_2", "server_momentum")],
                 function(value) !is.null(value) && value >= 1,
                 logical(1)))) {
    stop("Cross-validation strategy parameters are outside their contract.",
         call. = FALSE)
  }
  list(name = name, params = params)
}

.cv_job_training <- function(run_config) {
  loss <- tolower(.cv_job_scalar(
    run_config[["loss-name"]], "loss-name", "character"))
  allowed_losses <- c(
    "bce_logits", "cross_entropy", "mse", "poisson_nll",
    "multilabel_bce", "hinge", "negbin_nll", "gamma_nll", "huber",
    "quantile", "ordinal")
  if (!loss %in% allowed_losses) {
    stop("Cross-validation loss is unsupported.", call. = FALSE)
  }
  optimizer <- tolower(.cv_job_scalar(
    run_config[["optimizer-name"]] %||% "sgd",
    "optimizer-name", "character"))
  scheduler <- tolower(.cv_job_scalar(
    run_config[["scheduler-name"]] %||% "none",
    "scheduler-name", "character"))
  optimizer_fields <- c(
    "optimizer-momentum", "optimizer-nesterov", "optimizer-beta1",
    "optimizer-beta2", "optimizer-eps", "optimizer-amsgrad",
    "optimizer-rmsprop-alpha")
  expected_optimizer <- switch(optimizer,
    sgd = c("optimizer-momentum", "optimizer-nesterov"),
    adam = c("optimizer-beta1", "optimizer-beta2", "optimizer-eps",
             "optimizer-amsgrad"),
    adamw = c("optimizer-beta1", "optimizer-beta2", "optimizer-eps",
              "optimizer-amsgrad"),
    rmsprop = c("optimizer-momentum", "optimizer-eps",
                "optimizer-rmsprop-alpha"),
    stop("Cross-validation optimizer is unsupported.", call. = FALSE))
  present_optimizer <- intersect(names(run_config), optimizer_fields)
  if (!setequal(present_optimizer, expected_optimizer) ||
      length(present_optimizer) != length(expected_optimizer)) {
    stop("Cross-validation optimizer fields are not canonical.",
         call. = FALSE)
  }
  scheduler_fields <- c(
    "scheduler-step-size", "scheduler-gamma", "scheduler-min-lr")
  expected_scheduler <- switch(scheduler,
    none = character(),
    step = c("scheduler-step-size", "scheduler-gamma"),
    exponential = "scheduler-gamma",
    cosine = "scheduler-min-lr",
    stop("Cross-validation scheduler is unsupported.", call. = FALSE))
  present_scheduler <- intersect(names(run_config), scheduler_fields)
  if (!setequal(present_scheduler, expected_scheduler) ||
      length(present_scheduler) != length(expected_scheduler)) {
    stop("Cross-validation scheduler fields are not canonical.",
         call. = FALSE)
  }
  loss_fields <- c(
    "nb-dispersion", "gamma-shape", "huber-delta", "quantile-level")
  selected_loss <- switch(loss,
    negbin_nll = "nb-dispersion", gamma_nll = "gamma-shape",
    huber = "huber-delta", quantile = "quantile-level", NULL)
  expected_loss <- if (is.null(selected_loss)) character() else selected_loss
  present_loss <- intersect(names(run_config), loss_fields)
  if (!setequal(present_loss, expected_loss) ||
      length(present_loss) != length(expected_loss)) {
    stop("Cross-validation loss parameters are not canonical.",
         call. = FALSE)
  }
  number <- function(key, default, lower = 0, upper = Inf) {
    .cv_job_scalar(run_config[[key]] %||% default, key, "number", lower, upper)
  }
  integer <- function(key, default, lower, upper) {
    .cv_job_scalar(run_config[[key]] %||% default, key, "integer", lower, upper)
  }
  learning_rate <- number("learning-rate", 0.01, 0, 10)
  if (learning_rate <= 0) {
    stop("learning-rate is outside its public contract.", call. = FALSE)
  }
  optimizer_values <- list(
    name = optimizer,
    momentum = if ("optimizer-momentum" %in% expected_optimizer)
      number("optimizer-momentum", 0, 0, 1) else NULL,
    nesterov = if ("optimizer-nesterov" %in% expected_optimizer)
      .cv_job_bool(run_config[["optimizer-nesterov"]],
                   "optimizer-nesterov") else NULL,
    beta1 = if ("optimizer-beta1" %in% expected_optimizer)
      number("optimizer-beta1", 0.9, 0, 1) else NULL,
    beta2 = if ("optimizer-beta2" %in% expected_optimizer)
      number("optimizer-beta2", 0.999, 0, 1) else NULL,
    eps = if ("optimizer-eps" %in% expected_optimizer)
      number("optimizer-eps", 1e-8, 0, 1) else NULL,
    amsgrad = if ("optimizer-amsgrad" %in% expected_optimizer)
      .cv_job_bool(run_config[["optimizer-amsgrad"]],
                   "optimizer-amsgrad") else NULL,
    rmsprop_alpha = if ("optimizer-rmsprop-alpha" %in% expected_optimizer)
      number("optimizer-rmsprop-alpha", 0.99, 0, 1) else NULL)
  if ((!is.null(optimizer_values$momentum) &&
       optimizer_values$momentum >= 1) ||
      (!is.null(optimizer_values$beta1) && optimizer_values$beta1 >= 1) ||
      (!is.null(optimizer_values$beta2) && optimizer_values$beta2 >= 1) ||
      (!is.null(optimizer_values$eps) && optimizer_values$eps <= 0) ||
      (!is.null(optimizer_values$rmsprop_alpha) &&
       optimizer_values$rmsprop_alpha >= 1) ||
      (isTRUE(optimizer_values$nesterov) &&
       (!identical(optimizer, "sgd") || optimizer_values$momentum <= 0))) {
    stop("Cross-validation optimizer parameters are outside their contract.",
         call. = FALSE)
  }
  scheduler_values <- list(
    name = scheduler,
    step_size = if ("scheduler-step-size" %in% expected_scheduler)
      integer("scheduler-step-size", 1L, 1L, 1000L) else NULL,
    gamma = if ("scheduler-gamma" %in% expected_scheduler)
      number("scheduler-gamma", 0.1, 0, 10) else NULL,
    min_lr = if ("scheduler-min-lr" %in% expected_scheduler)
      number("scheduler-min-lr", 0, 0, 10) else NULL)
  if ((!is.null(scheduler_values$gamma) && scheduler_values$gamma <= 0) ||
      (!is.null(scheduler_values$min_lr) &&
       scheduler_values$min_lr > learning_rate)) {
    stop("Cross-validation scheduler parameters are outside their contract.",
         call. = FALSE)
  }
  loss_parameter <- if (is.null(selected_loss)) NULL else list(
    name = selected_loss,
    value = number(selected_loss, switch(selected_loss,
      `quantile-level` = 0.5, 1), 0, 1e12))
  if (!is.null(loss_parameter) &&
      ((identical(selected_loss, "quantile-level") &&
        (loss_parameter$value <= 0 || loss_parameter$value >= 1)) ||
       (!identical(selected_loss, "quantile-level") &&
        loss_parameter$value <= 0))) {
    stop("Cross-validation loss parameter is outside its contract.",
         call. = FALSE)
  }
  list(
    learning_rate = learning_rate,
    weight_decay = number("weight-decay", 0, 0, 1000),
    l1_penalty = number("l1-penalty", 0, 0, 1000),
    optimizer = optimizer_values,
    scheduler = scheduler_values,
    loss_parameter = loss_parameter)
}

.cv_job_sha256 <- function(run_config, feature_columns, target_column,
                           runner_abi, runner_sha256,
                           privacy_policy_sha256, privacy_clipping_norm) {
  if (is.null(run_config[["cv-contract-sha256"]])) return(NULL)
  features <- enc2utf8(as.character(unlist(
    feature_columns, use.names = FALSE)))
  targets <- enc2utf8(as.character(unlist(
    target_column, use.names = FALSE)))
  if (!length(features) || anyNA(features) || any(!nzchar(features)) ||
      anyDuplicated(features) || !length(targets) || anyNA(targets) ||
      any(!nzchar(targets)) || anyDuplicated(targets)) {
    stop("Cross-validation requires ordered unique public columns.",
         call. = FALSE)
  }
  n_features <- .cv_job_scalar(
    run_config[["num-features"]], "num-features", "integer", 1, 65536)
  if (!identical(n_features, as.integer(length(features)))) {
    stop("Cross-validation feature count differs from its ordered schema.",
         call. = FALSE)
  }
  bounds <- run_config[["feature-bounds"]] %||% NULL
  feature_lower <- if (is.null(bounds)) NULL else
    as.numeric(unlist(bounds$lower, use.names = FALSE))
  feature_upper <- if (is.null(bounds)) NULL else
    as.numeric(unlist(bounds$upper, use.names = FALSE))
  if (!is.null(bounds) &&
      (length(feature_lower) != n_features ||
       length(feature_upper) != n_features ||
       any(!is.finite(feature_lower)) || any(!is.finite(feature_upper)) ||
       any(feature_lower >= feature_upper))) {
    stop("Cross-validation feature bounds differ from its ordered schema.",
         call. = FALSE)
  }
  target_bounds <- run_config[["target-bounds"]] %||% NULL
  levels <- run_config[["target-levels"]] %||% NULL
  normalized_levels <- is.list(levels) && !is.null(levels$type) &&
    !is.null(levels$values)
  level_values <- if (is.null(levels)) NULL else unlist(
    if (normalized_levels) levels$values else levels, use.names = FALSE)
  level_type <- if (is.null(levels)) NULL else if (normalized_levels) {
    .cv_job_scalar(levels$type, "target-levels type", "character")
  } else if (is.character(level_values)) {
    "character"
  } else if (is.logical(level_values)) {
    "logical"
  } else {
    "numeric"
  }
  if (!is.null(levels)) {
    if (!level_type %in% c("character", "logical", "numeric") ||
        length(level_values) < 2L || anyNA(level_values) ||
        anyDuplicated(level_values)) {
      stop("Cross-validation target levels are not canonical.", call. = FALSE)
    }
    level_values <- switch(level_type,
      character = enc2utf8(as.character(level_values)),
      logical = as.logical(level_values), numeric = as.numeric(level_values))
  }
  target_lower <- if (is.null(target_bounds)) NULL else
    .cv_job_scalar(target_bounds$lower, "target lower", "number", -1e6, 1e6)
  target_upper <- if (is.null(target_bounds)) NULL else
    .cv_job_scalar(target_bounds$upper, "target upper", "number", -1e6, 1e6)
  if (!is.null(target_bounds) && target_lower >= target_upper) {
    stop("Cross-validation target bounds are invalid.", call. = FALSE)
  }
  if (!is.null(levels) && !is.null(target_bounds)) {
    stop("Cross-validation cannot combine target levels and bounds.",
         call. = FALSE)
  }
  task <- tolower(.cv_job_scalar(
    run_config[["task-type"]], "task-type", "character"))
  if (!task %in% c("classification", "regression", "count")) {
    stop("Cross-validation task is unsupported.", call. = FALSE)
  }
  folds <- .cv_job_scalar(
    run_config[["cv-folds"]], "cv-folds", "integer", 2, 10)
  bins <- .cv_job_scalar(
    run_config[["cv-validation-bins"]], "cv-validation-bins",
    "integer", 4, 512)
  n_nodes <- .cv_job_scalar(
    run_config[["cv-n-nodes"]], "cv-n-nodes", "integer", 1, 65536)
  rounds <- .cv_job_scalar(
    run_config[["num-server-rounds"]], "num-server-rounds",
    "integer", 1, 500)
  model_spec <- .cv_job_scalar(
    run_config[["model-spec-b64"]], "model-spec-b64", "character")
  loss <- tolower(.cv_job_scalar(
    run_config[["loss-name"]], "loss-name", "character"))
  payload <- list(
    version = "dsflower-cv-job-v1",
    runner = list(
      abi = .cv_job_scalar(runner_abi, "runner ABI", "integer", 1, 65536),
      sha256 = .cv_job_sha(runner_sha256, "runner SHA-256")),
    privacy = list(
      policy_sha256 = .cv_job_sha(
        privacy_policy_sha256, "privacy policy SHA-256"),
      clipping_norm = .cv_job_scalar(
        privacy_clipping_norm, "privacy clipping norm", "number", 0, 100)),
    cross_validation = list(
      version = .cv_job_scalar(
        run_config[["cv-version"]], "cv-version", "character"),
      method = .cv_job_scalar(
        run_config[["cv-method"]], "cv-method", "character"),
      assignment = .cv_job_scalar(
        run_config[["cv-assignment"]], "cv-assignment", "character"),
      folds = folds,
      privacy_unit = .cv_job_scalar(
        run_config[["cv-privacy-unit"]], "cv-privacy-unit", "character"),
      unit_canonicalization = .cv_job_scalar(
        run_config[["cv-unit-canonicalization"]],
        "cv-unit-canonicalization", "character"),
      contract_sha256 = .cv_job_sha(
        run_config[["cv-contract-sha256"]], "CV contract SHA-256"),
      validation_bins = bins),
    execution = list(
      rounds = rounds, n_nodes = n_nodes,
      strategy = .cv_job_strategy(run_config)),
    schema = list(
      features = features, targets = targets,
      feature_lower = feature_lower, feature_upper = feature_upper,
      target_level_type = level_type, target_levels = level_values,
      target_lower = target_lower, target_upper = target_upper,
      task = task),
    model = list(
      spec_b64 = model_spec, loss = loss,
      num_classes = .cv_job_scalar(
        run_config[["num-classes"]], "num-classes", "integer", 2, 1024),
      num_labels = .cv_job_scalar(
        run_config[["num-labels"]], "num-labels", "integer", 2, 1024),
      local_epochs = .cv_job_scalar(
        run_config[["local-epochs"]], "local-epochs", "integer", 1, 1000),
      batch_size = .cv_job_scalar(
        run_config[["batch-size"]], "batch-size", "integer", 1, 65536)),
    training = .cv_job_training(run_config))
  if (payload$privacy$clipping_norm <= 0) {
    stop("privacy clipping norm is outside its public contract.",
         call. = FALSE)
  }
  canonical <- as.character(jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", na = "null", digits = NA,
    always_decimal = TRUE, pretty = FALSE))
  digest::digest(charToRaw(enc2utf8(canonical)), algo = "sha256",
                 serialize = FALSE)
}
