# Module: Stateless resampling contracts
#
# Resampling is an ephemeral execution contract.  The only secret input is the
# existing custodial node root; there is no analyst-selectable seed, database,
# counter, or historical admission state.

.HOLDOUT_DENOMINATOR <- 1000000L
.HOLDOUT_TRAINING_BUDGET_FRACTION <- 0.8

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
