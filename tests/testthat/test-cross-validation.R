cv_config <- function(contract, rounds = 2L) {
  list(
    "dp-track" = "neural",
    "num-server-rounds" = rounds,
    "cv-version" = contract$version,
    "cv-method" = contract$method,
    "cv-assignment" = contract$assignment,
    "cv-folds" = contract$folds,
    "cv-privacy-unit" = contract$privacy_unit,
    "cv-unit-canonicalization" = contract$unit_canonicalization,
    "cv-contract-sha256" = contract$sha256,
    "cv-validation-bins" = 32L,
    "cv-n-nodes" = 2L,
    "cv-job-sha256" = strrep("a", 64L)
  )
}

cv_job_config <- function(contract) {
  list(
    "task-type" = "classification", "dp-track" = "neural",
    "num-server-rounds" = 2L, "num-features" = 2L,
    "cv-version" = contract$version, "cv-method" = contract$method,
    "cv-assignment" = contract$assignment, "cv-folds" = contract$folds,
    "cv-privacy-unit" = contract$privacy_unit,
    "cv-unit-canonicalization" = contract$unit_canonicalization,
    "cv-contract-sha256" = contract$sha256,
    "cv-validation-bins" = 32L, "cv-n-nodes" = 2L,
    "cv-job-sha256" = strrep("f", 64L),
    "strategy" = "fedadam", "strategy-eta" = 0.1,
    "strategy-eta-l" = 0.01, "strategy-beta-1" = 0.9,
    "strategy-beta-2" = 0.99, "strategy-tau" = 0.001,
    "model-spec-b64" = "e30=", "loss-name" = "bce_logits",
    "num-classes" = 2L, "num-labels" = 2L,
    "local-epochs" = 1L, "batch-size" = 32L,
    "learning-rate" = 0.01, "weight-decay" = 0, "l1-penalty" = 0,
    "optimizer-name" = "sgd", "optimizer-momentum" = 0,
    "optimizer-nesterov" = FALSE, "scheduler-name" = "none",
    "feature-bounds" = list(lower = c(0, -1), upper = c(1, 1)),
    "target-levels" = list(type = "character", values = c("no", "yes")))
}

test_that("CV job provenance has a mirrored golden and binds every public group", {
  config <- cv_job_config(dsFlower:::.crossValidationContract(3L, "row"))
  config[["target-levels"]] <- c("no", "yes")
  hash <- function(value = config, runner_abi = 3L,
                   runner_sha = strrep("a", 64L),
                   policy_sha = strrep("b", 64L), clipping = 1) {
    dsFlower:::.cv_job_sha256(
      value, c("x1", "x2"), "y", runner_abi, runner_sha,
      policy_sha, clipping)
  }
  baseline <- hash()
  expect_identical(
    baseline,
    "5a5dc6cac8b3407895a656fa834862d2dd7615203b0fca5fcdaddf8990ce739e")

  mutations <- list(
    cv_contract = c(config[-match("cv-contract-sha256", names(config))],
                    list("cv-contract-sha256" = strrep("c", 64L))),
    cv_bins = within(config, `cv-validation-bins` <- 64L),
    rounds = within(config, `num-server-rounds` <- 3L),
    nodes = within(config, `cv-n-nodes` <- 3L),
    strategy = within(config, `strategy-eta` <- 0.2),
    eta_l = within(config, `strategy-eta-l` <- 0.02),
    schema = within(config, `feature-bounds`$upper[[1L]] <- 2),
    task = within(config, `task-type` <- "count"),
    model_spec = within(config, `model-spec-b64` <- "W10="),
    loss = within(config, `loss-name` <- "hinge"),
    classes = within(config, `num-classes` <- 3L),
    labels = within(config, `num-labels` <- 3L),
    epochs = within(config, `local-epochs` <- 2L),
    batch = within(config, `batch-size` <- 16L),
    training = within(config, `learning-rate` <- 0.02))
  expect_true(all(vapply(mutations, function(value) {
    !identical(hash(value), baseline)
  }, logical(1))))
  expect_false(identical(hash(runner_abi = 4L), baseline))
  expect_false(identical(hash(runner_sha = strrep("d", 64L)), baseline))
  expect_false(identical(hash(policy_sha = strrep("e", 64L)), baseline))
  expect_false(identical(hash(clipping = 2), baseline))

  irrelevant <- config
  irrelevant$run_token <- "run_private"
  irrelevant$`results-dir` <- "/tmp/not-part-of-provenance"
  irrelevant$timestamp <- "2099-01-01"
  irrelevant$seed <- 123L
  irrelevant$n_samples <- 999L
  expect_identical(hash(irrelevant), baseline)
})

test_that("CV job mismatch is rejected before private staging", {
  state_dir <- withr::local_tempdir()
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"))
  withr::local_options(list(dsflower.dp_unit = "row"))
  name <- "test_cv_job_pre_staging"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    x1 = c(0, 1), x2 = c(0, 1), y = c("no", "yes"))))
  withr::defer(dsFlower:::.removeHandle(name))
  config <- cv_job_config(dsFlower:::.crossValidationContract(3L, "row"))
  config[["target-levels"]] <- c("no", "yes")
  reached_staging <- FALSE
  local_mocked_bindings(
    .compute_harness_hash = function() strrep("a", 64L),
    .stageData = function(...) {
      reached_staging <<- TRUE
      stop("private staging reached")
    },
    .package = "dsFlower")
  expect_error(
    flowerPrepareRunDS(name, "y", c("x1", "x2"), config),
    "cv-job-sha256")
  expect_false(reached_staging)
})

test_that("cross-validation contract is canonical, bounded, and seed-free", {
  contract <- dsFlower:::.crossValidationContract(3L, "patient")
  expect_identical(contract$version, "dsflower-cross-validation-v1")
  expect_identical(contract$method, "cross_validation")
  expect_identical(contract$assignment, "hmac-sha256-score-v1")
  expect_identical(contract$folds, 3L)
  expect_identical(contract$unit_canonicalization, "trim-utf8-v2")
  expect_identical(
    contract$sha256,
    "136791e925e1ccc94c3bc02c4ae6b959c783fb6855a1c61a46af2f78ffc80926")
  expect_false(any(grepl(
    "seed|salt|nonce", names(contract), ignore.case = TRUE)))
  expect_error(dsFlower:::.crossValidationContract(1L, "row"), "[2, 10]")
  expect_error(dsFlower:::.crossValidationContract(11L, "row"), "[2, 10]")
})

test_that("one server-owned job budget is split 80 percent over K folds", {
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.dp_per_training_epsilon = 2,
    dsflower.dp_per_training_delta = 1e-5
  ))
  contract <- dsFlower:::.crossValidationContract(4L, "row")
  normalized <- dsFlower:::.addDpConfigToRunConfig(cv_config(contract))
  expect_equal(normalized[["privacy-cv-training-epsilon"]], 1.6)
  expect_equal(normalized[["privacy-cv-fold-epsilon"]], 0.4)
  expect_equal(normalized[["privacy-cv-oof-epsilon"]], 0.4)
  expect_equal(normalized[["privacy-cv-training-delta"]], 8e-6)
  expect_equal(normalized[["privacy-cv-fold-delta"]], 2e-6)
  expect_equal(normalized[["privacy-cv-oof-delta"]], 2e-6)
  expect_equal(
    normalized[["privacy-cv-training-epsilon"]] +
      normalized[["privacy-cv-oof-epsilon"]],
    normalized[["privacy-epsilon"]])
  expect_false(any(grepl(
    "lifetime|remaining|counter|rate|quota|resource-budget",
    names(normalized), ignore.case = TRUE)))

  expect_error(
    dsFlower:::.addDpConfigToRunConfig(c(
      cv_config(contract), list("cv-seed" = 4L))),
    "unsupported field.*cv-seed")
  expect_error(
    dsFlower:::.normalizeCrossValidationConfig(
      cv_config(contract), "native_tree"),
    "neural")
})

test_that("holdout and cross-validation cannot share one job", {
  withr::local_options(list(dsflower.dp_unit = "row"))
  cv <- dsFlower:::.crossValidationContract(3L, "row")
  holdout <- dsFlower:::.holdoutContract(200000L, "row")
  both <- c(cv_config(cv), list(
    "resampling-version" = holdout$version,
    "resampling-method" = holdout$method,
    "resampling-assignment" = holdout$assignment,
    "resampling-test-numerator" = holdout$test_numerator,
    "resampling-test-denominator" = holdout$test_denominator,
    "resampling-privacy-unit" = holdout$privacy_unit,
    "resampling-unit-canonicalization" = holdout$unit_canonicalization,
    "resampling-contract-sha256" = holdout$sha256,
    "holdout-validation-bins" = 32L))
  expect_error(dsFlower:::.addDpConfigToRunConfig(both), "cannot be combined")
})

test_that("prepared manifest pins only the per-job CV allocation", {
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.dp_per_training_epsilon = 2,
    dsflower.dp_per_training_delta = 1e-5
  ))
  contract <- dsFlower:::.crossValidationContract(3L, "row")
  config <- dsFlower:::.addDpConfigToRunConfig(cv_config(contract))
  token <- "run_00000000000000000000000000000902"
  withr::defer(dsFlower:::.cleanupStaging(token))
  staged <- dsFlower:::.stageData(
    data.frame(x = seq_len(12), y = rep(0:1, 6)), token, "y", "x",
    extra_config = config)
  manifest <- jsonlite::fromJSON(
    file.path(staged, "manifest.json"), simplifyVector = TRUE)
  expect_identical(manifest[["cv-contract-sha256"]], contract$sha256)
  expect_identical(manifest[["cv-folds"]], 3L)
  expect_equal(manifest[["privacy-cv-fold-epsilon"]], 1.6 / 3)
  expect_equal(manifest[["privacy-cv-oof-epsilon"]], 0.4)
  expect_false(any(grepl(
    "lifetime|remaining|counter|quota", names(manifest), ignore.case = TRUE)))
})
