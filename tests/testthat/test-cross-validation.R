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

native_cv_wire <- function() paste0(
  '{"contract":"dsflower-native-tree-request-v1","engine":"xgboost",',
  '"mode":"native-tight","task":"binary","public_schema":{',
  '"version":1,"features":["age","marker"],"lower":[0.0,-5.0],',
  '"upper":[100.0,5.0],"cuts":[[18.0,40.0,65.0],[-1.0,0.0,1.0]],',
  '"target":{"name":"outcome","kind":"binary","levels":',
  '[{"type":"string","value":"control"},{"type":"string","value":"case"}],',
  '"lower":0.0,"upper":1.0},',
  '"sha256":"77a6e8d46a174381b8b4da168b833b2ee75f09f8ca8ac55f2c954be642ba9073"},',
  '"parameters":[{"name":"learning_rate","type":"number","value":0.25},',
  '{"name":"max_delta_step","type":"number","value":1.0},',
  '{"name":"max_depth","type":"integer","value":2},',
  '{"name":"min_child_weight","type":"number","value":1.0},',
  '{"name":"min_split_loss","type":"number","value":0.0},',
  '{"name":"num_boost_round","type":"integer","value":8},',
  '{"name":"reg_alpha","type":"number","value":0.0},',
  '{"name":"reg_lambda","type":"number","value":1.0}],',
  '"resources":{"max_features":2,"max_trees":8,"max_depth":2,',
  '"max_bins":4,"max_threads":32,"memory_mb":32768,',
  '"timeout_seconds":21600}}')

test_that("native-tree CV job KAT matches the client fixture without neural pins", {
  request <- dsFlower:::.validate_native_tree_manifest(
    jsonlite::fromJSON(native_cv_wire(), simplifyVector = FALSE),
    "193390a92a076bf9d4cdac0686e6542990b5948809cdc8f1dbbc9ccaac787692")
  contract <- dsFlower:::.crossValidationContract(3L, "row")
  config <- list(
    "task-type" = "classification", "dp-track" = "native_tree",
    "num-server-rounds" = 1L, "num-features" = 2L,
    "native-tree-request-b64" = request$b64,
    "native-tree-request-sha256" = request$sha256,
    "cv-version" = contract$version, "cv-method" = contract$method,
    "cv-assignment" = contract$assignment, "cv-folds" = contract$folds,
    "cv-privacy-unit" = contract$privacy_unit,
    "cv-unit-canonicalization" = contract$unit_canonicalization,
    "cv-contract-sha256" = contract$sha256,
    "cv-validation-bins" = 32L, "cv-n-nodes" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("control", "case"))
  hash <- function(value = config) dsFlower:::.cv_job_sha256(
    value, c("age", "marker"), "outcome", 3L, strrep("a", 64L),
    strrep("b", 64L), 1)
  baseline <- hash()
  expect_identical(
    baseline,
    "1f8d5520eb02029caad12b3a8e40e929ab58a142edd494b4cfe751dec826982c")

  irrelevant <- config
  irrelevant[["model-spec-b64"]] <- "W10="
  irrelevant[["loss-name"]] <- "mse"
  irrelevant[["optimizer-name"]] <- "adamw"
  irrelevant$strategy <- "fedadam"
  expect_identical(hash(irrelevant), baseline)
})

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
  feature_columns <- c("x1", "x2")
  expect_error(
    flowerPrepareRunDS(name, "y", feature_columns, config),
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
  native <- cv_config(contract)
  native[["dp-track"]] <- "native_tree"
  expect_identical(
    dsFlower:::.normalizeCrossValidationConfig(
      native, "native_tree")[["cv-contract-sha256"]],
    contract$sha256)
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

test_that("staged CV budget survives JSON at the release guard's tolerance", {
  # The trusted runner recomputes the fixed 80/20-over-K allocation in IEEE
  # doubles and requires the manifest values within rel_tol = 1e-15
  # (release_guard._fixed_manifest). A 15-significant-digit manifest decimal
  # (epsilon 4, folds 3 -> 3.2/3) is 3e-15 off, so every live CV round failed
  # closed as unavailable at 0.4.1. Assert the written manifest round-trips
  # each budget field to within one ulp of the exact allocation.
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.dp_per_training_epsilon = 4,
    dsflower.dp_per_training_delta = 1e-5
  ))
  contract <- dsFlower:::.crossValidationContract(3L, "row")
  config <- dsFlower:::.addDpConfigToRunConfig(cv_config(contract))
  token <- "run_00000000000000000000000000000903"
  withr::defer(dsFlower:::.cleanupStaging(token))
  staged <- dsFlower:::.stageData(
    data.frame(x = seq_len(12), y = rep(0:1, 6)), token, "y", "x",
    extra_config = config)
  manifest <- jsonlite::fromJSON(
    file.path(staged, "manifest.json"), simplifyVector = TRUE)
  epsilon <- as.numeric(manifest[["privacy-epsilon"]])
  delta <- as.numeric(manifest[["privacy-delta"]])
  expect_identical(epsilon, 4)
  expect_identical(delta, 1e-5)
  exact <- list(
    "privacy-cv-training-epsilon" = epsilon * 0.8,
    "privacy-cv-training-delta" = delta * 0.8,
    "privacy-cv-fold-epsilon" = epsilon * 0.8 / 3L,
    "privacy-cv-fold-delta" = delta * 0.8 / 3L,
    "privacy-cv-oof-epsilon" = epsilon - epsilon * 0.8,
    "privacy-cv-oof-delta" = delta - delta * 0.8)
  for (field in names(exact)) {
    relative_error <- abs(manifest[[field]] - exact[[field]]) /
      abs(exact[[field]])
    expect_lte(relative_error, 2.3e-16)  # one ulp; guard allows 1e-15
  }
})
