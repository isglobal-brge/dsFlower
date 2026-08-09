test_that("holdout contract is canonical and has no analyst seed axis", {
  contract <- dsFlower:::.holdoutContract(
    test_millionths = 200000L, privacy_unit = "patient")

  expect_identical(contract$version, "dsflower-resampling-v1")
  expect_identical(contract$method, "holdout")
  expect_identical(contract$assignment, "hmac-sha256-threshold-v1")
  expect_identical(contract$test_numerator, 200000L)
  expect_identical(contract$test_denominator, 1000000L)
  expect_identical(contract$privacy_unit, "patient")
  expect_identical(contract$unit_canonicalization, "trim-utf8-v2")
  expect_identical(
    contract$sha256,
    "00b0a490eb3d92fec7ce532e452523a32cbf73d19953372194faffc21eb4c75b")
  expect_false(any(grepl("seed|salt|nonce", names(contract), ignore.case = TRUE)))

  expect_identical(
    dsFlower:::.holdoutContract(200000L, "patient")$sha256,
    contract$sha256)
  expect_false(identical(
    dsFlower:::.holdoutContract(250000L, "patient")$sha256,
    contract$sha256))
})

test_that("holdout normalization is neural-only and hash pinned", {
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.dp_per_training_epsilon = 2,
    dsflower.dp_per_training_delta = 1e-5
  ))
  contract <- dsFlower:::.holdoutContract(200000L, "row")
  base <- list(
    "dp-track" = "neural", "num-server-rounds" = 2L,
    "resampling-version" = contract$version,
    "resampling-method" = contract$method,
    "resampling-assignment" = contract$assignment,
    "resampling-test-numerator" = contract$test_numerator,
    "resampling-test-denominator" = contract$test_denominator,
    "resampling-privacy-unit" = contract$privacy_unit,
    "resampling-unit-canonicalization" = contract$unit_canonicalization,
    "resampling-contract-sha256" = contract$sha256,
    "holdout-validation-bins" = 32L
  )

  normalized <- dsFlower:::.addDpConfigToRunConfig(base)
  expect_identical(normalized[["resampling-contract-sha256"]], contract$sha256)
  expect_equal(normalized[["privacy-training-epsilon"]], 1.6)
  expect_equal(normalized[["privacy-holdout-epsilon"]], 0.4)
  expect_equal(normalized[["privacy-training-delta"]], 8e-6)
  expect_equal(normalized[["privacy-holdout-delta"]], 2e-6)
  expect_equal(normalized[["privacy-epsilon"]], 2)
  expect_equal(normalized[["privacy-delta"]], 1e-5)

  expect_error(
    dsFlower:::.addDpConfigToRunConfig(c(base, list("resampling-seed" = 1L))),
    "seed|field", ignore.case = TRUE)
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(within(base, {
      `resampling-contract-sha256` <- paste(rep("0", 64L), collapse = "")
    })),
    "contract SHA-256"
  )
  expect_error(
    dsFlower:::.normalizeResamplingConfig(base, "native_tree"),
    "neural"
  )
})

test_that("prepared manifest pins the complete job allocation and contract", {
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.dp_per_training_epsilon = 2,
    dsflower.dp_per_training_delta = 1e-5
  ))
  contract <- dsFlower:::.holdoutContract(200000L, "row")
  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "neural", "num-server-rounds" = 2L,
    "resampling-version" = contract$version,
    "resampling-method" = contract$method,
    "resampling-assignment" = contract$assignment,
    "resampling-test-numerator" = contract$test_numerator,
    "resampling-test-denominator" = contract$test_denominator,
    "resampling-privacy-unit" = contract$privacy_unit,
    "resampling-unit-canonicalization" = contract$unit_canonicalization,
    "resampling-contract-sha256" = contract$sha256,
    "holdout-validation-bins" = 32L
  ))
  token <- "run_00000000000000000000000000000901"
  withr::defer(dsFlower:::.cleanupStaging(token))
  staged <- dsFlower:::.stageData(
    data.frame(x = seq_len(10), y = rep(0:1, 5)), token, "y", "x",
    extra_config = config)
  manifest <- jsonlite::fromJSON(
    file.path(staged, "manifest.json"), simplifyVector = TRUE)

  expect_identical(manifest[["resampling-contract-sha256"]], contract$sha256)
  expect_equal(manifest[["privacy-training-epsilon"]], 1.6)
  expect_equal(manifest[["privacy-holdout-epsilon"]], 0.4)
  expect_equal(
    manifest[["privacy-training-delta"]] +
      manifest[["privacy-holdout-delta"]],
    manifest[["privacy-delta"]])
})
