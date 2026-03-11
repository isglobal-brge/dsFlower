# Tests for R/staging.R — Manifest-Based Data Staging

test_that(".generate_run_token produces expected format", {
  token <- dsFlower:::.generate_run_token()
  expect_type(token, "character")
  expect_true(grepl("^run_\\d{8}_\\d{6}_[0-9a-f]{4}$", token))
})

test_that(".generate_run_token produces unique tokens", {
  t1 <- dsFlower:::.generate_run_token()
  Sys.sleep(0.01)
  t2 <- dsFlower:::.generate_run_token()
  # Tokens should differ (at least the hex suffix)
  expect_false(identical(t1, t2))
})

test_that(".loadTrainingData reads CSV correctly", {
  csv_path <- create_test_csv(n = 20)
  on.exit(unlink(csv_path))

  data <- dsFlower:::.loadTrainingData(csv_path, "csv")
  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 20)
  expect_true("target" %in% names(data))
  expect_true("f1" %in% names(data))
})

test_that(".loadTrainingData errors on missing file", {
  expect_error(
    dsFlower:::.loadTrainingData("/nonexistent/file.csv", "csv"),
    "not found"
  )
})

test_that(".loadTrainingData errors on unsupported format", {
  csv_path <- create_test_csv(n = 5)
  on.exit(unlink(csv_path))

  expect_error(
    dsFlower:::.loadTrainingData(csv_path, "hdf5"),
    "Unsupported data format"
  )
})

test_that(".validateDataSchema passes for valid data", {
  data <- data.frame(f1 = 1:5, f2 = 6:10, target = rep(1, 5))
  expect_true(
    dsFlower:::.validateDataSchema(data, "target", c("f1", "f2"))
  )
})

test_that(".validateDataSchema errors on empty data", {
  data <- data.frame(f1 = numeric(0), target = numeric(0))
  expect_error(
    dsFlower:::.validateDataSchema(data, "target"),
    "empty"
  )
})

test_that(".validateDataSchema errors on missing target", {
  data <- data.frame(f1 = 1:5, f2 = 6:10)
  expect_error(
    dsFlower:::.validateDataSchema(data, "nonexistent"),
    "not found"
  )
})

test_that(".validateDataSchema errors on missing features", {
  data <- data.frame(f1 = 1:5, target = 1:5)
  expect_error(
    dsFlower:::.validateDataSchema(data, "target", c("f1", "f99")),
    "not found"
  )
})

test_that(".stageData creates directory, CSV, and manifest", {
  data <- data.frame(f1 = 1:10, f2 = 11:20, target = rep(0:1, 5))
  token <- "run_test_staging_001"

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1", "f2"),
    extra_config = list(num_rounds = 5L)
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  expect_true(dir.exists(staging_dir))
  expect_true(file.exists(file.path(staging_dir, "train.csv")))
  expect_true(file.exists(file.path(staging_dir, "manifest.json")))

  # Verify CSV
  loaded <- utils::read.csv(file.path(staging_dir, "train.csv"))
  expect_equal(nrow(loaded), 10)

  # Verify manifest
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$run_token, token)
  expect_equal(manifest$data_file, "train.csv")
  expect_equal(manifest$n_samples, 10)
  expect_equal(manifest$target_column, "target")
  expect_equal(manifest$feature_columns, c("f1", "f2"))
  expect_equal(manifest$num_rounds, 5L)
})

test_that(".cleanupStaging removes the directory", {
  token <- "run_test_cleanup_001"
  staging_dir <- file.path(tempdir(), "dsflower", token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)
  writeLines("test", file.path(staging_dir, "test.txt"))

  expect_true(dir.exists(staging_dir))
  dsFlower:::.cleanupStaging(token)
  expect_false(dir.exists(staging_dir))
})

test_that(".cleanupStaging is safe for NULL token", {
  expect_true(dsFlower:::.cleanupStaging(NULL))
})

test_that(".getDataSummary returns correct structure", {
  csv_path <- create_test_csv(n = 30)
  on.exit(unlink(csv_path))

  summary <- dsFlower:::.getDataSummary(csv_path, "csv")
  expect_equal(summary$n_rows, 30)
  expect_equal(summary$n_cols, 6)
  expect_true("target" %in% summary$columns)
  expect_true("f1" %in% summary$columns)
})
