# Tests for R/staging.R — Manifest-Based Data Staging

test_that(".generate_run_token produces expected format", {
  token <- dsFlower:::.generate_run_token()
  expect_type(token, "character")
  # Format: run_YYYYMMDD_HHMMSS_PID_XXXXXXXXXXXX (12 hex chars)
  expect_true(grepl("^run_\\d{8}_\\d{6}_\\d+_[0-9a-f]{12}$", token))
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

test_that(".stageData creates directory, data file, and manifest", {
  data <- data.frame(f1 = 1:10, f2 = 11:20, target = rep(0:1, 5))
  token <- "run_test_staging_001"

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1", "f2"),
    extra_config = list(num_rounds = 5L)
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  expect_true(dir.exists(staging_dir))
  expect_true(file.exists(file.path(staging_dir, "manifest.json")))

  # Verify manifest
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$run_token, token)
  expect_equal(manifest$n_samples, 10)
  expect_equal(manifest$target_column, "target")
  expect_equal(manifest$feature_columns, c("f1", "f2"))
  expect_equal(manifest$num_rounds, 5L)

  # Data file should be either parquet or csv depending on arrow availability
  expect_true(manifest$data_format %in% c("csv", "parquet"))
  expect_true(file.exists(file.path(staging_dir, manifest$data_file)))

  # Verify directory permissions are strict (0700)
  dir_info <- file.info(staging_dir)
  dir_mode <- as.character(dir_info$mode)
  expect_true(dir_mode %in% c("700", "0700"))
})

test_that(".stageData writes privacy settings from extra_config", {
  data <- data.frame(f1 = 1:10, target = rep(0:1, 5))
  token <- "run_test_staging_privacy"

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1"),
    extra_config = list(
      allow_per_node_metrics = FALSE,
      allow_exact_num_examples = FALSE,
      require_secure_aggregation = TRUE,
      privacy_profile = "clinical_default"
    )
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_false(manifest$allow_per_node_metrics)
  expect_false(manifest$allow_exact_num_examples)
  expect_true(manifest$require_secure_aggregation)
  expect_equal(manifest$privacy_profile, "clinical_default")
})

test_that(".stageData drops incomplete target/feature rows before manifesting", {
  data <- data.frame(
    f1 = c(1, 2, NA, 4, 5, 6),
    f2 = c(1, 2, 3, Inf, 5, 6),
    unused = c(NA, NA, NA, NA, NA, NA),
    target = c(0, 1, 0, 1, NA, 0)
  )
  token <- "run_test_staging_missing"

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1", "f2")
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_input_samples, 6)
  expect_equal(manifest$n_samples, 3)
  expect_equal(manifest$dropped_missing, 3)
})

test_that(".stageData can preserve incomplete rows when explicitly requested", {
  data <- data.frame(f1 = c(1, NA, 3), target = c(0, 1, 0))
  token <- "run_test_staging_keep_missing"

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1"),
    extra_config = list(drop_missing = FALSE)
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_input_samples, 3)
  expect_equal(manifest$n_samples, 3)
  expect_equal(manifest$dropped_missing, 0)
})

test_that(".ensureStagingDir honors configured staging root", {
  root <- tempfile("dsflower-stage-root-")
  old <- getOption("dsflower.staging_root")
  options(dsflower.staging_root = root)
  on.exit({
    options(dsflower.staging_root = old)
    unlink(root, recursive = TRUE)
  }, add = TRUE)

  staging_dir <- dsFlower:::.ensureStagingDir("run_test_configured_root")
  expect_true(dir.exists(staging_dir))
  expect_true(startsWith(normalizePath(staging_dir, mustWork = TRUE),
                         normalizePath(root, mustWork = TRUE)))
})

test_that(".ensureImagePathColumn derives paths from dsImaging manifests", {
  samples <- data.frame(
    sample_id = c("LUNG1-001", "LUNG1-002"),
    label = c(0L, 1L)
  )
  sample_manifests <- data.frame(
    sample_id = c("LUNG1-001", "LUNG1-002"),
    primary_uri = c(
      "LUNG1-001.nii.gz",
      "s3://imaging-data/datasets/site/source/images/LUNG1-002.nii.gz"
    )
  )

  out <- dsFlower:::.ensureImagePathColumn(
    samples,
    sample_manifests = sample_manifests,
    image_uri = "s3://imaging-data/datasets/site/source/images/",
    downloaded_rels = c("LUNG1-001.nii.gz", "LUNG1-002.nii.gz")
  )

  expect_equal(out$relative_path,
               c("LUNG1-001.nii.gz", "LUNG1-002.nii.gz"))
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
