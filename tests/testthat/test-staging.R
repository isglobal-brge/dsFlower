# Tests for R/staging.R — Manifest-Based Data Staging

.test_run_token <- function(index) {
  paste0("run_", sprintf("%032x", as.integer(index)))
}

test_that(".generate_run_token produces expected format", {
  token <- dsFlower:::.generate_run_token()
  expect_type(token, "character")
  expect_match(token, "^run_[0-9a-f]{32}$")
})

test_that(".generate_run_token produces unique tokens", {
  t1 <- dsFlower:::.generate_run_token()
  Sys.sleep(0.01)
  t2 <- dsFlower:::.generate_run_token()
  # Tokens should differ (at least the hex suffix)
  expect_false(identical(t1, t2))
})

test_that("run tokens and staging paths fail closed before recursive deletion", {
  victim <- withr::local_tempdir()
  marker <- file.path(victim, "keep.txt")
  writeLines("keep", marker)
  for (bad in c("../../victim", "run_../../victim", "run_short",
                paste0("run_", strrep("A", 32)))) {
    expect_error(dsFlower:::.cleanupStaging(bad), "Invalid dsFlower run token")
  }
  expect_true(file.exists(marker))

  token <- .test_run_token(100)
  outside <- file.path(victim, token)
  dir.create(outside)
  expect_error(
    dsFlower:::.validateStagingDir(outside, token),
    "outside the permitted"
  )

  staging <- dsFlower:::.ensureStagingDir(token)
  withr::defer(dsFlower:::.cleanupStaging(token))
  link <- tempfile("staging-link-")
  if (isTRUE(file.symlink(staging, link))) {
    withr::defer(unlink(link))
    expect_error(
      dsFlower:::.validateStagingDir(link, token),
      "symbolic link"
    )
  }
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

test_that(".validateDataSchema accepts an empty frame with public schema", {
  data <- data.frame(f1 = numeric(0), target = numeric(0))
  expect_true(dsFlower:::.validateDataSchema(data, "target"))
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
  token <- .test_run_token(1)

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
  expect_equal(manifest$n_units, 10)
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
  token <- .test_run_token(2)

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1"),
    extra_config = list()
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  # DP-always contract: DP enabled, suppression + fixed sampling forced on.
  expect_true(manifest$dp_enabled)
  expect_false(manifest$allow_per_node_metrics)
  expect_false(manifest$allow_exact_num_examples)
  expect_true(manifest$fixed_client_sampling)
})

test_that("run_config cannot provide structural or duplicate manifest fields", {
  expect_error(
    dsFlower:::.validate_client_run_config(list(run_token = "attacker")),
    "server-owned manifest field"
  )
  expect_error(
    dsFlower:::.validate_client_run_config(list(patient_column = "row_id")),
    "server-owned manifest field"
  )
  expect_error(
    dsFlower:::.validate_client_run_config(list(n_units = 1L)),
    "server-owned manifest field"
  )
  expect_error(
    dsFlower:::.validate_client_run_config(list(`user-module` = "attacker")),
    "server-owned manifest field"
  )
  duplicated <- list(1L, 2L)
  names(duplicated) <- c("batch-size", "batch-size")
  expect_error(
    dsFlower:::.validate_client_run_config(duplicated),
    "unique, non-empty"
  )

  data <- data.frame(f1 = 1:10, target = rep(0:1, 5))
  token <- .test_run_token(3)
  expect_error(
    dsFlower:::.stageData(
      data, token, "target", "f1", extra_config = list(data_file = "../../data")
    ),
    "server-owned field"
  )
})

test_that("tabular staging preserves the explicit server patient DP unit", {
  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "patient_id"))
  data <- data.frame(
    patient_id = rep(sprintf("p%02d", 1:5), each = 2),
    f1 = 1:10,
    target = rep(0:1, 5)
  )
  token <- .test_run_token(4)
  staging_dir <- dsFlower:::.stageData(data, token, "target", "f1")
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$data_type, "tabular")
  expect_equal(manifest[["dp-unit"]], "patient")
  expect_equal(manifest$patient_column, "patient_id")
  expect_equal(manifest$n_units, 5L)
  expect_false("patient_id" %in% manifest$feature_columns)
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_true("patient_id" %in% names(staged))
})

test_that("Parquet descriptor staging preserves patient ID outside features", {
  skip_if_not_installed("arrow")
  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "subject_id"))
  source_file <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data.frame(
    subject_id = rep(sprintf("s%02d", 1:5), each = 2),
    f1 = 1:10,
    target = rep(0:1, 5)
  ), source_file)
  on.exit(unlink(source_file), add = TRUE)
  desc <- list(
    metadata = list(file = source_file),
    dataset_id = "test-dataset"
  )
  token <- .test_run_token(5)
  staging_dir <- dsFlower:::.stageFromDescriptor_parquet(
    desc, token, "target", "f1", list()
  )
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$patient_column, "subject_id")
  expect_equal(manifest$n_units, 5L)
  expect_equal(manifest$feature_columns, "f1")
  staged <- arrow::read_parquet(file.path(staging_dir, manifest$data_file))
  expect_true("subject_id" %in% names(staged))
})

test_that("image staging counts the server-selected patient privacy units", {
  image_root <- tempfile("dsflower-image-root-")
  dir.create(image_root)
  withr::defer(unlink(image_root, recursive = TRUE))
  withr::local_options(list(
    dsflower.image_data_root = image_root,
    dsflower.dp_unit = "patient",
    dsflower.patient_column = "patient_id"
  ))
  samples <- data.frame(
    patient_id = rep(c("p1", "p2", "p3"), each = 2L),
    relative_path = sprintf("image-%d.png", 1:6),
    target = rep(0:1, 3)
  )
  staging_dir <- dsFlower:::.stage_image_manifest(
    .test_run_token(6), "target", samples)
  withr::defer(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 6L)
  expect_equal(manifest$n_units, 3L)
  expect_equal(manifest[["dp-unit"]], "patient")
  expect_equal(manifest$patient_column, "patient_id")
})

test_that("privacy reservation update is atomic and keeps exact no-op budgets", {
  data <- data.frame(f1 = 1:10, target = rep(0:1, 5))
  token <- paste0("run_", strrep("c", 32))
  staging_dir <- dsFlower:::.stageData(
    data, token, "target", "f1",
    extra_config = list(
      "privacy-max-releases" = 5L,
      "privacy-reserved" = FALSE,
      "privacy-release-enabled" = FALSE,
      "privacy-epsilon" = 0,
      "privacy-delta" = 0
    )
  )
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)
  reservation <- list(
    run_token = token,
    max_releases = 5L,
    release_enabled = FALSE,
    domain = "node",
    allocation_index = 30L,
    epsilon = 2.79396772384644e-9,
    delta = 9.31322574615479e-15,
    dp_unit = "row",
    patient_column = NULL,
    unit_canonicalization = "trim-utf8-v1"
  )
  dsFlower:::.apply_privacy_reservation(staging_dir, reservation)

  manifest_path <- file.path(staging_dir, "manifest.json")
  manifest <- jsonlite::fromJSON(manifest_path)
  expect_false(manifest[["privacy-release-enabled"]])
  expect_equal(manifest[["privacy-epsilon"]], reservation$epsilon)
  expect_equal(manifest[["privacy-delta"]], reservation$delta)
  expect_length(list.files(staging_dir, pattern = "^\\.manifest-", all.files = TRUE), 0L)
  if (.Platform$OS.type == "unix") {
    expect_equal(as.integer(file.info(manifest_path)$mode),
                 as.integer(as.octmode("0600")))
  }

  reservation$run_token <- paste0("run_", strrep("d", 32))
  expect_error(
    dsFlower:::.apply_privacy_reservation(staging_dir, reservation),
    "run token does not match"
  )
})

test_that("public target levels give cohort-independent codes", {
  cfg <- dsFlower:::.normalizePublicTargetConfig(list(
    "task-type" = "classification", "dp-track" = "neural",
    "num-classes" = 3L, "target-levels" = c("b", "c", "d")))
  first <- dsFlower:::.transformPublicTarget(
    data.frame(y = c("b", "c", "d")), "y", cfg)
  second <- dsFlower:::.transformPublicTarget(
    data.frame(y = c("c", "d", "c")), "y", cfg)
  expect_equal(first$y, 0:2)
  expect_equal(second$y, c(1L, 2L, 1L))
  expect_equal(
    dsFlower:::.transformPublicTarget(data.frame(y = "unknown"), "y", cfg)$y,
    0L)

  token <- .test_run_token(7)
  staging_dir <- dsFlower:::.stageData(
    data.frame(x = 1:3, y = c("d", "b", "c")), token, "y", "x", cfg)
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_equal(staged$y, c(2L, 0L, 1L))
  expect_equal(manifest[["target-levels"]]$values, c("b", "c", "d"))
  expect_true(manifest[["target-preencoded"]])
})

test_that("public target bounds are mandatory and clip numeric targets", {
  expect_error(
    dsFlower:::.normalizePublicTargetConfig(list("task-type" = "regression")),
    "required")
  cfg <- dsFlower:::.normalizePublicTargetConfig(list(
    "task-type" = "regression",
    "target-bounds" = list(lower = 0, upper = 10)))
  out <- dsFlower:::.transformPublicTarget(
    data.frame(y = c(-5, 5, 20, NA, Inf, "not-a-number")), "y", cfg)
  expect_equal(out$y, c(0, 5, 10, 5, 5, 5))
  expect_error(
    dsFlower:::.normalizePublicTargetConfig(list(
      "task-type" = "regression",
      "target-bounds" = list(lower = -1e300, upper = 1e300))),
    "declared.*1e6"
  )
})

test_that("public feature totalisation cannot overflow float32", {
  cfg <- dsFlower:::.normalizePublicFeatureBounds(list(
    "feature-bounds" = list(lower = -10, upper = 10)))
  out <- dsFlower:::.totalizeModelFeatures(
    data.frame(x = c(1e300, -1e300, NA, "not-numeric")), "x", cfg)
  expect_true(all(is.finite(out$x)))
  expect_true(all(abs(out$x) <= 1e6))
  expect_equal(out$x[3:4], c(0, 0))
  expect_error(
    dsFlower:::.normalizePublicFeatureBounds(list(
      "feature-bounds" = list(lower = -1e300, upper = 1e300))),
    "declared.*1e6"
  )
})

test_that(".stageData totalises incomplete rows without a count oracle", {
  data <- data.frame(
    f1 = c(1, 2, NA, 4, 5, 6),
    f2 = c(1, 2, 3, Inf, 5, 6),
    unused = c(NA, NA, NA, NA, NA, NA),
    target = c(0, 1, 0, 1, NA, 0)
  )
  token <- .test_run_token(8)

  staging_dir <- dsFlower:::.stageData(
    data, token, "target", c("f1", "f2")
  )
  on.exit(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_input_samples, 6)
  expect_equal(manifest$n_samples, 6)
  expect_equal(manifest$n_units, 6)
  expect_equal(manifest$dropped_missing, 0)
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_true(all(is.finite(staged$f1)))
  expect_true(all(is.finite(staged$f2)))
  expect_true(all(is.finite(staged$target)))
})

test_that("patient staging collapses unusable identifiers into one safe unit", {
  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "patient_id"))
  data <- data.frame(
    patient_id = c("p1", NA, " ", "null", "p2"),
    f1 = seq_len(5),
    target = c(0, 1, 0, 1, 0)
  )
  token <- .test_run_token(81)
  staging_dir <- dsFlower:::.stageData(data, token, "target", "f1")
  withr::defer(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 5L)
  expect_equal(manifest$n_units, 3L)
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_false(anyNA(staged$patient_id))
  expect_equal(sum(staged$patient_id == "__dsflower_missing_patient_unit__"), 3L)
})

test_that("client config cannot preserve non-finite private rows", {
  data <- data.frame(f1 = c(1, NA, 3), target = c(0, 1, 0))
  token <- .test_run_token(9)

  expect_error(
    dsFlower:::.stageData(
      data, token, "target", c("f1"),
      extra_config = list(drop_missing = FALSE)
    ),
    "server-owned"
  )
})

test_that(".ensureStagingDir honors configured staging root", {
  root <- tempfile("dsflower-stage-root-")
  old <- getOption("dsflower.staging_root")
  options(dsflower.staging_root = root)
  on.exit({
    options(dsflower.staging_root = old)
    unlink(root, recursive = TRUE)
  }, add = TRUE)

  staging_dir <- dsFlower:::.ensureStagingDir(.test_run_token(10))
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

test_that("image metadata paths cannot escape their configured root", {
  expect_equal(
    dsFlower:::.safeRelativeAssetPath("nested/scan.png"),
    "nested/scan.png"
  )
  for (bad in c("../secret", "/absolute", "a/../../secret",
                "a\\..\\secret", "C:/secret", "a//secret", "./secret")) {
    expect_error(
      dsFlower:::.safeRelativeAssetPath(bad),
      "unsafe relative path"
    )
  }

  expect_equal(
    dsFlower:::.s3RelativePath(
      "s3://imaging/site/images/sub/scan.png",
      "s3://imaging/site/images/"),
    "sub/scan.png"
  )
  expect_error(
    dsFlower:::.s3RelativePath(
      "s3://imaging/site/private/scan.png",
      "s3://imaging/site/images/"),
    "outside its configured S3 prefix"
  )
  expect_error(
    dsFlower:::.s3RelativePath(
      "s3://other/site/images/scan.png",
      "s3://imaging/site/images/"),
    "outside its configured S3 prefix"
  )
  expect_error(
    dsFlower:::.s3RelativePath(
      "s3://imaging/site/images/../secret",
      "s3://imaging/site/images/"),
    "unsafe relative path"
  )
})

test_that("image path failures never echo private sample identifiers", {
  sentinel <- "PRIVATE_SAMPLE_ID_7329"
  root <- withr::local_tempdir()
  err <- tryCatch(
    dsFlower:::.ensureImagePathColumn(
      data.frame(sample_id = paste0("../", sentinel), label = 1L),
      image_root = root
    ),
    error = identity
  )
  expect_s3_class(err, "error")
  expect_false(grepl(sentinel, conditionMessage(err), fixed = TRUE))

  expect_error(
    dsFlower:::.ensureImagePathColumn(
      data.frame(relative_path = "../secret", label = 1L)
    ),
    "unsafe relative path"
  )
})

test_that("image descriptor asset names cannot traverse staging", {
  desc <- list(
    metadata = list(),
    assets = stats::setNames(
      list(list(type = "image_root", root = tempdir())),
      "../escape"),
    manifest = list()
  )
  expect_error(
    dsFlower:::.stageFromDescriptor_image(
      desc, .test_run_token(12), "label", NULL, list()),
    "safe single components"
  )
})

test_that(".cleanupStaging removes the directory", {
  token <- .test_run_token(11)
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
