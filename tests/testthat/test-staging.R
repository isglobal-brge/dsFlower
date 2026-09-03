# Tests for R/staging.R — Manifest-Based Data Staging

.test_run_token <- function(index) {
  paste0("run_", sprintf("%032x", as.integer(index)))
}

.test_imaging_metadata <- function(privacy_unit_col, ...) {
  c(list(
    privacy_unit = "patient",
    privacy_unit_col = privacy_unit_col,
    privacy_unit_canonicalization = "trim-utf8-v2"
  ), list(...))
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
  if (isTRUE(file.symlink(marker, link))) {
    withr::defer(expect_no_warning({
      if (.Platform$OS.type == "windows") {
        expect_identical(dsFlower:::.run_windows_powershell(paste0(
          "[IO.File]::Delete(",
          dsFlower:::.powershell_literal(link),
          ");Write-Output 'OK'"
        )), "OK")
      } else {
        unlink(link)
      }
      expect_false(file.exists(link) || dir.exists(link))
      expect_true(file.exists(marker))
    }))
    expect_error(
      dsFlower:::.validateStagingDir(link, token),
      "link or reparse point"
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
    "configured training data source is unavailable"
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
    extra_config = list("num-server-rounds" = 5L)
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
  expect_equal(manifest[["num-server-rounds"]], 5L)

  # Data file should be either parquet or csv depending on arrow availability
  expect_true(manifest$data_format %in% c("csv", "parquet"))
  expect_true(file.exists(file.path(staging_dir, manifest$data_file)))

  if (.Platform$OS.type == "windows") {
    expect_no_error(dsFlower:::.windows_validate_private_acl(staging_dir))
  } else {
    dir_info <- file.info(staging_dir)
    dir_mode <- as.character(dir_info$mode)
    expect_true(dir_mode %in% c("700", "0700"))
  }
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
  expect_error(
    dsFlower:::.validate_client_run_config(list(privacy_future_field = 1)),
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

test_that("stale staging cleanup covers configured roots", {
  root <- withr::local_tempdir()
  old <- file.path(root, "dsflower", "run_old")
  fresh <- file.path(root, "dsflower", "run_fresh")
  dir.create(old, recursive = TRUE)
  dir.create(fresh, recursive = TRUE)
  writeLines("private", file.path(old, "manifest.json"))
  Sys.setFileTime(old, Sys.time() - 48 * 60 * 60)

  dsFlower:::.cleanup_stale_staging(max_age_hours = 24, bases = root)

  expect_false(dir.exists(old))
  expect_true(dir.exists(fresh))
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

test_that("stateless privacy contract update is atomic", {
  data <- data.frame(f1 = 1:10, target = rep(0:1, 5))
  token <- paste0("run_", strrep("c", 32))
  run_config <- dsFlower:::.addDpConfigToRunConfig(list(
    "num-server-rounds" = 5L
  ))
  staging_dir <- dsFlower:::.stageData(
    data, token, "target", "f1",
    extra_config = run_config
  )
  on.exit(unlink(staging_dir, recursive = TRUE), add = TRUE)
  contract <- dsFlower:::.privacy_training_contract(token, 5L)
  dsFlower:::.apply_privacy_contract(staging_dir, contract)

  manifest_path <- file.path(staging_dir, "manifest.json")
  manifest <- jsonlite::fromJSON(manifest_path)
  expect_identical(manifest[["num-server-rounds"]], 5L)
  expect_equal(manifest[["privacy-epsilon"]], contract$epsilon)
  expect_equal(manifest[["privacy-delta"]], contract$delta)
  expect_identical(manifest[["privacy-policy-sha256"]], contract$policy_hash)
  expect_length(list.files(staging_dir, pattern = "^\\.manifest-", all.files = TRUE), 0L)
  if (.Platform$OS.type == "unix") {
    expect_equal(as.integer(file.info(manifest_path)$mode),
                 as.integer(as.octmode("0600")))
  }

  contract$run_token <- paste0("run_", strrep("d", 32))
  expect_error(
    dsFlower:::.apply_privacy_contract(staging_dir, contract),
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

test_that("multilabel staging applies one public binary contract per target", {
  cfg <- dsFlower:::.normalizePublicTargetConfig(list(
    "task-type" = "classification", "dp-track" = "neural",
    "loss-name" = "multilabel_bce", "num-classes" = 2L,
    "num-labels" = 2L, "target-levels" = c("absent", "present")))
  data <- data.frame(
    x = 1:3,
    first = c("absent", "present", "unknown"),
    second = c("present", "present", "absent"))

  transformed <- dsFlower:::.transformPublicTarget(
    data, c("first", "second"), cfg)
  expect_identical(transformed$first, c(0L, 1L, 0L))
  expect_identical(transformed$second, c(1L, 1L, 0L))

  token <- .test_run_token(70)
  staging_dir <- dsFlower:::.stageData(
    data, token, c("first", "second"), "x", cfg)
  withr::defer(unlink(staging_dir, recursive = TRUE))
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_identical(manifest$target_column, c("first", "second"))
  expect_equal(staged[c("first", "second")],
               transformed[c("first", "second")])

  expect_error(
    dsFlower:::.transformPublicTarget(data, "first", cfg),
    "num-labels=2"
  )
  expect_error(
    dsFlower:::.transformPublicTarget(data, c("first", "first"), cfg),
    "unique"
  )
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

test_that("patient staging applies the exact v2 identifier contract", {
  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "patient_id"))
  missing_unit <- "__dsflower_missing_patient_unit__"
  data <- data.frame(
    patient_id = c(
      NA, "", "  \t\r\n", "NA", "NaN", "NULL", "<NA>", "NaT",
      " patient ", "patient", "\u00a0patient\u00a0", missing_unit
    ),
    f1 = seq_len(12),
    target = rep(0:1, 6)
  )
  token <- .test_run_token(81)
  staging_dir <- dsFlower:::.stageData(data, token, "target", "f1")
  withr::defer(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 12L)
  expect_equal(manifest$n_units, 3L)
  expect_equal(manifest[["patient-id-canonicalization"]], "trim-utf8-v2")
  staged <- dsFlower:::.loadTrainingData(
    file.path(staging_dir, manifest$data_file), manifest$data_format)
  expect_false(anyNA(staged$patient_id))
  expect_equal(sum(staged$patient_id == missing_unit), 9L)
  expect_equal(staged$patient_id[9:10], rep("patient", 2L))
  # Non-ASCII whitespace is deliberately not trimmed in v2; this keeps R and
  # Python on one exact, locale-independent contract.
  expect_equal(staged$patient_id[[11]], "\u00a0patient\u00a0")
})

test_that("patient identifier v2 totalizes invalid encodings per value", {
  invalid_utf8 <- rawToChar(as.raw(0xff))
  Encoding(invalid_utf8) <- "bytes"
  expect_equal(
    dsFlower:::.canonicalPatientIdText(
      c(" \tpatient\r\n", "\u00a0patient\u00a0", invalid_utf8)),
    c("patient", "\u00a0patient\u00a0", NA_character_)
  )
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

test_that("image path failures are record-local and preserve row order", {
  root <- withr::local_tempdir()
  dir.create(file.path(root, "nested"))
  writeLines("image", file.path(root, "valid.png"))
  writeLines("image", file.path(root, "nested", "fallback.png"))

  samples <- data.frame(
    row_marker = seq_len(5),
    sample_id = c("valid", "unsafe", "missing", "fallback", "bad/id"),
    relative_path = c("valid.png", "../private", "missing.png", "", NA),
    label = c(1L, 0L, 1L, 0L, 1L)
  )
  sample_manifests <- data.frame(
    sample_id = "fallback",
    primary_uri = "nested/fallback.png"
  )

  out <- dsFlower:::.ensureImagePathColumn(
    samples,
    sample_manifests = sample_manifests,
    image_root = root
  )

  expect_equal(out$row_marker, seq_len(5))
  expect_equal(nrow(out), nrow(samples))
  expect_equal(
    out$relative_path,
    c(
      "valid.png",
      "__dsflower_invalid_image__",
      "__dsflower_invalid_image__",
      "nested/fallback.png",
      "__dsflower_invalid_image__"
    )
  )

  outside <- withr::local_tempdir()
  outside_file <- file.path(outside, "outside.png")
  writeLines("private", outside_file)
  linked <- file.path(root, "linked.png")
  if (isTRUE(suppressWarnings(file.symlink(outside_file, linked)))) {
    linked_out <- dsFlower:::.ensureImagePathColumn(
      data.frame(relative_path = "linked.png", label = 1L),
      image_root = root
    )
    expect_equal(linked_out$relative_path, "__dsflower_invalid_image__")
  }
})

test_that("malformed sample-manifest cells totalize independently", {
  samples <- data.frame(sample_id = c("outside", "traversal", "bad-json"))
  sample_manifests <- data.frame(
    sample_id = samples$sample_id,
    primary_uri = c(
      "s3://other/private/scan.png",
      "../private/scan.png",
      ""
    ),
    files_json = c("[]", "[]", "{not-json")
  )

  out <- dsFlower:::.ensureImagePathColumn(
    samples,
    sample_manifests = sample_manifests,
    image_uri = "s3://imaging/site/images/"
  )

  expect_equal(nrow(out), 3L)
  expect_equal(
    out$relative_path,
    rep("__dsflower_invalid_image__", 3L)
  )
})

test_that("image descriptor and root failures remain global preconditions", {
  expect_error(
    dsFlower:::.ensureImagePathColumn(data.frame(label = 1L)),
    "requires either"
  )
  expect_error(
    dsFlower:::.ensureImagePathColumn(
      data.frame(relative_path = "scan.png", label = 1L),
      image_root = file.path(tempdir(), "missing-global-image-root")
    ),
    "configured image asset root is unavailable"
  )

  metadata <- .test_imaging_metadata(
    "patient_id", uri = "s3://imaging/site/metadata/samples.parquet")
  s3_desc <- list(
    metadata = metadata,
    assets = list(images = list(
      type = "image_root", uri = "s3://imaging/site/source/images/")),
    manifest = list(metadata = metadata),
    dataset_id = "site"
  )
  expect_error(
    dsFlower:::.stageFromDescriptor_image(
      s3_desc, .test_run_token(14), "label", NULL, list()),
    "has no storage backend"
  )
})

test_that("S3 image descriptors stage metadata and assets without credentials", {
  skip_if_not_installed("dsImaging")
  root <- withr::local_tempdir()
  withr::local_options(list(dsflower.staging_root = root))
  metadata_uri <- "s3://imaging/site/metadata/samples.csv"
  manifests_uri <- "s3://imaging/site/metadata/sample_manifests.csv"
  image_root_uri <- "s3://imaging/site/source/images/"
  image_uris <- paste0(
    image_root_uri, c("case1.png", "nested/case2.png"))
  calls <- new.env(parent = emptyenv())
  calls$list <- character()
  calls$head <- character()
  calls$get <- character()

  local_mocked_bindings(
    backend_list = function(backend, prefix) {
      calls$list <- c(calls$list, prefix)
      image_uris
    },
    backend_head = function(backend, uri) {
      calls$head <- c(calls$head, uri)
      list(exists = TRUE, size = 5L)
    },
    backend_get_file = function(backend, uri, dest, overwrite = FALSE) {
      calls$get <- c(calls$get, uri)
      dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
      if (identical(uri, metadata_uri)) {
        utils::write.csv(data.frame(
          sample_id = c("case1", "case2"),
          patient_id = c("patient1", "patient2"),
          label = c("negative", "positive")), dest, row.names = FALSE)
      } else if (identical(uri, manifests_uri)) {
        utils::write.csv(data.frame(
          sample_id = c("case1", "case2"), primary_uri = image_uris),
          dest, row.names = FALSE)
      } else if (uri %in% image_uris) {
        writeBin(charToRaw("image"), dest)
      } else {
        stop("Unexpected mocked S3 URI: ", uri)
      }
      invisible(dest)
    },
    .package = "dsImaging"
  )

  backend <- structure(list(
    type = "s3",
    config = list(resource = list(
      identity = "private-access-key", secret = "private-secret-key"))
  ), class = "dsimaging_backend")
  metadata <- .test_imaging_metadata(
    "patient_id", uri = metadata_uri, format = "csv")
  desc <- list(
    metadata = metadata,
    assets = list(images = list(
      type = "image_root", uri = image_root_uri,
      path_col = "relative_path")),
    manifest = list(
      metadata = metadata,
      sample_manifests = list(uri = manifests_uri, format = "csv")),
    backend = backend,
    dataset_id = "site"
  )

  staging_dir <- dsFlower:::.stageFromDescriptor_image(
    desc, .test_run_token(15), "label", NULL,
    list("target-levels" = list(
           type = "character", values = c("negative", "positive")),
         "num-classes" = 2L))
  withr::defer(unlink(staging_dir, recursive = TRUE))

  manifest_path <- file.path(staging_dir, "manifest.json")
  manifest <- jsonlite::fromJSON(manifest_path)
  samples <- dsFlower:::.readStagedSamples(
    file.path(staging_dir, manifest$samples_file))

  expect_identical(calls$list, image_root_uri)
  expect_setequal(calls$head, image_uris)
  expect_setequal(
    calls$get, c(metadata_uri, manifests_uri, image_uris))
  expect_equal(samples$relative_path, c("case1.png", "nested/case2.png"))
  expect_equal(samples$label, c(0L, 1L))
  expect_true(all(file.exists(file.path(
    manifest$assets$images$root, samples$relative_path))))
  manifest_text <- paste(readLines(manifest_path, warn = FALSE), collapse = "\n")
  expect_false(grepl("private-access-key", manifest_text, fixed = TRUE))
  expect_false(grepl("private-secret-key", manifest_text, fixed = TRUE))
})

test_that("vision staging rejects multi-file samples before materialization", {
  root <- withr::local_tempdir()
  metadata <- file.path(root, "samples.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("series-", 1:3),
    patient_id = paste0("patient-", 1:3), diagnosis = c(0L, 1L, 0L)),
    metadata, row.names = FALSE)
  desc <- flower_dataset_descriptor(
    "dicom.series", "image_bundle",
    metadata = .test_imaging_metadata("patient_id", uri = metadata,
      file = metadata, format = "csv", label_col = "diagnosis"),
    assets = list(images = list(type = "image_root", uri = root)),
    manifest = list(metadata = .test_imaging_metadata(
      "patient_id", uri = metadata, file = metadata, format = "csv",
      label_col = "diagnosis")))
  desc$.collection_snapshot <- list(records = lapply(1:3, function(i) list(
    source_kind = "dicom_series", n_files = 2L)))

  expect_error(
    dsFlower:::.stageFromDescriptor_image(
      desc, paste0("run_", strrep("a", 32)), "diagnosis", NULL, list()),
    "Convert DICOM series to one NIfTI")
})

test_that("vision payload contract accepts only supported self-contained files", {
  root <- withr::local_tempdir()
  write_payload <- function(name, bytes) {
    path <- file.path(root, name)
    writeBin(bytes, path)
    path
  }
  write_payload("inline.nrrd", c(
    charToRaw(paste0(
      "NRRD0005\n", "type: uint8\n", "dimension: 2\n",
      "sizes: 1 1\n", "encoding: raw\n\n")), as.raw(0)))
  write_payload("inline.mha", c(
    charToRaw(paste0(
      "ObjectType = Image\n", "NDims = 2\n", "DimSize = 1 1\n",
      "ElementType = MET_UCHAR\n", "ElementDataFile = LOCAL\n")),
    as.raw(c(0, 10, 0))))
  for (name in c("scan.nii", "scan.nii.gz", "scan.dcm", "scan.png",
                 "scan.jpg", "scan.jpeg", "scan.tif", "scan.tiff")) {
    write_payload(name, charToRaw("placeholder"))
  }

  expect_invisible(dsFlower:::.validateImagePayloadContract(
    c("inline.nrrd", "inline.mha", "scan.nii", "scan.nii.gz", "scan.dcm",
      "scan.png", "scan.jpg", "scan.jpeg", "scan.tif", "scan.tiff"),
    root))

  write_payload("detached.nrrd", charToRaw(paste0(
    "NRRD0005\n", "type: uint8\n", "dimension: 2\n", "sizes: 1 1\n",
    "encoding: raw\n", "data file: private.raw\n\n")))
  write_payload("detached.mha", charToRaw(paste0(
    "ObjectType = Image\n", "ElementDataFile = private.raw\n")))
  write_payload("detached.mhd", charToRaw(paste0(
    "ObjectType = Image\n", "ElementDataFile = private.raw\n")))
  write_payload("slide.svs", charToRaw("placeholder"))

  for (name in c("detached.nrrd", "detached.mha", "detached.mhd")) {
    expect_error(
      dsFlower:::.validateImagePayloadContract(name, root),
      "Detached NRRD/MHD payloads are not supported")
  }
  expect_error(
    dsFlower:::.validateImagePayloadContract("slide.svs", root),
    "supports only single-file")
})

test_that("detached medical payloads fail staging before a run manifest exists", {
  image_root <- withr::local_tempdir()
  staging_root <- withr::local_tempdir()
  withr::local_options(list(dsflower.staging_root = staging_root))
  metadata <- .test_imaging_metadata("patient_id")

  stage_detached <- function(name, contents, token_index) {
    writeBin(charToRaw(contents), file.path(image_root, name))
    desc <- list(
      metadata = metadata,
      table_data = data.frame(
        sample_id = c("sample-a", "sample-b"),
        patient_id = c("patient-a", "patient-b"),
        relative_path = name,
        label = c(0L, 1L)),
      assets = list(images = list(
        type = "image_root", root = image_root,
        path_col = "relative_path")),
      manifest = list(metadata = metadata),
      dataset_id = "detached-payload-test")
    token <- .test_run_token(token_index)
    expect_error(
      dsFlower:::.stageFromDescriptor_image(
        desc, token, "label", NULL, list()),
      "Detached NRRD/MHD payloads are not supported")
    expect_false(file.exists(file.path(staging_root, token, "manifest.json")))
  }

  stage_detached(
    "scan.nrrd",
    paste0("NRRD0005\n", "type: uint8\n", "dimension: 2\n",
           "sizes: 1 1\n", "data file: private.raw\n\n"),
    112)
  stage_detached(
    "scan.mhd", "ObjectType = Image\nElementDataFile = private.raw\n",
    113)
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

test_that("image path failures become one fixed non-identifying marker", {
  sentinel <- "PRIVATE_SAMPLE_ID_7329"
  root <- withr::local_tempdir()
  out <- dsFlower:::.ensureImagePathColumn(
    data.frame(sample_id = paste0("../", sentinel), label = 1L),
    image_root = root
  )
  expect_equal(out$relative_path, "__dsflower_invalid_image__")
  expect_false(grepl(sentinel, out$relative_path, fixed = TRUE))

  out <- dsFlower:::.ensureImagePathColumn(
    data.frame(relative_path = "../secret", label = 1L)
  )
  expect_equal(out$relative_path, "__dsflower_invalid_image__")
})

test_that("image descriptors require an exact server-authored patient unit", {
  descriptor <- list(manifest = list(metadata = list()))
  expect_error(
    dsFlower:::.imagingPrivacyUnitPolicy(descriptor),
    "privacy_unit='patient'"
  )

  wrong_unit <- .test_imaging_metadata("patient_id")
  wrong_unit$privacy_unit <- "row"
  wrong_canonicalization <- .test_imaging_metadata("patient_id")
  wrong_canonicalization$privacy_unit_canonicalization <- "trim-utf8-v1"
  invalid <- list(
    wrong_unit,
    .test_imaging_metadata(""),
    .test_imaging_metadata("   "),
    .test_imaging_metadata(c("patient_id", "subject_id")),
    wrong_canonicalization
  )
  for (metadata in invalid) {
    expect_error(
      dsFlower:::.imagingPrivacyUnitPolicy(
        list(manifest = list(metadata = metadata))),
      "privacy_unit='patient'"
    )
  }
})

test_that("image staging counts distinct canonical patients, not scans", {
  root <- withr::local_tempdir()
  writeLines("image", file.path(root, "scan.png"))
  withr::local_options(list(
    dsflower.dp_unit = "row",
    dsflower.patient_column = "wrong_global_column"
  ))
  n_scans <- as.integer(getOption(
    "nfilter.subset", getOption("default.nfilter.subset", 3))) + 1L
  metadata <- .test_imaging_metadata("subject_code")
  descriptor <- list(
    dataset_id = "one-patient-many-scans",
    metadata = metadata,
    manifest = list(metadata = metadata),
    table_data = data.frame(
      subject_code = rep(c(" patient-1 ", "patient-1"), length.out = n_scans),
      sample_id = paste0("scan-", seq_len(n_scans)),
      relative_path = rep("scan.png", n_scans),
      target = rep(0:1, length.out = n_scans)),
    assets = list(images = list(
      type = "image_root", root = root, path_col = "relative_path"))
  )

  staging_dir <- dsFlower:::.stageFromDescriptor_image(
    descriptor, .test_run_token(16), "target", NULL, list())
  withr::defer(unlink(staging_dir, recursive = TRUE))
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  staged <- dsFlower:::.readStagedSamples(
    file.path(staging_dir, manifest$samples_file))

  expect_equal(manifest$n_samples, n_scans)
  expect_equal(manifest$n_units, 1L)
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$patient_column, "subject_code")
  expect_identical(
    manifest[["patient-id-canonicalization"]], "trim-utf8-v2")
  expect_identical(unique(staged$subject_code), "patient-1")

  expect_error(
    dsFlower:::.stageFromDescriptor_image(
      descriptor, .test_run_token(19), "subject_code", NULL, list()),
    "privacy-unit column cannot be a model target"
  )
})

test_that("image datasets keep their own privacy-unit column", {
  root <- withr::local_tempdir()
  writeLines("image", file.path(root, "scan.png"))
  stage_dataset <- function(dataset_id, privacy_column, token_index) {
    metadata <- .test_imaging_metadata(privacy_column)
    samples <- data.frame(
      sample_id = c("scan-a", "scan-b"),
      relative_path = "scan.png",
      target = c(0L, 1L), stringsAsFactors = FALSE)
    samples[[privacy_column]] <- c("person-a", "person-b")
    descriptor <- list(
      dataset_id = dataset_id,
      metadata = metadata,
      manifest = list(metadata = metadata),
      table_data = samples,
      assets = list(images = list(
        type = "image_root", root = root, path_col = "relative_path"))
    )
    dsFlower:::.stageFromDescriptor_image(
      descriptor, .test_run_token(token_index), "target", NULL, list())
  }

  first <- stage_dataset("dataset-a", "subject_id", 17)
  second <- stage_dataset("dataset-b", "case_owner", 18)
  withr::defer(unlink(first, recursive = TRUE))
  withr::defer(unlink(second, recursive = TRUE))
  first_manifest <- jsonlite::fromJSON(file.path(first, "manifest.json"))
  second_manifest <- jsonlite::fromJSON(file.path(second, "manifest.json"))

  expect_identical(first_manifest$patient_column, "subject_id")
  expect_identical(second_manifest$patient_column, "case_owner")
  expect_equal(first_manifest$n_units, 2L)
  expect_equal(second_manifest$n_units, 2L)
})

test_that("image descriptor staging totalizes private rows end to end", {
  root <- withr::local_tempdir()
  writeLines("image", file.path(root, "valid.png"))
  metadata <- .test_imaging_metadata("patient_key")
  desc <- list(
    metadata = metadata,
    table_data = data.frame(
      row_marker = 1:3,
      patient_key = c(" patient-a ", "patient-a", "patient-b"),
      sample_id = c("valid", "unsafe", "missing"),
      relative_path = c("valid.png", "../private", "missing.png"),
      target = c(1L, 99L, NA)
    ),
    assets = list(images = list(
      type = "image_root", root = root, path_col = "relative_path")),
    manifest = list(metadata = metadata),
    dataset_id = "private-image-test"
  )
  staging_dir <- dsFlower:::.stageFromDescriptor_image(
    desc, .test_run_token(13), "target", NULL, list())
  withr::defer(unlink(staging_dir, recursive = TRUE))

  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  staged <- dsFlower:::.readStagedSamples(
    file.path(staging_dir, manifest$samples_file))
  expect_equal(manifest$n_samples, 3L)
  expect_equal(manifest$n_units, 2L)
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$patient_column, "patient_key")
  expect_equal(staged$patient_key, c("patient-a", "patient-a", "patient-b"))
  expect_equal(staged$row_marker, 1:3)
  expect_equal(
    staged$relative_path,
    c("valid.png", rep("__dsflower_invalid_image__", 2L))
  )
  expect_equal(staged$target, c(1L, 0L, 0L))
})

test_that("image descriptor asset names cannot traverse staging", {
  metadata <- .test_imaging_metadata("patient_id")
  desc <- list(
    metadata = metadata,
    assets = stats::setNames(
      list(list(type = "image_root", root = tempdir())),
      "../escape"),
    manifest = list(metadata = metadata)
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

test_that(".cleanupStaging refuses to ACK a surviving directory", {
  token <- .test_run_token(111)
  staging_dir <- file.path(tempdir(), "dsflower", token)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)
  writeLines("private", file.path(staging_dir, "private.txt"))
  withr::defer(unlink(staging_dir, recursive = TRUE))

  # Model unlink() reporting success without establishing the required
  # postcondition. The retry target must remain visible to its caller.
  local_mocked_bindings(
    unlink = function(...) 0L,
    .package = "base"
  )
  expect_error(
    dsFlower:::.cleanupStaging(token),
    "Staging cleanup did not complete",
    fixed = TRUE
  )
  expect_true(dir.exists(staging_dir))
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
