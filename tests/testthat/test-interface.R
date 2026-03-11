# Tests for R/interface.R — DataSHIELD Methods

test_that("flowerPingDS returns correct structure", {
  result <- flowerPingDS()
  expect_type(result, "list")
  expect_equal(result$status, "ok")
  expect_equal(result$package, "dsFlower")
  expect_true(nchar(result$version) > 0)
  expect_true(nchar(result$timestamp) > 0)
})

test_that(".getHandle errors for missing symbol", {
  expect_error(
    dsFlower:::.getHandle("nonexistent_symbol"),
    "No Flower handle"
  )
})

test_that("Handle CRUD operations work", {
  env <- dsFlower:::.dsflower_env

  # Create and set
  handle <- mock_handle(data_path = "/tmp/test.csv")
  dsFlower:::.setHandle("test_crud", handle)

  # Get
  retrieved <- dsFlower:::.getHandle("test_crud")
  expect_equal(retrieved$data_path, "/tmp/test.csv")

  # Remove
  dsFlower:::.removeHandle("test_crud")
  expect_error(dsFlower:::.getHandle("test_crud"), "No Flower handle")
})

test_that(".ds_arg handles JSON strings", {
  result <- dsFlower:::.ds_arg('{"key": "value"}')
  expect_type(result, "list")
  expect_equal(result$key, "value")
})

test_that(".ds_arg handles B64 encoded strings", {
  json <- '{"num_rounds": 10}'
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  encoded <- paste0("B64:", b64)

  result <- dsFlower:::.ds_arg(encoded)
  expect_equal(result$num_rounds, 10)
})

test_that(".ds_arg passes through non-JSON values", {
  expect_equal(dsFlower:::.ds_arg(42), 42)
  expect_equal(dsFlower:::.ds_arg("simple_string"), "simple_string")
  expect_equal(dsFlower:::.ds_arg(TRUE), TRUE)
})

test_that(".dsf_option follows option chain", {
  # Default value

  expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "fallback")

  # Option set
  withr::with_options(list(dsflower.test_opt = "direct"), {
    expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "direct")
  })

  # Default prefix
  withr::with_options(list(default.dsflower.test_opt = "default_prefix"), {
    expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "default_prefix")
  })
})

test_that("flowerPrepareRunDS stages data correctly", {
  csv_path <- create_test_csv(n = 50)
  on.exit(unlink(csv_path))

  # Create a handle manually
  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_prepare", handle)
  on.exit(dsFlower:::.removeHandle("test_prepare"), add = TRUE)

  # Prepare the run
  result <- flowerPrepareRunDS("test_prepare", "target", c("f1", "f2", "f3"))

  expect_true(result$prepared)
  expect_equal(result$target_column, "target")
  expect_equal(result$feature_columns, c("f1", "f2", "f3"))
  expect_true(!is.null(result$run_token))
  expect_true(!is.null(result$staging_dir))
  expect_true(dir.exists(result$staging_dir))

  # Verify manifest exists
  manifest_path <- file.path(result$staging_dir, "manifest.json")
  expect_true(file.exists(manifest_path))

  manifest <- jsonlite::fromJSON(manifest_path)
  expect_equal(manifest$n_samples, 50)
  expect_equal(manifest$target_column, "target")

  # Clean up staging
  dsFlower:::.cleanupStaging(result$run_token)
})

test_that("flowerPrepareRunDS blocks on insufficient samples", {
  # Create tiny dataset
  tiny_dir <- tempdir()
  tiny_path <- file.path(tiny_dir, "tiny_test.csv")
  utils::write.csv(data.frame(f1 = 1:2, target = 0:1), tiny_path, row.names = FALSE)
  on.exit(unlink(tiny_path))

  handle <- mock_handle(data_path = tiny_path, data_format = "csv")
  dsFlower:::.setHandle("test_tiny", handle)
  on.exit(dsFlower:::.removeHandle("test_tiny"), add = TRUE)

  expect_error(
    flowerPrepareRunDS("test_tiny", "target"),
    "Disclosive"
  )
})

test_that("flowerCleanupRunDS resets handle state", {
  csv_path <- create_test_csv(n = 10)
  on.exit(unlink(csv_path))

  handle <- mock_handle(
    data_path = csv_path,
    run_token = "run_cleanup_test",
    staging_dir = file.path(tempdir(), "dsflower", "run_cleanup_test"),
    target_column = "target",
    feature_columns = c("f1"),
    prepared = TRUE
  )
  dsFlower:::.setHandle("test_cleanup", handle)
  on.exit(dsFlower:::.removeHandle("test_cleanup"), add = TRUE)

  result <- flowerCleanupRunDS("test_cleanup")
  expect_false(result$prepared)
  expect_false(result$node_ensured)
  expect_null(result$run_token)
  expect_null(result$staging_dir)
  expect_null(result$target_column)
})

test_that("flowerGetCapabilitiesDS returns expected structure", {
  caps <- flowerGetCapabilitiesDS()
  expect_type(caps, "list")
  expect_true("dsflower_version" %in% names(caps))
  expect_true("python_version" %in% names(caps))
  expect_true("flower_version" %in% names(caps))
  expect_true("templates" %in% names(caps))
  expect_true("max_rounds" %in% names(caps))
  expect_true("min_samples" %in% names(caps))
})

test_that("flowerGetCapabilitiesDS includes is_docker and hostname", {
  caps <- flowerGetCapabilitiesDS()
  expect_true("is_docker" %in% names(caps))
  expect_true("hostname" %in% names(caps))
  expect_type(caps$is_docker, "logical")
  expect_type(caps$hostname, "character")
  expect_true(nchar(caps$hostname) > 0)
})

test_that("flowerStatusDS returns status info", {
  handle <- mock_handle()
  dsFlower:::.setHandle("test_status", handle)
  on.exit(dsFlower:::.removeHandle("test_status"))

  status <- flowerStatusDS("test_status")
  expect_type(status, "list")
  expect_false(status$prepared)
  expect_false(status$node_ensured)
  expect_false(status$supernode_running)
})

test_that("flowerDestroyDS removes handle", {
  handle <- mock_handle()
  dsFlower:::.setHandle("test_destroy", handle)

  flowerDestroyDS("test_destroy")
  expect_error(dsFlower:::.getHandle("test_destroy"), "No Flower handle")
})

test_that(".parseFlowerMetrics handles missing log", {
  result <- dsFlower:::.parseFlowerMetrics(NULL)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)

  result2 <- dsFlower:::.parseFlowerMetrics("/nonexistent/log.txt")
  expect_s3_class(result2, "data.frame")
  expect_equal(nrow(result2), 0)
})

test_that(".parseFlowerMetrics extracts metrics from log", {
  log_path <- tempfile(fileext = ".log")
  writeLines(c(
    "[Round 1] loss = 0.693",
    "[Round 1] accuracy = 0.55",
    "[Round 2] loss = 0.500",
    "[Round 2] accuracy = 0.70",
    "Some other log line"
  ), log_path)
  on.exit(unlink(log_path))

  metrics <- dsFlower:::.parseFlowerMetrics(log_path)
  expect_s3_class(metrics, "data.frame")
  expect_true(nrow(metrics) >= 4)
  expect_true("loss" %in% metrics$metric)
  expect_true("accuracy" %in% metrics$metric)
})
