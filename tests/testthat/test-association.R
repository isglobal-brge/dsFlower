.association_test_token <- function(index) {
  paste0("run_", sprintf("%032x", as.integer(index)))
}

.association_test_config <- function(n_nodes = 2L) {
  contract_sha <- dsFlower:::.association_contract_sha256(
    "outcome", "exposure", c("no", "yes"), c(0, 1), "row")
  runner_sha <- dsFlower:::.compute_harness_hash()
  list(
    "dp-track" = "association",
    "num-server-rounds" = 1L,
    "association-outcome-levels" = c("no", "yes"),
    "association-exposure-levels" = c(0, 1),
    "association-contract-sha256" = contract_sha,
    "association-n-nodes" = n_nodes,
    "association-job-sha256" = dsFlower:::.association_job_sha256(
      contract_sha, 3L, runner_sha, n_nodes))
}

.association_test_project_parquet <- function(
    source, destination, columns, ...) {
  all_of <- utils::getFromNamespace("all_of", "tidyselect")
  selected <- as.data.frame(arrow::read_parquet(
    source, col_select = all_of(columns)))
  arrow::write_parquet(selected, destination)
  Sys.chmod(destination, "0600")
  list(
    file_bytes = as.numeric(file.info(destination)$size),
    materialized_bytes = as.numeric(object.size(selected)),
    rows = as.numeric(nrow(selected)),
    sha256 = digest::digest(
      file = destination, algo = "sha256", serialize = FALSE))
}

test_that("association request and job provenance have fixed canonical hashes", {
  expect_identical(
    dsFlower:::.association_contract_sha256(
      "outcome", "exposure", c("no", "yes"), c(0, 1), "row"),
    "d95556c2379b4ab24e65772f883e3d4da9f9b06a3d8769dd2ccaead0e36ae858")
  expect_identical(
    dsFlower:::.association_job_sha256(
      paste(rep("a", 64L), collapse = ""), 3L,
      paste(rep("b", 64L), collapse = ""), 2L),
    "ff0dcf02fc1b52f546733a6c2272834bebc7cab04d86e3436fc235c06bcc8268")

  expect_identical(
    dsFlower:::.association_level_spec(c(0, 1), "levels")$type,
    "number")
  expect_identical(
    dsFlower:::.association_level_spec(c(FALSE, TRUE), "levels")$type,
    "boolean")
  expect_error(
    dsFlower:::.association_level_spec(c("case", "case"), "levels"),
    "must be distinct")
  expect_error(
    dsFlower:::.association_level_spec(c(0, Inf), "levels"), "finite")
  expect_error(
    dsFlower:::.association_level_spec(c("", "case"), "levels"),
    "non-empty valid UTF-8")
})

test_that("association config is isolated and binds columns, runner, and roster", {
  withr::local_options(list(dsflower.dp_unit = "row"))
  normalized <- dsFlower:::.addDpConfigToRunConfig(
    .association_test_config())
  expect_identical(normalized[["task-type"]], "classification")
  expect_identical(normalized[["association-contract"]],
                   "dsflower-binary-association-3x3/v1")
  expect_identical(normalized[["association-privacy-unit"]], "row")
  expect_identical(normalized[["association-unit-semantics"]],
                   "row-one-hot/v1")
  expect_no_error(dsFlower:::.verifyAssociationContract(
    normalized, "exposure", "outcome"))

  swapped <- normalized
  expect_error(
    dsFlower:::.verifyAssociationContract(swapped, "outcome", "exposure"),
    "contract SHA-256")
  wrong_job <- normalized
  wrong_job[["association-job-sha256"]] <- paste(rep("0", 64L), collapse = "")
  expect_error(
    dsFlower:::.verifyAssociationContract(
      wrong_job, "exposure", "outcome"),
    "job SHA-256")
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(c(
      .association_test_config(), list("task-type" = "classification"))),
    "does not accept field.*task-type")
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(
      "dp-track" = "neural", "association-n-nodes" = 2L)),
    "association-\\* fields")
})

test_that("association in-memory staging preserves every row and unknown cell", {
  withr::local_options(list(dsflower.dp_unit = "row"))
  token <- .association_test_token(801)
  withr::defer(dsFlower:::.cleanupStaging(token))
  config <- .association_test_config()
  config[["association-outcome-levels"]] <- list(
    type = "string", values = c("no", "yes"))
  config[["association-exposure-levels"]] <- list(
    type = "number", values = c(0, 1))
  data <- data.frame(
    outcome = factor(c("no", "yes", NA, "other")),
    exposure = c(0, 1, NA, 2))
  staging <- dsFlower:::.stageAssociationData(
    data, token, "outcome", "exposure", config)
  manifest <- jsonlite::fromJSON(file.path(staging, "manifest.json"))
  staged <- if (identical(manifest$data_format, "parquet")) {
    as.data.frame(arrow::read_parquet(
      file.path(staging, manifest$data_file)))
  } else {
    utils::read.csv(file.path(staging, manifest$data_file))
  }
  expect_identical(as.integer(staged$outcome), c(0L, 1L, 2L, 2L))
  expect_identical(as.integer(staged$exposure), c(0L, 1L, 2L, 2L))
  expect_identical(nrow(staged), 4L)
  expect_identical(manifest$n_samples, 4L)
  expect_identical(manifest$dropped_missing, 0L)
  expect_true(manifest[["association-preencoded"]])
  expect_identical(manifest$feature_columns, "exposure")
})

test_that("association patient staging keeps the protected identifier only for grouping", {
  withr::local_options(list(
    dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id"))
  token <- .association_test_token(802)
  withr::defer(dsFlower:::.cleanupStaging(token))
  data <- data.frame(
    outcome = c("no", "yes", "other", NA),
    exposure = c(0, 1, 1, NA),
    patient_id = c(" p1 ", "p1", NA, " "))
  config <- .association_test_config()
  config[["association-outcome-levels"]] <- list(
    type = "string", values = c("no", "yes"))
  config[["association-exposure-levels"]] <- list(
    type = "number", values = c(0, 1))
  staging <- dsFlower:::.stageAssociationData(
    data, token, "outcome", "exposure", config)
  manifest <- jsonlite::fromJSON(file.path(staging, "manifest.json"))
  staged <- as.data.frame(arrow::read_parquet(
    file.path(staging, manifest$data_file)))
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$n_units, 2L)
  expect_identical(names(staged), c("outcome", "exposure", "patient_id"))
  expect_identical(staged$patient_id[[1L]], "p1")
  expect_identical(staged$patient_id[[2L]], "p1")
  expect_identical(
    staged$patient_id[[3L]], "__dsflower_missing_patient_unit__")
  expect_identical(
    staged$patient_id[[4L]], "__dsflower_missing_patient_unit__")
})

test_that("association staged_parquet descriptor selects and totalizes only required columns", {
  skip_if_not_installed("arrow")
  withr::local_options(list(dsflower.dp_unit = "row"))
  local_mocked_bindings(
    .materializeAssociationParquet = .association_test_project_parquet,
    .package = "dsFlower")
  source <- tempfile(fileext = ".parquet")
  withr::defer(unlink(source))
  arrow::write_parquet(data.frame(
    outcome = c("no", "yes", NA), exposure = c(0, 3, 1),
    unused = c("private-a", "private-b", "private-c")), source)
  descriptor <- list(
    source_kind = "staged_parquet", dataset_id = "public-dataset",
    metadata = list(file = source))
  token <- .association_test_token(803)
  withr::defer(dsFlower:::.cleanupStaging(token))
  config <- .association_test_config()
  config[["association-outcome-levels"]] <- list(
    type = "string", values = c("no", "yes"))
  config[["association-exposure-levels"]] <- list(
    type = "number", values = c(0, 1))
  staging <- dsFlower:::.stageFromDescriptor(
    descriptor, token, "outcome", "exposure", config)
  manifest <- jsonlite::fromJSON(file.path(staging, "manifest.json"))
  staged <- as.data.frame(arrow::read_parquet(
    file.path(staging, manifest$data_file)))
  expect_identical(names(staged), c("outcome", "exposure"))
  expect_identical(as.integer(staged$outcome), c(0L, 1L, 2L))
  expect_identical(as.integer(staged$exposure), c(0L, 2L, 1L))
  expect_identical(manifest$source_kind, "staged_parquet")
  expect_identical(manifest$n_samples, 3L)

  expect_error(
    dsFlower:::.stageFromDescriptor(
      list(source_kind = "image_bundle"),
      .association_test_token(804), "outcome", "exposure", config),
    "tabular descriptors only")
})

test_that("association Parquet projection wrapper is isolated and bounded", {
  root <- tempfile("association_projection_")
  runner <- file.path(root, "runner", "dsflower_runner")
  runtime <- file.path(root, "runtime")
  dir.create(runner, recursive = TRUE)
  dir.create(file.path(runtime, if (.Platform$OS.type == "windows") {
    "Scripts"
  } else {
    "bin"
  }), recursive = TRUE)
  writeLines("# pinned", file.path(runner, "association_parquet.py"))
  python <- dsFlower:::.native_tree_runtime_executable(runtime, "python")
  writeLines("", python)
  withr::defer(unlink(root, recursive = TRUE, force = TRUE))
  source <- normalizePath(file.path(root, "source.parquet"), mustWork = FALSE)
  destination <- normalizePath(
    file.path(root, "projection.parquet"), mustWork = FALSE)
  writeLines("source", source)
  seen <- NULL
  fake_run <- function(command, args, env, error_on_status, timeout) {
    seen <<- list(command = command, args = args, env = env,
                  error_on_status = error_on_status, timeout = timeout)
    list(status = 0L, stdout = paste0(
      '{"contract":"dsflower-association-parquet-projection/v1",',
      '"file_bytes":7,"materialized_bytes":11,"rows":3,',
      '"sha256":"', paste(rep("a", 64L), collapse = ''), '"}'))
  }
  value <- dsFlower:::.materializeAssociationParquet(
    source, destination, c("outcome", "exposure"),
    runner_dir = runner, runtime_root = runtime,
    run_projection = fake_run)
  expect_identical(value$rows, 3)
  expect_identical(seen$command, python)
  expect_identical(seen$args[1:2], c("-I", "-c"))
  expect_match(seen$args[[3L]], "materialize_bounded_projection", fixed = TRUE)
  expect_identical(seen$error_on_status, FALSE)
  expect_identical(seen$timeout, 300)
  expect_identical(unname(seen$env[["PYTHONNOUSERSITE"]]), "1")
  expect_identical(unname(seen$env[["VIRTUAL_ENV"]]), runtime)
})

test_that("association capability probe is targeted and explicit", {
  calls <- 0L
  local_mocked_bindings(
    .association_runtime_probe = function(...) {
      calls <<- calls + 1L
      TRUE
    },
    .package = "dsFlower")
  none <- flowerGetCapabilitiesDS("none", "none")
  expect_false(none$association$probed)
  expect_null(none$association$available)
  expect_identical(calls, 0L)

  runtime <- flowerGetCapabilitiesDS("none", "runtime")
  expect_true(runtime$association$probed)
  expect_true(runtime$association$available)
  expect_identical(runtime$association$execution_profile,
                   "dsflower-binary-association-execution/v1")
  expect_identical(runtime$association$privacy_units, c("row", "patient"))
  expect_identical(calls, 1L)
  expect_error(flowerGetCapabilitiesDS("none", "all"),
               "association_probe must be exactly")
})
