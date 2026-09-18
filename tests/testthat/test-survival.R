.survival_public_fixture <- function(distribution = "weibull") {
  list(schema_version = 1L, time_unit = "days", time_origin = "baseline",
       t_min = 1, horizon = 10, time_scale = 1,
       distribution = distribution, dispersion = 1)
}

.survival_wire_fixture <- function(value = .survival_public_fixture()) {
  list("dp-track" = "neural", "task-type" = "survival",
       "loss-name" = paste0("aft_", value$distribution, "_nll"),
       "num-server-rounds" = 1L, "num-features" = 1L,
       "survival-config-b64" = gsub("[\r\n]", "", jsonlite::base64_enc(
         charToRaw(as.character(jsonlite::toJSON(value, auto_unbox = TRUE,
                                                digits = I(17)))))))
}

.survival_patient_options <- function(.local_envir = parent.frame()) {
  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "id"),
                       .local_envir = .local_envir)
}

.hazard_wire_fixture <- function(edges = c(0, 2, 5, 10)) {
  value <- list(schema_version = 1L, time_unit = "days", time_origin = "baseline",
                t_min = 1, horizon = 10, edges = edges)
  wire <- .survival_wire_fixture(value)
  wire[["loss-name"]] <- "discrete_hazard_nll"
  wire
}

test_that("survival public pins are strict and custodian patient units mandatory", {
  withr::local_options(list(dsflower.dp_unit = "row"))
  expect_error(dsFlower:::.addDpConfigToRunConfig(.survival_wire_fixture()),
               "custodian-configured patient")
  uppercase <- .survival_wire_fixture()
  uppercase[["task-type"]] <- "SURVIVAL"
  uppercase[["loss-name"]] <- "AFT_WEIBULL_NLL"
  uppercase[["survival-config-b64"]] <- NULL
  expect_error(dsFlower:::.addDpConfigToRunConfig(uppercase),
               "custodian-configured patient")
  .survival_patient_options()
  for (distribution in c("weibull", "lognormal")) {
    config <- dsFlower:::.addDpConfigToRunConfig(
      .survival_wire_fixture(.survival_public_fixture(distribution)))
    expect_identical(config[["task-type"]], "survival")
    expect_identical(config[["survival-config"]]$distribution, distribution)
    expect_false(config$allow_exact_num_examples)
    expect_false(config$allow_per_node_metrics)
    expect_true(config$dp_enabled)
  }
  for (field in c("survival_file", "survival_schema", "survival_shape",
                  "survival_feature_columns", "survival_target_columns",
                  "survival-config", "patient_column", "n_units")) {
    wire <- .survival_wire_fixture()
    wire[[field]] <- "forged"
    expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "server-owned")
  }
  mutations <- list(
    list(dispersion = 3), list(distribution = "cox"), list(t_min = 0),
    list(horizon = Inf), list(time_scale = TRUE), list(time_unit = "years"),
    list(time_origin = "delayed_entry"), list(schema_version = 2),
    list(t_min = 11), list(time_scale = "1"), list(extra = 1))
  for (mutation in mutations) {
    value <- utils::modifyList(.survival_public_fixture(), mutation)
    wire <- .survival_wire_fixture(value)
    wire[["loss-name"]] <- "aft_weibull_nll"
    expect_error(dsFlower:::.addDpConfigToRunConfig(wire))
  }
  wire <- .survival_wire_fixture()
  wire[["survival-config-b64"]] <- paste0(wire[["survival-config-b64"]], "\n")
  expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "canonical")
  wire <- .survival_wire_fixture()
  wire[["loss-name"]] <- "aft_lognormal_nll"
  expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "distribution")
})

test_that("survival excludes private validation, holdout, CV, Cox and mixed roles", {
  .survival_patient_options()
  for (field in c("resampling-version", "cv-version", "validation-task")) {
    wire <- .survival_wire_fixture()
    wire[[field]] <- 1L
    expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "unsupported")
  }
  for (track in c("validation", "egress", "native_tree")) {
    wire <- .survival_wire_fixture()
    wire[["dp-track"]] <- track
    expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "trusted neural")
  }
  wire <- .survival_wire_fixture()
  wire[["loss-name"]] <- "cox"
  expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "trusted neural")
  wire <- .survival_wire_fixture()
  wire[["target-levels"]] <- c(0, 1)
  expect_error(dsFlower:::.addDpConfigToRunConfig(wire), "time/event")
  config <- dsFlower:::.addDpConfigToRunConfig(.survival_wire_fixture())
  expect_no_error(dsFlower:::.validateSurvivalColumns(config, c("time", "event"), "x"))
  for (features in list(NULL, "id", "time", "__survival_time", c("x", "z"))) {
    expect_error(dsFlower:::.validateSurvivalColumns(config, c("time", "event"),
                                                    features), "Survival requires")
  }
  expect_error(dsFlower:::.normalizePublicColumnSelection("time", "x", config),
               "two ordered")
})

test_that("survival public failures occur before private data access", {
  .survival_patient_options()
  name <- "test_survival_preflight"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    id = "public-fixture", x = 1, time = 1, event = 1)))
  withr::defer(dsFlower:::.removeHandle(name))
  reached <- FALSE
  local_mocked_bindings(.stageData = function(...) {
    reached <<- TRUE
    stop("private staging reached")
  }, .package = "dsFlower")
  wire <- .survival_wire_fixture()
  wire[["loss-name"]] <- "cox"
  targets <- c("time", "event")
  expect_error(flowerPrepareRunDS(name, targets, "x", wire),
               "trusted neural")
  wire <- .survival_wire_fixture()
  expect_error(flowerPrepareRunDS(name, targets, "id", wire), "roles must be distinct")
  expect_false(reached)
})

test_that("AFT staging preserves rows and units while totalizing invalid subjects", {
  .survival_patient_options()
  config <- dsFlower:::.addDpConfigToRunConfig(.survival_wire_fixture())
  config[["feature-bounds"]] <- list(lower = -2, upper = 4)
  data <- data.frame(
    id = c("a", "b", "c", "dup", "dup", "", NA, "bad", "tiny", "nf"),
    x = c(1, NA, 3, 4, 5, 6, 7, 8, 9, Inf),
    time = c(2, 10, 11, 3, 3, 1, 1, NA, 0.1, 0),
    event = c(1, 1, 1, 0, 0, 1, 1, 1, 0, 7))
  token <- dsFlower:::.generate_run_token()
  withr::defer(dsFlower:::.cleanupStaging(token))
  path <- dsFlower:::.stageData(data, token, c("time", "event"), "x", config)
  manifest <- jsonlite::read_json(file.path(path, "manifest.json"), simplifyVector = TRUE)
  manifest_arrays <- jsonlite::read_json(file.path(path, "manifest.json"))
  expect_type(manifest_arrays$feature_columns, "list")
  expect_type(manifest_arrays$survival_feature_columns, "list")
  expect_type(manifest_arrays[["feature-bounds"]]$lower, "list")
  expect_type(manifest_arrays[["feature-bounds"]]$upper, "list")
  result <- utils::read.csv(file.path(path, manifest$survival_file), check.names = FALSE)
  expect_equal(manifest$n_samples, nrow(data))
  expect_equal(manifest$n_input_samples, nrow(data))
  expect_equal(manifest$dropped_missing, 0)
  expect_equal(manifest$n_units, 8)
  expect_equal(manifest$survival_shape, c(8, 1, 3))
  expect_identical(result$id, c("a", "b", "c", "dup", "__dsflower_missing_patient_unit__",
                               "bad", "tiny", "nf"))
  expect_equal(result$`__survival_valid`, c(1, 1, 1, 0, 0, 0, 0, 0))
  expect_equal(result$`__survival_time`, c(2, 10, 10, 1, 1, 1, 1, 1))
  expect_equal(result$`__survival_event`, c(1, 1, 0, 0, 0, 0, 0, 0))
  expect_equal(result$x, c(1, 1, 3, 0, 0, 0, 0, 0))
  expect_false(manifest$allow_exact_num_examples)
  expect_false(manifest$allow_per_node_metrics)
})

test_that("survival data-frame and parquet descriptors preserve the same subject contract", {
  skip_if_not_installed("arrow")
  .survival_patient_options()
  config <- dsFlower:::.addDpConfigToRunConfig(.survival_wire_fixture())
  data <- data.frame(id = c("a", "b", "b"), x = 1:3,
                     time = c(1, 2, 2), event = c(1, 0, 0))
  parquet <- withr::local_tempfile(fileext = ".parquet")
  arrow::write_parquet(data, parquet)
  descriptors <- list(list(source_kind = "in_memory_df", table_data = data),
    list(source_kind = "staged_parquet", metadata = list(file = parquet)),
    list(source_kind = "asset_ref", asset_info = list(storage_backend = "local",
                                                     uri = parquet)))
  results <- lapply(descriptors, function(desc) {
    token <- dsFlower:::.generate_run_token()
    on.exit(dsFlower:::.cleanupStaging(token))
    path <- dsFlower:::.stageFromDescriptor(desc, token, c("time", "event"), "x", config)
    manifest <- jsonlite::read_json(file.path(path, "manifest.json"), simplifyVector = TRUE)
    expect_equal(manifest$n_samples, 3)
    expect_equal(manifest$n_units, 2)
    utils::read.csv(file.path(path, manifest$survival_file), check.names = FALSE)
  })
  expect_identical(results[[1]], results[[2]])
  expect_identical(results[[1]], results[[3]])
})

test_that("hazard pins require a strict fixed public grid with K at most 64", {
  .survival_patient_options()
  config <- dsFlower:::.addDpConfigToRunConfig(.hazard_wire_fixture())
  expect_identical(config[["task-type"]], "survival")
  expect_equal(config[["survival-config"]]$edges, c(0, 2, 5, 10))
  for (edges in list(c(1, 10), c(0, 5, 5, 10), c(0, 10, 5), c(0, 5, 9),
                     c(0, NA, 10), seq(0, 10, length.out = 66), list(0, TRUE, 10))) {
    expect_error(dsFlower:::.addDpConfigToRunConfig(.hazard_wire_fixture(edges)),
                 "Hazard edges")
  }
  expect_no_error(dsFlower:::.addDpConfigToRunConfig(
    .hazard_wire_fixture(seq(0, 10, length.out = 65))))
})

test_that("hazard staging applies interval-end event and censor conventions", {
  .survival_patient_options()
  config <- dsFlower:::.addDpConfigToRunConfig(.hazard_wire_fixture())
  data <- data.frame(id = letters[1:10], x = seq_len(10),
                     time = c(1, 2, 3, 5, 10, 11, 1, 2, 3, 5),
                     event = c(1, 1, 1, 1, 1, 1, 0, 0, 0, 0))
  token <- dsFlower:::.generate_run_token()
  withr::defer(dsFlower:::.cleanupStaging(token))
  path <- dsFlower:::.stageData(data, token, c("time", "event"), "x", config)
  manifest <- jsonlite::read_json(file.path(path, "manifest.json"), simplifyVector = TRUE)
  result <- utils::read.csv(file.path(path, manifest$survival_file), check.names = FALSE)
  expected_d <- rbind(c(1, 0, 0), c(1, 0, 0), c(0, 1, 0), c(0, 1, 0),
                      c(0, 0, 1), c(0, 0, 0), c(0, 0, 0), c(0, 0, 0),
                      c(0, 0, 0), c(0, 0, 0))
  expected_m <- rbind(c(1, 0, 0), c(1, 0, 0), c(1, 1, 0), c(1, 1, 0),
                      c(1, 1, 1), c(1, 1, 1), c(0, 0, 0), c(1, 0, 0),
                      c(1, 0, 0), c(1, 1, 0))
  expect_equal(unname(as.matrix(result[paste0("__survival_d_", 1:3)])), expected_d)
  expect_equal(unname(as.matrix(result[paste0("__survival_m_", 1:3)])), expected_m)
  expect_equal(result$`__survival_valid`, rep(1, 10))
  expect_equal(result$`__survival_event`, c(rep(1, 5), rep(0, 5)))
  expect_equal(manifest$survival_shape, c(nrow(data), 1, 9))
  expect_equal(manifest$n_units, nrow(data))
  expect_equal(manifest$n_samples, nrow(data))
})

test_that("K and invalid records never shrink or multiply the subject census", {
  .survival_patient_options()
  data <- data.frame(id = c("a", "b", "b", "", "bad", "subresolution"),
                     x = 1:6, time = c(2, 3, 3, 4, Inf, .5), event = 1)
  for (k in c(1L, 3L, 64L)) {
    wire <- .hazard_wire_fixture(seq(0, 10, length.out = k + 1L))
    config <- dsFlower:::.addDpConfigToRunConfig(wire)
    token <- dsFlower:::.generate_run_token()
    path <- dsFlower:::.stageData(data, token, c("time", "event"), "x", config)
    manifest <- jsonlite::read_json(file.path(path, "manifest.json"), simplifyVector = TRUE)
    result <- utils::read.csv(file.path(path, manifest$survival_file), check.names = FALSE)
    expect_equal(manifest$n_samples, nrow(data))
    expect_equal(manifest$n_units, 5)
    expect_equal(manifest$survival_shape, c(5, 1, 3 + 2 * k))
    expect_equal(result$`__survival_valid`, c(1, 0, 0, 0, 0))
    expect_true(all(as.matrix(result[-1, paste0("__survival_d_", seq_len(k)),
                                     drop = FALSE]) == 0))
    expect_true(all(as.matrix(result[-1, paste0("__survival_m_", seq_len(k)),
                                     drop = FALSE]) == 0))
    dsFlower:::.cleanupStaging(token)
  }
})

test_that("changing one subject cannot change other derived subject contributions", {
  .survival_patient_options()
  data <- data.frame(id = c("a", "b", "b", "c"), x = c(1, 2, 3, 4),
                     time = c(2, 4, 4, 6), event = c(1, 0, 0, 1))
  for (wire in list(.survival_wire_fixture(), .hazard_wire_fixture())) {
    config <- dsFlower:::.addDpConfigToRunConfig(wire)
    derived <- lapply(c(FALSE, TRUE), function(change) {
      frame <- data
      if (change) frame[frame$id == "b", c("x", "time", "event")] <- NA
      token <- dsFlower:::.generate_run_token()
      on.exit(dsFlower:::.cleanupStaging(token))
      path <- dsFlower:::.stageData(frame, token, c("time", "event"), "x", config)
      utils::read.csv(file.path(path, "survival_subjects.csv"), check.names = FALSE)
    })
    expect_identical(derived[[1]], derived[[2]])
  }
})

test_that("survival CSV round-trips precise interval boundaries and numeric outcomes", {
  .survival_patient_options()
  config <- dsFlower:::.addDpConfigToRunConfig(.hazard_wire_fixture())
  times <- c(2 - .Machine$double.eps, 2, 2 + 2 * .Machine$double.eps)
  data <- data.frame(id = c("a", "b", "c"), x = c(1, 2, 3),
                     time = times, event = c(TRUE, TRUE, TRUE))
  transformed <- dsFlower:::.transformPublicTarget(data, c("time", "event"), config)
  expect_identical(transformed$event, c(1, 1, 1))
  path <- withr::local_tempfile(fileext = ".csv")
  dsFlower:::.writeSurvivalCsv(transformed, path)
  restored <- utils::read.csv(path, check.names = FALSE)
  text <- utils::read.csv(path, colClasses = "character", check.names = FALSE)
  expect_identical(text$time, format(times, digits = 17L, scientific = TRUE, trim = TRUE))
  expected <- dsFlower:::.stageHazardTargets(times, rep(1, 3), rep(TRUE, 3),
                                             config[["survival-config"]]$edges)
  actual <- dsFlower:::.stageHazardTargets(restored$time, restored$event,
                                           rep(TRUE, 3),
                                           config[["survival-config"]]$edges)
  expect_identical(actual, expected)
  expect_equal(actual$`__survival_d_1`, c(1, 1, 0))
  expect_equal(actual$`__survival_d_2`, c(0, 0, 1))
})
