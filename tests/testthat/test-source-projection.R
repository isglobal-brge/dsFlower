test_that("private source sidecar retains raw invalid values behind equal effective rows", {
  directory <- withr::local_tempdir()
  source <- data.frame(x = c(NA_real_, NaN, Inf, -Inf), y = c(0, 0, 0, 0),
                       unrelated = letters[1:4])
  effective <- data.frame(x = rep(0, 4), y = rep(0, 4))
  utils::write.csv(effective, file.path(directory, "train.csv"), row.names = FALSE)
  manifest <- list(data_type = "tabular", data_file = "train.csv",
                   feature_columns = "x", target_column = "y", patient_column = NULL)
  manifest <- dsFlower:::.stageSourceProjection(source, effective, manifest, directory)
  lines <- readLines(file.path(directory, manifest$source_projection_file))
  header <- jsonlite::fromJSON(lines[[1]], simplifyVector = FALSE)
  expect_identical(unlist(header$columns), c("x", "y"))
  types <- vapply(lines[-1], function(line) {
    jsonlite::fromJSON(line, simplifyVector = FALSE)$values[[1]]$type
  }, character(1), USE.NAMES = FALSE)
  expect_identical(types, c("missing", "nan", "posinf", "neginf"))
  expect_identical(manifest$source_effective_sha256,
    digest::digest(file = file.path(directory, "train.csv"), algo = "sha256"))
  expect_identical(manifest$source_projection_sha256,
    digest::digest(file = file.path(directory, manifest$source_projection_file), algo = "sha256"))
  if (.Platform$OS.type == "unix") {
    expect_identical(as.integer(file.info(file.path(directory, manifest$source_projection_file))$mode),
                     strtoi("600", base = 8))
  }
})

test_that("source projection preserves canonical patient roster without locator fields", {
  directory <- withr::local_tempdir()
  source <- data.frame(id = c(" 001 ", NA), x = c("invalid-a", "invalid-b"), y = c(0, 0))
  effective <- source
  effective$id <- c("001", "__dsflower_missing_patient_unit__")
  effective$x <- 0
  utils::write.csv(effective, file.path(directory, "train.csv"), row.names = FALSE)
  manifest <- dsFlower:::.stageSourceProjection(source, effective,
    list(data_type = "tabular", data_file = "train.csv", feature_columns = "x",
         target_column = "y", patient_column = "id"), directory)
  records <- lapply(readLines(file.path(directory, manifest$source_projection_file))[-1],
                    jsonlite::fromJSON, simplifyVector = FALSE)
  expect_identical(vapply(records, `[[`, character(1), "patient_id"), effective$id)
  expect_identical(vapply(records, function(row) row$values[[1]]$value, character(1)), source$x)
  for (field in c("source_projection_file", "source_projection_schema",
                  "source_projection_sha256", "source_effective_sha256")) {
    expect_error(dsFlower:::.validate_client_run_config(stats::setNames(list("forged"), field)),
                 "server|reserved|structural")
  }
})

test_that("ordinary staging pins pre-totalization source alongside equal safe tensors", {
  root <- withr::local_tempdir()
  withr::local_options(list(dsflower.staging_root = root, dsflower.dp_unit = "row"))
  staged <- lapply(seq_len(2), function(index) {
    data <- data.frame(x = if (index == 1) Inf else NaN, y = 0)
    token <- paste0("run_", sprintf("%032x", as.integer(98700 + index)))
    directory <- dsFlower:::.stageData(data, token, "y", "x")
    manifest <- jsonlite::fromJSON(file.path(directory, "manifest.json"))
    list(directory = directory, manifest = manifest,
         data = dsFlower:::.loadTrainingData(
           file.path(directory, manifest$data_file), manifest$data_format))
  })
  expect_identical(staged[[1]]$data, staged[[2]]$data)
  expect_false(identical(staged[[1]]$manifest$source_projection_sha256,
                         staged[[2]]$manifest$source_projection_sha256))
  expect_identical(staged[[1]]$manifest$source_projection_schema,
                   "dsflower-source-projection-v1")
})
