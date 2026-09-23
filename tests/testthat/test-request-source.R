test_that("request source pins preserve manifest arrays and ignore run identities", {
  staged <- withr::local_tempdir()
  path <- file.path(staged, "manifest.json")
  initial <- list(run_token = "first", feature_columns = list("x"),
                  target_column = list("time", "event"),
                  "target-levels" = list(type = "numeric", values = list(0L, 1L)))
  dsFlower:::.write_manifest_atomic(initial, path)
  data <- data.frame(x = 1:3, target = c(0, 1, 0))
  handle <- dsFlower:::.createHandleFromTable(data, data_symbol = "D_a")
  dsFlower:::.pin_request_source(staged, handle)
  first <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  expect_identical(first[names(initial)], initial)
  expect_identical(first[["request-source"]],
                   list(source = "table", data_symbol = "D_a"))

  handle$run_token <- "another-run"
  handle$capability <- "another-handle"
  handle$staging_dir <- "/another/path"
  dsFlower:::.pin_request_source(staged, handle)
  replay <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  expect_identical(replay[["request-source"]], first[["request-source"]])

  handle$data_symbol <- "D_b"
  dsFlower:::.pin_request_source(staged, handle)
  changed <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  expect_false(identical(changed[["request-source"]], first[["request-source"]]))
  expect_identical(changed[names(initial)], first[names(initial)])
})

test_that("both prepare paths bind the source operand and refreshed descriptor", {
  state <- withr::local_tempdir()
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"))
  withr::local_options(list(dsflower.staging_root = state,
                           dsflower.nfilter.subset = 3L))
  data <- data.frame(x = seq_len(20), target = rep(c(0, 1), 10))
  for (name in c("source_a", "source_a_retry", "source_b")) {
    symbol <- if (name == "source_b") "D_b" else "D_a"
    dsFlower:::.setHandle(name, dsFlower:::.createHandleFromTable(
      data, data_symbol = symbol))
  }
  desc <- flower_dataset_descriptor("dataset-a", "in_memory_df", table_data = data)
  dsFlower:::.setHandle("source_descriptor", dsFlower:::.createHandleFromDescriptor(
    desc, data_symbol = "descriptor_a"))
  withr::defer(for (name in c("source_a", "source_a_retry", "source_b",
                             "source_descriptor")) dsFlower:::.removeHandle(name))

  flowerPrepareRunDS("source_a", "target", "x")
  flowerPrepareRunDS("source_a_retry", "target", "x")
  flowerPrepareRunDS("source_b", "target", "x")
  flowerPrepareRunDS("source_descriptor", "target", "x")
  read <- function(name) jsonlite::fromJSON(file.path(
    dsFlower:::.getHandle(name)$staging_dir, "manifest.json"), simplifyVector = FALSE)
  first <- read("source_a")
  replay <- read("source_a_retry")
  other <- read("source_b")
  descriptor <- read("source_descriptor")
  expect_false(identical(first$run_token, replay$run_token))
  expect_identical(first[["request-source"]], replay[["request-source"]])
  expect_false(identical(first[["request-source"]], other[["request-source"]]))
  expect_identical(descriptor[["request-source"]], list(
    source = "descriptor", data_symbol = "descriptor_a",
    dataset_id = "dataset-a", source_kind = "in_memory_df"))
})

test_that("analysts cannot provide request source key metadata", {
  forged <- list("request-source" = list(data_symbol = "forged"))
  expect_error(dsFlower:::.validate_client_run_config(forged), "server-owned")
  expect_error(dsFlower:::.validate_manifest_extra_config(forged), "server-owned")
})

test_that("request source uses the refreshed authorized descriptor", {
  staged <- withr::local_tempdir()
  path <- file.path(staged, "manifest.json")
  dsFlower:::.write_manifest_atomic(list(feature_columns = list("x")), path)
  previous <- flower_dataset_descriptor("old-dataset", "in_memory_df")
  refreshed <- flower_dataset_descriptor("current-dataset", "image_bundle")
  handle <- dsFlower:::.createHandleFromDescriptor(previous, data_symbol = "images")
  dsFlower:::.pin_request_source(staged, handle, refreshed)
  actual <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  expect_identical(actual[["request-source"]], list(
    source = "descriptor", data_symbol = "images",
    dataset_id = "current-dataset", source_kind = "image_bundle"))
})
