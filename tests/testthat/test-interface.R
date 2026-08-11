# Tests for R/interface.R — DataSHIELD Methods

local_interface_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-interface-state-")
  dir.create(state_dir, recursive = TRUE)
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
}

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
  # Create and set
  handle <- mock_handle(data_path = "/tmp/test.csv")
  reference <- dsFlower:::.setHandle("test_crud", handle)
  expect_named(reference, "capability")
  expect_match(reference$capability, "^hdl_[0-9a-f]{32}$")
  expect_false("data_path" %in% names(reference))

  # Get
  retrieved <- dsFlower:::.getHandle("test_crud")
  expect_equal(retrieved$data_path, "/tmp/test.csv")

  # Remove
  dsFlower:::.removeHandle("test_crud")
  expect_error(dsFlower:::.getHandle("test_crud"), "No Flower handle")
})

test_that("forged handle fields never become authoritative", {
  victim <- withr::local_tempdir()
  marker <- file.path(victim, "keep.txt")
  writeLines("keep", marker)
  forged_handle <- list(
    data_path = marker,
    run_token = "../../victim",
    staging_dir = victim,
    prepared = TRUE
  )
  expect_error(dsFlower:::.getHandle("forged_handle"), "forged")
  expect_error(flowerCleanupRunDS("forged_handle"), "forged")
  expect_true(file.exists(marker))

  unknown_handle <- structure(
    list(capability = paste0("hdl_", strrep("a", 32))),
    class = "dsflower_handle_ref")
  expect_error(dsFlower:::.getHandle("unknown_handle"), "stale")

  forged_fields_handle <- dsFlower:::.setHandle(
    "forged_fields_handle", mock_handle(data_path = "authoritative.csv"))
  forged_fields_handle$data_path <- marker
  forged_fields_handle$run_token <- "../../victim"
  expect_equal(
    dsFlower:::.getHandle("forged_fields_handle")$data_path,
    "authoritative.csv"
  )
  dsFlower:::.removeHandle("forged_fields_handle")
  expect_true(file.exists(marker))
})

test_that("handle capabilities are stale after destroy and bound to one session", {
  stale_handle <- dsFlower:::.setHandle("stale_handle", mock_handle())
  saved <- stale_handle
  dsFlower:::.removeHandle("stale_handle")
  stale_handle <- saved
  expect_error(dsFlower:::.getHandle("stale_handle"), "stale")

  owner_a <- new.env(parent = globalenv())
  owner_b <- new.env(parent = globalenv())
  reference <- dsFlower:::.registerHandle(mock_handle(), owner_env = owner_a)
  assign("owned_handle", reference, envir = owner_a)
  assign("owned_handle", reference, envir = owner_b)
  expect_equal(evalq(dsFlower:::.getHandle("owned_handle"), owner_a)$prepared,
               FALSE)
  expect_error(
    evalq(dsFlower:::.getHandle("owned_handle"), owner_b),
    "cross-session"
  )
  evalq(dsFlower:::.removeHandle("owned_handle"), owner_a)
})

test_that("opaque handles preserve the legitimate DSLite assign flow", {
  local_interface_privacy_state()
  skip_if_not_installed("DSLite")
  server <- DSLite::newDSLiteServer(tables = list(
    T = data.frame(f1 = seq_len(20), target = rep(0:1, 10))),
    config = list())
  server$assignMethod("flowerInitDS", "dsFlower::flowerInitDS")
  server$assignMethod("flowerPrepareRunDS", "dsFlower::flowerPrepareRunDS")
  server$assignMethod("flowerDestroyDS", "dsFlower::flowerDestroyDS")
  server$aggregateMethod("flowerStatusDS", "dsFlower::flowerStatusDS")
  server_name <- paste0("dsflower_handle_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSLite::dsConnect(
    DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSLite::dsDisconnect(connection))

  invisible(DSLite::dsAssignTable(connection, "D", "T"))
  invisible(DSLite::dsAssignExpr(
    connection, "flower", 'flowerInitDS("D")'))
  reference <- server$getSessionData(connection@sid, "flower")
  expect_named(reference, "capability")
  expect_false("data_path" %in% names(reference))

  invisible(DSLite::dsAssignExpr(
    connection, "flower",
    'flowerPrepareRunDS("flower", "target", "f1")'))
  status <- DSLite::dsFetch(DSLite::dsAggregate(
    connection, 'flowerStatusDS("flower")'))
  expect_true(status$prepared)
  invisible(DSLite::dsAssignExpr(
    connection, "flower", 'flowerDestroyDS("flower")'))
  expect_null(server$getSessionData(connection@sid, "flower"))
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

test_that("run rounds and data routing are pinned before staging", {
  expect_error(
    dsFlower:::.normalizeRunRounds(list("num-server-rounds" = 1.5)),
    "positive integer"
  )
  expect_identical(
    dsFlower:::.normalizeRunRounds(list())[["num-server-rounds"]], 1L)
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(num_rounds = 2L)),
    "num-server-rounds"
  )
  routed <- dsFlower:::.takeRunDataType(
    list(data_type = "IMAGE", "batch-size" = 8L), expected = "image")
  expect_equal(routed$data_type, "image")
  expect_null(routed$run_config$data_type)
  expect_error(
    dsFlower:::.takeRunDataType(list(data_type = "tabular"), expected = "image"),
    "disagrees"
  )
})

test_that("HookApp resource policy is finite, bounded, and unambiguous", {
  base <- list("num-server-rounds" = 1L)
  withr::local_options(list(
    dsflower.dp_sa_blocks = 8L,
    dsflower.dp_egress_timeout = 60L,
    dsflower.dp_egress_time_pad = 65,
    dsflower.dp_egress_memory_mb = 4096L,
    dsflower.dp_egress_file_mb = 512L,
    dsflower.dp_egress_processes = 32L
  ))
  cfg <- dsFlower:::.addDpConfigToRunConfig(base)
  expect_identical(cfg[["privacy-sa_blocks"]], 8L)
  expect_identical(cfg[["privacy-egress_timeout"]], 60L)
  expect_equal(cfg[["privacy-egress_time_pad"]], 65)
  expect_identical(cfg[["privacy-egress_memory_mb"]], 4096L)
  expect_identical(cfg[["privacy-egress_file_mb"]], 512L)
  expect_identical(cfg[["privacy-egress_processes"]], 32L)

  bad <- list(
    list(dsflower.dp_sa_blocks = 1.5),
    list(dsflower.dp_sa_blocks = Inf),
    list(dsflower.dp_sa_blocks = 65L),
    list(dsflower.dp_sa_blocks = c(2L, 3L)),
    list(dsflower.dp_egress_timeout = 3601L),
    list(dsflower.dp_egress_timeout = NaN),
    list(dsflower.dp_egress_time_pad = 230406),
    list(dsflower.dp_egress_time_pad = c(0, 1)),
    list(dsflower.dp_egress_memory_mb = 511L),
    list(dsflower.dp_egress_file_mb = 16385L),
    list(dsflower.dp_egress_processes = 0L)
  )
  for (opts in bad) {
    expect_error(withr::with_options(
      c(list(dsflower.dp_egress_timeout = 60L,
             dsflower.dp_egress_time_pad = 0), opts),
      dsFlower:::.addDpConfigToRunConfig(base)), "must be")
  }
  expect_error(withr::with_options(
    list(dsflower.dp_egress_timeout = 60L,
         dsflower.dp_egress_time_pad = 64),
    dsFlower:::.addDpConfigToRunConfig(base)), "at least 65")
})

test_that("HookApp public parameters are canonical and server hash pinned", {
  wire <- function(json) {
    gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(enc2utf8(json))))
  }
  encoded <- wire('{"alpha":0.25,"nested":{"labels":["a",null,true]}}')
  cfg <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "egress",
    "task-type" = "classification",
    "num-server-rounds" = 2L,
    "num-classes" = 2L,
    "app-params-b64" = encoded
  ))

  expect_identical(cfg[["app-params-b64"]], encoded)
  expect_identical(
    cfg[["app-params-sha256"]],
    digest::digest(jsonlite::base64_dec(encoded),
                   algo = "sha256", serialize = FALSE))
  expect_true("app-params-sha256" %in%
                dsFlower:::.server_owned_run_config_fields())

  defaults <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "egress", "task-type" = "classification",
    "num-server-rounds" = 1L, "num-classes" = 2L
  ))
  expect_identical(rawToChar(jsonlite::base64_dec(
    defaults[["app-params-b64"]])), "{}")
  expect_match(defaults[["app-params-sha256"]], "^[0-9a-f]{64}$")
})

test_that("HookApp manifest rejects aliases, tampering, and unsafe JSON", {
  wire <- function(json) {
    gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(enc2utf8(json))))
  }
  base <- list(
    "dp-track" = "egress", "task-type" = "classification",
    "num-server-rounds" = 1L, "num-classes" = 2L
  )
  invalid <- list(
    c(base, list("app_params" = list(alpha = 1))),
    c(base, list("app-params-extra" = wire("{}"))),
    c(base, list("app-params-sha256" = strrep("0", 64L))),
    c(base, list("app-params-b64" = wire('{"b":1,"a":2}'))),
    c(base, list("app-params-b64" = wire('{"a":1,"a":2}'))),
    c(base, list("app-params-b64" = wire('{"epsilon":1}'))),
    c(base, list("app-params-b64" = wire('{"training_epsilon":1}'))),
    c(base, list("app-params-b64" = wire('{"model_path":"weights"}'))),
    c(base, list("app-params-b64" = wire('{"value":"dir/file"}'))),
    c(base, list("app-params-b64" = wire('{"value":1e999}'))),
    c(base, list("app-params-b64" = "not-base64"))
  )
  for (config in invalid) {
    expect_error(dsFlower:::.addDpConfigToRunConfig(config))
  }

  too_large <- gsub(
    "[\r\n]", "",
    jsonlite::base64_enc(as.raw(rep.int(0L, 65537L))))
  expect_error(dsFlower:::.addDpConfigToRunConfig(c(
    base, list("app-params-b64" = too_large))), "bounded")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "neural", "num-server-rounds" = 1L,
    "app-params-b64" = wire("{}"))), "only valid")
})

test_that("validation config is pinned to one well-typed release", {
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "native_tree",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L)),
    "exact binary or regression.*pin set")

  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "validation-bins" = 24L,
    "task-type" = "classification", "loss-name" = "cross_entropy",
    "num-server-rounds" = 1L, "num-features" = 3L,
    "num-classes" = 4L, "num-labels" = 2L,
    "target-levels" = c("a", "b", "c", "d")))
  expect_identical(config[["dp-track"]], "validation")
  expect_identical(config[["num-server-rounds"]], 1L)
  expect_identical(config[["validation-bins"]], 24L)
  expect_false(config[["allow_per_node_metrics"]])

  maximum <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "cross_entropy", "num-server-rounds" = 1L,
    "num-features" = 3L, "num-classes" = 1024L,
    "num-labels" = 1024L))
  expect_identical(maximum[["num-classes"]], 1024L)
  expect_identical(maximum[["num-labels"]], 1024L)
  too_many <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "cross_entropy", "num-server-rounds" = 1L,
    "num-features" = 3L, "num-classes" = 1025L,
    "num-labels" = 1024L)
  expect_error(dsFlower:::.addDpConfigToRunConfig(too_many), "2, 1024")

  wrong_bounds <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = 0, upper = 1))
  expect_error(dsFlower:::.addDpConfigToRunConfig(wrong_bounds),
               "match num-features exactly")

  mixed_case <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "REGRESSION",
    "loss-name" = "MSE", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1)))
  expect_identical(mixed_case[["loss-name"]], "mse")

  quantile <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "regression",
    "loss-name" = "quantile", "quantile-level" = 0.9,
    "num-server-rounds" = 1L, "num-features" = 2L,
    "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1)))
  expect_identical(quantile[["loss-name"]], "quantile")

  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 2L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L)),
    "exactly one")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "regression",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1))),
    "disagrees")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L)),
    "binary")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multilabel", "task-type" = "classification",
    "loss-name" = "multilabel_bce", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L)),
    "binary target levels")
})

test_that("validation preparation persists the public contract before execution", {
  local_interface_privacy_state()
  name <- "test_validation_prepare"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    age = c(20, 30, 40), marker = c(-1, 0, 1), outcome = c(0, 1, 1))))
  withr::defer(dsFlower:::.removeHandle(name))
  config <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 16L,
    "task-type" = "classification", "loss-name" = "bce_logits",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(120, 5)),
    "target-levels" = c(0, 1))
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["validation-contract-sha256"]] <-
    dsFlower:::.validationContractSha256(
      normalized, c("age", "marker"), "outcome",
      dsFlower:::.dpUnitPolicy()$dp_unit)
  expect_error(
    flowerPrepareRunDS(name, "outcome", NULL, config),
    "explicit ordered public feature contract")
  expect_no_error(flowerPrepareRunDS(
    name, "outcome", c("age", "marker"), config))
  handle <- dsFlower:::.getHandle(name)
  manifest <- jsonlite::fromJSON(
    file.path(handle$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(manifest[["dp-track"]], "validation")
  expect_identical(manifest[["num-server-rounds"]], 1L)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
  expect_identical(manifest[["validation-model-track"]], "neural")
  expect_identical(manifest[["validation-task"]], "binary")
  expect_identical(manifest[["validation-bins"]], 16L)
  expect_identical(
    manifest[["validation-contract-sha256"]],
    config[["validation-contract-sha256"]])
})

test_that("validation never stages a patient identifier as model data", {
  local_interface_privacy_state()
  withr::local_options(list(
    dsflower.dp_unit = "patient",
    dsflower.patient_column = "subject_id"))
  name <- "test_validation_patient_overlap"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    subject_id = c("a", "b"), age = c(20, 30), outcome = c(0, 1))))
  withr::defer(dsFlower:::.removeHandle(name))
  base <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 16L,
    "task-type" = "classification", "loss-name" = "bce_logits",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 1L, "num-classes" = 2L, "num-labels" = 2L,
    "target-levels" = c(0, 1),
    "validation-contract-sha256" = strrep("a", 64L))
  expect_error(flowerPrepareRunDS(
    name, "outcome", "subject_id", base), "patient identifier")
  expect_error(flowerPrepareRunDS(
    name, "subject_id", "age", base), "patient identifier")
})

test_that("validation contract SHA-256 has a cross-package canonical wire", {
  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "validation-bins" = 24L,
    "task-type" = "classification", "loss-name" = "cross_entropy",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("a", "b", "c")))
  expect_identical(
    dsFlower:::.validationContractSha256(
      config, c("age", "marker"), "outcome", "patient"),
    "ecf31e300ff0087e26799f6a4fbe1894e8f8357e0fd35667936653318b040e3f")
})

test_that("flowerPrepareRunDS stages data correctly", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  # Create a handle manually
  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_prepare", handle)
  on.exit(dsFlower:::.removeHandle("test_prepare"), add = TRUE)

  # Prepare the run
  result <- flowerPrepareRunDS("test_prepare", "target", c("f1", "f2", "f3"))
  expect_named(result, "capability")
  state <- dsFlower:::.getHandle("test_prepare")

  expect_true(state$prepared)
  expect_equal(state$target_column, "target")
  expect_equal(state$feature_columns, c("f1", "f2", "f3"))
  expect_match(state$run_token, "^run_[0-9a-f]{32}$")
  expect_true(dir.exists(state$staging_dir))

  # Verify manifest exists
  manifest_path <- file.path(state$staging_dir, "manifest.json")
  expect_true(file.exists(manifest_path))

  manifest <- jsonlite::fromJSON(manifest_path)
  expect_equal(manifest$n_samples, 200)
  expect_equal(manifest$target_column, "target")
  expect_identical(manifest[["num-server-rounds"]], 1L)
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_gt(manifest[["privacy-delta"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
})

test_that("flowerPrepareRunDS does not expose a minimum-size admission bit", {
  local_interface_privacy_state()
  # Create tiny dataset
  tiny_dir <- tempdir()
  tiny_path <- file.path(tiny_dir, "tiny_test.csv")
  utils::write.csv(data.frame(f1 = 1:2, target = 0:1), tiny_path, row.names = FALSE)
  on.exit(unlink(tiny_path))

  handle <- mock_handle(data_path = tiny_path, data_format = "csv")
  dsFlower:::.setHandle("test_tiny", handle)
  on.exit(dsFlower:::.removeHandle("test_tiny"), add = TRUE)

  expect_no_error(flowerPrepareRunDS("test_tiny", "target"))
  state <- dsFlower:::.getHandle("test_tiny")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 2L)
  expect_equal(manifest$n_units, 2L)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
})

test_that("failed preparation does not alter a later training contract", {
  local_interface_privacy_state()
  dsFlower:::.setHandle(
    "test_failed_prepare_contract",
    mock_handle(table_data = data.frame(f1 = 1:5)))
  withr::defer(dsFlower:::.removeHandle("test_failed_prepare_contract"))
  expect_error(
    flowerPrepareRunDS("test_failed_prepare_contract", "target", "f1"),
    "Private data preparation failed on this node"
  )

  dsFlower:::.setHandle(
    "test_next_prepare_contract",
    mock_handle(table_data = data.frame(f1 = 1:5, target = rep(0:1, length.out = 5))))
  withr::defer(dsFlower:::.removeHandle("test_next_prepare_contract"))
  expect_no_error(
    flowerPrepareRunDS("test_next_prepare_contract", "target", "f1"))
  state <- dsFlower:::.getHandle("test_next_prepare_contract")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_gt(manifest[["privacy-delta"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
})

test_that("run admission is independent of a rare target class", {
  local_interface_privacy_state()
  # A class-count gate would turn prepare success/error into a label-dependent
  # transcript oracle. Only the fixed privacy-unit count may affect admission.
  withr::local_options(list(nfilter.subset = 3, nfilter.tab = 5))
  rare_path <- tempfile(fileext = ".csv")
  utils::write.csv(
    data.frame(f1 = seq_len(20), target = c(rep(0L, 19), 1L)),
    rare_path, row.names = FALSE)
  withr::defer(unlink(rare_path))

  handle <- mock_handle(data_path = rare_path, data_format = "csv")
  dsFlower:::.setHandle("test_rare_class_admission", handle)
  withr::defer(dsFlower:::.removeHandle("test_rare_class_admission"))

  expect_no_error(
    flowerPrepareRunDS("test_rare_class_admission", "target", "f1"))
  state <- dsFlower:::.getHandle("test_rare_class_admission")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 20L)
  expect_equal(manifest$n_units, 20L)
})

test_that("run admission fails closed without one exact privacy-unit count", {
  base_args <- list(
    handle = NULL, target_column = "target",
    n_samples = 20L, target_data = NULL,
    run_config = list("privacy-clipping_norm" = 1),
    data_type = "tabular"
  )
  expect_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = NULL))),
    "Invalid staged privacy-unit count"
  )
  expect_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = c(10L, 20L)))),
    "Invalid staged privacy-unit count"
  )
})

# --- TLS ca.pem handling ---

test_that("flowerEnsureSuperNodeDS writes ca.pem when ca_cert_pem provided", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_tls"), add = TRUE)

  # Prepare the handle first
  flowerPrepareRunDS("test_tls", "target", c("f1", "f2", "f3"))
  staging_dir <- dsFlower:::.getHandle("test_tls")$staging_dir

  # B64-encode a mock CA cert PEM (same as client would send)
  ca_pem <- "-----BEGIN CERTIFICATE-----\nMOCKCERT\n-----END CERTIFICATE-----"
  json <- as.character(jsonlite::toJSON(list(pem = ca_pem), auto_unbox = TRUE))
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  encoded <- paste0("B64:", b64)

  # Mock .supernode_ensure to avoid spawning real process
  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL,
                                 insecure = FALSE) {
      list(process = NULL, superlink_address = superlink_address,
           ca_cert_path = ca_cert_path)
    }
  )

  flowerEnsureSuperNodeDS("test_tls", "127.0.0.1:9092",
                          "fl-test", encoded)
  updated <- dsFlower:::.getHandle("test_tls")

  # Verify ca.pem was written
  ca_pem_path <- file.path(staging_dir, "ca.pem")
  expect_true(file.exists(ca_pem_path))
  written_pem <- paste(readLines(ca_pem_path, warn = FALSE), collapse = "\n")
  expect_true(grepl("MOCKCERT", written_pem))
  expect_equal(updated$ca_cert_path, ca_pem_path)
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
  pins <- jsonlite::fromJSON(
    file.path(staging_dir, "pinned_packages.json"), simplifyVector = FALSE)
  expect_identical(
    as.character(pins$dsflower_runner),
    dsFlower:::.compute_harness_hash())
})

test_that("flowerEnsureSuperNodeDS works without ca_cert_pem", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_no_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_no_tls"), add = TRUE)

  flowerPrepareRunDS("test_no_tls", "target", c("f1", "f2", "f3"))

  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL,
                                 insecure = FALSE) {
      list(process = NULL, superlink_address = superlink_address,
           ca_cert_path = ca_cert_path)
    }
  )

  flowerEnsureSuperNodeDS("test_no_tls", "127.0.0.1:9092", "fl-test")
  updated <- dsFlower:::.getHandle("test_no_tls")
  expect_null(updated$ca_cert_path)
  expect_true(updated$node_ensured)
})

test_that("a client-supplied hostname is not mistaken for a trusted coordinator", {
  handle_name <- "test_untrusted_hostname"
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  dsFlower:::.setHandle(handle_name, mock_handle(
    run_token = run_token, staging_dir = staging_dir, prepared = TRUE))
  withr::defer(dsFlower:::.removeHandle(handle_name))
  withr::local_options(list(
    dsflower.coordinator_address = "",
    dsflower.allow_untrusted_coordinator = FALSE
  ))
  env <- getFromNamespace(".dsflower_env", "dsFlower")
  old_port <- env$tunnel_forwarder_port
  env$tunnel_forwarder_port <- NULL
  withr::defer(env$tunnel_forwarder_port <- old_port)

  expect_error(
    flowerEnsureSuperNodeDS(handle_name, "evil.example:9092"),
    "Refusing SuperNode"
  )
  # Loopback is not an operator authorization either when supplied directly by
  # the analyst; only the server-created tunnel or an exact admin pin is trusted.
  expect_error(
    flowerEnsureSuperNodeDS(handle_name, "127.0.0.1:9092"),
    "Refusing SuperNode"
  )
})

test_that("flowerCleanupRunDS resets handle state", {
  csv_path <- create_test_csv(n = 10)
  on.exit(unlink(csv_path))

  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    data_path = csv_path,
    run_token = run_token,
    staging_dir = staging_dir,
    target_column = "target",
    feature_columns = c("f1"),
    prepared = TRUE
  )
  dsFlower:::.setHandle("test_cleanup", handle)
  on.exit(dsFlower:::.removeHandle("test_cleanup"), add = TRUE)

  flowerCleanupRunDS("test_cleanup")
  state <- dsFlower:::.getHandle("test_cleanup")
  expect_false(state$prepared)
  expect_false(state$node_ensured)
  expect_null(state$run_token)
  expect_null(state$staging_dir)
  expect_null(state$target_column)
})

test_that("flowerCleanupRunDS stops associated SuperNode before reset", {
  stopped <- character()
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    run_token = run_token,
    staging_dir = staging_dir,
    prepared = TRUE,
    node_ensured = TRUE
  )
  dsFlower:::.setHandle("test_cleanup_stop", handle)
  on.exit(dsFlower:::.removeHandle("test_cleanup_stop"), add = TRUE)

  local_mocked_bindings(
    .supernode_stop = function(manifest_dir) {
      stopped <<- c(stopped, manifest_dir)
      invisible(TRUE)
    },
    .cleanupStaging = function(run_token) invisible(TRUE)
  )

  flowerCleanupRunDS("test_cleanup_stop")
  expect_equal(stopped, handle$staging_dir)
})

test_that("flowerGetCapabilitiesDS returns expected structure", {
  xgboost_calls <- 0L
  pure_calls <- list()
  local_mocked_bindings(
    .native_tree_xgboost_probe = function(...) {
      xgboost_calls <<- xgboost_calls + 1L
      FALSE
    },
    .native_tree_pure_probes = function(engines, ...) {
      pure_calls[[length(pure_calls) + 1L]] <<- engines
      stats::setNames(rep.int(FALSE, length(engines)), engines)
    },
    .package = "dsFlower")
  caps <- flowerGetCapabilitiesDS()
  expect_type(caps, "list")
  expect_true("dsflower_version" %in% names(caps))
  expect_true("python_version" %in% names(caps))
  expect_true("flower_version" %in% names(caps))
  expect_true("torch_version" %in% names(caps))
  expect_true("opacus_version" %in% names(caps))
  expect_true("runtime_versions_sha256" %in% names(caps))
  expect_identical(caps$runner_abi, 3L)
  expect_match(caps$privacy_policy_sha256, "^[0-9a-f]{64}$")
  expect_identical(caps$privacy_clipping_norm, 1)
  expect_identical(caps$dp_tracks,
                   c("neural", "egress", "validation"))
  expect_setequal(
    caps$declarative_model_ops$layers,
    c("linear", "relu", "gelu", "tanh", "sigmoid", "elu", "silu",
      "leaky_relu", "dropout", "layernorm", "softmax", "reshape",
      "flatten", "conv1d", "conv2d", "maxpool2d",
      "adaptiveavgpool2d", "upsample", "lstm", "gru")
  )
  expect_setequal(
    caps$declarative_model_ops$graph,
    c("add", "mul", "sub", "div", "affine", "concat", "matmul",
      "transpose")
  )
  expect_setequal(
    caps$declarative_losses,
    c("bce_logits", "cross_entropy", "mse", "poisson_nll",
      "multilabel_bce", "hinge", "ordinal", "negbin_nll", "gamma_nll",
      "huber", "quantile")
  )
  expect_false("tree_objectives" %in% names(caps))
  expect_identical(caps$native_tree$contract,
                   "dsflower-native-tree-request-v1")
  expect_identical(caps$native_tree$probed_engines, character())
  expect_false(any(grepl(
    "_native_tight_available$", names(caps$native_tree))))
  expect_identical(xgboost_calls, 0L)
  expect_length(pure_calls, 0L)
  expect_setequal(
    caps$aggregation_strategies,
    c("fedavg", "fedadam", "fedadagrad", "fedyogi", "fedavgm")
  )
  expect_identical(caps$resampling$holdout$tracks, "neural")
  expect_identical(caps$resampling$holdout$data_kinds, "tabular")
  expect_true(caps$resampling$holdout$pooled_only)
  expect_true(caps$resampling$cross_validation$available)
  expect_identical(caps$resampling$cross_validation$tracks, "neural")
  expect_identical(caps$resampling$cross_validation$data_kinds, "tabular")
  expect_identical(caps$resampling$cross_validation$folds, c(2L, 10L))
  expect_true(caps$resampling$cross_validation$pooled_only)
  expect_true("max_rounds" %in% names(caps))
  expect_true("min_samples" %in% names(caps))
  expect_false("secure_aggregation_supported" %in% names(caps))
  expect_false(caps$hook_execution_configured)

  targeted <- flowerGetCapabilitiesDS("random_forest")
  expect_identical(targeted$native_tree$probed_engines, "random_forest")
  expect_false(targeted$native_tree$random_forest_native_tight_available)
  expect_false("xgboost_native_tight_available" %in%
                 names(targeted$native_tree))
  expect_identical(xgboost_calls, 0L)
  expect_identical(pure_calls, list("random_forest"))
  expect_error(flowerGetCapabilitiesDS("Random_Forest"),
               "native_tree_probe must be exactly")
})

test_that("unsupported run configuration fails before private preparation", {
  expect_identical(
    dsFlower:::.validate_client_run_config(list(label_set = "clinical")),
    list(label_set = "clinical")
  )
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(
      "dp-track" = "trees", "num-server-rounds" = 1L)),
    "dp-track must be one of")
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(
      "dp-track" = "neural", "num-server-rounds" = 1L,
      "unexpected-field" = "value")),
    "unsupported field.*unexpected-field")
})

test_that("Hook readiness capability reflects every public admin gate", {
  withr::local_options(list(
    dsflower.hook_enabled = TRUE,
    dsflower.hook_sandbox_attested = TRUE,
    dsflower.hook_resource_isolation_attested = TRUE,
    dsflower.dp_egress_timeout = 30,
    dsflower.dp_egress_time_pad = 35))
  caps <- flowerGetCapabilitiesDS()
  expect_true(caps$hook_time_envelope_configured)
  expect_true(caps$hook_execution_configured)
  expect_identical(caps$hook_required_time_pad_seconds, 35)
})

test_that("Hook sample-and-aggregate timing covers every sequential block", {
  withr::local_options(list(
    dsflower.hook_enabled = TRUE,
    dsflower.hook_sandbox_attested = TRUE,
    dsflower.hook_resource_isolation_attested = TRUE,
    dsflower.dp_sample_aggregate = TRUE,
    dsflower.dp_sa_blocks = 3L,
    dsflower.dp_egress_timeout = 30,
    dsflower.dp_egress_time_pad = 95))
  caps <- flowerGetCapabilitiesDS()
  expect_identical(caps$hook_required_time_pad_seconds, 95)
  expect_true(caps$hook_execution_configured)
})

test_that("flowerGetCapabilitiesDS omits infrastructure and session state", {
  withr::local_options(list(
    dsflower.hook_resource_isolation_attested = TRUE))
  caps <- flowerGetCapabilitiesDS()
  expect_false(any(c(
    "python_envs", "hostname", "is_docker", "active_supernodes",
    "zombie_processes"
  ) %in% names(caps)))
  expect_true(caps$hook_resource_isolation_attested)
})

test_that("flowerCheckConnectivityDS detects unreachable address", {
  result <- flowerCheckConnectivityDS("192.0.2.1:99999", timeout_secs = 1)
  expect_type(result, "list")
  expect_false(result$reachable)
})

test_that("flowerCheckConnectivityDS rejects bad format", {
  result <- flowerCheckConnectivityDS("not-a-valid-address")
  expect_false(result$reachable)
  expect_true(grepl("Invalid", result$error))
})

test_that("connectivity checks reject every non-global IPv4 class", {
  unsafe <- c(
    "0.1.2.3", "10.0.0.1", "100.64.0.1", "100.100.100.200",
    "127.0.0.1", "169.254.169.254", "172.31.255.255",
    "192.0.0.1", "192.0.2.1", "192.88.99.1", "192.168.1.1",
    "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1",
    "255.255.255.255"
  )
  expect_true(all(vapply(
    unsafe, dsFlower:::.is_private_or_local_host, logical(1))))
  expect_false(dsFlower:::.is_private_or_local_host("8.8.8.8"))

  # The RFC6598 range includes cloud metadata endpoints such as
  # 100.100.100.200, so rejection must happen before opening a socket.
  withr::local_options(list(dsflower.restrict_connectivity = TRUE,
                            dsflower.coordinator_address = ""))
  result <- flowerCheckConnectivityDS("100.100.100.200:80")
  expect_false(result$reachable)
  expect_match(result$error, "not allowed")
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
