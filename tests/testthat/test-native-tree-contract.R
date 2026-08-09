.native_tree_wire_fixture <- function() paste0(
  '{"contract":"dsflower-native-tree-request-v1","engine":"xgboost",',
  '"mode":"native-tight","task":"binary","public_schema":{',
  '"version":1,"features":["age","marker"],"lower":[0.0,-5.0],',
  '"upper":[100.0,5.0],"cuts":[[18.0,40.0,65.0],[-1.0,0.0,1.0]],',
  '"target":{"name":"outcome","kind":"binary","levels":',
  '[{"type":"string","value":"control"},{"type":"string","value":"case"}],',
  '"lower":0.0,"upper":1.0},',
  '"sha256":"77a6e8d46a174381b8b4da168b833b2ee75f09f8ca8ac55f2c954be642ba9073"},',
  '"parameters":[{"name":"learning_rate","type":"number","value":0.25},',
  '{"name":"max_delta_step","type":"number","value":1.0},',
  '{"name":"max_depth","type":"integer","value":2},',
  '{"name":"min_child_weight","type":"number","value":1.0},',
  '{"name":"min_split_loss","type":"number","value":0.0},',
  '{"name":"num_boost_round","type":"integer","value":8},',
  '{"name":"reg_alpha","type":"number","value":0.0},',
  '{"name":"reg_lambda","type":"number","value":1.0}],',
  '"resources":{"max_features":2,"max_trees":8,"max_depth":2,',
  '"max_bins":4,"max_threads":32,"memory_mb":32768,',
  '"timeout_seconds":21600}}')

.native_tree_manifest_fixture <- function() {
  jsonlite::fromJSON(.native_tree_wire_fixture(), simplifyVector = FALSE)
}

test_that("server validates the exact client native-tree wire", {
  manifest <- dsFlower:::.validate_native_tree_manifest(
    .native_tree_manifest_fixture(),
    "193390a92a076bf9d4cdac0686e6542990b5948809cdc8f1dbbc9ccaac787692")

  expect_identical(manifest$json, .native_tree_wire_fixture())
  expect_identical(
    manifest$value$public_schema$sha256,
    "77a6e8d46a174381b8b4da168b833b2ee75f09f8ca8ac55f2c954be642ba9073")
  expect_identical(
    digest::digest(jsonlite::base64_dec(manifest$b64),
                   algo = "sha256", serialize = FALSE),
    manifest$sha256)
})

test_that("server preserves one-feature arrays in the canonical wire", {
  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$features <- list("x")
  manifest$public_schema$lower <- list(0)
  manifest$public_schema$upper <- list(1)
  manifest$public_schema$cuts <- list(list(0.5))
  manifest$engine <- "catboost"
  core <- manifest$public_schema[c(
    "version", "features", "lower", "upper", "cuts", "target")]
  manifest$public_schema$sha256 <- digest::digest(
    dsFlower:::.native_tree_json(core), algo = "sha256", serialize = FALSE)
  manifest$parameters <- list(list(
    name = "one_element_array", type = "integer_array", value = list(1L)))

  pinned <- dsFlower:::.validate_native_tree_manifest(manifest)
  expect_match(pinned$json, '"features":\\["x"\\]')
  expect_match(pinned$json, '"lower":\\[0.0\\]')
  expect_match(pinned$json, '"cuts":\\[\\[0.5\\]\\]')
  expect_match(pinned$json, '"value":\\[1\\]')
  expect_identical(
    pinned$value$public_schema$sha256,
    "2d7a1f025f798f69165fdbbdd5fa2b19c6e95b5cf5b8ab78d434356a496ab234")
})

test_that("server rejects native-tree manifest and schema tampering", {
  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$upper[[1L]] <- 101
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "schema SHA-256")

  manifest <- .native_tree_manifest_fixture()
  manifest$unexpected <- TRUE
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "fields do not match")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$version <- "1"
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "Invalid public feature schema")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$target$name <- "age"
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "target name must differ")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$target$upper <- 2
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "binary task requires")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$target$levels[[2L]]$type <- "number"
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "number target level")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$target$levels[[2L]] <-
    manifest$public_schema$target$levels[[1L]]
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "must be distinct")

  manifest <- .native_tree_manifest_fixture()
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest, strrep("0", 64L)),
    "manifest SHA-256 mismatch")
})

test_that("server enforces tight-mode and resource constraints", {
  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema["cuts"] <- list(NULL)
  schema_core <- manifest$public_schema[c(
    "version", "features", "lower", "upper", "cuts", "target")]
  manifest$public_schema$sha256 <- digest::digest(
    dsFlower:::.native_tree_json(schema_core),
    algo = "sha256", serialize = FALSE)
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "requires public cuts")

  for (name in c(
      "eval_metric", "early_stopping_rounds", "seed", "tree_method")) {
    manifest <- .native_tree_manifest_fixture()
    manifest$parameters <- list(list(name = name, type = "string", value = "x"))
    expect_error(
      dsFlower:::.validate_native_tree_manifest(manifest),
      "native-tight forbids")
  }

  for (name in c(
      "callbacks", "custom_objective", "custom_objective_fn", "plugin_path",
      "max_rows_per_unit", "unit_canonicalization")) {
    manifest <- .native_tree_manifest_fixture()
    manifest$parameters <- list(list(name = name, type = "string", value = "x"))
    expect_error(
      dsFlower:::.validate_native_tree_manifest(manifest),
      "reserved by the server")
  }

  manifest <- .native_tree_manifest_fixture()
  manifest$mode <- "unsupported"
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "Unsupported tree mode")

  manifest <- .native_tree_manifest_fixture()
  parameter <- which(vapply(
    manifest$parameters, `[[`, character(1), "name") == "num_boost_round")
  manifest$parameters[[parameter]]$value <- 9L
  manifest$resources$max_trees <- 8L
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "exceeds resources\\$max_trees")
})

test_that("server enforces the exact typed XGBoost profile", {
  manifest <- .native_tree_manifest_fixture()
  manifest$parameters[[length(manifest$parameters) + 1L]] <- list(
    name = "subsample", type = "number", value = 0.8)
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "Unsupported XGBoost parameter.*subsample")

  for (name in c("max_delta_step", "reg_lambda")) {
    manifest <- .native_tree_manifest_fixture()
    index <- which(vapply(
      manifest$parameters, `[[`, character(1), "name") == name)
    manifest$parameters[[index]]$value <- 0
    expect_error(
      dsFlower:::.validate_native_tree_manifest(manifest),
      paste0("XGBoost parameter '", name, "'.*supported range"))
  }

  manifest <- .native_tree_manifest_fixture()
  index <- which(vapply(
    manifest$parameters, `[[`, character(1), "name") == "max_depth")
  manifest$parameters[[index]]$type <- "number"
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "XGBoost parameter 'max_depth'.*wrong declared type")

  manifest <- .native_tree_manifest_fixture()
  index <- which(vapply(
    manifest$parameters, `[[`, character(1), "name") == "max_depth")
  manifest$parameters[[index]]$value <- 31L
  manifest$resources$max_depth <- 31L
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "XGBoost parameter 'max_depth'.*supported range")

  manifest <- .native_tree_manifest_fixture()
  index <- which(vapply(
    manifest$parameters, `[[`, character(1), "name") == "learning_rate")
  manifest$parameters[[index]]$value <- 1e-300
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "learning_rate.*remain positive as float32")

  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$cuts[[1L]] <- list(18, 18 + 1e-10, 65)
  core <- manifest$public_schema[c(
    "version", "features", "lower", "upper", "cuts", "target")]
  manifest$public_schema$sha256 <- digest::digest(
    dsFlower:::.native_tree_json(core), algo = "sha256", serialize = FALSE)
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "cuts and bounds must remain strict as float32")
})

test_that("server bounds public cuts and canonical manifest bytes", {
  manifest <- .native_tree_manifest_fixture()
  manifest$public_schema$features <- list("x")
  manifest$public_schema$lower <- list(0)
  manifest$public_schema$upper <- list(20000)
  manifest$public_schema$cuts <- list(as.list(seq_len(16385L)))
  manifest$resources$max_bins <- 20000L
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "16384-cut contract cap")

  manifest <- .native_tree_manifest_fixture()
  manifest$engine <- "catboost"
  manifest$parameters <- lapply(seq_len(128L), function(i) list(
    name = sprintf("parameter_%03d", i),
    type = "string", value = strrep("x", 512L)))
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "exceeds 65536 bytes")
})

test_that("native-tree request wire re-pins exact bytes and digest", {
  pinned <- dsFlower:::.validate_native_tree_manifest(
    .native_tree_manifest_fixture())
  decoded <- dsFlower:::.validate_native_tree_request_wire(
    pinned$b64, pinned$sha256)
  expect_identical(decoded$json, pinned$json)
  expect_identical(decoded$value, pinned$value)
  expect_error(
    dsFlower:::.validate_native_tree_request_wire(
      pinned$b64, strrep("0", 64L)),
    "SHA-256 mismatch")
  expect_error(
    dsFlower:::.validate_native_tree_request_wire(
      paste0(pinned$b64, "\n"), pinned$sha256),
    "canonical bounded base64")
})

test_that("native-tree run config preserves and binds the exact request", {
  pinned <- dsFlower:::.validate_native_tree_manifest(
    .native_tree_manifest_fixture())
  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "native_tree",
    "task-type" = "classification",
    "num-server-rounds" = 1L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("control", "case"),
    "native-tree-request-b64" = pinned$b64,
    "native-tree-request-sha256" = pinned$sha256))
  expect_identical(config[["native-tree-request-b64"]], pinned$b64)
  expect_identical(config[["native-tree-request-sha256"]], pinned$sha256)
  expect_no_error(dsFlower:::.validatePreparedNativeTreeContract(
    config, c("age", "marker"), "outcome"))
  expect_error(dsFlower:::.validatePreparedNativeTreeContract(
    config, c("marker", "age"), "outcome"), "schema differs")

  tampered <- config
  tampered[["native-tree-request-sha256"]] <- strrep("0", 64L)
  expect_error(dsFlower:::.normalizeNativeTreeConfig(
    tampered, "native_tree"), "SHA-256 mismatch")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "neural", "num-server-rounds" = 1L,
    "native-tree-request-b64" = pinned$b64,
    "native-tree-request-sha256" = pinned$sha256)),
    "require dp-track='native_tree'")
})

test_that("native XGBoost capability is an operational fail-closed probe", {
  root <- withr::local_tempdir()
  runner <- file.path(root, "dsflower_runner")
  runtime <- file.path(root, "runtime")
  bundle <- file.path(root, "bundle")
  dir.create(runner)
  dir.create(bundle)
  dir.create(dirname(dsFlower:::.native_tree_runtime_executable(
    runtime, "python")), recursive = TRUE)
  file.create(file.path(runner, "native_tree_server_app.py"))
  file.create(file.path(runner, "native_tree_client_app.py"))
  file.create(dsFlower:::.native_tree_runtime_executable(runtime, "python"))
  file.create(dsFlower:::.native_tree_runtime_executable(
    runtime, "flower-supernode"))
  hook <- file.path(root, "sitecustomize.py")
  writeLines("dsflower_runner.native_tree_client_app:app", hook)
  calls <- 0L
  invocation <- NULL
  run_probe <- function(...) {
    calls <<- calls + 1L
    invocation <<- list(...)
    list(status = 0L, stdout = "available", stderr = "")
  }

  expect_true(dsFlower:::.native_tree_xgboost_probe(
    runner, hook, runtime, bundle, run_probe))
  expect_identical(calls, 1L)
  probe_code <- paste(invocation$args, collapse = "\n")
  expect_match(probe_code, "native_tree_client_app", fixed = TRUE)
  expect_match(probe_code, "native_tree_server_app", fixed = TRUE)
  expect_match(probe_code, "is_verified_bundle", fixed = TRUE)
  expect_identical(
    unname(invocation$env[["DSFLOWER_XGBOOST_BUNDLE_ROOT"]]),
    bundle)
  unlink(file.path(runner, "native_tree_client_app.py"))
  expect_false(dsFlower:::.native_tree_xgboost_probe(
    runner, hook, runtime, bundle, run_probe))
  expect_identical(calls, 1L)
})

test_that("native-tree contract capability reflects the fresh probe", {
  capabilities <- dsFlower:::.native_tree_contract_capabilities(FALSE)
  expect_identical(capabilities$contract, "dsflower-native-tree-request-v1")
  expect_identical(capabilities$modes, "native-tight")
  expect_setequal(
    capabilities$engines,
    c("xgboost", "lightgbm", "catboost", "random_forest"))
  expect_identical(capabilities$tasks, c("binary", "regression"))
  expect_true(capabilities$native_tight_requires_public_cuts)
  expect_identical(capabilities$max_manifest_bytes, 65536L)
  expect_identical(capabilities$max_total_public_cuts, 16384L)
  expect_false(capabilities$xgboost_native_tight_available)
  expect_setequal(
    capabilities$xgboost_required_parameters,
    c("learning_rate", "max_delta_step", "max_depth", "min_child_weight",
      "min_split_loss", "num_boost_round", "reg_alpha", "reg_lambda"))
  expect_length(capabilities$xgboost_optional_parameters, 0L)
  expect_true(dsFlower:::.native_tree_contract_capabilities(
    TRUE)$xgboost_native_tight_available)
})

test_that("native-tree prepare rejects an unavailable runtime before private IO", {
  pinned <- dsFlower:::.validate_native_tree_manifest(
    .native_tree_manifest_fixture())
  config <- list(
    "dp-track" = "native_tree",
    "task-type" = "classification",
    "num-server-rounds" = 1L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("control", "case"),
    "native-tree-request-b64" = pinned$b64,
    "native-tree-request-sha256" = pinned$sha256)
  dsFlower:::.setHandle(
    "native_tree_probe_false",
    mock_handle(data_path = "/private/data/must-not-be-read.csv"))
  withr::defer(dsFlower:::.removeHandle("native_tree_probe_false"))
  private_io <- FALSE
  testthat::local_mocked_bindings(
    .native_tree_xgboost_probe = function(...) FALSE,
    .loadTrainingData = function(...) {
      private_io <<- TRUE
      stop("private data was read", call. = FALSE)
    },
    .stageData = function(...) {
      private_io <<- TRUE
      stop("private data was staged", call. = FALSE)
    },
    .package = "dsFlower")

  expect_error(
    flowerPrepareRunDS(
      "native_tree_probe_false", "outcome", c("age", "marker"), config),
    "verified native XGBoost runtime is unavailable")
  image_config <- config
  image_config$data_type <- "image"
  expect_error(
    flowerPrepareRunDS(
      "native_tree_probe_false", "outcome", c("age", "marker"), image_config),
    "accepts tabular data only")
  expect_false(private_io)
})
