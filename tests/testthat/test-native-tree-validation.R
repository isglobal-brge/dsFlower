.native_validation_wire_fixture <- function() paste0(
  '{"contract":"dsflower-native-tree-request-v1","engine":"xgboost",',
  '"mode":"native-tight","task":"binary","public_schema":{',
  '"version":1,"features":["age","marker"],"lower":[0.0,-5.0],',
  '"upper":[100.0,5.0],"cuts":[[18.0,40.0,65.0],[-1.0,0.0,1.0]],',
  '"target":{"name":"outcome","kind":"binary","levels":',
  '[{"type":"string","value":"control"},',
  '{"type":"string","value":"case"}],"lower":0.0,"upper":1.0},',
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

.native_validation_config_fixture <- function() {
  wire <- charToRaw(enc2utf8(.native_validation_wire_fixture()))
  list(
    "dp-track" = "validation", "validation-model-track" = "native_tree",
    "validation-task" = "binary", "validation-bins" = 16L,
    "task-type" = "classification", "loss-name" = "bce_logits",
    "num-server-rounds" = 1L, "num-features" = 2L,
    "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("control", "case"),
    "validation-native-tree-request-b64" = gsub(
      "[\r\n]", "", jsonlite::base64_enc(wire)),
    "validation-native-tree-request-sha256" = digest::digest(
      wire, algo = "sha256", serialize = FALSE),
    "validation-artifact-format" = "dsflower-xgboost-ensemble-json-v1",
    "validation-artifact-sha256" = strrep("a", 64L),
    "validation-artifact-size-bytes" = 4096L,
    "validation-profile-sha256" = strrep("b", 64L),
    "validation-profile-size-bytes" = 1024L,
    "validation-public-schema-sha256" =
      "77a6e8d46a174381b8b4da168b833b2ee75f09f8ca8ac55f2c954be642ba9073")
}

.native_validation_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-native-validation-state-")
  dir.create(state_dir, recursive = TRUE)
  if (.Platform$OS.type == "windows") {
    dsFlower:::.windows_set_private_acl(state_dir, is_directory = TRUE)
  }
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
}

test_that("native XGBoost validation requires and canonicalizes every public pin", {
  config <- dsFlower:::.addDpConfigToRunConfig(
    .native_validation_config_fixture())
  expect_identical(config[["validation-model-track"]], "native_tree")
  expect_identical(config[["validation-artifact-size-bytes"]], 4096L)
  expect_identical(config[["validation-profile-size-bytes"]], 1024L)
  expect_no_error(dsFlower:::.validatePreparedNativeTreeContract(
    config, c("age", "marker"), "outcome"))
  expect_error(dsFlower:::.validatePreparedNativeTreeContract(
    config, c("marker", "age"), "outcome"), "schema differs")

  missing <- .native_validation_config_fixture()
  missing[["validation-profile-sha256"]] <- NULL
  expect_error(dsFlower:::.addDpConfigToRunConfig(missing),
               "exact binary or regression")
  tampered <- .native_validation_config_fixture()
  tampered[["validation-public-schema-sha256"]] <- strrep("0", 64L)
  expect_error(dsFlower:::.addDpConfigToRunConfig(tampered), "differs")
  neural <- .native_validation_config_fixture()
  neural[["validation-model-track"]] <- "neural"
  expect_error(dsFlower:::.addDpConfigToRunConfig(neural), "require.*native_tree")
})

test_that("native validation pins participate in the ephemeral execution contract", {
  config <- dsFlower:::.addDpConfigToRunConfig(
    .native_validation_config_fixture())
  first <- dsFlower:::.validationContractSha256(
    config, c("age", "marker"), "outcome", "row")
  changed <- config
  changed[["validation-artifact-sha256"]] <- strrep("c", 64L)
  second <- dsFlower:::.validationContractSha256(
    changed, c("age", "marker"), "outcome", "row")
  expect_match(first, "^[0-9a-f]{64}$")
  expect_false(identical(first, second))
})

test_that("native validation preparation stages only the bounded public pins", {
  .native_validation_privacy_state()
  name <- "test_native_validation_prepare"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    age = c(20, 30, 60), marker = c(-1, 0, 1),
    outcome = c("control", "case", "case"))))
  withr::defer(dsFlower:::.removeHandle(name))
  config <- .native_validation_config_fixture()
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["validation-contract-sha256"]] <-
    dsFlower:::.validationContractSha256(
      normalized, c("age", "marker"), "outcome",
      dsFlower:::.dpUnitPolicy()$dp_unit)

  local_mocked_bindings(
    .native_tree_validation_probe = function(engine, ...) {
      expect_identical(engine, "xgboost")
      TRUE
    },
    .package = "dsFlower")
  expect_no_error(flowerPrepareRunDS(
    name, "outcome", c("age", "marker"), config))
  handle <- dsFlower:::.getHandle(name)
  manifest <- jsonlite::fromJSON(
    file.path(handle$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(manifest[["dp-track"]], "validation")
  expect_identical(manifest[["validation-model-track"]], "native_tree")
  expect_identical(
    manifest[["validation-artifact-sha256"]], strrep("a", 64L))
  expect_identical(
    manifest[["validation-profile-sha256"]], strrep("b", 64L))
  expect_false(any(c(
    "validation-model-path-b64", "validation-profile-path-b64") %in%
    names(manifest)))

})

test_that("native validation rejects image routing before private staging", {
  name <- "test_native_validation_image"
  dsFlower:::.setHandle(name, mock_handle(
    data_path = "/private/data/must-not-be-read.csv"))
  withr::defer(dsFlower:::.removeHandle(name))
  config <- .native_validation_config_fixture()
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["validation-contract-sha256"]] <-
    dsFlower:::.validationContractSha256(
      normalized, c("age", "marker"), "outcome",
      dsFlower:::.dpUnitPolicy()$dp_unit)
  config$data_type <- "image"

  expect_error(flowerPrepareRunDS(
    name, "outcome", c("age", "marker"), config),
    "Native-tree validation accepts tabular data only")
})

test_that("native validation selects the dependency-light runtime without a training probe", {
  registry <- dsFlower:::.supernode_registry
  if (length(ls(registry))) rm(list = ls(registry), envir = registry)
  withr::defer(if (length(ls(registry))) {
    rm(list = ls(registry), envir = registry)
  })
  manifest_dir <- withr::local_tempdir()
  jsonlite::write_json(.native_validation_config_fixture(),
    file.path(manifest_dir, "manifest.json"), auto_unbox = TRUE)
  selected <- NULL
  local_mocked_bindings(
    .native_tree_xgboost_probe = function(...) {
      stop("native validation must not require the training bundle")
    },
    .native_tree_validation_probe = function(engine, ...) {
      expect_identical(engine, "xgboost")
      TRUE
    },
    .resolve_framework_runtime = function(framework) {
      selected <<- framework
      list(framework = framework, supernode_cmd = tempfile(),
           python = "python3", venv_path = tempdir())
    },
    .package = "dsFlower")

  expect_error(
    dsFlower:::.supernode_ensure("test:9092", manifest_dir),
    "CA certificate not found")
  expect_identical(selected, "native_tree")
})
