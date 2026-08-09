.native_tree_wire_fixture <- function() paste0(
  '{"contract":"dsflower-native-tree-request-v1","engine":"xgboost",',
  '"mode":"native-tight","task":"binary","public_schema":{',
  '"version":1,"features":["age","marker"],"lower":[0.0,-5.0],',
  '"upper":[100.0,5.0],"cuts":[[18.0,40.0,65.0],[-1.0,0.0,1.0]],',
  '"target":{"name":"outcome","kind":"binary","lower":0.0,"upper":1.0},',
  '"sha256":"a24299d5ccba8a1af70f0c2d5afa06937d9632a75bc69d20d3e1520ec96d5733"},',
  '"parameters":[{"name":"max_depth","type":"integer","value":6},',
  '{"name":"monotone_constraints","type":"integer_array","value":[1,-1]},',
  '{"name":"subsample","type":"number","value":0.8}],',
  '"resources":{"max_features":4096,"max_trees":4096,"max_depth":8,',
  '"max_bins":8,"max_threads":32,"memory_mb":32768,',
  '"timeout_seconds":21600}}')

.native_tree_manifest_fixture <- function() {
  jsonlite::fromJSON(.native_tree_wire_fixture(), simplifyVector = FALSE)
}

test_that("server validates the exact client native-tree wire", {
  manifest <- dsFlower:::.validate_native_tree_manifest(
    .native_tree_manifest_fixture(),
    "6b80230e762a3ab73c3f4d655ae3b3ff8304d05a6076a3975f065a227ee177bb")

  expect_identical(manifest$json, .native_tree_wire_fixture())
  expect_identical(
    manifest$value$public_schema$sha256,
    "a24299d5ccba8a1af70f0c2d5afa06937d9632a75bc69d20d3e1520ec96d5733")
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
  core <- manifest$public_schema[c(
    "version", "features", "lower", "upper", "cuts", "target")]
  manifest$public_schema$sha256 <- digest::digest(
    dsFlower:::.native_tree_json(core), algo = "sha256", serialize = FALSE)
  manifest$parameters <- list(list(
    name = "monotone_constraints", type = "integer_array", value = list(1L)))

  pinned <- dsFlower:::.validate_native_tree_manifest(manifest)
  expect_match(pinned$json, '"features":\\["x"\\]')
  expect_match(pinned$json, '"lower":\\[0.0\\]')
  expect_match(pinned$json, '"cuts":\\[\\[0.5\\]\\]')
  expect_match(pinned$json, '"value":\\[1\\]')
  expect_identical(
    pinned$value$public_schema$sha256,
    "fb4a74228657414d935f4dc4f068f0c53f83743d237c100db49c40dab9c622b7")
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
    manifest$mode <- "synopsis-flex"
    manifest$parameters <- list(list(name = name, type = "string", value = "x"))
    expect_error(
      dsFlower:::.validate_native_tree_manifest(manifest),
      "reserved by the server")
  }

  manifest <- .native_tree_manifest_fixture()
  manifest$parameters <- list(list(
    name = "n_estimators", type = "integer", value = 9L))
  manifest$resources$max_trees <- 8L
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "exceeds resources\\$max_trees")
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
  manifest$mode <- "synopsis-flex"
  manifest$parameters <- lapply(seq_len(128L), function(i) list(
    name = sprintf("parameter_%03d", i),
    type = "string", value = strrep("x", 512L)))
  expect_error(
    dsFlower:::.validate_native_tree_manifest(manifest),
    "exceeds 65536 bytes")
})

test_that("native-tree contract capabilities do not claim backend availability", {
  capabilities <- dsFlower:::.native_tree_contract_capabilities()
  expect_identical(capabilities$contract, "dsflower-native-tree-request-v1")
  expect_setequal(
    capabilities$engines,
    c("xgboost", "lightgbm", "catboost", "random_forest"))
  expect_identical(capabilities$tasks, c("binary", "regression"))
  expect_true(capabilities$native_tight_requires_public_cuts)
  expect_identical(capabilities$max_manifest_bytes, 65536L)
  expect_identical(capabilities$max_total_public_cuts, 16384L)
  expect_false("available" %in% names(capabilities))
})
