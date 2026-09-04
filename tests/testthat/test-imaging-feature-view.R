local_feature_view_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-feature-view-state-")
  dir.create(state_dir, recursive = TRUE)
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
}

imaging_feature_fixture <- function(owner_env, label_col = "diagnosis") {
  rows <- data.frame(
    sample_id = paste0("scan", 1:4),
    patient_id = c("patient1", "patient1", "patient2", "patient3"),
    diagnosis = c("case", "case", "control", "control"),
    stringsAsFactors = FALSE)
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(rows, metadata_path, row.names = FALSE)
  metadata <- list(
    uri = metadata_path, file = metadata_path, format = "csv",
    id_col = "sample_id", privacy_unit = "patient",
    privacy_unit_col = "patient_id",
    privacy_unit_canonicalization = "trim-utf8-v2")
  if (!is.null(label_col)) {
    metadata$label_col <- label_col
    metadata$label_levels <- c("case", "control")
  }
  manifest <- list(
    schema_version = 1L, dataset_id = "radiomics.site", modality = "image",
    metadata = metadata,
    assets = list(images = list(
      type = "image_root", uri = dirname(metadata_path))))
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  handle <- list(
    dataset_id = manifest$dataset_id, manifest = manifest, backend = NULL,
    manifest_uri = NULL, privacy = admission$contract,
    n_privacy_units = admission$n_privacy_units,
    privacy_roster = admission$roster,
    collection_seal = strrep("a", 64))
  reference <- dsImaging:::.register_imaging_handle(handle, owner_env)
  assign("img", reference, envir = owner_env)

  feature_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = rows$sample_id,
    radiomics_mean = c(1.5, 2.5, 3.5, 4.5)),
    feature_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, manifest$dataset_id, "feature_table", feature_path,
    visibility = "global", collection_seal = strrep("a", 64))
  dsImaging:::.asset_db_close(db)
  assign("asset_id", asset_id, envir = owner_env)
  list(rows = rows, asset_id = asset_id)
}

test_that("opaque imaging features stage with patient DP and exact roster", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(
    "imagingFeatureViewDS", envir = asNamespace("dsImaging"),
    inherits = FALSE))
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    dsflower.nfilter.subset = 3L,
    dsflower.dp_unit = "row",
    dsflower.patient_column = "wrong_global_column"))
  env <- new.env(parent = globalenv())
  fixture <- imaging_feature_fixture(env)
  assign("imagingFeatureViewDS", dsImaging::imagingFeatureViewDS, env)
  assign("flowerInitDS", dsFlower::flowerInitDS, env)
  assign("flowerPrepareRunDS", dsFlower::flowerPrepareRunDS, env)
  assign("flowerEnsureSuperNodeDS", dsFlower::flowerEnsureSuperNodeDS, env)

  feature_reference <- evalq(
    imagingFeatureViewDS("img", asset_id), envir = env)
  assign("features", feature_reference, envir = env)
  flower_reference <- evalq(flowerInitDS("features"), envir = env)
  assign("flower", flower_reference, envir = env)
  withr::defer(evalq(dsFlower:::.removeHandle("flower"), envir = env))

  bad_config <- list(
    "num-server-rounds" = 1L, "num-features" = 1L,
    "num-classes" = 2L, "loss-name" = "bce_logits",
    "target-levels" = c("wrong", "levels"))
  assign("bad_config", bad_config, envir = env)
  expect_error(
    evalq(flowerPrepareRunDS(
      "flower", "diagnosis", "radiomics_mean", bad_config), envir = env),
    "do not match.*label_levels")
  expect_null(evalq(dsFlower:::.getHandle("flower")$run_token, envir = env))

  config <- bad_config
  config[["target-levels"]] <- c("case", "control")
  assign("config", config, envir = env)
  expect_no_error(evalq(flowerPrepareRunDS(
    "flower", "diagnosis", "radiomics_mean", config), envir = env))
  testthat::local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .compute_harness_hash = function() strrep("a", 64L),
    .supernode_ensure = function(...) list(process = NULL),
    .package = "dsFlower")
  expect_no_error(evalq(flowerEnsureSuperNodeDS(
    "flower", "ignored.example:9092", "imaging-feature-test"), envir = env))
  prepared <- evalq(dsFlower:::.getHandle("flower"), envir = env)
  expect_true(prepared$node_ensured)
  manifest <- jsonlite::fromJSON(
    file.path(prepared$staging_dir, "manifest.json"), simplifyVector = FALSE)
  staged <- dsFlower:::.readStagedSamples(file.path(
    prepared$staging_dir, manifest$data_file))

  expect_identical(manifest$data_type, "tabular")
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$patient_column, "patient_id")
  expect_identical(manifest$n_units, 3L)
  expect_identical(unlist(manifest$feature_columns), "radiomics_mean")
  expect_setequal(staged$sample_id, fixture$rows$sample_id)
  expect_identical(
    staged$patient_id[match(fixture$rows$sample_id, staged$sample_id)],
    fixture$rows$patient_id)
})

test_that("externally linked clinical data prepare a patient-DP logreg study", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not("clinical_symbol" %in% names(formals(
    dsImaging::imagingFeatureViewDS)))
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    dsflower.nfilter.subset = 3L,
    dsflower.dp_unit = "row",
    dsflower.patient_column = "wrong_global_column"))
  env <- new.env(parent = globalenv())
  fixture <- imaging_feature_fixture(env, label_col = NULL)
  clinical <- data.frame(
    patient_id = c("patient2", "patient1", "patient3"),
    age = c(59, 48, 67),
    bmi = c(26.5, 28.0, 24.5),
    outcome = c("case", "control", "case"),
    stringsAsFactors = FALSE)
  assign("clinical", clinical, envir = env)
  assign("clinical_columns", dsImaging:::.dsr_encode(c("age", "bmi")),
         envir = env)
  assign("target_levels", dsImaging:::.dsr_encode(c("control", "case")),
         envir = env)
  assign("imagingFeatureViewDS", dsImaging::imagingFeatureViewDS, env)
  assign("flowerInitDS", dsFlower::flowerInitDS, env)
  assign("flowerPrepareRunDS", dsFlower::flowerPrepareRunDS, env)

  feature_reference <- evalq(imagingFeatureViewDS(
    "img", asset_id, columns = "radiomics_mean",
    clinical_symbol = "clinical", clinical_id_col = "patient_id",
    clinical_columns = clinical_columns, target_col = "outcome",
    target_levels = target_levels), envir = env)
  assign("study", feature_reference, envir = env)
  flower_reference <- evalq(flowerInitDS("study"), envir = env)
  assign("flower", flower_reference, envir = env)
  withr::defer(evalq(dsFlower:::.removeHandle("flower"), envir = env))

  model_spec <- list(
    kind = "sequential",
    layers = list(list(op = "linear", out = "@out")))
  model_spec_b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(
    as.character(jsonlite::toJSON(
      model_spec, auto_unbox = TRUE, null = "null")))))
  config <- list(
    "dp-track" = "neural", "task-type" = "classification",
    "num-server-rounds" = 1L, "num-features" = 3L,
    "num-classes" = 2L, "num-labels" = 2L,
    "model-spec-b64" = model_spec_b64, "loss-name" = "bce_logits",
    "target-levels" = c("control", "case"))
  assign("config", config, envir = env)
  assign("bad_features", dsImaging:::.dsr_encode(c(
    "radiomics_mean", "age", "patient_id")), envir = env)
  assign("study_features", dsImaging:::.dsr_encode(c(
    "radiomics_mean", "age", "bmi")), envir = env)

  expect_error(evalq(flowerPrepareRunDS(
    "flower", "outcome", bad_features, config), envir = env),
    "sample and patient identifiers")
  expect_null(evalq(dsFlower:::.getHandle("flower")$run_token, envir = env))

  expect_no_error(evalq(flowerPrepareRunDS(
    "flower", "outcome", study_features, config), envir = env))
  prepared <- evalq(dsFlower:::.getHandle("flower"), envir = env)
  manifest <- jsonlite::fromJSON(
    file.path(prepared$staging_dir, "manifest.json"), simplifyVector = FALSE)
  staged <- dsFlower:::.readStagedSamples(file.path(
    prepared$staging_dir, manifest$data_file))

  expect_identical(manifest$data_type, "tabular")
  expect_identical(manifest$target_column, "outcome")
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$patient_column, "patient_id")
  expect_identical(manifest$n_samples, 4L)
  expect_identical(manifest$n_units, 3L)
  expect_identical(
    unlist(manifest$feature_columns, use.names = FALSE),
    c("radiomics_mean", "age", "bmi"))
  expect_identical(manifest[["loss-name"]], "bce_logits")
  expect_identical(manifest[["model-spec-b64"]], model_spec_b64)
  expect_identical(
    jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(
      manifest[["model-spec-b64"]])), simplifyVector = FALSE),
    model_spec)

  expected_clinical <- clinical[match(
    fixture$rows$patient_id, clinical$patient_id), , drop = FALSE]
  staged_index <- match(fixture$rows$sample_id, staged$sample_id)
  expect_false(anyNA(staged_index))
  expect_identical(staged$patient_id[staged_index], fixture$rows$patient_id)
  expect_equal(staged$radiomics_mean[staged_index], c(1.5, 2.5, 3.5, 4.5))
  expect_equal(staged$age[staged_index], expected_clinical$age)
  expect_equal(staged$bmi[staged_index], expected_clinical$bmi)
  expect_identical(
    as.integer(staged$outcome[staged_index]),
    match(expected_clinical$outcome, c("control", "case")) - 1L)
})

test_that("imaging association uses the manifest patient privacy unit", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(
    "imagingFeatureViewDS", envir = asNamespace("dsImaging"),
    inherits = FALSE))
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    dsflower.nfilter.subset = 3L,
    dsflower.dp_unit = "row",
    dsflower.patient_column = "wrong_global_column"))
  testthat::local_mocked_bindings(
    .association_runtime_probe = function(...) TRUE,
    .package = "dsFlower")
  env <- new.env(parent = globalenv())
  imaging_feature_fixture(env)
  assign("imagingFeatureViewDS", dsImaging::imagingFeatureViewDS, env)
  assign("flowerInitDS", dsFlower::flowerInitDS, env)
  assign("flowerPrepareRunDS", dsFlower::flowerPrepareRunDS, env)

  feature_reference <- evalq(
    imagingFeatureViewDS("img", asset_id), envir = env)
  assign("features", feature_reference, envir = env)
  flower_reference <- evalq(flowerInitDS("features"), envir = env)
  assign("flower", flower_reference, envir = env)
  withr::defer(evalq(dsFlower:::.removeHandle("flower"), envir = env))

  status <- evalq(dsFlower::flowerStatusDS("flower"), envir = env)
  expect_identical(status$privacy_unit, "patient")

  contract_sha <- dsFlower:::.association_contract_sha256(
    "diagnosis", "radiomics_mean", c("case", "control"),
    c(1.5, 2.5), "patient")
  config <- list(
    "dp-track" = "association",
    "num-server-rounds" = 1L,
    "association-outcome-levels" = c("case", "control"),
    "association-exposure-levels" = c(1.5, 2.5),
    "association-contract-sha256" = contract_sha,
    "association-n-nodes" = 1L,
    "association-job-sha256" = dsFlower:::.association_job_sha256(
      contract_sha, 3L, dsFlower:::.compute_harness_hash(), 1L))
  assign("config", config, envir = env)

  expect_no_error(evalq(flowerPrepareRunDS(
    "flower", "diagnosis", "radiomics_mean", config), envir = env))
  prepared <- evalq(dsFlower:::.getHandle("flower"), envir = env)
  manifest <- jsonlite::fromJSON(
    file.path(prepared$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest[["association-privacy-unit"]], "patient")
  expect_identical(
    manifest[["association-unit-semantics"]], "patient-ever-positive/v1")
  expect_identical(manifest$patient_column, "patient_id")
  expect_identical(manifest$n_units, 3L)
  expect_identical(
    manifest[["association-contract-sha256"]], contract_sha)
  expect_identical(
    manifest[["association-job-sha256"]],
    config[["association-job-sha256"]])
})

test_that("naked imaging tables taint full, subset, copy, and rebound symbols", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(
    ".imaging_session_exported_feature_table",
    envir = asNamespace("dsImaging"), inherits = FALSE))
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    dsflower.nfilter.subset = 3L))
  env <- new.env(parent = globalenv())
  imaging_feature_fixture(env)
  assign("imagingLoadAssetDS", dsImaging::imagingLoadAssetDS, env)
  assign("imagingFeatureViewDS", dsImaging::imagingFeatureViewDS, env)
  assign("imagingDestroyDS", dsImaging::imagingDestroyDS, env)
  assign("flowerInitDS", dsFlower::flowerInitDS, env)
  raw <- evalq(imagingLoadAssetDS("img", asset_id), envir = env)
  assign("full", raw, env)
  assign("subset", raw[1:3, , drop = FALSE], env)
  assign("copy", raw, env)
  assign("matrix", as.matrix(raw), env)
  assign("rebound", raw, env)
  assign("rebound", raw[2:4, , drop = FALSE], env)

  for (symbol in c("full", "subset", "copy", "matrix", "rebound")) {
    assign("candidate", get(symbol, envir = env), envir = env)
    expect_error(
      evalq(flowerInitDS("candidate"), envir = env),
      "exported a naked imaging feature table")
  }
  safe_reference <- evalq(
    imagingFeatureViewDS("img", asset_id), envir = env)
  assign("safe_features", safe_reference, envir = env)
  safe_flower <- evalq(flowerInitDS("safe_features"), envir = env)
  assign("safe_flower", safe_flower, envir = env)
  expect_no_error(evalq(dsFlower:::.removeHandle("safe_flower"), envir = env))

  rm(list = c("full", "copy", "rebound"), envir = env)
  evalq(imagingDestroyDS("img"), envir = env)
  expect_error(
    evalq(flowerInitDS("subset"), envir = env),
    "exported a naked imaging feature table")

  clean <- new.env(parent = globalenv())
  assign("table", data.frame(x = 1:3, y = c(0, 1, 0)), clean)
  assign("flowerInitDS", dsFlower::flowerInitDS, clean)
  clean_reference <- evalq(flowerInitDS("table"), envir = clean)
  assign("flower", clean_reference, envir = clean)
  expect_no_error(evalq(dsFlower:::.removeHandle("flower"), envir = clean))
})

test_that("missing dsImaging safety hooks fail with a stable public error", {
  skip_if_not_installed("dsImaging")
  expect_error(
    dsFlower:::.dsImagingSafetyHook(".missing_test_safety_hook"),
    "installed dsImaging version does not provide.*safety contract")
})
