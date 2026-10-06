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

test_that("exact exported imaging frames and Arrow tables retain patient admission", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(".register_imaging_feature_table_export",
    envir = asNamespace("dsImaging"), inherits = FALSE))
  withr::local_options(list(dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L, dsflower.nfilter.subset = 3L,
    dsflower.dp_unit = "row"))
  env <- new.env(parent = globalenv())
  fixture <- imaging_feature_fixture(env)
  assign("imagingLoadAssetDS", dsImaging::imagingLoadAssetDS, env)
  assign("flowerInitDS", dsFlower::flowerInitDS, env)
  raw <- evalq(imagingLoadAssetDS("img", asset_id,
    include_metadata = TRUE), env)
  assign("rad", raw, env)
  parquet <- tempfile(fileext = ".parquet")
  arrow::write_parquet(raw, parquet)
  assign("rad_arrow", arrow::read_parquet(parquet, as_data_frame = FALSE), env)
  for (symbol in c("rad", "rad_arrow")) {
    reference <- eval(substitute(flowerInitDS(S), list(S = symbol)), env)
    assign("flower", reference, env)
    handle <- evalq(dsFlower:::.getHandle("flower"), env)
    expect_identical(handle$source_kind, "imaging_feature_view")
    expect_identical(dsFlower:::.imagingPrivacyUnitPolicy(handle$descriptor)$dp_unit,
                     "patient")
    authorized <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
      symbol, handle$imaging_feature_view_capability, env)
    expect_identical(authorized$privacy_roster$privacy_unit_count, 3L)
    expect_identical(nrow(authorized$data), 4L)
    expect_identical(authorized$data$patient_id, fixture$rows$patient_id)
    evalq(dsFlower:::.removeHandle("flower"), env)
  }
  changed <- raw
  changed$radiomics_mean[[1L]] <- changed$radiomics_mean[[1L]] + 1
  for (variant in list(raw[4:1, ], raw[-1L, ], changed)) {
    parquet <- tempfile(fileext = ".parquet")
    arrow::write_parquet(variant, parquet)
    for (candidate in list(variant, arrow::Table$create(variant),
                            arrow::RecordBatch$create(variant),
                            arrow::read_parquet(parquet, as_data_frame = FALSE))) {
      assign("bad", candidate, env)
      expect_error(evalq(flowerInitDS("bad"), env),
                   "only an unchanged admitted export")
    }
  }
  # Copying attributes or copying data to another session does not grant a view.
  other <- new.env(parent = globalenv())
  assign("rad", raw, other)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", owner_env = other), "Unknown, stale, or cross-session")
  evalq(dsImaging::imagingDestroyDS("img"), env)
  expect_error(evalq(flowerInitDS("rad"), env),
               "only an unchanged admitted export")
})

test_that("identical values from distinct imaging authorities are ambiguous", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(".register_imaging_feature_table_export",
    envir=asNamespace("dsImaging"),inherits=FALSE))
  withr::local_options(list(dsimaging.asset_db=tempfile(fileext=".sqlite"),
    dsimaging.nfilter.subset=3L))
  env <- new.env(parent=globalenv())
  imaging_feature_fixture(env)
  raw <- evalq(dsImaging::imagingLoadAssetDS("img", asset_id,
    include_metadata=TRUE), env)
  assign("rad", raw, env)
  state <- dsImaging:::.imaging_session_state(env)
  before <- length(ls(state$feature_views))
  # Reloading the same immutable authority is idempotent.
  evalq(dsImaging::imagingLoadAssetDS("img", asset_id,
    include_metadata=TRUE), env)
  expect_identical(length(ls(state$feature_views)), before)
  # A custodian creates another admitted authority with equal public values.
  # Even before that authority is resolved, no arbitrary match may be selected.
  entries <- state$feature_views
  original <- entries[[ls(entries)[[1]]]]
  original$source_handle_capability <- paste0("imgh_",strrep("b",64))
  entries[[paste0("imgf_",strrep("c",64))]] <- original
  expect_error(evalq(dsFlower::flowerInitDS("rad"),env),
    "only an unchanged admitted export")
})

test_that("whole-table name repair cannot replace an imaging structural role", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(".register_imaging_feature_table_export",
    envir=asNamespace("dsImaging"),inherits=FALSE))
  withr::local_options(list(dsimaging.asset_db=tempfile(fileext=".sqlite"),
    dsimaging.nfilter.subset=3L))
  env <- new.env(parent=globalenv())
  imaging_feature_fixture(env)
  auth <- dsImaging:::.authorized_imaging_dataset("img",owner_env=env)
  raw <- evalq(dsImaging::imagingLoadAssetDS("img", asset_id,
    include_metadata=TRUE),env)
  # A feature precedes the declared label and consumes its repaired name.
  raw[["diag-nosis"]] <- 1:4
  raw <- raw[c("sample_id","diag-nosis","diagnosis")]
  names(raw)[names(raw)=="diagnosis"] <- "diag.nosis"
  auth$privacy$label_col <- "diag.nosis"
  auth$manifest$metadata$label_col <- "diag.nosis"
  original_names <- names(raw)
  names(raw) <- make.names(names(raw),unique=TRUE)
  # R prioritizes already-syntactic names, preserving the actual target.
  expect_identical(names(raw),c("sample_id","diag.nosis.1","diag.nosis"))
  expect_type(dsImaging:::.register_imaging_feature_table_export(
    raw,auth,"img",env,TRUE,original_names), "character")
  # Column selection can drop the real target before repair: the remaining
  # feature must not be promoted into the missing target role.
  selected <- raw[c("sample_id","diag.nosis.1")]
  original_selected <- c("sample_id","diag-nosis")
  names(selected) <- make.names(original_selected,unique=TRUE)
  expect_identical(names(selected),c("sample_id","diag.nosis"))
  expect_null(dsImaging:::.register_imaging_feature_table_export(
    selected,auth,"img",env,TRUE,original_selected))
  # Also reject a conflicting structural position from any repaired-name path.
  names(raw) <- c("sample_id","diag.nosis","diag.nosis.1")
  expect_null(dsImaging:::.register_imaging_feature_table_export(
    raw,auth,"img",env,TRUE,original_names))
})

test_that("radiomics workflow loader registers the same admitted table contract", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  skip_if_not(exists(".register_imaging_feature_table_export",
    envir=asNamespace("dsImaging"),inherits=FALSE))
  withr::local_options(list(dsimaging.asset_db=tempfile(fileext=".sqlite"),
    dsimaging.nfilter.subset=3L))
  env <- new.env(parent=globalenv())
  fixture <- imaging_feature_fixture(env)
  request <- dsImaging:::.dsr_encode(list(handle="img", asset_id=fixture$asset_id,
    include_metadata=TRUE))
  rad <- eval(substitute(dsImaging::imagingLoadRadiomicsFeaturesDS(REQUEST),
    list(REQUEST=request)),env)
  assign("rad",rad,env)
  reference <- evalq(dsFlower::flowerInitDS("rad"),env)
  assign("flower",reference,env)
  handle <- evalq(dsFlower:::.getHandle("flower"),env)
  expect_identical(handle$source_kind,"imaging_feature_view")
  expect_identical(dsFlower:::.imagingPrivacyUnitPolicy(handle$descriptor)$dp_unit,
                   "patient")
})


test_that("legacy companion exports fail closed without table admission support", {
  local_feature_view_privacy_state()
  skip_if_not_installed("dsImaging")
  if (exists(".register_imaging_feature_table_export", envir = asNamespace("dsImaging"),
             inherits = FALSE)) skip("installed companion supports admitted table exports")
  withr::local_options(list(dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L, dsflower.nfilter.subset = 3L))
  env <- new.env(parent = globalenv())
  fixture <- imaging_feature_fixture(env)
  raw <- evalq(dsImaging::imagingLoadAssetDS("img", asset_id, include_metadata = TRUE), env)
  assign("rad", raw, env)
  expect_error(evalq(dsFlower::flowerInitDS("rad"), env),
               "only an unchanged admitted export")
})


test_that("missing companion cannot provide an imaging authorization", {
  if (requireNamespace("dsImaging", quietly = TRUE))
    skip("installed companion is covered by the admitted export tests")
  expect_error(dsFlower:::.dsImagingSafetyHook(
    ".resolve_imaging_feature_view_for_consumer"),
    "Package 'dsImaging' is required for this imaging safety contract", fixed = TRUE)
})
