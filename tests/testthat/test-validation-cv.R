valcv_resampling <- function(config, method) {
  if (identical(method, "holdout")) {
    c <- dsFlower:::.holdoutContract(200000L, "patient")
    keys <- c("version", "method", "assignment", "test_numerator", "test_denominator",
              "privacy_unit", "unit_canonicalization", "sha256")
    wire <- c("version", "method", "assignment", "test-numerator", "test-denominator",
              "privacy-unit", "unit-canonicalization", "contract-sha256")
    for (i in seq_along(keys)) config[[paste0("resampling-", wire[[i]])]] <- c[[keys[[i]]]]
    config[["holdout-validation-bins"]] <- 32L
  } else {
    c <- dsFlower:::.crossValidationContract(2L, "patient")
    keys <- c("version", "method", "assignment", "folds", "privacy_unit",
              "unit_canonicalization", "sha256")
    wire <- c("version", "method", "assignment", "folds", "privacy-unit",
              "unit-canonicalization", "contract-sha256")
    for (i in seq_along(keys)) config[[paste0("cv-", wire[[i]])]] <- c[[keys[[i]]]]
    config[["cv-validation-bins"]] <- 32L
    config[["cv-n-nodes"]] <- 2L
    config[["cv-job-sha256"]] <- strrep("a", 64)
  }
  config
}

valcv_survival <- function() {
  value <- list(schema_version = 1L, time_unit = "days", time_origin = "baseline",
                t_min = 1, horizon = 10, time_scale = 1, distribution = "weibull",
                dispersion = 1)
  list("dp-track" = "neural", "task-type" = "survival", "num-features" = 1L,
       "loss-name" = "aft_weibull_nll", "model-spec-b64" = "e30=",
       "survival-config-b64" = gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(
         as.character(jsonlite::toJSON(value, auto_unbox = TRUE, digits = I(17)))))))
}

valcv_fixed <- function(config, task) {
  config[["dp-track"]] <- "validation"
  config[["validation-model-track"]] <- "neural"
  config[["validation-task"]] <- task
  config[["validation-bins"]] <- 32L
  if (identical(task, "segmentation")) {
    config[["validation-artifact-format"]] <- "pytorch-state-dict-v1"
    config[["validation-artifact-sha256"]] <- strrep("a", 64)
    config[["validation-artifact-size-bytes"]] <- 100L
    config[["model-spec-b64"]] <- "e30="
  }
  config
}

test_that("segmentation and survival fixed metrics admit all three private tracks", {
  local_segmentation_roots()
  for (task in c("segmentation", "survival")) {
    config <- if (task == "segmentation") segmentation_config() else valcv_survival()
    for (track in c("validation", "holdout", "cv")) {
      request <- if (track == "validation") valcv_fixed(config, task) else
        valcv_resampling(config, track)
      accepted <- dsFlower:::.addDpConfigToRunConfig(request)
      expect_identical(accepted[["task-type"]], task)
      expect_identical(accepted[["num-labels"]], 2L)
      expect_true(accepted$dp_enabled)
      expect_false(accepted$allow_per_node_metrics)
      expect_false(accepted$allow_exact_num_examples)
      if (task == "survival") {
        expect_identical(accepted[["validation-survival-horizons"]], "[10]")
        expect_identical(accepted[["validation-survival-nll-bound"]], 20)
      }
      if (track == "cv") {
        expect_equal(accepted[["privacy-cv-fold-epsilon"]],
                     accepted[["privacy-epsilon"]] * 0.4)
        expect_equal(accepted[["privacy-cv-oof-epsilon"]],
                     accepted[["privacy-epsilon"]] * 0.2)
      } else if (track == "holdout") {
        expect_equal(accepted[["privacy-training-epsilon"]],
                     accepted[["privacy-epsilon"]] * 0.8)
      }
    }
  }
})

test_that("survival metric geometry is fixed before private staging", {
  local_segmentation_roots()
  request <- valcv_fixed(valcv_survival(), "survival")
  request[["validation-survival-horizons"]] <- "[2,5,10]"
  accepted <- dsFlower:::.addDpConfigToRunConfig(request)
  expect_identical(accepted[["validation-survival-horizons"]], "[2,5,10]")
  name <- "valcv_survival_guard"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    patient_id = "a", x = 0, time = 2, event = 1)))
  withr::defer(dsFlower:::.removeHandle(name))
  reached <- FALSE
  targets <- c("time", "event")
  local_mocked_bindings(.stageData = function(...) { reached <<- TRUE }, .package = "dsFlower")
  for (value in c("[]", "[0]", "[11]", "[5,2]", "[2,2]", "[true]", "2", "[NaN]")) {
    bad <- request
    bad[["validation-survival-horizons"]] <- value
    expect_error(flowerPrepareRunDS(name, targets, "x", bad), "horizons")
  }
  for (value in list(0, -1, Inf, TRUE, 1001)) {
    bad <- request
    bad[["validation-survival-nll-bound"]] <- value
    expect_error(flowerPrepareRunDS(name, targets, "x", bad), "NLL bound")
  }
  expect_false(reached)
  bad <- request
  bad[["validation-concordance"]] <- TRUE
  expect_error(dsFlower:::.addDpConfigToRunConfig(bad), "unsupported|Unknown|Unknown|not supported|unknown")
})

valcv_checkpoint_summary <- function() {
  list(snapshot_directory = "/private/snapshot", checkpoint_sha256 = strrep("b", 64),
    encoder_sha256 = digest::digest(raw(), "sha256", serialize = FALSE),
    identity_version = "dsflower-public-initialisation-identity/v1",
    tensor_schema = list(list(name = "arr_0", shape = list(1L), dtype = "float32",
                              sha256 = strrep("d", 64))),
    provenance = list(manifest_sha256 = strrep("a", 64), manifest = list(
      role = "tabular_model", model_id = "declarative_neural",
      model_config = list("loss-name" = "bce_logits", "num-features" = 1L,
                          "num-classes" = 2L, "num-labels" = 2L),
      feature_contract = list(features = list("x"), feature_lower = list(0),
        feature_upper = list(1), target_levels = list(0, 1), target_bounds = NULL))))
}

test_that("fixed analyst checkpoints are policy-gated and identity-bound before staging", {
  withr::local_options(list(dsflower.public_initialisation = "analyst_or_resource",
    dsflower.public_initialisation.declarative_neural = NULL,
    dsflower.dp_unit = "row"))
  owner <- new.env(parent = globalenv())
  snapshot <- valcv_checkpoint_summary()
  token <- paste0("cku_", strrep("a", 32))
  state <- dsFlower:::.checkpoint_state(owner, TRUE)
  state[[token]] <- list(origin = "analyst-declared", created = Sys.time(), snapshot = snapshot)
  request <- list("dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 32L,
    "loss-name" = "bce_logits", "num-features" = 1L, "model-spec-b64" = "e30=",
    "feature-bounds" = list(lower = 0, upper = 1), "target-levels" = c(0, 1),
    "segmentation-decoder-init" = paste0("client:", token))
  calls <- 0L
  local_mocked_bindings(.checkpoint_verify = function(...) {
    calls <<- calls + 1L
    snapshot
  }, .package = "dsFlower")
  for (policy in c("resource_only", "none")) {
    withr::with_options(list(dsflower.public_initialisation = policy), {
      expect_error(dsFlower:::.addDpConfigToRunConfig(request, owner_env = owner), "custodian policy")
    })
  }
  expect_identical(calls, 0L)
  accepted <- dsFlower:::.addDpConfigToRunConfig(request, owner_env = owner)
  expect_identical(accepted[["initialisation"]], "analyst-declared")
  expect_no_error(dsFlower:::.validatePreparedPublicCheckpoint(accepted, "x", "y"))
  expect_error(dsFlower:::.validatePreparedPublicCheckpoint(accepted, "changed", "y"), "geometry")
  other <- accepted
  other[["num-features"]] <- 2L
  expect_error(dsFlower:::.validatePreparedPublicCheckpoint(other, "x", "y"), "geometry")
  hash <- function(x) dsFlower:::.validationContractSha256(x, "x", "y", "row")
  changed <- accepted
  changed[["public-initialisation-checkpoint-sha256"]] <- strrep("f", 64)
  expect_false(identical(hash(accepted), hash(changed)))
  local_mocked_bindings(.checkpoint_verify = function(...) stop("digest mismatch"), .package = "dsFlower")
  expect_error(dsFlower:::.addDpConfigToRunConfig(request, owner_env = owner), "digest mismatch")
})

test_that("public initialisation changes CV job identity without changing folds", {
  withr::local_options(dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id")
  base <- valcv_resampling(valcv_survival(), "cv")
  base[["local-epochs"]] <- 1L
  base[["batch-size"]] <- 4L
  base[["optimizer-momentum"]] <- 0
  base[["optimizer-nesterov"]] <- FALSE
  first <- dsFlower:::.addDpConfigToRunConfig(base)
  second <- first
  second[["public-initialisation-origin"]] <- "analyst-declared"
  second[["public-initialisation-manifest-sha256"]] <- strrep("a", 64)
  second[["public-initialisation-checkpoint-sha256"]] <- strrep("b", 64)
  second[["public-initialisation-encoder-sha256"]] <- strrep("c", 64)
  second[["public-initialisation-identity-version"]] <- "dsflower-public-initialisation-identity/v1"
  expect_identical(first[["cv-contract-sha256"]], second[["cv-contract-sha256"]])
  hash <- function(config) dsFlower:::.cv_job_sha256(config, "x", c("time", "event"),
    3L, strrep("a", 64), strrep("b", 64), 1)
  expect_false(identical(hash(first), hash(second)))
})

test_that("generic bundle geometry and policy reject before private preparation", {
  withr::local_options(list(dsflower.public_initialisation = "analyst_or_resource",
    dsflower.public_initialisation.declarative_neural = NULL,
    dsflower.dp_unit = "row"))
  owner <- new.env(parent = globalenv())
  owner$TRAIN <- dsFlower:::.registerHandle(mock_handle(table_data = data.frame(x = 0, y = 1)), owner)
  snapshot <- valcv_checkpoint_summary()
  token <- paste0("cku_", strrep("b", 32))
  state <- dsFlower:::.checkpoint_state(owner, TRUE)
  state[[token]] <- list(origin = "analyst-declared", created = Sys.time(), snapshot = snapshot)
  owner$request <- list("dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 32L,
    "loss-name" = "bce_logits", "num-features" = 1L, "model-spec-b64" = "e30=",
    "feature-bounds" = list(lower = 0, upper = 1), "target-levels" = c(0, 1),
    "segmentation-decoder-init" = paste0("client:", token))
  reached <- FALSE
  local_mocked_bindings(.checkpoint_verify = function(...) snapshot,
    .stageData = function(...) { reached <<- TRUE }, .package = "dsFlower")
  expect_error(evalq(flowerPrepareRunDS("TRAIN", "y", "changed", request), owner), "geometry")
  for (policy in c("resource_only", "none")) {
    withr::with_options(list(dsflower.public_initialisation = policy), {
      expect_error(evalq(flowerPrepareRunDS("TRAIN", "y", "x", request), owner), "custodian policy")
    })
  }
  local_mocked_bindings(.checkpoint_verify = function(...) stop("digest mismatch"), .package = "dsFlower")
  expect_error(evalq(flowerPrepareRunDS("TRAIN", "y", "x", request), owner), "digest mismatch")
  expect_false(reached)
})

test_that("bundle policy follows the admitted profile and survival compares semantics", {
  snapshot <- valcv_checkpoint_summary()
  withr::local_options(list(dsflower.public_initialisation = "none",
    dsflower.public_initialisation.declarative_neural = "analyst_or_resource",
    dsflower.public_initialisation.pytorch_resnet18_segmentation = "resource_only"))
  expect_no_error(dsFlower:::.require_checkpoint_ingress_policy("analyst-declared"))
  expect_identical(dsFlower:::.checkpoint_snapshot_policy(snapshot, "analyst-declared"),
                   "analyst_or_resource")
  segmentation <- snapshot
  segmentation$provenance$manifest$role <- "segmentation_decoder"
  expect_error(dsFlower:::.checkpoint_snapshot_policy(segmentation, "analyst-declared"), "custodian policy")
  expect_identical(dsFlower:::.checkpoint_snapshot_policy(segmentation, "resource"), "resource_only")
  withr::local_options(dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id")
  config <- dsFlower:::.addDpConfigToRunConfig(valcv_survival())
  manifest <- snapshot$provenance$manifest
  manifest$model_config <- config[c("loss-name", "num-features", "num-classes", "num-labels")]
  decoded <- jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(config[["survival-config-b64"]])))
  manifest$model_config[["survival-config-b64"]] <- gsub("[\r\n]", "", jsonlite::base64_enc(
    charToRaw(as.character(jsonlite::toJSON(decoded[rev(names(decoded))], auto_unbox = TRUE, pretty = TRUE)))))
  manifest$feature_contract <- list(features = list("x"), feature_lower = NULL,
    feature_upper = NULL, target_levels = NULL, target_bounds = NULL)
  config[["public-initialisation-provenance"]] <- list(provenance = list(manifest = manifest))
  expect_no_error(dsFlower:::.validatePreparedPublicCheckpoint(config, "x", c("time", "event")))
  decoded$dispersion <- 2
  config[["survival-config-b64"]] <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(
    as.character(jsonlite::toJSON(decoded, auto_unbox = TRUE)))))
  expect_error(dsFlower:::.validatePreparedPublicCheckpoint(config, "x", c("time", "event")),
               "parametrisation")
})

test_that("segmentation identity survives JSON integer and double transport", {
  local_segmentation_roots()
  config <- dsFlower:::.addDpConfigToRunConfig(valcv_fixed(segmentation_config(), "segmentation"))
  config[["segmentation-alpha"]] <- 1
  transported <- jsonlite::fromJSON(as.character(jsonlite::toJSON(config,
    auto_unbox = TRUE, null = "null", digits = I(17))), simplifyVector = FALSE)
  hash <- function(value) dsFlower:::.validationContractSha256(value, NULL,
    "mask_path", "patient", data_kind = "image")
  expect_identical(hash(config), hash(transported))
})

test_that("generic checkpoint admission binds effective fitted loss parameters", {
  snapshot <- valcv_checkpoint_summary()
  manifest <- snapshot$provenance$manifest
  manifest$model_config[["loss-name"]] <- "quantile"
  manifest$feature_contract$target_levels <- NULL
  manifest$feature_contract$target_bounds <- list(lower = 0, upper = 5)
  config <- list("loss-name" = "quantile", "num-features" = 1L,
    "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = 0, upper = 1),
    "target-bounds" = list(lower = 0, upper = 5),
    "public-initialisation-provenance" = list(provenance = list(manifest = manifest)))
  expect_no_error(dsFlower:::.validatePreparedPublicCheckpoint(config, "x", "y"))
  config[["quantile-level"]] <- 0.25
  expect_error(dsFlower:::.validatePreparedPublicCheckpoint(config, "x", "y"), "loss parameter")
  config[["public-initialisation-provenance"]]$provenance$manifest$model_config[["quantile-level"]] <- 0.25
  expect_no_error(dsFlower:::.validatePreparedPublicCheckpoint(config, "x", "y"))
})
