segmentation_config <- function() {
  list(
    "dp-track" = "neural", "data_type" = "image", "task-type" = "segmentation",
    "loss-name" = "segmentation_bce_dice", "num-features" = 32768L,
    "num-classes" = 2L,
    "backbone" = "resnet18_layer2", "image-size" = 128L,
    "vision-extractor-profile" = "resnet18_layer2_128_v1",
    "segmentation-alpha" = 0.5, "segmentation-smooth" = 1,
    "segmentation-selection" = "canonical-image-id-lexicographic-v1",
    "segmentation-output-shape" = "1,128,128",
    "segmentation-preprocessing" = "rgb_bilinear_imagenet_128_v1",
    "segmentation-checkpoint-sha256" =
      "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec",
    "mask-vocabulary" = "0,255", image_asset = "images", mask_asset = "masks",
    image_path_col = "relative_path", mask_path_col = "mask_path",
    sample_id_col = "image_id", mask_empty_col = "mask_empty")
}

local_segmentation_roots <- function(.local_envir = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .local_envir)
  image <- file.path(root, "images")
  mask <- file.path(root, "masks")
  dir.create(image)
  dir.create(mask)
  writeBin(charToRaw("decoder verifies actual PNG bytes"), file.path(image, "a.png"))
  writeBin(charToRaw("decoder verifies actual PNG bytes"), file.path(mask, "a.png"))
  withr::local_options(list(
    dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id",
    dsflower.image_data_root = image, dsflower.mask_data_root = mask,
    dsflower.staging_root = root), .local_envir = .local_envir)
  list(root = root, image = image, mask = mask)
}

segmentation_table <- function() {
  data.frame(patient_id = c("patient-a", "patient-a", "patient-b", "patient-c"),
             image_id = c("image-2", "image-1", "image-3", "image-4"),
             relative_path = c("a.png", "a.png", "a.png", "../escape.png"),
             mask_path = c("a.png", "a.png", NA, "https://example.org/private.png"),
             mask_empty = c(FALSE, FALSE, TRUE, FALSE))
}

test_that("segmentation public pins require custodian patient policy", {
  withr::local_options(list(dsflower.dp_unit = "row"))
  config <- segmentation_config()
  expect_error(dsFlower:::.addDpConfigToRunConfig(config), "custodian patient")
  withr::local_options(list(
    dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id"))
  accepted <- dsFlower:::.addDpConfigToRunConfig(config)
  expect_identical(accepted[["task-type"]], "segmentation")
  expect_identical(accepted[["privacy-clipping_norm"]], 1)
  expect_true(accepted$dp_enabled)
  expect_false(accepted$allow_exact_num_examples)
  expect_false(accepted$allow_per_node_metrics)
  config$subject_id_col <- "alternate_patient"
  expect_error(dsFlower:::.addDpConfigToRunConfig(config), "custodian patient column")
  config$subject_id_col <- "patient_id"
  expect_no_error(dsFlower:::.addDpConfigToRunConfig(config))
})

test_that("segmentation authority rejects hostile pins and scalar target roles", {
  local_segmentation_roots()
  config <- segmentation_config()
  mutations <- list(
    "backbone" = "resnet18", "vision-extractor-profile" = "other",
    "image-size" = 64L, "num-features" = 512L, "segmentation-alpha" = 0.7,
    "segmentation-smooth" = 0, "segmentation-selection" = "largest-lesion",
    "segmentation-output-shape" = "1,256,256", "mask-vocabulary" = "0,128,255",
    "segmentation-preprocessing" = "adaptive",
    "segmentation-checkpoint-sha256" = strrep("a", 64L),
    "target-levels" = c(0L, 1L), "num-labels" = 16384L,
    "image_asset" = "../images", "mask_asset" = "images",
    "sample_id_col" = "patient_id")
  for (key in names(mutations)) {
    bad <- config
    bad[[key]] <- mutations[[key]]
    expect_error(dsFlower:::.addDpConfigToRunConfig(bad), "Segmentation")
  }
  for (key in c("privacy-epsilon", "privacy-delta", "privacy-clipping_norm",
                "dp-unit", "n_units", "patient_column", "assets")) {
    bad <- config
    bad[[key]] <- 1
    expect_error(dsFlower:::.addDpConfigToRunConfig(bad), "server-owned")
  }
  config[["segmentation-alpha"]] <- 1
  config[["mask-vocabulary"]] <- "0,1"
  expect_no_error(dsFlower:::.addDpConfigToRunConfig(config))
  expect_error(dsFlower:::.validateSegmentationColumns(config, "label"), "mask_path_col")
  expect_error(dsFlower:::.validateSegmentationColumns(
    config, "mask_path", "relative_path"), "no tabular features")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(mask_asset = "masks")),
               "require the segmentation contract")
})

test_that("new-task private evaluation rejects before private staging", {
  local_segmentation_roots()
  config <- segmentation_config()
  for (key in c("validation-bins", "resampling-method", "holdout-validation-bins",
                "cv-folds")) {
    bad <- config
    bad[[key]] <- 3L
    expect_error(dsFlower:::.addDpConfigToRunConfig(bad),
                 "Private segmentation validation, holdout, CV and HPO")
  }
  config[["dp-track"]] <- "validation"
  config[["validation-task"]] <- "segmentation"
  expect_error(dsFlower:::.addDpConfigToRunConfig(config), "neural image training")
})

test_that("direct segmentation retains source census and custodian mask roots", {
  roots <- local_segmentation_roots()
  config <- segmentation_config()
  config$data_type <- NULL
  token <- dsFlower:::.generate_run_token()
  staged <- dsFlower:::.stage_image_manifest(
    token, "mask_path", segmentation_table(), config)
  withr::defer(dsFlower:::.cleanupStaging(token))
  manifest <- jsonlite::fromJSON(file.path(staged, "manifest.json"))
  data <- dsFlower:::.readStagedSamples(file.path(staged, manifest$samples_file))
  expect_identical(manifest$n_samples, 4L)
  expect_identical(manifest$n_units, 3L)
  expect_identical(manifest$dropped_missing, 0L)
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$patient_column, "patient_id")
  expect_identical(manifest$assets$masks$root, normalizePath(roots$mask))
  expect_identical(manifest$assets$masks$type, "mask_root")
  expect_identical(manifest$assets$masks$path_col, "mask_path")
  expect_identical(data$mask_path,
    c("a.png", "a.png", "__dsflower_empty_mask__", "__dsflower_invalid_image__"))
  expect_identical(data$relative_path[[4]], "__dsflower_invalid_image__")
  expect_identical(data$image_id, segmentation_table()$image_id)
  expect_identical(data$mask_empty, segmentation_table()$mask_empty)
})

test_that("mask containment and explicit empty declarations totalize separately", {
  roots <- local_segmentation_roots()
  config <- segmentation_config()
  data <- data.frame(
    image_id = as.character(1:8), relative_path = rep("a.png", 8),
    mask_path = c(NA, "", "../escape.png", "https://example.org/a.png",
                  "missing.png", "not-a-mask.jpg", "a.png", NA),
    mask_empty = c(FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, NA))
  writeLines("mask", file.path(roots$mask, "not-a-mask.jpg"))
  assets <- list(images = list(root = roots$image), masks = list(root = roots$mask))
  actual <- dsFlower:::.totalizeSegmentationPaths(data, config, assets)
  expect_identical(actual$mask_path, c(
    "__dsflower_invalid_image__", "__dsflower_empty_mask__",
    rep("__dsflower_invalid_image__", 4), "a.png", "__dsflower_invalid_image__"))
  config$mask_empty_col <- NULL
  absent <- dsFlower:::.totalizeSegmentationPaths(data, config, assets)
  expect_identical(absent$mask_path[[2]], "__dsflower_invalid_image__")
  outside <- file.path(roots$root, "outside.png")
  writeLines("private external mask", outside)
  if (file.symlink(outside, file.path(roots$mask, "link.png"))) {
    data$mask_path[[1]] <- "link.png"
    linked <- dsFlower:::.totalizeSegmentationPaths(data, config, assets)
    expect_identical(linked$mask_path[[1]], "__dsflower_invalid_image__")
  }
})

test_that("descriptor segmentation preserves declared masks and patient authority", {
  roots <- local_segmentation_roots()
  config <- segmentation_config()
  metadata <- list(id_col = "image_id", privacy_unit = "patient",
                   privacy_unit_col = "patient_id",
                   privacy_unit_canonicalization = "trim-utf8-v2")
  desc <- list(source_kind = "image_bundle", dataset_id = "public-segmentation-test",
               metadata = metadata, manifest = list(metadata = metadata),
               table_data = segmentation_table(),
               assets = list(images = list(type = "image_root", root = roots$image,
                                           path_col = "relative_path"),
                             masks = list(type = "mask_root", root = roots$mask,
                                          path_col = "mask_path")))
  withr::local_options(list(dsflower.dp_unit = "row"))
  token <- dsFlower:::.generate_run_token()
  staged_config <- config
  staged_config$data_type <- NULL
  staged <- dsFlower:::.stageFromDescriptor_image(
    desc, token, "mask_path", NULL, staged_config)
  withr::defer(dsFlower:::.cleanupStaging(token))
  manifest <- jsonlite::fromJSON(file.path(staged, "manifest.json"))
  expect_identical(manifest[["dp-unit"]], "patient")
  expect_identical(manifest$n_samples, 4L)
  expect_identical(manifest$n_units, 3L)
  expect_identical(manifest$assets$masks$root, normalizePath(roots$mask))
  expect_no_error(dsFlower:::.addDpConfigToRunConfig(
    config, dsFlower:::.imagingPrivacyUnitPolicy(desc)))
  desc$assets$masks$path_col <- "label"
  expect_error(dsFlower:::.validateSegmentationColumns(config, "mask_path", NULL, desc),
               "descriptor assets")
  desc$manifest$metadata$id_col <- "wrong_id"
  expect_error(dsFlower:::.validateSegmentationColumns(config, "mask_path", NULL, desc),
               "sample_id_col")
})

test_that("failed S3 mask records preserve successful staging without partial masks", {
  skip_if_not_installed("dsImaging")
  root <- withr::local_tempdir()
  prefix <- "s3://fixture/masks/"
  testthat::local_mocked_bindings(
    backend_get_file = function(backend, uri, dest, ...) {
      writeLines("partial PNG", dest)
      if (endsWith(uri, "broken.png")) stop("private object unavailable")
      invisible(dest)
    }, .package = "dsImaging")
  actual <- dsFlower:::.downloadS3DirectoryAsset(
    list(), prefix, root, paste0(prefix, c("broken.png", "good.png")),
    totalize_records = TRUE)
  expect_identical(actual, "good.png")
  expect_false(file.exists(file.path(root, "broken.png")))
  expect_true(file.exists(file.path(root, "good.png")))
})

test_that("segmentation CSV routes preserve numeric-looking patient and image IDs", {
  roots <- local_segmentation_roots()
  config <- segmentation_config()
  config$data_type <- NULL
  data <- data.frame(patient_id = c("001", "01", "1"),
                     image_id = c("002", "02", "2"),
                     relative_path = rep("a.png", 3),
                     mask_path = rep("a.png", 3),
                     mask_empty = c("TRUE", "FALSE", "invalid"))
  source <- file.path(roots$root, "source.csv")
  utils::write.csv(data, source, row.names = FALSE)
  metadata <- list(file = source, id_col = "image_id", privacy_unit = "patient",
                   privacy_unit_col = "patient_id",
                   privacy_unit_canonicalization = "trim-utf8-v2")
  desc <- list(source_kind = "image_bundle", dataset_id = "numeric-id-fixture",
               metadata = metadata, manifest = list(metadata = metadata),
               assets = list(images = list(type = "image_root", root = roots$image,
                                           path_col = "relative_path"),
                             masks = list(type = "mask_root", root = roots$mask,
                                          path_col = "mask_path")))
  for (route in c("direct", "descriptor")) {
    token <- dsFlower:::.generate_run_token()
    staged <- if (identical(route, "direct")) {
      dsFlower:::.stage_image_manifest(token, "mask_path", source, config)
    } else {
      dsFlower:::.stageFromDescriptor_image(desc, token, "mask_path", NULL, config)
    }
    manifest <- jsonlite::fromJSON(file.path(staged, "manifest.json"))
    actual <- dsFlower:::.readStagedSamples(
      file.path(staged, manifest$samples_file), preserve_strings = TRUE)
    expect_identical(manifest$n_samples, 3L)
    expect_identical(manifest$n_units, 3L)
    expect_identical(actual$patient_id, data$patient_id)
    expect_identical(actual$image_id, data$image_id)
    expect_identical(actual$mask_empty, data$mask_empty)
    expect_identical(actual$mask_path, data$mask_path)
    dsFlower:::.cleanupStaging(token)
  }
})

test_that("sealed imaging snapshots retain the declared segmentation mask asset", {
  skip_if_not_installed("dsImaging")
  roots <- local_segmentation_roots()
  config <- segmentation_config()
  config$data_type <- NULL
  config$mask_asset <- "lesion_masks"
  samples <- data.frame(patient_id = c("001", "01", "1"),
                        image_id = c("003", "03", "3"),
                        relative_path = rep("untrusted-metadata.png", 3),
                        mask_path = rep("a.png", 3), mask_empty = rep(FALSE, 3))
  metadata <- list(id_col = "image_id", privacy_unit = "patient",
                   privacy_unit_col = "patient_id",
                   privacy_unit_canonicalization = "trim-utf8-v2")
  snapshot <- list(
    records = lapply(samples$image_id, function(id) {
      list(sample_id = id, relative_path = "a.png", source_kind = "single_file",
           n_files = 1L, size = 1)
    }),
    artifacts = list(metadata = list(format = "csv"),
                     sample_manifests = list(format = "csv")))
  local_mocked_bindings(
    .copy_imaging_snapshot_artifact = function(snapshot, backend, key, destination) {
      data <- if (identical(key, "metadata")) samples else data.frame(
        sample_id = samples$image_id, primary_uri = rep("a.png", 3))
      utils::write.csv(data, destination, row.names = FALSE)
    },
    .materialize_imaging_snapshot = function(snapshot, backend, destination) {
      list(root = roots$image, relative_paths = "a.png")
    }, .package = "dsImaging")
  desc <- list(source_kind = "image_bundle", dataset_id = "sealed-segmentation",
               metadata = metadata, manifest = list(metadata = metadata),
               .collection_snapshot = snapshot,
               assets = list(images = list(type = "image_root", root = roots$image,
                                           path_col = "relative_path"),
                             lesion_masks = list(type = "mask_root", root = roots$mask,
                                                  path_col = "mask_path")))
  token <- dsFlower:::.generate_run_token()
  staged <- dsFlower:::.stageFromDescriptor_image(desc, token, "mask_path", NULL, config)
  withr::defer(dsFlower:::.cleanupStaging(token))
  manifest <- jsonlite::fromJSON(file.path(staged, "manifest.json"))
  actual <- dsFlower:::.readStagedSamples(
    file.path(staged, manifest$samples_file), preserve_strings = TRUE)
  expect_identical(manifest$n_units, 3L)
  expect_named(manifest$assets, c("images", "lesion_masks"))
  expect_identical(actual$image_id, samples$image_id)
  expect_identical(actual$patient_id, samples$patient_id)
  expect_identical(actual$relative_path, rep("a.png", 3))
  expect_identical(actual$mask_path, samples$mask_path)
})

test_that("direct prepare uses image routing only for the segmentation contract", {
  roots <- local_segmentation_roots()
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(roots$root, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"))
  dsFlower:::.setHandle("segmentation_direct", mock_handle(table_data = segmentation_table()))
  withr::defer(dsFlower:::.removeHandle("segmentation_direct"))
  config <- segmentation_config()
  result <- flowerPrepareRunDS("segmentation_direct", "mask_path", NULL,
                              config)
  expect_named(result, "capability")
  handle <- dsFlower:::.getHandle("segmentation_direct")
  withr::defer(dsFlower:::.cleanupStaging(handle$run_token))
  manifest <- jsonlite::fromJSON(file.path(handle$staging_dir, "manifest.json"))
  expect_identical(manifest$data_type, "image")
  expect_identical(manifest[["task-type"]], "segmentation")
  expect_identical(manifest$n_samples, 4L)
  expect_identical(manifest$n_units, 3L)
  prior_token <- handle$run_token
  bad <- segmentation_config()
  bad[["segmentation-alpha"]] <- 0
  expect_error(flowerPrepareRunDS("segmentation_direct", "mask_path", NULL, bad),
               "outside its contract")
  expect_identical(dsFlower:::.getHandle("segmentation_direct")$run_token, prior_token)
  withr::local_options(list(dsflower.mask_data_root = NULL))
  expect_error(flowerPrepareRunDS("segmentation_direct", "mask_path", NULL,
                                 config), "mask_data_root")
  expect_identical(dsFlower:::.getHandle("segmentation_direct")$run_token, prior_token)
})

segmentation_public_provenance <- function(id = "busi-test") {
  bytes <- charToRaw("verified public decoder bytes")
  list(manifest_sha256 = strrep("a", 64L), manifest = list(
    checkpoint_id = id, checkpoint = list(
      sha256 = digest::digest(bytes, algo = "sha256", serialize = FALSE),
      size_bytes = length(bytes)),
    dataset = list(name = "public BUSI fixture", publisher_md5 = NULL,
                   training_loss = 0.12345678901234566),
    licence = list(declaration = "CC0-1.0", scope = "public mirror declaration")))
}

segmentation_public_payload <- function(id = "busi-test") {
  list(provenance = segmentation_public_provenance(id),
       checkpoint_base64 = gsub("[\r\n]", "", jsonlite::base64_enc(
         charToRaw("verified public decoder bytes"))))
}

segmentation_public_config <- function() {
  config <- segmentation_config()
  config[["segmentation-decoder-init"]] <- "public:busi-test"
  config[["model-spec-b64"]] <- gsub("[\r\n]", "", jsonlite::base64_enc(
    charToRaw('{"kind":"sequential","layers":[]}')))
  config
}

test_that("public decoder admission requires a custodian manifest allowlist", {
  local_segmentation_roots()
  config <- segmentation_public_config()
  calls <- 0L
  local_mocked_bindings(.verifySegmentationPublicCheckpoint = function(
      run_config, checkpoint_id, manifest_sha256) {
    calls <<- calls + 1L
    expect_identical(checkpoint_id, "busi-test")
    expect_identical(manifest_sha256, strrep("a", 64L))
    segmentation_public_payload(checkpoint_id)
  }, .package = "dsFlower")
  withr::local_options(list(dsflower.segmentation_public_checkpoints = character()))
  expect_error(dsFlower:::.addDpConfigToRunConfig(config), "not allowlisted")
  expect_identical(calls, 0L)
  withr::local_options(list(dsflower.segmentation_public_checkpoints =
    c("busi-test" = strrep("a", 64L))))
  actual <- dsFlower:::.addDpConfigToRunConfig(config)
  expect_identical(calls, 1L)
  expect_identical(actual[["segmentation-public-manifest-sha256"]], strrep("a", 64L))
  expect_identical(actual[["segmentation-public-checkpoint-sha256"]],
                   segmentation_public_provenance()$manifest$checkpoint$sha256)
  expect_identical(actual[["segmentation-public-provenance"]],
                   segmentation_public_provenance())
  for (key in dsFlower:::.segmentationPublicCheckpointFields()) {
    forged <- config
    forged[[key]] <- actual[[key]]
    expect_error(dsFlower:::.addDpConfigToRunConfig(forged), "server-owned")
  }
  config[["segmentation-public-initialization-b64"]] <- "forged"
  expect_error(dsFlower:::.addDpConfigToRunConfig(config), "unsupported")
})

test_that("decoder IDs and custodian checkpoint policies fail closed", {
  local_segmentation_roots()
  config <- segmentation_public_config()
  local_mocked_bindings(.verifySegmentationPublicCheckpoint = function(...) {
    stop("must not reach verifier")
  }, .package = "dsFlower")
  invalid <- list("", "PUBLIC:busi-test", "public:../file", "public:/file",
                  "public:https://example.org/file", "public:", NA_character_,
                  "public:busi-test\n", "public:busi-test\r\n",
                  c("random", "public:busi-test"), 1, list("random"))
  for (value in invalid) {
    bad <- config
    bad[["segmentation-decoder-init"]] <- value
    expect_error(dsFlower:::.addDpConfigToRunConfig(bad), "decoder_init")
  }
  invalid_policies <- list(NULL, list("busi-test" = strrep("a", 64L)),
    strrep("a", 64L), c("../busi-test" = strrep("a", 64L)),
    c("busi-test" = "bad-hash"), c("busi-test" = NA_character_),
    c("busi-test\n" = strrep("a", 64L)),
    c("busi-test" = paste0(strrep("a", 64L), "\n")),
    stats::setNames(rep(strrep("a", 64L), 2), rep("busi-test", 2)))
  for (policy in invalid_policies) {
    withr::with_options(list(dsflower.segmentation_public_checkpoints = policy), {
      expect_error(dsFlower:::.addDpConfigToRunConfig(config),
                   if (is.null(policy)) "not allowlisted" else "named character vector")
    })
  }
})

test_that("random decoder default preserves the existing admission contract", {
  local_segmentation_roots()
  local_mocked_bindings(.verifySegmentationPublicCheckpoint = function(...) {
    stop("random must not read public checkpoint files")
  }, .package = "dsFlower")
  config <- segmentation_config()
  before <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["segmentation-decoder-init"]] <- "random"
  attr(config, "segmentation_public_initialization") <- list(untrusted = "value")
  withr::local_options(list(dsflower.segmentation_public_checkpoints = "malformed"))
  expect_identical(dsFlower:::.addDpConfigToRunConfig(config), before)
  expect_length(intersect(names(before), c("segmentation-decoder-init",
    dsFlower:::.segmentationPublicCheckpointFields())), 0L)
})

test_that("public checkpoint failures occur before private staging and run creation", {
  local_segmentation_roots()
  withr::local_options(list(dsflower.segmentation_public_checkpoints =
    c("busi-test" = strrep("a", 64L))))
  local_mocked_bindings(
    .verifySegmentationPublicCheckpoint = function(...) {
      stop("Public segmentation checkpoint verification failed before private staging.")
    },
    .generate_run_token = function(...) stop("private boundary reached"),
    .stage_image_manifest = function(...) stop("private boundary reached"),
    .package = "dsFlower")
  dsFlower:::.setHandle("segmentation_public_failure",
                        mock_handle(table_data = segmentation_table()))
  withr::defer(dsFlower:::.removeHandle("segmentation_public_failure"))
  config <- segmentation_public_config()
  expect_error(flowerPrepareRunDS("segmentation_public_failure", "mask_path", NULL,
                                 config),
               "verification failed before private staging")
  expect_null(dsFlower:::.getHandle("segmentation_public_failure")$run_token)
})

test_that("trusted public verification validates subprocess evidence and spec syntax", {
  roots <- local_segmentation_roots()
  config <- segmentation_public_config()
  verify <- function(config, result) {
    dsFlower:::.verifySegmentationPublicCheckpoint(
      config, "busi-test", strrep("a", 64L), registry_root = roots$root,
      runner_dir = roots$root, runtime = list(python = "trusted-python", venv_path = "trusted"),
      run_probe = function(command, args, env, error_on_status, timeout) {
        expect_identical(command, "trusted-python")
        expect_identical(args[1:2], c("-I", "-c"))
        expect_match(args[[3]], "initialization_payload", fixed = TRUE)
        expect_match(args[[3]], "prepare_encoder(cfg)", fixed = TRUE)
        expect_identical(args[[5]], roots$root)
        result
      })
  }
  payload <- segmentation_public_payload()
  result <- list(status = 0L, stdout = as.character(jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", digits = I(17))))
  expect_identical(verify(config, result), payload)
  expect_error(verify(config, list(status = 1L, stdout = "digest mismatch")),
               "verification failed before private staging")
  for (stdout in c("{}", "not JSON", gsub(strrep("a", 64L), strrep("c", 64L),
                                          result$stdout, fixed = TRUE))) {
    expect_error(verify(config, list(status = 0L, stdout = stdout)), "verification failed")
  }
  for (spec in list(NULL, "not-base64", strrep("a", 16385L))) {
    bad <- config
    bad[["model-spec-b64"]] <- spec
    expect_error(verify(bad, result), "decoder spec")
  }
  payload$checkpoint_base64 <- jsonlite::base64_enc(charToRaw("altered checkpoint"))
  expect_error(verify(config, list(status = 0L, stdout = as.character(jsonlite::toJSON(
    payload, auto_unbox = TRUE)))), "transport verification")
})

test_that("public decoder provenance is stored in the prepared run manifest", {
  roots <- local_segmentation_roots()
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(roots$root, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"))
  withr::local_options(list(dsflower.segmentation_public_checkpoints =
    c("busi-test" = strrep("a", 64L))))
  local_mocked_bindings(.verifySegmentationPublicCheckpoint = function(...) {
    segmentation_public_payload()
  }, .package = "dsFlower")
  dsFlower:::.setHandle("segmentation_public_manifest",
                        mock_handle(table_data = segmentation_table()))
  withr::defer(dsFlower:::.removeHandle("segmentation_public_manifest"))
  config <- segmentation_public_config()
  flowerPrepareRunDS("segmentation_public_manifest", "mask_path", NULL,
                     config)
  handle <- dsFlower:::.getHandle("segmentation_public_manifest")
  withr::defer(dsFlower:::.cleanupStaging(handle$run_token))
  manifest <- jsonlite::fromJSON(file.path(handle$staging_dir, "manifest.json"),
                                simplifyVector = FALSE)
  expect_identical(manifest[["segmentation-decoder-init"]], "public:busi-test")
  expect_identical(manifest[["segmentation-public-provenance"]],
                   segmentation_public_provenance())
  expect_match(paste(readLines(file.path(handle$staging_dir, "manifest.json")),
                     collapse = "\n"), '"publisher_md5": null', fixed = TRUE)
  expect_identical(manifest[["segmentation-public-manifest-sha256"]], strrep("a", 64L))
  expect_identical(manifest[["segmentation-public-checkpoint-sha256"]],
                   segmentation_public_provenance()$manifest$checkpoint$sha256)
  expect_equal(manifest[["privacy-epsilon"]], 1)
  expect_equal(manifest[["privacy-clipping_norm"]], 1)
  expect_null(manifest$segmentation_public_initialization)
  expect_identical(flowerStatusDS("segmentation_public_manifest")$segmentation_public_initialization,
                   segmentation_public_payload())
  flowerCleanupRunDS("segmentation_public_manifest")
  expect_null(flowerStatusDS("segmentation_public_manifest")$segmentation_public_initialization)
  config <- segmentation_config()
  flowerPrepareRunDS("segmentation_public_manifest", "mask_path", NULL, config)
  handle <- dsFlower:::.getHandle("segmentation_public_manifest")
  withr::defer(dsFlower:::.cleanupStaging(handle$run_token))
  expect_null(flowerStatusDS("segmentation_public_manifest")$segmentation_public_initialization)
})

test_that("public preflight and trusted runner resolve the same encoder cache", {
  python <- unname(Sys.which("python3"))
  skip_if(!nzchar(python), "Python interpreter is unavailable")
  roots <- local_segmentation_roots()
  runtime <- list(python = python, venv_path = dirname(dirname(python)))
  withr::local_envvar(c(TORCH_HOME = file.path(roots$root, "foreign-torch"),
                        PYTHONPATH = file.path(roots$root, "foreign-python"),
                        XDG_CACHE_HOME = file.path(roots$root, "public-cache")))
  local_mocked_bindings(
    .privacy_runtime_bootstrap = function() list(secret_path = "public-test-key"),
    .validate_node_secret = function(path) path, .package = "dsFlower")
  launch_env <- dsFlower:::.build_clean_python_env(runtime$venv_path, roots$root)
  probe <- paste(
    "import json, os",
    "root = os.path.expanduser(os.getenv('TORCH_HOME', os.path.join(os.getenv('XDG_CACHE_HOME', '~/.cache'), 'torch')))",
    "print(json.dumps({'hub': os.path.join(root, 'hub'), 'home': os.path.expanduser('~'), 'TORCH_HOME': os.getenv('TORCH_HOME')}))",
    sep = "\n")
  launch_cache <- processx::run(python, c("-I", "-c", probe), env = launch_env)$stdout
  payload <- segmentation_public_payload()
  actual <- dsFlower:::.verifySegmentationPublicCheckpoint(
    segmentation_public_config(), "busi-test", strrep("a", 64L),
    registry_root = roots$root, runner_dir = roots$root, runtime = runtime,
    run_probe = function(command, args, env, error_on_status, timeout) {
      expect_false("current" %in% env)
      expect_false(any(c("HOME", "USERPROFILE", "TORCH_HOME", "PYTHONPATH") %in% names(env)))
      expect_identical(env, launch_env[names(env)])
      preflight_cache <- processx::run(command, c("-I", "-c", probe), env = env)$stdout
      expect_identical(preflight_cache, launch_cache)
      cache <- jsonlite::fromJSON(preflight_cache)
      expect_null(cache$TORCH_HOME)
      expect_identical(cache$hub, file.path(Sys.getenv("XDG_CACHE_HOME"), "torch", "hub"))
      list(status = 0L, stdout = as.character(jsonlite::toJSON(
        payload, auto_unbox = TRUE, null = "null", digits = I(17))))
    })
  expect_identical(actual, payload)
})
