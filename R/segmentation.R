# Public binary 2D segmentation contract. Patient policy and filesystem roots
# remain custodian-owned; record-local decoding and selection occur in Python.
.segmentationRequested <- function(run_config) {
  if (!is.list(run_config)) return(FALSE)
  matches <- function(key, expected) {
    value <- run_config[[key]]
    is.character(value) && length(value) == 1L && !is.na(value) &&
      identical(tolower(value), expected)
  }
  any(vapply(c("task-type", "task_type", "validation-task"),
             matches, logical(1), expected = "segmentation")) ||
    matches("loss-name", "segmentation_bce_dice")
}

.segmentationConfigFields <- function() {
  c("image_asset", "image_path_col", "mask_asset", "mask_path_col",
    "sample_id_col", "subject_id_col", "mask_empty_col", "mask-vocabulary",
    "segmentation-alpha", "segmentation-smooth", "segmentation-selection",
    "segmentation-checkpoint-sha256", "segmentation-output-shape",
    "segmentation-preprocessing", "segmentation-decoder-init")
}

.segmentationPublicCheckpointFields <- function() {
  c("public-initialisation-origin", "public-initialisation-manifest-sha256",
    "public-initialisation-checkpoint-sha256", "public-initialisation-provenance",
    "public-initialisation-directory", "public-initialisation-policy",
    "public-initialisation-encoder-sha256", "public-initialisation-identity-version",
    "initialisation", "segmentation-public-manifest-sha256",
    "segmentation-public-checkpoint-sha256", "segmentation-public-provenance")
}

.normalizeSegmentationDecoderInit <- function(run_config, owner_env = parent.frame()) {
  init <- run_config[["segmentation-decoder-init"]] %||% "random"
  if (!is.character(init) || length(init) != 1L || is.na(init) ||
      !(identical(init, "random") ||
        grepl("\\Aclient:cku_[0-9a-f]{32}\\z", init, perl = TRUE) ||
        grepl("\\Aresource:[A-Za-z][A-Za-z0-9_.]{0,127}\\z", init, perl = TRUE))) {
    stop("Segmentation decoder_init requires 'random', an admitted client upload, ",
         "or 'resource:<handle-symbol>'.", call. = FALSE)
  }
  if (identical(init, "random")) {
    run_config[["segmentation-decoder-init"]] <- NULL
    return(run_config)
  }
  origin <- if (startsWith(init, "client:")) "analyst-declared" else "resource"
  contract <- if (.segmentationRequested(run_config)) .CHECKPOINT_CONTRACT else "declarative_neural"
  policy <- .require_checkpoint_policy(origin, contract)
  if (identical(origin, "resource")) {
    snapshot <- .checkpoint_resolve(substring(init, 10L), owner_env)
  } else {
    state <- .checkpoint_state(owner_env)
    entry <- if (is.environment(state)) state[[substring(init, 8L)]] else NULL
    if (!is.list(entry) || !identical(entry$origin, "analyst-declared") ||
        is.null(entry$snapshot) ||
        difftime(Sys.time(), entry$created, units = "secs") > 3600) {
      stop("Public initialisation requires a completed same-session upload.", call. = FALSE)
    }
    snapshot <- entry$snapshot
  }
  verified <- .checkpoint_verify("verify", snapshot$snapshot_directory,
    snapshot$provenance$manifest_sha256, .checkpoint_decoder_spec(run_config))
  summary <- .checkpoint_public_summary(verified, origin)
  if (!.segmentationRequested(run_config) &&
      !identical(summary$provenance$manifest$role, "tabular_model")) {
    stop("Public checkpoint does not match the declarative tabular contract.", call. = FALSE)
  }
  run_config[["segmentation-decoder-init"]] <- if (identical(origin, "resource")) "resource" else "client"
  run_config[["public-initialisation-origin"]] <- origin
  run_config[["public-initialisation-manifest-sha256"]] <- summary$manifest_sha256
  run_config[["public-initialisation-checkpoint-sha256"]] <- summary$checkpoint_sha256
  run_config[["public-initialisation-encoder-sha256"]] <- summary$encoder_sha256
  run_config[["public-initialisation-identity-version"]] <- summary$identity_version
  run_config[["public-initialisation-provenance"]] <- summary[c("provenance",
    "checkpoint_sha256", "encoder_sha256", "tensor_schema", "identity_version")]
  run_config[["public-initialisation-directory"]] <- verified$snapshot_directory
  run_config[["public-initialisation-policy"]] <- policy
  run_config[["initialisation"]] <- if (identical(origin, "resource")) {
    paste0("resource:", summary$manifest_sha256)
  } else "analyst-declared"
  attr(run_config, "segmentation_public_initialization") <- summary
  run_config
}

.normalizeSegmentationConfig <- function(run_config, track, unit_policy = NULL,
                                         owner_env = parent.frame()) {
  fields <- intersect(names(run_config), .segmentationConfigFields())
  if (!.segmentationRequested(run_config)) {
    if (length(setdiff(fields, "segmentation-decoder-init"))) {
      stop("Segmentation fields require the segmentation contract.", call. = FALSE)
    }
    return(run_config)
  }
  if (!track %in% c("neural", "validation") ||
      !identical(run_config[["data_type"]], "image") ||
      !identical(run_config[["loss-name"]], "segmentation_bce_dice")) {
    stop("Segmentation requires neural image training or validation with segmentation_bce_dice.",
         call. = FALSE)
  }
  if (any(grepl("^hpo-", names(run_config)))) {
    stop("Private segmentation HPO is unsupported.",
         call. = FALSE)
  }
  unit <- .resolvePrivacyUnitPolicy(unit_policy)
  if (!identical(unit$dp_unit, "patient")) {
    stop("Segmentation requires the custodian patient privacy-unit policy.",
         call. = FALSE)
  }
  scalar <- function(key) {
    value <- run_config[[key]]
    if (!is.character(value) || length(value) != 1L || is.na(value) ||
        !nzchar(trimws(value))) {
      stop("Segmentation requires one public value for ", key, ".", call. = FALSE)
    }
    value
  }
  pins <- c(
    backbone = "resnet18_layer2",
    `vision-extractor-profile` = "resnet18_layer2_128_v1",
    `segmentation-selection` = "canonical-image-id-lexicographic-v1",
    `segmentation-output-shape` = "1,128,128",
    `segmentation-preprocessing` = "rgb_bilinear_imagenet_128_v1")
  for (key in names(pins)) {
    if (!identical(scalar(key), unname(pins[[key]]))) {
      stop("Segmentation public pin disagrees with its contract: ", key, ".",
           call. = FALSE)
    }
  }
  number <- function(key, allowed) {
    value <- unlist(run_config[[key]], use.names = FALSE)
    if (!is.numeric(value) || is.logical(value) || length(value) != 1L ||
        is.na(value) || !is.finite(value) || !value %in% allowed) {
      stop("Segmentation public value is outside its contract: ", key, ".",
           call. = FALSE)
    }
  }
  number("image-size", 128)
  number("num-features", 32768)
  number("num-classes", 2)
  number("segmentation-alpha", c(0.5, 1))
  number("segmentation-smooth", 1)
  if (!scalar("mask-vocabulary") %in% c("0,1", "0,255")) {
    stop("Segmentation mask-vocabulary must be '0,1' or '0,255'.", call. = FALSE)
  }
  if (!identical(scalar("segmentation-checkpoint-sha256"),
      "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec")) {
    stop("Segmentation requires the pinned checkpoint SHA-256.", call. = FALSE)
  }
  for (key in c("image_asset", "mask_asset", "image_path_col", "mask_path_col",
                "sample_id_col")) scalar(key)
  if (!identical(run_config$image_asset, "images") ||
      !grepl("^[A-Za-z][A-Za-z0-9_.-]{0,63}$", run_config$mask_asset) ||
      identical(run_config$mask_asset, "images")) {
    stop("Segmentation requires distinct declared image and mask asset aliases.",
         call. = FALSE)
  }
  if (!is.null(run_config$mask_empty_col)) scalar("mask_empty_col")
  if (!is.null(run_config$subject_id_col) &&
      !identical(scalar("subject_id_col"), unit$patient_column)) {
    stop("Segmentation subject_id_col must match the custodian patient column.",
         call. = FALSE)
  }
  roles <- c(run_config$image_path_col, run_config$mask_path_col,
             run_config$sample_id_col, run_config$mask_empty_col,
             unit$patient_column)
  if (anyDuplicated(roles)) {
    stop("Segmentation metadata roles must use distinct columns.", call. = FALSE)
  }
  if (length(intersect(names(run_config), c(
      "feature-bounds", "target-bounds", "target-levels"))) ||
      (!is.null(run_config[["num-labels"]]) &&
       !identical(as.numeric(run_config[["num-labels"]]), 2))) {
    stop("Segmentation does not accept scalar target or tabular feature contracts.",
         call. = FALSE)
  }
  # Pin the otherwise unused compatibility count for CV recipe equality.
  run_config[["num-labels"]] <- 2L
  .normalizeSegmentationDecoderInit(run_config, owner_env)
}

.validateSegmentationColumns <- function(run_config, target_column,
                                         feature_columns = NULL, desc = NULL) {
  if (!.segmentationRequested(run_config)) return(invisible(TRUE))
  if (!identical(as.character(target_column), run_config$mask_path_col) ||
      !is.null(feature_columns)) {
    stop("Segmentation target must be mask_path_col with no tabular features.",
         call. = FALSE)
  }
  if (!is.null(desc)) {
    if (!identical(desc$source_kind, "image_bundle")) {
      stop("Segmentation requires an image bundle descriptor.", call. = FALSE)
    }
    metadata <- desc$manifest$metadata
    if (!identical(metadata$id_col, run_config$sample_id_col)) {
      stop("Segmentation sample_id_col must match the imaging descriptor.",
           call. = FALSE)
    }
    image <- desc$assets[[run_config$image_asset]]
    mask <- desc$assets[[run_config$mask_asset]]
    if (!identical(image$type %||% image$kind, "image_root") ||
        !identical(mask$type %||% mask$kind, "mask_root") ||
        !identical(image$path_col %||% "relative_path", run_config$image_path_col) ||
        !identical(mask$path_col %||% "relative_path", run_config$mask_path_col)) {
      stop("Segmentation image and mask roles must match the descriptor assets.",
           call. = FALSE)
    }
  }
  invisible(TRUE)
}

.resolve_mask_data_root <- function() {
  root <- .dsf_option("mask_data_root", NULL)
  if (!is.character(root) || length(root) != 1L || is.na(root) || !nzchar(root)) {
    stop("dsflower.mask_data_root server option is not configured.", call. = FALSE)
  }
  if (!dir.exists(root)) {
    stop("The configured mask data root is unavailable.", call. = FALSE)
  }
  normalizePath(root, mustWork = TRUE)
}

.totalizeSegmentationPaths <- function(data, run_config, assets) {
  roles <- c(run_config$image_path_col, run_config$mask_path_col,
             run_config$sample_id_col, run_config$mask_empty_col)
  if (!all(roles %in% names(data))) {
    stop("Segmentation metadata is missing a declared role column.", call. = FALSE)
  }
  for (kind in c("image", "mask")) {
    column <- run_config[[paste0(kind, "_path_col")]]
    asset <- assets[[run_config[[paste0(kind, "_asset")]]]]
    if (is.null(asset$root)) {
      stop("Segmentation requires both declared asset roots.", call. = FALSE)
    }
    extensions <- if (identical(kind, "mask")) "png" else c("png", "jpg", "jpeg")
    original <- as.character(data[[column]])
    data[[column]] <- unname(vapply(original, function(path) {
      path <- .totalizeImageRecordAtRoot(path, asset$root)
      if (!tolower(tools::file_ext(path)) %in% extensions) {
        .DSFLOWER_INVALID_IMAGE_PATH
      } else path
    }, character(1)))
    if (identical(kind, "mask") && !is.null(run_config$mask_empty_col)) {
      declared <- data[[run_config$mask_empty_col]]
      empty <- !is.na(declared) & as.character(declared) %in% c("TRUE", "1")
      absent <- is.na(original) | !nzchar(trimws(original))
      data[[column]][empty & absent] <- "__dsflower_empty_mask__"
    }
  }
  data
}
