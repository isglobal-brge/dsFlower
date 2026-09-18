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
    "segmentation-preprocessing")
}

.normalizeSegmentationConfig <- function(run_config, track, unit_policy = NULL) {
  fields <- intersect(names(run_config), .segmentationConfigFields())
  if (!.segmentationRequested(run_config)) {
    if (length(fields)) {
      stop("Segmentation fields require the segmentation contract.", call. = FALSE)
    }
    return(run_config)
  }
  if (!identical(track, "neural") ||
      !identical(run_config[["data_type"]], "image") ||
      !identical(run_config[["loss-name"]], "segmentation_bce_dice")) {
    stop("Segmentation requires neural image training with segmentation_bce_dice.",
         call. = FALSE)
  }
  if (any(grepl("^(validation-|resampling-|holdout-|cv-|hpo-)",
                names(run_config)))) {
    stop("Private segmentation validation, holdout, CV and HPO are unsupported.",
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
      "feature-bounds", "target-bounds", "target-levels", "num-labels")))) {
    stop("Segmentation does not accept scalar target or tabular feature contracts.",
         call. = FALSE)
  }
  run_config
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
