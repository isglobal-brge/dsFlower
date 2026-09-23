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

