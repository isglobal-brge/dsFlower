#!/usr/bin/env Rscript
# Run as the node service account after installing the reviewed dsFlower 0.6.0.
# This command reads recovered public bytes only; it never enables node policy.
install_segmentation_public_checkpoints <- function(checkpoint_source) {
  if (as.character(utils::packageVersion("dsFlower")) != "0.6.0") {
    stop("Install the reviewed dsFlower 0.6.0 package first.", call. = FALSE)
  }
  bundle <- system.file("extdata", "segmentation-public-checkpoints",
                        package = "dsFlower", mustWork = TRUE)
  pins <- unlist(jsonlite::fromJSON(file.path(bundle, "allowlist.json")),
                 use.names = TRUE)
  ids <- paste0("busi-v5-epochs60-seed", 20260919:20260921)
  stopifnot(is.character(pins), identical(sort(names(pins)), ids))
  checkpoint_paths <- stats::setNames(character(length(ids)), ids)
  artifact_paths <- stats::setNames(vector("list", length(ids)), ids)
  verify_file <- function(path, sha256, size_bytes = NULL) {
    if (!dsFlower:::.path_is_regular_file(path) ||
        dsFlower:::.privacy_path_is_link(path) || file.info(path)$size > 2 * 1024^2 ||
        (!is.null(size_bytes) && file.info(path)$size != size_bytes) ||
        !identical(digest::digest(path, algo = "sha256", file = TRUE), sha256)) {
      stop("Public artifact missing or SHA-256/size mismatch: ", path,
           call. = FALSE)
    }
  }

  # Complete the original-byte and evidence preflight before any state writes.
  for (id in ids) {
    directory <- file.path(bundle, id)
    manifest_path <- file.path(directory, "manifest.json")
    verify_file(manifest_path, unname(pins[[id]]))
    manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
    stopifnot(identical(manifest$checkpoint_id, id),
              identical(manifest$decoder, "narrow"),
              identical(manifest$checkpoint$file, "checkpoint.npz"))
    evidence_paths <- vapply(manifest$evidence, function(record) {
      path <- file.path(directory, record$file)
      verify_file(path, record$sha256, record$size_bytes)
      path
    }, character(1))
    original <- jsonlite::fromJSON(file.path(directory, "original-manifest.json"))
    stopifnot(identical(id, paste0("busi-v5-epochs60-seed", original$seed)))
    checkpoint <- file.path(checkpoint_source, paste0("seed", original$seed, ".npz"))
    verify_file(checkpoint, manifest$checkpoint$sha256, manifest$checkpoint$size_bytes)
    checkpoint_paths[[id]] <- checkpoint
    artifact_paths[[id]] <- c(manifest_path, unname(evidence_paths))
  }

  secret <- dsFlower:::.node_secret_path()
  dsFlower:::.validate_node_secret(secret)
  parent <- dirname(secret)
  registry <- file.path(parent, "segmentation-public-checkpoints")
  if (dsFlower:::.privacy_path_is_link(registry) ||
      (file.exists(registry) && !dir.exists(registry))) {
    stop("The registry must be a protected real directory.", call. = FALSE)
  }
  if (dir.exists(registry)) {
    dsFlower:::.validate_node_secret_parent(file.path(registry, "checkpoint"))
  }
  destinations <- file.path(registry, ids)
  if (any(file.exists(destinations)) ||
      any(vapply(destinations, dsFlower:::.privacy_path_is_link, logical(1)))) {
    stop("A checkpoint destination already exists; refusing to overwrite.", call. = FALSE)
  }
  runtime <- dsFlower:::.resolve_framework_runtime("pytorch")
  # The runner also rejects extended ACLs that Unix mode bits cannot represent.
  protect_probe <- paste(
    "import os, sys",
    "sys.path.insert(0, sys.argv[1])",
    "from dsflower_runner.segmentation_checkpoints import _protected",
    "_protected(sys.argv[2], directory=True)",
    "if os.path.lexists(sys.argv[3]): _protected(sys.argv[3], directory=True)",
    sep = "\n")
  processx::run(runtime$python, c("-I", "-c", protect_probe,
    system.file("flower_app", package = "dsFlower", mustWork = TRUE), parent, registry),
    env = c(LD_LIBRARY_PATH = "", DYLD_LIBRARY_PATH = "", PYTHONHOME = "",
            PYTHONNOUSERSITE = "1"), error_on_status = TRUE, timeout = 120)
  old_umask <- Sys.umask("0077")
  on.exit(Sys.umask(old_umask), add = TRUE)
  protect <- function(path, directory = FALSE) {
    if (.Platform$OS.type == "windows") {
      dsFlower:::.windows_set_private_acl(path, is_directory = directory)
    } else {
      stopifnot(all(Sys.chmod(path, if (directory) "0700" else "0600")))
    }
  }
  create_directory <- function(path) {
    if (!dir.create(path, mode = "0700")) {
      stop("Cannot create checkpoint directory without overwriting: ", path,
           call. = FALSE)
    }
    protect(path, directory = TRUE)
  }
  staging <- tempfile(".segmentation-public-install-", tmpdir = parent)
  create_directory(staging)
  on.exit(unlink(staging, recursive = TRUE), add = TRUE)
  staged_registry <- file.path(staging, "segmentation-public-checkpoints")
  create_directory(staged_registry)
  for (id in ids) {
    destination <- file.path(staged_registry, id)
    create_directory(destination)
    stopifnot(all(file.copy(artifact_paths[[id]], destination, overwrite = FALSE)),
              file.copy(checkpoint_paths[[id]], file.path(destination, "checkpoint.npz"),
                        overwrite = FALSE))
    for (path in list.files(destination, full.names = TRUE)) protect(path)
  }

  # Use the same isolated runtime/environment and encoder probe as admission.
  spec <- paste0('{"kind":"sequential","layers":[',
    '{"op":"reshape","shape":[128,16,16]},',
    '{"op":"conv2d","out_channels":8,"kernel_size":3,"padding":1},',
    '{"op":"relu"},{"op":"upsample","scale_factor":2},',
    '{"op":"conv2d","out_channels":4,"kernel_size":3,"padding":1},',
    '{"op":"relu"},{"op":"upsample","scale_factor":4},',
    '{"op":"conv2d","out_channels":1,"kernel_size":1}]}')
  config <- list(
    "model-spec-b64" = gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(spec))),
    "task-type" = "segmentation", "loss-name" = "segmentation_bce_dice",
    "data-kind" = "image", "backbone" = "resnet18_layer2",
    "vision-extractor-profile" = "resnet18_layer2_128_v1", "num-features" = 32768L,
    "image-size" = 128L, "num-classes" = 2L,
    "segmentation-selection" = "canonical-image-id-lexicographic-v1",
    "segmentation-preprocessing" = "rgb_bilinear_imagenet_128_v1",
    "segmentation-checkpoint-sha256" =
      "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec",
    "segmentation-output-shape" = "1,128,128", "segmentation-alpha" = 0.5,
    "segmentation-smooth" = 1, "mask-vocabulary" = "0,255")
  verify_registry <- function(root) {
    for (id in ids) {
      config[["segmentation-decoder-init"]] <- paste0("public:", id)
      dsFlower:::.verifySegmentationPublicCheckpoint(
        config, id, unname(pins[[id]]), registry_root = root, runtime = runtime)
    }
  }
  verify_registry(staged_registry)

  # Exclusive directory creation refuses existing revisions. Roll back only
  # directories created by this invocation if copying or final verification fails.
  installed <- character()
  complete <- FALSE
  on.exit(if (!complete) unlink(installed, recursive = TRUE), add = TRUE)
  if (!dir.exists(registry)) create_directory(registry)
  for (id in ids) {
    destination <- file.path(registry, id)
    create_directory(destination)
    installed <- c(installed, destination)
    stopifnot(all(file.copy(list.files(file.path(staged_registry, id), full.names = TRUE),
                           destination, overwrite = FALSE)))
    for (path in list.files(destination, full.names = TRUE)) protect(path)
  }
  verify_registry(registry)
  complete <- TRUE
  cat("Verified and installed all three original public checkpoints in:\n", registry,
      "\nPersist the following in custodian-controlled node startup configuration:\n",
      sep = "")
  cat("options(dsflower.segmentation_public_checkpoints = ")
  dput(pins[ids])
  cat(")\n")
  invisible(pins[ids])
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 1L) {
    stop("Usage: Rscript --vanilla tools/install-segmentation-public-checkpoints.R /path/to/recovered-epochs60",
         call. = FALSE)
  }
  install_segmentation_public_checkpoints(args[[1]])
}
