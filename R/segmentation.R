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
  c("segmentation-public-manifest-sha256", "segmentation-public-checkpoint-sha256",
    "segmentation-public-provenance")
}

# This is custodian policy, never analyst-supplied run configuration. Pinning the
# manifest also pins its licence/protocol/evidence and exact tensor inventory.
.segmentationPublicCheckpointPolicy <- function() {
  policy <- .dsf_option("segmentation_public_checkpoints", character())
  if (!is.character(policy) || (length(policy) &&
      (is.null(names(policy)) || anyNA(names(policy)) ||
       anyDuplicated(names(policy)) ||
       any(!grepl("\\A[a-z0-9][a-z0-9._-]{0,63}\\z", names(policy), perl = TRUE)) ||
       anyNA(policy) || any(!grepl("\\A[0-9a-f]{64}\\z", policy, perl = TRUE))))) {
    stop("dsflower.segmentation_public_checkpoints must be a named character ",
         "vector of checkpoint IDs and manifest SHA-256 values.", call. = FALSE)
  }
  policy
}

# Run the installed verifier in the trusted runtime, before resolving private
# descriptors or staging metadata. The runner repeats verification at execution.
.verifySegmentationPublicCheckpoint <- function(
    run_config, checkpoint_id, manifest_sha256,
    registry_root = file.path(dirname(.node_secret_path()),
                              "segmentation-public-checkpoints"),
    runtime = .resolve_framework_runtime("pytorch"),
    runner_dir = system.file("flower_app", "dsflower_runner", package = "dsFlower"),
    run_probe = processx::run) {
  spec_b64 <- run_config[["model-spec-b64"]]
  if (!is.character(spec_b64) || length(spec_b64) != 1L || is.na(spec_b64) ||
      !nzchar(spec_b64) || nchar(spec_b64, type = "bytes") > 16384L) {
    stop("Public segmentation initialisation requires a bounded decoder spec.",
         call. = FALSE)
  }
  decoded <- tryCatch(jsonlite::base64_dec(spec_b64), error = function(e) NULL)
  canonical <- if (is.null(decoded)) NULL else
    gsub("[\r\n]", "", jsonlite::base64_enc(decoded))
  spec <- tryCatch(jsonlite::fromJSON(rawToChar(decoded), simplifyVector = FALSE),
                   error = function(e) NULL)
  if (!identical(canonical, spec_b64) || !is.list(spec) || is.null(names(spec))) {
    stop("Public segmentation initialisation requires a canonical decoder spec.",
         call. = FALSE)
  }
  cfg <- run_config
  cfg[["task-type"]] <- "segmentation"
  code <- paste(
    "import json, sys",
    "sys.path.insert(0, sys.argv[1])",
    "from dsflower_runner.segmentation_checkpoints import initialization_payload",
    "from dsflower_runner.segmentation import prepare_encoder, validate_decoder_spec",
    "cfg = json.loads(sys.argv[5])",
    "spec = json.loads(sys.argv[6])",
    "validate_decoder_spec(spec)",
    "payload = initialization_payload(sys.argv[2], sys.argv[3], sys.argv[4], decoder_spec=spec)",
    "prepare_encoder(cfg)",
    "sys.stdout.write(json.dumps(payload, allow_nan=False))", sep = "\n")
  result <- tryCatch({
    if (!nzchar(runner_dir) || !dir.exists(runner_dir)) stop("missing runner")
    # Match the trusted launch whitelist without creating staging or node state.
    # In particular TORCH_HOME/HOME must not select a different encoder cache.
    inherited_names <- c(
      "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR",
      "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
      "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
      "http_proxy", "https_proxy", "no_proxy",
      "CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES",
      "NVIDIA_DRIVER_CAPABILITIES", "ROCR_VISIBLE_DEVICES", "XDG_CACHE_HOME")
    inherited <- Sys.getenv(inherited_names, unset = NA_character_)
    inherited <- inherited[!is.na(inherited)]
    venv_bin <- file.path(runtime$venv_path,
                         if (.Platform$OS.type == "windows") "Scripts" else "bin")
    env <- c(inherited, LD_LIBRARY_PATH = "", DYLD_LIBRARY_PATH = "",
             PYTHONHOME = "", PYTHONNOUSERSITE = "1", PYTHONHASHSEED = "0",
             CUBLAS_WORKSPACE_CONFIG = ":4096:8", VIRTUAL_ENV = runtime$venv_path,
             PATH = paste0(venv_bin, .Platform$path.sep, Sys.getenv("PATH", "")))
    run_probe(command = runtime$python,
      args = c("-I", "-c", code, dirname(runner_dir), registry_root,
               checkpoint_id, manifest_sha256,
               as.character(jsonlite::toJSON(cfg, auto_unbox = TRUE, null = "null")),
               as.character(jsonlite::toJSON(spec, auto_unbox = TRUE, null = "null"))),
      env = env, error_on_status = FALSE, timeout = 120)
  }, error = function(e) NULL)
  payload <- if (is.list(result) && identical(as.integer(result$status), 0L) &&
      is.character(result$stdout) && length(result$stdout) == 1L &&
      !is.na(result$stdout) && nchar(result$stdout, type = "bytes") <= 2 * 1024^2) {
    tryCatch(jsonlite::fromJSON(result$stdout, simplifyVector = FALSE),
             error = function(e) NULL)
  } else NULL
  provenance <- if (is.list(payload)) payload$provenance else NULL
  manifest <- if (is.list(provenance)) provenance$manifest else NULL
  checkpoint_hash <- if (is.list(manifest) && is.list(manifest$checkpoint))
    manifest$checkpoint$sha256 else NULL
  if (!is.list(provenance) ||
      !identical(provenance$manifest_sha256, manifest_sha256) ||
      !is.list(manifest) || !identical(manifest$checkpoint_id, checkpoint_id) ||
      !is.character(checkpoint_hash) || length(checkpoint_hash) != 1L ||
      is.na(checkpoint_hash) ||
      !grepl("\\A[0-9a-f]{64}\\z", checkpoint_hash, perl = TRUE)) {
    stop("Public segmentation checkpoint verification failed before private staging.",
         call. = FALSE)
  }
  bytes <- tryCatch(jsonlite::base64_dec(payload$checkpoint_base64),
                     error = function(e) NULL)
  canonical <- if (is.null(bytes)) NULL else
    gsub("[\r\n]", "", jsonlite::base64_enc(bytes))
  if (!is.character(payload$checkpoint_base64) ||
      length(payload$checkpoint_base64) != 1L ||
      !identical(canonical, payload$checkpoint_base64) || !length(bytes) ||
      !identical(as.numeric(length(bytes)), as.numeric(manifest$checkpoint$size_bytes)) ||
      !identical(digest::digest(bytes, algo = "sha256", serialize = FALSE),
                 checkpoint_hash)) {
    stop("Public segmentation checkpoint transport verification failed.", call. = FALSE)
  }
  payload
}

.normalizeSegmentationDecoderInit <- function(run_config) {
  init <- run_config[["segmentation-decoder-init"]] %||% "random"
  if (!is.character(init) || length(init) != 1L || is.na(init) ||
      !(identical(init, "random") ||
        grepl("\\Apublic:[a-z0-9][a-z0-9._-]{0,63}\\z", init, perl = TRUE))) {
    stop("Segmentation decoder_init must be 'random' or 'public:<checkpoint-id>'.",
         call. = FALSE)
  }
  if (identical(init, "random")) {
    # Keep the pre-existing random request and seed contract byte-for-byte.
    run_config[["segmentation-decoder-init"]] <- NULL
    return(run_config)
  }
  checkpoint_id <- substring(init, 8L)
  policy <- .segmentationPublicCheckpointPolicy()
  if (!checkpoint_id %in% names(policy)) {
    stop("Public segmentation checkpoint is not allowlisted by the custodian.",
         call. = FALSE)
  }
  manifest_sha256 <- unname(policy[[checkpoint_id]])
  payload <- .verifySegmentationPublicCheckpoint(
    run_config, checkpoint_id, manifest_sha256)
  provenance <- payload$provenance
  run_config[["segmentation-public-manifest-sha256"]] <- manifest_sha256
  run_config[["segmentation-public-checkpoint-sha256"]] <-
    provenance$manifest$checkpoint$sha256
  run_config[["segmentation-public-provenance"]] <- provenance
  # Public bytes are returned through status for coordinator initialisation,
  # never written into the training manifest or derived from private arrays.
  attr(run_config, "segmentation_public_initialization") <- payload
  run_config
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
  .normalizeSegmentationDecoderInit(run_config)
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
