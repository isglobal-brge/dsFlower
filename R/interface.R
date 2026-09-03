# Module: DataSHIELD Exposed Methods
# All DataSHIELD assign/aggregate methods for Flower federated learning.

.VISION_VALIDATION_ARTIFACT_FORMAT <- "pytorch-state-dict-v1"
.VISION_VALIDATION_ARTIFACT_MAX_BYTES <- 1024^3
.VISION_EXTRACTOR_FEATURE_DIMS <- c(
  resnet18 = 512L, resnet18_3d = 512L,
  densenet121 = 1024L, densenet121_3d = 1024L)
.VISION_EXTRACTOR_MIN_IMAGE_SIZES <- c(
  resnet18 = 1L, resnet18_3d = 1L,
  densenet121 = 29L, densenet121_3d = 128L)
.VISION_EXTRACTOR_PROFILES <- c(
  resnet18 = "dsflower-resnet18-imagenet1k-v1-extractor-v1",
  resnet18_3d = "dsflower-resnet18-monai-seed0-extractor-v1",
  densenet121 = "dsflower-densenet121-imagenet1k-v1-extractor-v1",
  densenet121_3d = "dsflower-densenet121-monai-seed0-extractor-v1")
.VISION_EXTRACTOR_FIELDS <-
  c("backbone", "image-size", "vision-extractor-profile")

# --- Handle management ---

#' Generate an opaque Flower-handle capability
#' @keywords internal
.new_handle_capability <- function() {
  entropy <- tryCatch(.read_os_entropy(16L), error = function(e) raw(0))
  if (length(entropy) != 16L) {
    stop("Could not obtain 128 bits of operating-system entropy for a handle.",
         call. = FALSE)
  }
  paste0("hdl_", paste(sprintf("%02x", as.integer(entropy)), collapse = ""))
}

#' Validate an opaque Flower-handle capability
#' @keywords internal
.validate_handle_capability <- function(capability) {
  if (!is.character(capability) || length(capability) != 1L ||
      is.na(capability) || !grepl("^hdl_[0-9a-f]{32}$", capability)) {
    stop("Invalid Flower handle capability.", call. = FALSE)
  }
  capability
}

.handle_reference <- function(capability) {
  structure(list(capability = .validate_handle_capability(capability)),
            class = "dsflower_handle_ref")
}

.is_handle_reference <- function(object) {
  names <- if (is.list(object)) names(object) else NULL
  is.list(object) && !is.null(names) &&
    sum(names == "capability", na.rm = TRUE) == 1L &&
    is.character(object$capability) && length(object$capability) == 1L &&
    !is.na(object$capability) &&
    grepl("^hdl_[0-9a-f]{32}$", object$capability)
}

# The creating evaluation environment binds the capability to one in-process
# DataSHIELD session. A valid reference copied into another session is rejected.
.registerHandle <- function(handle, owner_env = parent.frame()) {
  if (!is.list(handle) || !is.environment(owner_env)) {
    stop("Invalid Flower handle registration.", call. = FALSE)
  }
  capability <- .new_handle_capability()
  .handle_registry[[capability]] <- list(
    handle = handle, owner_env = owner_env)
  .handle_reference(capability)
}

.find_handle_reference <- function(symbol) {
  if (!is.character(symbol) || length(symbol) != 1L || is.na(symbol) ||
      !nzchar(symbol)) {
    stop("Invalid Flower handle symbol.", call. = FALSE)
  }
  frames <- rev(sys.frames())
  saw_malformed <- FALSE
  for (env in frames) {
    if (exists(symbol, envir = env, inherits = FALSE)) {
      object <- get(symbol, envir = env, inherits = FALSE)
      if (.is_handle_reference(object)) {
        return(list(reference = object, owner_env = env))
      }
      saw_malformed <- TRUE
    }
  }
  if (saw_malformed) {
    stop("No Flower handle for symbol '", symbol,
         "' (forged or malformed reference).", call. = FALSE)
  }
  stop("No Flower handle for symbol '", symbol,
       "'. Call flowerInitDS first.", call. = FALSE)
}

.resolveHandle <- function(symbol) {
  found <- .find_handle_reference(symbol)
  capability <- .validate_handle_capability(found$reference$capability)
  entry <- .handle_registry[[capability]]
  if (is.null(entry) || !is.list(entry) || !is.list(entry$handle) ||
      !is.environment(entry$owner_env) ||
      !identical(entry$owner_env, found$owner_env)) {
    stop("No Flower handle for symbol '", symbol,
         "' (unknown, stale, or cross-session capability).", call. = FALSE)
  }
  list(capability = capability, reference = found$reference,
       owner_env = found$owner_env, handle = entry$handle)
}

#' Retrieve authoritative state for an opaque Flower handle
#' @keywords internal
.getHandle <- function(symbol) {
  .resolveHandle(symbol)$handle
}

# Store changed state and return the same opaque reference for the DataSHIELD
# ASSIGN expression to place back in the session workspace.
.storeHandle <- function(symbol, handle) {
  resolved <- .resolveHandle(symbol)
  .handle_registry[[resolved$capability]] <- list(
    handle = handle, owner_env = resolved$owner_env)
  resolved$reference
}

# Internal/test convenience. Production initialization uses .registerHandle
# because DataSHIELD chooses the output symbol after flowerInitDS returns.
.setHandle <- function(symbol, handle, owner_env = parent.frame()) {
  if (exists(symbol, envir = owner_env, inherits = FALSE)) {
    old <- get(symbol, envir = owner_env, inherits = FALSE)
    if (.is_handle_reference(old)) .handle_registry[[old$capability]] <- NULL
  }
  reference <- .registerHandle(handle, owner_env = owner_env)
  assign(symbol, reference, envir = owner_env)
  invisible(reference)
}

.validateHandleStaging <- function(handle, required = FALSE,
                                   must_exist = TRUE) {
  has_token <- !is.null(handle$run_token)
  has_dir <- !is.null(handle$staging_dir)
  if (!identical(has_token, has_dir)) {
    stop("Flower handle has inconsistent staging state.", call. = FALSE)
  }
  if (isTRUE(required) && !has_token) {
    stop("Flower handle has no prepared staging directory.", call. = FALSE)
  }
  if (has_token) {
    handle$run_token <- .validate_run_token(handle$run_token)
    handle$staging_dir <- .validateStagingDir(
      handle$staging_dir, handle$run_token, must_exist = must_exist)
  }
  pending <- handle$pending_cleanup_tokens %||% character()
  if (!is.character(pending) || anyNA(pending) || anyDuplicated(pending)) {
    stop("Flower handle has inconsistent staging state.", call. = FALSE)
  }
  if (length(pending)) {
    handle$pending_cleanup_tokens <- vapply(
      pending, .validate_run_token, character(1), USE.NAMES = FALSE)
  } else {
    handle$pending_cleanup_tokens <- NULL
  }
  handle
}

.cleanupPendingStaging <- function(handle) {
  pending <- handle$pending_cleanup_tokens %||% character()
  for (run_token in pending) .cleanupStaging(run_token)
  handle$pending_cleanup_tokens <- NULL
  handle
}

#' Remove authoritative state for an opaque Flower handle
#' @keywords internal
.removeHandle <- function(symbol, cleanup = TRUE) {
  resolved <- .resolveHandle(symbol)
  if (isTRUE(cleanup)) {
    handle <- .validateHandleStaging(
      resolved$handle, must_exist = FALSE)
    if (!is.null(handle$run_token)) .cleanupStaging(handle$run_token)
    .cleanupPendingStaging(handle)
  }
  .handle_registry[[resolved$capability]] <- NULL
  if (exists(symbol, envir = resolved$owner_env, inherits = FALSE)) {
    current <- get(symbol, envir = resolved$owner_env, inherits = FALSE)
    if (.is_handle_reference(current) &&
        identical(current$capability, resolved$capability)) {
      rm(list = symbol, envir = resolved$owner_env)
    }
  }
  invisible(NULL)
}

#' Create a Flower handle from a data.frame
#'
#' Builds the internal handle structure from an in-session data.frame.
#' The data is stored directly in the handle instead of referencing a file.
#'
#' @param df A data.frame.
#' @return A list representing the Flower handle.
#' @keywords internal
.createHandleFromTable <- function(df, data_symbol = NULL) {
  python_path <- Sys.which("python3")
  if (!nzchar(python_path)) python_path <- Sys.which("python")
  if (!nzchar(python_path)) python_path <- "python3"

  list(
    source             = "table",
    data_symbol        = data_symbol %||% "table",
    resource_client    = NULL,
    data_path          = NULL,
    data_format        = "table",
    python_path        = python_path,
    table_data         = df,
    run_token          = NULL,
    staging_dir        = NULL,
    superlink_address  = NULL,
    federation_id      = NULL,
    ca_cert_path       = NULL,
    target_column      = NULL,
    feature_columns    = NULL,
    prepared           = FALSE,
    node_ensured       = FALSE
  )
}

#' Create a Flower handle from a FlowerDatasetDescriptor
#'
#' Builds the internal handle from a descriptor. The descriptor carries
#' metadata, assets, and staging hints. Actual data is NOT loaded here --
#' that happens in \code{flowerPrepareRunDS} via \code{.stageFromDescriptor}.
#'
#' @param desc A \code{FlowerDatasetDescriptor}.
#' @return A list representing the Flower handle.
#' @keywords internal
.createHandleFromDescriptor <- function(desc, data_symbol = NULL) {
  stopifnot(inherits(desc, "FlowerDatasetDescriptor"))

  python_path <- Sys.which("python3")
  if (!nzchar(python_path)) python_path <- Sys.which("python")
  if (!nzchar(python_path)) python_path <- "python3"

  list(
    source             = "descriptor",
    data_symbol        = data_symbol %||% "descriptor",
    source_kind        = desc$source_kind,
    dataset_id         = desc$dataset_id,
    descriptor         = desc,
    resource_client    = NULL,
    data_path          = NULL,
    data_format        = "descriptor",
    python_path        = python_path,
    table_data         = desc$table_data,
    run_token          = NULL,
    staging_dir        = NULL,
    superlink_address  = NULL,
    federation_id      = NULL,
    ca_cert_path       = NULL,
    target_column      = NULL,
    feature_columns    = NULL,
    prepared           = FALSE,
    node_ensured       = FALSE
  )
}

# --- ASSIGN methods ---

#' Initialize Flower Handle
#'
#' DataSHIELD ASSIGN method. Creates a Flower federation handle from
#' a data.frame or matrix already assigned in the R session. The data
#' can come from any DataSHIELD operation: \code{datashield.assign.table},
#' \code{datashield.assign.resource} + \code{as.resource.data.frame},
#' or any transformation via \code{datashield.assign.expr}.
#'
#' @param data_symbol Character; symbol name of the data object.
#' @return A Flower handle object (assigned server-side).
#' @export
flowerInitDS <- function(data_symbol) {
  .dsflower_require_literal_arguments()
  # Initialize persistent privacy state before the first dsFlower operation
  # inspects a session object. This runs only in a live service/session, never
  # from package installation or namespace loading.
  .public_privacy_runtime_bootstrap()

  # data_symbol is a STRING (e.g. "D"), not the object itself.
  # Pattern matches dsOMOP: get(symbol, parent.frame())
  owner_env <- parent.frame()
  obj <- get(data_symbol, envir = owner_env, inherits = FALSE)

  if (is.matrix(obj)) obj <- as.data.frame(obj)

  # Existing path: data.frame
  if (is.data.frame(obj)) {
    return(.registerHandle(
      .createHandleFromTable(obj, data_symbol = data_symbol), owner_env))
  }

  # Imaging resources cross the package boundary only as an opaque dsImaging
  # handle created in this same DataSHIELD session.  This ensures dsImaging has
  # admitted the complete patient roster before dsFlower can consume it.
  is_imaging_reference <- is.list(obj) && identical(names(obj), "capability") &&
    is.character(obj$capability) && length(obj$capability) == 1L &&
    !is.na(obj$capability) &&
    grepl("^imgh_[0-9a-f]{64}$", obj$capability)
  if (is_imaging_reference) {
    if (!requireNamespace("dsImaging", quietly = TRUE)) {
      stop("Package 'dsImaging' is required for imaging handles.",
           call. = FALSE)
    }
    resolver <- utils::getFromNamespace(
      ".resolve_imaging_handle_for_consumer", "dsImaging")
    authorized <- resolver(
      data_symbol, expected_capability = obj$capability,
      owner_env = owner_env)
    if (!is.list(authorized) ||
        !inherits(authorized$descriptor, "ImagingDatasetDescriptor") ||
        !is.list(authorized$collection_snapshot)) {
      stop("The dsImaging handle is not an authorized imaging dataset.",
           call. = FALSE)
    }
    desc <- as_flower_dataset(authorized$descriptor)
    desc$backend <- authorized$backend %||% NULL
    desc$manifest_uri <- authorized$manifest_uri %||% NULL
    handle <- .createHandleFromDescriptor(desc, data_symbol = data_symbol)
    handle$imaging_handle_symbol <- data_symbol
    # Kept only in dsFlower's private registry. It binds later staging to the
    # exact dsImaging handle admitted here, even if the workspace symbol is
    # subsequently rebound.
    handle$imaging_handle_capability <- obj$capability
    return(.registerHandle(handle, owner_env))
  }

  # A Flower descriptor remains valid for tabular inputs. Imaging descriptors
  # must follow the authorized dsImaging-handle path above.
  if (inherits(obj, "FlowerDatasetDescriptor")) {
    if (obj$source_kind %in% c("image_bundle", "asset_ref")) {
      stop("Imaging data must be initialized with imagingInitDS() before ",
           "flowerInitDS().", call. = FALSE)
    }
    return(.registerHandle(
      .createHandleFromDescriptor(obj, data_symbol = data_symbol), owner_env))
  }

  if (inherits(obj, c("ImagingDatasetDescriptor",
                      "ImagingDatasetResourceClient"))) {
    stop("Imaging data must be initialized with imagingInitDS() before ",
         "flowerInitDS().", call. = FALSE)
  }

  if (inherits(obj, "ResourceClient")) {
    desc <- as_flower_dataset(obj)
    return(.registerHandle(
      .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
  }

  # Legacy full imaging handles exposed manifests and storage context in the
  # session workspace and could bypass dsImaging admission. Reject them.
  if (is.list(obj) && !is.null(obj$descriptor)) {
    stop("Legacy imaging handles are not accepted. Recreate the handle with ",
         "imagingInitDS().", call. = FALSE)
  }

  # Raw imaging resources have not yet passed dsImaging admission.
  if (is.list(obj) && !is.null(obj$url) &&
      grepl("^imaging\\+dataset://", obj$url %||% "")) {
    stop("Imaging resources must be initialized with imagingInitDS() before ",
         "flowerInitDS().", call. = FALSE)
  }

  # Feature assets must likewise be loaded through an authorized dsImaging
  # handle instead of resolving a shared registry identifier supplied by a user.
  if (is.list(obj) && !is.null(obj$asset_ref)) {
    stop("Imaging assets must be loaded with imagingLoadAssetDS() before ",
         "flowerInitDS().", call. = FALSE)
  }

  stop("Symbol '", data_symbol, "' is not a data.frame, matrix, ",
       "a tabular FlowerDatasetDescriptor, ResourceClient, or authorized ",
       "dsImaging handle. ",
       "Assign your data first with datashield.assign.table(), ",
       "imagingInitDS(), or similar.",
       call. = FALSE)
}

# Normalize the client-requested Flower round count to one server-pinned key.
.normalizeRunRounds <- function(run_config) {
  parse_rounds <- function(value, field) {
    if (is.null(value)) return(NULL)
    value <- suppressWarnings(as.numeric(unlist(value, use.names = FALSE)))
    if (length(value) != 1L || !is.finite(value) || value < 1 ||
        value != floor(value) || value > .Machine$integer.max) {
      stop(field, " must be a positive integer.", call. = FALSE)
    }
    as.integer(value)
  }
  rounds <- parse_rounds(run_config[["num-server-rounds"]],
                         "num-server-rounds") %||% 1L
  rounds <- .validateMaxRounds(rounds)
  run_config[["num-server-rounds"]] <- rounds
  run_config
}

.takeRunDataType <- function(run_config, expected = NULL) {
  requested <- run_config[["data_type"]] %||% expected %||% "tabular"
  requested <- as.character(unlist(requested, use.names = FALSE))
  if (length(requested) != 1L || is.na(requested) ||
      !tolower(requested) %in% c("tabular", "image")) {
    stop("data_type must be exactly 'tabular' or 'image'.", call. = FALSE)
  }
  requested <- tolower(requested)
  if (!is.null(expected) && !identical(requested, expected)) {
    stop("data_type disagrees with the server-side dataset descriptor.",
         call. = FALSE)
  }
  run_config[["data_type"]] <- NULL
  list(run_config = run_config, data_type = requested)
}

.normalizeVisionExtractorPins <- function(run_config, num_features = NULL) {
  backbone <- as.character(unlist(
    run_config[["backbone"]], use.names = FALSE))
  image_size <- suppressWarnings(as.numeric(unlist(
    run_config[["image-size"]], use.names = FALSE)))
  profile <- as.character(unlist(
    run_config[["vision-extractor-profile"]], use.names = FALSE))
  if (length(backbone) != 1L || is.na(backbone) ||
      !backbone %in% names(.VISION_EXTRACTOR_PROFILES)) {
    stop("Vision backbone is not canonical.", call. = FALSE)
  }
  if (length(profile) != 1L || is.na(profile) ||
      !identical(profile, unname(.VISION_EXTRACTOR_PROFILES[[backbone]]))) {
    stop("Vision extractor profile does not match its canonical backbone.",
         call. = FALSE)
  }
  expected_dim <- unname(.VISION_EXTRACTOR_FEATURE_DIMS[[backbone]])
  minimum_image_size <- unname(
    .VISION_EXTRACTOR_MIN_IMAGE_SIZES[[backbone]])
  if (length(image_size) != 1L || !is.finite(image_size) ||
      image_size != floor(image_size) || image_size < minimum_image_size ||
      image_size > 512L ||
      (!is.null(num_features) &&
       (!is.numeric(num_features) || is.logical(num_features) ||
        length(num_features) != 1L || is.na(num_features) ||
        !is.finite(num_features) || num_features != floor(num_features) ||
        num_features != expected_dim))) {
    stop("Vision image geometry is outside its public contract.",
         call. = FALSE)
  }
  run_config[["backbone"]] <- backbone
  run_config[["image-size"]] <- as.integer(image_size)
  run_config[["vision-extractor-profile"]] <- profile
  if (!is.null(num_features)) {
    run_config[["num-features"]] <- as.integer(expected_dim)
  }
  run_config
}

.normalizeValidationConfig <- function(run_config, track) {
  fields <- names(run_config)[startsWith(tolower(names(run_config)),
                                          "validation-")]
  if (!identical(track, "validation")) {
    present_vision <- intersect(.VISION_EXTRACTOR_FIELDS, names(run_config))
    if (length(fields)) {
      stop("validation-* fields require dp-track='validation'.",
           call. = FALSE)
    }
    data_type <- tolower(as.character(unlist(
      run_config[["data_type"]] %||% "tabular", use.names = FALSE)))
    if (identical(track, "neural") && identical(data_type, "image")) {
      if (!identical(sort(present_vision),
                     sort(.VISION_EXTRACTOR_FIELDS))) {
        stop("Image neural training requires the exact backbone, image-size ",
             "and vision-extractor-profile pin set.", call. = FALSE)
      }
      return(.normalizeVisionExtractorPins(
        run_config,
        num_features = unlist(
          run_config[["num-features"]] %||% NA_real_, use.names = FALSE)))
    }
    if (length(present_vision)) {
      stop("Vision extractor fields require neural image training or validation.",
           call. = FALSE)
    }
    return(run_config)
  }
  if (!identical(as.integer(run_config[["num-server-rounds"]]), 1L)) {
    stop("The validation track has exactly one private release.",
         call. = FALSE)
  }
  model_track <- tolower(as.character(unlist(
    run_config[["validation-model-track"]] %||% "", use.names = FALSE)))
  data_type <- tolower(as.character(unlist(
    run_config[["data_type"]] %||% "tabular", use.names = FALSE)))
  task <- tolower(as.character(unlist(
    run_config[["validation-task"]] %||% "", use.names = FALSE)))
  bins <- suppressWarnings(as.numeric(unlist(
    run_config[["validation-bins"]] %||% 32L, use.names = FALSE)))
  if (length(model_track) != 1L || is.na(model_track) ||
      !model_track %in% c("neural", "native_tree")) {
    stop("validation-model-track must be neural or native_tree.",
         call. = FALSE)
  }
  if (length(data_type) != 1L || is.na(data_type) ||
      !data_type %in% c("tabular", "image")) {
    stop("Validation data_type must be exactly tabular or image.",
         call. = FALSE)
  }
  if (length(task) != 1L || is.na(task) ||
      !task %in% c("binary", "multiclass", "ordinal", "multilabel",
                   "regression", "count")) {
    stop("validation-task is unsupported.", call. = FALSE)
  }
  if (length(bins) != 1L || !is.finite(bins) || bins != floor(bins) ||
      bins < 4L || bins > 512L) {
    stop("validation-bins must be an integer in [4, 512].", call. = FALSE)
  }
  n_classes <- suppressWarnings(as.numeric(unlist(
    run_config[["num-classes"]] %||% 2L, use.names = FALSE)))
  n_labels <- suppressWarnings(as.numeric(unlist(
    run_config[["num-labels"]] %||% 2L, use.names = FALSE)))
  n_features <- suppressWarnings(as.numeric(unlist(
    run_config[["num-features"]] %||% NA_integer_, use.names = FALSE)))
  if (length(n_classes) != 1L || !is.finite(n_classes) ||
      n_classes != floor(n_classes) || n_classes < 2L || n_classes > 1024L ||
      length(n_labels) != 1L || !is.finite(n_labels) ||
      n_labels != floor(n_labels) || n_labels < 2L || n_labels > 1024L) {
    stop("Validation class/label counts must be integers in [2, 1024].",
         call. = FALSE)
  }
  if (length(n_features) != 1L || !is.finite(n_features) ||
      n_features != floor(n_features) || n_features < 1L ||
      n_features > 65536L) {
    stop("Validation num-features must be an integer in [1, 65536].",
         call. = FALSE)
  }
  bounds <- run_config[["feature-bounds"]] %||% NULL
  if (!is.null(bounds) && (!is.list(bounds) ||
      length(unlist(bounds$lower, use.names = FALSE)) != n_features ||
      length(unlist(bounds$upper, use.names = FALSE)) != n_features)) {
    stop("Validation feature-bounds must match num-features exactly.",
         call. = FALSE)
  }
  loss <- tolower(as.character(unlist(
    run_config[["loss-name"]] %||% "", use.names = FALSE)))
  if (length(loss) != 1L || is.na(loss)) {
    stop("Validation loss-name must be one scalar value.", call. = FALSE)
  }
  if (identical(loss, "bce_logits") && n_classes != 2L) {
    stop("bce_logits validation is binary; use cross_entropy for multiclass.",
         call. = FALSE)
  }
  if (identical(task, "multilabel") && n_classes != 2L) {
    stop("Multilabel validation requires binary target levels.",
         call. = FALSE)
  }
  expected_task <- switch(loss,
    bce_logits = "binary",
    cross_entropy = if (n_classes > 2L) "multiclass" else "binary",
    hinge = if (n_classes > 2L) "multiclass" else "binary",
    ordinal = "ordinal", multilabel_bce = "multilabel",
    mse = "regression", huber = "regression", quantile = "regression",
    gamma_nll = "regression",
    poisson_nll = "count", negbin_nll = "count", "")
  if (!nzchar(expected_task) || !identical(task, expected_task)) {
    stop("validation-task disagrees with the pinned model loss.",
         call. = FALSE)
  }
  artifact_fields <- c(
    "validation-artifact-format", "validation-artifact-sha256",
    "validation-artifact-size-bytes")
  native_only_fields <- c(
    "validation-native-tree-request-b64",
    "validation-native-tree-request-sha256",
    "validation-profile-sha256", "validation-profile-size-bytes",
    "validation-public-schema-sha256")
  present_artifact <- intersect(artifact_fields, names(run_config))
  present_native_only <- intersect(native_only_fields, names(run_config))
  vision_fields <- .VISION_EXTRACTOR_FIELDS
  present_vision <- intersect(vision_fields, names(run_config))
  if (identical(model_track, "neural")) {
    if (length(present_native_only)) {
      stop("Native-tree validation pins require validation-model-track=",
           "'native_tree'.", call. = FALSE)
    }
    if (identical(data_type, "image")) {
      if (!identical(task, if (n_classes == 2L) "binary" else "multiclass") ||
          !identical(loss, "cross_entropy") || n_labels != 2L) {
        stop("Vision validation supports cross_entropy binary/multiclass ",
             "classification only.", call. = FALSE)
      }
      if (!identical(sort(present_vision), sort(vision_fields)) ||
          !identical(sort(present_artifact), sort(artifact_fields))) {
        stop("Vision validation requires the exact backbone, image-size, ",
             "extractor-profile and artifact pin set.", call. = FALSE)
      }
      if (!is.null(run_config[["feature-bounds"]]) ||
          !is.null(run_config[["target-bounds"]])) {
        stop("Vision validation does not accept tabular feature or target bounds.",
             call. = FALSE)
      }
      levels <- unlist(run_config[["target-levels"]] %||% NULL,
                       use.names = FALSE)
      if (length(levels) != n_classes || anyNA(levels) ||
          anyDuplicated(levels)) {
        stop("Vision validation requires exactly one public target level per class.",
             call. = FALSE)
      }
      run_config <- .normalizeVisionExtractorPins(
        run_config, num_features = n_features)
      artifact_hash <- run_config[["validation-artifact-sha256"]]
      artifact_size <- suppressWarnings(as.numeric(unlist(
        run_config[["validation-artifact-size-bytes"]], use.names = FALSE)))
      if (!identical(run_config[["validation-artifact-format"]],
                     .VISION_VALIDATION_ARTIFACT_FORMAT) ||
          !is.character(artifact_hash) || length(artifact_hash) != 1L ||
          is.na(artifact_hash) || !grepl("^[0-9a-f]{64}$", artifact_hash) ||
          length(artifact_size) != 1L || !is.finite(artifact_size) ||
          artifact_size != floor(artifact_size) || artifact_size < 1 ||
          artifact_size > .VISION_VALIDATION_ARTIFACT_MAX_BYTES) {
        stop("Vision validation artifact pins are outside their public contract.",
             call. = FALSE)
      }
      spec_b64 <- run_config[["model-spec-b64"]]
      if (!is.character(spec_b64) || length(spec_b64) != 1L ||
          is.na(spec_b64) || !nzchar(spec_b64)) {
        stop("Vision validation requires one declarative model spec.",
             call. = FALSE)
      }
      run_config[["validation-artifact-size-bytes"]] <-
        as.integer(artifact_size)
    } else if (length(present_vision) || length(present_artifact)) {
      stop("Vision-only validation pins require data_type='image'.",
           call. = FALSE)
    }
  } else {
    if (!identical(data_type, "tabular") || length(present_vision)) {
      stop("Native-tree validation accepts tabular data only.",
           call. = FALSE)
    }
    present_native <- c(present_artifact, present_native_only)
    native_fields <- c(artifact_fields, native_only_fields)
    if (!identical(task, if (identical(loss, "bce_logits")) "binary" else
                   if (identical(loss, "mse")) "regression" else "") ||
        !identical(sort(present_native), sort(native_fields))) {
      stop("Native-tree validation requires the exact binary or ",
           "regression public pin set.", call. = FALSE)
    }
    if (!is.null(run_config[["model-spec-b64"]])) {
      stop("Native-tree validation does not accept a neural model spec.",
           call. = FALSE)
    }
    request <- .validate_native_tree_request_wire(
      run_config[["validation-native-tree-request-b64"]],
      run_config[["validation-native-tree-request-sha256"]])
    expected_request_task <- if (identical(task, "binary")) "binary" else
      "regression"
    if (!request$value$engine %in% .NATIVE_TREE_ENGINES ||
        !identical(request$value$task, expected_request_task) ||
        !identical(request$value$public_schema$sha256,
                   run_config[["validation-public-schema-sha256"]])) {
      stop("Native-tree validation request differs from its public pins.",
           call. = FALSE)
    }
    release_spec <- .native_tree_release_spec(request$value$engine)
    digests <- c(
      run_config[["validation-artifact-sha256"]],
      run_config[["validation-profile-sha256"]],
      run_config[["validation-public-schema-sha256"]])
    if (any(vapply(digests, function(value) {
      !is.character(value) || length(value) != 1L || is.na(value) ||
        !grepl("^[0-9a-f]{64}$", value)
    }, logical(1)))) {
      stop("Native-tree validation SHA-256 pins are invalid.", call. = FALSE)
    }
    artifact_size <- suppressWarnings(as.numeric(unlist(
      run_config[["validation-artifact-size-bytes"]], use.names = FALSE)))
    profile_size <- suppressWarnings(as.numeric(unlist(
      run_config[["validation-profile-size-bytes"]], use.names = FALSE)))
    if (length(artifact_size) != 1L || !is.finite(artifact_size) ||
        artifact_size != floor(artifact_size) || artifact_size < 1 ||
        artifact_size > 64 * 1024^2 || length(profile_size) != 1L ||
        !is.finite(profile_size) || profile_size != floor(profile_size) ||
        profile_size < 1 || profile_size > 128 * 1024L ||
        !identical(run_config[["validation-artifact-format"]],
                   release_spec$artifact_format)) {
      stop("Native-tree validation artifact/profile pins are outside their ",
           "public bounds.", call. = FALSE)
    }
    run_config[["validation-native-tree-request-b64"]] <- request$b64
    run_config[["validation-native-tree-request-sha256"]] <- request$sha256
    run_config[["validation-artifact-size-bytes"]] <- as.integer(artifact_size)
    run_config[["validation-profile-size-bytes"]] <- as.integer(profile_size)
  }
  run_config[["validation-model-track"]] <- model_track
  run_config[["validation-task"]] <- task
  run_config[["validation-bins"]] <- as.integer(bins)
  run_config[["num-classes"]] <- as.integer(n_classes)
  run_config[["num-labels"]] <- as.integer(n_labels)
  run_config[["num-features"]] <- as.integer(n_features)
  run_config[["loss-name"]] <- loss
  run_config
}

.normalizeNativeTreeConfig <- function(run_config, track) {
  fields <- names(run_config)[startsWith(tolower(names(run_config)),
                                          "native-tree-")]
  expected <- c("native-tree-request-b64", "native-tree-request-sha256")
  if (!identical(track, "native_tree")) {
    if (length(fields)) {
      stop("native-tree-* fields require dp-track='native_tree'.",
           call. = FALSE)
    }
    return(run_config)
  }
  if (!identical(sort(fields), sort(expected))) {
    stop("native_tree requires exactly native-tree-request-b64 and ",
         "native-tree-request-sha256.", call. = FALSE)
  }
  if (!identical(as.integer(run_config[["num-server-rounds"]]), 1L)) {
    stop("The native_tree track has exactly one Flower round.",
         call. = FALSE)
  }
  pinned <- .validate_native_tree_request_wire(
    run_config[["native-tree-request-b64"]],
    run_config[["native-tree-request-sha256"]])
  if (!pinned$value$engine %in% .NATIVE_TREE_ENGINES) {
    stop("This release has no trusted adapter for the requested tree engine.",
         call. = FALSE)
  }
  run_config[["native-tree-request-b64"]] <- pinned$b64
  run_config[["native-tree-request-sha256"]] <- pinned$sha256
  run_config
}

.validatePreparedNativeTreeContract <- function(run_config, feature_columns,
                                                target_column) {
  native_training <- identical(run_config[["dp-track"]], "native_tree")
  native_validation <- identical(run_config[["dp-track"]], "validation") &&
    identical(run_config[["validation-model-track"]], "native_tree")
  if (!native_training && !native_validation) {
    return(invisible(TRUE))
  }
  request_b64 <- if (native_training) {
    run_config[["native-tree-request-b64"]]
  } else {
    run_config[["validation-native-tree-request-b64"]]
  }
  request_sha256 <- if (native_training) {
    run_config[["native-tree-request-sha256"]]
  } else {
    run_config[["validation-native-tree-request-sha256"]]
  }
  pinned <- .validate_native_tree_request_wire(
    request_b64, request_sha256)
  schema <- pinned$value$public_schema
  target <- schema$target
  request_features <- as.character(unlist(schema$features, use.names = FALSE))
  if (!identical(as.character(feature_columns), request_features) ||
      length(target_column) != 1L ||
      !identical(as.character(target_column), target$name)) {
    stop("Native-tree request schema differs from the prepared columns.",
         call. = FALSE)
  }
  bounds <- run_config[["feature-bounds"]]
  if (!is.list(bounds) ||
      !identical(as.numeric(bounds$lower), as.numeric(schema$lower)) ||
      !identical(as.numeric(bounds$upper), as.numeric(schema$upper))) {
    stop("Native-tree request bounds differ from the prepared public bounds.",
         call. = FALSE)
  }
  if (identical(pinned$value$task, "binary")) {
    levels <- run_config[["target-levels"]]
    type_map <- c(character = "string", logical = "boolean", numeric = "number")
    level_type <- if (is.list(levels)) as.character(levels$type %||% "") else ""
    tagged_type <- unname(type_map[[level_type]] %||% "")
    values <- if (is.list(levels)) unlist(levels$values, use.names = FALSE) else NULL
    expected <- target$levels
    if (!nzchar(tagged_type) || length(values) != 2L ||
        !all(vapply(expected, function(level) {
          identical(level$type, tagged_type)
        }, logical(1))) ||
        !identical(unname(values),
                   unname(vapply(expected, `[[`, level_type, "value")))) {
      stop("Native-tree request target levels differ from the prepared contract.",
           call. = FALSE)
    }
  } else {
    bounds <- run_config[["target-bounds"]]
    if (!is.list(bounds) ||
        !identical(as.numeric(bounds$lower), as.numeric(target$lower)) ||
        !identical(as.numeric(bounds$upper), as.numeric(target$upper))) {
      stop("Native-tree request target bounds differ from the prepared contract.",
           call. = FALSE)
    }
  }
  unit <- .dpUnitPolicy()
  if (identical(unit$dp_unit, "patient") &&
      unit$patient_column %in% c(feature_columns, as.character(target_column))) {
    stop("The server patient identifier cannot be a native-tree feature or target.",
         call. = FALSE)
  }
  invisible(TRUE)
}

.validationContractSha256 <- function(run_config, feature_columns,
                                      target_column, privacy_unit,
                                      data_kind = NULL) {
  bounds <- run_config[["feature-bounds"]] %||% NULL
  target_bounds <- run_config[["target-bounds"]] %||% NULL
  levels <- run_config[["target-levels"]] %||% NULL
  if (is.list(levels) && !is.null(levels$type) && !is.null(levels$values)) {
    level_type <- as.character(levels$type)
    level_values <- unlist(levels$values, use.names = FALSE)
  } else if (is.null(levels)) {
    level_type <- NULL
    level_values <- NULL
  } else {
    level_values <- unlist(levels, use.names = FALSE)
    level_type <- if (is.character(level_values)) "character" else
      if (is.logical(level_values)) "logical" else "numeric"
  }
  data_kind <- as.character(data_kind %||%
    run_config[["data_type"]] %||% "tabular")
  if (identical(data_kind, "image")) {
    backbone <- as.character(run_config[["backbone"]])
    feature_dim <- as.integer(run_config[["num-features"]])
    payload <- list(
      schema = 2L,
      privacy_unit = as.character(privacy_unit),
      patient_id_canonicalization = if (identical(privacy_unit, "patient"))
        "trim-utf8-v2" else NULL,
      data_kind = "image",
      targets = as.character(target_column),
      target_level_type = level_type,
      target_levels = level_values,
      model_track = run_config[["validation-model-track"]],
      task = run_config[["validation-task"]],
      bins = as.integer(run_config[["validation-bins"]]),
      loss = run_config[["loss-name"]],
      num_features = feature_dim,
      num_classes = as.integer(run_config[["num-classes"]]),
      num_labels = as.integer(run_config[["num-labels"]]),
      model_spec_b64 = run_config[["model-spec-b64"]] %||% NULL,
      backbone = backbone,
      image_size = as.integer(run_config[["image-size"]]),
      volumetric = endsWith(backbone, "_3d"),
      feature_dim = feature_dim,
      vision_extractor_profile = run_config[["vision-extractor-profile"]],
      artifact_format = run_config[["validation-artifact-format"]],
      artifact_sha256 = run_config[["validation-artifact-sha256"]],
      artifact_size_bytes = as.integer(
        run_config[["validation-artifact-size-bytes"]]))
  } else {
    payload <- list(
      schema = 1L,
      privacy_unit = as.character(privacy_unit),
      patient_id_canonicalization = if (identical(privacy_unit, "patient"))
        "trim-utf8-v2" else NULL,
      features = as.character(feature_columns),
      targets = as.character(target_column),
      feature_lower = if (is.null(bounds)) NULL else as.numeric(bounds$lower),
      feature_upper = if (is.null(bounds)) NULL else as.numeric(bounds$upper),
      target_level_type = level_type,
      target_levels = level_values,
      target_lower = if (is.null(target_bounds)) NULL else
        as.numeric(target_bounds$lower),
      target_upper = if (is.null(target_bounds)) NULL else
        as.numeric(target_bounds$upper),
      model_track = run_config[["validation-model-track"]],
      task = run_config[["validation-task"]],
      bins = as.integer(run_config[["validation-bins"]]),
      loss = run_config[["loss-name"]],
      num_features = as.integer(run_config[["num-features"]]),
      num_classes = as.integer(run_config[["num-classes"]]),
      num_labels = as.integer(run_config[["num-labels"]]),
      model_spec_b64 = run_config[["model-spec-b64"]] %||% NULL)
  }
  if (identical(run_config[["validation-model-track"]], "native_tree")) {
    payload$native_tree_request_b64 <-
      run_config[["validation-native-tree-request-b64"]]
    payload$native_tree_request_sha256 <-
      run_config[["validation-native-tree-request-sha256"]]
    payload$artifact_format <- run_config[["validation-artifact-format"]]
    payload$artifact_sha256 <- run_config[["validation-artifact-sha256"]]
    payload$artifact_size_bytes <-
      as.integer(run_config[["validation-artifact-size-bytes"]])
    payload$profile_sha256 <- run_config[["validation-profile-sha256"]]
    payload$profile_size_bytes <-
      as.integer(run_config[["validation-profile-size-bytes"]])
    payload$public_schema_sha256 <-
      run_config[["validation-public-schema-sha256"]]
  }
  canonical <- as.character(jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", na = "null",
    digits = NA, always_decimal = TRUE, pretty = FALSE))
  digest::digest(charToRaw(enc2utf8(canonical)), algo = "sha256",
                 serialize = FALSE)
}

.verifyCrossValidationJob <- function(run_config, feature_columns,
                                      target_column) {
  if (is.null(run_config[["cv-contract-sha256"]])) return(run_config)
  supplied <- .cv_job_sha(
    run_config[["cv-job-sha256"]], "cv-job-sha256")
  runner_hash <- .compute_harness_hash()
  if (!grepl("^[0-9a-f]{64}$", runner_hash)) {
    stop("The canonical runner is unavailable for CV provenance.",
         call. = FALSE)
  }
  actual <- .cv_job_sha256(
    run_config, feature_columns, target_column,
    runner_abi = 3L, runner_sha256 = runner_hash,
    privacy_policy_sha256 = run_config[["privacy-policy-sha256"]],
    privacy_clipping_norm = run_config[["privacy-clipping_norm"]])
  if (!identical(supplied, actual)) {
    stop("cv-job-sha256 does not match the normalized public CV recipe.",
         call. = FALSE)
  }
  run_config[["cv-job-sha256"]] <- actual
  run_config
}

.normalizePinnedTaskType <- function(run_config, track) {
  requested <- tolower(as.character(unlist(
    run_config[["task-type"]] %||% run_config[["task_type"]] %||% "",
    use.names = FALSE)))
  if (length(requested) != 1L || is.na(requested)) {
    stop("task-type must be one scalar value.", call. = FALSE)
  }
  if (identical(track, "validation")) {
    validation_task <- run_config[["validation-task"]]
    inferred <- if (validation_task %in% c("regression", "count")) {
      validation_task
    } else "classification"
  } else if (identical(track, "neural")) {
    loss_name <- tolower(as.character(unlist(
      run_config[["loss-name"]] %||% "bce_logits", use.names = FALSE)))
    if (length(loss_name) != 1L || is.na(loss_name)) {
      stop("loss-name must be one scalar value.", call. = FALSE)
    }
    inferred <- switch(
      loss_name,
      mse = "regression", huber = "regression", quantile = "regression",
      gamma_nll = "regression",
      poisson_nll = "count", negbin_nll = "count",
      "classification")
  } else if (identical(track, "native_tree")) {
    request <- .validate_native_tree_request_wire(
      run_config[["native-tree-request-b64"]],
      run_config[["native-tree-request-sha256"]])
    inferred <- if (identical(request$value$task, "regression")) {
      "regression"
    } else {
      "classification"
    }
  } else if (identical(track, "association")) {
    inferred <- "classification"
  } else {
    if (!requested %in% c("classification", "regression", "count")) {
      stop("HookApp task-type must be classification, regression, or count.",
           call. = FALSE)
    }
    inferred <- requested
  }
  if (nzchar(requested) && !identical(requested, inferred)) {
    stop("task-type disagrees with the pinned declarative loss/track.",
         call. = FALSE)
  }
  run_config[["task-type"]] <- inferred
  run_config[["task_type"]] <- NULL
  run_config
}

.HOOK_APP_PARAMS_MAX_DEPTH <- 8L
.HOOK_APP_PARAMS_MAX_ITEMS <- 2048L
.HOOK_APP_PARAMS_MAX_BYTES <- 65536L
.HOOK_APP_PARAMS_MAX_KEY_BYTES <- 128L
.HOOK_APP_PARAMS_MAX_STRING_BYTES <- 4096L

.hookAppReservedKey <- function(key) {
  normalized <- gsub("([a-z0-9])([A-Z])", "\\1_\\2", key, perl = TRUE)
  normalized <- gsub("[-.]", "_", tolower(normalized))
  startsWith(normalized, "privacy") || startsWith(normalized, "dp") ||
    normalized %in% c(
      "privacy", "dp", "epsilon", "delta", "clipping_norm",
      "user_module", "app_params", "app_params_b64", "app_params_sha256",
      "round", "round_index", "server_round", "num_rounds",
      "num_server_rounds", "task", "task_type", "num_classes",
      "runtime_profile", "backend", "requirements", "requirement",
      "dependencies", "dependency", "pip", "pythonpath", "python_path"
    ) ||
    grepl(
      "(^|_)(path|dir|directory|file|filename|secret|token|password|credential|requirements?|dependencies?)($|_)",
      normalized, perl = TRUE
    ) ||
    grepl(
      "(^|_)(privacy|dp|epsilon|delta|noise|sensitivity|accountant|clip|clipping)($|_)",
      normalized, perl = TRUE
    )
}

.validateHookAppKey <- function(key) {
  if (!is.character(key) || length(key) != 1L || is.na(key)) {
    stop("app-params object keys must be non-missing UTF-8 strings.",
         call. = FALSE)
  }
  key <- enc2utf8(key)
  if (!nzchar(key) || is.na(iconv(key, from = "UTF-8", to = "UTF-8",
                                  sub = NA_character_)) ||
      nchar(key, type = "bytes") > .HOOK_APP_PARAMS_MAX_KEY_BYTES ||
      grepl("[[:cntrl:]/\\\\]", key, perl = TRUE)) {
    stop("app-params object keys must be safe UTF-8 strings of at most 128 bytes.",
         call. = FALSE)
  }
  if (.hookAppReservedKey(key)) {
    stop("app-params contains a reserved or path/security-related key: ", key,
         call. = FALSE)
  }
  key
}

.canonicalHookAppValue <- function(value, depth, state, top = FALSE) {
  if (depth > .HOOK_APP_PARAMS_MAX_DEPTH) {
    stop("app-params exceeds the maximum nesting depth (8).", call. = FALSE)
  }
  if (is.object(value) || is.factor(value) || is.data.frame(value) ||
      is.matrix(value) ||
      is.array(value) || is.raw(value) || is.complex(value) ||
      inherits(value, c("Date", "POSIXt"))) {
    stop("app-params accepts only JSON-like null, scalar, array and object values.",
         call. = FALSE)
  }
  if (is.logical(value) || is.integer(value) || is.numeric(value) ||
      is.character(value)) {
    if (length(value) != 1L) {
      values <- as.list(value)
      names(values) <- names(value)
      return(.canonicalHookAppValue(values, depth, state, top = top))
    }
  }

  state$items <- state$items + 1L
  if (state$items > .HOOK_APP_PARAMS_MAX_ITEMS) {
    stop("app-params exceeds the maximum item count (2048).", call. = FALSE)
  }
  if (is.null(value)) return(NULL)
  if (is.logical(value) || is.integer(value) || is.numeric(value) ||
      is.character(value)) {
    if (is.na(value)) {
      stop("app-params scalar values cannot be missing.", call. = FALSE)
    }
    if (is.numeric(value) && !is.finite(value)) {
      stop("app-params numeric values must be finite.", call. = FALSE)
    }
    if (is.character(value)) {
      value <- enc2utf8(value)
      if (is.na(iconv(value, from = "UTF-8", to = "UTF-8",
                      sub = NA_character_)) ||
          nchar(value, type = "bytes") > .HOOK_APP_PARAMS_MAX_STRING_BYTES ||
          grepl("[[:cntrl:]]", value, perl = TRUE) ||
          grepl("(^~[/\\\\])|[/\\\\]|^[A-Za-z]:", value, perl = TRUE)) {
        stop("app-params strings must be safe UTF-8 non-path values of at most 4096 bytes.",
             call. = FALSE)
      }
    }
    return(value)
  }
  if (!is.list(value)) {
    stop("app-params accepts only JSON-like null, scalar, array and object values.",
         call. = FALSE)
  }

  object_names <- names(value)
  is_object <- isTRUE(top) ||
    (!is.null(object_names) && length(object_names) == length(value) &&
       all(nzchar(object_names)))
  if (!is_object && !is.null(object_names) && any(nzchar(object_names))) {
    stop("app-params containers cannot mix named and unnamed elements.",
         call. = FALSE)
  }
  if (is_object) {
    if (is.null(object_names)) object_names <- rep.int("", length(value))
    if (length(value) &&
        (any(!nzchar(object_names)) || anyDuplicated(object_names))) {
      stop("app-params objects require unique, non-empty keys.", call. = FALSE)
    }
    safe_names <- vapply(object_names, .validateHookAppKey, character(1))
    order_index <- order(safe_names, method = "radix")
    out <- structure(vector("list", length(value)),
                     names = unname(safe_names[order_index]))
    for (i in seq_along(order_index)) {
      out[i] <- list(.canonicalHookAppValue(
        value[[order_index[[i]]]], depth + 1L, state, top = FALSE))
    }
    return(out)
  }
  out <- vector("list", length(value))
  for (i in seq_along(value)) {
    out[i] <- list(.canonicalHookAppValue(
      value[[i]], depth + 1L, state, top = FALSE))
  }
  out
}

# Decode, validate and re-encode the public HookApp configuration before any
# private source is opened.  The server-written digest is the authoritative pin;
# the client-supplied Flower config must match it at node execution time.
.normalizeHookAppParams <- function(run_config, track) {
  app_fields <- names(run_config)[grepl(
    "^app[-_.]?params", tolower(names(run_config)), perl = TRUE)]
  aliases <- setdiff(app_fields, "app-params-b64")
  if (length(aliases)) {
    stop("Use only the canonical app-params-b64 HookApp field.", call. = FALSE)
  }
  supplied <- run_config[["app-params-b64"]] %||% NULL
  if (!identical(track, "egress")) {
    if (!is.null(supplied)) {
      stop("app-params-b64 is only valid for the HookApp egress track.",
           call. = FALSE)
    }
    return(run_config)
  }
  if (is.null(supplied)) {
    supplied <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw("{}")))
  }
  if (!is.character(supplied) || length(supplied) != 1L || is.na(supplied) ||
      !nzchar(supplied) || nchar(supplied, type = "bytes") >
        4L * ceiling(.HOOK_APP_PARAMS_MAX_BYTES / 3L)) {
    stop("app-params-b64 must be one bounded canonical base64 string.",
         call. = FALSE)
  }
  decoded <- tryCatch(jsonlite::base64_dec(supplied),
                      error = function(e) NULL)
  canonical_b64 <- if (is.null(decoded)) NULL else
    gsub("[\r\n]", "", jsonlite::base64_enc(decoded))
  if (is.null(decoded) || length(decoded) > .HOOK_APP_PARAMS_MAX_BYTES ||
      !identical(canonical_b64, supplied)) {
    stop("app-params-b64 is not canonical bounded base64.", call. = FALSE)
  }
  json <- tryCatch(rawToChar(decoded), error = function(e) NULL)
  if (is.null(json) || length(json) != 1L ||
      is.na(iconv(json, from = "UTF-8", to = "UTF-8", sub = NA_character_))) {
    stop("app-params-b64 must decode to valid UTF-8 JSON.", call. = FALSE)
  }
  parsed <- tryCatch(jsonlite::fromJSON(json, simplifyVector = FALSE),
                     error = function(e) NULL)
  if (!is.list(parsed)) {
    stop("app-params must decode to a JSON object.", call. = FALSE)
  }
  state <- new.env(parent = emptyenv())
  state$items <- 0L
  value <- .canonicalHookAppValue(parsed, 0L, state, top = TRUE)
  canonical_json <- as.character(jsonlite::toJSON(
    value, auto_unbox = TRUE, null = "null", na = "null",
    digits = NA, always_decimal = TRUE, pretty = FALSE))
  canonical_raw <- charToRaw(enc2utf8(canonical_json))
  if (length(canonical_raw) > .HOOK_APP_PARAMS_MAX_BYTES ||
      !identical(canonical_raw, decoded)) {
    stop("app-params JSON must use the canonical encoding.", call. = FALSE)
  }
  run_config[["app-params-b64"]] <- supplied
  run_config[["app-params-sha256"]] <- digest::digest(
    canonical_raw, algo = "sha256", serialize = FALSE)
  run_config
}

# Differential privacy is ALWAYS enforced as node-side central DP before egress
# (the node is the trusted curator; there is no Secure Aggregation), and
# disclosure is non-disclosive by default. Normalise the manifest run_config to
# the DP-always contract. epsilon/delta are placeholders until prepare-time,
# when the server-owned per-training contract is bound before private staging.
.bounded_server_number <- function(name, default, lower, upper,
                                   integer = FALSE) {
  value <- suppressWarnings(as.numeric(unlist(
    .dsf_option(name, default), use.names = FALSE)))
  valid <- length(value) == 1L && is.finite(value) &&
    value >= lower && value <= upper
  if (isTRUE(integer)) valid <- valid && value == floor(value)
  if (!valid) {
    kind <- if (isTRUE(integer)) "integer" else "number"
    stop("dsflower.", name, " must be one finite ", kind, " in [",
         lower, ", ", upper, "].", call. = FALSE)
  }
  if (isTRUE(integer)) as.integer(value) else value
}

.serverDpClippingNorm <- function() {
  value <- suppressWarnings(as.numeric(unlist(
    .dsf_option("dp_clipping_norm", 1.0), use.names = FALSE)))
  ceiling <- suppressWarnings(as.numeric(unlist(
    .dsf_option("dp_clip_ceiling", 100), use.names = FALSE)))
  if (length(value) != 1L || !is.finite(value) || value <= 0 ||
      length(ceiling) != 1L || !is.finite(ceiling) || ceiling <= 0 ||
      value > min(ceiling, 100)) {
    stop("Server DP clipping_norm must be finite, positive, and no greater ",
         "than the node ceiling.", call. = FALSE)
  }
  value
}

.addDpConfigToRunConfig <- function(run_config, unit_policy = NULL) {
  run_config <- .validate_client_run_config(run_config)
  run_config <- .normalizeRunRounds(run_config)
  track <- as.character(unlist(
    run_config[["dp-track"]] %||% "neural", use.names = FALSE))
  if (length(track) != 1L || is.na(track) ||
      !tolower(track) %in% c(
        "neural", "egress", "native_tree", "validation", "association")) {
    stop("dp-track must be one of neural, egress, native_tree, validation, or association.",
         call. = FALSE)
  }
  track <- tolower(track)
  run_config[["dp-track"]] <- track
  run_config <- .normalizeAssociationConfig(run_config, track)
  run_config <- .normalizeValidationConfig(run_config, track)
  run_config <- .normalizeNativeTreeConfig(run_config, track)
  run_config <- .normalizeResamplingConfig(run_config, track, unit_policy)
  run_config <- .normalizeCrossValidationConfig(run_config, track)
  run_config <- .normalizePinnedTaskType(run_config, track)
  run_config <- .normalizeHookAppParams(run_config, track)
  run_config <- .normalizePublicFeatureBounds(run_config)
  run_config <- .normalizePublicTargetConfig(run_config)
  run_config[["dp_enabled"]]               <- TRUE
  run_config[["allow_per_node_metrics"]]   <- FALSE
  run_config[["allow_exact_num_examples"]] <- FALSE
  run_config[["fixed_client_sampling"]]    <- TRUE
  policy <- .privacy_policy(unit_policy)  # validate server policy before private data
  run_config[["privacy-adjacency"]] <- policy$adjacency
  run_config[["privacy-policy-sha256"]] <- policy$policy_hash
  run_config[["privacy-epsilon"]] <- policy$per_training_epsilon
  run_config[["privacy-delta"]] <- policy$per_training_delta
  run_config <- .applyHoldoutPrivacyAllocation(run_config)
  run_config <- .applyCrossValidationPrivacyAllocation(run_config)
  run_config[["privacy-clipping_norm"]] <- .serverDpClippingNorm()
  # Improved Tier-2 floor policy (sample-and-aggregate): the node may split its private
  # data into a FIXED, administrator-pinned k, run the uploaded black-box update per
  # block, and release the clipped mean at sensitivity min(2C, 4C/k). k must not depend
  # on a private row/patient count: changing the output variance at a threshold would
  # itself be an unbounded transcript channel. Empty blocks safely map to zero deltas.
  # Encoded numerically (1/0 for the on/off flag) for stable manifest transport.
  sample_aggregate <- isTRUE(as.logical(
    .dsf_option("dp_sample_aggregate", FALSE)))
  sa_blocks <- .bounded_server_number(
    "dp_sa_blocks", 8L, 2L, 64L, integer = TRUE)
  run_config[["privacy-sample_aggregate"]] <- as.numeric(sample_aggregate)
  run_config[["privacy-sa_blocks"]] <- sa_blocks
  # Minimum-duration padding for the egress sandbox (seconds; 0 = off). Set this ABOVE
  # the egress timeout (timeout + a few seconds of kill/cleanup guard). This reduces
  # direct sleep/fast-return channels but is not a formal constant-time guarantee:
  # cleanup and availability remain deployment concerns. Off by default (it adds latency).
  egress_timeout <- .bounded_server_number(
    "dp_egress_timeout", 900L, 1L, 3600L, integer = TRUE)
  egress_time_pad <- .bounded_server_number(
    "dp_egress_time_pad", 0, 0, 230405, integer = FALSE)
  # Keep this identical to tier2_lib._PAD_GUARD.  Accepting a shorter envelope
  # here would look enabled to the custodian but be rejected by the Python gate.
  required_time_pad <- egress_timeout *
    (if (sample_aggregate) sa_blocks else 1L) + 5
  if (egress_time_pad > 0 && egress_time_pad < required_time_pad) {
    stop("dsflower.dp_egress_time_pad must be zero (HookApp no-op) or at ",
         "least ", required_time_pad, " seconds for the configured HookApp ",
         "timeout and block count.", call. = FALSE)
  }
  run_config[["privacy-egress_time_pad"]] <- egress_time_pad
  run_config[["privacy-egress_timeout"]] <- egress_timeout
  run_config[["privacy-egress_memory_mb"]] <- .bounded_server_number(
    "dp_egress_memory_mb", 8192L, 512L, 131072L, integer = TRUE)
  run_config[["privacy-egress_file_mb"]] <- .bounded_server_number(
    "dp_egress_file_mb", 1024L, 16L, 16384L, integer = TRUE)
  run_config[["privacy-egress_processes"]] <- .bounded_server_number(
    "dp_egress_processes", 128L, 1L, 1024L, integer = TRUE)
  run_config[["privacy-hook_enabled"]]     <- as.numeric(isTRUE(as.logical(
                                                .dsf_option("hook_enabled", FALSE))))
  run_config
}

# Shared structural + DP enforcement for both prepare paths. DP is
# unconditional and admission applies the server-owned DataSHIELD minimum to
# the staged privacy-unit count (rows for row adjacency, patients for patient
# adjacency).
.enforceDisclosureAndDp <- function(handle, target_column,
                                    n_samples, target_data, run_config,
                                    data_type = "tabular",
                                    n_units = n_samples) {
  # Validate the server-authored structural count before applying the standard
  # minimum-size gate. The caller wraps threshold failures in one generic node
  # error, so neither the exact count nor the shortfall leaves the node.
  unit_count <- suppressWarnings(as.numeric(unlist(n_units, use.names = FALSE)))
  if (length(unit_count) != 1L || !is.finite(unit_count) || unit_count < 0 ||
      unit_count != floor(unit_count)) {
    stop("Invalid staged privacy-unit count.", call. = FALSE)
  }
  .assertMinSamples(unit_count)
  # The stateless per-training policy was validated before private staging. The
  # clipping bound is likewise server-owned and cannot come from the analyst.
  dp_clip <- suppressWarnings(as.numeric(unlist(
    run_config[["privacy-clipping_norm"]], use.names = FALSE)))
  if (length(dp_clip) != 1L || !is.finite(dp_clip) || dp_clip <= 0 ||
      !identical(dp_clip, .serverDpClippingNorm())) {
    stop("Server DP clipping_norm differs from the node-owned contract.",
         call. = FALSE)
  }
  invisible(NULL)
}

#' Prepare a Training Run
#'
#' DataSHIELD ASSIGN method. Validates the fixed stateless per-training privacy
#' contract, applies total public preprocessing and stages the training data.
#' Earlier trainings never change admission or the per-training contract.
#'
#' @param handle_symbol Character; symbol of the initialized handle.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character; JSON-encoded feature column names, or NULL.
#' @param run_config Character; JSON-encoded additional run configuration.
#' @return Updated handle with staging information.
#' @export
flowerPrepareRunDS <- function(handle_symbol, target_column,
                                feature_columns = NULL, run_config = "{}") {
  .dsflower_require_literal_arguments()
  target_column <- .ds_arg(target_column)   # decode (B64 for multi-col survival targets;
                                            # .ds_arg passes a raw single string through)
  feature_columns <- .ds_arg(feature_columns)
  run_config <- .ds_arg(run_config)
  if (is.character(run_config) && length(run_config) == 1 &&
      !startsWith(run_config, "{")) {
    run_config <- list()
  }
  if (is.character(feature_columns) && length(feature_columns) == 1 &&
      startsWith(feature_columns, "B64:")) {
    feature_columns <- .ds_arg(feature_columns)
  }

  resolved_handle <- .resolveHandle(handle_symbol)
  handle <- resolved_handle$handle
  owner_env <- resolved_handle$owner_env
  handle <- .validateHandleStaging(handle)
  if (length(handle$pending_cleanup_tokens %||% character())) {
    handle <- .cleanupPendingStaging(handle)
    .storeHandle(handle_symbol, handle)
  }
  previous_run_token <- handle$run_token %||% NULL

  # Validate every analyst/admin-controlled value and the fixed privacy contract
  # before touching private data. There is no historical counter or budget.
  descriptor_data_type <- if (identical(handle$source, "descriptor")) {
    # source_kind is copied into the handle at initialization, so routing does
    # not need to inspect private descriptor contents.
    if (identical(handle$source_kind, "image_bundle")) "image" else "tabular"
  } else "tabular"
  if (identical(descriptor_data_type, "image")) {
    capability <- handle$imaging_handle_capability %||% ""
    if (!is.character(handle$imaging_handle_symbol) ||
        length(handle$imaging_handle_symbol) != 1L ||
        is.na(handle$imaging_handle_symbol) ||
        !nzchar(handle$imaging_handle_symbol) ||
        !is.character(capability) || length(capability) != 1L ||
        is.na(capability) || !grepl("^imgh_[0-9a-f]{64}$", capability)) {
      stop("Image training requires an authorized dsImaging handle created ",
           "with imagingInitDS().", call. = FALSE)
    }
  }
  imaging_unit_policy <- if (identical(descriptor_data_type, "image")) {
    .imagingPrivacyUnitPolicy(handle$descriptor)
  } else {
    NULL
  }
  run_config <- .addDpConfigToRunConfig(run_config, imaging_unit_policy)
  routed <- .takeRunDataType(run_config, expected = descriptor_data_type)
  run_config <- routed$run_config
  data_type <- routed$data_type
  native_tabular <- identical(run_config[["dp-track"]], "native_tree") ||
    (identical(run_config[["dp-track"]], "validation") &&
     identical(run_config[["validation-model-track"]], "native_tree"))
  association_tabular <- identical(run_config[["dp-track"]], "association")
  if ((native_tabular || association_tabular) &&
      !identical(data_type, "tabular")) {
    label <- if (association_tabular) {
      "The association track"
    } else if (identical(run_config[["dp-track"]], "native_tree")) {
      "The native_tree track"
    } else {
      "Native-tree validation"
    }
    stop(label, " accepts tabular data only.", call. = FALSE)
  }
  columns <- .normalizePublicColumnSelection(
    target_column, feature_columns, run_config)
  target_column <- columns$target_column
  feature_columns <- columns$feature_columns
  if (!is.null(imaging_unit_policy)) {
    label_column <- handle$descriptor$manifest$metadata$label_col %||% NULL
    if (!is.character(label_column) || length(label_column) != 1L ||
        is.na(label_column) || !nzchar(trimws(label_column)) ||
        !identical(as.character(target_column), trimws(label_column))) {
      stop("Image training requires the single manifest-declared label_col ",
           "as its target.", call. = FALSE)
    }
    feature_columns <- .excludePatientFeature(
      feature_columns, imaging_unit_policy$patient_column)
  }
  run_config <- .verifyAssociationContract(
    run_config, feature_columns, target_column)
  .validatePreparedNativeTreeContract(
    run_config, feature_columns, target_column)
  if (association_tabular && !isTRUE(.association_runtime_probe())) {
    stop("The trusted association runtime is unavailable on this node.",
         call. = FALSE)
  }
  if (native_tabular) {
    engine <- .native_tree_engine_from_config(run_config)
    available <- if (identical(run_config[["dp-track"]], "native_tree")) {
      .native_tree_engine_probe(engine)
    } else {
      .native_tree_validation_probe(engine)
    }
    if (!available) {
      stop("The trusted native-tree runtime for '", engine,
           "' is unavailable on this node.", call. = FALSE)
    }
  }
  if (identical(run_config[["dp-track"]], "validation")) {
    if (identical(data_type, "image")) {
      if (!is.null(feature_columns)) {
        stop("Vision validation does not accept tabular feature columns.",
             call. = FALSE)
      }
    } else {
      if (!is.character(feature_columns) || !length(feature_columns) ||
          anyNA(feature_columns) || any(!nzchar(feature_columns)) ||
          anyDuplicated(feature_columns) ||
          length(feature_columns) != run_config[["num-features"]]) {
        stop("Validation requires the explicit ordered public feature contract.",
             call. = FALSE)
      }
    }
    unit_policy <- .resolvePrivacyUnitPolicy(imaging_unit_policy)
    if (identical(unit_policy$dp_unit, "patient") &&
        unit_policy$patient_column %in%
          c(feature_columns, as.character(target_column))) {
      stop("The server patient identifier cannot be a validation feature or target.",
           call. = FALSE)
    }
    supplied_contract <- tolower(as.character(unlist(
      run_config[["validation-contract-sha256"]] %||% "",
      use.names = FALSE)))
    if (length(supplied_contract) != 1L || is.na(supplied_contract) ||
        !grepl("^[0-9a-f]{64}$", supplied_contract)) {
      stop("Validation requires one canonical contract SHA-256 pin.",
           call. = FALSE)
    }
    actual_contract <- .validationContractSha256(
      run_config, feature_columns, target_column, unit_policy$dp_unit,
      data_kind = data_type)
    if (!identical(supplied_contract, actual_contract)) {
      stop("Validation contract SHA-256 does not match the prepared public contract.",
           call. = FALSE)
    }
    run_config[["validation-contract-sha256"]] <- actual_contract
  }
  # CV provenance is entirely public and must be verified before a run token is
  # created or any table/descriptor is opened.
  run_config <- .verifyCrossValidationJob(
    run_config, feature_columns, target_column)
  run_token <- .generate_run_token()
  # Record the exact rollback target before any private staging begins. If the
  # best-effort on.exit deletion itself fails, an explicit cleanup/destroy retry
  # can still find and remove the partially created directory.
  handle$pending_cleanup_tokens <- unique(c(
    handle$pending_cleanup_tokens %||% character(), run_token))
  .storeHandle(handle_symbol, handle)
  admitted <- FALSE
  on.exit(if (!admitted) {
    removed <- tryCatch({
      .cleanupStaging(run_token)
      TRUE
    }, error = function(e) FALSE)
    if (removed) {
      try({
        current <- .validateHandleStaging(
          .getHandle(handle_symbol), must_exist = FALSE)
        current$pending_cleanup_tokens <- setdiff(
          current$pending_cleanup_tokens %||% character(), run_token)
        .storeHandle(handle_symbol, current)
      }, silent = TRUE)
    }
  }, add = TRUE)
  .public_privacy_runtime_bootstrap()
  num_rounds <- as.integer(run_config[["num-server-rounds"]])
  contract <- .privacy_training_contract(
    run_token, num_rounds, imaging_unit_policy)

  # From this point onward errors can be caused by private storage/content or
  # third-party decoders. Keep their text inside the node: the exterior DSI
  # response carries one constant diagnostic while on.exit still removes any
  # partial staging. Success/error and timing remain operational signals outside
  # the numeric DP transcript, as documented in ARCHITECTURE.md.
  tryCatch({
    # Load data: from descriptor, in-memory table, or file
    if (identical(handle$source, "descriptor") && !is.null(handle$descriptor)) {
      # Descriptor path: delegate staging entirely to .stageFromDescriptor,
      # which handles in_memory_df, staged_parquet, and image_bundle.
      desc <- handle$descriptor
      imaging_authorized <- NULL
      if (identical(handle$source_kind, "image_bundle") &&
          !is.null(handle$imaging_handle_symbol)) {
        if (!requireNamespace("dsImaging", quietly = TRUE)) {
          stop("Package 'dsImaging' is required for imaging handles.",
               call. = FALSE)
        }
        resolver <- utils::getFromNamespace(
          ".resolve_imaging_handle_for_consumer", "dsImaging")
        # Resolve through dsImaging again immediately before reading private
        # objects. The expected capability makes a symbol rebind fail closed.
        imaging_authorized <- resolver(
          handle$imaging_handle_symbol,
          expected_capability = handle$imaging_handle_capability,
          owner_env = owner_env)
        # Consume the current descriptor returned through dsImaging's pinned
        # collection boundary. The private snapshot never enters the workspace
        # object or an Aggregate result.
        desc <- as_flower_dataset(imaging_authorized$descriptor)
        desc$backend <- imaging_authorized$backend
        desc$manifest_uri <- imaging_authorized$manifest_uri
        desc$.collection_snapshot <- imaging_authorized$collection_snapshot
      }
      staging_dir <- .stageFromDescriptor(
        desc, run_token, target_column, feature_columns, run_config)

      # Read the server-authored structural counts from the staged manifest.
      manifest_path <- file.path(staging_dir, "manifest.json")
      staged_manifest <- jsonlite::fromJSON(
        manifest_path, simplifyVector = TRUE)

      if (!is.null(imaging_authorized)) {
        # Close the read-time race: dsImaging checks the publish lock and the
        # original admitted roster once more, then dsFlower verifies that the
        # exact sample-to-patient mapping it staged is that same roster.
        resolver(
          handle$imaging_handle_symbol,
          expected_capability = handle$imaging_handle_capability,
          owner_env = owner_env)
        staged_data <- .readStagedSamples(file.path(
          staging_dir, staged_manifest$samples_file))
        privacy <- imaging_authorized$privacy
        assert_roster <- utils::getFromNamespace(
          ".assert_exact_imaging_roster", "dsImaging")
        assert_roster(
          staged_data[[privacy$id_col]],
          imaging_authorized$privacy_roster,
          privacy_ids = staged_data[[privacy$privacy_unit_col]],
          context = "staged imaging data")
      }

      data_type <- staged_manifest$data_type %||% "tabular"
      .enforceDisclosureAndDp(
        handle, target_column, staged_manifest$n_samples,
        NULL, run_config, data_type = data_type,
        n_units = staged_manifest$n_units)

      .apply_privacy_contract(staging_dir, contract)

      handle$run_token       <- run_token
      handle$staging_dir     <- staging_dir
      handle$target_column   <- target_column
      handle$feature_columns <- feature_columns
      handle$prepared        <- TRUE
      if (!is.null(previous_run_token) &&
          !identical(previous_run_token, run_token)) {
        .cleanupStaging(previous_run_token)
      }
      handle$pending_cleanup_tokens <- setdiff(
        handle$pending_cleanup_tokens %||% character(), run_token)
      admitted <- TRUE
      return(.storeHandle(handle_symbol, handle))
    }

    if (identical(handle$source, "table") && !is.null(handle$table_data)) {
      data <- handle$table_data
    } else {
      data <- .loadTrainingData(handle$data_path, handle$data_format)
    }
    .validateDataSchema(data, target_column, feature_columns)

    # Non-descriptor handles are always tabular. Image collections must cross
    # the dsImaging admission boundary above.
    staging_dir <- .stageData(
      data, run_token, target_column, feature_columns, run_config)

    staged_manifest <- jsonlite::fromJSON(
      file.path(staging_dir, "manifest.json"), simplifyVector = TRUE)
    .enforceDisclosureAndDp(
      handle, target_column, staged_manifest$n_samples,
      NULL, run_config, data_type = data_type,
      n_units = staged_manifest$n_units)
    .apply_privacy_contract(staging_dir, contract)

    handle$run_token       <- run_token
    handle$staging_dir     <- staging_dir
    handle$target_column   <- target_column
    handle$feature_columns <- feature_columns
    handle$prepared        <- TRUE

    if (!is.null(previous_run_token) &&
        !identical(previous_run_token, run_token)) {
      .cleanupStaging(previous_run_token)
    }
    handle$pending_cleanup_tokens <- setdiff(
      handle$pending_cleanup_tokens %||% character(), run_token)
    admitted <- TRUE
    .storeHandle(handle_symbol, handle)
  }, error = function(e) {
    stop("Private data preparation failed on this node; contact the node ",
         "administrator.", call. = FALSE)
  })
}

#' Ensure SuperNode is Running
#'
#' DataSHIELD ASSIGN method. After connectivity and manifest validation, it
#' idempotently confirms the stateless privacy contract established by
#' \code{flowerPrepareRunDS} before any SuperNode can perform private computation.
#' It then uses the singleton registry to ensure exactly one SuperNode per
#' SuperLink address.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param superlink_address Character; the SuperLink address (host:port).
#' @param federation_id Character or NULL; unique token identifying the
#'   SuperLink instance. Used by the client to verify all nodes joined the
#'   same federation.
#' @param ca_cert_pem Character or NULL; B64-encoded CA certificate PEM for
#'   TLS verification. The SuperNode uses \code{--root-certificates} to
#'   verify the SuperLink's identity.
#' @param torch_backend Character or NULL; requested CPU/GPU backend. The node
#'   validates availability and applies its own backend policy.
#' @return Updated handle with SuperNode information.
#' @export
flowerEnsureSuperNodeDS <- function(handle_symbol, superlink_address,
                                     federation_id = NULL,
                                     ca_cert_pem = NULL,
                                     torch_backend = NULL) {
  .dsflower_require_literal_arguments()
  handle <- .getHandle(handle_symbol)

  # Per-run torch backend the researcher requested (cpu/gpu/auto). Recorded so
  # .resolve_backend / .framework_venv select the cpu vs gpu venv for THIS run's
  # SuperNode (default NULL -> "auto"). The node still validates GPU availability
  # (.gpu_present) -- the client can REQUEST gpu but only a node that actually has
  # one, with its CUDA venv built, will run it.
  if (!is.null(torch_backend)) {
    tb <- .ds_arg(torch_backend)
    if (is.list(tb)) tb <- tb[[1]]
    tb <- as.character(tb)[1]
    .dsflower_runtime$torch_backend <- if (!is.na(tb) && nzchar(tb)) tb else NULL
  } else {
    .dsflower_runtime$torch_backend <- NULL
  }

  if (!handle$prepared) {
    stop("Handle is not prepared. Call flowerPrepareRunDS first.", call. = FALSE)
  }
  handle <- .validateHandleStaging(handle, required = TRUE)

  # DSI tunnel transport: the node-local tunnel forwarder (flowerTunnelUpDS)
  # carries the SuperNode<->SuperLink bytes over DataSHIELD, so the SuperNode
  # dials its own loopback forwarder and runs insecure. The outer DSI connector
  # is the transport-security boundary; dsFlowerClient requires HTTPS by default
  # and makes any plaintext exception explicit. This is the only transport -- no
  # Tor or tailnet is assumed by the protocol.
  # A cached port is not authorization. The exact capability-bound forwarder
  # must still be alive and must have published readiness after binding.
  tunnel_port <- .active_tunnel_port()
  via_tunnel <- !is.null(tunnel_port)
  if (via_tunnel) {
    superlink_address <- paste0("127.0.0.1:", tunnel_port)
  }

  # SuperLink pinning: if the operator pinned a coordinator on this node, a
  # client must NOT be able to redirect this node's SuperNode to a rogue
  # SuperLink (which would harvest its model updates).
  pinned_addr <- .dsf_option("coordinator_address",
                             Sys.getenv("DSFLOWER_COORDINATOR_ADDRESS", ""))
  if (!is.null(pinned_addr) && nzchar(pinned_addr) &&
      !identical(superlink_address, pinned_addr)) {
    stop("Refusing SuperNode: the requested coordinator does not match the ",
         "node-pinned coordinator.", call. = FALSE)
  }

  # Coordinator-trust gate: the current runner applies node-side central DP but does not
  # implement Flower SecAgg. A client-supplied manifest flag is therefore never
  # evidence of secure aggregation; only the node administrator may opt into a
  # public coordinator that can observe each already-DP node update.
  operator_authorized <- isTRUE(via_tunnel) ||
    (!is.null(pinned_addr) && nzchar(pinned_addr) &&
       identical(superlink_address, pinned_addr))
  if (!operator_authorized &&
      !isTRUE(as.logical(.dsf_option("allow_untrusted_coordinator", FALSE)))) {
    stop("Refusing SuperNode: the coordinator is not operator-authorized and ",
         "this runner has no Secure Aggregation. It could ",
         "observe each already-DP node update. Set the server-only option ",
         "dsflower.allow_untrusted_coordinator=TRUE only after accepting that ",
         "threat model.", call. = FALSE)
  }

  # Pin the trusted runner for the default-deny code-integrity hook
  # (sitecustomize.py; ARCHITECTURE.md §7). The node writes the hash of its own
  # node-resident canonical runner; the submitted FAB's `dsflower_runner`
  # package may run only if it is byte-identical to it. This is the content-hash
  # verification that makes the trusted training loop guaranteed, without trusting
  # the researcher who provisioned the app.
  harness_hash <- tryCatch(
    suppressWarnings(suppressMessages(.compute_harness_hash())),
    error = function(e) "")
  if (!nzchar(harness_hash)) {
    stop("The canonical runner (dsflower_runner) is not installed on this node.",
         call. = FALSE)
  }
  pins_path <- file.path(handle$staging_dir, "pinned_packages.json")
  if (file.exists(pins_path)) {
    pinned <- tryCatch(
      jsonlite::fromJSON(pins_path, simplifyVector = FALSE),
      error = function(e) NULL)
    if (!is.list(pinned) ||
        !identical(as.character(pinned[["dsflower_runner"]] %||% ""),
                   harness_hash)) {
      stop("Prepared package pins do not match the canonical runner.",
           call. = FALSE)
    }
  } else {
    pins_tmp <- tempfile(pattern = ".pinned-packages-",
                         tmpdir = handle$staging_dir)
    on.exit(unlink(pins_tmp), add = TRUE)
    jsonlite::write_json(
      list(dsflower_runner = harness_hash), pins_tmp, auto_unbox = TRUE)
    Sys.chmod(pins_tmp, "0600")
    if (!file.rename(pins_tmp, pins_path)) {
      stop("Could not atomically write the canonical runner pin.",
           call. = FALSE)
    }
  }

  # Decode ca_cert_pem if B64-encoded from DSI transport
  ca_cert_pem <- .ds_arg(ca_cert_pem)
  ca_cert_path <- NULL

  if (!is.null(ca_cert_pem)) {
    pem_text <- if (is.list(ca_cert_pem)) ca_cert_pem$pem else ca_cert_pem
    if (!is.null(pem_text) && nzchar(pem_text)) {
      ca_cert_path <- file.path(handle$staging_dir, "ca.pem")
      writeLines(pem_text, ca_cert_path)
    }
  }

  # Egress preflight: confirm this node can actually reach the SuperLink
  # before spawning a SuperNode that would otherwise fail to connect
  # silently and time out 30s later on the client. When the DSI tunnel is
  # active the address is the node-local loopback forwarder WE created, so
  # there is nothing external to probe.
  if (via_tunnel) {
    # The tunnel forwarder accepts exactly one connection (the SuperNode); a
    # probe here would consume that slot, so trust it (we just started it).
    conn_check <- list(reachable = TRUE)
  } else {
    conn_check <- flowerCheckConnectivityDS(superlink_address)
  }
  if (!isTRUE(conn_check$reachable)) {
    stop("This node cannot reach the configured SuperLink. ",
         "Open outbound access from this server (the DSI tunnel transport ",
         "is the DataSHIELD tunnel, so this path is normally unused).",
         call. = FALSE)
  }

  # Revalidate the root secret and stateless policy after every non-private
  # preflight, but before a ClientApp can release a model.
  .public_privacy_runtime_bootstrap()
  manifest_path <- file.path(handle$staging_dir, "manifest.json")
  manifest <- tryCatch(
    jsonlite::fromJSON(manifest_path, simplifyVector = FALSE),
    error = function(e) stop("Could not read the prepared privacy manifest.",
                             call. = FALSE))
  num_rounds <- suppressWarnings(as.integer(
    manifest[["num-server-rounds"]] %||% NA_integer_))
  imaging_unit_policy <- if (identical(handle$source, "descriptor") &&
      identical(handle$source_kind, "image_bundle")) {
    .imagingPrivacyUnitPolicy(handle$descriptor)
  } else {
    NULL
  }
  contract <- .privacy_training_contract(
    handle$run_token, num_rounds, imaging_unit_policy)
  .apply_privacy_contract(handle$staging_dir, contract)

  # Ensure SuperNode via singleton registry
  entry <- tryCatch(
    suppressWarnings(suppressMessages(.supernode_ensure(
      superlink_address = superlink_address,
      manifest_dir      = handle$staging_dir,
      python_path       = handle$python_path,
      ca_cert_path      = ca_cert_path,
      insecure          = via_tunnel
    ))),
    error = function(e) stop("SuperNode is unavailable.", call. = FALSE))

  handle$superlink_address <- superlink_address
  handle$federation_id     <- federation_id
  handle$ca_cert_path      <- ca_cert_path
  handle$node_ensured      <- TRUE

  .storeHandle(handle_symbol, handle)
}

#' Clean Up Run Staging
#'
#' DataSHIELD ASSIGN method. Removes staging directory and resets
#' handle state. Stops the per-run SuperNode before deleting staging.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return Updated handle with reset state.
#' @export
flowerCleanupRunDS <- function(handle_symbol) {
  .dsflower_require_literal_arguments()
  # An interrupted earlier cleanup may already have removed the exact staging
  # directory while leaving this private handle state intact. Validate its
  # canonical location without requiring it to remain present so retry converges.
  handle <- .validateHandleStaging(
    .getHandle(handle_symbol), must_exist = FALSE)

  # Stop SuperNode if associated. This must happen before staging deletion
  # because orphan cleanup uses the manifest_dir embedded in the process args.
  if (!is.null(handle$staging_dir)) {
    .supernode_stop(handle$staging_dir)
  }

  # Clean up staging
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }
  handle <- .cleanupPendingStaging(handle)

  handle$run_token       <- NULL
  handle$staging_dir     <- NULL
  handle$target_column   <- NULL
  handle$feature_columns <- NULL
  handle$prepared        <- FALSE
  handle$node_ensured    <- FALSE

  .storeHandle(handle_symbol, handle)
}

#' Destroy Flower Handle
#'
#' DataSHIELD ASSIGN method. Full cleanup: removes staging, stops
#' the associated SuperNode, and removes the handle. A retry is idempotent only
#' when the same session still contains the well-formed opaque reference after
#' its private registry entry has already been removed.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return NULL.
#' @export
flowerDestroyDS <- function(handle_symbol) {
  .dsflower_require_literal_arguments()
  owner_env <- parent.frame()
  unavailable <- function() {
    stop("Unknown or unavailable Flower handle reference.", call. = FALSE)
  }
  if (!is.environment(owner_env) ||
      !is.character(handle_symbol) || length(handle_symbol) != 1L ||
      is.na(handle_symbol) || !nzchar(handle_symbol) ||
      !exists(handle_symbol, envir = owner_env, inherits = FALSE)) {
    unavailable()
  }
  reference <- get(handle_symbol, envir = owner_env, inherits = FALSE)
  if (!.is_handle_reference(reference) ||
      bindingIsLocked(handle_symbol, owner_env)) {
    unavailable()
  }
  capability <- .validate_handle_capability(reference$capability)
  entry <- .handle_registry[[capability]]
  if (is.null(entry)) {
    # Idempotent retry after authoritative state was removed but the session
    # symbol could not be cleared (for example, a lost destroy response).
    rm(list = handle_symbol, envir = owner_env)
    return(NULL)
  }
  if (!is.list(entry) || !is.list(entry$handle) ||
      !is.environment(entry$owner_env) ||
      !identical(entry$owner_env, owner_env)) {
    unavailable()
  }
  # Accept the same safe partial-cleanup state as flowerCleanupRunDS(): the
  # directory may be gone, but its token/path binding must still be exact.
  handle <- .validateHandleStaging(entry$handle, must_exist = FALSE)

  # Stop SuperNode if associated
  if (!is.null(handle$staging_dir)) {
    .supernode_stop(handle$staging_dir)
  }
  # Clean up staging
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }
  handle <- .cleanupPendingStaging(handle)

  .handle_registry[[capability]] <- NULL
  rm(list = handle_symbol, envir = owner_env)
  NULL
}

# --- AGGREGATE methods ---

#' Ping Health Check
#'
#' DataSHIELD AGGREGATE method. Returns a simple health check confirming
#' the dsFlower package is loaded and operational.
#'
#' @return Named list with status, version, timestamp.
#' @export
flowerPingDS <- function() {
  .dsflower_require_literal_arguments()
  list(
    status = "ok",
    package = "dsFlower",
    version = as.character(utils::packageVersion("dsFlower")),
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
}

#' Get Server Capabilities
#'
#' DataSHIELD AGGREGATE method. Returns information about the server's
#' Flower capabilities including Python version, the hash-pinned declarative
#' runner vocabulary, and disclosure settings. The response is independent of
#' cohort contents, handle state, and other sessions, and does not disclose
#' filesystem paths. Native-tree and association availability are probed only
#' when explicitly requested. Resampling advertises atomic holdout for tabular
#' neural/native-tree runs and native dsFlower neural vision; cross-validation
#' remains tabular for the neural and native-tree tracks.
#'
#' @param native_tree_probe Exactly \code{"none"} (the default), \code{"all"},
#'   or one implemented native-tree engine name. This controls an operational
#'   readiness check only; it is not a privacy permission or operation catalog.
#' @param association_probe Exactly \code{"none"} (the default) or
#'   \code{"runtime"}. This requests only the dependency-light association
#'   runtime probe and never provisions or imports the neural runtime.
#' @return Named list of capabilities.
#' @export
flowerGetCapabilitiesDS <- function(native_tree_probe = "none",
                                    association_probe = "none") {
  .dsflower_require_literal_arguments()
  native_tree_probe <- .validate_native_tree_probe(native_tree_probe)
  association_probe <- .validate_association_probe(association_probe)
  runtime <- tryCatch(
    suppressWarnings(suppressMessages(.python_runtime_capabilities())),
    error = function(e) list(
      python_version = "unavailable",
      flower_version = "unavailable",
      torch_version = "unavailable",
      opacus_version = "unavailable",
      runtime_versions_sha256 = "unavailable"))
  runner_sha256 <- tryCatch(
    suppressWarnings(suppressMessages(.compute_harness_hash())),
    error = function(e) "unavailable")

  # Disclosure settings
  settings <- .flowerDisclosureSettings()

  privacy_policy <- .privacy_policy()
  runner_caps <- .RUNNER_PUBLIC_CAPABILITIES
  native_tree <- .native_tree_contract_capabilities(native_tree_probe)
  association <- .association_contract_capabilities(association_probe)
  hook_enabled <- isTRUE(as.logical(.dsf_option("hook_enabled", FALSE)))
  hook_sandbox <- isTRUE(as.logical(
    .dsf_option("hook_sandbox_attested", FALSE)))
  hook_resources <- isTRUE(as.logical(
    .dsf_option("hook_resource_isolation_attested", FALSE)))
  hook_timeout <- .bounded_server_number(
    "dp_egress_timeout", 900L, 1L, 3600L, integer = TRUE)
  hook_pad <- .bounded_server_number(
    "dp_egress_time_pad", 0, 0, 230405, integer = FALSE)
  hook_sample_aggregate <- isTRUE(as.logical(
    .dsf_option("dp_sample_aggregate", FALSE)))
  hook_sa_blocks <- .bounded_server_number(
    "dp_sa_blocks", 8L, 2L, 64L, integer = TRUE)
  hook_required_pad <- hook_timeout *
    (if (hook_sample_aggregate) hook_sa_blocks else 1L) + 5
  hook_time_ready <- hook_pad >= hook_required_pad

  caps <- list(
    dsflower_version    = as.character(utils::packageVersion("dsFlower")),
    python_version      = runtime$python_version,
    flower_version      = runtime$flower_version,
    torch_version       = runtime$torch_version,
    opacus_version      = runtime$opacus_version,
    runtime_versions_sha256 = runtime$runtime_versions_sha256,
    dp_tracks           = runner_caps$dp_tracks,
    declarative_model_ops = runner_caps$declarative_model_ops,
    declarative_losses  = runner_caps$declarative_losses,
    aggregation_strategies = runner_caps$aggregation_strategies,
    resampling          = runner_caps$resampling,
    native_tree         = native_tree,
    association         = association,
    max_rounds          = settings$max_rounds,
    min_samples         = .disclosure_min_rows(),
    min_clients_per_round = 1L,
    dp_required         = TRUE,
    privacy_accountant  = "stateless-per-training-v1",
    privacy_scope       = "per-training",
    privacy_per_training_epsilon = privacy_policy$per_training_epsilon,
    privacy_per_training_delta = privacy_policy$per_training_delta,
    privacy_policy_sha256 = privacy_policy$policy_hash,
    privacy_clipping_norm = .serverDpClippingNorm(),
    privacy_unit        = privacy_policy$dp_unit,
    privacy_patient_column = privacy_policy$patient_column,
    runner_abi          = 3L,
    runner_sha256       = runner_sha256,
    dp_app_schema_versions = 1L,
    hook_abi            = 2L,
    hook_enabled        = hook_enabled,
    hook_sandbox_attested = hook_sandbox,
    hook_resource_isolation_attested = hook_resources,
    hook_sample_aggregate = hook_sample_aggregate,
    hook_sa_blocks = hook_sa_blocks,
    hook_timeout_seconds = hook_timeout,
    hook_time_pad_seconds = hook_pad,
    hook_required_time_pad_seconds = hook_required_pad,
    hook_time_envelope_configured = hook_time_ready,
    hook_execution_configured = hook_enabled && hook_sandbox &&
      hook_resources && hook_time_ready
  )

  caps
}

#' Get Handle Status
#'
#' DataSHIELD AGGREGATE method. Returns the current status of the handle
#' including whether data is prepared and a SuperNode is ensured.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return Named list with status information.
#' @export
flowerStatusDS <- function(handle_symbol) {
  .dsflower_require_literal_arguments()
  handle <- .validateHandleStaging(.getHandle(handle_symbol))

  supernode_running <- FALSE
  if (!is.null(handle$staging_dir)) {
    entry <- .supernode_lookup(handle$staging_dir)
    supernode_running <- !is.null(entry)
  }

  list(
    prepared           = handle$prepared,
    node_ensured       = handle$node_ensured,
    supernode_running  = supernode_running,
    superlink_address  = handle$superlink_address,
    federation_id      = handle$federation_id,
    target_column      = handle$target_column,
    feature_columns    = handle$feature_columns
  )
}

#' Query the server-owned stateless privacy policy
#'
#' Returns only public, administrator-pinned per-training values.
#' @return Named list describing the server-owned per-training privacy policy.
#' @export
flowerPrivacyPolicyDS <- function() {
  .dsflower_require_literal_arguments()
  .privacy_policy_status()
}

#' Check TCP connectivity from this node to a given address
#'
#' DataSHIELD AGGREGATE method. Attempts a TCP connection to the specified
#' host:port to verify the SuperLink is reachable from this Opal/Rock.
#'
#' @param address Character; "host:port" to test.
#' @param timeout_secs Numeric; connection timeout in seconds (default 5).
#' @return Named list with \code{reachable} (logical) and \code{error} (char).
#' @export
flowerCheckConnectivityDS <- function(address, timeout_secs = 3) {
  .dsflower_require_literal_arguments()
  parts <- strsplit(address, ":", fixed = TRUE)[[1]]
  if (length(parts) != 2) {
    return(list(reachable = FALSE,
                error = "Invalid address format, expected host:port"))
  }
  host <- parts[1]
  port <- suppressWarnings(as.integer(parts[2]))
  if (is.na(port) || port < 1L || port > 65535L) {
    return(list(reachable = FALSE, error = "Invalid port"))
  }

  # Cap the timeout so this cannot be turned into a slow connection-holding
  # primitive (callers cannot tune it upward).
  timeout_secs <- min(suppressWarnings(as.numeric(timeout_secs)), 5)
  if (is.na(timeout_secs) || timeout_secs <= 0) timeout_secs <- 3

  # Authorization: this method makes the node open an outbound socket, so it
  # must not become an internal port scanner (SSRF). When a coordinator is
  # configured, ONLY that endpoint may be probed; otherwise refuse private,
  # loopback and link-local targets.
  allowed <- c(.dsf_option("coordinator_address",
                           Sys.getenv("DSFLOWER_COORDINATOR_ADDRESS", "")),
               .dsf_option("coordinator_control_address",
                           Sys.getenv("DSFLOWER_COORDINATOR_CONTROL_ADDRESS", "")))
  allowed <- allowed[!is.null(allowed) & nzchar(allowed %||% "")]
  restrict <- isTRUE(as.logical(.dsf_option("restrict_connectivity", TRUE)))
  if (length(allowed) > 0) {
    if (!address %in% allowed) {
      return(list(reachable = FALSE,
                  error = "Connectivity checks are restricted to the configured coordinator address."))
    }
  } else if (restrict && .is_private_or_local_host(host)) {
    return(list(reachable = FALSE,
                error = paste0("Connectivity checks to private/loopback/link-local hosts are ",
                               "not allowed. Pin dsflower.coordinator_address, or set ",
                               "dsflower.restrict_connectivity=FALSE for trusted local dev.")))
  }

  .probe_tcp(host, port, timeout_secs)
}

#' Raw TCP reachability probe (no SSRF guard)
#'
#' Internal helper: opens and immediately closes a socket to host:port.
#' Callers are responsible for authorizing the target. flowerCheckConnectivityDS
#' wraps this with the anti-SSRF restriction for client-facing use; the egress
#' preflight in flowerEnsureSuperNodeDS calls it directly on the loopback overlay
#' forwarder it created itself (a legitimate, non-client-controlled target).
#' @keywords internal
.probe_tcp <- function(host, port, timeout_secs = 3) {
  # For TLS ports the TCP handshake succeeds even though the TLS handshake
  # won't complete -- that's fine, we only need to verify the port is reachable.
  tryCatch({
    con <- suppressWarnings(
      socketConnection(host = host, port = port,
                       open = "wb", blocking = TRUE,
                       timeout = timeout_secs)
    )
    close(con)
    list(reachable = TRUE, error = NULL)
  }, warning = function(w) {
    list(reachable = FALSE, error = "TCP connection failed.")
  }, error = function(e) {
    list(reachable = FALSE, error = "TCP connection failed.")
  })
}


#' Compute the canonical SHA-256 hash of the node-resident Tier-1 harness
#'
#' Hashes the \code{dsflower_runner} Python package shipped with this node
#' package, byte-for-byte identically to \code{_hash_package} in
#' sitecustomize.py and \code{.hash_pkg_dir}: forward-slash relative
#' paths, radix sort, each as relpath + "\\n" + content + "\\x00", excluding
#' compiled artifacts. Used to pin the trusted runner for code verification.
#' @return Character; hex SHA-256, or "" if the runner is not installed.
#' @keywords internal
.compute_harness_hash <- function() {
  pkg_dir <- system.file("flower_app", "dsflower_runner", package = "dsFlower")
  if (!nzchar(pkg_dir) || !dir.exists(pkg_dir)) return("")
  rel_files <- list.files(pkg_dir, recursive = TRUE, full.names = FALSE,
                          all.files = TRUE, no.. = TRUE)
  rel_files <- rel_files[!grepl("(^|/)__pycache__(/|$)", rel_files)]
  rel_files <- rel_files[!grepl("\\.(pyc|pyo)$", rel_files)]
  rel_files <- sort(rel_files, method = "radix")
  blob <- raw(0)
  for (rel in rel_files) {
    full <- file.path(pkg_dir, rel)
    content <- readBin(full, "raw", file.info(full)$size)
    blob <- c(blob, charToRaw(rel), charToRaw("\n"), content, as.raw(0x00))
  }
  digest::digest(blob, algo = "sha256", serialize = FALSE)
}
