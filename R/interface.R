# Module: DataSHIELD Exposed Methods
# All DataSHIELD assign/aggregate methods for Flower federated learning.

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
  if (!any(vapply(frames, identical, logical(1), y = .GlobalEnv))) {
    frames <- c(frames, list(.GlobalEnv))
  }
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

.validateHandleStaging <- function(handle, required = FALSE) {
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
      handle$staging_dir, handle$run_token, must_exist = TRUE)
  }
  handle
}

#' Remove authoritative state for an opaque Flower handle
#' @keywords internal
.removeHandle <- function(symbol, cleanup = TRUE) {
  resolved <- .resolveHandle(symbol)
  if (isTRUE(cleanup)) {
    handle <- .validateHandleStaging(resolved$handle)
    if (!is.null(handle$run_token)) .cleanupStaging(handle$run_token)
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
  # Initialize persistent privacy state before the first dsFlower operation
  # inspects a session object. This runs only in a live service/session, never
  # from package installation or namespace loading, and consumes no allocation.
  .privacy_runtime_bootstrap()

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

  # Descriptor path: FlowerDatasetDescriptor or ResourceClient
  if (inherits(obj, "FlowerDatasetDescriptor")) {
    return(.registerHandle(
      .createHandleFromDescriptor(obj, data_symbol = data_symbol), owner_env))
  }

  if (inherits(obj, "ResourceClient")) {
    desc <- as_flower_dataset(obj)
    return(.registerHandle(
      .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
  }

  # Imaging handle path: list with descriptor field (from imagingInitDS)
  # Note: S3 classes may be lost during Opal serialization, so we check
  # structurally rather than by class.
  if (is.list(obj) && !is.null(obj$descriptor)) {
    desc <- obj$descriptor
    # Carry backend from imaging handle to descriptor (needed for S3 staging)
    if (!is.null(obj$backend) && is.null(desc$backend)) {
      desc$backend <- obj$backend
    }
    if (inherits(desc, "FlowerDatasetDescriptor")) {
      return(.registerHandle(
        .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
    }
    if (is.list(desc) && !is.null(desc$dataset_id) &&
        !is.null(desc$source_kind)) {
      desc <- flower_dataset_descriptor(
        dataset_id  = desc$dataset_id,
        source_kind = desc$source_kind,
        metadata    = desc$metadata,
        assets      = desc$assets %||% list(),
        manifest    = desc$manifest,
        table_data  = desc$table_data
      )
      desc$backend <- obj$backend
      return(.registerHandle(
        .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
    }
  }

  # Raw Resource path: list with imaging+dataset:// URL (from datashield.assign.resource)
  if (is.list(obj) && !is.null(obj$url) &&
      grepl("^imaging\\+dataset://", obj$url %||% "")) {
    if (requireNamespace("dsImaging", quietly = TRUE)) {
      dataset_id <- sub("^imaging\\+dataset://", "", strsplit(obj$url, "\\?")[[1]][1])
      resolve_dataset <- utils::getFromNamespace("resolve_dataset", "dsImaging")
      parse_manifest <- utils::getFromNamespace("parse_manifest", "dsImaging")
      resolved <- resolve_dataset(dataset_id)
      manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
      desc <- dsImaging::imaging_dataset_descriptor(manifest)
      return(.registerHandle(
        .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
    }
  }

  # Asset reference path: list with asset_ref (from ds.flower.nodes.init inputs)
  if (is.list(obj) && !is.null(obj$asset_ref)) {
    if (requireNamespace("dsImaging", quietly = TRUE)) {
      aref <- obj$asset_ref
      asset_info <- dsImaging::resolve_feature_table_asset(
        aref$dataset_id, aref$alias_or_id)
      desc <- flower_dataset_descriptor(
        dataset_id = asset_info$dataset_id,
        source_kind = "asset_ref",
        asset_info = asset_info)
      return(.registerHandle(
        .createHandleFromDescriptor(desc, data_symbol = data_symbol), owner_env))
    }
  }

  stop("Symbol '", data_symbol, "' is not a data.frame, matrix, ",
       "FlowerDatasetDescriptor, ResourceClient, or imaging handle. ",
       "Assign your data first with datashield.assign.table(), ",
       "imagingInitDS(), or similar.",
       call. = FALSE)
}

# Normalize the client-requested Flower horizon to one server-pinned key.  The
# aliases are accepted only for compatibility and may not disagree.
.normalizeRunHorizon <- function(run_config) {
  parse_rounds <- function(value, field) {
    if (is.null(value)) return(NULL)
    value <- suppressWarnings(as.numeric(unlist(value, use.names = FALSE)))
    if (length(value) != 1L || !is.finite(value) || value < 1 ||
        value != floor(value) || value > .Machine$integer.max) {
      stop(field, " must be a positive integer.", call. = FALSE)
    }
    as.integer(value)
  }
  modern <- parse_rounds(run_config[["num-server-rounds"]],
                         "num-server-rounds")
  legacy <- parse_rounds(run_config[["num_rounds"]], "num_rounds")
  if (!is.null(modern) && !is.null(legacy) && modern != legacy) {
    stop("num-server-rounds and num_rounds disagree.", call. = FALSE)
  }
  rounds <- modern %||% legacy %||% 1L
  rounds <- .validateMaxRounds(rounds)
  run_config[["num-server-rounds"]] <- rounds
  run_config[["num_rounds"]] <- NULL
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

.normalizeValidationConfig <- function(run_config, track) {
  fields <- names(run_config)[startsWith(tolower(names(run_config)),
                                          "validation-")]
  if (!identical(track, "validation")) {
    if (length(fields)) {
      stop("validation-* fields require dp-track='validation'.",
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
  task <- tolower(as.character(unlist(
    run_config[["validation-task"]] %||% "", use.names = FALSE)))
  bins <- suppressWarnings(as.numeric(unlist(
    run_config[["validation-bins"]] %||% 32L, use.names = FALSE)))
  if (length(model_track) != 1L || is.na(model_track) ||
      !model_track %in% c("neural", "trees")) {
    stop("validation-model-track must be neural or trees.", call. = FALSE)
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
  if (identical(model_track, "trees") &&
      !task %in% c("binary", "regression")) {
    stop("The current dp_gbdt validator supports binary or regression tasks.",
         call. = FALSE)
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
  expected_task <- if (identical(model_track, "trees")) {
    if (identical(loss, "bce_logits")) "binary" else
      if (identical(loss, "mse")) "regression" else ""
  } else {
    switch(loss,
      bce_logits = "binary",
      cross_entropy = if (n_classes > 2L) "multiclass" else "binary",
      hinge = if (n_classes > 2L) "multiclass" else "binary",
      ordinal = "ordinal", multilabel_bce = "multilabel",
      mse = "regression", huber = "regression", gamma_nll = "regression",
      poisson_nll = "count", negbin_nll = "count", "")
  }
  if (!nzchar(expected_task) || !identical(task, expected_task)) {
    stop("validation-task disagrees with the pinned model loss.",
         call. = FALSE)
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

.validationContractSha256 <- function(run_config, feature_columns,
                                      target_column, privacy_unit) {
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
  canonical <- as.character(jsonlite::toJSON(
    payload, auto_unbox = TRUE, null = "null", na = "null",
    digits = NA, always_decimal = TRUE, pretty = FALSE))
  digest::digest(charToRaw(enc2utf8(canonical)), algo = "sha256",
                 serialize = FALSE)
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
  } else if (identical(track, "trees")) {
    spec <- run_config[["gbdt-spec"]]
    if (!is.list(spec)) {
      stop("The trees track requires a declarative gbdt-spec object.",
           call. = FALSE)
    }
    objective <- as.character(unlist(
      spec$objective %||% "", use.names = FALSE))
    if (length(objective) != 1L || is.na(objective) ||
        !objective %in% c("binary:logistic", "reg:squarederror")) {
      stop("gbdt-spec objective must be binary:logistic or reg:squarederror.",
           call. = FALSE)
    }
    inferred <- if (identical(objective, "reg:squarederror")) {
      "regression"
    } else "classification"
  } else if (identical(track, "neural")) {
    loss_name <- tolower(as.character(unlist(
      run_config[["loss-name"]] %||% "bce_logits", use.names = FALSE)))
    if (length(loss_name) != 1L || is.na(loss_name)) {
      stop("loss-name must be one scalar value.", call. = FALSE)
    }
    inferred <- switch(
      loss_name,
      mse = "regression", huber = "regression", gamma_nll = "regression",
      poisson_nll = "count", negbin_nll = "count",
      "classification")
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

.normalizeGbdtConfig <- function(run_config, track) {
  spec <- run_config[["gbdt-spec"]] %||% NULL
  if (!identical(track, "trees")) {
    if (!is.null(spec)) {
      stop("gbdt-spec is only valid for the trees track.", call. = FALSE)
    }
    return(run_config)
  }
  if (!is.list(spec) || is.null(names(spec)) || any(!nzchar(names(spec))) ||
      anyDuplicated(names(spec))) {
    stop("gbdt-spec must be a uniquely named object.", call. = FALSE)
  }
  allowed <- c(
    "objective", "max_depth", "n_trees", "learning_rate", "reg_lambda",
    "n_bins", "feature_ranges", "target_bounds", "margin_bounds",
    "gradient_clip")
  unknown <- setdiff(names(spec), allowed)
  if (length(unknown)) {
    stop("Unknown gbdt-spec field(s): ", paste(unknown, collapse = ", "),
         ".", call. = FALSE)
  }
  required <- c(
    "objective", "max_depth", "n_trees", "learning_rate", "reg_lambda",
    "n_bins")
  missing <- setdiff(required, names(spec))
  if (length(missing)) {
    stop("gbdt-spec is missing field(s): ", paste(missing, collapse = ", "),
         ".", call. = FALSE)
  }
  scalar <- function(name, lower, upper, integer = FALSE,
                     lower_open = FALSE) {
    value <- suppressWarnings(as.numeric(unlist(spec[[name]], use.names = FALSE)))
    valid <- length(value) == 1L && is.finite(value) &&
      if (lower_open) value > lower else value >= lower
    valid <- valid && value <= upper && (!integer || value == floor(value))
    if (!valid) {
      stop("gbdt-spec ", name, " is outside its allowed public range.",
           call. = FALSE)
    }
    if (integer) as.integer(value) else value
  }
  spec$max_depth <- scalar("max_depth", 1, 6, integer = TRUE)
  spec$n_trees <- scalar("n_trees", 1, 200, integer = TRUE)
  spec$learning_rate <- scalar("learning_rate", 0, 10, lower_open = TRUE)
  spec$reg_lambda <- scalar("reg_lambda", 0, 1e6, lower_open = TRUE)
  spec$n_bins <- scalar("n_bins", 2, 64, integer = TRUE)

  interval <- function(value, field) {
    value <- suppressWarnings(as.numeric(unlist(value, use.names = FALSE)))
    if (length(value) != 2L || any(!is.finite(value)) || value[[1L]] >= value[[2L]] ||
        any(abs(value) > .DSFLOWER_FLOAT32_SAFE_MAX)) {
      stop("gbdt-spec ", field,
           " must be finite [lower, upper] public bounds.", call. = FALSE)
    }
    unname(value)
  }
  if (!is.null(spec$feature_ranges)) {
    if (!is.list(spec$feature_ranges) || !length(spec$feature_ranges) ||
        length(spec$feature_ranges) > 65536L) {
      stop("gbdt-spec feature_ranges must be a bounded list of intervals.",
           call. = FALSE)
    }
    n_features <- suppressWarnings(as.numeric(unlist(
      run_config[["num-features"]] %||% NA_real_, use.names = FALSE)))
    if (length(n_features) != 1L || !is.finite(n_features) ||
        n_features < 1 || n_features != floor(n_features) ||
        length(spec$feature_ranges) != n_features) {
      stop("gbdt-spec feature_ranges must match the pinned num-features.",
           call. = FALSE)
    }
    spec$feature_ranges <- lapply(
      spec$feature_ranges, interval, field = "feature_ranges")
  }
  objective <- as.character(spec$objective)
  if (identical(objective, "reg:squarederror")) {
    spec$target_bounds <- interval(spec$target_bounds, "target_bounds")
    pinned <- run_config[["target-bounds"]]
    pinned <- c(pinned$lower, pinned$upper)
    if (!identical(as.numeric(spec$target_bounds), as.numeric(pinned))) {
      stop("gbdt-spec target_bounds disagree with the pinned target-bounds.",
           call. = FALSE)
    }
    spec$margin_bounds <- interval(
      spec$margin_bounds %||% spec$target_bounds, "margin_bounds")
    if (!is.null(spec$gradient_clip)) {
      spec$gradient_clip <- scalar(
        "gradient_clip", 0, 2e6, lower_open = TRUE)
    }
  } else if (!is.null(spec$target_bounds) || !is.null(spec$margin_bounds) ||
             !is.null(spec$gradient_clip)) {
    stop("Regression bounds/clips are invalid for binary:logistic.",
         call. = FALSE)
  }
  run_config[["gbdt-spec"]] <- spec
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
# when the persistent accountant reserves before private staging.
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

.addDpConfigToRunConfig <- function(run_config) {
  run_config <- .validate_client_run_config(run_config)
  run_config <- .normalizeRunHorizon(run_config)
  track <- as.character(unlist(
    run_config[["dp-track"]] %||% "neural", use.names = FALSE))
  if (length(track) != 1L || is.na(track) ||
      !tolower(track) %in% c("neural", "trees", "egress", "validation")) {
    stop("dp-track must be one of neural, trees, egress, or validation.",
         call. = FALSE)
  }
  track <- tolower(track)
  run_config[["dp-track"]] <- track
  run_config <- .normalizeValidationConfig(run_config, track)
  run_config <- .normalizePinnedTaskType(run_config, track)
  run_config <- .normalizeHookAppParams(run_config, track)
  run_config <- .normalizePublicFeatureBounds(run_config)
  run_config <- .normalizePublicTargetConfig(run_config)
  run_config <- .normalizeGbdtConfig(run_config, track)
  run_config[["dp_enabled"]]               <- TRUE
  run_config[["allow_per_node_metrics"]]   <- FALSE
  run_config[["allow_exact_num_examples"]] <- FALSE
  run_config[["fixed_client_sampling"]]    <- TRUE
  policy <- .privacy_policy()  # validate admin policy before touching private data
  max_releases <- if (track %in% c("trees", "validation")) 1L else
    as.integer(run_config[["num-server-rounds"]])
  run_config[["privacy-domain"]] <- policy$domain
  run_config[["privacy-adjacency"]] <- policy$adjacency
  run_config[["privacy-reserved"]] <- FALSE
  run_config[["privacy-release-enabled"]] <- FALSE
  run_config[["privacy-max-releases"]] <- max_releases
  run_config[["privacy-epsilon"]] <- 0
  run_config[["privacy-delta"]] <- 0
  run_config[["privacy-clipping_norm"]] <- as.numeric(.dsf_option("dp_clipping_norm", 1.0))
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
# unconditional and admission never branches on a private count.
.enforceDisclosureAndDp <- function(handle, target_column, template_name,
                                    n_samples, target_data, run_config,
                                    data_type = "tabular",
                                    n_units = n_samples) {
  # Validate the server-authored structural count, but never branch on a minimum
  # derived from private rows or identifiers. A threshold would expose an exact
  # success/error predicate; tiny/empty inputs instead reach the trusted
  # mechanism without a prepare-time size disclosure.
  unit_count <- suppressWarnings(as.numeric(unlist(n_units, use.names = FALSE)))
  if (length(unit_count) != 1L || !is.finite(unit_count) || unit_count < 0 ||
      unit_count != floor(unit_count)) {
    stop("Invalid staged privacy-unit count.", call. = FALSE)
  }
  # The lifetime policy and reservation were validated before private staging.
  # The clipping bound is likewise server-owned and cannot come from the analyst.
  dp_clip <- as.numeric(run_config[["privacy-clipping_norm"]] %||% 1.0)
  clip_ceiling  <- suppressWarnings(as.numeric(.dsf_option("dp_clip_ceiling", 100)))
  if (length(dp_clip) != 1L || !is.finite(dp_clip) || dp_clip <= 0 ||
      (!is.na(clip_ceiling) && dp_clip > clip_ceiling)) {
    stop("Server DP clipping_norm must be finite, positive, and no greater than ",
         clip_ceiling, ".", call. = FALSE)
  }
  invisible(NULL)
}

#' Prepare a Training Run
#'
#' DataSHIELD ASSIGN method. Reserves the server-owned lifetime allocation,
#' and, when that allocation is numerically viable, applies total public
#' preprocessing and stages the training data. A non-viable geometric-tail
#' allocation stages only a public no-op manifest and never opens the private
#' source. No minimum-size or private-value admission bit is returned.
#'
#' @param handle_symbol Character; symbol of the initialized handle.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character; JSON-encoded feature column names, or NULL.
#' @param run_config Character; JSON-encoded additional run configuration.
#' @return Updated handle with staging information.
#' @export
flowerPrepareRunDS <- function(handle_symbol, target_column,
                                feature_columns = NULL, run_config = "{}") {
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

  handle <- .getHandle(handle_symbol)
  handle <- .validateHandleStaging(handle)

  # Clean up previous staging if any
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }

  # Validate every analyst/admin-controlled value before charging or touching
  # private data. The reservation then happens before file/table contents,
  # labels, completeness or patient identifiers are inspected. Failed attempts
  # are intentionally never refunded; the lifetime schedule remains
  # non-blocking and simply assigns the next geometrically smaller allocation.
  descriptor_data_type <- if (identical(handle$source, "descriptor")) {
    # source_kind is copied into the handle at initialization.  Do not inspect
    # the descriptor merely to route a data-free privacy-tail allocation.
    if (identical(handle$source_kind, "image_bundle")) "image" else "tabular"
  } else {
    NULL
  }
  run_config <- .addDpConfigToRunConfig(run_config)
  routed <- .takeRunDataType(run_config, expected = descriptor_data_type)
  run_config <- routed$run_config
  data_type <- routed$data_type
  columns <- .normalizePublicColumnSelection(
    target_column, feature_columns, run_config)
  target_column <- columns$target_column
  feature_columns <- columns$feature_columns
  if (identical(run_config[["dp-track"]], "validation")) {
    if (!is.character(feature_columns) || !length(feature_columns) ||
        anyNA(feature_columns) || any(!nzchar(feature_columns)) ||
        anyDuplicated(feature_columns) ||
        length(feature_columns) != run_config[["num-features"]]) {
      stop("Validation requires the explicit ordered public feature contract.",
           call. = FALSE)
    }
    unit_policy <- .dpUnitPolicy()
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
      run_config, feature_columns, target_column, unit_policy$dp_unit)
    if (!identical(supplied_contract, actual_contract)) {
      stop("Validation contract SHA-256 does not match the prepared public contract.",
           call. = FALSE)
    }
    run_config[["validation-contract-sha256"]] <- actual_contract
  }
  template_name <- run_config[["template_name"]] %||% NULL
  run_token <- .generate_run_token()
  .privacy_runtime_bootstrap()
  max_releases <- as.integer(run_config[["privacy-max-releases"]])
  reservation <- .reserve_privacy_run(run_token, max_releases)
  admitted <- FALSE
  on.exit(if (!admitted) .cleanupStaging(run_token), add = TRUE)

  if (!isTRUE(reservation$release_enabled)) {
    staging_dir <- .stagePublicNoopManifest(
      run_token, target_column, feature_columns, data_type,
      run_config, reservation)
    .apply_privacy_reservation(staging_dir, reservation)

    handle$run_token       <- run_token
    handle$staging_dir     <- staging_dir
    handle$target_column   <- target_column
    handle$feature_columns <- feature_columns
    handle$template_name   <- template_name
    handle$runtime_desc    <- NULL
    handle$prepared        <- TRUE
    admitted <- TRUE
    return(.storeHandle(handle_symbol, handle))
  }

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
      staging_dir <- .stageFromDescriptor(
        desc, run_token, target_column, feature_columns, run_config)

      # Read the server-authored structural counts from the staged manifest.
      manifest_path <- file.path(staging_dir, "manifest.json")
      staged_manifest <- jsonlite::fromJSON(
        manifest_path, simplifyVector = TRUE)

      data_type <- staged_manifest$data_type %||% "tabular"
      .enforceDisclosureAndDp(
        handle, target_column, template_name, staged_manifest$n_samples,
        NULL, run_config, data_type = data_type,
        n_units = staged_manifest$n_units)

      # Inject validated mask paths from dsImaging (segmentation tasks)
      seg_generation_id <-
        run_config[["segmentation_generation_id"]] %||% NULL
      if (!is.null(seg_generation_id) &&
          requireNamespace("dsImaging", quietly = TRUE)) {
        mask_paths <-
          dsImaging::imagingSegmentationGetMaskPaths(seg_generation_id)
        if (length(mask_paths) > 0) {
          # Create a mask root directory with symlinks to actual artifacts
          mask_root <- file.path(staging_dir, "masks")
          dir.create(mask_root, showWarnings = FALSE)
          for (sid in names(mask_paths)) {
            src <- mask_paths[[sid]]
            dst <- file.path(mask_root, basename(src))
            if (file.exists(src) && !file.exists(dst)) file.symlink(src, dst)
          }
          # Update manifest with masks asset
          staged_manifest$assets$masks <- list(
            type = "image_root",
            root = normalizePath(mask_root),
            path_col = "mask_path"
          )
          staged_manifest$segmentation_generation_id <- seg_generation_id
          .write_manifest_atomic(staged_manifest, manifest_path)
        }
      }

      # Resolve runtime descriptor: template -> framework -> venv -> paths
      runtime_desc <- NULL
      if (!is.null(template_name)) {
        runtime_desc <- .resolve_template_runtime(template_name)
        writeLines(
          jsonlite::toJSON(runtime_desc, auto_unbox = TRUE, pretty = TRUE),
          file.path(staging_dir, "runtime.json"))
      }
      .apply_privacy_reservation(staging_dir, reservation)

      handle$run_token       <- run_token
      handle$staging_dir     <- staging_dir
      handle$target_column   <- target_column
      handle$feature_columns <- feature_columns
      handle$template_name   <- template_name
      handle$runtime_desc    <- runtime_desc
      handle$prepared        <- TRUE
      admitted <- TRUE
      return(.storeHandle(handle_symbol, handle))
    }

    if (identical(handle$source, "table") && !is.null(handle$table_data)) {
      data <- handle$table_data
    } else {
      data <- .loadTrainingData(handle$data_path, handle$data_format)
    }
    .validateDataSchema(data, target_column, feature_columns)

    # Stage the total, row-preserving preprocessing selected by public config.
    staging_dir <- if (identical(data_type, "image")) {
      # Image pipeline: the data.frame is sample metadata; pixels stay on disk.
      if (!("relative_path" %in% names(data))) {
        stop("Image data requires a 'relative_path' column in the data. ",
             "The data.frame should contain sample metadata, not pixel data.",
             call. = FALSE)
      }
      .stage_image_manifest(run_token, target_column, data, run_config)
    } else {
      .stageData(data, run_token, target_column, feature_columns, run_config)
    }

    staged_manifest <- jsonlite::fromJSON(
      file.path(staging_dir, "manifest.json"), simplifyVector = TRUE)
    .enforceDisclosureAndDp(
      handle, target_column, template_name, staged_manifest$n_samples,
      NULL, run_config, data_type = data_type,
      n_units = staged_manifest$n_units)
    .apply_privacy_reservation(staging_dir, reservation)

    # Resolve runtime descriptor
    runtime_desc <- NULL
    if (!is.null(template_name)) {
      runtime_desc <- .resolve_template_runtime(template_name)
      writeLines(jsonlite::toJSON(runtime_desc, auto_unbox = TRUE, pretty = TRUE),
                 file.path(staging_dir, "runtime.json"))
    }

    handle$run_token       <- run_token
    handle$staging_dir     <- staging_dir
    handle$target_column   <- target_column
    handle$feature_columns <- feature_columns
    handle$template_name   <- template_name
    handle$runtime_desc    <- runtime_desc
    handle$prepared        <- TRUE

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
#' idempotently confirms the allocation already reserved by
#' \code{flowerPrepareRunDS} before any SuperNode can perform private computation.
#' It then uses the singleton registry to ensure exactly one SuperNode per
#' SuperLink address. Failed runs are not refunded.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param superlink_address Character; the SuperLink address (host:port).
#' @param federation_id Character or NULL; unique token identifying the
#'   SuperLink instance. Used by the client to verify all nodes joined the
#'   same federation.
#' @param ca_cert_pem Character or NULL; B64-encoded CA certificate PEM for
#'   TLS verification. The SuperNode uses \code{--root-certificates} to
#'   verify the SuperLink's identity.
#' @param template_name Deprecated compatibility argument. Named executable
#'   templates are retired; non-NULL values fail before any run-side effect.
#' @param torch_backend Character or NULL; requested CPU/GPU backend. The node
#'   validates availability and applies its own backend policy.
#' @return Updated handle with SuperNode information.
#' @export
flowerEnsureSuperNodeDS <- function(handle_symbol, superlink_address,
                                     federation_id = NULL,
                                     ca_cert_pem = NULL,
                                     template_name = NULL,
                                     torch_backend = NULL) {
  if (!is.null(template_name)) {
    stop("Named executable templates are retired. The prepared declarative DP ",
         "track selects the trusted runtime.", call. = FALSE)
  }
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

  # Resolve template_name: explicit arg > handle > NULL
  if (!is.null(template_name) && nzchar(template_name %||% "")) {
    template_name <- .ds_arg(template_name)
    if (is.list(template_name)) template_name <- template_name[[1]]
  } else if (!is.null(handle$template_name)) {
    template_name <- handle$template_name
  }

  # template_name resolved: explicit > handle > runtime.json

  # Pin the trusted runner for the default-deny code-integrity hook
  # (sitecustomize.py; ARCHITECTURE.md §7). The node writes the hash of its own
  # node-resident canonical runner; the submitted FAB's `dsflower_runner`
  # package may run only if it is byte-identical to it. This is the content-hash
  # verification that makes the trusted training loop guaranteed, without trusting
  # the researcher who provisioned the app.
  harness_hash <- .compute_harness_hash()
  if (nzchar(harness_hash)) {
    writeLines(harness_hash,
               file.path(handle$staging_dir, "expected_hash.txt"))
    writeLines("dsflower_runner",
               file.path(handle$staging_dir, "expected_template.txt"))
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

  # Revalidate cryptographic + accounting state after every non-private
  # preflight, but before a ClientApp can release a model. The reservation was
  # made before prepare touched private data and is idempotent by run_token.
  .privacy_runtime_bootstrap()
  manifest_path <- file.path(handle$staging_dir, "manifest.json")
  manifest <- tryCatch(
    jsonlite::fromJSON(manifest_path, simplifyVector = FALSE),
    error = function(e) stop("Could not read the prepared privacy manifest.",
                             call. = FALSE))
  max_releases <- suppressWarnings(as.integer(
    manifest[["privacy-max-releases"]] %||% NA_integer_))
  reservation <- .reserve_privacy_run(handle$run_token, max_releases)
  .apply_privacy_reservation(handle$staging_dir, reservation)

  # Ensure SuperNode via singleton registry
  entry <- .supernode_ensure(
    superlink_address = superlink_address,
    manifest_dir      = handle$staging_dir,
    python_path       = handle$python_path,
    ca_cert_path      = ca_cert_path,
    template_name     = template_name,
    insecure          = via_tunnel
  )

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
  handle <- .validateHandleStaging(.getHandle(handle_symbol))

  # Stop SuperNode if associated. This must happen before staging deletion
  # because orphan cleanup uses the manifest_dir embedded in the process args.
  if (!is.null(handle$staging_dir)) {
    .supernode_stop(handle$staging_dir)
  }

  # Clean up staging
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }

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
#' the associated SuperNode, and removes the handle.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return NULL.
#' @export
flowerDestroyDS <- function(handle_symbol) {
  handle <- .validateHandleStaging(.getHandle(handle_symbol))

  # Stop SuperNode if associated
  if (!is.null(handle$staging_dir)) {
    .supernode_stop(handle$staging_dir)
  }
  # Clean up staging
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }

  .removeHandle(handle_symbol, cleanup = FALSE)
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
#' cohort contents, handle state, filesystem paths, and other sessions. The
#' legacy template catalogue is retained as an explicitly deprecated empty
#' field.
#'
#' @param handle_symbol Ignored compatibility argument.
#' @return Named list of capabilities.
#' @export
flowerGetCapabilitiesDS <- function(handle_symbol = NULL) {
  runtime <- .python_runtime_capabilities()

  # Disclosure settings
  settings <- .flowerDisclosureSettings()

  privacy_policy <- .privacy_policy()
  runner_caps <- .RUNNER_PUBLIC_CAPABILITIES
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
    templates           = settings$allowed_templates,
    templates_deprecated = settings$allowed_templates_deprecated,
    dp_tracks           = runner_caps$dp_tracks,
    declarative_model_ops = runner_caps$declarative_model_ops,
    declarative_losses  = runner_caps$declarative_losses,
    tree_objectives     = runner_caps$tree_objectives,
    aggregation_strategies = runner_caps$aggregation_strategies,
    max_rounds          = settings$max_rounds,
    allow_custom_config = settings$allow_custom_config,
    allow_custom_config_deprecated = settings$allow_custom_config_deprecated,
    min_samples         = 0L,
    min_clients_per_round = 1L,
    dp_required         = TRUE,
    privacy_accountant  = "bounded-geometric-basic-composition-v2",
    privacy_total_epsilon = privacy_policy$total_epsilon,
    privacy_total_delta = privacy_policy$total_delta,
    privacy_unit        = privacy_policy$dp_unit,
    privacy_patient_column = privacy_policy$patient_column,
    privacy_nonblocking = TRUE,
    runner_abi          = 2L,
    runner_sha256       = .compute_harness_hash(),
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

#' Disabled node-metrics compatibility endpoint
#'
#' Node metrics can encode data-dependent loss, counts, failures or timing and
#' are outside the model mechanism's DP proof. This DataSHIELD AGGREGATE symbol
#' is retained for protocol compatibility and always returns an empty data frame.
#'
#' @param handle_symbol Ignored compatibility argument.
#' @param since_round Ignored compatibility argument.
#' @return Empty data.frame with columns: round, metric, value.
#' @export
flowerMetricsDS <- function(handle_symbol, since_round = 0L) {
  # A log-derived metric can encode a data-dependent failure, duration, count,
  # or loss and is not covered by the model mechanism's DP proof. Keep this
  # aggregate for protocol compatibility, but expose no node-side metrics.
  data.frame(round = integer(0), metric = character(0),
             value = numeric(0), stringsAsFactors = FALSE)
}

#' Disabled node-log compatibility endpoint
#'
#' Node logs can encode data-dependent failures and timing and are not a DP
#' mechanism. This DataSHIELD AGGREGATE symbol is retained for protocol
#' compatibility and always returns an empty character vector.
#'
#' @param handle_symbol Ignored compatibility argument.
#' @param last_n Ignored compatibility argument.
#' @return Empty character vector.
#' @export
flowerLogDS <- function(handle_symbol, last_n = 50L) {
  # Logs contain data-dependent failures/timing and are not a DP mechanism.
  # Keep the aggregate for wire compatibility, but never release raw lines.
  character(0)
}

#' Legacy per-feature statistics endpoint (DP-safe compatibility response)
#'
#' Exact sums and sums-of-squares are incompatible with dsFlower's DP-only egress
#' contract.  The symbol remains callable by older clients, but returns a
#' data-independent identity transform (n/sum/sumsq all zero).  New clients use
#' public, analyst-supplied feature bounds instead.
#'
#' @param data_symbol Ignored compatibility argument.
#' @param feature_columns Character or JSON-encoded character vector; feature
#'   names supplied by the caller (order is preserved in the returned vectors).
#'   NULL returns empty vectors because server-side schema is not inspected.
#' @return Named list with the requested \code{features}, zero-valued
#'   \code{n}/\code{sum}/\code{sumsq} vectors, \code{disabled = TRUE}, and a
#'   reason. No statistic computed from feature values is returned.
#' @export
flowerFeatureStatsDS <- function(data_symbol, feature_columns = NULL) {
  feats <- .ds_arg(feature_columns)
  if (is.null(feats) || length(feats) == 0L) feats <- character()
  feats <- as.character(unlist(feats, use.names = FALSE))
  zero <- numeric(length(feats))
  list(features = feats, n = zero, sum = zero, sumsq = zero,
       disabled = TRUE, reason = "exact-feature-statistics-disabled")
}

#' Query the server-owned lifetime privacy policy
#'
#' By default this exposes only the fixed policy, not other analysts' allocation
#' count.  A custodian may set \code{dsflower.expose_privacy_status=TRUE} to expose
#' the operational ledger status as well; neither response contains cohort data.
#' @param handle_symbol Ignored compatibility argument.
#' @param target_column Ignored compatibility argument.
#' @return Named list describing the privacy accountant.
#' @export
flowerPrivacyBudgetDS <- function(handle_symbol = NULL, target_column = NULL) {
  if (isTRUE(as.logical(.dsf_option("expose_privacy_status", FALSE)))) {
    return(.privacy_budget_status())
  }
  policy <- .privacy_policy()
  list(
    accountant = "bounded-geometric-basic-composition-v2",
    domain = policy$domain,
    total_epsilon = policy$total_epsilon,
    total_delta = policy$total_delta,
    decay = policy$decay,
    dp_unit = policy$dp_unit,
    patient_column = policy$patient_column,
    unit_canonicalization = policy$unit_canonicalization,
    adjacency = policy$adjacency,
    nonblocking = TRUE)
}

# --- Internal metric parsing ---

#' Parse training metrics from Flower log output
#'
#' @param log_path Character; path to the log file.
#' @return Data.frame with columns: round, metric, value.
#' @keywords internal
.parseFlowerMetrics <- function(log_path) {
  if (is.null(log_path) || !file.exists(log_path)) {
    return(data.frame(
      round = integer(0), metric = character(0),
      value = numeric(0), stringsAsFactors = FALSE
    ))
  }

  lines <- readLines(log_path, warn = FALSE)
  rows <- list()

  for (line in lines) {
    # Pattern 1: [ROUND N] metric = value
    m <- regmatches(line, regexec(
      "\\[?[Rr]ound\\s+(\\d+)\\]?.*?(loss|accuracy|f1|precision|recall|auc|mse|mae|rmse|r2|num_examples|num_clients)\\s*[=:]\\s*([0-9eE.+-]+)",
      line
    ))[[1]]
    if (length(m) == 4) {
      rows[[length(rows) + 1]] <- data.frame(
        round = as.integer(m[2]),
        metric = m[3],
        value = as.numeric(m[4]),
        stringsAsFactors = FALSE
      )
      next
    }

    # Pattern 2: Flower evaluate format
    m2 <- regmatches(line, regexec(
      "round\\s+(\\d+)[^)]*\\bloss['\"]?\\s*[=:]\\s*([0-9eE.+-]+)",
      line, ignore.case = TRUE
    ))[[1]]
    if (length(m2) == 3) {
      rows[[length(rows) + 1]] <- data.frame(
        round = as.integer(m2[2]),
        metric = "loss",
        value = as.numeric(m2[3]),
        stringsAsFactors = FALSE
      )
    }

    # Pattern 3: metrics_distributed format
    m3 <- regmatches(line, regexec(
      "'(accuracy|loss|f1|precision|recall)'\\s*:\\s*\\(\\s*(\\d+)\\s*,\\s*([0-9eE.+-]+)",
      line
    ))[[1]]
    if (length(m3) == 4) {
      rows[[length(rows) + 1]] <- data.frame(
        round = as.integer(m3[3]),
        metric = m3[2],
        value = as.numeric(m3[4]),
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(rows) == 0) {
    return(data.frame(
      round = integer(0), metric = character(0),
      value = numeric(0), stringsAsFactors = FALSE
    ))
  }

  result <- do.call(rbind, rows)
  rownames(result) <- NULL
  result
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

  # Per-session rate limit to blunt scanning even within the allowed scope.
  if (!.connectivity_rate_ok()) {
    return(list(reachable = FALSE,
                error = "Connectivity check rate limit exceeded; try again shortly."))
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
