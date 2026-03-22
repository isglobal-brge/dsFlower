# Module: DataSHIELD Exposed Methods
# All DataSHIELD assign/aggregate methods for Flower federated learning.

# --- Handle management ---

#' Retrieve a stored Flower handle
#'
#' Looks up the handle by symbol name. First checks the calling session's
#' workspace (used by DSLite where handles are stored as session variables),
#' then falls back to the global package environment (for Opal single-process).
#'
#' @param symbol Character; the handle symbol name (e.g. "flower").
#' @return The Flower handle object (a named list).
#' @keywords internal
.getHandle <- function(symbol) {
  # Try DSLite session workspace: the handle is stored at `symbol` in the
  # eval environment, which is parent.frame(2) from here (one frame for
  # the calling DS method, one frame for .getHandle itself).
  for (depth in 1:3) {
    env <- tryCatch(sys.frame(-(depth)), error = function(e) NULL)
    if (!is.null(env) && exists(symbol, envir = env, inherits = FALSE)) {
      obj <- get(symbol, envir = env, inherits = FALSE)
      if (is.list(obj) && "data_path" %in% names(obj)) {
        return(obj)
      }
    }
  }

  # Opal/Rock stores session variables in the global environment.
  # The handle was assigned there by a previous datashield.assign call.
  if (exists(symbol, envir = .GlobalEnv, inherits = FALSE)) {
    obj <- get(symbol, envir = .GlobalEnv, inherits = FALSE)
    if (is.list(obj) && "data_path" %in% names(obj)) {
      return(obj)
    }
  }

  # Fallback: package-level environment
  key <- paste0("handle_", symbol)
  if (exists(key, envir = .dsflower_env)) {
    return(get(key, envir = .dsflower_env))
  }

  stop("No Flower handle for symbol '", symbol,
       "'. Call flowerInitDS first.", call. = FALSE)
}

#' Store a Flower handle
#'
#' Stores the handle in the global package environment as a fallback
#' for Opal single-process mode. In DSLite, the handle is also stored
#' in the session workspace by DSI's assign mechanism.
#'
#' @param symbol Character; the handle symbol name.
#' @param handle The Flower handle object to store.
#' @return Invisible NULL.
#' @keywords internal
.setHandle <- function(symbol, handle) {
  key <- paste0("handle_", symbol)
  assign(key, handle, envir = .dsflower_env)
}

#' Remove a Flower handle
#'
#' Cleans up staging data and removes the handle from storage.
#' Does NOT stop SuperNodes (they are singleton by SuperLink address).
#'
#' @param symbol Character; the handle symbol name.
#' @return Invisible NULL.
#' @keywords internal
.removeHandle <- function(symbol) {
  key <- paste0("handle_", symbol)
  if (exists(key, envir = .dsflower_env)) {
    handle <- get(key, envir = .dsflower_env)
    # Clean up staging
    if (!is.null(handle$run_token)) {
      .cleanupStaging(handle$run_token)
    }
    rm(list = key, envir = .dsflower_env)
  }
}

#' Create a Flower handle from a data.frame
#'
#' Builds the internal handle structure from an in-session data.frame.
#' The data is stored directly in the handle instead of referencing a file.
#'
#' @param df A data.frame.
#' @return A list representing the Flower handle.
#' @keywords internal
.createHandleFromTable <- function(df) {
  python_path <- Sys.which("python3")
  if (!nzchar(python_path)) python_path <- Sys.which("python")
  if (!nzchar(python_path)) python_path <- "python3"

  list(
    source             = "table",
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
.createHandleFromDescriptor <- function(desc) {
  stopifnot(inherits(desc, "FlowerDatasetDescriptor"))

  python_path <- Sys.which("python3")
  if (!nzchar(python_path)) python_path <- Sys.which("python")
  if (!nzchar(python_path)) python_path <- "python3"

  list(
    source             = "descriptor",
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
  obj <- get(data_symbol, envir = parent.frame())

  if (is.matrix(obj)) obj <- as.data.frame(obj)

  # Existing path: data.frame
  if (is.data.frame(obj)) {
    return(.createHandleFromTable(obj))
  }

  # Descriptor path: FlowerDatasetDescriptor or ResourceClient
  if (inherits(obj, "FlowerDatasetDescriptor")) {
    return(.createHandleFromDescriptor(obj))
  }

  if (inherits(obj, "ResourceClient")) {
    desc <- as_flower_dataset(obj)
    return(.createHandleFromDescriptor(desc))
  }

  # Imaging handle path: list with descriptor field (from imagingInitDS)
  # Note: S3 classes may be lost during Opal serialization, so we check
  # structurally rather than by class.
  if (is.list(obj) && !is.null(obj$descriptor)) {
    desc <- obj$descriptor
    if (inherits(desc, "FlowerDatasetDescriptor")) {
      return(.createHandleFromDescriptor(desc))
    }
    # Reconstruct descriptor if class was lost during serialization
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
      return(.createHandleFromDescriptor(desc))
    }
  }

  # Raw Resource path: list with imaging+dataset:// URL (from datashield.assign.resource)
  if (is.list(obj) && !is.null(obj$url) &&
      grepl("^imaging\\+dataset://", obj$url %||% "")) {
    if (requireNamespace("dsImaging", quietly = TRUE)) {
      parsed <- dsImaging:::.parse_imaging_url(obj$url)
      resolved <- dsImaging:::resolve_dataset(parsed$dataset_id)
      manifest <- dsImaging:::parse_manifest(resolved$manifest_uri, resolved$backend)
      desc <- dsImaging::imaging_dataset_descriptor(manifest)
      return(.createHandleFromDescriptor(desc))
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
      return(.createHandleFromDescriptor(desc))
    }
  }

  stop("Symbol '", data_symbol, "' is not a data.frame, matrix, ",
       "FlowerDatasetDescriptor, ResourceClient, or imaging handle. ",
       "Assign your data first with datashield.assign.table(), ",
       "imagingInitDS(), or similar.",
       call. = FALSE)
}

#' Prepare a Training Run
#'
#' DataSHIELD ASSIGN method. Loads training data, validates schema,
#' asserts minimum sample size, and stages data with a JSON manifest.
#'
#' @param handle_symbol Character; symbol of the initialized handle.
#' @param target_column Character; name of the target column.
#' @param feature_columns Character; JSON-encoded feature column names, or NULL.
#' @param run_config Character; JSON-encoded additional run configuration.
#' @return Updated handle with staging information.
#' @export
flowerPrepareRunDS <- function(handle_symbol, target_column,
                                feature_columns = NULL, run_config = "{}") {
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

  # Clean up previous staging if any
  if (!is.null(handle$run_token)) {
    .cleanupStaging(handle$run_token)
  }

  # Validate max rounds if specified in run_config
  if (!is.null(run_config$num_rounds)) {
    .validateMaxRounds(run_config$num_rounds)
  }

  # Load data: from descriptor, in-memory table, or file
  if (identical(handle$source, "descriptor") && !is.null(handle$descriptor)) {
    # Descriptor path: delegate staging entirely to .stageFromDescriptor
    # which handles in_memory_df, staged_parquet, and image_bundle
    desc <- handle$descriptor
    run_token <- .generate_run_token()
    staging_dir <- .stageFromDescriptor(desc, run_token, target_column,
                                         feature_columns, run_config)

    # Read n_samples from staged manifest for policy checks
    manifest_path <- file.path(staging_dir, "manifest.json")
    staged_manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = TRUE)
    n_samples <- staged_manifest$n_samples

    # Enforce trust profile min_train_rows
    trust <- .flowerTrustProfile()
    effective_min <- max(trust$min_train_rows,
                         .flowerDisclosureSettings()$nfilter_subset)
    template_name <- run_config[["template_name"]] %||% NULL
    if (!is.null(template_name)) {
      template_min <- .templateMinRows(template_name, trust$name)
      if (!is.null(template_min)) {
        effective_min <- max(effective_min, template_min)
      }
      .validateTemplateProfile(template_name, trust$name)
    }
    .assertMinSamples(n_samples, min_n = effective_min)

    # Class distribution validation (descriptor path: read target column only)
    if (!is.null(target_column) && length(target_column) > 0 &&
        trust$min_positive_examples > 0) {
      tryCatch({
        staged_data_path <- file.path(staging_dir, staged_manifest$data_file)
        if (file.exists(staged_data_path) &&
            grepl("\\.parquet$", staged_data_path, ignore.case = TRUE)) {
          target_data <- as.data.frame(
            arrow::read_parquet(staged_data_path, col_select = target_column)
          )
        } else if (file.exists(staged_data_path)) {
          target_data <- utils::read.csv(staged_data_path,
                                          stringsAsFactors = FALSE)
        } else {
          target_data <- NULL
        }
        if (!is.null(target_data)) {
          .validateClassDistribution(target_data, target_column, trust)
        }
      }, error = function(e) {
        if (grepl("Disclosive", conditionMessage(e))) stop(e)
      })
    }

    # DP enforcement
    requested_mode <- run_config[["privacy-mode"]] %||% "clinical_default"
    if (trust$dp_required &&
        !requested_mode %in% c("clinical_dp", "high_sensitivity_dp", "dp")) {
      stop("Trust profile '", trust$name, "' requires differential privacy. ",
           "Set privacy mode to 'clinical_dp' or 'high_sensitivity_dp'.",
           call. = FALSE)
    }
    if (requested_mode %in% c("clinical_dp", "high_sensitivity_dp", "dp")) {
      dp_epsilon <- as.numeric(run_config[["privacy-epsilon"]] %||% 1.0)
      dp_delta   <- as.numeric(run_config[["privacy-delta"]] %||% 1e-5)
      dataset_key <- paste0("descriptor:", desc$dataset_id, ":", target_column)
      .check_budget(dataset_key, dp_epsilon, dp_delta)
    }

    # evaluation_only handling
    if (isTRUE(trust$evaluation_only)) {
      run_config[["evaluation_only"]] <- TRUE
    }

    handle$run_token       <- run_token
    handle$staging_dir     <- staging_dir
    handle$target_column   <- target_column
    handle$feature_columns <- feature_columns
    handle$template_name   <- template_name
    handle$prepared        <- TRUE
    return(handle)
  }

  if (identical(handle$source, "table") && !is.null(handle$table_data)) {
    data <- handle$table_data
  } else {
    data <- .loadTrainingData(handle$data_path, handle$data_format)
  }
  .validateDataSchema(data, target_column, feature_columns)

  # Enforce trust profile min_train_rows (uses the higher of nfilter + profile)
  trust <- .flowerTrustProfile()
  effective_min <- max(trust$min_train_rows,
                       .flowerDisclosureSettings()$nfilter_subset)

  # Apply per-template minimum row count if stricter than profile default
  template_name <- run_config[["template_name"]] %||% NULL
  if (!is.null(template_name)) {
    template_min <- .templateMinRows(template_name, trust$name)
    if (!is.null(template_min)) {
      effective_min <- max(effective_min, template_min)
    }
    # Enforce template/profile compatibility
    .validateTemplateProfile(template_name, trust$name)
  }

  .assertMinSamples(nrow(data), min_n = effective_min)

  # Class distribution validation (table path)
  if (!is.null(target_column) && length(target_column) > 0 &&
      trust$min_positive_examples > 0) {
    .validateClassDistribution(data, target_column, trust)
  }

  # Enforce DP requirement from trust profile
  requested_mode <- run_config[["privacy-mode"]] %||% "clinical_default"
  if (trust$dp_required &&
      !requested_mode %in% c("clinical_dp", "high_sensitivity_dp", "dp")) {
    stop("Trust profile '", trust$name, "' requires differential privacy. ",
         "Set privacy mode to 'clinical_dp' or 'high_sensitivity_dp'.",
         call. = FALSE)
  }

  # Check privacy budget before staging when DP is required
  if (requested_mode %in% c("clinical_dp", "high_sensitivity_dp", "dp")) {
    dp_epsilon <- as.numeric(run_config[["privacy-epsilon"]] %||% 1.0)
    dp_delta   <- as.numeric(run_config[["privacy-delta"]] %||% 1e-5)
    dataset_key <- paste0(handle$source, ":", target_column)
    .check_budget(dataset_key, dp_epsilon, dp_delta)
  }

  # evaluation_only handling
  if (isTRUE(trust$evaluation_only)) {
    run_config[["evaluation_only"]] <- TRUE
  }

  # Write privacy config into run_config for manifest
  run_config[["privacy_profile"]]            <- trust$name
  run_config[["allow_per_node_metrics"]]     <- trust$allow_per_node_metrics
  run_config[["allow_exact_num_examples"]]   <- trust$allow_exact_num_examples
  run_config[["require_secure_aggregation"]] <- trust$require_secure_aggregation
  run_config[["dp_required"]]                <- trust$dp_required
  run_config[["min_clients_per_round"]]      <- trust$min_clients_per_round
  run_config[["fixed_client_sampling"]]      <- trust$fixed_client_sampling
  run_config[["dp_scope"]]                   <- trust$dp_scope
  run_config[["evaluation_only"]]            <- trust$evaluation_only

  # Stage data with manifest
  run_token <- .generate_run_token()
  data_type <- run_config[["data_type"]] %||% "tabular"

  if (identical(data_type, "image")) {
    # Image pipeline: the handle's data.frame is the samples metadata
    # (sample_id, relative_path, label). Images stay on disk (zero-copy).
    if (!("relative_path" %in% names(data))) {
      stop("Image data requires a 'relative_path' column in the data. ",
           "The data.frame should contain sample metadata, not pixel data.",
           call. = FALSE)
    }
    staging_dir <- .stage_image_manifest(run_token, target_column,
                                          data, run_config)
  } else {
    staging_dir <- .stageData(data, run_token, target_column,
                              feature_columns, run_config)
  }

  # Update handle
  handle$run_token       <- run_token
  handle$staging_dir     <- staging_dir
  handle$target_column   <- target_column
  handle$feature_columns <- feature_columns
  handle$template_name   <- template_name
  handle$prepared        <- TRUE

  handle
}

#' Ensure SuperNode is Running
#'
#' DataSHIELD ASSIGN method. Uses the singleton registry to ensure
#' exactly one SuperNode per SuperLink address.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param superlink_address Character; the SuperLink address (host:port).
#' @param federation_id Character or NULL; unique token identifying the
#'   SuperLink instance. Used by the client to verify all nodes joined the
#'   same federation.
#' @param ca_cert_pem Character or NULL; B64-encoded CA certificate PEM for
#'   TLS verification. The SuperNode uses \code{--root-certificates} to
#'   verify the SuperLink's identity.
#' @return Updated handle with SuperNode information.
#' @export
flowerEnsureSuperNodeDS <- function(handle_symbol, superlink_address,
                                     federation_id = NULL,
                                     ca_cert_pem = NULL,
                                     template_name = NULL) {
  handle <- .getHandle(handle_symbol)

  if (!handle$prepared) {
    stop("Handle is not prepared. Call flowerPrepareRunDS first.", call. = FALSE)
  }

  # Resolve template_name: explicit arg > handle > NULL
  if (!is.null(template_name) && nzchar(template_name %||% "")) {
    template_name <- .ds_arg(template_name)
    if (is.list(template_name)) template_name <- template_name[[1]]
  } else if (!is.null(handle$template_name)) {
    template_name <- handle$template_name
  }

  # Write code verification artifacts to staging directory
  if (!is.null(template_name) && nzchar(template_name %||% "")) {

    # Compute expected hash from server's own template copy and write
    # to staging directory. The sitecustomize.py hook (injected via
    # PYTHONPATH) reads this file to verify code integrity at runtime.
    template_hash <- .compute_template_hash(template_name)
    writeLines(template_hash,
               file.path(handle$staging_dir, "expected_hash.txt"))
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

  # Ensure SuperNode via singleton registry
  entry <- .supernode_ensure(
    superlink_address = superlink_address,
    manifest_dir      = handle$staging_dir,
    python_path       = handle$python_path,
    ca_cert_path      = ca_cert_path,
    template_name     = template_name
  )

  handle$superlink_address <- superlink_address
  handle$federation_id     <- federation_id
  handle$ca_cert_path      <- ca_cert_path
  handle$node_ensured      <- TRUE

  handle
}

#' Clean Up Run Staging
#'
#' DataSHIELD ASSIGN method. Removes staging directory and resets
#' handle state. Does NOT stop the SuperNode (it's a singleton).
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return Updated handle with reset state.
#' @export
flowerCleanupRunDS <- function(handle_symbol) {
  handle <- .getHandle(handle_symbol)

  # Record privacy spend if this was a DP run
  if (!is.null(handle$staging_dir) && dir.exists(handle$staging_dir)) {
    manifest_path <- file.path(handle$staging_dir, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- tryCatch(
        jsonlite::fromJSON(manifest_path, simplifyVector = TRUE),
        error = function(e) NULL
      )
      if (!is.null(manifest) &&
          identical(manifest[["privacy-mode"]], "dp")) {
        dataset_key <- paste0(handle$source %||% "table", ":",
                              manifest$target_column)
        .record_spend(
          dataset_key  = dataset_key,
          epsilon      = as.numeric(manifest[["privacy-epsilon"]] %||% 1.0),
          delta        = as.numeric(manifest[["privacy-delta"]] %||% 1e-5),
          run_token    = handle$run_token
        )
      }
    }
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

  handle
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
  handle <- tryCatch(.getHandle(handle_symbol), error = function(e) NULL)

  if (!is.null(handle)) {
    # Clean up staging
    if (!is.null(handle$run_token)) {
      .cleanupStaging(handle$run_token)
    }
    # Stop SuperNode if associated
    if (!is.null(handle$staging_dir)) {
      .supernode_stop(handle$staging_dir)
    }
  }

  .removeHandle(handle_symbol)
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
#' Flower capabilities including Python version, available templates,
#' and disclosure settings.
#'
#' @param handle_symbol Character; symbol of the handle (optional).
#' @return Named list of capabilities.
#' @export
flowerGetCapabilitiesDS <- function(handle_symbol = NULL) {
  # Check Python availability
  python_version <- tryCatch({
    out <- system2("python3", "--version", stdout = TRUE, stderr = TRUE)
    trimws(gsub("Python\\s*", "", out[1]))
  }, error = function(e) "not available")

  # Check Flower version
  flower_version <- tryCatch({
    out <- system2("python3", c("-c", shQuote("import flwr; print(flwr.__version__)")),
                   stdout = TRUE, stderr = TRUE)
    trimws(out[1])
  }, error = function(e) "not installed")

  # Disclosure settings
  settings <- .flowerDisclosureSettings()

  # Trust profile
  trust <- .flowerTrustProfile()

  # SuperNode status
  node_list <- .supernode_list()

  # Environment detection for SuperLink auto-discovery
  is_docker <- .detect_container_env()

  caps <- list(
    dsflower_version    = as.character(utils::packageVersion("dsFlower")),
    python_version      = python_version,
    flower_version      = flower_version,
    templates           = settings$allowed_templates,
    max_rounds          = settings$max_rounds,
    allow_custom_config = settings$allow_custom_config,
    min_samples         = settings$nfilter_subset,
    active_supernodes   = nrow(node_list[node_list$alive, , drop = FALSE]),
    is_docker           = is_docker,
    hostname            = Sys.info()[["nodename"]],
    privacy_profile     = trust$name,
    trust_profile       = trust
  )

  # Add handle-specific info if symbol provided
  if (!is.null(handle_symbol)) {
    tryCatch({
      handle <- .getHandle(handle_symbol)
      if (identical(handle$source, "table") && !is.null(handle$table_data)) {
        caps$data_n_rows <- nrow(handle$table_data)
        caps$data_n_cols <- ncol(handle$table_data)
        caps$data_columns <- names(handle$table_data)
      } else {
        data_summary <- .getDataSummary(handle$data_path, handle$data_format)
        caps$data_n_rows <- data_summary$n_rows
        caps$data_n_cols <- data_summary$n_cols
        caps$data_columns <- data_summary$columns
      }
      caps$data_source <- handle$source %||% "resource"
      caps$prepared <- handle$prepared
      caps$node_ensured <- handle$node_ensured
    }, error = function(e) NULL)
  }

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
  handle <- .getHandle(handle_symbol)

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
    run_token          = handle$run_token,
    staging_dir        = handle$staging_dir,
    target_column      = handle$target_column,
    feature_columns    = handle$feature_columns
  )
}

#' Get Sanitized Training Metrics
#'
#' DataSHIELD AGGREGATE method. Parses and sanitizes metrics from
#' the SuperNode log.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param since_round Integer; return only metrics from this round onward.
#' @return Data.frame with columns: round, metric, value.
#' @export
flowerMetricsDS <- function(handle_symbol, since_round = 0L) {
  handle <- .getHandle(handle_symbol)
  since_round <- as.integer(since_round %||% 0L)

  if (is.null(handle$staging_dir)) {
    return(data.frame(
      round = integer(0), metric = character(0),
      value = numeric(0), stringsAsFactors = FALSE
    ))
  }

  # Read log and parse metrics
  entry <- .supernode_lookup(handle$staging_dir)
  if (is.null(entry) || is.null(entry$log_path) || !file.exists(entry$log_path)) {
    return(data.frame(
      round = integer(0), metric = character(0),
      value = numeric(0), stringsAsFactors = FALSE
    ))
  }

  metrics <- .parseFlowerMetrics(entry$log_path)

  if (nrow(metrics) == 0) return(metrics)

  # Filter by round
  if (since_round > 0) {
    metrics <- metrics[metrics$round >= since_round, , drop = FALSE]
  }

  # Apply disclosure controls
  metrics <- .sanitizeMetrics(metrics)

  # Apply trust profile metric suppression (defense in depth)
  trust <- .flowerTrustProfile()
  if (!trust$allow_per_node_metrics && nrow(metrics) > 0) {
    # Keep only aggregated metrics (loss, num_clients), strip per-node detail
    per_node_metrics <- c("accuracy", "f1", "f1_score", "precision",
                          "recall", "auc", "roc_auc", "mse", "mae",
                          "rmse", "r2")
    if ("metric" %in% names(metrics)) {
      metrics <- metrics[!tolower(metrics$metric) %in% per_node_metrics,
                         , drop = FALSE]
    }
  }
  if (!trust$allow_exact_num_examples && nrow(metrics) > 0) {
    if ("metric" %in% names(metrics) && "value" %in% names(metrics)) {
      ne_idx <- tolower(metrics$metric) == "num_examples"
      if (any(ne_idx)) {
        metrics$value[ne_idx] <- vapply(
          metrics$value[ne_idx], .bucket_count, integer(1)
        )
      }
    }
  }

  rownames(metrics) <- NULL
  metrics
}

#' Get Sanitized Log Output
#'
#' DataSHIELD AGGREGATE method. Returns sanitized log output from the
#' SuperNode.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param last_n Integer; number of lines to return (max 200).
#' @return Character vector of sanitized log lines.
#' @export
flowerLogDS <- function(handle_symbol, last_n = 50L) {
  handle <- .getHandle(handle_symbol)
  last_n <- as.integer(last_n %||% 50L)

  if (is.null(handle$staging_dir)) {
    return(character(0))
  }

  .supernode_read_log(handle$staging_dir, last_n)
}

#' Query Privacy Budget
#'
#' DataSHIELD AGGREGATE method. Returns the remaining privacy budget
#' for the dataset associated with the handle.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @return Named list with spent/remaining epsilon and delta.
#' @export
flowerPrivacyBudgetDS <- function(handle_symbol) {
  handle <- .getHandle(handle_symbol)
  target <- handle$target_column %||% "unknown"
  dataset_key <- paste0(handle$source %||% "table", ":", target)
  .get_budget(dataset_key)
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

# --- Container/environment detection ---

#' Detect whether the current process runs inside a container
#'
#' Checks multiple signals: \code{/.dockerenv}, \code{/run/.containerenv}
#' (Podman), cgroup v1/v2, and the \code{container} environment variable
#' (set by systemd-nspawn, Kubernetes, etc.).
#'
#' @return Logical; TRUE if a container environment is detected.
#' @keywords internal
.detect_container_env <- function() {
  # Signal 1: Docker creates this file
  if (file.exists("/.dockerenv")) return(TRUE)

  # Signal 2: Podman creates this file
  if (file.exists("/run/.containerenv")) return(TRUE)

  # Signal 3: $container env var (systemd-nspawn, some K8s setups)
  ctr_env <- Sys.getenv("container", unset = "")
  if (nzchar(ctr_env)) return(TRUE)

  # Signal 4: cgroup v1 — /proc/1/cgroup mentions docker/containerd/kubepods
  cg <- tryCatch(
    readLines("/proc/1/cgroup", warn = FALSE),
    warning = function(w) character(0),
    error   = function(e) character(0)
  )
  if (length(cg) > 0 &&
      any(grepl("docker|containerd|kubepods|lxc|podman", cg,
                ignore.case = TRUE))) {
    return(TRUE)
  }

  # Signal 5: cgroup v2 — /proc/self/mountinfo with overlay/cgroup hints
  mi <- tryCatch(
    readLines("/proc/self/mountinfo", warn = FALSE),
    warning = function(w) character(0),
    error   = function(e) character(0)
  )
  if (length(mi) > 0 &&
      any(grepl("/docker/|/kubepods/|/containerd/", mi))) {
    return(TRUE)
  }

  FALSE
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
  port <- as.integer(parts[2])

  # Use socketConnection with open="wb" and immediately close.
  # For TLS ports the TCP handshake succeeds even though the TLS
  # handshake won't complete -- that's fine, we only need to verify
  # the port is reachable.
  result <- tryCatch({
    con <- suppressWarnings(
      socketConnection(host = host, port = port,
                       open = "wb", blocking = TRUE,
                       timeout = timeout_secs)
    )
    close(con)
    list(reachable = TRUE, error = NULL)
  }, warning = function(w) {
    # socketConnection emits a warning when connect fails
    list(reachable = FALSE, error = conditionMessage(w))
  }, error = function(e) {
    list(reachable = FALSE, error = conditionMessage(e))
  })
  result
}

# --- Template serving methods ---

#' List Available Templates
#'
#' DataSHIELD AGGREGATE method. Returns the names of all Flower app
#' templates installed on this server.
#'
#' @return Character vector of template names.
#' @export
flowerListTemplatesDS <- function() {
  templates_dir <- system.file("flower_templates", package = "dsFlower")
  if (!nzchar(templates_dir) || !dir.exists(templates_dir)) {
    return(character(0))
  }
  all_templates <- list.dirs(templates_dir, full.names = FALSE, recursive = FALSE)

  # Filter: only return templates that support the active trust profile.
  # Templates whose family has NA for the active profile are hidden.
  profile <- tryCatch(.flowerTrustProfile(), error = function(e) {
    list(name = "clinical_default")
  })

  Filter(function(t) {
    family <- .TEMPLATE_FAMILIES[[t]]
    if (is.null(family)) return(FALSE)
    profile_idx <- match(profile$name, .PROFILE_ORDER)
    if (is.na(profile_idx)) return(FALSE)
    min_rows_vec <- .FAMILY_MIN_ROWS[[family]]
    if (is.null(min_rows_vec)) return(FALSE)
    !is.na(min_rows_vec[profile_idx])
  }, all_templates)
}

#' Get a Template
#'
#' DataSHIELD AGGREGATE method. Returns the Python source files for a
#' specific Flower app template. The server controls which templates are
#' available -- if a template is not installed, the request is denied.
#'
#' @param template_name Character; name of the template (e.g. "sklearn_logreg").
#' @return Named list with \code{name} and \code{files} (a named list mapping
#'   relative file paths to their contents as character strings).
#' @export
flowerGetTemplateDS <- function(template_name) {
  # Validate template exists
  available <- flowerListTemplatesDS()
  if (!template_name %in% available) {
    stop("Template '", template_name, "' is not available on this server. ",
         "Available: ", paste(available, collapse = ", "), ".",
         call. = FALSE)
  }

  # Read all files from the template
  template_dir <- system.file("flower_templates", template_name,
                               package = "dsFlower")
  all_files <- list.files(template_dir, recursive = TRUE, full.names = FALSE)

  files <- list()
  for (f in all_files) {
    full_path <- file.path(template_dir, f)
    files[[f]] <- paste(readLines(full_path, warn = FALSE), collapse = "\n")
  }

  hash <- .compute_template_hash(template_name)
  list(name = template_name, files = files, hash = hash)
}

#' Verify App Hash
#'
#' DataSHIELD AGGREGATE method. Compares the hash of the Flower app built
#' by the client against the server's expected hash for that template.
#' If the hashes do not match, the staging directory is destroyed so that
#' the SuperNode cannot access any training data.
#'
#' @param handle_symbol Character; symbol of the handle.
#' @param app_hash Character; SHA-256 hash of the app computed by the client.
#' @param template_name Character; name of the template.
#' @return Named list with \code{verified}, \code{expected}, \code{received}.
#' @export
flowerVerifyAppHashDS <- function(handle_symbol, app_hash, template_name) {
  handle <- .getHandle(handle_symbol)

  if (is.null(handle$staging_dir) || !dir.exists(handle$staging_dir)) {
    return(list(verified = FALSE, error = "No staging directory"))
  }

  expected <- .compute_template_hash(template_name)
  verified <- identical(app_hash, expected)

  if (!verified) {
    # Destroy staging data -- no training possible with tampered code
    unlink(handle$staging_dir, recursive = TRUE)
    warning("CODE VERIFICATION FAILED. Staging data destroyed. ",
            "Expected: ", expected, ", received: ", app_hash,
            call. = FALSE)
  }

  list(verified = verified, expected = expected, received = app_hash)
}

#' Compute SHA-256 hash of a template's Python files
#'
#' Reads all .py files from the template package directory, sorts by
#' filename, and computes a deterministic SHA-256 hash. This must match
#' the Python-side algorithm in \code{_dsflower_verify.py}.
#'
#' @param template_name Character; template name.
#' @return Character; hex-encoded SHA-256 hash.
#' @keywords internal
.compute_template_hash <- function(template_name) {
  template_dir <- system.file("flower_templates", template_name,
                               package = "dsFlower")
  pkg_dir <- file.path(template_dir, template_name)

  py_files <- sort(list.files(pkg_dir, pattern = "\\.py$",
                               full.names = FALSE))

  # Build binary blob matching Python's algorithm:
  # for each file: filename + "\n" + content + "\x00"
  blob <- raw(0)
  for (fname in py_files) {
    content <- readBin(file.path(pkg_dir, fname), "raw",
                       file.info(file.path(pkg_dir, fname))$size)
    blob <- c(blob, charToRaw(fname), charToRaw("\n"), content, as.raw(0x00))
  }

  digest::digest(blob, algo = "sha256", serialize = FALSE)
}
