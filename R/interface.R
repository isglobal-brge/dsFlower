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
.createHandleFromTable <- function(df, data_symbol = NULL) {
  python_path <- Sys.which("python3")
  if (!nzchar(python_path)) python_path <- Sys.which("python")
  if (!nzchar(python_path)) python_path <- "python3"

  list(
    source             = "table",
    data_symbol        = data_symbol %||% "table",
    table_fingerprint  = digest::digest(df, algo = "xxhash64"),
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
  # data_symbol is a STRING (e.g. "D"), not the object itself.
  # Pattern matches dsOMOP: get(symbol, parent.frame())
  obj <- get(data_symbol, envir = parent.frame(), inherits = FALSE)

  if (is.matrix(obj)) obj <- as.data.frame(obj)

  # Existing path: data.frame
  if (is.data.frame(obj)) {
    return(.createHandleFromTable(obj, data_symbol = data_symbol))
  }

  # Descriptor path: FlowerDatasetDescriptor or ResourceClient
  if (inherits(obj, "FlowerDatasetDescriptor")) {
    return(.createHandleFromDescriptor(obj, data_symbol = data_symbol))
  }

  if (inherits(obj, "ResourceClient")) {
    desc <- as_flower_dataset(obj)
    return(.createHandleFromDescriptor(desc, data_symbol = data_symbol))
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
      return(.createHandleFromDescriptor(desc, data_symbol = data_symbol))
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
      return(.createHandleFromDescriptor(desc, data_symbol = data_symbol))
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
      return(.createHandleFromDescriptor(desc, data_symbol = data_symbol))
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
      return(.createHandleFromDescriptor(desc, data_symbol = data_symbol))
    }
  }

  stop("Symbol '", data_symbol, "' is not a data.frame, matrix, ",
       "FlowerDatasetDescriptor, ResourceClient, or imaging handle. ",
       "Assign your data first with datashield.assign.table(), ",
       "imagingInitDS(), or similar.",
       call. = FALSE)
}

# Differential privacy is ALWAYS enforced (local DP, no Secure Aggregation) and
# disclosure is non-disclosive by default. Normalise the manifest run_config to
# the DP-always contract.
.addDpConfigToRunConfig <- function(run_config) {
  run_config[["dp_enabled"]]               <- TRUE
  run_config[["allow_per_node_metrics"]]   <- FALSE
  run_config[["allow_exact_num_examples"]] <- FALSE
  run_config[["fixed_client_sampling"]]    <- TRUE
  run_config
}

# Shared disclosure + DP budget enforcement for both prepare paths. DP is
# unconditional; thresholds come from DataSHIELD options.
.enforceDisclosureAndDp <- function(handle, target_column, template_name,
                                    n_samples, target_data, run_config,
                                    data_type = "tabular") {
  # Admission disclosure checks (input side): never train below the configured
  # minimum rows / on a degenerate class distribution (DataSHIELD nfilter.*).
  #
  # Image collections: group the admission unit by PATIENT when a patient/subject
  # column is present (a patient with many slices counts once) so both the
  # minimum collection size and the minimum per-class count are over distinct
  # patients. Falls back to per-image when no patient column exists.
  n_for_min <- n_samples
  class_dist_data <- target_data
  if (identical(data_type, "image")) {
    grp <- .imageDisclosureUnits(target_data, target_column, run_config)
    if (!is.null(grp)) {
      n_for_min <- grp$n_patients
      class_dist_data <- grp$data
    }
  }
  .assertMinSamples(n_for_min, min_n = .disclosure_min_rows())
  if (!is.null(class_dist_data) && !is.null(target_column) && length(target_column) > 0) {
    .validateClassDistribution(
      class_dist_data, target_column,
      task_type = run_config[["task-type"]] %||% run_config[["task_type"]]
    )
  }
  # DP budget check -- DP is always on.
  dp_epsilon <- as.numeric(run_config[["privacy-epsilon"]] %||% 3.0)
  dp_delta   <- as.numeric(run_config[["privacy-delta"]] %||% 1e-5)
  eps_ceiling <- suppressWarnings(as.numeric(.dsf_option("dp_epsilon_ceiling", 10)))
  if (!is.na(eps_ceiling) && (is.na(dp_epsilon) || dp_epsilon > eps_ceiling)) {
    stop("Requested DP epsilon (", dp_epsilon, ") exceeds this server's ceiling (",
         eps_ceiling, "). Lower epsilon, or have the operator raise ",
         "dsflower.dp_epsilon_ceiling.", call. = FALSE)
  }
  dataset_key <- .privacy_dataset_key(handle, target_column)
  .check_budget(dataset_key, dp_epsilon, dp_delta)
  invisible(TRUE)
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

    run_config <- .addDpConfigToRunConfig(run_config)

    staging_dir <- .stageFromDescriptor(desc, run_token, target_column,
                                         feature_columns, run_config)

    # Read n_samples from staged manifest for policy checks
    manifest_path <- file.path(staging_dir, "manifest.json")
    staged_manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = TRUE)
    n_samples <- staged_manifest$n_samples

    template_name <- run_config[["template_name"]] %||% NULL
    data_type <- staged_manifest$data_type %||% "tabular"
    # Read the target frame (descriptor path) for class-distribution + patient
    # grouping. For image collections this is the small samples metadata table
    # (sample_id, relative_path, label, patient_id, ...); for tabular runs we
    # select only the target column(s) to avoid loading wide feature matrices.
    target_data <- NULL
    if (!is.null(target_column) && length(target_column) > 0) {
      staged_data_path <- if (identical(data_type, "image"))
        file.path(staging_dir, staged_manifest$samples_file %||% "")
      else file.path(staging_dir, staged_manifest$data_file %||% "")
      if (nzchar(staged_data_path) && file.exists(staged_data_path)) {
        # Fail-CLOSED: a read error here must NOT silently leave target_data NULL,
        # which would skip the class-distribution admission check (a disclosure
        # bypass). For images load the whole (small) samples table; for tabular
        # select only the target column(s) to avoid loading wide feature matrices.
        target_data <- tryCatch({
          if (grepl("\\.parquet$", staged_data_path, ignore.case = TRUE)) {
            if (identical(data_type, "image"))
              as.data.frame(arrow::read_parquet(staged_data_path))
            else
              as.data.frame(arrow::read_parquet(staged_data_path,
                                                 col_select = target_column))
          } else {
            utils::read.csv(staged_data_path, stringsAsFactors = FALSE)
          }
        }, error = function(e)
          stop("Could not read staged data for the disclosure class-distribution ",
               "check: ", conditionMessage(e), call. = FALSE))
      }
    }
    .enforceDisclosureAndDp(handle, target_column, template_name,
                            n_samples, target_data, run_config,
                            data_type = data_type)

    # Inject validated mask paths from dsImaging (segmentation tasks)
    seg_generation_id <- run_config[["segmentation_generation_id"]] %||% NULL
    if (!is.null(seg_generation_id) && requireNamespace("dsImaging", quietly = TRUE)) {
      mask_paths <- dsImaging::imagingSegmentationGetMaskPaths(seg_generation_id)
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
        jsonlite::write_json(staged_manifest, manifest_path,
                             auto_unbox = TRUE, pretty = TRUE, null = "null")
      }
    }

    # Resolve runtime descriptor: template -> framework -> venv -> absolute paths
    runtime_desc <- NULL
    if (!is.null(template_name)) {
      runtime_desc <- .resolve_template_runtime(template_name)
      # Persist to staging dir for flowerEnsureSuperNodeDS to read
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
    return(handle)
  }

  if (identical(handle$source, "table") && !is.null(handle$table_data)) {
    data <- handle$table_data
  } else {
    data <- .loadTrainingData(handle$data_path, handle$data_format)
  }
  .validateDataSchema(data, target_column, feature_columns)

  run_config <- .addDpConfigToRunConfig(run_config)
  template_name <- run_config[["template_name"]] %||% NULL
  .enforceDisclosureAndDp(handle, target_column, template_name,
                          nrow(data), data, run_config,
                          data_type = run_config[["data_type"]] %||% "tabular")

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
#' @param template_name Character or NULL; Flower template name used to resolve
#'   the server-side runtime and write code-verification artifacts.
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

  # DSI tunnel transport: the node-local tunnel forwarder (flowerTunnelUpDS)
  # carries the SuperNode<->SuperLink bytes over DataSHIELD, so the SuperNode
  # dials its own loopback forwarder and runs insecure (the DSI channel already
  # provides TLS). This is the only transport -- no Tor, no tailnet.
  via_tunnel <- !is.null(.dsflower_env$tunnel_forwarder_port)
  if (via_tunnel) {
    superlink_address <- paste0("127.0.0.1:", .dsflower_env$tunnel_forwarder_port)
  }

  # SuperLink pinning: if the operator pinned a coordinator on this node, a
  # client must NOT be able to redirect this node's SuperNode to a rogue
  # SuperLink (which would harvest its model updates).
  pinned_addr <- .dsf_option("coordinator_address",
                             Sys.getenv("DSFLOWER_COORDINATOR_ADDRESS", ""))
  if (!is.null(pinned_addr) && nzchar(pinned_addr) &&
      !identical(superlink_address, pinned_addr)) {
    stop("Refusing SuperNode: client requested SuperLink '", superlink_address,
         "' but this node is pinned to coordinator '", pinned_addr,
         "'. Redirecting a pinned node is not allowed.", call. = FALSE)
  }

  # Coordinator-trust gate: a remote/public coordinator can observe every
  # per-round model update, so weak privacy profiles are refused there unless
  # the operator explicitly opts in.
  sl_host <- sub(":.*", "", superlink_address)
  if (!.is_private_or_local_host(sl_host) &&
      !isTRUE(as.logical(.dsf_option("allow_untrusted_coordinator", FALSE)))) {
    # DP is always enforced (individual records stay protected), but without
    # Secure Aggregation a remote/public coordinator can observe each node's
    # per-node update. Require SecAgg for remote coordinators unless overridden.
    rsa <- FALSE
    mpath <- file.path(handle$staging_dir %||% "", "manifest.json")
    if (nzchar(mpath) && file.exists(mpath)) {
      m <- tryCatch(jsonlite::fromJSON(mpath, simplifyVector = TRUE),
                    error = function(e) NULL)
      rsa <- isTRUE(m[["require_secure_aggregation"]])
    }
    if (!rsa) {
      stop("Refusing SuperNode: coordinator '", superlink_address, "' is remote/public ",
           "but this run has no Secure Aggregation (e.g. fewer than 3 nodes). A ",
           "remote coordinator could observe per-node updates. Use >=3 nodes for ",
           "SecAgg, or set dsflower.allow_untrusted_coordinator=TRUE to override ",
           "(DP still protects individual records).", call. = FALSE)
    }
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
    stop("This node has no outbound egress to the SuperLink at '",
         superlink_address, "' (", conn_check$error %||% "unreachable", "). ",
         "Open outbound access from this server to that host:port (v2 transport ",
         "is the DataSHIELD tunnel, so this path is normally unused).",
         call. = FALSE)
  }

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

  handle
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
  handle <- .getHandle(handle_symbol)

  # Record privacy spend if this was a DP run
  if (!is.null(handle$staging_dir) && dir.exists(handle$staging_dir)) {
    manifest_path <- file.path(handle$staging_dir, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- tryCatch(
        jsonlite::fromJSON(manifest_path, simplifyVector = TRUE),
        error = function(e) NULL
      )
      # DP is always enforced, so every run spends from the privacy budget.
      if (!is.null(manifest)) {
        dataset_key <- .privacy_dataset_key(
          handle,
          manifest$target_column %||% handle$target_column %||% "unknown"
        )
        .record_spend(
          dataset_key  = dataset_key,
          epsilon      = as.numeric(manifest[["privacy-epsilon"]] %||% 1.0),
          delta        = as.numeric(manifest[["privacy-delta"]] %||% 1e-5),
          run_token    = handle$run_token
        )
      }
    }
  }

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
    # Stop SuperNode if associated
    if (!is.null(handle$staging_dir)) {
      .supernode_stop(handle$staging_dir)
    }
    # Clean up staging
    if (!is.null(handle$run_token)) {
      .cleanupStaging(handle$run_token)
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
  runtime <- .python_runtime_capabilities()

  # Disclosure settings
  settings <- .flowerDisclosureSettings()

  # SuperNode status
  node_list <- .supernode_list()

  # Environment detection for SuperLink auto-discovery
  is_docker <- .detect_container_env()

  caps <- list(
    dsflower_version    = as.character(utils::packageVersion("dsFlower")),
    python_version      = runtime$python_version,
    flower_version      = runtime$flower_version,
    python_envs         = runtime$python_envs,
    templates           = settings$allowed_templates,
    max_rounds          = settings$max_rounds,
    allow_custom_config = settings$allow_custom_config,
    min_samples         = settings$nfilter_subset,
    min_clients_per_round = 1L,
    dp_required         = TRUE,
    active_supernodes   = nrow(node_list[node_list$alive, , drop = FALSE]),
    is_docker           = is_docker,
    hostname            = Sys.info()[["nodename"]]
  )

  # Add handle-specific info if symbol provided
  if (!is.null(handle_symbol)) {
    tryCatch({
      handle <- .getHandle(handle_symbol)
      if (identical(handle$source, "table") && !is.null(handle$table_data)) {
        caps$data_n_rows <- dsImaging::safe_metadata_count(nrow(handle$table_data))
        caps$data_n_cols <- ncol(handle$table_data)
        caps$data_columns <- names(handle$table_data)
      } else {
        data_summary <- .getDataSummary(handle$data_path, handle$data_format)
        caps$data_n_rows <- dsImaging::safe_metadata_count(data_summary$n_rows)
        caps$data_n_cols <- data_summary$n_cols
        caps$data_columns <- data_summary$columns
      }
      caps$data_source <- handle$source %||% "resource"
      caps$prepared <- handle$prepared
      caps$node_ensured <- handle$node_ensured

      # Detect imaging data
      if (identical(handle$source, "descriptor") && !is.null(handle$descriptor)) {
        sk <- handle$descriptor$source_kind
        if (sk %in% c("image_bundle", "imaging_resource")) {
          caps$has_imagedata <- TRUE
          assets <- handle$descriptor$assets
          if (!is.null(assets)) {
            caps$image_assets <- names(assets)
          }
        }
      }
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

  # Non-disclosive by default: always strip per-node metrics + bucket counts.
  if (nrow(metrics) > 0) {
    # Keep only aggregated metrics (loss, num_clients), strip per-node detail
    per_node_metrics <- c("accuracy", "f1", "f1_score", "precision",
                          "recall", "auc", "roc_auc", "mse", "mae",
                          "rmse", "r2")
    if ("metric" %in% names(metrics)) {
      metrics <- metrics[!tolower(metrics$metric) %in% per_node_metrics,
                         , drop = FALSE]
    }
  }
  if (nrow(metrics) > 0) {
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
  dataset_key <- .privacy_dataset_key(handle, target)
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
    list(reachable = FALSE, error = conditionMessage(w))
  }, error = function(e) {
    list(reachable = FALSE, error = conditionMessage(e))
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

