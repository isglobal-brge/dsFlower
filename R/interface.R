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

#' Create a Flower handle from a resource client
#'
#' Builds the internal handle structure from a resolved FlowerResourceClient.
#'
#' @param resource_client A FlowerResourceClient object.
#' @return A list representing the Flower handle.
#' @keywords internal
.createHandle <- function(resource_client) {
  parsed <- resource_client$getParsed()

  list(
    resource_client    = resource_client,
    data_path          = parsed$data_path,
    data_format        = parsed$data_format,
    python_path        = parsed$python_path,
    run_token          = NULL,
    staging_dir        = NULL,
    superlink_address  = NULL,
    target_column      = NULL,
    feature_columns    = NULL,
    prepared           = FALSE,
    node_ensured       = FALSE
  )
}

# --- ASSIGN methods ---

#' Initialize Flower Handle from Resource
#'
#' DataSHIELD ASSIGN method. Creates a Flower federation handle from
#' a DataSHIELD resource. Handles both DSLite (where the resource is
#' already resolved to a ResourceClient) and Opal (raw resource descriptor).
#'
#' @param resource_symbol Character; symbol name of the assigned resource.
#' @return A Flower handle object (assigned server-side).
#' @export
flowerInitDS <- function(resource_symbol) {
  # Get the resource object from the session workspace
  res_obj <- get(resource_symbol, envir = parent.frame())

  # DSLite resolves resources to ResourceClient objects automatically.
  # Opal passes raw resource descriptors that need resolution.
  if (inherits(res_obj, "ResourceClient")) {
    resource_client <- res_obj
  } else {
    resource_client <- resourcer::newResourceClient(res_obj)
  }

  # Create the handle
  handle <- .createHandle(resource_client)
  handle
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

  # Load and validate data
  data <- .loadTrainingData(handle$data_path, handle$data_format)
  .validateDataSchema(data, target_column, feature_columns)
  .assertMinSamples(nrow(data))

  # Stage data with manifest

  run_token <- .generate_run_token()
  staging_dir <- .stageData(data, run_token, target_column,
                            feature_columns, run_config)

  # Update handle
  handle$run_token       <- run_token
  handle$staging_dir     <- staging_dir
  handle$target_column   <- target_column
  handle$feature_columns <- feature_columns
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
#' @return Updated handle with SuperNode information.
#' @export
flowerEnsureSuperNodeDS <- function(handle_symbol, superlink_address) {
  handle <- .getHandle(handle_symbol)

  if (!handle$prepared) {
    stop("Handle is not prepared. Call flowerPrepareRunDS first.", call. = FALSE)
  }

  # Ensure SuperNode via singleton registry
  entry <- .supernode_ensure(
    superlink_address = superlink_address,
    manifest_dir      = handle$staging_dir,
    python_path       = handle$python_path
  )

  handle$superlink_address <- superlink_address
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

  # SuperNode status
  node_list <- .supernode_list()

  # Environment detection for SuperLink auto-discovery
  is_docker <- file.exists("/.dockerenv") ||
    tryCatch({
      any(grepl("docker|containerd",
                readLines("/proc/1/cgroup", warn = FALSE)))
    }, warning = function(w) FALSE,
       error = function(e) FALSE)

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
    hostname            = Sys.info()[["nodename"]]
  )

  # Add handle-specific info if symbol provided
  if (!is.null(handle_symbol)) {
    tryCatch({
      handle <- .getHandle(handle_symbol)
      data_summary <- .getDataSummary(handle$data_path, handle$data_format)
      caps$data_n_rows <- data_summary$n_rows
      caps$data_n_cols <- data_summary$n_cols
      caps$data_columns <- data_summary$columns
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
  .sanitizeMetrics(metrics)
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
