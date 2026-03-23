# Module: SuperNode Registry
# Uses processx for SuperNode lifecycle management.
# SuperNodes are keyed by manifest_dir (unique per run token) in a global registry.
# This allows multiple SuperNodes in the same process (e.g. DSLite testing).

# Port range for clientappio — a random port is chosen from this range
# to avoid collisions between concurrent SuperNodes (even across sessions).
.CLIENTAPPIO_PORT_MIN <- 10000L
.CLIENTAPPIO_PORT_MAX <- 60000L

#' Pick a random available TCP port
#'
#' Selects a random port from the configured range, checking that it is
#' not already in use by another registered SuperNode. Falls back to
#' retries (up to 20) if the port is taken.
#'
#' @return Integer; an available port number.
#' @keywords internal
.random_available_port <- function() {
  # Collect ports already used by registered SuperNodes
  used_ports <- integer(0)
  for (key in ls(.supernode_registry)) {
    entry <- tryCatch(get(key, envir = .supernode_registry), error = function(e) NULL)
    if (!is.null(entry) && !is.null(entry$clientappio_port) &&
        !is.null(entry$process) && entry$process$is_alive()) {
      used_ports <- c(used_ports, entry$clientappio_port)
    }
  }

  for (i in seq_len(20)) {
    port <- sample(.CLIENTAPPIO_PORT_MIN:.CLIENTAPPIO_PORT_MAX, 1)
    if (!port %in% used_ports) return(port)
  }

  # Extremely unlikely fallback — just return the last sampled port
  port
}

#' Look up a SuperNode in the registry
#'
#' Checks the registry for a SuperNode serving the given manifest directory.
#' If the process is dead, removes it from the registry.
#'
#' @param manifest_dir Character; path to the staging directory with manifest.
#' @return The registry entry (list) if alive, or NULL.
#' @keywords internal
.supernode_lookup <- function(manifest_dir) {
  key <- manifest_dir
  if (!exists(key, envir = .supernode_registry)) {
    return(NULL)
  }

  entry <- get(key, envir = .supernode_registry)

  # Verify alive
  if (!is.null(entry$process) && entry$process$is_alive()) {
    return(entry)
  }

  # Dead — remove from registry
  rm(list = key, envir = .supernode_registry)
  NULL
}

#' Resolve template runtime descriptor
#'
#' Maps template_name -> framework -> venv -> absolute paths.
#' This is the single source of truth for how to launch a SuperNode.
#'
#' @param template_name Character.
#' @return Named list: template_name, framework, venv_path, supernode_cmd.
#' @keywords internal
.resolve_template_runtime <- function(template_name) {
  caps <- .TEMPLATE_METADATA[[template_name]]
  if (is.null(caps) || is.null(caps$framework))
    stop("Unknown template or no framework for: ", template_name, call. = FALSE)

  framework <- caps$framework
  venv_root <- .venv_root()
  venv_path <- file.path(venv_root, framework)
  python <- file.path(venv_path, "bin", "python")
  cmd <- file.path(venv_path, "bin", "flower-supernode")

  # File-based validation only -- no subprocess calls
  if (!dir.exists(venv_path))
    stop("Venv not found: ", venv_path, ". Run dsFlower configure.", call. = FALSE)
  if (!file.exists(python))
    stop("Python not found: ", python, call. = FALSE)
  if (!file.exists(cmd))
    stop("flower-supernode not found: ", cmd, call. = FALSE)

  list(
    template_name = template_name,
    framework = framework,
    venv_path = venv_path,
    supernode_cmd = cmd,
    python = python
  )
}

#' Build a clean Python environment for subprocess launch
#'
#' Strips R's LD_LIBRARY_PATH and other variables that conflict
#' with Python native libraries (pyarrow, torch).
#'
#' @param venv_path Character; path to the Python venv.
#' @param staging_dir Character; staging directory for the run.
#' @return Named character vector suitable for processx env parameter.
#' @keywords internal
.build_clean_python_env <- function(venv_path, staging_dir,
                                     extra_pypath = NULL) {
  venv_bin <- file.path(venv_path, "bin")
  current_path <- Sys.getenv("PATH", "")

  env <- c("current",
    LD_LIBRARY_PATH = "",
    DYLD_LIBRARY_PATH = "",
    PYTHONHOME = "",
    PYTHONNOUSERSITE = "1",
    VIRTUAL_ENV = venv_path,
    PATH = paste0(venv_bin, ":", current_path),
    DSFLOWER_MANIFEST_DIR = staging_dir)

  if (!is.null(extra_pypath))
    env <- c(env, PYTHONPATH = extra_pypath)

  env
}

#' Ensure a SuperNode is running (idempotent)
#'
#' Looks up the registry -> if alive, reuse -> if dead/absent, spawn new
#' -> register. This is the main entry point for starting SuperNodes.
#'
#' @param superlink_address Character; the SuperLink Fleet API address (host:port).
#' @param manifest_dir Character; path to the staging directory with manifest.
#' @param python_path Character; path to the Python executable.
#' @return The registry entry (list).
#' @keywords internal
.supernode_ensure <- function(superlink_address, manifest_dir,
                              python_path = "python3", ca_cert_path = NULL,
                              template_name = NULL) {
  # Policy check
  settings <- .flowerDisclosureSettings()
  if (!settings$allow_supernode_spawn) {
    stop("SuperNode spawning is disabled by server policy ",
         "(dsflower.allow_supernode_spawn = FALSE).", call. = FALSE)
  }

  # Check if already running for this manifest_dir
  existing <- .supernode_lookup(manifest_dir)
  if (!is.null(existing)) {
    return(existing)
  }

  # Check concurrent limit
  alive_count <- sum(vapply(ls(.supernode_registry), function(addr) {
    e <- get(addr, envir = .supernode_registry)
    !is.null(e$process) && e$process$is_alive()
  }, logical(1)))

  if (alive_count >= settings$max_concurrent_runs) {
    stop("Maximum concurrent SuperNode limit reached (",
         settings$max_concurrent_runs, "). Stop an existing SuperNode first.",
         call. = FALSE)
  }

  # Resolve SuperNode command from runtime descriptor (written by prepare)
  # This ensures we use the absolute venv path, never PATH fallback
  runtime_json <- file.path(manifest_dir, "runtime.json")
  runtime_desc <- NULL
  env_info <- NULL

  if (file.exists(runtime_json)) {
    runtime_desc <- jsonlite::fromJSON(runtime_json, simplifyVector = FALSE)
    supernode_cmd <- runtime_desc$supernode_cmd
    if (!file.exists(supernode_cmd))
      stop("SuperNode binary not found: ", supernode_cmd, call. = FALSE)
    env_info <- list(
      python = runtime_desc$python,
      flower_supernode = supernode_cmd,
      source = "venv")
  } else if (!is.null(template_name)) {
    # Fallback: resolve at launch time
    runtime_desc <- .resolve_template_runtime(template_name)
    supernode_cmd <- runtime_desc$supernode_cmd
    env_info <- list(
      python = runtime_desc$python,
      flower_supernode = supernode_cmd,
      source = "venv")
  } else {
    stop("No runtime.json and no template_name. Cannot resolve SuperNode binary.",
         call. = FALSE)
  }

  # Create log directory
  log_dir <- file.path(tempdir(), "dsflower", "supernodes")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  # Sanitize address for filename
  log_name <- gsub("[^a-zA-Z0-9._-]", "_", superlink_address)
  log_path <- file.path(log_dir, paste0(log_name, ".log"))

  # Pick a random available port for clientappio (avoids collisions between
  # concurrent users/sessions sharing the same Rock process)
  clientappio_port <- .random_available_port()

  # TLS mode (always required)
  if (is.null(ca_cert_path) || !file.exists(ca_cert_path)) {
    stop("CA certificate not found. The SuperLink must provide a TLS certificate.",
         call. = FALSE)
  }
  tls_args <- c("--root-certificates", ca_cert_path)

  # Build command for flower-supernode
  args <- c(
    tls_args,
    "--superlink", superlink_address,
    "--node-config", paste0('manifest-dir="', manifest_dir, '"'),
    "--clientappio-api-address", paste0("0.0.0.0:", clientappio_port)
  )

  # Inject code integrity hook via PYTHONPATH + sitecustomize.py
  # The sitecustomize.py lives in dsFlower's inst/python/ (server filesystem)
  # and is copied to a private directory in the staging area. Python loads
  # it automatically before any application code, including Flower's
  # ClientApp subprocesses.
  hook_src <- system.file("python", "sitecustomize.py", package = "dsFlower")
  hook_dir <- file.path(manifest_dir, ".dsflower_hook")
  dir.create(hook_dir, showWarnings = FALSE)
  if (nzchar(hook_src) && file.exists(hook_src)) {
    file.copy(hook_src, file.path(hook_dir, "sitecustomize.py"),
              overwrite = TRUE)
  }

  # Set PYTHONPATH so sitecustomize.py is auto-loaded, and pass the
  # manifest dir so the hook can find expected_hash.txt
  existing_pypath <- Sys.getenv("PYTHONPATH", "")
  new_pypath <- if (nzchar(existing_pypath)) {
    paste0(hook_dir, ":", existing_pypath)
  } else {
    hook_dir
  }

  # Build clean Python environment (strips R's LD_LIBRARY_PATH etc.)
  venv_path <- if (!is.null(runtime_desc)) runtime_desc$venv_path
               else if (!is.null(env_info)) dirname(dirname(env_info$python))
               else ""
  spawn_env <- .build_clean_python_env(venv_path, manifest_dir,
                                        extra_pypath = new_pypath)

  # Spawn via processx
  proc <- processx::process$new(
    command = supernode_cmd,
    args = args,
    stdout = log_path,
    stderr = "2>&1",
    cleanup = TRUE,
    cleanup_tree = TRUE,
    env = spawn_env
  )

  entry <- list(
    process           = proc,
    superlink_address = superlink_address,
    manifest_dir      = manifest_dir,
    ca_cert_path      = ca_cert_path,
    clientappio_port  = clientappio_port,
    log_path          = log_path,
    pid               = proc$get_pid(),
    started_at        = Sys.time()
  )

  assign(manifest_dir, entry, envir = .supernode_registry)
  entry
}

#' Stop a SuperNode
#'
#' Sends SIGTERM, waits 5 seconds, then SIGKILL if needed.
#' Removes from registry.
#'
#' @param manifest_dir Character; the manifest directory (registry key).
#' @return Invisible TRUE.
#' @keywords internal
.supernode_stop <- function(manifest_dir) {
  entry <- .supernode_lookup(manifest_dir)
  if (is.null(entry)) return(invisible(TRUE))

  proc <- entry$process
  if (proc$is_alive()) {
    proc$signal(15L)  # SIGTERM
    proc$wait(timeout = 5000)
    if (proc$is_alive()) {
      proc$kill()
    }
  }

  key <- manifest_dir
  if (exists(key, envir = .supernode_registry)) {
    rm(list = key, envir = .supernode_registry)
  }
  invisible(TRUE)
}

#' List all registered SuperNodes
#'
#' @return A data.frame with columns: manifest_dir, superlink_address, pid, alive, started_at.
#' @keywords internal
.supernode_list <- function() {
  keys <- ls(.supernode_registry)
  if (length(keys) == 0) {
    return(data.frame(
      manifest_dir = character(0),
      superlink_address = character(0),
      pid = integer(0),
      alive = logical(0),
      started_at = character(0),
      stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(keys, function(key) {
    entry <- get(key, envir = .supernode_registry)
    data.frame(
      manifest_dir = key,
      superlink_address = entry$superlink_address,
      pid = entry$pid,
      alive = !is.null(entry$process) && entry$process$is_alive(),
      started_at = format(entry$started_at, "%Y-%m-%dT%H:%M:%S"),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

#' Read sanitized SuperNode log
#'
#' @param manifest_dir Character; the manifest directory (registry key).
#' @param last_n Integer; number of lines to return (default 50).
#' @return Character vector of sanitized log lines.
#' @keywords internal
.supernode_read_log <- function(manifest_dir, last_n = 50L) {
  entry <- .supernode_lookup(manifest_dir)
  if (is.null(entry)) {
    log_path <- NULL
  } else {
    log_path <- entry$log_path
  }

  if (is.null(log_path) || !file.exists(log_path)) {
    return(character(0))
  }

  lines <- readLines(log_path, warn = FALSE)
  if (length(lines) > last_n) {
    lines <- utils::tail(lines, last_n)
  }
  .sanitizeLogs(lines, last_n)
}
