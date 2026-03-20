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

  # Resolve Python environment for this template's framework.
  # If the template is known, ensure the correct venv is ready.
  supernode_cmd <- "flower-supernode"
  env_info <- NULL
  if (!is.null(template_name)) {
    caps <- .TEMPLATE_METADATA[[template_name]]
    if (!is.null(caps) && !is.null(caps$framework)) {
      env_info <- tryCatch(
        .ensure_python_env(caps$framework),
        error = function(e) {
          warning("Could not provision Python env for '", caps$framework,
                  "': ", e$message, ". Falling back to system Python.",
                  call. = FALSE)
          NULL
        }
      )
      if (!is.null(env_info)) {
        supernode_cmd <- env_info$flower_supernode
      }
    }
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

  # Build environment: prepend venv bin to PATH so subprocess
  # (flwr-clientapp) also uses the venv Python, not the system Python.
  spawn_env <- c("current",
                 PYTHONPATH = new_pypath,
                 DSFLOWER_MANIFEST_DIR = manifest_dir)
  if (!is.null(env_info) && identical(env_info$source, "venv")) {
    venv_bin <- dirname(env_info$python)
    current_path <- Sys.getenv("PATH", "")
    spawn_env <- c(spawn_env, PATH = paste0(venv_bin, ":", current_path))
  }

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
