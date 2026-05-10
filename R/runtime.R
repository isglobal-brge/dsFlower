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
#' Selects a random port from the configured range, checking both the
#' in-process registry AND OS-level port availability (via serverSocket).
#' This prevents collisions between concurrent Rserve sessions on the
#' same Rock server.
#'
#' @return Integer; an available port number.
#' @keywords internal
.random_available_port <- function() {
  # Collect ports already used by registered SuperNodes (this session)
  used_ports <- integer(0)
  for (key in ls(.supernode_registry)) {
    entry <- tryCatch(get(key, envir = .supernode_registry), error = function(e) NULL)
    if (!is.null(entry) && !is.null(entry$clientappio_port) &&
        !is.null(entry$process) && entry$process$is_alive()) {
      used_ports <- c(used_ports, entry$clientappio_port)
    }
  }

  for (i in seq_len(50)) {
    port <- sample(.CLIENTAPPIO_PORT_MIN:.CLIENTAPPIO_PORT_MAX, 1)
    if (port %in% used_ports) next
    # OS-level check: verify port is actually free (catches cross-session usage)
    if (.port_is_free(port)) return(port)
  }

  stop("Could not find an available port after 50 attempts. ",
       "Too many SuperNodes may be running on this server.", call. = FALSE)
}

#' Check if a TCP port is free at the OS level
#'
#' Tries to bind a server socket on the port. If it succeeds, the port
#' is free. Catches cross-session collisions that the in-process registry
#' cannot detect (e.g. orphaned SuperNodes, other Rserve sessions).
#'
#' @param port Integer; port number.
#' @return Logical; TRUE if the port is available.
#' @keywords internal
.port_is_free <- function(port) {
  tryCatch({
    s <- serverSocket(port)
    close(s)
    TRUE
  }, error = function(e) FALSE)
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

  # Clean orphaned SuperNodes from crashed sessions before checking limits
  .cleanup_orphaned_supernodes()

  # Try exclusive lock for atomic count-check + spawn + PID-write.
  # If lock fails (permissions, tmpdir issues), proceed without lock.
  lock_acquired <- tryCatch(.acquire_spawn_lock(timeout_secs = 10),
                             error = function(e) FALSE)
  if (lock_acquired) {
    on.exit(.release_spawn_lock(), add = TRUE)
  }

  # Check concurrent limit -- server-global via PID files, not just per-session
  global_alive <- .count_global_supernodes()

  if (global_alive >= settings$max_concurrent_runs) {
    stop("Maximum concurrent SuperNode limit reached (",
         settings$max_concurrent_runs, " server-wide, ", global_alive,
         " currently running). Stop an existing SuperNode first.",
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

  # TLS mode (always required)
  if (is.null(ca_cert_path) || !file.exists(ca_cert_path)) {
    stop("CA certificate not found. The SuperLink must provide a TLS certificate.",
         call. = FALSE)
  }

  # Inject code integrity hook via PYTHONPATH + sitecustomize.py
  hook_src <- system.file("python", "sitecustomize.py", package = "dsFlower")
  hook_dir <- file.path(manifest_dir, ".dsflower_hook")
  dir.create(hook_dir, showWarnings = FALSE)
  if (nzchar(hook_src) && file.exists(hook_src)) {
    file.copy(hook_src, file.path(hook_dir, "sitecustomize.py"),
              overwrite = TRUE)
  }

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

  # Create log directory
  log_dir <- file.path(tempdir(), "dsflower", "supernodes")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  log_name <- gsub("[^a-zA-Z0-9._-]", "_", superlink_address)
  log_path <- file.path(log_dir, paste0(log_name, ".log"))

  # Spawn with retry on port collision (up to 3 attempts)
  proc <- NULL
  clientappio_port <- NULL
  for (attempt in seq_len(3L)) {
    clientappio_port <- .random_available_port()
    tls_args <- c("--root-certificates", ca_cert_path)
    args <- c(
      tls_args,
      "--superlink", superlink_address,
      "--node-config", paste0('manifest-dir="', manifest_dir, '"'),
      "--clientappio-api-address", paste0("0.0.0.0:", clientappio_port)
    )

    proc <- processx::process$new(
      command = supernode_cmd,
      args = args,
      stdout = log_path,
      stderr = "2>&1",
      cleanup = TRUE,
      cleanup_tree = TRUE,
      env = spawn_env
    )

    # Verify SuperNode started (catches port collisions, binary issues)
    Sys.sleep(2)
    if (proc$is_alive()) break

    # Process died -- likely port collision or config error
    if (attempt < 3L) {
      log_tail <- tryCatch(
        paste(utils::tail(readLines(log_path, warn = FALSE), 5), collapse = "\n"),
        error = function(e) "")
      warning("SuperNode failed on attempt ", attempt, " (port ", clientappio_port,
              "). Retrying...", call. = FALSE)
    }
  }

  if (!proc$is_alive()) {
    log_tail <- tryCatch(
      paste(utils::tail(readLines(log_path, warn = FALSE), 10), collapse = "\n"),
      error = function(e) "(no log)")
    stop("SuperNode failed to start after 3 attempts.\nLog:\n", log_tail,
         call. = FALSE)
  }

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

  # Write PID file for server-global tracking (cross-session visibility)
  .write_supernode_pid(proc$get_pid(), clientappio_port)

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
  stop_pid <- function(pid) {
    tryCatch({
      tools::pskill(pid, signal = 15L)
      Sys.sleep(1)
      if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
    }, error = function(e) NULL)
    .remove_supernode_pid(pid)
  }

  entry <- .supernode_lookup(manifest_dir)
  if (is.null(entry)) {
    # Cleanup can run in a different Rserve process than the one that spawned
    # the SuperNode. In that case the processx object is not in this registry,
    # so stop by matching the manifest_dir recorded in the SuperNode cmdline.
    procs <- .list_supernode_processes()
    if (nrow(procs) > 0L) {
      matches <- procs[!is.na(procs$manifest_dir) &
                         procs$manifest_dir == manifest_dir, , drop = FALSE]
      for (pid in matches$pid) {
        stop_pid(pid)
      }
    }
    return(invisible(TRUE))
  }

  proc <- entry$process
  pid <- entry$pid
  tryCatch({
    if (proc$is_alive()) {
      proc$signal(15L)  # SIGTERM
      proc$wait(timeout = 5000)
      if (proc$is_alive()) {
        proc$kill()
      }
    }
  }, error = function(e) NULL)

  if (.pid_is_alive(pid)) stop_pid(pid)

  # Remove PID file
  .remove_supernode_pid(pid)

  # A stale SuperNode may be visible by manifest_dir even if the processx
  # object did not survive across Rserve workers. Clean those matches too.
  procs <- .list_supernode_processes()
  if (nrow(procs) > 0L) {
    matches <- procs[!is.na(procs$manifest_dir) &
                       procs$manifest_dir == manifest_dir, , drop = FALSE]
    for (match_pid in matches$pid) {
      if (match_pid != pid) stop_pid(match_pid)
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

# ---------------------------------------------------------------------------
# Server-global PID tracking (cross-session SuperNode visibility)
# ---------------------------------------------------------------------------
# Each running SuperNode writes a PID file to a shared directory.
# This allows concurrent Rserve sessions to see each other's SuperNodes
# and enforce a true server-wide concurrent limit.

#' Get the shared PID directory for SuperNode tracking
#' @keywords internal
.supernode_pid_dir <- function() {
  root <- .venv_root()
  pid_dir <- file.path(dirname(root), "supernode_pids")
  if (!dir.exists(pid_dir)) {
    tryCatch(
      dir.create(pid_dir, recursive = TRUE, showWarnings = FALSE),
      error = function(e) NULL
    )
  }
  # Fallback to temp if server dir not writable
  if (!dir.exists(pid_dir) || file.access(pid_dir, 2) != 0L) {
    pid_dir <- file.path(tempdir(), "dsflower_supernode_pids")
    dir.create(pid_dir, recursive = TRUE, showWarnings = FALSE)
  }
  pid_dir
}

#' Write a PID file for a SuperNode (atomic via temp+rename)
#' @keywords internal
.write_supernode_pid <- function(pid, port) {
  tryCatch({
    pid_dir <- .supernode_pid_dir()
    pid_file <- file.path(pid_dir, paste0(pid, ".pid"))
    tmp_file <- paste0(pid_file, ".tmp.", Sys.getpid())
    writeLines(
      c(as.character(pid), as.character(port),
        format(Sys.time(), "%Y-%m-%dT%H:%M:%S")),
      tmp_file
    )
    file.rename(tmp_file, pid_file)
  }, error = function(e) NULL)
}

#' Remove a PID file for a SuperNode
#' @keywords internal
.remove_supernode_pid <- function(pid) {
  tryCatch({
    pid_file <- file.path(.supernode_pid_dir(), paste0(pid, ".pid"))
    if (file.exists(pid_file)) unlink(pid_file)
  }, error = function(e) NULL)
}

#' Read tracked SuperNode PIDs from the shared PID directory
#' @keywords internal
.tracked_supernode_pids <- function() {
  pid_dir <- .supernode_pid_dir()
  pid_files <- list.files(pid_dir, pattern = "\\.pid$", full.names = TRUE)
  if (length(pid_files) == 0L) return(integer())
  pids <- vapply(pid_files, function(pf) {
    lines <- tryCatch(readLines(pf, warn = FALSE), error = function(e) NULL)
    if (is.null(lines) || length(lines) < 1L) return(NA_integer_)
    suppressWarnings(as.integer(lines[1]))
  }, integer(1))
  stats::na.omit(pids)
}

#' Count server-global alive SuperNodes via PID files
#'
#' Reads the shared PID directory, checks each PID is still alive,
#' removes stale entries, and includes untracked live SuperNode processes
#' discovered through /proc on Linux. Counting untracked processes prevents
#' overcommitting a host after a previous Rserve session crashed before cleanup.
#'
#' @return Integer; number of alive SuperNodes across all sessions.
#' @keywords internal
.count_global_supernodes <- function() {
  pid_dir <- .supernode_pid_dir()
  pid_files <- list.files(pid_dir, pattern = "\\.pid$", full.names = TRUE)
  alive <- 0L
  tracked <- integer()
  for (pf in pid_files) {
    lines <- tryCatch(readLines(pf, warn = FALSE), error = function(e) NULL)
    if (is.null(lines) || length(lines) < 1L) {
      unlink(pf)
      next
    }
    pid <- suppressWarnings(as.integer(lines[1]))
    if (is.na(pid)) { unlink(pf); next }

    # Check if process is still alive
    if (.pid_is_alive(pid)) {
      alive <- alive + 1L
      tracked <- c(tracked, pid)
    } else {
      # Orphaned PID file -- process died, clean up
      unlink(pf)
    }
  }

  untracked <- .list_supernode_processes()
  if (nrow(untracked) > 0L) {
    alive <- alive + sum(!untracked$pid %in% tracked)
  }
  alive
}

#' Check if a process with the given PID is alive
#' @keywords internal
.pid_is_alive <- function(pid) {
  # kill(pid, 0) returns 0 if process exists, error otherwise
  tryCatch({
    rc <- tools::pskill(pid, signal = 0L)
    # pskill returns TRUE if signal was sent successfully
    isTRUE(rc)
  }, error = function(e) FALSE)
}

#' List live flower-supernode processes on Linux without spawning subprocesses
#' @keywords internal
.list_supernode_processes <- function() {
  if (!dir.exists("/proc")) {
    return(data.frame(pid = integer(), cmdline = character(),
                      manifest_dir = character(), started_at = as.POSIXct(character()),
                      stringsAsFactors = FALSE))
  }

  proc_dirs <- list.dirs("/proc", recursive = FALSE, full.names = FALSE)
  pids <- suppressWarnings(as.integer(proc_dirs))
  pids <- pids[!is.na(pids)]
  rows <- list()

  for (pid in pids) {
    cmd_path <- file.path("/proc", pid, "cmdline")
    raw <- tryCatch({
      con <- file(cmd_path, open = "rb")
      tryCatch(readBin(con, what = "raw", n = 65536L),
               finally = close(con))
    }, error = function(e) raw(), warning = function(w) raw())
    if (length(raw) == 0L) next
    raw[raw == as.raw(0)] <- charToRaw(" ")
    cmd <- rawToChar(raw)
    if (!grepl("flower-supernode", cmd, fixed = TRUE)) next

    manifest_dir <- NA_character_
    match <- regmatches(cmd, regexpr("manifest-dir=\"?[^\" ]+", cmd))
    if (length(match) > 0L && nzchar(match)) {
      manifest_dir <- sub("^manifest-dir=\"?", "", match)
    }

    started_at <- as.POSIXct(NA)
    if (!is.na(manifest_dir) && dir.exists(manifest_dir)) {
      started_at <- file.info(manifest_dir)$mtime
    }
    if (is.na(started_at) && !is.na(manifest_dir)) {
      token <- basename(manifest_dir)
      stamp <- sub("^run_([0-9]{8}_[0-9]{6}).*$", "\\1", token)
      if (!identical(stamp, token)) {
        started_at <- suppressWarnings(
          as.POSIXct(stamp, format = "%Y%m%d_%H%M%S", tz = Sys.timezone())
        )
      }
    }

    rows[[length(rows) + 1L]] <- data.frame(
      pid = pid,
      cmdline = cmd,
      manifest_dir = manifest_dir,
      started_at = started_at,
      stringsAsFactors = FALSE
    )
  }

  if (length(rows) == 0L) {
    return(data.frame(pid = integer(), cmdline = character(),
                      manifest_dir = character(), started_at = as.POSIXct(character()),
                      stringsAsFactors = FALSE))
  }
  do.call(rbind, rows)
}

#' Clean up orphaned SuperNode processes
#'
#' Scans PID files, kills orphaned SuperNode processes that are still
#' running but have no owning Rserve session. It also scans /proc on Linux for
#' flower-supernode processes without PID files and reclaims old untracked
#' processes. Called before spawning to reclaim resources.
#'
#' @return Integer; number of orphans cleaned.
#' @keywords internal
.cleanup_orphaned_supernodes <- function() {
  pid_dir <- .supernode_pid_dir()
  pid_files <- list.files(pid_dir, pattern = "\\.pid$", full.names = TRUE)
  cleaned <- 0L

  for (pf in pid_files) {
    lines <- tryCatch(readLines(pf, warn = FALSE), error = function(e) NULL)
    if (is.null(lines) || length(lines) < 1L) { unlink(pf); next }

    pid <- suppressWarnings(as.integer(lines[1]))
    if (is.na(pid)) { unlink(pf); next }

    # Check age -- only clean orphans older than 2 hours
    info <- file.info(pf)
    if (is.na(info$mtime)) { unlink(pf); next }
    age_hours <- as.numeric(difftime(Sys.time(), info$mtime, units = "hours"))
    if (age_hours < 2) next

    if (.pid_is_alive(pid)) {
      # Process alive but PID file is old -- likely orphaned
      tryCatch({
        tools::pskill(pid, signal = 15L)
        Sys.sleep(1)
        if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
      }, error = function(e) NULL)
      cleaned <- cleaned + 1L
    }
    unlink(pf)
  }

  tracked <- .tracked_supernode_pids()
  untracked <- .list_supernode_processes()
  if (nrow(untracked) > 0L) {
    grace_mins <- as.numeric(.dsf_option("supernode_orphan_grace_minutes", 30))
    for (i in seq_len(nrow(untracked))) {
      pid <- untracked$pid[[i]]
      if (pid %in% tracked) next

      age_mins <- Inf
      started_at <- untracked$started_at[[i]]
      if (!is.na(started_at)) {
        age_mins <- as.numeric(difftime(Sys.time(), started_at, units = "mins"))
      }
      if (!is.na(age_mins) && age_mins < grace_mins) next

      tryCatch({
        tools::pskill(pid, signal = 15L)
        Sys.sleep(1)
        if (.pid_is_alive(pid)) tools::pskill(pid, signal = 9L)
      }, error = function(e) NULL)
      cleaned <- cleaned + 1L
    }
  }
  cleaned
}

# ---------------------------------------------------------------------------
# Exclusive spawn lock (prevents TOCTTOU in count-check + spawn)
# ---------------------------------------------------------------------------

#' Acquire exclusive lock for SuperNode spawning
#'
#' Uses exclusive file creation as an atomic lock mechanism.
#' Prevents two concurrent sessions from both passing the count check
#' and exceeding the concurrent SuperNode limit.
#'
#' @param timeout_secs Numeric; max seconds to wait for lock.
#' @return Logical; TRUE if lock acquired.
#' @keywords internal
.acquire_spawn_lock <- function(timeout_secs = 15) {
  lock_path <- file.path(.supernode_pid_dir(), ".spawn_lock")
  deadline <- Sys.time() + timeout_secs

  repeat {
    # Try exclusive creation (atomic on POSIX)
    tryCatch({
      con <- file(lock_path, open = "wx")
      writeLines(as.character(Sys.getpid()), con)
      close(con)
      return(TRUE)
    }, error = function(e) NULL)

    # Lock exists -- check if stale (holder crashed)
    info <- file.info(lock_path)
    if (!is.na(info$mtime)) {
      age_secs <- as.numeric(difftime(Sys.time(), info$mtime, units = "secs"))
      if (age_secs > 30) {
        # Stale lock -- remove and retry
        unlink(lock_path)
        next
      }
    }

    if (Sys.time() > deadline) return(FALSE)
    Sys.sleep(0.5)
  }
}

#' Release the spawn lock
#' @keywords internal
.release_spawn_lock <- function() {
  lock_path <- file.path(.supernode_pid_dir(), ".spawn_lock")
  tryCatch(unlink(lock_path), error = function(e) NULL)
}
