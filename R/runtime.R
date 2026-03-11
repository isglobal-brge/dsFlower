# Module: SuperNode Registry
# Uses processx for SuperNode lifecycle management.
# SuperNodes are keyed by manifest_dir (unique per run token) in a global registry.
# This allows multiple SuperNodes in the same process (e.g. DSLite testing).

# Base port for clientappio — incremented per SuperNode on the same machine
.CLIENTAPPIO_BASE_PORT <- 9094L

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
.supernode_ensure <- function(superlink_address, manifest_dir, python_path = "python3") {
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

  # Create log directory
  log_dir <- file.path(tempdir(), "dsflower", "supernodes")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  # Sanitize address for filename
  log_name <- gsub("[^a-zA-Z0-9._-]", "_", superlink_address)
  log_path <- file.path(log_dir, paste0(log_name, ".log"))

  # Assign a unique clientappio port (needed when multiple SuperNodes run locally)
  clientappio_port <- .CLIENTAPPIO_BASE_PORT + alive_count

  # Build command for flower-supernode
  args <- c(
    "--insecure",
    "--superlink", superlink_address,
    "--node-config", paste0('manifest-dir="', manifest_dir, '"'),
    "--clientappio-api-address", paste0("0.0.0.0:", clientappio_port)
  )

  # Spawn via processx
  proc <- processx::process$new(
    command = "flower-supernode",
    args = args,
    stdout = log_path,
    stderr = "2>&1",
    cleanup = TRUE,
    cleanup_tree = TRUE
  )

  entry <- list(
    process           = proc,
    superlink_address = superlink_address,
    manifest_dir      = manifest_dir,
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
