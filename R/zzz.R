# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level handle storage
.dsflower_env <- new.env(parent = emptyenv())

# SuperNode singleton registry -- keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

#' Ensure a per-node secret for deterministic-yet-secret DP noise
#'
#' Created once and persisted, never released. It lets a repeated IDENTICAL run
#' (same data + same config) return the SAME model, so an analyst cannot average
#' many runs to cancel the DP noise -- while the secret keeps that noise
#' unpredictable to the analyst. Best-effort: if the directory is not writable the
#' Python harness simply falls back to fresh OS-entropy noise (still DP-safe, just
#' non-deterministic). Stored as a 64-char hex string; the harness reads this file.
#' @keywords internal
.ensure_node_secret <- function() {
  path <- Sys.getenv(
    "DSFLOWER_NODE_SECRET_FILE",
    unset = getOption("dsflower.node_secret_path", "/var/lib/dsflower/node_secret")
  )
  if (file.exists(path)) return(invisible(path))
  tryCatch({
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    raw <- tryCatch(readBin("/dev/urandom", "raw", 32L), error = function(e) NULL)
    if (is.null(raw) || length(raw) < 32L)
      raw <- as.raw((sample.int(256L, 32L, replace = TRUE) - 1L))
    writeLines(paste(sprintf("%02x", as.integer(raw)), collapse = ""), path)
    Sys.chmod(path, "0600")
  }, error = function(e) NULL)
  invisible(path)
}

#' Package load hook -- verify Python venv root exists
#'
#' Fallback for when the configure script did not run (e.g. binary install,
#' devtools::load_all, or missing permissions during configure).  Ensures the
#' venv root directory is present so that .ensure_python_env() can create
#' per-framework venvs on first use without failing on a missing parent.
#'
#' Resolution order for the venv root path:
#'   1. DSFLOWER_VENV_ROOT environment variable
#'   2. dsflower.venv_root R option
#'   3. /var/lib/dsflower/venvs  (primary default)
#'   4. /srv/dsflower/venvs      (fallback if primary is not writable)
#'
#' @param libname Library path.
#' @param pkgname Package name.
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Ensure venv root directory exists.
  # configure creates it during install_github (as root).
  # This fallback handles API installs where configure doesn't run.
  venv_root <- Sys.getenv(
    "DSFLOWER_VENV_ROOT",
    unset = getOption("dsflower.venv_root", "/var/lib/dsflower/venvs")
  )

  if (!dir.exists(venv_root)) {
    created <- tryCatch(
      dir.create(venv_root, recursive = TRUE, showWarnings = FALSE),
      error = function(e) FALSE
    )
    # If the configured path is not writable, cascade through fallbacks so the
    # package self-provisions with ZERO root: /srv (Rock persistent volume)
    # first, then a user-space dir. This makes a plain `install_github` install
    # (as the unprivileged Rock R user) work without a root configure step.
    if (!isTRUE(created) && !dir.exists(venv_root)) {
      fallbacks <- c(
        "/srv/dsflower/venvs",
        file.path(tools::R_user_dir("dsFlower", "data"), "venvs")
      )
      for (fb in fallbacks) {
        ok <- tryCatch(
          dir.create(fb, recursive = TRUE, showWarnings = FALSE),
          error = function(e) FALSE
        )
        if (isTRUE(ok) || dir.exists(fb)) {
          options(dsflower.venv_root = fb)
          break
        }
      }
    }
  }

  # Per-node secret for deterministic-yet-secret DP noise (see .ensure_node_secret).
  tryCatch(.ensure_node_secret(), error = function(e) NULL)

  # Check Python availability
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) {
    packageStartupMessage(
      "dsFlower: python3 not found. ",
      "SuperNode operations will not work without Python.")
  }
}

#' Package attach hook
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage(
    "dsFlower v", utils::packageVersion("dsFlower"), " loaded."
  )

  # Stale staging janitor: remove staging directories older than 24 hours
  .cleanup_stale_staging()

  # Clean orphaned SuperNode processes from crashed sessions
  orphans <- tryCatch(.cleanup_orphaned_supernodes(), error = function(e) 0L)
  if (orphans > 0L) {
    packageStartupMessage(
      "dsFlower: cleaned ", orphans, " orphaned SuperNode process(es).")
  }
}

#' Remove stale staging directories older than 24 hours
#' @keywords internal
.cleanup_stale_staging <- function(max_age_hours = 24) {
  for (base in c("/dev/shm", tempdir())) {
    dsflower_dir <- file.path(base, "dsflower")
    if (!dir.exists(dsflower_dir)) next
    subdirs <- list.dirs(dsflower_dir, full.names = TRUE, recursive = FALSE)
    for (d in subdirs) {
      info <- file.info(d)
      if (!is.na(info$mtime) &&
          difftime(Sys.time(), info$mtime, units = "hours") > max_age_hours) {
        tryCatch(unlink(d, recursive = TRUE), error = function(e) NULL)
      }
    }
  }
}

#' Package detach hook
#'
#' Kills all registered SuperNodes.
#'
#' @param lib Library path.
#' @return Invisible NULL; called for its side effect.
#' @keywords internal
.onDetach <- function(lib) {
  for (addr in ls(.supernode_registry)) {
    tryCatch({
      entry <- get(addr, envir = .supernode_registry)
      if (!is.null(entry$process) && entry$process$is_alive()) {
        entry$process$signal(15L)
        entry$process$wait(timeout = 5000)
        if (entry$process$is_alive()) entry$process$kill()
      }
      # Clean PID file
      if (!is.null(entry$pid)) .remove_supernode_pid(entry$pid)
    }, error = function(e) NULL)
  }
  rm(list = ls(.supernode_registry), envir = .supernode_registry)
}
