# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level handle storage
.dsflower_env <- new.env(parent = emptyenv())

# SuperNode singleton registry -- keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

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
    # If primary path is not writable, try /srv (Rock persistent volume)
    if (!isTRUE(created) && venv_root == "/var/lib/dsflower/venvs") {
      fallback <- "/srv/dsflower/venvs"
      tryCatch(
        dir.create(fallback, recursive = TRUE, showWarnings = FALSE),
        error = function(e) NULL
      )
      if (dir.exists(fallback)) {
        options(dsflower.venv_root = fallback)
      }
    }
  }

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
    }, error = function(e) NULL)
  }
  rm(list = ls(.supernode_registry), envir = .supernode_registry)
}
