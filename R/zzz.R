# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level transport state
.dsflower_env <- new.env(parent = emptyenv())

# Authoritative Flower-handle state. Session workspaces receive only an opaque
# capability; sensitive paths, data and lifecycle flags stay in this registry.
.handle_registry <- new.env(parent = emptyenv())

# SuperNode singleton registry -- keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

.read_os_entropy <- function(n) {
  con <- file("/dev/urandom", open = "rb", raw = TRUE)
  on.exit(close(con), add = TRUE)
  readBin(con, "raw", n = as.integer(n))
}

#' Resolve the dedicated dsFlower node-secret path
#' @keywords internal
.node_secret_path <- function() {
  configured <- .dsf_option(
    "node_secret_path", "/var/lib/dsflower/node_secret")
  from_env <- Sys.getenv("DSFLOWER_NODE_SECRET_FILE", unset = "")
  path <- if (nzchar(from_env)) from_env else configured
  if (length(path) != 1L || is.na(path) || !nzchar(as.character(path)) ||
      !.path_is_absolute(as.character(path))) {
    stop("The dsFlower node-secret path must be absolute.", call. = FALSE)
  }
  path <- as.character(path)
  if (.path_is_symlink(path)) {
    stop("The dsFlower node secret must not be a symbolic link.", call. = FALSE)
  }
  path <- .canonical_state_path(path)
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET", ""), "1")
  if (.privacy_path_is_ephemeral(path) && !allow_test_tmp) {
    stop("The dsFlower node secret must be persistent; /tmp, /var/tmp and ",
         "/dev/shm are not allowed.", call. = FALSE)
  }
  path
}

#' Validate the parent directory of a dsFlower node secret
#' @keywords internal
.validate_node_secret_parent <- function(path, euid = NULL) {
  parent <- dirname(path)
  if (.path_is_symlink(parent)) {
    stop("The dsFlower node-secret parent must be a real directory.",
         call. = FALSE)
  }
  info <- file.info(parent)
  if (nrow(info) != 1L || is.na(info$isdir[[1]]) || !isTRUE(info$isdir[[1]])) {
    stop("The dsFlower node-secret parent must be a real directory.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    if (is.null(euid)) euid <- .privacy_effective_uid()
    owner <- suppressWarnings(as.integer(info$uid[[1]]))
    if (is.na(owner) || !owner %in% c(as.integer(euid), 0L)) {
      stop("The dsFlower node-secret parent must be owned by the node EUID or root.",
           call. = FALSE)
    }
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    unsafe_write <- as.integer(strtoi("22", base = 8))
    if (is.na(mode) || bitwAnd(mode, unsafe_write) != 0L) {
      stop("The dsFlower node-secret parent must not be writable by group or other users.",
           call. = FALSE)
    }
  }
  normalizePath(parent, winslash = "/", mustWork = TRUE)
}

#' Validate a dedicated 256-bit dsFlower node secret
#' @keywords internal
.validate_node_secret <- function(path) {
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NULL
  parent_before <- .validate_node_secret_parent(path, euid)
  if (!file.exists(path) || .path_is_symlink(path)) {
    stop("The dsFlower node secret is missing or is a symbolic link: ", path,
         call. = FALSE)
  }
  info <- file.info(path)
  if (isTRUE(info$isdir[[1]])) {
    stop("The dsFlower node secret must be a regular file.", call. = FALSE)
  }
  # Accept 64 hex bytes with no terminator, LF, or CRLF. Read one byte beyond
  # the largest valid representation so a valid first line plus hidden trailing
  # content cannot pass validation.
  bytes <- tryCatch(readBin(path, "raw", n = 67L),
                    error = function(e) raw(0))
  if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0a))) {
    bytes <- bytes[-length(bytes)]
    if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0d))) {
      bytes <- bytes[-length(bytes)]
    }
  }
  value <- tryCatch(rawToChar(bytes), error = function(e) "")
  if (length(bytes) != 64L ||
      !grepl("^[0-9a-fA-F]{64}$", value, perl = TRUE)) {
    stop("The dsFlower node secret must contain exactly 32 bytes as 64 hex digits.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    expected_mode <- as.integer(strtoi("600", base = 8))
    if (is.na(mode) || !identical(mode, expected_mode)) {
      stop("The dsFlower node secret must have Unix mode exactly 0600.",
           call. = FALSE)
    }
    current_user <- unname(Sys.info()[["effective_user"]])
    if (is.null(current_user) || is.na(current_user) || !nzchar(current_user)) {
      current_user <- unname(Sys.info()[["user"]])
    }
    if (is.null(current_user) || is.na(current_user) || !nzchar(current_user) ||
        is.na(info$uname[[1]]) || !identical(info$uname[[1]], current_user)) {
      stop("The dsFlower node secret is not owned by the current service user.",
           call. = FALSE)
    }
  }
  parent_after <- .validate_node_secret_parent(path, euid)
  if (!identical(parent_after, parent_before)) {
    stop("The dsFlower node-secret parent changed while validating the key.",
         call. = FALSE)
  }
  invisible(path)
}

#' Ensure a dedicated per-node 256-bit secret for deterministic releases
#'
#' The secret is created at RUN TIME, never from `.onLoad`, so an image build cannot
#' accidentally bake one key into every deployed node.  There is deliberately no
#' statistical-RNG fallback: missing CSPRNG entropy or unsafe permissions fail closed.
#' @keywords internal
.ensure_node_secret <- function() {
  path <- .node_secret_path()
  if (file.exists(path)) return(.validate_node_secret(path))
  if (!file.exists("/dev/urandom")) {
    stop("/dev/urandom is unavailable; refusing to create a DP node secret.",
         call. = FALSE)
  }
  parent <- dirname(path)
  parent_existed <- dir.exists(parent)
  old_umask <- if (.Platform$OS.type == "unix") Sys.umask("0077") else NULL
  umask_restored <- FALSE
  on.exit({
    if (!umask_restored && !is.null(old_umask)) Sys.umask(old_umask)
  }, add = TRUE)
  dir.create(parent, recursive = TRUE, mode = "0700", showWarnings = FALSE)
  if (!is.null(old_umask)) Sys.umask(old_umask)
  umask_restored <- TRUE
  if (!dir.exists(parent)) {
    stop("Could not create the dsFlower secret directory: ", parent,
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix" && !parent_existed) {
    Sys.chmod(parent, "0700")
  }
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NULL
  parent_before <- .validate_node_secret_parent(path, euid)

  lock <- filelock::lock(paste0(path, ".lock"), timeout = 10000)
  if (is.null(lock)) stop("Timed out creating the dsFlower node secret.", call. = FALSE)
  on.exit(filelock::unlock(lock), add = TRUE)
  if (!file.exists(path)) {
    entropy <- tryCatch(.read_os_entropy(32L),
                        error = function(e) raw(0))
    if (length(entropy) != 32L) {
      stop("Could not read 32 bytes of operating-system entropy; refusing a DP release.",
           call. = FALSE)
    }
    value <- paste(sprintf("%02x", as.integer(entropy)), collapse = "")
    tmp <- tempfile(pattern = ".node-secret-", tmpdir = parent)
    on.exit(unlink(tmp), add = TRUE)
    # base::file.create() has no `mode` formal: passing mode="0600" is
    # interpreted through `...` as a second filename. Use a restrictive umask
    # at creation time so there is no group/world-readable window before chmod.
    old_umask <- Sys.umask("0077")
    created <- tryCatch(
      file.create(tmp),
      finally = Sys.umask(old_umask)
    )
    if (length(created) != 1L || !isTRUE(created)) {
      stop("Could not create a private temporary node-secret file.",
           call. = FALSE)
    }
    writeLines(value, tmp, useBytes = TRUE)
    Sys.chmod(tmp, "0600")
    if (!file.rename(tmp, path)) {
      stop("Could not atomically install the dsFlower node secret.", call. = FALSE)
    }
  }
  parent_after <- .validate_node_secret_parent(path, euid)
  if (!identical(parent_after, parent_before)) {
    stop("The dsFlower node-secret parent changed while creating the key.",
         call. = FALSE)
  }
  .validate_node_secret(path)
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

}

#' Package attach hook
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage(
    "dsFlower v", utils::packageVersion("dsFlower"), " loaded."
  )
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) {
    packageStartupMessage(
      "dsFlower: python3 not found. ",
      "SuperNode operations will not work without Python.")
  }

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
  # A long-running federated job may legitimately outlive the age threshold.
  # Protect both processes owned by this R session and live SuperNodes discovered
  # through /proc before considering any directory for deletion.
  active <- character()
  for (key in ls(.supernode_registry, all.names = TRUE)) {
    entry <- tryCatch(get(key, envir = .supernode_registry),
                      error = function(e) NULL)
    alive <- tryCatch(!is.null(entry$process) && entry$process$is_alive(),
                      error = function(e) FALSE)
    if (isTRUE(alive)) active <- c(active, key)
  }
  live <- tryCatch(.list_supernode_processes(), error = function(e) NULL)
  if (!is.null(live) && nrow(live)) {
    active <- c(active, live$manifest_dir[!is.na(live$manifest_dir)])
  }
  active <- unique(vapply(active, function(path)
    normalizePath(path, winslash = "/", mustWork = FALSE), character(1)))

  for (base in c("/dev/shm", tempdir())) {
    dsflower_dir <- file.path(base, "dsflower")
    if (!dir.exists(dsflower_dir)) next
    subdirs <- list.dirs(dsflower_dir, full.names = TRUE, recursive = FALSE)
    for (d in subdirs) {
      canonical <- normalizePath(d, winslash = "/", mustWork = FALSE)
      if (canonical %in% active) next
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
