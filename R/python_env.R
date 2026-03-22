# Module: Python Environment Management
#
# All dsFlower-framework packages use the same pattern:
#   1. Ensure uv is available (download if needed)
#   2. uv creates Python venvs (downloads Python if needed)
#   3. Use the venv's Python
#
# Zero system dependencies. uv is a single static binary (~30MB)
# that manages Python installations and venvs autonomously.

# --- Framework dependency map ---

.BASE_PYTHON_DEPS <- c("flwr>=1.13.0", "numpy>=1.21.0", "pandas>=1.3.0",
                        "pyarrow>=10.0.0")

.FRAMEWORK_PYTHON_DEPS <- list(
  sklearn = c("scikit-learn>=1.0.0"),
  pytorch = c("torch>=2.0.0", "opacus>=1.4.0"),
  pytorch_vision = c("torch>=2.0.0", "torchvision>=0.15.0", "Pillow>=9.0.0"),
  xgboost = c("xgboost>=1.7.0")
)

.FRAMEWORK_HEALTH_IMPORT <- list(
  sklearn = "sklearn",
  pytorch = "torch",
  pytorch_vision = "torchvision",
  xgboost = "xgboost"
)

.dsflower_runtime <- new.env(parent = emptyenv())

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

#' Get the venv root directory
#' @keywords internal
.venv_root <- function() {
  .dsf_option("venv_root", "/var/lib/dsflower/venvs")
}

#' Get all pip dependencies for a framework
#' @keywords internal
.python_deps_for_framework <- function(framework) {
  extra <- .FRAMEWORK_PYTHON_DEPS[[framework]]
  if (is.null(extra)) return(.BASE_PYTHON_DEPS)
  c(.BASE_PYTHON_DEPS, extra)
}

#' Compute a hash of the dependency list for staleness detection
#' @keywords internal
.deps_hash <- function(deps) {
  digest::digest(paste(sort(deps), collapse = "\n"), algo = "sha256",
                 serialize = FALSE)
}

#' Check if a Python venv is healthy
#' @keywords internal
.venv_is_healthy <- function(venv_path, framework) {
  python <- file.path(venv_path, "bin", "python")
  if (!file.exists(python)) return(FALSE)
  marker <- file.path(venv_path, ".dsflower_ready")
  if (!file.exists(marker)) return(FALSE)
  check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]]
  if (is.null(check_mod)) check_mod <- "flwr"
  cmd <- paste0(shQuote(python), " -c ", shQuote(paste0("import ", check_mod)))
  rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
  rc == 0L
}

#' Check if the system Python already satisfies framework dependencies
#' @keywords internal
.system_python_has_framework <- function(framework) {
  # Only check if there happens to be a system Python; we don't require it
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) return(FALSE)

  check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]]
  if (is.null(check_mod)) return(TRUE)
  cmd <- paste0(shQuote(python), " -c ", shQuote(paste0("import ", check_mod)))
  rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
  if (rc != 0L) return(FALSE)

  cmd_pa <- paste0(shQuote(python), " -c ", shQuote("import pyarrow"))
  rc_pa <- suppressWarnings(system(cmd_pa, ignore.stdout = TRUE, ignore.stderr = TRUE))
  rc_pa == 0L
}

#' Create or verify a Python venv for a framework
#'
#' Uses uv as the primary tool. uv downloads Python if not available.
#' No system Python required.
#'
#' @param framework Character; framework name ("sklearn", "pytorch", etc.).
#' @param timeout_secs Numeric; max seconds to wait for install (default 600).
#' @return Named list with \code{python} and \code{flower_supernode} paths.
#' @keywords internal
.ensure_python_env <- function(framework, timeout_secs = 600) {
  # Fast path: system Python already has everything
  if (.system_python_has_framework(framework)) {
    python <- Sys.which("python3")
    if (!nzchar(python)) python <- Sys.which("python")
    supernode <- Sys.which("flower-supernode")
    if (nzchar(supernode)) {
      return(list(python = python, flower_supernode = supernode,
                  source = "system"))
    }
  }

  root <- .venv_root()
  venv_path <- file.path(root, framework)

  # Fast path: existing healthy venv
  if (.venv_is_healthy(venv_path, framework)) {
    return(list(
      python = file.path(venv_path, "bin", "python"),
      flower_supernode = file.path(venv_path, "bin", "flower-supernode"),
      source = "venv"
    ))
  }

  # Need to create/rebuild. Acquire lock.
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  lock_path <- file.path(root, paste0(".", framework, ".lock"))
  lock_acquired <- FALSE

  tryCatch({
    deadline <- Sys.time() + timeout_secs
    repeat {
      if (!file.exists(lock_path)) {
        tryCatch({
          con <- file(lock_path, open = "wx")
          writeLines(as.character(Sys.getpid()), con)
          close(con)
          lock_acquired <- TRUE
        }, error = function(e) {})
      }

      if (lock_acquired) break

      lock_age <- difftime(Sys.time(), file.info(lock_path)$mtime, units = "mins")
      if (!is.na(lock_age) && lock_age > 15) { unlink(lock_path); next }

      if (Sys.time() > deadline) {
        stop("Timeout waiting for Python environment lock for '", framework,
             "'. Remove ", lock_path, " if stuck.", call. = FALSE)
      }

      if (.venv_is_healthy(venv_path, framework)) {
        return(list(
          python = file.path(venv_path, "bin", "python"),
          flower_supernode = file.path(venv_path, "bin", "flower-supernode"),
          source = "venv"
        ))
      }

      Sys.sleep(5)
    }

    # We hold the lock. Double-check.
    if (.venv_is_healthy(venv_path, framework)) {
      return(list(
        python = file.path(venv_path, "bin", "python"),
        flower_supernode = file.path(venv_path, "bin", "flower-supernode"),
        source = "venv"
      ))
    }

    # Create the venv via uv (uv downloads Python if needed)
    message("dsFlower: creating Python environment for '", framework, "'...")
    message("  This may take several minutes on first use.")

    if (dir.exists(venv_path)) unlink(venv_path, recursive = TRUE)

    uv <- .ensure_uv()
    rc <- system2(uv, c("venv", "--python", "3.11", "--quiet", venv_path),
                  stdout = "", stderr = "")
    if (rc != 0L)
      stop("Failed to create venv at ", venv_path, call. = FALSE)

    # Install deps via uv
    deps <- .python_deps_for_framework(framework)
    message("  Installing: ", paste(deps, collapse = ", "))
    venv_python <- file.path(venv_path, "bin", "python")
    result <- processx::run(
      command = uv,
      args = c("pip", "install", "--python", venv_python, "--quiet", deps),
      error_on_status = FALSE,
      timeout = timeout_secs
    )

    if (result$status != 0L) {
      unlink(venv_path, recursive = TRUE)
      stop("pip install failed for '", framework, "':\n", result$stderr,
           call. = FALSE)
    }

    # Verify
    check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]] %||% "flwr"
    venv_python <- file.path(venv_path, "bin", "python")
    cmd <- paste0(shQuote(venv_python), " -c ", shQuote(paste0("import ", check_mod)))
    rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
    if (rc != 0L) {
      unlink(venv_path, recursive = TRUE)
      stop("'", check_mod, "' import failed after install.", call. = FALSE)
    }

    supernode <- file.path(venv_path, "bin", "flower-supernode")
    if (!file.exists(supernode)) {
      unlink(venv_path, recursive = TRUE)
      stop("flower-supernode not found in venv.", call. = FALSE)
    }

    dep_hash <- .deps_hash(deps)
    writeLines(dep_hash, file.path(venv_path, ".dsflower_ready"))
    message("  Python environment for '", framework, "' ready.")

    list(python = venv_python, flower_supernode = supernode, source = "venv")
  }, finally = {
    if (lock_acquired && file.exists(lock_path)) unlink(lock_path)
  })
}

#' List installed Python environments
#' @keywords internal
.list_python_envs <- function() {
  root <- .venv_root()
  if (!dir.exists(root)) {
    return(data.frame(framework = character(0), path = character(0),
                       healthy = logical(0), stringsAsFactors = FALSE))
  }
  dirs <- intersect(list.dirs(root, recursive = FALSE, full.names = FALSE),
                     names(.FRAMEWORK_PYTHON_DEPS))
  if (length(dirs) == 0) {
    return(data.frame(framework = character(0), path = character(0),
                       healthy = logical(0), stringsAsFactors = FALSE))
  }
  rows <- lapply(dirs, function(fw) {
    venv_path <- file.path(root, fw)
    data.frame(framework = fw, path = venv_path,
               healthy = .venv_is_healthy(venv_path, fw),
               stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# uv bootstrap
# ---------------------------------------------------------------------------

#' Ensure uv is available (find or download)
#' @keywords internal
.ensure_uv <- function() {
  cached <- .dsflower_runtime$uv_path
  if (!is.null(cached) && file.exists(cached)) return(cached)

  # PATH
  uv <- Sys.which("uv")
  if (nzchar(uv)) { .dsflower_runtime$uv_path <- uv; return(uv) }

  # Common locations
  home <- Sys.getenv("HOME", "~")
  for (p in c(file.path(home, ".local", "bin", "uv"),
              file.path(home, ".cargo", "bin", "uv"),
              "/usr/local/bin/uv")) {
    if (file.exists(p)) { .dsflower_runtime$uv_path <- p; return(p) }
  }

  # Download standalone binary
  tools_dir <- file.path(.venv_root(), ".tools")
  dir.create(tools_dir, recursive = TRUE, showWarnings = FALSE)
  uv_path <- file.path(tools_dir, "uv")
  if (file.exists(uv_path)) {
    .dsflower_runtime$uv_path <- uv_path
    return(uv_path)
  }

  message("dsFlower: downloading uv...")
  sysname <- tolower(Sys.info()[["sysname"]])
  machine <- Sys.info()[["machine"]]
  os <- switch(sysname,
    darwin = "apple-darwin", linux = "unknown-linux-gnu",
    stop("Unsupported OS: ", sysname, ". Install uv: https://docs.astral.sh/uv/",
         call. = FALSE))
  arch <- switch(machine,
    x86_64 = "x86_64", amd64 = "x86_64",
    aarch64 = "aarch64", arm64 = "aarch64",
    stop("Unsupported arch: ", machine, call. = FALSE))

  url <- paste0("https://github.com/astral-sh/uv/releases/latest/download/uv-",
                arch, "-", os, ".tar.gz")
  tmp <- tempfile(fileext = ".tar.gz")
  tmp_dir <- tempfile()
  on.exit({ unlink(tmp); unlink(tmp_dir, recursive = TRUE) }, add = TRUE)

  rc <- tryCatch(utils::download.file(url, tmp, mode = "wb", quiet = TRUE),
                  error = function(e) 1L)
  if (!identical(rc, 0L))
    stop("Failed to download uv. Install manually: https://docs.astral.sh/uv/",
         call. = FALSE)

  dir.create(tmp_dir, showWarnings = FALSE)
  utils::untar(tmp, exdir = tmp_dir)
  bins <- list.files(tmp_dir, pattern = "^uv$", recursive = TRUE, full.names = TRUE)
  if (length(bins) == 0) stop("uv binary not found in archive.", call. = FALSE)

  file.copy(bins[1], uv_path, overwrite = TRUE)
  Sys.chmod(uv_path, "0755")
  message("dsFlower: uv installed at ", uv_path)
  .dsflower_runtime$uv_path <- uv_path
  uv_path
}
