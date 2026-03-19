# Module: Python Environment Management
# Manages per-framework virtual environments for SuperNode execution.
#
# Each ML framework (sklearn, pytorch, pytorch_vision, xgboost) gets its own
# venv under a configurable root directory. Venvs are created on first use
# and reused for subsequent runs. This avoids polluting the system Python
# and allows multiple frameworks to coexist without version conflicts.
#
# The venv root is controlled by the server option:
#   dsflower.venv_root  (default: /var/lib/dsflower/venvs)

# --- Framework dependency map ---
# Maps framework names to their pip requirements. Base deps (flwr, numpy,
# pandas, pyarrow) are always included.

.BASE_PYTHON_DEPS <- c("flwr>=1.13.0", "numpy>=1.21.0", "pandas>=1.3.0",
                        "pyarrow>=10.0.0")

.FRAMEWORK_PYTHON_DEPS <- list(
  sklearn = c("scikit-learn>=1.0.0"),
  pytorch = c("torch>=2.0.0", "opacus>=1.4.0"),
  pytorch_vision = c("torch>=2.0.0", "torchvision>=0.15.0", "Pillow>=9.0.0"),
  xgboost = c("xgboost>=1.7.0")
)

# Minimal import check per framework: one module that proves deps are working
.FRAMEWORK_HEALTH_IMPORT <- list(
  sklearn = "sklearn",
  pytorch = "torch",
  pytorch_vision = "torchvision",
  xgboost = "xgboost"
)

#' Get the venv root directory
#'
#' @return Character; path to venv root.
#' @keywords internal
.venv_root <- function() {
  root <- .dsf_option("venv_root", "/var/lib/dsflower/venvs")
  root
}

#' Get all pip dependencies for a framework
#'
#' @param framework Character; framework name.
#' @return Character vector of pip requirement strings.
#' @keywords internal
.python_deps_for_framework <- function(framework) {
  extra <- .FRAMEWORK_PYTHON_DEPS[[framework]]
  if (is.null(extra)) {
    return(.BASE_PYTHON_DEPS)
  }
  c(.BASE_PYTHON_DEPS, extra)
}

#' Compute a hash of the dependency list for staleness detection
#'
#' @param deps Character vector of pip requirement strings.
#' @return Character; SHA-256 hex digest.
#' @keywords internal
.deps_hash <- function(deps) {
  digest::digest(paste(sort(deps), collapse = "\n"), algo = "sha256",
                 serialize = FALSE)
}

#' Check if a Python venv is healthy
#'
#' Verifies the venv exists, the Python binary works, and the key
#' framework import succeeds.
#'
#' @param venv_path Character; path to the venv directory.
#' @param framework Character; framework name for import check.
#' @return Logical; TRUE if healthy.
#' @keywords internal
.venv_is_healthy <- function(venv_path, framework) {
  python <- file.path(venv_path, "bin", "python")
  if (!file.exists(python)) return(FALSE)

  # Check ready marker (written by configure script or by .ensure_python_env)
  marker <- file.path(venv_path, ".dsflower_ready")
  if (!file.exists(marker)) return(FALSE)

  # Quick import check -- the definitive test of health
  check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]]
  if (is.null(check_mod)) check_mod <- "flwr"
  cmd <- paste0(shQuote(python), " -c ", shQuote(paste0("import ", check_mod)))
  rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
  rc == 0L
}

#' Check if the system Python already satisfies framework dependencies
#'
#' If the system Python can import the framework, no venv is needed.
#'
#' @param framework Character; framework name.
#' @return Logical; TRUE if system Python has the deps.
#' @keywords internal
.system_python_has_framework <- function(framework) {
  python <- Sys.which("python3")
  if (!nzchar(python)) return(FALSE)

  # Check framework module
  check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]]
  if (is.null(check_mod)) return(TRUE)

  cmd <- paste0(shQuote(python), " -c ", shQuote(paste0("import ", check_mod)))
  rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
  if (rc != 0L) return(FALSE)

  # Also verify pyarrow (required for Parquet data loading)
  cmd_pa <- paste0(shQuote(python), " -c ", shQuote("import pyarrow"))
  rc_pa <- suppressWarnings(system(cmd_pa, ignore.stdout = TRUE, ignore.stderr = TRUE))
  rc_pa == 0L
}

#' Create or verify a Python venv for a framework
#'
#' Idempotent: if the venv exists and is healthy, returns immediately.
#' If the venv is missing or stale, creates/rebuilds it.
#'
#' Uses a lock file to prevent concurrent creation by parallel sessions.
#'
#' @param framework Character; framework name ("sklearn", "pytorch", etc.).
#' @param timeout_secs Numeric; max seconds to wait for install (default 600).
#' @return Named list with \code{python} and \code{flower_supernode} paths.
#' @keywords internal
.ensure_python_env <- function(framework, timeout_secs = 600) {
  # Fast path: system Python already has everything
  if (.system_python_has_framework(framework)) {
    python <- Sys.which("python3")
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
    # Simple file-based lock with timeout
    deadline <- Sys.time() + timeout_secs
    repeat {
      # Try to create lock file atomically
      if (!file.exists(lock_path)) {
        tryCatch({
          con <- file(lock_path, open = "wx")  # exclusive create
          writeLines(as.character(Sys.getpid()), con)
          close(con)
          lock_acquired <- TRUE
        }, error = function(e) {})
      }

      if (lock_acquired) break

      # Another process holds the lock -- check if it's stale (>15min)
      lock_age <- difftime(Sys.time(),
                           file.info(lock_path)$mtime,
                           units = "mins")
      if (!is.na(lock_age) && lock_age > 15) {
        unlink(lock_path)
        next
      }

      if (Sys.time() > deadline) {
        stop("Timeout waiting for Python environment lock for '", framework,
             "'. Another session may be installing. ",
             "Remove ", lock_path, " if stuck.", call. = FALSE)
      }

      # Check if another process finished while we waited
      if (.venv_is_healthy(venv_path, framework)) {
        return(list(
          python = file.path(venv_path, "bin", "python"),
          flower_supernode = file.path(venv_path, "bin", "flower-supernode"),
          source = "venv"
        ))
      }

      Sys.sleep(5)
    }

    # We hold the lock. Double-check (another session might have finished).
    if (.venv_is_healthy(venv_path, framework)) {
      return(list(
        python = file.path(venv_path, "bin", "python"),
        flower_supernode = file.path(venv_path, "bin", "flower-supernode"),
        source = "venv"
      ))
    }

    # Create the venv
    message("dsFlower: creating Python environment for '", framework, "'...")
    message("  This may take several minutes on first use.")

    system_python <- Sys.which("python3")
    if (!nzchar(system_python)) {
      stop("python3 not found on PATH. Cannot create virtual environment.",
           call. = FALSE)
    }

    # Clear stale venv if any
    if (dir.exists(venv_path)) {
      unlink(venv_path, recursive = TRUE)
    }

    # Create venv with system site-packages access for pre-installed deps
    rc <- system2(system_python,
                  c("-m", "venv", "--system-site-packages", venv_path),
                  stdout = "", stderr = "")
    if (rc != 0L) {
      stop("Failed to create Python venv at ", venv_path,
           ". Check that python3-venv is installed.", call. = FALSE)
    }

    # Install framework-specific deps -- prefer uv (17x faster), fall back to pip
    deps <- .python_deps_for_framework(framework)
    message("  Installing: ", paste(deps, collapse = ", "))

    uv <- Sys.which("uv")
    if (nzchar(uv)) {
      venv_python <- file.path(venv_path, "bin", "python")
      install_args <- c("pip", "install", "--python", venv_python, "--quiet", deps)
      result <- processx::run(
        command = uv, args = install_args,
        error_on_status = FALSE,
        timeout = timeout_secs
      )
    } else {
      pip <- file.path(venv_path, "bin", "pip")
      pip_args <- c("install", "--quiet", "--no-cache-dir", deps)
      result <- processx::run(
        command = pip, args = pip_args,
        error_on_status = FALSE,
        timeout = timeout_secs
      )
    }

    if (result$status != 0L) {
      # Clean up broken venv
      unlink(venv_path, recursive = TRUE)
      stop("pip install failed for '", framework, "' environment:\n",
           result$stderr, call. = FALSE)
    }

    # Verify the install worked
    check_mod <- .FRAMEWORK_HEALTH_IMPORT[[framework]] %||% "flwr"
    venv_python <- file.path(venv_path, "bin", "python")
    cmd <- paste0(shQuote(venv_python), " -c ", shQuote(paste0("import ", check_mod)))
    rc <- suppressWarnings(system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE))
    if (rc != 0L) {
      unlink(venv_path, recursive = TRUE)
      stop("Python environment created but '", check_mod,
           "' import failed. Installation may be incomplete.", call. = FALSE)
    }

    # Verify flower-supernode is available
    supernode <- file.path(venv_path, "bin", "flower-supernode")
    if (!file.exists(supernode)) {
      unlink(venv_path, recursive = TRUE)
      stop("flower-supernode not found in venv. ",
           "flwr may not have installed correctly.", call. = FALSE)
    }

    # Write ready marker with deps hash
    dep_hash <- .deps_hash(deps)
    writeLines(dep_hash, file.path(venv_path, ".dsflower_ready"))

    message("  Python environment for '", framework, "' ready.")

    list(
      python = venv_python,
      flower_supernode = supernode,
      source = "venv"
    )
  }, finally = {
    # Release lock
    if (lock_acquired && file.exists(lock_path)) {
      unlink(lock_path)
    }
  })
}

#' List installed Python environments
#'
#' @return A data.frame with framework, path, healthy, size columns.
#' @keywords internal
.list_python_envs <- function() {
  root <- .venv_root()
  if (!dir.exists(root)) {
    return(data.frame(
      framework = character(0), path = character(0),
      healthy = logical(0), stringsAsFactors = FALSE
    ))
  }

  dirs <- list.dirs(root, recursive = FALSE, full.names = FALSE)
  # Filter to known frameworks
  known <- names(.FRAMEWORK_PYTHON_DEPS)
  dirs <- intersect(dirs, known)

  if (length(dirs) == 0) {
    return(data.frame(
      framework = character(0), path = character(0),
      healthy = logical(0), stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(dirs, function(fw) {
    venv_path <- file.path(root, fw)
    data.frame(
      framework = fw,
      path = venv_path,
      healthy = .venv_is_healthy(venv_path, fw),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}
