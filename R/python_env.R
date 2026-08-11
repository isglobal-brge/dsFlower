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

.BASE_PYTHON_DEPS <- c("flwr==1.31.0", "numpy>=1.21.0", "pandas>=1.3.0",
                        "pyarrow>=10.0.0", "cryptography>=42.0.0")

# Dedicated dependency-light runtime for trusted native-tree training,
# validation and portable data-only predictor probes. XGBoost itself remains in
# its separately verified native bundle; no upstream tree package is installed
# here. Exact pins keep this runtime identical to the three-OS contract suite.
.NATIVE_TREE_PYTHON_DEPS <- c(
  "flwr==1.31.0", "numpy==2.4.6", "pandas==3.0.3",
  "pyarrow==23.0.1", "cryptography==46.0.7"
)

# Provisioned frameworks: one PyTorch/Opacus environment for neural and imaging
# tracks plus the dependency-light native-tree runtime declared above.
# sklearn was dropped: its linear models are redundant with pytorch_logreg (and
# torch gives rigorous Opacus DP-SGD), and its tree models have no federated
# protocol. Each torch build is GPU- or CPU-adaptive (see .gpu_present).
.FRAMEWORK_PYTHON_DEPS <- list(
  # ONE torch venv serves BOTH tabular (logreg/mlp) and vision — pytorch_vision is
  # a superset, so it's merged in (the "pytorch_vision" framework aliases to this
  # venv via .framework_venv). opacus: DP-SGD (REQUIRED). torchvision/monai: 2D/3D
  # backbones. SimpleITK: .mha/.mhd/.dcm. nibabel/pynrrd: .nii/.nrrd. Keep aligned
  # with the FAB vision deps in dsFlowerClient::.harness_dependencies(vision=TRUE).
  pytorch = c("torch>=2.0.0,<3.0.0", "opacus>=1.4.0,<2.0.0",
              "torchvision>=0.15.0,<1.0.0",
              "Pillow>=9.0.0", "nibabel>=5.0.0", "pydicom>=2.4.0",
              "pynrrd>=1.0.0", "SimpleITK>=2.2.0", "monai>=1.3.0"),
  `native-tree` = character()
)

.FRAMEWORK_HEALTH_IMPORT <- list(
  # A single import statement verifies every direct runtime dependency, so a
  # partially populated venv is reported unhealthy and re-provisioned.
  pytorch = paste(c(
    "flwr", "numpy", "pandas", "pyarrow", "cryptography", "torch",
    "opacus", "torchvision", "PIL", "nibabel", "pydicom", "nrrd",
    "SimpleITK", "monai"
  ), collapse = ", "),
  `native-tree` = paste(
    c("flwr", "numpy", "pandas", "pyarrow", "cryptography"),
    collapse = ", ")
)

#' Resolve the effective torch backend for THIS run. GPU presence is detected at
#' RUN time via a timeout-bounded processx nvidia-smi (\code{.gpu_present}, which is
#' Rserve-safe), so a GPU handed to the container AFTER install is seen. We only
#' SELECT among already-built venvs, though -- the multi-GB CUDA venv is built by
#' (re)provision, not lazily in a live DS session -- so if a GPU is visible but its
#' venv isn't built yet, we say so and the operator re-provisions.
#'
#'   * Source: per-run override \code{.dsflower_runtime$torch_backend} (set by
#'     flowerEnsureSuperNodeDS from the researcher's call) or the node option
#'     \code{dsflower.torch_backend}; default "auto".
#'   * "auto" -> "gpu" iff a GPU is visible now AND pytorch-gpu is built, else "cpu".
#'   * "cpu"  -> "cpu".
#'   * "gpu"/"cuda"/"cuNNN" -> "gpu", or a clear error (no GPU visible / venv not built).
#' @keywords internal
.resolve_backend <- function(requested = NULL) {
  req <- requested %||% .dsflower_runtime$torch_backend %||%
    .dsf_option("torch_backend", "auto")
  if (!is.character(req) || length(req) != 1L || is.na(req)) {
    stop("dsflower.torch_backend must be one of auto, cpu, gpu, cuda, or ",
         "cu<digits>.", call. = FALSE)
  }
  req <- tolower(req)
  if (identical(req, "cpu")) return("cpu")
  gpu_venv <- dir.exists(file.path(.venv_root(), "pytorch-gpu"))
  if (req %in% c("", "auto"))
    return(if (gpu_venv && .gpu_present()) "gpu" else "cpu")
  if (req %in% c("gpu", "cuda") || grepl("^cu[0-9]+$", req)) {
    if (!.gpu_present())
      stop("torch_backend='", req, "' but no GPU is visible to this node ",
           "(nvidia-smi found none). Give the container GPU access at the node ",
           "(nvidia runtime / --gpus).", call. = FALSE)
    if (!gpu_venv)
      stop("A GPU is visible but the CUDA torch venv is not built on this node. ",
           "Re-provision (the configure builds pytorch-gpu when a GPU is visible); ",
           "the multi-GB CUDA venv is not built lazily mid-session.", call. = FALSE)
    return("gpu")
  }
  stop("dsflower.torch_backend must be one of auto, cpu, gpu, cuda, or ",
       "cu<digits>.", call. = FALSE)
}

#' Normalize a framework / dp-track to its venv. dsFlower runs in ONE torch venv
#' (torch + opacus + torchvision + monai + numpy); every dp-track runs there.
#' Neural training uses Opacus DP-SGD. The egress track uses output perturbation
#' only when every HookApp
#' execution gate holds; otherwise it is a data-independent no-op. The backend
#' picks WHICH copy: "pytorch" (cpu,
#' the universal default) or "pytorch-gpu" (cuda), resolved as late as possible
#' (run time) so a GPU added AFTER install is usable with just a re-provision (to
#' build pytorch-gpu) -- no code change, no install-time backend lock-in.
#' @keywords internal
.framework_venv <- function(framework, backend = NULL) {
  if (identical(framework, "native_tree") ||
      identical(framework, "native-tree")) return("native-tree")
  if (is.null(backend)) backend <- .resolve_backend()
  if (identical(backend, "gpu")) "pytorch-gpu" else "pytorch"
}

.dsflower_runtime <- new.env(parent = emptyenv())

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

.default_venv_root <- function(os_type = .Platform$OS.type) {
  if (identical(os_type, "windows")) {
    user_data <- Sys.getenv("LOCALAPPDATA", "")
    if (!nzchar(user_data)) user_data <- Sys.getenv("APPDATA", "")
    if (!nzchar(user_data)) {
      profile <- Sys.getenv("USERPROFILE", "")
      if (nzchar(profile)) user_data <- file.path(profile, "AppData", "Local")
    }
    if (!nzchar(user_data)) user_data <- path.expand("~")
    return(file.path(user_data, "dsflower", "venvs"))
  }
  "/var/lib/dsflower/venvs"
}

.venv_executable <- function(venv_path, name,
                             os_type = .Platform$OS.type) {
  windows <- identical(os_type, "windows")
  file.path(
    venv_path,
    if (windows) "Scripts" else "bin",
    paste0(name, if (windows) ".exe" else "")
  )
}

#' Get the venv root directory
#' @keywords internal
.venv_root <- function() {
  root <- Sys.getenv("DSFLOWER_VENV_ROOT", "")
  if (nzchar(root)) root else .dsf_option("venv_root", .default_venv_root())
}

#' TRUE when a usable NVIDIA GPU is visible to this process.
#'
#' The single GPU check used to pick CUDA-vs-CPU torch builds. In a container the
#' GPU is visible only if it was started GPU-enabled (nvidia runtime / --gpus),
#' so this correctly reflects what the node can actually use. Force with
#' \code{DSFLOWER_FORCE_GPU=1} / \code{0}.
#' @keywords internal
.gpu_present <- function() {
  ov <- Sys.getenv("DSFLOWER_FORCE_GPU", "")
  if (nzchar(ov)) return(tolower(ov) %in% c("1", "true", "yes"))
  nvidia <- Sys.which("nvidia-smi")
  if (!nzchar(nvidia)) return(FALSE)
  # processx (not a bare system2): fd-/signal-safe AND timeout-bounded, so it is
  # safe to call inside a Rock DS session -- a 5s-bounded processx cannot hang,
  # whereas a bare system2 can deadlock on inherited pipes/fds. `nvidia-smi -L` is
  # instant and emits "GPU 0: ..." per visible device, so a GPU given to the
  # container AFTER install is detected here at run time.
  res <- tryCatch(
    processx::run(nvidia, "-L", error_on_status = FALSE, timeout = 5),
    error = function(e) NULL)
  !is.null(res) && isTRUE(res$status == 0L) &&
    isTRUE(grepl("GPU [0-9]", res$stdout %||% ""))
}

#' Choose the uv torch backend for this environment.
#'
#' "auto" only when an NVIDIA GPU is present (so uv installs the matching CUDA
#' build and the GPU is usable); otherwise "cpu", which keeps the venv small AND
#' avoids uv's "auto" picking the large +xpu build on GPU-less Intel hosts.
#' Override with \code{DSFLOWER_TORCH_BACKEND} / \code{dsflower.torch_backend} (e.g. "cu126").
#' @keywords internal
.torch_backend <- function() {
  ov <- Sys.getenv("DSFLOWER_TORCH_BACKEND",
                   unset = getOption("dsflower.torch_backend", ""))
  if (nzchar(ov)) return(ov)
  if (.gpu_present()) "auto" else "cpu"
}

#' Get all pip dependencies for a framework (GPU/CPU-adaptive)
#' @keywords internal
.python_deps_for_framework <- function(framework) {
  if (identical(framework, "native_tree") ||
      identical(framework, "native-tree")) {
    return(.NATIVE_TREE_PYTHON_DEPS)
  }
  # cpu and gpu venvs share the SAME dependency set (torch etc.); the backend is a
  # uv install FLAG, not a dep, so the deps-hash marker stays backend-independent
  # and identical across pytorch / pytorch-gpu (matches the configure's deps_hash).
  c(.BASE_PYTHON_DEPS, .FRAMEWORK_PYTHON_DEPS[["pytorch"]])
}

#' Compute a hash of the dependency list for staleness detection
#' @keywords internal
.deps_hash <- function(deps) {
  digest::digest(paste(sort(deps, method = "radix"), collapse = "\n"), algo = "sha256",
                 serialize = FALSE)
}

# Optional administrator-owned, fully hashed requirements file. When present it
# replaces range resolution and uv enforces hashes for every transitive package.
.python_lock_required <- function(framework = NULL) {
  native_tree <- identical(framework, "native_tree") ||
    identical(framework, "native-tree")
  environment <- if (native_tree) {
    "DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK"
  } else {
    "DSFLOWER_REQUIRE_PYTHON_LOCK"
  }
  option <- if (native_tree) {
    "native_tree_require_python_lock"
  } else {
    "require_python_lock"
  }
  value <- Sys.getenv(environment, "")
  if (nzchar(value)) {
    value <- tolower(value)
    if (value %in% c("1", "true", "yes")) return(TRUE)
    if (value %in% c("0", "false", "no")) return(FALSE)
    stop(environment, " must be true or false.", call. = FALSE)
  }
  isTRUE(as.logical(.dsf_option(option, FALSE)))
}

.python_lock_path <- function(must_exist = FALSE, framework = NULL) {
  native_tree <- identical(framework, "native_tree") ||
    identical(framework, "native-tree")
  environment <- if (native_tree) {
    "DSFLOWER_NATIVE_TREE_PYTHON_LOCK"
  } else {
    "DSFLOWER_PYTHON_LOCK"
  }
  option <- if (native_tree) "native_tree_python_lock" else "python_lock"
  path <- Sys.getenv(environment, "")
  if (!nzchar(path)) path <- as.character(.dsf_option(option, ""))[1]
  if (!nzchar(path)) {
    if (must_exist && .python_lock_required(framework)) {
      stop("A hash-locked Python environment is required, but ",
           environment, "/dsflower.", option, " is unset.", call. = FALSE)
    }
    return("")
  }
  if (must_exist && (!file.exists(path) || dir.exists(path) || file.access(path, 4L) != 0L)) {
    stop("Configured dsflower Python lock is not a readable regular file: ",
         path, call. = FALSE)
  }
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

.python_version_spec <- function() {
  version <- Sys.getenv("DSFLOWER_PYTHON_VERSION", "")
  if (!nzchar(version)) {
    version <- as.character(.dsf_option("python_version", "3.11"))[1]
  }
  if (!grepl("^[0-9]+\\.[0-9]+(\\.[0-9]+)?$", version)) {
    stop("DSFLOWER_PYTHON_VERSION must be major.minor or major.minor.patch.",
         call. = FALSE)
  }
  version
}

.python_env_spec_hash <- function(framework) {
  python_spec <- paste0("python=", .python_version_spec())
  lock <- .python_lock_path(framework = framework)
  if (!nzchar(lock) && .python_lock_required(framework)) return(NA_character_)
  if (nzchar(lock)) {
    if (!file.exists(lock) || dir.exists(lock) || file.access(lock, 4L) != 0L) {
      return(NA_character_)
    }
    return(paste0(python_spec, ";lock-sha256:",
                  digest::digest(file = lock, algo = "sha256")))
  }
  .deps_hash(c(python_spec, .python_deps_for_framework(framework)))
}

.uv_bootstrap_config <- function() {
  version <- Sys.getenv("DSFLOWER_UV_VERSION", "")
  if (!nzchar(version)) version <- as.character(.dsf_option("uv_version", ""))[1]
  sha256 <- Sys.getenv("DSFLOWER_UV_SHA256", "")
  if (!nzchar(sha256)) sha256 <- as.character(.dsf_option("uv_sha256", ""))[1]
  if (!nzchar(version) || !nzchar(sha256)) {
    stop("uv is not installed and mutable 'latest' bootstrap is disabled. ",
         "Install uv through the operating system, or configure both ",
         "DSFLOWER_UV_VERSION and DSFLOWER_UV_SHA256 for an audited release.",
         call. = FALSE)
  }
  if (!grepl("^[0-9]+\\.[0-9]+\\.[0-9]+([.-][0-9A-Za-z.-]+)?$", version)) {
    stop("DSFLOWER_UV_VERSION is not a valid release tag.", call. = FALSE)
  }
  if (!grepl("^[0-9A-Fa-f]{64}$", sha256)) {
    stop("DSFLOWER_UV_SHA256 must be 64 hexadecimal characters.", call. = FALSE)
  }
  list(version = version, sha256 = tolower(sha256))
}

# Record the exact resolved environment for deployment audit/rebuild evidence.
# This is metadata only: the dependency-range hash remains the health marker.
.record_venv_versions <- function(venv_path) {
  site_dirs <- .venv_site_packages_dirs(venv_path)
  rows <- character()
  for (site_dir in site_dirs) {
    infos <- list.files(site_dir, pattern = "\\.dist-info$", full.names = TRUE)
    for (info in infos) {
      lines <- tryCatch(readLines(file.path(info, "METADATA"), warn = FALSE),
                        error = function(e) character())
      name <- sub("^Name:\\s*", "", lines[grepl("^Name:\\s*", lines)])
      version <- sub("^Version:\\s*", "", lines[grepl("^Version:\\s*", lines)])
      if (length(name) && length(version) && nzchar(name[[1]]) && nzchar(version[[1]])) {
        rows <- c(rows, paste0(name[[1]], "==", version[[1]]))
      }
    }
  }
  target <- file.path(venv_path, ".dsflower_versions.txt")
  tmp <- tempfile(pattern = ".dsflower-versions-", tmpdir = venv_path)
  on.exit(unlink(tmp), add = TRUE)
  writeLines(sort(unique(rows), method = "radix"), tmp)
  if (!file.rename(tmp, target)) return(FALSE)
  TRUE
}

#' Check if a Python venv is healthy
#' @keywords internal
.venv_is_healthy <- function(venv_path, framework) {
  # File-based health check ONLY -- no subprocess calls.
  # system()/system2() hang in Rserve child processes (Rock DS sessions).
  # The configure script writes .dsflower_ready after verifying imports,
  # so if the marker + binaries exist, the venv is healthy.
  python <- .venv_executable(venv_path, "python")
  if (!file.exists(python)) return(FALSE)
  marker <- file.path(venv_path, ".dsflower_ready")
  if (!file.exists(marker)) return(FALSE)
  expected_hash <- .python_env_spec_hash(framework)
  current_hash <- tryCatch(readLines(marker, warn = FALSE, n = 1),
                           error = function(e) "")
  if (!identical(current_hash, expected_hash)) return(FALSE)
  supernode <- .venv_executable(venv_path, "flower-supernode")
  if (!file.exists(supernode)) return(FALSE)
  TRUE
}

#' Check if the system Python already satisfies framework dependencies
#' @keywords internal
.system_python_has_framework <- function(framework) {
  # Skip system Python check entirely in server contexts.
  # system()/system2() hang in Rserve (Rock DataSHIELD sessions).
  # The venv check (.venv_is_healthy) is the correct fast path.
  FALSE
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
  framework <- .framework_venv(framework)  # pytorch_vision -> merged pytorch venv
  # Fast path: check venv FIRST (avoids system() calls that hang in Rserve)
  root <- .venv_root()
  venv_path <- file.path(root, framework)
  if (.venv_is_healthy(venv_path, framework)) {
    return(list(
      python = .venv_executable(venv_path, "python"),
      flower_supernode = .venv_executable(venv_path, "flower-supernode"),
      source = "venv"
    ))
  }

  # System Python check (skip in Rserve contexts to avoid hangs)
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
      python = .venv_executable(venv_path, "python"),
      flower_supernode = .venv_executable(venv_path, "flower-supernode"),
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
          python = .venv_executable(venv_path, "python"),
          flower_supernode = .venv_executable(venv_path, "flower-supernode"),
          source = "venv"
        ))
      }

      Sys.sleep(5)
    }

    # We hold the lock. Double-check.
    if (.venv_is_healthy(venv_path, framework)) {
      return(list(
        python = .venv_executable(venv_path, "python"),
        flower_supernode = .venv_executable(venv_path, "flower-supernode"),
        source = "venv"
      ))
    }

    # Create the venv via uv (uv downloads Python if needed)
    message("dsFlower: creating Python environment for '", framework, "'...")
    message("  This may take several minutes on first use.")

    if (dir.exists(venv_path)) unlink(venv_path, recursive = TRUE)

    uv <- .ensure_uv()
    # processx passes args without a shell, so a venv_path containing spaces
    # (e.g. macOS "Application Support") is handled correctly.
    rc <- processx::run(uv, c("venv", "--python", .python_version_spec(),
                              "--quiet", venv_path),
                        error_on_status = FALSE)$status
    if (rc != 0L)
      stop("Failed to create venv at ", venv_path, call. = FALSE)

    # Install deps via uv with an ADAPTIVE torch backend (CUDA when a GPU is
    # visible, else CPU; see .torch_backend). CPU avoids the CUDA build's unused
    # ~3.5 GB nvidia/triton libs on GPU-less nodes.
    deps <- .python_deps_for_framework(framework)
    lock <- .python_lock_path(must_exist = TRUE, framework = framework)
    torch_flag <- if (any(grepl("torch", deps, fixed = TRUE)))
      c("--torch-backend", .torch_backend()) else character(0)
    install_spec <- if (nzchar(lock)) c("--require-hashes", "-r", lock) else deps
    if (nzchar(lock)) {
      message("  Installing administrator hash-locked Python requirements")
    } else {
      message("  Installing: ", paste(deps, collapse = ", "))
    }
    venv_python <- .venv_executable(venv_path, "python")
    result <- processx::run(
      command = uv,
      args = c("pip", "install", "--python", venv_python, "--quiet",
               torch_flag, install_spec),
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
    venv_python <- .venv_executable(venv_path, "python")
    verified <- tryCatch(
      processx::run(
        venv_python, c("-c", paste0("import ", check_mod)),
        error_on_status = FALSE, timeout = timeout_secs
      ),
      error = function(e) NULL
    )
    if (is.null(verified) || !identical(verified$status, 0L)) {
      unlink(venv_path, recursive = TRUE)
      stop("'", check_mod, "' import failed after install.", call. = FALSE)
    }

    supernode <- .venv_executable(venv_path, "flower-supernode")
    if (!file.exists(supernode)) {
      unlink(venv_path, recursive = TRUE)
      stop("flower-supernode not found in venv.", call. = FALSE)
    }

    # Reclaim disk: the uv download cache (~1.5 GB of wheels) is only needed to
    # (re)install -- the built venv is self-contained -- so drop it once provisioning
    # succeeds. This keeps EVERY node lean (not just the Docker image), enforced by the
    # package itself. Best-effort; opt out with options(dsflower.clean_uv_cache=FALSE) to
    # keep the cache warm for frequent re-provisioning.
    if (isTRUE(as.logical(.dsf_option("clean_uv_cache", TRUE)))) {
      tryCatch(processx::run(uv, c("cache", "clean"), error_on_status = FALSE,
                             timeout = 120), error = function(e) NULL)
    }

    .record_venv_versions(venv_path)
    dep_hash <- .python_env_spec_hash(framework)
    marker_tmp <- tempfile(pattern = ".dsflower-ready-", tmpdir = venv_path)
    writeLines(dep_hash, marker_tmp)
    if (!file.rename(marker_tmp, file.path(venv_path, ".dsflower_ready"))) {
      unlink(marker_tmp)
      stop("Could not atomically record the Python environment marker.", call. = FALSE)
    }
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

#' Read the Python version recorded by a virtual environment
#' @keywords internal
.read_venv_python_version <- function(venv_path) {
  cfg <- file.path(venv_path, "pyvenv.cfg")
  if (!file.exists(cfg)) return("unknown")
  lines <- tryCatch(readLines(cfg, warn = FALSE), error = function(e) character())
  version <- sub("^version_info\\s*=\\s*", "",
                 lines[grepl("^version_info\\s*=", lines)])
  if (length(version) > 0 && nzchar(version[1])) return(version[1])
  "unknown"
}

#' Read a Python package version from venv dist-info metadata
#' @keywords internal
.read_venv_package_version <- function(venv_path, package) {
  site_dirs <- .venv_site_packages_dirs(venv_path)
  if (length(site_dirs) == 0) return("unknown")

  pattern <- paste0("^", package, "-.*\\.dist-info$")
  for (site_dir in site_dirs) {
    infos <- list.files(site_dir, pattern = pattern, full.names = TRUE)
    for (info in infos) {
      metadata <- file.path(info, "METADATA")
      if (!file.exists(metadata)) next
      lines <- tryCatch(readLines(metadata, warn = FALSE),
                        error = function(e) character())
      version <- sub("^Version:\\s*", "", lines[grepl("^Version:\\s*", lines)])
      if (length(version) > 0 && nzchar(version[1])) return(version[1])
    }
  }
  "unknown"
}

#' Locate site-packages directories inside a virtual environment
#' @keywords internal
.venv_site_packages_dirs <- function(venv_path) {
  lib_roots <- unique(file.path(venv_path, c("lib", "Lib")))
  lib_roots <- lib_roots[dir.exists(lib_roots)]
  if (!length(lib_roots)) return(character())
  lib_dirs <- unique(unlist(lapply(lib_roots, function(lib_root) {
    tryCatch(
      list.dirs(lib_root, recursive = TRUE, full.names = TRUE),
      error = function(e) character()
    )
  }), use.names = FALSE))
  lib_dirs[tolower(basename(lib_dirs)) == "site-packages"]
}

#' Summarise provisioned Python runtime without spawning subprocesses
#' @keywords internal
.python_runtime_capabilities <- function() {
  envs <- .list_python_envs()
  healthy <- envs[isTRUE(envs$healthy) | envs$healthy, , drop = FALSE]
  if (nrow(healthy) == 0) {
    return(list(
      python_version = "not provisioned",
      flower_version = "not provisioned",
      torch_version = "not provisioned",
      opacus_version = "not provisioned",
      runtime_versions_sha256 = "not recorded",
      python_envs = envs
    ))
  }

  torch <- healthy[healthy$framework %in% c("pytorch", "pytorch-gpu"),
                   , drop = FALSE]
  first <- if (nrow(torch)) torch$path[[1L]] else healthy$path[[1L]]
  list(
    python_version = .read_venv_python_version(first),
    flower_version = .read_venv_package_version(first, "flwr"),
    torch_version = .read_venv_package_version(first, "torch"),
    opacus_version = .read_venv_package_version(first, "opacus"),
    runtime_versions_sha256 = {
      versions <- file.path(first, ".dsflower_versions.txt")
      if (file.exists(versions)) digest::digest(file = versions, algo = "sha256")
      else "not recorded"
    },
    python_envs = envs
  )
}

# ---------------------------------------------------------------------------
# uv bootstrap
# ---------------------------------------------------------------------------

.uv_release_asset <- function(sysname = Sys.info()[["sysname"]],
                              machine = Sys.info()[["machine"]]) {
  sysname <- tolower(sysname)
  machine <- tolower(machine)
  os <- switch(sysname,
    darwin = "apple-darwin",
    linux = "unknown-linux-gnu",
    windows = "pc-windows-msvc",
    stop("Unsupported OS: ", sysname,
         ". Install uv: https://docs.astral.sh/uv/", call. = FALSE)
  )
  arch <- switch(machine,
    x86_64 = "x86_64", amd64 = "x86_64", `x86-64` = "x86_64",
    aarch64 = "aarch64", arm64 = "aarch64",
    stop("Unsupported arch: ", machine, call. = FALSE)
  )
  windows <- identical(os, "pc-windows-msvc")
  triple <- paste(arch, os, sep = "-")
  executable <- if (windows) "uv.exe" else "uv"
  list(
    triple = triple,
    archive_ext = if (windows) ".zip" else ".tar.gz",
    executable = executable,
    member = paste0("uv-", triple, "/", executable)
  )
}

#' Ensure uv is available (find or download)
#' @keywords internal
.ensure_uv <- function() {
  cached <- .dsflower_runtime$uv_path
  if (!is.null(cached) && file.exists(cached)) return(cached)

  # PATH
  uv <- Sys.which("uv")
  if (nzchar(uv)) { .dsflower_runtime$uv_path <- uv; return(uv) }

  # Common locations
  asset <- .uv_release_asset()
  home <- Sys.getenv("HOME", "~")
  for (p in c(file.path(home, ".local", "bin", asset$executable),
              file.path(home, ".cargo", "bin", asset$executable),
              file.path("/usr/local/bin", asset$executable))) {
    if (file.exists(p)) { .dsflower_runtime$uv_path <- p; return(p) }
  }

  # Download one immutable, administrator-pinned standalone archive.
  tools_dir <- file.path(.venv_root(), ".tools")
  dir.create(tools_dir, recursive = TRUE, showWarnings = FALSE)
  uv_path <- file.path(tools_dir, asset$executable)
  if (file.exists(uv_path)) {
    .dsflower_runtime$uv_path <- uv_path
    return(uv_path)
  }

  bootstrap <- .uv_bootstrap_config()
  message("dsFlower: downloading pinned uv ", bootstrap$version, "...")
  url <- paste0("https://github.com/astral-sh/uv/releases/download/",
                bootstrap$version, "/uv-", asset$triple,
                asset$archive_ext)
  tmp <- tempfile(fileext = asset$archive_ext)
  tmp_dir <- tempfile()
  on.exit({ unlink(tmp); unlink(tmp_dir, recursive = TRUE) }, add = TRUE)

  rc <- tryCatch(utils::download.file(url, tmp, mode = "wb", quiet = TRUE),
                  error = function(e) 1L)
  if (!identical(rc, 0L))
    stop("Failed to download pinned uv. Install manually: https://docs.astral.sh/uv/",
         call. = FALSE)

  actual <- digest::digest(file = tmp, algo = "sha256")
  if (!identical(tolower(actual), bootstrap$sha256)) {
    stop("Downloaded uv archive SHA-256 mismatch; refusing to extract it.",
         call. = FALSE)
  }

  dir.create(tmp_dir, showWarnings = FALSE)
  entries <- if (identical(asset$archive_ext, ".zip")) {
    utils::unzip(tmp, list = TRUE)$Name
  } else {
    utils::untar(tmp, list = TRUE)
  }
  if (!asset$member %in% entries) {
    stop("uv binary not found in verified archive.", call. = FALSE)
  }
  if (identical(asset$archive_ext, ".zip")) {
    utils::unzip(tmp, files = asset$member, exdir = tmp_dir)
  } else {
    utils::untar(tmp, files = asset$member, exdir = tmp_dir)
  }
  source <- file.path(tmp_dir, asset$member)
  install_tmp <- tempfile(pattern = ".uv-", tmpdir = tools_dir)
  if (!file.copy(source, install_tmp, overwrite = TRUE)) {
    stop("Could not stage verified uv binary.", call. = FALSE)
  }
  Sys.chmod(install_tmp, "0755")
  if (!file.rename(install_tmp, uv_path)) {
    unlink(install_tmp)
    stop("Could not atomically install verified uv binary.", call. = FALSE)
  }
  message("dsFlower: uv installed at ", uv_path)
  .dsflower_runtime$uv_path <- uv_path
  uv_path
}
