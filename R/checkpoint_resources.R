# Public checkpoint admission. Platform resource ACLs authorize the resource
# route; the separate analyst upload route is gated by the server policy.
.CHECKPOINT_CONTRACT <- "pytorch_resnet18_segmentation"
.CHECKPOINT_MAX_BYTES <- 64 * 1024^2
.CHECKPOINT_CHUNK_BYTES <- 512 * 1024

.checkpoint_sha256 <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) &&
    grepl("\\A[0-9a-f]{64}\\z", x, perl = TRUE)
}

.checkpoint_symbol <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) &&
    grepl("\\A[A-Za-z][A-Za-z0-9_.]{0,127}\\z", x, perl = TRUE)
}

.public_initialisation_policy <- function(contract = .CHECKPOINT_CONTRACT) {
  policy <- .dsf_option("public_initialisation", "analyst_or_resource")
  if (!is.null(contract)) {
    policy <- .dsf_option(paste0("public_initialisation.", contract), policy)
  }
  if (!is.character(policy) || length(policy) != 1L || is.na(policy) ||
      !policy %in% c("analyst_or_resource", "resource_only", "none")) {
    stop("dsflower.public_initialisation must be 'analyst_or_resource', ",
         "'resource_only', or 'none'.", call. = FALSE)
  }
  policy
}

.public_initialisation_policy_status <- function() {
  list(default = .public_initialisation_policy(NULL), contracts =
    stats::setNames(list(.public_initialisation_policy()), .CHECKPOINT_CONTRACT))
}

.require_checkpoint_policy <- function(origin) {
  policy <- .public_initialisation_policy()
  if (identical(policy, "none") ||
      (identical(origin, "analyst-declared") &&
       !identical(policy, "analyst_or_resource"))) {
    stop("Public initialisation is refused by the custodian policy.", call. = FALSE)
  }
  policy
}

.checkpoint_cache_root <- function() {
  root <- .dsf_option("checkpoint_cache_dir", file.path(
    tools::R_user_dir("dsFlower", "data"), "checkpoints"))
  if (!is.character(root) || length(root) != 1L || is.na(root) ||
      !.path_is_absolute(root) || .privacy_path_is_link(root)) {
    stop("The checkpoint cache must be an absolute protected directory.", call. = FALSE)
  }
  # Resolve platform aliases such as macOS /var before checking every remaining
  # component. A link at the configured cache itself is never accepted.
  root <- .canonical_state_path(root)
  probe <- root
  repeat {
    if (.privacy_path_is_link(probe)) {
      stop("The checkpoint cache must not contain symbolic links.", call. = FALSE)
    }
    if (identical(dirname(probe), probe)) break
    probe <- dirname(probe)
  }
  forbidden <- c(file.path(.stagingBaseCandidates(FALSE), "dsflower"),
    .venv_root(), .dsf_option("app_spool_root", Sys.getenv(
      "DSFLOWER_APP_SPOOL_ROOT", unset = "/var/lib/dsflower/appstore")),
    dirname(.node_secret_path()))
  forbidden <- vapply(forbidden, .canonical_state_path, character(1))
  if (any(vapply(forbidden, function(path) identical(path, root) ||
      startsWith(root, paste0(path, "/")) || startsWith(path, paste0(root, "/")),
      logical(1)))) {
    stop("The checkpoint cache must be outside staging, Hook mounts and node-secret storage.",
         call. = FALSE)
  }
  previous <- Sys.umask("0077")
  on.exit(Sys.umask(previous), add = TRUE)
  if (!dir.exists(root)) {
    dir.create(root, recursive = TRUE, mode = "0700")
    if (.Platform$OS.type == "windows") .windows_set_private_acl(root, is_directory = TRUE)
  }
  .checkpoint_private_directory(root)
  root
}

.checkpoint_private_directory <- function(path) {
  info <- file.info(path)
  if (.privacy_path_is_link(path) || !dir.exists(path)) {
    stop("Checkpoint storage must be service-owned with mode 0700.", call. = FALSE)
  }
  if (.Platform$OS.type == "windows") {
    .windows_validate_private_acl(path)
  } else if (.Platform$OS.type != "unix" ||
      !identical(as.integer(info$uid[[1L]]), .privacy_effective_uid()) ||
      bitwAnd(as.integer(info$mode[[1L]]), strtoi("777", 8L)) != strtoi("700", 8L)) {
    stop("Checkpoint storage must be service-owned with mode 0700.", call. = FALSE)
  }
  invisible(path)
}

.checkpoint_work_directory <- function() {
  root <- .checkpoint_cache_root()
  path <- tempfile(".acquire-", tmpdir = root)
  if (!dir.create(path, mode = "0700")) {
    stop("Checkpoint acquisition storage is unavailable.", call. = FALSE)
  }
  if (.Platform$OS.type == "windows") .windows_set_private_acl(path, is_directory = TRUE)
  .checkpoint_private_directory(path)
  path
}

.checkpoint_public_summary <- function(summary, origin = NULL) {
  fields <- c("provenance", "checkpoint_sha256", "encoder_sha256",
              "tensor_schema", "identity_version")
  if (!is.list(summary) || !is.list(summary$provenance) ||
      !.checkpoint_sha256(summary$provenance$manifest_sha256) ||
      !.checkpoint_sha256(summary$checkpoint_sha256) ||
      !.checkpoint_sha256(summary$encoder_sha256) ||
      !is.list(summary$provenance$manifest) || !is.list(summary$tensor_schema) ||
      !is.character(summary$identity_version) || length(summary$identity_version) != 1L) {
    stop("Public checkpoint verification failed before private staging.", call. = FALSE)
  }
  result <- summary[fields]
  result$manifest_sha256 <- summary$provenance$manifest_sha256
  if (!is.null(origin)) result$origin <- origin
  result
}

# An isolated trusted verifier receives only node-owned filenames, never a
# resource credential or analyst-selected node path. Tensor parsing is Python's
# closed data-only bundle implementation shared with the runner.
.checkpoint_verify <- function(action, path, expected_sha256 = NULL, spec = NULL,
    runtime = .resolve_framework_runtime("pytorch"),
    runner_dir = system.file("flower_app", "dsflower_runner", package = "dsFlower"),
    run_probe = processx::run) {
  if (!action %in% c("admit", "verify") ||
      (!is.null(expected_sha256) && !.checkpoint_sha256(expected_sha256))) {
    stop("Invalid checkpoint verification request.", call. = FALSE)
  }
  code <- paste(
    "import json, sys", "sys.path.insert(0, sys.argv[1])",
    "from dsflower_runner.segmentation_checkpoints import admit_bundle, verify_snapshot",
    "p = json.loads(sys.argv[2])",
    "if p['action'] == 'admit':",
    "    out = admit_bundle(p['path'], p['cache'], expected_bundle_sha256=p['sha256'], decoder_spec=p['spec'])",
    "else:",
    "    out = verify_snapshot(p['path'], p['sha256'], decoder_spec=p['spec'])",
    "print(json.dumps(out, allow_nan=False))", sep = "\n")
  args <- list(action = action, path = path, sha256 = expected_sha256,
               spec = spec, cache = .checkpoint_cache_root())
  inherited_names <- c("LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR",
    "CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES", "XDG_CACHE_HOME")
  inherited <- Sys.getenv(inherited_names, unset = NA_character_)
  inherited <- inherited[!is.na(inherited)]
  env <- c(inherited, LD_PRELOAD = "", LD_LIBRARY_PATH = "", DYLD_LIBRARY_PATH = "",
    PYTHONHOME = "", PYTHONPATH = "", PYTHONNOUSERSITE = "1", PYTHONHASHSEED = "0",
    CUBLAS_WORKSPACE_CONFIG = ":4096:8", VIRTUAL_ENV = runtime$venv_path,
    PATH = paste(file.path(runtime$venv_path, "bin"), Sys.getenv("PATH", ""),
                 sep = .Platform$path.sep))
  result <- tryCatch({
    if (!nzchar(runner_dir) || !dir.exists(runner_dir)) stop("missing runner")
    run_probe(runtime$python, c("-I", "-c", code, dirname(runner_dir),
      as.character(jsonlite::toJSON(args, auto_unbox = TRUE, null = "null",
                                    digits = I(17)))),
      env = env, error_on_status = FALSE, timeout = 180)
  }, error = function(e) NULL)
  summary <- if (is.list(result) && identical(as.integer(result$status), 0L) &&
      is.character(result$stdout) && length(result$stdout) == 1L &&
      nchar(result$stdout, type = "bytes") <= 2 * 1024^2) {
    tryCatch(jsonlite::fromJSON(result$stdout, simplifyVector = FALSE),
             error = function(e) NULL)
  } else NULL
  .checkpoint_public_summary(summary)
  if (!is.null(expected_sha256) &&
      !identical(if (identical(action, "verify")) {
        summary$provenance$manifest_sha256
      } else summary$bundle_sha256, expected_sha256)) {
    stop("Checkpoint verifier returned a different admitted identity.", call. = FALSE)
  }
  root <- .checkpoint_cache_root()
  directory <- summary$snapshot_directory
  if (!is.character(directory) || length(directory) != 1L || is.na(directory) ||
      .privacy_path_is_link(directory) || !dir.exists(directory)) {
    stop("Checkpoint verifier returned invalid protected state.", call. = FALSE)
  }
  directory <- normalizePath(directory, winslash = "/", mustWork = TRUE)
  if (!identical(dirname(directory), root)) {
    stop("Checkpoint verifier returned invalid protected state.", call. = FALSE)
  }
  .checkpoint_private_directory(directory)
  summary$snapshot_directory <- directory
  summary
}

.checkpoint_decoder_spec <- function(run_config) {
  b64 <- run_config[["model-spec-b64"]]
  if (!is.character(b64) || length(b64) != 1L || is.na(b64) ||
      nchar(b64, type = "bytes") > 16384L) {
    stop("Public initialisation requires a bounded decoder spec.", call. = FALSE)
  }
  raw <- tryCatch(jsonlite::base64_dec(b64), error = function(e) NULL)
  canonical <- if (!is.null(raw)) gsub("[\r\n]", "", jsonlite::base64_enc(raw))
  spec <- tryCatch(jsonlite::fromJSON(rawToChar(raw), simplifyVector = FALSE),
                   error = function(e) NULL)
  if (!identical(canonical, b64) || !is.list(spec) || is.null(names(spec))) {
    stop("Public initialisation requires a canonical decoder spec.", call. = FALSE)
  }
  spec
}

.is_checkpoint_resource_format <- function(format) {
  is.character(format) && length(format) == 1L && !is.na(format) &&
    grepl("\\Adsflower-checkpoint-v1:[0-9a-f]{64}\\z", format, perl = TRUE)
}

#' Resolver for custodian-registered checkpoint bundles
#' @export
CheckpointResourceResolver <- R6::R6Class("CheckpointResourceResolver",
  inherit = resourcer::ResourceResolver, public = list(
    #' @description Match only the digest-pinned dsFlower checkpoint format.
    #' @param x Resource descriptor.
    isFor = function(x) .is_checkpoint_resource_format(tryCatch(
      x[["format", exact = TRUE]], error = function(e) NULL)),
    #' @description Materialize a verified checkpoint snapshot immediately.
    #' @param x Resource descriptor.
    newClient = function(x) CheckpointResourceClient$new(x)))

# Download through resourcer's file-client contract with bounded, private
# acquisition. S3 needs a registered getter with an explicit bounded interface.
.CheckpointFileGetter <- R6::R6Class("CheckpointFileGetter",
  inherit = resourcer::FileResourceGetter,
  private = list(directory = NULL), public = list(
    initialize = function(directory) private$directory <- directory,
    isFor = function(resource) TRUE,
    downloadFile = function(resource, ...) {
      target <- file.path(private$directory, "bundle.zip")
      scheme <- tolower(sub(":.*$", "", resource$url))
      if (scheme %in% c("http", "https")) {
        headers <- if (!is.null(resource$identity) && nzchar(resource$identity)) {
          httr::authenticate(resource$identity, resource$secret %||% "")
        } else if (!is.null(resource$secret) && nzchar(resource$secret)) {
          httr::add_headers(Authorization = paste("Bearer", resource$secret))
        } else NULL
        progress <- function(total, now, ...) {
          !any(c(total, now) > .CHECKPOINT_MAX_BYTES)
        }
        response <- httr::GET(resource$url, headers, httr::write_disk(target, overwrite = TRUE),
          handle = httr::handle(resource$url),
          httr::timeout(120), httr::config(noprogress = FALSE,
            progressfunction = progress, maxfilesize_large = .CHECKPOINT_MAX_BYTES))
        httr::stop_for_status(response)
      } else if (identical(scheme, "s3")) {
        getter <- resourcer::findFileResourceGetter(resource)
        bounded <- if (is.environment(getter)) getter$downloadFileBounded else NULL
        if (!is.function(bounded)) {
          stop("S3 checkpoint transport requires a registered downloadFileBounded getter.", call. = FALSE)
        }
        result <- bounded(resource, destination = target,
                          max_bytes = .CHECKPOINT_MAX_BYTES, timeout = 120)
        if (!identical(result, target) || .privacy_path_is_link(target) ||
            !.path_is_regular_file(target)) {
          stop("S3 checkpoint transport did not return its protected destination.", call. = FALSE)
        }
      } else if (identical(scheme, "file")) {
        getter <- resourcer::LocalFileResourceGetter$new()
        file <- getter$downloadFile(resource)
        if (isTRUE(file$temp)) on.exit(unlink(file$path), add = TRUE)
        size <- file.info(file$path)$size
        if (length(size) != 1L || is.na(size) || size < 1 ||
            size > .CHECKPOINT_MAX_BYTES || !.path_is_regular_file(file$path) ||
            !file.copy(file$path, target, copy.mode = FALSE)) {
          stop("Checkpoint acquisition failed.", call. = FALSE)
        }
      } else {
        stop("Unsupported checkpoint transport; use file, HTTP(S), or a bounded S3 getter.",
             call. = FALSE)
      }
      Sys.chmod(target, "0600")
      if (.Platform$OS.type == "windows") .windows_set_private_acl(target, is_directory = FALSE)
      size <- file.info(target)$size
      if (length(size) != 1L || is.na(size) || size < 1 || size > .CHECKPOINT_MAX_BYTES) {
        stop("Checkpoint archive exceeds the acquisition limit.", call. = FALSE)
      }
      structure(list(path = target, temp = FALSE), class = "resource.file")
    }))

#' A verified public checkpoint resource
#'
#' Resource credentials are discarded after immediate acquisition. Generic
#' table conversion is deliberately unavailable.
#' @export
CheckpointResourceClient <- R6::R6Class("CheckpointResourceClient",
  inherit = resourcer::ResourceClient,
  private = list(snapshot = NULL), public = list(
    #' @description Acquire and verify a registered descriptor.
    #' @param resource A custodian-registered resource descriptor.
    initialize = function(resource) {
      .require_checkpoint_policy("resource")
      if (!inherits(resource, "resource") ||
          !.is_checkpoint_resource_format(resource$format) ||
          !is.character(resource$url) || length(resource$url) != 1L ||
          is.na(resource$url) || !grepl("\\A(file|https?|s3):", resource$url,
                                      perl = TRUE)) {
        stop("Invalid checkpoint resource descriptor.", call. = FALSE)
      }
      if (startsWith(resource$url, "s3:")) {
        getter <- resourcer::findFileResourceGetter(resource)
        if (!is.environment(getter) || !is.function(getter$downloadFileBounded)) {
          stop("S3 checkpoint transport requires an installed, registered downloadFileBounded getter.",
               call. = FALSE)
        }
      }
      work <- .checkpoint_work_directory()
      on.exit(unlink(work, recursive = TRUE), add = TRUE)
      private$snapshot <- tryCatch({
        file_client <- resourcer::FileResourceClient$new(resource,
          file.getter = .CheckpointFileGetter$new(work))
        path <- file_client$downloadFile()
        .checkpoint_verify("admit", path, substring(resource$format, 24L))
      }, error = function(e) {
        stop("Checkpoint resource acquisition or verification failed; reassign the registered resource.",
             call. = FALSE)
      })
      # Do not keep transport URL, credentials or the original descriptor.
      super$initialize(resourcer::newResource(name = "checkpoint", url = "",
        format = resource$format))
    },
    #' @description Return verified node-private snapshot state to trusted R code.
    getSnapshot = function() private$snapshot,
    #' @description Refuse generic dataset conversion.
    #' @param ... Ignored.
    asDataFrame = function(...) stop("Checkpoint resources require flowerCheckpointInitDS().", call. = FALSE),
    #' @description Refuse generic dataset conversion.
    #' @param ... Ignored.
    asTbl = function(...) stop("Checkpoint resources require flowerCheckpointInitDS().", call. = FALSE)))

.checkpoint_state <- function(owner_env, create = FALSE) {
  .flower_session_state(owner_env, create)$checkpoints
}

.checkpoint_reference <- function(snapshot, owner_env) {
  capability <- sub("^hdl_", "ckph_", .new_handle_capability())
  state <- .checkpoint_state(owner_env, TRUE)
  state[[capability]] <- list(snapshot = snapshot, origin = "resource")
  structure(list(capability = capability), class = "dsflower_checkpoint_ref")
}

.checkpoint_resolve <- function(symbol, owner_env) {
  if (!.checkpoint_symbol(symbol) || !exists(symbol, owner_env, inherits = FALSE)) {
    stop("Unknown or unassigned checkpoint handle.", call. = FALSE)
  }
  reference <- get(symbol, owner_env, inherits = FALSE)
  if (!inherits(reference, "dsflower_checkpoint_ref") || !is.list(reference) ||
      !identical(names(reference), "capability") ||
      !is.character(reference$capability) || length(reference$capability) != 1L ||
      !grepl("\\Ackph_[0-9a-f]{32}\\z", reference$capability, perl = TRUE)) {
    stop("Checkpoint initialisation requires an admitted typed handle.", call. = FALSE)
  }
  state <- .checkpoint_state(owner_env)
  entry <- if (is.environment(state)) state[[reference$capability]] else NULL
  if (!is.list(entry) || !identical(entry$origin, "resource")) {
    stop("Unknown, stale, or cross-session checkpoint handle.", call. = FALSE)
  }
  entry$snapshot
}

#' Admit an assigned checkpoint resource in this DataSHIELD session
#' @param resource_symbol Symbol of a platform-assigned CheckpointResourceClient.
#' @return An opaque session-bound checkpoint handle, assigned server-side.
#' @export
flowerCheckpointInitDS <- function(resource_symbol) {
  .dsflower_require_literal_arguments()
  .require_checkpoint_policy("resource")
  owner_env <- parent.frame()
  if (!.checkpoint_symbol(resource_symbol) ||
      !exists(resource_symbol, owner_env, inherits = FALSE)) {
    stop("Checkpoint resource is not assigned in this session.", call. = FALSE)
  }
  client <- get(resource_symbol, owner_env, inherits = FALSE)
  if (!is.environment(client) ||
      !all(c("CheckpointResourceClient", "ResourceClient", "R6") %in% class(client))) {
    stop("Checkpoint admission requires an assigned CheckpointResourceClient.", call. = FALSE)
  }
  snapshot <- client$getSnapshot()
  verified <- .checkpoint_verify("verify", snapshot$snapshot_directory,
                                  snapshot$provenance$manifest_sha256)
  .checkpoint_reference(verified, owner_env)
}

#' Inspect the public identity of an admitted checkpoint
#' @param handle_symbol Symbol of a checkpoint admission handle.
#' @return Public provenance, content digests, tensor geometry and server policy.
#' @export
flowerCheckpointStatusDS <- function(handle_symbol) {
  .dsflower_require_literal_arguments()
  policy <- .require_checkpoint_policy("resource")
  snapshot <- .checkpoint_resolve(handle_symbol, parent.frame())
  verified <- .checkpoint_verify("verify", snapshot$snapshot_directory,
                                  snapshot$provenance$manifest_sha256)
  result <- .checkpoint_public_summary(verified, "resource")
  result$policy <- policy
  result
}

#' Transfer declared public initialisation material in bounded chunks
#'
#' This is analyst ingress, distinct from resource admission. Upload tokens are
#' session-bound and expire after one hour. At most two pending uploads and
#' sixteen admitted upload handles are retained per session.
#' @param action One of begin, chunk, finish, or abort.
#' @param upload_id Opaque token returned by begin.
#' @param chunk_b64 Canonical B64:-prefixed URL-safe base64 (at most 512 KiB).
#' @param index Sequential one-based chunk number; identical retries are accepted.
#' @param manifest_sha256 Declared canonical manifest digest, for begin.
#' @param bundle_sha256 Declared archive digest, for begin.
#' @param total_bytes Declared archive byte count, for begin (at most 64 MiB).
#' @return Upload acknowledgement, and public provenance after finish.
#' @export
flowerCheckpointUploadDS <- function(action, upload_id = NULL, chunk_b64 = NULL,
    index = NULL, manifest_sha256 = NULL, bundle_sha256 = NULL, total_bytes = NULL) {
  .dsflower_require_literal_arguments()
  if (!is.character(action) || length(action) != 1L || is.na(action) ||
      !action %in% c("begin", "chunk", "finish", "abort")) {
    stop("Invalid public checkpoint upload action.", call. = FALSE)
  }
  if (!identical(action, "abort")) .require_checkpoint_policy("analyst-declared")
  state <- .checkpoint_state(parent.frame(), TRUE)
  for (token in ls(state, all.names = TRUE)) {
    entry <- state[[token]]
    if (identical(entry$origin, "analyst-declared") &&
        difftime(Sys.time(), entry$created, units = "secs") > 3600) {
      if (!is.null(entry$work)) unlink(entry$work, recursive = TRUE)
      state[[token]] <- NULL
    }
  }
  if (identical(action, "begin")) {
    entries <- as.list(state)
    pending <- sum(vapply(entries, function(e) !is.null(e$work), logical(1)))
    uploads <- sum(vapply(entries, function(e) identical(e$origin, "analyst-declared"), logical(1)))
    if (pending >= 2L || uploads >= 16L) {
      stop("The session public checkpoint admission limit is reached.", call. = FALSE)
    }
    if (!.checkpoint_sha256(manifest_sha256) || !.checkpoint_sha256(bundle_sha256) ||
        !is.numeric(total_bytes) || length(total_bytes) != 1L ||
        !is.finite(total_bytes) || total_bytes < 1 ||
        total_bytes != floor(total_bytes) || total_bytes > .CHECKPOINT_MAX_BYTES ||
        !is.null(upload_id) || !is.null(chunk_b64) || !is.null(index)) {
      stop("Invalid declared public checkpoint upload.", call. = FALSE)
    }
    token <- sub("^hdl_", "cku_", .new_handle_capability())
    state[[token]] <- list(origin = "analyst-declared", created = Sys.time(),
      work = .checkpoint_work_directory(), received = 0, next_index = 1L,
      manifest_sha256 = manifest_sha256, bundle_sha256 = bundle_sha256,
      total_bytes = total_bytes, chunks = character())
    return(list(upload_id = token, next_index = 1L))
  }
  if (!is.character(upload_id) || length(upload_id) != 1L || is.na(upload_id) ||
      !grepl("\\Acku_[0-9a-f]{32}\\z", upload_id, perl = TRUE)) {
    stop("Invalid public checkpoint upload token.", call. = FALSE)
  }
  entry <- state[[upload_id]]
  if (!is.list(entry) || !identical(entry$origin, "analyst-declared")) {
    stop("Unknown or cross-session public checkpoint upload.", call. = FALSE)
  }
  if (identical(action, "abort")) {
    if (!is.null(entry$work)) unlink(entry$work, recursive = TRUE)
    state[[upload_id]] <- NULL
    return(list(aborted = TRUE))
  }
  if (!is.null(manifest_sha256) || !is.null(bundle_sha256) || !is.null(total_bytes)) {
    stop("Declared upload identity cannot be replaced.", call. = FALSE)
  }
  if (identical(action, "chunk")) {
    if (is.null(entry$work) || !is.character(chunk_b64) || length(chunk_b64) != 1L ||
        is.na(chunk_b64) || nchar(chunk_b64, type = "bytes") > 4 + 4 * ceiling(.CHECKPOINT_CHUNK_BYTES / 3) ||
        !is.numeric(index) || length(index) != 1L || !is.finite(index) ||
        index != floor(index) || index < 1 || index > entry$next_index) {
      stop("Invalid public checkpoint upload chunk.", call. = FALSE)
    }
    bytes <- tryCatch(.app_b64_dec(chunk_b64, .CHECKPOINT_CHUNK_BYTES),
                      error = function(e) NULL)
    canonical <- if (length(bytes)) {
      encoded <- gsub("[\r\n]", "", jsonlite::base64_enc(bytes))
      paste0("B64:", sub("=+$", "", chartr("+/", "-_", encoded)))
    } else NULL
    if (!length(bytes) || length(bytes) > .CHECKPOINT_CHUNK_BYTES ||
        !identical(canonical, chunk_b64)) {
      stop("Invalid public checkpoint upload encoding.", call. = FALSE)
    }
    hash <- digest::digest(bytes, algo = "sha256", serialize = FALSE)
    if (index < entry$next_index) {
      if (!identical(entry$chunks[[index]], hash)) {
        stop("Public checkpoint chunk retry changed its bytes.", call. = FALSE)
      }
    } else {
      if (entry$received + length(bytes) > entry$total_bytes) {
        stop("Public checkpoint upload exceeds its declared size.", call. = FALSE)
      }
      .checkpoint_private_directory(entry$work)
      path <- file.path(entry$work, "bundle.zip")
      if (.privacy_path_is_link(path)) stop("Unsafe checkpoint upload state.", call. = FALSE)
      previous <- Sys.umask("0077")
      connection <- file(path, open = "ab")
      tryCatch(writeBin(bytes, connection), finally = {
        close(connection)
        Sys.umask(previous)
      })
      Sys.chmod(path, "0600")
      if (.Platform$OS.type == "windows") .windows_set_private_acl(path, is_directory = FALSE)
      entry$received <- entry$received + length(bytes)
      entry$next_index <- entry$next_index + 1L
      entry$chunks <- c(entry$chunks, hash)
      state[[upload_id]] <- entry
    }
    return(list(upload_id = upload_id, next_index = entry$next_index))
  }
  if (!is.null(chunk_b64) || !is.null(index)) stop("Invalid upload finish arguments.", call. = FALSE)
  if (!is.null(entry$snapshot)) {
    return(list(upload_id = upload_id,
      public_initialisation = .checkpoint_public_summary(entry$snapshot, "analyst-declared")))
  }
  if (entry$received != entry$total_bytes) stop("Public checkpoint upload is incomplete.", call. = FALSE)
  success <- FALSE
  upload_work <- entry$work
  on.exit({
    unlink(upload_work, recursive = TRUE)
    if (!success) state[[upload_id]] <- NULL
  }, add = TRUE)
  snapshot <- .checkpoint_verify("admit", file.path(entry$work, "bundle.zip"), entry$bundle_sha256)
  if (!identical(snapshot$provenance$manifest_sha256, entry$manifest_sha256)) {
    stop("Declared checkpoint manifest digest does not match the bundle.", call. = FALSE)
  }
  entry$snapshot <- snapshot
  entry$work <- NULL
  state[[upload_id]] <- entry
  success <- TRUE
  list(upload_id = upload_id,
       public_initialisation = .checkpoint_public_summary(snapshot, "analyst-declared"))
}
