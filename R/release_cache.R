# Administrator-owned durable Hook release state. Operational settings live in
# an owner-only receipt, separate from the semantic training manifest.

.release_cache_control_key <- function(key) {
  key <- gsub("([a-z0-9])([A-Z])", "\\1_\\2", key, perl = TRUE)
  key <- gsub("[-.]", "_", tolower(key))
  grepl("(^|_)(cache|deadline)($|_)", key, perl = TRUE)
}

.release_cache_settings <- function(settings = NULL) {
  if (is.null(settings)) {
    settings <- list(
      directory = .dsf_option("release_cache_dir", file.path(
        dirname(.node_secret_path()), "release-cache")),
      capacity = .dsf_option("release_cache_bytes", 1024^3))
  }
  directory <- settings$directory
  capacity <- settings$capacity
  if (!is.character(directory) || length(directory) != 1L ||
      is.na(directory) || !.path_is_absolute(directory) ||
      .privacy_path_is_link(directory)) {
    stop("dsflower.release_cache_dir must be an absolute non-link path.",
         call. = FALSE)
  }
  probe <- directory
  repeat {
    if (.privacy_path_is_link(probe)) {
      stop("The release cache path must not contain links.", call. = FALSE)
    }
    if (identical(dirname(probe), probe)) break
    probe <- dirname(probe)
  }
  if (!is.numeric(capacity) || length(capacity) != 1L ||
      !is.finite(capacity) || capacity < 1 || capacity != floor(capacity) ||
      capacity > 2^53 - 1) {
    stop("dsflower.release_cache_bytes must be one positive exact integer.",
         call. = FALSE)
  }
  directory <- .canonical_state_path(directory)
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_RELEASE_CACHE", ""), "1")
  if (.privacy_path_is_ephemeral(directory) && !allow_test_tmp) {
    stop("The dsFlower release cache must use persistent storage.",
         call. = FALSE)
  }
  forbidden <- c(
    file.path(.stagingBaseCandidates(create = FALSE), "dsflower"),
    .venv_root(),
    .dsf_option("app_spool_root", Sys.getenv(
      "DSFLOWER_APP_SPOOL_ROOT", unset = "/var/lib/dsflower/appstore")))
  forbidden <- vapply(forbidden, .canonical_state_path, character(1))
  overlaps <- vapply(forbidden, function(root) {
    identical(directory, root) || startsWith(directory, paste0(root, "/")) ||
      startsWith(root, paste0(directory, "/"))
  }, logical(1))
  if (any(overlaps)) {
    stop("The release cache must be outside staging and Hook mounts.",
         call. = FALSE)
  }
  list(directory = directory, capacity = as.numeric(capacity))
}

.release_cache_command <- function(action, run_token, settings, rounds = NULL) {
  runtime <- .resolve_framework_runtime("pytorch")
  script <- system.file("flower_app", "dsflower_runner", "release_cache.py",
                        package = "dsFlower")
  if (!nzchar(script) || .privacy_path_is_link(script) ||
      !.path_is_regular_file(script)) {
    stop("The trusted release cache runtime is unavailable.", call. = FALSE)
  }
  args <- c("-I", script, action, "--run-token", .validate_run_token(run_token))
  if (!is.null(rounds)) args <- c(args, "--rounds", as.character(rounds))
  result <- processx::run(runtime$python, args, error_on_status = FALSE,
    env = c(LD_PRELOAD = "", LD_LIBRARY_PATH = "", DYLD_LIBRARY_PATH = "",
      PYTHONHOME = "", PYTHONPATH = "", PYTHONNOUSERSITE = "1",
      DSFLOWER_RELEASE_CACHE_DIR = settings$directory,
      DSFLOWER_RELEASE_CACHE_BYTES = format(
        settings$capacity, scientific = FALSE, trim = TRUE)),
    echo = FALSE)
  if (!identical(result$status, 0L)) {
    stop("The durable Hook release cache is unavailable.", call. = FALSE)
  }
  invisible(TRUE)
}

.release_cache_receipt <- function(staging_dir) {
  receipt <- file.path(staging_dir, ".release-cache.json")
  if (!file.exists(receipt) && !.privacy_path_is_link(receipt)) return(NULL)
  if (.privacy_path_is_link(receipt) || !.path_is_regular_file(receipt)) {
    stop("The Hook release cache receipt is unsafe.", call. = FALSE)
  }
  info <- file.info(receipt)
  parent_info <- file.info(staging_dir)
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NA_integer_
  if (.Platform$OS.type != "unix" ||
      .privacy_path_is_link(staging_dir) ||
      !identical(as.integer(parent_info$uid[[1L]]), euid) ||
      bitwAnd(as.integer(parent_info$mode[[1L]]), strtoi("777", 8L)) !=
        strtoi("700", 8L) ||
      !identical(as.integer(info$uid[[1L]]), euid) ||
      bitwAnd(as.integer(info$mode[[1L]]), strtoi("777", 8L)) !=
        strtoi("600", 8L) || info$size[[1L]] > 16384) {
    stop("The Hook release cache receipt requires owner-only permissions.",
         call. = FALSE)
  }
  settings <- jsonlite::fromJSON(receipt, simplifyVector = TRUE)
  if (!is.list(settings) ||
      !setequal(names(settings), c("directory", "capacity"))) {
    stop("The Hook release cache receipt is invalid.", call. = FALSE)
  }
  .release_cache_settings(settings)
}

.release_cache_admit <- function(run_token, run_config) {
  if (!identical(run_config[["dp-track"]], "egress") ||
      !isTRUE(as.logical(run_config[["privacy-hook_enabled"]]))) {
    return(invisible(NULL))
  }
  tryCatch({
    settings <- .release_cache_settings()
    staging_dir <- .ensureStagingDir(run_token)
    receipt <- file.path(staging_dir, ".release-cache.json")
    # Register the exact cleanup target before reserve can persist any pins.
    .write_manifest_atomic(settings, receipt)
    .release_cache_command("reserve", run_token, settings,
                           run_config[["num-server-rounds"]])
    invisible(settings)
  }, error = function(e) stop(
    "The durable Hook release cache is unavailable.", call. = FALSE))
}

.release_cache_environment <- function(staging_dir) {
  settings <- .release_cache_receipt(staging_dir)
  # Public reservation can choose a different eligible staging root before the
  # later data-size check. Find its receipt using only the server run token.
  if (is.null(settings) && grepl("^run_[0-9a-f]{32}$", basename(staging_dir))) {
    for (candidate in .expectedStagingDirs(basename(staging_dir))) {
      settings <- .release_cache_receipt(candidate)
      if (!is.null(settings)) break
    }
  }
  if (is.null(settings)) return(character())
  c(DSFLOWER_RELEASE_CACHE_DIR = settings$directory,
    DSFLOWER_RELEASE_CACHE_BYTES = format(
      settings$capacity, scientific = FALSE, trim = TRUE))
}

.release_cache_close_staging <- function(run_token, staging_dirs) {
  settings <- lapply(staging_dirs, .release_cache_receipt)
  settings <- Filter(Negate(is.null), settings)
  if (!length(settings)) return(invisible(TRUE))
  # Stop every possible worker before closing a reservation. If stopping or
  # closing fails, leave the receipt and staging in place for an exact retry.
  for (staging_dir in staging_dirs) .supernode_stop(staging_dir)
  for (setting in settings) {
    .release_cache_command("close", run_token, setting)
  }
  invisible(TRUE)
}
