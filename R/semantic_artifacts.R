# Module: durable memoization for semantic DP artifacts
#
# A future synopsis/native mechanism can claim a canonical semantic identity
# before reserving privacy. Concurrent callers receive the same run token, so
# the existing accountant reservation remains idempotent. Once committed, the
# exact artifact is replayed without another release. Missing or corrupted
# service-owned artifacts create a new audited generation instead of blocking;
# unsafe paths, symlinks, or foreign ownership remain fail-closed.

.semantic_hex64 <- function(value, field) {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !grepl("^[0-9a-f]{64}$", value)) {
    stop(field, " must be a lowercase SHA-256 hex digest.", call. = FALSE)
  }
  value
}

.semantic_mechanism <- function(value) {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !grepl("^[a-z][a-z0-9_.-]{0,63}$", value)) {
    stop("Semantic mechanism name is invalid.", call. = FALSE)
  }
  value
}

.semantic_run_token <- function(value) {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !grepl("^run_[0-9a-f]{32}$", value)) {
    stop("Semantic release run token is invalid.", call. = FALSE)
  }
  value
}

.semantic_generation <- function(value) {
  numeric_value <- suppressWarnings(as.numeric(value))
  if (length(numeric_value) != 1L || !is.finite(numeric_value) ||
      numeric_value < 1 || numeric_value != floor(numeric_value) ||
      numeric_value > .Machine$integer.max) {
    stop("Semantic artifact generation is invalid.", call. = FALSE)
  }
  as.integer(numeric_value)
}

.semantic_ensure_private_dir <- function(path) {
  if (.path_is_symlink(path)) {
    stop("Semantic artifact directories must not be symbolic links.",
         call. = FALSE)
  }
  if (!dir.exists(path)) {
    if (!isTRUE(dir.create(path, recursive = FALSE, mode = "0700",
                           showWarnings = FALSE)) && !dir.exists(path)) {
      stop("Could not create the semantic artifact directory.", call. = FALSE)
    }
  }
  if (.path_is_symlink(path)) {
    stop("Semantic artifact directories must not be symbolic links.",
         call. = FALSE)
  }
  info <- file.info(path)
  if (nrow(info) != 1L || !isTRUE(info$isdir[[1L]])) {
    stop("Semantic artifact path must be a real directory.", call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    euid <- .privacy_effective_uid()
    owner <- suppressWarnings(as.integer(info$uid[[1L]]))
    if (is.na(owner) || !identical(owner, euid)) {
      stop("Semantic artifact directories must be owned by the node EUID.",
           call. = FALSE)
    }
    expected <- as.integer(strtoi("700", base = 8L))
    mode <- suppressWarnings(as.integer(info$mode[[1L]]))
    if (!identical(mode, expected)) {
      chmod_ok <- suppressWarnings(Sys.chmod(path, "0700"))
      if (length(chmod_ok) != 1L || !isTRUE(chmod_ok) ||
          !identical(as.integer(file.info(path)$mode[[1L]]), expected)) {
        stop("Semantic artifact directories must have Unix mode 0700.",
             call. = FALSE)
      }
    }
  }
  normalizePath(path, winslash = "/", mustWork = TRUE)
}

.semantic_artifact_root <- function() {
  ledger <- .privacy_ledger_path()
  parent <- .privacy_validate_ledger_parent(
    ledger, if (.Platform$OS.type == "unix") .privacy_effective_uid() else NA_integer_)
  root <- file.path(parent, "semantic-artifacts")
  .semantic_ensure_private_dir(root)
}

.semantic_domain_hash <- function(domain) {
  digest::digest(enc2utf8(as.character(domain)), algo = "sha256",
                 serialize = FALSE)
}

.semantic_artifact_relpath <- function(domain, semantic_key, mechanism,
                                       generation) {
  semantic_key <- .semantic_hex64(semantic_key, "semantic_key")
  mechanism <- .semantic_mechanism(mechanism)
  generation <- .semantic_generation(generation)
  file.path(
    .semantic_domain_hash(domain), mechanism, substr(semantic_key, 1L, 2L),
    semantic_key, paste0(generation, ".artifact"))
}

.semantic_artifact_path <- function(domain, semantic_key, mechanism,
                                    generation, create = FALSE) {
  root <- .semantic_artifact_root()
  relative <- .semantic_artifact_relpath(
    domain, semantic_key, mechanism, generation)
  path <- file.path(root, relative)
  parts <- unique(c(
    file.path(root, .semantic_domain_hash(domain)),
    file.path(root, .semantic_domain_hash(domain), mechanism),
    file.path(root, .semantic_domain_hash(domain), mechanism,
              substr(semantic_key, 1L, 2L)),
    dirname(path)))
  if (isTRUE(create)) {
    for (directory in parts) .semantic_ensure_private_dir(directory)
  } else {
    # A final-component check is not enough: an attacker able to replace a
    # service-owned intermediate directory with a symlink could redirect a
    # replay outside the private store. Missing directories remain recoverable
    # artifact loss, but every existing component must retain the same private
    # directory contract as the root.
    for (directory in parts) {
      if (.path_is_symlink(directory)) {
        stop("Semantic artifact directories must not be symbolic links.",
             call. = FALSE)
      }
      if (!dir.exists(directory)) break
      .semantic_ensure_private_dir(directory)
    }
  }
  list(path = path, relpath = relative)
}

.semantic_artifact_max_bytes <- function() {
  value <- suppressWarnings(as.numeric(
    .dsf_option("semantic_artifact_max_bytes", 1024^3)))
  if (length(value) != 1L || !is.finite(value) || value < 1 ||
      value != floor(value) || value > 2^31) {
    stop("dsflower.semantic_artifact_max_bytes must be an integer in [1, 2^31].",
         call. = FALSE)
  }
  value
}

.semantic_validate_artifact_file <- function(path, expected_sha = NULL,
                                             expected_bytes = NULL,
                                             allow_missing = FALSE) {
  if (!file.exists(path) && !.path_is_symlink(path)) {
    if (isTRUE(allow_missing)) return(list(valid = FALSE, reason = "missing"))
    stop("Semantic artifact is missing.", call. = FALSE)
  }
  if (.path_is_symlink(path) || !.path_is_regular_file(path)) {
    stop("Semantic artifact must be a regular file, not a symbolic link.",
         call. = FALSE)
  }
  info <- file.info(path)
  if (.Platform$OS.type == "unix") {
    owner <- suppressWarnings(as.integer(info$uid[[1L]]))
    if (is.na(owner) || !identical(owner, .privacy_effective_uid())) {
      stop("Semantic artifact must be owned by the node EUID.", call. = FALSE)
    }
    expected_mode <- as.integer(strtoi("600", base = 8L))
    if (!identical(as.integer(info$mode[[1L]]), expected_mode)) {
      chmod_ok <- suppressWarnings(Sys.chmod(path, "0600"))
      if (length(chmod_ok) != 1L || !isTRUE(chmod_ok) ||
          !identical(as.integer(file.info(path)$mode[[1L]]), expected_mode)) {
        stop("Semantic artifact must have Unix mode 0600.", call. = FALSE)
      }
      info <- file.info(path)
    }
  }
  bytes <- suppressWarnings(as.numeric(info$size[[1L]]))
  if (!is.finite(bytes) || bytes < 0 || bytes > .semantic_artifact_max_bytes()) {
    stop("Semantic artifact exceeds the configured size limit.", call. = FALSE)
  }
  sha <- digest::digest(file = path, algo = "sha256", serialize = FALSE)
  valid <- (is.null(expected_sha) || identical(sha, expected_sha)) &&
    (is.null(expected_bytes) || identical(bytes, as.numeric(expected_bytes)))
  list(valid = valid, reason = if (valid) "ok" else "corrupt",
       sha256 = sha, bytes = bytes, path = path)
}

.semantic_active_row <- function(con, domain, semantic_key, mechanism) {
  DBI::dbGetQuery(
    con,
    paste(
      "SELECT generation,policy_hash,run_token,status,artifact_relpath,",
      "artifact_sha256,artifact_bytes FROM privacy_semantic_releases",
      "WHERE domain = ? AND semantic_key = ? AND mechanism = ?",
      "AND status IN ('reserved','committed')",
      "ORDER BY generation DESC LIMIT 1"),
    params = list(domain, semantic_key, mechanism))
}

.semantic_assert_accounted_release <- function(con, domain, run_token) {
  accounting <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT r.max_releases,r.claimed_releases,COUNT(c.message_id) AS claims",
      "FROM privacy_reservations r",
      "LEFT JOIN privacy_release_claims c ON c.run_token=r.run_token",
      "WHERE r.domain=? AND r.run_token=?",
      "GROUP BY r.max_releases,r.claimed_releases"),
    params = list(domain, run_token))
  if (nrow(accounting) != 1L) {
    stop("Semantic artifact has no privacy reservation.", call. = FALSE)
  }
  horizon <- suppressWarnings(as.numeric(accounting$max_releases[[1L]]))
  claimed <- suppressWarnings(as.numeric(accounting$claimed_releases[[1L]]))
  rows <- suppressWarnings(as.numeric(accounting$claims[[1L]]))
  if (length(horizon) != 1L || !is.finite(horizon) || horizon < 1 ||
      horizon != floor(horizon) || !identical(claimed, horizon) ||
      !identical(rows, horizon)) {
    stop("Semantic artifact privacy releases are incomplete.", call. = FALSE)
  }
  invisible(TRUE)
}

.semantic_claim_row <- function(con, policy, semantic_key, mechanism,
                                generation, run_token) {
  row <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT status,policy_hash,artifact_sha256,artifact_bytes",
      "FROM privacy_semantic_releases",
      "WHERE domain=? AND semantic_key=? AND mechanism=? AND generation=?",
      "AND run_token=?"),
    params = list(
      policy$domain, semantic_key, mechanism, generation, run_token))
  if (nrow(row) != 1L || !row$status[[1L]] %in% c("reserved", "committed")) {
    stop("Semantic artifact has no active ledger claim.", call. = FALSE)
  }
  if (!identical(as.character(row$policy_hash[[1L]]), policy$policy_hash)) {
    stop("Semantic artifact policy hash disagrees with the node policy.",
         call. = FALSE)
  }
  row
}

#' Claim or replay one semantic DP artifact
#' @keywords internal
.claim_semantic_artifact <- function(semantic_key, mechanism, run_token) {
  semantic_key <- .semantic_hex64(semantic_key, "semantic_key")
  mechanism <- .semantic_mechanism(mechanism)
  run_token <- .semantic_run_token(run_token)
  policy <- .privacy_policy()
  con <- .privacy_db_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (attempt in seq_len(4L)) {
    active <- .semantic_active_row(
      con, policy$domain, semantic_key, mechanism)
    if (nrow(active) == 1L &&
        !identical(as.character(active$policy_hash[[1L]]), policy$policy_hash)) {
      stop("Semantic artifact policy hash disagrees with the node policy.",
           call. = FALSE)
    }
    if (nrow(active) == 1L && identical(active$status[[1L]], "committed")) {
      generation <- .semantic_generation(active$generation[[1L]])
      expected <- .semantic_artifact_path(
        policy$domain, semantic_key, mechanism, generation)
      if (!identical(as.character(active$artifact_relpath[[1L]]),
                     expected$relpath)) {
        stop("Semantic artifact ledger path is invalid.", call. = FALSE)
      }
      probe <- .semantic_validate_artifact_file(
        expected$path,
        expected_sha = as.character(active$artifact_sha256[[1L]]),
        expected_bytes = active$artifact_bytes[[1L]], allow_missing = TRUE)
      if (isTRUE(probe$valid)) {
        return(list(
          status = "replay", semantic_key = semantic_key,
          mechanism = mechanism, generation = generation,
          run_token = as.character(active$run_token[[1L]]),
          artifact_path = probe$path, artifact_sha256 = probe$sha256,
          artifact_bytes = probe$bytes))
      }
    }

    .privacy_db_begin(con)
    committed <- FALSE
    tryCatch({
      current <- .semantic_active_row(
        con, policy$domain, semantic_key, mechanism)
      if (nrow(current) == 1L &&
          !identical(as.character(current$policy_hash[[1L]]), policy$policy_hash)) {
        stop("Semantic artifact policy hash disagrees with the node policy.",
             call. = FALSE)
      }
      if (nrow(current) == 1L && identical(current$status[[1L]], "reserved")) {
        DBI::dbExecute(con, "COMMIT")
        committed <- TRUE
        return(list(
          status = "resume", semantic_key = semantic_key,
          mechanism = mechanism,
          generation = .semantic_generation(current$generation[[1L]]),
          run_token = as.character(current$run_token[[1L]]),
          artifact_path = NULL))
      }
      if (nrow(current) == 1L) {
        changed <- DBI::dbExecute(
          con,
          paste(
            "UPDATE privacy_semantic_releases SET status='lost',",
            "lost_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
            "WHERE domain=? AND semantic_key=? AND mechanism=?",
            "AND generation=? AND status='committed'"),
          params = list(
            policy$domain, semantic_key, mechanism,
            .semantic_generation(current$generation[[1L]])))
        if (!identical(as.integer(changed), 1L)) {
          stop("Semantic artifact state changed during recovery.", call. = FALSE)
        }
      }
      latest <- DBI::dbGetQuery(
        con,
        paste(
          "SELECT COALESCE(MAX(generation),0) AS generation",
          "FROM privacy_semantic_releases",
          "WHERE domain=? AND semantic_key=? AND mechanism=?"),
        params = list(policy$domain, semantic_key, mechanism))
      generation <- as.numeric(latest$generation[[1L]]) + 1
      generation <- .semantic_generation(generation)
      DBI::dbExecute(
        con,
        paste(
          "INSERT INTO privacy_semantic_releases",
          "(domain,semantic_key,mechanism,generation,policy_hash,run_token,",
          "status,created_at) VALUES (?,?,?,?,?,?,'reserved',",
          "strftime('%Y-%m-%dT%H:%M:%fZ','now'))"),
        params = list(
          policy$domain, semantic_key, mechanism, generation,
          policy$policy_hash, run_token))
      DBI::dbExecute(con, "COMMIT")
      committed <- TRUE
      return(list(
        status = "new", semantic_key = semantic_key, mechanism = mechanism,
        generation = generation, run_token = run_token, artifact_path = NULL))
    }, error = function(error) {
      if (!committed) .privacy_db_rollback(con)
      stop(error)
    })
  }
  stop("Could not establish a stable semantic artifact claim.", call. = FALSE)
}

#' Atomically commit a semantic DP artifact
#' @keywords internal
.commit_semantic_artifact <- function(semantic_key, mechanism, generation,
                                      run_token, source_path) {
  semantic_key <- .semantic_hex64(semantic_key, "semantic_key")
  mechanism <- .semantic_mechanism(mechanism)
  generation <- .semantic_generation(generation)
  run_token <- .semantic_run_token(run_token)
  if (!is.character(source_path) || length(source_path) != 1L ||
      is.na(source_path) || !nzchar(source_path)) {
    stop("Semantic artifact source path is invalid.", call. = FALSE)
  }
  policy <- .privacy_policy()
  con <- .privacy_db_connect()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  # Claiming a semantic key deliberately precedes accounting so a replay costs
  # nothing. Publishing is the opposite boundary: every promised release must
  # already have a durable reservation and release claim.
  .semantic_assert_accounted_release(con, policy$domain, run_token)
  .semantic_claim_row(
    con, policy, semantic_key, mechanism, generation, run_token)
  source_probe <- .semantic_validate_artifact_file(source_path)
  destination <- .semantic_artifact_path(
    policy$domain, semantic_key, mechanism, generation, create = TRUE)
  lock_dir <- .semantic_ensure_private_dir(
    file.path(.semantic_artifact_root(), ".locks"))
  lock_path <- file.path(
    lock_dir, paste0(.semantic_domain_hash(policy$domain), "-", mechanism,
                     "-", semantic_key, "-", generation, ".lock"))
  lock <- filelock::lock(lock_path, timeout = 10000)
  if (is.null(lock)) stop("Timed out locking the semantic artifact.", call. = FALSE)
  on.exit(filelock::unlock(lock), add = TRUE)
  if (.Platform$OS.type == "unix") suppressWarnings(Sys.chmod(lock_path, "0600"))

  temp <- tempfile(pattern = ".semantic-artifact-", tmpdir = dirname(destination$path))
  on.exit(unlink(temp), add = TRUE)
  if (!isTRUE(file.copy(source_probe$path, temp, overwrite = FALSE,
                        copy.mode = FALSE, copy.date = FALSE))) {
    stop("Could not stage the semantic artifact atomically.", call. = FALSE)
  }
  if (.Platform$OS.type == "unix" && !isTRUE(Sys.chmod(temp, "0600"))) {
    stop("Could not make the semantic artifact private.", call. = FALSE)
  }
  staged <- .semantic_validate_artifact_file(temp)

  if (file.exists(destination$path) || .path_is_symlink(destination$path)) {
    existing <- .semantic_validate_artifact_file(destination$path)
    if (!identical(existing$sha256, staged$sha256) ||
        !identical(existing$bytes, staged$bytes)) {
      stop("A semantic artifact generation already contains different bytes.",
           call. = FALSE)
    }
  } else if (!isTRUE(file.rename(temp, destination$path))) {
    stop("Could not publish the semantic artifact atomically.", call. = FALSE)
  }
  published <- .semantic_validate_artifact_file(destination$path)

  .privacy_db_begin(con)
  committed <- FALSE
  on.exit(if (!committed) .privacy_db_rollback(con), add = TRUE)
  .semantic_assert_accounted_release(con, policy$domain, run_token)
  row <- .semantic_claim_row(
    con, policy, semantic_key, mechanism, generation, run_token)
  if (identical(row$status[[1L]], "committed")) {
    if (!identical(as.character(row$artifact_sha256[[1L]]), published$sha256) ||
        !identical(as.numeric(row$artifact_bytes[[1L]]), published$bytes)) {
      stop("Committed semantic artifact metadata changed.", call. = FALSE)
    }
  } else {
    changed <- DBI::dbExecute(
      con,
      paste(
        "UPDATE privacy_semantic_releases SET status='committed',",
        "artifact_relpath=?,artifact_sha256=?,artifact_bytes=?,",
        "committed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        "WHERE domain=? AND semantic_key=? AND mechanism=? AND generation=?",
        "AND run_token=? AND status='reserved'"),
      params = list(
        destination$relpath, published$sha256, published$bytes,
        policy$domain, semantic_key, mechanism, generation, run_token))
    if (!identical(as.integer(changed), 1L)) {
      stop("Semantic artifact commit lost its atomic race.", call. = FALSE)
    }
  }
  DBI::dbExecute(con, "COMMIT")
  committed <- TRUE
  list(
    status = "committed", semantic_key = semantic_key,
    mechanism = mechanism, generation = generation, run_token = run_token,
    artifact_path = published$path, artifact_sha256 = published$sha256,
    artifact_bytes = published$bytes)
}
