# Module: Persistent privacy accounting
#
# The default lifetime-geometric mode is the historical bounded accountant: each
# new run receives one term of a convergent schedule.  Numerically unusable tail
# allocations become public no-release records, preserving the finite lifetime
# bound without silently weakening the mechanism.
#
# The explicit per-release-audit mode instead assigns one administrator-pinned
# epsilon/delta pair to every new training reservation and never exhausts a
# budget.  Its ledger reports basic composition for each finite prefix, but does
# not claim a finite lifetime bound.  This distinction is unavoidable: standard
# DP composition cannot provide both a finite lifetime epsilon/delta and
# unlimited semantically new releases with a fixed positive privacy allocation.

.privacy_allocation_slack <- 1 - 1e-12

.privacy_policy <- function() {
  accounting_mode <- .dsf_option(
    "dp_accounting_mode", "lifetime-geometric")
  if (length(accounting_mode) != 1L || is.na(accounting_mode)) {
    stop("dsflower.dp_accounting_mode must be one scalar string.",
         call. = FALSE)
  }
  accounting_mode <- tolower(trimws(as.character(accounting_mode)))
  if (!accounting_mode %in% c("lifetime-geometric", "per-release-audit")) {
    stop("dsflower.dp_accounting_mode must be exactly 'lifetime-geometric' ",
         "or 'per-release-audit'.", call. = FALSE)
  }

  total_epsilon <- suppressWarnings(as.numeric(
    .dsf_option("dp_total_epsilon", 3.0)))
  total_delta <- suppressWarnings(as.numeric(
    .dsf_option("dp_total_delta", 1e-5)))
  decay <- suppressWarnings(as.numeric(
    .dsf_option("dp_budget_decay", 0.5)))
  min_release_epsilon <- suppressWarnings(as.numeric(
    .dsf_option("dp_min_release_epsilon", 1e-6)))
  min_release_delta <- suppressWarnings(as.numeric(
    .dsf_option("dp_min_release_delta", 1e-12)))
  domain <- as.character(.dsf_option("dp_privacy_domain", "node"))
  unit_policy <- .dpUnitPolicy()
  adjacency <- "replace_one"

  if (length(total_epsilon) != 1L || !is.finite(total_epsilon) ||
      total_epsilon <= 0 || total_epsilon > 10) {
    stop("dsflower.dp_total_epsilon must be finite and in (0, 10].",
         call. = FALSE)
  }
  if (length(total_delta) != 1L || !is.finite(total_delta) ||
      total_delta <= 0 || total_delta > 1e-3) {
    stop("dsflower.dp_total_delta must be finite and in (0, 1e-3].",
         call. = FALSE)
  }
  if (length(decay) != 1L || !is.finite(decay) ||
      decay < 0.5 || decay > 0.99) {
    stop("dsflower.dp_budget_decay must be finite and in [0.5, 0.99].",
         call. = FALSE)
  }
  if (length(min_release_epsilon) != 1L ||
      !is.finite(min_release_epsilon) || min_release_epsilon < 1e-6 ||
      min_release_epsilon > 10) {
    stop("dsflower.dp_min_release_epsilon must be finite and in [1e-6, 10].",
         call. = FALSE)
  }
  if (length(min_release_delta) != 1L ||
      !is.finite(min_release_delta) || min_release_delta < 1e-12 ||
      min_release_delta > 1e-3) {
    stop("dsflower.dp_min_release_delta must be finite and in [1e-12, 1e-3].",
         call. = FALSE)
  }
  if (length(domain) != 1L || is.na(domain) ||
      !grepl("^[A-Za-z0-9_.:-]{1,128}$", domain)) {
    stop("dsflower.dp_privacy_domain must match [A-Za-z0-9_.:-]{1,128}.",
         call. = FALSE)
  }

  per_training_epsilon <- NULL
  per_training_delta <- NULL
  if (identical(accounting_mode, "per-release-audit")) {
    per_training_epsilon <- suppressWarnings(as.numeric(
      .dsf_option("dp_per_training_epsilon", NULL)))
    per_training_delta <- suppressWarnings(as.numeric(
      .dsf_option("dp_per_training_delta", NULL)))
    if (length(per_training_epsilon) != 1L ||
        !is.finite(per_training_epsilon) ||
        per_training_epsilon < min_release_epsilon ||
        per_training_epsilon > 10) {
      stop("dsflower.dp_per_training_epsilon must be explicitly configured, ",
           "finite, and in [dsflower.dp_min_release_epsilon, 10].",
           call. = FALSE)
    }
    if (length(per_training_delta) != 1L ||
        !is.finite(per_training_delta) ||
        per_training_delta < min_release_delta ||
        per_training_delta > 1e-3) {
      stop("dsflower.dp_per_training_delta must be explicitly configured, ",
           "finite, and in [dsflower.dp_min_release_delta, 1e-3].",
           call. = FALSE)
    }
  }

  policy_text <- function(canonicalization) paste(
    "dsflower-privacy-v5", domain,
    format(total_epsilon, digits = 17, scientific = TRUE),
    format(total_delta, digits = 17, scientific = TRUE),
    format(decay, digits = 17, scientific = TRUE),
    format(min_release_epsilon, digits = 17, scientific = TRUE),
    format(min_release_delta, digits = 17, scientific = TRUE),
    unit_policy$dp_unit,
    unit_policy$patient_column %||% "<none>",
    canonicalization,
    adjacency,
    format(.privacy_allocation_slack, digits = 17, scientific = TRUE),
    sep = "|")
  lifetime_policy_hash <- digest::digest(
    policy_text(unit_policy$canonicalization),
    algo = "sha256", serialize = FALSE)
  policy_hash <- lifetime_policy_hash
  if (identical(accounting_mode, "per-release-audit")) {
    audit_text <- paste(
      "dsflower-privacy-per-release-audit-v1", domain,
      format(per_training_epsilon, digits = 17, scientific = TRUE),
      format(per_training_delta, digits = 17, scientific = TRUE),
      format(min_release_epsilon, digits = 17, scientific = TRUE),
      format(min_release_delta, digits = 17, scientific = TRUE),
      unit_policy$dp_unit,
      unit_policy$patient_column %||% "<none>",
      unit_policy$canonicalization,
      adjacency,
      sep = "|")
    policy_hash <- digest::digest(
      audit_text, algo = "sha256", serialize = FALSE)
  }

  list(
    accounting_mode = accounting_mode,
    domain = domain,
    total_epsilon = total_epsilon,
    total_delta = total_delta,
    decay = decay,
    min_release_epsilon = min_release_epsilon,
    min_release_delta = min_release_delta,
    per_training_epsilon = per_training_epsilon,
    per_training_delta = per_training_delta,
    dp_unit = unit_policy$dp_unit,
    patient_column = unit_policy$patient_column,
    unit_canonicalization = unit_policy$canonicalization,
    adjacency = adjacency,
    policy_hash = policy_hash,
    legacy_v1_policy_hash = digest::digest(
      policy_text("trim-utf8-v1"), algo = "sha256", serialize = FALSE)
  )
}

.path_is_absolute <- function(path) {
  length(path) == 1L && !is.na(path) && nzchar(path) &&
    (grepl("^/", path) || grepl("^[A-Za-z]:[/\\\\]", path))
}

.path_is_symlink <- function(path) {
  link <- Sys.readlink(path)
  length(link) == 1L && !is.na(link) && nzchar(link)
}

# utils::file_test("-f") means "exists and is not a directory" in R and
# therefore accepts FIFOs. Privacy state must use the POSIX regular-file test so
# a crafted pipe cannot block key reads or SQLite open indefinitely.
.path_is_regular_file <- function(path) {
  if (.Platform$OS.type != "unix") {
    return(utils::file_test("-f", path))
  }
  test_bin <- c("/usr/bin/test", "/bin/test")
  test_bin <- test_bin[file.exists(test_bin)][1L]
  if (length(test_bin) != 1L || is.na(test_bin) || !nzchar(test_bin)) {
    stop("No trusted POSIX regular-file test is available.", call. = FALSE)
  }
  status <- suppressWarnings(system2(
    test_bin, c("-f", shQuote(path)), stdout = FALSE, stderr = FALSE))
  identical(as.integer(status), 0L)
}

# normalizePath() deliberately leaves '..' components untouched when part of
# the path does not exist.  That would let an apparent /var path resolve into
# /tmp only after dir.create().  Collapse those components first, then resolve
# the nearest existing parent (including any parent symlinks), while preserving
# the final filename so the caller can still reject a final-component symlink.
.canonical_state_path <- function(path) {
  path <- as.character(path)
  if (!.path_is_absolute(path)) return(path)
  slash_path <- gsub("\\\\", "/", path)
  drive <- if (grepl("^[A-Za-z]:/", slash_path)) substr(slash_path, 1L, 2L) else ""
  rest <- if (nzchar(drive)) substring(slash_path, 4L) else substring(slash_path, 2L)
  parts <- strsplit(rest, "/", fixed = TRUE)[[1]]
  stack <- character(0)
  for (part in parts) {
    if (!nzchar(part) || identical(part, ".")) next
    if (identical(part, "..")) {
      if (length(stack)) stack <- stack[-length(stack)]
    } else {
      stack <- c(stack, part)
    }
  }
  prefix <- if (nzchar(drive)) paste0(drive, "/") else "/"
  lexical <- if (length(stack)) paste0(prefix, paste(stack, collapse = "/")) else prefix

  leaf <- basename(lexical)
  parent <- dirname(lexical)
  suffix <- character(0)
  probe <- parent
  while (!dir.exists(probe) && !identical(dirname(probe), probe)) {
    suffix <- c(basename(probe), suffix)
    probe <- dirname(probe)
  }
  if (dir.exists(probe)) {
    probe <- normalizePath(probe, winslash = "/", mustWork = TRUE)
    parent <- if (length(suffix)) do.call(file.path, as.list(c(probe, suffix))) else probe
  }
  file.path(parent, leaf)
}

.privacy_dir_writable <- function(path) {
  existed <- dir.exists(path)
  ok <- tryCatch({
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
    dir.exists(path)
  }, error = function(e) FALSE)
  if (!isTRUE(ok)) return(FALSE)
  if (!existed && .Platform$OS.type == "unix") {
    chmod_ok <- suppressWarnings(Sys.chmod(path, "0700"))
    if (length(chmod_ok) != 1L || !isTRUE(chmod_ok)) return(FALSE)
  }
  probe <- file.path(path, paste0(".dsflower-write-probe-", Sys.getpid()))
  tryCatch({
    con <- file(probe, open = "wx")
    close(con)
    unlink(probe)
    TRUE
  }, error = function(e) FALSE, warning = function(w) FALSE)
}

.privacy_path_is_ephemeral <- function(path) {
  p <- .canonical_state_path(path)
  roots <- unique(normalizePath(c("/tmp", "/var/tmp", "/dev/shm", tempdir()),
                                winslash = "/", mustWork = FALSE))
  any(vapply(roots, function(root)
    identical(p, root) || startsWith(p, paste0(root, "/")), logical(1)))
}

#' Resolve the persistent SQLite privacy ledger
#' @keywords internal
.privacy_ledger_path <- function() {
  from_env <- Sys.getenv("DSFLOWER_PRIVACY_LEDGER_PATH", "")
  configured <- .dsf_option("privacy_ledger_path", "")
  configured_supplied <- !(is.null(configured) || length(configured) == 0L ||
    (length(configured) == 1L && !is.na(configured) &&
       !nzchar(as.character(configured))))
  if (nzchar(from_env) && configured_supplied) {
    if (length(configured) != 1L || is.na(configured) ||
        !nzchar(as.character(configured)) ||
        !.path_is_absolute(as.character(configured)) ||
        !.path_is_absolute(from_env)) {
      stop("dsflower.privacy_ledger_path must be one absolute path.",
           call. = FALSE)
    }
    if (!identical(.canonical_state_path(from_env),
                   .canonical_state_path(as.character(configured)))) {
      stop("DSFLOWER_PRIVACY_LEDGER_PATH conflicts with the configured R option; ",
           "refusing to select a different privacy ledger.", call. = FALSE)
    }
  }
  # Preserve the historical R-option precedence. A simultaneous ENV is accepted
  # only when it resolves to the same ledger, so upgrades cannot silently reset
  # accounting by selecting a different empty database.
  explicit <- if (configured_supplied) configured else from_env
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER", ""), "1")

  if (!is.null(explicit) && length(explicit) > 0L) {
    if (length(explicit) != 1L || is.na(as.character(explicit)[1])) {
      stop("dsflower.privacy_ledger_path must be one absolute path.",
           call. = FALSE)
    }
    path <- as.character(explicit)[1]
    if (!nzchar(path)) {
      path <- NULL
    }
  } else {
    path <- NULL
  }

  if (!is.null(path)) {
    if (!.path_is_absolute(path)) {
      stop("dsflower.privacy_ledger_path must be an absolute path.",
           call. = FALSE)
    }
    if (.path_is_symlink(path)) {
      stop("Privacy ledger must not be a symbolic link.", call. = FALSE)
    }
    path <- .canonical_state_path(path)
    if (.privacy_path_is_ephemeral(path) && !allow_test_tmp) {
      stop("The privacy ledger must be persistent; /tmp, /var/tmp and /dev/shm ",
           "are not allowed.", call. = FALSE)
    }
    if (!.privacy_dir_writable(dirname(path))) {
      stop("Privacy ledger directory is not writable: ", dirname(path),
           call. = FALSE)
    }
    return(path)
  }

  rock_home <- Sys.getenv("ROCK_HOME", "")
  candidates <- c(
    "/var/lib/dsflower/privacy/ledger.sqlite",
    if (nzchar(rock_home)) file.path(rock_home, "dsflower", "privacy", "ledger.sqlite"),
    file.path(rappdirs::user_data_dir("dsFlower"), "privacy", "ledger.sqlite")
  )
  for (path in unique(candidates)) {
    if (.path_is_absolute(path) && !.privacy_path_is_ephemeral(path) &&
        .privacy_dir_writable(dirname(path))) {
      return(.canonical_state_path(path))
    }
  }
  stop("No persistent writable privacy-ledger path is available. Set ",
       "options(dsflower.privacy_ledger_path='/persistent/path/ledger.sqlite').",
       call. = FALSE)
}

.privacy_effective_uid <- function() {
  probe <- tempfile(pattern = ".dsflower-euid-")
  on.exit(unlink(probe), add = TRUE)
  if (!isTRUE(file.create(probe))) {
    stop("Could not establish the node process owner.", call. = FALSE)
  }
  uid <- suppressWarnings(as.integer(file.info(probe)$uid[[1]]))
  if (is.na(uid)) {
    stop("Could not establish the node process owner.", call. = FALSE)
  }
  uid
}

.privacy_validate_ledger_parent <- function(path, euid) {
  parent <- dirname(path)
  if (.path_is_symlink(parent)) {
    stop("Privacy ledger parent must be a real directory.", call. = FALSE)
  }
  info <- file.info(parent)
  if (nrow(info) != 1L || is.na(info$isdir[[1]]) || !isTRUE(info$isdir[[1]])) {
    stop("Privacy ledger parent must be a real directory.", call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    owner <- suppressWarnings(as.integer(info$uid[[1]]))
    if (is.na(owner) || !identical(owner, as.integer(euid))) {
      stop("Privacy ledger parent must be owned by the node EUID.",
           call. = FALSE)
    }
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    unsafe_write <- as.integer(strtoi("22", base = 8))
    if (is.na(mode) || bitwAnd(mode, unsafe_write) != 0L) {
      stop("Privacy ledger parent must not be writable by group or other users.",
           call. = FALSE)
    }
  }
  normalizePath(parent, winslash = "/", mustWork = TRUE)
}

.privacy_validate_ledger_file <- function(path, euid, repair_mode = FALSE) {
  if (!file.exists(path) || .path_is_symlink(path) ||
      !.path_is_regular_file(path)) {
    stop("Privacy ledger must be a regular file, not a symbolic link.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    info <- file.info(path)
    owner <- suppressWarnings(as.integer(info$uid[[1]]))
    if (is.na(owner) || !identical(owner, as.integer(euid))) {
      stop("Privacy ledger must be owned by the node EUID.", call. = FALSE)
    }
    expected_mode <- as.integer(strtoi("600", base = 8))
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    if (!identical(mode, expected_mode) && isTRUE(repair_mode)) {
      chmod_ok <- suppressWarnings(Sys.chmod(path, "0600"))
      if (length(chmod_ok) != 1L || !isTRUE(chmod_ok)) {
        stop("Could not set privacy ledger permissions to 0600.", call. = FALSE)
      }
      mode <- suppressWarnings(as.integer(file.info(path)$mode[[1]]))
    }
    if (is.na(mode) || !identical(mode, expected_mode)) {
      stop("Privacy ledger must have Unix mode exactly 0600.", call. = FALSE)
    }
  }
  normalizePath(path, winslash = "/", mustWork = TRUE)
}

.privacy_db_connect <- function(path = .privacy_ledger_path()) {
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NA_integer_
  parent_before <- .privacy_validate_ledger_parent(path, euid)
  if (file.exists(path) || .path_is_symlink(path)) {
    .privacy_validate_ledger_file(path, euid, repair_mode = TRUE)
  }

  # A restrictive umask avoids a group/world-readable creation window.  DBI
  # does not expose SQLite's file descriptor, so path security is revalidated
  # after opening; the Python release guard performs the stronger inode check.
  old_umask <- if (.Platform$OS.type == "unix") Sys.umask("0077") else NULL
  umask_restored <- FALSE
  on.exit({
    if (!umask_restored && !is.null(old_umask)) Sys.umask(old_umask)
  }, add = TRUE)
  con <- tryCatch(
    DBI::dbConnect(RSQLite::SQLite(), dbname = path),
    error = function(e) stop("Could not open the privacy ledger: ",
                             conditionMessage(e), call. = FALSE))
  if (!is.null(old_umask)) Sys.umask(old_umask)
  umask_restored <- TRUE
  ok <- FALSE
  on.exit(if (!ok) try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
  parent_after <- .privacy_validate_ledger_parent(path, euid)
  if (!identical(parent_after, parent_before)) {
    stop("Privacy ledger parent changed while opening.", call. = FALSE)
  }
  expected_path <- .privacy_validate_ledger_file(path, euid)
  opened <- DBI::dbGetQuery(con, "PRAGMA database_list")
  main <- opened$file[opened$name == "main"]
  if (length(main) != 1L || !nzchar(main[[1]]) ||
      !identical(normalizePath(main[[1]], winslash = "/", mustWork = TRUE),
                 expected_path)) {
    stop("SQLite opened an unexpected privacy ledger path.", call. = FALSE)
  }
  DBI::dbExecute(con, "PRAGMA busy_timeout = 10000")
  DBI::dbExecute(con, "PRAGMA journal_mode = WAL")
  DBI::dbExecute(con, "PRAGMA synchronous = FULL")
  DBI::dbExecute(con, "PRAGMA foreign_keys = ON")
  DBI::dbExecute(con, paste(
    "CREATE TABLE IF NOT EXISTS privacy_policy (",
    "domain TEXT PRIMARY KEY,",
    "total_epsilon REAL NOT NULL,",
    "total_delta REAL NOT NULL,",
    "decay REAL NOT NULL,",
    "policy_hash TEXT NOT NULL,",
    "next_index INTEGER NOT NULL CHECK(next_index >= 1))"))
  DBI::dbExecute(con, paste(
    "CREATE TABLE IF NOT EXISTS privacy_reservations (",
    "run_token TEXT PRIMARY KEY,",
    "domain TEXT NOT NULL,",
    "allocation_index INTEGER NOT NULL,",
    "epsilon REAL NOT NULL,",
    "delta REAL NOT NULL,",
    "max_releases INTEGER NOT NULL CHECK(max_releases >= 1),",
    "claimed_releases INTEGER NOT NULL DEFAULT 0 CHECK(claimed_releases >= 0),",
    "created_at TEXT NOT NULL,",
    "UNIQUE(domain, allocation_index),",
    "FOREIGN KEY(domain) REFERENCES privacy_policy(domain))"))
  DBI::dbExecute(con, paste(
    "CREATE TABLE IF NOT EXISTS privacy_release_claims (",
    "run_token TEXT NOT NULL,",
    "message_id TEXT NOT NULL,",
    "release_index INTEGER NOT NULL CHECK(release_index >= 1),",
    "created_at TEXT NOT NULL,",
    "PRIMARY KEY(run_token, message_id),",
    "UNIQUE(run_token, release_index),",
    "FOREIGN KEY(run_token) REFERENCES privacy_reservations(run_token))"))
  DBI::dbExecute(con, paste(
    "CREATE TABLE IF NOT EXISTS privacy_key_epochs (",
    "key_epoch INTEGER PRIMARY KEY CHECK(key_epoch >= 1),",
    "key_fingerprint TEXT NOT NULL",
    "CHECK(length(key_fingerprint) = 64 AND",
    "key_fingerprint NOT GLOB '*[^0-9a-f]*'),",
    "created_at TEXT NOT NULL)"))
  DBI::dbExecute(con, paste(
    "CREATE TABLE IF NOT EXISTS privacy_semantic_releases (",
    "domain TEXT NOT NULL,",
    "semantic_key TEXT NOT NULL",
    "CHECK(length(semantic_key) = 64 AND",
    "semantic_key NOT GLOB '*[^0-9a-f]*'),",
    "mechanism TEXT NOT NULL,",
    "generation INTEGER NOT NULL CHECK(generation >= 1),",
    "policy_hash TEXT NOT NULL",
    "CHECK(length(policy_hash) = 64 AND",
    "policy_hash NOT GLOB '*[^0-9a-f]*'),",
    "run_token TEXT NOT NULL UNIQUE,",
    "status TEXT NOT NULL CHECK(status IN ('reserved','committed','lost')),",
    "artifact_relpath TEXT,",
    "artifact_sha256 TEXT",
    "CHECK(artifact_sha256 IS NULL OR (length(artifact_sha256) = 64 AND",
    "artifact_sha256 NOT GLOB '*[^0-9a-f]*')),",
    "artifact_bytes INTEGER CHECK(artifact_bytes IS NULL OR artifact_bytes >= 0),",
    "created_at TEXT NOT NULL,",
    "committed_at TEXT,",
    "lost_at TEXT,",
    "PRIMARY KEY(domain, semantic_key, mechanism, generation))"))
  DBI::dbExecute(con, paste(
    "CREATE UNIQUE INDEX IF NOT EXISTS privacy_semantic_one_active",
    "ON privacy_semantic_releases(domain, semantic_key, mechanism)",
    "WHERE status IN ('reserved','committed')"))
  ok <- TRUE
  con
}

.privacy_db_begin <- function(con) {
  DBI::dbExecute(con, "BEGIN IMMEDIATE")
}

.privacy_db_rollback <- function(con) {
  try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
}

.privacy_db_quick_check <- function(con) {
  result <- tryCatch(
    DBI::dbGetQuery(con, "PRAGMA quick_check"),
    error = function(e) NULL
  )
  healthy <- !is.null(result) && ncol(result) == 1L && nrow(result) == 1L &&
    identical(tolower(as.character(result[[1L]][[1L]])), "ok")
  if (!healthy) {
    stop("The dsFlower privacy ledger failed SQLite quick_check.",
         call. = FALSE)
  }
  invisible(TRUE)
}

.read_node_secret_raw <- function(path = .node_secret_path()) {
  .validate_node_secret(path)
  bytes <- .read_node_secret_bytes(path)
  if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0a))) {
    bytes <- bytes[-length(bytes)]
    if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0d))) {
      bytes <- bytes[-length(bytes)]
    }
  }
  value <- tryCatch(rawToChar(bytes), error = function(e) "")
  decoded <- suppressWarnings(strtoi(
    substring(value, seq(1L, 63L, 2L), seq(2L, 64L, 2L)), base = 16L))
  if (length(bytes) != 64L || length(decoded) != 32L || anyNA(decoded)) {
    stop("The dsFlower node secret changed while it was being read.",
         call. = FALSE)
  }
  .validate_node_secret(path)
  as.raw(decoded)
}

.node_secret_fingerprint <- function(path = .node_secret_path()) {
  digest::digest(.read_node_secret_raw(path), algo = "sha256", serialize = FALSE)
}

#' Bootstrap persistent differential-privacy state at service runtime
#'
#' Creates or validates the SQLite accountant and the per-node CSPRNG secret,
#' records key rotations for audit, and performs no privacy allocation. Package
#' installation and loading deliberately never call this function, so a secret
#' cannot be baked into a reusable image.
#'
#' @return Invisibly, an operational status list containing `ok`, `key_epoch`
#'   and `key_action`. No secret, fingerprint, or filesystem path is returned.
#' @export
flowerPrivacyBootstrap <- function() {
  state <- .privacy_runtime_bootstrap()
  invisible(list(
    ok = TRUE,
    key_epoch = state$key_epoch,
    key_action = state$key_action
  ))
}

.privacy_runtime_bootstrap <- function() {
  ledger_path <- .privacy_ledger_path()
  con <- .privacy_db_connect(ledger_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  checked <- .dsflower_env$privacy_quick_checked %||% character(0)
  if (!ledger_path %in% checked) {
    .privacy_db_quick_check(con)
    .dsflower_env$privacy_quick_checked <- unique(c(checked, ledger_path))
  }
  .privacy_db_begin(con)
  committed <- FALSE
  on.exit(if (!committed) .privacy_db_rollback(con), add = TRUE)

  secret_path <- .ensure_node_secret()
  fingerprint <- .node_secret_fingerprint(secret_path)
  latest <- DBI::dbGetQuery(
    con,
    paste("SELECT key_epoch, key_fingerprint FROM privacy_key_epochs",
          "ORDER BY key_epoch DESC LIMIT 1"))
  if (nrow(latest) == 0L) {
    key_epoch <- 1L
    key_action <- "initialized"
  } else if (identical(as.character(latest$key_fingerprint[[1L]]),
                       fingerprint)) {
    key_epoch <- as.integer(latest$key_epoch[[1L]])
    key_action <- "reused"
  } else {
    previous_epoch <- suppressWarnings(as.numeric(latest$key_epoch[[1L]]))
    if (length(previous_epoch) != 1L || !is.finite(previous_epoch) ||
        previous_epoch < 1 || previous_epoch != floor(previous_epoch) ||
        previous_epoch >= 2^53 - 1) {
      stop("The dsFlower privacy key epoch is invalid.", call. = FALSE)
    }
    key_epoch <- previous_epoch + 1
    key_action <- "rotated"
  }

  if (!identical(key_action, "reused")) {
    DBI::dbExecute(
      con,
      paste("INSERT INTO privacy_key_epochs",
            "(key_epoch,key_fingerprint,created_at) VALUES (?,?,?)"),
      params = list(
        key_epoch, fingerprint,
        format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
  }
  DBI::dbExecute(con, "COMMIT")
  committed <- TRUE
  list(
    key_epoch = as.integer(key_epoch),
    key_action = key_action,
    secret_path = secret_path,
    ledger_path = ledger_path
  )
}

# One-way compatibility migration for the corrected patient-ID contract. Row
# adjacency never uses identifier canonicalisation, so preserving the allocation
# counter is always safe. Patient mode is migrated automatically only when the
# ledger proves that no v1 release was ever claimed; otherwise the two possible
# equivalence-class partitions must not be mixed without an offline roster audit.
.migrate_v1_policy_hash <- function(con, policy, stored_hash) {
  if (identical(as.character(stored_hash), policy$policy_hash)) return(TRUE)
  if (!identical(policy$accounting_mode, "lifetime-geometric")) return(FALSE)
  if (!identical(as.character(stored_hash), policy$legacy_v1_policy_hash) ||
      !identical(policy$unit_canonicalization, "trim-utf8-v2")) {
    return(FALSE)
  }

  safe <- identical(policy$dp_unit, "row")
  if (identical(policy$dp_unit, "patient")) {
    counts <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT COALESCE(SUM(r.claimed_releases), 0) AS claimed,",
        "COUNT(c.message_id) AS claim_rows",
        "FROM privacy_reservations r",
        "LEFT JOIN privacy_release_claims c ON c.run_token = r.run_token",
        "WHERE r.domain = ?"),
      params = list(policy$domain))
    safe <- nrow(counts) == 1L &&
      identical(as.numeric(counts$claimed[[1]]), 0) &&
      identical(as.numeric(counts$claim_rows[[1]]), 0)
  }
  if (!isTRUE(safe)) return(FALSE)

  if (identical(policy$dp_unit, "patient")) {
    DBI::dbExecute(
      con,
      paste(
        "UPDATE privacy_reservations",
        "SET claimed_releases = max_releases",
        "WHERE domain = ?"),
      params = list(policy$domain))
  }

  updated <- DBI::dbExecute(
    con,
    paste("UPDATE privacy_policy SET policy_hash = ?",
          "WHERE domain = ? AND policy_hash = ?"),
    params = list(
      policy$policy_hash, policy$domain, policy$legacy_v1_policy_hash))
  identical(as.integer(updated), 1L)
}

.privacy_release_is_viable <- function(policy, epsilon, delta, max_releases = 1L) {
  horizon <- suppressWarnings(as.numeric(max_releases))
  length(horizon) == 1L && is.finite(horizon) && horizon >= 1 &&
    length(epsilon) == 1L && length(delta) == 1L &&
    is.finite(epsilon) && is.finite(delta) && epsilon > 0 && delta > 0 &&
    epsilon / horizon >= policy$min_release_epsilon &&
    delta / horizon >= policy$min_release_delta
}

.privacy_geometric_allocation <- function(policy, index, max_releases = 1L) {
  exponent <- (as.numeric(index) - 1) * log(policy$decay)
  # A tiny fixed slack keeps the sum below one even after independent IEEE-754
  # evaluation of every geometric term (some rho values otherwise overshoot by
  # a few ulps). The utility difference is negligible.
  weight <- .privacy_allocation_slack *
    (1 - policy$decay) * exp(exponent)
  epsilon <- policy$total_epsilon * weight
  delta <- policy$total_delta * weight
  # Never round an allocation up to a positive floor: that would make the
  # infinite schedule diverge.  Keep the exact representable allocation in the
  # ledger/manifest, but mark numerically impractical per-release budgets as a
  # public no-op so downstream calibrators cannot fail on an infinitesimal tail.
  list(epsilon = epsilon, delta = delta,
       release_enabled = .privacy_release_is_viable(
         policy, epsilon, delta, max_releases))
}

.privacy_per_training_allocation <- function(policy, max_releases = 1L) {
  epsilon <- policy$per_training_epsilon
  delta <- policy$per_training_delta
  if (!.privacy_release_is_viable(policy, epsilon, delta, max_releases)) {
    stop(
      "The configured per-training epsilon/delta is below the numerical ",
      "minimum after division across privacy max_releases. Increase the ",
      "administrator-pinned per-training allocation or reduce the public ",
      "release horizon.", call. = FALSE)
  }
  list(epsilon = epsilon, delta = delta, release_enabled = TRUE)
}

.privacy_run_allocation <- function(policy, index, max_releases = 1L) {
  if (identical(policy$accounting_mode, "per-release-audit")) {
    return(.privacy_per_training_allocation(policy, max_releases))
  }
  .privacy_geometric_allocation(policy, index, max_releases)
}

.privacy_reservation_is_enabled <- function(policy, epsilon, delta,
                                            max_releases = 1L) {
  if (identical(policy$accounting_mode, "per-release-audit")) return(TRUE)
  .privacy_release_is_viable(policy, epsilon, delta, max_releases)
}

#' Atomically reserve the next configured privacy allocation for a run
#' @keywords internal
.reserve_privacy_run <- function(run_token, max_releases) {
  if (length(run_token) != 1L || is.na(run_token)) {
    stop("Invalid privacy run token.", call. = FALSE)
  }
  run_token <- as.character(run_token)
  numeric_horizon <- suppressWarnings(as.numeric(max_releases))
  if (length(numeric_horizon) != 1L || !is.finite(numeric_horizon) ||
      numeric_horizon < 1 || numeric_horizon != floor(numeric_horizon) ||
      numeric_horizon > .Machine$integer.max) {
    stop("privacy max_releases must be a positive integer.", call. = FALSE)
  }
  max_releases <- as.integer(numeric_horizon)
  if (!grepl("^run_[0-9a-f]{32}$", run_token, perl = TRUE)) {
    stop("Invalid privacy run token.", call. = FALSE)
  }

  policy <- .privacy_policy()
  # In audit-only mode this validates the complete public calibration contract
  # before resolving or creating any persistent state, and therefore before a
  # caller may proceed to private data access.
  if (identical(policy$accounting_mode, "per-release-audit")) {
    .privacy_per_training_allocation(policy, max_releases)
  }
  path <- .privacy_ledger_path()
  con <- .privacy_db_connect(path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  .privacy_db_begin(con)
  committed <- FALSE
  on.exit(if (!committed) .privacy_db_rollback(con), add = TRUE)

  ledger_domains <- DBI::dbGetQuery(
    con, "SELECT domain FROM privacy_policy ORDER BY domain")$domain
  allow_multiple_domains <- isTRUE(as.logical(
    .dsf_option("dp_allow_multiple_domains", FALSE)))
  foreign_domains <- setdiff(as.character(ledger_domains), policy$domain)
  if (length(foreign_domains) && !allow_multiple_domains) {
    stop(
      "This privacy ledger is already bound to a different privacy domain. ",
      "Changing dsflower.dp_privacy_domain must not reset a lifetime budget. ",
      "Only set dsflower.dp_allow_multiple_domains=TRUE when domains represent ",
      "provably disjoint populations.", call. = FALSE)
  }

  bound <- DBI::dbGetQuery(
    con, "SELECT policy_hash FROM privacy_policy WHERE domain = ?",
    params = list(policy$domain))
  if (nrow(bound) == 1L && !.migrate_v1_policy_hash(
      con, policy, bound$policy_hash[[1]])) {
    stop("The configured dsFlower privacy policy differs from the policy already ",
         "bound to this ledger/domain.", call. = FALSE)
  }

  existing <- DBI::dbGetQuery(
    con,
    paste("SELECT domain, allocation_index, epsilon, delta, max_releases",
          "FROM privacy_reservations WHERE run_token = ?"),
    params = list(run_token))
  if (nrow(existing) == 1L) {
    if (!identical(as.character(existing$domain[[1]]), policy$domain)) {
      stop("Existing privacy reservation belongs to a different domain.",
           call. = FALSE)
    }
    if (as.integer(existing$max_releases[[1]]) != max_releases) {
      stop("Existing privacy reservation has a different release horizon.",
           call. = FALSE)
    }
    eps <- as.numeric(existing$epsilon[[1]])
    del <- as.numeric(existing$delta[[1]])
    if (identical(policy$accounting_mode, "per-release-audit") &&
        (!identical(eps, policy$per_training_epsilon) ||
         !identical(del, policy$per_training_delta))) {
      stop("Existing audit-only reservation differs from the configured ",
           "per-training epsilon/delta.", call. = FALSE)
    }
    DBI::dbExecute(con, "COMMIT")
    committed <- TRUE
    return(list(
      domain = policy$domain,
      allocation_index = as.numeric(existing$allocation_index[[1]]),
      epsilon = eps, delta = del, max_releases = max_releases,
      dp_unit = policy$dp_unit,
      patient_column = policy$patient_column,
      unit_canonicalization = policy$unit_canonicalization,
      adjacency = policy$adjacency,
      accounting_mode = policy$accounting_mode,
      lifetime_bound = identical(
        policy$accounting_mode, "lifetime-geometric"),
      release_enabled = .privacy_reservation_is_enabled(
        policy, eps, del, max_releases),
      run_token = run_token, ledger_path = path, idempotent = TRUE))
  }

  stored <- DBI::dbGetQuery(
    con,
    paste("SELECT total_epsilon, total_delta, decay, policy_hash, next_index",
          "FROM privacy_policy WHERE domain = ?"),
    params = list(policy$domain))
  if (nrow(stored) == 0L) {
    stored_epsilon <- if (identical(
        policy$accounting_mode, "per-release-audit")) {
      policy$per_training_epsilon
    } else {
      policy$total_epsilon
    }
    stored_delta <- if (identical(
        policy$accounting_mode, "per-release-audit")) {
      policy$per_training_delta
    } else {
      policy$total_delta
    }
    DBI::dbExecute(
      con,
      paste("INSERT INTO privacy_policy",
            "(domain,total_epsilon,total_delta,decay,policy_hash,next_index)",
            "VALUES (?,?,?,?,?,1)"),
      params = list(policy$domain, stored_epsilon, stored_delta,
                    policy$decay, policy$policy_hash))
    index <- 1
  } else {
    if (!identical(as.character(stored$policy_hash[[1]]), policy$policy_hash)) {
      stop("The configured dsFlower privacy policy differs from the policy already ",
           "bound to this ledger/domain. Keep the original values; do not reset a ",
           "lifetime budget by changing options.", call. = FALSE)
    }
    index <- suppressWarnings(as.numeric(stored$next_index[[1]]))
    if (length(index) != 1L || !is.finite(index) || index < 1 ||
        index != floor(index) || index > 2^53 - 1) {
      stop("The privacy allocation counter is invalid or outside its exact range.",
           call. = FALSE)
    }
  }

  allocation <- .privacy_run_allocation(policy, index, max_releases)
  DBI::dbExecute(
    con,
    paste("INSERT INTO privacy_reservations",
          "(run_token,domain,allocation_index,epsilon,delta,max_releases,created_at)",
          "VALUES (?,?,?,?,?,?,?)"),
    params = list(
      run_token, policy$domain, index, allocation$epsilon, allocation$delta,
      max_releases, format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
  DBI::dbExecute(
    con, "UPDATE privacy_policy SET next_index = ? WHERE domain = ?",
    params = list(index + 1, policy$domain))
  DBI::dbExecute(con, "COMMIT")
  committed <- TRUE

  c(allocation, list(
    domain = policy$domain, allocation_index = index,
    accounting_mode = policy$accounting_mode,
    lifetime_bound = identical(
      policy$accounting_mode, "lifetime-geometric"),
    dp_unit = policy$dp_unit, patient_column = policy$patient_column,
    unit_canonicalization = policy$unit_canonicalization,
    adjacency = policy$adjacency,
    max_releases = max_releases, run_token = run_token,
    ledger_path = path, idempotent = FALSE))
}

.privacy_budget_status <- function() {
  policy <- .privacy_policy()
  path <- .privacy_ledger_path()
  con <- .privacy_db_connect(path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  row <- DBI::dbGetQuery(
    con,
    "SELECT next_index FROM privacy_policy WHERE domain = ?",
    params = list(policy$domain))
  n <- if (nrow(row)) as.numeric(row$next_index[[1]]) - 1 else 0
  if (identical(policy$accounting_mode, "per-release-audit")) {
    composition <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT COUNT(*) AS reservations,",
        "COALESCE(SUM(epsilon), 0) AS epsilon,",
        "COALESCE(SUM(delta), 0) AS delta,",
        "COALESCE(SUM(claimed_releases), 0) AS claimed_releases",
        "FROM privacy_reservations WHERE domain = ?"),
      params = list(policy$domain))
    return(list(
      accountant = "audit-only-basic-composition-v1",
      accounting_mode = policy$accounting_mode,
      domain = policy$domain,
      lifetime_bound = FALSE,
      nonblocking = TRUE,
      release_availability_unbounded = TRUE,
      guarantee_scope = "per-training-release",
      per_training_epsilon = policy$per_training_epsilon,
      per_training_delta = policy$per_training_delta,
      dp_unit = policy$dp_unit,
      patient_column = policy$patient_column,
      unit_canonicalization = policy$unit_canonicalization,
      adjacency = policy$adjacency,
      allocations = as.numeric(composition$reservations[[1L]]),
      claimed_releases = as.numeric(
        composition$claimed_releases[[1L]]),
      allocated_epsilon = as.numeric(composition$epsilon[[1L]]),
      allocated_delta = as.numeric(composition$delta[[1L]]),
      remaining_epsilon = NA_real_,
      remaining_delta = NA_real_,
      composition_statement = paste(
        "Finite-prefix basic composition is reported for audit only.",
        "Unlimited semantically new releases with a fixed positive",
        "allocation do not have a finite lifetime epsilon/delta bound.")))
  }
  allocated_weight <- if (n <= 0) 0 else
    .privacy_allocation_slack * (1 - policy$decay^n)
  list(
    accountant = "bounded-geometric-basic-composition-v2",
    accounting_mode = policy$accounting_mode,
    domain = policy$domain,
    lifetime_bound = TRUE,
    nonblocking = TRUE,
    release_availability_unbounded = FALSE,
    guarantee_scope = "node-domain-lifetime",
    total_epsilon = policy$total_epsilon,
    total_delta = policy$total_delta,
    decay = policy$decay,
    min_release_epsilon = policy$min_release_epsilon,
    min_release_delta = policy$min_release_delta,
    dp_unit = policy$dp_unit,
    patient_column = policy$patient_column,
    unit_canonicalization = policy$unit_canonicalization,
    adjacency = policy$adjacency,
    allocations = n,
    allocated_epsilon = policy$total_epsilon * allocated_weight,
    allocated_delta = policy$total_delta * allocated_weight,
    remaining_epsilon = policy$total_epsilon * (1 - allocated_weight),
    remaining_delta = policy$total_delta * (1 - allocated_weight))
}
