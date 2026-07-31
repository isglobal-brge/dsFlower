# Module: Persistent, non-blocking privacy accounting
#
# Each new run receives one term of a geometric privacy schedule.  Basic
# adaptive composition then bounds every finite prefix (and the infinite
# transcript) by the server-owned lifetime policy.  The schedule never rejects a
# run because a budget was "exhausted": allocations decrease towards zero.  Once
# a per-release allocation is too small to calibrate robustly, the runner returns
# the incoming public model unchanged while the exact allocation remains recorded.

.privacy_allocation_slack <- 1 - 1e-12

.privacy_policy <- function() {
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

  policy_text <- paste(
    "dsflower-privacy-v5", domain,
    format(total_epsilon, digits = 17, scientific = TRUE),
    format(total_delta, digits = 17, scientific = TRUE),
    format(decay, digits = 17, scientific = TRUE),
    format(min_release_epsilon, digits = 17, scientific = TRUE),
    format(min_release_delta, digits = 17, scientific = TRUE),
    unit_policy$dp_unit,
    unit_policy$patient_column %||% "<none>",
    unit_policy$canonicalization,
    adjacency,
    format(.privacy_allocation_slack, digits = 17, scientific = TRUE),
    sep = "|")
  list(
    domain = domain,
    total_epsilon = total_epsilon,
    total_delta = total_delta,
    decay = decay,
    min_release_epsilon = min_release_epsilon,
    min_release_delta = min_release_delta,
    dp_unit = unit_policy$dp_unit,
    patient_column = unit_policy$patient_column,
    unit_canonicalization = unit_policy$canonicalization,
    adjacency = adjacency,
    policy_hash = digest::digest(policy_text, algo = "sha256", serialize = FALSE)
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
  ok <- tryCatch({
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
    dir.exists(path)
  }, error = function(e) FALSE)
  if (!isTRUE(ok)) return(FALSE)
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
  explicit <- .dsf_option(
    "privacy_ledger_path",
    Sys.getenv("DSFLOWER_PRIVACY_LEDGER_PATH", ""))
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

.privacy_db_connect <- function(path = .privacy_ledger_path()) {
  if (.path_is_symlink(path)) {
    stop("Privacy ledger must not be a symbolic link.", call. = FALSE)
  }
  con <- tryCatch(
    DBI::dbConnect(RSQLite::SQLite(), dbname = path),
    error = function(e) stop("Could not open the privacy ledger: ",
                             conditionMessage(e), call. = FALSE))
  ok <- FALSE
  on.exit(if (!ok) try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
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
  Sys.chmod(path, "0600")
  ok <- TRUE
  con
}

.privacy_db_begin <- function(con) {
  DBI::dbExecute(con, "BEGIN IMMEDIATE")
}

.privacy_db_rollback <- function(con) {
  try(DBI::dbExecute(con, "ROLLBACK"), silent = TRUE)
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

#' Atomically reserve the next lifetime allocation for a run
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

  existing <- DBI::dbGetQuery(
    con,
    paste("SELECT allocation_index, epsilon, delta, max_releases",
          "FROM privacy_reservations WHERE run_token = ?"),
    params = list(run_token))
  if (nrow(existing) == 1L) {
    bound <- DBI::dbGetQuery(
      con, "SELECT policy_hash FROM privacy_policy WHERE domain = ?",
      params = list(policy$domain))
    if (nrow(bound) != 1L ||
        !identical(as.character(bound$policy_hash[[1]]), policy$policy_hash)) {
      stop("The configured dsFlower privacy policy differs from the policy already ",
           "bound to this ledger/domain.", call. = FALSE)
    }
    if (as.integer(existing$max_releases[[1]]) != max_releases) {
      stop("Existing privacy reservation has a different release horizon.",
           call. = FALSE)
    }
    DBI::dbExecute(con, "COMMIT")
    committed <- TRUE
    eps <- as.numeric(existing$epsilon[[1]])
    del <- as.numeric(existing$delta[[1]])
    return(list(
      domain = policy$domain,
      allocation_index = as.numeric(existing$allocation_index[[1]]),
      epsilon = eps, delta = del, max_releases = max_releases,
      dp_unit = policy$dp_unit,
      patient_column = policy$patient_column,
      unit_canonicalization = policy$unit_canonicalization,
      adjacency = policy$adjacency,
      release_enabled = .privacy_release_is_viable(
        policy, eps, del, max_releases),
      run_token = run_token, ledger_path = path, idempotent = TRUE))
  }

  stored <- DBI::dbGetQuery(
    con,
    paste("SELECT total_epsilon, total_delta, decay, policy_hash, next_index",
          "FROM privacy_policy WHERE domain = ?"),
    params = list(policy$domain))
  if (nrow(stored) == 0L) {
    DBI::dbExecute(
      con,
      paste("INSERT INTO privacy_policy",
            "(domain,total_epsilon,total_delta,decay,policy_hash,next_index)",
            "VALUES (?,?,?,?,?,1)"),
      params = list(policy$domain, policy$total_epsilon, policy$total_delta,
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

  allocation <- .privacy_geometric_allocation(policy, index, max_releases)
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
  allocated_weight <- if (n <= 0) 0 else
    .privacy_allocation_slack * (1 - policy$decay^n)
  list(
    accountant = "bounded-geometric-basic-composition-v2",
    domain = policy$domain,
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
