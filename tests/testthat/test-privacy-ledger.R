# Tests for the persistent bounded-geometric privacy accountant.

local_test_privacy_ledger <- function(path = tempfile(fileext = ".sqlite"),
                                      .local_envir = parent.frame()) {
  withr::local_options(list(
    dsflower.privacy_ledger_path = path,
    dsflower.dp_total_epsilon = 3,
    dsflower.dp_total_delta = 1e-5,
    dsflower.dp_budget_decay = 0.5,
    dsflower.dp_min_release_epsilon = 1e-6,
    dsflower.dp_min_release_delta = 1e-12,
    dsflower.dp_privacy_domain = "node",
    dsflower.dp_unit = "row",
    dsflower.patient_column = NULL,
    dsflower.dp_allow_multiple_domains = FALSE
  ), .local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_PRIVACY_LEDGER_PATH = NA_character_,
    DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
  ), .local_envir = .local_envir)
  path
}

test_that("geometric schedule is bounded and tiny allocations become exact no-ops", {
  withr::local_options(list(
    dsflower.dp_total_epsilon = 3,
    dsflower.dp_total_delta = 1e-5,
    dsflower.dp_budget_decay = 0.5,
    dsflower.dp_min_release_epsilon = 1e-6,
    dsflower.dp_min_release_delta = 1e-12
  ))
  policy <- dsFlower:::.privacy_policy()
  allocations <- lapply(1:30, function(i) {
    dsFlower:::.privacy_geometric_allocation(policy, i)
  })
  epsilon <- vapply(allocations, `[[`, numeric(1), "epsilon")
  delta <- vapply(allocations, `[[`, numeric(1), "delta")

  expect_equal(epsilon, (1 - 1e-12) * 3 * 0.5^(1:30), tolerance = 1e-15)
  expect_equal(delta, (1 - 1e-12) * 1e-5 * 0.5^(1:30), tolerance = 1e-15)
  expect_lt(sum(epsilon), policy$total_epsilon)
  expect_lt(sum(delta), policy$total_delta)
  expect_true(allocations[[1]]$release_enabled)
  expect_false(allocations[[30]]$release_enabled)
  expect_gt(allocations[[30]]$epsilon, 0)
  expect_gt(allocations[[30]]$delta, 0)

  for (rho in seq(0.5, 0.99, length.out = 100)) {
    p <- withr::with_options(
      list(dsflower.dp_budget_decay = rho),
      dsFlower:::.privacy_policy())
    represented <- sum(
      p$total_epsilon * (1 - 1e-12) * (1 - rho) *
        exp((0:9999) * log(rho)))
    expect_lte(represented, p$total_epsilon)
  }
})

test_that("release viability accounts for the per-release horizon", {
  withr::local_options(list(
    dsflower.dp_total_epsilon = 3,
    dsflower.dp_total_delta = 1e-5,
    dsflower.dp_budget_decay = 0.5,
    dsflower.dp_min_release_epsilon = 1e-6,
    dsflower.dp_min_release_delta = 1e-12
  ))
  policy <- dsFlower:::.privacy_policy()
  expect_true(dsFlower:::.privacy_geometric_allocation(policy, 10L, 1L)$release_enabled)
  expect_false(dsFlower:::.privacy_geometric_allocation(policy, 30L, 500L)$release_enabled)
})

test_that("release viability thresholds cannot weaken numeric safety floors", {
  expect_error(
    withr::with_options(
      list(dsflower.dp_min_release_epsilon = 1e-7),
      dsFlower:::.privacy_policy()),
    "\\[1e-6, 10\\]"
  )
  expect_error(
    withr::with_options(
      list(dsflower.dp_min_release_delta = 1e-13),
      dsFlower:::.privacy_policy()),
    "\\[1e-12, 1e-3\\]"
  )
})

test_that("reservation is atomic and idempotent by run token", {
  ledger <- local_test_privacy_ledger()
  token_a <- paste0("run_", strrep("a", 32))
  token_b <- paste0("run_", strrep("b", 32))

  first <- dsFlower:::.reserve_privacy_run(token_a, 5L)
  replay <- dsFlower:::.reserve_privacy_run(token_a, 5L)
  second <- dsFlower:::.reserve_privacy_run(token_b, 5L)

  expect_false(first$idempotent)
  expect_true(replay$idempotent)
  expect_equal(replay[c("allocation_index", "epsilon", "delta")],
               first[c("allocation_index", "epsilon", "delta")])
  expect_equal(second$allocation_index, 2L)
  expect_equal(first$run_token, token_a)
  expect_error(
    dsFlower:::.reserve_privacy_run(token_a, 6L),
    "different release horizon"
  )

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM privacy_reservations")$n,
               2L)
})

test_that("privacy ledger is a private regular file and unsafe paths fail closed", {
  ledger <- local_test_privacy_ledger()
  dsFlower:::.reserve_privacy_run(paste0("run_", strrep("9", 32)), 1L)

  expect_true(utils::file_test("-f", ledger))
  expect_false(dsFlower:::.path_is_symlink(ledger))
  if (.Platform$OS.type == "unix") {
    expected_mode <- as.integer(strtoi("600", base = 8))
    expect_identical(as.integer(file.info(ledger)$mode[[1]]), expected_mode)

    Sys.chmod(ledger, "0644")
    con <- dsFlower:::.privacy_db_connect(ledger)
    DBI::dbDisconnect(con)
    expect_identical(as.integer(file.info(ledger)$mode[[1]]), expected_mode)
  }

  directory <- tempfile()
  dir.create(directory)
  expect_error(
    dsFlower:::.privacy_db_connect(directory),
    "regular file"
  )

  if (.Platform$OS.type == "unix") {
    target <- tempfile(fileext = ".sqlite")
    file.create(target)
    link <- tempfile(fileext = ".sqlite")
    expect_true(file.symlink(target, link))
    expect_error(
      dsFlower:::.privacy_db_connect(link),
      "symbolic link"
    )

    mkfifo <- Sys.which("mkfifo")
    if (nzchar(mkfifo)) {
      fifo <- tempfile(fileext = ".sqlite")
      expect_identical(system2(mkfifo, shQuote(fifo)), 0L)
      expect_error(
        dsFlower:::.privacy_db_connect(fifo),
        "regular file"
      )
    }
  }
})

test_that("privacy ledger ownership and parent-directory trust fail closed", {
  skip_on_os("windows")
  root <- tempfile()
  dir.create(root, mode = "0700")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  Sys.chmod(root, "0700")
  ledger <- file.path(root, "ledger.sqlite")
  con <- dsFlower:::.privacy_db_connect(ledger)
  DBI::dbDisconnect(con)

  euid <- dsFlower:::.privacy_effective_uid()
  expect_identical(as.integer(file.info(ledger)$uid[[1]]), euid)
  expect_error(
    dsFlower:::.privacy_validate_ledger_file(ledger, euid + 1L),
    "owned by the node EUID"
  )
  expect_error(
    dsFlower:::.privacy_validate_ledger_parent(ledger, euid + 1L),
    "owned by the node EUID"
  )

  Sys.chmod(root, "0770", use_umask = FALSE)
  expect_error(
    dsFlower:::.privacy_db_connect(ledger),
    "group or other"
  )
  Sys.chmod(root, "0700")

  parent_link <- paste0(root, "-link")
  expect_true(file.symlink(root, parent_link))
  on.exit(unlink(parent_link), add = TRUE)
  expect_error(
    dsFlower:::.privacy_validate_ledger_parent(
      file.path(parent_link, "ledger.sqlite"), euid),
    "real directory"
  )
})

test_that("new privacy ledger and directory are private at creation", {
  skip_on_os("windows")
  parent <- tempfile()
  expect_true(dsFlower:::.privacy_dir_writable(parent))
  on.exit(unlink(parent, recursive = TRUE), add = TRUE)
  expect_identical(
    as.integer(file.info(parent)$mode[[1]]),
    as.integer(strtoi("700", base = 8))
  )

  ledger <- file.path(parent, "ledger.sqlite")
  old_umask <- Sys.umask("0000")
  on.exit(Sys.umask(old_umask), add = TRUE)
  con <- dsFlower:::.privacy_db_connect(ledger)
  DBI::dbDisconnect(con)
  expect_identical(
    as.integer(file.info(ledger)$mode[[1]]),
    as.integer(strtoi("600", base = 8))
  )
  observed <- Sys.umask()
  expect_identical(as.integer(observed), as.integer(strtoi("0", base = 8)))
})

test_that("policy drift and accidental domain resets fail closed", {
  local_test_privacy_ledger()
  dsFlower:::.reserve_privacy_run(paste0("run_", strrep("1", 32)), 1L)

  withr::with_options(list(dsflower.dp_total_epsilon = 2), {
    expect_error(
      dsFlower:::.reserve_privacy_run(paste0("run_", strrep("2", 32)), 1L),
      "privacy policy differs"
    )
  })
  withr::with_options(list(dsflower.dp_privacy_domain = "other"), {
    expect_error(
      dsFlower:::.reserve_privacy_run(paste0("run_", strrep("3", 32)), 1L),
      "different privacy domain"
    )
  })
  withr::with_options(list(
    dsflower.dp_privacy_domain = "disjoint-population",
    dsflower.dp_allow_multiple_domains = TRUE
  ), {
    allowed <- dsFlower:::.reserve_privacy_run(
      paste0("run_", strrep("4", 32)), 1L)
    expect_equal(allowed$domain, "disjoint-population")
    expect_equal(allowed$allocation_index, 1L)
  })
})

test_that("lifetime policy hash binds the DP unit, patient column, and adjacency", {
  row_policy <- withr::with_options(list(
    dsflower.dp_unit = "row", dsflower.patient_column = NULL),
    dsFlower:::.privacy_policy())
  patient_a <- withr::with_options(list(
    dsflower.dp_unit = "patient", dsflower.patient_column = "patient_id"),
    dsFlower:::.privacy_policy())
  patient_b <- withr::with_options(list(
    dsflower.dp_unit = "patient", dsflower.patient_column = "subject_id"),
    dsFlower:::.privacy_policy())
  expect_false(identical(row_policy$policy_hash, patient_a$policy_hash))
  expect_false(identical(patient_a$policy_hash, patient_b$policy_hash))
  expect_equal(patient_a$dp_unit, "patient")
  expect_equal(patient_a$patient_column, "patient_id")
  expect_identical(patient_a$adjacency, "replace_one")
})

test_that("row ledgers migrate v1 to v2 without resetting accounting state", {
  ledger <- local_test_privacy_ledger()
  first_token <- paste0("run_", strrep("a", 32))
  next_token <- paste0("run_", strrep("b", 32))
  dsFlower:::.reserve_privacy_run(first_token, 2L)
  policy <- dsFlower:::.privacy_policy()

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  DBI::dbExecute(
    con, "UPDATE privacy_policy SET policy_hash = ? WHERE domain = ?",
    params = list(policy$legacy_v1_policy_hash, policy$domain))
  DBI::dbExecute(
    con,
    "UPDATE privacy_reservations SET claimed_releases = 1 WHERE run_token = ?",
    params = list(first_token))
  DBI::dbExecute(con, paste(
    "INSERT INTO privacy_release_claims",
    "(run_token,message_id,release_index,created_at) VALUES (?,?,?,?)"),
    params = list(first_token, "v1-message", 1L, "2026-01-01T00:00:00Z"))
  DBI::dbDisconnect(con)

  next_run <- dsFlower:::.reserve_privacy_run(next_token, 2L)
  expect_equal(next_run$allocation_index, 2L)

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  stored <- DBI::dbGetQuery(
    con, "SELECT policy_hash, next_index FROM privacy_policy WHERE domain = ?",
    params = list(policy$domain))
  expect_identical(stored$policy_hash[[1]], policy$policy_hash)
  expect_equal(stored$next_index[[1]], 3L)
  expect_equal(DBI::dbGetQuery(
    con, "SELECT COUNT(*) AS n FROM privacy_release_claims")$n, 1L)
})

test_that("patient migration exhausts legacy tokens and claimed v1 fails closed", {
  unclaimed_ledger <- local_test_privacy_ledger()
  withr::local_options(list(
    dsflower.dp_unit = "patient",
    dsflower.patient_column = "patient_id"
  ))
  policy <- dsFlower:::.privacy_policy()
  first_token <- paste0("run_", strrep("c", 32))
  next_token <- paste0("run_", strrep("d", 32))
  dsFlower:::.reserve_privacy_run(first_token, 2L)
  con <- DBI::dbConnect(RSQLite::SQLite(), unclaimed_ledger)
  DBI::dbExecute(
    con, "UPDATE privacy_policy SET policy_hash = ? WHERE domain = ?",
    params = list(policy$legacy_v1_policy_hash, policy$domain))
  DBI::dbDisconnect(con)
  migrated <- dsFlower:::.reserve_privacy_run(next_token, 1L)
  expect_equal(migrated$allocation_index, 2L)

  con <- DBI::dbConnect(RSQLite::SQLite(), unclaimed_ledger)
  reservations <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT run_token, claimed_releases, max_releases",
      "FROM privacy_reservations ORDER BY allocation_index"))
  expect_identical(reservations$run_token, c(first_token, next_token))
  expect_equal(reservations$claimed_releases, c(2L, 0L))
  expect_equal(reservations$max_releases, c(2L, 1L))
  expect_equal(DBI::dbExecute(
    con,
    paste(
      "UPDATE privacy_reservations",
      "SET claimed_releases = claimed_releases + 1",
      "WHERE run_token = ? AND claimed_releases = ?",
      "AND claimed_releases < max_releases"),
    params = list(first_token, 2L)), 0L)
  expect_equal(DBI::dbExecute(
    con,
    paste(
      "UPDATE privacy_reservations",
      "SET claimed_releases = claimed_releases + 1",
      "WHERE run_token = ? AND claimed_releases = ?",
      "AND claimed_releases < max_releases"),
    params = list(next_token, 0L)), 1L)
  DBI::dbDisconnect(con)

  claimed_ledger <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsflower.privacy_ledger_path = claimed_ledger))
  claimed_token <- paste0("run_", strrep("e", 32))
  dsFlower:::.reserve_privacy_run(claimed_token, 1L)
  con <- DBI::dbConnect(RSQLite::SQLite(), claimed_ledger)
  DBI::dbExecute(
    con, "UPDATE privacy_policy SET policy_hash = ? WHERE domain = ?",
    params = list(policy$legacy_v1_policy_hash, policy$domain))
  DBI::dbExecute(
    con,
    "UPDATE privacy_reservations SET claimed_releases = 1 WHERE run_token = ?",
    params = list(claimed_token))
  DBI::dbExecute(con, paste(
    "INSERT INTO privacy_release_claims",
    "(run_token,message_id,release_index,created_at) VALUES (?,?,?,?)"),
    params = list(claimed_token, "v1-message", 1L, "2026-01-01T00:00:00Z"))
  DBI::dbDisconnect(con)

  expect_error(
    dsFlower:::.reserve_privacy_run(
      paste0("run_", strrep("f", 32)), 1L),
    "privacy policy differs"
  )
  con <- DBI::dbConnect(RSQLite::SQLite(), claimed_ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  expect_identical(
    DBI::dbGetQuery(
      con, "SELECT policy_hash FROM privacy_policy WHERE domain = ?",
      params = list(policy$domain))$policy_hash[[1]],
    policy$legacy_v1_policy_hash)
})

test_that("ambiguous legacy policy fails closed without mutating its ledger", {
  ledger <- local_test_privacy_ledger()
  token <- paste0("run_", strrep("a", 32))
  dsFlower:::.reserve_privacy_run(token, 2L)

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  DBI::dbExecute(con,
    "UPDATE privacy_policy SET policy_hash = ? WHERE domain = ?",
    params = list("ambiguous-pre-unit-policy", "node"))
  DBI::dbExecute(con,
    "UPDATE privacy_reservations SET claimed_releases = 1 WHERE run_token = ?",
    params = list(token))
  DBI::dbExecute(con, paste(
    "INSERT INTO privacy_release_claims",
    "(run_token,message_id,release_index,created_at) VALUES (?,?,?,?)"),
    params = list(token, "legacy-message", 1L, "2026-01-01T00:00:00Z"))
  before_policy <- DBI::dbGetQuery(con, "SELECT * FROM privacy_policy")
  before_reservations <- DBI::dbGetQuery(
    con, "SELECT * FROM privacy_reservations ORDER BY run_token")
  before_claims <- DBI::dbGetQuery(
    con, "SELECT * FROM privacy_release_claims ORDER BY run_token,message_id")
  DBI::dbDisconnect(con)

  expect_error(
    dsFlower:::.reserve_privacy_run(
      paste0("run_", strrep("b", 32)), 1L),
    "privacy policy differs"
  )

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  expect_equal(DBI::dbGetQuery(con, "SELECT * FROM privacy_policy"),
               before_policy)
  expect_equal(DBI::dbGetQuery(
    con, "SELECT * FROM privacy_reservations ORDER BY run_token"),
    before_reservations)
  expect_equal(DBI::dbGetQuery(
    con, "SELECT * FROM privacy_release_claims ORDER BY run_token,message_id"),
    before_claims)
})

test_that("ephemeral and lexically disguised ledger paths are rejected", {
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = ""))
  disguised <- file.path("/opt", "not-created", "..", "..", "tmp",
                          "dsflower-ledger.sqlite")
  withr::local_options(list(dsflower.privacy_ledger_path = disguised))
  expect_error(dsFlower:::.privacy_ledger_path(), "must be persistent")

  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"))
  resolved <- dsFlower:::.privacy_ledger_path()
  expect_true(dsFlower:::.privacy_path_is_ephemeral(resolved))
  expect_false(any(grepl("(^|/)\\.\\.(/|$)", resolved)))
})

test_that("reservation inputs are strict scalars", {
  local_test_privacy_ledger()
  token <- paste0("run_", strrep("a", 32))
  expect_error(dsFlower:::.reserve_privacy_run(NULL, 1L), "Invalid privacy run token")
  expect_error(dsFlower:::.reserve_privacy_run(token, 1.5), "positive integer")
  expect_error(dsFlower:::.reserve_privacy_run(c(token, token), 1L),
               "Invalid privacy run token")
})

test_that("node secret is generated from exactly 32 bytes and rejects trailing data", {
  withr::with_tempdir({
    secret <- file.path(getwd(), "node-secret")
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
    ))

    expect_invisible(dsFlower:::.ensure_node_secret())
    expect_false(file.exists(file.path(getwd(), "0600")))
    value <- readLines(secret, warn = FALSE)
    expect_length(value, 1L)
    expect_match(value, "^[0-9a-f]{64}$")
    decoded <- strtoi(substring(value, seq(1L, 63L, 2L), seq(2L, 64L, 2L)),
                      base = 16L)
    expect_length(decoded, 32L)
    expect_equal(dsFlower:::.ensure_node_secret(), secret)

    if (.Platform$OS.type == "unix") {
      for (mode in c("0400", "0640", "0700")) {
        Sys.chmod(secret, mode)
        expect_error(
          dsFlower:::.validate_node_secret(secret),
          "exactly 0600",
          info = paste("mode", mode, "must be rejected")
        )
      }
      Sys.chmod(secret, "0600")
      expect_equal(dsFlower:::.validate_node_secret(secret), secret)
    }

    writeLines(c(strrep("a", 64), "trailing-content"), secret, useBytes = TRUE)
    Sys.chmod(secret, "0600")
    expect_error(dsFlower:::.validate_node_secret(secret), "exactly 32 bytes")
  })
})

test_that("concurrent first use installs one stable node secret", {
  skip_on_os("windows")
  source_root <- normalizePath(testthat::test_path("..", ".."))
  withr::with_tempdir({
    secret <- file.path(getwd(), "node-secret")
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
    ))

    command <- file.path(R.home("bin"), "Rscript")
    expression <- paste(
      "loadNamespace('dsFlower');",
      "if (exists('.ensure_node_secret', asNamespace('dsFlower'),",
      "inherits=FALSE)) {",
      "getFromNamespace('.ensure_node_secret', 'dsFlower')()",
      "} else {",
      "root <- Sys.getenv('DSFLOWER_TEST_SOURCE_ROOT');",
      "for (f in list.files(file.path(root, 'R'), pattern='[.]R$',",
      "full.names=TRUE)) sys.source(f, envir=.GlobalEnv);",
      ".ensure_node_secret()",
      "}"
    )
    workers <- lapply(seq_len(8L), function(...) {
      processx::process$new(
        command, c("-e", expression),
        env = c(
          "current",
          DSFLOWER_TEST_SOURCE_ROOT = source_root,
          DSFLOWER_NODE_SECRET_FILE = secret,
          DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
        ),
        stdout = "|", stderr = "|", cleanup = TRUE
      )
    })
    withr::defer(lapply(workers, function(worker) {
      if (worker$is_alive()) worker$kill_tree()
    }))
    lapply(workers, function(worker) worker$wait(timeout = 10000))
    expect_true(all(vapply(
      workers, function(worker) identical(worker$get_exit_status(), 0L),
      logical(1)
    )))
    expect_length(readLines(secret, warn = FALSE), 1L)
    expect_match(readLines(secret, warn = FALSE), "^[0-9a-f]{64}$")
    expect_false(any(grepl(
      "^\\.node-secret-", list.files(getwd(), all.files = TRUE)
    )))
    expect_identical(dsFlower:::.validate_node_secret(secret), secret)
  })
})

test_that("node-secret parent is private, owned and stable", {
  skip_if(.Platform$OS.type != "unix")
  withr::with_tempdir({
    root <- file.path(getwd(), "secret-root")
    secret <- file.path(root, "node-secret")
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
    ))

    expect_invisible(dsFlower:::.ensure_node_secret())
    mode <- suppressWarnings(as.integer(file.info(root)$mode[[1L]]))
    expect_identical(
      bitwAnd(mode, as.integer(strtoi("077", base = 8))), 0L
    )

    withr::defer(Sys.chmod(root, "0700"))
    Sys.chmod(root, "0770", use_umask = FALSE)
    expect_error(
      dsFlower:::.validate_node_secret(secret),
      "must not be writable by group or other users"
    )
  })
})

test_that("node-secret validation rejects non-regular filesystem objects", {
  skip_on_os("windows")
  mkfifo <- Sys.which("mkfifo")
  skip_if(!nzchar(mkfifo), "mkfifo is required")
  withr::with_tempdir({
    fifo <- file.path(getwd(), "node-secret-fifo")
    expect_identical(system2(mkfifo, shQuote(fifo)), 0L)
    Sys.chmod(fifo, "0600")
    expect_error(dsFlower:::.validate_node_secret(fifo), "regular file")
  })
})

test_that("ephemeral node-secret paths require the explicit test escape hatch", {
  secret <- file.path(tempdir(), "dsflower-ephemeral-secret")
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = secret,
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = ""
  ))
  expect_error(dsFlower:::.node_secret_path(), "must be persistent")
})

test_that("loading the package never materializes runtime privacy state", {
  withr::with_tempdir({
    secret <- file.path(getwd(), "privacy", "noise_root")
    ledger <- file.path(getwd(), "privacy", "ledger.sqlite")
    venv <- file.path(getwd(), "venvs")
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_VENV_ROOT = venv,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))

    dsFlower:::.onLoad("", "dsFlower")

    expect_true(dir.exists(venv))
    expect_false(file.exists(secret) || dsFlower:::.path_is_symlink(secret))
    expect_false(file.exists(ledger) || dsFlower:::.path_is_symlink(ledger))
  })
})

test_that("runtime bootstrap creates no allocation and rotates unusable secrets", {
  withr::with_tempdir({
    state <- file.path(getwd(), "privacy")
    secret <- file.path(state, "noise_root")
    ledger <- file.path(state, "ledger.sqlite")
    withr::local_options(list(dsflower.privacy_ledger_path = ledger))
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))

    initialized <- flowerPrivacyBootstrap()
    first_secret <- readLines(secret, warn = FALSE)
    expect_identical(initialized$key_epoch, 1L)
    expect_identical(initialized$key_action, "initialized")
    expect_match(first_secret, "^[0-9a-f]{64}$")

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT COUNT(*) AS n FROM privacy_policy")$n, 0L)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT COUNT(*) AS n FROM privacy_reservations")$n, 0L)
    DBI::dbDisconnect(con)

    reused <- flowerPrivacyBootstrap()
    expect_identical(reused$key_epoch, 1L)
    expect_identical(reused$key_action, "reused")
    expect_identical(readLines(secret, warn = FALSE), first_secret)

    writeLines("corrupt", secret, useBytes = TRUE)
    Sys.chmod(secret, "0600")
    repaired <- flowerPrivacyBootstrap()
    second_secret <- readLines(secret, warn = FALSE)
    expect_identical(repaired$key_epoch, 2L)
    expect_identical(repaired$key_action, "rotated")
    expect_match(second_secret, "^[0-9a-f]{64}$")
    expect_false(identical(second_secret, first_secret))

    unlink(secret)
    regenerated <- flowerPrivacyBootstrap()
    expect_identical(regenerated$key_epoch, 3L)
    expect_identical(regenerated$key_action, "rotated")
    expect_match(readLines(secret, warn = FALSE), "^[0-9a-f]{64}$")

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    epochs <- DBI::dbGetQuery(
      con,
      paste("SELECT key_epoch, key_fingerprint",
            "FROM privacy_key_epochs ORDER BY key_epoch"))
    expect_identical(epochs$key_epoch, 1:3)
    expect_true(all(grepl("^[0-9a-f]{64}$", epochs$key_fingerprint)))
    expect_equal(DBI::dbGetQuery(
      con, "SELECT COUNT(*) AS n FROM privacy_policy")$n, 0L)
  })
})

test_that("ledger quick-check runs once per process and path", {
  withr::with_tempdir({
    secret <- file.path(getwd(), "privacy", "noise_root")
    ledger <- file.path(getwd(), "privacy", "ledger.sqlite")
    withr::local_options(list(dsflower.privacy_ledger_path = ledger))
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))
    checks <- 0L
    local_mocked_bindings(
      .privacy_db_quick_check = function(con) {
        checks <<- checks + 1L
        invisible(TRUE)
      }
    )

    flowerPrivacyBootstrap()
    flowerPrivacyBootstrap()
    expect_identical(checks, 1L)
  })
})

test_that("runtime bootstrap rotates an exposed key but never follows a symlink", {
  skip_on_os("windows")
  withr::with_tempdir({
    state <- file.path(getwd(), "privacy")
    secret <- file.path(state, "noise_root")
    ledger <- file.path(state, "ledger.sqlite")
    withr::local_options(list(dsflower.privacy_ledger_path = ledger))
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))

    flowerPrivacyBootstrap()
    original <- readLines(secret, warn = FALSE)
    Sys.chmod(secret, "0644")
    rotated <- flowerPrivacyBootstrap()
    expect_identical(rotated$key_epoch, 2L)
    expect_false(identical(readLines(secret, warn = FALSE), original))
    expect_identical(
      as.integer(file.info(secret)$mode[[1]]),
      as.integer(strtoi("600", base = 8))
    )

    target <- file.path(getwd(), "attacker-target")
    writeLines(strrep("a", 64), target, useBytes = TRUE)
    unlink(secret)
    expect_true(file.symlink(target, secret))
    expect_error(flowerPrivacyBootstrap(), "symbolic link")
    expect_true(dsFlower:::.path_is_symlink(secret))
    expect_identical(readLines(target, warn = FALSE), strrep("a", 64))

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT MAX(key_epoch) AS epoch FROM privacy_key_epochs")$epoch, 2L)
  })
})

test_that("conflicting ledger environment and R options fail closed", {
  withr::with_tempdir({
    from_env <- file.path(getwd(), "env", "ledger.sqlite")
    from_option <- file.path(getwd(), "option", "ledger.sqlite")
    withr::local_envvar(c(
      DSFLOWER_PRIVACY_LEDGER_PATH = from_env,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))
    withr::local_options(list(dsflower.privacy_ledger_path = from_option))
    expect_error(dsFlower:::.privacy_ledger_path(), "conflicts")

    withr::local_options(list(dsflower.privacy_ledger_path = from_env))
    expect_identical(dsFlower:::.privacy_ledger_path(), from_env)
  })
})

test_that("bootstrap preserves an existing accountant when regenerating its key", {
  withr::with_tempdir({
    secret <- file.path(getwd(), "privacy", "noise_root")
    ledger <- file.path(getwd(), "privacy", "ledger.sqlite")
    withr::local_options(list(dsflower.privacy_ledger_path = ledger))
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))
    token <- paste0("run_", strrep("7", 32))
    reservation <- dsFlower:::.reserve_privacy_run(token, 3L)

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    before_policy <- DBI::dbGetQuery(con, "SELECT * FROM privacy_policy")
    before_reservations <- DBI::dbGetQuery(
      con, "SELECT * FROM privacy_reservations")
    DBI::dbDisconnect(con)

    expect_false(file.exists(secret))
    state <- flowerPrivacyBootstrap()
    expect_identical(state$key_epoch, 1L)
    expect_identical(state$key_action, "initialized")

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    expect_equal(DBI::dbGetQuery(con, "SELECT * FROM privacy_policy"),
                 before_policy)
    expect_equal(DBI::dbGetQuery(con, "SELECT * FROM privacy_reservations"),
                 before_reservations)
    DBI::dbDisconnect(con)

    writeLines("invalid", secret, useBytes = TRUE)
    Sys.chmod(secret, "0600")
    state <- flowerPrivacyBootstrap()
    expect_identical(state$key_epoch, 2L)
    expect_identical(state$key_action, "rotated")
    expect_identical(
      dsFlower:::.reserve_privacy_run(token, 3L)$allocation_index,
      reservation$allocation_index
    )

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    expect_equal(DBI::dbGetQuery(con, "SELECT * FROM privacy_policy"),
                 before_policy)
    expect_equal(DBI::dbGetQuery(con, "SELECT * FROM privacy_reservations"),
                 before_reservations)
  })
})

test_that("concurrent runtime bootstraps install one key epoch", {
  skip_on_os("windows")
  source_root <- normalizePath(testthat::test_path("..", ".."))
  withr::with_tempdir({
    secret <- file.path(getwd(), "privacy", "noise_root")
    ledger <- file.path(getwd(), "privacy", "ledger.sqlite")
    command <- file.path(R.home("bin"), "Rscript")
    expression <- paste(
      "loadNamespace('dsFlower');",
      "if (exists('.privacy_runtime_bootstrap', asNamespace('dsFlower'),",
      "inherits=FALSE)) {",
      "getFromNamespace('.privacy_runtime_bootstrap', 'dsFlower')()",
      "} else {",
      "root <- Sys.getenv('DSFLOWER_TEST_SOURCE_ROOT');",
      "for (f in list.files(file.path(root, 'R'), pattern='[.]R$',",
      "full.names=TRUE)) sys.source(f, envir=.GlobalEnv);",
      ".privacy_runtime_bootstrap()",
      "}"
    )
    workers <- lapply(seq_len(8L), function(...) {
      processx::process$new(
        command, c("-e", expression),
        env = c(
          "current",
          DSFLOWER_TEST_SOURCE_ROOT = source_root,
          DSFLOWER_NODE_SECRET_FILE = secret,
          DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
          DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
          DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
        ),
        stdout = "|", stderr = "|", cleanup = TRUE
      )
    })
    withr::defer(lapply(workers, function(worker) {
      if (worker$is_alive()) worker$kill_tree()
    }))
    lapply(workers, function(worker) worker$wait(timeout = 15000))
    expect_true(all(vapply(
      workers, function(worker) identical(worker$get_exit_status(), 0L),
      logical(1)
    )), info = paste(vapply(workers, function(worker) {
      paste(worker$read_all_error(), collapse = "\n")
    }, character(1)), collapse = "\n"))

    expect_identical(dsFlower:::.validate_node_secret(secret), secret)
    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT COUNT(*) AS n FROM privacy_key_epochs")$n, 1L)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT MAX(key_epoch) AS epoch FROM privacy_key_epochs")$epoch, 1L)
    expect_false(any(grepl(
      "^\\.node-secret-", list.files(dirname(secret), all.files = TRUE)
    )))
  })
})
