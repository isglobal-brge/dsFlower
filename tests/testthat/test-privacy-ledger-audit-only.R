# Tests for explicit, non-blocking per-training privacy accounting.

local_audit_privacy_ledger <- function(
    path = tempfile(fileext = ".sqlite"),
    epsilon = 0.75,
    delta = 2e-6,
    .local_envir = parent.frame()) {
  withr::local_options(list(
    dsflower.privacy_ledger_path = path,
    dsflower.dp_accounting_mode = "per-release-audit",
    dsflower.dp_per_training_epsilon = epsilon,
    dsflower.dp_per_training_delta = delta,
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

test_that("audit-only mode canonicalizes and validates its public policy", {
  local_audit_privacy_ledger(epsilon = "0.75", delta = "2e-6")
  withr::local_options(list(
    dsflower.dp_accounting_mode = "  Per-Release-Audit  "))
  canonical <- dsFlower:::.privacy_policy()
  expect_identical(canonical$accounting_mode, "per-release-audit")
  expect_identical(canonical$per_training_epsilon, 0.75)
  expect_identical(canonical$per_training_delta, 2e-6)

  numeric_policy <- withr::with_options(list(
    dsflower.dp_accounting_mode = "per-release-audit",
    dsflower.dp_per_training_epsilon = 0.75,
    dsflower.dp_per_training_delta = 2e-6),
    dsFlower:::.privacy_policy())
  expect_identical(canonical$policy_hash, numeric_policy$policy_hash)

  expect_error(withr::with_options(list(
    dsflower.dp_accounting_mode = "unlimited"),
    dsFlower:::.privacy_policy()), "must be exactly")
  expect_error(withr::with_options(list(
    dsflower.dp_accounting_mode = c("per-release-audit", "other")),
    dsFlower:::.privacy_policy()), "one scalar string")
  expect_error(withr::with_options(list(
    dsflower.dp_per_training_epsilon = NULL,
    default.dsflower.dp_per_training_epsilon = NULL),
    dsFlower:::.privacy_policy()), "must be explicitly configured")
  expect_error(withr::with_options(list(
    dsflower.dp_per_training_delta = Inf),
    dsFlower:::.privacy_policy()), "must be explicitly configured")
})

test_that("invalid audit-only calibration fails before ledger creation", {
  ledger <- local_audit_privacy_ledger(epsilon = 1e-6, delta = 1e-12)
  token <- paste0("run_", strrep("a", 32))

  expect_error(
    dsFlower:::.reserve_privacy_run(token, 2L),
    "below the numerical minimum"
  )
  expect_false(file.exists(ledger))
})

test_that("audit-only accounting permits over 100 claimed trainings", {
  epsilon <- 0.75
  delta <- 2e-6
  ledger <- local_audit_privacy_ledger(epsilon = epsilon, delta = delta)
  tokens <- sprintf("run_%032x", seq_len(101L))

  allocations <- lapply(tokens, function(token) {
    dsFlower:::.reserve_privacy_run(token, 1L)
  })
  expect_true(all(vapply(
    allocations, `[[`, logical(1), "release_enabled")))
  expect_true(all(vapply(
    allocations, `[[`, character(1), "accounting_mode") ==
      "per-release-audit"))
  expect_true(all(vapply(
    allocations, `[[`, numeric(1), "epsilon") == epsilon))
  expect_true(all(vapply(
    allocations, `[[`, numeric(1), "delta") == delta))

  # Materialize the same claimed-release rows used by the production guard.
  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  DBI::dbExecute(con, "PRAGMA foreign_keys = ON")
  DBI::dbBegin(con)
  for (i in seq_along(tokens)) {
    changed <- DBI::dbExecute(
      con,
      paste(
        "UPDATE privacy_reservations",
        "SET claimed_releases = claimed_releases + 1",
        "WHERE run_token = ? AND claimed_releases = 0",
        "AND claimed_releases < max_releases"),
      params = list(tokens[[i]]))
    expect_identical(changed, 1L)
    DBI::dbExecute(
      con,
      paste(
        "INSERT INTO privacy_release_claims",
        "(run_token,message_id,release_index,created_at)",
        "VALUES (?,?,1,?)"),
      params = list(tokens[[i]], paste0("message-", i),
                    "2026-01-01T00:00:00Z"))
  }
  DBI::dbCommit(con)
  before_replay <- DBI::dbGetQuery(con, paste(
    "SELECT",
    "(SELECT COUNT(*) FROM privacy_reservations) AS reservations,",
    "(SELECT COUNT(*) FROM privacy_release_claims) AS claims,",
    "(SELECT next_index FROM privacy_policy WHERE domain='node') AS next_index"))
  DBI::dbDisconnect(con)

  replay <- dsFlower:::.reserve_privacy_run(tokens[[1L]], 1L)
  expect_true(replay$idempotent)
  expect_true(replay$release_enabled)
  expect_identical(replay$epsilon, epsilon)
  expect_identical(replay$delta, delta)

  status <- dsFlower:::.privacy_budget_status()
  expect_identical(status$accountant, "audit-only-basic-composition-v1")
  expect_identical(status$accounting_mode, "per-release-audit")
  expect_false(status$lifetime_bound)
  expect_true(status$nonblocking)
  expect_identical(status$guarantee_scope, "per-training-release")
  expect_equal(status$allocations, 101)
  expect_equal(status$claimed_releases, 101)
  expect_equal(status$allocated_epsilon, 101 * epsilon)
  expect_equal(status$allocated_delta, 101 * delta)
  expect_true(is.na(status$remaining_epsilon))
  expect_true(is.na(status$remaining_delta))
  expect_match(status$composition_statement, "do not have a finite lifetime")

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  after_replay <- DBI::dbGetQuery(con, paste(
    "SELECT",
    "(SELECT COUNT(*) FROM privacy_reservations) AS reservations,",
    "(SELECT COUNT(*) FROM privacy_release_claims) AS claims,",
    "(SELECT next_index FROM privacy_policy WHERE domain='node') AS next_index"))
  expect_equal(after_replay, before_replay)
})

test_that("audit-only policy drift cannot reinterpret prior reservations", {
  local_audit_privacy_ledger()
  first <- paste0("run_", strrep("a", 32))
  second <- paste0("run_", strrep("b", 32))
  dsFlower:::.reserve_privacy_run(first, 1L)

  withr::with_options(list(dsflower.dp_per_training_epsilon = 0.5), {
    expect_error(
      dsFlower:::.reserve_privacy_run(second, 1L),
      "privacy policy differs"
    )
  })
  withr::with_options(list(dsflower.dp_accounting_mode = "lifetime-geometric"), {
    expect_error(
      dsFlower:::.reserve_privacy_run(second, 1L),
      "privacy policy differs"
    )
  })
})

test_that("public budget metadata distinguishes audit-only from lifetime DP", {
  local_audit_privacy_ledger()
  public <- flowerPrivacyBudgetDS()
  expect_identical(public$accountant, "audit-only-basic-composition-v1")
  expect_identical(public$accounting_mode, "per-release-audit")
  expect_false(public$lifetime_bound)
  expect_true(public$release_availability_unbounded)
  expect_identical(public$guarantee_scope, "per-training-release")
  expect_identical(public$per_training_epsilon, 0.75)
  expect_identical(public$per_training_delta, 2e-6)
  expect_match(public$composition_statement, "no finite lifetime")

  withr::local_options(list(
    dsflower.dp_accounting_mode = "lifetime-geometric"))
  lifetime <- flowerPrivacyBudgetDS()
  expect_identical(
    lifetime$accountant, "bounded-geometric-basic-composition-v2")
  expect_true(lifetime$lifetime_bound)
  expect_false(lifetime$release_availability_unbounded)
})
