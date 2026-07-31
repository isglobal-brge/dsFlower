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

test_that("ephemeral node-secret paths require the explicit test escape hatch", {
  secret <- file.path(tempdir(), "dsflower-ephemeral-secret")
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = secret,
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = ""
  ))
  expect_error(dsFlower:::.node_secret_path(), "must be persistent")
})
