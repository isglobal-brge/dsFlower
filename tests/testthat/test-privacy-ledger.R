# Tests for R/privacy_ledger.R — Privacy Accountant (RDP)

test_that(".rdp_gaussian computes correct RDP cost", {
  # For sigma=1, alpha=2: RDP = alpha / (2 * sigma^2) = 2/2 = 1.0
  expect_equal(dsFlower:::.rdp_gaussian(2, 1.0), 1.0)
  # For sigma=2, alpha=4: RDP = 4 / (2 * 4) = 0.5
  expect_equal(dsFlower:::.rdp_gaussian(4, 2.0), 0.5)
  # sigma=0 should return Inf
  expect_equal(dsFlower:::.rdp_gaussian(2, 0), Inf)
})

test_that(".rdp_to_dp converts correctly", {
  # Simple case: single order
  orders <- c(2)
  rdp <- c(1.0)
  delta <- 1e-5
  # epsilon = 1.0 + log(1/1e-5) / (2 - 1) = 1.0 + 11.5129... = 12.5129...
  result <- dsFlower:::.rdp_to_dp(rdp, orders, delta)
  expect_equal(result, 1.0 + log(1/delta), tolerance = 1e-6)
})

test_that(".compute_rdp_vector returns correct length", {
  orders <- dsFlower:::.RDP_ORDERS
  rdp <- dsFlower:::.compute_rdp_vector(1.0, 1e-5)
  expect_equal(length(rdp), length(orders))
  # All values should be positive finite
  expect_true(all(is.finite(rdp)))
  expect_true(all(rdp > 0))
})

test_that(".read_ledger returns empty list when no ledger exists", {
  withr::with_tempdir({
    ledger_path <- file.path(getwd(), "nonexistent_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )
    ledger <- dsFlower:::.read_ledger()
    expect_type(ledger, "list")
    expect_equal(length(ledger), 0)
  })
})

test_that(".record_spend creates RDP-based ledger entry", {
  withr::with_tempdir({
    ledger_path <- file.path(getwd(), "test_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )

    dsFlower:::.record_spend("ds:target", 1.0, 1e-5, "run_001")

    ledger <- dsFlower:::.read_ledger()
    entry <- ledger[["ds:target"]]
    expect_true(!is.null(entry))
    expect_true(!is.null(entry$rdp_vector))
    expect_true(!is.null(entry$epsilon_current))
    expect_equal(length(entry$runs), 1)
    expect_equal(entry$runs[[1]]$accountant, "RDP")
  })
})

test_that(".check_budget uses RDP composition for tighter bounds", {
  withr::with_tempdir({
    ledger_path <- file.path(getwd(), "test_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )

    # Record several runs
    for (i in 1:5) {
      dsFlower:::.record_spend("ds:y", 1.0, 1e-5, paste0("run_", i))
    }

    # RDP composition should give a tighter bound than 5*1.0 = 5.0
    budget <- dsFlower:::.get_budget("ds:y")
    expect_true(budget$epsilon_rdp < budget$spent_epsilon_additive)
    expect_equal(budget$spent_epsilon_additive, 5.0)
    expect_equal(budget$accountant, "RDP")
  })
})

test_that(".check_budget blocks when budget exhausted", {
  withr::with_tempdir({
    ledger_path <- file.path(getwd(), "test_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )

    # Use up most of the budget with high-epsilon runs
    for (i in 1:20) {
      dsFlower:::.record_spend("exhausted:y", 2.0, 1e-5, paste0("run_", i))
    }

    # Should be blocked
    expect_error(
      dsFlower:::.check_budget("exhausted:y", 2.0, 1e-5),
      "Privacy budget exhausted"
    )
  })
})

test_that(".get_budget returns correct structure", {
  withr::with_tempdir({
    ledger_path <- file.path(getwd(), "test_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )

    budget <- dsFlower:::.get_budget("fresh:y")
    expect_equal(budget$epsilon_rdp, 0)
    expect_equal(budget$n_runs, 0L)
    expect_equal(budget$accountant, "RDP")
    expect_equal(budget$max_epsilon, 10.0)
    expect_true("remaining_epsilon" %in% names(budget))
  })
})
