# Tests for R/privacy_ledger.R — Privacy Accountant

test_that(".read_ledger returns empty list when no ledger exists", {
  withr::with_tempdir({
    withr::with_options(list(dsflower.ledger_dir_override = getwd()), {
      # The ledger file won't exist in a fresh tempdir
      ledger <- dsFlower:::.read_ledger()
      expect_type(ledger, "list")
      expect_equal(length(ledger), 0)
    })
  })
})

test_that(".record_spend and .check_budget work together", {
  withr::with_tempdir({
    # Override ledger path for testing
    ledger_path <- file.path(getwd(), "test_ledger.json")
    local_mocked_bindings(
      .ledger_path = function() ledger_path,
      .package = "dsFlower"
    )

    # Record some spend
    dsFlower:::.record_spend("test_dataset:target", 1.0, 1e-5, "run_001")

    # Check budget passes
    expect_true(dsFlower:::.check_budget("test_dataset:target", 1.0, 1e-5))

    # Record more spend
    dsFlower:::.record_spend("test_dataset:target", 8.0, 5e-4, "run_002")

    # Budget should now be exhausted
    expect_error(
      dsFlower:::.check_budget("test_dataset:target", 2.0, 1e-5),
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

    budget <- dsFlower:::.get_budget("fresh_dataset:y")
    expect_equal(budget$spent_epsilon, 0)
    expect_equal(budget$spent_delta, 0)
    expect_equal(budget$n_runs, 0L)
    expect_equal(budget$max_epsilon, 10.0)

    dsFlower:::.record_spend("fresh_dataset:y", 2.5, 1e-5, "run_x")
    budget <- dsFlower:::.get_budget("fresh_dataset:y")
    expect_equal(budget$spent_epsilon, 2.5)
    expect_equal(budget$remaining_epsilon, 7.5)
    expect_equal(budget$n_runs, 1L)
  })
})
