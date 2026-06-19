# Tests for R/policy.R -- disclosure controls.
# Differential privacy is always enforced; there are no trust profiles.
# All thresholds come from DataSHIELD options (nfilter.*) with dsflower.* overrides.

test_that("disclosure thresholds inherit DataSHIELD options", {
  withr::local_options(list(nfilter.tab = NULL, nfilter.subset = NULL,
                            dsflower.min_cell_count = NULL,
                            dsflower.min_train_rows = NULL))
  expect_equal(dsFlower:::.disclosure_min_cell(), 3L)
  expect_equal(dsFlower:::.disclosure_min_rows(), 3L)

  withr::local_options(list(nfilter.tab = 5, nfilter.subset = 10))
  expect_equal(dsFlower:::.disclosure_min_cell(), 5L)
  expect_equal(dsFlower:::.disclosure_min_rows(), 10L)
})

test_that("dsflower.* options can raise (never lower) the floors", {
  withr::local_options(list(nfilter.tab = 3, dsflower.min_cell_count = 8,
                            nfilter.subset = 3, dsflower.min_train_rows = 50))
  expect_equal(dsFlower:::.disclosure_min_cell(), 8L)
  expect_equal(dsFlower:::.disclosure_min_rows(), 50L)
})

test_that(".bucket_count suppresses small counts and buckets the rest", {
  withr::local_options(list(nfilter.subset = 3))
  expect_equal(dsFlower:::.bucket_count(0), 0L)
  # Small counts are never returned exactly larger than the input; large counts
  # stay positive. (Exact-zero suppression is the no-dsImaging fallback.)
  expect_lte(dsFlower:::.bucket_count(2), 2L)
  expect_gt(dsFlower:::.bucket_count(100), 0L)
})

test_that(".assertMinSamples blocks tiny datasets generically", {
  withr::local_options(list(nfilter.subset = 50))
  expect_error(dsFlower:::.assertMinSamples(10), "Disclosive")
  expect_true(dsFlower:::.assertMinSamples(100))
})

test_that(".validateClassDistribution enforces the small-cell rule", {
  withr::local_options(list(nfilter.tab = 5))
  # binary with a class below the cell floor -> blocked
  d <- data.frame(y = c(rep(1, 100), rep(0, 2)))
  expect_error(
    dsFlower:::.validateClassDistribution(d, "y", task_type = "classification"),
    "Disclosive"
  )
  # both classes above floor -> ok
  d2 <- data.frame(y = c(rep(1, 100), rep(0, 100)))
  expect_true(
    dsFlower:::.validateClassDistribution(d2, "y", task_type = "classification")
  )
  # regression target -> no class-count check
  d3 <- data.frame(y = rnorm(50))
  expect_true(
    dsFlower:::.validateClassDistribution(d3, "y", task_type = "regression")
  )
})

test_that(".validateMaxRounds honours the option ceiling", {
  withr::local_options(list(dsflower.max_rounds = 100))
  expect_equal(dsFlower:::.validateMaxRounds(50), 50)
  expect_error(dsFlower:::.validateMaxRounds(500))
})

test_that("trust-profile API has been removed", {
  ns <- asNamespace("dsFlower")
  expect_false(exists(".TRUST_PROFILES", where = ns, inherits = FALSE))
  expect_false(exists(".flowerTrustProfile", where = ns, inherits = FALSE))
  expect_false(exists(".validateTemplateProfile", where = ns, inherits = FALSE))
})
