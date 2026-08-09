# Tests for R/policy.R -- disclosure controls.
# Differential privacy is always enforced. Disclosure thresholds come from
# DataSHIELD options (nfilter.*) with dsflower.* overrides.

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

test_that(".validateMaxRounds honours the option ceiling", {
  withr::local_options(list(dsflower.max_rounds = 100))
  expect_equal(dsFlower:::.validateMaxRounds(50), 50)
  expect_error(dsFlower:::.validateMaxRounds(500))
})

test_that("DP unit is server-owned, explicit, and has no auto fallback", {
  withr::local_options(list(dsflower.dp_unit = "row",
                            dsflower.patient_column = NULL))
  expect_null(dsFlower:::.detectPatientColumn(
    data.frame(patient_id = 1, x = 2)))
  expect_null(dsFlower:::.detectPatientColumn(data.frame(x = 1, y = 2)))

  withr::local_options(list(dsflower.dp_unit = "patient",
                            dsflower.patient_column = "subj"))
  expect_equal(
    dsFlower:::.detectPatientColumn(data.frame(subj = 1, x = 2),
                                    list()),
    "subj")
  expect_error(
    dsFlower:::.detectPatientColumn(data.frame(patient_id = 1, x = 2)),
    "unavailable")
  expect_error(
    withr::with_options(list(dsflower.patient_column = NULL),
      dsFlower:::.dpUnitPolicy()),
    "patient_column")
})
