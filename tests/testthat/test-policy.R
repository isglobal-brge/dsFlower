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

test_that(".detectPatientColumn finds common identifiers + honours overrides", {
  expect_equal(
    dsFlower:::.detectPatientColumn(data.frame(patient_id = 1, x = 2)),
    "patient_id")
  expect_equal(
    dsFlower:::.detectPatientColumn(data.frame(SubjectID = 1, x = 2)),
    "SubjectID")  # case-insensitive
  expect_null(dsFlower:::.detectPatientColumn(data.frame(x = 1, y = 2)))
  # run_config override picks a non-standard column name
  expect_equal(
    dsFlower:::.detectPatientColumn(data.frame(subj = 1, x = 2),
                                    list(patient_column = "subj")),
    "subj")
})

test_that(".imageDisclosureUnits groups disclosure by distinct patient", {
  # 5 patients x 6 slices; patient-level label, 3 in class 0, 2 in class 1.
  pid <- rep(paste0("P", 1:5), each = 6)
  lab <- rep(c(0, 0, 0, 1, 1), each = 6)
  sm  <- data.frame(patient_id = pid, y = lab)
  grp <- dsFlower:::.imageDisclosureUnits(sm, "y")
  expect_equal(grp$n_patients, 5)                 # patients, not 30 images
  expect_equal(nrow(grp$data), 5)                 # one row per (patient,label)
  expect_equal(as.integer(table(grp$data$y)), c(3L, 2L))  # distinct patients/class
})

test_that(".imageDisclosureUnits excludes NA/empty patient ids (no count inflation)", {
  # class 1 has ONE real patient (P5) + 3 NA-patient slices; the NA rows must not
  # inflate the per-class count (regression for the dedup-key NA leak).
  sm <- data.frame(
    patient_id = c(rep("P1", 4), rep("P2", 4), rep("P3", 4), rep("P4", 4),
                   "P5", NA, NA, NA),
    y = c(rep(0, 16), 1, 1, 1, 1))
  grp <- dsFlower:::.imageDisclosureUnits(sm, "y")
  expect_equal(grp$n_patients, 5)                 # NA is not a patient
  expect_equal(as.integer(table(grp$data$y)), c(4L, 1L))  # class 1 == 1, not 4
})

test_that(".imageDisclosureUnits returns NULL when no patient column (per-image)", {
  expect_null(dsFlower:::.imageDisclosureUnits(
    data.frame(sample_id = 1:3, y = c(0, 1, 0)), "y"))
})
