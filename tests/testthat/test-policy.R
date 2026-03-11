# Tests for R/policy.R — Disclosure Controls

test_that(".flowerDisclosureSettings returns all expected fields", {
  settings <- dsFlower:::.flowerDisclosureSettings()
  expect_type(settings, "list")
  expect_true("nfilter_subset" %in% names(settings))
  expect_true("max_rounds" %in% names(settings))
  expect_true("allow_custom_config" %in% names(settings))
  expect_true("allowed_templates" %in% names(settings))
  expect_true("allow_supernode_spawn" %in% names(settings))
  expect_true("max_concurrent_runs" %in% names(settings))
})

test_that(".flowerDisclosureSettings returns correct defaults", {
  settings <- dsFlower:::.flowerDisclosureSettings()
  expect_equal(settings$nfilter_subset, 3)
  expect_equal(settings$max_rounds, 500)
  expect_false(settings$allow_custom_config)
  expect_true(settings$allow_supernode_spawn)
  expect_equal(settings$max_concurrent_runs, 5L)
  expect_true("sklearn_logreg" %in% settings$allowed_templates)
  expect_true("pytorch_mlp" %in% settings$allowed_templates)
})

test_that(".flowerDisclosureSettings respects option overrides", {
  withr::with_options(list(dsflower.max_rounds = 100), {
    settings <- dsFlower:::.flowerDisclosureSettings()
    expect_equal(settings$max_rounds, 100)
  })
})

test_that(".assertMinSamples passes for sufficient samples", {
  expect_true(dsFlower:::.assertMinSamples(10))
  expect_true(dsFlower:::.assertMinSamples(3))
})

test_that(".assertMinSamples blocks insufficient samples", {
  expect_error(
    dsFlower:::.assertMinSamples(2),
    "Disclosive"
  )
  expect_error(
    dsFlower:::.assertMinSamples(0),
    "Disclosive"
  )
})

test_that(".sanitizeMetrics returns empty df for NULL input", {
  result <- dsFlower:::.sanitizeMetrics(NULL)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
})

test_that(".sanitizeMetrics filters to allowlisted metrics", {
  metrics <- data.frame(
    round = c(1L, 1L, 1L),
    metric = c("loss", "accuracy", "secret_gradient"),
    value = c(0.5, 0.8, 99.0),
    stringsAsFactors = FALSE
  )
  result <- dsFlower:::.sanitizeMetrics(metrics)
  expect_true("loss" %in% result$metric)
  expect_true("accuracy" %in% result$metric)
  expect_false("secret_gradient" %in% result$metric)
})

test_that(".sanitizeMetrics strips unsafe columns", {
  metrics <- data.frame(
    round = 1L, metric = "loss", value = 0.5,
    path = "/secret/path", ip_address = "10.0.0.1",
    stringsAsFactors = FALSE
  )
  result <- dsFlower:::.sanitizeMetrics(metrics)
  expect_false("path" %in% names(result))
  expect_false("ip_address" %in% names(result))
})

test_that(".validateMaxRounds allows valid rounds", {
  expect_equal(dsFlower:::.validateMaxRounds(10), 10L)
  expect_equal(dsFlower:::.validateMaxRounds(500), 500L)
})

test_that(".validateMaxRounds blocks excessive rounds", {
  expect_error(dsFlower:::.validateMaxRounds(501), "exceeds server maximum")
})

test_that(".validateMaxRounds rejects non-positive values", {
  expect_error(dsFlower:::.validateMaxRounds(0), "positive integer")
  expect_error(dsFlower:::.validateMaxRounds(-1), "positive integer")
})

test_that(".sanitizeLogs removes paths and IPs", {
  lines <- c(
    "Loading data from /home/user/data/train.csv",
    "Connected to 192.168.1.100:9092",
    "pid= 12345 started"
  )
  result <- dsFlower:::.sanitizeLogs(lines)
  expect_false(any(grepl("/home", result)))
  expect_false(any(grepl("192\\.168", result)))
  expect_false(any(grepl("12345", result)))
  expect_true(any(grepl("<path>", result)))
  expect_true(any(grepl("<ip>", result)))
  expect_true(any(grepl("pid=<pid>", result)))
})

test_that(".sanitizeLogs respects last_n", {
  lines <- paste("line", 1:100)
  result <- dsFlower:::.sanitizeLogs(lines, last_n = 10L)
  expect_equal(length(result), 10)
})

test_that(".sanitizeLogs caps at 200 lines", {
  lines <- paste("line", 1:300)
  result <- dsFlower:::.sanitizeLogs(lines, last_n = 300L)
  expect_equal(length(result), 200)
})

test_that(".validateTemplate allows built-in templates", {
  expect_true(dsFlower:::.validateTemplate("sklearn_logreg"))
  expect_true(dsFlower:::.validateTemplate("sklearn_ridge"))
  expect_true(dsFlower:::.validateTemplate("sklearn_sgd"))
  expect_true(dsFlower:::.validateTemplate("pytorch_mlp"))
})

test_that(".validateTemplate blocks unknown templates", {
  expect_error(
    dsFlower:::.validateTemplate("custom_evil_code"),
    "not in the approved list"
  )
})

test_that(".validateTemplate allows custom when enabled", {
  withr::with_options(list(dsflower.allow_custom_config = TRUE), {
    expect_true(dsFlower:::.validateTemplate("anything_goes"))
  })
})
