# Extracted from test-policy.R:163

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "dsFlower", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
withr::with_options(list(dsflower.privacy_profile = "secure"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "secure")
    expect_equal(profile$min_train_rows, 100)
    expect_false(profile$allow_per_node_metrics)
    expect_false(profile$allow_exact_num_examples)
    expect_true(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
    expect_equal(profile$model_release, "gated")
  })
