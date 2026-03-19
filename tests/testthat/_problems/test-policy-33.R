# Extracted from test-policy.R:33

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "dsFlower", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
settings <- dsFlower:::.flowerDisclosureSettings()
expect_equal(settings$nfilter_subset, 3)
expect_equal(settings$max_rounds, 500)
expect_false(settings$allow_custom_config)
expect_true(settings$allow_supernode_spawn)
expect_equal(settings$max_concurrent_runs, 5L)
expect_true("sklearn_logreg" %in% settings$allowed_templates)
expect_true("pytorch_mlp" %in% settings$allowed_templates)
expect_true("pytorch_logreg" %in% settings$allowed_templates)
expect_true("pytorch_linear_regression" %in% settings$allowed_templates)
expect_true("pytorch_coxph" %in% settings$allowed_templates)
expect_true("pytorch_multiclass" %in% settings$allowed_templates)
expect_true("xgboost_tabular" %in% settings$allowed_templates)
expect_true("pytorch_resnet18" %in% settings$allowed_templates)
expect_true("pytorch_densenet121" %in% settings$allowed_templates)
expect_true("pytorch_unet2d" %in% settings$allowed_templates)
expect_true("pytorch_tcn" %in% settings$allowed_templates)
expect_true("pytorch_lstm" %in% settings$allowed_templates)
expect_equal(length(settings$allowed_templates), 14)
