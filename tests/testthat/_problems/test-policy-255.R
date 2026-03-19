# Extracted from test-policy.R:255

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "dsFlower", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
caps <- dsFlower:::.TEMPLATE_CAPABILITIES
expected <- c("sklearn_logreg", "sklearn_ridge", "sklearn_sgd",
                "pytorch_mlp", "pytorch_logreg", "pytorch_linear_regression",
                "pytorch_coxph", "pytorch_multiclass", "xgboost_tabular",
                "pytorch_resnet18", "pytorch_densenet121", "pytorch_unet2d",
                "pytorch_tcn", "pytorch_lstm")
for (tmpl in expected) {
    expect_true(tmpl %in% names(caps), label = paste(tmpl, "in capabilities"))
  }
expect_equal(length(caps), 14)
