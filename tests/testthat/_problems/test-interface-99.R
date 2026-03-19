# Extracted from test-interface.R:99

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "dsFlower", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
csv_path <- create_test_csv(n = 200)
on.exit(unlink(csv_path))
handle <- mock_handle(data_path = csv_path, data_format = "csv")
dsFlower:::.setHandle("test_prepare", handle)
on.exit(dsFlower:::.removeHandle("test_prepare"), add = TRUE)
result <- flowerPrepareRunDS("test_prepare", "target", c("f1", "f2", "f3"))
expect_true(result$prepared)
expect_equal(result$target_column, "target")
expect_equal(result$feature_columns, c("f1", "f2", "f3"))
expect_true(!is.null(result$run_token))
expect_true(!is.null(result$staging_dir))
expect_true(dir.exists(result$staging_dir))
manifest_path <- file.path(result$staging_dir, "manifest.json")
expect_true(file.exists(manifest_path))
manifest <- jsonlite::fromJSON(manifest_path)
expect_equal(manifest$n_samples, 50)
