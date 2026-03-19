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
  expect_equal(length(settings$allowed_templates), 15)
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
  expect_true(dsFlower:::.validateTemplate("pytorch_logreg"))
  expect_true(dsFlower:::.validateTemplate("pytorch_linear_regression"))
  expect_true(dsFlower:::.validateTemplate("pytorch_coxph"))
  expect_true(dsFlower:::.validateTemplate("pytorch_multiclass"))
  expect_true(dsFlower:::.validateTemplate("xgboost_tabular"))
  expect_true(dsFlower:::.validateTemplate("pytorch_resnet18"))
  expect_true(dsFlower:::.validateTemplate("pytorch_densenet121"))
  expect_true(dsFlower:::.validateTemplate("pytorch_unet2d"))
  expect_true(dsFlower:::.validateTemplate("pytorch_tcn"))
  expect_true(dsFlower:::.validateTemplate("pytorch_lstm"))
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

# --- Trust Profile tests ---

test_that(".flowerTrustProfile defaults to secure (not research)", {
  withr::with_options(list(dsflower.privacy_profile = NULL), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "secure")
    expect_equal(profile$min_train_rows, 100)
    expect_false(profile$allow_per_node_metrics)
    expect_false(profile$allow_exact_num_examples)
    expect_true(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
  })
})

test_that(".flowerTrustProfile permanently blocks research profile", {
  withr::with_options(list(dsflower.privacy_profile = "research"), {
    expect_error(dsFlower:::.flowerTrustProfile(),
                 "research.*not available")
  })
})

test_that(".flowerTrustProfile returns secure profile", {
  withr::with_options(list(dsflower.privacy_profile = "secure"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "secure")
    expect_equal(profile$min_train_rows, 100)
    expect_false(profile$allow_per_node_metrics)
    expect_false(profile$allow_exact_num_examples)
    expect_true(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
    expect_equal(profile$model_release, "advisory_gated")
  })
})

test_that(".flowerTrustProfile returns secure_dp profile", {
  withr::with_options(list(dsflower.privacy_profile = "secure_dp"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "secure_dp")
    expect_equal(profile$min_train_rows, 200)
    expect_true(profile$dp_required)
  })
})

test_that(".flowerTrustProfile errors on unknown profile", {
  withr::with_options(list(dsflower.privacy_profile = "invalid"), {
    expect_error(dsFlower:::.flowerTrustProfile(), "Unknown privacy profile")
  })
})

test_that(".flowerTrustProfile overrides can only strengthen secure profile", {
  withr::with_options(list(
    dsflower.privacy_profile = "secure",
    dsflower.min_train_rows = 500
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    # min_train_rows should be max(100, 500) = 500
    expect_equal(profile$min_train_rows, 500)
    # Other secure defaults unchanged
    expect_false(profile$allow_per_node_metrics)
    expect_true(profile$require_secure_aggregation)
  })
})

test_that(".flowerTrustProfile overrides cannot weaken secure profile", {
  withr::with_options(list(
    dsflower.privacy_profile = "secure",
    dsflower.min_train_rows = 10
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    # min_train_rows should stay 100 (profile floor is higher)
    expect_equal(profile$min_train_rows, 100)
  })
})

test_that(".bucket_count returns correct values", {
  expect_equal(dsFlower:::.bucket_count(0), 0L)
  expect_equal(dsFlower:::.bucket_count(1), 1L)
  expect_equal(dsFlower:::.bucket_count(3), 3L)
  expect_equal(dsFlower:::.bucket_count(100), 128L)
  expect_equal(dsFlower:::.bucket_count(1000), 1024L)
  expect_equal(dsFlower:::.bucket_count(50), 64L)
})

# --- Template Capability Matrix tests ---

test_that(".TEMPLATE_CAPABILITIES has entries for all built-in templates", {
  caps <- dsFlower:::.TEMPLATE_CAPABILITIES
  expected <- c("sklearn_logreg", "sklearn_ridge", "sklearn_sgd",
                "pytorch_mlp", "pytorch_logreg", "pytorch_linear_regression",
                "pytorch_coxph", "pytorch_multiclass", "xgboost_tabular",
                "pytorch_resnet18", "pytorch_densenet121", "pytorch_unet2d",
                "pytorch_tcn", "pytorch_lstm")
  for (tmpl in expected) {
    expect_true(tmpl %in% names(caps), label = paste(tmpl, "in capabilities"))
  }
  expect_equal(length(caps), 15)
})

test_that("sklearn templates do not support secure_dp", {
  for (tmpl in c("sklearn_logreg", "sklearn_ridge", "sklearn_sgd")) {
    caps <- dsFlower:::.TEMPLATE_CAPABILITIES[[tmpl]]
    expect_false(caps$supports_secure_dp,
                 label = paste(tmpl, "should not support secure_dp"))
    expect_true(caps$supports_secure,
                label = paste(tmpl, "should support secure"))
  }
})

test_that("pytorch_mlp supports secure_dp", {
  caps <- dsFlower:::.TEMPLATE_CAPABILITIES[["pytorch_mlp"]]
  expect_true(caps$supports_secure_dp)
  expect_true(caps$supports_secure)
  expect_equal(caps$min_rows_secure_dp, 500)
})

test_that("new pytorch tabular templates support secure_dp", {
  for (tmpl in c("pytorch_logreg", "pytorch_linear_regression",
                 "pytorch_coxph", "pytorch_multiclass",
                 "pytorch_tcn", "pytorch_lstm")) {
    caps <- dsFlower:::.TEMPLATE_CAPABILITIES[[tmpl]]
    expect_true(caps$supports_secure_dp,
                label = paste(tmpl, "should support secure_dp"))
    expect_true(caps$supports_secure,
                label = paste(tmpl, "should support secure"))
  }
})

test_that("image templates support secure but not secure_dp", {
  for (tmpl in c("pytorch_resnet18", "pytorch_densenet121", "pytorch_unet2d")) {
    caps <- dsFlower:::.TEMPLATE_CAPABILITIES[[tmpl]]
    expect_false(caps$supports_secure_dp,
                 label = paste(tmpl, "should not support secure_dp"))
    expect_true(caps$supports_secure,
                label = paste(tmpl, "should support secure"))
  }
})

test_that("xgboost does not support secure or secure_dp", {
  caps <- dsFlower:::.TEMPLATE_CAPABILITIES[["xgboost_tabular"]]
  expect_false(caps$supports_secure,
               label = "xgboost should not support secure (bagging exposes trees)")
  expect_false(caps$supports_secure_dp,
               label = "xgboost should not support secure_dp")
})

test_that(".validateTemplateProfile rejects sklearn in secure_dp", {
  expect_error(
    dsFlower:::.validateTemplateProfile("sklearn_logreg", "secure_dp"),
    "does not support.*secure_dp"
  )
  expect_error(
    dsFlower:::.validateTemplateProfile("sklearn_ridge", "secure_dp"),
    "does not support.*secure_dp"
  )
  expect_error(
    dsFlower:::.validateTemplateProfile("sklearn_sgd", "secure_dp"),
    "does not support.*secure_dp"
  )
})

test_that(".validateTemplateProfile allows pytorch_mlp in secure_dp", {
  expect_true(dsFlower:::.validateTemplateProfile("pytorch_mlp", "secure_dp"))
})

test_that(".validateTemplateProfile rejects non-DP templates in secure_dp", {
  for (tmpl in c("pytorch_resnet18", "pytorch_densenet121", "pytorch_unet2d")) {
    expect_error(
      dsFlower:::.validateTemplateProfile(tmpl, "secure_dp"),
      "does not support.*secure_dp",
      label = paste(tmpl, "should be rejected in secure_dp")
    )
  }
})

test_that(".validateTemplateProfile rejects xgboost in secure", {
  expect_error(
    dsFlower:::.validateTemplateProfile("xgboost_tabular", "secure"),
    "does not support.*secure"
  )
})

test_that(".validateTemplateProfile rejects xgboost in secure_dp", {
  expect_error(
    dsFlower:::.validateTemplateProfile("xgboost_tabular", "secure_dp"),
    "does not support.*secure_dp"
  )
})

test_that(".validateTemplateProfile allows new DP templates in secure_dp", {
  for (tmpl in c("pytorch_logreg", "pytorch_linear_regression",
                 "pytorch_coxph", "pytorch_multiclass",
                 "pytorch_tcn", "pytorch_lstm")) {
    expect_true(dsFlower:::.validateTemplateProfile(tmpl, "secure_dp"),
                label = paste(tmpl, "should be allowed in secure_dp"))
  }
})

test_that(".validateTemplateProfile allows all templates in research", {
  all_templates <- names(dsFlower:::.TEMPLATE_CAPABILITIES)
  for (tmpl in all_templates) {
    expect_true(dsFlower:::.validateTemplateProfile(tmpl, "research"),
                label = paste(tmpl, "should be allowed in research"))
  }
})

test_that(".validateTemplateProfile rejects unknown template in secure_dp", {
  expect_error(
    dsFlower:::.validateTemplateProfile("unknown_model", "secure_dp"),
    "no registered capability"
  )
})

test_that(".templateMinRows returns per-template minimums", {
  expect_equal(dsFlower:::.templateMinRows("pytorch_mlp", "secure_dp"), 500)
  expect_equal(dsFlower:::.templateMinRows("sklearn_sgd", "secure"), 200)
  expect_equal(dsFlower:::.templateMinRows("pytorch_logreg", "secure_dp"), 200)
  expect_equal(dsFlower:::.templateMinRows("pytorch_coxph", "secure_dp"), 500)
  expect_equal(dsFlower:::.templateMinRows("pytorch_resnet18", "secure"), 1000)
  expect_equal(dsFlower:::.templateMinRows("pytorch_unet2d", "secure"), 500)
  expect_null(dsFlower:::.templateMinRows("unknown_template", "secure"))
})

test_that("model_release is advisory_gated not gated in secure profiles", {
  for (profile in c("secure", "secure_dp")) {
    p <- dsFlower:::.TRUST_PROFILES[[profile]]
    expect_equal(p$model_release, "advisory_gated",
                 label = paste(profile, "model_release"))
  }
})
