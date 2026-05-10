# Tests for R/policy.R -- Disclosure Controls

# --- Disclosure Settings ---

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
  expect_true("xgboost" %in% settings$allowed_templates)
  expect_true("pytorch_resnet18" %in% settings$allowed_templates)
  expect_true("pytorch_densenet121" %in% settings$allowed_templates)
  expect_true("pytorch_unet2d" %in% settings$allowed_templates)
  expect_true("pytorch_tcn" %in% settings$allowed_templates)
  expect_true("pytorch_lstm" %in% settings$allowed_templates)
  expect_equal(length(settings$allowed_templates),
               length(dsFlower:::.TEMPLATE_METADATA))
})

test_that(".flowerDisclosureSettings respects option overrides", {
  withr::with_options(list(dsflower.max_rounds = 100), {
    settings <- dsFlower:::.flowerDisclosureSettings()
    expect_equal(settings$max_rounds, 100)
  })
})

# --- Assert Min Samples ---

test_that(".assertMinSamples passes for sufficient samples", {
  expect_true(dsFlower:::.assertMinSamples(10))
  expect_true(dsFlower:::.assertMinSamples(3))
})

test_that(".assertMinSamples blocks insufficient samples", {
  expect_error(dsFlower:::.assertMinSamples(2), "Disclosive")
  expect_error(dsFlower:::.assertMinSamples(0), "Disclosive")
})

# --- Sanitize Metrics ---

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

# --- Validate Max Rounds ---

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

# --- Sanitize Logs ---

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

# --- Validate Template ---

test_that(".validateTemplate allows built-in templates", {
  expect_true(dsFlower:::.validateTemplate("sklearn_logreg"))
  expect_true(dsFlower:::.validateTemplate("sklearn_ridge"))
  expect_true(dsFlower:::.validateTemplate("sklearn_sgd"))
  expect_true(dsFlower:::.validateTemplate("pytorch_mlp"))
  expect_true(dsFlower:::.validateTemplate("pytorch_logreg"))
  expect_true(dsFlower:::.validateTemplate("pytorch_linear_regression"))
  expect_true(dsFlower:::.validateTemplate("pytorch_coxph"))
  expect_true(dsFlower:::.validateTemplate("pytorch_multiclass"))
  expect_true(dsFlower:::.validateTemplate("xgboost"))
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

# --- Bucket Count ---

test_that(".bucket_count returns correct values", {
  expect_equal(dsFlower:::.bucket_count(0), 0L)
  expect_equal(dsFlower:::.bucket_count(1), 1L)
  expect_equal(dsFlower:::.bucket_count(3), 3L)
  expect_equal(dsFlower:::.bucket_count(100), 128L)
  expect_equal(dsFlower:::.bucket_count(1000), 1024L)
  expect_equal(dsFlower:::.bucket_count(50), 64L)
})

# --- Profile Order ---

test_that(".PROFILE_ORDER has 7 entries matching .TRUST_PROFILES", {
  expect_equal(length(dsFlower:::.PROFILE_ORDER), 7)
  expect_equal(sort(dsFlower:::.PROFILE_ORDER),
               sort(names(dsFlower:::.TRUST_PROFILES)))
})

# --- Trust Profile: All 7 Profiles ---

test_that(".flowerTrustProfile defaults to clinical_default", {
  withr::with_options(list(dsflower.privacy_profile = NULL), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "clinical_default")
    expect_equal(profile$min_train_rows, 100)
    expect_false(profile$allow_per_node_metrics)
    expect_false(profile$allow_exact_num_examples)
    expect_true(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
    expect_equal(profile$model_release, "advisory_gated")
    expect_equal(profile$min_clients_per_round, 3L)
    expect_true(profile$fixed_client_sampling)
    expect_equal(profile$dp_scope, "none")
    expect_equal(profile$min_positive_examples, 20L)
    expect_equal(profile$min_per_class, 20L)
    expect_equal(profile$min_events, 20L)
    expect_false(profile$evaluation_only)
  })
})

test_that(".flowerTrustProfile blocks sandbox_open without admin opt-in", {
  withr::with_options(list(dsflower.privacy_profile = "sandbox_open"), {
    expect_error(dsFlower:::.flowerTrustProfile(),
                 "sandbox_open.*requires explicit admin opt-in")
  })
})

test_that(".flowerTrustProfile allows sandbox_open with admin opt-in", {
  withr::with_options(list(
    dsflower.privacy_profile = "sandbox_open",
    dsflower.allow_sandbox = TRUE
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "sandbox_open")
    expect_equal(profile$min_train_rows, 3)
    expect_true(profile$allow_per_node_metrics)
    expect_true(profile$allow_exact_num_examples)
    expect_false(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
    expect_equal(profile$model_release, "allowed")
    expect_equal(profile$min_clients_per_round, 1L)
    expect_false(profile$fixed_client_sampling)
    expect_equal(profile$dp_scope, "none")
    expect_equal(profile$min_positive_examples, 0L)
  })
})

test_that(".flowerTrustProfile returns trusted_internal", {
  withr::with_options(list(dsflower.privacy_profile = "trusted_internal"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "trusted_internal")
    expect_equal(profile$min_train_rows, 50)
    expect_true(profile$allow_per_node_metrics)
    expect_false(profile$allow_exact_num_examples)
    expect_false(profile$require_secure_aggregation)
    expect_equal(profile$min_clients_per_round, 1L)
    expect_equal(profile$min_positive_examples, 5L)
    expect_equal(profile$min_per_class, 5L)
    expect_equal(profile$min_events, 5L)
  })
})

test_that(".flowerTrustProfile returns consortium_internal", {
  withr::with_options(list(dsflower.privacy_profile = "consortium_internal"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "consortium_internal")
    expect_equal(profile$min_train_rows, 50)
    expect_false(profile$allow_per_node_metrics)
    expect_true(profile$require_secure_aggregation)
    expect_equal(profile$min_clients_per_round, 3L)
    expect_true(profile$fixed_client_sampling)
    expect_equal(profile$min_positive_examples, 10L)
  })
})

test_that(".flowerTrustProfile returns clinical_hardened", {
  withr::with_options(list(dsflower.privacy_profile = "clinical_hardened"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "clinical_hardened")
    expect_equal(profile$min_train_rows, 200)
    expect_true(profile$require_secure_aggregation)
    expect_false(profile$dp_required)
    expect_equal(profile$min_clients_per_round, 3L)
    expect_equal(profile$min_positive_examples, 30L)
  })
})

test_that(".flowerTrustProfile returns clinical_update_noise", {
  withr::with_options(list(dsflower.privacy_profile = "clinical_update_noise"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "clinical_update_noise")
    expect_equal(profile$min_train_rows, 200)
    expect_true(profile$require_secure_aggregation)
    expect_true(profile$dp_required)
    expect_equal(profile$dp_scope, "update_noise_only")
    expect_equal(profile$min_clients_per_round, 3L)
  })
})

test_that(".flowerTrustProfile returns high_sensitivity_dp", {
  withr::with_options(list(dsflower.privacy_profile = "high_sensitivity_dp"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$name, "high_sensitivity_dp")
    expect_equal(profile$min_train_rows, 500)
    expect_true(profile$require_secure_aggregation)
    expect_true(profile$dp_required)
    expect_equal(profile$dp_scope, "patient_level_dp_sgd")
    expect_equal(profile$min_clients_per_round, 3L)
    expect_equal(profile$min_positive_examples, 50L)
  })
})

test_that(".flowerTrustProfile errors on unknown profile", {
  withr::with_options(list(dsflower.privacy_profile = "invalid"), {
    expect_error(dsFlower:::.flowerTrustProfile(), "Unknown privacy profile")
  })
})

# --- Authoritative Override System ---

test_that("overrides can raise values (authoritative)", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.min_train_rows = 500
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$min_train_rows, 500)
  })
})

test_that("overrides can lower values (authoritative)", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.min_train_rows = 10
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    # Authoritative override sets to 10, but absolute floor is 3
    expect_equal(profile$min_train_rows, 10)
  })
})

test_that("overrides can enable per_node_metrics on clinical_default", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.allow_per_node_metrics = TRUE
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_true(profile$allow_per_node_metrics)
  })
})

test_that("overrides can disable SecAgg on clinical_default", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.require_secure_aggregation = FALSE
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_false(profile$require_secure_aggregation)
  })
})

test_that("absolute floor: min_train_rows never below 3", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.min_train_rows = 1
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$min_train_rows, 3)
  })
})

test_that("absolute floor: SecAgg profiles require at least 3 clients", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.min_clients_per_round = 0
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_equal(profile$min_clients_per_round, 3L)
  })
})

# --- DP-Locked Profile Protections ---

test_that("cannot disable SecAgg on clinical_update_noise (DP-locked)", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_update_noise",
    dsflower.require_secure_aggregation = FALSE
  ), {
    expect_warning(
      profile <- dsFlower:::.flowerTrustProfile(),
      "Cannot disable secure aggregation"
    )
    expect_true(profile$require_secure_aggregation)
  })
})

test_that("cannot disable DP on clinical_update_noise (DP-locked)", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_update_noise",
    dsflower.dp_required = FALSE
  ), {
    expect_warning(
      profile <- dsFlower:::.flowerTrustProfile(),
      "Cannot disable DP"
    )
    expect_true(profile$dp_required)
  })
})

test_that("cannot disable SecAgg on high_sensitivity_dp (DP-locked)", {
  withr::with_options(list(
    dsflower.privacy_profile = "high_sensitivity_dp",
    dsflower.require_secure_aggregation = FALSE
  ), {
    expect_warning(
      profile <- dsFlower:::.flowerTrustProfile(),
      "Cannot disable secure aggregation"
    )
    expect_true(profile$require_secure_aggregation)
  })
})

test_that("cannot disable DP on high_sensitivity_dp (DP-locked)", {
  withr::with_options(list(
    dsflower.privacy_profile = "high_sensitivity_dp",
    dsflower.dp_required = FALSE
  ), {
    expect_warning(
      profile <- dsFlower:::.flowerTrustProfile(),
      "Cannot disable DP"
    )
    expect_true(profile$dp_required)
  })
})

# --- Evaluation Only Modifier ---

test_that("evaluation_only forces model_release=blocked and metrics=FALSE", {
  withr::with_options(list(
    dsflower.privacy_profile = "clinical_default",
    dsflower.evaluation_only = TRUE
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_true(profile$evaluation_only)
    expect_equal(profile$model_release, "blocked")
    expect_false(profile$allow_per_node_metrics)
  })
})

test_that("evaluation_only defaults to FALSE", {
  withr::with_options(list(dsflower.privacy_profile = "clinical_default"), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_false(profile$evaluation_only)
  })
})

# --- All override fields ---

test_that("all overrideable fields work", {
  withr::with_options(list(
    dsflower.privacy_profile = "trusted_internal",
    dsflower.fixed_client_sampling = TRUE,
    dsflower.dp_scope = "update_noise_only",
    dsflower.min_positive_examples = 99,
    dsflower.min_per_class = 88,
    dsflower.min_events = 77
  ), {
    profile <- dsFlower:::.flowerTrustProfile()
    expect_true(profile$fixed_client_sampling)
    expect_equal(profile$dp_scope, "update_noise_only")
    expect_equal(profile$min_positive_examples, 99L)
    expect_equal(profile$min_per_class, 88L)
    expect_equal(profile$min_events, 77L)
  })
})

# --- Template Family Matrix ---

test_that(".TEMPLATE_FAMILIES has entries for all built-in templates", {
  families <- dsFlower:::.TEMPLATE_FAMILIES
  expected <- names(dsFlower:::.TEMPLATE_METADATA)
  for (tmpl in expected) {
    expect_true(tmpl %in% names(families), label = paste(tmpl, "in families"))
  }
  expect_equal(length(families), length(expected))
})

test_that(".FAMILY_MIN_ROWS has correct families", {
  fmr <- dsFlower:::.FAMILY_MIN_ROWS
  expect_true("closed_form_linear" %in% names(fmr))
  expect_true("iterative_linear" %in% names(fmr))
  expect_true("tabular_deep" %in% names(fmr))
  expect_true("vision" %in% names(fmr))
  expect_true("segmentation" %in% names(fmr))
  expect_true("xgboost_secure" %in% names(fmr))
  expect_equal(length(fmr), 6)
  # Each vector has 7 entries (one per profile)
  for (nm in names(fmr)) {
    expect_equal(length(fmr[[nm]]), 7, label = paste(nm, "length"))
  }
})

test_that("closed_form_linear has NA for DP profiles", {
  fmr <- dsFlower:::.FAMILY_MIN_ROWS[["closed_form_linear"]]
  # clinical_update_noise is index 6, high_sensitivity_dp is index 7
  expect_true(is.na(fmr[6]))
  expect_true(is.na(fmr[7]))
})

test_that("xgboost_secure has NA for DP profiles", {
  fmr <- dsFlower:::.FAMILY_MIN_ROWS[["xgboost_secure"]]
  expect_true(is.na(fmr[1]))  # sandbox
  expect_true(is.na(fmr[6]))  # clinical_update_noise
  expect_true(is.na(fmr[7]))  # high_sensitivity_dp
  expect_equal(fmr[2], 100L)  # trusted
  expect_equal(fmr[4], 200L)  # clinical_default
})

# --- Template Validation ---

test_that(".validateTemplateProfile rejects sklearn in clinical_update_noise", {
  expect_error(
    dsFlower:::.validateTemplateProfile("sklearn_logreg", "clinical_update_noise"),
    "not supported"
  )
  expect_error(
    dsFlower:::.validateTemplateProfile("sklearn_ridge", "clinical_update_noise"),
    "not supported"
  )
})

test_that(".validateTemplateProfile allows pytorch_mlp in clinical_update_noise", {
  # tabular_deep supports update-level noise.
  expect_true(dsFlower:::.validateTemplateProfile("pytorch_mlp", "clinical_update_noise"))
})

test_that(".validateTemplateProfile allows xgboost in clinical_default", {
  expect_true(dsFlower:::.validateTemplateProfile("xgboost", "clinical_default"))
})

test_that(".validateTemplateProfile rejects xgboost in sandbox_open", {
  # xgboost_secure has NA for sandbox
  expect_error(
    dsFlower:::.validateTemplateProfile("xgboost", "sandbox_open"),
    "not supported"
  )
})

test_that(".validateTemplateProfile allows only DP-SGD validated templates in high_sensitivity_dp", {
  for (tmpl in c("pytorch_logreg", "pytorch_linear_regression",
                 "pytorch_multiclass", "pytorch_multilabel",
                 "pytorch_mlp", "pytorch_poisson")) {
    expect_true(dsFlower:::.validateTemplateProfile(tmpl, "high_sensitivity_dp"),
                label = paste(tmpl, "should be allowed in high_sensitivity_dp"))
  }
  for (tmpl in c("sklearn_sgd", "pytorch_coxph", "pytorch_lognormal_aft",
                 "pytorch_cause_specific_cox", "pytorch_tcn",
                 "pytorch_lstm", "pytorch_resnet18",
                 "pytorch_densenet121", "pytorch_unet2d")) {
    expect_error(dsFlower:::.validateTemplateProfile(tmpl, "high_sensitivity_dp"),
                 "cannot be used",
                 label = paste(tmpl, "should be blocked in high_sensitivity_dp"))
  }
})

test_that(".validateTemplateProfile allows all templates in sandbox_open (except xgboost_secure)", {
  allowed_in_sandbox <- c("sklearn_logreg", "sklearn_ridge", "sklearn_sgd",
                          "pytorch_mlp", "pytorch_logreg",
                          "pytorch_linear_regression", "pytorch_coxph",
                          "pytorch_poisson", "pytorch_multilabel",
                          "pytorch_lognormal_aft",
                          "pytorch_cause_specific_cox",
                          "pytorch_multiclass", "pytorch_resnet18",
                          "pytorch_densenet121", "pytorch_unet2d",
                          "pytorch_tcn", "pytorch_lstm")
  for (tmpl in allowed_in_sandbox) {
    expect_true(dsFlower:::.validateTemplateProfile(tmpl, "sandbox_open"),
                label = paste(tmpl, "should be allowed in sandbox_open"))
  }
})

test_that(".validateTemplateProfile rejects unknown template in clinical_update_noise", {
  expect_error(
    dsFlower:::.validateTemplateProfile("unknown_model", "clinical_update_noise"),
    "no registered family"
  )
})

# --- Template Min Rows ---

test_that(".templateMinRows returns family-based minimums", {
  # tabular_deep / clinical_default = 500
  expect_equal(dsFlower:::.templateMinRows("pytorch_mlp", "clinical_default"), 500L)
  # iterative_linear / clinical_default = 200
  expect_equal(dsFlower:::.templateMinRows("sklearn_sgd", "clinical_default"), 200L)
  # closed_form_linear / clinical_default = 100
  expect_equal(dsFlower:::.templateMinRows("sklearn_logreg", "clinical_default"), 100L)
  # vision / clinical_default = 1000
  expect_equal(dsFlower:::.templateMinRows("pytorch_resnet18", "clinical_default"), 1000L)
  # segmentation / clinical_default = 1000
  expect_equal(dsFlower:::.templateMinRows("pytorch_unet2d", "clinical_default"), 1000L)
  # tabular_deep / high_sensitivity_dp = 1000
  expect_equal(dsFlower:::.templateMinRows("pytorch_mlp", "high_sensitivity_dp"), 1000L)
  # vision / high_sensitivity_dp = 5000
  expect_equal(dsFlower:::.templateMinRows("pytorch_resnet18", "high_sensitivity_dp"), 5000L)
  # closed_form_linear / clinical_update_noise = NA -> NULL
  expect_null(dsFlower:::.templateMinRows("sklearn_logreg", "clinical_update_noise"))
  # unknown template
  expect_null(dsFlower:::.templateMinRows("unknown_template", "clinical_default"))
})

# --- model_release ---

test_that("model_release is advisory_gated in clinical profiles", {
  for (pname in c("clinical_default", "clinical_hardened",
                  "clinical_update_noise", "high_sensitivity_dp",
                  "consortium_internal")) {
    p <- dsFlower:::.TRUST_PROFILES[[pname]]
    expect_equal(p$model_release, "advisory_gated",
                 label = paste(pname, "model_release"))
  }
})

test_that("model_release is allowed in sandbox and trusted", {
  for (pname in c("sandbox_open", "trusted_internal")) {
    p <- dsFlower:::.TRUST_PROFILES[[pname]]
    expect_equal(p$model_release, "allowed",
                 label = paste(pname, "model_release"))
  }
})

# --- Template Metadata ---

test_that(".TEMPLATE_METADATA has entries for all built-in templates", {
  meta <- dsFlower:::.TEMPLATE_METADATA
  expect_equal(length(meta), length(dsFlower:::.TEMPLATE_FAMILIES))
  for (nm in names(meta)) {
    expect_true("framework" %in% names(meta[[nm]]),
                label = paste(nm, "has framework"))
    expect_true("requires_secagg" %in% names(meta[[nm]]),
                label = paste(nm, "has requires_secagg"))
  }
})

test_that("xgboost requires secagg", {
  meta <- dsFlower:::.TEMPLATE_METADATA[["xgboost"]]
  expect_true(meta$requires_secagg)
})

# --- Class Distribution Validation ---

test_that(".validateClassDistribution passes binary with sufficient positives", {
  data <- data.frame(target = c(rep(0, 80), rep(1, 20)))
  trust <- list(min_positive_examples = 20L, min_per_class = 0L, min_events = 0L)
  expect_true(dsFlower:::.validateClassDistribution(data, "target", trust))
})

test_that(".validateClassDistribution blocks binary with insufficient positives", {
  data <- data.frame(target = c(rep(0, 99), 1))
  trust <- list(min_positive_examples = 20L, min_per_class = 0L, min_events = 0L)
  expect_error(
    dsFlower:::.validateClassDistribution(data, "target", trust),
    "Disclosive.*insufficient class counts"
  )
})

test_that(".validateClassDistribution passes multiclass with sufficient per-class", {
  data <- data.frame(target = c(rep("A", 30), rep("B", 30), rep("C", 30)))
  trust <- list(min_positive_examples = 0L, min_per_class = 20L, min_events = 0L)
  expect_true(dsFlower:::.validateClassDistribution(data, "target", trust))
})

test_that(".validateClassDistribution blocks multiclass with rare class", {
  data <- data.frame(target = c(rep("A", 50), rep("B", 50), rep("C", 5)))
  trust <- list(min_positive_examples = 0L, min_per_class = 20L, min_events = 0L)
  expect_error(
    dsFlower:::.validateClassDistribution(data, "target", trust),
    "Disclosive.*insufficient class counts"
  )
})

test_that(".validateClassDistribution passes survival with sufficient events", {
  data <- data.frame(time = 1:100, event = c(rep(1, 30), rep(0, 70)))
  trust <- list(min_positive_examples = 0L, min_per_class = 0L, min_events = 20L)
  expect_true(
    dsFlower:::.validateClassDistribution(data, c("time", "event"), trust)
  )
})

test_that(".validateClassDistribution blocks survival with insufficient events", {
  data <- data.frame(time = 1:100, event = c(rep(1, 5), rep(0, 95)))
  trust <- list(min_positive_examples = 0L, min_per_class = 0L, min_events = 20L)
  expect_error(
    dsFlower:::.validateClassDistribution(data, c("time", "event"), trust),
    "Disclosive.*insufficient event counts"
  )
})

test_that(".validateClassDistribution detects survival from single event column name", {
  data <- data.frame(event = c(rep(1, 5), rep(0, 95)))
  trust <- list(min_positive_examples = 0L, min_per_class = 0L, min_events = 20L)
  expect_error(
    dsFlower:::.validateClassDistribution(data, "event", trust),
    "Disclosive.*insufficient event counts"
  )
})

test_that(".validateClassDistribution skips when min thresholds are 0", {
  data <- data.frame(target = c(rep(0, 99), 1))
  trust <- list(min_positive_examples = 0L, min_per_class = 0L, min_events = 0L)
  # With min_positive_examples = 0, binary check still applies but threshold is 0
  expect_true(dsFlower:::.validateClassDistribution(data, "target", trust))
})

# --- Absolute Floors ---

test_that(".enforce_absolute_floors enforces min_train_rows >= 3", {
  profile <- list(min_train_rows = 1, min_clients_per_round = 2L)
  result <- dsFlower:::.enforce_absolute_floors(profile)
  expect_equal(result$min_train_rows, 3)
})

test_that(".enforce_absolute_floors enforces min_clients_per_round >= 1", {
  profile <- list(min_train_rows = 100, min_clients_per_round = 0L)
  result <- dsFlower:::.enforce_absolute_floors(profile)
  expect_equal(result$min_clients_per_round, 1L)
})

test_that(".enforce_absolute_floors enforces SecAgg min_clients_per_round >= 3", {
  profile <- list(
    min_train_rows = 100,
    min_clients_per_round = 1L,
    require_secure_aggregation = TRUE
  )
  result <- dsFlower:::.enforce_absolute_floors(profile)
  expect_equal(result$min_clients_per_round, 3L)
})
