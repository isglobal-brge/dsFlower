test_that("trainable vision requires strict custodian permission", {
  old <- options(dsflower.allow_trainable_backbone = NULL,
                 default.dsflower.allow_trainable_backbone = NULL,
                 dsflower.dp_unit = "patient", dsflower.patient_column = "patient")
  on.exit(options(old), add = TRUE)
  cfg <- list("data_type" = "image", "backbone" = "resnet18",
              "backbone-mode" = "scratch", "image-size" = 32L,
              "loss-name" = "bce_logits", "local-epochs" = 1L,
              "num-server-rounds" = 1L, "batch-size" = 2L)
  expect_false(.trainableBackboneAllowed())
  expect_error(.normalizeBackboneMode(cfg, "neural"), "custodian permission")
  options(default.dsflower.allow_trainable_backbone = TRUE)
  admitted <- .normalizeBackboneMode(cfg, "neural")
  expect_true(admitted[["allow-trainable-backbone"]])
  expect_identical(admitted[["backbone-mode"]], "scratch")
  cfg[["backbone-mode"]] <- "finetune"
  expect_true(.normalizeBackboneMode(cfg, "neural")[["allow-trainable-backbone"]])
  options(dsflower.allow_trainable_backbone = FALSE)
  expect_error(.normalizeBackboneMode(cfg, "neural"), "custodian permission")
  for (bad in list(1, "TRUE", NA, c(TRUE, FALSE))) {
    options(dsflower.allow_trainable_backbone = bad)
    expect_error(.trainableBackboneAllowed(), "TRUE or FALSE")
  }
})

test_that("frozen config and public vision resource admission are stable", {
  old <- options(dsflower.allow_trainable_backbone = TRUE,
                 dsflower.dp_unit = "patient", dsflower.patient_column = "patient")
  on.exit(options(old), add = TRUE)
  cfg <- list("data_type" = "image", "backbone" = "resnet18",
              "image-size" = 32L, "loss-name" = "bce_logits",
              "local-epochs" = 1L, "num-server-rounds" = 1L, "batch-size" = 2L)
  expect_identical(.normalizeBackboneMode(cfg, "neural"), cfg)
  frozen <- cfg
  frozen[["backbone-mode"]] <- "frozen"
  expect_identical(.normalizeBackboneMode(frozen, "neural"), cfg)
  cfg[["backbone-mode"]] <- "scratch"
  for (key in c("image-size", "local-epochs", "num-server-rounds", "batch-size")) {
    bad <- cfg
    bad[[key]] <- 512L
    expect_error(.normalizeBackboneMode(bad, "neural"), "Trainable vision limits")
  }
  bad <- cfg
  bad[["cv-folds"]] <- 2L
  expect_error(.normalizeBackboneMode(bad, "neural"), "holdout or CV")
  options(dsflower.dp_unit = "row")
  expect_error(.normalizeBackboneMode(cfg, "neural"), "patient-level")
  expect_error(.validate_client_run_config(
    list("allow-trainable-backbone" = TRUE)), "server-owned")
  expect_error(.normalizeBackboneMode(cfg, "validation"), "classification training")
})
