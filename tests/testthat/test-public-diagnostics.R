capture_public_conditions <- function(expr) {
  warnings <- character()
  messages <- character()
  value <- withCallingHandlers(
    tryCatch(force(expr), error = identity),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    },
    message = function(m) {
      messages <<- c(messages, conditionMessage(m))
      invokeRestart("muffleMessage")
    })
  list(value = value, warnings = warnings, messages = messages)
}

expect_no_private_diagnostic <- function(result, private_fragments) {
  public <- paste(
    c(if (inherits(result$value, "condition")) conditionMessage(result$value),
      result$warnings, result$messages), collapse = "\n")
  for (fragment in private_fragments) {
    expect_false(grepl(fragment, public, fixed = TRUE))
  }
}

test_that("registered privacy bootstrap failures use one constant diagnostic", {
  private_fragments <- c(
    "/var/lib/dsflower/privacy/node-secret",
    "s3://private-bucket/state", "patient-007")
  private_error <- paste(private_fragments, collapse = " ")
  local_mocked_bindings(
    .privacy_runtime_bootstrap = function() {
      warning(private_error, call. = FALSE)
      message(private_error)
      stop(private_error, call. = FALSE)
    },
    .package = "dsFlower")

  init <- capture_public_conditions(flowerInitDS("private_table"))
  expect_s3_class(init$value, "error")
  expect_identical(conditionMessage(init$value),
                   "dsFlower privacy state is unavailable.")
  expect_length(init$warnings, 0L)
  expect_length(init$messages, 0L)
  expect_no_private_diagnostic(init, private_fragments)

  withr::local_options(list(
    nfilter.subset = 3L, dsflower.min_train_rows = 3L))
  prepare_name <- "private_bootstrap_prepare"
  dsFlower:::.setHandle(prepare_name, mock_handle(table_data = data.frame(
    feature = seq_len(5), target = rep(0:1, length.out = 5))))
  withr::defer(dsFlower:::.removeHandle(prepare_name, cleanup = FALSE))
  prepare <- capture_public_conditions(
    flowerPrepareRunDS(prepare_name, "target", "feature"))
  expect_s3_class(prepare$value, "error")
  expect_identical(conditionMessage(prepare$value),
                   "dsFlower privacy state is unavailable.")
  expect_length(prepare$warnings, 0L)
  expect_length(prepare$messages, 0L)
  expect_no_private_diagnostic(prepare, private_fragments)
})

test_that("capability runtime failures do not reflect infrastructure details", {
  private_fragments <- c(
    "/var/lib/dsflower/venvs/pytorch/.dsflower_versions.txt",
    "s3://private-bucket/runtime", "patient-007")
  private_error <- paste(private_fragments, collapse = " ")
  local_mocked_bindings(
    .python_runtime_capabilities = function() {
      warning(private_error, call. = FALSE)
      message(private_error)
      stop(private_error, call. = FALSE)
    },
    .compute_harness_hash = function() {
      warning(private_error, call. = FALSE)
      message(private_error)
      stop(private_error, call. = FALSE)
    },
    .package = "dsFlower")

  result <- capture_public_conditions(flowerGetCapabilitiesDS())
  expect_type(result$value, "list")
  expect_identical(unname(unlist(result$value[c(
    "python_version", "flower_version", "torch_version", "opacus_version",
    "runtime_versions_sha256")], use.names = FALSE)),
    rep("unavailable", 5L))
  expect_identical(result$value$runner_sha256, "unavailable")
  expect_length(result$warnings, 0L)
  expect_length(result$messages, 0L)
  expect_no_private_diagnostic(result, private_fragments)
})

test_that("runner hash failures do not expose package paths during ensure", {
  private_fragments <- c(
    "/usr/local/lib/R/site-library/dsFlower/flower_app/dsflower_runner",
    "s3://private-bucket/runner", "patient-007")
  private_error <- paste(private_fragments, collapse = " ")
  run_token <- paste0("run_", paste(rep("b", 32L), collapse = ""))
  staging <- dsFlower:::.ensureStagingDir(run_token)
  withr::defer(unlink(staging, recursive = TRUE))
  name <- "private_runner_hash_failure"
  dsFlower:::.setHandle(name, mock_handle(
    run_token = run_token, staging_dir = staging, prepared = TRUE))
  withr::defer(dsFlower:::.removeHandle(name, cleanup = FALSE))
  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .compute_harness_hash = function() {
      warning(private_error, call. = FALSE)
      message(private_error)
      stop(private_error, call. = FALSE)
    },
    .package = "dsFlower")

  result <- capture_public_conditions(
    flowerEnsureSuperNodeDS(name, "ignored.example:9092"))
  expect_s3_class(result$value, "error")
  expect_identical(
    conditionMessage(result$value),
    "The canonical runner (dsflower_runner) is not installed on this node.")
  expect_length(result$warnings, 0L)
  expect_length(result$messages, 0L)
  expect_no_private_diagnostic(result, private_fragments)
})

test_that("SuperNode failures hide paths and cross-session process counts", {
  private_fragments <- c(
    "/var/lib/dsflower/venvs/pytorch/bin/flower-supernode",
    "7 active SuperNodes", "patient-007")
  private_error <- paste(private_fragments, collapse = " ")
  staging <- dsFlower:::.ensureStagingDir(
    paste0("run_", paste(rep("a", 32L), collapse = "")))
  withr::defer(unlink(staging, recursive = TRUE))
  jsonlite::write_json(
    list("num-server-rounds" = 1L), file.path(staging, "manifest.json"),
    auto_unbox = TRUE)
  name <- "private_supernode_failure"
  dsFlower:::.setHandle(name, mock_handle(
    run_token = basename(staging), staging_dir = staging,
    prepared = TRUE, python_path = "/private/python"))
  withr::defer(dsFlower:::.removeHandle(name, cleanup = FALSE))
  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .compute_harness_hash = function() strrep("a", 64L),
    .privacy_runtime_bootstrap = function() list(key_action = "reused"),
    .privacy_training_contract = function(...) list(),
    .apply_privacy_contract = function(...) invisible(TRUE),
    .supernode_ensure = function(...) {
      warning(private_error, call. = FALSE)
      message(private_error)
      stop(private_error, call. = FALSE)
    },
    .package = "dsFlower")

  result <- capture_public_conditions(
    flowerEnsureSuperNodeDS(name, "ignored.example:9092"))
  expect_s3_class(result$value, "error")
  expect_identical(conditionMessage(result$value),
                   "SuperNode is unavailable.")
  expect_length(result$warnings, 0L)
  expect_length(result$messages, 0L)
  expect_no_private_diagnostic(result, private_fragments)
})
