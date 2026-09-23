local_release_cache_state <- function(.local_envir = parent.frame()) {
  # Other dependency fixtures can make R's shared tempdir group-writable.
  # The durable cache deliberately rejects that ancestry, so isolate its state.
  root <- normalizePath(withr::local_tempdir(
    tmpdir = dirname(tempdir()), .local_envir = .local_envir))
  Sys.chmod(root, "0700")
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(root, "privacy", "noise_root"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
    DSFLOWER_TEST_ALLOW_EPHEMERAL_RELEASE_CACHE = "1"
  ), .local_envir = .local_envir)
  withr::local_options(list(
    dsflower.staging_root = file.path(root, "staging"),
    dsflower.hook_enabled = TRUE,
    dsflower.release_cache_dir = file.path(root, "releases"),
    dsflower.release_cache_bytes = 1024^3
  ), .local_envir = .local_envir)
  root
}

test_that("cache and deadline controls are rejected at analyst boundaries", {
  controls <- c("release_cache_dir", "release-cache-bytes", "releaseCacheBytes",
                "cache", "hook-deadline", "deadlineSeconds")
  for (key in controls) {
    config <- setNames(list(1024), key)
    expect_error(dsFlower:::.validate_client_run_config(config),
                 "unsupported|server-owned")
    expect_error(dsFlower:::.validate_manifest_extra_config(config),
                 "server-owned")
    state <- new.env(parent = emptyenv())
    state$items <- 0L
    expect_error(dsFlower:::.canonicalHookAppValue(
      list(nested = config), 0L, state, top = TRUE), "reserved")
  }
})

test_that("release cache options are administrator-owned and bounded", {
  root <- local_release_cache_state()
  withr::local_envvar(c(DSFLOWER_RELEASE_CACHE_DIR = "/attacker/cache",
                       DSFLOWER_RELEASE_CACHE_BYTES = "1"))
  settings <- dsFlower:::.release_cache_settings()
  expect_identical(settings$directory, file.path(root, "releases"))
  expect_identical(settings$capacity, 1024^3)
  for (value in list(0, -1, 0.5, Inf, NA_real_, c(1, 2), "1024", 2^53)) {
    expect_error(withr::with_options(list(dsflower.release_cache_bytes = value),
      dsFlower:::.release_cache_settings()), "positive exact integer")
  }
  expect_error(withr::with_options(list(dsflower.release_cache_dir = "relative"),
    dsFlower:::.release_cache_settings()), "absolute")
  expect_error(withr::with_options(list(dsflower.release_cache_dir = file.path(
    root, "staging", "dsflower", "cache")),
    dsFlower:::.release_cache_settings()), "outside staging")
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_RELEASE_CACHE = NA))
  expect_error(withr::with_options(list(dsflower.release_cache_dir = file.path(
    normalizePath(tempdir()), "release-cache")),
    dsFlower:::.release_cache_settings()), "persistent")
})

test_that("declarative admission does not create or inspect cache state", {
  local_mocked_bindings(
    .release_cache_settings = function(...) stop("must not inspect cache"))
  expect_null(dsFlower:::.release_cache_admit(
    paste0("run_", strrep("a", 32)), list("dp-track" = "neural")))
  expect_null(dsFlower:::.release_cache_admit(
    paste0("run_", strrep("a", 32)), list(
      "dp-track" = "egress", "privacy-hook_enabled" = 0)))
})

test_that("Hook reservation precedes private staging and preserves rollback", {
  local_release_cache_state()
  seen <- character()
  local_mocked_bindings(
    .release_cache_command = function(action, run_token, settings, rounds = NULL) {
      seen <<- c(seen, action)
      if (identical(action, "reserve")) stop("capacity exhausted")
      invisible(TRUE)
    },
    .stageData = function(...) stop("private staging must not run"))
  dsFlower:::.setHandle("cache_admission", mock_handle(
    data_path = "/private-data-must-not-be-read.csv", data_format = "csv"))
  withr::defer(dsFlower:::.removeHandle("cache_admission"))
  config <- list("dp-track" = "egress", "task-type" = "classification")
  expect_error(flowerPrepareRunDS("cache_admission", "target", "feature",
    config),
    "durable Hook release cache is unavailable")
  expect_identical(seen, c("reserve", "close"))
  expect_length(dsFlower:::.getHandle("cache_admission")$pending_cleanup_tokens, 0L)
})

test_that("cache receipts preserve settings and require owner-only state", {
  skip_on_os("windows")
  root <- local_release_cache_state()
  token <- dsFlower:::.generate_run_token()
  commands <- list()
  local_mocked_bindings(
    .release_cache_command = function(action, run_token, settings, rounds = NULL) {
      commands[[length(commands) + 1L]] <<- list(
        action = action, settings = settings, rounds = rounds)
      invisible(TRUE)
    })
  dsFlower:::.release_cache_admit(token, list(
    "dp-track" = "egress", "privacy-hook_enabled" = 1,
    "num-server-rounds" = 3L))
  staging <- dsFlower:::.expectedStagingDirs(token)[[1L]]
  withr::defer(dsFlower:::.cleanupStaging(token))
  receipt <- file.path(staging, ".release-cache.json")
  expect_true(file.exists(receipt))
  expect_identical(commands[[1L]]$rounds, 3L)
  withr::local_options(list(dsflower.release_cache_dir = file.path(root, "changed"),
                           dsflower.release_cache_bytes = 99))
  env <- dsFlower:::.release_cache_environment(staging)
  expect_identical(unname(env[["DSFLOWER_RELEASE_CACHE_DIR"]]),
                   file.path(root, "releases"))
  expect_identical(unname(env[["DSFLOWER_RELEASE_CACHE_BYTES"]]), "1073741824")
  expect_equal(as.integer(file.info(receipt)$mode), strtoi("600", 8L))
  Sys.chmod(receipt, "0644")
  expect_error(dsFlower:::.release_cache_environment(staging), "owner-only")
  Sys.chmod(receipt, "0600")
  Sys.chmod(staging, "0755")
  expect_error(dsFlower:::.release_cache_environment(staging), "owner-only")
  Sys.chmod(staging, "0700")
  original <- paste0(receipt, ".saved")
  expect_true(file.rename(receipt, original))
  expect_true(file.symlink(original, receipt))
  expect_error(dsFlower:::.release_cache_environment(staging), "unsafe")
  unlink(receipt)
  expect_true(file.rename(original, receipt))
})

test_that("authoritative cleanup stops workers before close and keeps failed receipts", {
  local_release_cache_state()
  token <- dsFlower:::.generate_run_token()
  events <- character()
  fail_close <- TRUE
  local_mocked_bindings(
    .release_cache_command = function(action, run_token, settings, rounds = NULL) {
      events <<- c(events, action)
      if (identical(action, "close") && fail_close) stop("closure interrupted")
      invisible(TRUE)
    },
    .supernode_stop = function(manifest_dir) {
      events <<- c(events, "stop")
      invisible(TRUE)
    })
  dsFlower:::.release_cache_admit(token, list(
    "dp-track" = "egress", "privacy-hook_enabled" = 1,
    "num-server-rounds" = 1L))
  staging <- dsFlower:::.expectedStagingDirs(token)[[1L]]
  expect_error(dsFlower:::.cleanupStaging(token), "closure interrupted")
  expect_true(file.exists(file.path(staging, ".release-cache.json")))
  expect_true(all(events[seq.int(2L, length(events) - 1L)] == "stop"))
  expect_identical(tail(events, 1L), "close")
  fail_close <- FALSE
  expect_true(dsFlower:::.cleanupStaging(token))
  expect_false(dir.exists(staging))
})

test_that("age-based recovery retains uncertain cached-run receipts", {
  root <- local_release_cache_state()
  token <- dsFlower:::.generate_run_token()
  local_mocked_bindings(
    .release_cache_command = function(...) invisible(TRUE),
    .list_supernode_processes = function() data.frame())
  dsFlower:::.release_cache_admit(token, list(
    "dp-track" = "egress", "privacy-hook_enabled" = 1,
    "num-server-rounds" = 1L))
  staging <- dsFlower:::.expectedStagingDirs(token)[[1L]]
  withr::defer(dsFlower:::.cleanupStaging(token))
  Sys.setFileTime(staging, Sys.time() - 3 * 86400)
  dsFlower:::.cleanup_stale_staging(bases = file.path(root, "staging"))
  expect_true(file.exists(file.path(staging, ".release-cache.json")))
})

test_that("the isolated cache CLI reserves durable state and closes exact runs", {
  skip_on_os("windows")
  python <- unname(Sys.which("python3"))
  skip_if(!nzchar(python), "Python is required for the cache admission CLI")
  local_release_cache_state()
  local_mocked_bindings(
    .resolve_framework_runtime = function(framework) list(python = python))
  token <- dsFlower:::.generate_run_token()
  settings <- dsFlower:::.release_cache_settings()
  expect_true(dsFlower:::.release_cache_command("reserve", token, settings, 2L))
  expect_true(dsFlower:::.release_cache_command("reserve", token, settings, 2L))
  expect_equal(as.integer(file.info(settings$directory)$mode), strtoi("700", 8L))
  files <- list.files(settings$directory, full.names = TRUE)
  expect_true(length(files) > 0L)
  expect_true(all(as.integer(file.info(files)$mode) == strtoi("600", 8L)))
  expect_true(dsFlower:::.release_cache_command("close", token, settings))
  expect_error(dsFlower:::.release_cache_command("reserve", token, settings, 2L),
               "durable Hook release cache is unavailable")
  constrained <- settings
  constrained$capacity <- 1
  expect_error(dsFlower:::.release_cache_command(
    "reserve", dsFlower:::.generate_run_token(), constrained, 1L),
    "durable Hook release cache is unavailable")
})
