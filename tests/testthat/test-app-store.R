# Tests for R/app_store.R -- Tier-2 uploaded-app receive + verify over DSI

.enc_b64 <- function(r) {
  b <- gsub("[\r\n]", "", jsonlite::base64_enc(r))
  b <- gsub("\\+", "-", b); b <- gsub("/", "_", b); b <- gsub("=+$", "", b)
  paste0("B64:", b)
}

.test_app_token <- function(index, prefix = "app") {
  paste0(prefix, "_", sprintf("%032x", as.integer(index)))
}

.local_app_spool <- function() {
  test_env <- parent.frame()
  root <- withr::local_tempdir(
    pattern = "dsflower-app-spool-", .local_envir = test_env)
  withr::local_options(
    list(dsflower.app_spool_root = root), .local_envir = test_env)
  withr::local_envvar(
    c(DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL = "1"),
    .local_envir = test_env)
  root
}

.make_fab <- function(dir) {
  writeLines("print('hi')", file.path(dir, "client_app.py"))
  fab <- file.path(dir, "app.fab")
  wd <- getwd(); setwd(dir); on.exit(setwd(wd))
  utils::zip("app.fab", files = "client_app.py", flags = "-q")
  file.path(dir, "app.fab")
}

.make_hostile_fab <- function(dir, name, symlink = FALSE) {
  py <- dsFlower:::.scan_python()
  if (!nzchar(py)) return("")
  fab <- file.path(dir, "hostile.fab")
  code <- paste(
    "import stat, sys, zipfile",
    "archive, name, is_link = sys.argv[1], sys.argv[2], sys.argv[3] == '1'",
    "with zipfile.ZipFile(archive, 'w') as zf:",
    "    if is_link:",
    "        zi = zipfile.ZipInfo(name)",
    "        zi.create_system = 3",
    "        zi.external_attr = (stat.S_IFLNK | 0o777) << 16",
    "        zf.writestr(zi, '../../outside.py')",
    "    else:",
    "        zf.writestr(name, \"print('hostile')\")",
    sep = "\n"
  )
  res <- processx::run(py, c("-c", code, fab, name, if (symlink) "1" else "0"),
                       error_on_status = FALSE)
  if (res$status != 0L) return("")
  fab
}

test_that("chunked push + install verifies a FAB by sha256 and unpacks it", {
  .local_app_spool()
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  sha <- digest::digest(file = fab, algo = "sha256")
  token <- .test_app_token(1)
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  half <- floor(length(raw) / 2)
  first_chunk <- .enc_b64(raw[1:half])
  second_chunk <- .enc_b64(raw[(half + 1):length(raw)])
  r1 <- dsFlower::flowerAppPushDS(token, first_chunk, 0)
  expect_equal(r1$size, half)
  r2 <- dsFlower::flowerAppPushDS(token, second_chunk, half)
  expect_equal(r2$size, length(raw))

  # idempotent: re-pushing the last chunk at the same offset does not grow the file
  r2b <- dsFlower::flowerAppPushDS(token, second_chunk, half)
  expect_true(r2b$ok)
  expect_equal(r2b$size, length(raw))
  conflict <- raw[(half + 1):length(raw)]
  conflict[[1L]] <- as.raw(bitwXor(as.integer(conflict[[1L]]), 1L))
  conflict_chunk <- .enc_b64(conflict)
  bad_content <- dsFlower::flowerAppPushDS(token, conflict_chunk, half)
  expect_false(bad_content$ok)
  expect_identical(bad_content$error, "conflict")
  short_chunk <- .enc_b64(raw[(half + 1):(length(raw) - 1L)])
  bad_length <- dsFlower::flowerAppPushDS(token, short_chunk, half)
  expect_false(bad_length$ok)
  expect_identical(bad_length$error, "conflict")
  expect_equal(file.size(file.path(
    dsFlower:::.app_spool_dir(token, create = FALSE), "app.fab")),
    length(raw))

  res <- dsFlower::flowerAppInstallDS(token, sha)
  expect_true(res$ok)
  expect_equal(res$sha256, sha)
  expect_null(res$path)
  expect_true(file.exists(file.path(
    dsFlower:::.app_spool_dir(token, create = FALSE), "unpacked", "client_app.py")))
})

test_that("install rejects a hash mismatch and destroys the spool", {
  .local_app_spool()
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  token <- .test_app_token(2)
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  chunk <- .enc_b64(raw)
  dsFlower::flowerAppPushDS(token, chunk, 0)
  expect_error(
    dsFlower::flowerAppInstallDS(token, "deadbeef_wrong_hash"),
    "integrity check"
  )
  # spool destroyed -> a second install finds nothing
  expect_error(dsFlower::flowerAppInstallDS(token, "x"), "No uploaded app")
})

test_that("registered app-store errors do not reflect the configured root", {
  private_root <- "/dev/null/private-appstore/patient-007"
  withr::local_options(list(dsflower.app_spool_root = private_root))
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL = ""))
  token <- .test_app_token(99)
  chunk <- .enc_b64(charToRaw("x"))

  error <- tryCatch(
    dsFlower::flowerAppPushDS(token, chunk, 0),
    error = identity)
  expect_s3_class(error, "error")
  expect_identical(conditionMessage(error),
                   "App spool storage is unavailable.")
  expect_false(grepl(private_root, conditionMessage(error), fixed = TRUE))
  expect_false(grepl("patient-007", conditionMessage(error), fixed = TRUE))
})

test_that("install rejects an app that fails the exfiltration scan", {
  .local_app_spool()
  skip_if(!nzchar(dsFlower:::.scan_python()), "no python for scan")
  dir <- withr::local_tempdir()
  writeLines(c("import socket", "x = 1"), file.path(dir, "client_app.py"))
  wd <- getwd(); setwd(dir); utils::zip("app.fab", files = "client_app.py", flags = "-q"); setwd(wd)
  fab <- file.path(dir, "app.fab")
  raw <- readBin(fab, "raw", file.size(fab))
  sha <- digest::digest(file = fab, algo = "sha256")
  token <- .test_app_token(3)
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  chunk <- .enc_b64(raw)
  dsFlower::flowerAppPushDS(token, chunk, 0)
  expect_error(dsFlower::flowerAppInstallDS(token, sha), "safety scan")
})

test_that("archive extraction failures do not reflect private spool paths", {
  .local_app_spool()
  payload <- charToRaw("not-a-zip")
  token <- .test_app_token(100)
  chunk <- .enc_b64(payload)
  sha <- digest::digest(payload, algo = "sha256", serialize = FALSE)
  private <- paste(
    "Errno 63", "/var/lib/dsflower/appstore/private-candidate",
    "patient-007")
  withr::defer(try(dsFlower::flowerAppDeleteDS(token), silent = TRUE))
  dsFlower::flowerAppPushDS(token, chunk, 0)
  testthat::local_mocked_bindings(
    .safe_extract_fab = function(...) list(ok = FALSE, first = private),
    .package = "dsFlower")

  error <- tryCatch(
    dsFlower::flowerAppInstallDS(token, sha), error = identity)
  expect_s3_class(error, "error")
  expect_identical(conditionMessage(error),
                   "Uploaded app is an unsafe FAB archive; rejected.")
  expect_false(grepl(private, conditionMessage(error), fixed = TRUE))
  expect_false(grepl("/var/lib/dsflower", conditionMessage(error),
                     fixed = TRUE))
  expect_false(grepl("patient-007", conditionMessage(error), fixed = TRUE))
})

test_that("install enforces the max_fab_bytes cap", {
  .local_app_spool()
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  token <- .test_app_token(4)
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  chunk <- .enc_b64(raw)
  dsFlower::flowerAppPushDS(token, chunk, 0)
  withr::local_options(list(dsflower.max_fab_bytes = 1))
  sha <- digest::digest(file = fab, algo = "sha256")
  expect_error(
    dsFlower::flowerAppInstallDS(token, sha),
    "max_fab_bytes"
  )
})

test_that("app tokens are validated exactly and never sanitized into aliases", {
  .local_app_spool()
  bad <- list("", NA_character_, "app/other", "app-other", "app.other", "app token",
              "app_valid_01", paste0("app_", strrep("a", 31)),
              paste0("app_", strrep("A", 32)), paste0("bad_", strrep("a", 32)),
              c(.test_app_token(1), .test_app_token(2)))
  for (token in bad) {
    expect_error(dsFlower:::.app_spool_dir(token), "Invalid app token")
  }
  valid <- .test_app_token(5)
  expect_match(dsFlower:::.app_spool_dir(valid), paste0(valid, "$"))
  dsFlower::flowerAppDeleteDS(valid)
})

test_that("app spool root is absolute, persistent, private, and not a symlink", {
  root <- file.path(tempdir(), paste0("dsflower-spool-root-", Sys.getpid()))
  withr::defer(unlink(root, recursive = TRUE, force = TRUE))
  withr::local_options(list(dsflower.app_spool_root = root))
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL = NA))
  expect_error(dsFlower:::.app_spool_root(), "must be persistent")

  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL = "1"))
  expect_identical(dsFlower:::.app_spool_root(),
                   normalizePath(root, winslash = "/", mustWork = TRUE))
  info <- file.info(root)
  if (.Platform$OS.type == "unix") {
    expect_equal(bitwAnd(as.integer(info$mode[[1]]),
                         as.integer(strtoi("077", base = 8))), 0L)
    expect_equal(info$uname[[1]], unname(Sys.info()[["effective_user"]]))
  }

  withr::local_options(list(dsflower.app_spool_root = "relative/appstore"))
  expect_error(dsFlower:::.app_spool_root(), "one absolute path")
})

test_that("app spool rejects root, token, and nested symbolic links", {
  skip_on_os("windows")
  holder <- withr::local_tempdir()
  target <- file.path(holder, "target")
  link <- file.path(holder, "root-link")
  dir.create(target)
  expect_true(file.symlink(target, link))
  withr::local_options(list(dsflower.app_spool_root = link))
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL = "1"))
  expect_error(dsFlower:::.app_spool_root(), "symbolic link")

  root <- .local_app_spool()
  token <- .test_app_token(30)
  token_target <- file.path(holder, "token-target")
  dir.create(token_target)
  expect_true(file.symlink(token_target, file.path(root, token)))
  expect_error(dsFlower:::.app_spool_dir(token), "symbolic link")
  unlink(file.path(root, token))

  spool <- dsFlower:::.app_spool_dir(token)
  outside <- file.path(holder, "outside")
  writeBin(as.raw(1), outside)
  expect_true(file.symlink(outside, file.path(spool, "nested-link")))
  expect_error(dsFlower:::.app_spool_usage(root), "Symbolic links")
})

test_that("global byte cap is enforced without a catalogue-count quota", {
  root <- .local_app_spool()
  withr::local_options(list(
    dsflower.app_spool_max_bytes = 3
  ))
  first <- .test_app_token(31)
  second <- .test_app_token(32)
  first_chunk <- .enc_b64(as.raw(c(1, 2)))
  second_chunk <- .enc_b64(as.raw(c(3, 4)))
  dsFlower::flowerAppPushDS(first, first_chunk, 0)
  expect_error(
    dsFlower::flowerAppPushDS(second, second_chunk, 0),
    "app_spool_max_bytes"
  )
  expect_false(dir.exists(file.path(root, second)))

  dsFlower::flowerAppDeleteDS(first)
  withr::local_options(list(dsflower.app_spool_max_bytes = 100))
  first_chunk <- .enc_b64(as.raw(1))
  second_chunk <- .enc_b64(as.raw(2))
  dsFlower::flowerAppPushDS(first, first_chunk, 0)
  expect_true(dsFlower::flowerAppPushDS(second, second_chunk, 0)$ok)
  expect_equal(dsFlower:::.app_spool_usage(root)$uploads, 2L)
})

test_that("concurrent uploads have no catalogue-count quota", {
  skip_on_os("windows")
  root <- .local_app_spool()
  withr::local_options(list(dsflower.app_spool_max_bytes = 1024))
  sync <- withr::local_tempdir()
  go <- file.path(sync, "go")
  ready <- file.path(sync, c("ready-1", "ready-2"))
  tokens <- c(.test_app_token(33), .test_app_token(34))
  payload <- .enc_b64(as.raw(rep(7, 64)))
  worker <- function(token, ready_path) {
    writeLines("ready", ready_path)
    deadline <- Sys.time() + 5
    while (!file.exists(go) && Sys.time() < deadline) Sys.sleep(0.005)
    tryCatch({
      dsFlower::flowerAppPushDS(token, payload, 0)
      TRUE
    }, error = function(e) conditionMessage(e))
  }
  jobs <- list(
    parallel::mcparallel(worker(tokens[[1]], ready[[1]]), silent = TRUE),
    parallel::mcparallel(worker(tokens[[2]], ready[[2]]), silent = TRUE)
  )
  deadline <- Sys.time() + 5
  while (!all(file.exists(ready)) && Sys.time() < deadline) Sys.sleep(0.005)
  expect_true(all(file.exists(ready)))
  file.create(go)
  results <- parallel::mccollect(jobs)
  expect_true(all(vapply(results, isTRUE, logical(1))))
  expect_equal(dsFlower:::.app_spool_usage(root)$uploads, 2L)
})

test_that("TTL GC removes stale spools but skips a locked active upload", {
  skip_on_os("windows")
  root <- .local_app_spool()
  withr::local_options(list(dsflower.app_spool_ttl_seconds = 1))
  stale <- .test_app_token(35)
  fresh <- .test_app_token(36)
  now <- Sys.time()
  stale_dir <- dsFlower:::.app_spool_dir(stale)
  fresh_dir <- dsFlower:::.app_spool_dir(fresh)
  dsFlower:::.touch_app_activity(stale_dir, now - 10)
  dsFlower:::.touch_app_activity(fresh_dir, now)
  first_gc <- dsFlower:::.app_spool_gc(now)
  expect_contains(first_gc$removed, stale)
  expect_false(dir.exists(stale_dir))
  expect_true(dir.exists(fresh_dir))

  dsFlower:::.touch_app_activity(fresh_dir, now - 10)
  ready <- file.path(root, "active-ready")
  job <- parallel::mcparallel(
    dsFlower:::.with_app_lock(fresh, {
      writeLines("ready", ready)
      Sys.sleep(1.5)
      TRUE
    }),
    silent = TRUE
  )
  deadline <- Sys.time() + 5
  while (!file.exists(ready) && Sys.time() < deadline) Sys.sleep(0.005)
  expect_true(file.exists(ready))
  active_gc <- dsFlower:::.app_spool_gc(now)
  expect_contains(active_gc$skipped_active, fresh)
  expect_true(dir.exists(fresh_dir))
  expect_true(isTRUE(parallel::mccollect(job)[[1]]))

  final_gc <- dsFlower:::.app_spool_gc(now)
  expect_contains(final_gc$removed, fresh)
  expect_false(dir.exists(fresh_dir))
})

test_that("TTL GC retains a pinned app until its staging run is cleaned", {
  root <- .local_app_spool()
  withr::local_options(list(dsflower.app_spool_ttl_seconds = 1))
  token <- .test_app_token(37, "usr")
  run_token <- dsFlower:::.generate_run_token()
  staging <- dsFlower:::.ensureStagingDir(run_token)
  withr::defer(dsFlower:::.cleanupStaging(run_token))
  spool <- dsFlower:::.app_spool_dir(token)
  now <- Sys.time()
  dsFlower:::.record_app_run_lease(spool, run_token)
  dsFlower:::.touch_app_activity(spool, now - 10)

  active_gc <- dsFlower:::.app_spool_gc(now)
  expect_contains(active_gc$skipped_referenced, token)
  expect_true(dir.exists(spool))
  expect_true(dir.exists(staging))

  dsFlower:::.cleanupStaging(run_token)
  expired_gc <- dsFlower:::.app_spool_gc(now)
  expect_contains(expired_gc$removed, token)
  expect_false(dir.exists(spool))
  expect_equal(dsFlower:::.app_spool_usage(root)$uploads, 0L)
})

test_that("TTL garbage collection never expires installed catalogue apps", {
  root <- .local_app_spool()
  withr::local_options(list(dsflower.app_spool_ttl_seconds = 1))
  token <- .test_app_token(39, "usr")
  spool <- dsFlower:::.app_spool_dir(token)
  installed <- file.path(spool, "unpacked")
  dir.create(installed, mode = "0700")
  writeLines("verified", file.path(installed, "module.py"))
  now <- Sys.time()
  dsFlower:::.touch_app_activity(spool, now - 100)

  collected <- dsFlower:::.app_spool_gc(now)
  expect_false(token %in% collected$removed)
  expect_true(dir.exists(installed))
  expect_equal(dsFlower:::.app_spool_usage(root)$uploads, 1L)
})

test_that("a live run lease makes verified app bytes immutable", {
  .local_app_spool()
  token <- .test_app_token(38, "usr")
  spool <- dsFlower:::.app_spool_dir(token)
  fab <- file.path(spool, "app.fab")
  writeBin(as.raw(1), fab)
  run_token <- dsFlower:::.generate_run_token()
  dsFlower:::.ensureStagingDir(run_token)
  withr::defer(dsFlower:::.cleanupStaging(run_token))
  dsFlower:::.record_app_run_lease(spool, run_token)

  chunk <- .enc_b64(as.raw(2))
  expect_error(
    dsFlower::flowerAppPushDS(token, chunk, 1),
    "pinned by an active run"
  )
  sha <- digest::digest(file = fab, algo = "sha256")
  expect_error(
    dsFlower::flowerAppInstallDS(token, sha),
    "pinned by an active run"
  )
  expect_error(
    dsFlower::flowerAppDeleteDS(token),
    "pinned by an active run"
  )

  dsFlower:::.cleanupStaging(run_token)
  expect_true(dsFlower::flowerAppDeleteDS(token))
})

test_that("push enforces max_fab_bytes before appending", {
  .local_app_spool()
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  token <- .test_app_token(6)
  withr::defer(dsFlower::flowerAppDeleteDS(token))
  withr::local_options(list(dsflower.max_fab_bytes = length(raw) - 1))

  chunk <- .enc_b64(raw)
  expect_error(dsFlower::flowerAppPushDS(token, chunk, 0),
               "max_fab_bytes")
  expect_false(file.exists(file.path(
    dsFlower:::.app_spool_dir(token, create = FALSE), "app.fab")))
})

test_that("install rejects traversal, absolute paths, and ZIP symlinks", {
  .local_app_spool()
  skip_if(!nzchar(dsFlower:::.scan_python()), "no python for hostile ZIP fixtures")
  cases <- list(
    list(name = "../escape.py", symlink = FALSE),
    list(name = "/tmp/dsflower_absolute_escape.py", symlink = FALSE),
    list(name = "link.py", symlink = TRUE)
  )

  for (i in seq_along(cases)) {
    dir <- withr::local_tempdir()
    fab <- .make_hostile_fab(dir, cases[[i]]$name, cases[[i]]$symlink)
    expect_true(nzchar(fab) && file.exists(fab))
    raw <- readBin(fab, "raw", file.size(fab))
    token <- .test_app_token(10 + i)
    withr::defer(dsFlower::flowerAppDeleteDS(token))
    chunk <- .enc_b64(raw)
    dsFlower::flowerAppPushDS(token, chunk, 0)
    sha <- digest::digest(file = fab, algo = "sha256")
    expect_error(
      dsFlower::flowerAppInstallDS(token, sha),
      "unsafe FAB archive"
    )
  }
  expect_false(file.exists("/tmp/dsflower_absolute_escape.py"))
})

test_that("package pins recognize only regular packages with __init__.py", {
  root <- withr::local_tempdir()
  dir.create(file.path(root, "validpkg"))
  writeLines("x = 1", file.path(root, "validpkg", "__init__.py"))
  dir.create(file.path(root, "namespacepkg"))
  writeLines("x = 2", file.path(root, "namespacepkg", "module.py"))

  hashes <- dsFlower:::.compute_pkg_hashes(root)
  expect_named(hashes, "validpkg")
})

test_that("Tier-2 pinning derives one authoritative module without returning paths", {
  .local_app_spool()
  run_token <- dsFlower:::.generate_run_token()
  staging <- dsFlower:::.ensureStagingDir(run_token)
  jsonlite::write_json(list(dp_track = "egress"),
                       file.path(staging, "manifest.json"), auto_unbox = TRUE)
  handle_name <- "test_tier2_authoritative_module"
  dsFlower:::.setHandle(handle_name, list(
    data_path = "unused", run_token = run_token, staging_dir = staging,
    prepared = TRUE, node_ensured = FALSE))

  token <- .test_app_token(20, "usr")
  withr::defer(dsFlower::flowerAppDeleteDS(token))
  withr::defer(dsFlower:::.cleanupStaging(run_token))
  # Deferred callbacks are LIFO: drop the validating handle first, then its
  # staging lease, then the app that is no longer referenced by that lease.
  withr::defer(dsFlower:::.removeHandle(handle_name))
  apps <- file.path(dsFlower:::.app_spool_dir(token), "unpacked")
  dir.create(file.path(apps, "hookpkg"), recursive = TRUE)
  writeLines(c("def initial_arrays(cfg, n): return []",
               "def local_update(old, X, y, cfg): return old"),
             file.path(apps, "hookpkg", "__init__.py"))

  res <- dsFlower::flowerTier2PinDS(handle_name, token)
  expect_true(res$ok)
  expect_equal(res$user_module, "hookpkg")
  expect_null(res$user_path)
  manifest <- jsonlite::fromJSON(file.path(staging, "manifest.json"),
                                 simplifyVector = FALSE)
  expect_equal(manifest[["user-module"]], "hookpkg")
  expect_true(file.exists(file.path(
    dsFlower:::.app_spool_dir(token, create = FALSE),
    ".active_runs", run_token)))
})

test_that("Tier-2 pinning keeps private staging diagnostics out of errors", {
  .local_app_spool()
  run_token <- dsFlower:::.generate_run_token()
  staging <- dsFlower:::.ensureStagingDir(run_token)
  handle_name <- "test_tier2_private_manifest_error"
  dsFlower:::.setHandle(handle_name, list(
    data_path = "unused", run_token = run_token, staging_dir = staging,
    prepared = TRUE, node_ensured = FALSE))

  token <- .test_app_token(22, "usr")
  withr::defer(dsFlower::flowerAppDeleteDS(token))
  withr::defer(dsFlower:::.cleanupStaging(run_token))
  withr::defer(dsFlower:::.removeHandle(handle_name))
  apps <- file.path(dsFlower:::.app_spool_dir(token), "unpacked")
  dir.create(file.path(apps, "hookpkg"), recursive = TRUE)
  writeLines("x = 1", file.path(apps, "hookpkg", "__init__.py"))

  warnings <- character()
  error <- tryCatch(
    withCallingHandlers(
      dsFlower::flowerTier2PinDS(handle_name, token),
      warning = function(w) {
        warnings <<- c(warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }
    ),
    error = conditionMessage
  )
  expect_identical(error, "Prepared run manifest is unreadable.")
  expect_length(warnings, 0L)
  expect_false(any(grepl(staging, c(error, warnings), fixed = TRUE)))
})

test_that("Tier-2 pinning rejects ambiguous and reserved package sets", {
  .local_app_spool()
  run_token <- dsFlower:::.generate_run_token()
  staging <- dsFlower:::.ensureStagingDir(run_token)
  jsonlite::write_json(list(dp_track = "egress"),
                       file.path(staging, "manifest.json"), auto_unbox = TRUE)
  handle_name <- "test_tier2_reject_packages"
  dsFlower:::.setHandle(handle_name, list(
    data_path = "unused", run_token = run_token, staging_dir = staging,
    prepared = TRUE, node_ensured = FALSE))
  withr::defer(dsFlower:::.removeHandle(handle_name))

  token <- .test_app_token(21, "usr")
  withr::defer(dsFlower::flowerAppDeleteDS(token))
  apps <- file.path(dsFlower:::.app_spool_dir(token), "unpacked")
  for (pkg in c("onepkg", "twopkg")) {
    dir.create(file.path(apps, pkg), recursive = TRUE)
    writeLines("x = 1", file.path(apps, pkg, "__init__.py"))
  }
  expect_error(dsFlower::flowerTier2PinDS(handle_name, token), "exactly one")

  unlink(apps, recursive = TRUE)
  dir.create(file.path(apps, "dsflower_runner"), recursive = TRUE)
  writeLines("x = 1", file.path(apps, "dsflower_runner", "__init__.py"))
  expect_error(dsFlower::flowerTier2PinDS(handle_name, token), "reserved")
})
