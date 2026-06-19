# Tests for R/app_store.R -- Tier-2 uploaded-app receive + verify over DSI

.enc_b64 <- function(r) {
  b <- gsub("[\r\n]", "", jsonlite::base64_enc(r))
  b <- gsub("\\+", "-", b); b <- gsub("/", "_", b); b <- gsub("=+$", "", b)
  paste0("B64:", b)
}

.make_fab <- function(dir) {
  writeLines("print('hi')", file.path(dir, "client_app.py"))
  fab <- file.path(dir, "app.fab")
  wd <- getwd(); setwd(dir); on.exit(setwd(wd))
  utils::zip("app.fab", files = "client_app.py", flags = "-q")
  file.path(dir, "app.fab")
}

test_that("chunked push + install verifies a FAB by sha256 and unpacks it", {
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  sha <- digest::digest(file = fab, algo = "sha256")
  token <- "test_appstore_ok"
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  half <- floor(length(raw) / 2)
  r1 <- dsFlower::flowerAppPushDS(token, .enc_b64(raw[1:half]), 0)
  expect_equal(r1$size, half)
  r2 <- dsFlower::flowerAppPushDS(token, .enc_b64(raw[(half + 1):length(raw)]), half)
  expect_equal(r2$size, length(raw))

  # idempotent: re-pushing the last chunk at the same offset does not grow the file
  r2b <- dsFlower::flowerAppPushDS(token, .enc_b64(raw[(half + 1):length(raw)]), half)
  expect_equal(r2b$size, length(raw))

  res <- dsFlower::flowerAppInstallDS(token, sha)
  expect_true(res$ok)
  expect_equal(res$sha256, sha)
  expect_true(file.exists(file.path(res$path, "client_app.py")))
})

test_that("install rejects a hash mismatch and destroys the spool", {
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  token <- "test_appstore_bad"
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  dsFlower::flowerAppPushDS(token, .enc_b64(raw), 0)
  expect_error(
    dsFlower::flowerAppInstallDS(token, "deadbeef_wrong_hash"),
    "integrity check"
  )
  # spool destroyed -> a second install finds nothing
  expect_error(dsFlower::flowerAppInstallDS(token, "x"), "No uploaded app")
})

test_that("install rejects an app that fails the exfiltration scan", {
  skip_if(!nzchar(dsFlower:::.scan_python()), "no python for scan")
  dir <- withr::local_tempdir()
  writeLines(c("import socket", "x = 1"), file.path(dir, "client_app.py"))
  wd <- getwd(); setwd(dir); utils::zip("app.fab", files = "client_app.py", flags = "-q"); setwd(wd)
  fab <- file.path(dir, "app.fab")
  raw <- readBin(fab, "raw", file.size(fab))
  sha <- digest::digest(file = fab, algo = "sha256")
  token <- "test_appstore_scan"
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  dsFlower::flowerAppPushDS(token, .enc_b64(raw), 0)
  expect_error(dsFlower::flowerAppInstallDS(token, sha), "safety scan")
})

test_that("install enforces the max_fab_bytes cap", {
  dir <- withr::local_tempdir()
  fab <- .make_fab(dir)
  raw <- readBin(fab, "raw", file.size(fab))
  token <- "test_appstore_cap"
  withr::defer(dsFlower::flowerAppDeleteDS(token))

  dsFlower::flowerAppPushDS(token, .enc_b64(raw), 0)
  withr::local_options(list(dsflower.max_fab_bytes = 1))
  expect_error(
    dsFlower::flowerAppInstallDS(token, digest::digest(file = fab, algo = "sha256")),
    "max_fab_bytes"
  )
})
