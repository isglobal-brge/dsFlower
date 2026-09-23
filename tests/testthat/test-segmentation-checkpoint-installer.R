checkpoint_installer <- function(bundle = system.file(
    "extdata", "segmentation-public-checkpoints", package = "dsFlower")) {
  script <- testthat::test_path("..", "..", "tools",
                                "install-segmentation-public-checkpoints.R")
  testthat::skip_if_not(file.exists(script), "installer is a source-review tool")
  env <- new.env(parent = globalenv())
  env$system.file <- function(...) bundle
  sys.source(script, envir = env)
  env$install_segmentation_public_checkpoints
}

test_that("checkpoint installation rejects missing originals before accessing node state", {
  recovered <- withr::local_tempdir()
  withr::local_envvar(DSFLOWER_NODE_SECRET_FILE = file.path(recovered, "missing-secret"))
  policy <- c(existing = strrep("a", 64L))
  withr::local_options(dsflower.segmentation_public_checkpoints = policy)
  install <- checkpoint_installer()
  expect_error(install(recovered), "Public artifact missing or SHA-256/size mismatch:.*seed20260919.npz")
  expect_length(list.files(recovered, all.files = TRUE, no.. = TRUE), 0L)
  expect_identical(getOption("dsflower.segmentation_public_checkpoints"), policy)
})

test_that("checkpoint installation rejects wrong sizes and same-size forged originals", {
  recovered <- withr::local_tempdir()
  withr::local_envvar(DSFLOWER_NODE_SECRET_FILE = file.path(recovered, "missing-secret"))
  install <- checkpoint_installer()
  checkpoint <- file.path(recovered, "seed20260919.npz")
  for (size in c(1L, 39510L)) {
    # Temporary corrupt input only: no generated checkpoint is shipped or admitted.
    writeBin(raw(size), checkpoint)
    expect_error(install(recovered), "Public artifact missing or SHA-256/size mismatch:.*seed20260919.npz")
    expect_identical(list.files(recovered, all.files = TRUE, no.. = TRUE),
                     "seed20260919.npz")
  }
})

test_that("checkpoint installation checks manifest and evidence pins before copying", {
  recovered <- withr::local_tempdir()
  copied <- withr::local_tempdir()
  bundle <- system.file("extdata", "segmentation-public-checkpoints", package = "dsFlower")
  expect_true(file.copy(bundle, copied, recursive = TRUE))
  bundle <- file.path(copied, basename(bundle))
  install <- checkpoint_installer(bundle)
  directory <- file.path(bundle, "busi-v5-epochs60-seed20260919")
  manifest <- file.path(directory, "manifest.json")
  original <- readBin(manifest, "raw", n = file.info(manifest)$size)
  writeLines("{}", manifest)
  expect_error(install(recovered), "Public artifact missing or SHA-256/size mismatch:.*manifest.json")
  writeBin(original, manifest)
  writeLines("changed evidence", file.path(directory, "protocol.md"))
  expect_error(install(recovered), "Public artifact missing or SHA-256/size mismatch:.*protocol.md")
  expect_length(list.files(recovered, all.files = TRUE, no.. = TRUE), 0L)
})

# Exercise installer control flow at a mocked trusted-verifier boundary. These
# temporary corrupt NPZ bytes are never passed off as recovered BUSI artifacts.
local_checkpoint_install_fixture <- function(.local_envir = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .local_envir)
  source_bundle <- system.file("extdata", "segmentation-public-checkpoints",
                               package = "dsFlower")
  stopifnot(file.copy(source_bundle, root, recursive = TRUE))
  bundle <- file.path(root, basename(source_bundle))
  recovered <- file.path(root, "recovered")
  dir.create(recovered)
  pins <- jsonlite::fromJSON(file.path(bundle, "allowlist.json"))
  for (id in names(pins)) {
    path <- file.path(bundle, id, "manifest.json")
    manifest <- jsonlite::fromJSON(path, simplifyVector = FALSE)
    checkpoint <- file.path(recovered, paste0(sub(".*seed", "seed", id), ".npz"))
    writeBin(raw(39510L), checkpoint)
    manifest$checkpoint$sha256 <- digest::digest(checkpoint, algo = "sha256", file = TRUE)
    writeLines(jsonlite::toJSON(manifest, auto_unbox = TRUE), path)
    pins[[id]] <- digest::digest(path, algo = "sha256", file = TRUE)
  }
  writeLines(jsonlite::toJSON(pins, auto_unbox = TRUE), file.path(bundle, "allowlist.json"))
  state <- file.path(root, "node")
  dir.create(state, mode = "0700")
  secret <- file.path(state, "noise_root")
  writeChar(strrep("a", 64L), secret, eos = NULL)
  Sys.chmod(secret, "0600")
  testthat::local_mocked_bindings(
    .node_secret_path = function() secret,
    .resolve_framework_runtime = function(...) list(python = "mock-python"),
    .package = "dsFlower", .env = .local_envir)
  testthat::local_mocked_bindings(
    run = function(...) list(status = 0L),
    .package = "processx", .env = .local_envir)
  list(install = checkpoint_installer(bundle), recovered = recovered,
       registry = file.path(state, "segmentation-public-checkpoints"),
       pins = unlist(pins, use.names = TRUE))
}

test_that("checkpoint installer preserves existing revisions and emits no policy on failure", {
  fixture <- local_checkpoint_install_fixture()
  dir.create(fixture$registry, mode = "0700")
  existing <- file.path(fixture$registry, names(fixture$pins)[[1L]])
  dir.create(existing, mode = "0700")
  writeLines("keep", file.path(existing, "sentinel"))
  output <- capture.output(expect_error(fixture$install(fixture$recovered),
                                        "refusing to overwrite"))
  expect_identical(readLines(file.path(existing, "sentinel")), "keep")
  expect_identical(list.files(fixture$registry), basename(existing))
  expect_false(any(grepl("options\\(", output)))
})

test_that("checkpoint installer rolls back its destinations if final verification fails", {
  fixture <- local_checkpoint_install_fixture()
  testthat::local_mocked_bindings(
    .verifySegmentationPublicCheckpoint = function(..., registry_root) {
      if (identical(registry_root, fixture$registry)) stop("late verification rejected")
      list()
    }, .package = "dsFlower")
  output <- capture.output(expect_error(fixture$install(fixture$recovered),
                                        "late verification rejected"))
  expect_length(list.files(fixture$registry), 0L)
  expect_false(any(grepl("options\\(", output)))
  expect_identical(list.files(dirname(fixture$registry), all.files = TRUE, no.. = TRUE),
                   c("noise_root", "segmentation-public-checkpoints"))
})

test_that("checkpoint installer prints usable policy only after all verification succeeds", {
  fixture <- local_checkpoint_install_fixture()
  verified <- character()
  testthat::local_mocked_bindings(
    .verifySegmentationPublicCheckpoint = function(..., checkpoint_id, registry_root) {
      verified <<- c(verified, registry_root)
      list()
    }, .package = "dsFlower")
  withr::local_options(dsflower.segmentation_public_checkpoints = character())
  output <- capture.output(pins <- fixture$install(fixture$recovered))
  expect_identical(pins, fixture$pins)
  expect_identical(list.files(fixture$registry), names(fixture$pins))
  expect_identical(length(verified), 6L)
  expect_identical(sum(verified == fixture$registry), 3L)
  expect_identical(getOption("dsflower.segmentation_public_checkpoints"), character())
  policy_start <- grep("^options\\(", output)
  expect_length(policy_start, 1L)
  eval(parse(text = paste(output[policy_start:length(output)], collapse = "\n")))
  expect_identical(getOption("dsflower.segmentation_public_checkpoints"), fixture$pins)
})
