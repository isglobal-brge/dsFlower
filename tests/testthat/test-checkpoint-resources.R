checkpoint_test_summary <- function(directory = "/private/snapshot") {
  list(snapshot_directory = directory, checkpoint_sha256 = strrep("b", 64),
    encoder_sha256 = strrep("c", 64), identity_version = "dsflower-public-initialisation-identity/v1",
    tensor_schema = list(list(name = "arr_0", shape = list(1L), dtype = "float32",
                              sha256 = strrep("d", 64))),
    provenance = list(manifest_sha256 = strrep("a", 64),
      manifest = list(dataset = list(name = "public synthetic fixture"),
                      licence = list(declaration = "CC0-1.0"))))
}

checkpoint_test_options <- function(.local_envir = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .local_envir)
  withr::local_options(list(dsflower.checkpoint_cache_dir = file.path(root, "checkpoints"),
    dsflower.public_initialisation = "analyst_or_resource",
    dsflower.node_secret_path = file.path(root, "privacy", "noise_root")),
    .local_envir = .local_envir)
  withr::local_envvar(c(DSFLOWER_NODE_SECRET_FILE = "",
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"), .local_envir = .local_envir)
  root
}

checkpoint_test_resource <- function(file) {
  resourcer::newResource(name = "fixture", url = paste0("file://", file),
    identity = "must-be-forgotten", secret = "must-also-be-forgotten",
    format = paste0("dsflower-checkpoint-v1:", digest::digest(file = file, algo = "sha256")))
}

checkpoint_test_b64 <- function(bytes) {
  paste0("B64:", sub("=+$", "", chartr("+/", "-_",
    gsub("[\r\n]", "", jsonlite::base64_enc(bytes)))))
}

test_that("checkpoint resolver accepts only the exact digest-bearing resource format", {
  resolver <- CheckpointResourceResolver$new()
  good <- resourcer::newResource(url = "file:///approved.zip",
    format = paste0("dsflower-checkpoint-v1:", strrep("a", 64)))
  expect_true(resolver$isFor(good))
  expect_s3_class(resourcer::resolveResource(good), "CheckpointResourceResolver")
  for (value in list(NULL, "dsflower-checkpoint-v1", paste0(good$format, "\n"),
                    toupper(good$format), c(good$format, good$format), "csv")) {
    good$format <- value
    expect_false(resolver$isFor(good))
  }
})

test_that("public initialisation policy is server-owned with contract fallback", {
  withr::local_options(list(dsflower.public_initialisation = NULL,
    default.dsflower.public_initialisation = "resource_only",
    dsflower.public_initialisation.pytorch_resnet18_segmentation = NULL))
  expect_identical(dsFlower:::.public_initialisation_policy(), "resource_only")
  expect_error(dsFlower:::.require_checkpoint_policy("analyst-declared"), "custodian policy")
  expect_identical(dsFlower:::.require_checkpoint_policy("resource"), "resource_only")
  withr::local_options(dsflower.public_initialisation.pytorch_resnet18_segmentation = "none")
  expect_error(dsFlower:::.require_checkpoint_policy("resource"), "custodian policy")
  expect_error(dsFlower:::.require_checkpoint_policy("analyst-declared"), "custodian policy")
  expect_identical(flowerPrivacyPolicyDS()$public_initialisation$contracts[[1]], "none")
  withr::local_options(dsflower.public_initialisation.pytorch_resnet18_segmentation = "invalid")
  expect_error(dsFlower:::.public_initialisation_policy(), "must be")
  for (key in c("dsflower.public_initialisation", "public_initialisation",
                "public-initialisation-policy", "public-initialisation-directory",
                "public-initialisation-origin", "initialisation")) {
    bad <- stats::setNames(list("analyst_or_resource"), key)
    expect_error(dsFlower:::.validate_client_run_config(bad), "server-owned|unsupported")
  }
})

test_that("cache custody rejects staging, Hook and node-secret locations", {
  root <- checkpoint_test_options()
  cache <- dsFlower:::.checkpoint_cache_root()
  expect_identical(as.integer(file.info(cache)$mode), strtoi("700", 8L))
  for (path in c(file.path(tempdir(), "dsflower", "checkpoint"),
                 file.path(root, "privacy", "checkpoint"))) {
    withr::with_options(list(dsflower.checkpoint_cache_dir = path), {
      expect_error(dsFlower:::.checkpoint_cache_root(), "outside")
    })
  }
  Sys.chmod(cache, "0755")
  expect_error(dsFlower:::.checkpoint_cache_root(), "0700")
})

test_that("resources snapshot eagerly, discard credentials and refuse table conversion", {
  root <- checkpoint_test_options()
  archive <- file.path(root, "source.zip")
  writeBin(charToRaw("public archive fixture"), archive)
  calls <- 0L
  local_mocked_bindings(.checkpoint_verify = function(action, path, expected_sha256, ...) {
    calls <<- calls + 1L
    expect_identical(action, "admit")
    expect_false(identical(path, archive))
    expect_identical(readBin(path, "raw", n = 100), charToRaw("public archive fixture"))
    expect_identical(expected_sha256, digest::digest(file = archive, algo = "sha256"))
    expect_identical(as.integer(file.info(path)$mode), strtoi("600", 8L))
    checkpoint_test_summary()
  }, .package = "dsFlower")
  client <- resourcer::newResourceClient(checkpoint_test_resource(archive))
  expect_identical(calls, 1L)
  descriptor <- client$getResource()
  expect_identical(descriptor$url, "")
  expect_null(descriptor$identity)
  expect_null(descriptor$secret)
  expect_error(client$asDataFrame(), "flowerCheckpointInitDS")
  expect_error(client$asTbl(), "flowerCheckpointInitDS")
  expect_error(as_flower_dataset(client), "flowerCheckpointInitDS")
  expect_length(list.files(dsFlower:::.checkpoint_cache_root(), all.files = TRUE,
                           pattern = "^\\.acquire-"), 0L)
})

test_that("S3 requires an explicitly bounded registered file getter", {
  checkpoint_test_options()
  withr::local_options(resourcer.file.getters = list())
  resource <- resourcer::newResource(url = "s3://approved-bucket/checkpoint.zip",
    format = paste0("dsflower-checkpoint-v1:", strrep("a", 64)))
  expect_error(CheckpointResourceClient$new(resource), "downloadFileBounded")
  plain <- R6::R6Class("CheckpointTestUnboundedS3Getter",
    inherit = resourcer::FileResourceGetter, public = list(
      isFor = function(resource) startsWith(resource$url, "s3:"),
      downloadFile = function(...) stop("unbounded method reached")))$new()
  resourcer::registerFileResourceGetter(plain)
  expect_error(CheckpointResourceClient$new(resource), "downloadFileBounded")
  resourcer::unregisterFileResourceGetter("CheckpointTestUnboundedS3Getter")
  bounded <- R6::R6Class("CheckpointTestBoundedS3Getter",
    inherit = resourcer::FileResourceGetter, public = list(
      isFor = function(resource) startsWith(resource$url, "s3:"),
      downloadFileBounded = function(resource, destination, max_bytes, timeout) {
        expect_identical(max_bytes, 64 * 1024^2)
        expect_identical(timeout, 120)
        expect_identical(basename(destination), "bundle.zip")
        expect_identical(as.integer(file.info(dirname(destination))$mode), strtoi("700", 8L))
        writeBin(charToRaw("bounded extension fixture"), destination)
        destination
      }))$new()
  resourcer::registerFileResourceGetter(bounded)
  local_mocked_bindings(.checkpoint_verify = function(...) checkpoint_test_summary(),
                       .package = "dsFlower")
  expect_s3_class(CheckpointResourceClient$new(resource), "CheckpointResourceClient")
})

test_that("HTTP acquisition checks status and enforces its declared byte limit", {
  python <- unname(Sys.which("python3"))
  skip_if(!nzchar(python), "Python interpreter is unavailable")
  checkpoint_test_options()
  code <- paste(
    "from http.server import BaseHTTPRequestHandler, HTTPServer",
    "class Handler(BaseHTTPRequestHandler):",
    " def do_GET(self):",
    "  code = 404 if self.path == '/missing' else 200",
    "  self.send_response(code)",
    "  self.send_header('Content-Length', str(67108865 if self.path == '/huge' else 7))",
    "  self.end_headers()",
    "  self.wfile.write(b'fixture')",
    " def log_message(self, *args): pass",
    "server = HTTPServer(('127.0.0.1', 0), Handler)",
    "print(server.server_address[1], flush=True)",
    "server.serve_forever()", sep = "\n")
  server <- processx::process$new(python, c("-I", "-c", code), stdout = "|", stderr = "|")
  withr::defer(server$kill())
  server$poll_io(5000)
  port <- suppressWarnings(as.integer(server$read_output_lines(n = 1L)))
  expect_length(port, 1L)
  expect_false(is.na(port))
  work <- dsFlower:::.checkpoint_work_directory()
  withr::defer(unlink(work, recursive = TRUE))
  getter <- dsFlower:::.CheckpointFileGetter$new(work)
  resource <- resourcer::newResource(url = paste0("http://127.0.0.1:", port, "/ok"))
  downloaded <- getter$downloadFile(resource)
  expect_identical(readBin(downloaded$path, "raw", n = 10L), charToRaw("fixture"))
  resource$url <- paste0("http://127.0.0.1:", port, "/missing")
  expect_error(getter$downloadFile(resource), "404")
  resource$url <- paste0("http://127.0.0.1:", port, "/huge")
  expect_error(getter$downloadFile(resource), "maximum|large|size")
})

test_that("resource admission is typed, session-bound and independent of symbol rebinding", {
  root <- checkpoint_test_options()
  archive <- file.path(root, "source.zip")
  writeBin(charToRaw("fixture"), archive)
  local_mocked_bindings(.checkpoint_verify = function(...) checkpoint_test_summary(),
                       .package = "dsFlower")
  owner <- new.env(parent = globalenv())
  owner$CKPT_R <- CheckpointResourceClient$new(checkpoint_test_resource(archive))
  owner$CKPT <- evalq(flowerCheckpointInitDS("CKPT_R"), owner)
  owner$CKPT_R <- list(url = "file:///arbitrary")
  expect_identical(evalq(flowerCheckpointStatusDS("CKPT"), owner)$origin, "resource")
  other <- new.env(parent = globalenv())
  other$CKPT <- owner$CKPT
  expect_error(evalq(flowerCheckpointStatusDS("CKPT"), other), "cross-session")
  expect_error(evalq(flowerCheckpointInitDS("CKPT_R"), owner), "assigned CheckpointResourceClient")
  for (forged in list(checkpoint_test_resource(archive),
      structure(list(getSnapshot = function() checkpoint_test_summary()),
                class = c("CheckpointResourceClient", "ResourceClient", "R6")),
      resourcer::ResourceClient$new(checkpoint_test_resource(archive)))) {
    owner$CKPT_R <- forged
    expect_error(evalq(flowerCheckpointInitDS("CKPT_R"), owner), "assigned CheckpointResourceClient")
  }
  expect_error(evalq(flowerCheckpointInitDS("MISSING"), owner), "not assigned")
  for (value in c("file:///x", "/path/to/bundle", "https://host/bundle")) {
    owner$input <- value
    expect_error(evalq(flowerCheckpointInitDS(input), owner), "not assigned")
  }
  withr::local_options(dsflower.public_initialisation = "none")
  expect_error(evalq(flowerCheckpointStatusDS("CKPT"), owner), "custodian policy")
})

test_that("analyst upload chunks are bounded, retryable and sealed in one session", {
  checkpoint_test_options()
  owner <- new.env(parent = globalenv())
  bytes <- charToRaw("a bounded synthetic public archive")
  owner$manifest <- strrep("a", 64)
  owner$bundle <- digest::digest(bytes, algo = "sha256", serialize = FALSE)
  owner$total <- length(bytes)
  owner$chunk <- checkpoint_test_b64(bytes)
  admission <- evalq(flowerCheckpointUploadDS("begin", manifest_sha256 = manifest,
    bundle_sha256 = bundle, total_bytes = total), owner)
  owner$token <- admission$upload_id
  expect_identical(admission$next_index, 1L)
  expect_error(evalq(flowerCheckpointUploadDS("finish", token), owner), "incomplete")
  first <- evalq(flowerCheckpointUploadDS("chunk", token, chunk, 1L), owner)
  expect_identical(first$next_index, 2L)
  expect_identical(evalq(flowerCheckpointUploadDS("chunk", token, chunk, 1L), owner), first)
  owner$changed <- checkpoint_test_b64(charToRaw("changed"))
  expect_error(evalq(flowerCheckpointUploadDS("chunk", token, changed, 1L), owner), "changed its bytes")
  other <- new.env(parent = globalenv())
  other$token <- owner$token
  expect_error(evalq(flowerCheckpointUploadDS("finish", token), other), "cross-session")
  local_mocked_bindings(.checkpoint_verify = function(action, path, expected_sha256, ...) {
    expect_identical(readBin(path, "raw", n = 100), bytes)
    expect_identical(expected_sha256, owner$bundle)
    checkpoint_test_summary()
  }, .package = "dsFlower")
  finished <- evalq(flowerCheckpointUploadDS("finish", token), owner)
  expect_identical(finished$public_initialisation$origin, "analyst-declared")
  expect_false(any(c("snapshot_directory", "bundle_sha256", "checkpoint_base64") %in%
    names(finished$public_initialisation)))
  expect_identical(evalq(flowerCheckpointUploadDS("finish", token), owner), finished)
  expect_length(list.files(dsFlower:::.checkpoint_cache_root(), all.files = TRUE,
                           pattern = "^\\.acquire-"), 0L)
  withr::with_options(list(dsflower.public_initialisation = "none"), {
    expect_true(evalq(flowerCheckpointUploadDS("abort", token), owner)$aborted)
  })
})

test_that("analyst denied policies allocate no upload state or private staging", {
  checkpoint_test_options()
  for (policy in c("resource_only", "none")) {
    withr::with_options(list(dsflower.public_initialisation = policy), {
      local_mocked_bindings(.checkpoint_work_directory = function(...) stop("allocation reached"),
        .package = "dsFlower")
      expect_error(flowerCheckpointUploadDS("begin"), "custodian policy")
    })
  }
})

test_that("upload identity mismatch fails closed and clears its acquisition directory", {
  checkpoint_test_options()
  owner <- new.env(parent = globalenv())
  owner$manifest <- strrep("f", 64)
  owner$bundle <- strrep("b", 64)
  owner$chunk <- checkpoint_test_b64(as.raw(1))
  owner$token <- evalq(flowerCheckpointUploadDS("begin", manifest_sha256 = manifest,
    bundle_sha256 = bundle, total_bytes = 1L), owner)$upload_id
  evalq(flowerCheckpointUploadDS("chunk", token, chunk, 1L), owner)
  local_mocked_bindings(.checkpoint_verify = function(...) checkpoint_test_summary(),
                       .package = "dsFlower")
  expect_error(evalq(flowerCheckpointUploadDS("finish", token), owner), "manifest digest")
  expect_error(evalq(flowerCheckpointUploadDS("finish", token), owner), "Unknown")
  expect_length(list.files(dsFlower:::.checkpoint_cache_root(), all.files = TRUE,
                           pattern = "^\\.acquire-"), 0L)
  owner$huge <- dsFlower:::.CHECKPOINT_MAX_BYTES + 1
  expect_error(evalq(flowerCheckpointUploadDS("begin", manifest_sha256 = manifest,
    bundle_sha256 = bundle, total_bytes = huge), owner), "Invalid declared")
})

test_that("resource-only and none policies reject preparation before any staging", {
  roots <- local_segmentation_roots()
  owner <- new.env(parent = globalenv())
  owner$TRAIN <- dsFlower:::.registerHandle(mock_handle(table_data = segmentation_table()), owner)
  owner$config <- segmentation_config()
  local_mocked_bindings(.checkpoint_verify = function(...) stop("verifier reached"),
    .generate_run_token = function(...) stop("private staging reached"), .package = "dsFlower")
  for (policy in c("resource_only", "none")) {
    withr::with_options(list(dsflower.public_initialisation = policy), {
      owner$config[["segmentation-decoder-init"]] <- paste0("client:cku_", strrep("a", 32))
      expect_error(evalq(flowerPrepareRunDS("TRAIN", "mask_path", NULL, config), owner), "custodian policy")
      if (identical(policy, "none")) {
        owner$config[["segmentation-decoder-init"]] <- "resource:CKPT"
        expect_error(evalq(flowerPrepareRunDS("TRAIN", "mask_path", NULL, config), owner), "custodian policy")
      }
    })
  }
})

test_that("trusted verification validates identity and never accepts partial subprocess output", {
  root <- checkpoint_test_options()
  cache <- dsFlower:::.checkpoint_cache_root()
  directory <- file.path(cache, strrep("a", 64))
  dir.create(directory, mode = "0700")
  summary <- checkpoint_test_summary(directory)
  run <- function(payload, status = 0L) dsFlower:::.checkpoint_verify("verify", directory,
    strrep("a", 64), runtime = list(python = "trusted-python", venv_path = "trusted"),
    runner_dir = root, run_probe = function(command, args, env, error_on_status, timeout) {
      expect_identical(command, "trusted-python")
      expect_identical(args[1:2], c("-I", "-c"))
      expect_false(any(c("HOME", "TORCH_HOME") %in% names(env)))
      expect_identical(env[["PYTHONPATH"]], "")
      list(status = status, stdout = as.character(jsonlite::toJSON(payload,
        auto_unbox = TRUE, null = "null")))
    })
  expect_equal(run(summary), summary)
  expect_error(run(summary, 1L), "verification failed")
  expect_error(run(list()), "verification failed")
  summary$provenance$manifest_sha256 <- strrep("f", 64)
  expect_error(run(summary), "different admitted identity")
})

test_that("preparation refuses legacy selectors, paths, unassigned and forged handles", {
  owner <- new.env(parent = globalenv())
  for (selector in c("public:busi", "resource:https://host/file", "resource:/tmp/file",
                      "client:/tmp/bundle", "resource:UNASSIGNED")) {
    config <- list("segmentation-decoder-init" = selector)
    expect_error(dsFlower:::.normalizeSegmentationDecoderInit(config, owner),
                 "decoder_init|unassigned")
  }
  owner$CKPT <- list(capability = paste0("ckph_", strrep("a", 32)))
  config <- list("segmentation-decoder-init" = "resource:CKPT")
  expect_error(dsFlower:::.normalizeSegmentationDecoderInit(config, owner), "typed handle")
})

test_that("initialisation identity is stored in manifest without status byte export", {
  roots <- local_segmentation_roots()
  checkpoint_test_options()
  withr::local_envvar(c(DSFLOWER_NODE_SECRET_FILE = file.path(roots$root, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"))
  owner <- new.env(parent = globalenv())
  snapshot <- checkpoint_test_summary()
  owner$CKPT <- dsFlower:::.checkpoint_reference(snapshot, owner)
  owner$TRAIN <- dsFlower:::.registerHandle(mock_handle(table_data = segmentation_table()), owner)
  owner$config <- segmentation_config()
  owner$config[["segmentation-decoder-init"]] <- "resource:CKPT"
  owner$config[["model-spec-b64"]] <- gsub("[\r\n]", "", jsonlite::base64_enc(
    charToRaw('{"kind":"sequential","layers":[]}')))
  local_mocked_bindings(.checkpoint_verify = function(...) snapshot, .package = "dsFlower")
  evalq(flowerPrepareRunDS("TRAIN", "mask_path", NULL, config), owner)
  handle <- evalq(dsFlower:::.getHandle("TRAIN"), owner)
  withr::defer(dsFlower:::.cleanupStaging(handle$run_token))
  manifest <- jsonlite::fromJSON(file.path(handle$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(manifest$initialisation, paste0("resource:", strrep("a", 64)))
  expect_identical(manifest[["segmentation-decoder-init"]], "resource")
  expect_identical(manifest[["public-initialisation-policy"]], "analyst_or_resource")
  expect_identical(manifest[["public-initialisation-checkpoint-sha256"]], strrep("b", 64))
  status <- evalq(flowerStatusDS("TRAIN"), owner)$public_initialisation
  expect_identical(status$origin, "resource")
  encoded <- jsonlite::toJSON(status, auto_unbox = TRUE)
  expect_false(grepl("checkpoint_base64|snapshot_directory|private/snapshot|secret|CKPT", encoded))
  local_mocked_bindings(.checkpoint_verify = function(...) stop("digest mismatch"),
    .generate_run_token = function(...) stop("private staging reached"), .package = "dsFlower")
  expect_error(evalq(flowerPrepareRunDS("TRAIN", "mask_path", NULL, config), owner), "digest mismatch")
})
