# Tests for R/interface.R — DataSHIELD Methods

local_interface_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-interface-state-")
  dir.create(state_dir, recursive = TRUE)
  if (.Platform$OS.type == "windows") {
    dsFlower:::.windows_set_private_acl(state_dir, is_directory = TRUE)
  }
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
}

vision_validation_config <- function(n_classes = 3L) {
  list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = if (n_classes == 2L) "binary" else "multiclass",
    "validation-bins" = 24L, "task-type" = "classification",
    "loss-name" = "cross_entropy", "model-spec-b64" = "e30=",
    "num-server-rounds" = 1L, "num-features" = 1024L,
    "num-classes" = as.integer(n_classes), "num-labels" = 2L,
    "target-levels" = paste0("class-", seq_len(n_classes)),
    "data_type" = "image", "backbone" = "densenet121_3d",
    "image-size" = 128L,
    "vision-extractor-profile" =
      "dsflower-densenet121-monai-seed0-extractor-v1",
    "validation-artifact-format" = "pytorch-state-dict-v1",
    "validation-artifact-sha256" = strrep("a", 64L),
    "validation-artifact-size-bytes" = 4096L)
}

test_that("flowerPingDS returns correct structure", {
  result <- flowerPingDS()
  expect_type(result, "list")
  expect_equal(result$status, "ok")
  expect_equal(result$package, "dsFlower")
  expect_true(nchar(result$version) > 0)
  expect_true(nchar(result$timestamp) > 0)
})

test_that(".getHandle errors for missing symbol", {
  expect_error(
    dsFlower:::.getHandle("nonexistent_symbol"),
    "No Flower handle"
  )
})

test_that("Handle CRUD operations work", {
  # Create and set
  handle <- mock_handle(data_path = "/tmp/test.csv")
  reference <- dsFlower:::.setHandle("test_crud", handle)
  expect_named(reference, "capability")
  expect_match(reference$capability, "^hdl_[0-9a-f]{32}$")
  expect_false("data_path" %in% names(reference))

  # Get
  retrieved <- dsFlower:::.getHandle("test_crud")
  expect_equal(retrieved$data_path, "/tmp/test.csv")

  # Remove
  dsFlower:::.removeHandle("test_crud")
  expect_error(dsFlower:::.getHandle("test_crud"), "No Flower handle")
})

test_that("forged handle fields never become authoritative", {
  victim <- withr::local_tempdir()
  marker <- file.path(victim, "keep.txt")
  writeLines("keep", marker)
  forged_handle <- list(
    data_path = marker,
    run_token = "../../victim",
    staging_dir = victim,
    prepared = TRUE
  )
  expect_error(dsFlower:::.getHandle("forged_handle"), "forged")
  expect_error(flowerCleanupRunDS("forged_handle"), "forged")
  expect_true(file.exists(marker))

  unknown_handle <- structure(
    list(capability = paste0("hdl_", strrep("a", 32))),
    class = "dsflower_handle_ref")
  expect_error(dsFlower:::.getHandle("unknown_handle"), "stale")

  forged_fields_handle <- dsFlower:::.setHandle(
    "forged_fields_handle", mock_handle(data_path = "authoritative.csv"))
  forged_fields_handle$data_path <- marker
  forged_fields_handle$run_token <- "../../victim"
  expect_equal(
    dsFlower:::.getHandle("forged_fields_handle")$data_path,
    "authoritative.csv"
  )
  dsFlower:::.removeHandle("forged_fields_handle")
  expect_true(file.exists(marker))
})

test_that("handle capabilities are stale after destroy and bound to one session", {
  stale_handle <- dsFlower:::.setHandle("stale_handle", mock_handle())
  saved <- stale_handle
  dsFlower:::.removeHandle("stale_handle")
  stale_handle <- saved
  expect_error(dsFlower:::.getHandle("stale_handle"), "stale")

  owner_a <- new.env(parent = globalenv())
  owner_b <- new.env(parent = globalenv())
  reference <- dsFlower:::.registerHandle(mock_handle(), owner_env = owner_a)
  assign("owned_handle", reference, envir = owner_a)
  assign("owned_handle", reference, envir = owner_b)
  expect_equal(evalq(dsFlower:::.getHandle("owned_handle"), owner_a)$prepared,
               FALSE)
  expect_error(
    evalq(dsFlower:::.getHandle("owned_handle"), owner_b),
    "cross-session"
  )
  evalq(dsFlower:::.removeHandle("owned_handle"), owner_a)
})

test_that("flowerDestroyDS retries only owner-local opaque tombstones", {
  owner_a <- new.env(parent = globalenv())
  owner_b <- new.env(parent = globalenv())
  assign("flowerDestroyDS", dsFlower::flowerDestroyDS, envir = owner_a)
  assign("flowerDestroyDS", dsFlower::flowerDestroyDS, envir = owner_b)

  active <- dsFlower:::.registerHandle(mock_handle(), owner_env = owner_a)
  assign("active", active, envir = owner_a)
  assign("active", active, envir = owner_b)
  expect_error(evalq(flowerDestroyDS("active"), owner_b), "unavailable")
  expect_true(exists(active$capability, envir = dsFlower:::.handle_registry,
                     inherits = FALSE))

  rm(list = active$capability, envir = dsFlower:::.handle_registry)
  expect_null(evalq(flowerDestroyDS("active"), owner_a))
  expect_false(exists("active", envir = owner_a, inherits = FALSE))

  forged <- structure(list(capability = paste0("hdl_", strrep("f", 32))),
                      class = "dsflower_handle_ref")
  assign("forged", forged, envir = owner_a)
  expect_null(evalq(flowerDestroyDS("forged"), owner_a))
  expect_false(exists("forged", envir = owner_a, inherits = FALSE))

  assign("malformed", list(capability = "not-a-capability"), envir = owner_a)
  expect_error(evalq(flowerDestroyDS("malformed"), owner_a), "unavailable")
  expect_true(exists("malformed", envir = owner_a, inherits = FALSE))
})

test_that("handle resolution never falls back to a global session", {
  local_interface_privacy_state()
  global_reference <- dsFlower:::.registerHandle(
    mock_handle(), owner_env = .GlobalEnv)
  registry <- dsFlower:::.handle_registry
  assign("global_only_flower_handle", global_reference, envir = .GlobalEnv)
  withr::defer({
    if (exists("global_only_flower_handle", envir = .GlobalEnv,
               inherits = FALSE)) {
      rm("global_only_flower_handle", envir = .GlobalEnv)
    }
    registry[[global_reference$capability]] <- NULL
  })

  session <- new.env(parent = globalenv())
  expect_error(
    evalq(dsFlower:::.getHandle("global_only_flower_handle"), session),
    "Call flowerInitDS first"
  )

  session_reference <- dsFlower:::.registerHandle(
    mock_handle(data_path = "session.csv"), owner_env = session)
  assign("session_flower_handle", session_reference, envir = session)
  expect_identical(
    evalq(dsFlower:::.getHandle("session_flower_handle"), session)$data_path,
    "session.csv"
  )
  evalq(dsFlower:::.removeHandle("session_flower_handle"), session)
})

test_that("opaque handles preserve the legitimate DSLite assign flow", {
  local_interface_privacy_state()
  skip_if_not_installed("DSLite")
  server <- DSLite::newDSLiteServer(tables = list(
    T = data.frame(f1 = seq_len(20), target = rep(0:1, 10))),
    config = list())
  server$assignMethod("flowerInitDS", "dsFlower::flowerInitDS")
  server$assignMethod("flowerPrepareRunDS", "dsFlower::flowerPrepareRunDS")
  server$assignMethod("flowerDestroyDS", "dsFlower::flowerDestroyDS")
  server$aggregateMethod("flowerStatusDS", "dsFlower::flowerStatusDS")
  server_name <- paste0("dsflower_handle_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSLite::dsConnect(
    DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSLite::dsDisconnect(connection))

  invisible(DSLite::dsAssignTable(connection, "D", "T"))
  invisible(DSLite::dsAssignExpr(
    connection, "flower", 'flowerInitDS("D")'))
  reference <- server$getSessionData(connection@sid, "flower")
  expect_named(reference, "capability")
  expect_false("data_path" %in% names(reference))

  invisible(DSLite::dsAssignExpr(
    connection, "flower",
    'flowerPrepareRunDS("flower", "target", "f1")'))
  status <- DSLite::dsFetch(DSLite::dsAggregate(
    connection, 'flowerStatusDS("flower")'))
  expect_true(status$prepared)
  invisible(DSLite::dsAssignExpr(
    connection, "flower", 'flowerDestroyDS("flower")'))
  expect_null(server$getSessionData(connection@sid, "flower"))
})

test_that("flowerInitDS consumes an independent dsImaging handle", {
  local_interface_privacy_state()
  skip_if_not_installed("dsImaging")
  withr::local_options(list(
    dsimaging.nfilter.subset = 3L,
    dsflower.dp_unit = "row",
    dsflower.patient_column = "wrong_global_column"))
  root <- withr::local_tempdir()
  image_root <- file.path(root, "source", "images")
  metadata_root <- file.path(root, "metadata")
  index_root <- file.path(root, "indexes")
  dir.create(image_root, recursive = TRUE)
  dir.create(metadata_root, recursive = TRUE)
  dir.create(index_root, recursive = TRUE)
  metadata_path <- file.path(metadata_root, "samples.csv")
  relative_paths <- paste0("scan-", 1:4, ".png")
  image_paths <- file.path(image_root, relative_paths)
  for (i in seq_along(image_paths)) {
    writeBin(charToRaw(paste0("image-", i)), image_paths[[i]])
  }
  hashes <- vapply(image_paths, digest::digest, character(1),
                   algo = "sha256", file = TRUE)
  utils::write.csv(data.frame(
    sample_id = paste0("scan-", 1:4),
    patient_id = c(" patient-1 ", "patient-1", "patient-2", "patient-3"),
    source_kind = "single_file", n_files = 1L,
    relative_path = relative_paths,
    target = c(0L, 1L, 0L, 1L)
  ), metadata_path, row.names = FALSE)
  sample_manifests_path <- file.path(metadata_root, "sample_manifests.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("scan-", 1:4), source_kind = "single_file",
    primary_uri = relative_paths,
    files_json = vapply(relative_paths, function(path) jsonlite::toJSON(
      list(list(path = path, role = "primary")), auto_unbox = TRUE),
      character(1)),
    content_hash = hashes, n_files = 1L),
    sample_manifests_path, row.names = FALSE)
  content_index_path <- file.path(index_root, "content_hash_index.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("scan-", 1:4), uri = image_paths,
    content_hash = hashes, size = as.numeric(file.info(image_paths)$size),
    source_kind = "single_file"), content_index_path, row.names = FALSE)
  manifest <- list(
    schema_version = 1L,
    dataset_id = "pathmnist.site",
    metadata = list(
      uri = metadata_path, file = metadata_path, format = "csv",
      id_col = "sample_id", privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2",
      label_col = "target"),
    assets = list(images = list(
      type = "image_root", uri = image_root, path_col = "relative_path")),
    sample_manifests = list(uri = sample_manifests_path, format = "csv"),
    content_hash_index = list(uri = content_index_path, format = "csv")
  )
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  descriptor <- dsImaging::imaging_dataset_descriptor(manifest)
  imaging <- dsImaging:::.make_imaging_handle(
    descriptor, "img", backend = dsImaging::storage_backend("file"),
    manifest_uri = manifest_path, require_snapshot = TRUE)

  env <- new.env(parent = globalenv())
  resolver_owner_envs <- list()
  original_imaging_resolver <- utils::getFromNamespace(
    ".resolve_imaging_handle_for_consumer", "dsImaging")
  testthat::local_mocked_bindings(
    .resolve_imaging_handle_for_consumer = function(
        symbol, expected_capability = NULL, owner_env = NULL) {
      resolver_owner_envs[[length(resolver_owner_envs) + 1L]] <<- owner_env
      original_imaging_resolver(
        symbol, expected_capability = expected_capability,
        owner_env = owner_env)
    },
    .package = "dsImaging")
  reference <- dsImaging:::.register_imaging_handle(imaging, env)
  assign("img", reference, envir = env)
  assign("flowerInitDS", dsFlower::flowerInitDS, envir = env)
  assign("flowerPrepareRunDS", dsFlower::flowerPrepareRunDS, envir = env)
  flower_reference <- eval(quote(flowerInitDS("img")), envir = env)
  assign("flower", flower_reference, envir = env)
  state <- evalq(dsFlower:::.getHandle("flower"), envir = env)

  expect_identical(state$descriptor$manifest, manifest)
  expect_identical(state$imaging_handle_symbol, "img")
  expect_identical(state$imaging_handle_capability, reference$capability)
  expect_no_error(eval(
    quote(flowerPrepareRunDS("flower", "target")), envir = env))
  prepared <- evalq(dsFlower:::.getHandle("flower"), envir = env)
  expect_true(prepared$prepared)
  expect_gte(length(resolver_owner_envs), 3L)
  expect_true(all(vapply(resolver_owner_envs[seq_len(3L)],
    identical, logical(1), y = env)))
  staged_manifest <- jsonlite::fromJSON(
    file.path(prepared$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(staged_manifest[["dp-unit"]], "patient")
  expect_identical(staged_manifest$patient_column, "patient_id")
  expect_identical(staged_manifest$n_units, 3L)
  expect_identical(
    staged_manifest[["patient-id-canonicalization"]], "trim-utf8-v2")
  expect_error(
    eval(quote(flowerPrepareRunDS("flower", "relative_path")), envir = env),
    "manifest-declared label_col")
  after_rejected_prepare <- evalq(dsFlower:::.getHandle("flower"), envir = env)
  expect_identical(after_rejected_prepare$run_token, prepared$run_token)
  expect_true(dir.exists(after_rejected_prepare$staging_dir))

  # A Flower handle remains bound to the exact dsImaging capability it
  # consumed. Rebinding the source symbol, even to the same dataset, must not
  # silently authorize a different handle at prepare time.
  assign("img", reference, envir = env)
  flower_reference_2 <- eval(quote(flowerInitDS("img")), envir = env)
  assign("flower2", flower_reference_2, envir = env)
  replacement <- dsImaging:::.register_imaging_handle(imaging, env)
  assign("img", replacement, envir = env)
  expect_error(
    eval(quote(flowerPrepareRunDS("flower2", "target")), envir = env),
    "Private data preparation failed"
  )
  evalq(dsFlower:::.removeHandle("flower2"), envir = env)

  # An active binding cannot swap capabilities between flowerInitDS reading
  # the opaque reference and dsImaging authorizing it.
  rm("img", envir = env)
  binding_reads <- 0L
  makeActiveBinding("img", function(value) {
    if (!missing(value)) stop("read-only test binding")
    binding_reads <<- binding_reads + 1L
    if (binding_reads == 1L) reference else replacement
  }, env)
  expect_error(
    eval(quote(flowerInitDS("img")), envir = env),
    "capability changed"
  )
  rm("img", envir = env)

  other_env <- new.env(parent = globalenv())
  assign("img", reference, envir = other_env)
  assign("flowerInitDS", dsFlower::flowerInitDS, envir = other_env)
  expect_error(
    eval(quote(flowerInitDS("img")), envir = other_env),
    "cross-session"
  )

  if (requireNamespace("DSLite", quietly = TRUE)) {
    server <- DSLite::newDSLiteServer(config = list())
    server$assignMethod("flowerInitDS", "dsFlower::flowerInitDS")
    server_name <- paste0("dsflower_owner_server_", Sys.getpid())
    assign(server_name, server, envir = .GlobalEnv)
    withr::defer(rm(list = server_name, envir = .GlobalEnv))
    connection_a <- DSLite::dsConnect(
      DSLite::DSLite(), name = "site_a", url = server_name)
    connection_b <- DSLite::dsConnect(
      DSLite::DSLite(), name = "site_b", url = server_name)
    withr::defer(DSLite::dsDisconnect(connection_a))
    withr::defer(DSLite::dsDisconnect(connection_b))
    session_a <- server$getSession(connection_a@sid)
    session_b <- server$getSession(connection_b@sid)
    imaging_registry <- dsImaging:::.imaging_handle_registry
    dslite_reference <- dsImaging:::.register_imaging_handle(
      imaging, session_a)
    assign("img", dslite_reference, envir = session_a)
    assign("img", dslite_reference, envir = session_b)

    expect_no_error(DSLite::dsAssignExpr(
      connection_a, "flower", 'flowerInitDS("img")', async = FALSE))
    expect_error(DSLite::dsAssignExpr(
      connection_b, "flower", 'flowerInitDS("img")', async = FALSE),
      "cross-session")

    evalq(dsFlower:::.removeHandle("flower"), session_a)
    imaging_registry[[dslite_reference$capability]] <- NULL
    rm("img", envir = session_a)
    rm("img", envir = session_b)
  }
  evalq(dsFlower:::.removeHandle("flower"), envir = env)
})

test_that("flowerInitDS rejects imaging inputs that bypass dsImaging admission", {
  local_interface_privacy_state()
  manifest <- list(
    dataset_id = "unadmitted",
    metadata = list(
      id_col = "sample_id", privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2"),
    assets = list())
  imaging_descriptor <- structure(list(
    dataset_id = "unadmitted", source_kind = "image_bundle",
    metadata = manifest$metadata, assets = list(), manifest = manifest
  ), class = "ImagingDatasetDescriptor")
  flower_descriptor <- flower_dataset_descriptor(
    "unadmitted", "image_bundle", metadata = manifest$metadata,
    manifest = manifest)
  values <- list(
    imaging_descriptor,
    flower_descriptor,
    list(url = "imaging+dataset://bucket/unadmitted"),
    list(descriptor = imaging_descriptor),
    list(asset_ref = list(dataset_id = "unadmitted", alias_or_id = "x"))
  )

  for (i in seq_along(values)) {
    env <- new.env(parent = globalenv())
    assign("candidate", values[[i]], envir = env)
    assign("flowerInitDS", dsFlower::flowerInitDS, envir = env)
    expect_error(
      eval(quote(flowerInitDS("candidate")), envir = env),
      "imagingInitDS|imagingLoadAssetDS|Legacy imaging"
    )
  }
})

test_that("forged internal image handles cannot reach preparation", {
  local_interface_privacy_state()
  descriptor <- flower_dataset_descriptor(
    dataset_id = "forged.image",
    source_kind = "image_bundle",
    metadata = list(),
    manifest = list(metadata = list()),
    table_data = data.frame(
      subject_code = "person-1", relative_path = "scan.png", target = 0L),
    assets = list(images = list(
      type = "image_root", root = tempdir(), path_col = "relative_path"))
  )
  name <- "test_forged_imaging_handle"
  dsFlower:::.setHandle(name, dsFlower:::.createHandleFromDescriptor(descriptor))
  withr::defer(dsFlower:::.removeHandle(name))

  expect_error(
    flowerPrepareRunDS(name, "target"), "authorized dsImaging handle")
})

test_that(".ds_arg handles JSON strings", {
  result <- dsFlower:::.ds_arg('{"key": "value"}')
  expect_type(result, "list")
  expect_equal(result$key, "value")
})

test_that(".ds_arg handles B64 encoded strings", {
  json <- '{"num_rounds": 10}'
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  encoded <- paste0("B64:", b64)

  result <- dsFlower:::.ds_arg(encoded)
  expect_equal(result$num_rounds, 10)
})

test_that(".ds_arg passes through non-JSON values", {
  expect_equal(dsFlower:::.ds_arg(42), 42)
  expect_equal(dsFlower:::.ds_arg("simple_string"), "simple_string")
  expect_equal(dsFlower:::.ds_arg(TRUE), TRUE)
})

test_that(".dsf_option follows option chain", {
  # Default value

  expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "fallback")

  # Option set
  withr::with_options(list(dsflower.test_opt = "direct"), {
    expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "direct")
  })

  # Default prefix
  withr::with_options(list(default.dsflower.test_opt = "default_prefix"), {
    expect_equal(dsFlower:::.dsf_option("test_opt", "fallback"), "default_prefix")
  })
})

test_that("run rounds and data routing are pinned before staging", {
  expect_error(
    dsFlower:::.normalizeRunRounds(list("num-server-rounds" = 1.5)),
    "positive integer"
  )
  expect_identical(
    dsFlower:::.normalizeRunRounds(list())[["num-server-rounds"]], 1L)
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(num_rounds = 2L)),
    "num-server-rounds"
  )
  routed <- dsFlower:::.takeRunDataType(
    list(data_type = "IMAGE", "batch-size" = 8L), expected = "image")
  expect_equal(routed$data_type, "image")
  expect_null(routed$run_config$data_type)
  expect_error(
    dsFlower:::.takeRunDataType(list(data_type = "tabular"), expected = "image"),
    "disagrees"
  )
})

test_that("HookApp resource policy is finite, bounded, and unambiguous", {
  base <- list("num-server-rounds" = 1L)
  withr::local_options(list(
    dsflower.dp_sa_blocks = 8L,
    dsflower.dp_egress_timeout = 60L,
    dsflower.dp_egress_time_pad = 65,
    dsflower.dp_egress_memory_mb = 4096L,
    dsflower.dp_egress_file_mb = 512L,
    dsflower.dp_egress_processes = 32L
  ))
  cfg <- dsFlower:::.addDpConfigToRunConfig(base)
  expect_identical(cfg[["privacy-sa_blocks"]], 8L)
  expect_identical(cfg[["privacy-egress_timeout"]], 60L)
  expect_equal(cfg[["privacy-egress_time_pad"]], 65)
  expect_identical(cfg[["privacy-egress_memory_mb"]], 4096L)
  expect_identical(cfg[["privacy-egress_file_mb"]], 512L)
  expect_identical(cfg[["privacy-egress_processes"]], 32L)

  bad <- list(
    list(dsflower.dp_sa_blocks = 1.5),
    list(dsflower.dp_sa_blocks = Inf),
    list(dsflower.dp_sa_blocks = 65L),
    list(dsflower.dp_sa_blocks = c(2L, 3L)),
    list(dsflower.dp_egress_timeout = 3601L),
    list(dsflower.dp_egress_timeout = NaN),
    list(dsflower.dp_egress_time_pad = 230406),
    list(dsflower.dp_egress_time_pad = c(0, 1)),
    list(dsflower.dp_egress_memory_mb = 511L),
    list(dsflower.dp_egress_file_mb = 16385L),
    list(dsflower.dp_egress_processes = 0L)
  )
  for (opts in bad) {
    expect_error(withr::with_options(
      c(list(dsflower.dp_egress_timeout = 60L,
             dsflower.dp_egress_time_pad = 0), opts),
      dsFlower:::.addDpConfigToRunConfig(base)), "must be")
  }
  expect_error(withr::with_options(
    list(dsflower.dp_egress_timeout = 60L,
         dsflower.dp_egress_time_pad = 64),
    dsFlower:::.addDpConfigToRunConfig(base)), "at least 65")
})

test_that("HookApp public parameters are canonical and server hash pinned", {
  wire <- function(json) {
    gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(enc2utf8(json))))
  }
  encoded <- wire('{"alpha":0.25,"nested":{"labels":["a",null,true]}}')
  cfg <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "egress",
    "task-type" = "classification",
    "num-server-rounds" = 2L,
    "num-classes" = 2L,
    "app-params-b64" = encoded
  ))

  expect_identical(cfg[["app-params-b64"]], encoded)
  expect_identical(
    cfg[["app-params-sha256"]],
    digest::digest(jsonlite::base64_dec(encoded),
                   algo = "sha256", serialize = FALSE))
  expect_true("app-params-sha256" %in%
                dsFlower:::.server_owned_run_config_fields())

  defaults <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "egress", "task-type" = "classification",
    "num-server-rounds" = 1L, "num-classes" = 2L
  ))
  expect_identical(rawToChar(jsonlite::base64_dec(
    defaults[["app-params-b64"]])), "{}")
  expect_match(defaults[["app-params-sha256"]], "^[0-9a-f]{64}$")
})

test_that("HookApp manifest rejects aliases, tampering, and unsafe JSON", {
  wire <- function(json) {
    gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(enc2utf8(json))))
  }
  base <- list(
    "dp-track" = "egress", "task-type" = "classification",
    "num-server-rounds" = 1L, "num-classes" = 2L
  )
  invalid <- list(
    c(base, list("app_params" = list(alpha = 1))),
    c(base, list("app-params-extra" = wire("{}"))),
    c(base, list("app-params-sha256" = strrep("0", 64L))),
    c(base, list("app-params-b64" = wire('{"b":1,"a":2}'))),
    c(base, list("app-params-b64" = wire('{"a":1,"a":2}'))),
    c(base, list("app-params-b64" = wire('{"epsilon":1}'))),
    c(base, list("app-params-b64" = wire('{"training_epsilon":1}'))),
    c(base, list("app-params-b64" = wire('{"model_path":"weights"}'))),
    c(base, list("app-params-b64" = wire('{"value":"dir/file"}'))),
    c(base, list("app-params-b64" = wire('{"value":1e999}'))),
    c(base, list("app-params-b64" = "not-base64"))
  )
  for (config in invalid) {
    expect_error(dsFlower:::.addDpConfigToRunConfig(config))
  }

  too_large <- gsub(
    "[\r\n]", "",
    jsonlite::base64_enc(as.raw(rep.int(0L, 65537L))))
  expect_error(dsFlower:::.addDpConfigToRunConfig(c(
    base, list("app-params-b64" = too_large))), "bounded")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "neural", "num-server-rounds" = 1L,
    "app-params-b64" = wire("{}"))), "only valid")
})

test_that("validation config is pinned to one well-typed release", {
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "native_tree",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L)),
    "exact binary or regression.*pin set")

  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "validation-bins" = 24L,
    "task-type" = "classification", "loss-name" = "cross_entropy",
    "num-server-rounds" = 1L, "num-features" = 3L,
    "num-classes" = 4L, "num-labels" = 2L,
    "target-levels" = c("a", "b", "c", "d")))
  expect_identical(config[["dp-track"]], "validation")
  expect_identical(config[["num-server-rounds"]], 1L)
  expect_identical(config[["validation-bins"]], 24L)
  expect_false(config[["allow_per_node_metrics"]])

  maximum <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "cross_entropy", "num-server-rounds" = 1L,
    "num-features" = 3L, "num-classes" = 1024L,
    "num-labels" = 1024L))
  expect_identical(maximum[["num-classes"]], 1024L)
  expect_identical(maximum[["num-labels"]], 1024L)
  too_many <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "cross_entropy", "num-server-rounds" = 1L,
    "num-features" = 3L, "num-classes" = 1025L,
    "num-labels" = 1024L)
  expect_error(dsFlower:::.addDpConfigToRunConfig(too_many), "2, 1024")

  wrong_bounds <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = 0, upper = 1))
  expect_error(dsFlower:::.addDpConfigToRunConfig(wrong_bounds),
               "match num-features exactly")

  mixed_case <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "REGRESSION",
    "loss-name" = "MSE", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1)))
  expect_identical(mixed_case[["loss-name"]], "mse")

  quantile <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "regression",
    "loss-name" = "quantile", "quantile-level" = 0.9,
    "num-server-rounds" = 1L, "num-features" = 2L,
    "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1)))
  expect_identical(quantile[["loss-name"]], "quantile")

  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 2L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L)),
    "exactly one")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "regression", "task-type" = "regression",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "target-bounds" = list(lower = 0, upper = 1))),
    "disagrees")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "task-type" = "classification",
    "loss-name" = "bce_logits", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L)),
    "binary")
  expect_error(dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multilabel", "task-type" = "classification",
    "loss-name" = "multilabel_bce", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L)),
    "binary target levels")
})

test_that("vision validation accepts only its exact public pin set", {
  config <- dsFlower:::.addDpConfigToRunConfig(vision_validation_config())
  expect_identical(config[["data_type"]], "image")
  expect_identical(config[["backbone"]], "densenet121_3d")
  expect_identical(config[["image-size"]], 128L)
  expect_identical(
    config[["vision-extractor-profile"]],
    "dsflower-densenet121-monai-seed0-extractor-v1")
  expect_identical(config[["num-features"]], 1024L)
  expect_identical(
    config[["validation-artifact-format"]], "pytorch-state-dict-v1")

  missing <- vision_validation_config()
  missing[["validation-artifact-sha256"]] <- NULL
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(missing), "exact backbone.*pin set")

  unsupported <- vision_validation_config()
  unsupported[["backbone"]] <- "resnet50"
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(unsupported), "not canonical")

  wrong_dim <- vision_validation_config()
  wrong_dim[["num-features"]] <- 512L
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(wrong_dim), "image geometry")

  dense_3d_too_small <- vision_validation_config()
  dense_3d_too_small[["image-size"]] <- 127L
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(dense_3d_too_small),
    "image geometry")

  dense_2d <- vision_validation_config()
  dense_2d[["backbone"]] <- "densenet121"
  dense_2d[["vision-extractor-profile"]] <-
    "dsflower-densenet121-imagenet1k-v1-extractor-v1"
  dense_2d[["image-size"]] <- 28L
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(dense_2d), "image geometry")
  dense_2d[["image-size"]] <- 29L
  expect_identical(
    dsFlower:::.addDpConfigToRunConfig(dense_2d)[["image-size"]], 29L)

  wrong_profile <- vision_validation_config()
  wrong_profile[["vision-extractor-profile"]] <-
    "dsflower-densenet121-imagenet1k-v1-extractor-v1"
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(wrong_profile),
    "profile does not match")

  no_levels <- vision_validation_config()
  no_levels[["target-levels"]] <- NULL
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(no_levels),
    "one public target level per class")

  tabular <- vision_validation_config()
  tabular[["data_type"]] <- "tabular"
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(tabular),
    "Vision-only validation pins")
})

test_that("vision training requires its canonical extractor profile", {
  config <- list(
    "dp-track" = "neural", "data_type" = "image",
    "num-server-rounds" = 1L, "num-features" = 512L,
    "backbone" = "resnet18",
    "image-size" = 224L,
    "vision-extractor-profile" =
      "dsflower-resnet18-imagenet1k-v1-extractor-v1")
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  expect_identical(
    normalized[["vision-extractor-profile"]],
    "dsflower-resnet18-imagenet1k-v1-extractor-v1")

  missing <- config
  missing[["vision-extractor-profile"]] <- NULL
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(missing),
    "exact backbone.*profile pin set")

  wrong <- config
  wrong[["vision-extractor-profile"]] <-
    "dsflower-resnet18-monai-seed0-extractor-v1"
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(wrong), "profile does not match")

  wrong_dim <- config
  wrong_dim[["num-features"]] <- 1024L
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(wrong_dim), "image geometry")

  missing_dim <- config
  missing_dim[["num-features"]] <- NULL
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(missing_dim), "image geometry")
})

test_that("validation preparation persists the public contract before execution", {
  local_interface_privacy_state()
  name <- "test_validation_prepare"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    age = c(20, 30, 40), marker = c(-1, 0, 1), outcome = c(0, 1, 1))))
  withr::defer(dsFlower:::.removeHandle(name))
  config <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 16L,
    "task-type" = "classification", "loss-name" = "bce_logits",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 2L, "num-labels" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(120, 5)),
    "target-levels" = c(0, 1))
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["validation-contract-sha256"]] <-
    dsFlower:::.validationContractSha256(
      normalized, c("age", "marker"), "outcome",
      dsFlower:::.dpUnitPolicy()$dp_unit)
  expect_error(
    flowerPrepareRunDS(name, "outcome", NULL, config),
    "explicit ordered public feature contract")
  feature_columns <- c("age", "marker")
  expect_no_error(flowerPrepareRunDS(
    name, "outcome", feature_columns, config))
  handle <- dsFlower:::.getHandle(name)
  manifest <- jsonlite::fromJSON(
    file.path(handle$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(manifest[["dp-track"]], "validation")
  expect_identical(manifest[["num-server-rounds"]], 1L)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
  expect_identical(manifest[["validation-model-track"]], "neural")
  expect_identical(manifest[["validation-task"]], "binary")
  expect_identical(manifest[["validation-bins"]], 16L)
  expect_identical(
    manifest[["validation-contract-sha256"]],
    config[["validation-contract-sha256"]])
})

test_that("validation never stages a patient identifier as model data", {
  local_interface_privacy_state()
  withr::local_options(list(
    dsflower.dp_unit = "patient",
    dsflower.patient_column = "subject_id"))
  name <- "test_validation_patient_overlap"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    subject_id = c("a", "b"), age = c(20, 30), outcome = c(0, 1))))
  withr::defer(dsFlower:::.removeHandle(name))
  base <- list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "binary", "validation-bins" = 16L,
    "task-type" = "classification", "loss-name" = "bce_logits",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 1L, "num-classes" = 2L, "num-labels" = 2L,
    "target-levels" = c(0, 1),
    "validation-contract-sha256" = strrep("a", 64L))
  expect_error(flowerPrepareRunDS(
    name, "outcome", "subject_id", base), "patient identifier")
  expect_error(flowerPrepareRunDS(
    name, "subject_id", "age", base), "patient identifier")
})

test_that("validation contract SHA-256 has a cross-package canonical wire", {
  config <- dsFlower:::.addDpConfigToRunConfig(list(
    "dp-track" = "validation", "validation-model-track" = "neural",
    "validation-task" = "multiclass", "validation-bins" = 24L,
    "task-type" = "classification", "loss-name" = "cross_entropy",
    "model-spec-b64" = "e30=", "num-server-rounds" = 1L,
    "num-features" = 2L, "num-classes" = 3L, "num-labels" = 2L,
    "feature-bounds" = list(lower = c(0, -5), upper = c(100, 5)),
    "target-levels" = c("a", "b", "c")))
  expect_identical(
    dsFlower:::.validationContractSha256(
      config, c("age", "marker"), "outcome", "patient"),
    "ecf31e300ff0087e26799f6a4fbe1894e8f8357e0fd35667936653318b040e3f")
})

test_that("vision validation contract has a cross-package schema-2 wire", {
  config <- dsFlower:::.addDpConfigToRunConfig(vision_validation_config())
  hash <- dsFlower:::.validationContractSha256(
    config, NULL, "diagnosis", "patient")
  expect_identical(
    hash,
    "95b536d6b8e0902691170a463177bfa64fd3b8dde4c5d002e2e098da94a37959")

  changed <- config
  changed[["validation-artifact-sha256"]] <- strrep("b", 64L)
  expect_false(identical(
    hash,
    dsFlower:::.validationContractSha256(
      changed, NULL, "diagnosis", "patient")))

  changed_profile <- config
  changed_profile[["vision-extractor-profile"]] <-
    "dsflower-densenet121-imagenet1k-v1-extractor-v1"
  expect_false(identical(
    hash,
    dsFlower:::.validationContractSha256(
      changed_profile, NULL, "diagnosis", "patient")))
})

test_that("table-backed vision cannot bypass dsImaging admission", {
  local_interface_privacy_state()
  image_root <- withr::local_tempdir()
  withr::local_options(list(dsflower.image_data_root = image_root))
  name <- "test_empty_vision_validation"
  dsFlower:::.setHandle(name, mock_handle(table_data = data.frame(
    relative_path = character(), diagnosis = character())))
  withr::defer(dsFlower:::.removeHandle(name))

  config <- vision_validation_config()
  normalized <- dsFlower:::.addDpConfigToRunConfig(config)
  config[["validation-contract-sha256"]] <-
    dsFlower:::.validationContractSha256(
      normalized, NULL, "diagnosis", dsFlower:::.dpUnitPolicy()$dp_unit)

  expect_error(
    flowerPrepareRunDS(name, "diagnosis", NULL, config),
    "data_type disagrees with the server-side dataset descriptor")
  expect_null(dsFlower:::.getHandle(name)$run_token)

  expect_error(
    flowerPrepareRunDS(name, "diagnosis", "relative_path", config),
    "data_type disagrees with the server-side dataset descriptor")
  expect_null(dsFlower:::.getHandle(name)$run_token)
})

test_that("flowerPrepareRunDS stages data correctly", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  # Create a handle manually
  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_prepare", handle)
  on.exit(dsFlower:::.removeHandle("test_prepare"), add = TRUE)

  # Prepare the run
  feature_columns <- c("f1", "f2", "f3")
  result <- flowerPrepareRunDS("test_prepare", "target", feature_columns)
  expect_named(result, "capability")
  state <- dsFlower:::.getHandle("test_prepare")

  expect_true(state$prepared)
  expect_equal(state$target_column, "target")
  expect_equal(state$feature_columns, c("f1", "f2", "f3"))
  expect_match(state$run_token, "^run_[0-9a-f]{32}$")
  expect_true(dir.exists(state$staging_dir))

  # Verify manifest exists
  manifest_path <- file.path(state$staging_dir, "manifest.json")
  expect_true(file.exists(manifest_path))

  manifest <- jsonlite::fromJSON(manifest_path)
  expect_equal(manifest$n_samples, 200)
  expect_equal(manifest$target_column, "target")
  expect_identical(manifest[["num-server-rounds"]], 1L)
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_gt(manifest[["privacy-delta"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
})

test_that("assigned data frames and matrices obey the privacy-unit minimum", {
  local_interface_privacy_state()
  threshold <- 4L
  withr::local_options(list(
    nfilter.subset = threshold,
    dsflower.min_train_rows = NULL))

  prepare_assigned <- function(value) {
    env <- new.env(parent = globalenv())
    assign("D", value, envir = env)
    assign("flowerInitDS", dsFlower::flowerInitDS, envir = env)
    assign("flowerPrepareRunDS", dsFlower::flowerPrepareRunDS, envir = env)
    assign("flowerDestroyDS", dsFlower::flowerDestroyDS, envir = env)
    assign("flower", evalq(flowerInitDS("D"), envir = env), envir = env)
    on.exit(try(evalq(flowerDestroyDS("flower"), envir = env), silent = TRUE))

    error <- tryCatch({
      evalq(flowerPrepareRunDS("flower", "target", "f1"), envir = env)
      NULL
    }, error = identity)
    state <- evalq(dsFlower:::.getHandle("flower"), envir = env)
    manifest <- if (isTRUE(state$prepared)) {
      jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
    } else {
      NULL
    }
    list(error = error, state = state, manifest = manifest)
  }

  make_table <- function(n, as_matrix) {
    value <- data.frame(
      f1 = seq_len(n), target = rep(0:1, length.out = n))
    if (as_matrix) as.matrix(value) else value
  }

  for (as_matrix in c(FALSE, TRUE)) {
    below <- prepare_assigned(make_table(threshold - 1L, as_matrix))
    expect_s3_class(below$error, "error")
    expect_identical(
      conditionMessage(below$error),
      paste0("Private data preparation failed on this node; contact the node ",
             "administrator."))
    expect_false(below$state$prepared)
    expect_null(below$state$run_token)

    boundary <- prepare_assigned(make_table(threshold, as_matrix))
    expect_null(boundary$error)
    expect_true(boundary$state$prepared)
    expect_equal(boundary$manifest$n_samples, threshold)
    expect_equal(boundary$manifest$n_units, threshold)
  }
})

test_that("failed preparation does not alter a later training contract", {
  local_interface_privacy_state()
  dsFlower:::.setHandle(
    "test_failed_prepare_contract",
    mock_handle(table_data = data.frame(f1 = 1:5)))
  withr::defer(dsFlower:::.removeHandle("test_failed_prepare_contract"))
  expect_error(
    flowerPrepareRunDS("test_failed_prepare_contract", "target", "f1"),
    "Private data preparation failed on this node"
  )

  dsFlower:::.setHandle(
    "test_next_prepare_contract",
    mock_handle(table_data = data.frame(f1 = 1:5, target = rep(0:1, length.out = 5))))
  withr::defer(dsFlower:::.removeHandle("test_next_prepare_contract"))
  expect_no_error(
    flowerPrepareRunDS("test_next_prepare_contract", "target", "f1"))
  state <- dsFlower:::.getHandle("test_next_prepare_contract")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_gt(manifest[["privacy-delta"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
})

test_that("failed prepare retains an exact staging rollback token", {
  local_interface_privacy_state()
  withr::local_options(list(
    nfilter.subset = 4L,
    dsflower.min_train_rows = NULL
  ))
  symbol <- "test_prepare_cleanup_retry"
  dsFlower:::.setHandle(symbol, mock_handle(table_data = data.frame(
    f1 = 1:3, target = c(0L, 1L, 0L))))
  withr::defer(try(dsFlower:::.removeHandle(symbol), silent = TRUE))

  fail_cleanup <- TRUE
  real_cleanup <- dsFlower:::.cleanupStaging
  local_mocked_bindings(
    .cleanupStaging = function(run_token) {
      if (fail_cleanup) {
        fail_cleanup <<- FALSE
        stop("mock rollback deletion failed")
      }
      real_cleanup(run_token)
    },
    .package = "dsFlower"
  )

  expect_error(
    flowerPrepareRunDS(symbol, "target", "f1"),
    "Private data preparation failed on this node",
    fixed = TRUE
  )
  retained <- dsFlower:::.getHandle(symbol)
  expect_false(retained$prepared)
  expect_null(retained$run_token)
  expect_length(retained$pending_cleanup_tokens, 1L)
  staging_dirs <- dsFlower:::.expectedStagingDirs(
    retained$pending_cleanup_tokens, create_roots = FALSE)
  expect_true(any(dir.exists(staging_dirs)))

  expect_no_error(flowerCleanupRunDS(symbol))
  cleaned <- dsFlower:::.getHandle(symbol)
  expect_null(cleaned$pending_cleanup_tokens)
  expect_false(any(dir.exists(staging_dirs)))
})

test_that("run admission is independent of a rare target class", {
  local_interface_privacy_state()
  # A class-count gate would turn prepare success/error into a label-dependent
  # transcript oracle. Only the fixed privacy-unit count may affect admission.
  withr::local_options(list(nfilter.subset = 3, nfilter.tab = 5))
  rare_path <- tempfile(fileext = ".csv")
  utils::write.csv(
    data.frame(f1 = seq_len(20), target = c(rep(0L, 19), 1L)),
    rare_path, row.names = FALSE)
  withr::defer(unlink(rare_path))

  handle <- mock_handle(data_path = rare_path, data_format = "csv")
  dsFlower:::.setHandle("test_rare_class_admission", handle)
  withr::defer(dsFlower:::.removeHandle("test_rare_class_admission"))

  expect_no_error(
    flowerPrepareRunDS("test_rare_class_admission", "target", "f1"))
  state <- dsFlower:::.getHandle("test_rare_class_admission")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 20L)
  expect_equal(manifest$n_units, 20L)
})

test_that("run admission fails closed without one exact privacy-unit count", {
  base_args <- list(
    handle = NULL, target_column = "target",
    n_samples = 20L, target_data = NULL,
    run_config = list("privacy-clipping_norm" = 1),
    data_type = "tabular"
  )
  expect_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = NULL))),
    "Invalid staged privacy-unit count"
  )
  expect_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = c(10L, 20L)))),
    "Invalid staged privacy-unit count"
  )
})

test_that("minimum-size admission uses privacy units rather than sample rows", {
  withr::local_options(list(
    nfilter.subset = 4L,
    dsflower.min_train_rows = NULL))
  base_args <- list(
    handle = NULL, target_column = "target",
    n_samples = 40L, target_data = NULL,
    run_config = list("privacy-clipping_norm" = 1),
    data_type = "image"
  )
  expect_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = 3L))),
    "Disclosive: operation blocked")
  expect_no_error(
    do.call(dsFlower:::.enforceDisclosureAndDp,
            c(base_args, list(n_units = 4L))))
})

# --- TLS ca.pem handling ---

test_that("flowerEnsureSuperNodeDS writes ca.pem when ca_cert_pem provided", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_tls"), add = TRUE)

  # Prepare the handle first
  feature_columns <- c("f1", "f2", "f3")
  flowerPrepareRunDS("test_tls", "target", feature_columns)
  staging_dir <- dsFlower:::.getHandle("test_tls")$staging_dir

  # B64-encode a mock CA cert PEM (same as client would send)
  ca_pem <- "-----BEGIN CERTIFICATE-----\nMOCKCERT\n-----END CERTIFICATE-----"
  json <- as.character(jsonlite::toJSON(list(pem = ca_pem), auto_unbox = TRUE))
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  encoded <- paste0("B64:", b64)

  # Mock .supernode_ensure to avoid spawning real process
  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL,
                                 insecure = FALSE) {
      list(process = NULL, superlink_address = superlink_address,
           ca_cert_path = ca_cert_path)
    }
  )

  flowerEnsureSuperNodeDS("test_tls", "127.0.0.1:9092",
                          "fl-test", encoded)
  updated <- dsFlower:::.getHandle("test_tls")

  # Verify ca.pem was written
  ca_pem_path <- file.path(staging_dir, "ca.pem")
  expect_true(file.exists(ca_pem_path))
  written_pem <- paste(readLines(ca_pem_path, warn = FALSE), collapse = "\n")
  expect_true(grepl("MOCKCERT", written_pem))
  expect_equal(updated$ca_cert_path, ca_pem_path)
  manifest <- jsonlite::fromJSON(file.path(staging_dir, "manifest.json"))
  expect_gt(manifest[["privacy-epsilon"]], 0)
  expect_match(manifest[["privacy-policy-sha256"]], "^[0-9a-f]{64}$")
  pins <- jsonlite::fromJSON(
    file.path(staging_dir, "pinned_packages.json"), simplifyVector = FALSE)
  expect_identical(
    as.character(pins$dsflower_runner),
    dsFlower:::.compute_harness_hash())
})

test_that("flowerEnsureSuperNodeDS works without ca_cert_pem", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_no_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_no_tls"), add = TRUE)

  feature_columns <- c("f1", "f2", "f3")
  flowerPrepareRunDS("test_no_tls", "target", feature_columns)

  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL,
                                 insecure = FALSE) {
      list(process = NULL, superlink_address = superlink_address,
           ca_cert_path = ca_cert_path)
    }
  )

  flowerEnsureSuperNodeDS("test_no_tls", "127.0.0.1:9092", "fl-test")
  updated <- dsFlower:::.getHandle("test_no_tls")
  expect_null(updated$ca_cert_path)
  expect_true(updated$node_ensured)
})

test_that("a client-supplied hostname is not mistaken for a trusted coordinator", {
  handle_name <- "test_untrusted_hostname"
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  dsFlower:::.setHandle(handle_name, mock_handle(
    run_token = run_token, staging_dir = staging_dir, prepared = TRUE))
  withr::defer(dsFlower:::.removeHandle(handle_name))
  withr::local_options(list(
    dsflower.coordinator_address = "",
    dsflower.allow_untrusted_coordinator = FALSE
  ))
  env <- getFromNamespace(".dsflower_env", "dsFlower")
  old_port <- env$tunnel_forwarder_port
  env$tunnel_forwarder_port <- NULL
  withr::defer(env$tunnel_forwarder_port <- old_port)

  expect_error(
    flowerEnsureSuperNodeDS(handle_name, "evil.example:9092"),
    "Refusing SuperNode"
  )
  # Loopback is not an operator authorization either when supplied directly by
  # the analyst; only the server-created tunnel or an exact admin pin is trusted.
  expect_error(
    flowerEnsureSuperNodeDS(handle_name, "127.0.0.1:9092"),
    "Refusing SuperNode"
  )
})

test_that("flowerCleanupRunDS resets handle state", {
  csv_path <- create_test_csv(n = 10)
  on.exit(unlink(csv_path))

  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    data_path = csv_path,
    run_token = run_token,
    staging_dir = staging_dir,
    target_column = "target",
    feature_columns = c("f1"),
    prepared = TRUE
  )
  dsFlower:::.setHandle("test_cleanup", handle)
  on.exit(dsFlower:::.removeHandle("test_cleanup"), add = TRUE)

  flowerCleanupRunDS("test_cleanup")
  state <- dsFlower:::.getHandle("test_cleanup")
  expect_false(state$prepared)
  expect_false(state$node_ensured)
  expect_null(state$run_token)
  expect_null(state$staging_dir)
  expect_null(state$target_column)
})

test_that("flowerCleanupRunDS stops associated SuperNode before reset", {
  stopped <- character()
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    run_token = run_token,
    staging_dir = staging_dir,
    prepared = TRUE,
    node_ensured = TRUE
  )
  dsFlower:::.setHandle("test_cleanup_stop", handle)
  on.exit(dsFlower:::.removeHandle("test_cleanup_stop"), add = TRUE)

  local_mocked_bindings(
    .supernode_stop = function(manifest_dir) {
      stopped <<- c(stopped, manifest_dir)
      invisible(TRUE)
    },
    .cleanupStaging = function(run_token) invisible(TRUE)
  )

  flowerCleanupRunDS("test_cleanup_stop")
  expect_equal(stopped, handle$staging_dir)
})

test_that("flowerCleanupRunDS preserves state on failure and retries exactly", {
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    run_token = run_token,
    staging_dir = staging_dir,
    target_column = "target",
    prepared = TRUE,
    node_ensured = TRUE
  )
  dsFlower:::.setHandle("test_cleanup_retry", handle)
  withr::defer(try(
    dsFlower:::.removeHandle("test_cleanup_retry"), silent = TRUE))

  attempts <- 0L
  real_cleanup <- dsFlower:::.cleanupStaging
  local_mocked_bindings(
    .supernode_stop = function(...) invisible(TRUE),
    .cleanupStaging = function(token) {
      attempts <<- attempts + 1L
      if (attempts == 1L) stop("mock staging deletion failed")
      real_cleanup(token)
    },
    .package = "dsFlower"
  )

  expect_error(
    flowerCleanupRunDS("test_cleanup_retry"),
    "mock staging deletion failed",
    fixed = TRUE
  )
  retained <- dsFlower:::.getHandle("test_cleanup_retry")
  expect_identical(retained$run_token, run_token)
  expect_identical(retained$staging_dir, staging_dir)
  expect_true(retained$prepared)

  expect_no_error(flowerCleanupRunDS("test_cleanup_retry"))
  cleaned <- dsFlower:::.getHandle("test_cleanup_retry")
  expect_null(cleaned$run_token)
  expect_null(cleaned$staging_dir)
  expect_false(cleaned$prepared)
  expect_false(dir.exists(staging_dir))
})

test_that("cleanup and destroy retry after staging is already absent", {
  make_partial <- function(symbol) {
    run_token <- dsFlower:::.generate_run_token()
    staging_dir <- dsFlower:::.ensureStagingDir(run_token)
    unlink(staging_dir, recursive = TRUE)
    dsFlower:::.setHandle(symbol, mock_handle(
      run_token = run_token, staging_dir = staging_dir,
      target_column = "target", prepared = TRUE, node_ensured = TRUE),
      owner_env = parent.frame())
    list(token = run_token, path = staging_dir)
  }

  cleanup_partial <- make_partial("test_cleanup_absent")
  withr::defer(try(
    dsFlower:::.removeHandle("test_cleanup_absent"), silent = TRUE))
  expect_no_error(flowerCleanupRunDS("test_cleanup_absent"))
  cleaned <- dsFlower:::.getHandle("test_cleanup_absent")
  expect_null(cleaned$run_token)
  expect_null(cleaned$staging_dir)
  expect_false(cleaned$prepared)
  expect_false(dir.exists(cleanup_partial$path))

  destroy_partial <- make_partial("test_destroy_absent")
  expect_no_error(flowerDestroyDS("test_destroy_absent"))
  expect_error(
    dsFlower:::.getHandle("test_destroy_absent"),
    "No Flower handle",
    fixed = TRUE
  )
  expect_false(dir.exists(destroy_partial$path))
})

test_that("flowerDestroyDS retains its registry entry until cleanup succeeds", {
  run_token <- dsFlower:::.generate_run_token()
  staging_dir <- dsFlower:::.ensureStagingDir(run_token)
  handle <- mock_handle(
    run_token = run_token, staging_dir = staging_dir,
    target_column = "target", prepared = TRUE, node_ensured = TRUE)
  dsFlower:::.setHandle("test_destroy_retry", handle)
  withr::defer(try(
    dsFlower:::.removeHandle("test_destroy_retry"), silent = TRUE))

  attempts <- 0L
  real_cleanup <- dsFlower:::.cleanupStaging
  local_mocked_bindings(
    .supernode_stop = function(...) invisible(TRUE),
    .cleanupStaging = function(token) {
      attempts <<- attempts + 1L
      if (attempts == 1L) stop("mock staging deletion failed")
      real_cleanup(token)
    },
    .package = "dsFlower"
  )

  expect_error(
    flowerDestroyDS("test_destroy_retry"),
    "mock staging deletion failed",
    fixed = TRUE
  )
  retained <- dsFlower:::.getHandle("test_destroy_retry")
  expect_identical(retained$run_token, run_token)
  expect_identical(retained$staging_dir, staging_dir)

  expect_no_error(flowerDestroyDS("test_destroy_retry"))
  expect_error(
    dsFlower:::.getHandle("test_destroy_retry"),
    "No Flower handle",
    fixed = TRUE
  )
  expect_false(dir.exists(staging_dir))
})

test_that("flowerGetCapabilitiesDS returns expected structure", {
  withr::local_options(list(
    nfilter.subset = 4L,
    dsflower.min_train_rows = 6L))
  xgboost_calls <- 0L
  pure_calls <- list()
  local_mocked_bindings(
    .native_tree_xgboost_probe = function(...) {
      xgboost_calls <<- xgboost_calls + 1L
      FALSE
    },
    .native_tree_pure_probes = function(engines, ...) {
      pure_calls[[length(pure_calls) + 1L]] <<- engines
      stats::setNames(rep.int(FALSE, length(engines)), engines)
    },
    .package = "dsFlower")
  caps <- flowerGetCapabilitiesDS()
  expect_type(caps, "list")
  expect_true("dsflower_version" %in% names(caps))
  expect_true("python_version" %in% names(caps))
  expect_true("flower_version" %in% names(caps))
  expect_true("torch_version" %in% names(caps))
  expect_true("opacus_version" %in% names(caps))
  expect_true("runtime_versions_sha256" %in% names(caps))
  expect_identical(caps$runner_abi, 3L)
  expect_match(caps$privacy_policy_sha256, "^[0-9a-f]{64}$")
  expect_identical(caps$privacy_clipping_norm, 1)
  expect_identical(caps$dp_tracks,
                   c("neural", "egress", "validation"))
  expect_setequal(
    caps$declarative_model_ops$layers,
    c("linear", "relu", "gelu", "tanh", "sigmoid", "elu", "silu",
      "leaky_relu", "dropout", "layernorm", "softmax", "reshape",
      "flatten", "conv1d", "conv2d", "maxpool2d",
      "adaptiveavgpool2d", "upsample", "lstm", "gru")
  )
  expect_setequal(
    caps$declarative_model_ops$graph,
    c("add", "mul", "sub", "div", "affine", "concat", "matmul",
      "transpose")
  )
  expect_setequal(
    caps$declarative_losses,
    c("bce_logits", "cross_entropy", "mse", "poisson_nll",
      "multilabel_bce", "hinge", "ordinal", "negbin_nll", "gamma_nll",
      "huber", "quantile")
  )
  expect_false("tree_objectives" %in% names(caps))
  expect_identical(caps$native_tree$contract,
                   "dsflower-native-tree-request-v1")
  expect_identical(caps$native_tree$probed_engines, character())
  expect_false(any(grepl(
    "_native_tight_available$", names(caps$native_tree))))
  expect_identical(xgboost_calls, 0L)
  expect_length(pure_calls, 0L)
  expect_setequal(
    caps$aggregation_strategies,
    c("fedavg", "fedadam", "fedadagrad", "fedyogi", "fedavgm")
  )
  expect_identical(caps$resampling$holdout$tracks,
                   c("neural", "native_tree"))
  expect_identical(caps$resampling$holdout$data_kinds,
                   c("tabular", "image"))
  expect_true(caps$resampling$holdout$pooled_only)
  expect_true(caps$resampling$cross_validation$available)
  expect_identical(caps$resampling$cross_validation$tracks,
                   c("neural", "native_tree"))
  expect_identical(caps$resampling$cross_validation$data_kinds, "tabular")
  expect_identical(caps$resampling$cross_validation$folds, c(2L, 10L))
  expect_true(caps$resampling$cross_validation$pooled_only)
  expect_true("max_rounds" %in% names(caps))
  expect_identical(caps$min_samples, 6L)
  expect_false("secure_aggregation_supported" %in% names(caps))
  expect_false(caps$hook_execution_configured)

  targeted <- flowerGetCapabilitiesDS("random_forest")
  expect_identical(targeted$native_tree$probed_engines, "random_forest")
  expect_false(targeted$native_tree$random_forest_native_tight_available)
  expect_false("xgboost_native_tight_available" %in%
                 names(targeted$native_tree))
  expect_identical(xgboost_calls, 0L)
  expect_identical(pure_calls, list("random_forest"))
  expect_error(flowerGetCapabilitiesDS("Random_Forest"),
               "native_tree_probe must be exactly")
})

test_that("unsupported run configuration fails before private preparation", {
  expect_error(
    dsFlower:::.validate_client_run_config(list(label_set = "clinical")),
    "unsupported")
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(
      "dp-track" = "trees", "num-server-rounds" = 1L)),
    "dp-track must be one of")
  expect_error(
    dsFlower:::.addDpConfigToRunConfig(list(
      "dp-track" = "neural", "num-server-rounds" = 1L,
      "unexpected-field" = "value")),
    "unsupported field.*unexpected-field")
})

test_that("Hook readiness capability reflects every public admin gate", {
  withr::local_options(list(
    dsflower.hook_enabled = TRUE,
    dsflower.hook_sandbox_attested = TRUE,
    dsflower.hook_resource_isolation_attested = TRUE,
    dsflower.dp_egress_timeout = 30,
    dsflower.dp_egress_time_pad = 35))
  caps <- flowerGetCapabilitiesDS()
  expect_true(caps$hook_time_envelope_configured)
  expect_true(caps$hook_execution_configured)
  expect_identical(caps$hook_required_time_pad_seconds, 35)
})

test_that("Hook sample-and-aggregate timing covers every sequential block", {
  withr::local_options(list(
    dsflower.hook_enabled = TRUE,
    dsflower.hook_sandbox_attested = TRUE,
    dsflower.hook_resource_isolation_attested = TRUE,
    dsflower.dp_sample_aggregate = TRUE,
    dsflower.dp_sa_blocks = 3L,
    dsflower.dp_egress_timeout = 30,
    dsflower.dp_egress_time_pad = 95))
  caps <- flowerGetCapabilitiesDS()
  expect_identical(caps$hook_required_time_pad_seconds, 95)
  expect_true(caps$hook_execution_configured)
})

test_that("flowerGetCapabilitiesDS omits infrastructure and session state", {
  withr::local_options(list(
    dsflower.hook_resource_isolation_attested = TRUE))
  caps <- flowerGetCapabilitiesDS()
  expect_false(any(c(
    "python_envs", "hostname", "is_docker", "active_supernodes",
    "zombie_processes"
  ) %in% names(caps)))
  expect_true(caps$hook_resource_isolation_attested)
})

test_that("flowerCheckConnectivityDS detects unreachable address", {
  result <- flowerCheckConnectivityDS("192.0.2.1:99999", timeout_secs = 1)
  expect_type(result, "list")
  expect_false(result$reachable)
})

test_that("flowerCheckConnectivityDS rejects bad format", {
  result <- flowerCheckConnectivityDS("not-a-valid-address")
  expect_false(result$reachable)
  expect_true(grepl("Invalid", result$error))
})

test_that("connectivity checks reject every non-global IPv4 class", {
  unsafe <- c(
    "0.1.2.3", "10.0.0.1", "100.64.0.1", "100.100.100.200",
    "127.0.0.1", "169.254.169.254", "172.31.255.255",
    "192.0.0.1", "192.0.2.1", "192.88.99.1", "192.168.1.1",
    "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1",
    "255.255.255.255"
  )
  expect_true(all(vapply(
    unsafe, dsFlower:::.is_private_or_local_host, logical(1))))
  expect_false(dsFlower:::.is_private_or_local_host("8.8.8.8"))

  # The RFC6598 range includes cloud metadata endpoints such as
  # 100.100.100.200, so rejection must happen before opening a socket.
  withr::local_options(list(dsflower.restrict_connectivity = TRUE,
                            dsflower.coordinator_address = ""))
  result <- flowerCheckConnectivityDS("100.100.100.200:80")
  expect_false(result$reachable)
  expect_match(result$error, "not allowed")
})

test_that("flowerStatusDS returns status info", {
  handle <- mock_handle()
  dsFlower:::.setHandle("test_status", handle)
  on.exit(dsFlower:::.removeHandle("test_status"))

  status <- flowerStatusDS("test_status")
  expect_type(status, "list")
  expect_false(status$prepared)
  expect_false(status$node_ensured)
  expect_false(status$supernode_running)
})

test_that("flowerDestroyDS removes handle", {
  handle <- mock_handle()
  dsFlower:::.setHandle("test_destroy", handle)

  flowerDestroyDS("test_destroy")
  expect_error(dsFlower:::.getHandle("test_destroy"), "No Flower handle")
})
