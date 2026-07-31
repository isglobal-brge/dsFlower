# Tests for R/interface.R — DataSHIELD Methods

local_interface_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-interface-state-")
  dir.create(state_dir, recursive = TRUE)
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_options(list(
    dsflower.privacy_ledger_path = file.path(state_dir, "ledger.sqlite")
  ), .local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(state_dir, "node-secret"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1",
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
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

test_that("run horizon and data routing are pinned before staging", {
  expect_error(
    dsFlower:::.normalizeRunHorizon(list("num-server-rounds" = 1.5)),
    "positive integer"
  )
  expect_error(
    dsFlower:::.normalizeRunHorizon(list(
      "num-server-rounds" = 2L, num_rounds = 3L
    )),
    "disagree"
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
    list(dsflower.dp_egress_time_pad = 3661),
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
    dsFlower:::.addDpConfigToRunConfig(base)), "timeout \\+ 5")
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
  result <- flowerPrepareRunDS("test_prepare", "target", c("f1", "f2", "f3"))
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
  expect_true(manifest[["privacy-reserved"]])
  expect_equal(manifest[["privacy-allocation-index"]], 1L)
})

test_that("flowerPrepareRunDS does not expose a minimum-size admission bit", {
  local_interface_privacy_state()
  # Create tiny dataset
  tiny_dir <- tempdir()
  tiny_path <- file.path(tiny_dir, "tiny_test.csv")
  utils::write.csv(data.frame(f1 = 1:2, target = 0:1), tiny_path, row.names = FALSE)
  on.exit(unlink(tiny_path))

  handle <- mock_handle(data_path = tiny_path, data_format = "csv")
  dsFlower:::.setHandle("test_tiny", handle)
  on.exit(dsFlower:::.removeHandle("test_tiny"), add = TRUE)

  expect_no_error(flowerPrepareRunDS("test_tiny", "target"))
  state <- dsFlower:::.getHandle("test_tiny")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_equal(manifest$n_samples, 2L)
  expect_equal(manifest$n_units, 2L)
  expect_true(manifest[["privacy-reserved"]])
})

test_that("prepare reserves before schema access and never refunds failures", {
  local_interface_privacy_state()
  dsFlower:::.setHandle(
    "test_failed_prepare_reservation",
    mock_handle(table_data = data.frame(f1 = 1:5)))
  withr::defer(dsFlower:::.removeHandle("test_failed_prepare_reservation"))
  expect_error(
    flowerPrepareRunDS("test_failed_prepare_reservation", "target", "f1"),
    "Private data preparation failed on this node"
  )

  dsFlower:::.setHandle(
    "test_next_prepare_reservation",
    mock_handle(table_data = data.frame(f1 = 1:5, target = rep(0:1, length.out = 5))))
  withr::defer(dsFlower:::.removeHandle("test_next_prepare_reservation"))
  expect_no_error(
    flowerPrepareRunDS("test_next_prepare_reservation", "target", "f1"))
  state <- dsFlower:::.getHandle("test_next_prepare_reservation")
  manifest <- jsonlite::fromJSON(file.path(state$staging_dir, "manifest.json"))
  expect_equal(manifest[["privacy-allocation-index"]], 2L)
})

test_that("privacy-tail prepare never opens private sources and remains nonblocking", {
  local_interface_privacy_state()
  withr::local_options(list(
    dsflower.dp_min_release_epsilon = 10,
    dsflower.dp_unit = "patient",
    dsflower.patient_column = "subject_id"
  ))

  private_access <- function(...) {
    stop("private source was accessed", call. = FALSE)
  }
  local_mocked_bindings(
    .loadTrainingData = private_access,
    .stageData = private_access,
    .stageFromDescriptor = private_access,
    .stage_image_manifest = private_access
  )

  tree_config <- list(
    "dp-track" = "trees",
    "num-server-rounds" = 1L,
    "num-features" = 2L,
    "num-classes" = 2L,
    "target-levels" = c(0, 1),
    "feature-bounds" = list(lower = c(-1, -2), upper = c(1, 2)),
    "gbdt-spec" = list(
      objective = "binary:logistic", max_depth = 2L, n_trees = 3L,
      learning_rate = 0.1, reg_lambda = 1, n_bins = 8L,
      feature_ranges = list(c(-1, 1), c(-2, 2))
    )
  )

  handles <- list(
    tail_file = mock_handle(
      data_path = "/private/must-not-be-opened.csv", data_format = "csv"),
    tail_table = mock_handle(
      table_data = data.frame(private = "must-not-be-read")),
    tail_image = within(mock_handle(), {
      source <- "descriptor"
      source_kind <- "image_bundle"
      descriptor <- structure(
        list(source_kind = "image_bundle", private = "must-not-be-read"),
        class = "FlowerDatasetDescriptor")
      table_data <- NULL
      data_format <- "descriptor"
    })
  )
  withr::defer(for (handle_name in names(handles)) {
    try(dsFlower:::.removeHandle(handle_name), silent = TRUE)
  })

  for (name in names(handles)) {
    dsFlower:::.setHandle(name, handles[[name]])
    config <- if (identical(name, "tail_file")) tree_config else list()
    expect_no_error(flowerPrepareRunDS(name, "target", c("f1", "f2"), config))

    state <- dsFlower:::.getHandle(name)
    expect_true(state$prepared)
    expect_false(state$node_ensured)
    manifest_path <- file.path(state$staging_dir, "manifest.json")
    expect_true(file.exists(manifest_path))
    expect_setequal(list.files(state$staging_dir), "manifest.json")
    manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
    expect_true(manifest[["privacy-reserved"]])
    expect_false(manifest[["privacy-release-enabled"]])
    expect_identical(manifest$n_samples, 0L)
    expect_identical(manifest$n_units, 0L)
    expect_identical(manifest$source_kind, "privacy_noop")
    expect_identical(manifest[["dp-unit"]], "patient")
    expect_identical(manifest$patient_column, "subject_id")
    expect_false("data_file" %in% names(manifest))
    expect_false("samples_file" %in% names(manifest))
  }

  tree_state <- dsFlower:::.getHandle("tail_file")
  tree_manifest <- jsonlite::fromJSON(
    file.path(tree_state$staging_dir, "manifest.json"), simplifyVector = FALSE)
  expect_identical(tree_manifest[["dp-track"]], "trees")
  expect_identical(tree_manifest[["num-features"]], 2L)
  expect_identical(tree_manifest[["gbdt-spec"]]$n_trees, 3L)

  # A new query receives the next public no-op allocation; it is not rejected.
  old_token <- tree_state$run_token
  expect_no_error(flowerPrepareRunDS(
    "tail_file", "target", c("f1", "f2"), tree_config))
  expect_false(identical(dsFlower:::.getHandle("tail_file")$run_token, old_token))
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
    handle = NULL, target_column = "target", template_name = NULL,
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

# --- TLS ca.pem handling ---

test_that("flowerEnsureSuperNodeDS writes ca.pem when ca_cert_pem provided", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_tls"), add = TRUE)

  # Prepare the handle first
  flowerPrepareRunDS("test_tls", "target", c("f1", "f2", "f3"))
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
                                 template_name = NULL, insecure = FALSE) {
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
  expect_true(manifest[["privacy-reserved"]])
  expect_equal(manifest[["privacy-allocation-index"]], 1L)
  expect_gt(manifest[["privacy-epsilon"]], 0)
})

test_that("flowerEnsureSuperNodeDS works without ca_cert_pem (backwards compat)", {
  local_interface_privacy_state()
  csv_path <- create_test_csv(n = 200)
  on.exit(unlink(csv_path))

  handle <- mock_handle(data_path = csv_path, data_format = "csv")
  dsFlower:::.setHandle("test_no_tls", handle)
  on.exit(dsFlower:::.removeHandle("test_no_tls"), add = TRUE)

  flowerPrepareRunDS("test_no_tls", "target", c("f1", "f2", "f3"))

  local_mocked_bindings(
    .active_tunnel_port = function() 18080L,
    .supernode_ensure = function(superlink_address, manifest_dir,
                                 python_path, ca_cert_path = NULL,
                                 template_name = NULL, insecure = FALSE) {
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

test_that("flowerGetCapabilitiesDS returns expected structure", {
  caps <- flowerGetCapabilitiesDS()
  expect_type(caps, "list")
  expect_true("dsflower_version" %in% names(caps))
  expect_true("python_version" %in% names(caps))
  expect_true("flower_version" %in% names(caps))
  expect_true("torch_version" %in% names(caps))
  expect_true("opacus_version" %in% names(caps))
  expect_true("runtime_versions_sha256" %in% names(caps))
  expect_true("templates" %in% names(caps))
  expect_identical(caps$templates, character())
  expect_true(caps$templates_deprecated)
  expect_false(caps$allow_custom_config)
  expect_true(caps$allow_custom_config_deprecated)
  expect_identical(caps$dp_tracks, c("neural", "trees", "egress"))
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
      "multilabel_bce", "hinge", "ordinal", "negbin_nll", "gamma_nll")
  )
  expect_identical(caps$tree_objectives, "binary:logistic")
  expect_setequal(
    caps$aggregation_strategies,
    c("fedavg", "fedadam", "fedadagrad", "fedyogi", "fedavgm")
  )
  expect_true("max_rounds" %in% names(caps))
  expect_true("min_samples" %in% names(caps))
  expect_false("secure_aggregation_supported" %in% names(caps))
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

test_that("capabilities are invariant to existing or missing handle symbols", {
  name <- "test_capabilities_no_rows"
  dsFlower:::.setHandle(name, list(
    data_path = tempfile(), source = "table",
    table_data = data.frame(x = 1:8, y = 9:16),
    prepared = FALSE, node_ensured = FALSE))
  withr::defer(dsFlower:::.removeHandle(name))

  existing <- flowerGetCapabilitiesDS(name)
  missing <- flowerGetCapabilitiesDS("definitely_missing_handle")
  expect_identical(existing, missing)
  expect_false(any(c(
    "data_n_rows", "data_n_cols", "data_columns", "data_source",
    "prepared", "node_ensured", "has_imagedata", "image_assets"
  ) %in% names(existing)))
})

test_that("disabled compatibility egress never looks up private symbols", {
  existing_data <- data.frame(secret_a = 1:8, secret_b = 9:16)
  requested <- c("requested_b", "requested_a")

  existing <- flowerFeatureStatsDS("existing_data", requested)
  missing <- flowerFeatureStatsDS("definitely_missing_data", requested)
  expect_identical(existing, missing)
  expect_identical(existing$features, requested)
  expect_identical(existing$n, c(0, 0))
  expect_identical(existing$sum, c(0, 0))
  expect_identical(existing$sumsq, c(0, 0))
  expect_true(existing$disabled)

  # NULL cannot mean "all server columns" without creating a schema oracle.
  empty <- flowerFeatureStatsDS("existing_data", NULL)
  expect_identical(empty$features, character())
  expect_identical(empty$n, numeric())

  handle_name <- "test_disabled_egress_handle"
  dsFlower:::.setHandle(handle_name, list(private = "state"))
  withr::defer(dsFlower:::.removeHandle(handle_name))
  expect_identical(
    flowerMetricsDS(handle_name),
    flowerMetricsDS("definitely_missing_handle")
  )
  expect_identical(
    flowerLogDS(handle_name),
    flowerLogDS("definitely_missing_handle")
  )
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

test_that(".parseFlowerMetrics handles missing log", {
  result <- dsFlower:::.parseFlowerMetrics(NULL)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)

  result2 <- dsFlower:::.parseFlowerMetrics("/nonexistent/log.txt")
  expect_s3_class(result2, "data.frame")
  expect_equal(nrow(result2), 0)
})

test_that(".parseFlowerMetrics extracts metrics from log", {
  log_path <- tempfile(fileext = ".log")
  writeLines(c(
    "[Round 1] loss = 0.693",
    "[Round 1] accuracy = 0.55",
    "[Round 2] loss = 0.500",
    "[Round 2] accuracy = 0.70",
    "Some other log line"
  ), log_path)
  on.exit(unlink(log_path))

  metrics <- dsFlower:::.parseFlowerMetrics(log_path)
  expect_s3_class(metrics, "data.frame")
  expect_true(nrow(metrics) >= 4)
  expect_true("loss" %in% metrics$metric)
  expect_true("accuracy" %in% metrics$metric)
})
