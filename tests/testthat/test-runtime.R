# Tests for R/runtime.R — SuperNode Registry

local_runtime_privacy_state <- function(.local_envir = parent.frame()) {
  state_dir <- tempfile("dsflower-runtime-state-")
  dir.create(state_dir, recursive = TRUE)
  secret <- file.path(state_dir, "node-secret")
  ledger <- file.path(state_dir, "ledger.sqlite")
  writeChar(strrep("a", 64), secret, eos = NULL)
  Sys.chmod(secret, "0600")
  file.create(ledger)
  withr::defer(unlink(state_dir, recursive = TRUE), envir = .local_envir)
  withr::local_options(list(dsflower.privacy_ledger_path = ledger),
                       .local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = secret,
    DSFLOWER_PRIVACY_LEDGER_PATH = NA_character_,
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
    DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
  ), .local_envir = .local_envir)
  invisible(state_dir)
}

test_that("the final Python boundary creates and repairs missing privacy state", {
  withr::with_tempdir({
    state_dir <- file.path(getwd(), "privacy")
    secret <- file.path(state_dir, "noise_root")
    ledger <- file.path(state_dir, "ledger.sqlite")
    withr::local_options(list(dsflower.privacy_ledger_path = ledger))
    withr::local_envvar(c(
      DSFLOWER_NODE_SECRET_FILE = secret,
      DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
      DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1",
      DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
    ))

    expect_false(file.exists(secret))
    expect_false(file.exists(ledger))
    dir.create(file.path(getwd(), "staging-1"))
    first_env <- dsFlower:::.build_clean_python_env(
      file.path(getwd(), "venv"), file.path(getwd(), "staging-1"))
    first_secret <- readLines(secret, warn = FALSE)
    expect_match(first_secret, "^[0-9a-f]{64}$")
    expect_true(file.exists(ledger))
    expect_identical(unname(first_env[["DSFLOWER_NODE_SECRET_FILE"]]), secret)
    expect_identical(unname(first_env[["DSFLOWER_PRIVACY_LEDGER_PATH"]]), ledger)

    unlink(secret)
    dir.create(file.path(getwd(), "staging-2"))
    second_env <- dsFlower:::.build_clean_python_env(
      file.path(getwd(), "venv"), file.path(getwd(), "staging-2"))
    expect_match(readLines(secret, warn = FALSE), "^[0-9a-f]{64}$")
    expect_false(identical(readLines(secret, warn = FALSE), first_secret))
    expect_identical(unname(second_env[["DSFLOWER_NODE_SECRET_FILE"]]), secret)

    con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    expect_equal(DBI::dbGetQuery(
      con, "SELECT COUNT(*) AS n FROM privacy_key_epochs")$n, 2L)
  })
})

test_that("the mandatory Python integrity bootstrap installs or fails closed", {
  staging <- withr::local_tempdir()
  source_dir <- withr::local_tempdir()
  source <- file.path(source_dir, "sitecustomize.py")
  writeLines("PIN = 'trusted'", source)

  hook_dir <- dsFlower:::.install_integrity_hook(staging, source)
  installed <- file.path(hook_dir, "sitecustomize.py")
  expect_true(file.exists(installed))
  expect_identical(
    digest::digest(file = installed, algo = "sha256"),
    digest::digest(file = source, algo = "sha256")
  )
  expect_error(
    dsFlower:::.install_integrity_hook(
      staging, file.path(source_dir, "missing.py")),
    "mandatory"
  )
})

test_that("the trusted Python environment does not inherit injection variables", {
  local_runtime_privacy_state()
  withr::local_envvar(c(
    PYTHONPATH = "/attacker/path",
    PYTHONSTARTUP = "/attacker/startup.py",
    PYTHONINSPECT = "1",
    LD_PRELOAD = "/attacker/preload.so",
    DSFLOWER_ATTACKER_VALUE = "present"
  ))
  staging <- withr::local_tempdir()
  env <- dsFlower:::.build_clean_python_env(
    tempfile("venv-"), staging, extra_pypath = "/trusted/hook")
  expect_identical(unname(env[["PYTHONPATH"]]), "/trusted/hook")
  expect_false(any(names(env) %in% c(
    "PYTHONSTARTUP", "PYTHONINSPECT", "LD_PRELOAD", "DSFLOWER_ATTACKER_VALUE")))
  expect_identical(unname(env[["PYTHONNOUSERSITE"]]), "1")
  expect_identical(
    unname(env[["FLWR_HOME"]]), file.path(staging, ".flwr"))
  expect_true(dir.exists(env[["FLWR_HOME"]]))
  expect_equal(
    bitwAnd(as.integer(file.info(env[["FLWR_HOME"]])$mode),
            as.integer(strtoi("77", base = 8))),
    0L
  )
  expect_identical(unname(env[["DSF_SAA_SANDBOX_OK"]]), "0")
  expect_identical(
    unname(env[["DSF_HOOK_RESOURCE_ISOLATION_OK"]]), "0")

  attested <- withr::with_options(list(
    dsflower.hook_sandbox_attested = TRUE,
    dsflower.hook_resource_isolation_attested = TRUE
  ), dsFlower:::.build_clean_python_env(
    tempfile("venv-"), withr::local_tempdir()))
  expect_identical(unname(attested[["DSF_SAA_SANDBOX_OK"]]), "1")
  expect_identical(
    unname(attested[["DSF_HOOK_RESOURCE_ISOLATION_OK"]]), "1")

  unsafe_staging <- withr::local_tempdir()
  expect_true(file.symlink(withr::local_tempdir(),
                           file.path(unsafe_staging, ".flwr")))
  expect_error(
    dsFlower:::.build_clean_python_env(tempfile("venv-"), unsafe_staging),
    "Flower home is unsafe"
  )
})

test_that(".supernode_lookup returns NULL for unknown manifest_dir", {
  result <- dsFlower:::.supernode_lookup("/nonexistent/path")
  expect_null(result)
})

test_that(".supernode_list returns empty data.frame when no supernodes", {
  # Clear registry
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  result <- dsFlower:::.supernode_list()
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
  expect_true("manifest_dir" %in% names(result))
  expect_true("superlink_address" %in% names(result))
  expect_true("pid" %in% names(result))
  expect_true("alive" %in% names(result))
  expect_true("started_at" %in% names(result))
})

test_that(".supernode_stop is safe for unknown manifest_dir", {
  expect_true(dsFlower:::.supernode_stop("/nonexistent/path"))
})

test_that("Registry behavior works with mock process", {
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  mock_manifest <- file.path(tempdir(), "mock_manifest_dir")

  # Create a mock process object
  mock_proc <- list(
    is_alive = function() TRUE,
    get_pid = function() 99999L,
    signal = function(sig) invisible(NULL),
    wait = function(timeout = 5000) invisible(NULL),
    kill = function() invisible(NULL)
  )

  # Manually insert an entry
  entry <- list(
    process = mock_proc,
    superlink_address = "mock:9092",
    manifest_dir = mock_manifest,
    log_path = file.path(tempdir(), "mock.log"),
    pid = 99999L,
    started_at = Sys.time()
  )
  assign(mock_manifest, entry, envir = reg)

  # Lookup should find it
  found <- dsFlower:::.supernode_lookup(mock_manifest)
  expect_false(is.null(found))
  expect_equal(found$pid, 99999L)

  # List should include it
  node_list <- dsFlower:::.supernode_list()
  expect_equal(nrow(node_list), 1)
  expect_equal(node_list$superlink_address, "mock:9092")
  expect_equal(node_list$manifest_dir, mock_manifest)
  expect_true(node_list$alive)

  # Clean up
  rm(list = ls(reg), envir = reg)
})

test_that("Registry removes dead processes on lookup", {
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  dead_manifest <- file.path(tempdir(), "dead_manifest_dir")

  # Create a mock dead process
  mock_proc <- list(
    is_alive = function() FALSE,
    get_pid = function() 88888L,
    signal = function(sig) invisible(NULL),
    wait = function(timeout = 5000) invisible(NULL),
    kill = function() invisible(NULL)
  )

  entry <- list(
    process = mock_proc,
    superlink_address = "dead:9092",
    manifest_dir = dead_manifest,
    log_path = file.path(tempdir(), "dead.log"),
    pid = 88888L,
    started_at = Sys.time()
  )
  assign(dead_manifest, entry, envir = reg)

  # Lookup should return NULL and clean the entry
  result <- dsFlower:::.supernode_lookup(dead_manifest)
  expect_null(result)
  expect_false(exists(dead_manifest, envir = reg))
})

test_that(".random_available_port returns port in valid range", {
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  port <- dsFlower:::.random_available_port()
  expect_true(port >= 10000L && port <= 60000L)
})

test_that(".random_available_port avoids ports used by live SuperNodes", {
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  # Register a mock SuperNode using port 12345
  mock_manifest <- file.path(tempdir(), "port_test_manifest")
  mock_proc <- list(
    is_alive = function() TRUE,
    get_pid = function() 77777L
  )
  entry <- list(
    process = mock_proc,
    clientappio_port = 12345L,
    superlink_address = "mock:9092",
    manifest_dir = mock_manifest,
    log_path = file.path(tempdir(), "mock.log"),
    pid = 77777L,
    started_at = Sys.time()
  )
  assign(mock_manifest, entry, envir = reg)

  # Generate 50 ports — none should be 12345
  ports <- replicate(50, dsFlower:::.random_available_port())
  expect_true(all(ports != 12345L))

  # Clean up
  rm(list = ls(reg), envir = reg)
})

test_that(".supernode_ensure blocks when policy disables spawning", {
  withr::with_options(list(dsflower.allow_supernode_spawn = FALSE), {
    expect_error(
      dsFlower:::.supernode_ensure("test:9092", tempdir()),
      "disabled by server policy"
    )
  })
})

test_that(".supernode_ensure uses --root-certificates when ca_cert_path provided", {
  local_runtime_privacy_state()
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  # Create a temporary ca.pem file
  ca_pem_path <- tempfile(fileext = ".pem")
  writeLines("-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----",
             ca_pem_path)
  on.exit(unlink(ca_pem_path))

  captured_args <- NULL
  # Mock processx::process$new to capture args
  mock_proc <- list(
    is_alive = function() TRUE,
    get_pid = function() 55555L,
    signal = function(sig) invisible(NULL),
    wait = function(timeout = 5000) invisible(NULL),
    kill = function() invisible(NULL)
  )

  local_mocked_bindings(
    .random_available_port = function() 11111L
  )

  mock_manifest <- file.path(tempdir(), "tls_test_manifest")
  dir.create(mock_manifest, showWarnings = FALSE)
  fake_supernode <- tempfile("flower-supernode")
  file.create(fake_supernode)
  jsonlite::write_json(
    list(supernode_cmd = fake_supernode, python = "python3",
         venv_path = tempdir()),
    file.path(mock_manifest, "runtime.json"),
    auto_unbox = TRUE
  )

  # We need to mock processx::process$new — use a different approach:
  # intercept at the registry level by checking args after the fact
  local_mocked_bindings(
    process = list(new = function(command, args, ...) {
      captured_args <<- args
      mock_proc
    }),
    .package = "processx"
  )

  entry <- dsFlower:::.supernode_ensure(
    "test:9092", mock_manifest, "python3", ca_cert_path = ca_pem_path
  )

  expect_true("--root-certificates" %in% captured_args)
  expect_true(ca_pem_path %in% captured_args)
  expect_equal(entry$ca_cert_path, ca_pem_path)

  rm(list = ls(reg), envir = reg)
})

test_that(".supernode_ensure errors when no ca_cert_path", {
  reg <- dsFlower:::.supernode_registry
  rm(list = ls(reg), envir = reg)

  mock_manifest <- file.path(tempdir(), "no_cert_test_manifest")
  dir.create(mock_manifest, showWarnings = FALSE)
  fake_supernode <- tempfile("flower-supernode")
  file.create(fake_supernode)
  jsonlite::write_json(
    list(supernode_cmd = fake_supernode, python = "python3",
         venv_path = tempdir()),
    file.path(mock_manifest, "runtime.json"),
    auto_unbox = TRUE
  )

  expect_error(
    dsFlower:::.supernode_ensure("test:9092", mock_manifest, "python3"),
    "CA certificate not found"
  )

  rm(list = ls(reg), envir = reg)
})

test_that("resolved Python dependency versions are recorded deterministically", {
  venv <- withr::local_tempdir()
  site <- file.path(venv, "lib", "python3.11", "site-packages")
  dir.create(file.path(site, "flwr-1.31.0.dist-info"), recursive = TRUE)
  dir.create(file.path(site, "opacus-1.6.0.dist-info"), recursive = TRUE)
  writeLines(c("Name: flwr", "Version: 1.31.0"),
             file.path(site, "flwr-1.31.0.dist-info", "METADATA"))
  writeLines(c("Name: opacus", "Version: 1.6.0"),
             file.path(site, "opacus-1.6.0.dist-info", "METADATA"))

  expect_true(dsFlower:::.record_venv_versions(venv))
  expect_equal(readLines(file.path(venv, ".dsflower_versions.txt")),
               c("flwr==1.31.0", "opacus==1.6.0"))
})

test_that("uv bootstrap requires an immutable release and archive digest", {
  withr::local_envvar(c(DSFLOWER_UV_VERSION = "", DSFLOWER_UV_SHA256 = ""))
  withr::local_options(list(dsflower.uv_version = "", dsflower.uv_sha256 = ""))
  expect_error(dsFlower:::.uv_bootstrap_config(), "mutable 'latest'.*disabled")

  withr::local_envvar(c(
    DSFLOWER_UV_VERSION = "0.11.14",
    DSFLOWER_UV_SHA256 = strrep("a", 64)
  ))
  expect_equal(
    dsFlower:::.uv_bootstrap_config(),
    list(version = "0.11.14", sha256 = strrep("a", 64))
  )

  withr::local_envvar(DSFLOWER_UV_SHA256 = "not-a-digest")
  expect_error(dsFlower:::.uv_bootstrap_config(), "64 hexadecimal")

  withr::local_envvar(c(
    DSFLOWER_UV_VERSION = "latest",
    DSFLOWER_UV_SHA256 = strrep("a", 64)
  ))
  expect_error(dsFlower:::.uv_bootstrap_config(), "valid release tag")
})

test_that("hash-locked Python requirements bind the venv marker", {
  lock <- withr::local_tempfile()
  writeLines("example==1.0 --hash=sha256:aaaaaaaa", lock)
  withr::local_envvar(c(DSFLOWER_PYTHON_LOCK = lock,
                        DSFLOWER_PYTHON_VERSION = "",
                        DSFLOWER_REQUIRE_PYTHON_LOCK = ""))
  withr::local_options(dsflower.python_version = "3.11")

  expected <- paste0("python=3.11;lock-sha256:",
                     digest::digest(file = lock, algo = "sha256"))
  expect_equal(dsFlower:::.python_env_spec_hash("pytorch"), expected)
})

test_that("strict Python provisioning fails closed without a lock", {
  withr::local_envvar(c(
    DSFLOWER_PYTHON_LOCK = "",
    DSFLOWER_REQUIRE_PYTHON_LOCK = "true"
  ))
  withr::local_options(dsflower.python_lock = "")
  expect_error(dsFlower:::.python_lock_path(must_exist = TRUE),
               "hash-locked Python environment is required")
  expect_true(is.na(dsFlower:::.python_env_spec_hash("pytorch")))
})

test_that(".supernode_read_log returns empty for unknown manifest_dir", {
  result <- dsFlower:::.supernode_read_log("/nonexistent/path")
  expect_type(result, "character")
  expect_equal(length(result), 0)
})
