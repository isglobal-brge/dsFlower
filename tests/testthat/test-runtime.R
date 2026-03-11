# Tests for R/runtime.R — SuperNode Registry

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

test_that(".supernode_read_log returns empty for unknown manifest_dir", {
  result <- dsFlower:::.supernode_read_log("/nonexistent/path")
  expect_type(result, "character")
  expect_equal(length(result), 0)
})
