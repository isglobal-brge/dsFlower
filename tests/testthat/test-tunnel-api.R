test_that("only the production tunnel API is exported and registered", {
  production <- c(
    "flowerTunnelUpDS", "flowerTunnelExchangeDS", "flowerTunnelDownDS"
  )
  removed <- c(
    "flowerTunnelResetDS", "flowerTunnelPollDS", "flowerTunnelPushDS",
    "flowerTunnelInjectDS", "flowerTunnelDrainDS", "flowerTunnelReapDS",
    "flowerTunnelTestClientDS", "flowerTunnelTestResultDS",
    "flowerTunnelSupernodeDS", "flowerTunnelLogDS"
  )

  exports <- getNamespaceExports("dsFlower")
  expect_true(all(production %in% exports))
  expect_false(any(removed %in% exports))

  ns <- asNamespace("dsFlower")
  expect_false(any(vapply(
    removed, exists, logical(1), envir = ns, inherits = FALSE
  )))

  aggregate <- packageDescription("dsFlower")$AggregateMethods
  registered <- trimws(strsplit(aggregate, ",", fixed = TRUE)[[1]])
  expect_true(all(production %in% registered))
  expect_false(any(removed %in% registered))
})

test_that("raw base constructors are never registered as aggregate methods", {
  aggregate <- packageDescription("dsFlower")$AggregateMethods
  registered <- trimws(strsplit(aggregate, ",", fixed = TRUE)[[1]])
  expect_false(any(grepl("^(c|list)(=|$)", registered)))
})

test_that("tunnel connection ids are strict, collision-free capabilities", {
  valid <- paste0("dsf_", strrep("a", 32))
  expect_identical(dsFlower:::.tunnel_conn_id(valid), valid)
  expect_error(dsFlower:::.tunnel_conn_id("dsfabcdefghij"), "Invalid")
  expect_error(dsFlower:::.tunnel_conn_id("dsf_../../escape"), "Invalid")
  expect_error(dsFlower:::.tunnel_conn_id(c(valid, valid)), "Invalid")
})

test_that("exchange rejects a nonce that was not opened by Up", {
  cid <- paste0("dsf_", strrep("b", 32))
  expect_error(
    dsFlower::flowerTunnelExchangeDS(cid),
    "Unknown or inactive tunnel session"
  )
  expect_false(dir.exists(dsFlower:::.tunnel_spool(cid, create = FALSE)))
})

test_that("tunnel ports are finite integers in the TCP range", {
  expect_identical(dsFlower:::.tunnel_port(1), 1L)
  expect_identical(dsFlower:::.tunnel_port("65535"), 65535L)
  for (bad in list(0, 65536, 1.5, NA_real_, Inf, "not-a-port", c(1, 2))) {
    expect_error(dsFlower:::.tunnel_port(bad), "Invalid tunnel listen port")
  }
})

test_that("a tunnel authorizes loopback only while its capability is ready and alive", {
  cid <- paste0("dsf_", strrep("c", 32))
  env <- dsFlower:::.dsflower_env
  key <- dsFlower:::.tunnel_forwarder_key(cid)
  old_cid <- env$tunnel_conn_id
  old_port <- env$tunnel_forwarder_port
  old_process <- env[[key]]
  withr::defer({
    env$tunnel_conn_id <- old_cid
    env$tunnel_forwarder_port <- old_port
    env[[key]] <- old_process
    unlink(dsFlower:::.tunnel_spool(cid, create = FALSE), recursive = TRUE)
  })

  alive <- TRUE
  env$tunnel_conn_id <- cid
  env$tunnel_forwarder_port <- 18080L
  env[[key]] <- structure(list(
    is_alive = function() alive,
    kill = function() { alive <<- FALSE; TRUE }
  ), class = "process")
  spool <- dsFlower:::.tunnel_spool(cid)

  expect_null(dsFlower:::.active_tunnel_port())
  file.create(file.path(spool, "ready"))
  expect_identical(dsFlower:::.active_tunnel_port(), 18080L)

  alive <- FALSE
  expect_null(dsFlower:::.active_tunnel_port())
  alive <- TRUE
  env$tunnel_conn_id <- "forged"
  expect_null(dsFlower:::.active_tunnel_port())
})

test_that("failed forwarder startup kills its process and clears all state", {
  cid <- paste0("dsf_", strrep("d", 32))
  env <- dsFlower:::.dsflower_env
  old_cid <- env$tunnel_conn_id
  old_port <- env$tunnel_forwarder_port
  withr::defer({
    env$tunnel_conn_id <- old_cid
    env$tunnel_forwarder_port <- old_port
  })
  env$tunnel_conn_id <- NULL
  env$tunnel_forwarder_port <- NULL
  checks <- 0L
  killed <- FALSE
  fake_process <- structure(list(
    is_alive = function() {
      checks <<- checks + 1L
      checks == 1L
    },
    kill = function() { killed <<- TRUE; TRUE },
    wait = function(timeout = -1) invisible(NULL)
  ), class = "process")
  local_mocked_bindings(
    process = list(new = function(...) fake_process),
    .package = "processx"
  )

  expect_error(
    dsFlower::flowerTunnelUpDS(cid, 18080L, "site"),
    "failed to start"
  )
  expect_null(env$tunnel_conn_id)
  expect_null(env$tunnel_forwarder_port)
  expect_true(killed)
  expect_null(env[[dsFlower:::.tunnel_forwarder_key(cid)]])
  expect_false(dir.exists(dsFlower:::.tunnel_spool(cid, create = FALSE)))
})

test_that("tunnel I/O is chunked and spool growth is capped", {
  cid <- paste0("dsf_", strrep("e", 32))
  spool <- dsFlower:::.tunnel_spool(cid)
  withr::defer(unlink(spool, recursive = TRUE))
  bytes <- as.raw(seq_len(10))
  dsFlower:::.tunnel_append(spool, "up.bin", bytes)

  first <- dsFlower:::.tunnel_read_at(spool, "up.bin", 0, max_bytes = 4)
  second <- dsFlower:::.tunnel_read_at(spool, "up.bin", first$eof, max_bytes = 4)
  expect_identical(first$data, bytes[1:4])
  expect_identical(first$eof, 4)
  expect_identical(second$data, bytes[5:8])
  expect_identical(second$eof, 8)

  expect_error(
    dsFlower:::.tunnel_append_at(
      spool, "down.bin", 0, as.raw(1:5), max_bytes = 4
    ),
    "spool limit"
  )
  expect_error(
    dsFlower:::.tunnel_dec(dsFlower:::.tunnel_enc(as.raw(1:5)), max_bytes = 4),
    "chunk size|oversized"
  )
})
