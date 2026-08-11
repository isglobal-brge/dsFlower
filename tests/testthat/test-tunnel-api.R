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
    dsFlower::flowerTunnelUpDS(cid, 18080L, "site", protocol_abi = 4L),
    "failed to start"
  )
  expect_null(env$tunnel_conn_id)
  expect_null(env$tunnel_forwarder_port)
  expect_true(killed)
  expect_null(env[[dsFlower:::.tunnel_forwarder_key(cid)]])
  expect_false(dir.exists(dsFlower:::.tunnel_spool(cid, create = FALSE)))
})

test_that("tunnel startup rejects a mismatched ABI before side effects", {
  cid <- paste0("dsf_", strrep("6", 32))
  env <- dsFlower:::.dsflower_env
  old_cid <- env$tunnel_conn_id
  old_port <- env$tunnel_forwarder_port
  withr::defer({
    env$tunnel_conn_id <- old_cid
    env$tunnel_forwarder_port <- old_port
  })
  env$tunnel_conn_id <- NULL
  env$tunnel_forwarder_port <- NULL

  expect_error(
    dsFlower::flowerTunnelUpDS(cid, 18080L, "site"),
    "Incompatible dsFlower tunnel protocol ABI"
  )
  expect_error(
    dsFlower::flowerTunnelUpDS(cid, 18080L, "site", protocol_abi = 3L),
    "Incompatible dsFlower tunnel protocol ABI"
  )
  expect_null(env$tunnel_conn_id)
  expect_null(env$tunnel_forwarder_port)
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

test_that("absolute offsets remain idempotent across spool compaction", {
  cid <- paste0("dsf_", strrep("9", 32))
  spool <- dsFlower:::.tunnel_spool(cid)
  withr::defer(unlink(spool, recursive = TRUE))
  dsFlower:::.tunnel_reset_spool_file(spool, "down.bin", base = 100)
  dsFlower:::.tunnel_append(spool, "down.bin", charToRaw("abcdef"))
  expect_identical(
    dsFlower:::.tunnel_read_at(spool, "down.bin", 102, 4),
    list(data = charToRaw("cdef"), eof = 106)
  )

  expect_error(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 104, charToRaw("efGH"), max_bytes = 8
  ), "replay geometry")
  expect_identical(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 106, charToRaw("GH"), max_bytes = 8
  ), 108)
  expect_error(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 106, charToRaw("GX"), max_bytes = 8
  ), "replay payload")
  expect_error(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 106, charToRaw("G"), max_bytes = 8
  ), "replay geometry")
  # Exact retry has identical offset, length, and bytes.
  expect_identical(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 106, charToRaw("GH"), max_bytes = 8
  ), 108)
  expect_error(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 108, charToRaw("I"), max_bytes = 8
  ), "spool limit")

  # Model an atomic compaction that drops acknowledged bytes [100, 106).
  dsFlower:::.tunnel_reset_spool_file(spool, "down.bin", base = 106)
  dsFlower:::.tunnel_append(spool, "down.bin", charToRaw("GH"))
  expect_identical(dsFlower:::.tunnel_spool_state(spool, "down.bin"), list(
    base = 106, bytes = 2, eof = 108
  ))
  expect_identical(dsFlower:::.tunnel_append_at(
    spool, "down.bin", 106, charToRaw("GH"), max_bytes = 8
  ), 108)
  expect_identical(
    dsFlower:::.tunnel_read_at(spool, "down.bin", 106, 2)$data,
    charToRaw("GH")
  )
  expect_identical(dsFlower:::.tunnel_publish_up_ack(spool, 107), 107)
  expect_identical(dsFlower:::.tunnel_publish_up_ack(spool, 107), 107)
  expect_error(
    dsFlower:::.tunnel_publish_up_ack(spool, 106),
    "cannot move backwards"
  )
})

.activate_test_tunnel <- function(cid, node = "site1", generation = 1) {
  env <- dsFlower:::.dsflower_env
  key <- dsFlower:::.tunnel_forwarder_key(cid)
  old <- list(
    cid = env$tunnel_conn_id,
    port = env$tunnel_forwarder_port,
    process = env[[key]],
    name = env[[paste0("tunnel_name_", cid)]]
  )
  spool <- dsFlower:::.tunnel_spool(cid)
  withr::defer({
    env$tunnel_conn_id <- old$cid
    env$tunnel_forwarder_port <- old$port
    env[[key]] <- old$process
    env[[paste0("tunnel_name_", cid)]] <- old$name
    unlink(spool, recursive = TRUE)
  }, envir = parent.frame())

  env$tunnel_conn_id <- cid
  env$tunnel_forwarder_port <- 18080L
  env[[key]] <- structure(list(
    is_alive = function() TRUE,
    kill = function() TRUE
  ), class = "process")
  env[[paste0("tunnel_name_", cid)]] <- node
  file.create(file.path(spool, "ready"))
  dsFlower:::.tunnel_reset_spool_file(spool, "up.bin")
  dsFlower:::.tunnel_reset_spool_file(spool, "down.bin")
  writeLines("0", file.path(spool, "up.ack"))
  writeLines("0", file.path(spool, "down.ack"))
  writeLines(as.character(generation), file.path(spool, "gen"))
  spool
}

test_that("the maximum DSI-safe tunnel chunk round-trips without truncation", {
  payload <- as.raw((seq_len(512 * 1024L) - 1L) %% 256L)
  encoded_payload <- dsFlower:::.tunnel_enc(payload)
  expect_lt(nchar(encoded_payload, type = "bytes"), 1000000L)
  expect_identical(
    dsFlower:::.tunnel_dec(encoded_payload, max_bytes = 512 * 1024L),
    payload
  )
  expect_identical(
    dsFlower:::.app_b64_dec(encoded_payload, max_bytes = 512 * 1024L),
    payload
  )

  cid <- paste0("dsf_", strrep("f", 32))
  spool <- .activate_test_tunnel(cid)
  result <- dsFlower::flowerTunnelExchangeDS(
    cid, pa = 0, pd = encoded_payload, pf = 0, g = 1
  )
  expect_identical(result$g, 1)
  expect_identical(result$sz, as.numeric(length(payload)))
  expect_identical(
    dsFlower:::.tunnel_read_at(
      spool, "down.bin", 0, max_bytes = length(payload)
    )$data,
    payload
  )
})

test_that("tunnel chunks cannot exceed the DSI expression-safe maximum", {
  expect_identical(dsFlower:::.tunnel_chunk_bytes(), 512L * 1024L)
  withr::local_options(list(dsflower.tunnel_chunk_bytes = 512L * 1024L + 1L))
  expect_error(dsFlower:::.tunnel_chunk_bytes(), "Invalid.*tunnel_chunk_bytes")
})

test_that("512 KiB tunnel chunks traverse real DSI and DSLite", {
  skip_if_not_installed("DSLite")
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  skip_if(!nzchar(python), "Python is required for the tunnel forwarder")
  withr::local_options(list(
    dsflower.tunnel_chunk_bytes = 512 * 1024L,
    dsflower.tunnel_spool_max_bytes = 4 * 1024^2
  ))

  server <- DSLite::newDSLiteServer(tables = list())
  for (method in c(
    "flowerTunnelUpDS", "flowerTunnelExchangeDS", "flowerTunnelDownDS"
  )) {
    server$aggregateMethod(method, paste0("dsFlower::", method))
  }
  server_name <- paste0("dsflower_tunnel_dslite_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSLite::dsConnect(
    DSLite::DSLite(), name = "site", url = server_name
  )
  withr::defer(DSLite::dsDisconnect(connection))
  conns <- list(site = connection)

  port_result <- processx::run(python, c("-c", paste(
    "import socket", "s=socket.socket()", "s.bind(('127.0.0.1',0))",
    "print(s.getsockname()[1])", "s.close()", sep = ";"
  )))
  port <- as.integer(trimws(port_result$stdout))
  cid <- paste0("dsf_", strrep("6", 32))
  ready <- DSI::datashield.aggregate(conns, call(
    "flowerTunnelUpDS", cid, port, "site", protocol_abi = 4L
  ))
  expect_true(ready$site$ok)
  expect_equal(ready$site$chunk_bytes, 512 * 1024)
  withr::defer(tryCatch(
    DSI::datashield.aggregate(conns, call("flowerTunnelDownDS", cid)),
    error = function(e) NULL
  ))

  payload <- as.raw((seq_len(512L * 1024L) - 1L) %% 256L)
  result <- DSI::datashield.aggregate(conns, call(
    "flowerTunnelExchangeDS", cid, pa = 0,
    pd = dsFlower:::.tunnel_enc(payload), pf = 0, g = 0
  ))
  expect_true(result$site$ok)
  expect_equal(result$site$sz, length(payload))
  expect_identical(dsFlower:::.tunnel_read_at(
    dsFlower:::.tunnel_spool(cid, create = FALSE), "down.bin", 0,
    max_bytes = length(payload)
  )$data, payload)
})

test_that("generation fencing rejects stale reconnect traffic", {
  cid <- paste0("dsf_", strrep("1", 32))
  spool <- .activate_test_tunnel(cid, generation = 2)
  result <- dsFlower::flowerTunnelExchangeDS(
    cid, pa = 0, pd = dsFlower:::.tunnel_enc(charToRaw("stale")),
    pf = 0, g = 1
  )
  expect_identical(result, list(
    ok = TRUE, node = "site1", sz = 0, ud = "", ue = 0, g = 2))
  expect_identical(dsFlower:::.tunnel_spool_state(
    spool, "down.bin"
  )$bytes, 0)

  result <- dsFlower::flowerTunnelExchangeDS(
    cid, pa = 0, pd = dsFlower:::.tunnel_enc(charToRaw("current")),
    pf = 0, g = 2
  )
  expect_identical(result$sz, 7)
  expect_identical(
    dsFlower:::.tunnel_read_at(spool, "down.bin", 0, max_bytes = 7)$data,
    charToRaw("current")
  )
  expect_identical(dsFlower::flowerTunnelExchangeDS(
    cid, pa = 7, pd = "", pf = 0, g = 2
  )$sz, 7)
})

test_that("tunnel exchange accepts only direct transport arguments", {
  expect_identical(
    names(formals(dsFlower::flowerTunnelExchangeDS)),
    c("conn_id", "pa", "pd", "pf", "g"))
})

test_that("downstream backpressure still drains the upstream stream", {
  cid <- paste0("dsf_", strrep("5", 32))
  spool <- .activate_test_tunnel(cid, generation = 1)
  chunk <- 16 * 1024L
  cap <- 1024^2L
  withr::local_options(list(
    dsflower.tunnel_chunk_bytes = chunk,
    dsflower.tunnel_spool_max_bytes = cap
  ))
  dsFlower:::.tunnel_append(spool, "down.bin", as.raw(rep(0x22, cap)))
  upstream <- charToRaw("upstream-must-progress")
  dsFlower:::.tunnel_append(spool, "up.bin", upstream)

  result <- dsFlower::flowerTunnelExchangeDS(
    cid, pa = cap,
    pd = dsFlower:::.tunnel_enc(as.raw(rep(0x33, chunk))),
    pf = 0, g = 1
  )

  expect_identical(result$sz, as.numeric(cap))
  expect_identical(result$ue, as.numeric(length(upstream)))
  expect_identical(dsFlower:::.tunnel_dec(result$ud, chunk), upstream)
  expect_identical(
    dsFlower:::.tunnel_spool_state(spool, "down.bin")$eof,
    as.numeric(cap)
  )
})

test_that("the forwarder compacts both streams and fences a concurrent reconnect", {
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  skip_if(!nzchar(python), "Python is required for the tunnel forwarder")
  script <- system.file("python", "dsi_tunnel_forward.py", package = "dsFlower")
  skip_if(!nzchar(script), "Tunnel forwarder is not installed")

  port_result <- processx::run(
    python,
    c("-c", paste(
      "import socket",
      "s=socket.socket()",
      "s.bind(('127.0.0.1',0))",
      "print(s.getsockname()[1])",
      "s.close()",
      sep = ";"
    ))
  )
  port <- as.integer(trimws(port_result$stdout))
  spool <- tempfile("dsflower-forwarder-")
  dir.create(spool, mode = "0700")
  writeLines(".", file.path(spool, "relay_hb"))
  proc <- processx::process$new(
    python,
    c(script, "--listen", paste0("127.0.0.1:", port), "--spool", spool),
    env = c(
      # This test targets compaction/reconnect, not heartbeat expiry. Leave
      # enough headroom for slow Windows file-lock and process scheduling.
      "current", DSFLOWER_RELAY_TTL = "300",
      DSFLOWER_TUNNEL_SPOOL_MAX_BYTES = as.character(128 * 1024)
    ),
    stdout = "|", stderr = "|", cleanup = TRUE, cleanup_tree = TRUE
  )
  withr::defer({
    if (proc$is_alive()) proc$kill()
    unlink(spool, recursive = TRUE)
  })
  wait_for <- function(predicate, timeout = 5) {
    deadline <- Sys.time() + timeout
    while (Sys.time() < deadline) {
      if (isTRUE(predicate())) return(TRUE)
      if (!proc$is_alive()) return(FALSE)
      Sys.sleep(0.02)
    }
    FALSE
  }
  read_gen <- function() {
    path <- file.path(spool, "gen")
    if (!file.exists(path)) return(0)
    suppressWarnings(as.numeric(readLines(path, n = 1L, warn = FALSE)))
  }

  expect_true(wait_for(function() file.exists(file.path(spool, "ready"))))
  total <- 512 * 1024L
  expected <- as.raw((seq_len(total) - 1L) %% 256L)

  # SuperNode -> SuperLink: a sender pushes four times the retained-spool cap.
  sender_code <- paste(
    "import socket,sys",
    "port=int(sys.argv[1]); total=int(sys.argv[2])",
    "payload=(bytes(range(256))*((total+255)//256))[:total]",
    "s=socket.create_connection(('127.0.0.1',port))",
    "s.sendall(payload)",
    "s.close()",
    sep = "\n"
  )
  sender <- processx::process$new(
    python, c("-c", sender_code, as.character(port), as.character(total)),
    stdout = "|", stderr = "|", cleanup = TRUE, cleanup_tree = TRUE
  )
  withr::defer(if (sender$is_alive()) sender$kill())
  expect_true(wait_for(function() identical(read_gen(), 1)))
  received <- raw(0)
  up_off <- 0
  deadline <- Sys.time() + 15
  while (up_off < total && Sys.time() < deadline) {
    part <- dsFlower:::.with_tunnel_lock(spool, {
      value <- dsFlower:::.tunnel_read_at(
        spool, "up.bin", up_off, max_bytes = 32 * 1024
      )
      dsFlower:::.tunnel_publish_up_ack(spool, value$eof)
      value
    })
    if (length(part$data) > 0L) {
      received <- c(received, part$data)
      up_off <- part$eof
    } else {
      Sys.sleep(0.01)
    }
  }
  expect_identical(up_off, as.numeric(total))
  expect_identical(received, expected)
  sender$wait(timeout = 5000)
  expect_identical(sender$get_exit_status(), 0L)
  expect_true(wait_for(function() {
    state <- dsFlower:::.tunnel_spool_state(spool, "up.bin")
    state$base > 0 && state$bytes < 128 * 1024
  }))

  # SuperLink -> SuperNode: append with idempotent absolute offsets while a
  # receiver drains. A full retained spool may reject one attempt, but
  # compaction must make that same retry succeed without duplication.
  receiver_code <- paste(
    "import hashlib,socket,sys",
    "port=int(sys.argv[1]); total=int(sys.argv[2])",
    "s=socket.create_connection(('127.0.0.1',port))",
    "h=hashlib.sha256(); n=0",
    "while n < total:",
    " data=s.recv(min(65536,total-n))",
    " if not data: break",
    " h.update(data); n+=len(data)",
    "print(str(n)+' '+h.hexdigest(),flush=True)",
    "s.close()",
    sep = "\n"
  )
  receiver <- processx::process$new(
    python,
    c("-u", "-c", receiver_code, as.character(port), as.character(total)),
    stdout = "|", stderr = "|", cleanup = TRUE, cleanup_tree = TRUE
  )
  withr::defer(if (receiver$is_alive()) receiver$kill())
  expect_true(wait_for(function() identical(read_gen(), 2)))
  down_sent <- 0
  deadline <- Sys.time() + 15
  while (down_sent < total && Sys.time() < deadline) {
    last <- min(total, down_sent + 32 * 1024)
    chunk <- expected[(down_sent + 1):last]
    next_sent <- tryCatch(
      dsFlower:::.with_tunnel_lock(spool, {
        dsFlower:::.tunnel_append_at(
          spool, "down.bin", down_sent, chunk,
          max_bytes = 128 * 1024
        )
      }),
      error = function(e) {
        if (!grepl("spool limit", conditionMessage(e), fixed = TRUE)) stop(e)
        down_sent
      }
    )
    if (next_sent == down_sent) {
      Sys.sleep(0.01)
    } else {
      dsFlower:::.tunnel_publish_down_ack(spool, next_sent)
    }
    down_sent <- next_sent
  }
  expect_identical(down_sent, as.numeric(total))
  receiver$wait(timeout = 5000)
  expect_identical(receiver$get_exit_status(), 0L)
  output <- trimws(receiver$read_all_output())
  fields <- strsplit(output, " ", fixed = TRUE)[[1]]
  expect_equal(as.integer(fields[[1]]), total)
  expect_identical(
    fields[[2]], digest::digest(expected, algo = "sha256", serialize = FALSE)
  )
  expect_true(wait_for(function() {
    state <- dsFlower:::.tunnel_spool_state(spool, "down.bin")
    state$base > 0 && state$bytes < 128 * 1024
  }))

  # Hold the exact lock used by flowerTunnelExchangeDS while the SuperNode
  # redials. The forwarder must not reset or publish generation 3 early.
  lock <- filelock::lock(file.path(spool, "exchange.lock"), timeout = 1000)
  expect_false(is.null(lock))
  third <- socketConnection(
    "127.0.0.1", port, open = "r+b", blocking = TRUE, timeout = 2
  )
  withr::defer(tryCatch(close(third), error = function(e) NULL))
  Sys.sleep(0.2)
  expect_identical(read_gen(), 2)
  filelock::unlock(lock)

  expect_true(wait_for(function() identical(read_gen(), 3)))
  expect_identical(dsFlower:::.tunnel_spool_state(
    spool, "up.bin"
  )$bytes, 0)
  expect_identical(dsFlower:::.tunnel_spool_state(
    spool, "down.bin"
  )$bytes, 0)

  # A SuperNode may redial before its previous TCP socket has visibly closed.
  # The queued replacement must become a new generation without waiting for the
  # old half-open stream to time out.
  fourth <- socketConnection(
    "127.0.0.1", port, open = "r+b", blocking = TRUE, timeout = 2
  )
  withr::defer(tryCatch(close(fourth), error = function(e) NULL))
  expect_true(wait_for(function() identical(read_gen(), 4)))
  expect_identical(dsFlower:::.tunnel_spool_state(
    spool, "up.bin"
  )$bytes, 0)
  expect_identical(dsFlower:::.tunnel_spool_state(
    spool, "down.bin"
  )$bytes, 0)
  replacement_payload <- charToRaw("replacement-stream")
  writeBin(replacement_payload, fourth)
  flush(fourth)
  expect_true(wait_for(function() {
    dsFlower:::.tunnel_spool_state(spool, "up.bin")$bytes ==
      length(replacement_payload)
  }))
  expect_identical(
    dsFlower:::.tunnel_read_at(
      spool, "up.bin", 0, max_bytes = length(replacement_payload)
    )$data,
    replacement_payload
  )
})
