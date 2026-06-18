# Module: DSI tunnel (node side) -- carry Flower's SuperNode<->SuperLink bytes
# over the DataSHIELD channel instead of gRPC/Tor.
#
# A node-side forwarder (dsi_tunnel_forward.py) bridges the SuperNode's local TCP
# connection to an append-only byte spool; these DataSHIELD methods let the
# researcher's R relay drain/fill that spool, carrying the bytes to/from the
# SuperLink. Flower (SuperLink, SuperNode, SecAgg+) is untouched -- only the
# transport changes. Spool files (append-only + reader offset, no locking):
#   up.bin   : SuperNode -> SuperLink  (forwarder appends; flowerTunnelPollDS reads)
#   down.bin : SuperLink -> SuperNode  (flowerTunnelPushDS appends; forwarder reads)

#' @keywords internal
.tunnel_spool <- function(conn_id) {
  cid <- gsub("[^A-Za-z0-9_]", "", as.character(conn_id))
  d <- file.path(tempdir(), "dsflower_tunnel", cid)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

#' @keywords internal
.tunnel_enc <- function(raw) {
  if (length(raw) == 0) return("")
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(raw))
  b64 <- gsub("\\+", "-", b64); b64 <- gsub("/", "_", b64); b64 <- gsub("=+$", "", b64)
  paste0("B64:", b64)
}

#' @keywords internal
.tunnel_dec <- function(s) {
  if (!is.character(s) || length(s) != 1 || !nzchar(s) || !startsWith(s, "B64:")) {
    return(raw(0))
  }
  b64 <- substring(s, 5)
  b64 <- gsub("-", "+", b64); b64 <- gsub("_", "/", b64)
  pad <- (4 - nchar(b64) %% 4) %% 4
  if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
  jsonlite::base64_dec(b64)
}

#' @keywords internal
.tunnel_drain <- function(spool, binname, offname) {
  bin <- file.path(spool, binname); offf <- file.path(spool, offname)
  if (!file.exists(bin)) return(raw(0))
  off <- if (file.exists(offf)) suppressWarnings(as.numeric(readLines(offf, n = 1, warn = FALSE))) else 0
  if (length(off) == 0 || is.na(off)) off <- 0
  sz <- file.size(bin)
  if (sz <= off) return(raw(0))
  con <- file(bin, "rb"); on.exit(close(con))
  seek(con, off)
  r <- readBin(con, "raw", sz - off)
  writeLines(as.character(sz), offf)
  r
}

#' @keywords internal
.tunnel_append <- function(spool, binname, raw) {
  con <- file(file.path(spool, binname), "ab"); on.exit(close(con))
  if (length(raw) > 0) writeBin(raw, con)
  invisible(TRUE)
}

#' Read a byte range [from, EOF) from a spool file (relay-owned offset).
#' @keywords internal
.tunnel_read_at <- function(spool, binname, from) {
  bin <- file.path(spool, binname)
  if (!file.exists(bin)) return(list(data = raw(0), eof = 0))
  sz <- file.size(bin)
  from <- suppressWarnings(as.numeric(from))
  if (length(from) == 0 || is.na(from) || from < 0) from <- 0
  if (sz <= from) return(list(data = raw(0), eof = sz))
  con <- file(bin, "rb"); on.exit(close(con))
  seek(con, from)
  list(data = readBin(con, "raw", sz - from), eof = sz)
}

#' Idempotent append: 'at' is the size the relay believes the file has; append
#' only the bytes of 'raw' not already present, so a retried push never
#' duplicates or loses bytes. Returns the new file size.
#' @keywords internal
.tunnel_append_at <- function(spool, binname, at, raw) {
  bin <- file.path(spool, binname)
  sz <- if (file.exists(bin)) file.size(bin) else 0
  at <- suppressWarnings(as.numeric(at))
  if (length(at) == 0 || is.na(at)) at <- sz
  already <- sz - at                          # bytes of 'raw' already appended
  if (already < 0) return(sz)                 # gap (at > sz): refuse, report size
  if (already >= length(raw)) return(sz)      # nothing new to append
  .tunnel_append(spool, binname, raw[(already + 1):length(raw)])
  if (file.exists(bin)) file.size(bin) else 0
}

#' Reset/initialise a tunnel spool (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel connection id.
#' @return TRUE.
#' @keywords internal
#' @export
flowerTunnelResetDS <- function(conn_id) {
  d <- .tunnel_spool(conn_id)
  unlink(list.files(d, full.names = TRUE))
  for (f in c("up.bin", "down.bin")) file.create(file.path(d, f))
  TRUE
}

#' Drain SuperNode->SuperLink bytes for the relay (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel connection id.
#' @return URL-safe base64 of the new up bytes, or "" if none.
#' @keywords internal
#' @export
flowerTunnelPollDS <- function(conn_id) {
  .tunnel_enc(.tunnel_drain(.tunnel_spool(conn_id), "up.bin", "up.read_offset"))
}

#' Deliver SuperLink->SuperNode bytes from the relay (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel connection id.
#' @param data_b64 Character; B64: url-safe payload to append to down.bin.
#' @return TRUE.
#' @keywords internal
#' @export
flowerTunnelPushDS <- function(conn_id, data_b64) {
  .tunnel_append(.tunnel_spool(conn_id), "down.bin", .tunnel_dec(data_b64))
  TRUE
}

#' Idempotent bidirectional tunnel exchange in one fan-out call (AGGREGATE)
#'
#' The RELAY owns the byte offsets, so this method is loss-free and idempotent: a
#' retried call re-delivers / re-reads the same byte ranges without duplication
#' or loss. \code{req} is a \code{.ds_encode}'d named list keyed by node name;
#' this node's entry is list(pa = down append-offset the relay believes,
#' pd = "B64:" SuperLink->SuperNode bytes, pf = up read-offset). Returns this
#' node's list(sz = new down.bin size, ud = "B64:" SuperNode->SuperLink bytes
#' from pf, ue = new up.bin EOF).
#' @param conn_id Character; tunnel connection id.
#' @param req Character; \code{.ds_encode}'d named request map, or "".
#' @return list(sz, ud, ue) for this node.
#' @keywords internal
#' @export
flowerTunnelExchangeDS <- function(conn_id, req = "") {
  spool <- .tunnel_spool(conn_id)
  nm <- .dsflower_env$tunnel_name
  pa <- NA; pd <- ""; pf <- 0
  if (is.character(req) && length(req) == 1L && nzchar(req)) {
    rm <- tryCatch(.ds_arg(req), error = function(e) NULL)
    if (is.list(rm) && !is.null(nm) && !is.null(rm[[nm]])) {
      r <- rm[[nm]]
      if (!is.null(r$pa)) pa <- r$pa
      if (!is.null(r$pd)) pd <- r$pd
      if (!is.null(r$pf)) pf <- r$pf
    }
  }
  # down: idempotently append the relay's SuperLink->SuperNode bytes
  down_sz <- if (is.character(pd) && nzchar(pd)) {
    .tunnel_append_at(spool, "down.bin", pa, .tunnel_dec(pd))
  } else {
    b <- file.path(spool, "down.bin"); if (file.exists(b)) file.size(b) else 0
  }
  # up: read SuperNode->SuperLink bytes from the relay-owned offset
  up <- .tunnel_read_at(spool, "up.bin", pf)
  list(sz = down_sz, ud = .tunnel_enc(up$data), ue = up$eof)
}

#' TEST: inject bytes as if the SuperNode sent them (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel connection id.
#' @param data_b64 Character; B64: url-safe payload to append to up.bin.
#' @return TRUE.
#' @keywords internal
#' @export
flowerTunnelInjectDS <- function(conn_id, data_b64) {
  .tunnel_append(.tunnel_spool(conn_id), "up.bin", .tunnel_dec(data_b64))
  TRUE
}

#' TEST: drain down bytes as the forwarder would (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel connection id.
#' @return URL-safe base64 of the new down bytes, or "" if none.
#' @keywords internal
#' @export
flowerTunnelDrainDS <- function(conn_id) {
  .tunnel_enc(.tunnel_drain(.tunnel_spool(conn_id), "down.bin", "down.read_offset"))
}

#' @keywords internal
.tunnel_python <- function() {
  py <- Sys.which("python3")
  if (!nzchar(py)) py <- Sys.which("python")
  if (!nzchar(py)) stop("python3 not found on this node for the DSI tunnel.", call. = FALSE)
  py
}

#' Start the node-side tunnel forwarder (DataSHIELD AGGREGATE)
#'
#' Spawns dsi_tunnel_forward.py listening on 127.0.0.1:listen_port; the Flower
#' SuperNode dials that local port and its bytes are bridged to the spool.
#' @param conn_id Character; tunnel connection id.
#' @param listen_port Integer; local port the SuperNode will dial.
#' @return list(ok, listen).
#' @keywords internal
#' @export
flowerTunnelUpDS <- function(conn_id, listen_port, node_name = "") {
  spool <- .tunnel_spool(conn_id)
  unlink(list.files(spool, full.names = TRUE))
  for (f in c("up.bin", "down.bin")) file.create(file.path(spool, f))
  # Record this node's federation name so flowerTunnelExchangeDS can pick its
  # slice out of the single fan-out down-payload.
  if (is.character(node_name) && nzchar(node_name)) .dsflower_env$tunnel_name <- node_name
  fwd <- system.file("python", "dsi_tunnel_forward.py", package = "dsFlower")
  p <- processx::process$new(
    .tunnel_python(),
    c(fwd, "--listen", paste0("127.0.0.1:", as.integer(listen_port)), "--spool", spool),
    stdout = file.path(spool, "fwd.log"), stderr = "2>&1",
    cleanup = FALSE, cleanup_tree = FALSE)
  .dsflower_env[[paste0("tunnel_fwd_", conn_id)]] <- p
  # Signal to flowerEnsureSuperNodeDS that the SuperNode must dial this loopback
  # forwarder (insecure) instead of a remote/Tor SuperLink address.
  .dsflower_env$tunnel_forwarder_port <- as.integer(listen_port)
  ready <- FALSE
  for (i in 1:60) { if (file.exists(file.path(spool, "ready"))) { ready <- TRUE; break }; Sys.sleep(0.1) }
  list(ok = ready, listen = paste0("127.0.0.1:", as.integer(listen_port)))
}

#' TEST: spawn a local client that round-trips through the forwarder (AGGREGATE)
#' @keywords internal
#' @export
flowerTunnelTestClientDS <- function(conn_id, listen_port, msg = "HELLO-TUNNEL", nrep = 1000L) {
  spool <- .tunnel_spool(conn_id)
  out <- file.path(spool, "testclient.bin"); unlink(out)
  cli <- system.file("python", "dsi_tunnel_testclient.py", package = "dsFlower")
  p <- processx::process$new(
    .tunnel_python(),
    c(cli, "--port", as.integer(listen_port), "--msg", msg, "--nrep", as.integer(nrep), "--out", out),
    stdout = file.path(spool, "tc.log"), stderr = "2>&1",
    cleanup = FALSE, cleanup_tree = FALSE)
  .dsflower_env[[paste0("tunnel_tc_", conn_id)]] <- p
  list(ok = TRUE, expected_bytes = nchar(msg) * as.integer(nrep))
}

#' TEST: read the round-tripped bytes the client received (AGGREGATE)
#' @keywords internal
#' @export
flowerTunnelTestResultDS <- function(conn_id) {
  out <- file.path(.tunnel_spool(conn_id), "testclient.bin")
  if (!file.exists(out)) return(list(done = FALSE, bytes = 0L, data = ""))
  raw <- readBin(out, "raw", file.size(out))
  list(done = TRUE, bytes = length(raw), data = .tunnel_enc(raw))
}

#' Stop the node-side tunnel forwarder/test processes and clean the spool (AGGREGATE)
#' @keywords internal
#' @export
flowerTunnelDownDS <- function(conn_id) {
  for (k in c("tunnel_fwd_", "tunnel_tc_", "tunnel_sn_")) {
    p <- .dsflower_env[[paste0(k, conn_id)]]
    if (!is.null(p) && inherits(p, "process")) {
      tryCatch({ p$kill() }, error = function(e) NULL)
    }
    .dsflower_env[[paste0(k, conn_id)]] <- NULL
  }
  unlink(.tunnel_spool(conn_id), recursive = TRUE)
  .dsflower_env$tunnel_forwarder_port <- NULL
  .dsflower_env$tunnel_name <- NULL
  TRUE
}

#' Start a Flower SuperNode pointed at the local tunnel forwarder (AGGREGATE)
#'
#' The SuperNode dials 127.0.0.1:forwarder_port (the forwarder), so its entire
#' Fleet-API conversation with the SuperLink is carried over DSI -- no Tor, no
#' public address. Flower + SecAgg+ run unchanged.
#' @keywords internal
#' @export
flowerTunnelSupernodeDS <- function(conn_id, forwarder_port, clientappio_port = 19000L) {
  spool <- .tunnel_spool(conn_id)
  cands <- c(Sys.glob("/srv/dsflower/venvs/*/bin/flower-supernode"),
             Sys.glob(file.path(tools::R_user_dir("dsFlower", "data"), "venvs", "*", "bin", "flower-supernode")),
             Sys.which("flower-supernode"))
  cands <- cands[nzchar(cands) & file.exists(cands)]
  if (length(cands) == 0) stop("flower-supernode binary not found on node.", call. = FALSE)
  sn <- cands[1]
  # The SuperNode spawns sibling binaries (flower-superexec); put the venv bin/
  # on PATH so they are found.
  child_env <- Sys.getenv()
  child_env[["PATH"]] <- paste0(dirname(sn), ":", child_env[["PATH"]])
  p <- processx::process$new(
    sn,
    c("--insecure",
      "--superlink", paste0("127.0.0.1:", as.integer(forwarder_port)),
      "--clientappio-api-address", paste0("127.0.0.1:", as.integer(clientappio_port))),
    env = child_env,
    stdout = file.path(spool, "sn.log"), stderr = "2>&1",
    cleanup = FALSE, cleanup_tree = FALSE)
  .dsflower_env[[paste0("tunnel_sn_", conn_id)]] <- p
  Sys.sleep(2.5)
  list(ok = p$is_alive(), supernode = sn)
}

#' Read a tunnel-related log tail (DataSHIELD AGGREGATE)
#' @param conn_id Character; tunnel id.
#' @param which One of "sn" (supernode), "fwd" (forwarder), "tc" (test client).
#' @return Character; last lines of the log.
#' @keywords internal
#' @export
flowerTunnelLogDS <- function(conn_id, which = "sn") {
  base <- if (identical(which, "fwd")) "fwd" else if (identical(which, "tc")) "tc" else "sn"
  f <- file.path(.tunnel_spool(conn_id), paste0(base, ".log"))
  if (!file.exists(f)) return("")
  paste(utils::tail(readLines(f, warn = FALSE), 40), collapse = "\n")
}
