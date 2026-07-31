# Module: DSI tunnel (node side) -- carry Flower's SuperNode<->SuperLink bytes
# over the DataSHIELD channel instead of gRPC/Tor.
#
# A node-side forwarder (dsi_tunnel_forward.py) bridges the SuperNode's local TCP
# connection to a bounded, compactable byte spool; these DataSHIELD methods let the
# researcher's R relay drain/fill that spool, carrying the bytes to/from the
# SuperLink. The Flower SuperLink/SuperNode protocol is untouched; this relay
# does not add Secure Aggregation -- only the transport changes. External
# offsets are absolute while acknowledged prefixes are compacted atomically;
# flowerTunnelExchangeDS owns those offsets.

#' Validate the unguessable capability identifying one tunnel session
#' @keywords internal
.tunnel_conn_id <- function(conn_id) {
  if (!is.character(conn_id) || length(conn_id) != 1L || is.na(conn_id) ||
      !grepl("^dsf_[0-9a-f]{32}$", conn_id)) {
    stop("Invalid tunnel connection id.", call. = FALSE)
  }
  conn_id
}

#' Validate a TCP port used only by the node-local tunnel forwarder
#' @keywords internal
.tunnel_port <- function(port) {
  value <- suppressWarnings(as.numeric(port))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value != floor(value) || value < 1 || value > 65535) {
    stop("Invalid tunnel listen port.", call. = FALSE)
  }
  as.integer(value)
}

#' Return one bounded, server-owned tunnel resource limit
#' @keywords internal
.tunnel_limit <- function(name, default, minimum, maximum) {
  value <- suppressWarnings(as.numeric(.dsf_option(name, default)))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value != floor(value) || value < minimum || value > maximum) {
    stop("Invalid dsflower.", name, " option.", call. = FALSE)
  }
  value
}

#' Tunnel payload chunk size selected by the node administrator
#' @keywords internal
.tunnel_chunk_bytes <- function() {
  as.integer(.tunnel_limit(
    # DSI transports character arguments as R expressions. DSLite's real parser
    # fails near one million encoded characters; 512 KiB raw stays safely below
    # that boundary after URL-safe base64 while total streams remain unbounded.
    "tunnel_chunk_bytes", 512 * 1024, 16 * 1024, 512 * 1024
  ))
}

#' Maximum bytes retained in either tunnel spool file
#' @keywords internal
.tunnel_spool_max_bytes <- function(chunk_bytes = .tunnel_chunk_bytes()) {
  .tunnel_limit(
    "tunnel_spool_max_bytes", 1024^3,
    max(8 * as.numeric(chunk_bytes), 1024^2), 64 * 1024^3
  )
}

#' Maximum legacy encoded request accepted before JSON decoding
#' @keywords internal
.tunnel_request_max_bytes <- function(chunk_bytes = .tunnel_chunk_bytes()) {
  # The raw chunk is base64-encoded inside JSON and that JSON is base64-encoded
  # once more for the DataSHIELD expression transport.
  minimum_request <- 4 * ceiling(
    (4 * ceiling(as.numeric(chunk_bytes) / 3) + 1024) / 3
  ) + 4096
  .tunnel_limit(
    "tunnel_request_max_bytes", 64 * 1024^2,
    max(1024^2, minimum_request),
    256 * 1024^2
  )
}

#' Validate one absolute byte offset in a tunnel stream
#' @keywords internal
.tunnel_offset <- function(value) {
  value <- suppressWarnings(as.numeric(value))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value < 0 || value > 2^53 || value != floor(value)) {
    stop("Invalid tunnel byte offset.", call. = FALSE)
  }
  value
}

#' Validate a tunnel connection generation
#' @keywords internal
.tunnel_generation <- function(value) {
  value <- suppressWarnings(as.numeric(value))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value < 0 || value != floor(value)) {
    stop("Invalid tunnel generation.", call. = FALSE)
  }
  value
}

#' Read the generation published by the node-side forwarder
#' @keywords internal
.tunnel_current_generation <- function(spool) {
  path <- file.path(spool, "gen")
  if (!file.exists(path)) return(0)
  value <- readLines(path, n = 1L, warn = FALSE)
  if (length(value) == 0L) return(0)
  .tunnel_generation(value)
}

#' Bytes occupied by the absolute-base header in each compactable spool
#' @keywords internal
.tunnel_spool_header_bytes <- function() 8L

#' Replace one spool with an empty stream starting at an absolute offset
#' @keywords internal
.tunnel_reset_spool_file <- function(spool, binname, base = 0) {
  base <- .tunnel_offset(base)
  path <- file.path(spool, binname)
  con <- file(path, "wb")
  on.exit(close(con))
  writeBin(as.double(base), con, size = 8L, endian = "big")
  invisible(TRUE)
}

#' Read the absolute base and EOF of one compactable spool
#' @keywords internal
.tunnel_spool_state <- function(spool, binname, create = FALSE) {
  path <- file.path(spool, binname)
  if (!file.exists(path)) {
    if (!isTRUE(create)) return(list(base = 0, bytes = 0, eof = 0))
    .tunnel_reset_spool_file(spool, binname)
  }
  size <- file.size(path)
  header <- .tunnel_spool_header_bytes()
  if (is.na(size) || size < header) {
    if (isTRUE(create) && identical(as.numeric(size), 0)) {
      .tunnel_reset_spool_file(spool, binname)
      size <- header
    } else {
      stop("Invalid tunnel spool header.", call. = FALSE)
    }
  }
  con <- file(path, "rb")
  on.exit(close(con))
  base <- readBin(con, "double", n = 1L, size = 8L, endian = "big")
  base <- .tunnel_offset(base)
  bytes <- as.numeric(size) - header
  list(base = base, bytes = bytes, eof = base + bytes)
}

#' Publish one monotonic absolute tunnel acknowledgement
#' @keywords internal
.tunnel_publish_ack <- function(spool, name, offset) {
  offset <- .tunnel_offset(offset)
  if (!name %in% c("up.ack", "down.ack")) {
    stop("Invalid tunnel acknowledgment stream.", call. = FALSE)
  }
  path <- file.path(spool, name)
  if (file.exists(path)) {
    current <- tryCatch(
      .tunnel_offset(readLines(path, n = 1L, warn = FALSE)),
      error = function(e) NULL
    )
    if (!is.null(current) && current == offset) return(invisible(offset))
    if (!is.null(current) && current > offset) {
      stop("Tunnel acknowledgment cannot move backwards.", call. = FALSE)
    }
  }
  tmp <- paste0(path, ".", Sys.getpid(), ".tmp")
  writeLines(format(offset, scientific = FALSE, trim = TRUE), tmp)
  if (!file.rename(tmp, path)) {
    unlink(tmp)
    stop("Could not publish tunnel acknowledgement.", call. = FALSE)
  }
  invisible(offset)
}

#' Publish the relay's absolute node-to-SuperLink acknowledgement
#' @keywords internal
.tunnel_publish_up_ack <- function(spool, offset) {
  .tunnel_publish_ack(spool, "up.ack", offset)
}

#' Publish the relay's absolute SuperLink-to-node acknowledgement
#' @keywords internal
.tunnel_publish_down_ack <- function(spool, offset) {
  .tunnel_publish_ack(spool, "down.ack", offset)
}

#' Registry key for the forwarder owned by one tunnel session
#' @keywords internal
.tunnel_forwarder_key <- function(conn_id) {
  paste0("tunnel_fwd_", .tunnel_conn_id(conn_id))
}

#' @keywords internal
.tunnel_spool <- function(conn_id, create = TRUE) {
  cid <- .tunnel_conn_id(conn_id)
  d <- file.path(tempdir(), "dsflower_tunnel", cid)
  if (isTRUE(create)) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE, mode = "0700")
    Sys.chmod(d, mode = "0700")
  }
  d
}

#' Serialize aggregate exchanges belonging to one tunnel session
#' @keywords internal
.with_tunnel_lock <- function(spool, code) {
  lock <- tryCatch(
    filelock::lock(file.path(spool, "exchange.lock"), timeout = 5000),
    error = function(e) NULL
  )
  if (is.null(lock)) {
    stop("Tunnel session is busy.", call. = FALSE)
  }
  on.exit(filelock::unlock(lock), add = TRUE)
  force(code)
}

#' @keywords internal
.tunnel_enc <- function(raw) {
  if (length(raw) == 0) return("")
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(raw))
  b64 <- gsub("\\+", "-", b64); b64 <- gsub("/", "_", b64); b64 <- gsub("=+$", "", b64)
  paste0("B64:", b64)
}

#' @keywords internal
.tunnel_dec <- function(s, max_bytes = Inf) {
  if (!is.character(s) || length(s) != 1 || is.na(s) ||
      !nzchar(s) || !startsWith(s, "B64:")) {
    stop("Invalid tunnel payload.", call. = FALSE)
  }
  b64 <- substring(s, first = 5L, last = nchar(s, type = "chars"))
  max_encoded <- 4 * ceiling(as.numeric(max_bytes) / 3) + 4
  if (is.finite(max_bytes) && nchar(b64, type = "bytes") > max_encoded) {
    stop("Tunnel payload exceeds the configured chunk size.", call. = FALSE)
  }
  b64 <- gsub("-", "+", b64); b64 <- gsub("_", "/", b64)
  pad <- (4 - nchar(b64) %% 4) %% 4
  if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
  value <- tryCatch(jsonlite::base64_dec(b64), error = function(e) NULL)
  if (is.null(value) || length(value) > max_bytes) {
    stop("Invalid or oversized tunnel payload.", call. = FALSE)
  }
  value
}

#' @keywords internal
.tunnel_append <- function(spool, binname, raw) {
  .tunnel_spool_state(spool, binname, create = TRUE)
  con <- file(file.path(spool, binname), "ab"); on.exit(close(con))
  if (length(raw) > 0) writeBin(raw, con)
  invisible(TRUE)
}

#' Read a byte range [from, EOF) from a spool file (relay-owned offset).
#' @keywords internal
.tunnel_read_at <- function(spool, binname, from, max_bytes = Inf) {
  bin <- file.path(spool, binname)
  if (!file.exists(bin)) return(list(data = raw(0), eof = 0))
  state <- .tunnel_spool_state(spool, binname)
  from <- .tunnel_offset(from)
  if (from < state$base) {
    stop("Tunnel offset precedes the compacted spool base.", call. = FALSE)
  }
  if (state$eof <= from) return(list(data = raw(0), eof = state$eof))
  con <- file(bin, "rb"); on.exit(close(con))
  seek(con, .tunnel_spool_header_bytes() + from - state$base)
  to_read <- min(state$eof - from, as.numeric(max_bytes))
  data <- readBin(con, "raw", to_read)
  list(data = data, eof = from + length(data))
}

#' Idempotent append with fixed message geometry
#'
#' A new message must start exactly at EOF. A replay is accepted only when its
#' offset, length, and every retained byte match the one complete message already
#' stored. Returns the new absolute EOF.
#' @keywords internal
.tunnel_append_at <- function(spool, binname, at, raw, max_bytes = Inf) {
  state <- .tunnel_spool_state(spool, binname, create = TRUE)
  sz <- state$eof
  at <- .tunnel_offset(at)
  if (!is.raw(raw) || length(raw) < 1L) {
    stop("Invalid tunnel append payload.", call. = FALSE)
  }
  end <- at + length(raw)
  if (!is.finite(end) || end > 2^53) {
    stop("Invalid tunnel append geometry.", call. = FALSE)
  }
  if (at > sz) return(sz)                     # gap: refuse and report current EOF
  if (at == sz) {
    if (state$bytes + length(raw) > max_bytes) {
      stop("Tunnel spool limit exceeded.", call. = FALSE)
    }
    .tunnel_append(spool, binname, raw)
    return(.tunnel_spool_state(spool, binname)$eof)
  }
  if (at < state$base || end != sz) {
    stop("Conflicting tunnel replay geometry.", call. = FALSE)
  }
  existing <- .tunnel_read_at(
    spool, binname, at, max_bytes = length(raw))$data
  if (!identical(existing, raw)) {
    stop("Conflicting tunnel replay payload.", call. = FALSE)
  }
  if (state$bytes > max_bytes) {
    stop("Tunnel spool limit exceeded.", call. = FALSE)
  }
  sz
}

#' Idempotent bidirectional tunnel exchange in one fan-out call (AGGREGATE)
#'
#' The RELAY owns the byte offsets, so this method is loss-free and idempotent: a
#' retried call re-delivers / re-reads the same byte ranges without duplication
#' or loss. Current clients pass the scalar \code{pa}, \code{pd}, \code{pf}, and
#' \code{g} arguments directly, avoiding a second JSON/base64 layer. The legacy
#' \code{req} envelope (direct or keyed by node name) remains accepted. Returns this
#' node's list(ok = TRUE, node, sz = new down-stream absolute EOF,
#' ud = "B64:" SuperNode->SuperLink bytes from pf, ue = new up.bin EOF,
#' g = connection generation). Payloads are
#' bounded to the node-owned chunk size and exchanges are serialized per tunnel.
#' @param conn_id Character; tunnel connection id.
#' @param req Character; legacy \code{.ds_encode}'d request, or "".
#' @param pa Numeric; down append-offset believed by the relay.
#' @param pd Character; URL-safe base64 SuperLink-to-SuperNode bytes.
#' @param pf Numeric; up read-offset acknowledged by the relay.
#' @param g Numeric; expected forwarder connection generation.
#' @return list(ok, node, sz, ud, ue, g) for this node.
#' @keywords internal
#' @export
flowerTunnelExchangeDS <- function(conn_id, req = "", pa = NULL, pd = "",
                                   pf = 0, g = NULL) {
  cid <- .tunnel_conn_id(conn_id)
  p <- .dsflower_env[[.tunnel_forwarder_key(cid)]]
  if (!identical(.dsflower_env$tunnel_conn_id, cid) ||
      is.null(.active_tunnel_port()) ||
      is.null(p) || !inherits(p, "process") ||
      !isTRUE(tryCatch(p$is_alive(), error = function(e) FALSE))) {
    stop("Unknown or inactive tunnel session.", call. = FALSE)
  }
  spool <- .tunnel_spool(conn_id)
  chunk_bytes <- .tunnel_chunk_bytes()
  spool_max_bytes <- .tunnel_spool_max_bytes(chunk_bytes)
  if (!is.character(req) || length(req) != 1L || is.na(req)) {
    stop("Invalid tunnel request.", call. = FALSE)
  }
  if (nchar(req, type = "bytes") > .tunnel_request_max_bytes(chunk_bytes)) {
    stop("Tunnel request exceeds the configured size limit.", call. = FALSE)
  }
  .with_tunnel_lock(spool, {
    # Relay heartbeat: the forwarder self-terminates if this stops updating (the
    # researcher's relay died / lost connection), which lets its SuperNode notice
    # the SuperLink is gone and self-terminate too.
    cat(".", file = file.path(spool, "relay_hb"))
    nm <- .dsflower_env[[paste0("tunnel_name_", cid)]]
    r <- if (!is.null(pa) || !is.null(g)) {
      list(pa = pa, pd = pd, pf = pf, g = g)
    } else {
      NULL
    }
    if (is.null(r) && is.character(req) && length(req) == 1L &&
        !is.na(req) && nzchar(req)) {
      decoded <- tryCatch(.ds_arg(req), error = function(e) NULL)
      if (is.list(decoded)) {
        # Accept both former envelope shapes when they carry a generation fence.
        if (all(c("pa", "pd", "pf", "g") %in% names(decoded))) {
          r <- decoded
        } else if (!is.null(nm) && is.list(decoded[[nm]])) {
          r <- decoded[[nm]]
        }
      }
    }
    gen <- .tunnel_current_generation(spool)
    request_gen <- if (!is.null(r) && !is.null(r$g)) {
      tryCatch(.tunnel_generation(r$g), error = function(e) NULL)
    } else {
      NULL
    }
    if (is.null(request_gen) || request_gen != gen) {
      # A reconnect may have truncated both spools after the relay built this
      # request. Never apply stale bytes: only advertise the new generation so
      # the relay can reset its socket and offsets.
      list(ok = TRUE, node = nm, sz = 0, ud = "", ue = 0, g = gen)
    } else {
      pa <- .tunnel_offset(r$pa %||% NA)
      pd <- r$pd %||% ""
      pf <- .tunnel_offset(r$pf %||% 0)
      up_state <- .tunnel_spool_state(spool, "up.bin", create = TRUE)
      if (pf < up_state$base || pf > up_state$eof) {
        stop("Invalid tunnel read acknowledgment.", call. = FALSE)
      }
      .tunnel_publish_up_ack(spool, pf)
      down_state <- .tunnel_spool_state(spool, "down.bin", create = TRUE)
      if (pa < down_state$base || pa > down_state$eof) {
        stop("Invalid tunnel append acknowledgment.", call. = FALSE)
      }
      .tunnel_publish_down_ack(spool, pa)
      # down: idempotently append one bounded SuperLink->SuperNode chunk
      down_sz <- if (is.character(pd) && length(pd) == 1L &&
                     !is.na(pd) && nzchar(pd)) {
        payload <- .tunnel_dec(pd, chunk_bytes)
        current <- .tunnel_spool_state(spool, "down.bin", create = TRUE)
        if (pa == current$eof &&
            current$bytes + length(payload) > spool_max_bytes) {
          # Capacity is ordinary backpressure, not an invalid request. Keep the
          # exact chunk unacknowledged but still return upstream bytes so a
          # full-duplex peer cannot deadlock with both bounded spools full.
          current$eof
        } else {
          .tunnel_append_at(
            spool, "down.bin", pa, payload,
            max_bytes = spool_max_bytes
          )
        }
      } else {
        .tunnel_spool_state(spool, "down.bin", create = TRUE)$eof
      }
      # up: return at most one bounded SuperNode->SuperLink chunk
      up <- .tunnel_read_at(spool, "up.bin", pf, max_bytes = chunk_bytes)
      list(
        ok = TRUE, node = nm, sz = down_sz,
        ud = .tunnel_enc(up$data), ue = up$eof, g = gen
      )
    }
  })
}

#' @keywords internal
.tunnel_python <- function() {
  py <- Sys.which("python3")
  if (!nzchar(py)) py <- Sys.which("python")
  if (!nzchar(py)) stop("python3 not found on this node for the DSI tunnel.", call. = FALSE)
  py
}

#' Test whether the registered tunnel is an operator-created live endpoint
#'
#' A stored port alone is never authorization: the capability must still be
#' valid, its exact forwarder process alive, and that process must have published
#' the ready marker after binding its loopback listener.
#' @keywords internal
.active_tunnel_port <- function() {
  cid <- tryCatch(.tunnel_conn_id(.dsflower_env$tunnel_conn_id),
                  error = function(e) NULL)
  if (is.null(cid)) return(NULL)
  port <- tryCatch(.tunnel_port(.dsflower_env$tunnel_forwarder_port),
                   error = function(e) NULL)
  if (is.null(port)) return(NULL)
  p <- .dsflower_env[[.tunnel_forwarder_key(cid)]]
  alive <- !is.null(p) && inherits(p, "process") &&
    isTRUE(tryCatch(p$is_alive(), error = function(e) FALSE))
  ready <- file.exists(file.path(.tunnel_spool(cid, create = FALSE), "ready"))
  if (!alive || !ready) return(NULL)
  port
}

#' Kill and forget one tunnel forwarder and its private spool
#' @keywords internal
.cleanup_tunnel <- function(conn_id) {
  cid <- .tunnel_conn_id(conn_id)
  key <- .tunnel_forwarder_key(cid)
  p <- .dsflower_env[[key]]
  if (!is.null(p) && inherits(p, "process")) {
    tryCatch(p$kill(), error = function(e) NULL)
    tryCatch(p$wait(timeout = 1000), error = function(e) NULL)
  }
  .dsflower_env[[key]] <- NULL
  unlink(.tunnel_spool(cid, create = FALSE), recursive = TRUE)
  .dsflower_env[[paste0("tunnel_name_", cid)]] <- NULL
  if (identical(.dsflower_env$tunnel_conn_id, cid)) {
    .dsflower_env$tunnel_conn_id <- NULL
    .dsflower_env$tunnel_forwarder_port <- NULL
  }
  invisible(TRUE)
}

#' Start the node-side tunnel forwarder (DataSHIELD AGGREGATE)
#'
#' Spawns dsi_tunnel_forward.py listening on 127.0.0.1:listen_port; the Flower
#' SuperNode dials that local port and its bytes are bridged to the spool.
#' @param conn_id Character; tunnel connection id.
#' @param listen_port Integer; local port the SuperNode will dial.
#' @param node_name Character; this node's federation name.
#' @param protocol_abi Numeric; exact tunnel protocol ABI expected by the client.
#' @return list(ok, listen, chunk_bytes, protocol_abi). The ABI marker makes a
#'   mixed client/server deployment fail before exchanging stream bytes.
#' @keywords internal
#' @export
flowerTunnelUpDS <- function(conn_id, listen_port, node_name = "",
                             protocol_abi = NULL) {
  cid <- .tunnel_conn_id(conn_id)
  port <- .tunnel_port(listen_port)
  abi <- suppressWarnings(as.numeric(protocol_abi))
  if (length(abi) != 1L || is.na(abi) || !is.finite(abi) || abi != 3) {
    stop("Incompatible dsFlower tunnel protocol ABI; deploy matching server and client versions.",
         call. = FALSE)
  }
  chunk_bytes <- .tunnel_chunk_bytes()
  spool_max_bytes <- .tunnel_spool_max_bytes(chunk_bytes)
  .tunnel_request_max_bytes(chunk_bytes)
  if (!is.character(node_name) || length(node_name) != 1L ||
      is.na(node_name) || !nzchar(node_name)) {
    stop("Invalid tunnel node name.", call. = FALSE)
  }
  active_cid <- .dsflower_env$tunnel_conn_id
  if (!is.null(active_cid)) {
    valid_active_cid <- tryCatch(.tunnel_conn_id(active_cid),
                                 error = function(e) NULL)
    active_port <- if (!is.null(valid_active_cid)) {
      .active_tunnel_port()
    } else {
      NULL
    }
    if (!is.null(active_port) && identical(valid_active_cid, cid)) {
      active_name <- .dsflower_env[[paste0("tunnel_name_", cid)]]
      if (!identical(active_port, port) || !identical(active_name, node_name)) {
        stop("Conflicting tunnel startup replay.", call. = FALSE)
      }
      return(list(
        ok = TRUE,
        listen = paste0("127.0.0.1:", port),
        chunk_bytes = chunk_bytes,
        protocol_abi = 3L
      ))
    }
    if (!is.null(active_port)) {
      stop("A tunnel session is already active in this DataSHIELD session.",
           call. = FALSE)
    }
    if (!is.null(valid_active_cid)) {
      .cleanup_tunnel(valid_active_cid)
    } else {
      .dsflower_env$tunnel_conn_id <- NULL
      .dsflower_env$tunnel_forwarder_port <- NULL
    }
  }
  spool <- .tunnel_spool(cid)
  unlink(list.files(spool, full.names = TRUE))
  for (f in c("up.bin", "down.bin")) {
    .tunnel_reset_spool_file(spool, f)
    Sys.chmod(file.path(spool, f), mode = "0600")
  }
  writeLines("0", file.path(spool, "up.ack"))
  Sys.chmod(file.path(spool, "up.ack"), mode = "0600")
  writeLines("0", file.path(spool, "down.ack"))
  Sys.chmod(file.path(spool, "down.ack"), mode = "0600")
  cat(".", file = file.path(spool, "relay_hb"))   # seed the relay heartbeat
  started <- FALSE
  on.exit(if (!started) .cleanup_tunnel(cid), add = TRUE)
  # Record this node's federation name so flowerTunnelExchangeDS can pick its
  # slice out of the single fan-out down-payload.
  .dsflower_env[[paste0("tunnel_name_", cid)]] <- node_name
  fwd <- system.file("python", "dsi_tunnel_forward.py", package = "dsFlower")
  # One configurable knob (default 180s) drives both the forwarder's relay-loss
  # tolerance and the SuperNode's --max-wait-time; set per node via
  # options(dsflower.tunnel_loss_tolerance = <seconds>).
  ttl <- as.character(.tunnel_limit(
    "tunnel_loss_tolerance", 180, 5, 86400
  ))
  p <- processx::process$new(
    .tunnel_python(),
    c(fwd, "--listen", paste0("127.0.0.1:", port), "--spool", spool),
    env = c(
      "current",
      DSFLOWER_RELAY_TTL = ttl,
      DSFLOWER_TUNNEL_SPOOL_MAX_BYTES = as.character(spool_max_bytes)
    ),
    stdout = file.path(spool, "fwd.log"), stderr = "2>&1",
    cleanup = FALSE, cleanup_tree = FALSE)
  .dsflower_env[[.tunnel_forwarder_key(cid)]] <- p
  .dsflower_env$tunnel_conn_id <- cid
  ready <- FALSE
  for (i in 1:60) {
    alive <- isTRUE(tryCatch(p$is_alive(), error = function(e) FALSE))
    if (!alive) break
    if (file.exists(file.path(spool, "ready"))) {
      ready <- TRUE
      break
    }
    Sys.sleep(0.1)
  }
  ready <- ready && isTRUE(tryCatch(p$is_alive(), error = function(e) FALSE))
  if (!ready) {
    stop("Tunnel forwarder failed to start.", call. = FALSE)
  }
  # Publish the port only after bind/listen readiness and a final liveness check.
  .dsflower_env$tunnel_forwarder_port <- port
  started <- TRUE
  list(
    ok = TRUE,
    listen = paste0("127.0.0.1:", port),
    chunk_bytes = chunk_bytes,
    protocol_abi = 3L
  )
}

#' Stop this tunnel session's forwarder and clean its spool (AGGREGATE)
#' @keywords internal
#' @export
flowerTunnelDownDS <- function(conn_id) {
  cid <- .tunnel_conn_id(conn_id)
  .cleanup_tunnel(cid)
  TRUE
}
