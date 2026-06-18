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
