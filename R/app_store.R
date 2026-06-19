# Module: App store (node side) -- receive + verify researcher-uploaded Flower
# app FABs (Tier-2) over the DataSHIELD channel. Mirrors the relay's idempotent
# offset-framing: the client pushes base64 chunks via datashield.aggregate (never
# assign.expr), the node appends them loss-free, then verifies sha256 before any
# unpack. A hash mismatch or oversize FAB is rejected and the spool destroyed, so
# unverified bytes never become runnable code.

#' @keywords internal
.app_spool_dir <- function(token) {
  tok <- gsub("[^A-Za-z0-9_]", "", as.character(token))
  if (!nzchar(tok)) stop("Invalid app token.", call. = FALSE)
  d <- file.path(tempdir(), "dsflower_appstore", tok)
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

#' @keywords internal
.app_b64_dec <- function(s) {
  if (!is.character(s) || length(s) != 1 || !nzchar(s) || !startsWith(s, "B64:")) {
    return(raw(0))
  }
  b64 <- substring(s, 5)
  b64 <- gsub("-", "+", b64); b64 <- gsub("_", "/", b64)
  pad <- (4 - nchar(b64) %% 4) %% 4
  if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
  jsonlite::base64_dec(b64)
}

#' Push a chunk of an uploaded FAB (DataSHIELD AGGREGATE)
#'
#' Idempotent at \code{offset}: the client passes the size it believes the node's
#' file has, and only the bytes of \code{chunk_b64} not already present are
#' appended, so a retried push never duplicates or loses bytes.
#' @param token Character; upload token (per-run).
#' @param chunk_b64 Character; \code{B64:} url-safe payload, or "".
#' @param offset Numeric; the size the client believes the node file has.
#' @return list(size = new file size), or list(size, error="gap") on a gap.
#' @keywords internal
#' @export
flowerAppPushDS <- function(token, chunk_b64 = "", offset = NULL) {
  token <- .ds_arg(token)
  spool <- .app_spool_dir(token)
  bin <- file.path(spool, "app.fab")
  sz <- if (file.exists(bin)) file.size(bin) else 0
  off <- suppressWarnings(as.numeric(offset))
  if (length(off) == 0 || is.na(off)) off <- sz
  # chunk_b64 is our own binary-safe "B64:" payload (raw FAB bytes); it must NOT
  # go through .ds_arg, which rawToChar()s decoded bytes and would corrupt binary.
  raw <- .app_b64_dec(chunk_b64)
  already <- sz - off                       # bytes of 'raw' already on disk
  if (already < 0) return(list(size = sz, error = "gap"))
  if (length(raw) > 0 && already < length(raw)) {
    con <- file(bin, "ab")
    writeBin(raw[(already + 1):length(raw)], con)
    close(con)   # close before reading size, so the flushed bytes are counted
  }
  list(size = if (file.exists(bin)) file.size(bin) else 0)
}

#' Verify + unpack an uploaded FAB (DataSHIELD AGGREGATE)
#'
#' Enforces the size cap (\code{dsflower.max_fab_bytes}) and sha256 integrity
#' before unpacking. On any failure the spool is destroyed so no unverified code
#' can run. Returns the validated hash and unpack path; the run executes only the
#' app registered by this hash.
#' @param token Character; upload token.
#' @param expected_sha256 Character; sha256 the client computed over the FAB.
#' @return list(ok, sha256, size, path).
#' @keywords internal
#' @export
flowerAppInstallDS <- function(token, expected_sha256) {
  token <- .ds_arg(token)
  spool <- .app_spool_dir(token)
  bin <- file.path(spool, "app.fab")
  if (!file.exists(bin)) {
    stop("No uploaded app for this token; push the FAB first.", call. = FALSE)
  }
  cap <- suppressWarnings(as.numeric(
    .dsf_option("max_fab_bytes", 50 * 1024 * 1024)))
  if (!is.na(cap) && file.size(bin) > cap) {
    unlink(spool, recursive = TRUE)
    stop("Uploaded app exceeds dsflower.max_fab_bytes (", cap, " bytes).",
         call. = FALSE)
  }
  actual <- digest::digest(file = bin, algo = "sha256")
  expected <- as.character(.ds_arg(expected_sha256))
  if (!identical(actual, expected)) {
    unlink(spool, recursive = TRUE)
    stop("Uploaded app failed integrity check (sha256 mismatch); rejected.",
         call. = FALSE)
  }
  apps_dir <- file.path(spool, "unpacked")
  if (dir.exists(apps_dir)) unlink(apps_dir, recursive = TRUE)
  dir.create(apps_dir, showWarnings = FALSE)
  unpacked <- tryCatch(utils::unzip(bin, exdir = apps_dir),
                       error = function(e) character(0))
  if (length(unpacked) == 0) {
    unlink(spool, recursive = TRUE)
    stop("Uploaded app is not a valid FAB archive; rejected.", call. = FALSE)
  }
  list(ok = TRUE, sha256 = actual, size = file.size(bin), path = apps_dir)
}

#' Remove an uploaded app's spool (DataSHIELD AGGREGATE)
#' @param token Character; upload token.
#' @return TRUE.
#' @keywords internal
#' @export
flowerAppDeleteDS <- function(token) {
  unlink(.app_spool_dir(.ds_arg(token)), recursive = TRUE)
  TRUE
}
