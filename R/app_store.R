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

#' Hash a Python package dir, byte-identically to sitecustomize.py _hash_package
#' (and .compute_harness_hash): radix-sorted relpaths, relpath+"\n"+content+"\x00",
#' excluding compiled artifacts. This is the hash the integrity hook pins against.
#' @keywords internal
.hash_pkg_dir <- function(pkg_dir) {
  rel_files <- list.files(pkg_dir, recursive = TRUE, full.names = FALSE,
                          all.files = TRUE, no.. = TRUE)
  rel_files <- rel_files[!grepl("(^|/)__pycache__(/|$)", rel_files)]
  rel_files <- rel_files[!grepl("\\.(pyc|pyo)$", rel_files)]
  rel_files <- sort(rel_files, method = "radix")
  blob <- raw(0)
  for (rel in rel_files) {
    full <- file.path(pkg_dir, rel)
    content <- readBin(full, "raw", file.info(full)$size)
    blob <- c(blob, charToRaw(rel), charToRaw("\n"), content, as.raw(0x00))
  }
  digest::digest(blob, algo = "sha256", serialize = FALSE)
}

#' Node-computed hashes of each top-level Python package in an unpacked app.
#' These are the trust anchors the integrity hook pins (the node computes them
#' itself, so a client cannot forge what is allowed to run).
#' @keywords internal
.compute_pkg_hashes <- function(apps_dir) {
  subdirs <- list.dirs(apps_dir, recursive = FALSE, full.names = TRUE)
  is_pkg <- vapply(subdirs, function(d)
    length(list.files(d, pattern = "\\.py$")) > 0, logical(1))
  pkgs <- subdirs[is_pkg]
  hashes <- list()
  for (p in pkgs) hashes[[basename(p)]] <- .hash_pkg_dir(p)
  hashes
}

#' Resolve a python (with stdlib ast) for the exfiltration scan.
#' @keywords internal
.scan_python <- function() {
  cands <- c(Sys.which("python3"), Sys.which("python"),
             Sys.glob("/srv/dsflower/venvs/*/bin/python"),
             Sys.glob(file.path(tools::R_user_dir("dsFlower", "data"),
                                "venvs", "*", "bin", "python")))
  cands <- cands[nzchar(cands) & file.exists(cands)]
  if (length(cands)) cands[1] else ""
}

#' Run the Tier-2 exfiltration scan on an unpacked app (fail-closed).
#' @keywords internal
.run_exfil_scan <- function(app_dir) {
  scanner <- system.file("python", "exfil_scan.py", package = "dsFlower")
  if (!nzchar(scanner) || !file.exists(scanner)) {
    return(list(ok = FALSE, first = "scanner not installed"))
  }
  py <- .scan_python()
  if (!nzchar(py)) return(list(ok = FALSE, first = "no python for scan"))
  res <- tryCatch(
    processx::run(py, c(scanner, app_dir), error_on_status = FALSE),
    error = function(e) NULL)
  if (is.null(res)) return(list(ok = FALSE, first = "scan failed to run"))
  out <- tryCatch(jsonlite::fromJSON(res$stdout, simplifyVector = FALSE),
                  error = function(e) NULL)
  if (is.null(out) || is.null(out$ok)) {
    return(list(ok = FALSE, first = "scan output unparseable"))
  }
  first <- ""
  if (length(out$violations)) {
    v <- out$violations[[1]]
    first <- paste0(v$rule, ":", v$detail, " @ ", v$file, ":", v$line)
  }
  list(ok = isTRUE(out$ok), violations = out$violations, first = first)
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

  # Tier-2 exfiltration scan gates install (fail-closed): an app that imports
  # network/process-escape modules or uses dynamic code is rejected before any
  # data is touched. Defence-in-depth ahead of the sandbox + egress gate.
  scan <- .run_exfil_scan(apps_dir)
  if (!isTRUE(scan$ok)) {
    unlink(spool, recursive = TRUE)
    stop("Uploaded app failed the Tier-2 safety scan (", scan$first,
         "); rejected.", call. = FALSE)
  }
  # Node-computed per-package hashes -> the integrity hook's trust anchors for a
  # Tier-2 run (so the client cannot dictate what is allowed to run).
  pkg_hashes <- .compute_pkg_hashes(apps_dir)
  list(ok = TRUE, sha256 = actual, size = file.size(bin), path = apps_dir,
       packages = pkg_hashes)
}

#' Hash a node-resident trusted app package (e.g. the dsflower_tier2 runner).
#' @keywords internal
.compute_app_pkg_hash <- function(pkg_name) {
  pkg_dir <- system.file("flower_app", pkg_name, package = "dsFlower")
  if (!nzchar(pkg_dir) || !dir.exists(pkg_dir)) return("")
  .hash_pkg_dir(pkg_dir)
}

#' Pin a Tier-2 run: trusted runner + verified uploaded app (DataSHIELD AGGREGATE)
#'
#' Writes \code{pinned_packages.json} into the run's staging dir so the integrity
#' hook (multi-package, default-deny) allows exactly the node-resident
#' \code{dsflower_tier2} runner AND the uploaded user app — both pinned to
#' \emph{node-computed} hashes, so a client cannot dictate what runs. Records the
#' user app's import directory for the SuperNode PYTHONPATH. The uploaded app must
#' already be pushed + installed (sha256-verified + exfiltration-scanned).
#' @param handle_symbol Character; the prepared run handle.
#' @param app_token Character; the uploaded app's token.
#' @return list(ok, pinned, user_path).
#' @keywords internal
#' @export
flowerTier2PinDS <- function(handle_symbol, app_token) {
  handle <- .getHandle(handle_symbol)
  if (is.null(handle$staging_dir) || !dir.exists(handle$staging_dir)) {
    stop("Run is not prepared (no staging dir); call flowerPrepareRunDS first.",
         call. = FALSE)
  }
  apps_dir <- file.path(.app_spool_dir(.ds_arg(app_token)), "unpacked")
  if (!dir.exists(apps_dir)) {
    stop("No installed app for that token; push + install the app first.",
         call. = FALSE)
  }
  user_hashes <- .compute_pkg_hashes(apps_dir)
  if (length(user_hashes) == 0) {
    stop("Uploaded app contains no importable Python package to pin.",
         call. = FALSE)
  }
  runner_hash <- .compute_app_pkg_hash("dsflower_runner")
  if (!nzchar(runner_hash)) {
    stop("The trusted runner (dsflower_runner) is not installed on this node.",
         call. = FALSE)
  }
  pinned <- c(list(dsflower_runner = runner_hash), user_hashes)
  jsonlite::write_json(pinned,
                       file.path(handle$staging_dir, "pinned_packages.json"),
                       auto_unbox = TRUE)
  writeLines(apps_dir, file.path(handle$staging_dir, "tier2_pythonpath.txt"))
  list(ok = TRUE, pinned = names(pinned), user_path = apps_dir)
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
