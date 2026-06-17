# Module: Tor transport (zero-account NAT traversal)
# Brings the node onto the Tor network so its SuperNode can reach a SuperLink
# published as a Tor hidden service by the researcher -- no account, no key, no
# public host. Reuses the same SOCKS5 forwarder + ensure integration as the
# Tailscale overlay (both set .dsflower_env$overlay_socks_port).

#' @keywords internal
.tor_dir <- function() {
  d <- file.path(tools::R_user_dir("dsFlower", "data"), "tor")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

#' Download + extract the Tor expert bundle (self-contained) on demand
#' @keywords internal
.tor_download <- function() {
  d <- .tor_dir()
  torbin <- file.path(d, "tor", "tor")
  if (file.exists(torbin)) return(list(tor = torbin, libdir = file.path(d, "tor")))

  # On the PATH already? (e.g. dev machine)
  pth <- Sys.which("tor")
  if (nzchar(pth)) return(list(tor = pth, libdir = NULL))

  m <- tolower(Sys.info()[["machine"]])
  arch <- if (grepl("aarch64|arm64", m)) "aarch64" else "x86_64"
  ver <- tryCatch(
    jsonlite::fromJSON("https://aus1.torproject.org/torbrowser/update_3/release/downloads.json")$version,
    error = function(e) "15.0.16")
  url <- sprintf("https://archive.torproject.org/tor-package-archive/torbrowser/%s/tor-expert-bundle-linux-%s-%s.tar.gz",
                 ver, arch, ver)
  tgz <- file.path(d, "teb.tar.gz")
  if (!identical(tryCatch(utils::download.file(url, tgz, mode = "wb", quiet = TRUE),
                          error = function(e) 1L), 0L)) {
    stop("Failed to download Tor expert bundle from ", url, call. = FALSE)
  }
  utils::untar(tgz, exdir = d)
  unlink(tgz)
  if (!file.exists(torbin)) stop("tor binary not found after extracting bundle.", call. = FALSE)
  Sys.chmod(torbin, "0755")
  list(tor = torbin, libdir = file.path(d, "tor"))
}

#' Ensure Tor is running with a SOCKS5 proxy on this node
#' @keywords internal
.tor_ensure <- function() {
  bin <- .tor_download()
  d <- .tor_dir()
  socks_port <- as.integer(.dsf_option("tor_socks_port", 9050L))

  running <- !is.null(.dsflower_env$tor_proc) &&
    inherits(.dsflower_env$tor_proc, "process") &&
    tryCatch(.dsflower_env$tor_proc$is_alive(), error = function(e) FALSE)
  if (!running) {
    datadir <- file.path(d, "data"); dir.create(datadir, showWarnings = FALSE)
    logf <- file.path(d, "tor.log")
    torrc <- file.path(d, "torrc")
    writeLines(c(
      paste0("SocksPort 127.0.0.1:", socks_port),
      paste0("DataDirectory ", datadir),
      # We don't need cover traffic on this client; shed padding to cut
      # setup overhead and latency.
      "ConnectionPadding 0",
      "ReducedConnectionPadding 1",
      paste0("Log notice file ", logf)
    ), torrc)
    env <- c("current")
    if (!is.null(bin$libdir)) env <- c(env, LD_LIBRARY_PATH = bin$libdir)
    p <- processx::process$new(bin$tor, c("-f", torrc),
      env = env, stdout = file.path(d, "tor.out"), stderr = "2>&1",
      cleanup = FALSE, cleanup_tree = FALSE)
    .dsflower_env$tor_proc <- p
    deadline <- Sys.time() + 120
    repeat {
      lg <- tryCatch(readLines(logf, warn = FALSE), error = function(e) character(0))
      if (any(grepl("Bootstrapped 100%", lg))) break
      if (Sys.time() > deadline || !p$is_alive()) break
      Sys.sleep(3)
    }
  }
  .dsflower_env$overlay_socks_port <- socks_port  # reuse ensure forwarder path
  lg <- tryCatch(readLines(file.path(d, "tor.log"), warn = FALSE), error = function(e) character(0))
  list(ready = any(grepl("Bootstrapped 100%", lg)), socks_port = socks_port)
}

#' Bring this node onto the Tor network (zero-account NAT traversal)
#'
#' DataSHIELD ASSIGN method. Downloads the Tor expert bundle, starts Tor with a
#' local SOCKS5 proxy, so the SuperNode can reach a SuperLink published as a Tor
#' hidden service by the researcher. No account, key or public host needed.
#'
#' @return Named list: ready, socks_port.
#' @export
flowerTorUpDS <- function() {
  .tor_ensure()
}

#' Tear down Tor on this node
#' @return Invisible TRUE.
#' @export
flowerTorDownDS <- function() {
  for (key in c("overlay_fwd", "tor_proc")) {
    p <- .dsflower_env[[key]]
    if (!is.null(p) && inherits(p, "process")) {
      tryCatch({ p$signal(15L); p$wait(timeout = 3000); if (p$is_alive()) p$kill() },
               error = function(e) NULL)
    }
    .dsflower_env[[key]] <- NULL
  }
  .dsflower_env$overlay_socks_port <- NULL
  # Wipe this session's Tor state (circuits, logs, torrc) so no trace of the run
  # is left. The cached tor binary is kept to avoid re-downloading next time.
  d <- .tor_dir()
  for (f in c("data", "torrc", "tor.log", "tor.out")) {
    tryCatch(unlink(file.path(d, f), recursive = TRUE, force = TRUE),
             error = function(e) NULL)
  }
  TRUE
}
