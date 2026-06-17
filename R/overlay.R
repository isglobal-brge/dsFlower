# Module: Overlay transport (Tailscale userspace)
# Brings the data node onto a Tailscale tailnet WITHOUT root or a TUN device,
# so a SuperNode can reach a SuperLink that lives on the researcher's machine
# across NAT. Uses the static tailscale binaries (auto-downloaded) + userspace
# networking + a SOCKS5 forwarder. The overlay carries Flower's own gRPC/TLS.

#' Directory for overlay binaries + state
#' @keywords internal
.overlay_dir <- function() {
  d <- file.path(tools::R_user_dir("dsFlower", "data"), "overlay")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

#' Resolve a python3 for the (stdlib-only) forwarder
#' @keywords internal
.overlay_python <- function() {
  py <- Sys.which("python3")
  if (!nzchar(py)) py <- Sys.which("python")
  if (!nzchar(py)) stop("python3 not found for the overlay forwarder.", call. = FALSE)
  py
}

#' Download the static tailscale binaries (tailscaled + tailscale) on demand
#' @keywords internal
.overlay_download_tailscale <- function() {
  d <- .overlay_dir()
  tsd <- file.path(d, "tailscaled"); ts <- file.path(d, "tailscale")
  if (file.exists(tsd) && file.exists(ts)) return(list(tailscaled = tsd, tailscale = ts))

  m <- tolower(Sys.info()[["machine"]])
  arch <- if (grepl("aarch64|arm64", m)) "arm64"
          else if (grepl("x86_64|amd64", m)) "amd64"
          else stop("Unsupported arch for tailscale static binary: ", m, call. = FALSE)

  ver <- tryCatch(
    jsonlite::fromJSON("https://pkgs.tailscale.com/stable/?mode=json")$Version,
    error = function(e) "1.98.4")
  url <- sprintf("https://pkgs.tailscale.com/stable/tailscale_%s_%s.tgz", ver, arch)
  tgz <- file.path(d, "ts.tgz")
  if (!identical(tryCatch(utils::download.file(url, tgz, mode = "wb", quiet = TRUE),
                          error = function(e) 1L), 0L)) {
    stop("Failed to download tailscale static binary from ", url, call. = FALSE)
  }
  utils::untar(tgz, exdir = d)
  exd <- file.path(d, sprintf("tailscale_%s_%s", ver, arch))
  file.copy(file.path(exd, "tailscaled"), tsd, overwrite = TRUE)
  file.copy(file.path(exd, "tailscale"), ts, overwrite = TRUE)
  Sys.chmod(tsd, "0755"); Sys.chmod(ts, "0755")
  unlink(tgz)
  list(tailscaled = tsd, tailscale = ts)
}

#' Ensure this node is joined to the tailnet (userspace, no root)
#' @keywords internal
.overlay_ensure_tailscale <- function(authkey, hostname = "dsflower-node") {
  bins <- .overlay_download_tailscale()
  d <- .overlay_dir()
  sock <- file.path(d, "tailscaled.sock")
  state <- file.path(d, "tailscaled.state")
  socks_port <- as.integer(.dsf_option("overlay_socks_port", 1055L))

  running <- !is.null(.dsflower_env$overlay_tsd) &&
    inherits(.dsflower_env$overlay_tsd, "process") &&
    tryCatch(.dsflower_env$overlay_tsd$is_alive(), error = function(e) FALSE)
  if (!running) {
    p <- processx::process$new(
      bins$tailscaled,
      c("--tun=userspace-networking",
        paste0("--socks5-server=127.0.0.1:", socks_port),
        paste0("--state=", state),
        paste0("--socket=", sock)),
      stdout = file.path(d, "tailscaled.log"), stderr = "2>&1",
      cleanup = FALSE, cleanup_tree = FALSE)
    .dsflower_env$overlay_tsd <- p
    Sys.sleep(2)
  }

  up <- processx::run(
    bins$tailscale,
    c(paste0("--socket=", sock), "up", "--reset",
      paste0("--authkey=", authkey),
      paste0("--hostname=", hostname)),
    error_on_status = FALSE, timeout = 90)
  ip <- tryCatch(trimws(processx::run(
    bins$tailscale, c(paste0("--socket=", sock), "ip", "-4"),
    error_on_status = FALSE)$stdout), error = function(e) "")

  .dsflower_env$overlay_socks_port <- socks_port
  list(joined = up$status == 0L, tailnet_ip = ip, socks_port = socks_port,
       error = if (up$status != 0L) substr(up$stderr, 1, 300) else NULL)
}

#' Start a SOCKS5 forwarder: loopback port -> tailnet SuperLink address
#' @keywords internal
.overlay_start_forward <- function(target_addr) {
  socks_port <- .dsflower_env$overlay_socks_port
  if (is.null(socks_port)) {
    stop("Overlay not initialised; call flowerOverlayUpDS first.", call. = FALSE)
  }
  fwd <- system.file("python", "overlay_forward.py", package = "dsFlower")
  local_port <- .random_available_port()
  p <- processx::process$new(
    .overlay_python(),
    c(fwd, "--listen", paste0("127.0.0.1:", local_port),
      "--socks", paste0("127.0.0.1:", socks_port),
      "--target", target_addr),
    stdout = file.path(.overlay_dir(), "forward.log"), stderr = "2>&1",
    cleanup = FALSE, cleanup_tree = FALSE)
  .dsflower_env$overlay_fwd <- p
  Sys.sleep(1)
  paste0("127.0.0.1:", local_port)
}

#' Bring this node onto the tailnet
#'
#' DataSHIELD ASSIGN method. Downloads the static tailscale binary, starts
#' tailscaled in userspace mode (no root/TUN) and joins the tailnet with the
#' provided auth key, so the SuperNode can reach the researcher's SuperLink.
#'
#' @param authkey Character; a Tailscale auth key (tskey-...).
#' @param hostname Character; tailnet hostname for this node.
#' @return Named list: joined, tailnet_ip, socks_port.
#' @export
flowerOverlayUpDS <- function(authkey, hostname = "dsflower-node") {
  authkey <- .ds_arg(authkey)
  if (is.list(authkey)) authkey <- authkey[[1]]
  hostname <- .ds_arg(hostname)
  if (is.list(hostname)) hostname <- hostname[[1]]
  .overlay_ensure_tailscale(authkey, hostname)
}

#' Tear down the overlay on this node
#'
#' DataSHIELD ASSIGN method. Stops the forwarder and tailscaled.
#' @return Invisible TRUE.
#' @export
flowerOverlayDownDS <- function() {
  for (key in c("overlay_fwd", "overlay_tsd")) {
    p <- .dsflower_env[[key]]
    if (!is.null(p) && inherits(p, "process")) {
      tryCatch({ p$signal(15L); p$wait(timeout = 3000); if (p$is_alive()) p$kill() },
               error = function(e) NULL)
    }
    .dsflower_env[[key]] <- NULL
  }
  .dsflower_env$overlay_socks_port <- NULL
  TRUE
}
