# Module: Transport + Options
# JSON transport for Opal compatibility and option chain helpers.

#' Deserialize a possibly-JSON argument
#'
#' When complex R objects (lists, named vectors) are passed through
#' \code{datashield.assign.expr()} or \code{datashield.aggregate()}, Opal
#' serializes them via \code{deparse()}, which generates \code{structure()} or
#' \code{c()} calls not in the DataSHIELD method whitelist.
#'
#' The client wraps complex arguments in JSON, and the server calls this
#' helper to transparently deserialize them. DSLite passes native R objects
#' directly, so this function is a no-op for non-strings.
#'
#' @param x An argument that may be a JSON string or an already-parsed R object.
#' @return The deserialized R object.
#' @keywords internal
.ds_arg <- function(x) {
  if (is.character(x) && length(x) == 1) {
    if (startsWith(x, "B64:")) {
      # URL-safe base64 -> standard base64
      b64 <- substring(x, 5)
      b64 <- gsub("-", "+", b64)
      b64 <- gsub("_", "/", b64)
      # Restore padding
      pad <- (4 - nchar(b64) %% 4) %% 4
      if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
      json <- rawToChar(jsonlite::base64_dec(b64))
      return(jsonlite::fromJSON(json, simplifyVector = FALSE))
    }
    if (nchar(x) > 0 && substr(x, 1, 1) %in% c("{", "[")) {
      return(jsonlite::fromJSON(x, simplifyVector = FALSE))
    }
  }
  x
}

#' Read a dsFlower option with DataSHIELD double-fallback
#'
#' Option chain: \code{dsflower.\{name\}} -> \code{default.dsflower.\{name\}}
#' -> \code{default}.
#'
#' @param name Character; the option name (without the dsflower. prefix).
#' @param default The fallback value if no option is set.
#' @return The option value.
#' @keywords internal
.dsf_option <- function(name, default = NULL) {
  getOption(
    paste0("dsflower.", name),
    getOption(paste0("default.dsflower.", name), default)
  )
}

#' Whether a host is loopback / private (RFC1918) / link-local / non-routable
#'
#' Used to stop flowerCheckConnectivityDS from being used as an internal port
#' scanner. Non-literal hostnames are treated as private (deny by default) when
#' no coordinator is pinned, since they could resolve to internal addresses.
#'
#' @param host Character; a hostname or IP literal.
#' @return Logical.
#' @keywords internal
.is_private_or_local_host <- function(host) {
  h <- tolower(trimws(host %||% ""))
  if (h %in% c("", "localhost", "localhost.localdomain", "ip6-localhost")) return(TRUE)
  if (h %in% c("::1", "0:0:0:0:0:0:0:1")) return(TRUE)
  if (grepl("^fe80:", h)) return(TRUE)              # IPv6 link-local
  if (grepl("^f[cd][0-9a-f]{2}:", h)) return(TRUE)  # IPv6 unique-local fc00::/7
  if (grepl("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$", h)) {
    o <- suppressWarnings(as.integer(strsplit(h, ".", fixed = TRUE)[[1]]))
    if (any(is.na(o)) || any(o < 0L) || any(o > 255L)) return(TRUE)
    if (o[1] == 127L) return(TRUE)                                  # loopback
    if (o[1] == 10L) return(TRUE)                                   # 10/8
    if (o[1] == 172L && o[2] >= 16L && o[2] <= 31L) return(TRUE)    # 172.16/12
    if (o[1] == 192L && o[2] == 168L) return(TRUE)                  # 192.168/16
    if (o[1] == 169L && o[2] == 254L) return(TRUE)                  # link-local
    if (o[1] == 0L) return(TRUE)
    return(FALSE)
  }
  # Non-literal hostname, no coordinator pinned: deny by default.
  TRUE
}

#' Per-session rate limit for connectivity checks
#' @keywords internal
.connectivity_rate_ok <- function(max_per_min = 30L) {
  now <- Sys.time()
  hist <- .dsflower_env$.conn_check_times
  if (is.null(hist)) hist <- now[0]
  hist <- hist[difftime(now, hist, units = "secs") < 60]
  if (length(hist) >= max_per_min) {
    .dsflower_env$.conn_check_times <- hist
    return(FALSE)
  }
  .dsflower_env$.conn_check_times <- c(hist, now)
  TRUE
}
