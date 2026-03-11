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
