# Module: DataSHIELD Expression Boundary

#' Require side-effect-free DataSHIELD argument expressions
#'
#' DSLite and some server evaluators validate only the outer allowlisted call.
#' Inspect the still-lazy arguments before any public method forces them so an
#' unregistered nested call cannot execute as a promise.
#' @keywords internal
.dsflower_require_literal_arguments <- function(call = sys.call(-1L)) {
  if (!is.call(call)) {
    stop("DataSHIELD method arguments could not be validated.",
         call. = FALSE)
  }
  arguments <- as.list(call)[-1L]
  safe <- vapply(arguments, function(expr) {
    is.symbol(expr) || is.atomic(expr) || is.null(expr)
  }, logical(1))
  if (any(!safe)) {
    stop("DataSHIELD method arguments must be literal values or assigned ",
         "server symbols.", call. = FALSE)
  }
  invisible(TRUE)
}
