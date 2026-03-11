# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level handle storage
.dsflower_env <- new.env(parent = emptyenv())

# SuperNode singleton registry — keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

# Mutable package state (resolver reference)
.pkg_state <- new.env(parent = emptyenv())
.pkg_state$resolver <- NULL

#' Package attach hook
#'
#' Registers the Flower federation resource resolver and displays a startup
#' message with the package version.
#'
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  .pkg_state$resolver <- FlowerResourceResolver$new()
  resourcer::registerResourceResolver(.pkg_state$resolver)

  packageStartupMessage(
    "dsFlower v", utils::packageVersion("dsFlower"),
    " loaded. Flower federation resource resolver registered."
  )
}

#' Package detach hook
#'
#' Iterates the SuperNode registry, kills all SuperNodes, and
#' unregisters the resource resolver.
#'
#' @param lib Library path.
#' @return Invisible NULL; called for its side effect.
#' @keywords internal
.onDetach <- function(lib) {
  # Kill all registered SuperNodes
  for (addr in ls(.supernode_registry)) {
    tryCatch({
      entry <- get(addr, envir = .supernode_registry)
      if (!is.null(entry$process) && entry$process$is_alive()) {
        entry$process$signal(15L)
        entry$process$wait(timeout = 5000)
        if (entry$process$is_alive()) entry$process$kill()
      }
    }, error = function(e) NULL)
  }
  rm(list = ls(.supernode_registry), envir = .supernode_registry)

  # Unregister resolver
  if (!is.null(.pkg_state$resolver)) {
    tryCatch(
      resourcer::unregisterResourceResolver(.pkg_state$resolver),
      error = function(e) NULL
    )
    .pkg_state$resolver <- NULL
  }
}
