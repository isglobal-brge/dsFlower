# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level handle storage
.dsflower_env <- new.env(parent = emptyenv())

# SuperNode singleton registry -- keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

#' Package attach hook
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage(
    "dsFlower v", utils::packageVersion("dsFlower"), " loaded."
  )
}

#' Package detach hook
#'
#' Kills all registered SuperNodes.
#'
#' @param lib Library path.
#' @return Invisible NULL; called for its side effect.
#' @keywords internal
.onDetach <- function(lib) {
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
}
