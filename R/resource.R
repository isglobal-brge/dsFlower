# Module: Resource Resolver
# Simplified ResourceR integration for Flower federation node connections.
#
# URL format: flower+federation:///data_path=/data/study.csv;data_format=csv;python_path=python3
# No template or features in URL — those are per-run concerns from the researcher.

#' Flower Federation Resource Client
#'
#' R6 class that wraps a DataSHIELD resource pointing to a Flower federation
#' node. Extracts configuration from the resource URL.
#'
#' @section URL Format:
#' \code{flower+federation:///data_path=/data/train.csv;data_format=csv;python_path=python3}
#'
#' @importFrom R6 R6Class
#' @keywords internal
FlowerResourceClient <- R6::R6Class(

  "FlowerResourceClient",
  inherit = resourcer::ResourceClient,

  private = list(
    .parsed = NULL,

    parse_url = function() {
      url <- self$getResource()$url
      # URL format: flower+federation:///key=value;key=value;...
      body <- sub("^flower\\+federation:///", "", url)
      pairs <- strsplit(body, ";", fixed = TRUE)[[1]]
      params <- list()
      for (pair in pairs) {
        eq_pos <- regexpr("=", pair, fixed = TRUE)
        if (eq_pos > 0) {
          key <- substr(pair, 1, eq_pos - 1)
          val <- substr(pair, eq_pos + 1, nchar(pair))
          params[[key]] <- val
        }
      }

      private$.parsed <- list(
        data_path   = params$data_path,
        data_format = params$data_format %||% "csv",
        python_path = params$python_path %||% "python3"
      )
    }
  ),

  public = list(
    #' @description Create a new Flower resource client
    #' @param resource A resourcer resource object
    #' @param ... Additional arguments passed to parent
    initialize = function(resource, ...) {
      super$initialize(resource)
      private$parse_url()
    },

    #' @description Get parsed URL parameters
    #' @return Named list with all parsed parameters
    getParsed = function() {
      private$.parsed
    },

    #' @description Get the training data path
    #' @return Character string or NULL
    getDataPath = function() {
      private$.parsed$data_path
    },

    #' @description Get the data format
    #' @return Character string (csv, parquet, feather)
    getDataFormat = function() {
      private$.parsed$data_format
    },

    #' @description Get the Python executable path
    #' @return Character string
    getPythonPath = function() {
      private$.parsed$python_path
    },

    #' @description Close the resource client (no-op for federation)
    close = function() {
      invisible(NULL)
    }
  )
)


#' Flower Federation Resource Resolver
#'
#' A \code{resourcer::ResourceResolver} subclass that creates Flower
#' federation clients from DataSHIELD resource descriptors.
#'
#' @importFrom R6 R6Class
#' @keywords internal
FlowerResourceResolver <- R6::R6Class(
  "FlowerResourceResolver",
  inherit = resourcer::ResourceResolver,

  public = list(
    #' @description Check if this resolver can handle the given resource
    #' @param resource A resourcer resource object
    #' @return Logical
    isFor = function(resource) {
      if (!super$isFor(resource)) return(FALSE)
      fmt <- resource$format
      if (is.null(fmt)) return(FALSE)
      tolower(fmt) %in% c("flower.federation.node", "flower-federation-node")
    },

    #' @description Create a new client for the given resource
    #' @param resource A resourcer resource object
    #' @return A FlowerResourceClient, or NULL
    newClient = function(resource) {
      tryCatch(
        FlowerResourceClient$new(resource),
        error = function(e) {
          warning("Failed to create Flower resource client: ", e$message)
          NULL
        }
      )
    }
  )
)
