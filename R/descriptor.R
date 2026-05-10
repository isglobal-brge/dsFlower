# Module: Dataset Descriptor Layer
# Canonical dataset descriptors for Flower federation. All non-data.frame
# data paths (resources, imaging datasets) flow through this abstraction.

#' Create a FlowerDatasetDescriptor
#'
#' A lightweight S3 class that describes a dataset for Flower training
#' without materializing it in R. Descriptors carry metadata, asset
#' references, and staging hints that the prepare step uses to stage
#' data for the Python SuperNode.
#'
#' @param dataset_id Character; canonical dataset identifier
#'   (e.g. "radiology.chest_xray.v3").
#' @param source_kind Character; one of "in_memory_df", "staged_parquet",
#'   "image_bundle".
#' @param metadata Named list with file/format/column information for the
#'   samples metadata table.
#' @param assets Named list of asset descriptors (image roots, feature tables).
#' @param ... Additional fields stored in the descriptor.
#' @return An object of class \code{FlowerDatasetDescriptor}.
#' @export
flower_dataset_descriptor <- function(dataset_id, source_kind,
                                       metadata = NULL, assets = list(),
                                       ...) {
  stopifnot(
    is.character(dataset_id), length(dataset_id) == 1L, nzchar(dataset_id),
    is.character(source_kind), length(source_kind) == 1L,
    source_kind %in% c("in_memory_df", "staged_parquet", "image_bundle", "asset_ref")
  )

  desc <- list(
    dataset_id  = dataset_id,
    source_kind = source_kind,
    metadata    = metadata,
    assets      = assets
  )
  extra <- list(...)
  if (length(extra) > 0) {
    desc <- c(desc, extra)
  }

  structure(desc, class = "FlowerDatasetDescriptor")
}

#' Convert an object to a FlowerDatasetDescriptor
#'
#' Generic function. Methods exist for \code{data.frame} (backward
#' compatibility), \code{ResourceClient} (resourcer ecosystem), and
#' any class that provides its own method.
#'
#' @param x Object to convert.
#' @param ... Additional arguments passed to methods.
#' @return A \code{FlowerDatasetDescriptor}.
#' @export
as_flower_dataset <- function(x, ...) {
  UseMethod("as_flower_dataset")
}

#' @describeIn as_flower_dataset Wrap a data.frame as an in-memory descriptor.
#' @param dataset_id Character; optional dataset identifier (default: auto-generated).
#' @export
as_flower_dataset.data.frame <- function(x, dataset_id = NULL, ...) {
  if (is.null(dataset_id)) {
    dataset_id <- paste0("table_", digest::digest(x, algo = "xxhash32"))
  }
  flower_dataset_descriptor(
    dataset_id  = dataset_id,
    source_kind = "in_memory_df",
    metadata    = list(
      n_samples = nrow(x),
      columns   = names(x)
    ),
    table_data  = x
  )
}

#' @describeIn as_flower_dataset Convert a resourcer ResourceClient.
#'
#' Calls \code{resourcer::as.data.frame()} on the client to materialize
#' the resource, then wraps the result. If the client has class
#' \code{ImagingDatasetResourceClient} (from dsImaging), delegates to
#' its own method.
#'
#' @export
as_flower_dataset.ResourceClient <- function(x, ...) {
  # If dsImaging provides a specialized subclass, dispatch to it

  if (inherits(x, "ImagingDatasetResourceClient")) {
    return(as_flower_dataset.ImagingDatasetResourceClient(x, ...))
  }

  # Default: materialize to data.frame
  if (!requireNamespace("resourcer", quietly = TRUE)) {
    stop("Package 'resourcer' is required for ResourceClient support.",
         call. = FALSE)
  }

  df <- as.data.frame(x)
  as_flower_dataset.data.frame(df, ...)
}

#' @describeIn as_flower_dataset Convert an ImagingDatasetResourceClient.
#'
#' Extracts the imaging manifest and builds a descriptor with
#' \code{source_kind = "image_bundle"}.
#'
#' @export
as_flower_dataset.ImagingDatasetResourceClient <- function(x, ...) {
  # The client carries a parsed manifest from dsImaging
  manifest <- x$getManifest()
  if (is.null(manifest)) {
    stop("ImagingDatasetResourceClient has no manifest. ",
         "Was the resource resolved correctly?", call. = FALSE)
  }

  flower_dataset_descriptor(
    dataset_id  = manifest$dataset_id,
    source_kind = "image_bundle",
    metadata    = manifest$metadata,
    assets      = manifest$assets %||% list(),
    manifest    = manifest
  )
}

#' @describeIn as_flower_dataset Pass through an existing descriptor.
#' @export
as_flower_dataset.FlowerDatasetDescriptor <- function(x, ...) {
  x
}

#' @describeIn as_flower_dataset Error with helpful message for unsupported types.
#' @export
as_flower_dataset.default <- function(x, ...) {
  stop(
    "Cannot convert object of class '", paste(class(x), collapse = "/"),
    "' to a FlowerDatasetDescriptor. ",
    "Supported types: data.frame, ResourceClient, ",
    "ImagingDatasetResourceClient, FlowerDatasetDescriptor.",
    call. = FALSE
  )
}

#' Print method for FlowerDatasetDescriptor
#' @param x A FlowerDatasetDescriptor.
#' @param ... Ignored.
#' @export
print.FlowerDatasetDescriptor <- function(x, ...) {
  cat("FlowerDatasetDescriptor\n")
  cat("  dataset_id:  ", x$dataset_id, "\n")
  cat("  source_kind: ", x$source_kind, "\n")
  if (!is.null(x$metadata$n_samples)) {
    cat("  n_samples:   ", x$metadata$n_samples, "\n")
  }
  if (length(x$assets) > 0) {
    cat("  assets:      ", paste(names(x$assets), collapse = ", "), "\n")
  }
  invisible(x)
}
