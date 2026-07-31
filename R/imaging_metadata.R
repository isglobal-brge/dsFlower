# Module: Public Imaging Metadata
# Structural manifest discovery for dsFlowerClient. These methods deliberately
# avoid dsImaging's data-derived catalog and return no paths, URIs, counts, or
# sample values.

.emptyImageLabels <- function() {
  data.frame(
    name = character(0), type = character(0), columns = character(0),
    description = character(0), stringsAsFactors = FALSE
  )
}

.emptyImageAssets <- function() {
  data.frame(
    alias = character(0), kind = character(0), provider = character(0),
    stringsAsFactors = FALSE
  )
}

.emptyImageMasks <- function() {
  data.frame(
    alias = character(0), provider = character(0), status = character(0),
    stringsAsFactors = FALSE
  )
}

.publicManifestText <- function(value, default = NA_character_) {
  if (is.null(value) || is.list(value) || length(value) != 1L) return(default)
  value <- as.character(value)
  if (is.na(value) || !nzchar(value)) default else value
}

.publicImageDescriptor <- function(handle_symbol) {
  handle <- .getHandle(handle_symbol)
  descriptor <- handle$descriptor
  source_kind <- descriptor$source_kind %||% handle$source_kind
  if (!identical(handle$source, "descriptor") || is.null(descriptor) ||
      !source_kind %in% c("image_bundle", "imaging_resource")) {
    return(NULL)
  }
  descriptor
}

#' List public imaging label definitions
#'
#' Returns only the label schema declared in the node-owned imaging manifest.
#' Label values, distributions, counts, file locations, and catalog state are
#' never inspected or returned.
#'
#' @param handle_symbol Character; symbol of an initialized Flower handle.
#' @return A data.frame with public label names, types, columns, and descriptions.
#' @export
flowerImageLabelsDS <- function(handle_symbol) {
  descriptor <- .publicImageDescriptor(handle_symbol)
  if (is.null(descriptor)) return(.emptyImageLabels())

  labels <- descriptor$manifest$labels %||% list()
  if (!is.list(labels) || length(labels) == 0L) return(.emptyImageLabels())

  rows <- lapply(labels, function(label) {
    if (!is.list(label)) return(NULL)
    columns <- label$columns
    if (is.null(columns) || is.list(columns)) {
      columns <- character(0)
    } else {
      columns <- as.character(columns)
      columns <- columns[!is.na(columns) & nzchar(columns)]
    }
    data.frame(
      name = .publicManifestText(label$name),
      type = .publicManifestText(label$type),
      columns = paste(columns, collapse = ", "),
      description = .publicManifestText(label$description),
      stringsAsFactors = FALSE
    )
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0L) return(.emptyImageLabels())
  do.call(rbind, rows)
}

#' List public imaging asset definitions
#'
#' Returns only aliases, kinds, and providers declared in the node-owned
#' imaging manifest. In particular, storage locations and data-derived catalog
#' fields are excluded.
#'
#' @param handle_symbol Character; symbol of an initialized Flower handle.
#' @return A data.frame with public asset aliases, kinds, and providers.
#' @export
flowerImageAssetsDS <- function(handle_symbol) {
  descriptor <- .publicImageDescriptor(handle_symbol)
  if (is.null(descriptor)) return(.emptyImageAssets())

  assets <- descriptor$assets %||% descriptor$manifest$assets %||% list()
  if (!is.list(assets) || length(assets) == 0L) return(.emptyImageAssets())

  asset_names <- names(assets)
  if (is.null(asset_names)) asset_names <- rep("", length(assets))
  rows <- lapply(seq_along(assets), function(i) {
    asset <- assets[[i]]
    if (!is.list(asset)) return(NULL)
    alias <- .publicManifestText(
      asset_names[[i]], .publicManifestText(asset$alias, .publicManifestText(asset$name))
    )
    if (is.na(alias)) return(NULL)
    data.frame(
      alias = alias,
      kind = .publicManifestText(asset$kind, .publicManifestText(asset$type, "unknown")),
      provider = .publicManifestText(
        asset$provider,
        .publicManifestText(asset$processor, .publicManifestText(asset$segmenter))
      ),
      stringsAsFactors = FALSE
    )
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0L) return(.emptyImageAssets())
  do.call(rbind, rows)
}

#' List public imaging mask definitions
#'
#' Returns mask assets declared in the public structural manifest. It does not
#' query the data-derived dsImaging catalog, so completion state and sample
#' counts cannot cross the dsFlower trust boundary.
#'
#' @param handle_symbol Character; symbol of an initialized Flower handle.
#' @return A data.frame with public mask aliases, providers, and the constant
#'   status \code{"declared"}.
#' @export
flowerImageMasksDS <- function(handle_symbol) {
  assets <- flowerImageAssetsDS(handle_symbol)
  masks <- assets[assets$kind == "mask_root", c("alias", "provider"), drop = FALSE]
  if (nrow(masks) == 0L) return(.emptyImageMasks())
  masks$status <- rep("declared", nrow(masks))
  masks
}
