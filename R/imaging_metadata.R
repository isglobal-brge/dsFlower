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

.publicManifestIdentifier <- function(value, default = NA_character_) {
  if (is.null(value) || is.list(value) || length(value) != 1L) return(default)
  value <- enc2utf8(as.character(value))
  valid <- !is.na(value) && nzchar(value) &&
    nchar(value, type = "bytes") <= 128L &&
    grepl("^[A-Za-z0-9][A-Za-z0-9._:-]*$", value) &&
    !grepl("..", value, fixed = TRUE) &&
    !grepl("^[A-Za-z][A-Za-z0-9+.-]*:", value)
  if (isTRUE(valid)) unname(value) else default
}

.publicImageDescriptor <- function(handle_symbol) {
  handle <- .getHandle(handle_symbol)
  descriptor <- handle$descriptor
  source_kind <- descriptor$source_kind %||% handle$source_kind
  if (!identical(handle$source, "descriptor") || is.null(descriptor) ||
      !source_kind %in% c(
        "image_bundle", "imaging_resource", "imaging_feature_view")) {
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
  .dsflower_require_literal_arguments()
  descriptor <- .publicImageDescriptor(handle_symbol)
  if (is.null(descriptor)) return(.emptyImageLabels())
  metadata <- descriptor$manifest$metadata %||% list()
  label_column <- .publicManifestIdentifier(metadata$label_col)
  if (is.na(label_column)) return(.emptyImageLabels())
  data.frame(
    name = label_column,
    type = "declared_label",
    columns = label_column,
    description = NA_character_,
    stringsAsFactors = FALSE)
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
  .dsflower_require_literal_arguments()
  descriptor <- .publicImageDescriptor(handle_symbol)
  if (is.null(descriptor)) return(.emptyImageAssets())

  assets <- descriptor$assets %||% descriptor$manifest$assets %||% list()
  if (!is.list(assets) || length(assets) == 0L) return(.emptyImageAssets())

  asset_names <- names(assets)
  if (is.null(asset_names)) asset_names <- rep("", length(assets))
  rows <- lapply(seq_along(assets), function(i) {
    asset <- assets[[i]]
    if (!is.list(asset)) return(NULL)
    alias <- .publicManifestIdentifier(
      asset_names[[i]], .publicManifestIdentifier(
        asset$alias, .publicManifestIdentifier(asset$name))
    )
    if (is.na(alias)) return(NULL)
    data.frame(
      alias = alias,
      kind = .publicManifestIdentifier(
        asset$kind, .publicManifestIdentifier(asset$type, "unknown")),
      provider = .publicManifestIdentifier(
        asset$provider,
        .publicManifestIdentifier(
          asset$processor,
          .publicManifestIdentifier(asset$segmenter, "unknown"))
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
  .dsflower_require_literal_arguments()
  assets <- flowerImageAssetsDS(handle_symbol)
  masks <- assets[assets$kind == "mask_root", c("alias", "provider"), drop = FALSE]
  if (nrow(masks) == 0L) return(.emptyImageMasks())
  masks$status <- rep("declared", nrow(masks))
  masks
}
