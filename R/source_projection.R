# Node-private selected source projection. This is never an analyst artifact.
# Typed JSONL preserves invalid/missing source values before safe totalization.
.sourceProjectionCell <- function(value) {
  if (is.factor(value)) value <- as.character(value)
  if (length(value) != 1L || is.list(value)) {
    stop("Selected source values must be scalar atomic cells.", call. = FALSE)
  }
  if (is.numeric(value)) {
    if (is.nan(value)) return(list(type = "nan"))
    if (is.na(value)) return(list(type = "missing"))
    if (is.infinite(value)) {
      return(list(type = if (value > 0) "posinf" else "neginf"))
    }
    return(list(type = "number", value = sprintf("%.17g", as.numeric(value))))
  }
  if (is.na(value)) return(list(type = "missing"))
  if (is.logical(value)) return(list(type = "bool", value = isTRUE(value)))
  list(type = "utf8", value = enc2utf8(as.character(value)))
}

.stageSourceProjection <- function(source, effective, manifest, staging_dir) {
  if (!is.data.frame(source)) source <- as.data.frame(source)
  if (!is.data.frame(effective) || nrow(source) != nrow(effective)) {
    stop("Private source projection no longer matches staged rows.", call. = FALSE)
  }
  columns <- if (identical(manifest$data_type, "image")) {
    if (identical(manifest[["loss-name"]], "segmentation_bce_dice")) {
      manifest$mask_empty_col %||% character()
    } else manifest$target_column
  } else c(manifest$feature_columns, manifest$target_column)
  columns <- as.character(unlist(columns, use.names = FALSE))
  if (anyNA(columns) || any(!columns %in% names(source))) {
    stop("Private source projection columns are unavailable.", call. = FALSE)
  }
  patient <- manifest$patient_column %||% NULL
  patient_ids <- if (is.null(patient)) NULL else {
    if (!patient %in% names(effective)) {
      stop("Private source projection patient roster is unavailable.", call. = FALSE)
    }
    .canonicalPatientIdText(effective[[patient]])
  }
  filename <- "source-projection.jsonl"
  path <- file.path(staging_dir, filename)
  connection <- file(path, open = "wb")
  on.exit(close(connection), add = TRUE)
  Sys.chmod(path, "0600")
  header <- list(schema = "dsflower-source-projection-v1",
                 columns = unname(as.list(columns)), patient_column = patient)
  writeLines(jsonlite::toJSON(header, auto_unbox = TRUE, null = "null"), connection)
  for (index in seq_len(nrow(source))) {
    record <- list(values = unname(lapply(columns, function(column) {
      .sourceProjectionCell(source[[column]][index])
    })), patient_id = if (is.null(patient_ids)) NULL else patient_ids[[index]])
    writeLines(jsonlite::toJSON(record, auto_unbox = TRUE, null = "null"), connection)
  }
  close(connection)
  on.exit(NULL, add = FALSE)
  effective_file <- manifest$data_file %||% manifest$samples_file
  manifest$source_projection_file <- filename
  manifest$source_projection_schema <- "dsflower-source-projection-v1"
  manifest$source_projection_sha256 <- digest::digest(file = path, algo = "sha256")
  manifest$source_effective_sha256 <- digest::digest(
    file = file.path(staging_dir, effective_file), algo = "sha256")
  manifest
}
