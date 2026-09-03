# Module: bounded binary epidemiologic association
#
# This file owns only the public protocol and total, row-preserving staging
# needed by the dedicated association runner.  It deliberately has no privacy
# parameters, historical state, model catalogue, or fallback execution path.

.ASSOCIATION_CONTRACT <- "dsflower-binary-association-3x3/v1"
.ASSOCIATION_RESULT_CONTRACT <- "dsflower-binary-association-result/v1"
.ASSOCIATION_MECHANISM <- "binary-association-joint-gaussian/v1"
.ASSOCIATION_EXECUTION_PROFILE <- "dsflower-binary-association-execution/v1"
.ASSOCIATION_MAX_LEVEL_BYTES <- 4096L
.ASSOCIATION_MAX_MANIFEST_BYTES <- 65536L
.ASSOCIATION_MAX_NODES <- 65536L
.ASSOCIATION_MAX_PARQUET_ROWS <- 5000000L
.ASSOCIATION_MAX_PARQUET_MATERIALIZED_BYTES <- 512 * 1024^2
.ASSOCIATION_PARQUET_PROJECTION_CONTRACT <-
  "dsflower-association-parquet-projection/v1"

.association_scalar_text <- function(value, name) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop(name, " must be one non-missing character value.", call. = FALSE)
  }
  value <- tryCatch(
    iconv(value, from = "", to = "UTF-8", sub = NA_character_),
    error = function(e) NA_character_)
  if (is.na(value) || !nzchar(value) ||
      nchar(value, type = "bytes") > .ASSOCIATION_MAX_LEVEL_BYTES) {
    stop(name, " must be non-empty valid UTF-8 of at most 4096 bytes.",
         call. = FALSE)
  }
  enc2utf8(value)
}

.association_column <- function(value, name) {
  .association_scalar_text(value, name)
}

.association_level_kind <- function(value) {
  if (is.character(value)) return("string")
  if (is.logical(value)) return("boolean")
  if ((is.integer(value) || is.double(value)) && !is.object(value)) {
    return("number")
  }
  "unsupported"
}

.association_level_spec <- function(value, name) {
  if (is.factor(value)) value <- as.character(value)

  claimed <- NULL
  if (is.list(value) && !is.null(names(value))) {
    if (!identical(sort(names(value), method = "radix"),
                   c("type", "values"))) {
      stop(name, " must be a two-level vector or typed level object.",
           call. = FALSE)
    }
    claimed <- value[["type"]]
    if (!is.character(claimed) || length(claimed) != 1L ||
        is.na(claimed) || !claimed %in% c("string", "boolean", "number")) {
      stop(name, " has an invalid public level type.", call. = FALSE)
    }
    raw_values <- value[["values"]]
    if (!is.list(raw_values)) raw_values <- as.list(raw_values)
    if (length(raw_values) != 2L ||
        any(vapply(raw_values, length, integer(1)) != 1L)) {
      stop(name, " must contain exactly two public levels.", call. = FALSE)
    }
    kinds <- vapply(raw_values, .association_level_kind, character(1))
    if (any(kinds != claimed)) {
      stop(name, " values disagree with their public level type.",
           call. = FALSE)
    }
    value <- unlist(raw_values, use.names = FALSE)
  } else if (is.list(value)) {
    stop(name, " must be a two-level vector or typed level object.",
         call. = FALSE)
  }

  if (!is.atomic(value) || length(value) != 2L || anyNA(value)) {
    stop(name, " must contain exactly two non-missing public levels.",
         call. = FALSE)
  }
  kind <- .association_level_kind(value)
  if (identical(kind, "unsupported") ||
      (!is.null(claimed) && !identical(kind, claimed))) {
    stop(name, " levels must share one string, boolean, or numeric type.",
         call. = FALSE)
  }

  if (identical(kind, "string")) {
    value <- vapply(seq_along(value), function(index) {
      .association_scalar_text(value[[index]], paste0(name, "[", index, "]"))
    }, character(1))
  } else if (identical(kind, "number")) {
    value <- as.numeric(value)
    if (any(!is.finite(value))) {
      stop(name, " numeric levels must be finite.", call. = FALSE)
    }
    value[value == 0] <- 0
  } else {
    value <- as.logical(value)
  }
  if (anyDuplicated(value)) {
    stop(name, " reference and positive levels must be distinct.",
         call. = FALSE)
  }
  list(type = kind, values = unname(value))
}

.association_privacy_unit <- function(value) {
  value <- tolower(as.character(unlist(value, use.names = FALSE)))
  if (length(value) != 1L || is.na(value) ||
      !value %in% c("row", "patient")) {
    stop("association privacy unit must be exactly row or patient.",
         call. = FALSE)
  }
  value
}

.association_unit_semantics <- function(privacy_unit) {
  if (identical(.association_privacy_unit(privacy_unit), "row")) {
    "row-one-hot/v1"
  } else {
    "patient-ever-positive/v1"
  }
}

.association_contract_payload <- function(
    outcome_column, exposure_column, outcome_levels, exposure_levels,
    privacy_unit) {
  outcome_column <- .association_column(outcome_column, "outcome column")
  exposure_column <- .association_column(exposure_column, "exposure column")
  if (identical(outcome_column, exposure_column)) {
    stop("Association outcome and exposure columns must be distinct.",
         call. = FALSE)
  }
  unit <- .association_privacy_unit(privacy_unit)
  list(
    contract = .ASSOCIATION_CONTRACT,
    schema = 1L,
    exposure = list(
      column = exposure_column,
      levels = .association_level_spec(
        exposure_levels, "association-exposure-levels")),
    outcome = list(
      column = outcome_column,
      levels = .association_level_spec(
        outcome_levels, "association-outcome-levels")),
    order = "exposure-major/outcome-minor",
    privacy_unit = unit,
    shape = c(3L, 3L),
    unit_semantics = .association_unit_semantics(unit),
    unknown = "all-other-values/v1"
  )
}

.association_canonical_json <- function(value) {
  canonical <- as.character(jsonlite::toJSON(
    value, auto_unbox = TRUE, null = "null", na = "null", digits = NA,
    always_decimal = TRUE, pretty = FALSE))
  raw <- charToRaw(enc2utf8(canonical))
  if (length(raw) > .ASSOCIATION_MAX_MANIFEST_BYTES) {
    stop("Canonical association contract exceeds 65536 bytes.",
         call. = FALSE)
  }
  canonical
}

.association_contract_sha256 <- function(
    outcome_column, exposure_column, outcome_levels, exposure_levels,
    privacy_unit) {
  payload <- .association_contract_payload(
    outcome_column, exposure_column, outcome_levels, exposure_levels,
    privacy_unit)
  digest::digest(
    charToRaw(enc2utf8(.association_canonical_json(payload))),
    algo = "sha256", serialize = FALSE)
}

.association_sha256 <- function(value, name) {
  value <- as.character(unlist(value, use.names = FALSE))
  if (length(value) != 1L || is.na(value) ||
      !identical(value, tolower(value)) ||
      !grepl("^[0-9a-f]{64}$", value)) {
    stop(name, " must be one lowercase SHA-256 digest.", call. = FALSE)
  }
  value
}

.association_node_count <- function(value) {
  value <- suppressWarnings(as.numeric(unlist(value, use.names = FALSE)))
  if (length(value) != 1L || !is.finite(value) || value != floor(value) ||
      value < 1L || value > .ASSOCIATION_MAX_NODES) {
    stop("association-n-nodes must be an integer in [1, 65536].",
         call. = FALSE)
  }
  as.integer(value)
}

.association_job_payload <- function(
    association_contract_sha256, runner_abi, runner_sha256, n_nodes) {
  contract_sha <- .association_sha256(
    association_contract_sha256, "association-contract-sha256")
  abi <- suppressWarnings(as.numeric(unlist(runner_abi, use.names = FALSE)))
  if (length(abi) != 1L || !is.finite(abi) || abi != 3) {
    stop("Association jobs require runner_abi=3.", call. = FALSE)
  }
  list(
    schema = 1L,
    "association-contract-sha256" = contract_sha,
    runner_abi = 3L,
    runner_sha256 = .association_sha256(
      runner_sha256, "runner_sha256"),
    n_nodes = .association_node_count(n_nodes),
    dp_track = "association"
  )
}

.association_job_sha256 <- function(
    association_contract_sha256, runner_abi, runner_sha256, n_nodes) {
  payload <- .association_job_payload(
    association_contract_sha256, runner_abi, runner_sha256, n_nodes)
  digest::digest(
    charToRaw(enc2utf8(.association_canonical_json(payload))),
    algo = "sha256", serialize = FALSE)
}

.normalizeAssociationConfig <- function(run_config, track, unit_policy = NULL) {
  association_fields <- names(run_config)[startsWith(
    tolower(names(run_config)), "association-")]
  if (!identical(track, "association")) {
    if (length(association_fields)) {
      stop("association-* fields require dp-track='association'.",
           call. = FALSE)
    }
    return(run_config)
  }

  allowed <- c(
    "dp-track", "data_type", "num-server-rounds",
    "association-outcome-levels", "association-exposure-levels",
    "association-contract-sha256", "association-n-nodes",
    "association-job-sha256")
  unsupported <- setdiff(names(run_config), allowed)
  if (length(unsupported)) {
    stop("The association track does not accept field(s): ",
         paste(unsupported, collapse = ", "), ".", call. = FALSE)
  }
  if (!identical(as.integer(run_config[["num-server-rounds"]]), 1L)) {
    stop("The association track has exactly one private release.",
         call. = FALSE)
  }
  data_type <- tolower(as.character(unlist(
    run_config[["data_type"]] %||% "tabular", use.names = FALSE)))
  if (length(data_type) != 1L || is.na(data_type) ||
      !identical(data_type, "tabular")) {
    stop("The association track accepts tabular data only.", call. = FALSE)
  }
  run_config[["data_type"]] <- "tabular"
  run_config[["association-outcome-levels"]] <- .association_level_spec(
    run_config[["association-outcome-levels"]],
    "association-outcome-levels")
  run_config[["association-exposure-levels"]] <- .association_level_spec(
    run_config[["association-exposure-levels"]],
    "association-exposure-levels")
  run_config[["association-contract-sha256"]] <- .association_sha256(
    run_config[["association-contract-sha256"]],
    "association-contract-sha256")
  run_config[["association-n-nodes"]] <- .association_node_count(
    run_config[["association-n-nodes"]])
  run_config[["association-job-sha256"]] <- .association_sha256(
    run_config[["association-job-sha256"]], "association-job-sha256")

  policy <- .resolvePrivacyUnitPolicy(unit_policy)
  run_config[["association-contract"]] <- .ASSOCIATION_CONTRACT
  run_config[["association-privacy-unit"]] <- policy$dp_unit
  run_config[["association-unit-semantics"]] <-
    .association_unit_semantics(policy$dp_unit)
  run_config
}

.verifyAssociationContract <- function(
    run_config, feature_columns, target_column, unit_policy = NULL) {
  if (!identical(run_config[["dp-track"]], "association")) return(run_config)
  target <- as.character(unlist(target_column, use.names = FALSE))
  features <- as.character(unlist(feature_columns, use.names = FALSE))
  if (length(target) != 1L || length(features) != 1L ||
      anyNA(c(target, features)) || any(!nzchar(c(target, features))) ||
      identical(target, features)) {
    stop("Association requires one distinct outcome and exposure column.",
         call. = FALSE)
  }
  policy <- .resolvePrivacyUnitPolicy(unit_policy)
  if (identical(policy$dp_unit, "patient") &&
      policy$patient_column %in% c(target, features)) {
    stop("The server patient identifier cannot be the association outcome or exposure.",
         call. = FALSE)
  }
  actual_contract <- .association_contract_sha256(
    target, features,
    run_config[["association-outcome-levels"]],
    run_config[["association-exposure-levels"]], policy$dp_unit)
  supplied_contract <- .association_sha256(
    run_config[["association-contract-sha256"]],
    "association-contract-sha256")
  if (!identical(actual_contract, supplied_contract)) {
    stop("Association contract SHA-256 does not match the normalized public request.",
         call. = FALSE)
  }
  runner_hash <- .association_sha256(
    .compute_harness_hash(), "runner_sha256")
  actual_job <- .association_job_sha256(
    actual_contract, 3L, runner_hash,
    run_config[["association-n-nodes"]])
  supplied_job <- .association_sha256(
    run_config[["association-job-sha256"]], "association-job-sha256")
  if (!identical(actual_job, supplied_job)) {
    stop("Association job SHA-256 does not match the runner and node roster pins.",
         call. = FALSE)
  }
  run_config[["association-contract-sha256"]] <- actual_contract
  run_config[["association-job-sha256"]] <- actual_job
  run_config
}

# Total private-value mapping.  Every malformed or non-matching value maps to
# the public unknown code, so row preservation never depends on its contents.
.association_encode_values <- function(value, levels) {
  spec <- .association_level_spec(levels, "association levels")
  codes <- rep.int(2L, length(value))
  if (!length(codes)) return(codes)

  if (identical(spec$type, "string")) {
    if (is.factor(value)) value <- as.character(value)
    if (!is.character(value)) return(codes)
    candidate <- tryCatch(
      iconv(value, from = "", to = "UTF-8", sub = NA_character_),
      error = function(e) rep(NA_character_, length(value)))
    valid <- !is.na(candidate)
  } else if (identical(spec$type, "boolean")) {
    if (!is.logical(value)) return(codes)
    candidate <- value
    valid <- !is.na(candidate)
  } else {
    if (!((is.integer(value) || is.double(value)) &&
          !is.object(value))) return(codes)
    candidate <- suppressWarnings(as.numeric(value))
    valid <- !is.na(candidate) & is.finite(candidate)
    candidate[candidate == 0] <- 0
  }
  codes[valid & candidate == spec$values[[1L]]] <- 0L
  codes[valid & candidate == spec$values[[2L]]] <- 1L
  codes
}

.association_prepared_frame <- function(
    data, target_column, feature_columns, extra_config,
    unit_policy = NULL, identity_columns = character()) {
  if (!is.data.frame(data)) data <- as.data.frame(data)
  target <- .association_column(target_column, "outcome column")
  exposure <- as.character(unlist(feature_columns, use.names = FALSE))
  if (length(exposure) != 1L) {
    stop("Association requires exactly one exposure column.", call. = FALSE)
  }
  exposure <- .association_column(exposure, "exposure column")
  if (length(intersect(c(target, exposure), identity_columns))) {
    stop("Imaging feature-view identity columns cannot be analysis columns.",
         call. = FALSE)
  }
  .validateDataSchema(data, target, exposure)
  unit <- if (is.null(unit_policy)) {
    .prepareDpUnitFrame(data)
  } else {
    .prepareImagingPrivacyUnitFrame(data, unit_policy)
  }
  data <- unit$data
  data[[target]] <- .association_encode_values(
    data[[target]], extra_config[["association-outcome-levels"]])
  data[[exposure]] <- .association_encode_values(
    data[[exposure]], extra_config[["association-exposure-levels"]])
  prepared <- .prepareTrainingFrame(
    data, target_column = target, feature_columns = exposure,
    drop_missing = FALSE, select_columns = TRUE,
    patient_column = unit$patient_column)
  output <- prepared$data
  for (column in setdiff(identity_columns, names(output))) {
    output[[column]] <- unit$data[[column]]
  }
  list(
    data = output, prepared = prepared, unit = unit,
    target = target, exposure = exposure)
}

.stageAssociationData <- function(
    data, run_token, target_column, feature_columns, extra_config = list(),
    source_config = list(), required_bytes = 0, unit_policy = NULL,
    identity_columns = character()) {
  extra_config <- .validate_manifest_extra_config(extra_config)
  if (!identical(extra_config[["dp-track"]], "association")) {
    stop("Association staging requires dp-track='association'.",
         call. = FALSE)
  }
  frame <- .association_prepared_frame(
    data, target_column, feature_columns, extra_config,
    unit_policy = unit_policy, identity_columns = identity_columns)
  staging_dir <- .ensureStagingDir(run_token, required_bytes = required_bytes)
  use_parquet <- requireNamespace("arrow", quietly = TRUE)
  data_file <- if (use_parquet) "train.parquet" else "train.csv"
  data_format <- if (use_parquet) "parquet" else "csv"
  output <- file.path(staging_dir, data_file)
  if (use_parquet) {
    arrow::write_parquet(frame$data, output)
  } else {
    utils::write.csv(frame$data, output, row.names = FALSE)
  }
  Sys.chmod(output, "0600")

  manifest <- list(
    run_token = run_token,
    data_type = "tabular",
    data_file = data_file,
    data_format = data_format,
    n_samples = frame$prepared$n_samples,
    n_units = .countDpUnits(
      frame$data, frame$unit$dp_unit, frame$unit$patient_column),
    n_input_samples = frame$prepared$n_input_samples,
    dropped_missing = frame$prepared$dropped_missing,
    target_column = frame$target,
    feature_columns = frame$exposure,
    "dp-unit" = frame$unit$dp_unit,
    patient_column = frame$unit$patient_column,
    "patient-id-canonicalization" = frame$unit$canonicalization,
    "target-preencoded" = TRUE,
    "association-preencoded" = TRUE,
    staged_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )
  manifest <- .merge_manifest_config(manifest, source_config)
  manifest <- .merge_manifest_config(manifest, extra_config)
  manifest <- .normalize_dp_manifest(manifest)
  .write_manifest_atomic(manifest, file.path(staging_dir, "manifest.json"))
  staging_dir
}

.stageAssociationDescriptorParquet <- function(
    desc, run_token, target_column, feature_columns, extra_config) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for staged_parquet descriptors.",
         call. = FALSE)
  }
  meta <- desc$metadata
  if (is.null(meta) || is.null(meta$file) || !file.exists(meta$file)) {
    stop("The configured association Parquet source is unavailable.",
         call. = FALSE)
  }
  target <- .association_column(target_column, "outcome column")
  exposure <- as.character(unlist(feature_columns, use.names = FALSE))
  if (length(exposure) != 1L) {
    stop("Association requires exactly one exposure column.", call. = FALSE)
  }
  exposure <- .association_column(exposure, "exposure column")
  policy <- .dpUnitPolicy()
  patient_column <- if (identical(policy$dp_unit, "patient")) {
    policy$patient_column
  } else {
    NULL
  }
  cols_needed <- unique(c(target, exposure, patient_column))
  staging_dir <- .ensureStagingDir(
    run_token,
    required_bytes = .ASSOCIATION_MAX_PARQUET_MATERIALIZED_BYTES)
  projected_path <- file.path(staging_dir, "association-projection.parquet")
  on.exit(if (file.exists(projected_path)) unlink(projected_path), add = TRUE)
  bounded <- .materializeAssociationParquet(
    meta$file, projected_path, cols_needed)
  before <- file.info(projected_path)
  first_digest <- if (file.exists(projected_path)) {
    digest::digest(file = projected_path, algo = "sha256", serialize = FALSE)
  } else {
    ""
  }
  if (!file.exists(projected_path) || isTRUE(before$isdir) ||
      .privacy_path_is_link(projected_path) ||
      !identical(as.numeric(before$size), bounded$file_bytes) ||
      !identical(first_digest, bounded$sha256)) {
    stop("Trusted association Parquet projection is invalid.", call. = FALSE)
  }
  input <- arrow::ReadableFile$create(projected_path)
  on.exit(if (!is.null(input)) try(input$close(), silent = TRUE), add = TRUE)
  second_digest <- digest::digest(
    file = projected_path, algo = "sha256", serialize = FALSE)
  if (!identical(as.numeric(input$GetSize()), bounded$file_bytes) ||
      !identical(second_digest, bounded$sha256)) {
    stop("Trusted association Parquet projection changed before reading.",
         call. = FALSE)
  }
  selected <- as.data.frame(arrow::read_parquet(input))
  input$close()
  input <- NULL
  selected_bytes <- as.numeric(utils::object.size(selected))
  if (nrow(selected) != bounded$rows ||
      !identical(names(selected), cols_needed) ||
      !is.finite(selected_bytes) ||
      selected_bytes + bounded$rows * 64 >
        .ASSOCIATION_MAX_PARQUET_MATERIALIZED_BYTES) {
    stop("Trusted association Parquet projection changed during reading.",
         call. = FALSE)
  }
  unlink(projected_path)
  .stageAssociationData(
    selected, run_token, target, exposure, extra_config,
    source_config = list(
      dataset_id = desc$dataset_id,
      source_kind = "staged_parquet"),
    required_bytes = max(bounded$file_bytes, bounded$materialized_bytes))
}

.materializeAssociationParquet <- function(
    source, destination, columns,
    max_rows = .ASSOCIATION_MAX_PARQUET_ROWS,
    max_bytes = .ASSOCIATION_MAX_PARQUET_MATERIALIZED_BYTES,
    runner_dir = system.file(
      "flower_app", "dsflower_runner", package = "dsFlower"),
    runtime_root = .native_tree_runtime_root(),
    run_projection = processx::run) {
  max_rows <- suppressWarnings(as.numeric(max_rows))
  max_bytes <- suppressWarnings(as.numeric(max_bytes))
  if (length(max_rows) != 1L || !is.finite(max_rows) || max_rows < 1 ||
      max_rows != floor(max_rows) ||
      length(max_bytes) != 1L || !is.finite(max_bytes) || max_bytes < 1 ||
      max_bytes != floor(max_bytes)) {
    stop("Association Parquet materialization cap is invalid.", call. = FALSE)
  }
  if (!is.character(source) || length(source) != 1L || is.na(source) ||
      !.path_is_absolute(source) ||
      !is.character(destination) || length(destination) != 1L ||
      is.na(destination) || !.path_is_absolute(destination) ||
      !is.character(columns) || !length(columns) || length(columns) > 3L ||
      anyNA(columns) || any(!nzchar(columns)) || anyDuplicated(columns) ||
      !nzchar(runner_dir) || !dir.exists(runner_dir) ||
      !file.exists(file.path(runner_dir, "association_parquet.py")) ||
      !nzchar(runtime_root) || !dir.exists(runtime_root)) {
    stop("Trusted association Parquet projection is unavailable.",
         call. = FALSE)
  }
  python <- .native_tree_runtime_executable(runtime_root, "python")
  if (!file.exists(python) || dir.exists(python)) {
    stop("Trusted association Parquet projection is unavailable.",
         call. = FALSE)
  }
  code <- paste(
    "import json,sys",
    "sys.path.insert(0,sys.argv[1])",
    paste0("from dsflower_runner.association_parquet import ",
           "materialize_bounded_projection"),
    paste0("value=materialize_bounded_projection(sys.argv[2],sys.argv[3],",
           "json.loads(sys.argv[4]),max_rows=int(sys.argv[5]),",
           "max_bytes=int(sys.argv[6]))"),
    paste0("sys.stdout.write(json.dumps(value,ensure_ascii=True,",
           "allow_nan=False,sort_keys=True,separators=(',',':')))"),
    sep = "\n")
  loader_names <- names(Sys.getenv())[
    grepl("^(LD_|DYLD_)", names(Sys.getenv()), perl = TRUE)]
  cleared <- stats::setNames(rep.int("", length(loader_names)), loader_names)
  env <- c("current", cleared,
           PYTHONHOME = "", PYTHONPATH = "", PYTHONSTARTUP = "",
           PYTHONINSPECT = "", PYTHONNOUSERSITE = "1",
           DSFLOWER_MANIFEST_DIR = "", DSFLOWER_PINNED_APP_DIR = "",
           DSFLOWER_XGBOOST_BUNDLE_ROOT = "", VIRTUAL_ENV = runtime_root)
  result <- tryCatch(run_projection(
    command = python,
    args = c(
      "-I", "-c", code, dirname(runner_dir), source, destination,
      as.character(jsonlite::toJSON(
        unname(columns), auto_unbox = FALSE, pretty = FALSE)),
      format(max_rows, scientific = FALSE, trim = TRUE),
      format(max_bytes, scientific = FALSE, trim = TRUE)),
    env = env, error_on_status = FALSE, timeout = 300),
    error = function(e) NULL)
  if (is.null(result) || !identical(as.integer(result$status), 0L) ||
      !is.character(result$stdout) || length(result$stdout) != 1L ||
      nchar(result$stdout, type = "bytes") > 4096L) {
    stop("Trusted association Parquet projection failed.", call. = FALSE)
  }
  value <- tryCatch(
    jsonlite::fromJSON(result$stdout, simplifyVector = TRUE),
    error = function(e) NULL)
  expected <- c(
    "contract", "file_bytes", "materialized_bytes", "rows", "sha256")
  numeric_fields <- c("file_bytes", "materialized_bytes", "rows")
  valid_numeric <- is.list(value) && all(vapply(numeric_fields, function(name) {
    item <- value[[name]]
    (is.integer(item) || is.double(item)) && !is.object(item) &&
      !is.logical(item) && length(item) == 1L && is.finite(item) &&
      item >= 0 && item == floor(item)
  }, logical(1)))
  if (!valid_numeric || is.null(names(value)) || anyDuplicated(names(value)) ||
      !setequal(names(value), expected) ||
      !identical(value$contract, .ASSOCIATION_PARQUET_PROJECTION_CONTRACT) ||
      value$file_bytes < 1 || value$file_bytes > max_bytes ||
      value$materialized_bytes + value$rows * 64 > max_bytes ||
      value$rows > max_rows ||
      !identical(.association_sha256(value$sha256, "projection sha256"),
                 value$sha256)) {
    stop("Trusted association Parquet projection returned invalid metadata.",
         call. = FALSE)
  }
  list(
    file_bytes = as.numeric(value$file_bytes),
    materialized_bytes = as.numeric(value$materialized_bytes),
    rows = as.numeric(value$rows), sha256 = value$sha256)
}

.validate_association_probe <- function(value = "none") {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !value %in% c("none", "runtime")) {
    stop("association_probe must be exactly 'none' or 'runtime'.",
         call. = FALSE)
  }
  value
}

.association_runtime_probe <- function(
    runner_dir = system.file(
      "flower_app", "dsflower_runner", package = "dsFlower"),
    integrity_hook = system.file(
      "python", "sitecustomize.py", package = "dsFlower"),
    runtime_root = .native_tree_runtime_root(),
    run_probe = processx::run) {
  tryCatch({
    if (!nzchar(runner_dir) || !dir.exists(runner_dir) ||
        !nzchar(integrity_hook) || !file.exists(integrity_hook) ||
        dir.exists(integrity_hook)) return(FALSE)
    required <- file.path(runner_dir, c(
      "epi_association.py", "association_parquet.py",
      "association_client_app.py",
      "association_server_app.py", "association_runtime_probe.py"))
    if (!all(file.exists(required)) || any(dir.exists(required))) return(FALSE)
    guard <- readLines(integrity_hook, warn = FALSE)
    if (!any(grepl("dsflower_runner.association_client_app:app",
                   guard, fixed = TRUE))) return(FALSE)
    if (!nzchar(runtime_root) || !dir.exists(runtime_root)) return(FALSE)
    python <- .native_tree_runtime_executable(runtime_root, "python")
    supernode <- .native_tree_runtime_executable(
      runtime_root, "flower-supernode")
    if (!file.exists(python) || dir.exists(python) ||
        !file.exists(supernode) || dir.exists(supernode)) return(FALSE)

    code <- paste(
      "import sys",
      "sys.path.insert(0, sys.argv[1])",
      "from dsflower_runner import association_client_app as client_entry",
      "from dsflower_runner import association_server_app as server_entry",
      "from dsflower_runner import association_runtime_probe as runtime_probe",
      "from flwr.clientapp import ClientApp",
      "from flwr.serverapp import ServerApp",
      paste0("if not (isinstance(client_entry.app, ClientApp) and ",
             "isinstance(server_entry.app, ServerApp)): raise SystemExit(3)"),
      "if runtime_probe.probe_association_runtime() is not True: raise SystemExit(4)",
      "sys.stdout.write('available')",
      sep = "\n")
    loader_names <- names(Sys.getenv())[
      grepl("^(LD_|DYLD_)", names(Sys.getenv()), perl = TRUE)]
    cleared <- stats::setNames(rep.int("", length(loader_names)), loader_names)
    env <- c("current", cleared,
             PYTHONHOME = "", PYTHONPATH = "", PYTHONSTARTUP = "",
             PYTHONINSPECT = "", PYTHONNOUSERSITE = "1",
             DSFLOWER_MANIFEST_DIR = "", DSFLOWER_PINNED_APP_DIR = "",
             DSFLOWER_XGBOOST_BUNDLE_ROOT = "", VIRTUAL_ENV = runtime_root)
    result <- run_probe(
      command = python,
      args = c("-I", "-c", code, dirname(runner_dir)),
      env = env, error_on_status = FALSE, timeout = 30)
    identical(as.integer(result$status), 0L) &&
      identical(result$stdout, "available")
  }, error = function(e) FALSE)
}

.association_contract_capabilities <- function(probe = "none") {
  probe <- .validate_association_probe(probe)
  probed <- identical(probe, "runtime")
  list(
    contract = .ASSOCIATION_CONTRACT,
    result_contract = .ASSOCIATION_RESULT_CONTRACT,
    mechanism = .ASSOCIATION_MECHANISM,
    execution_profile = .ASSOCIATION_EXECUTION_PROFILE,
    shape = c(3L, 3L),
    order = "exposure-major/outcome-minor",
    pooled_only = TRUE,
    privacy_units = c("row", "patient"),
    availability_semantics = "fresh-executable-node-probe",
    probed = probed,
    available = if (probed) isTRUE(.association_runtime_probe()) else NULL
  )
}
