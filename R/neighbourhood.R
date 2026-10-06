# Custodian-owned neighbourhood release policy. These values are operational
# state, never analyst manifest fields or additions to fresh v3 R/B/K identity.

.neighbourhood_settings <- function() {
  integer_option <- function(name, fallback, floor_value = 1) {
    value <- .dsf_option(name, fallback)
    if (!is.numeric(value) || length(value) != 1L || !is.finite(value) ||
        value < 1 || value != floor(value) || value > 2^53 - 1) {
      stop("dsflower.", name, " must be one positive exact integer.",
           call. = FALSE)
    }
    max(as.numeric(value), floor_value)
  }
  k <- integer_option("neighbourhood_k",
    getOption("nfilter.subset", getOption("default.nfilter.subset", 3)), 2)
  max_anchors <- integer_option("neighbourhood_max_anchors", 256)
  capacity <- integer_option("neighbourhood_store_bytes", 64 * 1024^3)
  directory <- .dsf_option("neighbourhood_state_dir", paste0(
    .node_secret_path(), ".neighbourhood"))
  if (!is.character(directory) || length(directory) != 1L ||
      is.na(directory) || !.path_is_absolute(directory)) {
    stop("dsflower.neighbourhood_state_dir must be an absolute non-link path.",
         call. = FALSE)
  }
  probe <- directory
  repeat {
    if (.privacy_path_is_link(probe)) {
      stop("The neighbourhood state path must not contain links.", call. = FALSE)
    }
    if (identical(dirname(probe), probe)) break
    probe <- dirname(probe)
  }
  directory <- .canonical_state_path(directory)
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET", ""), "1")
  if (.privacy_path_is_ephemeral(directory) && !allow_test_tmp) {
    stop("The dsFlower neighbourhood store must use persistent storage.",
         call. = FALSE)
  }
  forbidden <- c(
    file.path(.stagingBaseCandidates(create = FALSE), "dsflower"),
    .venv_root(),
    .dsf_option("app_spool_root", Sys.getenv(
      "DSFLOWER_APP_SPOOL_ROOT", unset = "/var/lib/dsflower/appstore")))
  forbidden <- vapply(forbidden, .canonical_state_path, character(1))
  if (any(vapply(forbidden, function(root) {
    identical(directory, root) || startsWith(directory, paste0(root, "/")) ||
      startsWith(root, paste0(directory, "/"))
  }, logical(1)))) {
    stop("The neighbourhood store must be outside staging and Hook mounts.",
         call. = FALSE)
  }
  store_id <- .dsf_option("neighbourhood_store_id", "")
  if (!is.character(store_id) || length(store_id) != 1L || is.na(store_id) ||
      (nzchar(store_id) && !grepl(
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        store_id))) {
    stop("dsflower.neighbourhood_store_id must be empty or a lowercase UUID.",
         call. = FALSE)
  }
  list(directory = directory, k = k, max_anchors = max_anchors,
       capacity = capacity, store_id = store_id)
}

.neighbourhood_environment <- function() {
  settings <- .neighbourhood_settings()
  number <- function(value) format(value, scientific = FALSE, trim = TRUE)
  c(DSFLOWER_NEIGHBOURHOOD_DIR = settings$directory,
    DSFLOWER_NEIGHBOURHOOD_K = number(settings$k),
    DSFLOWER_NEIGHBOURHOOD_MAX_ANCHORS = number(settings$max_anchors),
    DSFLOWER_NEIGHBOURHOOD_STORE_BYTES = number(settings$capacity),
    DSFLOWER_NEIGHBOURHOOD_STORE_ID = settings$store_id)
}

# A retained local UUID pin, permanent bootstrap lock, directory or external pin
# means the root may already authenticate released answers. Never replace it.
.neighbourhood_require_retained_secret <- function(path) {
  directory <- .dsf_option("neighbourhood_state_dir", paste0(
    path, ".neighbourhood"))
  markers <- c(paste0(path, ".neighbourhood-id"),
               paste0(path, ".neighbourhood-id.lock"), directory)
  established <- any(vapply(markers, function(marker) {
    file.exists(marker) || dir.exists(marker) || .privacy_path_is_link(marker)
  }, logical(1)))
  external_id <- .dsf_option("neighbourhood_store_id", "")
  if (established || !identical(external_id, "")) {
    stop("The established dsFlower neighbourhood state requires its original ",
         "node secret. Custodian action required: restore protected state; ",
         "do not replace or rotate the secret to retry this analysis.",
         call. = FALSE)
  }
  invisible(TRUE)
}
