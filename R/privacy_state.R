# Module: stateless privacy policy and persistent node secret
#
# Every training receives the same administrator-pinned privacy contract, and
# sticky randomness is derived from the persistent node secret plus a canonical
# semantic identity in the trusted runner. Historical use never disables a new
# operation.

.privacy_policy <- function(unit_policy = NULL) {
  epsilon <- suppressWarnings(as.numeric(
    .dsf_option("dp_per_training_epsilon", 1.0)))
  delta <- suppressWarnings(as.numeric(
    .dsf_option("dp_per_training_delta", 1e-6)))
  unit_policy <- .resolvePrivacyUnitPolicy(unit_policy)
  adjacency <- "replace_one"

  if (length(epsilon) != 1L || !is.finite(epsilon) ||
      epsilon <= 0 || epsilon > 10) {
    stop("dsflower.dp_per_training_epsilon must be finite and in (0, 10].",
         call. = FALSE)
  }
  if (length(delta) != 1L || !is.finite(delta) ||
      delta <= 0 || delta > 1e-3) {
    stop("dsflower.dp_per_training_delta must be finite and in (0, 1e-3].",
         call. = FALSE)
  }
  material <- paste(
    "dsflower-stateless-per-training-v1",
    format(epsilon, digits = 17, scientific = TRUE),
    format(delta, digits = 17, scientific = TRUE),
    unit_policy$dp_unit,
    unit_policy$patient_column %||% "<none>",
    unit_policy$canonicalization,
    adjacency,
    sep = "|")

  list(
    contract_version = "per-training-v1",
    per_training_epsilon = epsilon,
    per_training_delta = delta,
    dp_unit = unit_policy$dp_unit,
    patient_column = unit_policy$patient_column,
    unit_canonicalization = unit_policy$canonicalization,
    adjacency = adjacency,
    policy_hash = digest::digest(
      material, algo = "sha256", serialize = FALSE)
  )
}

.path_is_absolute <- function(path) {
  length(path) == 1L && !is.na(path) && nzchar(path) &&
    (grepl("^/", path) || grepl("^[A-Za-z]:[/\\\\]", path))
}

.path_is_symlink <- function(path) {
  link <- Sys.readlink(path)
  length(link) == 1L && !is.na(link) && nzchar(link)
}

# utils::file_test("-f") accepts FIFOs on Unix. Secret state must be a regular
# file so a crafted pipe cannot block the trusted runtime.
.path_is_regular_file <- function(path) {
  if (.Platform$OS.type != "unix") {
    return(utils::file_test("-f", path))
  }
  test_bin <- c("/usr/bin/test", "/bin/test")
  test_bin <- test_bin[file.exists(test_bin)][1L]
  if (length(test_bin) != 1L || is.na(test_bin) || !nzchar(test_bin)) {
    stop("No trusted POSIX regular-file test is available.", call. = FALSE)
  }
  status <- suppressWarnings(system2(
    test_bin, c("-f", shQuote(path)), stdout = FALSE, stderr = FALSE))
  identical(as.integer(status), 0L)
}

# Collapse lexical '..' components, resolve the nearest existing parent and
# preserve the final filename. Callers can then reject final-component symlinks.
.canonical_state_path <- function(path) {
  path <- as.character(path)
  if (!.path_is_absolute(path)) return(path)
  slash_path <- gsub("\\\\", "/", path)
  drive <- if (grepl("^[A-Za-z]:/", slash_path)) substr(slash_path, 1L, 2L) else ""
  rest <- if (nzchar(drive)) substring(slash_path, 4L) else substring(slash_path, 2L)
  parts <- strsplit(rest, "/", fixed = TRUE)[[1L]]
  stack <- character(0)
  for (part in parts) {
    if (!nzchar(part) || identical(part, ".")) next
    if (identical(part, "..")) {
      if (length(stack)) stack <- stack[-length(stack)]
    } else {
      stack <- c(stack, part)
    }
  }
  prefix <- if (nzchar(drive)) paste0(drive, "/") else "/"
  lexical <- if (length(stack)) {
    paste0(prefix, paste(stack, collapse = "/"))
  } else {
    prefix
  }

  leaf <- basename(lexical)
  parent <- dirname(lexical)
  suffix <- character(0)
  probe <- parent
  while (!dir.exists(probe) && !identical(dirname(probe), probe)) {
    suffix <- c(basename(probe), suffix)
    probe <- dirname(probe)
  }
  if (dir.exists(probe)) {
    probe <- normalizePath(probe, winslash = "/", mustWork = TRUE)
    parent <- if (length(suffix)) {
      do.call(file.path, as.list(c(probe, suffix)))
    } else {
      probe
    }
  }
  file.path(parent, leaf)
}

.privacy_path_is_ephemeral <- function(path) {
  p <- .canonical_state_path(path)
  roots <- unique(normalizePath(c("/tmp", "/var/tmp", "/dev/shm", tempdir()),
                                winslash = "/", mustWork = FALSE))
  any(vapply(roots, function(root) {
    identical(p, root) || startsWith(p, paste0(root, "/"))
  }, logical(1)))
}

.privacy_effective_uid <- function() {
  probe <- tempfile(pattern = ".dsflower-euid-")
  on.exit(unlink(probe), add = TRUE)
  if (!isTRUE(file.create(probe))) {
    stop("Could not establish the node process owner.", call. = FALSE)
  }
  uid <- suppressWarnings(as.integer(file.info(probe)$uid[[1L]]))
  if (is.na(uid)) {
    stop("Could not establish the node process owner.", call. = FALSE)
  }
  uid
}

.privacy_training_contract <- function(run_token, num_rounds,
                                       unit_policy = NULL) {
  run_token <- .validate_run_token(run_token)
  horizon <- suppressWarnings(as.numeric(num_rounds))
  if (length(horizon) != 1L || !is.finite(horizon) || horizon < 1 ||
      horizon != floor(horizon) || horizon > .Machine$integer.max) {
    stop("num_rounds must be a positive integer.", call. = FALSE)
  }
  horizon <- .validateMaxRounds(as.integer(horizon))
  policy <- .privacy_policy(unit_policy)
  list(
    epsilon = policy$per_training_epsilon,
    delta = policy$per_training_delta,
    num_rounds = horizon,
    dp_unit = policy$dp_unit,
    patient_column = policy$patient_column,
    unit_canonicalization = policy$unit_canonicalization,
    adjacency = policy$adjacency,
    policy_hash = policy$policy_hash,
    run_token = run_token
  )
}

#' Bootstrap persistent differential-privacy state at service runtime
#'
#' Creates or validates only the per-node CSPRNG root. No database, query log,
#' counter or model cache is created. Package installation and loading
#' deliberately never call this function, so reusable images cannot inherit a
#' node identity.
#'
#' @return Invisibly, an operational status with `ok` and `key_action`.
#' @export
flowerPrivacyBootstrap <- function() {
  state <- .privacy_runtime_bootstrap()
  invisible(list(ok = TRUE, key_action = state$key_action))
}

.privacy_runtime_bootstrap <- function() {
  path <- .node_secret_path()
  existed <- file.exists(path) || .path_is_symlink(path)
  valid_before <- if (isTRUE(existed)) {
    tryCatch({
      .validate_node_secret(path)
      TRUE
    }, error = function(e) FALSE)
  } else {
    FALSE
  }
  secret_path <- .ensure_node_secret()
  list(
    key_action = if (isTRUE(valid_before)) {
      "reused"
    } else if (isTRUE(existed)) {
      "rotated"
    } else {
      "initialized"
    },
    secret_path = secret_path
  )
}

# Registered DataSHIELD methods must not relay secret-file diagnostics, which
# can include the node's absolute privacy-state path.
.public_privacy_runtime_bootstrap <- function() {
  tryCatch(
    suppressWarnings(suppressMessages(.privacy_runtime_bootstrap())),
    error = function(e) stop("dsFlower privacy state is unavailable.",
                             call. = FALSE))
}

.privacy_policy_status <- function() {
  policy <- .privacy_policy()
  list(
    accountant = "stateless-per-training-v1",
    guarantee_scope = "per-training",
    per_training_epsilon = policy$per_training_epsilon,
    per_training_delta = policy$per_training_delta,
    dp_unit = policy$dp_unit,
    patient_column = policy$patient_column,
    unit_canonicalization = policy$unit_canonicalization,
    adjacency = policy$adjacency
  )
}
