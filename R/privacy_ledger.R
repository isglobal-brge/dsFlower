# Module: Privacy Accountant
# JSON-based ledger tracking (epsilon, delta) spend per dataset.
# Uses basic sequential composition (epsilons add).

#' Get the path to the privacy ledger file
#' @return Character; path to the ledger JSON file.
#' @keywords internal
.ledger_path <- function() {
  ledger_dir <- file.path(rappdirs::user_data_dir("dsFlower"), "privacy")
  dir.create(ledger_dir, recursive = TRUE, showWarnings = FALSE)
  file.path(ledger_dir, "privacy_ledger.json")
}

#' Read the privacy ledger
#' @return Named list of dataset entries, each with spent_epsilon, spent_delta, entries.
#' @keywords internal
.read_ledger <- function() {
  path <- .ledger_path()
  if (!file.exists(path)) return(list())
  tryCatch(
    jsonlite::fromJSON(path, simplifyVector = FALSE),
    error = function(e) list()
  )
}

#' Write the privacy ledger
#' @param ledger Named list (the ledger data).
#' @return Invisible NULL.
#' @keywords internal
.write_ledger <- function(ledger) {
  path <- .ledger_path()
  jsonlite::write_json(ledger, path, auto_unbox = TRUE, pretty = TRUE)
  Sys.chmod(path, "0600")
  invisible(NULL)
}

#' Record privacy spend for a dataset
#'
#' Adds (epsilon, delta) to the running total for the given dataset key.
#' Uses basic sequential composition: epsilons add, deltas add.
#'
#' @param dataset_key Character; unique identifier for the dataset.
#' @param epsilon Numeric; epsilon spent in this run.
#' @param delta Numeric; delta spent in this run.
#' @param run_token Character; the run token for audit trail.
#' @return Invisible NULL.
#' @keywords internal
.record_spend <- function(dataset_key, epsilon, delta, run_token = NULL) {
  ledger <- .read_ledger()

  entry <- ledger[[dataset_key]]
  if (is.null(entry)) {
    entry <- list(
      spent_epsilon = 0,
      spent_delta   = 0,
      entries       = list()
    )
  }

  entry$spent_epsilon <- entry$spent_epsilon + epsilon
  entry$spent_delta   <- entry$spent_delta + delta
  entry$entries[[length(entry$entries) + 1]] <- list(
    epsilon   = epsilon,
    delta     = delta,
    run_token = run_token,
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  ledger[[dataset_key]] <- entry
  .write_ledger(ledger)
  invisible(NULL)
}

#' Check whether a dataset has sufficient privacy budget remaining
#'
#' @param dataset_key Character; unique identifier for the dataset.
#' @param epsilon Numeric; epsilon to be spent.
#' @param delta Numeric; delta to be spent.
#' @param max_epsilon Numeric; maximum total epsilon (default from option).
#' @param max_delta Numeric; maximum total delta (default from option).
#' @return TRUE invisibly if budget is sufficient, otherwise stops with error.
#' @keywords internal
.check_budget <- function(dataset_key, epsilon, delta,
                          max_epsilon = NULL, max_delta = NULL) {
  max_epsilon <- max_epsilon %||% as.numeric(.dsf_option("max_epsilon", 10.0))
  max_delta   <- max_delta %||% as.numeric(.dsf_option("max_delta", 1e-3))

  ledger <- .read_ledger()
  entry <- ledger[[dataset_key]]

  spent_eps <- if (!is.null(entry)) entry$spent_epsilon else 0
  spent_del <- if (!is.null(entry)) entry$spent_delta else 0

  if ((spent_eps + epsilon) > max_epsilon) {
    stop("Privacy budget exhausted for dataset '", dataset_key, "'. ",
         "Spent: epsilon=", round(spent_eps, 4),
         ", requested: ", round(epsilon, 4),
         ", limit: ", max_epsilon, ".",
         call. = FALSE)
  }

  if ((spent_del + delta) > max_delta) {
    stop("Privacy budget exhausted for dataset '", dataset_key, "'. ",
         "Spent: delta=", format(spent_del, scientific = TRUE),
         ", requested: ", format(delta, scientific = TRUE),
         ", limit: ", format(max_delta, scientific = TRUE), ".",
         call. = FALSE)
  }

  invisible(TRUE)
}

#' Get remaining budget for a dataset
#'
#' @param dataset_key Character; unique identifier for the dataset.
#' @return Named list with spent_epsilon, spent_delta, remaining_epsilon, remaining_delta.
#' @keywords internal
.get_budget <- function(dataset_key) {
  max_epsilon <- as.numeric(.dsf_option("max_epsilon", 10.0))
  max_delta   <- as.numeric(.dsf_option("max_delta", 1e-3))

  ledger <- .read_ledger()
  entry <- ledger[[dataset_key]]

  spent_eps <- if (!is.null(entry)) entry$spent_epsilon else 0
  spent_del <- if (!is.null(entry)) entry$spent_delta else 0

  list(
    dataset_key       = dataset_key,
    spent_epsilon     = spent_eps,
    spent_delta       = spent_del,
    remaining_epsilon = max(0, max_epsilon - spent_eps),
    remaining_delta   = max(0, max_delta - spent_del),
    max_epsilon       = max_epsilon,
    max_delta         = max_delta,
    n_runs            = if (!is.null(entry)) length(entry$entries) else 0L
  )
}
