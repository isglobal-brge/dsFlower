# Module: Privacy Accountant
# JSON-based ledger tracking privacy spend per dataset using
# Renyi Differential Privacy (RDP) composition.
#
# RDP provides tighter composition bounds than naive additive accounting.
# We maintain a vector of RDP costs over a set of orders alpha, then
# convert to (epsilon, delta)-DP via the standard RDP->DP conversion.

# Default RDP orders for accounting
.RDP_ORDERS <- c(1.25, 1.5, 2, 4, 8, 16, 32, 64, 128, 256)

#' Get the path to the privacy ledger file
#' @return Character; path to the ledger JSON file.
#' @keywords internal
.ledger_path <- function() {
  ledger_dir <- file.path(rappdirs::user_data_dir("dsFlower"), "privacy")
  dir.create(ledger_dir, recursive = TRUE, showWarnings = FALSE)
  file.path(ledger_dir, "privacy_ledger.json")
}

#' Read the privacy ledger
#' @return Named list of dataset entries with RDP vectors.
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

#' Compute RDP epsilon for the Gaussian mechanism at a given order alpha
#'
#' For the Gaussian mechanism with noise multiplier sigma (noise_std / sensitivity),
#' the RDP cost at order alpha is: alpha / (2 * sigma^2).
#'
#' @param alpha Numeric; the RDP order (> 1).
#' @param sigma Numeric; noise multiplier (noise_std / sensitivity).
#' @return Numeric; RDP epsilon at order alpha.
#' @keywords internal
.rdp_gaussian <- function(alpha, sigma) {
  if (sigma <= 0) return(Inf)
  alpha / (2 * sigma^2)
}

#' Convert RDP guarantees to (epsilon, delta)-DP
#'
#' Uses the standard conversion:
#'   epsilon(delta) = min over alpha of [rdp_eps(alpha) + log(1/delta) / (alpha - 1)]
#'
#' @param rdp_vector Numeric vector; RDP costs at each order.
#' @param orders Numeric vector; the RDP orders.
#' @param delta Numeric; target delta.
#' @return Numeric; the (epsilon, delta)-DP epsilon.
#' @keywords internal
.rdp_to_dp <- function(rdp_vector, orders, delta) {
  if (delta <= 0 || delta >= 1) return(Inf)
  if (all(rdp_vector == 0)) return(0)
  candidates <- rdp_vector + log(1 / delta) / (orders - 1)
  min(candidates)
}

#' Compute the RDP vector for a single DP event (Gaussian mechanism)
#'
#' @param epsilon Numeric; the (eps, delta)-DP epsilon claimed for this run.
#' @param delta Numeric; the delta for this run.
#' @param clipping_norm Numeric; the clipping norm used.
#' @param noise_multiplier Numeric or NULL; if provided, used directly.
#'   Otherwise computed from epsilon/delta/clipping_norm via the Gaussian mechanism.
#' @param orders Numeric vector; RDP orders.
#' @return Numeric vector; RDP costs at each order.
#' @keywords internal
.compute_rdp_vector <- function(epsilon, delta, clipping_norm = 1.0,
                                noise_multiplier = NULL,
                                orders = .RDP_ORDERS) {
  # If noise_multiplier not given, derive from the Gaussian mechanism formula
  if (is.null(noise_multiplier)) {
    # sigma = sqrt(2 * ln(1.25/delta)) * clipping_norm / epsilon
    # noise_multiplier = sigma / clipping_norm = sqrt(2 * ln(1.25/delta)) / epsilon
    noise_multiplier <- sqrt(2 * log(1.25 / delta)) / epsilon
  }

  vapply(orders, function(a) .rdp_gaussian(a, noise_multiplier), numeric(1))
}

#' Record privacy spend for a dataset using RDP composition
#'
#' Adds the RDP cost of a run to the running RDP vector for the dataset.
#' RDP composes by summation of RDP costs at each order.
#'
#' @param dataset_key Character; unique identifier for the dataset.
#' @param epsilon Numeric; epsilon used in this run.
#' @param delta Numeric; delta used in this run.
#' @param run_token Character; the run token for audit trail.
#' @param noise_multiplier Numeric or NULL; if known, used directly for tighter accounting.
#' @param template Character or NULL; template name for audit.
#' @return Invisible NULL.
#' @keywords internal
.record_spend <- function(dataset_key, epsilon, delta, run_token = NULL,
                          noise_multiplier = NULL, template = NULL) {
  ledger <- .read_ledger()
  orders <- .RDP_ORDERS

  entry <- ledger[[dataset_key]]
  if (is.null(entry)) {
    entry <- list(
      orders     = orders,
      rdp_vector = rep(0, length(orders)),
      delta      = delta,
      runs       = list()
    )
  }

  # Compute RDP cost for this run
  run_rdp <- .compute_rdp_vector(epsilon, delta,
                                  noise_multiplier = noise_multiplier,
                                  orders = orders)

  # RDP composition: add element-wise
  current_rdp <- as.numeric(entry$rdp_vector)
  if (length(current_rdp) != length(orders)) {
    current_rdp <- rep(0, length(orders))
  }
  entry$rdp_vector <- current_rdp + run_rdp

  # Compute current (epsilon, delta)-DP from composed RDP
  target_delta <- as.numeric(entry$delta %||% delta)
  entry$epsilon_current <- .rdp_to_dp(entry$rdp_vector, orders, target_delta)

  # Also maintain simple additive totals for backward compatibility
  entry$spent_epsilon <- (entry$spent_epsilon %||% 0) + epsilon
  entry$spent_delta   <- (entry$spent_delta %||% 0) + delta

  # Audit trail
  entry$runs[[length(entry$runs) + 1]] <- list(
    epsilon          = epsilon,
    delta            = delta,
    noise_multiplier = noise_multiplier,
    template         = template,
    run_token        = run_token,
    accountant       = "RDP",
    timestamp        = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  )

  ledger[[dataset_key]] <- entry
  .write_ledger(ledger)
  invisible(NULL)
}

#' Check whether a dataset has sufficient privacy budget remaining
#'
#' Uses the RDP-composed epsilon for budget checking (tighter than additive).
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
  orders <- .RDP_ORDERS

  # Get current RDP state
  current_rdp <- if (!is.null(entry)) as.numeric(entry$rdp_vector) else rep(0, length(orders))
  if (length(current_rdp) != length(orders)) {
    current_rdp <- rep(0, length(orders))
  }

  # Compute what the RDP would be after this run
  run_rdp <- .compute_rdp_vector(epsilon, delta, orders = orders)
  projected_rdp <- current_rdp + run_rdp
  target_delta <- if (!is.null(entry)) as.numeric(entry$delta %||% delta) else delta
  projected_epsilon <- .rdp_to_dp(projected_rdp, orders, target_delta)

  if (projected_epsilon > max_epsilon) {
    current_eps <- .rdp_to_dp(current_rdp, orders, target_delta)
    stop("Privacy budget exhausted for dataset '", dataset_key, "'. ",
         "Current epsilon (RDP): ", round(current_eps, 4),
         ", projected after run: ", round(projected_epsilon, 4),
         ", limit: ", max_epsilon, ".",
         call. = FALSE)
  }

  # Also check additive delta as a simple guard
  spent_del <- if (!is.null(entry)) (entry$spent_delta %||% 0) else 0
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
#' @return Named list with RDP state and derived epsilon.
#' @keywords internal
.get_budget <- function(dataset_key) {
  max_epsilon <- as.numeric(.dsf_option("max_epsilon", 10.0))
  max_delta   <- as.numeric(.dsf_option("max_delta", 1e-3))
  orders <- .RDP_ORDERS

  ledger <- .read_ledger()
  entry <- ledger[[dataset_key]]

  current_rdp <- if (!is.null(entry)) as.numeric(entry$rdp_vector) else rep(0, length(orders))
  if (length(current_rdp) != length(orders)) {
    current_rdp <- rep(0, length(orders))
  }

  target_delta <- if (!is.null(entry)) as.numeric(entry$delta %||% max_delta) else max_delta
  epsilon_rdp <- .rdp_to_dp(current_rdp, orders, target_delta)

  # Backward-compatible additive totals
  spent_eps <- if (!is.null(entry)) (entry$spent_epsilon %||% 0) else 0
  spent_del <- if (!is.null(entry)) (entry$spent_delta %||% 0) else 0

  list(
    dataset_key       = dataset_key,
    accountant        = "RDP",
    epsilon_rdp       = epsilon_rdp,
    remaining_epsilon = max(0, max_epsilon - epsilon_rdp),
    spent_epsilon_additive = spent_eps,
    spent_delta       = spent_del,
    remaining_delta   = max(0, max_delta - spent_del),
    max_epsilon       = max_epsilon,
    max_delta         = max_delta,
    delta_target      = target_delta,
    n_runs            = if (!is.null(entry)) length(entry$runs) else 0L
  )
}
