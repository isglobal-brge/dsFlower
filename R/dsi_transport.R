# Module: DSI-relay transport (Phase 0 — fast federated rounds over DataSHIELD)
#
# A researcher-orchestrated round loop that runs ENTIRELY over the existing
# DataSHIELD channel (the researcher dials OUT to the public Opal endpoints),
# so there is no NAT problem and no Tor. Phase 0 computes one logistic-regression
# gradient step per node per round and returns the LOCAL GRADIENT SUM (an
# aggregate over the site's samples) plus the row count. The client sums the
# gradients (FedSGD) and takes a global step. SecAgg+ masking is layered on top
# in a later phase; here the gradient is returned in the clear for validation.

#' Decode a B64:-prefixed JSON payload argument
#'
#' DataSHIELD's restricted parser rejects raw JSON string literals (brackets and
#' quotes break its lexer), so payloads cross the wire base64-encoded.
#' @keywords internal
.dsi_dec <- function(s) {
  jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(sub("^B64:", "", s))))
}

#' Federated feature statistics for standardization (DSI transport)
#'
#' DataSHIELD AGGREGATE method. Returns this site's per-feature sum, sum of
#' squares and row count so the client can form a GLOBAL mean/sd without any raw
#' value leaving the node (site-level sums only).
#'
#' @param data_symbol Character; data.frame symbol name.
#' @param features_json Character; JSON array of feature column names.
#' @param min_rows Integer; disclosure guard.
#' @return Named list: \code{sum}, \code{sumsq} (numeric, per feature), \code{n}.
#' @keywords internal
#' @export
flowerDsiStatsDS <- function(data_symbol, features_json, min_rows = 10L) {
  obj <- get(data_symbol, envir = parent.frame(), inherits = FALSE)
  df <- as.data.frame(obj)
  features <- as.character(.dsi_dec(features_json))
  X <- data.matrix(df[, features, drop = FALSE])
  keep <- stats::complete.cases(X)
  X <- X[keep, , drop = FALSE]
  if (nrow(X) < as.integer(min_rows)) {
    stop("Disclosive: too few rows for federated statistics.", call. = FALSE)
  }
  list(sum = colSums(X), sumsq = colSums(X^2), n = nrow(X))
}

#' One federated logistic-regression gradient step (DSI transport, Phase 0)
#'
#' DataSHIELD AGGREGATE method. Loads the data symbol, optionally standardizes
#' features with the supplied global mean/sd, evaluates the logistic gradient at
#' the global weights over this site's rows, and returns the gradient SUM and the
#' (guarded) row count. The returned vector is an additive aggregate over the
#' site's samples; under the trust model it is the quantity SecAgg+ will later
#' mask so no single site's gradient is revealed.
#'
#' @param data_symbol Character; name of the data.frame symbol (e.g. "D").
#' @param target Character; binary 0/1 target column name.
#' @param features_json Character; JSON array of feature column names.
#' @param weights_json Character; JSON array of global weights (intercept first).
#' @param stats_json Character or NULL; JSON \code{{mu:[...], sd:[...]}} for
#'   feature standardization (recommended for raw clinical scales).
#' @param min_rows Integer; refuse to compute on fewer rows (disclosure guard).
#' @return Named list: \code{grad} (numeric, length = n_features+1) and \code{n}.
#' @keywords internal
#' @export
flowerDsiStepDS <- function(data_symbol, target, features_json, weights_json,
                            stats_json = NULL, min_rows = 10L) {
  obj <- get(data_symbol, envir = parent.frame(), inherits = FALSE)
  df <- as.data.frame(obj)

  features <- as.character(.dsi_dec(features_json))
  w <- as.numeric(.dsi_dec(weights_json))

  X <- data.matrix(df[, features, drop = FALSE])
  y <- as.numeric(df[[target]])
  keep <- stats::complete.cases(X) & !is.na(y)
  X <- X[keep, , drop = FALSE]
  y <- y[keep]

  if (nrow(X) < as.integer(min_rows)) {
    stop("Disclosive: too few rows for a federated step.", call. = FALSE)
  }

  if (!is.null(stats_json) && is.character(stats_json) && nzchar(stats_json)) {
    st <- .dsi_dec(stats_json)
    X <- sweep(X, 2, as.numeric(st$mu), "-")
    X <- sweep(X, 2, as.numeric(st$sd), "/")
  }

  Xb <- cbind(1, X)                       # intercept column first
  p <- 1 / (1 + exp(-as.numeric(Xb %*% w)))
  grad <- as.numeric(crossprod(Xb, p - y))  # sum of per-sample gradients

  list(grad = grad, n = nrow(Xb))
}
