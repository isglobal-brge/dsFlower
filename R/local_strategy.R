# Public local post-processing only; this does not set any privacy parameter.
.validate_local_strategy <- function(config) {
  name <- config[["strategy"]] %||% "fedavg"
  if (!is.character(name) || length(name) != 1L || is.na(name)) {
    stop("strategy must be one public strategy name.", call. = FALSE)
  }
  name <- tolower(name)
  if (name == "prox") name <- "fedprox"
  track <- config[["dp-track"]] %||% "neural"
  allowed <- switch(name,
    fedavg = character(), fedprox = "strategy-mu",
    fedadam = c("strategy-eta", "strategy-eta-l", "strategy-beta-1", "strategy-beta-2", "strategy-tau"),
    fedyogi = c("strategy-eta", "strategy-eta-l", "strategy-beta-1", "strategy-beta-2", "strategy-tau"),
    fedadagrad = c("strategy-eta", "strategy-eta-l", "strategy-tau"),
    fedavgm = c("strategy-server-learning-rate", "strategy-server-momentum"), NULL)
  if (is.null(allowed) || length(setdiff(names(config)[startsWith(names(config), "strategy-")], allowed))) {
    stop("Unknown or inapplicable strategy fields.", call. = FALSE)
  }
  if (identical(track, "egress") && !name %in% c("fedavg", "fedprox")) {
    stop("HookApp supports only FedAvg or FedProx.", call. = FALSE)
  }
  if (name == "fedprox") {
    if (!track %in% c("neural", "egress")) {
      stop("FedProx is unsupported for trees, association and standalone validation.", call. = FALSE)
    }
    mu <- config[["strategy-mu"]]
    if (!is.numeric(mu) || is.logical(mu) || length(mu) != 1L || is.na(mu) ||
        !is.finite(mu) || mu < 0 || mu > 1) {
      stop("FedProx mu must be one finite numeric value in [0, 1].", call. = FALSE)
    }
    if (mu == 0) {
      name <- "fedavg"
      config[["strategy-mu"]] <- NULL
    } else if (track == "neural") {
      base <- config[["learning-rate"]] %||% 0.01
      rounds <- config[["num-server-rounds"]] %||% 1
      epochs <- config[["local-epochs"]] %||% 1
      scheduler <- config[["scheduler-name"]] %||% "none"
      largest <- base
      if (scheduler %in% c("step", "exponential")) {
        last <- rounds * epochs - 1
        exponent <- if (scheduler == "step") last %/% (config[["scheduler-step-size"]] %||% 1) else last
        largest <- max(base, base * (config[["scheduler-gamma"]] %||% 0.1)^exponent)
      } else if (scheduler == "cosine") {
        largest <- max(base, config[["scheduler-min-lr"]] %||% 0)
      }
      if (!is.finite(largest) || largest * mu > 1) {
        stop("FedProx requires scheduled learning_rate * mu <= 1 for every step.", call. = FALSE)
      }
    }
  }
  if (!is.null(config[["strategy"]])) config[["strategy"]] <- name
  config
}
