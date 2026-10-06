test_that("Node staging validates, normalizes and pins public FedProx", {
  parse <- dsFlower:::.validate_local_strategy
  avg <- list("dp-track" = "neural", strategy = "fedavg")
  expect_identical(parse(list("dp-track" = "neural", strategy = "fedprox", "strategy-mu" = 0)), avg)
  expect_identical(parse(avg), avg)
  expect_identical(parse(list("dp-track" = "neural")), list("dp-track" = "neural"))
  for (bad in list(TRUE, "0.1", numeric(), c(0, 1), NA_real_, NaN, Inf, -0.01, 1.01)) {
    expect_error(parse(list("dp-track" = "neural", strategy = "fedprox", "strategy-mu" = bad)), "mu")
  }
  for (track in c("native_tree", "association", "validation")) {
    expect_error(parse(list("dp-track" = track, strategy = "fedprox", "strategy-mu" = 0)), "unsupported")
  }
  expect_error(parse(list("dp-track" = "neural", strategy = "fedprox", "strategy-mu" = 1,
                         "learning-rate" = .5, "num-server-rounds" = 2L, "local-epochs" = 2L,
                         "scheduler-name" = "exponential", "scheduler-gamma" = 2)), "learning_rate")
  expect_error(parse(list(strategy = "fedavg", "strategy-mu" = .2)), "inapplicable")
})
