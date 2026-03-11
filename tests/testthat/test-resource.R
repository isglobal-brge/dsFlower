# Tests for R/resource.R — Simplified Resource Resolver

test_that("FlowerResourceResolver.isFor matches correct formats", {
  resolver <- dsFlower:::FlowerResourceResolver$new()

  # Matching formats
  res1 <- structure(list(format = "flower.federation.node", url = "flower+federation:///"), class = "resource")
  expect_true(resolver$isFor(res1))

  res2 <- structure(list(format = "flower-federation-node", url = "flower+federation:///"), class = "resource")
  expect_true(resolver$isFor(res2))

  # Non-matching formats
  res3 <- structure(list(format = "csv", url = "http://example.com/data.csv"), class = "resource")
  expect_false(resolver$isFor(res3))

  res4 <- structure(list(format = NULL, url = "flower+federation:///"), class = "resource")
  expect_false(resolver$isFor(res4))
})

test_that("FlowerResourceClient parses simplified URL", {
  res <- list(
    url = "flower+federation:///data_path=/data/study.csv;data_format=csv;python_path=python3",
    format = "flower.federation.node"
  )
  class(res) <- "resource"

  client <- dsFlower:::FlowerResourceClient$new(res)
  parsed <- client$getParsed()

  expect_equal(parsed$data_path, "/data/study.csv")
  expect_equal(parsed$data_format, "csv")
  expect_equal(parsed$python_path, "python3")
})

test_that("FlowerResourceClient handles defaults", {
  res <- list(
    url = "flower+federation:///data_path=/data/train.csv",
    format = "flower.federation.node"
  )
  class(res) <- "resource"

  client <- dsFlower:::FlowerResourceClient$new(res)
  parsed <- client$getParsed()

  expect_equal(parsed$data_path, "/data/train.csv")
  expect_equal(parsed$data_format, "csv")
  expect_equal(parsed$python_path, "python3")
})

test_that("FlowerResourceClient accessor methods work", {
  res <- list(
    url = "flower+federation:///data_path=/tmp/data.csv;data_format=parquet;python_path=/usr/bin/python3",
    format = "flower.federation.node"
  )
  class(res) <- "resource"

  client <- dsFlower:::FlowerResourceClient$new(res)

  expect_equal(client$getDataPath(), "/tmp/data.csv")
  expect_equal(client$getDataFormat(), "parquet")
  expect_equal(client$getPythonPath(), "/usr/bin/python3")
})

test_that("FlowerResourceClient close is a no-op", {
  res <- list(
    url = "flower+federation:///data_path=/data/x.csv",
    format = "flower.federation.node"
  )
  class(res) <- "resource"

  client <- dsFlower:::FlowerResourceClient$new(res)
  expect_null(client$close())
})
