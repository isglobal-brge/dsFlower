# Tests for R/descriptor.R

test_that("flower_dataset_descriptor creates valid object", {
  desc <- flower_dataset_descriptor(
    dataset_id = "test.dataset.v1",
    source_kind = "in_memory_df",
    metadata = list(n_samples = 100, columns = c("x", "y"))
  )
  expect_s3_class(desc, "FlowerDatasetDescriptor")
  expect_equal(desc$dataset_id, "test.dataset.v1")
  expect_equal(desc$source_kind, "in_memory_df")
})

test_that("flower_dataset_descriptor rejects invalid source_kind", {
  expect_error(
    flower_dataset_descriptor("test", "invalid_kind"),
    "source_kind"
  )
})

test_that("flower_dataset_descriptor stores extra fields", {
  desc <- flower_dataset_descriptor(
    dataset_id = "test.ds",
    source_kind = "image_bundle",
    metadata = list(),
    custom_field = "hello"
  )
  expect_equal(desc$custom_field, "hello")
})

test_that("as_flower_dataset.data.frame creates descriptor", {
  df <- data.frame(x = 1:10, y = letters[1:10])
  desc <- as_flower_dataset(df)
  expect_s3_class(desc, "FlowerDatasetDescriptor")
  expect_equal(desc$source_kind, "in_memory_df")
  expect_equal(desc$metadata$n_samples, 10)
  expect_true(is.data.frame(desc$table_data))
})

test_that("as_flower_dataset.FlowerDatasetDescriptor is pass-through", {
  desc <- flower_dataset_descriptor("test", "in_memory_df")
  result <- as_flower_dataset(desc)
  expect_identical(result, desc)
})

test_that("as_flower_dataset.default errors with helpful message", {
  expect_error(
    as_flower_dataset(42),
    "Cannot convert"
  )
  expect_error(
    as_flower_dataset("string"),
    "Cannot convert"
  )
})

test_that("print.FlowerDatasetDescriptor works", {
  desc <- flower_dataset_descriptor(
    dataset_id = "test.ds.v1",
    source_kind = "image_bundle",
    metadata = list(n_samples = 500),
    assets = list(images = list(), masks = list())
  )
  output <- capture.output(print(desc))
  expect_true(any(grepl("FlowerDatasetDescriptor", output)))
  expect_true(any(grepl("test.ds.v1", output)))
  expect_true(any(grepl("image_bundle", output)))
  expect_true(any(grepl("500", output)))
})

test_that(".createHandleFromDescriptor produces correct handle", {
  desc <- flower_dataset_descriptor(
    dataset_id = "test.ds",
    source_kind = "in_memory_df",
    metadata = list(n_samples = 5),
    table_data = data.frame(x = 1:5, y = 6:10)
  )
  handle <- dsFlower:::.createHandleFromDescriptor(desc)
  expect_equal(handle$source, "descriptor")
  expect_equal(handle$source_kind, "in_memory_df")
  expect_equal(handle$dataset_id, "test.ds")
  expect_false(handle$prepared)
  expect_s3_class(handle$descriptor, "FlowerDatasetDescriptor")
})

test_that("flowerInitDS accepts FlowerDatasetDescriptor", {
  desc <- flower_dataset_descriptor(
    dataset_id = "test.init",
    source_kind = "in_memory_df",
    metadata = list(n_samples = 3),
    table_data = data.frame(a = 1:3)
  )

  # Simulate what DataSHIELD does: assign in parent frame
  env <- new.env(parent = baseenv())
  assign("my_desc", desc, envir = env)
  assign("flowerInitDS", dsFlower::flowerInitDS, envir = env)

  result <- eval(quote(flowerInitDS("my_desc")), envir = env)
  expect_equal(result$source, "descriptor")
  expect_equal(result$dataset_id, "test.init")
})
