test_that("server Python contract matches the FAB cryptography floor", {
  expect_true("cryptography>=42.0.0" %in% dsFlower:::.BASE_PYTHON_DEPS)
})

test_that("server venv health check covers all direct runtime imports", {
  imports <- strsplit(
    dsFlower:::.FRAMEWORK_HEALTH_IMPORT$pytorch, ", ", fixed = TRUE
  )[[1L]]
  expect_setequal(
    imports,
    c(
      "flwr", "numpy", "pandas", "pyarrow", "cryptography", "torch",
      "opacus", "torchvision", "PIL", "nibabel", "pydicom", "nrrd",
      "SimpleITK", "monai"
    )
  )
})
