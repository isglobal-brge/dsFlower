local_neighbourhood_options <- function(.local_envir = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(root, "noise_root"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  keys <- c("dsflower.neighbourhood_k", "default.dsflower.neighbourhood_k",
            "nfilter.subset", "default.nfilter.subset",
            "dsflower.neighbourhood_max_anchors",
            "default.dsflower.neighbourhood_max_anchors",
            "dsflower.neighbourhood_store_bytes",
            "default.dsflower.neighbourhood_store_bytes",
            "dsflower.neighbourhood_state_dir",
            "default.dsflower.neighbourhood_state_dir",
            "dsflower.neighbourhood_store_id",
            "default.dsflower.neighbourhood_store_id")
  withr::local_options(setNames(rep(list(NULL), length(keys)), keys),
                      .local_envir = .local_envir)
  root
}

test_that("neighbourhood k resolves custodian defaults and floors at two", {
  local_neighbourhood_options()
  settings <- dsFlower:::.neighbourhood_settings
  expect_identical(settings()$k, 3)
  options(default.nfilter.subset = 4)
  expect_identical(settings()$k, 4)
  options(nfilter.subset = 5)
  expect_identical(settings()$k, 5)
  options(default.dsflower.neighbourhood_k = 6)
  expect_identical(settings()$k, 6)
  options(dsflower.neighbourhood_k = 7)
  expect_identical(settings()$k, 7)
  options(dsflower.neighbourhood_k = 1)
  expect_identical(settings()$k, 2)
  expect_identical(settings()$max_anchors, 256)
  expect_identical(settings()$capacity, 64 * 1024^3)
  for (value in list(0, -1, 2.5, Inf, NA_real_, c(2, 3), "3", TRUE, 2^53)) {
    expect_error(withr::with_options(list(dsflower.neighbourhood_k = value),
      settings()), "positive exact integer")
  }
  options(dsflower.neighbourhood_k = NULL,
          default.dsflower.neighbourhood_k = NULL, nfilter.subset = 2.5)
  expect_error(settings(), "positive exact integer")
})

test_that("neighbourhood storage settings are bounded and administrator-owned", {
  root <- local_neighbourhood_options()
  withr::local_envvar(c(DSFLOWER_NEIGHBOURHOOD_DIR = "/attacker/store",
    DSFLOWER_NEIGHBOURHOOD_K = "1", DSFLOWER_NEIGHBOURHOOD_STORE_BYTES = "1"))
  settings <- dsFlower:::.neighbourhood_settings()
  expect_identical(settings$directory, file.path(root, "noise_root.neighbourhood"))
  expect_identical(settings$k, 3)
  expect_identical(settings$capacity, 64 * 1024^3)
  for (key in c("neighbourhood_max_anchors", "neighbourhood_store_bytes")) {
    for (value in list(0, -1, 1.5, Inf, NA_real_, c(1, 2), "256", 2^53)) {
      expect_error(withr::with_options(setNames(list(value), paste0("dsflower.", key)),
        dsFlower:::.neighbourhood_settings()), "positive exact integer")
    }
  }
  expect_error(withr::with_options(list(dsflower.neighbourhood_state_dir = "relative"),
    dsFlower:::.neighbourhood_settings()), "absolute")
  expect_error(withr::with_options(list(dsflower.neighbourhood_store_id = "wrong"),
    dsFlower:::.neighbourhood_settings()), "UUID")
  expect_error(withr::with_options(list(
    dsflower.staging_root = file.path(root, "staging"),
    dsflower.neighbourhood_state_dir = file.path(root, "staging", "dsflower", "anchors")),
    dsFlower:::.neighbourhood_settings()), "outside staging")
  withr::local_envvar(c(DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = NA))
  expect_error(dsFlower:::.neighbourhood_settings(), "persistent")
})

test_that("all analyst config boundaries reject neighbourhood controls", {
  for (key in c("neighbourhood_k", "neighbourhood-max-anchors",
                "neighbourhoodStoreBytes", "neighbourhood.store.id")) {
    value <- setNames(list(2), key)
    expect_error(dsFlower:::.validate_client_run_config(value), "unsupported|server-owned")
    expect_error(dsFlower:::.validate_manifest_extra_config(value), "server-owned")
    state <- new.env(parent = emptyenv())
    state$items <- 0L
    expect_error(dsFlower:::.canonicalHookAppValue(list(nested = value), 0L,
      state, top = TRUE), "reserved")
  }
})

test_that("the common Python launch receives validated neighbourhood policy", {
  root <- local_neighbourhood_options()
  id <- "b70eaf66-9c20-4a8f-a443-1ce197bb38ec"
  withr::local_options(list(default.dsflower.neighbourhood_k = 4,
    default.dsflower.neighbourhood_max_anchors = 7,
    default.dsflower.neighbourhood_store_bytes = 987654321,
    default.dsflower.neighbourhood_store_id = id))
  env <- dsFlower:::.neighbourhood_environment()
  expect_identical(unname(env[["DSFLOWER_NEIGHBOURHOOD_DIR"]]),
                   file.path(root, "noise_root.neighbourhood"))
  expect_identical(unname(env[["DSFLOWER_NEIGHBOURHOOD_K"]]), "4")
  expect_identical(unname(env[["DSFLOWER_NEIGHBOURHOOD_MAX_ANCHORS"]]), "7")
  expect_identical(unname(env[["DSFLOWER_NEIGHBOURHOOD_STORE_BYTES"]]), "987654321")
  expect_identical(unname(env[["DSFLOWER_NEIGHBOURHOOD_STORE_ID"]]), id)
})

test_that("established neighbourhood state never regenerates a missing root", {
  local_neighbourhood_options()
  path <- dsFlower:::.node_secret_path()
  pin <- paste0(path, ".neighbourhood-id")
  writeLines("b70eaf66-9c20-4a8f-a443-1ce197bb38ec", pin)
  expect_error(dsFlower:::flowerPrivacyBootstrap(), "requires its original node secret")
  expect_false(file.exists(path))
  unlink(pin)
  dir.create(paste0(path, ".neighbourhood"))
  expect_error(dsFlower:::flowerPrivacyBootstrap(), "requires its original node secret")
  expect_false(file.exists(path))
  unlink(paste0(path, ".neighbourhood"), recursive = TRUE)
  file.create(paste0(path, ".neighbourhood-id.lock"))
  expect_error(dsFlower:::flowerPrivacyBootstrap(), "requires its original node secret")
  expect_false(file.exists(path))
  unlink(paste0(path, ".neighbourhood-id.lock"))
  withr::local_options(list(dsflower.neighbourhood_store_id =
    "b70eaf66-9c20-4a8f-a443-1ce197bb38ec"))
  expect_error(dsFlower:::flowerPrivacyBootstrap(), "requires its original node secret")
  expect_false(file.exists(path))
})

test_that("nodes sharing a secret parent retain separate store defaults", {
  root <- local_neighbourhood_options()
  first <- dsFlower:::.neighbourhood_settings()$directory
  withr::local_envvar(c(DSFLOWER_NODE_SECRET_FILE = file.path(root, "other-secret")))
  second <- dsFlower:::.neighbourhood_settings()$directory
  expect_false(identical(first, second))
  expect_identical(second, file.path(root, "other-secret.neighbourhood"))
})
