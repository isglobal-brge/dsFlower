test_that("Rock image privacy state is runtime-only", {
  root <- normalizePath(testthat::test_path("..", ".."), mustWork = TRUE)
  dockerfile <- file.path(root, "docker", "Dockerfile")
  vision_dockerfile <- file.path(root, "docker", "Dockerfile.vision")
  wrapper <- file.path(root, "docker", "dsflower-rock-start.sh")
  skip_if_not(file.exists(dockerfile) && file.exists(vision_dockerfile) &&
                file.exists(wrapper))

  docker_text <- paste(readLines(dockerfile, warn = FALSE), collapse = "\n")
  vision_text <- paste(
    readLines(vision_dockerfile, warn = FALSE), collapse = "\n")
  wrapper_text <- paste(readLines(wrapper, warn = FALSE), collapse = "\n")
  configure_text <- paste(
    readLines(file.path(root, "configure"), warn = FALSE), collapse = "\n")

  expect_match(docker_text, "test ! -e /var/lib/dsflower/privacy/noise_root",
               fixed = TRUE)
  expect_match(docker_text, "test ! -e /var/lib/dsflower/privacy/ledger.sqlite",
               fixed = TRUE)
  expect_match(vision_text, "test ! -e /var/lib/dsflower/privacy/noise_root",
               fixed = TRUE)
  expect_match(vision_text,
               "test ! -e /var/lib/dsflower/privacy/ledger.sqlite",
               fixed = TRUE)
  expect_match(wrapper_text, "DSFLOWER_NODE_SECRET_FILE:-", fixed = TRUE)
  expect_match(wrapper_text, "DSFLOWER_PRIVACY_LEDGER_PATH:-", fixed = TRUE)
  expect_match(wrapper_text, "deferred to the first session", fixed = TRUE)
  expect_match(docker_text, "exec[[:space:]]+gosu", fixed = TRUE)
  expect_match(docker_text, "start-rock-upstream.sh", fixed = TRUE)
  expect_match(wrapper_text, ".privacy_runtime_bootstrap", fixed = TRUE)
  expect_match(wrapper_text, "exec /opt/obiba/bin/start-rock-upstream.sh",
               fixed = TRUE)
  expect_false(grepl("ensure_node_secret|privacy_runtime_bootstrap",
                     configure_text))

  bash <- Sys.which("bash")
  skip_if(!nzchar(bash), "bash is required for wrapper syntax validation")
  expect_identical(
    system2(bash, c("-n", shQuote(wrapper)), stdout = TRUE, stderr = TRUE),
    character()
  )
})
