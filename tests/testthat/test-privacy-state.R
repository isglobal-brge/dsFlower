local_stateless_privacy <- function(.local_envir = parent.frame(),
                                    epsilon = 1, delta = 1e-6) {
  root <- tempfile("dsflower-privacy-state-")
  dir.create(root, recursive = TRUE)
  if (.Platform$OS.type == "windows") {
    dsFlower:::.windows_set_private_acl(root, is_directory = TRUE)
  }
  withr::defer(unlink(root, recursive = TRUE), envir = .local_envir)
  withr::local_options(list(
    dsflower.dp_per_training_epsilon = epsilon,
    dsflower.dp_per_training_delta = delta
  ), .local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_NODE_SECRET_FILE = file.path(root, "noise_root"),
    DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET = "1"
  ), .local_envir = .local_envir)
  root
}

test_that("OS entropy is exact and the Windows CSPRNG parser fails closed", {
  expect_length(dsFlower:::.read_os_entropy(32L), 32L)
  expect_error(dsFlower:::.read_os_entropy(0L), "positive integer")

  command <- NULL
  expected <- as.raw(0:31)
  encoded <- paste(sprintf("%02X", as.integer(expected)), collapse = "")
  runner <- function(script) {
    command <<- script
    encoded
  }
  expect_identical(
    dsFlower:::.read_windows_os_entropy(32L, runner = runner), expected)
  expect_match(command, "RandomNumberGenerator", fixed = TRUE)
  expect_error(
    dsFlower:::.read_windows_os_entropy(
      32L, runner = function(script) strrep("0", 62L)),
    "requested operating-system entropy"
  )
})

test_that("PowerShell encoded commands are a single exact argument", {
  script <- paste(rep("[Console]::Out.Write('OK');", 8L), collapse = "")
  encoded <- dsFlower:::.encode_windows_powershell_command(script)

  expect_false(grepl("[\r\n]", encoded, perl = TRUE))
  expect_identical(
    jsonlite::base64_dec(encoded),
    iconv(script, from = "UTF-8", to = "UTF-16LE", toRaw = TRUE)[[1L]]
  )
})

test_that("Windows PowerShell output is captured as one complete line", {
  skip_on_os("linux")
  skip_on_os("mac")

  expect_identical(
    dsFlower:::.run_windows_powershell("Write-Output 'OK'"),
    "OK"
  )
})

test_that("Windows private ACL and atomic replacement round-trip", {
  skip_on_os("linux")
  skip_on_os("mac")

  diagnostic_runner <- function(script) {
    wrapped <- paste0(
      "$stage='PATH';$code='';try{", script, "}",
      "catch{$safe='';if(([string]$code) -match '^[0-9]{1,10}$')",
      "{$safe=[string]$code};$bits='';",
      "if(([string]$diag) -match '^[01]{9}$'){$bits=[string]$diag};",
      "Write-Output ('FAIL|'+$stage+'|'+$safe+'|'+$bits)}"
    )
    result <- paste(dsFlower:::.run_windows_powershell(wrapped), collapse = "")
    if (!identical(result, "OK")) {
      if (!grepl("^FAIL\\|(PATH|ADD_TYPE|NATIVE)\\|[0-9]{0,10}\\|[01]{0,9}$",
                 result, perl = TRUE)) {
        result <- "FAIL|OUTPUT||"
      }
      stop("Windows replacement diagnostic: ", result, call. = FALSE)
    }
    result
  }

  root <- tempfile("dsflower-windows-acl-")
  dir.create(root)
  withr::defer(unlink(root, recursive = TRUE))
  dsFlower:::.windows_set_private_acl(root, is_directory = TRUE)
  expect_no_error(dsFlower:::.windows_validate_private_acl(root))

  replacement <- file.path(root, "replacement")
  destination <- file.path(root, "destination")
  writeLines("new", replacement)
  writeLines("old", destination)
  dsFlower:::.windows_set_private_acl(replacement, is_directory = FALSE)
  dsFlower:::.windows_set_private_acl(destination, is_directory = FALSE)
  dsFlower:::.windows_replace_file_atomic(
    replacement, destination, runner = diagnostic_runner)

  expect_identical(readLines(destination, warn = FALSE), "new")
  expect_false(file.exists(replacement))
  expect_no_error(dsFlower:::.windows_validate_private_acl(destination))
})

test_that("Windows path and ACL command boundaries are strict and injectable", {
  root <- tempfile("dsflower-O'Brien-")
  dir.create(root)
  withr::defer(unlink(root, recursive = TRUE))
  secret <- file.path(root, "noise_root")
  writeLines(strrep("a", 64L), secret)
  commands <- character(0)
  runner <- function(script) {
    commands <<- c(commands, script)
    if (grepl("Get-Item", script, fixed = TRUE)) "0" else "OK"
  }

  expect_false(dsFlower:::.windows_path_has_reparse_point(root, runner))
  expect_no_error(dsFlower:::.windows_set_private_acl(root, TRUE, runner))
  expect_no_error(dsFlower:::.windows_set_private_acl(secret, FALSE, runner))
  expect_no_error(dsFlower:::.windows_validate_private_acl(secret, runner))
  expect_no_error(dsFlower:::.windows_replace_file_atomic(
    secret, file.path(root, "destination"), runner))
  expect_true(any(grepl("O''Brien", commands, fixed = TRUE)))
  expect_true(any(grepl("RemoveAccessRuleSpecific", commands, fixed = TRUE)))
  replace_command <- commands[grepl("ReplaceFileW", commands, fixed = TRUE)]
  expect_length(replace_command, 1L)
  expect_match(replace_command, "EntryPoint=\"ReplaceFileW\"", fixed = TRUE)
  expect_match(replace_command, "CharSet=CharSet.Unicode", fixed = TRUE)
  expect_match(replace_command, "ExactSpelling=true", fixed = TRUE)
  expect_match(replace_command, "SetLastError=true", fixed = TRUE)
  expect_match(replace_command, "GetDirectoryName($s)", fixed = TRUE)
  expect_match(replace_command, "GetDirectoryName($d)", fixed = TRUE)
  expect_match(replace_command, "$stage='ADD_TYPE'", fixed = TRUE)
  expect_match(replace_command, "$stage='NATIVE'", fixed = TRUE)
  expect_match(replace_command, "private static extern bool ReplaceFileW(",
               fixed = TRUE)
  expect_match(replace_command, "return Marshal.GetLastWin32Error()",
               fixed = TRUE)
  expect_match(replace_command, "$code=[DsFlower.NativeFile]::Replace($d,$s)",
               fixed = TRUE)
  expect_false(grepl("[IO.File]::Replace", replace_command, fixed = TRUE))
  expect_error(
    dsFlower:::.windows_replace_file_atomic(
      secret, file.path(root, "destination"), function(script) "FAILED"),
    "atomically install"
  )

  expect_error(
    dsFlower:::.windows_validate_private_acl(
      secret, runner = function(script) "UNSAFE"),
    "ACL is not private"
  )
  expect_error(
    dsFlower:::.windows_path_has_reparse_point(
      root, runner = function(script) "unexpected"),
    "Could not validate"
  )
})

test_that("privacy policy is fixed per training and creates no call state", {
  root <- local_stateless_privacy(epsilon = 0.75, delta = 2e-6)
  policy <- dsFlower:::.privacy_policy()

  expect_identical(policy$per_training_epsilon, 0.75)
  expect_identical(policy$per_training_delta, 2e-6)
  expect_identical(policy$adjacency, "replace_one")
  expect_match(policy$policy_hash, "^[0-9a-f]{64}$")

  contracts <- lapply(seq_len(1000), function(index) {
    dsFlower:::.privacy_training_contract(
      paste0("run_", sprintf("%032x", index)), num_rounds = 2L)
  })
  expect_true(all(vapply(
    contracts, function(contract) identical(contract$epsilon, 0.75),
    logical(1))))
  expect_true(all(vapply(
    contracts, function(contract) identical(contract$delta, 2e-6),
    logical(1))))
  expect_length(list.files(root, recursive = TRUE), 0L)
})

test_that("training horizon is validated from public inputs alone", {
  local_stateless_privacy(epsilon = 1, delta = 1e-6)
  token <- paste0("run_", strrep("a", 32))

  expect_error(
    dsFlower:::.privacy_training_contract(token, 0), "positive integer")
  expect_error(
    dsFlower:::.privacy_training_contract(token, 1.5), "positive integer")
  expect_error(
    dsFlower:::.privacy_training_contract(token, 501), "exceeds server maximum")
  expect_identical(
    dsFlower:::.privacy_training_contract(token, 500L)$num_rounds, 500L)
})

test_that("bootstrap persists only one custodial noise root", {
  root <- local_stateless_privacy()
  secret <- file.path(root, "noise_root")

  first <- flowerPrivacyBootstrap()
  first_bytes <- readLines(secret, warn = FALSE)
  second <- flowerPrivacyBootstrap()

  expect_identical(first$key_action, "initialized")
  expect_identical(second$key_action, "reused")
  expect_identical(readLines(secret, warn = FALSE), first_bytes)
  expect_match(first_bytes, "^[0-9a-f]{64}$")
  expect_setequal(list.files(root), c("noise_root", "noise_root.lock"))
  expect_identical(file.info(paste0(secret, ".lock"))$size[[1L]], 0)

  writeLines("malformed", secret)
  repaired <- flowerPrivacyBootstrap()
  expect_identical(repaired$key_action, "rotated")
  expect_match(readLines(secret, warn = FALSE), "^[0-9a-f]{64}$")
  expect_setequal(list.files(root), c("noise_root", "noise_root.lock"))
})

test_that("the default node-secret path is platform-aware", {
  local_data <- file.path("C:", "Users", "dsflower-test", "AppData", "Local")
  withr::local_envvar(c(
    LOCALAPPDATA = local_data,
    APPDATA = NA,
    USERPROFILE = NA
  ))
  expect_identical(
    dsFlower:::.default_node_secret_path("windows"),
    file.path(local_data, "dsflower", "privacy", "noise_root")
  )
  expect_identical(
    dsFlower:::.default_node_secret_path("unix"),
    "/var/lib/dsflower/privacy/noise_root"
  )
})

test_that("unsafe secret paths fail closed", {
  skip_on_os("windows")
  root <- local_stateless_privacy()
  target <- file.path(root, "target")
  writeLines(strrep("a", 64), target)
  link <- file.path(root, "noise_root")
  expect_true(file.symlink(target, link))
  expect_error(flowerPrivacyBootstrap(), "link or reparse point")
})

test_that("public privacy status reports only the per-training contract", {
  local_stateless_privacy(epsilon = 0.5, delta = 5e-7)
  status <- flowerPrivacyPolicyDS()

  expect_identical(status$accountant, "stateless-per-training-v1")
  expect_identical(status$guarantee_scope, "per-training")
  expect_identical(status$per_training_epsilon, 0.5)
  expect_identical(status$per_training_delta, 5e-7)
  expect_named(status, c(
    "accountant", "guarantee_scope", "per_training_epsilon",
    "per_training_delta", "dp_unit", "patient_column",
    "unit_canonicalization", "adjacency"
  ))
})
