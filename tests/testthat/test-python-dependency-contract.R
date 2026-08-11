test_that("server Python contract matches the FAB cryptography floor", {
  expect_true("flwr==1.31.0" %in% dsFlower:::.BASE_PYTHON_DEPS)
  expect_true("cryptography>=42.0.0" %in% dsFlower:::.BASE_PYTHON_DEPS)
})

test_that("native-tree has one exact dependency-light provisioned runtime", {
  expect_identical(
    dsFlower:::.python_deps_for_framework("native_tree"),
    c(
      "flwr==1.31.0", "numpy==2.4.6", "pandas==3.0.3",
      "pyarrow==23.0.1", "cryptography==46.0.7"
    )
  )
  expect_identical(dsFlower:::.framework_venv("native_tree"), "native-tree")
  expect_identical(dsFlower:::.framework_venv("native-tree"), "native-tree")

  root <- normalizePath(file.path(testthat::test_path(), "..", ".."),
                        winslash = "/", mustWork = TRUE)
  configure_path <- file.path(root, "configure")
  testthat::skip_if_not(file.exists(configure_path),
                        "source configure script is not installed")
  configure <- paste(readLines(configure_path, warn = FALSE),
                     collapse = "\n")
  expect_match(
    configure,
    paste0(
      'NATIVE_TREE_DEPS="flwr==1\\.31\\.0 numpy==2\\.4\\.6 ',
      'pandas==3\\.0\\.3 pyarrow==23\\.0\\.1 cryptography==46\\.0\\.7"'
    )
  )
  expect_match(
    configure,
    'provision_framework "native-tree" "cpu" .* "true"'
  )
  configure_win <- readLines(file.path(root, "configure.win"), warn = FALSE)
  expect_true(any(grepl("exec sh ./configure", configure_win, fixed = TRUE)))
  expect_match(configure, "pc-windows-msvc", fixed = TRUE)
  expect_match(configure, "venv_executable", fixed = TRUE)
  expect_match(configure, 'LOCALAPPDATA:-${APPDATA:-}', fixed = TRUE)
  expect_match(configure, 'USERPROFILE}/AppData/Local', fixed = TRUE)
  expect_false(grepl("PROGRAMDATA", configure, fixed = TRUE))
  expect_false(grepl("venv_path}/bin/(python|pip)", configure))
})

test_that("venv and pinned uv paths are portable to Windows", {
  root <- file.path("C:", "Users", "dsflower-test", "AppData", "Local")
  withr::local_envvar(c(
    LOCALAPPDATA = root,
    APPDATA = NA,
    USERPROFILE = NA
  ))
  expect_identical(
    dsFlower:::.default_venv_root("windows"),
    file.path(root, "dsflower", "venvs")
  )
  roaming <- file.path("C:", "Users", "dsflower-test", "AppData", "Roaming")
  expect_identical(
    withr::with_envvar(
      c(LOCALAPPDATA = NA, APPDATA = roaming, USERPROFILE = NA),
      dsFlower:::.default_venv_root("windows")
    ),
    file.path(roaming, "dsflower", "venvs")
  )
  profile <- file.path("C:", "Users", "dsflower-test")
  expect_identical(
    withr::with_envvar(
      c(LOCALAPPDATA = NA, APPDATA = NA, USERPROFILE = profile),
      dsFlower:::.default_venv_root("windows")
    ),
    file.path(profile, "AppData", "Local", "dsflower", "venvs")
  )
  expect_identical(
    dsFlower:::.default_venv_root("unix"),
    "/var/lib/dsflower/venvs"
  )
  expect_identical(
    dsFlower:::.venv_executable(root, "python", "windows"),
    file.path(root, "Scripts", "python.exe")
  )
  expect_identical(
    dsFlower:::.venv_executable(root, "flower-supernode", "windows"),
    file.path(root, "Scripts", "flower-supernode.exe")
  )
  expect_identical(
    dsFlower:::.venv_executable("/srv/dsflower", "python", "unix"),
    "/srv/dsflower/bin/python"
  )

  windows <- dsFlower:::.uv_release_asset("Windows", "AMD64")
  expect_identical(windows$triple, "x86_64-pc-windows-msvc")
  expect_identical(windows$archive_ext, ".zip")
  expect_identical(
    windows$member,
    "uv-x86_64-pc-windows-msvc/uv.exe"
  )
  expect_identical(
    dsFlower:::.uv_release_asset("Linux", "aarch64")$member,
    "uv-aarch64-unknown-linux-gnu/uv"
  )
})

test_that("runtime venv root honors the documented environment precedence", {
  environment_root <- file.path(tempdir(), "dsflower-env-venvs")
  option_root <- file.path(tempdir(), "dsflower-option-venvs")
  withr::local_envvar(DSFLOWER_VENV_ROOT = environment_root)
  withr::local_options(dsflower.venv_root = option_root)

  expect_identical(dsFlower:::.venv_root(), environment_root)
})

test_that("venv metadata discovery accepts Unix and Windows layouts", {
  root <- withr::local_tempdir()
  unix <- file.path(root, "lib", "python3.11", "site-packages")
  windows <- file.path(root, "Lib", "site-packages")
  dir.create(unix, recursive = TRUE)
  dir.create(windows, recursive = TRUE)
  expect_setequal(
    normalizePath(dsFlower:::.venv_site_packages_dirs(root), winslash = "/"),
    normalizePath(c(unix, windows), winslash = "/")
  )
})

test_that("configure skip gate is validated before host Python setup", {
  root <- normalizePath(file.path(testthat::test_path(), "..", ".."),
                        winslash = "/", mustWork = TRUE)
  configure_path <- file.path(root, "configure")
  testthat::skip_if_not(file.exists(configure_path),
                        "source configure script is not installed")
  lines <- readLines(configure_path, warn = FALSE)
  gate <- grep("DSFLOWER_SKIP_PYTHON_SETUP:-", lines, fixed = TRUE)[[1L]]
  venv <- grep("^if \\[ -n .*DSFLOWER_VENV_ROOT", lines)[[1L]]
  bootstrap <- grep("^ensure_uv \\|\\|", lines)[[1L]]
  expect_lt(gate, venv)
  expect_lt(gate, bootstrap)

  output <- system2(
    "sh", shQuote(configure_path),
    env = c("DSFLOWER_SKIP_PYTHON_SETUP=1", "HOME="),
    stdout = TRUE, stderr = TRUE)
  expect_match(paste(output, collapse = "\n"), "explicitly skipped")
  expect_false(any(grepl("Venv root|Provisioning", output)))

  invalid <- suppressWarnings(system2(
    "sh", shQuote(configure_path),
    env = c("DSFLOWER_SKIP_PYTHON_SETUP=maybe", "HOME="),
    stdout = TRUE, stderr = TRUE))
  expect_match(paste(invalid, collapse = "\n"), "must be true or false")
  expect_identical(attr(invalid, "status"), 1L)
})

test_that("required Python locks fail closed before host provisioning", {
  root <- normalizePath(file.path(testthat::test_path(), "..", ".."),
                        winslash = "/", mustWork = TRUE)
  configure_path <- file.path(root, "configure")
  testthat::skip_if_not(file.exists(configure_path),
                        "source configure script is not installed")

  missing_base <- suppressWarnings(system2(
    "sh", shQuote(configure_path),
    env = c(
      "DSFLOWER_SKIP_PYTHON_SETUP=0",
      "DSFLOWER_REQUIRE_PYTHON_LOCK=true",
      "DSFLOWER_PYTHON_LOCK="
    ), stdout = TRUE, stderr = TRUE))
  expect_identical(attr(missing_base, "status"), 1L)
  expect_match(paste(missing_base, collapse = "\n"),
               "REQUIRE_PYTHON_LOCK is enabled")

  unreadable <- suppressWarnings(system2(
    "sh", shQuote(configure_path),
    env = c(
      "DSFLOWER_SKIP_PYTHON_SETUP=0",
      "DSFLOWER_REQUIRE_PYTHON_LOCK=true",
      paste0("DSFLOWER_PYTHON_LOCK=", shQuote(root))
    ), stdout = TRUE, stderr = TRUE))
  expect_identical(attr(unreadable, "status"), 1L)
  expect_match(paste(unreadable, collapse = "\n"),
               "not a readable regular file")

  missing_tree <- suppressWarnings(system2(
    "sh", shQuote(configure_path),
    env = c(
      "DSFLOWER_SKIP_PYTHON_SETUP=0",
      "DSFLOWER_REQUIRE_PYTHON_LOCK=false",
      "DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK=true",
      "DSFLOWER_NATIVE_TREE_PYTHON_LOCK="
    ), stdout = TRUE, stderr = TRUE))
  expect_identical(attr(missing_tree, "status"), 1L)
  expect_match(paste(missing_tree, collapse = "\n"),
               "NATIVE_TREE_REQUIRE_PYTHON_LOCK is enabled")

  invalid <- suppressWarnings(system2(
    "sh", shQuote(configure_path),
    env = c(
      "DSFLOWER_SKIP_PYTHON_SETUP=0",
      "DSFLOWER_REQUIRE_PYTHON_LOCK=maybe"
    ), stdout = TRUE, stderr = TRUE))
  expect_identical(attr(invalid, "status"), 1L)
  expect_match(paste(invalid, collapse = "\n"), "must be true or false")
})

.fake_uv_lock_fixture <- function(root, mode) {
  dir.create(root, recursive = TRUE)
  root <- normalizePath(root, winslash = "/", mustWork = TRUE)
  bin <- file.path(root, "bin")
  home <- file.path(root, "home")
  venv_root <- file.path(root, "venvs")
  python <- file.path(root, "fake-python")
  uv <- file.path(bin, "uv")
  log <- file.path(root, "uv.log")
  lock <- file.path(root, "requirements.lock")
  dir.create(bin, recursive = TRUE)
  dir.create(home, recursive = TRUE)
  writeLines(c(
    "#!/bin/sh",
    'case "$1" in',
    "  --version) printf '%s\\n' 'Python 3.11.0' ;;",
    "  -c) exit 0 ;;",
    "  -) printf '%064d\\n' 0 ;;",
    "esac",
    "exit 0"
  ), python)
  writeLines(c(
    "#!/bin/sh",
    'printf \'%s\\n\' "$*" >> "${FAKE_UV_LOG}"',
    'if [ "$1" = python ] && [ "$2" = find ]; then',
    '  printf \'%s\\n\' "${FAKE_PYTHON}"',
    "  exit 0",
    "fi",
    'if [ "$1" = venv ]; then',
    '  for target in "$@"; do :; done',
    '  if [ "${target}" = "${FAKE_STALE_VENV}" ] &&',
    '     [ -e "${FAKE_STALE_SENTINEL}" ]; then',
    "    printf '%s\\n' 'stale sentinel survived'",
    "    exit 40",
    "  fi",
    '  printf \'clean-venv:%s\\n\' "${target}" >> "${FAKE_UV_LOG}"',
    '  if [ "${FAKE_UV_MODE}" = venv-fail ]; then exit 41; fi',
    '  mkdir -p "${target}/bin" "${target}/Scripts" || exit 42',
    '  cp "${FAKE_PYTHON}" "${target}/bin/python" || exit 42',
    '  cp "${FAKE_PYTHON}" "${target}/Scripts/python.exe" || exit 42',
    '  chmod 0755 "${target}/bin/python" "${target}/Scripts/python.exe" || exit 42',
    "  exit 0",
    "fi",
    'if [ "$1" = pip ]; then',
    "  printf '%s\\n' 'fake pip failure sentinel'",
    "  exit 43",
    "fi",
    "exit 44"
  ), uv)
  Sys.chmod(c(python, uv), mode = "0755")
  writeLines(
    paste0("flwr==1.31.0 --hash=sha256:", strrep("0", 64L)),
    lock
  )
  file.create(log)
  list(
    home = home, venv_root = venv_root, python = python, bin = bin,
    log = log, lock = lock, mode = mode
  )
}

test_that("required server locks propagate uv venv and pip failures", {
  root <- normalizePath(file.path(testthat::test_path(), "..", ".."),
                        winslash = "/", mustWork = TRUE)
  configure_path <- file.path(root, "configure")
  testthat::skip_if_not(file.exists(configure_path),
                        "source configure script is not installed")
  fixtures <- withr::local_tempdir()

  for (lock_kind in c("base", "native-tree")) {
    for (mode in c("venv-fail", "pip-fail")) {
      fixture <- .fake_uv_lock_fixture(
        file.path(fixtures, lock_kind, mode), mode
      )
      if (identical(lock_kind, "base")) {
        required_var <- "DSFLOWER_REQUIRE_PYTHON_LOCK"
        target_venv <- "pytorch"
        lock_env <- c(
          paste0("DSFLOWER_PYTHON_LOCK=", fixture$lock),
          "DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK=false",
          "DSFLOWER_NATIVE_TREE_PYTHON_LOCK="
        )
      } else {
        required_var <- "DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK"
        target_venv <- "native-tree"
        lock_env <- c(
          "DSFLOWER_REQUIRE_PYTHON_LOCK=false",
          "DSFLOWER_PYTHON_LOCK=",
          paste0("DSFLOWER_NATIVE_TREE_PYTHON_LOCK=", fixture$lock)
        )
      }
      stale_sentinel <- file.path(
        fixture$venv_root, target_venv, ".unexpected-package"
      )
      stale_venv <- dirname(stale_sentinel)
      if (identical(mode, "pip-fail")) {
        dir.create(file.path(stale_venv, "bin"), recursive = TRUE)
        dir.create(file.path(stale_venv, "Scripts"), recursive = TRUE)
        file.copy(fixture$python, file.path(stale_venv, "bin", "python"))
        file.copy(
          fixture$python,
          file.path(stale_venv, "Scripts", "python.exe")
        )
        Sys.chmod(c(
          file.path(stale_venv, "bin", "python"),
          file.path(stale_venv, "Scripts", "python.exe")
        ), mode = "0755")
        writeLines("stale-lock-marker", file.path(
          stale_venv, ".dsflower_ready"
        ))
        writeLines("must be removed", stale_sentinel)
      }
      fixture_env <- c(
        "DSFLOWER_SKIP_PYTHON_SETUP=0",
        lock_env,
        paste0("DSFLOWER_VENV_ROOT=", fixture$venv_root),
        "DSFLOWER_FORCE_GPU=0",
        paste0("HOME=", fixture$home),
        paste0("FAKE_PYTHON=", fixture$python),
        paste0("FAKE_UV_LOG=", fixture$log),
        paste0("FAKE_UV_MODE=", mode),
        paste0("FAKE_STALE_VENV=", stale_venv),
        paste0("FAKE_STALE_SENTINEL=", stale_sentinel),
        paste0("PATH=", fixture$bin, .Platform$path.sep,
               Sys.getenv("PATH"))
      )
      output <- suppressWarnings(system2(
        "sh", shQuote(configure_path),
        env = c(
          paste0(required_var, "=true"),
          fixture_env
        ), stdout = TRUE, stderr = TRUE
      ))
      info <- paste(lock_kind, mode)
      expect_identical(attr(output, "status"), 1L, info = info)
      uv_log <- readLines(fixture$log, warn = FALSE)
      expect_true(any(grepl(file.path(fixture$venv_root, target_venv),
                            uv_log, fixed = TRUE)), info = info)
      if (identical(mode, "pip-fail")) {
        expect_true(any(grepl(
          paste0("clean-venv:", file.path(fixture$venv_root, target_venv)),
          uv_log, fixed = TRUE
        )))
        expect_false(any(grepl("stale sentinel survived", output,
                               fixed = TRUE)))
        expect_true(any(grepl("--require-hashes", uv_log, fixed = TRUE)))
        expect_match(paste(output, collapse = "\n"),
                     "fake pip failure sentinel")
      }
      expect_false(file.exists(file.path(
        fixture$venv_root, target_venv, ".dsflower_ready"
      )), info = info)

      optional <- suppressWarnings(system2(
        "sh", shQuote(configure_path),
        env = c(paste0(required_var, "=false"), fixture_env),
        stdout = TRUE, stderr = TRUE
      ))
      expect_null(attr(optional, "status"), info = info)
    }
  }
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
  expect_setequal(
    strsplit(
      dsFlower:::.FRAMEWORK_HEALTH_IMPORT[["native-tree"]],
      ", ", fixed = TRUE
    )[[1L]],
    c("flwr", "numpy", "pandas", "pyarrow", "cryptography")
  )
})
