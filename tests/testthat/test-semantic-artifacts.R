local_semantic_artifact_state <- function(.local_envir = parent.frame()) {
  root <- tempfile("semantic-state-")
  dir.create(root, mode = "0700")
  Sys.chmod(root, "0700")
  ledger <- file.path(root, "ledger.sqlite")
  withr::local_options(list(
    dsflower.privacy_ledger_path = ledger,
    dsflower.dp_privacy_domain = "node",
    dsflower.dp_unit = "row",
    dsflower.patient_column = NULL
  ), .local_envir = .local_envir)
  withr::local_envvar(c(
    DSFLOWER_PRIVACY_LEDGER_PATH = NA_character_,
    DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
  ), .local_envir = .local_envir)
  withr::defer(unlink(root, recursive = TRUE), envir = .local_envir)
  ledger
}

account_semantic_test_release <- function(ledger, run_token, max_releases = 1L) {
  allocation <- dsFlower:::.reserve_privacy_run(run_token, max_releases)
  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(
    con,
    "UPDATE privacy_reservations SET claimed_releases=? WHERE run_token=?",
    params = list(max_releases, run_token))
  for (release in seq_len(max_releases)) {
    DBI::dbExecute(
      con,
      paste(
        "INSERT OR IGNORE INTO privacy_release_claims",
        "(run_token,message_id,release_index,created_at)",
        "VALUES (?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"),
      params = list(run_token, paste0("semantic-test-", release), release))
  }
  allocation
}

test_that("semantic artifacts are claimed once and replayed byte-identically", {
  ledger <- local_semantic_artifact_state()
  key <- strrep("a", 64L)
  first_token <- paste0("run_", strrep("1", 32L))
  other_token <- paste0("run_", strrep("2", 32L))

  first <- dsFlower:::.claim_semantic_artifact(
    key, "synopsis-v1", first_token)
  concurrent <- dsFlower:::.claim_semantic_artifact(
    key, "synopsis-v1", other_token)
  expect_identical(first$status, "new")
  expect_identical(concurrent$status, "resume")
  expect_identical(concurrent$run_token, first_token)
  expect_identical(concurrent$generation, 1L)
  allocation <- dsFlower:::.reserve_privacy_run(first$run_token, 1L)
  resumed_allocation <- dsFlower:::.reserve_privacy_run(
    concurrent$run_token, 1L)
  expect_false(allocation$idempotent)
  expect_true(resumed_allocation$idempotent)
  expect_identical(resumed_allocation$allocation_index,
                   allocation$allocation_index)
  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  DBI::dbExecute(
    con,
    "UPDATE privacy_reservations SET claimed_releases=1 WHERE run_token=?",
    params = list(first_token))
  DBI::dbExecute(
    con,
    paste(
      "INSERT INTO privacy_release_claims",
      "(run_token,message_id,release_index,created_at)",
      "VALUES (?,'semantic-test-1',1,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"),
    params = list(first_token))
  DBI::dbDisconnect(con)

  source <- tempfile()
  writeBin(charToRaw("private synopsis release"), source)
  committed <- dsFlower:::.commit_semantic_artifact(
    key, "synopsis-v1", 1L, first_token, source)
  replay <- dsFlower:::.claim_semantic_artifact(
    key, "synopsis-v1", other_token)
  expect_identical(replay$status, "replay")
  expect_identical(readBin(replay$artifact_path, "raw", n = 1000L),
                   charToRaw("private synopsis release"))
  expect_identical(replay$artifact_sha256, committed$artifact_sha256)

  audit_con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(audit_con), add = TRUE)
  rows <- DBI::dbGetQuery(audit_con, "SELECT * FROM privacy_semantic_releases")
  expect_equal(nrow(rows), 1L)
  expect_identical(rows$status, "committed")
})

test_that("lost semantic artifacts rotate without blocking or overwriting audit", {
  ledger <- local_semantic_artifact_state()
  key <- strrep("b", 64L)
  token1 <- paste0("run_", strrep("3", 32L))
  token2 <- paste0("run_", strrep("4", 32L))
  claim <- dsFlower:::.claim_semantic_artifact(key, "synopsis-v1", token1)
  account_semantic_test_release(ledger, token1)
  source <- tempfile()
  writeBin(charToRaw("generation one"), source)
  committed <- dsFlower:::.commit_semantic_artifact(
    key, "synopsis-v1", claim$generation, token1, source)
  writeBin(charToRaw("corrupted generation"), committed$artifact_path)

  replacement <- dsFlower:::.claim_semantic_artifact(
    key, "synopsis-v1", token2)
  expect_identical(replacement$status, "new")
  expect_identical(replacement$generation, 2L)
  expect_identical(replacement$run_token, token2)
  account_semantic_test_release(ledger, token2)

  source2 <- tempfile()
  writeBin(charToRaw("generation two"), source2)
  committed2 <- dsFlower:::.commit_semantic_artifact(
    key, "synopsis-v1", replacement$generation, token2, source2)
  unlink(committed2$artifact_path)
  token3 <- paste0("run_", strrep("8", 32L))
  replacement2 <- dsFlower:::.claim_semantic_artifact(
    key, "synopsis-v1", token3)
  expect_identical(replacement2$status, "new")
  expect_identical(replacement2$generation, 3L)

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  rows <- DBI::dbGetQuery(
    con, "SELECT generation,status FROM privacy_semantic_releases ORDER BY generation")
  expect_equal(rows$generation, c(1L, 2L, 3L))
  expect_equal(rows$status, c("lost", "lost", "reserved"))
})

test_that("semantic artifact unsafe paths fail closed", {
  skip_on_os("windows")
  ledger <- local_semantic_artifact_state()
  key <- strrep("c", 64L)
  token <- paste0("run_", strrep("5", 32L))
  claim <- dsFlower:::.claim_semantic_artifact(key, "synopsis-v1", token)
  account_semantic_test_release(ledger, token)
  source <- tempfile()
  writeBin(charToRaw("safe"), source)
  committed <- dsFlower:::.commit_semantic_artifact(
    key, "synopsis-v1", claim$generation, token, source)
  unlink(committed$artifact_path)
  expect_true(file.symlink(source, committed$artifact_path))
  expect_error(
    dsFlower:::.claim_semantic_artifact(
      key, "synopsis-v1", paste0("run_", strrep("6", 32L))),
    "regular file, not a symbolic link")
})

test_that("semantic artifact replay rejects an intermediate symlink", {
  skip_on_os("windows")
  ledger <- local_semantic_artifact_state()
  key <- strrep("e", 64L)
  token <- paste0("run_", strrep("9", 32L))
  claim <- dsFlower:::.claim_semantic_artifact(key, "synopsis-v1", token)
  account_semantic_test_release(ledger, token)
  source <- tempfile()
  writeBin(charToRaw("safe"), source)
  committed <- dsFlower:::.commit_semantic_artifact(
    key, "synopsis-v1", claim$generation, token, source)
  domain_directory <- dirname(dirname(dirname(dirname(committed$artifact_path))))
  moved <- paste0(domain_directory, "-real")
  expect_true(file.rename(domain_directory, moved))
  expect_true(file.symlink(moved, domain_directory))
  expect_error(
    dsFlower:::.claim_semantic_artifact(
      key, "synopsis-v1", paste0("run_", strrep("0", 32L))),
    "directories must not be symbolic links")
})

test_that("semantic artifacts cannot commit before every DP release is claimed", {
  ledger <- local_semantic_artifact_state()
  key <- strrep("f", 64L)
  token <- paste0("run_", strrep("a", 32L))
  claim <- dsFlower:::.claim_semantic_artifact(key, "synopsis-v1", token)
  source <- tempfile()
  writeBin(charToRaw("not accounted"), source)
  expect_error(
    dsFlower:::.commit_semantic_artifact(
      key, "synopsis-v1", claim$generation, token, source),
    "no privacy reservation")

  dsFlower:::.reserve_privacy_run(token, 2L)
  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  DBI::dbExecute(
    con,
    "UPDATE privacy_reservations SET claimed_releases=1 WHERE run_token=?",
    params = list(token))
  DBI::dbExecute(
    con,
    paste(
      "INSERT INTO privacy_release_claims",
      "(run_token,message_id,release_index,created_at)",
      "VALUES (?,'semantic-test-1',1,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"),
    params = list(token))
  DBI::dbDisconnect(con)
  expect_error(
    dsFlower:::.commit_semantic_artifact(
      key, "synopsis-v1", claim$generation, token, source),
    "releases are incomplete")

  account_semantic_test_release(ledger, token, 2L)
  expect_identical(
    dsFlower:::.commit_semantic_artifact(
      key, "synopsis-v1", claim$generation, token, source)$status,
    "committed")
})

test_that("semantic commit verifies the exact claim before publishing bytes", {
  ledger <- local_semantic_artifact_state()
  key <- strrep("1", 64L)
  token <- paste0("run_", strrep("b", 32L))
  account_semantic_test_release(ledger, token)
  source <- tempfile()
  writeBin(charToRaw("must remain unpublished"), source)
  expect_error(
    dsFlower:::.commit_semantic_artifact(
      key, "synopsis-v1", 1L, token, source),
    "no active ledger claim")
  destination <- dsFlower:::.semantic_artifact_path(
    "node", key, "synopsis-v1", 1L)
  expect_false(file.exists(destination$path))
})

test_that("semantic artifact contracts reject ambiguous identifiers", {
  local_semantic_artifact_state()
  token <- paste0("run_", strrep("7", 32L))
  expect_error(dsFlower:::.claim_semantic_artifact(
    "ABC", "synopsis-v1", token), "lowercase SHA-256")
  expect_error(dsFlower:::.claim_semantic_artifact(
    strrep("a", 64L), "../synopsis", token), "mechanism")
  expect_error(dsFlower:::.claim_semantic_artifact(
    strrep("a", 64L), "synopsis-v1", "run_bad"), "run token")
})

test_that("concurrent semantic claims converge on one active generation", {
  skip_on_os("windows")
  root <- tempfile("semantic-concurrent-")
  dir.create(root, mode = "0700")
  Sys.chmod(root, "0700")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  ledger <- file.path(root, "ledger.sqlite")
  source_root <- normalizePath(testthat::test_path("..", ".."))
  command <- file.path(R.home("bin"), "Rscript")
  expression <- paste(
    "loadNamespace('dsFlower');",
    "if (exists('.claim_semantic_artifact',asNamespace('dsFlower'),",
    "inherits=FALSE)) {",
    "getFromNamespace('.claim_semantic_artifact','dsFlower')(",
    "strrep('d',64),'synopsis-v1',Sys.getenv('DSFLOWER_TEST_TOKEN'))",
    "} else {",
    "root <- Sys.getenv('DSFLOWER_TEST_SOURCE_ROOT');",
    "for (f in list.files(file.path(root,'R'),pattern='[.]R$',",
    "full.names=TRUE)) sys.source(f,envir=.GlobalEnv);",
    ".claim_semantic_artifact(strrep('d',64),'synopsis-v1',",
    "Sys.getenv('DSFLOWER_TEST_TOKEN'))",
    "}"
  )
  tokens <- sprintf("run_%032x", seq_len(8L))
  workers <- lapply(tokens, function(token) {
    processx::process$new(
      command, c("-e", expression),
      env = c(
        "current",
        DSFLOWER_TEST_SOURCE_ROOT = source_root,
        DSFLOWER_TEST_TOKEN = token,
        DSFLOWER_PRIVACY_LEDGER_PATH = ledger,
        DSFLOWER_TEST_ALLOW_EPHEMERAL_LEDGER = "1"
      ),
      stdout = "|", stderr = "|", cleanup = TRUE)
  })
  withr::defer(lapply(workers, function(worker) {
    if (worker$is_alive()) worker$kill_tree()
  }))
  lapply(workers, function(worker) worker$wait(timeout = 15000))
  expect_true(all(vapply(
    workers, function(worker) identical(worker$get_exit_status(), 0L),
    logical(1))), info = paste(vapply(
      workers, function(worker) paste(worker$read_all_error(), collapse = "\n"),
      character(1)), collapse = "\n"))

  con <- DBI::dbConnect(RSQLite::SQLite(), ledger)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  rows <- DBI::dbGetQuery(
    con, "SELECT generation,status,run_token FROM privacy_semantic_releases")
  expect_equal(nrow(rows), 1L)
  expect_identical(rows$generation, 1L)
  expect_identical(rows$status, "reserved")
  expect_true(rows$run_token %in% tokens)
})
