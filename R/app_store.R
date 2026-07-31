# Module: App store (node side) -- receive + verify researcher-uploaded Flower
# app FABs (Tier-2) over the DataSHIELD channel. Mirrors the relay's idempotent
# offset-framing: the client pushes base64 chunks via datashield.aggregate (never
# assign.expr), the node appends them loss-free, then verifies sha256 before any
# unpack. A hash mismatch or oversize FAB is rejected and the spool destroyed, so
# unverified bytes never become runnable code.

#' @keywords internal
.validate_app_token <- function(token) {
  if (!is.character(token) || length(token) != 1L || is.na(token) ||
      nchar(token, type = "bytes") != 36L ||
      !grepl("^(app|usr)_[0-9a-f]{32}$", token)) {
    stop("Invalid app token.", call. = FALSE)
  }
  token
}

#' @keywords internal
.app_spool_root <- function() {
  configured <- .dsf_option(
    "app_spool_root",
    Sys.getenv("DSFLOWER_APP_SPOOL_ROOT",
               unset = "/var/lib/dsflower/appstore"))
  if (length(configured) != 1L || is.na(configured) ||
      !nzchar(as.character(configured)) ||
      !.path_is_absolute(as.character(configured))) {
    stop("dsflower.app_spool_root must be one absolute path.", call. = FALSE)
  }
  root <- as.character(configured)
  if (.path_is_symlink(root)) {
    stop("App spool root must not be a symbolic link.", call. = FALSE)
  }
  root <- .canonical_state_path(root)
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_APP_SPOOL", ""), "1")
  if (.privacy_path_is_ephemeral(root) && !allow_test_tmp) {
    stop("The app spool must be persistent; /tmp, /var/tmp and /dev/shm ",
         "are not allowed.", call. = FALSE)
  }
  if ((file.exists(root) || dir.exists(root)) && !dir.exists(root)) {
    stop("App spool root is not a directory.", call. = FALSE)
  }
  if (!dir.exists(root)) {
    old_umask <- Sys.umask("0077")
    created <- tryCatch(
      dir.create(root, recursive = TRUE, mode = "0700", showWarnings = FALSE),
      finally = Sys.umask(old_umask)
    )
    if (!isTRUE(created) && !dir.exists(root)) {
      stop("Could not create app spool root: ", root, call. = FALSE)
    }
  }
  if (.path_is_symlink(root) || !dir.exists(root)) {
    stop("App spool root must be a regular directory, not a symbolic link.",
         call. = FALSE)
  }
  Sys.chmod(root, "0700")
  info <- file.info(root)
  if (.Platform$OS.type == "unix") {
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    current_user <- unname(Sys.info()[["effective_user"]])
    if (is.null(current_user) || is.na(current_user) || !nzchar(current_user)) {
      current_user <- unname(Sys.info()[["user"]])
    }
    if (is.na(mode) ||
        bitwAnd(mode, as.integer(strtoi("077", base = 8))) != 0L) {
      stop("App spool root must not be group/world accessible (use 0700).",
           call. = FALSE)
    }
    if (is.null(current_user) || is.na(current_user) || !nzchar(current_user) ||
        is.na(info$uname[[1]]) || !identical(info$uname[[1]], current_user)) {
      stop("App spool root is not owned by the current service user.",
           call. = FALSE)
    }
  }
  if (file.access(root, 2L) != 0L) {
    stop("App spool root is not writable by the current service user.",
         call. = FALSE)
  }
  root
}

#' @keywords internal
.app_spool_dir <- function(token, create = TRUE) {
  tok <- .validate_app_token(token)
  d <- file.path(.app_spool_root(), tok)
  link <- Sys.readlink(d)
  if (!is.na(link) && nzchar(link)) {
    stop("App spool directory must not be a symbolic link.", call. = FALSE)
  }
  if ((file.exists(d) || dir.exists(d)) && !dir.exists(d)) {
    stop("App spool path is not a directory.", call. = FALSE)
  }
  if (isTRUE(create) && !dir.exists(d)) {
    old_umask <- Sys.umask("0077")
    tryCatch(
      dir.create(d, mode = "0700", showWarnings = FALSE),
      finally = Sys.umask(old_umask)
    )
  }
  if (isTRUE(create) && !dir.exists(d)) {
    stop("Could not create app spool directory.", call. = FALSE)
  }
  if (dir.exists(d)) Sys.chmod(d, "0700")
  d
}

#' @keywords internal
.app_lock_path <- function(root, token) {
  tok <- .validate_app_token(token)
  # A fixed number of lock stripes prevents a stream of otherwise valid tokens
  # from creating an unbounded number of persistent lock files. A collision only
  # serializes two uploads; it cannot merge their spool directories.
  stripe <- substr(digest::digest(tok, algo = "sha256", serialize = FALSE), 1L, 2L)
  file.path(root, paste0(".upload-", stripe, ".lock"))
}

#' @keywords internal
.lock_app_file <- function(path, timeout, message) {
  if (.path_is_symlink(path) || dir.exists(path) ||
      (file.exists(path) && !utils::file_test("-f", path))) {
    stop("App spool lock path is unsafe.", call. = FALSE)
  }
  lock <- tryCatch(filelock::lock(path, timeout = timeout),
                   error = function(e) NULL)
  if (is.null(lock)) stop(message, call. = FALSE)
  Sys.chmod(path, "0600")
  lock
}

#' @keywords internal
.with_app_lock <- function(token, code, timeout = 10000) {
  tok <- .validate_app_token(token)
  root <- .app_spool_root()
  lock <- .lock_app_file(
    .app_lock_path(root, tok), timeout,
    "Timed out waiting for the app upload lock.")
  on.exit(filelock::unlock(lock), add = TRUE)
  force(code)
}

#' @keywords internal
.with_app_store_lock <- function(code) {
  root <- .app_spool_root()
  lock <- .lock_app_file(
    file.path(root, ".store.lock"), 120000,
    "Timed out waiting for the global app-store lock.")
  on.exit(filelock::unlock(lock), add = TRUE)
  force(code)
}

#' @keywords internal
.app_positive_integer_option <- function(name, default) {
  value <- suppressWarnings(as.numeric(.dsf_option(name, default)))
  if (length(value) != 1L || !is.finite(value) || value < 1 ||
      value != floor(value) || value > 2^53) {
    stop("dsflower.", name, " must be a positive finite integer no larger than 2^53.",
         call. = FALSE)
  }
  value
}

#' @keywords internal
.app_spool_policy <- function() {
  list(
    max_bytes = .app_positive_integer_option(
      "app_spool_max_bytes", 1024^3),
    max_uploads = .app_positive_integer_option(
      "app_spool_max_uploads", 128),
    ttl_seconds = .app_positive_integer_option(
      "app_spool_ttl_seconds", 24 * 60 * 60)
  )
}

#' @keywords internal
.app_token_dirs <- function(root = .app_spool_root()) {
  entries <- list.files(root, all.files = TRUE, no.. = TRUE,
                        full.names = TRUE, recursive = FALSE)
  names <- basename(entries)
  selected <- grepl("^(app|usr)_[0-9a-f]{32}$", names)
  dirs <- entries[selected]
  for (path in dirs) {
    if (.path_is_symlink(path) || !dir.exists(path)) {
      stop("Unsafe app spool entry: ", basename(path), call. = FALSE)
    }
  }
  dirs
}

#' @keywords internal
.app_tree_bytes <- function(path) {
  if (.path_is_symlink(path) || !dir.exists(path)) {
    stop("Unsafe app spool directory.", call. = FALSE)
  }
  total <- 0
  queue <- path
  while (length(queue)) {
    current <- queue[[1L]]
    queue <- queue[-1L]
    entries <- list.files(current, all.files = TRUE, no.. = TRUE,
                          full.names = TRUE, recursive = FALSE)
    for (entry in entries) {
      if (.path_is_symlink(entry)) {
        stop("Symbolic links are forbidden in the app spool.", call. = FALSE)
      }
      info <- file.info(entry)
      if (nrow(info) != 1L || is.na(info$isdir[[1]])) {
        stop("Unreadable entry in the app spool.", call. = FALSE)
      }
      if (isTRUE(info$isdir[[1]])) {
        queue <- c(queue, entry)
      } else {
        if (!utils::file_test("-f", entry)) {
          stop("Non-regular files are forbidden in the app spool.", call. = FALSE)
        }
        size <- suppressWarnings(as.numeric(info$size[[1]]))
        if (!is.finite(size) || size < 0) {
          stop("Invalid file size in the app spool.", call. = FALSE)
        }
        total <- total + size
        if (!is.finite(total) || total > 2^53) {
          stop("App spool size cannot be represented safely.", call. = FALSE)
        }
      }
    }
  }
  total
}

#' @keywords internal
.app_spool_usage <- function(root = .app_spool_root()) {
  dirs <- .app_token_dirs(root)
  sizes <- vapply(dirs, .app_tree_bytes, numeric(1))
  list(uploads = length(dirs), bytes = sum(sizes))
}

#' @keywords internal
.touch_app_activity <- function(spool, now = Sys.time()) {
  marker <- file.path(spool, ".last_activity")
  if (.path_is_symlink(marker) || dir.exists(marker) ||
      (file.exists(marker) && !utils::file_test("-f", marker))) {
    stop("App activity marker path is unsafe.", call. = FALSE)
  }
  if (!file.exists(marker)) {
    old_umask <- Sys.umask("0077")
    created <- tryCatch(file.create(marker), finally = Sys.umask(old_umask))
    if (length(created) != 1L || !isTRUE(created)) {
      stop("Could not create the app activity marker.", call. = FALSE)
    }
  }
  Sys.chmod(marker, "0600")
  if (!isTRUE(Sys.setFileTime(marker, now))) {
    stop("Could not update the app activity marker.", call. = FALSE)
  }
  invisible(marker)
}

#' @keywords internal
.app_last_activity <- function(spool) {
  marker <- file.path(spool, ".last_activity")
  if (file.exists(marker)) {
    if (.path_is_symlink(marker) || dir.exists(marker) ||
        !utils::file_test("-f", marker)) {
      stop("App activity marker path is unsafe.", call. = FALSE)
    }
    stamp <- file.info(marker)$mtime[[1]]
    if (!is.na(stamp)) return(stamp)
  }
  stamp <- file.info(spool)$mtime[[1]]
  if (is.na(stamp)) stop("Could not read app spool activity time.", call. = FALSE)
  stamp
}

#' Record that a verified app is referenced by one authoritative run staging dir
#' @keywords internal
.record_app_run_lease <- function(spool, run_token) {
  run_token <- .validate_run_token(run_token)
  leases <- file.path(spool, ".active_runs")
  if (.path_is_symlink(leases) ||
      ((file.exists(leases) || dir.exists(leases)) && !dir.exists(leases))) {
    stop("App run-lease directory is unsafe.", call. = FALSE)
  }
  if (!dir.exists(leases)) {
    old_umask <- Sys.umask("0077")
    tryCatch(
      dir.create(leases, mode = "0700", showWarnings = FALSE),
      finally = Sys.umask(old_umask)
    )
  }
  if (!dir.exists(leases) || .path_is_symlink(leases)) {
    stop("Could not create the app run-lease directory.", call. = FALSE)
  }
  Sys.chmod(leases, "0700")
  lease <- file.path(leases, run_token)
  if (.path_is_symlink(lease) || dir.exists(lease) ||
      (file.exists(lease) && !utils::file_test("-f", lease))) {
    stop("App run-lease path is unsafe.", call. = FALSE)
  }
  if (!file.exists(lease)) {
    old_umask <- Sys.umask("0077")
    created <- tryCatch(file.create(lease), finally = Sys.umask(old_umask))
    if (length(created) != 1L || !isTRUE(created)) {
      stop("Could not create the app run lease.", call. = FALSE)
    }
  }
  Sys.chmod(lease, "0600")
  invisible(lease)
}

# Called with both global and token locks held. A lease is live exactly while its
# server-generated run token still resolves to an extant permitted staging dir.
#' @keywords internal
.active_app_run_leases <- function(spool) {
  leases <- file.path(spool, ".active_runs")
  if (.path_is_symlink(leases)) {
    stop("App run-lease directory is unsafe.", call. = FALSE)
  }
  if (!file.exists(leases) && !dir.exists(leases)) return(character(0))
  if (!dir.exists(leases) || file.access(leases, 4L) != 0L) {
    stop("App run-lease directory is unsafe.", call. = FALSE)
  }
  entries <- tryCatch(
    list.files(leases, all.files = TRUE, no.. = TRUE,
               full.names = TRUE, recursive = FALSE),
    warning = function(w) stop("Could not inspect app run leases.", call. = FALSE),
    error = function(e) stop("Could not inspect app run leases.", call. = FALSE)
  )
  active <- character(0)
  for (lease in entries) {
    run_token <- .validate_run_token(basename(lease))
    if (.path_is_symlink(lease) || dir.exists(lease) ||
        !utils::file_test("-f", lease)) {
      stop("App run-lease path is unsafe.", call. = FALSE)
    }
    candidates <- .expectedStagingDirs(run_token, create_roots = FALSE)
    live <- any(vapply(candidates, function(path) {
      if (!dir.exists(path)) return(FALSE)
      identical(.validateStagingDir(path, run_token, must_exist = TRUE), path)
    }, logical(1)))
    if (isTRUE(live)) {
      active <- c(active, run_token)
    } else {
      unlink(lease, force = TRUE)
    }
  }
  active
}

# Called while the global store lock is held. An app referenced by a live run is
# immutable: otherwise delete/reinstall could race the Python re-hash and swap
# bytes before the isolated child opens its verified package.
#' @keywords internal
.assert_app_not_leased <- function(spool) {
  if (dir.exists(spool) && length(.active_app_run_leases(spool))) {
    stop("App is pinned by an active run and cannot be modified or deleted.",
         call. = FALSE)
  }
  invisible(TRUE)
}

# Called only while the global store lock is held. Token locks are deliberately
# attempted without waiting: a busy token is active and is never collected.
#' @keywords internal
.app_spool_gc_locked <- function(root, ttl_seconds, now = Sys.time(),
                                 exclude_token = character(0)) {
  removed <- character(0)
  skipped <- character(0)
  referenced <- character(0)
  for (spool in .app_token_dirs(root)) {
    token <- basename(spool)
    if (token %in% exclude_token) {
      skipped <- c(skipped, token)
      next
    }
    lock <- tryCatch(
      .lock_app_file(.app_lock_path(root, token), 0, "busy"),
      error = function(e) NULL)
    if (is.null(lock)) {
      skipped <- c(skipped, token)
      next
    }
    tryCatch({
      # Re-check after taking the token lock; a preceding operation may have
      # deleted or refreshed the entry while GC waited for the global lock.
      if (dir.exists(spool) && !.path_is_symlink(spool)) {
        .app_tree_bytes(spool)
        if (length(.active_app_run_leases(spool))) {
          referenced <- c(referenced, token)
          next
        }
        age <- as.numeric(difftime(now, .app_last_activity(spool), units = "secs"))
        if (is.finite(age) && age >= ttl_seconds) {
          unlink(spool, recursive = TRUE, force = TRUE)
          if (dir.exists(spool) || file.exists(spool)) {
            stop("Could not remove an expired app spool.", call. = FALSE)
          }
          removed <- c(removed, token)
        }
      }
    }, finally = filelock::unlock(lock))
  }
  list(removed = removed, skipped_active = skipped,
       skipped_referenced = referenced)
}

#' Collect expired, inactive app uploads
#' @keywords internal
.app_spool_gc <- function(now = Sys.time()) {
  policy <- .app_spool_policy()
  .with_app_store_lock({
    root <- .app_spool_root()
    .app_spool_gc_locked(root, policy$ttl_seconds, now)
  })
}

# Called with the global store lock held.
#' @keywords internal
.assert_app_spool_quota <- function(root, policy, additional_bytes = 0,
                                    new_upload = FALSE) {
  usage <- .app_spool_usage(root)
  if (usage$uploads + as.integer(isTRUE(new_upload)) > policy$max_uploads) {
    stop("App upload store reached dsflower.app_spool_max_uploads (",
         format(policy$max_uploads, scientific = FALSE), ").", call. = FALSE)
  }
  if (usage$bytes + additional_bytes > policy$max_bytes) {
    stop("App upload store exceeds dsflower.app_spool_max_bytes (",
         format(policy$max_bytes, scientific = FALSE), " bytes).", call. = FALSE)
  }
  invisible(usage)
}

#' @keywords internal
.max_fab_bytes <- function() {
  .app_positive_integer_option("max_fab_bytes", 50 * 1024 * 1024)
}

#' @keywords internal
.app_b64_dec <- function(s, max_bytes = NULL) {
  if (!is.character(s) || length(s) != 1 || !nzchar(s) || !startsWith(s, "B64:")) {
    return(raw(0))
  }
  b64 <- substring(s, first = 5L, last = nchar(s, type = "chars"))
  if (!grepl("^[A-Za-z0-9_-]+={0,2}$", b64)) {
    stop("Invalid base64 app chunk.", call. = FALSE)
  }
  if (!is.null(max_bytes) &&
      nchar(b64, type = "bytes") > ceiling(as.numeric(max_bytes) * 4 / 3) + 4) {
    stop("App chunk exceeds dsflower.max_fab_bytes.", call. = FALSE)
  }
  b64 <- gsub("-", "+", b64); b64 <- gsub("_", "/", b64)
  pad <- (4 - nchar(b64) %% 4) %% 4
  if (pad > 0) b64 <- paste0(b64, strrep("=", pad))
  decoded <- tryCatch(jsonlite::base64_dec(b64), error = function(e) NULL)
  if (is.null(decoded)) stop("Invalid base64 app chunk.", call. = FALSE)
  if (!is.null(max_bytes) && length(decoded) > as.numeric(max_bytes)) {
    stop("App chunk exceeds dsflower.max_fab_bytes.", call. = FALSE)
  }
  decoded
}

#' Push a chunk of an uploaded FAB (DataSHIELD AGGREGATE)
#'
#' Idempotent at \code{offset}: a new chunk must start exactly at EOF. A replay is
#' acknowledged only when its offset, length, and content are byte-identical to
#' the one complete chunk already stored, so a lost ACK cannot silently change
#' message geometry or content.
#' @param token Character; upload token (per-run).
#' @param chunk_b64 Character; \code{B64:} url-safe payload, or "".
#' @param offset Numeric; the size the client believes the node file has.
#' @return A fixed acknowledgement containing \code{ok}, \code{offset},
#'   \code{size}, \code{bytes}, and the chunk \code{sha256}; failed geometry also
#'   contains a public \code{error} code.
#' @keywords internal
#' @export
flowerAppPushDS <- function(token, chunk_b64 = "", offset = NULL) {
  token <- .validate_app_token(.ds_arg(token))
  .with_app_lock(token, {
    cap <- .max_fab_bytes()
    # chunk_b64 is our own binary-safe "B64:" payload (raw FAB bytes); it must NOT
    # go through .ds_arg, which rawToChar()s decoded bytes and would corrupt binary.
    raw <- .app_b64_dec(chunk_b64, max_bytes = cap)
    policy <- .app_spool_policy()
    .with_app_store_lock({
      root <- .app_spool_root()
      .app_spool_gc_locked(root, policy$ttl_seconds,
                           exclude_token = token)
      spool <- .app_spool_dir(token, create = FALSE)
      is_new <- !dir.exists(spool)
      if (!is_new) .assert_app_not_leased(spool)
      bin <- file.path(spool, "app.fab")
      if (.path_is_symlink(bin) || dir.exists(bin) ||
          (file.exists(bin) && !utils::file_test("-f", bin))) {
        stop("Uploaded app path is unsafe.", call. = FALSE)
      }
      sz <- if (file.exists(bin)) file.size(bin) else 0
      if (!is.finite(sz) || sz > cap) {
        if (dir.exists(spool)) unlink(spool, recursive = TRUE)
        stop("Uploaded app exceeds dsflower.max_fab_bytes (", cap, " bytes).",
             call. = FALSE)
      }
      off <- suppressWarnings(as.numeric(offset))
      if (length(off) != 1L || !is.finite(off) || off < 0 || off != floor(off)) {
        stop("Invalid app upload offset.", call. = FALSE)
      }
      if (length(raw) < 1L || off + length(raw) > 2^53) {
        stop("Invalid app upload chunk geometry.", call. = FALSE)
      }
      chunk_hash <- digest::digest(raw, algo = "sha256", serialize = FALSE)
      ack <- function(ok, error = NULL) {
        value <- list(
          ok = isTRUE(ok), offset = off, size = sz,
          bytes = length(raw), sha256 = chunk_hash
        )
        if (!is.null(error)) value$error <- error
        value
      }
      if (off > sz) {
        if (!is_new) .touch_app_activity(spool)
        ack(FALSE, "gap")
      } else {
        replay <- off < sz
        if (replay && off + length(raw) != sz) {
          if (!is_new) .touch_app_activity(spool)
          return(ack(FALSE, "conflict"))
        }
        if (replay) {
          con <- file(bin, "rb")
          existing <- tryCatch({
            seek(con, where = off, origin = "start")
            readBin(con, "raw", n = length(raw))
          }, finally = close(con))
          if (!identical(existing, raw)) {
            .touch_app_activity(spool)
            return(ack(FALSE, "conflict"))
          }
        }
        append_n <- if (replay) 0 else length(raw)
        if (sz + append_n > cap) {
          if (dir.exists(spool)) unlink(spool, recursive = TRUE)
          stop("Uploaded app exceeds dsflower.max_fab_bytes (", cap, " bytes).",
               call. = FALSE)
        }
        .assert_app_spool_quota(
          root, policy, additional_bytes = append_n, new_upload = is_new)
        if (is_new) spool <- .app_spool_dir(token, create = TRUE)
        if (append_n > 0) {
          con <- file(bin, "ab")
          tryCatch(
            writeBin(raw, con),
            finally = close(con)
          )
          Sys.chmod(bin, "0600")
        }
        .touch_app_activity(spool)
        new_size <- if (file.exists(bin)) file.size(bin) else 0
        if (!is.finite(new_size) || new_size > cap) {
          unlink(spool, recursive = TRUE)
          stop("Uploaded app exceeds dsflower.max_fab_bytes (", cap, " bytes).",
               call. = FALSE)
        }
        .assert_app_spool_quota(root, policy)
        sz <- new_size
        ack(TRUE)
      }
    })
  })
}

#' Hash a Python package directory for integrity pinning
#'
#' Matches \code{sitecustomize.py} and \code{.compute_harness_hash}: radix-sorted
#' relative paths followed by a newline byte, file content and a NUL byte, with
#' compiled artifacts excluded.
#' @keywords internal
.hash_pkg_dir <- function(pkg_dir) {
  rel_files <- list.files(pkg_dir, recursive = TRUE, full.names = FALSE,
                          all.files = TRUE, no.. = TRUE)
  rel_files <- rel_files[!grepl("(^|/)__pycache__(/|$)", rel_files)]
  rel_files <- rel_files[!grepl("\\.(pyc|pyo)$", rel_files)]
  rel_files <- sort(rel_files, method = "radix")
  blob <- raw(0)
  for (rel in rel_files) {
    full <- file.path(pkg_dir, rel)
    content <- readBin(full, "raw", file.info(full)$size)
    blob <- c(blob, charToRaw(rel), charToRaw("\n"), content, as.raw(0x00))
  }
  digest::digest(blob, algo = "sha256", serialize = FALSE)
}

#' Node-computed hashes of each top-level Python package in an unpacked app.
#' These are the trust anchors the integrity hook pins (the node computes them
#' itself, so a client cannot forge what is allowed to run).
#' @keywords internal
.compute_pkg_hashes <- function(apps_dir) {
  subdirs <- list.dirs(apps_dir, recursive = FALSE, full.names = TRUE)
  is_pkg <- vapply(subdirs, function(d) {
    init <- file.path(d, "__init__.py")
    file.exists(init) && !dir.exists(init) && !nzchar(Sys.readlink(init))
  }, logical(1))
  pkgs <- subdirs[is_pkg]
  hashes <- list()
  for (p in pkgs) hashes[[basename(p)]] <- .hash_pkg_dir(p)
  hashes
}

#' Resolve a python (with stdlib ast) for the exfiltration scan.
#' @keywords internal
.scan_python <- function() {
  cands <- c(Sys.which("python3"), Sys.which("python"),
             Sys.glob("/srv/dsflower/venvs/*/bin/python"),
             Sys.glob(file.path(tools::R_user_dir("dsFlower", "data"),
                                "venvs", "*", "bin", "python")))
  cands <- cands[nzchar(cands) & file.exists(cands)]
  if (length(cands)) cands[1] else ""
}

#' Run the HookApp exfiltration scan on an unpacked app (fail-closed).
#' @keywords internal
.run_exfil_scan <- function(app_dir) {
  scanner <- system.file("python", "exfil_scan.py", package = "dsFlower")
  if (!nzchar(scanner) || !file.exists(scanner)) {
    return(list(ok = FALSE, first = "scanner not installed"))
  }
  py <- .scan_python()
  if (!nzchar(py)) return(list(ok = FALSE, first = "no python for scan"))
  res <- tryCatch(
    processx::run(py, c(scanner, app_dir), error_on_status = FALSE),
    error = function(e) NULL)
  if (is.null(res)) return(list(ok = FALSE, first = "scan failed to run"))
  out <- tryCatch(jsonlite::fromJSON(res$stdout, simplifyVector = FALSE),
                  error = function(e) NULL)
  if (is.null(out) || is.null(out$ok)) {
    return(list(ok = FALSE, first = "scan output unparseable"))
  }
  first <- ""
  if (length(out$violations)) {
    v <- out$violations[[1]]
    first <- paste0(v$rule, ":", v$detail, " @ ", v$file, ":", v$line)
  }
  list(ok = isTRUE(out$ok), violations = out$violations, first = first)
}

#' Extract a FAB after rejecting unsafe ZIP members (fail-closed).
#' @keywords internal
.safe_extract_fab <- function(archive, destination, max_bytes) {
  extractor <- system.file("python", "safe_zip_extract.py", package = "dsFlower")
  if (!nzchar(extractor) || !file.exists(extractor)) {
    return(list(ok = FALSE, first = "safe ZIP extractor not installed"))
  }
  py <- .scan_python()
  if (!nzchar(py)) return(list(ok = FALSE, first = "no python for ZIP extraction"))
  res <- tryCatch(
    processx::run(
      py,
      c(extractor, "--archive", archive, "--destination", destination,
        "--max-bytes", format(max_bytes, scientific = FALSE, trim = TRUE)),
      error_on_status = FALSE,
      timeout = 60
    ),
    error = function(e) NULL
  )
  if (is.null(res)) return(list(ok = FALSE, first = "safe ZIP extraction failed"))
  out <- tryCatch(jsonlite::fromJSON(res$stdout, simplifyVector = TRUE),
                  error = function(e) NULL)
  if (is.null(out) || !isTRUE(out$ok) || res$status != 0L) {
    first <- if (!is.null(out$error) && nzchar(out$error)) out$error
             else "safe ZIP extraction failed"
    return(list(ok = FALSE, first = first))
  }
  list(ok = TRUE, first = "")
}

#' Verify + unpack an uploaded FAB (DataSHIELD AGGREGATE)
#'
#' Enforces the size cap (\code{dsflower.max_fab_bytes}) and sha256 integrity
#' before unpacking. On any failure the spool is destroyed so no unverified code
#' can run. Returns only public installation metadata; the node keeps the unpack
#' path private and executes only the app registered by the validated hash.
#' @param token Character; upload token.
#' @param expected_sha256 Character; sha256 the client computed over the FAB.
#' @return list(ok, sha256, size, packages).
#' @keywords internal
#' @export
flowerAppInstallDS <- function(token, expected_sha256) {
  token <- .validate_app_token(.ds_arg(token))
  .with_app_lock(token, {
    cap <- .max_fab_bytes()
    policy <- .app_spool_policy()
    .with_app_store_lock({
      root <- .app_spool_root()
      spool <- .app_spool_dir(token, create = FALSE)
      .assert_app_not_leased(spool)
      bin <- file.path(spool, "app.fab")
      if (.path_is_symlink(bin) || dir.exists(bin) ||
          (file.exists(bin) && !utils::file_test("-f", bin))) {
        stop("Uploaded app path is unsafe.", call. = FALSE)
      }
      if (!file.exists(bin)) {
        stop("No uploaded app for this token; push the FAB first.", call. = FALSE)
      }
      size <- file.size(bin)
      if (!is.finite(size) || size > cap) {
        unlink(spool, recursive = TRUE)
        stop("Uploaded app exceeds dsflower.max_fab_bytes (", cap, " bytes).",
             call. = FALSE)
      }
      .assert_app_spool_quota(root, policy)
      actual <- digest::digest(file = bin, algo = "sha256")
      expected <- as.character(.ds_arg(expected_sha256))
      if (!identical(actual, expected)) {
        unlink(spool, recursive = TRUE)
        stop("Uploaded app failed integrity check (sha256 mismatch); rejected.",
             call. = FALSE)
      }

      apps_dir <- file.path(spool, "unpacked")
      candidate <- tempfile(pattern = ".unpacked-", tmpdir = spool)
      on.exit(unlink(candidate, recursive = TRUE), add = TRUE)
      usage <- .app_spool_usage(root)
      remaining <- floor(policy$max_bytes - usage$bytes)
      if (!is.finite(remaining) || remaining < 1) {
        stop("App upload store exceeds dsflower.app_spool_max_bytes (",
             format(policy$max_bytes, scientific = FALSE), " bytes).",
             call. = FALSE)
      }
      extract_cap <- min(cap, remaining)
      extracted <- .safe_extract_fab(bin, candidate, extract_cap)
      if (!isTRUE(extracted$ok)) {
        if (extract_cap < cap &&
            grepl("unpacked archive exceeds size limit", extracted$first,
                  fixed = TRUE)) {
          stop("App upload store exceeds dsflower.app_spool_max_bytes (",
               format(policy$max_bytes, scientific = FALSE), " bytes).",
               call. = FALSE)
        }
        unlink(spool, recursive = TRUE)
        stop("Uploaded app is an unsafe FAB archive (", extracted$first,
             "); rejected.", call. = FALSE)
      }
      .assert_app_spool_quota(root, policy)

      # Tier-2 exfiltration scan gates install (fail-closed): an app that imports
      # network/process-escape modules or uses dynamic code is rejected before any
      # data is touched. Defence-in-depth ahead of the sandbox + egress gate.
      scan <- .run_exfil_scan(candidate)
      if (!isTRUE(scan$ok)) {
        unlink(spool, recursive = TRUE)
        stop("Uploaded app failed the Tier-2 safety scan (", scan$first,
             "); rejected.", call. = FALSE)
      }
      # Node-computed per-package hashes -> the integrity hook's trust anchors for a
      # Tier-2 run (so the client cannot dictate what is allowed to run).
      pkg_hashes <- .compute_pkg_hashes(candidate)
      if (dir.exists(apps_dir)) unlink(apps_dir, recursive = TRUE)
      if (!file.rename(candidate, apps_dir)) {
        unlink(spool, recursive = TRUE)
        stop("Could not atomically install the uploaded app; rejected.", call. = FALSE)
      }
      .touch_app_activity(spool)
      .assert_app_spool_quota(root, policy)
      list(ok = TRUE, sha256 = actual, size = size, packages = pkg_hashes)
    })
  })
}

#' Hash a node-resident trusted app package (for example, the canonical runner).
#' @keywords internal
.compute_app_pkg_hash <- function(pkg_name) {
  pkg_dir <- system.file("flower_app", pkg_name, package = "dsFlower")
  if (!nzchar(pkg_dir) || !dir.exists(pkg_dir)) return("")
  .hash_pkg_dir(pkg_dir)
}

#' Pin a HookApp run: trusted runner + verified upload (DataSHIELD AGGREGATE)
#'
#' Writes \code{pinned_packages.json} into the run's staging dir so the integrity
#' hook (multi-package, default-deny) allows exactly the node-resident
#' canonical \code{dsflower_runner} AND the uploaded user app — both pinned to
#' \emph{node-computed} hashes, so a client cannot dictate what runs. The upload
#' must contain exactly one regular top-level Python package; its package name is
#' pinned authoritatively in the server-owned run manifest. The uploaded app must
#' already be pushed + installed (sha256-verified + exfiltration-scanned).
#' @param handle_symbol Character; the prepared run handle.
#' @param app_token Character; the uploaded app's token.
#' @return list(ok, pinned, user_module).
#' @keywords internal
#' @export
flowerTier2PinDS <- function(handle_symbol, app_token) {
  handle <- .validateHandleStaging(.getHandle(handle_symbol), required = TRUE)
  app_token <- .validate_app_token(.ds_arg(app_token))
  .with_app_lock(app_token, {
    spool <- .app_spool_dir(app_token, create = FALSE)
    apps_dir <- file.path(spool, "unpacked")
    if (!dir.exists(apps_dir)) {
      stop("No installed app for that token; push + install the app first.",
           call. = FALSE)
    }
    user_hashes <- .compute_pkg_hashes(apps_dir)
    if (length(user_hashes) != 1L) {
      stop("Uploaded app must contain exactly one top-level Python package ",
           "with a regular __init__.py; found ", length(user_hashes), ".",
           call. = FALSE)
    }
    user_module <- names(user_hashes)[[1]]
    if (identical(user_module, "dsflower_runner")) {
      stop("Uploaded app package name 'dsflower_runner' is reserved.",
           call. = FALSE)
    }
    # Declarative DPTrainingApps and isolated HookApps share one canonical runner;
    # the uploaded package is never imported in that trusted parent process.
    runner_hash <- .compute_app_pkg_hash("dsflower_runner")
    if (!nzchar(runner_hash)) {
      stop("The canonical runner (dsflower_runner) is not installed on this node.",
           call. = FALSE)
    }
    pinned <- c(list(dsflower_runner = runner_hash), user_hashes)
    # Establish the GC lease before staging starts referring to this app. If a
    # later atomic staging write fails, cleanup of that staging dir also makes
    # the lease stale; GC never sees an unleased, partially pinned active run.
    .with_app_store_lock({
      .record_app_run_lease(spool, handle$run_token)
      .touch_app_activity(spool)
    })
    pins_path <- file.path(handle$staging_dir, "pinned_packages.json")
    pins_tmp <- tempfile(pattern = ".pinned-packages-",
                         tmpdir = handle$staging_dir)
    on.exit(unlink(pins_tmp), add = TRUE)
    jsonlite::write_json(pinned, pins_tmp, auto_unbox = TRUE)
    Sys.chmod(pins_tmp, "0600")
    if (!file.rename(pins_tmp, pins_path)) {
      stop("Could not atomically write the package pin map.", call. = FALSE)
    }

    manifest_path <- file.path(handle$staging_dir, "manifest.json")
    manifest <- tryCatch(
      jsonlite::fromJSON(manifest_path, simplifyVector = FALSE),
      error = function(e) stop("Prepared run manifest is unreadable: ",
                               conditionMessage(e), call. = FALSE))
    manifest[["user-module"]] <- user_module
    .write_manifest_atomic(manifest, manifest_path)
    writeLines(apps_dir, file.path(handle$staging_dir, "tier2_pythonpath.txt"))
    list(ok = TRUE, pinned = names(pinned), user_module = user_module)
  })
}

#' Remove an uploaded app's spool (DataSHIELD AGGREGATE)
#' @param token Character; upload token.
#' @return TRUE.
#' @keywords internal
#' @export
flowerAppDeleteDS <- function(token) {
  token <- .validate_app_token(.ds_arg(token))
  .with_app_lock(token, {
    .with_app_store_lock({
      spool <- .app_spool_dir(token, create = FALSE)
      .assert_app_not_leased(spool)
      unlink(spool, recursive = TRUE, force = TRUE)
      if (dir.exists(spool) || file.exists(spool)) {
        stop("Could not remove the app spool.", call. = FALSE)
      }
      TRUE
    })
  })
}
