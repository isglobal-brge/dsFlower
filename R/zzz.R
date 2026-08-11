# Module: Package Hooks + Environments
# Package load/detach hooks and internal environments for dsFlower.

# Null-coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Session-level transport state
.dsflower_env <- new.env(parent = emptyenv())

# Authoritative Flower-handle state. Session workspaces receive only an opaque
# capability; sensitive paths, data and lifecycle flags stay in this registry.
.handle_registry <- new.env(parent = emptyenv())

# SuperNode singleton registry -- keyed by SuperLink address
.supernode_registry <- new.env(parent = emptyenv())

.encode_windows_powershell_command <- function(script) {
  if (!is.character(script) || length(script) != 1L || is.na(script) ||
      !nzchar(script)) {
    stop("The Windows privacy bootstrap command is invalid.", call. = FALSE)
  }
  utf16 <- iconv(script, from = "UTF-8", to = "UTF-16LE", toRaw = TRUE)[[1L]]
  if (is.null(utf16)) {
    stop("Could not encode the Windows privacy bootstrap command.",
         call. = FALSE)
  }
  # jsonlite wraps base64 output at 76 columns. PowerShell's -EncodedCommand
  # requires one argument, so line breaks must not reach system2().
  encoded <- gsub("[\r\n]", "", jsonlite::base64_enc(utf16), perl = TRUE)
  if (!grepl("^[A-Za-z0-9+/]+={0,2}$", encoded, perl = TRUE)) {
    stop("Could not encode the Windows privacy bootstrap command.",
         call. = FALSE)
  }
  encoded
}

.run_windows_powershell <- function(script) {
  system_root <- Sys.getenv("SystemRoot", unset = "")
  program_files <- Sys.getenv("ProgramFiles", unset = "")
  candidates <- c(
    if (nzchar(system_root)) file.path(
      system_root, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
    if (nzchar(program_files)) file.path(
      program_files, "PowerShell", "7", "pwsh.exe")
  )
  candidates <- candidates[file.exists(candidates)]
  if (!length(candidates)) {
    stop("Windows PowerShell is unavailable; refusing to manage a DP node secret.",
         call. = FALSE)
  }
  script <- paste0("$ErrorActionPreference='Stop';", script)
  encoded <- .encode_windows_powershell_command(script)
  output <- suppressWarnings(system2(
    candidates[[1L]],
    c("-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded),
    stdout = TRUE, stderr = FALSE
  ))
  status <- attr(output, "status", exact = TRUE)
  if (!is.null(status) && !identical(as.integer(status), 0L)) {
    stop("The Windows privacy bootstrap command failed closed.", call. = FALSE)
  }
  output
}

.powershell_literal <- function(value) {
  paste0("'", gsub("'", "''", enc2utf8(value), fixed = TRUE), "'")
}

.read_windows_os_entropy <- function(n, runner = .run_windows_powershell) {
  script <- paste0(
    "$b=New-Object byte[] ", as.integer(n), ";",
    "$r=[Security.Cryptography.RandomNumberGenerator]::Create();",
    "try{$r.GetBytes($b);",
    "Write-Output (([BitConverter]::ToString($b)).Replace('-',''))}",
    "finally{$r.Dispose()}"
  )
  encoded <- paste(runner(script), collapse = "")
  if (!grepl(paste0("^[0-9A-Fa-f]{", 2L * n, "}$"), encoded, perl = TRUE)) {
    stop("Windows did not return the requested operating-system entropy.",
         call. = FALSE)
  }
  pairs <- substring(encoded, seq.int(1L, nchar(encoded), by = 2L),
                     seq.int(2L, nchar(encoded), by = 2L))
  as.raw(strtoi(pairs, base = 16L))
}

.read_os_entropy <- function(n) {
  value <- suppressWarnings(as.numeric(n))
  if (length(value) != 1L || is.na(value) || !is.finite(value) || value < 1 ||
      value != floor(value) || value > .Machine$integer.max) {
    stop("The operating-system entropy size must be a positive integer.",
         call. = FALSE)
  }
  n <- as.integer(value)
  if (.Platform$OS.type == "windows") {
    return(.read_windows_os_entropy(n))
  }
  if (.Platform$OS.type != "unix") {
    stop("This platform has no supported operating-system entropy source.",
         call. = FALSE)
  }
  con <- file("/dev/urandom", open = "rb", raw = TRUE)
  on.exit(close(con), add = TRUE)
  readBin(con, "raw", n = n)
}

.windows_path_has_reparse_point <- function(
    path, runner = .run_windows_powershell) {
  if (!file.exists(path) && !dir.exists(path)) return(FALSE)
  literal <- .powershell_literal(path)
  script <- paste0(
    "$i=Get-Item -LiteralPath ", literal, " -Force -ErrorAction Stop;",
    "$found=$false;while($null -ne $i){",
    "if(($i.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)",
    "{$found=$true;break};$i=$i.Parent};",
    "if($found){Write-Output '1'}else{Write-Output '0'}"
  )
  result <- paste(runner(script), collapse = "")
  if (!result %in% c("0", "1")) {
    stop("Could not validate the Windows node-secret path.", call. = FALSE)
  }
  identical(result, "1")
}

.privacy_path_is_link <- function(path) {
  if (.Platform$OS.type == "windows") {
    return(.windows_path_has_reparse_point(path))
  }
  .path_is_symlink(path)
}

.windows_set_private_acl <- function(
    path, is_directory, runner = .run_windows_powershell) {
  literal <- .powershell_literal(path)
  inheritance <- if (isTRUE(is_directory)) {
    paste0(
      "([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor ",
      "[Security.AccessControl.InheritanceFlags]::ObjectInherit)"
    )
  } else {
    "[Security.AccessControl.InheritanceFlags]::None"
  }
  script <- paste0(
    "$p=", literal, ";$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User;",
    "$acl=Get-Acl -LiteralPath $p -ErrorAction Stop;",
    "$acl.SetAccessRuleProtection($true,$false);",
    "$rules=@($acl.GetAccessRules($true,$false,",
    "[Security.Principal.SecurityIdentifier]));foreach($existing in $rules){",
    "[void]$acl.RemoveAccessRuleSpecific($existing)};",
    "$acl.SetOwner($sid);",
    "$rule=[Security.AccessControl.FileSystemAccessRule]::new($sid,",
    "[Security.AccessControl.FileSystemRights]::FullControl,", inheritance, ",",
    "[Security.AccessControl.PropagationFlags]::None,",
    "[Security.AccessControl.AccessControlType]::Allow);",
    "$acl.SetAccessRule($rule);Set-Acl -LiteralPath $p -AclObject $acl;",
    "Write-Output 'OK'"
  )
  result <- paste(runner(script), collapse = "")
  if (!identical(result, "OK")) {
    stop("Could not protect the Windows node-secret path.", call. = FALSE)
  }
  .windows_validate_private_acl(path, runner = runner)
  invisible(path)
}

.windows_validate_private_acl <- function(
    path, runner = .run_windows_powershell) {
  literal <- .powershell_literal(path)
  script <- paste0(
    "$p=", literal, ";$acl=Get-Acl -LiteralPath $p -ErrorAction Stop;",
    "$me=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value;",
    "$trusted=@($me,'S-1-5-18','S-1-5-32-544');",
    "$owner=$acl.GetOwner([Security.Principal.SecurityIdentifier]).Value;",
    "$ok=$acl.AreAccessRulesProtected -and ($trusted -contains $owner);$mine=$false;",
    "$rules=$acl.GetAccessRules($true,$true,",
    "[Security.Principal.SecurityIdentifier]);foreach($r in $rules){",
    "if($r.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow){",
    "$sid=$r.IdentityReference.Value;if($trusted -notcontains $sid){$ok=$false};",
    "if($sid -eq $me -and (($r.FileSystemRights -band ",
    "[Security.AccessControl.FileSystemRights]::FullControl) -eq ",
    "[Security.AccessControl.FileSystemRights]::FullControl)){$mine=$true}}};",
    "if($ok -and $mine){Write-Output 'OK'}",
    "else{Write-Output 'UNSAFE'}"
  )
  result <- paste(runner(script), collapse = "")
  if (!identical(result, "OK")) {
    stop("The Windows node-secret ACL is not private to the service identity.",
         call. = FALSE)
  }
  invisible(path)
}

.windows_replace_file_atomic <- function(
    replacement, destination, runner = .run_windows_powershell) {
  # Windows PowerShell 5's File.Replace wrapper adds an unsupported
  # REPLACEFILE_WRITE_THROUGH flag. Call ReplaceFileW with only its documented
  # ignore-merge flag so the built-in shell remains usable.
  script <- paste0(
    "$stage='PATH';",
    "$s=[IO.Path]::GetFullPath(", .powershell_literal(replacement), ");",
    "$d=[IO.Path]::GetFullPath(", .powershell_literal(destination), ");",
    "$comparison=[StringComparison]::OrdinalIgnoreCase;",
    "if(-not [String]::Equals([IO.Path]::GetDirectoryName($s),",
    "[IO.Path]::GetDirectoryName($d),$comparison))",
    "{throw 'Atomic replacement requires one directory'};",
    "$interop='using System;using System.Runtime.InteropServices;",
    "namespace DsFlower{public static class NativeFile{",
    "[DllImport(\"kernel32.dll\",EntryPoint=\"ReplaceFileW\",",
    "CharSet=CharSet.Unicode,ExactSpelling=true,SetLastError=true)]",
    "[return:MarshalAs(UnmanagedType.Bool)]public static extern bool ReplaceFileW(",
    "string replaced,string replacement,string backup,uint flags,",
    "IntPtr exclude,IntPtr reserved);}}';",
    "$stage='ADD_TYPE';",
    "Add-Type -TypeDefinition $interop -ErrorAction Stop;",
    "$stage='NATIVE';",
    "$ok=[DsFlower.NativeFile]::ReplaceFileW($d,$s,$null,[uint32]2,",
    "[IntPtr]::Zero,[IntPtr]::Zero);",
    "if(-not $ok){$code=[Runtime.InteropServices.Marshal]::GetLastWin32Error();",
    "throw ('ReplaceFileW failed with Win32 error '+$code)};",
    "Write-Output 'OK'"
  )
  result <- paste(runner(script), collapse = "")
  if (!identical(result, "OK")) {
    stop("Could not atomically install the Windows node secret.", call. = FALSE)
  }
  .windows_set_private_acl(destination, is_directory = FALSE, runner = runner)
  invisible(destination)
}

# Default the sticky secret beside the platform-specific runtime root. Services
# can select a different absolute path through DSFLOWER_NODE_SECRET_FILE.
.default_node_secret_path <- function(os_type = .Platform$OS.type) {
  file.path(dirname(.default_venv_root(os_type)), "privacy", "noise_root")
}

#' Resolve the dedicated dsFlower node-secret path
#' @keywords internal
.node_secret_path <- function() {
  configured <- .dsf_option(
    "node_secret_path", .default_node_secret_path())
  from_env <- Sys.getenv("DSFLOWER_NODE_SECRET_FILE", unset = "")
  # Process-level configuration is authoritative and is also what the runtime
  # wrapper can see before a DataSHIELD session injects profile R options.
  # Selecting it consistently avoids blocking a safe key rotation merely
  # because a stale profile option names another path.
  path <- if (nzchar(from_env)) from_env else configured
  if (length(path) != 1L || is.na(path) || !nzchar(as.character(path)) ||
      !.path_is_absolute(as.character(path))) {
    stop("The dsFlower node-secret path must be absolute.", call. = FALSE)
  }
  path <- as.character(path)
  if (.privacy_path_is_link(path)) {
    stop("The dsFlower node secret must not be a link or reparse point.",
         call. = FALSE)
  }
  path <- .canonical_state_path(path)
  allow_test_tmp <- identical(
    Sys.getenv("DSFLOWER_TEST_ALLOW_EPHEMERAL_SECRET", ""), "1")
  if (.privacy_path_is_ephemeral(path) && !allow_test_tmp) {
    stop("The dsFlower node secret must be persistent; /tmp, /var/tmp and ",
         "/dev/shm are not allowed.", call. = FALSE)
  }
  path
}

#' Validate the parent directory of a dsFlower node secret
#' @keywords internal
.validate_node_secret_parent <- function(path, euid = NULL) {
  parent <- dirname(path)
  if (.privacy_path_is_link(parent)) {
    stop("The dsFlower node-secret parent must be a real directory.",
         call. = FALSE)
  }
  info <- file.info(parent)
  if (nrow(info) != 1L || is.na(info$isdir[[1]]) || !isTRUE(info$isdir[[1]])) {
    stop("The dsFlower node-secret parent must be a real directory.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    if (is.null(euid)) euid <- .privacy_effective_uid()
    owner <- suppressWarnings(as.integer(info$uid[[1]]))
    if (is.na(owner) || !owner %in% c(as.integer(euid), 0L)) {
      stop("The dsFlower node-secret parent must be owned by the node EUID or root.",
           call. = FALSE)
    }
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    unsafe_write <- as.integer(strtoi("22", base = 8))
    if (is.na(mode) || bitwAnd(mode, unsafe_write) != 0L) {
      stop("The dsFlower node-secret parent must not be writable by group or other users.",
           call. = FALSE)
    }
  } else if (.Platform$OS.type == "windows") {
    .windows_validate_private_acl(parent)
  }
  normalizePath(parent, winslash = "/", mustWork = TRUE)
}

.read_node_secret_bytes <- function(path) {
  con <- tryCatch(
    file(path, open = "rb"),
    warning = function(w) {
      stop("The dsFlower node secret must be a readable regular file.",
           call. = FALSE)
    },
    error = function(e) {
      stop("The dsFlower node secret must be a readable regular file.",
           call. = FALSE)
    }
  )
  on.exit(close(con), add = TRUE)
  tryCatch(readBin(con, "raw", n = 67L), error = function(e) raw(0))
}

#' Validate a dedicated 256-bit dsFlower node secret
#' @keywords internal
.validate_node_secret <- function(path) {
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NULL
  parent_before <- .validate_node_secret_parent(path, euid)
  if (!file.exists(path) || .privacy_path_is_link(path)) {
    stop("The dsFlower node secret is missing or is a link/reparse point: ", path,
         call. = FALSE)
  }
  info <- file.info(path)
  if (nrow(info) != 1L || is.na(info$isdir[[1]]) ||
      isTRUE(info$isdir[[1]]) || !.path_is_regular_file(path)) {
    stop("The dsFlower node secret must be a regular file.", call. = FALSE)
  }
  if (.Platform$OS.type == "unix") {
    mode <- suppressWarnings(as.integer(info$mode[[1]]))
    expected_mode <- as.integer(strtoi("600", base = 8))
    if (is.na(mode) || !identical(mode, expected_mode)) {
      stop("The dsFlower node secret must have Unix mode exactly 0600.",
           call. = FALSE)
    }
    owner <- suppressWarnings(as.integer(info$uid[[1]]))
    if (is.na(owner) || !identical(owner, as.integer(euid))) {
      stop("The dsFlower node secret is not owned by the current service user.",
           call. = FALSE)
    }
  } else if (.Platform$OS.type == "windows") {
    .windows_validate_private_acl(path)
  }
  # Accept 64 hex bytes with no terminator, LF, or CRLF. Read one byte beyond
  # the largest valid representation so a valid first line plus hidden trailing
  # content cannot pass validation.
  bytes <- .read_node_secret_bytes(path)
  if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0a))) {
    bytes <- bytes[-length(bytes)]
    if (length(bytes) && identical(bytes[[length(bytes)]], as.raw(0x0d))) {
      bytes <- bytes[-length(bytes)]
    }
  }
  value <- tryCatch(rawToChar(bytes), error = function(e) "")
  if (length(bytes) != 64L ||
      !grepl("^[0-9a-fA-F]{64}$", value, perl = TRUE)) {
    stop("The dsFlower node secret must contain exactly 32 bytes as 64 hex digits.",
         call. = FALSE)
  }
  parent_after <- .validate_node_secret_parent(path, euid)
  if (!identical(parent_after, parent_before)) {
    stop("The dsFlower node-secret parent changed while validating the key.",
         call. = FALSE)
  }
  invisible(path)
}

#' Atomically write a fresh dsFlower node secret
#' @keywords internal
.write_node_secret_atomic <- function(path, parent) {
  entropy <- tryCatch(.read_os_entropy(32L), error = function(e) raw(0))
  if (length(entropy) != 32L) {
    stop("Could not read 32 bytes of operating-system entropy; refusing a DP release.",
         call. = FALSE)
  }
  value <- paste(sprintf("%02x", as.integer(entropy)), collapse = "")
  tmp <- tempfile(pattern = ".node-secret-", tmpdir = parent)
  on.exit(unlink(tmp), add = TRUE)
  # file.create() has no mode formal. A restrictive umask prevents a readable
  # window before chmod; rename then replaces an invalid regular key atomically.
  old_umask <- if (.Platform$OS.type == "unix") Sys.umask("0077") else NULL
  created <- tryCatch(
    file.create(tmp),
    finally = if (!is.null(old_umask)) Sys.umask(old_umask)
  )
  if (length(created) != 1L || !isTRUE(created)) {
    stop("Could not create a private temporary node-secret file.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "windows") {
    .windows_set_private_acl(tmp, is_directory = FALSE)
  }
  writeLines(value, tmp, useBytes = TRUE)
  if (.Platform$OS.type == "unix") Sys.chmod(tmp, "0600")
  if (.Platform$OS.type == "windows" && file.exists(path)) {
    .windows_replace_file_atomic(tmp, path)
  } else if (!file.rename(tmp, path)) {
    stop("Could not atomically install the dsFlower node secret.",
         call. = FALSE)
  }
  invisible(path)
}

#' Ensure a dedicated per-node 256-bit secret for deterministic releases
#'
#' The secret is created at RUN TIME, never from `.onLoad`, so an image build
#' cannot accidentally bake one key into every deployed node. Missing, malformed
#' or permissively-mode'd regular files owned by the service user are replaced
#' with fresh operating-system entropy. Unsafe paths and ownership fail closed.
#' @keywords internal
.ensure_node_secret <- function() {
  path <- .node_secret_path()
  parent <- dirname(path)
  parent_existed <- dir.exists(parent)
  old_umask <- if (.Platform$OS.type == "unix") Sys.umask("0077") else NULL
  umask_restored <- FALSE
  on.exit({
    if (!umask_restored && !is.null(old_umask)) Sys.umask(old_umask)
  }, add = TRUE)
  dir.create(parent, recursive = TRUE, mode = "0700", showWarnings = FALSE)
  if (!is.null(old_umask)) Sys.umask(old_umask)
  umask_restored <- TRUE
  if (!dir.exists(parent)) {
    stop("Could not create the dsFlower secret directory: ", parent,
         call. = FALSE)
  }
  if (.privacy_path_is_link(parent)) {
    stop("The dsFlower node-secret parent must be a real directory.",
         call. = FALSE)
  }
  if (.Platform$OS.type == "unix" && !parent_existed) {
    Sys.chmod(parent, "0700")
  } else if (.Platform$OS.type == "windows" && !parent_existed) {
    .windows_set_private_acl(parent, is_directory = TRUE)
  }
  euid <- if (.Platform$OS.type == "unix") .privacy_effective_uid() else NULL
  parent_before <- .validate_node_secret_parent(path, euid)

  lock <- filelock::lock(paste0(path, ".lock"), timeout = 10000)
  if (is.null(lock)) {
    stop("Timed out creating the dsFlower node secret.", call. = FALSE)
  }
  on.exit(filelock::unlock(lock), add = TRUE)

  if (.privacy_path_is_link(path)) {
    stop("The dsFlower node secret must not be a link or reparse point.",
         call. = FALSE)
  }
  if (file.exists(path)) {
    valid <- tryCatch({
      .validate_node_secret(path)
      TRUE
    }, error = function(e) e)
    if (isTRUE(valid)) return(invisible(path))

    info <- file.info(path)
    if (nrow(info) != 1L || is.na(info$isdir[[1]]) ||
        isTRUE(info$isdir[[1]]) || !.path_is_regular_file(path)) {
      stop(conditionMessage(valid), call. = FALSE)
    }
    if (.Platform$OS.type == "unix") {
      owner <- suppressWarnings(as.integer(info$uid[[1]]))
      if (is.na(owner) || !identical(owner, as.integer(euid))) {
        stop(conditionMessage(valid), call. = FALSE)
      }
    } else if (.Platform$OS.type == "windows") {
      # Malformed content may be rotated, but an unsafe ACL is an ownership
      # failure and must never be papered over by replacement.
      .windows_validate_private_acl(path)
    }
  }

  .write_node_secret_atomic(path, parent)
  parent_after <- .validate_node_secret_parent(path, euid)
  if (!identical(parent_after, parent_before)) {
    stop("The dsFlower node-secret parent changed while creating the key.",
         call. = FALSE)
  }
  .validate_node_secret(path)
}

#' Package load hook -- verify Python venv root exists
#'
#' Fallback for when the configure script did not run (e.g. binary install,
#' devtools::load_all, or missing permissions during configure).  Ensures the
#' venv root directory is present so that .ensure_python_env() can create
#' per-framework venvs on first use without failing on a missing parent.
#'
#' Resolution order for the venv root path:
#'   1. DSFLOWER_VENV_ROOT environment variable
#'   2. dsflower.venv_root R option
#'   3. /var/lib/dsflower/venvs  (primary default)
#'   4. /srv/dsflower/venvs      (fallback if primary is not writable)
#'
#' @param libname Library path.
#' @param pkgname Package name.
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Ensure venv root directory exists.
  # configure creates it during install_github (as root).
  # This fallback handles API installs where configure doesn't run.
  venv_root <- Sys.getenv(
    "DSFLOWER_VENV_ROOT",
    unset = getOption("dsflower.venv_root", .default_venv_root())
  )

  if (!dir.exists(venv_root)) {
    created <- tryCatch(
      dir.create(venv_root, recursive = TRUE, showWarnings = FALSE),
      error = function(e) FALSE
    )
    # If the configured path is not writable, cascade through fallbacks so the
    # package self-provisions with ZERO root: /srv (Rock persistent volume)
    # first, then a user-space dir. This makes a plain `install_github` install
    # (as the unprivileged Rock R user) work without a root configure step.
    if (!isTRUE(created) && !dir.exists(venv_root)) {
      fallbacks <- if (identical(.Platform$OS.type, "windows")) {
        file.path(tools::R_user_dir("dsFlower", "data"), "venvs")
      } else {
        c(
          "/srv/dsflower/venvs",
          file.path(tools::R_user_dir("dsFlower", "data"), "venvs")
        )
      }
      for (fb in fallbacks) {
        ok <- tryCatch(
          dir.create(fb, recursive = TRUE, showWarnings = FALSE),
          error = function(e) FALSE
        )
        if (isTRUE(ok) || dir.exists(fb)) {
          options(dsflower.venv_root = fb)
          break
        }
      }
    }
  }

}

#' Package attach hook
#' @param lib Library path.
#' @param pkg Package name.
#' @keywords internal
.onAttach <- function(lib, pkg) {
  packageStartupMessage(
    "dsFlower v", utils::packageVersion("dsFlower"), " loaded."
  )
  python <- Sys.which("python3")
  if (!nzchar(python)) python <- Sys.which("python")
  if (!nzchar(python)) {
    packageStartupMessage(
      "dsFlower: python3 not found. ",
      "SuperNode operations will not work without Python.")
  }

  # Stale staging janitor: remove staging directories older than 24 hours
  .cleanup_stale_staging()

  # Clean orphaned SuperNode processes from crashed sessions
  orphans <- tryCatch(.cleanup_orphaned_supernodes(), error = function(e) 0L)
  if (orphans > 0L) {
    packageStartupMessage(
      "dsFlower: cleaned ", orphans, " orphaned SuperNode process(es).")
  }
}

#' Remove stale staging directories older than 24 hours
#' @keywords internal
.cleanup_stale_staging <- function(max_age_hours = 24, bases = NULL) {
  # A long-running federated job may legitimately outlive the age threshold.
  # Protect both processes owned by this R session and live SuperNodes discovered
  # through /proc before considering any directory for deletion.
  active <- character()
  for (key in ls(.supernode_registry, all.names = TRUE)) {
    entry <- tryCatch(get(key, envir = .supernode_registry),
                      error = function(e) NULL)
    alive <- tryCatch(!is.null(entry$process) && entry$process$is_alive(),
                      error = function(e) FALSE)
    if (isTRUE(alive)) active <- c(active, key)
  }
  live <- tryCatch(.list_supernode_processes(), error = function(e) NULL)
  if (!is.null(live) && nrow(live)) {
    active <- c(active, live$manifest_dir[!is.na(live$manifest_dir)])
  }
  active <- unique(vapply(active, function(path)
    normalizePath(path, winslash = "/", mustWork = FALSE), character(1)))

  if (is.null(bases)) {
    temp_parent <- dirname(tempdir())
    old_session_roots <- tryCatch(
      list.dirs(temp_parent, full.names = TRUE, recursive = FALSE),
      error = function(e) character())
    old_session_roots <- old_session_roots[
      startsWith(basename(old_session_roots), "Rtmp")]
    bases <- unique(c(.stagingBaseCandidates(create = FALSE),
                      old_session_roots))
  }

  for (base in bases) {
    dsflower_dir <- file.path(base, "dsflower")
    if (!dir.exists(dsflower_dir)) next
    subdirs <- list.dirs(dsflower_dir, full.names = TRUE, recursive = FALSE)
    for (d in subdirs) {
      canonical <- normalizePath(d, winslash = "/", mustWork = FALSE)
      if (canonical %in% active) next
      info <- file.info(d)
      if (!is.na(info$mtime) &&
          difftime(Sys.time(), info$mtime, units = "hours") > max_age_hours) {
        tryCatch(unlink(d, recursive = TRUE), error = function(e) NULL)
      }
    }
  }
}

#' Package detach hook
#'
#' Kills all registered SuperNodes.
#'
#' @param lib Library path.
#' @return Invisible NULL; called for its side effect.
#' @keywords internal
.onDetach <- function(lib) {
  for (addr in ls(.supernode_registry)) {
    tryCatch({
      entry <- get(addr, envir = .supernode_registry)
      if (!is.null(entry$process) && entry$process$is_alive()) {
        entry$process$signal(15L)
        entry$process$wait(timeout = 5000)
        if (entry$process$is_alive()) entry$process$kill()
      }
      # Clean PID file
      if (!is.null(entry$pid)) .remove_supernode_pid(entry$pid)
    }, error = function(e) NULL)
  }
  rm(list = ls(.supernode_registry), envir = .supernode_registry)
}
