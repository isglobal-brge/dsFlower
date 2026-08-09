#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
TEST_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$TEST_DIR/.." && pwd)
[ "$#" -eq 2 ] || {
  printf '%s\n' "usage: $0 PATCHED_UPSTREAM_SOURCE TEST_WORK_DIRECTORY" >&2
  exit 2
}
SOURCE=$1
WORK=$2

expect_provenance_failure() {
  expected_message=$1
  output=$2
  if "$ROOT/scripts/verify_patched.sh" "$SOURCE" >"$output" 2>&1; then
    printf '%s\n' "verify_patched accepted modified source" >&2
    exit 1
  fi
  grep -F "$expected_message" "$output" >/dev/null || {
    printf '%s\n' "unexpected verify_patched failure:" >&2
    sed -n '1,120p' "$output" >&2
    exit 1
  }

  if CMAKE=true "$ROOT/scripts/build.sh" "$SOURCE" "$WORK/forbidden-build" \
      >"$output.build" 2>&1; then
    printf '%s\n' "build accepted modified source or reached the CMake sentinel" >&2
    exit 1
  fi
  grep -F "$expected_message" "$output.build" >/dev/null || {
    printf '%s\n' "build did not fail at the provenance gate:" >&2
    sed -n '1,120p' "$output.build" >&2
    exit 1
  }
}

TOP_BACKUP="$WORK/CMakeLists.txt.authorized"
cp -p "$SOURCE/CMakeLists.txt" "$TOP_BACKUP"
printf '\n# unauthorized provenance test mutation\n' >>"$SOURCE/CMakeLists.txt"
expect_provenance_failure "patched source tree mismatch" "$WORK/top-level-rejection.log"
mv "$TOP_BACKUP" "$SOURCE/CMakeLists.txt"
"$ROOT/scripts/verify_patched.sh" "$SOURCE" >/dev/null

UNTRACKED_FILE="$SOURCE/unauthorized-provenance-test.txt"
printf '%s\n' "unauthorized untracked content" >"$UNTRACKED_FILE"
expect_provenance_failure "patched source tree mismatch" "$WORK/untracked-rejection.log"
unlink "$UNTRACKED_FILE"
"$ROOT/scripts/verify_patched.sh" "$SOURCE" >/dev/null

IGNORED_DIR="$SOURCE/build"
IGNORED_FILE="$IGNORED_DIR/unauthorized-provenance-test.txt"
mkdir "$IGNORED_DIR"
printf '%s\n' "unauthorized ignored content" >"$IGNORED_FILE"
expect_provenance_failure "patched source tree mismatch" "$WORK/ignored-rejection.log"
unlink "$IGNORED_FILE"
rmdir "$IGNORED_DIR"
"$ROOT/scripts/verify_patched.sh" "$SOURCE" >/dev/null

DMLC_BACKUP="$WORK/dmlc-CMakeLists.txt.authorized"
cp -p "$SOURCE/dmlc-core/CMakeLists.txt" "$DMLC_BACKUP"
printf '\n# unauthorized submodule provenance test mutation\n' \
  >>"$SOURCE/dmlc-core/CMakeLists.txt"
expect_provenance_failure "dmlc-core worktree mismatch" "$WORK/dmlc-rejection.log"
mv "$DMLC_BACKUP" "$SOURCE/dmlc-core/CMakeLists.txt"
"$ROOT/scripts/verify_patched.sh" "$SOURCE" >/dev/null

printf '%s\n' "XGBoost patched-source provenance rejection tests: ok"
