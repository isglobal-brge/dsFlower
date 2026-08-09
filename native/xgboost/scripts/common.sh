#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu

DSFLOWER_XGB_SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DSFLOWER_XGB_ROOT=$(CDPATH= cd -- "$DSFLOWER_XGB_SCRIPT_DIR/.." && pwd)

# shellcheck source=../UPSTREAM.env
. "$DSFLOWER_XGB_ROOT/UPSTREAM.env"

die() {
  printf '%s\n' "dsFlower XGBoost: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "sha256sum or shasum is required"
  fi
}

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    die "sha256sum or shasum is required"
  fi
}

verify_patch_checksum() {
  patch=$1
  patch_name=$(basename -- "$patch")
  expected=$(awk -v name="patches/$patch_name" '$2 == name { print $1 }' \
    "$DSFLOWER_XGB_ROOT/PATCHES.sha256")
  [ -n "$expected" ] || die "missing checksum for patches/$patch_name"
  actual=$(sha256_file "$patch")
  [ "$actual" = "$expected" ] || die "checksum mismatch for patches/$patch_name"
}
