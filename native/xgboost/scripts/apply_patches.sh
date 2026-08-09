#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 1 ] || die "usage: $0 UPSTREAM_SOURCE"
SOURCE=$1
"$SCRIPT_DIR/verify_upstream.sh" "$SOURCE"

for PATCH in "$DSFLOWER_XGB_ROOT"/patches/*.patch; do
  PATCH_NAME=$(basename -- "$PATCH")
  verify_patch_checksum "$PATCH"

  if git -C "$SOURCE" apply --reverse --check "$PATCH" >/dev/null 2>&1; then
    printf '%s\n' "already applied: $PATCH_NAME"
  else
    git -C "$SOURCE" apply --check "$PATCH"
    git -C "$SOURCE" apply "$PATCH"
    printf '%s\n' "applied: $PATCH_NAME"
  fi
done

git -C "$SOURCE" diff --check
[ -f "$SOURCE/plugin/dsflower/dsflower_updater.cc" ] ||
  die "patchset marker is missing"
"$SCRIPT_DIR/verify_patched.sh" "$SOURCE"
