#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 1 ] || die "usage: $0 PATCHED_UPSTREAM_SOURCE"
SOURCE=$1
"$SCRIPT_DIR/verify_upstream.sh" "$SOURCE"

INDEX_DIR=$(mktemp -d "${TMPDIR:-/tmp}/dsflower-xgb-indices.XXXXXX")
EXPECTED_INDEX="$INDEX_DIR/expected.index"
ACTUAL_INDEX="$INDEX_DIR/actual.index"
cleanup_indices() {
  for path in \
    "$EXPECTED_INDEX.lock" "$EXPECTED_INDEX" \
    "$ACTUAL_INDEX.lock" "$ACTUAL_INDEX"; do
    [ ! -e "$path" ] || unlink "$path"
  done
  rmdir "$INDEX_DIR"
}
trap cleanup_indices EXIT HUP INT TERM

# Reconstruct the only authorized patched tree without consulting the worktree.
GIT_INDEX_FILE="$EXPECTED_INDEX" git -C "$SOURCE" read-tree \
  "$DSFLOWER_XGB_UPSTREAM_COMMIT"
for PATCH in "$DSFLOWER_XGB_ROOT"/patches/*.patch; do
  verify_patch_checksum "$PATCH"
  GIT_INDEX_FILE="$EXPECTED_INDEX" git -C "$SOURCE" apply \
    --cached --whitespace=error-all "$PATCH"
done
EXPECTED_TREE=$(GIT_INDEX_FILE="$EXPECTED_INDEX" git -C "$SOURCE" write-tree)

# Materialize every build-visible worktree entry, including ignored and
# untracked files, in an independent index.  The submodule contents are checked
# separately by verify_upstream.sh; this tree contains its pinned gitlink.
GIT_INDEX_FILE="$ACTUAL_INDEX" git -C "$SOURCE" read-tree \
  "$DSFLOWER_XGB_UPSTREAM_COMMIT"
# Respect the checkout's filemode capability.  On filesystems without POSIX
# mode bits, read-tree preserves the pinned modes while add still captures all
# build-visible content.
GIT_INDEX_FILE="$ACTUAL_INDEX" git -C "$SOURCE" add -A -f -- .
ACTUAL_TREE=$(GIT_INDEX_FILE="$ACTUAL_INDEX" git -C "$SOURCE" write-tree)

[ "$ACTUAL_TREE" = "$EXPECTED_TREE" ] ||
  die "patched source tree mismatch: tracked, untracked, or ignored content is not authorized"

printf '%s\n' "verified exact patched XGBoost tree ($ACTUAL_TREE)"
