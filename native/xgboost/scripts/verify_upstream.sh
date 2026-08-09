#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 1 ] || die "usage: $0 UPSTREAM_SOURCE"
SOURCE=$1
[ -d "$SOURCE/.git" ] || die "not a Git checkout: $SOURCE"
require_command git

ACTUAL_COMMIT=$(git -C "$SOURCE" rev-parse HEAD)
[ "$ACTUAL_COMMIT" = "$DSFLOWER_XGB_UPSTREAM_COMMIT" ] ||
  die "commit mismatch: expected $DSFLOWER_XGB_UPSTREAM_COMMIT, got $ACTUAL_COMMIT"

ACTUAL_TAG=$(git -C "$SOURCE" rev-parse "refs/tags/$DSFLOWER_XGB_UPSTREAM_TAG^{}")
[ "$ACTUAL_TAG" = "$DSFLOWER_XGB_UPSTREAM_COMMIT" ] ||
  die "tag mismatch: $DSFLOWER_XGB_UPSTREAM_TAG does not resolve to the pinned commit"

ACTUAL_TREE=$(git -C "$SOURCE" rev-parse "$DSFLOWER_XGB_UPSTREAM_COMMIT^{tree}")
[ "$ACTUAL_TREE" = "$DSFLOWER_XGB_UPSTREAM_TREE" ] ||
  die "tree mismatch: expected $DSFLOWER_XGB_UPSTREAM_TREE, got $ACTUAL_TREE"

ACTUAL_ARCHIVE_SHA=$(
  git -C "$SOURCE" archive --format=tar "$DSFLOWER_XGB_UPSTREAM_COMMIT" | sha256_stream
)
[ "$ACTUAL_ARCHIVE_SHA" = "$DSFLOWER_XGB_UPSTREAM_ARCHIVE_SHA256" ] ||
  die "upstream archive SHA-256 mismatch"

[ -d "$SOURCE/dmlc-core/.git" ] || [ -f "$SOURCE/dmlc-core/.git" ] ||
  die "dmlc-core submodule is not initialized"
ACTUAL_DMLC=$(git -C "$SOURCE/dmlc-core" rev-parse HEAD)
[ "$ACTUAL_DMLC" = "$DSFLOWER_DMLC_CORE_COMMIT" ] ||
  die "dmlc-core mismatch: expected $DSFLOWER_DMLC_CORE_COMMIT, got $ACTUAL_DMLC"

INDEX_DIR=$(mktemp -d "${TMPDIR:-/tmp}/dsflower-xgb-dmlc-index.XXXXXX")
DMLC_INDEX="$INDEX_DIR/actual.index"
cleanup_index() {
  [ ! -e "$DMLC_INDEX.lock" ] || unlink "$DMLC_INDEX.lock"
  [ ! -e "$DMLC_INDEX" ] || unlink "$DMLC_INDEX"
  rmdir "$INDEX_DIR"
}
trap cleanup_index EXIT HUP INT TERM

GIT_INDEX_FILE="$DMLC_INDEX" git -C "$SOURCE/dmlc-core" read-tree \
  "$DSFLOWER_DMLC_CORE_COMMIT"
# Respect the checkout's filemode capability.  Git for Windows uses
# core.filemode=false because NTFS does not reproduce POSIX executable bits;
# the modes already loaded by read-tree remain authoritative there.
GIT_INDEX_FILE="$DMLC_INDEX" git -C "$SOURCE/dmlc-core" add -A -f -- .
ACTUAL_DMLC_TREE=$(GIT_INDEX_FILE="$DMLC_INDEX" \
  git -C "$SOURCE/dmlc-core" write-tree)
EXPECTED_DMLC_TREE=$(git -C "$SOURCE/dmlc-core" rev-parse \
  "$DSFLOWER_DMLC_CORE_COMMIT^{tree}")
[ "$ACTUAL_DMLC_TREE" = "$EXPECTED_DMLC_TREE" ] ||
  die "dmlc-core worktree mismatch: tracked, untracked, or ignored content differs from the pin"

printf '%s\n' "verified XGBoost $DSFLOWER_XGB_UPSTREAM_TAG ($ACTUAL_COMMIT)"
