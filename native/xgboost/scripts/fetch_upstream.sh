#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 1 ] || die "usage: $0 ABSOLUTE_DESTINATION"
DEST=$1
case "$DEST" in
  /*) ;;
  *) die "destination must be an absolute path" ;;
esac
[ ! -e "$DEST" ] || die "destination already exists: $DEST"

require_command git
mkdir -p -- "$DEST"
git -C "$DEST" init --quiet
git -C "$DEST" remote add origin "$DSFLOWER_XGB_UPSTREAM_REPOSITORY"
git -C "$DEST" fetch --quiet --depth 1 origin \
  "$DSFLOWER_XGB_UPSTREAM_COMMIT" \
  "refs/tags/$DSFLOWER_XGB_UPSTREAM_TAG:refs/tags/$DSFLOWER_XGB_UPSTREAM_TAG"
git -C "$DEST" checkout --quiet --detach "$DSFLOWER_XGB_UPSTREAM_COMMIT"
git -C "$DEST" submodule update --init --recursive --depth 1

"$SCRIPT_DIR/verify_upstream.sh" "$DEST"
printf '%s\n' "$DEST"
