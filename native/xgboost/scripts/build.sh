#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -ge 1 ] && [ "$#" -le 2 ] ||
  die "usage: $0 PATCHED_UPSTREAM_SOURCE [BUILD_DIRECTORY]"
SOURCE=$(CDPATH= cd -- "$1" && pwd)
BUILD=${2:-"$SOURCE.dsflower-build"}
[ -f "$SOURCE/plugin/dsflower/dsflower_updater.cc" ] ||
  die "apply the dsFlower patchset before building"

CMAKE_COMMAND=${CMAKE:-cmake}
"$SCRIPT_DIR/verify_patched.sh" "$SOURCE"
require_command "$CMAKE_COMMAND"

"$CMAKE_COMMAND" -S "$SOURCE" -B "$BUILD" \
  -DPLUGIN_DSFLOWER_DP=ON \
  -DUSE_OPENMP=OFF \
  -DGOOGLE_TEST=OFF \
  -DBUILD_STATIC_LIB=OFF \
  -DKEEP_BUILD_ARTIFACTS_IN_BINARY_DIR=ON \
  -DCMAKE_BUILD_TYPE=Release
"$CMAKE_COMMAND" --build "$BUILD" --target xgboost \
  --parallel "${DSFLOWER_XGB_BUILD_JOBS:-2}"

printf '%s\n' "built fail-closed scaffold in $BUILD"
