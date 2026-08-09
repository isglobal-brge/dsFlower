#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 4 ] ||
  die "usage: $0 PATCHED_UPSTREAM_SOURCE BUILD_DIRECTORY DP_INCLUDE_DIR DP_LINK_LIBRARY"
SOURCE=$(CDPATH= cd -- "$1" && pwd)
BUILD=$2
DP_INCLUDE=$(CDPATH= cd -- "$3" && pwd)
DP_LIBRARY_DIR=$(CDPATH= cd -- "$(dirname -- "$4")" && pwd)
DP_LIBRARY="$DP_LIBRARY_DIR/$(basename -- "$4")"

[ -f "$SOURCE/plugin/dsflower/dsflower_updater.cc" ] ||
  die "apply the dsFlower patchset before building"
[ -f "$DP_INCLUDE/dsflower_dp_primitives.h" ] ||
  die "DP primitive ABI header was not found"
[ -f "$DP_LIBRARY" ] || die "DP primitive link library was not found"

CMAKE_COMMAND=${CMAKE:-cmake}
"$SCRIPT_DIR/verify_patched.sh" "$SOURCE"
require_command "$CMAKE_COMMAND"

case "$(uname -s 2>/dev/null || true)" in
  Darwin) BUNDLE_RPATH='@loader_path' ;;
  Linux) BUNDLE_RPATH='$ORIGIN' ;;
  *) BUNDLE_RPATH='' ;;
esac

"$CMAKE_COMMAND" -S "$SOURCE" -B "$BUILD" \
  -DPLUGIN_DSFLOWER_DP=ON \
  -DDSFLOWER_DP_CORE=ON \
  -DDSFLOWER_DP_PRIMITIVES_INCLUDE_DIR="$DP_INCLUDE" \
  -DDSFLOWER_DP_PRIMITIVES_LIBRARY="$DP_LIBRARY" \
  -DUSE_OPENMP=OFF \
  -DUSE_CUDA=OFF \
  -DUSE_NCCL=OFF \
  -DPLUGIN_RMM=OFF \
  -DPLUGIN_SYCL=OFF \
  -DUSE_DEBUG_OUTPUT=OFF \
  -DGOOGLE_TEST=OFF \
  -DBUILD_STATIC_LIB=OFF \
  -DKEEP_BUILD_ARTIFACTS_IN_BINARY_DIR=ON \
  -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_CXX_STANDARD_REQUIRED=ON \
  -DCMAKE_CXX_EXTENSIONS=OFF \
  -DCMAKE_BUILD_RPATH="$BUNDLE_RPATH" \
  -DCMAKE_BUILD_TYPE=Release
"$CMAKE_COMMAND" --build "$BUILD" --target xgboost \
  --config Release \
  --parallel "${DSFLOWER_XGB_BUILD_JOBS:-2}"

printf '%s\n' "built curated dsFlower DP core in $BUILD"
