#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
TEST_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$TEST_DIR/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dsflower-xgboost-test.XXXXXX")

cleanup() {
  case "$WORK" in
    "${TMPDIR:-/tmp}"/dsflower-xgboost-test.*) rm -rf -- "$WORK" ;;
    *) printf '%s\n' "refusing to remove unexpected test path: $WORK" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

SOURCE="$WORK/xgboost"
BUILD="$WORK/build"
CORE_BUILD="$BUILD"
DP_TARGET="$WORK/dp-target"
DP_ROOT=$(CDPATH= cd -- "$ROOT/../dp_primitives" && pwd)
"$ROOT/scripts/fetch_upstream.sh" "$SOURCE"
"$ROOT/scripts/apply_patches.sh" "$SOURCE"
"$ROOT/scripts/apply_patches.sh" "$SOURCE"
"$TEST_DIR/test_provenance_rejections.sh" "$SOURCE" "$WORK"
"$ROOT/scripts/build.sh" "$SOURCE" "$BUILD"

LIBRARY=$(find "$BUILD" \( -type f -o -type l \) \( \
  -name 'libxgboost.so' -o -name 'libxgboost.dylib' -o \
  -name 'libxgboost.so.[0-9]*' -o \
  -name 'libxgboost.[0-9]*.so' -o -name 'libxgboost.[0-9]*.dylib' \
  -o -name 'xgboost.dll' \
\) -print | head -n 1)
[ -n "$LIBRARY" ] || {
  printf '%s\n' "built libxgboost was not found under $BUILD" >&2
  exit 1
}

python3 "$TEST_DIR/context_abi_smoke.py" "$LIBRARY"

cargo build --manifest-path "$DP_ROOT/Cargo.toml" --release --locked \
  --target-dir "$DP_TARGET"
case "$(uname -s 2>/dev/null || true)" in
  Darwin)
    DP_LIBRARY=$(find "$DP_TARGET/release" -maxdepth 1 -type f \
      -name 'libdsflower_dp_primitives.dylib' -print | head -n 1)
    DP_RUNTIME_LIBRARY=$DP_LIBRARY
    ;;
  Linux)
    DP_LIBRARY=$(find "$DP_TARGET/release" -maxdepth 1 -type f \
      -name 'libdsflower_dp_primitives.so' -print | head -n 1)
    DP_RUNTIME_LIBRARY=$DP_LIBRARY
    ;;
  CYGWIN*|MINGW*|MSYS*)
    DP_LIBRARY=$(find "$DP_TARGET/release" -maxdepth 1 -type f \( \
      -name 'dsflower_dp_primitives.dll.lib' -o \
      -name 'libdsflower_dp_primitives.dll.a' \) -print | head -n 1)
    DP_RUNTIME_LIBRARY=$(find "$DP_TARGET/release" -maxdepth 1 -type f \
      -name 'dsflower_dp_primitives.dll' -print | head -n 1)
    ;;
  *)
    printf '%s\n' "unsupported test platform for DP primitives" >&2
    exit 1
    ;;
esac
[ -n "$DP_LIBRARY" ] || {
  printf '%s\n' "DP primitives link library was not found under $DP_TARGET/release" >&2
  exit 1
}
[ -n "$DP_RUNTIME_LIBRARY" ] || {
  printf '%s\n' "DP primitives runtime library was not found under $DP_TARGET/release" >&2
  exit 1
}

CMAKE_COMMAND=${CMAKE:-cmake}
"$CMAKE_COMMAND" -S "$SOURCE" -B "$CORE_BUILD" \
  -DPLUGIN_DSFLOWER_DP=ON \
  -DDSFLOWER_DP_CORE_TESTING=ON \
  -DDSFLOWER_DP_PRIMITIVES_INCLUDE_DIR="$DP_ROOT/include" \
  -DDSFLOWER_DP_PRIMITIVES_LIBRARY="$DP_LIBRARY" \
  -DUSE_OPENMP=OFF \
  -DGOOGLE_TEST=OFF \
  -DBUILD_STATIC_LIB=OFF \
  -DKEEP_BUILD_ARTIFACTS_IN_BINARY_DIR=ON \
  -DCMAKE_BUILD_TYPE=Release
"$CMAKE_COMMAND" --build "$CORE_BUILD" --target xgboost \
  --config Release \
  --parallel "${DSFLOWER_XGB_BUILD_JOBS:-2}"

CORE_LIBRARY=$(find "$CORE_BUILD" \( -type f -o -type l \) \( \
  -name 'libxgboost.so' -o -name 'libxgboost.dylib' -o \
  -name 'libxgboost.so.[0-9]*' -o \
  -name 'libxgboost.[0-9]*.so' -o -name 'libxgboost.[0-9]*.dylib' \
  -o -name 'xgboost.dll' \
\) -print | head -n 1)
[ -n "$CORE_LIBRARY" ] || {
  printf '%s\n' "built test-core libxgboost was not found under $CORE_BUILD" >&2
  exit 1
}

PATH="$DP_TARGET/release:$PATH" \
LD_LIBRARY_PATH="$DP_TARGET/release${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
DYLD_LIBRARY_PATH="$DP_TARGET/release${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
  python3 "$TEST_DIR/dp_core_smoke.py" "$CORE_LIBRARY" "$DP_RUNTIME_LIBRARY"
