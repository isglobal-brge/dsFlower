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
"$ROOT/scripts/fetch_upstream.sh" "$SOURCE"
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
