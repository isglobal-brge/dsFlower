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
BUILD="$WORK/scaffold-build"
BUNDLE="$WORK/bundle"
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

"$ROOT/scripts/build_bundle.sh" "$SOURCE" "$BUNDLE"
case "$(uname -s 2>/dev/null || true)" in
  Darwin)
    CORE_LIBRARY="$BUNDLE/lib/libxgboost.dylib"
    DP_RUNTIME_LIBRARY="$BUNDLE/lib/libdsflower_dp_primitives.dylib"
    ;;
  Linux)
    CORE_LIBRARY="$BUNDLE/lib/libxgboost.so"
    DP_RUNTIME_LIBRARY="$BUNDLE/lib/libdsflower_dp_primitives.so"
    ;;
  CYGWIN*|MINGW*|MSYS*)
    CORE_LIBRARY="$BUNDLE/lib/xgboost.dll"
    DP_RUNTIME_LIBRARY="$BUNDLE/lib/dsflower_dp_primitives.dll"
    ;;
  *)
    printf '%s\n' "unsupported test platform for DP primitives" >&2
    exit 1
    ;;
esac
[ -f "$CORE_LIBRARY" ] || {
  printf '%s\n' "packaged curated libxgboost was not found" >&2
  exit 1
}
[ -f "$DP_RUNTIME_LIBRARY" ] || {
  printf '%s\n' "packaged DP primitive library was not found" >&2
  exit 1
}

# Run again after build_bundle.sh has removed all intermediate build paths.
python3 "$ROOT/scripts/verify_bundle.py" "$BUNDLE"
python3 "$TEST_DIR/dp_core_smoke.py" "$CORE_LIBRARY" "$DP_RUNTIME_LIBRARY"

cp -R "$BUNDLE" "$WORK/tampered-bundle"
printf 'x' >> "$WORK/tampered-bundle/lib/$(basename -- "$DP_RUNTIME_LIBRARY")"
if python3 "$ROOT/scripts/verify_bundle.py" "$WORK/tampered-bundle" >/dev/null 2>&1; then
  printf '%s\n' "tampered bundle unexpectedly passed verification" >&2
  exit 1
fi
