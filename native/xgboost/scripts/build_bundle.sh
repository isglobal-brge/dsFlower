#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$SCRIPT_DIR/common.sh"

[ "$#" -eq 2 ] || die "usage: $0 PATCHED_UPSTREAM_SOURCE OUTPUT_BUNDLE_DIRECTORY"
SOURCE=$(CDPATH= cd -- "$1" && pwd)
OUTPUT=$2
[ ! -e "$OUTPUT" ] && [ ! -L "$OUTPUT" ] || die "bundle output already exists"

DP_ROOT=$(CDPATH= cd -- "$DSFLOWER_XGB_ROOT/../dp_primitives" && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/dsflower-xgboost-bundle.XXXXXX")
cleanup() {
  case "$WORK" in
    "${TMPDIR:-/tmp}"/dsflower-xgboost-bundle.*) rm -rf -- "$WORK" ;;
    *) printf '%s\n' "refusing to remove unexpected bundle work path: $WORK" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

require_command cargo
cargo build --manifest-path "$DP_ROOT/Cargo.toml" --release --locked \
  --target-dir "$WORK/dp-target"

case "$(uname -s 2>/dev/null || true)" in
  Darwin)
    DP_LIBRARY="$WORK/dp-target/release/libdsflower_dp_primitives.dylib"
    DP_RUNTIME_LIBRARY=$DP_LIBRARY
    ;;
  Linux)
    DP_LIBRARY="$WORK/dp-target/release/libdsflower_dp_primitives.so"
    DP_RUNTIME_LIBRARY=$DP_LIBRARY
    ;;
  CYGWIN*|MINGW*|MSYS*)
    DP_LIBRARY=$(find "$WORK/dp-target/release" -maxdepth 1 -type f \( \
      -name 'dsflower_dp_primitives.dll.lib' -o \
      -name 'libdsflower_dp_primitives.dll.a' \) -print | head -n 1)
    DP_RUNTIME_LIBRARY="$WORK/dp-target/release/dsflower_dp_primitives.dll"
    ;;
  *) die "unsupported bundle platform" ;;
esac
[ -n "$DP_LIBRARY" ] && [ -f "$DP_LIBRARY" ] ||
  die "DP primitive link library was not produced"
[ -f "$DP_RUNTIME_LIBRARY" ] || die "DP primitive runtime library was not produced"

"$SCRIPT_DIR/build_core.sh" \
  "$SOURCE" "$WORK/xgboost-build" "$DP_ROOT/include" "$DP_LIBRARY"

XGBOOST_LIBRARY=$(find "$WORK/xgboost-build" \( -type f -o -type l \) \( \
  -name 'libxgboost.so' -o -name 'libxgboost.dylib' -o \
  -name 'libxgboost.so.[0-9]*' -o \
  -name 'libxgboost.[0-9]*.so' -o -name 'libxgboost.[0-9]*.dylib' \
  -o -name 'xgboost.dll' \
\) -print | head -n 1)
[ -n "$XGBOOST_LIBRARY" ] || die "curated libxgboost was not produced"

python3 "$SCRIPT_DIR/package_bundle.py" \
  "$SOURCE" "$XGBOOST_LIBRARY" "$DP_RUNTIME_LIBRARY" "$OUTPUT"
python3 "$SCRIPT_DIR/verify_bundle.py" "$OUTPUT"
