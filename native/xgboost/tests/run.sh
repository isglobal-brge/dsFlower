#!/bin/sh
# Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
# SPDX-License-Identifier: MIT

set -eu
TEST_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

python3 "$TEST_DIR/test_metadata.py"
if [ "${DSFLOWER_XGB_SKIP_BUILD:-0}" = "1" ]; then
  printf '%s\n' "skipped network/build smoke by explicit request"
else
  "$TEST_DIR/test_end_to_end.sh"
fi
