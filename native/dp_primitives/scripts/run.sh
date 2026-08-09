#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)

cargo test --manifest-path "$root_dir/Cargo.toml" --locked
cargo build --manifest-path "$root_dir/Cargo.toml" --release --locked
PYTHONDONTWRITEBYTECODE=1 python3 "$root_dir/tests/abi_smoke.py"
PYTHONDONTWRITEBYTECODE=1 python3 "$root_dir/tests/test_metadata.py"

if command -v cc >/dev/null 2>&1; then
  cc -std=c11 -Wall -Wextra -Werror -I"$root_dir/include" \
    -fsyntax-only "$root_dir/tests/header_smoke.c"
fi
if command -v c++ >/dev/null 2>&1; then
  c++ -std=c++17 -Wall -Wextra -Werror -I"$root_dir/include" \
    -x c++ -fsyntax-only "$root_dir/tests/header_smoke.c"
fi

case "$(uname -s 2>/dev/null || true)" in
  Darwin|Linux)
    target_dir=${CARGO_TARGET_DIR:-"$root_dir/target"}
    case "$target_dir" in
      /*) ;;
      *) target_dir="$root_dir/$target_dir" ;;
    esac
    library_dir="$target_dir/release"
    consumer_dir=$(mktemp -d "${TMPDIR:-/tmp}/dsflower-dp-consumer.XXXXXX")
    cleanup_consumer() {
      case "$consumer_dir" in
        "${TMPDIR:-/tmp}"/dsflower-dp-consumer.*) rm -rf -- "$consumer_dir" ;;
        *) printf '%s\n' "refusing to remove unexpected path: $consumer_dir" >&2 ;;
      esac
    }
    trap cleanup_consumer EXIT HUP INT TERM
    cc -std=c11 -Wall -Wextra -Werror -I"$root_dir/include" \
      "$root_dir/tests/header_smoke.c" -L"$library_dir" \
      -ldsflower_dp_primitives -Wl,-rpath,"$library_dir" \
      -o "$consumer_dir/header-smoke-c"
    c++ -std=c++17 -Wall -Wextra -Werror -I"$root_dir/include" \
      -x c++ "$root_dir/tests/header_smoke.c" -L"$library_dir" \
      -ldsflower_dp_primitives -Wl,-rpath,"$library_dir" \
      -o "$consumer_dir/header-smoke-cpp"
    "$consumer_dir/header-smoke-c"
    "$consumer_dir/header-smoke-cpp"
    ;;
esac
