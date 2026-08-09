# dsFlower native XGBoost patchset

This directory contains the reproducible, source-pinned start of dsFlower's
native XGBoost backend.  It does **not** vendor XGBoost and it does **not** yet
advertise a production differentially private training capability.

The first patch adds an opt-in TreeUpdater registration and a C ABI for a
server-authoritative privacy context.  The second patch upgrades that C ABI to
v2 and binds the complete public training shape.  The third adds the curated,
deterministic fixed-point histogram core and moves the ABI to v3.  A normal
build remains fail closed; only `DSFLOWER_DP_CORE=ON` compiles the core:

- an incomplete or malformed context is rejected;
- the context cannot be replaced without an explicit wipe;
- its 32-byte noise key is never written to model configuration;
- the context owns exactly one in-memory CPU DMatrix and rejects another one;
- public numeric feature bounds and cuts are copied and float-validated;
- only single-output `binary:logistic` and `reg:squarederror`, with explicit
  public target bounds/base score, requested total-tree bound, and exact depth,
  are accepted;
- gradients are tagged by the built-in objective path, so public custom-gradient
  APIs cannot impersonate either accepted objective;
- even a complete context cannot train unless the curated core switch is
  deliberately enabled with the separately built exact primitive.

The scaffold ABI accepts `privacy_unit=row|patient` but fixes
`adjacency=replace_one`, `unit_canonicalization=trim-utf8-v2`,
`contribution_strategy=one-record-per-unit-v1`, and `max_rows_per_unit=1`.
For this contribution contract, dsFlower must materialize exactly one DMatrix
row per declared privacy unit before entering XGBoost.  The trusted adapter
performs deterministic bounded patient aggregation and numeric totalization;
the fork does not receive or silently pool raw patient rows.
ABI v3 additionally rejects external-memory/categorical matrices, ranking,
row/feature weights, base margins, survival bounds, and labels outside the
server-pinned task bounds.  It seals labels against later metadata mutation,
binds updater configuration to a process-unique context generation (including
across threads), and restricts execution to CPU, one output, one tree per round,
the exclusive trusted updater, and a non-data-derived base score.  All C
pointers are borrowed only for the call;
the implementation copies the DMatrix shared pointer, schema, cuts, strings,
and derived key.

The curated core counts successful trees across Boosters sharing one installed
context and rejects tree index `max_trees`.  Clearing the context clears the
counter; it is per-training defense in depth, never lifetime state.

The server derives the v3 `noise_key` from its custodial root and the complete
canonical semantic training identity. Every sampler and mechanism coordinate
must consume only a deterministic, domain-separated PRF stream from that key;
operating-system randomness and persisted reply artifacts are not valid
alternatives.

The trusted adapter resolves the per-training accountant before crossing the
native boundary. The ABI therefore carries the derived 32-byte key and the
effective integer scales used by the core, not caller identity hashes or raw
epsilon/delta request values.

The core uses the fixed binary PRF domain `label-with-NUL || release-kind-u8 ||
tree-index-u64be || depth-u32be`. It privatizes the complete joint level vector
before choosing any split, missing direction, topology, leaf, gain or cover.
The root total is part of the first release and later totals derive only from
already privatized bins.  Binary logistic and bounded squared-error objectives
share this path.

The bundle switch is deliberately not connected to dsFlower's public
capabilities or R interface.  Its exact internal status is
`bundle-core:fixed-point-discrete-v1:internal-only`; the normal scaffold reports
`scaffold-only:no-dp-histogram-privatization`.  Public discovery must remain off
until the trusted loader, adapter and remaining release gates are closed.

## Reproduce and verify

The scripts accept explicit destinations so no source or build output is
written into this repository:

```sh
work_dir="$(mktemp -d)"
native/xgboost/scripts/fetch_upstream.sh "$work_dir/xgboost"
native/xgboost/scripts/apply_patches.sh "$work_dir/xgboost"
native/xgboost/scripts/verify_patched.sh "$work_dir/xgboost"
native/xgboost/scripts/build.sh "$work_dir/xgboost" "$work_dir/build"
native/xgboost/tests/context_abi_smoke.py \
  "$work_dir/build/lib/libxgboost.so"
native/xgboost/scripts/build_bundle.sh \
  "$work_dir/xgboost" "$work_dir/bundle"
native/xgboost/scripts/verify_bundle.py "$work_dir/bundle"
```

On macOS the library suffix is `.dylib`; the end-to-end test discovers either
form automatically.  Run the complete verification with:

```sh
native/xgboost/tests/run.sh
```

The complete test also builds `native/dp_primitives` with Rust 1.88, links a
separate `DSFLOWER_DP_CORE=ON` XGBoost library, packages it, verifies it after
all intermediate build paths are gone, and exercises training, model/prediction
parity, deterministic replay and the per-training tree counter.  `build.sh`
never enables that switch; `build_bundle.sh` is the explicit curated path.

## Curated bundle layout

The bundle is a directory, not an installer, and contains exactly three regular
files with no symlinks.  Linux uses `lib/libxgboost.so` and
`lib/libdsflower_dp_primitives.so`; macOS uses the corresponding `.dylib`
names; Windows uses `lib/xgboost.dll` and
`lib/dsflower_dp_primitives.dll`.  Every platform also has `manifest.json` at
the root.  No compiler output, import library, training state, model, log or
intermediate file is packaged.

`manifest.json` is ASCII JSON with sorted keys, compact separators and one
trailing newline.  Schema `dsflower-xgboost-bundle-v1` records bundle version,
normalized system/machine, the relative paths and SHA-256 hashes of both
libraries, privacy-context ABI v3, primitive ABI v2, both exact mechanism IDs,
the internal status, upstream commit/tree, patched tree and patchset version.
It has no signature or self-referential hash.  The trusted loader derives the
bundle identity as SHA-256 of these canonical manifest bytes, whose library
hashes bind the actual code.  `verify_bundle.py` rejects extra files, symlinks,
noncanonical JSON, provenance/platform/contract mismatches and altered bytes,
then checks the exported primitive ABI/mechanism and XGBoost status.

Set `DSFLOWER_XGB_SKIP_BUILD=1` only for the fast offline metadata test.  A
release gate must run the default network/build path.

`verify_patched.sh` reconstructs the expected `HEAD + checksummed patches` tree
in a temporary Git index and independently materializes the complete worktree
with tracked, untracked, and ignored files.  Builds stop before CMake unless the
trees match exactly.  The `dmlc-core` worktree is checked the same way against
its pinned commit tree.

## What remains before production DP can be claimed

The service adapter must still enforce patient contribution bounding, calculate
the exact integer scales from the per-training accountant, bind every effective
input into the derived key, exclude private evaluation paths, and sanitize model
egress.  Interruption behavior, timing isolation, adversarial release tests and
the end-to-end proof also require review.  Until those conditions are closed,
the normal build's unconditional training rejection is a security invariant.
