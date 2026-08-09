# dsFlower native XGBoost patchset

This directory contains the reproducible, source-pinned start of dsFlower's
native XGBoost backend.  It does **not** vendor XGBoost and it does **not** yet
provide differentially private training.

The first patch adds an opt-in TreeUpdater registration and a C ABI for a
server-authoritative privacy context.  Both layers fail closed:

- an incomplete or malformed context is rejected;
- the context cannot be replaced without an explicit wipe;
- its 32-byte noise key is never written to model configuration;
- even a complete context cannot train because histogram privatization has not
  yet been implemented and proven.

The scaffold ABI accepts `privacy_unit=row|patient` but fixes
`adjacency=replace_one`, `unit_canonicalization=trim-utf8-v2`,
`contribution_strategy=one-record-per-unit-v1`, and `max_rows_per_unit=1`.
For v1, dsFlower must materialize exactly one DMatrix row per declared privacy
unit before entering XGBoost.  The fork does not silently pool raw patient rows.

Consequently, this scaffold is deliberately not connected to dsFlower's public
capabilities or runner.  `grow_dsflower_dp_hist` must not be advertised or used
in production.

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
```

On macOS the library suffix is `.dylib`; the end-to-end test discovers either
form automatically.  Run the complete verification with:

```sh
native/xgboost/tests/run.sh
```

Set `DSFLOWER_XGB_SKIP_BUILD=1` only for the fast offline metadata test.  A
release gate must run the default network/build path.

`verify_patched.sh` reconstructs the expected `HEAD + checksummed patches` tree
in a temporary Git index and independently materializes the complete worktree
with tracked, untracked, and ignored files.  Builds stop before CMake unless the
trees match exactly.  The `dmlc-core` worktree is checked the same way against
its pinned commit tree.

## What remains before DP can be claimed

The next patch must implement patient contribution bounding and add noise to
gradient/hessian histograms before *any* split, topology, leaf, metric, log, or
model artifact can depend on private statistics.  It also needs a formal
accountant, domain-separated CSPRNG derivation, replay tests, adversarial egress
tests, and a written privacy proof.  Until those conditions are independently
reviewed, the unconditional training rejection is a security invariant.
