# Provenance and integrity

- Upstream project: XGBoost, <https://github.com/dmlc/xgboost>
- Upstream release: `v3.4.0`
- Commit: `06335b125dccb859aacef142675506bfb84401b3`
- Git tree: `bfea7a1cb9cca3156478da0a077bd637d0749dea`
- Canonical SHA-256 over every path, mode, object type and raw file content in
  the pinned tree:
  `e0b63c3d15af4338f4c37e4272091ca1d8fa0df9ce847228d42681f49aa53481`
- Pinned `dmlc-core` submodule commit:
  `4baa84e627849e675a3f99c92990ef9c39e4269e`
- Local patchset version: `3`
- Exact Git tree after applying the checksummed patchset:
  `376740e907457f52c96745e52440a94a9aab4177`

Patch 0003's curated bundle core links the separately pinned
`native/dp_primitives` ABI v2 mechanism
`cks20-discrete-gaussian-i64-hmac-sha256-v1`.  That dependency is built from
the same dsFlower checkout and packaged beside the XGBoost shared library; it
is not copied into the XGBoost source tree.

`scripts/verify_upstream.sh` recomputes and checks all source pins before a
patch can be applied or built.  `PATCHES.sha256` separately authenticates every
local patch.  `canonical_tree_sha256.py` frames and hashes raw Git blob content
directly, avoiding platform-dependent tar metadata and line-ending filters.
The submodule commit is included as a gitlink and its complete worktree is
verified independently.

Before CMake runs, `scripts/verify_patched.sh` builds two temporary Git indices:
the authorized tree obtained from the pinned commit plus the checksummed
patchset, and the actual worktree including forced ignored/untracked entries.
Their tree IDs must match.  A third temporary index requires the complete
`dmlc-core` worktree to match its pinned commit tree, so a matching submodule
HEAD alone is insufficient.

The upstream tag is lightweight, so it does not carry a signed tag object.  The
immutable commit, tree, canonical source SHA-256, and submodule commit are
therefore the enforced provenance boundary.
