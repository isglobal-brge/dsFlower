# Provenance and integrity

- Upstream project: XGBoost, <https://github.com/dmlc/xgboost>
- Upstream release: `v3.4.0`
- Commit: `06335b125dccb859aacef142675506bfb84401b3`
- Git tree: `bfea7a1cb9cca3156478da0a077bd637d0749dea`
- SHA-256 of `git archive --format=tar` for the pinned commit:
  `1f7bac18b617c3fc3dac515d7d74c00d0fc96663bce3a41aed0f8a15499f3917`
- Pinned `dmlc-core` submodule commit:
  `4baa84e627849e675a3f99c92990ef9c39e4269e`

`scripts/verify_upstream.sh` recomputes and checks all source pins before a
patch can be applied or built.  `PATCHES.sha256` separately authenticates every
local patch.  The source archive digest covers the upstream superproject tree;
the submodule commit is verified independently because Git archives store a
gitlink rather than the submodule contents.

Before CMake runs, `scripts/verify_patched.sh` builds two temporary Git indices:
the authorized tree obtained from the pinned commit plus the checksummed
patchset, and the actual worktree including forced ignored/untracked entries.
Their tree IDs must match.  A third temporary index requires the complete
`dmlc-core` worktree to match its pinned commit tree, so a matching submodule
HEAD alone is insufficient.

The upstream tag is lightweight, so it does not carry a signed tag object.  The
immutable commit, tree, archive SHA-256, and submodule commit are therefore the
enforced provenance boundary.
