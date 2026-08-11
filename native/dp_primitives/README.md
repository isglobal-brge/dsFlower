# dsFlower native DP primitives

This directory builds the small, platform-neutral C ABI shared by dsFlower's
native tree engines.  It currently exposes one primitive: vector-valued exact
discrete Gaussian noise for signed 64-bit fixed-point statistics.

The exact CKS20 sampler is a minimal source port from OpenDP 0.15.1 commit
`c34d3d04a8872a51af523d9a2244be6171173b7d`. It keeps OpenDP's
arbitrary-precision rational arithmetic and rejection rules, but does not link
OpenDP or OpenSSL. Each invocation consumes a domain-separated HMAC-SHA256
stream keyed by a 32-byte key derived from the custodial root and the complete
semantic training identity. The ABI never accepts the durable root or an
analyst-provided seed. Equal semantic work reproduces identical noise without
a database; a different mechanism coordinate uses a different domain. Noise is
added in arbitrary precision and then saturated to `i64`; saturation is
deterministic post-processing of the release. The exact sampler is therefore
driven by a stream computationally indistinguishable from uniform bits under
the HMAC-SHA256 PRF assumption.

The source algorithm was exposed as `contrib` by pre-1.0 OpenDP. Porting and
pinning it is not a substitute for dsFlower's own proof review, known-issue
audit and cross-platform release tests. `UPSTREAM.env` records the exact
upstream tree, archive and source-file hashes used for line-by-line review.

Only a per-training derived key crosses the trusted ABI. The caller must bind it
to the canonical effective statistic and complete mechanism configuration, and
must use a unique canonical domain for every tree/level release coordinate.

This library is not itself a complete DP algorithm.  A caller must prove and
enforce contribution bounds, fixed-point sensitivity, scale calibration,
within-training composition, semantic PRF binding, and egress sanitization.
Until the native engine adapters do so, no capability may be advertised.

The exact rejection sampler and arbitrary-precision arithmetic are
variable-time. Its output-distribution proof does not cover timing or resource
side channels. The advertised numeric DP guarantee therefore excludes timing,
availability and resource-observation channels. Process isolation and a
coarsened public completion schedule remain deployment hardening when those
channels are in scope; they are not properties supplied by this primitive.

## Build and test

Rust 1.88 or newer is required.  Dependencies and their checksums are locked.
Every distributed shared library must include `LICENSES.md` and the generated
`THIRD_PARTY_LICENSES.html` inventory.

```sh
cargo test --manifest-path native/dp_primitives/Cargo.toml --locked
cargo build --manifest-path native/dp_primitives/Cargo.toml --release --locked
python3 native/dp_primitives/tests/abi_smoke.py
```

The same source and C header are used on Linux, macOS and Windows.  The release
gate must run the test matrix on all three; host success alone is not a claim
of cross-platform support.
