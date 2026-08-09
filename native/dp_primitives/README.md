# dsFlower native DP primitives

This directory builds the small, platform-neutral C ABI shared by dsFlower's
native tree engines.  It currently exposes one primitive: vector-valued exact
discrete Gaussian noise for signed 64-bit fixed-point statistics.

The exact CKS20 sampler is a minimal source port from OpenDP 0.15.1 commit
`c34d3d04a8872a51af523d9a2244be6171173b7d`. It keeps OpenDP's
arbitrary-precision rational arithmetic and rejection rules, but does not link
OpenDP or OpenSSL. Each invocation consumes buffered operating-system random
bytes through `getrandom`; refills fail closed and never fall back to another
generator. The buffer only amortizes system calls—it does not expand a seed
through another PRG. Noise is added in arbitrary precision and then saturated
to `i64`; saturation is deterministic post-processing of the release.
The sampler distribution is exact conditional on independent uniform random
bytes; a production build relies computationally on the operating-system RNG
to realize that ideal source.

The source algorithm was exposed as `contrib` by pre-1.0 OpenDP. Porting and
pinning it is not a substitute for dsFlower's own proof review, known-issue
audit and cross-platform release tests. `UPSTREAM.env` records the exact
upstream tree, archive and source-file hashes used for line-by-line review.

No seed crosses the current ABI, so this primitive is not yet compatible with
dsFlower's stateless sticky contract. Before activation, its adapter must feed
the sampler a domain-separated stream derived from the custodial node root and
the canonical semantic training identity. Equivalent retries must therefore
recompute the same bytes without a database or artifact cache; a changed
effective statistic or mechanism configuration must derive a different stream.

This library is not itself a complete DP algorithm.  A caller must prove and
enforce contribution bounds, fixed-point sensitivity, scale calibration,
within-training composition, semantic PRF binding, and egress sanitization. Until the native
engine adapters do so, no capability may be advertised.

The exact rejection sampler and arbitrary-precision arithmetic are
variable-time. Its output-distribution proof does not cover timing or resource
side channels. Before an analyst-visible capability is enabled, the enclosing
service must isolate or coarsen completion timing according to a reviewed
public schedule; raw sampler/training latency must not be an egress channel.

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
