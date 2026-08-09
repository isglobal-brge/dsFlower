# Native XGBoost v1 privacy specification

## Status and mechanism identities

This document is a specification and proof oracle, not a production claim.
The updater remains fail-closed until its implementation, sampler, adapter and
every egress path have passed the release gates below.  In particular, the
presence of this file must never enable or advertise a capability.

V1 keeps two mechanism identities separate:

- `binary64-box-muller-practical-v1` is a computational/practical profile.  Its
  accountant is the ideal real-arithmetic continuous Gaussian accountant, but
  a keyed binary64 Box--Muller stream is not the ideal Gaussian distribution.
- `fixed-point-discrete-v1` is a mathematical fixed-point discrete-Gaussian
  profile with exact integer sensitivity and rational budget allocation.  The
  repository contains a pinned minimal CKS20 source port and deterministic C
  ABI under `native/dp_primitives`.  Patch 0003 connects it only to the
  compile-time-isolated test core; that does not make the end-to-end tree
  mechanism complete or enable a capability.

One profile's semantic randomness identity is never valid for the other. The
mechanism ID, effective fixed-point/noise scales or floating-point profile,
accountant version, effective cuts and base score, training shape,
privacy-unit contract, and canonical binned matrix/target belong in the
semantic key derivation. Admission bounds that leave those effective values
unchanged do not create a fresh noise stream.

The independent executable oracle is
`reference/mechanism_v1.py`.  It has no updater dependency and can be checked
with:

```sh
python3 -m unittest discover -s native/xgboost/reference -p 'test_*.py' -v
```

## Threat model and privacy unit

The analyst, submitted application and network are untrusted. The Rock
process, vetted native adapter, persistent node secret, native fork and pinned
sampler are inside the trusted computing base. Compromise of that boundary or
the raw server filesystem is out of scope. No query history, artifact cache or
privacy database belongs to the mechanism.

Adjacency is bounded `replace_one`: two datasets have the same public unit
slots and differ in all values of at most one slot.  Before the native ABI is
entered, dsFlower must materialize exactly one row for every declared privacy
unit using `one-record-per-unit-v1`.  A patient unit therefore requires
server-side contribution bounding and canonicalization before XGBoost sees the
matrix.  Row multiplicity, weights or duplicated unit identifiers are not
accepted.

Sticky randomness protects an identical semantic training from averaging. Its
authenticated key derivation binds the mechanism and runtime versions,
effective public configuration, per-training privacy contract, public model
input and the effective bounded private statistic. The native core then
domain-separates release kind, tree and depth; coordinates within each joint
vector consume that stream sequentially. The same training therefore
recomputes the same model without persistent query state; any effective
semantic change derives a different stream. The guarantee is per training, and
the accountant composes only that training's fixed tree and depth schedule.

The discrete primitive consumes a deterministic HMAC-SHA256 byte stream keyed
by a 32-byte context-derived key. The core supplies the canonical binary domain
`label-with-NUL || release-kind-u8 || tree-index-u64be || depth-u32be`;
coordinates within one joint vector consume the stream sequentially. This makes
the operational claim computational, even though the fixed-point sampler
arithmetic remains exact conditional on uniform bytes.

## V1 training domain

The proof applies only to all of the following restrictions:

- binary logistic classification restricted to labels in `{0, 1}`, or bounded
  squared-error regression restricted to targets inside public bounds; the
  adapter rejects out-of-domain values and the native boundary revalidates
  them rather than clipping labels or targets;
- CPU, in-memory, single-process training, one output and one tree per round;
- a public, fixed number `T >= 1` of trees and `D >= 1` histogram levels per
  tree, depthwise construction and a worst-case `T * D` release schedule;
- exactly one materialized DMatrix row per privacy unit;
- a complete public feature schema, public finite feature/target bounds and
  complete public cuts; a sketch over the private training matrix is forbidden;
- a public binary base score in `(0, 1)`, or a server-pinned value inside the
  public target bounds for regression (the midpoint is the default);
  private-label base-score estimation is forbidden;
- server-owned objectives, gradient and Hessian clipping, learning rate,
  regularization, finite positive denominator protection and leaf clipping;
- every stopping, split, missing-direction, topology, weight, gain and cover
  decision uses public values or already privatized values only.

V1 rejects GPU, distributed and external-memory execution; row/column
subsampling; DART; categorical, ranking, survival, multiclass and multi-target
objectives; random forests; custom objectives, metrics and callbacks; early
stopping; private evaluation sets; sample weights; base margins; input models;
and private cut or parameter selection.  These features require separate
mechanisms and proofs, not parameter validation alone.

## Joint histogram query and sensitivity

At a tree/depth pair, the mechanism releases one joint vector containing every
active node, feature, bin and gradient/Hessian component.  A unit contributes
to one node/bin coordinate per feature, irrespective of the number of bins or
active nodes.  At the root it also contributes to one total-gradient/Hessian
coordinate.  Later node totals must be derived only from the previous DP
parent and DP child values.  A raw root sum, raw empty-node test, raw sibling
subtraction or raw data-dependent stopping rule invalidates the proof.

Let `F` be the public feature count.  The number of contribution groups is

```text
k_0 = F + 1       at the root (feature histograms plus the total)
k_d = F           at later depths
```

Condition on all previous DP outputs.  Clip and normalize one unit's gradient
and Hessian before histogram accumulation:

```text
a = clip(g, -G, G) / G  in [-1, 1]
b = clip(h,  0, H) / H  in [ 0, 1]    binary
b = 1                                  bounded squared error
```

For binary training, replacement in the same coordinate changes a group by at
most `(2, 1)`, whose squared norm is `5`.  If the coordinate changes, the old
and new contributions occupy disjoint coordinates and their combined squared
norm is at most `4`.  Thus the conservative normalized L2 sensitivity is

```text
Delta_binary(k) = sqrt(5 k).
```

For bounded squared error, the normalized Hessian is the same constant on both
datasets.  Both the same-coordinate and different-coordinate cases have
squared norm at most `4`, giving

```text
Delta_regression(k) = 2 sqrt(k).
```

These bounds also cover replacement across different active nodes.  They do
not grow with the number of bins because a unit occupies only one coordinate
per group.  The oracle enumerates all pairs in the extremal binary rectangle
and regression domain, with multiple node/bin locations, and obtains exactly
`5k` and `4k` as the maximum squared distances for small geometries.

### Fixed-point sensitivity

For `fixed-point-discrete-v1`, let the public scale be `Q = 2^q`.  A
server-owned deterministic quantizer must produce integer gradient/Hessian
contributions in `[-Q, Q] x [0, Q]` for binary training and
`[-Q, Q] x {Q}` for regression.  Accumulators must be checked wide enough for
the public maximum row count; overflow, saturation or wraparound is forbidden.

The exact integer upper bounds used by the oracle are

```text
Delta_int_binary(k, Q)     = ceil(sqrt(5 k Q^2))
Delta_int_regression(k, Q) = ceil(sqrt(4 k Q^2)).
```

The second expression is the minimal integer ceiling of `2 Q sqrt(k)`, not the
larger shortcut `2 Q ceil(sqrt(k))`.

## Composition and calibration

### Ideal continuous-Gaussian reference

For an ideal real-arithmetic Gaussian vector with L2 sensitivity `Delta` and
independent coordinate noise `N(0, sigma^2)`, one release is
`rho = Delta^2 / (2 sigma^2)`-zCDP.  zCDP composes adaptively by addition and
converts to `(epsilon, delta)`-DP as

```text
epsilon = rho + 2 sqrt(rho ln(1/delta)).
```

Writing `L = ln(1/delta)`, the stable positive solution for a requested total
`epsilon` is

```text
sqrt(rho_total) = epsilon / (sqrt(L + epsilon) + sqrt(L))
rho_total       = epsilon^2 / (sqrt(L + epsilon) + sqrt(L))^2.
```

V1 allocates uniformly over the worst-case schedule:

```text
rho_level = rho_total / (T D)
sigma_d   = Delta(k_d) / sqrt(2 rho_level).
```

The reference oracle evaluates this at 160 decimal digits, rounds both rho
values downward and sigma/sensitivity upward to binary64, adds an outward ULP
guard, and verifies the upward-rounded composed epsilon against the request.
This protects the accountant from ordinary binary64 under-noising; it does not
turn a finite Box--Muller sampler into an ideal Gaussian sampler.

### Fixed-point discrete Gaussian

For an exact discrete Gaussian on the integer lattice with probability mass
proportional to `exp(-||z||^2 / (2 sigma_int^2))`, integer translation by a
query difference `u` has, for every Renyi order `alpha > 1`,

```text
D_alpha <= alpha ||u||^2 / (2 sigma_int^2).
```

Completing the square leaves a shifted Gaussian lattice sum.  By Poisson
summation its Fourier coefficients are non-negative, so that sum is maximized
at an integer shift.  Consequently the mathematical mechanism is
`Delta_int^2/(2 sigma_int^2)`-zCDP.  This statement assumes the specified
discrete Gaussian distribution is sampled exactly; an implementation with an
approximate sampler needs its own proved error term and delta allocation.

Production randomness must come only from a deterministic, domain-separated
PRF stream keyed by the sole custodial node root and the complete canonical
semantic training identity. This gives byte-identical recomputation without
query history or stored reply artifacts. The finite PRF stream adds a
computational pseudorandomness assumption and therefore must carry an explicit
computational qualifier or a proved quantitative sampler-error term; it must
not silently inherit the information-theoretic formal label.

`native/dp_primitives` ports the exact CKS20 arithmetic and rejection rules from
pinned OpenDP 0.15.1 source. It takes an integer scale directly, adds noise in
arbitrary precision and applies saturating `i64` post-processing. It does not
link OpenDP or OpenSSL and exposes neither the custodial root nor a public seed.
ABI v2 requires a nonzero 32-byte derived key and a bounded caller domain, then
expands uniform bytes as HMAC-SHA256 blocks with a fixed primitive-level prefix,
domain length and big-endian counter. OpenDP labels the upstream pre-1.0
constructor `contrib`, so source pinning and passing ABI/integration tests are
necessary but not sufficient: dsFlower still requires an independent
proof/code review and known-issue audit before treating the sampler gate as
closed.

The discrete oracle avoids transcendental calibration.  Represent public
`epsilon` and `delta` as exact rationals and set

```text
B         = ceil(log2(1/delta))
rho_total = epsilon^2 / (4 (B + epsilon))
rho_level = rho_total / (T D).
```

Because `B >= ln(1/delta)`, let `t = epsilon/(B + epsilon)`.  Then

```text
(rho_total + 2 sqrt(rho_total ln(1/delta))) / epsilon
 <= sqrt(1 - t) + t/4
 <= 1 - t/2 + t/4
 <= 1.
```

The oracle computes `B` and both rho values with integer/Fraction arithmetic.
It chooses the smallest integer `sigma_int` satisfying

```text
Delta_int^2 / (2 sigma_int^2) <= rho_level
```

and verifies both that inequality and the failure of `sigma_int - 1` exactly.

## Post-processing and tree construction

Noise is added to the complete joint histogram before any split candidate,
default missing direction, gain, topology or leaf value is selected.  Building
both children is simplest.  Histogram subtraction is allowed only as
`DP parent - DP child`; subtracting a raw child or exposing a raw sibling is
forbidden.

Noisy Hessian totals may be mapped to `max(0, total)`.  For each feature, noisy
Hessian bins may be deterministically projected onto
`{x: x >= 0, sum(x) <= total}` and the missing bin set to the non-negative
residual.  `reference/mechanism_v1.py` contains an auditable Euclidean
projection.  This and all tree decisions based exclusively on DP histograms
are post-processing and add no further privacy loss.

## Egress contract

The only successful analyst-visible training artifact is a sanitized standard
XGBoost model whose topology, public cut thresholds and finite leaf statistics
are deterministic functions of public configuration and privatized
histograms.  The sanitizer and adapter must ensure all of the following:

- no raw histogram, gradient, Hessian, count, sketch, row prediction, leaf
  assignment, per-row error, training metric or private validation metric;
- no feature/target names, unit identifiers, dataset paths, stack traces,
  seeds, keys, sampler state or native debug dumps;
- no partial model or data-dependent diagnostic on failure;
- no raw-data-dependent logs, callbacks, timing-controlled output, model
  attributes or objective/base-score estimation;
- no analyst-visible fine-grained completion timing from the variable-time
  sampler or native training path; isolation or a reviewed public coarsening
  schedule is required before capability activation;
- finite, size-bounded model fields and an exact allowed-field/schema check
  before egress;
- deterministic PRF binding to the complete canonical training identity, with
  byte-identical recomputation for the same identity and no persistent cache.

Predictions on analyst-supplied public records are post-processing.  Accuracy,
loss, calibration, confusion matrices, survival metrics or cross-validation
computed on node-private records are new queries and require their own bounded,
accounted DP validation mechanism.

## Release gates

Neither mechanism may be advertised until an independent review confirms:

1. exact one-row-per-unit materialization and every V1 restriction at the
   native boundary;
2. public cuts only and no upstream sketch, raw root sum, raw stopping path or
   unnoised collective/buffer;
3. joint noise before every data-dependent tree decision, with tests matching
   the oracle's root and later sensitivity geometry;
4. mechanism-specific deterministic recomputation and concurrency tests,
   domain separation, key wipe and cross-platform PRF-stream tests;
5. for `fixed-point-discrete-v1`, an exact sampler or a reviewed quantitative
   sampler theorem plus explicit error accounting; for the practical profile,
   an explicit computational threat model and no formal-DP label;
6. adversarial egress tests over success, rejection, interruption and lost-ACK
   replay paths; and
7. canonical semantic identity and per-training composition invariants holding
   before capability discovery is enabled; and
8. identical source, ABI and artifact tests passing on supported Linux, macOS
   and Windows builds; and
9. analyst-visible timing/resource behavior isolated or coarsened independently
   of the private execution path.

## Why Box--Muller is not the formal profile

An ideal continuous Gaussian has uncountable support.  A finite-input
binary64 Box--Muller implementation has finite or countable support, so its
total-variation distance from the ideal continuous distribution is one.  The
real-arithmetic proof therefore cannot be transferred by claiming a small
floating-point approximation error.  Keying the stream with HMAC/ChaCha and
keeping the key secret can support a computational pseudorandomness argument
and sticky replay, but it also changes the guarantee from information-theoretic
DP to a computational/practical one.  That is why its mechanism identity and
claims remain separate from `fixed-point-discrete-v1`.
