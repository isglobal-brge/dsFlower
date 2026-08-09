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
  repository contains a pinned minimal CKS20 source port and C ABI under
  `native/dp_primitives`, but it is not yet connected to this updater and does
  not make the end-to-end tree mechanism complete.

An artifact, ledger claim or replay identity for one profile is never valid for
the other.  The mechanism ID, fixed-point scale or floating-point profile,
accountant version, all public bounds, cuts and training parameters belong in
the canonical mechanism configuration hash.

The independent executable oracle is
`reference/mechanism_v1.py`.  It has no updater dependency and can be checked
with:

```sh
python3 -m unittest discover -s native/xgboost/reference -p 'test_*.py' -v
```

## Threat model and privacy unit

The analyst, submitted application and network are untrusted.  The Rock
process, vetted native adapter, persistent node identity secret, semantic
artifact store, privacy ledger, native fork and pinned sampler are inside the
trusted computing base.  Compromise of that boundary or the raw server
filesystem is out of scope.  A persistent noise key is needed only by the
separate practical PRF profile; the discrete profile takes fresh private
randomness from the operating system and persists the resulting artifact.

Adjacency is bounded `replace_one`: two datasets have the same public unit
slots and differ in all values of at most one slot.  Before the native ABI is
entered, dsFlower must materialize exactly one row for every declared privacy
unit using `one-record-per-unit-v1`.  A patient unit therefore requires
server-side contribution bounding and canonicalization before XGBoost sees the
matrix.  Row multiplicity, weights or duplicated unit identifiers are not
accepted.

Sticky release identity protects a repeated *identical* semantic query from
averaging: the committed artifact is replayed byte-for-byte.  Its authenticated
identity must bind at least the mechanism ID, node, privacy epoch, dataset
snapshot, cohort, canonical query, allocation and every mechanism parameter.
The practical PRF profile must additionally bind tree, depth, node, feature,
bin and component into each noise coordinate.  The discrete profile does not
derive deterministic noise: it samples once from private operating-system
entropy and relies on atomic artifact commit/replay.  Changing any semantic
input creates a distinct release and consumes a distinct allocation.  Replays
of an already committed artifact are post-processing and do not add privacy
loss; distinct releases compose.  If deployment policy does not cap distinct
releases, this specification gives a per-training guarantee, not a finite
lifetime epsilon for an unlimited sequence of new queries.

## V1 training domain

The proof applies only to all of the following restrictions:

- binary logistic classification with labels clipped to `{0, 1}`, or bounded
  squared-error regression with targets clipped to public bounds;
- CPU, in-memory, single-process training, one output and one tree per round;
- a public, fixed number `T >= 1` of trees and `D >= 1` histogram levels per
  tree, depthwise construction and a worst-case `T * D` release schedule;
- exactly one materialized DMatrix row per privacy unit;
- a complete public feature schema, public finite feature/target bounds and
  complete public cuts; a sketch over the private training matrix is forbidden;
- a public binary base score of `0.5`, or the midpoint of the public target
  bounds for regression; private-label base-score estimation is forbidden;
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

An exact first release may use true sampler randomness and make replay sticky
by persisting the committed artifact.  Replacing those coins with output from
a finite persistent PRF/CSPRNG seed adds a computational assumption and no
longer follows from the information-theoretic theorem alone.  Such a design
must either carry an explicit computational qualifier or prove and charge a
quantitative sampler error; it must not silently inherit the formal label.

`native/dp_primitives` ports the exact CKS20 arithmetic and rejection rules from
pinned OpenDP 0.15.1 source. It takes an integer scale directly, adds noise in
arbitrary precision and applies saturating `i64` post-processing. It does not
link OpenDP or OpenSSL and deliberately exposes no seed. Random bits come from
a fail-closed, buffered operating-system source; the mathematical statement is
conditional on those bits being independent and uniform, while the deployment
trusts the platform RNG to realize that assumption. OpenDP labels the upstream
pre-1.0 constructor `contrib`, so source pinning and a passing ABI test are
necessary but not sufficient: dsFlower still requires an independent
proof/code review, known-issue audit and Linux/macOS/Windows test matrix before
treating the sampler gate as closed.

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
are post-processing and spend no additional privacy budget.

## Egress contract

The only successful analyst-visible training artifact is a sanitized standard
XGBoost model whose topology, public cut thresholds and finite leaf statistics
are deterministic functions of public configuration and privatized
histograms.  The sanitizer and adapter must ensure all of the following:

- no raw histogram, gradient, Hessian, count, sketch, row prediction, leaf
  assignment, per-row error, training metric or private validation metric;
- no feature/target names, unit identifiers, dataset paths, stack traces,
  seeds, keys, key IDs, sampler state, ledger paths or native debug dumps;
- no partial model or data-dependent diagnostic on failure;
- no raw-data-dependent logs, callbacks, timing-controlled output, model
  attributes or objective/base-score estimation;
- no analyst-visible fine-grained completion timing from the variable-time
  sampler or native training path; isolation or a reviewed public coarsening
  schedule is required before capability activation;
- finite, size-bounded model fields and an exact allowed-field/schema check
  before the semantic artifact is committed;
- a privacy reservation and exact semantic claim before computation, followed
  by atomic commit and byte-identical replay for the same canonical identity.

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
4. mechanism-specific replay and concurrency tests; operating-system-randomness
   and ABI tests with no seed input for the discrete profile, and
   domain-separation plus key-wipe tests only for the practical PRF profile;
5. for `fixed-point-discrete-v1`, an exact sampler or a reviewed quantitative
   sampler theorem plus explicit error accounting; for the practical profile,
   an explicit computational threat model and no formal-DP label;
6. adversarial egress tests over success, rejection, interruption and lost-ACK
   replay paths; and
7. semantic artifact and privacy-ledger invariants holding before capability
   discovery is enabled; and
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
