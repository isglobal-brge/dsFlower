# Private validation, public starts and patient metrics — dsFlower 0.7.0

Token: `FLOWER_VALCV_2026-09-23`. Continue `feat/public-initialisation-routes`
in both packages at version 0.7.0. This extends `DESIGN_PUBLIC_INIT.md`; its
closed bundle verification, two admission routes, snapshot protection and
no-checkpoint-byte-export rules remain binding.

## Fixed analyst checkpoints

`ds.flower.validate(model = "client:<bundle>", ...)` validates the complete
bundle and manifest locally, declares the canonical manifest/content, checkpoint
and ordered tensor digests, and uses the existing bounded public artifact/array
transport. The node independently verifies the admitted bundle and derives its
digests, checks the trusted model, feature, loss, task and metric geometry, and
checks that incoming arrays are the declared fixed checkpoint before private
access. Validation releases bind the digests and actual arrays into their request
identity, as saved-model validation does. Labels, predictions, private counts and
site metrics never leave the node. Selecting one connection requests a single-node
DP figure using the same protocol.

Admission requires `dsflower.public_initialisation = "analyst_or_resource"`,
including any contract override and `default.dsflower.*` fallback. Both
`resource_only` and `none` refuse analyst-declared bundles before private access.
This policy is appropriate for validation because the bounded, noised release
protects the private data for any fixed admitted predictor; public provenance
does not establish utility or prove an analyst's training history. The trusted
predictor contract, safe parsing, public geometry and bounded contributions still
apply. A bundle cannot carry executable model code or select arbitrary paths.

The existing segmentation decoder profile keeps its pinned encoder and exact
contract. A tabular neural profile additionally binds a declarative trusted model
specification, ordered public features and bounds, task/loss and target semantics.
It reuses the same closed, digest-declared bundle and both admission routes. This
is needed for tabular CV from an admitted checkpoint, rather than treating an
unverified NPZ file as an admission authority.

## Public starts in holdout and CV

Atomic holdout trains from the admitted checkpoint once. Every CV fold starts
from a fresh copy of that same admitted checkpoint, using either analyst material
or a `flowerCheckpointInitDS` resource handle. Resource status contains only
public identity/provenance; the coordinator must provide its own matching local
checkpoint. It cannot recover weights from a node.

Each fold's first round checks incoming tensor geometry, hashes and equality to
the admitted material. Release identity includes the admitted identity and fold
coordinate. Completed folds cannot initialize later folds. Random starts retain
their existing deterministic behavior. The custodian-secret HMAC partition
continues to depend only on the existing resampling contract and privacy-unit
identifier; model, hyperparameters, checkpoint digest, provenance, aliases and
fold-training noise cannot change membership.

The existing privacy allocation remains 80% for training and 20% for the one
pooled metric release. CV divides training allocation across folds, retains OOF
statistics in private runtime state, and releases one pooled OOF vector after
all folds. No fold models, fold metrics or per-site metrics are released.

## Fixed segmentation layout

The trusted binary segmentation contract selects one image/mask contribution per
patient using its existing canonical selection and 128 by 128 mask geometry.
Threshold foreground predictions at probability 0.5. Let `M = 16384`, `I` be
foreground intersection, `P` predicted foreground size and `R` reference size.
Clip the per-patient counts to `[0,M]`. The fixed vector is

`[1, I/M, P/M, R/M]`.

Invalid selected records remain in the census and contribute zero mask statistics.
Every coordinate is bounded by one. Under replace-one adjacency, the fixed first
coordinate cancels, so a conservative L2 sensitivity is `sqrt(3)`. A keyed holdout
partition must also cover an absent contribution: its norm is at most `2`, so
holdout uses sensitivity `2`. The complete pooled OOF vector contains each patient
exactly once across all folds, and uses replacement sensitivity `sqrt(3)`. There is one Gaussian-noised sum per node.

After pooling the exact-layout vectors, floor negative mask statistics at zero
and cap them individually at the nonnegative noised census. Form foreground Dice `2*I/(P+R)` with a positive denominator floor, and clamp to
`[0,1]`. This is a pooled, size-weighted foreground Dice based on bounded patient
statistics, not the mean of individual patient Dice values. Any reported census
is also derived from the noised count. Empty/noisy denominator handling is public
post-processing; it does not inspect private sample counts.

## Fixed survival layouts

Only patient-decomposable metrics are admitted: observed-status Brier score at
public fixed horizons and negative log-likelihood under the fitted trusted
Weibull AFT, log-normal AFT or discrete-hazard parametrisation. The public model
time domain, administrative censoring, dispersion, hazard edges and invalid-row
rules continue to apply. Horizons are strictly ordered, fixed before private
access and within the public model horizon; the default is the model horizon.

For horizon `h`, a valid patient's status is known if an event occurred at or
before `h`, or follow-up reaches `h`. For known status, contribute the squared
error between predicted survival and `1 - 1(event observed at or before h)`; otherwise contribute zero
error and zero eligibility. Both error and eligibility are in `[0,1]`. Divide
the pooled noised error by the pooled noised eligible count, with public floors
and `[0,1]` clipping. This estimates the observed-status Brier score. It is not
IPCW Brier and is not claimed to correct censoring selection bias. No private
censoring model is estimated as part of this release.

Continuous event densities can exceed one and have negative NLL. Preserve that
possibility by clipping each valid patient's fitted NLL to `[-C,C]`, with public
`C=20` by default and `0 < C <= 1000`, and normalizing as `(clipped_NLL+C)/(2*C)`. Invalid patients
contribute zero and have zero valid count. The discrete-hazard NLL uses the
contract's per-subject likelihood convention and public number of intervals.

For `H` public horizons, the vector contains valid count, normalized clipped NLL,
`H` Brier error sums and `H` eligibility counts: size `2+2H`. A conservative L2
bound for both replacement and absence is `sqrt(2+2H)`. All fields, horizons and
the NLL bound are part of the fixed geometry/release identity. Pooled NLL is
post-processed as `2*C*sum_normalized/noised_valid_count-C`, clipped
back to `[-C,C]`; a nonpositive noised count yields a missing metric. Each node releases only one Gaussian-noised vector.

The concordance index is pairwise, not patient-decomposable under this contract.
Private validation, atomic holdout and CV do not release it. It remains available
only as a public-split/authorized analyst-local evaluation metric.

## Verification and delivery

Synthetic DSLite fixtures exercise analyst validation on two nodes and on one,
policy refusals and changed digest/geometry before private access, and two folds
of two actual DP rounds for tabular and segmentation public starts. Layout tests
compare pooled metrics against plain synthetic computations within calibrated
noise, and test replacement/absence bounds and unchanged partition assignment.
Existing public route tests retain resource admission and no-byte-export checks.

Run both packages' complete R/Python suites, standalone DP safety and runner
synchronization on clean committed checkouts. Record counts, skips, failures,
commands, exact analyst calls and canonical runner hash in
`VALCV_CONFIRMATION.md`. No tag, push or thesis edits.
