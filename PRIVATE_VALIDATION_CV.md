# Private validation, holdout and cross-validation in 0.7.3

`ds.flower.validate()` accepts a saved dsFlower model or a complete
`client:<checkpoint-bundle>` supplied by the analyst. Validation returns only
pooled node-DP metrics. To obtain one site's figure, select that connection.
Checkpoint validation is permitted only when the custodian admits analyst
material with `dsflower.public_initialisation = "analyst_or_resource"`.
`resource_only` and `none` refuse analyst bundle validation.

```r
library(dsFlowerClient)
validation <- ds.flower.validate(
  conns, symbol = "D_test", target = "outcome",
  model = "client:/analyst/public/tabular_bundle.zip"
)
single_site <- ds.flower.validate(
  conns["node1"], symbol = "D_test", target = "outcome",
  model = "client:/analyst/public/tabular_bundle.zip"
)
```

The bundle specifies the ordered features, public bounds, task/loss, model
geometry, checkpoint and per-tensor digests. Both client and node verify it.
The node also verifies incoming arrays against the admitted checkpoint before
private access. The model identity is part of the validation release identity.
Neither an NPZ file alone nor a manifest with missing evidence is sufficient.

For segmentation, use a complete segmentation bundle with its pinned encoder:

```r
seg_metrics <- ds.flower.validate(
  conns, symbol = "images_test", target = "mask_path",
  model = "client:/analyst/public/segmentation_bundle.zip"
)
seg_metrics$metrics$foreground_dice
```

The existing model/feature contract remains trusted: bundles contain arrays and
declarative metadata, never executable predictor code. Admission verifies the
declaration and its contents; it does not establish a model's predictive utility.
Saved-model validation uses the same fixed metric layouts. Analyst segmentation
bundle validation uses the default input contract: `images`/`masks` assets,
`relative_path` image paths, `image_id` sample IDs and mask values `0,255`;
`target` supplies the mask path column. Saved segmentation artifacts retain their
stored input roles and mask vocabulary. An independent test dataset gives external
validation; reusing training data gives resubstitution.

## Public starts for resampling

Tabular neural training accepts `public_initialisation`; segmentation continues
to use the model's `decoder_init`. Both accept `client:<bundle>` or
`resource:<handle-symbol>`. Every CV fold starts from a fresh copy of the same
admitted checkpoint. Holdout training starts from it once. Model, parameters and
initialisation cannot change the private HMAC row/patient partition.

```r
model <- ds.flower.model.pytorch_logreg(batch_size = 8L)
cv <- ds.flower.cross_validate(
  conns, symbol = "D", target = "outcome", features = c("x1", "x2"),
  model = model, feature_bounds = list(lower = c(-1, -1), upper = c(1, 1)),
  target_levels = c(0, 1),
  public_initialisation = "client:/analyst/public/tabular_bundle.zip",
  folds = 2L, rounds = 2L
)
holdout <- ds.flower.fit(
  conns, symbol = "D", target = "outcome", features = c("x1", "x2"),
  model = model, feature_bounds = list(lower = c(-1, -1), upper = c(1, 1)),
  target_levels = c(0, 1),
  public_initialisation = "client:/analyst/public/tabular_bundle.zip",
  holdout = 0.2, rounds = 2L
)
```

For a resource, first assign the custodian's registered checkpoint and admit it:

```r
DSI::datashield.assign.resource(conns, "CKPT_R", "PublicModels.tabular")
DSI::datashield.assign.expr(conns, "CKPT", quote(flowerCheckpointInitDS("CKPT_R")))
cv <- ds.flower.cross_validate(
  conns, symbol = "D", target = "outcome", features = c("x1", "x2"),
  model = model, feature_bounds = list(lower = c(-1, -1), upper = c(1, 1)),
  target_levels = c(0, 1), public_initialisation = "resource:CKPT",
  public_checkpoint_file = "/analyst/public/checkpoint.npz",
  folds = 2L, rounds = 2L
)
```

The coordinator's local file must match every node's admitted identity. It grants
no node authority. Resource names and handles do not enter scientific identity;
node status never exports checkpoint bytes. See
[public initialisation](PUBLIC_INITIALISATION.md) for registration and policy.

```r
segmentation <- ds.flower.model.pytorch_resnet18_segmentation(
  decoder = "narrow", decoder_init = "client:/analyst/public/segmentation_bundle.zip",
  batch_size = 8L, local_epochs = 1L
)
seg_cv <- ds.flower.cross_validate(
  conns, symbol = "images", target = "mask_path", model = segmentation,
  task = "segmentation", data_kind = "image", folds = 2L, rounds = 2L
)
seg_holdout <- ds.flower.fit(
  conns, symbol = "images", target = "mask_path", model = segmentation,
  task = "segmentation", data_kind = "image", holdout = 0.2, rounds = 2L
)
```

Use `decoder_init = "resource:SEG_CKPT"` and pass
`public_checkpoint_file = "/analyst/public/checkpoint.npz"` for the registered
segmentation route. The patient policy and permitted image/mask files are custodian-owned.
Cross-validation returns `cv.json` with pooled OOF metrics; it exports no fold
models or fold metrics. Training receives 80% of the job budget (divided across
CV folds), and the one pooled metric release receives the remaining 20%.

## Patient metric layouts

| Contract | Per-patient vector | L2 sensitivity: validation / holdout / pooled OOF |
| --- | --- | --- |
| Segmentation | `[1, I/16384, P/16384, R/16384]` | `sqrt(3)` / `2` / `sqrt(3)` |
| Survival, H horizons | `[valid, normalized clipped NLL, H errors, H eligible indicators]` | `sqrt(2+2H)` / `sqrt(2+2H)` / `sqrt(2+2H)` |

Segmentation selects one canonical image per patient and thresholds predictions
at 0.5. Intersection `I`, prediction size `P` and reference size `R` are bounded
by the fixed 128×128 grid. Invalid records contribute zero mask statistics and
remain in the census. After pooling noised sums, post-processing clips each nonnegative mask statistic
to the noised census, computes `2I/(P+R)` with denominator floor `1e-12`,
and clamps to `[0,1]`. `foreground_dice` is pooled foreground Dice, not mean
patient Dice. The both-empty public metric convention does not override this
private noisy-denominator rule. IoU, empty-mask strata and per-patient results are
not released by this layout.

Survival supports Weibull AFT, log-normal AFT and discrete hazard. Public horizons
must increase within `[t_min, horizon]`; default is the model horizon. The NLL
bound is public, in `(0,1000]`, default 20. Each valid patient's NLL is clipped
to `[-C,C]`, normalized to `[0,1]`, summed and noised. Post-processing transforms
its ratio to the noised valid count back to `[-C,C]`. A nonpositive noised count
gives a missing metric. Hazard likelihood follows the fitted contract's division
by the public number of intervals.

For Brier at `h`, status is known when an event occurred at/before `h`, or
follow-up reached `h`. The target survival status is zero for such an event and
one otherwise, including censoring exactly at `h`. Only known statuses contribute
squared error and an eligibility indicator. Ratios of pooled noised sums give
`metrics$brier$scores`, with public `horizons` and noised `eligible_n`.
This is **observed-status Brier**, not IPCW Brier; it does not correct censoring
selection bias. No private censoring model is fitted.

```r
survival_model <- ds.flower.model.pytorch_aft(horizon = 10, distribution = "weibull")
survival_cv <- ds.flower.cross_validate(
  conns, symbol = "subjects", target = c("time", "event"),
  features = c("x1", "x2"), model = survival_model, task = "survival",
  feature_bounds = list(lower = c(-1, -1), upper = c(1, 1)),
  survival_horizons = c(5, 10), survival_nll_bound = 20,
  folds = 2L, rounds = 2L
)
survival_validation <- ds.flower.validate(
  conns, symbol = "subjects_test", target = c("time", "event"),
  model = "client:/analyst/public/survival_bundle.zip",
  survival_horizons = c(5, 10), survival_nll_bound = 20
)
```

Concordance is pairwise. These private tracks do not release the concordance
index; it remains a public-split/authorized analyst-local metric. Private HPO for
survival and segmentation remains outside these contracts.

## Tabular bundle profile

The shared `dsflower-public-initialisation-bundle/v1` envelope admits
`role = "tabular_model"`, `model_id = "declarative_neural"`. It contains
`checkpoint_id`, `model_spec`, its canonical `model_spec_sha256`, `model_config`,
`feature_contract`, `dataset`, `licence`, `checkpoint`, ordered `tensors`,
`evidence`, `pretraining_protocol_sha256` and `creation`. This profile has no
encoder file. The model specification must be accepted by the trusted declarative
builder and reproduce every tensor shape.

`model_config` pins `loss-name`, `num-features`, `num-classes`, `num-labels`, plus
`survival-config-b64` for survival. The matching loss constant (`nb-dispersion`,
`gamma-shape`, `huber-delta` or `quantile-level`) is also bound; omitted constants
use their trusted defaults and have the same identity as explicit defaults. `feature_contract` contains ordered `features`,
`feature_lower`, `feature_upper`, `target_levels`, `target_bounds` (unused fields
are null). These must match the requested training/validation semantics.
`checkpoint.npz` contains ordered little-endian float32 tensors named `0`, `1`,
etc.; each manifest tensor declares its shape, dtype and byte SHA-256.

The six evidence files remain mandatory: original manifest, protocol, provenance,
audit, licence and mirror metadata. Artifact records pin filename, byte size and
SHA-256; the original manifest binds checkpoint/tensor/model-spec digests,
dataset and protocol/provenance/audit evidence and declares `public_nonprivate`.
The archive has an exact closed roster and the existing bounded safe parsing
(64 MiB total bundle, 2 MiB checkpoint/evidence member limit).
The trusted `segmentation_checkpoints` module's `pack`/`inspect` operations verify
both profiles. The synthetic fixture at
`tools/integration/validation-cv-fixture.py` illustrates the tabular schema; its
synthetic provenance is test evidence, not a template for claims about real data.

The [design note](DESIGN_VALIDATION_CV.md) records the privacy reasoning and
release identity. Synthetic tests establish implementation behavior, not model
utility or live Opal/Armadillo deployment.

## Content-based assignment and sensitivity in 0.7.3

The selected source-unit multiset is canonicalized before pooling, prediction or
partitioning. Row tokens bind selected content and duplicate occurrence; patient
tokens retain canonical grouping IDs. Row shuffling, symbol aliases and run tokens
preserve assignments. A changed row can move to another fold; changing selected
columns can change all row tokens. Model settings and holdout fraction do not
redraw the score. Default neural starts are specification-seeded and all CV folds
share the same initial model. These 0.7.1 rules are unchanged.

### Numeric holdout layout and exact sensitivity

Version 0.7.3 introduces the explicit numeric holdout layout
`validation-vector-v4`. It applies to row and patient holdout for bounded
regression, gamma and count models, including native-tree regression. Normalize
clipped targets and predictions to `y,p` in `[0,1]`, set `e = |y-p|`, and write

```
s = (e, e^2, y, y^2)                 regression/gamma/native-tree regression
s = (e, e^2, y, y^2, deviance/cap)   count
c = (1, s - 1/4)                    released contribution
```

The count deviance and its public cap are unchanged. The first coordinate
remains one per privacy unit. The fixed offset `1/4` is part of the trusted
layout, with no analyst setting. Each other coordinate now lies in
`[-1/4, 3/4]`; this translates the original bounded domain and does not loosen
any bound or change clipping. The zero contribution for a unit outside test, or
an empty test subset, remains the all-zero vector: no offset is subtracted for
an absent unit.

Let `d` be the number of coordinates after the count: four for regression and
five for count. For two present units,

```
||c(u) - c(v)||_2^2 = ||s(u) - s(v)||_2^2 <= d.
```

For one present unit versus an absent contribution,

```
||c(u) - 0||_2^2 <= 1 + d (3/4)^2.
```

Thus the required sensitivity is `max(sqrt(d), sqrt(1+9d/16)) = sqrt(d)`.
These are exact uniform bounds for these implemented layouts, including the
relationships between their coordinates, rather than just unattainable box
corners. For regression, `(y,p)=(1,0)` has all four statistics equal to one;
`(y,p)=(0,0)` has all four zero. This attains both the replacement diameter and
the stated absent radius. For count, the admitted public target bounds `[1,10]`
make the cap equal to the deviance at `(target,prediction)=(10,1)`. That record
has all five normalized statistics equal to one, and `(1,1)` has all five zero.
They attain both bounds. At some particular public count bounds, the coupling
of deviance to error makes a smaller diameter possible; the fixed layout uses
the tight bound uniform over all admitted public bounds, with no new
bound-dependent privacy calibration.

| Numeric holdout layout | 0.7.1 replacement diameter | 0.7.1 absent radius / calibration | 0.7.3 absent radius | 0.7.3 calibration |
| --- | --- | --- | --- | --- |
| Regression/gamma | `2` | `sqrt(5)` | `sqrt(13)/2` | `2` |
| Native-tree regression | `2` | `sqrt(5)` | `sqrt(13)/2` | `2` |
| Count | `sqrt(5)` | `sqrt(6)` | `sqrt(61)/4` | `sqrt(5)` |

The replacement diameter is unchanged in 0.7.3. For a patient with multiple
records, use its average bounded contribution. An average lies in the convex
hull of the row contributions; neither its norm nor the diameter between two
such averages exceeds the same bounds. Singleton patients attain the examples
above. Therefore the change benefits patient holdout as well as row holdout;
it never substitutes a row bound for an unbounded sum of patient records.

### All neighbour cases under the content-based split

Fix the public evaluation model, including any already-DP trained model. Match
unchanged source units using the canonical multiset and duplicate-occurrence
rules. Their assignment and contribution multiset is unchanged. Under one
replacement of a complete privacy unit, write its old and new test indicators
as `b,b'` in `{0,1}`. The change in the test sufficient vector is exactly
`b*c(u) - b'*c(v)`:

| Old/new membership | Change | Bound |
| --- | --- | --- |
| Test / test | `c(u)-c(v)` | replacement diameter `sqrt(d)` |
| Train / train | `0` | zero |
| Test / train | `c(u)` | absent radius `sqrt(1+9d/16)` |
| Train / test | `-c(v)` | absent radius `sqrt(1+9d/16)` |
| Present test contribution / absent, including an empty test subset | `+/-c(u)` | same absent radius |

A move across the split is one deletion from or one insertion into the test
sum, not two test contributions. A replacement within train can change the
trained model, but training already has its own unchanged DP mechanism and
budget. Evaluation has the stated bound for every fixed model, so its
sensitivity argument applies in the existing adaptive composition with
training. This does not condition away or omit the training charge. The
absent-contribution bound covers internal changes of test membership; it does
not expand the global fixed-census replacement contract to an unaccounted
add/remove contract.

Both empty and nonempty holdout paths keep
`include_zero_neighbor=True`. The Gaussian calibration function takes the
maximum of replacement and absent bounds; the accountant, epsilon/delta policy,
training/metric budget split and number of releases are unchanged.

### Exact recovery and the choice of offset

The node releases one vector `Z = sum(c) + Gaussian(0, sigma^2 I)`. For each
statistic the researcher recovers

```
N_hat = Z[0]
S_hat[j] = Z[j] + (1/4) * Z[0]
```

using the signed released count before any projection or clamping. Without
noise this is the exact original sufficient vector, including patient-average
statistics. With noise it is an unbiased estimate of that original vector:
`E[N_hat]=N` and `E[S_hat[j]]=S[j]`. Only these reconstructed sufficient
statistics feed the existing metric formulas and feasible-domain projection.
No exact count or second private release is used. The metrics retain the same
noiseless meaning. Ratios, clipping, square roots and R-squared were already
nonlinear post-processing and are not in general unbiased estimators; the
layout conversion adds no deterministic bias and does not claim otherwise.

This choice accounts for the noise added back with the released count. If the
common Gaussian multiplier supplied by the unchanged accountant is `k`, then
`sigma = k * sensitivity`. Recovery gives
`Var(S_hat[j]) = sigma^2 * (1 + a^2)` for offset `a`, with covariance
`a^2 sigma^2` between different reconstructed sums and covariance `a sigma^2`
with the count. Pooling independent node releases preserves these statements
with their variances summed.

| Layout | 0.7.1 released coordinate variance / `k^2` | 0.7.3 released coordinate variance / `k^2` | 0.7.3 recovered sum variance / `k^2` |
| --- | --- | --- | --- |
| Regression/gamma/native-tree regression | `5` | `4` | `17/4 = 4.25` |
| Count | `6` | `5` | `85/16 = 5.3125` |

The Gaussian standard deviation therefore returns to the pre-0.7.1 level,
while the marginal variance of each reconstructed sufficient sum also improves
on 0.7.1. These marginal statements are not a universal error guarantee for
every nonlinear metric or every linear combination of the correlated recovered
statistics.

Alternatives considered:

- **Midpoint centering (`a=1/2`).** This is private, with exact absent radii
  `sqrt(2)` and `3/2`, and the same calibrated sensitivities `2` and `sqrt(5)`.
  But recovered sum variances become `5 k^2` for regression and `6.25 k^2` for
  count: no regression improvement on 0.7.1 and a count regression. The quarter
  offset already makes replacement dominate, with less reconstruction noise.
- **Smallest uniform offset.** The coordinate-bound calculation needs
  `a >= 1 - sqrt(1-1/d)`, approximately `0.133975` for regression and `0.105573`
  for count. These boundary choices, or smaller safe rational choices than a
  quarter, further reduce recovery covariance. The fixed dyadic quarter is
  chosen for simple exact representation, a strict margin below the replacement
  bound, and one clear contract across all affected holdouts. It is not claimed
  to optimize metric utility.
- **Keep the unshifted vector and lower sigma.** Unsafe: the endpoint witnesses
  above attain the larger absent radii when a unit enters or leaves test.
- **Position-based or repeatedly resampled splits.** This would lose the
  required row-order invariance or create fresh releases through nuisance
  inputs. Stable extra row identifiers would require a new trusted input
  contract. Neither is necessary for this fix.
- **Padding, a separate count release or a different Gaussian covariance.**
  These require a different sufficient-vector or noise mechanism and a separate
  joint analysis. The reversible fixed translation obtains the requested
  sensitivity with the existing isotropic Gaussian calibration and one release.

### Layout compatibility, validation and pooled OOF

Only the new numeric holdout layout carries `validation-vector-v4`. An old
unversioned layout retains its unshifted meaning and original absent bound;
unknown versions are rejected. The discriminator is included in the existing
canonical layout and public release request identity, separating legacy and v4
releases, including their persistent neighbourhood anchors. The request identity
remains `private-validation-vector/v3`; it already binds the effective layout.
Bare wire vectors and ephemeral cached replies do not carry a layout discriminator:
the same five- or six-element geometry alone cannot prevent an old vector from
being decoded with v4 semantics. Ordinary cross-version execution is prevented by
exact runner hash admission, staging pins and the import guard, together with
normal staging/state cleanup. Importing or migrating raw vectors or ephemeral
caches would require an envelope that includes and verifies the layout version;
these representations are not self-describing. The content binding, sticky replay,
row-order invariance and FedProx contracts are unchanged. A layout version is a
fixed implementation contract, never a new analyst-controlled privacy input.

Standalone validation and pooled OOF retain their original numeric layouts.
They already use the replacement diameter at fixed unit count, so shifting
would not reduce their Gaussian scale and would add count noise during
recovery. Condition on the already-DP public fold models. A pooled OOF
replacement changes one contribution from `c(u,M_a)` to `c(v,M_b)`, even if the
fold changes. Both lie in the same bounded contribution set; patient means lie
in its convex hull. Every source unit contributes once to the pooled OOF sum.
The replacement diameter remains valid with no factor of two or K and no
per-fold metric release. Raw fold vectors are accumulated in fold order and
the ordered public model hashes are bound.

Other layouts retain their complete existing representation and bounds:

| Layout | Replacement bound | Absent bound | Holdout calibration | Validation / pooled OOF calibration |
| --- | --- | --- | --- | --- |
| Binary | `sqrt(2)` | `1` | `sqrt(2)` | `sqrt(2)` |
| Multiclass/ordinal, J classes | `sqrt(2(J+1))` | `sqrt(J+1)` | `sqrt(2(J+1))` | `sqrt(2(J+1))` |
| Multilabel, L labels | `sqrt(2L)` | `sqrt(L)` | `sqrt(2L)` | `sqrt(2L)` |
| Segmentation | `sqrt(3)` | `2` | `2` | `sqrt(3)` |
| Survival, H horizons | `sqrt(2+2H)` | `sqrt(2+2H)` | `sqrt(2+2H)` | `sqrt(2+2H)` |
| Legacy regression/gamma/native-tree regression | `2` | `sqrt(5)` | `sqrt(5)` | `2` |
| Legacy count | `sqrt(5)` | `sqrt(6)` | `sqrt(6)` | `sqrt(5)` |

For histogram layouts, each unit contributes one unit of mass per histogram
family, so the existing absent radius is already below the replacement bound.
Survival includes an actual zero contribution for an invalid record; the
existing bound covers that case already. A shifted segmentation holdout could
also reduce its bound, but it would change a separate patient-mask metric
contract outside the numeric noise regression addressed here. No improvement
is assumed for those untouched layouts, and no bound is loosened.

The changed unit participates in all K training comparisons: two complements
can gain/lose it and the others replace it. Training still receives
`0.8*epsilon/K, 0.8*delta/K` per fold. Neural sampling geometry uses the fixed
parent population, with the existing add/remove conversion and empty-complement
noise schedule. The sole OOF vector receives 20% of the job budget. Native
training sensitivities already cover an absent unit and are unchanged. Complete
parent source/assignment bindings are checked across phases; a mid-job change
fails closed.

## Neighbourhood replay and transcript limitation

Version 0.7.2 mitigates the equality oracle described in
[isglobal-brge/dsFlower#7](https://github.com/isglobal-brge/dsFlower/issues/7)
with immutable neighbourhood anchors. For each public request R (including its
incoming model and round), the node scans **all** retained anchors and returns
the complete stored payload of the **oldest** anchor at distance `d < k`. If none
is eligible, it computes the unchanged v3 content-bound release and appends one
new anchor. Near inputs never become anchors. Newer anchors cannot displace an
older eligible anchor, so replay is stable without per-input bindings.

Distance counts unordered canonical privacy units with multiplicity:
`d = max(|M| - c, |N| - c)`, where `c = sum(min(M[t], N[t]))`. One insertion,
deletion or replacement counts once; in patient mode a patient's complete
selected records form one unit. The default `k` is the node's `nfilter.subset`
(or its `default.` option), otherwise 3, with a floor of 2. Custodians can set
`dsflower.neighbourhood_k` (or `default.dsflower.neighbourhood_k`); the effective
value is frozen per R. Analysts cannot change it or force refresh.

An analyst can no longer test a one-unit difference against an existing release
by obtaining a fresh answer inside that anchor's neighbourhood. This is a
**mitigation, not transcript DP**: the hard `k-1`/`k` boundary and boundaries
between anchors still distinguish some one-unit neighbours. An input must be
at least k from **every** anchor to receive a fresh release. Small updates can
therefore return stale models or statistics; uncertainty intervals do not
include this staleness. No hit/near/fresh status, distance or anchor identifier is
returned. Timing and availability remain outside the guarantee.

The rule covers every neural DP-SGD round, gated Hook release, all five native
tree engines, private validation, holdout, CV fold training and OOF output, and
association. Each round/fold/evaluation has its own R. Completed federation
trajectories replay when the same incoming public models and eligible anchor
choices recur; this is not a universal neighbouring-world transcript claim.
Calibration, sensitivities, accounting, FedProx and the fresh R/B/K identity
remain unchanged. Conditional DP mechanisms compose under their existing
assumptions; no lifetime privacy budget is introduced.

Holdout and CV anchors use complete canonical parent source units, not only the
current fold's records or reduced validation vector. Training folds and the final
OOF release have separate public R values; the final R includes the ordered
public fold-model contents. Every hit still performs current-job admission and
source-integrity checks. Private fold accumulation is not an extra release, and
OOF replay still completes/purges that job's accumulator. An anchored training
model and separately anchored validation result need not describe one coherent
current-data snapshot. Original fixed-count adjacency, absent-unit holdout
sensitivity, CV replacement bounds and privacy allocations remain unchanged.

The permanent node-local store defaults to `<node-secret-path>.neighbourhood`.
First release initializes it automatically and pins its random UUID at
`<node-secret-path>.neighbourhood-id`, beside the secret rather than inside the
store. The store uses owner-only directories/files (`0700`/`0600`), keyed unit
fingerprints, MAC-authenticated records and complete payload bytes; it never
stores raw source records or noise seeds. Records are verified before decoding,
and a per-R lock serializes selection and durable anchor commit before release.
There is no eviction, expiry or per-attempt ticket.

The default limits are 256 anchors per R and 64 GiB of store capacity. Fresh
commits must fit both logical retained bytes (payloads, fingerprints and record
overhead) and SQLite allocated pages plus fixed state headroom. Provision extra
physical disk space for transient SQLite journals and filesystem allocation slack. Only an
input that would create a new anchor is refused at a limit, with one stable
error; exact and near replays remain available. A refusal substitutes for a
fresh release and reveals the same fact that the input is far from every anchor.
The shared byte cap also exposes a weak cross-user aggregate signal about prior
store growth. These resource settings are not privacy parameters. Increase
capacity with all retained state intact; never delete anchors to make space.

Missing or corrupt established state, missing original keys, unsafe permissions
or a mismatched UUID fail closed. The permanent
`<node-secret-path>.neighbourhood-id.lock` also detects loss of both the store
and local UUID pin. Optional `dsflower.neighbourhood_store_id` externally pins
the UUID and detects loss of all local store markers; without it, losing the
store, UUID pin and initialization lock together can resemble first use. Stop
all workers before restoring a consistent backup of the secret, store, UUID pin,
permanent locks and retained Hook cache. MACs do not detect rollback to an older
complete valid snapshot: avoiding rollback remains a custodian/storage assumption.
Runtime upgrades create new public request domains, so drain jobs and upgrade
both packages together; retain old state and treat new domains as additional
releases. Existing per-release DP does not prove the private anchor-selection
transcript DP.
