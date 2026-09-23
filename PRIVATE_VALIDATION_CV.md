# Private validation, holdout and cross-validation in 0.7.0

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
