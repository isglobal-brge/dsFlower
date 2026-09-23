# dsFlower privacy and security architecture

This document describes the enforced contract implemented by the current
`dsFlower`/`dsFlowerClient` runner. It is a security specification, not a list of
aspirational features.

## 1. Security objective

Private rows remain on the data node. Every numeric model release that leaves a
SuperNode is produced by a node-installed, hash-pinned runner under an explicit
`(epsilon, delta)` guarantee. The researcher controls what valid computation is
requested; the data custodian controls the privacy policy and executable trust
boundary.

The trusted computing base is:

- the node administrator, R/DataSHIELD service, dsFlower package and Python
  environment;
- the canonical `dsflower_runner` whose recursive SHA-256 is computed by the
  node;
- the operating-system entropy source, persistent node noise root and, for
  HookApps, the attested OS sandbox.

The researcher, Flower ServerApp, submitted configuration and uploaded HookApp
code are not trusted. A compromised node administrator, kernel or leaked node
secret is outside this guarantee.

dsFlower uses node-side central DP: each data node is a trusted curator for its
local dataset and applies DP before egress. This is not formal local DP (LDP),
where every individual randomizes their own record. dsFlower does not implement
Secure Aggregation. A coordinator can therefore observe each already-private node
update. Public coordinators are rejected unless the node administrator explicitly
sets `dsflower.allow_untrusted_coordinator = TRUE`.

## 2. Computation contracts

### Declarative DP training (recommended)

The researcher supplies data-only model and training specifications. The node
builds and executes the computation with the canonical runner:

- neural/vision models use Opacus DP-SGD with per-example or per-patient gradient
  clipping and a CSPRNG-backed Gaussian stream;
- only allowlisted, per-sample-safe operations and losses are admitted;
- the manifest pins mechanism, model spec, loss, batch size, local epochs,
  horizon, feature count, optimizer/scheduler configuration and public
  preprocessing bounds before execution.

This contract reaches `nn.Module`-level granularity because the trusted runner
owns the training loop and observes per-sample gradients. Extending the
declarative vocabulary is the safe way to add flexibility.

The separate native-tree ABI exposes only reviewed node-owned mechanisms:
curated native XGBoost, data-independent ExtraTrees, adaptive private Random
Forest, and dsFlower's LightGBM-style and CatBoost-style numeric boosters.
Random Forest assigns every effective privacy unit to exactly one tree through
a node-owned disjoint partition; it is not upstream bootstrap/bagging Random
Forest, so small cohorts can have fewer effective units per tree. Its public
defaults are benchmark-oriented, never a private-count admission rule.
Public bounds, cuts and the exact typed profile are validated before private
reads. Each engine has its own accounting and sanitized data-only artifact;
LightGBM-style and CatBoost-style deliberately do not load or emit the upstream
binary formats. Runtime availability is a fresh executable probe, not a
server-side model catalogue or privacy permission list.
The standard installer provisions the four pure dsFlower engines. XGBoost stays
fail-closed until a custodian explicitly configures its separately built,
platform-specific verified bundle; `configure` never downloads or compiles that
native trust artifact implicitly.

### Subject-level survival

`pytorch_aft` and `pytorch_discrete_hazard` use the existing neural DP-SGD
mechanism and require custodian-owned `dp_unit="patient"` and `patient_column`.
The public contract declares two distinct ordered time/event targets, explicit
baseline features, days from baseline, minimum resolution and administrative
horizon. Versioned `survival-config-b64` is strictly validated before private
reads; the decoded `survival-config` and all derived artifact fields are
server-owned. AFT pins Weibull shape or log-normal sigma to `{0.5,1,2}` and
clamps the scalar location to `[-10,10]`; dispersion is never fitted privately.
Cox losses remain outside the allowlist because risk sets couple subjects.

The original staged frame retains `M` source rows (`n_samples=M`) and the
canonical subject census (`n_units=N`). A separate server-written
`survival_subjects.csv` holds exactly `N` subject feature/outcome rows. Duplicate
subjects are invalid as a whole; missing identifiers retain the existing common
protected sentinel and are invalid even when only one such row occurs. Invalid
or nonfinite outcomes, nonbinary event codes and times below public `t_min`
produce safe features and targets with `valid=0`. Those units stay in sampling
and the batch-mean denominator. Finite times beyond the public horizon become
administratively censored at the horizon; an event exactly at the horizon
remains an event. No completeness, event or invalid-record counts are released.

The hazard grid has `0=b0<...<bK=T` with `K<=64`. An event in
`(b[j-1],b[j]]` contributes through interval `j`; a censor contributes only
completed intervals whose end is no later than censoring. In-interval censoring
does not label the unobserved interval end. Each subject loss is
`valid * sum(mask * BCEWithLogits(logits,event)) / K`, followed by a mean over
subjects. A whole period vector is one Opacus sample with one all-parameter
clip. `K` never replaces `N` in sampling, clipping or accounting; budget
conversion, secure Poisson sampling and the accountant are unchanged.

The runner independently checks the source census, subject order, complete
derived tensor geometry and totalization. Sticky identity binds distribution,
dispersion, public time/grid conventions, validity and effective feature/target
tensors. Only DP parameters and existing availability metadata leave the node;
no training losses, subject weights, survival predictions or outcome tensors
are added to the training release. Private validation, holdout and CV add one
fixed-layout Gaussian release for observed-status Brier and bounded fitted NLL.
Concordance stays on public/authorized local splits and private HPO remains
unsupported. [The validation design](DESIGN_VALIDATION_CV.md) derives sensitivities.

### HookApp

A HookApp exposes only:

```text
initial_arrays(config, input_dim) -> numeric arrays
local_update(global_arrays, X, y, config) -> numeric arrays
```

It is not a general node-side Flower App. The node never imports its
`local_update` in the trusted parent. It verifies the uploaded ZIP and package
hash, launches a fresh isolated child with a minimal environment, validates the
numeric result, clips the complete update to the `C` ball, and applies an
RDP-calibrated Gaussian mechanism with sensitivity `2C`.

The analyst may supply one canonical JSON-like `app_params` object, but keys
related to privacy, paths, dependencies, secrets, runtime selection and rounds
are rejected. The trusted parent supplies `round_index`, `num_rounds`, task and
class count. The initial and every update array have fixed count/shape, and the
per-round noise scale composes all `num_rounds` releases within that training.
A child failure maps to the zero update and still traverses that
numeric mechanism.

When an attested Bubblewrap filesystem/network sandbox and a minimum-duration
timing envelope are available, the node may run the hook on a fixed, public `k` disjoint,
isolated blocks and release the clipped mean at conservative sensitivity
`min(2C, 4C/k)` (a changed patient identifier may affect two blocks). `k` is
administrator-pinned and never derived from private cohort size, because a
data-dependent Gaussian variance would itself leak.
Without all required controls, HookApps are not executed and the Flower operation
is marked unavailable; the incoming public model is not accepted as a trained
release.

The timing envelope is a defense-in-depth lower bound, not a formal
constant-time guarantee. Cleanup, resource exhaustion and availability remain
outside the numeric mechanism proof unless the deployment adds system-level
quotas and an outer release deadline.

Arbitrary code cannot generically receive DP-SGD granularity. Static inspection
cannot prove that a custom training loop has per-sample-independent gradients or
that it has no side channel. To obtain Tier-1 granularity, express the operation
as a declarative spec or add a custodian-reviewed operation to the canonical
runner.

### Private validation

Validation is its own one-release DP track. The ServerApp loads a saved public
declarative artifact and sends it to every selected node. Before opening private
data, each ClientApp validates the model, feature/loss/task geometry and fixed
metric layout. Each row or configured patient then contributes one bounded
histogram/sufficient-statistic vector; the node releases only its Gaussian-noised
sum. The ServerApp pools vectors only when every expected node returns the exact
fixed geometry, and computes metrics as post-processing. Otherwise it writes the
public result `available=false` with no metrics, node status or zero-filled
substitute; this operational availability signal is outside the DP transcript.
Exact labels, predictions, counts and node metrics are never released.

The current track validates tabular neural artifacts, saved first-party vision
artifacts and sanitized native-tree ensembles. Supported layouts cover binary,
multiclass, ordinal and multilabel classification plus bounded regression/count
outcomes. Probability bins are public and bounded at 512; class/label counts are
public and bounded at 1024. Validation on an independently assigned dataset is
external validation; evaluating training data is resubstitution. Eligible tabular
classification/regression/count and native dsFlower vision training have an atomic holdout
workflow. K-fold is tabular-only and supports neural models plus native-tree
binary classification and bounded regression. K-fold runs use a
canonical secret-keyed patient/row assignment, cleanly initialize and train all
fold models inside one job, keep raw OOF sufficient statistics in node memory,
release one final DP vector per node only when every fold succeeds, and publish
one pooled metrics artifact. Fold models, predictions and per-fold/per-node
metrics are never released.

### Atomic training holdout

Eligible tabular classification/regression/count and native dsFlower vision
training may opt into one
holdout fraction. The client encodes that fraction exactly as integer
millionths and the node combines
the canonical contract with its custodial secret in an HMAC-SHA256 PRF. There is
no submitted seed, run identifier, clock, database, counter or history input.
Row mode hashes the stable staged row ordinal; patient mode hashes the canonical
patient identifier, so every row for one patient is assigned to the same side.
Repeating the same contract under the same node secret recreates the same
partition. The fraction changes only the PRF threshold, not its domain, so
fractions are nested instead of acting as analyst-controlled partition rerolls.
Assignments are intentionally node-owned; duplicate people observed at different
nodes are not linkable or jointly assigned without a separately governed
cross-site identity protocol.
Row ordinals are stable only for the staged dataset contract: reordering rows
constitutes a different dataset and can therefore change row-level assignments.

Assignment happens before patient pooling and before any training step. Every
neural round, or the single native-tree round, trains only on the complement.
After training completes, the ServerApp sends the final aggregate once to the
exact training roster. Each node validates that public artifact before reading
the test side, permits only one artifact identity with exact in-memory replay,
and releases one fixed-layout DP sufficient-statistic vector. The ServerApp
retains those vectors only in memory, pools them, and derives the task-appropriate
metrics as post-processing. Labels, predictions, unit assignments, per-node
metrics and per-node vectors are never written to the result.

The custodian's per-training epsilon/delta pair is the total job budget. The
manifest applies the fixed 80/20 split between the training mechanism and the
one holdout vector; this is ephemeral composition within that job, not a
lifetime or resource balance. The result directory uses `history.json` as its
commit marker: the trained model and pooled `holdout.json` are accepted by the R
client only when both have been produced in the same run. Native-tree acceptance
also binds the exact resampling contract, request, public schema, sanitized
artifact and node count. A failed evaluation therefore yields neither an
accepted model nor metrics.

This release implements atomic holdout for tabular declarative neural/native-tree
models and native dsFlower vision models with their exact extractor profile.
HookApp backends fail explicitly before private preparation and are not
advertised as holdout-capable. K-fold cross-validation supports tabular neural
models (including survival), patient segmentation and native-tree binary
classification or bounded regression. Ordinary image classification CV remains
unsupported. Both admitted public starts apply independently to every fold.
Extending the same engine-agnostic resampling contract to another backend
requires a reviewed backend-specific training/evaluation adapter; accepting a
contract without executing both sides is forbidden.

### Binary 2D segmentation

The experimental `pytorch_resnet18_segmentation` contract uses the neural
mechanism with a mandatory custodian-selected patient privacy unit. It is
implemented but not promoted (`vetted = FALSE`): the packaged v5 public-data
benchmark evidence remains subject to reviewer acceptance. Private validation,
atomic holdout and CV use bounded patient intersection/prediction/reference
statistics for pooled foreground Dice; private HPO remains unsupported. See
[the metric and initialisation contract](DESIGN_VALIDATION_CV.md).

The frozen `resnet18_layer2_128_v1` encoder resides outside the released module.
It runs under `eval()` and `no_grad()` with fixed pretrained BatchNorm statistics.
The public ResNet18 IMAGENET1K_V1 checkpoint SHA-256 is
`f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec`.
The decoder maps 128-by-16-by-16 features to a 1-by-128-by-128 logit image using
three convolutions and nearest-neighbour upsampling. It has no BatchNorm or
frozen encoder buffers. The trusted BCE/Dice mixture reduces over pixels within
each subject before the subject mean; batch-wide Dice is excluded.

Staging keeps all M source rows and the separate N-subject census. The trusted
runner selects one image per subject by the pinned canonical image-ID ordering,
unions all masks on ties that identify the same image path, and produces exactly
N subject tensors. Conflicting paths, invalid image IDs, unavailable/corrupt
assets, incompatible original geometry and out-of-vocabulary masks map to fixed
safe tensors with validity zero. The subject remains in sampling and accounting.
An explicit empty annotation authorizes an absent mask only when its original
path was absent/blank; it cannot conceal an invalid supplied path.

Direct-table preparation requires the custodian image and mask root options.
The dsImaging route verifies image/sample/mask roles against the admitted
descriptor and preserves the declared `mask_root` asset. Both use the existing
containment checks, and segmentation does not guess replacement filenames for
invalid paths. Individual S3 download failures remove partial files and become
invalid records. Numeric-looking identity strings retain their exact text across
CSV staging. The source census, masks, features and validity summaries remain
node-local. Effective tensors, selection/profile/preprocessing and checkpoint
pins contribute to sticky semantic identity; path names alone do not identify
the training data.

The decoder defaults to `decoder_init = "random"`. Two digest-bound public
initialisation routes are available in 0.7.0:

- `client:<path-to-bundle>` validates researcher-local public material and declares
  its provenance and digests. This route supports research iteration.
- `resource:<handle-symbol>` uses a custodian-registered checkpoint resource admitted
  through `datashield.assign.resource()` and `flowerCheckpointInitDS()`. This route
  supports curated institutional material, large artifacts, and nodes that admit
  no analyst-supplied material. The coordinator independently obtains the same
  public weights through its local `public_checkpoint_file` argument.

The custodian sets `dsflower.public_initialisation` to `analyst_or_resource`
(default), `resource_only`, or `none`, optionally overridden by
`dsflower.public_initialisation.pytorch_resnet18_segmentation`. Analysts cannot
change this policy. The former `public:<id>` selector, registry beside the node
secret, installer and manifest allowlist no longer authorize initialisation.

Both routes use a complete verified bundle, including the frozen encoder,
checkpoint, manifest, provenance, licence, protocol and audit evidence. The node
verifies a protected snapshot before private staging; the trusted runner verifies
it again before private access and checks the first round's public arrays against
its admitted tensors. Missing or altered material fails closed. Status returns
public identity, provenance and geometry; it never exports checkpoint bytes.
Canonical content identity excludes resource names, session symbols, locations
and archive packaging, so aliases and repacking do not create another noise draw.
Run manifests and release records retain the origin and verified digests.

See [public initialisation procedures](PUBLIC_INITIALISATION.md) for exact
Opal, Armadillo and DSLite commands, bundle requirements and policy configuration.
The [BUSI reference records](inst/extdata/segmentation-public-checkpoints/README.md)
retain the original hashes and evidence. The three original decoder binaries are
still absent; synthetic tests do not establish recovery of those checkpoints.
Public initialisation does not change the DP accountant, clipping, sampler,
training algorithm, release cache or identity-v2 machinery. Segmentation remains
experimental; the mirror's licence declaration retains its original qualification.

Private-data-dependent releases are restricted to DP-trained decoder parameters;
public reconstruction metadata and verified public provenance may also be returned.
Checkpoint bytes are never returned by the resource or status interfaces. Public or authorized local evaluation uses the
canonical output grid, threshold 0.5 and subject-mean Dice/IoU, with foreground
and empty-mask strata.
Both-empty Dice is 1 and one-empty Dice is 0; these are not private metric routes.

## 3. Per-training privacy

The custodian pins a positive epsilon/delta pair for each training. Its rounds
are composed by that training's mechanism. The runtime maintains no cumulative
privacy-budget balance or cross-training accounting ledger. Bounded coordinate
claims and cached releases protect against duplicate releases and support exact
retries; they do not account for cumulative privacy expenditure. Distinct
trainings compose sequentially when they are analysed together; parallel
composition applies only to genuinely disjoint populations.

If one person occurs at multiple observed nodes, their per-node guarantees also
compose sequentially. A federation-wide guarantee over overlapping sites
requires a separately governed person-level accounting system outside this
package. That external system is not a prerequisite for dsFlower training.

## 4. Deterministic randomness

Determinism is retained as semantic-scoped, secret-keyed randomness:

```text
release_key = HMAC-SHA256(noise_root,
                          protocol_version || mechanism || semantic_id)
subkey      = HMAC-SHA256(release_key, mechanism_axis)
```

The dedicated node secret is 32 bytes from the operating-system CSPRNG, created
at runtime and never exposed to a HookApp. Unix stores it outside staging with
mode `0600`; Windows uses a protected ACL restricted to the service identity,
SYSTEM and Administrators. Its path is checked before and after opening and may
not cross a symlink or Windows reparse point. There is no fallback to R's RNG, a client seed,
`datashield.seed`, a predictable constant
or a secret baked into a container image.

The official image wraps Rock's existing service-start hook, which already runs
as the `rock` UID after runtime mounts are prepared. If a deployment explicitly
provides the key-path environment variable, that hook initializes the secret
before the service opens its port. Otherwise it defers until the first
`flowerInitDS()`, when DataSHIELD profile options are available. `configure`,
`.onLoad()` and Docker build never create privacy state; both image builds contain
negative assertions for the key. If runtime storage is unavailable, Rock
remains up for operational repair, while private entry points retry and remain
fail-closed.

Missing, malformed or permissively-mode'd service-owned regular keys are
atomically regenerated. Symlinks, foreign-owned files and unsafe parents remain
fail-closed. Rotation starts an independent deterministic-noise domain and never
introduces a query-count lockout.

DP Gaussian values come from a ChaCha20 stream with domain-separated subkeys.
Poisson sampling and HookApp partitioning use their own ChaCha20 subkeys. Torch
initialization/dropout is data-independent and uses a separate HMAC-derived seed
through the framework PRNG; it is not a DP-noise source. No privacy-critical
stream is reused. The canonical identity includes the effective configuration,
per-training policy, round, incoming public arrays, transformed or patient-pooled
private tensors and a runtime fingerprint. The `dsflower-semantic-randomness-v2`
contract also binds a public request-selection block derived from the node's
server-authored manifest: target columns (including ordered survival time/event
roles), ordered feature columns, patient column and unit policy, public target
vocabularies/bounds, imaging asset aliases and path/id/mask column roles, and
holdout/CV geometry and assignment contracts. Public segmentation initialisation
also binds its origin and versioned canonical manifest, checkpoint, tensor-contract
and encoder digests; checkpoint labels are excluded.
Native engines bind their validated schema, engine parameters and contribution
policy, including the active CV fold; validation and association bind their
public request contracts even when their
private sufficient statistics coincide. Hook keys bind the verified uploaded
package contents as well as manifest selections and the validated update.
The server preserves selected source operands and descriptor identities in
protected `request-source` manifest metadata. Canonical selection/schema digests
keep large public column lists within the outer key encoder's limits. Pooled CV
also binds the ordered public fold-model digests held in existing node RAM.
The node never accepts an analyst-authored request-selection blob. Operational paths, tokens, message
IDs and timestamps are excluded, and the private digest remains node-local.

A single fixed noise vector for all distinct queries is unsafe because correlated
answers can cancel it. Sticky noise only solves repeated identical queries. In
dsFlower, equivalent canonical identities reuse one deterministic noise stream,
while distinct semantic identities receive domain-separated keys.
In particular, selecting different private columns cannot reuse a key merely
because their contents happen to match. Request-selection changes affect key
derivation only: noise distribution, calibration, accounting, clipping,
partition-assignment algorithms and training paths are unchanged. Earlier-runner
evidence remains valid as measurements of the same mechanism, although v2 draws
different deterministic noise realizations. See the runner's
[`SEEDING.md`](inst/flower_app/dsflower_runner/SEEDING.md) for the identity boundary.

Trusted built-in tracks request strict deterministic Torch kernels. HookApps are
seeded for Python, NumPy and Torch and bind final noise to the validated clipped
update, but arbitrary native uploaded code cannot be proven deterministic by
static inspection. Every admitted HookApp, deterministic or not, therefore uses
a durable node-owned release cache. Its domain-separated key is derived from the
v2 Hook master identity before execution. The cache stores exact final noised
array dtype, shape and bytes and constant reply metrics, including noised-zero
outcomes, without storing the master or noise keys. Exact retries replay that
release without executing the application; current Flower transport metadata is
reconstructed for the current request. Declarative tracks retain their existing
deterministic kernels and do not use this cache.

Cache capacity is reserved from public bounds before private work. Concurrent
identical requests serialize, and entries are pinned throughout every active
run that uses them. Only authoritative run closure releases those pins; crash
recovery retains uncertain pins. Eviction removes only unpinned entries, oldest
first. Exact retry thus applies to every Hook application throughout its active
run and across runs while its release remains retained. Evicted nondeterministic
releases cannot be reconstructed, so indefinite cross-run replay is not claimed.
Changed private data or request selections miss even after an in-memory reply,
and a committed coordinate cannot authorize a second release under another key.

Replay is deterministic post-processing of the first mechanism output, without
another observation from application randomness. It does not change clipping,
sensitivity, noise calibration or accounting, make distinct requests free, or
create a cross-training privacy budget. The numeric guarantee still assumes
cache lookup, hit/miss timing and storage failures do not become additional
private-dependent transcript signals. The existing minimum-duration envelope
remains defense in depth; the cache does not establish a fixed-duration deadline.

The Gaussian implementation uses a hardened Box--Muller transform over a finite
IEEE-754 support. The keyed ChaCha20 stream prevents prediction and averaging,
but this remains a practical computational-DP implementation rather than a
formally verified exact continuous Gaussian. A discrete-Gaussian or
interval-refining replacement must be introduced as a versioned mechanism with
matching sensitivity and mechanism proofs plus a versioned privacy contract.

## 5. Server-authoritative manifest

The client can request a valid declarative computation but cannot set or weaken:

- per-training epsilon/delta;
- clipping norm;
- number of rounds in the current training;
- DP unit, patient column and identifier canonicalisation;
- HookApp enablement, sandbox attestation, timeout or timing envelope;
- gated-release cache directory or byte capacity;
- exact metrics, counts, logs or feature statistics.

Server-owned structural manifest fields cannot be duplicated or overridden by
the client. The trusted runner validates the server-authored mechanism, round count,
epsilon, delta and canonical semantic identity before private computation.

`manifest.json` is a run-local staging contract, not cross-training privacy
history or a memoization database. It pins the effective schema, mechanism and
public parameters. A separate hidden SQLite ledger in the same private staging
directory atomically records the bounded release coordinates already claimed by
that run and is mirrored into Flower `NodeState`; this survives ClientApp
process restarts without becoming a budget shared across trainings.
For gated Hooks, committed coordinates additionally bind the semantic cache key;
an older-round retry may replay its retained release after data identity is
verified, but may never claim another key for that coordinate. Authoritative
cleanup closes cache admission for the run before removing inputs and the
per-run ledger and releasing cache pins. The durable cache lives separately from
staging. An equivalent new declarative training reconstructs the same semantic
PRF input; an equivalent new Hook training replays while its entry is retained.

The node pins the recursive runner hash. The client's bundled runner must be
byte-identical. The coordinated release check
`dsFlowerClient/tools/check-runner-sync.py --server ../dsFlower` verifies both
working trees before promotion. Uploaded
archives are capped while streaming, SHA-256 verified, safely extracted without
path traversal/symlinks/devices/ZIP bombs, scanned, re-hashed immediately before
execution and mounted read-only in the HookApp sandbox.

## 6. Data and release minimization

- Exact feature sums, sums of squares and sample counts are disabled. New runs
  may use analyst-supplied public lower/upper bounds; training and prediction
  apply the same clipping and affine transform.
- Node-side logs and metrics are not returned through DataSHIELD.
- Flower aggregation weights are fixed to one instead of revealing local cohort
  size.
- The node secret, staging path and run token are not returned.
- Patient identifiers are preserved only for local grouping, excluded from model
  features and selected only by administrator policy. The default is
  row-level. Patient mode requires one explicit `dsflower.patient_column`, a
  stable identifier roster across releases; there is no auto-detection or row
  fallback. Missing/empty/reserved identifiers are collapsed into one fixed,
  conservative sentinel unit, so they cannot create a prepare error oracle. A
  per-person interpretation still requires a complete roster. Unit, column and
  canonicalisation version are bound into the semantic mechanism identity.
- Target labels/ranges are public manifest inputs, not cohort statistics.
  Ordered `target_levels` define classification codes; regression/count requires
  finite `target_bounds`. Unknown/missing labels map to public code zero;
  non-finite/unparseable numeric targets map to the public-bounds midpoint and
  are clipped. Selected numeric model features use the public-bounds midpoint or
  zero; numeric values and public bounds are limited to magnitude `1e6` before
  float32 runner arithmetic. These maps are record-local and never drop a row.
- Declarative operations are total over that numeric domain: division uses a
  fixed denominator floor; every operation saturates non-finite/intermediate
  values; parameters/intermediates are bounded to `1e6`; and heads use `30` for
  logits/log-links or `1e6` for direct MSE regression. Before Opacus computes
  its global per-sample L2 clip, every `grad_sample` is coordinate-totalised at
  the same server-owned `C`, preventing an overflowing backward pass from
  turning `inf * 0` into a noise-erasing `NaN`.
- Vision decoders enforce regular-file, byte, header-shape and decoded-element
  ceilings before materialising pixels/voxels. Resized records flow from paths
  through a 128 MiB batch buffer rather than a cohort-sized image list.
  Detached NRRD and `.mhd` sidecars are rejected before opening their payload
  and totalised to the fixed zero-image record; inline NRRD and `.mha` remain
  supported.
- Admission does not inspect class/event frequencies, because success versus
  error would otherwise be a label-dependent release outside DP. It has no
  class-cell threshold. It does enforce the server-owned DataSHIELD minimum on
  the staged privacy-unit count (`n_units`): rows for row adjacency and distinct
  canonical patients for patient adjacency, including image runs.
  Below-threshold runs receive one generic refusal without an exact count or
  shortfall.
- Adjacency is explicitly bounded/replace-one with a fixed number of privacy
  units. Neighbours replace one row, or one complete configured patient unit.
  The package does not claim unbounded add/remove membership privacy for a
  changing unit count.

The server-owned per-training policy is validated before prepare reads private
contents. Policy validation alone does not privatise an exception, which is why
the value-dependent preprocessing above is total rather than merely
error-sanitised.

The model itself is the intentional DP release. Model inversion is not made
impossible; DP bounds how much the output distribution changes when one privacy
unit is replaced.

## 7. Custodian options

Options follow DataSHIELD's `dsflower.*` / `default.dsflower.*` fallback. The
important privacy options are:

The supplied Rock runtime performs early bootstrap when the deployment provides
`DSFLOWER_NODE_SECRET_FILE`. Otherwise it defers to the first session because
Opal/Armadillo inject profile R options only after that session exists. The
environment path takes precedence over a stale key-path option so recoverable
regeneration never blocks. Policy options such as epsilon and delta remain
session/profile options.

| Option | Default | Meaning |
|---|---:|---|
| `dp_per_training_epsilon` | `1` | Fixed epsilon per training release, maximum `10` |
| `dp_per_training_delta` | `1e-6` | Fixed delta per training release, maximum `1e-3` |
| `dp_unit` | `row` | Adjacency unit (`row` or `patient`) |
| `patient_column` | unset | Required explicit stable ID column in patient mode |
| `image_data_root` | unset | Custodian root for direct segmentation image paths |
| `mask_data_root` | unset | Custodian root for direct segmentation PNG-mask paths |
| `public_initialisation` | `analyst_or_resource` | Custodian admission policy: `analyst_or_resource`, `resource_only`, or `none`; optional per-contract suffix. |
| `checkpoint_cache_dir` | `checkpoints` under `tools::R_user_dir("dsFlower", "data")` | Protected service-owned cache outside staging, Hook mounts and node-secret storage. |
| `dp_clipping_norm` | `1` | Server-owned clipping bound |
| `node_secret_path` | Unix: `/var/lib/dsflower/privacy/noise_root`; Windows: `%LOCALAPPDATA%/dsflower/privacy/noise_root` | Runtime-generated key; deployment ENV takes precedence when it selects another path |
| `release_cache_dir` | `release-cache` beside the node secret | Persistent gated-Hook release cache, outside private staging and Hook mounts, with protected ownership and permissions |
| `release_cache_bytes` | `1073741824` | Administrator-owned logical cache capacity; reserves the complete public worst-case run before private work and never evicts active-run pins |
| `tunnel_chunk_bytes` | `524288` | Per-exchange decoded tunnel payload cap (16--512 KiB); larger streams use multiple exact chunks below DSI's expression-parser limit |
| `tunnel_spool_max_bytes` | `1073741824` | Per-direction tunnel spool cap; TCP backpressure when full |
| `tunnel_loss_tolerance` | `180` | Relay-heartbeat timeout in seconds (`5`--`86400`) |
| `hook_enabled` | `FALSE` | Permit HookApp execution |
| `hook_sandbox_attested` | `FALSE` | Custodian attests the Bubblewrap boundary |
| `hook_resource_isolation_attested` | `FALSE` | Custodian attests external cgroup and writable-volume quotas |
| `dp_sample_aggregate` | `FALSE` | Enable fixed-block HookApp sample-and-aggregate behind every sandbox gate |
| `dp_sa_blocks` | `8` | Fixed public block count in `[2, 64]`; never private-size adaptive |
| `dp_egress_timeout` | `900` | Hook child timeout in seconds |
| `dp_egress_time_pad` | `0` | One release-global minimum duration across all S&A children; zero disables HookApp execution; otherwise at least `dp_egress_timeout + 5`, or `dp_sa_blocks * dp_egress_timeout + 5` for sequential S&A |
| `dp_egress_memory_mb` | `8192` | Hook child address-space limit in MiB (`512` to `131072`) |
| `dp_egress_file_mb` | `1024` | Per-file Hook child write limit in MiB (`16` to `16384`) |
| `dp_egress_processes` | `128` | Hook child process/thread limit (`1` to `1024`, where supported) |
| `allow_untrusted_coordinator` | `FALSE` | Permit observation of already-private per-node updates |

The resource-isolation attestation is valid only when the SuperNode process and
all Hook descendants inherit cgroup v2 `memory.max`, `pids.max` and `cpu.max`
controls, and the writable Hook temporary directory resides on a size-limited
tmpfs or quota-enforced volume. It is independent of the Bubblewrap filesystem/
network attestation and RLIMIT defense in depth; both attestations are required.
The timing pad wraps one complete Hook release, not every S&A block, while every
child retains its own timeout. It remains a minimum-duration mitigation rather
than a constant-time or availability guarantee.

Example Unix node policy (Windows services should pin an absolute secret path):

```r
options(
  default.dsflower.dp_per_training_epsilon = 1,
  default.dsflower.dp_per_training_delta = 1e-6,
  default.dsflower.dp_unit = "row",
  default.dsflower.node_secret_path = "/var/lib/dsflower/privacy/noise_root",
  default.dsflower.release_cache_dir = "/var/lib/dsflower/privacy/release-cache",
  default.dsflower.release_cache_bytes = 1024^3,
  default.dsflower.hook_enabled = FALSE
)
```

Every semantically new training uses the same server-owned per-training
contract. Metric and threshold selection over one released DP model is
post-processing; HPO or CV that trains new models creates new per-training
releases.

Seed loss, malformation or an unsafe mode causes automatic CSPRNG rotation and
does not depend on cumulative privacy-budget history. Coordinate claims and
cache-capacity checks still govern release admission. Administrators can select a
secret-manager path through `DSFLOWER_NODE_SECRET_FILE`.
That process-level path is authoritative if a stale DataSHIELD profile option
names another key, so the mismatch never blocks a recoverable rotation.

Deterministic noise is generated from HMAC(node secret, canonical semantic
identity) and a domain-separated ChaCha20 stream. It gives computational DP
conditional on key secrecy and identity non-reuse. A later compromise of a
persistent key can recreate streams for known semantic identities. Preserving
the root preserves deterministic recomputation; rotating it intentionally starts
a new randomness domain.

The DSI tunnel is authorized only by a valid session capability whose exact
forwarder process is alive and has published readiness after binding loopback.
Startup requires the same explicit tunnel ABI in both directions, so an
incompatible server/client deployment fails before stream bytes are exchanged.
Startup is transactional on both sides: failures kill/clean the node process and
the client aborts and tears down every attempted site. Per-session exchange
locks, negotiated chunks, bounded client buffers and capped node spools prevent
unbounded request, memory and disk growth. Spools reset at connection-generation
boundaries. Within a generation they carry absolute offsets and compact only
acknowledged prefixes under the shared exchange lock, preserving reconnect and
retry semantics while keeping long-lived transfers within the configured cap.
Because DSI 1.8 maps per-node failures to named `NULL` values, all mutating paths
require an exact node-bound success ACK. Tunnel and upload ACKs additionally bind
generation, offset, length and content identity; an ambiguous attempt can replay
only the identical in-flight bytes against an idempotent store, without changing
chunk geometry.

### Deployment reproducibility and state

The provisioner admits Flower 1.31.0 exactly, Torch 2.x, Opacus 1.x and
torchvision 0.x, and provisions the exact dependency-light native-tree runtime
separately. It records each resolved environment in `.dsflower_versions.txt`. The
capability response includes the core versions and that manifest's SHA-256. This
records a range-based resolution but cannot reproduce it later. Production nodes
should instead provide a complete, root-owned `DSFLOWER_PYTHON_LOCK` whose every
transitive PyTorch artifact is pinned and hashed, plus a separate
`DSFLOWER_NATIVE_TREE_PYTHON_LOCK` for the tree dependency graph. `uv pip install
--require-hashes` then fails closed on an incomplete lock. The locks are never
reused across environments. `DSFLOWER_REQUIRE_PYTHON_LOCK=true` and
`DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK=true` prevent accidental fallback to
unlocked resolution. `DSFLOWER_PYTHON_VERSION` should also name an exact
patch release to prevent patch drift (the default `3.11` is a compatibility
selector); the immutable container digest remains the exact deployment identity.
Missing `uv` is bootstrapped only from an exact release
tag plus an administrator-provided archive SHA-256, never `latest` or `curl|sh`.
Production images should be selected by immutable digest, including the Rock base
image used to build dsFlower; the supplied Dockerfiles deliberately have no base
image default.

The node secret is runtime state, never image content. Container deployments
should persist `/var/lib/dsflower/privacy/noise_root` when stable deterministic
noise across replacements is desired, together with the gated-Hook cache
(default `/var/lib/dsflower/privacy/release-cache`) for exact replay of retained
releases. Cache directories and files require service ownership, modes `0700`
and `0600` respectively, and no symlinks. A missing seed is recoverable and creates
an independent noise domain. Do not clone one secret to concurrent nodes.
Mounting all of `/var/lib/dsflower` would hide the baked `venvs/` and is therefore
not recommended. The package intentionally leaves volume wiring to the Rock or
cluster orchestrator.

## 8. Residual boundaries

The documented computational/practical DP guarantee covers numeric releases
from the canonical mechanisms. It does not claim protection against:

- administrator or kernel compromise;
- disclosure or cloning of the node noise root;
- denial of service, process crashes or all timing/availability channels in the
  declarative Tier-1 runtime;
- privacy loss accumulated by other DataSHIELD packages or outputs outside
  dsFlower;
- composition across overlapping node populations.

The R preparation layer totalises target-domain, selected-feature completeness
and missing-patient-ID cases. Minimum-size admission is the explicit
DataSHIELD privacy-unit gate and returns one generic refusal. Other schema,
storage, image-decoding and runtime failures can still depend on inputs outside
the supported mechanism domain.

The ClientApp transport boundary catches ordinary Python exceptions and returns
the same Flower record schema with a constant aggregation weight, public/no-op
arrays and a cause-free unavailable marker; it never sends the exception reason
or traceback. The ServerApp does not aggregate that fallback or report it as a
trained release. This prevents direct private-value disclosure through errors,
but does not make an invalid input or a data-dependent failure indistinguishable
from a successful model: no-op content, process termination, availability and
execution time remain outside the formal valid-input transcript guarantee.
Covering those channels would require total preprocessing and a fixed execution
envelope for every declarative mechanism.
The canonical validators and numeric gates are therefore required to make
data-dependent exceptions unreachable throughout the declared valid-input
domain; a runtime that violates that requirement is a mechanism bug, not a
property repaired by the transport fallback. Architecture probes use only
public synthetic fixtures in tests and are never run on private training rows.

HookApps have the stricter timing/network/filesystem gate because arbitrary code
can intentionally create such channels. Declarative apps remain the only
recommended path for strong privacy, useful accuracy and broad model support.
