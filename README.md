# dsFlower

`dsFlower` is the node-side DataSHIELD package for running
[Flower](https://flower.ai/) federated learning under a privacy policy controlled
by the data custodian. It is installed on each Opal/Rock node and pairs with the
researcher-side
[`dsFlowerClient`](https://github.com/isglobal-brge/dsFlowerClient) package.

The node, its custodial root secret and its installed canonical runner are
trusted. The researcher, submitted configuration, Flower coordinator and
uploaded code are not. The supported privacy contract is therefore deliberately
narrow: every numeric model update released by a SuperNode is produced by the
node-installed, hash-pinned runner under a server-owned per-training
`(epsilon, delta)` contract. `dsFlower` is not an unrestricted remote Python executor.

## Package roles

| Package | Installed at | Responsibility |
|---|---|---|
| `dsFlower` | Data-owning Opal/Rock node | Validate and stage local data, bind the per-training privacy contract, enforce the selected DP mechanism, run a Flower SuperNode and minimize DataSHIELD egress. |
| `dsFlowerClient` | Researcher workstation | Create declarative requests, verify runner compatibility, operate the SuperLink and coordinate cleanup. |

Raw rows, images, masks and patient identifiers remain on the data node. Model
updates travel through Flower only after the node-side mechanism has processed
them. Each node acts as a trusted curator and applies central DP to its local
dataset before egress; this is not formal local DP (LDP). dsFlower does not
implement Secure Aggregation, so an untrusted/public coordinator is rejected unless the
custodian explicitly opts in with `dsflower.allow_untrusted_coordinator = TRUE`.

## Installation

```r
remotes::install_github("isglobal-brge/dsFlower")
```

The `configure` script prepares two node-owned Python runtime families: a
PyTorch/Opacus environment for neural and vision training, and a small
`native-tree` environment for trusted tree training, validation and executable
capability probes. The latter pins Flower 1.31.0, NumPy 2.4.6, pandas 3.0.3,
PyArrow 23.0.1 and cryptography 46.0.7 exactly. It does not install upstream
XGBoost, LightGBM or CatBoost: XGBoost remains in its separately verified native
bundle, while the other two names identify dsFlower-style numeric engines.

## Computation contracts

| Request | Enforced node-side behavior |
|---|---|
| Declarative neural/vision specification | Opacus DP-SGD with per-example or, when a server-selected patient identifier exists, per-patient clipping and noise. |
| HookApp | Complete-update clipping and conservatively RDP-calibrated Gaussian output perturbation; optional fixed-block sample-and-aggregate only inside the required sandbox. |
| Private model validation | One fixed Gaussian-noised vector of bounded per-unit sufficient statistics; only pooled metrics are post-processed by the ServerApp. |

Declarative specifications are data, not researcher code. They provide the
granularity of an `nn.Module` because the trusted runner owns the training loop
and can observe per-sample gradients. Adding a reviewed operation to this
declarative vocabulary is the safe extension path.

A HookApp is more restricted than a general Flower App. It exposes
`initial_arrays()` and `local_update()` and is never imported into the trusted
parent. Arbitrary code cannot generically receive DP-SGD-level guarantees:
static inspection cannot establish per-sample gradient independence or exclude
side channels. A HookApp executes only when all of the following are true:

- the custodian enables it;
- a Bubblewrap filesystem/network boundary is available and explicitly attested;
- the configured minimum-duration timing envelope is valid;
- the uploaded package passes archive validation, scanning and hash pinning.

Both child functions receive only a bounded, hash-pinned public `app_params`
object plus trusted `round_index`, `num_rounds`, task and class-count fields.
Privacy, path, dependency, secret and runtime keys are reserved. Array count and
shape are fixed for the run, and the Gaussian scale composes the complete round
transcript. If a public execution gate is absent, the HookApp is not executed and
the run is reported as `available=false` without a model artifact. A child crash,
timeout or malformed result inside the sandbox becomes a zero delta and still
receives the same Gaussian output mechanism. A failure in the trusted runtime
after private execution starts is also reported only as unavailable, without its
cause or fallback being accepted as a trained model.

The Hook timing envelope is defense in depth, not a formal constant-time
guarantee: cleanup, process availability and storage behavior remain outside the
numeric DP proof and require deployment-level quotas/isolation when in scope.

The dedicated native-tree ABI implements reviewed node-owned mechanisms for
XGBoost, ExtraTrees, adaptive Random Forest, dsFlower LightGBM-style boosting
and dsFlower CatBoost-style boosting. Every request pins public bounds, public
cuts and a fixed typed parameter profile before private data are opened; the
node owns privacy calibration and sticky custodial randomness. LightGBM-style
and CatBoost-style are safe dsFlower numeric engines, not wrappers around the
upstream binaries or model formats. Capability fields report fresh executable
probes on each node; the engine list describes request syntax and is not a
privacy permission catalogue.

The Random Forest mechanism uses a disjoint node-owned partition: each
effective privacy unit contributes to exactly one tree. It is not upstream
bootstrap/bagging Random Forest, and small cohorts can consequently have fewer
effective units per tree. The public defaults (8 depth-4 binary trees; 4 depth-4
regression trees) are benchmark-oriented starting points, not private-size
admission rules.

The four pure dsFlower engines are operational after the standard
dependency-light runtime is provisioned. Native XGBoost remains fail-closed on
a clean install: a custodian must separately build, verify and configure the
platform-specific curated bundle. The installer does not compile or download
that native trust artifact implicitly.

Private validation loads an already public declarative, native dsFlower vision,
or sanitized native-tree model before opening the staged validation data.
Each row or configured patient contributes one bounded
histogram/sufficient-statistic vector. The node releases its sum once through the
Gaussian mechanism; exact predictions, labels, counts and per-node metrics never
leave the node. The ServerApp requires every selected node and derives binary,
multiclass, ordinal, multilabel, bounded-regression or count metrics only by
post-processing the pooled DP vector. This is external validation when the
assigned dataset is independent and resubstitution validation otherwise; it does
not relabel reuse as cross-validation.
If any expected node does not provide the fixed private release, the pooled
artifact reports `available=false` and omits metrics. It never substitutes exact
or zero-filled metrics, and this does not introduce a query-count lockout.

Atomic holdout is available for tabular neural/native-tree training and native
dsFlower 2D/3D vision models. Nodes derive the same secret-keyed row/patient
split before training, spend the fixed 80/20 job budget on training and one
pooled test release, and publish the model plus metrics only after the exact
roster completes both phases. Vision paths and patient IDs are partitioned
before pixel decode, so the backbone extracts only the selected side.

## Per-training privacy

Privacy is server-authoritative. The client cannot set epsilon, delta, clipping
or HookApp controls. The custodian pins one positive epsilon/delta pair for each
training, and its accountant composes that contract across the training's own
rounds. There is no historical database, quota or resource-specific balance.
Distinct trainings are independent releases and compose in the standard way
when an analyst chooses to reason about them together. Metric and threshold
selection over one released DP model is ordinary post-processing; training a
different model is a new per-training release.

The guarantee is accounted independently at each node. If one person can occur
at `m` observed nodes, their federation-wide guarantee composes across those
nodes (at most the sums of their epsilons and deltas); only genuinely disjoint
node populations get parallel composition. A deployment needing one bound for
overlapping sites must add a shared, person-level federation accountant.

The formal adjacency is bounded/replace-one with a fixed number of privacy
units: neighbouring datasets replace one row, or all records belonging to one
configured patient.
This protects the values contributed by that unit; it is not an unbounded
add/remove membership guarantee for a changing number of privacy units.

## Deterministic, semantic-scoped randomness

Determinism prevents averaging only for an exact replay. Reusing one fixed noise
vector for distinct or adaptively related queries is unsafe because correlated
answers can cancel it. `dsFlower` derives deterministic randomness from a
canonical, mechanism-bound semantic identity:

```text
release_key = HMAC-SHA256(noise_root,
                          protocol_version || mechanism || semantic_id)
subkey      = HMAC-SHA256(release_key, mechanism_axis)
```

The node secret is 32 bytes from the operating-system CSPRNG, created at runtime,
stored outside staging with mode `0600` and never exposed to submitted code. Its
file must be owned by the Rock service UID; its real parent directory may be
owned by that UID or root, but must not be writable by group or other users.
The parent is checked before and after the key is opened. When its path is
explicitly provided as a process environment variable, the supplied Rock image
bootstraps after runtime mounts are ready and before opening its port.
Otherwise it deliberately waits for the first `flowerInitDS()`, when DataSHIELD
profile options exist. Neither `configure` nor `.onLoad()` creates privacy state,
and both Docker builds assert that no seed entered the image. A
bootstrap storage error does not take Rock down; every private entry point
retries and remains fail-closed until the mount is repaired.

A missing, malformed or permissively-mode'd regular key owned by the service UID
is atomically replaced from fresh OS entropy. A symlink, foreign-owned file or
unsafe parent is never followed or overwritten. Rotation starts an independent
noise domain and never blocks a query. The secret is deliberately independent
of R's mutable RNG and `datashield.seed`. DP
Gaussian noise, Poisson sampling and HookApp partitioning use separate
ChaCha20-backed streams. Data-independent Torch initialization/dropout uses a
separate HMAC-derived seed in the framework PRNG; it is not used as the DP noise
source. The semantic identity binds the effective public configuration, policy,
round, incoming public arrays, transformed or patient-pooled private tensors and
a runtime fingerprint (runner bytes, dependency versions and selected backend).
Paths, run tokens, message IDs and timestamps are deliberately excluded. The
private-input digest never leaves the node.

Within one Flower run, a bounded claim ledger in the private staging directory
reserves every operation/fold/round coordinate atomically before private work.
It is mirrored into `NodeState`, survives ClientApp process restarts while that
run's staging remains, and prevents concurrent processes from claiming the same
coordinate. A changed payload cannot reuse a claim, and an older exact request
fails closed once its cached reply has advanced. This per-run replay control is
distinct from a cross-training privacy-budget ledger; separate authorized
trainings still compose under the custodian's deployment policy.

The trusted built-in tracks request strict deterministic Torch kernels. HookApps
receive deterministic Python, NumPy and Torch seeds, and their final noise key is
also bound to the validated clipped update. Arbitrary native user code cannot be
certified deterministic by a static scanner, so exact stateless retry stickiness
for HookApps applies only to deterministic HookApps. Hook execution is disabled
by default and remains the deliberately weaker, custodian-gated extension path.

The current Gaussian sampler is a hardened Box--Muller construction over
IEEE-754 values. ChaCha20 makes its finite random choices unpredictable, but it
does not turn floating-point output into an exact continuous Gaussian. The
implemented guarantee is therefore the documented computational/practical DP
contract, not a claim of a formally verified finite-precision mechanism. Moving
to a verified discrete-Gaussian sampler would require a new mechanism/accountant
ABI and a reviewed privacy-contract change, rather than a drop-in RNG change.

DataSHIELD does not define a portable, connector-level confidential seed for
Opal or Armadillo. Current Opal releases inject a short `datashield.seed` R
option derived from the Opal service secret and log the resulting integer;
Armadillo stores an administrator-editable nine-digit value in each profile.
`dsBase::setSeedDS()` separately replaces R's mutable `.Random.seed` from an
analyst-supplied integer and returns the resulting state. These are
reproducibility/masking facilities, not confidential 256-bit DP mechanism keys.
dsFlower intentionally neither depends on nor exposes them; the dedicated node
secret is the fail-closed source of keyed release randomness.

## Data and output minimization

Exact feature counts, sums and sums of squares are disabled. For tabular utility,
the analyst may provide data-independent public lower/upper feature bounds; the
same clipping and affine transform is applied during training and prediction.
Without bounds, neural inputs remain unscaled but are locally coerced and
saturated to `[-1e6, 1e6]`.

Target preprocessing is also public and per-record. Classification strings or
factors use an ordered `target_levels`; numeric labels may instead arrive already
coded in `[0, K-1]`. Missing/unknown classification values map to the public code
zero. Regression/count models require finite public `target_bounds`; values are
coerced, non-finite/unparseable values map to the public bounds midpoint, and all
values are clipped locally. Selected tabular features are likewise coerced to
numeric and non-finite/unparseable values use the public bounds midpoint (or zero
when bounds are absent); public numeric values/bounds are capped at `1e6` to
stay below unsafe float32 center/span arithmetic. These are fixed per-record maps: no node derives a
vocabulary, imputation value or range from its cohort, and no row is dropped.

Vision inputs are decoded under fixed node-side limits (256 MiB source and
decoded payload, 32M elements) and embedded from paths in batches capped at
128 MiB; the full resized-image cohort is never retained. Raster, NIfTI,
inline NRRD, MHA and DICOM inputs are supported. Detached NRRD payloads and
`.mhd` sidecars are conservatively mapped to the same zero-image record as a
corrupt input, before any sidecar is opened.

The declarative neural runner also totalises its arithmetic: safe division,
finite saturation after every graph operation, parameters/intermediates bounded
to magnitude `1e6`, and loss-aware heads (`1e6` for direct MSE regression, `30`
for logits and log-links). Per-sample gradients are made finite coordinate-wise
before Opacus applies the server-owned L2 clip, so an overflowing backward pass
cannot suppress the Gaussian noise. Neural learning rates must be in `(0, 10]`.
The same preprocessing and head saturation are replayed by local
prediction helpers.

Run admission never inspects class or event frequencies. Such a check would turn
prepare success/failure into a label-dependent oracle outside the DP mechanism.
It does enforce the server-owned DataSHIELD minimum on the staged privacy-unit
count: rows in row mode and distinct canonical patients in patient mode,
including image runs.
The threshold is at least `nfilter.subset` (default 3) and can be raised with
`dsflower.min_train_rows`; a refusal returns one generic node error and never the
exact count or shortfall. In patient mode unusable identifiers are collapsed
into one fixed sentinel privacy unit; there is never a silent row-level fallback.
This is deliberately conservative (it can protect several unidentified subjects
together and reduce utility). A meaningful per-person interpretation still
requires the custodian to provide a complete, stable identifier roster across
releases.

`flowerPrepareRunDS()` validates and pins the complete per-training contract
before it reads private table/file contents. `flowerEnsureSuperNodeDS()` checks
the same pins again before launching the runner. Private-value preprocessing is
totalised rather than returned as a success/error bit.

Node-side training logs and metrics are not returned through DataSHIELD, and
Flower aggregation weights are fixed rather than revealing cohort size. The
server exposes no log, metric or exact feature-statistics endpoint. The global
model is the intended DP release; DP bounds an individual's influence on its
distribution, not the possibility of every form of model inversion.

## Custodian options

Options use the `dsflower.*` prefix, with the standard
`default.dsflower.*` DataSHIELD fallback.

The supplied Rock image performs a pre-service key bootstrap only when a
deployment explicitly provides `DSFLOWER_NODE_SECRET_FILE`. Otherwise it waits
for the first session so Opal/Armadillo profile R options retain their normal
precedence. The environment variable takes precedence over the R option, so a
stale option cannot block safe regeneration. Epsilon and all other policy
controls remain normal DataSHIELD profile options.

| Option suffix | Default | Meaning |
|---|---:|---|
| `dp_per_training_epsilon` | `1` | Administrator-pinned epsilon for every training release; maximum `10`. |
| `dp_per_training_delta` | `1e-6` | Administrator-pinned delta for every training release; maximum `1e-3`. |
| `dp_unit` | `row` | Adjacency unit: exactly `row` or `patient`. |
| `patient_column` | unset | Required explicit stable identifier when `dp_unit="patient"`; never auto-detected. |
| `dp_clipping_norm` | `1` | Server-owned clipping bound. |
| `node_secret_path` | Unix: `/var/lib/dsflower/privacy/noise_root`; Windows: `%LOCALAPPDATA%/dsflower/privacy/noise_root` | Runtime-generated 256-bit node key; `DSFLOWER_NODE_SECRET_FILE` takes precedence when a deployment selects a service or secret-manager path. |
| `app_spool_root` | `/var/lib/dsflower/appstore` | Private, persistent, service-owned upload spool; ephemeral and symlink paths are rejected. |
| `max_fab_bytes` | `52428800` | Per-FAB compressed upload cap. |
| `app_spool_max_bytes` | `1073741824` | Global logical-byte cap across all uploaded FABs and unpacked apps. |
| `app_spool_ttl_seconds` | `86400` | Incomplete-upload retention; installed catalogue apps persist until explicit deletion. Locked operations and staging-referenced apps are skipped by GC. |
| `tunnel_chunk_bytes` | `524288` | Maximum decoded payload in one DSI tunnel exchange; constrained to 16--512 KiB and negotiated with the client. The upper bound stays below DSI's expression-parser limit; larger streams are carried as multiple exact chunks. |
| `tunnel_spool_max_bytes` | `1073741824` | Per-direction tunnel spool cap; at least eight chunks and at most 64 GiB. TCP backpressure applies when full. |
| `tunnel_loss_tolerance` | `180` | Seconds without a relay heartbeat before the node forwarder exits; constrained to 5--86400. |
| `hook_enabled` | `FALSE` | Allow HookApp execution, subject to every other gate. |
| `hook_sandbox_attested` | `FALSE` | Custodian attestation of the Bubblewrap boundary. |
| `hook_resource_isolation_attested` | `FALSE` | Custodian attestation of externally enforced Hook resource isolation. |
| `dp_sample_aggregate` | `FALSE` | Enable fixed-block HookApp sample-and-aggregate when every sandbox gate is present. |
| `dp_sa_blocks` | `8` | Public, fixed HookApp block count, constrained to `[2, 64]`; never derived from private cohort size. |
| `dp_egress_timeout` | `900` | Hook child timeout in seconds. |
| `dp_egress_time_pad` | `0` | One minimum-duration envelope for the complete Hook release; zero disables execution. It must be at least `dp_egress_timeout + 5`, or `dp_sa_blocks * dp_egress_timeout + 5` when sequential sample-and-aggregate is enabled. It is timing defense in depth, not a formal constant-time proof. |
| `dp_egress_memory_mb` | `8192` | Hook child address-space limit in MiB (`512` to `131072`). |
| `dp_egress_file_mb` | `1024` | Maximum size of any Hook child output file in MiB (`16` to `16384`). |
| `dp_egress_processes` | `128` | Hook child process/thread limit (`1` to `1024`, where supported). |
| `allow_untrusted_coordinator` | `FALSE` | Permit a coordinator to observe already-private per-node updates. |

`hook_resource_isolation_attested=TRUE` is an operator assertion, not an
in-process control. Set it only when the SuperNode and every inherited Hook
child are confined by cgroup v2 memory, PID and CPU limits (`memory.max`,
`pids.max`, `cpu.max`) and their writable temporary filesystem is a size-limited
tmpfs or quota-enforced volume. Bubblewrap/RLIMIT alone do not satisfy this
second gate. Without both attestations, the time envelope and `hook_enabled`, a
HookApp remains a data-independent no-op.

Upload admission and writes are serialized by a node-global lock, so the
physical byte cap is atomic across R sessions. There is no catalogue-entry or
call-count quota. Before each admitted chunk, lazy TTL collection removes only
expired incomplete uploads and only when their per-upload lock can be acquired
without waiting. Verified installed apps persist until explicit
`flowerAppDeleteDS()`. Pinning also records the server-generated run token in the
app spool so active bytes remain immutable for the complete run.

The DSI transport is capability-bound and all-or-nothing. It does not add
encryption to DataSHIELD itself: production Opal/Armadillo frontends must enforce
TLS and certificate validation. The official `dsFlowerClient` can inspect the
URL and retained verification options of DSOpal connections. A recognized
Armadillo connection with a valid `https://` URL is accepted automatically for
connector parity; its frontend or reverse proxy remains authoritative for
certificate and hostname validation. Unknown or unidentifiable connectors
require an explicit per-site operator attestation. Plaintext HTTP is rejected by
default and can be enabled only for exact named sites through the client-side
`dsflower.dsi_allow_insecure_http` exception; that requires an independently
trusted network layer. A loopback tunnel
is operator-authorized only while its exact registered forwarder is alive and
its post-bind `ready` marker exists. Failed startup kills the child and removes
its registry/spool state; the client tears down every attempted site if any site
fails. Exchanges use a per-session lock, bounded negotiated chunks and bounded
client buffers. Spools are reset on a new SuperNode connection; acknowledged
prefixes are compacted atomically under the same exchange lock while external
offsets remain absolute. This keeps long sessions bounded without racing the R
exchange, and TCP backpressure applies whenever a configured cap is reached.

DSI 1.8 can represent a per-node failure as a named `NULL`. Every mutating
dsFlower path therefore requires an explicit, correctly named `ok = TRUE` ACK;
`NULL` is never delivery. Upload and tunnel ACKs also bind the generation,
offset, length and content identity. After an ambiguous response, only the
byte-identical in-flight chunk may be replayed against an idempotent store: its
geometry is never reduced or extended until that ACK is resolved.

Example Unix node policy (Windows services should set an absolute
`DSFLOWER_NODE_SECRET_FILE` or `default.dsflower.node_secret_path` explicitly):

```r
options(
  default.dsflower.dp_per_training_epsilon = 1,
  default.dsflower.dp_per_training_delta = 1e-6,
  default.dsflower.dp_unit = "row",
  default.dsflower.node_secret_path = "/var/lib/dsflower/privacy/noise_root",
  default.dsflower.app_spool_root = "/var/lib/dsflower/appstore",
  default.dsflower.hook_enabled = FALSE
)
```

The epsilon/delta pair describes one training. Its rounds are calibrated as one
mechanism; later trainings do not consume a stored balance. Metric and threshold
selection over one released DP model is post-processing; HPO or CV that trains
new models creates new per-training releases.

The Python privacy runtime is constrained to the audited compatibility families:
Flower 1.31.0 exactly, Torch 2.x, Opacus 1.x and torchvision 0.x. The dedicated
native-tree runtime uses the exact dependency set documented above. Provisioning writes the
exact resolved distribution set to `.dsflower_versions.txt`; capabilities report
the Flower/Torch/Opacus versions and the file's SHA-256. That post-install
manifest is audit evidence, not a reproducible lock. A production administrator
can set `DSFLOWER_PYTHON_LOCK` (or `dsflower.python_lock`) to a complete
PyTorch requirements file with hashes for every transitive artifact, and
`DSFLOWER_NATIVE_TREE_PYTHON_LOCK` (or
`dsflower.native_tree_python_lock`) to a separate complete native-tree lock;
provisioning then
uses `uv pip install --require-hashes` and binds the lock SHA-256 into the venv
marker. Never reuse either lock for the other dependency graph. Keep the same
root-owned locks available for later health checks and re-provisioning. Set
`DSFLOWER_REQUIRE_PYTHON_LOCK=true` and/or
`DSFLOWER_NATIVE_TREE_REQUIRE_PYTHON_LOCK=true` to make an absent or invalid
corresponding lock a fail-closed provisioning error. Set `DSFLOWER_PYTHON_VERSION` to
an exact `major.minor.patch` to prevent interpreter patch drift; the flexible
default `3.11` intentionally tracks a compatible patch release. Immutable
container digests remain the deployment identity for byte-for-byte artifacts.

An existing OS-managed `uv` is part of the administrator's trusted computing
base. If no `uv` is installed, dsFlower does not execute a mutable remote
installer or a `latest` URL. Automatic Python bootstrap requires both an exact
official release tag in `DSFLOWER_UV_VERSION` and its platform archive digest
in `DSFLOWER_UV_SHA256`; a mismatch fails before extraction. For containers,
persist `/var/lib/dsflower/privacy/noise_root` and the app-store directory. A
secret-manager file may instead be selected through
`DSFLOWER_NODE_SECRET_FILE`. Do not mount all of `/var/lib/dsflower`, because
that path also contains the baked venvs.

The deterministic CSPRNG is keyed by the dedicated node secret and a canonical
semantic release identity. This prevents averaging exact retries and stream reuse,
but is a computational guarantee: disclosure of the persistent key can recreate
streams for known semantic identities. Production nodes should keep it in a
KMS/HSM-backed secret lifecycle. Preserving the same root preserves deterministic
recomputation; rotating it intentionally starts a new randomness domain.

## Binary association runner

The `association` track stages exactly one outcome and one exposure as a
row-preserving 3x3 table (`reference`, `positive`, `unknown`). It performs one
sticky joint-Gaussian release per node and allows only an all-or-nothing pooled
result. Public typed levels and the row/patient estimand are bound by the
association contract SHA-256; a second job SHA-256 binds that request to runner
ABI 3, the exact runner hash and the public node roster. Neither hash contains a
data path, run token, cohort size or privacy policy value.

The track uses the dependency-light Flower/NumPy runtime and has dedicated app
entrypoints; it never falls back to the neural, HookApp, native-tree or
validation runners. Association privacy remains the existing stateless
per-job/node policy: there is no database, counter, catalogue, historical budget
or rate limiter.

## Server-side lifecycle

| Stage | Exported DataSHIELD methods |
|---|---|
| Connectivity/capabilities | `flowerPingDS`, `flowerCheckConnectivityDS`, `flowerGetCapabilitiesDS` |
| Handle lifecycle | `flowerInitDS`, `flowerDestroyDS` |
| Staging and validation | `flowerPrepareRunDS` |
| SuperNode lifecycle | `flowerEnsureSuperNodeDS`, `flowerCleanupRunDS`, `flowerStatusDS` |
| App integrity | `flowerAppPushDS`, `flowerAppInstallDS`, `flowerAppDeleteDS`, `flowerTier2PinDS` |
| Privacy policy | `flowerPrivacyPolicyDS` |

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the precise trust boundary,
mechanism contracts, deployment requirements and residual limitations.

## Minimal researcher-side example

```r
library(dsFlowerClient)
library(DSI)
library(DSOpal)

builder <- DSI::newDSLoginBuilder()
builder$append(server = "site1", url = "https://opal1.example.org",
               user = "researcher", password = "...",
               table = "PROJECT.training_data", driver = "OpalDriver")
conns <- DSI::datashield.login(builder$build(), assign = TRUE, symbol = "D")

# Bounds are public/domain-knowledge constants, not estimates queried from a node.
fit <- ds.flower.fit(
  conns, symbol = "D", target = "diagnosis",
  features = c("radius", "texture"), model = "pytorch_logreg",
  feature_bounds = list(lower = c(0, 0), upper = c(100, 1)),
  target_levels = c("control", "case")
)

DSI::datashield.logout(conns)
```

## Authors

- **David Sarrat González** — david.sarrat@isglobal.org
- **Juan R González** — juanr.gonzalez@isglobal.org

[Barcelona Institute for Global Health (ISGlobal)](https://www.isglobal.org/)
