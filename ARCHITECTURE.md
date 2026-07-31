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
- the operating-system entropy source, persistent privacy ledger and, for
  HookApps, the attested OS sandbox.

The researcher, Flower ServerApp, submitted configuration and uploaded HookApp
code are not trusted. A compromised node administrator, kernel, rollbackable
ledger volume or leaked node secret is outside this guarantee.

dsFlower uses node-side central DP: each data node is a trusted curator for its
local dataset and applies DP before egress. This is not formal local DP (LDP),
where every individual randomizes their own record. dsFlower does not implement
Secure Aggregation. A coordinator can therefore observe each already-private node
update. Public coordinators are rejected unless the node administrator explicitly
sets `dsflower.allow_untrusted_coordinator = TRUE`.

## 2. Two computation contracts

### Declarative DP training (recommended)

The researcher supplies data-only model and training specifications. The node
builds and executes the computation with the canonical runner:

- neural/vision models use Opacus DP-SGD with per-example or per-patient gradient
  clipping and a CSPRNG-backed Gaussian stream;
- DP-GBDT uses data-independent tree structure, bounded gradients/Hessians and
  node-side Gaussian leaf-histogram noise;
- only allowlisted, per-sample-safe operations and losses are admitted;
- the manifest pins mechanism, model spec, loss, batch size, local epochs,
  horizon, feature count and public preprocessing bounds before execution.

This contract reaches `nn.Module`-level granularity because the trusted runner
owns the training loop and observes per-sample gradients. Extending the
declarative vocabulary is the safe way to add flexibility.

### HookApp (legacy name: Tier2)

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

When an attested Bubblewrap filesystem/network sandbox and a minimum-duration
timing envelope are available, the node may run the hook on a fixed, public `k` disjoint,
isolated blocks and release the clipped mean at conservative sensitivity
`min(2C, 4C/k)` (a changed patient identifier may affect two blocks). `k` is
administrator-pinned and never derived from private cohort size, because a
data-dependent Gaussian variance would itself leak.
Without all required controls, HookApps are not executed and the Flower operation
returns the incoming public model unchanged.

The timing envelope is a defense-in-depth lower bound, not a formal
constant-time guarantee. Cleanup, resource exhaustion and availability remain
outside the numeric mechanism proof unless the deployment adds system-level
quotas and an outer release deadline.

Arbitrary code cannot generically receive DP-SGD granularity. Static inspection
cannot prove that a custom training loop has per-sample-independent gradients or
that it has no side channel. To obtain Tier-1 granularity, express the operation
as a declarative spec or add a custodian-reviewed operation to the canonical
runner.

`ds.flower.tier2.run()` remains a compatibility alias for the HookApp API. The
old independent `dsflower_tier2` runner has been removed so there is only one
code path to audit and pin.

## 3. Lifetime accounting without query blocking

Exact replays of a Flower message never create a second private release. The
cached response is reused when available; otherwise the incoming public model is
returned unchanged. Every new release is claimed transactionally before any
private computation.

For new run `n`, starting at one, the persistent accountant reserves:

```text
w_n       = s (1 - rho) rho^(n - 1),  s = 1 - 10^-12
epsilon_n = epsilon_total w_n
delta_n   = delta_total w_n
```

Because the weights sum to less than one (the tiny slack avoids floating-point
overshoot), basic adaptive composition bounds every finite
prefix and the infinite transcript by
`(epsilon_total, delta_total)`. A failed execution after reservation is not
refunded.

This schedule never rejects a run because a budget counter reached a hard cap.
However, finite total privacy and infinitely many informative queries are
mathematically incompatible: `epsilon_n` and `delta_n` must tend to zero. Once an
allocation is below the node's numerical release threshold, the operation
completes with a data-independent unchanged model. Adding a positive epsilon
floor would make the lifetime sum diverge and is not allowed.

The default privacy domain is the entire node, not a dataset fingerprint.
Subsetting, renaming or changing one row therefore cannot reset the lifetime
budget. Multiple domains are disabled by default and are sound only when the
custodian certifies that their populations are disjoint.

Accounting is per node. For a person present at multiple nodes whose private
updates are all observed, the node guarantees compose sequentially: their
epsilons and deltas add. Parallel composition applies only when node populations
are disjoint. A federation-wide guarantee over overlapping sites therefore
requires a shared person-level accountant beyond this package's node ledger.

The SQLite ledger uses `BEGIN IMMEDIATE`, WAL, `synchronous=FULL`, unique run and
release constraints, and mode `0600`. The ledger file and its parent directory
must be owned by the Rock process, and the directory must not be writable by
group or other users. The Python release guard compares the directory and ledger
device/inode before and after `sqlite3.connect()`; R performs the portable
owner/mode/path checks before and after DBI opens it. Policy values are bound on
first use; changing them later fails closed. These path checks prevent another
UID from replacing database entries when the enclosing mount path is trusted,
but cannot close a swap-out-and-back race by a malicious process sharing the
Rock UID. SQLite is a single-host backend and the checks are not anti-rollback
storage. Replicas that protect the same population require one shared
transactional accountant and anti-rollback operational controls; independent
local ledgers multiply the declared guarantee.

## 4. Deterministic randomness

Determinism is retained only as release-scoped, secret-keyed randomness:

```text
release_key = HMAC-SHA256(node_secret,
                          protocol_version || release_id)
subkey      = HMAC-SHA256(release_key, mechanism_axis)
```

The dedicated node secret is 32 bytes from `/dev/urandom`, created at runtime,
stored outside staging with mode `0600`, and never exposed to a HookApp. The key
file is owned by the Rock service UID. Its non-symlink parent may be owned by that
UID or root, must not be writable by group/other, and is checked before and after
the key is opened. There is no fallback to R's RNG, a client seed,
`datashield.seed`, a predictable constant
or a secret baked into a container image.

DP Gaussian values come from a ChaCha20 stream with domain-separated subkeys.
Poisson sampling and HookApp partitioning use their own ChaCha20 subkeys. Torch
initialization/dropout is data-independent and uses a separate HMAC-derived seed
through the framework PRNG; it is not a DP-noise source. No privacy-critical
stream is reused. The derivation deliberately does not include configuration or
private data: mechanism randomness must be independent of the protected dataset.

A single fixed noise vector for all distinct queries is unsafe because correlated
answers can cancel it. Sticky noise only solves repeated identical queries. In
dsFlower, exact protocol retries are memoized, while distinct releases receive
unique keys and are covered by the lifetime accountant.

The Gaussian implementation uses a hardened Box--Muller transform over a finite
IEEE-754 support. The keyed ChaCha20 stream prevents prediction and averaging,
but this remains a practical computational-DP implementation rather than a
formally verified exact continuous Gaussian. A discrete-Gaussian or
interval-refining replacement must be introduced as a versioned mechanism with
matching sensitivity/accountant proofs and ledger-policy migration.

## 5. Server-authoritative manifest

The client can request a valid declarative computation but cannot set or weaken:

- total or per-run epsilon/delta;
- clipping norm;
- privacy domain or allocation index;
- release horizon;
- lifetime DP unit, patient column and identifier canonicalisation;
- HookApp enablement, sandbox attestation, timeout or timing envelope;
- exact metrics, counts, logs or feature statistics.

Server-owned structural manifest fields cannot be duplicated or overridden by
the client. The Python release guard cross-checks domain, allocation, horizon,
epsilon and delta against SQLite; the manifest alone is never authoritative.

The node pins the recursive runner hash. The client's bundled runner must be
byte-identical, and `tools/check-runner-sync.py` provides a CI check. Uploaded
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
- The node secret, ledger path, staging path and run token are not returned.
- Patient identifiers are preserved only for local grouping, excluded from model
  features and selected only by lifetime administrator policy. The default is
  row-level. Patient mode requires one explicit `dsflower.patient_column`, a
  stable identifier roster across releases; there is no auto-detection or row
  fallback. Missing/empty/reserved identifiers are collapsed into one fixed,
  conservative sentinel unit, so they cannot create a prepare error oracle. A
  per-person interpretation still requires a complete roster. Unit, column and
  canonicalisation version are bound into the persistent policy hash.
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
  minimum row/patient threshold either: tiny and empty runs reach the trusted
  mechanism instead of returning an exact prepare-time count predicate.
- Adjacency is explicitly bounded/replace-one with a fixed number of privacy
  units. Neighbours replace one row, or one complete configured patient unit.
  The package does not claim unbounded add/remove membership privacy for a
  changing unit count.

The next lifetime allocation is reserved before prepare reads private contents;
failed attempts are charged without refund and ensure only repeats the idempotent
lookup. Accounting alone does not privatise an exception, which is why the
value-dependent preprocessing above is total rather than merely error-sanitised.

The model itself is the intentional DP release. Model inversion is not made
impossible; DP bounds how much the output distribution changes when one privacy
unit is replaced.

## 7. Custodian options

Options follow DataSHIELD's `dsflower.*` / `default.dsflower.*` fallback. The
important privacy options are:

| Option | Default | Meaning |
|---|---:|---|
| `dp_total_epsilon` | `3` | Node/domain lifetime epsilon, maximum `10` |
| `dp_total_delta` | `1e-5` | Node/domain lifetime delta, maximum `1e-3`; choose materially below `1 / protected_units` |
| `dp_budget_decay` | `0.5` | Geometric `rho`, in `[0.5, 0.99]` |
| `dp_min_release_epsilon` | `1e-6` | Per-message numerical viability threshold and hard safety minimum, not an allocation floor |
| `dp_min_release_delta` | `1e-12` | Per-message numerical viability threshold and hard safety minimum |
| `privacy_ledger_path` | persistent node path | SQLite ledger; ephemeral paths rejected |
| `dp_privacy_domain` | `node` | Accountant domain |
| `dp_unit` | `row` | Lifetime adjacency unit (`row` or `patient`) |
| `patient_column` | unset | Required explicit stable ID column in patient mode |
| `dp_allow_multiple_domains` | `FALSE` | Requires certified disjoint populations |
| `dp_clipping_norm` | `1` | Server-owned clipping bound |
| `node_secret_path` | `/var/lib/dsflower/node_secret` | Dedicated 256-bit key |
| `tunnel_chunk_bytes` | `1048576` | Per-exchange decoded tunnel payload cap (16 KiB--8 MiB) |
| `tunnel_spool_max_bytes` | `1073741824` | Per-direction tunnel spool cap; TCP backpressure when full |
| `tunnel_request_max_bytes` | `67108864` | Pre-decode cap for an encoded fan-out request |
| `tunnel_loss_tolerance` | `180` | Relay-heartbeat timeout in seconds (`5`--`86400`) |
| `hook_enabled` | `FALSE` | Permit HookApp execution |
| `hook_sandbox_attested` | `FALSE` | Custodian attests the Bubblewrap boundary |
| `hook_resource_isolation_attested` | `FALSE` | Custodian attests external cgroup and writable-volume quotas |
| `dp_sample_aggregate` | `FALSE` | Enable fixed-block HookApp sample-and-aggregate behind every sandbox gate |
| `dp_sa_blocks` | `8` | Fixed public block count in `[2, 64]`; never private-size adaptive |
| `dp_egress_timeout` | `900` | Hook child timeout in seconds |
| `dp_egress_time_pad` | `0` | One release-global minimum duration across all S&A children; zero disables HookApp execution; otherwise at least `dp_egress_timeout + 5` |
| `dp_egress_memory_mb` | `8192` | Hook child address-space limit in MiB (`512` to `131072`) |
| `dp_egress_file_mb` | `1024` | Per-file Hook child write limit in MiB (`16` to `16384`) |
| `dp_egress_processes` | `128` | Hook child process/thread limit (`1` to `1024`, where supported) |
| `expose_privacy_status` | `FALSE` | Expose allocation count/status to clients |
| `allow_untrusted_coordinator` | `FALSE` | Permit observation of already-private per-node updates |

The resource-isolation attestation is valid only when the SuperNode process and
all Hook descendants inherit cgroup v2 `memory.max`, `pids.max` and `cpu.max`
controls, and the writable Hook temporary directory resides on a size-limited
tmpfs or quota-enforced volume. It is independent of the Bubblewrap filesystem/
network attestation and RLIMIT defense in depth; both attestations are required.
The timing pad wraps one complete Hook release, not every S&A block, while every
child retains its own timeout. It remains a minimum-duration mitigation rather
than a constant-time or availability guarantee.

Example node policy:

```r
options(
  default.dsflower.dp_total_epsilon = 3,
  default.dsflower.dp_total_delta = 1e-5,
  default.dsflower.dp_budget_decay = 0.5,
  default.dsflower.dp_unit = "row",
  default.dsflower.privacy_ledger_path = "/var/lib/dsflower/privacy/ledger.sqlite",
  default.dsflower.node_secret_path = "/run/secrets/dsflower_node_key",
  default.dsflower.hook_enabled = FALSE
)
```

Changing a bound after the ledger has been initialized is rejected. To rotate a
node secret without changing past releases, keep the old key available until no
active/retryable run references it; production secret rotation should use key IDs
and a secret manager.

Deterministic noise is generated from HMAC(node secret, unique ledger release
identity) and a domain-separated ChaCha20 stream. It gives computational DP
conditional on key secrecy and identity non-reuse. A later compromise of a
persistent key can recreate historical streams for known release identities;
high-assurance deployments should use versioned KMS/HSM keys, bounded retry
windows and cached exact replies so retired key versions can be destroyed.

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

A pre-unit legacy ledger is not migrated automatically. Its historical adjacency
cannot be inferred from stored state: changing a patient identifier can affect
two patient groups even when only one row changes. Such a ledger therefore fails
closed until an administrator performs a separately audited migration with an
explicitly attested historical unit and adjacency contract.

The narrower identifier-canonicalisation migration from `trim-utf8-v1` to
`trim-utf8-v2` is automatic for row adjacency because identifiers do not define
the unit. In patient mode it is automatic only when the ledger proves that no v1
release was claimed; the same transaction exhausts all outstanding v1 tokens
before binding the v2 hash. A patient ledger with any v1 claim remains fail
closed pending an offline roster-equivalence audit.

### Deployment reproducibility and state

The provisioner admits Flower 1.31.x, Torch 2.x, Opacus 1.x and torchvision 0.x,
then records the exact resolved environment in `.dsflower_versions.txt`. The
capability response includes the core versions and that manifest's SHA-256. This
records a range-based resolution but cannot reproduce it later. Production nodes
should instead provide a complete, root-owned `DSFLOWER_PYTHON_LOCK` whose every
transitive artifact is pinned and hashed; `uv pip install --require-hashes` then
fails closed
on an incomplete lock. `DSFLOWER_REQUIRE_PYTHON_LOCK=true` prevents accidental
fallback to range resolution. `DSFLOWER_PYTHON_VERSION` should also name an exact
patch release to prevent patch drift (the default `3.11` is a compatibility
selector); the immutable container digest remains the exact deployment identity.
Missing `uv` is bootstrapped only from an exact release
tag plus an administrator-provided archive SHA-256, never `latest` or `curl|sh`.
Production images should be selected by immutable digest, including the Rock base
image used to build dsFlower; the supplied Dockerfiles deliberately have no base
image default.

The ledger directory and node secret are persistent state, not image contents.
They must survive rescheduling/upgrades and be backed up consistently as one node
identity; neither may be rolled back or cloned to another node. Container
deployments should persist `/var/lib/dsflower/privacy/` and inject the secret file
separately. Mounting all of `/var/lib/dsflower` would hide the baked `venvs/` and
is therefore not recommended. The package intentionally leaves volume/secret
wiring to the Rock or cluster orchestrator.

## 8. Residual boundaries

The formal guarantee covers numeric releases from the canonical
mechanisms. It does not claim protection against:

- administrator or kernel compromise;
- ledger deletion, snapshot rollback or cloning;
- independent replicas with independent ledgers;
- denial of service, process crashes or all timing/availability channels in the
  declarative Tier-1 runtime;
- privacy loss accumulated by other DataSHIELD packages or outputs outside
  dsFlower;
- populations that overlap across explicitly enabled privacy domains.

The R preparation layer totalises target-domain, selected-feature completeness,
minimum-size and missing-patient-ID cases. Other schema, storage, image-decoding
and runtime failures can still depend on inputs outside the supported mechanism
domain.

The ClientApp transport boundary catches ordinary Python exceptions and returns
the same Flower record schema with a constant aggregation weight and public/no-op
arrays; it never sends the exception reason or traceback. This prevents direct
private-value disclosure through errors, but does not make an invalid input or a
data-dependent failure indistinguishable from a successful model: no-op content,
process termination, availability and execution time remain outside the formal
valid-input transcript guarantee. Covering those channels would require total
preprocessing and a fixed execution envelope for every declarative mechanism.
The canonical validators and numeric gates are therefore required to make
data-dependent exceptions unreachable throughout the declared valid-input
domain; a runtime that violates that requirement is a mechanism bug, not a
property repaired by the transport fallback. Architecture probes use only
public synthetic fixtures in tests and are never run on private training rows.

HookApps have the stricter timing/network/filesystem gate because arbitrary code
can intentionally create such channels. Declarative apps remain the only
recommended path for strong privacy, useful accuracy and broad model support.
