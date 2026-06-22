# dsFlower 2.0 — Architecture & Implementation Plan

> Status: **COMPLETE — all three tiers federated-validated** on live nodes
> (nairobi/dakar/douala), each status 0.
> Pre-reset recovery tags: `pre-reset-dp-validated` (dsFlower `acf18a1`, dsFlowerClient `aa17da7`).
>
> Validated end-to-end:
> - **Tier-1** (model submission): trusted harness, always-on Opacus DP-SGD + PRV,
>   new Message API. 3/3 nodes, status 0.
> - **Tier-2 ingestion**: chunked FAB upload over DSI (`flowerAppPush/InstallDS`) +
>   sha256 verify + fail-closed exfiltration scan (`exfil_scan.py`) + install.
> - **Tier-2 execution**: upload *arbitrary* training code → node scans + computes
>   its hash → `flowerTier2PinDS` pins {`dsflower_tier2` runner, user app} with
>   node-computed hashes → the multi-package integrity hook verifies both before
>   load → the trusted runner runs the untrusted `local_update` under the
>   **output-perturbation DP gate** → FedAvg aggregates. "Received 3 results and 0
>   failures", status 0.
>
> Transport is the salvaged transparent DSI byte tunnel (no SecAgg, no `assign.expr`,
> no open ports). Trust chain: content-hash code integrity + node-computed pins +
> tamper-proof manifest DP params + the gate. Remaining is optional polish: a real
> OS sandbox (gVisor/bwrap needs node root; current containment is `--isolation` +
> exfiltration scan + DP gate), DataSHIELD-option docs, and inert dead-code cleanup
> (template-store, coordinator/non-tunnel path).
>
> Bring-up learnings (live nodes): the DSI-tunnel SuperNode runs the ClientApp from
> the client-built FAB in the node's framework venv (use a pytorch-family model so a
> torch+opacus venv is selected); `sitecustomize.py` is **default-deny** and must pin
> the harness (not a template) hash or it kills the ClientApp; the DP budget ledger
> (`~/.local/share/dsFlower/privacy/`, cap `dsflower.max_epsilon`=10) accumulates per
> dataset across *all* runs incl. failed ones — reset it or raise the cap for repeated
> testing/demos.

A system that lets researchers **upload their own Flower apps** which **install on
DataSHIELD server nodes**, while **guaranteeing by construction** that:

1. **Always-on, calibrated local Differential Privacy** protects every release.
2. **Arbitrary/untrusted app code cannot retrieve raw data or emit anything
   disclosive** — nothing that reconstructs the data reaches the researcher/SuperLink.
3. **DataSHIELD disclosure controls** (minimum counts, etc.) are enforced as
   **DataSHIELD options** (inheriting `nfilter.*`).
4. **No Secure Aggregation** — only DP + isolation.
5. The **new Flower Message API** is used (no legacy Strategy/compat layer).
6. The **DataSHIELD (DSI) channel** is used as efficiently as possible.

Goals: good utility (numbers not destroyed by DP) · disclosure-safe · fast ·
flexible (bring your own app).

---

## 1. The trust model (the core invariant)

You **cannot** statically certify that arbitrary, data-accessing code satisfies
DP (Rice's theorem), and arbitrary code can **exfiltrate raw records encoded in
its outputs**. Therefore the only sound posture is: **mediate all data access
through a trusted boundary the app cannot bypass, and gate every release.**

Two distinct threats → two distinct controls (ship **both**):

| Threat | Control |
|---|---|
| **Exfiltration** (network / filesystem / env / process escape) | **Sandbox** the app (no net/FS, locked down) |
| **Leakage through the legitimate model-update channel** (data packed into weights/metrics) | **DP + disclosure egress gate** (clip + noise + count filters) |

The sandbox stops exfiltration; **only DP bounds leakage through the one channel
the app is allowed to use.**

## 2. Trust separation — who is trusted

The researcher **provisions the app** but is **NOT trusted** by the data custodian.
On each node, two things coexist with opposite trust levels:

| On the node | Installed by | Trusted? |
|---|---|---|
| **dsFlower framework** (sandbox, validation, egress gate, DP harness, relay, disclosure controls) | the **node admin / data custodian** (vetted, audited code) | **YES** |
| **The researcher's app** (model + training code) | the **researcher**, per-run via upload | **NO** |

The researcher brings the *what to train*; the framework owns the *what may leave
the node*. The app can never touch the framework's enforcement machinery.

## 3. Runtime topology

```
[ NODE — data custodian's machine ]                          [ RESEARCHER ]
  ClientApp (researcher's app / trusted Tier-1 harness, in SANDBOX: no net/FS)
       │  emits one Flower Message (RecordDict / ArrayRecord)
       ▼  (ClientAppIo gRPC, localhost)
  ┌─────────────────────────────────────────┐
  │  TRUSTED DP + DISCLOSURE EGRESS GATE     │  ← ClientAppIo interceptor: the one
  │  clip + noise + count-filter the Message │    place the update is in parseable
  └─────────────────────────────────────────┘    RecordDict form. Sole gate off-node.
       │  sanitized Message only
       ▼  (localhost)
  SuperNode (framework, trusted)
       │  Fleet-API gRPC bytes (HTTP/2 over TCP)
       ▼  transparent byte tunnel over DSI (datashield.aggregate; no open ports)
                                              SuperLink + ServerApp (aggregation)
```

- The researcher's **ClientApp runs sandboxed** (`--isolation=process` + gVisor /
  bubblewrap; no network, read-only data mount, scrubbed env, seccomp, cgroups).
- The **egress gate is a ClientAppIo interceptor** — trusted framework code sitting
  between the (untrusted, sandboxed) ClientApp and the SuperNode. This is where the
  update is still a parseable Flower `RecordDict`, so the gate can clip + noise the
  tensors and count-filter the metrics. It is a separate trusted process, **not** an
  in-app Flower Mod (Mods run in-process with the untrusted ClientApp and can be
  disabled by it). Everything downstream is post-gate, so the SuperNode↔SuperLink
  transport stays a transparent byte tunnel — no need to parse Fleet RPCs.
- The **SuperLink + ServerApp (aggregation)** run on the researcher side (standard
  Flower). They are not trusted for client-egress enforcement (the gate already
  sanitized everything before it left the node).

## 4. The two disclosure checkpoints

Disclosure control runs at **two moments**, both option-driven (same thresholds):

1. **Admission (input) — before training.** At prepare time the node checks the
   local dataset is usable + non-disclosive: minimum rows (`nfilter.subset` /
   `dsflower.min_train_rows`), minimum per-class / event counts (`nfilter.tab`),
   class-distribution validation. Too small / too imbalanced → **refuse to
   train** (DataSHIELD convention: never operate below the filter; and DP on a
   tiny set is both useless and risky).
2. **Egress (output) — the gate.** Every object leaving the node (weights,
   metrics, sizes, even the initial-params request) passes the DP + disclosure
   gate.

Defense in depth: admission prevents disclosive-tiny training at all; egress
guarantees what comes out.

## 5. The egress gate — DP + disclosure, two tiers

The gate is a trusted node-side component at the **ClientAppIo boundary**,
intercepting the Message **after** the sandbox produces it and **before** the
SuperNode can ship it. It treats every Message as adversarial: wrong shape/dtype →
reject; conforming tensors → **clipped + noised unconditionally**; metrics/sizes →
deterministic backstop. For **Tier 1** the trusted harness already produced a
DP-SGD update, so the gate is a backstop; for **Tier 2** the gate is where DP is
actually applied (output perturbation on the whole update).

**Tier 1 — submit a MODEL (best utility).** The app provides only a forward-pass
`nn.Module` + declarative config. The trusted harness runs **Opacus DP-SGD**:
per-sample gradient clipping to `C`, Gaussian noise, RDP **+ PRV accountant**,
amplification by subsampling → tight DP, good numbers. The sensitivity bound
comes from the **harness's clipping**, not user code, so DP holds for *any*
module.

**Tier 2 — submit an arbitrary APP (max flexibility).** The gate hard-clips the
whole returned update to L2 norm `C` and adds Gaussian noise calibrated to
`(ε, δ)` composed over rounds (DP-FedAvg, McMahan et al. 2018). Guaranteed for
*any* app, but coarser (no per-sample structure, no amplification → noise larger
by ~√(model-dim)). Use when arbitrary client code is required and the utility
cost is accepted.

**DataSHIELD backstop (deterministic).** Independent of DP, every release passes
minimum-count control: counts ≤ threshold are suppressed/bucketed; `num_examples`
/ sizes are count-bucketed. *Any metric not noised-or-bucketed is an exfiltration
channel* → the backstop must cover all of them.

**Accountant.** RDP **+ PRV** (tighter ⇒ less noise for the same ε). Reject if
spent ε > `dsflower.dp_max_epsilon`.

**Guarantee & boundary.** Output is `(ε, δ)`-DP and passes minimum-count control
for any app (both are post-processing on enforced-bounded outputs). Boundary: a
sandbox escape, a metric routed around the gate, or an over-large ε void it.

## 5b. Images (dsImaging collections) — DP linear-probing on frozen features

Image inputs arrive as **dsImaging collections**, not pixel tables: a manifest +
a samples metadata table (`sample_id`, a per-sample `relative_path`, the label,
optional `patient_id`) over a zero-copy image root. The R staging
(`.stageFromDescriptor_image`) resolves the collection to a local root + samples
table; the harness reads **paths + labels only** — pixels stay on disk and are
read lazily during feature extraction. Image bytes never leave the node.

**Why not naive DP-SGD on the pixels.** DP-SGD noise scales `~ σ·C·√d`; a full
vision net has enormous `d`, so naive vision DP-SGD is poor (literature: ~61% at
ε≈47). So the harness **freezes a pretrained backbone** (resnet18/50/densenet121;
MONAI 3D opt-in) — no gradients, `eval`, no-grad extraction — and DP-trains **only
a small linear head** on the extracted features. Tiny effective `d` ⇒ small noise
(last-layer / linear-probe DP, the dominant practical recipe). The frozen
backbone's BatchNorm never enters the trainable graph, so Opacus' per-sample
requirement is met without touching it.

**The disclosure vector for images is the model UPDATE.** Gradient-inversion
attacks (DLG / Inverting-Gradients / GradInversion) reconstruct *training images*
from a single update; batches `<32` are unsafe, and **dsFlower has no Secure
Aggregation**, so the per-node **local DP gate is the sole defense**. Mitigations:
(1) the communicated update is a **low-dim feature-space head gradient**, far
harder to invert into pixels than a full-network gradient; (2) Opacus **noises**
it; (3) vision batch size is **floored to ≥32** (clamped up, never down) before
training. Raw pixels never transit.

**2D/3D, auto + plug-and-play.** File format and dimensionality are auto-detected
at read time (`.nii/.nii.gz`→nibabel, `.nrrd`→pynrrd, `.mha/.mhd/.dcm`→SimpleITK,
else PIL). The default **2D backbone handles both** 2D images directly and 3D
volumes via a representative middle slice; `volumetric=TRUE` opts into a true-3D
MONAI backbone for precision. `feature_dim` is **fixed per backbone**, so the
ServerApp builds the head **without any images** and every node shares the same
feature space (a **pinned** weights enum, not the version-dependent `DEFAULT`) ⇒
FedAvg over heads is valid. Segmentation (U-Net) is **rejected** — linear-probing
is classification only. *Air-gap, fail-closed:* 2D backbone weights come from the
torchvision cache and the 3D backbone needs MONAI; if either is missing the
harness raises a **clear error** rather than silently substituting random weights
— a per-node random init would put nodes in different feature spaces and make
FedAvg over heads invalid. An offline node must pre-seed `TORCH_HOME` / install
MONAI; all nodes must match.

**Disclosure unit = the PATIENT (admission).** Image collections hold one row per
*image*, but several images can share a patient. Admission auto-detects a
patient/subject column (or `dsflower.patient_column`) and counts **distinct
patients** for both the minimum collection size *and* the minimum per-class count
(`.imageDisclosureUnits` dedups to one row per `(patient,label)`). This catches
the "many images, few patients" leak that per-image counting misses (e.g. 18-vs-12
*images* but only 2 *patients* in a class → refused). No patient column → per-image.

**DP unit = the IMAGE (caveat, documented).** Admission groups by patient, but the
Opacus DP-SGD noise is applied **per-image example**, so the formal DP unit is the
image. A patient contributing `k` images therefore has a per-patient guarantee
weaker by up to a factor `k` (group privacy). This is the **decided** design
(per-patient admission + per-image DP); the natural future upgrade to true
per-patient DP is to **mean-pool features per patient** before the DP head step
(one DP example per patient), at the cost of assuming patient-level labels.

## 6. Transport — DSI as a transparent byte tunnel (salvaged)

**The DataSHIELD connection we are handed *is* the channel** — the researcher's R
client cannot assume any open ports, and the node never dials out. All transport
rides `datashield.aggregate` (request/response, researcher-initiated polling).

**Mechanics (the hard constraint).** Payloads move as **base64 string arguments
of `aggregate` function calls** (`flowerTunnelExchangeDS(cid, <b64>)`), never via
`datashield.assign(assign.expr=…)` — building data into an assigned R expression
hits R/Opal's expression-size ceiling and forces pathological chunking. The
aggregate-with-direct-string path is the efficient one; we do our **own** offset
framing on top of it for anything over a single call's ceiling.

**Transparent, not RPC-parsing.** The new Fleet API is discrete RPCs, but on the
wire it is still gRPC/HTTP-2 **over a TCP byte stream**. We tunnel those bytes
verbatim — no need to parse `PullMessages`/`PushMessages`, because the egress gate
already sanitized the update upstream at ClientAppIo (§5). This keeps the relay
simple, Flower-version-agnostic, and protocol-agnostic.

- **Salvage ~verbatim** the loss-free, offset-based idempotent relay
  (`dsi_pump.R` researcher side / `dsi_tunnel.R` node side): the relay owns the
  byte offsets (`up_off`/`down_sent`), one fan-out `flowerTunnelExchangeDS` per
  cycle keeps all nodes lock-step, a generation counter resets a node on SuperNode
  redial, `relay_hb` gives the forwarder a liveness TTL, orphan reaper cleans up.
- **Keep** the node-side `dsi_tunnel_forward.py` TCP↔spool bridge: the SuperNode
  must dial a local TCP endpoint, and the bridge is what moves those bytes into the
  spool the relay drains. It is protocol-agnostic loopback IPC (not an open
  cross-host port), so it satisfies the no-open-ports constraint.
- **Few-round / one-shot default** (`dsflower.default_rounds = 1`): DP-SGD's RDP
  accounting composes over total local steps regardless of round count, so many
  local epochs in few rounds = same ε, far fewer expensive round-trips. The ~1-min
  federation setup + per-round latency dominate, so minimizing rounds is the main
  speed lever (SecAgg removal already gave ~1.8×; one-shot adds more).

## 7. App lifecycle: build → upload → install → validate → run

1. Researcher: `flwr build` → **FAB** (zip) + **sha256**.
2. `flowerAppPushDS(chunk_b64, offset)` (**aggregate**-side, idempotent offset
   append — same direct-string discipline as the relay, never `assign.expr`) until
   complete → `flowerAppInstallDS(token, expected_hash)`.
3. Node: reassemble, **verify sha256 == expected_hash** (reject mismatch), unpack
   into an isolated venv.
4. **Validation pipeline** (gates install — see §8). Pass → register by hash;
   fail → purge + generic error.
5. Run executes **only the validated FAB, by hash** — the researcher cannot run
   un-validated code.

**How the node runs *trusted* code without Flower node-pinning.** In Flower
deployment the SuperNode runs the ClientApp from the **submitted FAB** (even under
`--isolation=process`), so we cannot simply substitute a node-local app. Instead we
use **content-hash verification** (`flowerVerifyAppHashDS`, already present):

- **Tier 1 (model submission).** The node ships the canonical harness app + its
  content hash. The client builds a bit-identical copy and submits it; the node
  **verifies the submitted app's content hash == its trusted harness hash** and
  rejects any mismatch. The Opacus DP-SGD loop is therefore guaranteed to be the
  trusted one. The model architecture + hyperparameters arrive **only via the
  server-written, tamper-proof manifest** (`model_zoo` builds the module from the
  spec) — the client ships *no* training code.
- **Tier 2 (arbitrary app).** The submitted FAB's hash will not match the harness,
  so it is treated as untrusted: the **exfiltration scan** (§8) gates install, then
  it runs in the **sandbox** behind the **ClientAppIo egress gate**
  (output-perturbation DP). Code is contained + every release is gated, so
  node-trust is not required.

## 8. Validation pipeline (before any real data is touched)

Validation differs by tier, because the tiers carry DP differently. (These are
distinct checks — do **not** apply Tier-1's per-sample-gradient rules to a Tier-2
app, which legitimately runs its own `.backward`.)

**Tier 1 (model submission) — MODEL validation.** The researcher ships only an
`nn.Module` + config; the trusted harness owns the loop, so we validate the
*architecture* is DP-SGD-compatible:
1. **Opacus `ModuleValidator.is_valid`** — hard reject if false (no silent
   `fix()`). DP-incompatible layers (BatchNorm couples samples) break the
   per-sample sensitivity bound.
2. **Synthetic dry-run** in the harness on tiny synthetic data: confirm per-sample
   grads attach (`.grad_sample` per param) and the output is a fixed,
   row-independent shape — catches batch-coupled losses (Cox risk sets,
   contrastive) that silently break the bound.

**Tier 2 (arbitrary app) — EXFILTRATION scan.** The app trains itself and the
egress gate applies output-perturbation DP, so the app *may* call `.backward`
freely; validation is purely about containment (defence-in-depth for the sandbox,
which is the real boundary):
1. **AST scan** (Python `ast`): reject dynamic-code / process-escape / network-exfil
   constructs — `eval`/`exec`/`compile`/`__import__`, dunder reflection, and imports
   of `os`/`subprocess`/`socket`/`ctypes`/`requests`/`urllib`/`pickle`/`marshal`.
2. **Hash-pin the reviewed FAB** (§7): only the exact validated bytes may run.

The scan only closes trivial bypasses; the sandbox (M4) + egress gate (DP +
disclosure) are the actual guarantees. The exact import allow/deny list is settled
together with the sandbox's filesystem/data-access model (a Tier-2 app reads the
staged data, so `open` on the data mount must be permitted while everything else
is denied).

## 9. DataSHIELD options (all node-side, the `.dsf_option` double-fallback chain)

All read via `getOption("dsflower.X", getOption("default.dsflower.X", default))`,
so a node operator sets them like any DataSHIELD option.

- **Inherited disclosure filters:** `nfilter.subset` (min rows, default 3),
  `nfilter.tab` (min cell count, default 3), `nfilter.levels.max` (default 40).
- **DP budget / mechanism:** `dsflower.max_epsilon` (cumulative ε cap per dataset,
  default 10), `dsflower.max_delta`, `dsflower.dp_epsilon_ceiling` (per-run ε
  ceiling, default 10), `dsflower.privacy_ledger_path` / `dsflower.privacy_ledger_namespace`.
- **Disclosure:** `dsflower.min_train_rows`, `dsflower.min_cell_count`.
- **Run / upload limits:** `dsflower.max_rounds`, `dsflower.max_fab_bytes`
  (Tier-2 upload cap, default 50 MiB), `dsflower.max_concurrent_runs`,
  `dsflower.staging_root`, `dsflower.tunnel_loss_tolerance`, `dsflower.venv_root`,
  `dsflower.max_obj_pulls` / `dsflower.max_obj_pushes`,
  `dsflower.supernode_orphan_grace_minutes`.

## 10. Package module layout

**dsFlowerClient (researcher side):**
`options.R` · `app_build.R` (FAB + hash) · `app_upload.R` (chunked push + install)
· `dsi_relay.R` (salvaged, RPC-envelope framing) · `run.R` (install→validate→run)
· `collect.R` (pull sanitized results) · `dp_request.R` (declare ε/δ/clip; server
re-validates).

**dsFlower (node side):**
`options.R` · `app_store.R` (receive + hash-verify FAB) · `validate.R`
(AST + ModuleValidator + dry-run) · `sandbox.R` (locked-down exec) ·
`superlink.R` (per-run loopback SuperLink + SuperNode) · `dsi_exchange.R`
(salvaged, RPC-envelope framing) · **`egress_gate.R` (trusted DP + disclosure
gate)** · `dp_harness.R` (Opacus + RDP/PRV, always on) · `admission.R`
(input disclosure checks) · `policy.R` (salvaged disclosure controls, no
templates) · `staging.R` (salvaged manifest/descriptors) · `lifecycle.R`
(link up/down, orphan reaper).

## 11. Salvage / Rebuild / Delete

- **Salvage (port ~verbatim):** offset-based idempotent relay framing +
  generation/reap/heartbeat (`dsi_tunnel.R`/`dsi_pump.R`); `policy.R` disclosure
  controls (`.disclosure_min_cell/rows`, `.assertMinSamples`,
  `.validateClassDistribution`, `.sanitizeMetrics`, `.bucket_count`) minus the
  `.TEMPLATE_*` matrices; `staging.R` manifest/zero-copy; the `.dsf_option` chain;
  the client-side orphan reaper fix.
- **Rebuild (new):** `app_build/upload/store` + fab-hash; `sandbox.R`;
  `egress_gate.R` (ClientAppIo interceptor); `dp_harness.R` (RDP/PRV); `validate.R`;
  `admission.R`.
- **Delete:** all SecAgg paths; the `secure_aggregation` flag + `.resolve_secagg`
  + `use_secagg` plumbing; `.TEMPLATE_*` matrices + the 18 built-in templates;
  the legacy Strategy/compat (`start_grid`/`LegacyContext`) server entry; Tor /
  remote-SuperLink addressing. (The `dsi_tunnel_forward.py` bridge is **kept** — the
  transparent tunnel needs it; only the RPC-envelope idea was dropped.)

## 12. Implementation milestones (incremental, recoverable)

- **M0 — Reset.** Clean slate on a branch; remove deleted items; keep salvaged
  primitives; scaffold the new module layout. (Recovery tag in place.)
- **M1 — Transport.** RPC-envelope relay + per-run loopback SuperLink/SuperNode +
  one-shot loop → validate a plain (no-DP) run end-to-end over DSI.
- **M2 — DP harness (Tier 1) + minimal egress gate.** Opacus + PRV; validate DP
  on an uploaded model federated.
- **M3 — App lifecycle + validation.** Build/upload/install + hash verify; AST +
  ModuleValidator + synthetic dry-run.
- **M4 — Sandbox + Tier-2 gate.** gVisor/bwrap isolation; output-perturbation DP
  for arbitrary apps.
- **M5 — DataSHIELD options + disclosure backstop** on every output; admission
  checks.
- **M6 — Hardening.** Large payloads, reaping, concurrency limits, federated
  smoke tests, docs.

## 13. Key decisions (locked)

- Egress gate **at the ClientAppIo boundary** as a trusted interceptor (no Flower
  fork, not an in-app Mod). Transport stays a transparent DSI byte tunnel.
- Standard Flower **ServerApp aggregation on the researcher side** (revisit custom
  relay-side aggregation only if round-trips must be cut further).
- **Tier-1 (model submission)** is the recommended path for good numbers;
  **Tier-2 (arbitrary app)** is the flexible, coarser-DP fallback.
- **Cox / coupled-loss survival:** rigorous DP via **output perturbation on the
  dfbeta sensitivity**, or run as **Tier-2**; never present coupled-loss DP-SGD
  as rigorous.
