# dsFlower 2.0 — Architecture & Implementation Plan

> Status: design locked, implementation starting (from-scratch rewrite).
> Pre-reset recovery tags: `pre-reset-dp-validated` (dsFlower `acf18a1`, dsFlowerClient `aa17da7`).

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
  ClientApp (researcher's app, in SANDBOX: no net/FS)
       │  emits one Flower Message (the update)
       ▼  (ClientAppIo, local)
  SuperNode (framework, trusted)
       ▼
  ┌─────────────────────────────────────────┐
  │  TRUSTED DP + DISCLOSURE EGRESS GATE     │  ← the ONLY path off the node
  │  (integrated into the DSI relay)         │
  └─────────────────────────────────────────┘
       │  sanitized Message only
       ▼  DSI channel (datashield.aggregate)
                                              SuperLink + ServerApp (aggregation)
```

- The researcher's **ClientApp runs sandboxed** (`--isolation=process` + gVisor /
  bubblewrap; no network, read-only data mount, scrubbed env, seccomp, cgroups).
- The **egress gate is integrated into the node-side DSI relay** — our own
  trusted framework code, the **sole** writer to the DataSHIELD channel. Mods are
  rejected as an enforcement boundary because they run *in-process* with the
  untrusted ClientApp and can be disabled by it.
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

The gate is a trusted node-side component, sole writer to the transport,
intercepting the Message **after** the sandbox produces it. It treats every
Message as adversarial: wrong shape/dtype → reject; conforming tensors →
**clipped + noised unconditionally**; metrics/sizes → deterministic backstop.

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

## 6. Transport — DSI as an RPC-envelope relay (efficient)

The **new Flower Fleet API is discrete request/response RPCs**
(`RegisterNode`, `ActivateNode`, `PullMessages`, `PushMessages`, `GetRun`,
`GetFab`, `PushObject`/`PullObject`, `SendNodeHeartbeat`) — gRPC byte-streaming
was removed. So we relay **framed RPC envelopes** `{rpc, seq, body_b64}`, not raw
socket bytes — a far better fit for DataSHIELD's request/response channel.

- **Salvage** the loss-free, offset-based idempotent framing from the current
  relay (`dsi_pump.R` / `dsi_tunnel.R`): relay owns offsets, generation counter
  resets a node on restart, `relay_hb` heartbeat TTL, orphan reaper.
- **Drop** the SuperNode↔forwarder TCP bridge (`dsi_tunnel_forward.py`).
- **Synthesize heartbeats locally** on the node (TTL file) → 0 round-trips.
- **Few-round / one-shot default** (`dsflower.default_rounds = 1`): DP-SGD's RDP
  accounting composes over total local steps regardless of round count, so many
  local epochs in few rounds = same ε, far fewer expensive round-trips. The ~1-min
  federation setup + per-round latency dominate, so minimizing rounds is the main
  speed lever (SecAgg removal already gave ~1.8×; one-shot adds more).

## 7. App lifecycle: build → upload → install → validate → run

1. Researcher: `flwr build` → **FAB** (zip) + **sha256**.
2. `flowerAppPushDS(chunk, offset)` (assign-side, idempotent offset append) until
   complete → `flowerAppInstallDS(token, expected_hash)`.
3. Node: reassemble, **verify sha256 == expected_hash** (reject mismatch), unpack
   into an isolated venv.
4. **Validation pipeline** (gates install — see §8). Pass → register by hash;
   fail → purge + generic error.
5. Run executes **only the validated FAB, by hash** — the researcher cannot run
   un-validated code.

## 8. App validation pipeline (before any real data is touched)

1. **AST scan** (Python `ast`): reject `os.system`/`subprocess`/`socket`/`eval`/
   `exec`/`__import__`/dunder reflection / network+file-escape imports / custom
   training loops calling `.backward`/`.grad` / `torch.no_grad` / in-place grad
   mutation; restricted unpickler.
2. **Opacus `ModuleValidator.is_valid`** — hard reject if false (do **not**
   auto-`fix()` the researcher's architecture silently). Rejects DP-incompatible
   layers (BatchNorm couples samples → unbounded sensitivity).
3. **Synthetic dry-run**: one round in the sandbox on tiny synthetic data;
   confirm per-sample grads attach (`.grad_sample` per param), the output shape
   is fixed and row-independent, and only allow-listed records are emitted. This
   catches batch-coupled losses (Cox risk sets, contrastive) the AST misses.

## 9. DataSHIELD options (all thresholds)

- **Inherited:** `nfilter.subset`, `nfilter.tab`, `nfilter.levels.max`
  (standard `getOption("nfilter.x", getOption("default.nfilter.x", default))`).
- **`dsflower.*`:** `dp_max_epsilon`, `dp_delta`, `dp_clip_norm`,
  `dp_noise_multiplier_min`, `min_train_rows`, `min_cell_count`,
  `default_rounds`, `max_rounds`, `max_fab_bytes`, `allow_app_upload`,
  `sandbox_backend`, `relay_loss_tolerance`, `staging_root`, `max_concurrent_runs`.

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
- **Refactor:** relay payload from raw socket bytes → framed Fleet-RPC envelopes.
- **Rebuild (new):** `app_build/upload/store` + fab-hash; `sandbox.R`;
  `egress_gate.R`; `dp_harness.R` (RDP/PRV); `validate.R`; `admission.R`.
- **Delete:** all SecAgg paths; the `secure_aggregation` flag + `.resolve_secagg`
  + `use_secagg` plumbing; `.TEMPLATE_*` matrices + the 18 built-in templates;
  the legacy Strategy/compat (`start_grid`/`LegacyContext`) server entry; Tor /
  remote-SuperLink addressing; `dsi_tunnel_forward.py`.

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

- Egress gate **integrated into the trusted DSI relay** (no Flower fork).
- Standard Flower **ServerApp aggregation on the researcher side** (revisit custom
  relay-side aggregation only if round-trips must be cut further).
- **Tier-1 (model submission)** is the recommended path for good numbers;
  **Tier-2 (arbitrary app)** is the flexible, coarser-DP fallback.
- **Cox / coupled-loss survival:** rigorous DP via **output perturbation on the
  dfbeta sensitivity**, or run as **Tier-2**; never present coupled-loss DP-SGD
  as rigorous.
