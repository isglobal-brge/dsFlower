# dsFlower 0.6.0

* Add a durable node-owned release cache for gated HookApps. Identical requests
  replay the exact released arrays and constant metrics even for nondeterministic
  applications. Cache keys are domain-separated from the v2 semantic identity,
  including verified Hook contents, effective private data and node-authored
  source/column selections; changed data or selections miss.
* Reserve bounded cache capacity before private work, serialize identical
  requests, and retain active-run pins through authoritative cleanup. Only
  unpinned entries may be evicted; cross-run replay lasts while an entry remains
  retained. Committed coordinates cannot authorize a second release.
* Add administrator-only persistent cache directory and byte-capacity settings
  with protected ownership and permissions. Declarative mechanisms, privacy
  calibration and the existing minimum-duration Hook envelope are unchanged;
  this release does not introduce a fixed-duration deadline.

# dsFlower 0.5.2

* Correct deterministic release identity to include server-authored request
  selections, including source operands, ordered columns, public vocabularies, imaging roles and
  resampling contracts. The semantic randomness contract is now v2. Identical
  requests and data still replay; distinct selections receive separate keys even
  when their private tensors or sufficient statistics are identical.
* Noise distribution, calibration and training paths are unchanged. Evidence
  produced under earlier runners remains valid as measurements of the same
  mechanism; exact noise realizations change under the corrected identity.

# dsFlower 0.5.1

* Recognize PyTorch's generated non-scriptable RemoteModule as installed runtime
  code only after checking its installed generator, private generation directory
  and exact template source. Execute the checked source without loading cached
  bytecode. All other foreign imports retain the default-deny package-pin rule.
  This fixes guarded LSTM/GRU construction with torch 2.6 and Opacus 1.6 without
  changing the canonical runner or any training/privacy mechanism.

# dsFlower 0.5.0

* Admit patient-level Weibull and lognormal AFT survival contracts, with bounded
  subject likelihoods, server-authoritative staging, and sticky release semantics.
* Add subject-level discrete-hazard training; the companion campaign packages
  the preregistered hazard-v2 confirmation and its selected h06 schedule while
  retaining the original v1 matrix as boundary evidence.
* Add patient-level binary segmentation with verified public encoder bytes and
  pinned spatial decoders. The companion campaign documents the public
  initialisation benchmark route and retains random-initialisation comparisons
  separately. Segmentation remains experimental (`vetted = FALSE`); the evidence
  route does not add an analyst checkpoint-loading API.
* Merge and synchronize the survival and segmentation runner contracts. The
  companion dsFlowerClient 0.5.0 evidence package replaces stale segmentation
  result files with verified protocol-v5 evidence and retains the v3/v4 boundary
  results and stale-file reconciliation provenance under explicit version labels.

# dsFlower 0.4.5

* SuperNode startup now revalidates opaque dsImaging feature views with the
  same server-authored patient privacy unit used during preparation. This fixes
  the linked clinical-radiomics workflow when several images belong to one
  patient without weakening its patient-level DP contract.

# dsFlower 0.4.4

### Complete dsImaging feature handoff

* Private Flower handles now live in locked session-owned state, so dropping a
  DataSHIELD session releases its tables, credentials, and staging metadata even
  when explicit handle cleanup did not run.
* `flowerInitDS()` accepts the new opaque dsImaging feature view and stages its
  full, seal-bound sample roster using patient-level adjacency. The source
  capability, collection seal, data hash, public label vocabulary, and exact
  sample-to-patient mapping are checked again before and after staging.
* A session that exported a raw imaging feature table cannot initialize a
  generic table or matrix in dsFlower. This conservative session taint blocks
  full-table, subset, copy, rebinding, and symbol-destruction laundering while
  preserving ordinary tabular workflows in clean sessions.
* Imaging target levels must match the operator-declared ordered vocabulary,
  and sample/patient identifiers cannot become targets or model features.
* Association contracts now use the server-authored privacy unit bound to the
  initialized imaging handle, so a global row-level default cannot replace the
  collection's patient-level adjacency.
* NIfTI, inline NRRD/MHA, single-file DICOM, PNG/JPEG and TIFF remain supported;
  detached sidecar containers and unsupported multi-file formats fail before
  a training manifest is created.

# dsFlower 0.4.3

### Disclosure admission

* The Python release guard now keeps a bounded, run- and policy-bound SQLite
  ledger of every claimed operation/fold/round coordinate in private staging
  before work begins and mirrors it into Flower `NodeState`. Claims are atomic
  across ClientApp processes and survive process restarts while the run remains.
  Alternating rounds can no longer evict replay history, changed payloads for a
  claimed coordinate fail closed, and holdout evaluation is restricted to the
  single final round budgeted by the manifest.
* Training preparation now enforces the server-owned DataSHIELD minimum on the
  staged privacy-unit count: rows for row-level adjacency and distinct patients
  for patient-level adjacency, including image runs. Below-threshold data frames
  and matrices fail with the same generic node error, while
  `flowerGetCapabilitiesDS()` advertises the effective threshold. This remains a
  per-training DP contract; it does not add or claim a cumulative privacy-budget
  ledger.

### dsImaging session boundary

* `flowerInitDS()` accepts an imaging collection only through an opaque,
  same-session dsImaging handle created by `imagingInitDS()`. Raw imaging
  resources, manifests, descriptors, storage references, legacy handles, and
  direct derived-asset references fail closed.
* The Flower handle is bound to the exact dsImaging capability and admitted
  sample-to-patient roster. Preparation checks the publish lock and roster
  immediately before and after staging, then verifies the exact staged mapping
  before training can start.
* Image targets/features cannot replace the protected patient identifier, and
  the staged manifest carries the server-authored patient privacy-unit
  contract used by the DP runner.
* Generic `ResourceClient` objects are inspected before materialization and
  cannot disguise an `imaging+dataset` resource to bypass dsImaging admission.

# dsFlower 0.4.2

### Fixes

* Live-federation cross-validation no longer fails closed with every round
  reported unavailable. The staged manifest was written with 15 significant
  digits (`jsonlite` `digits = NA`), which loses the low bits of the computed
  cross-validation budget allocation (for example
  `epsilon * 0.8 / folds = 4 * 0.8 / 3`); the trusted runner's release guard
  recomputes that fixed split in IEEE doubles and requires the manifest values
  within `rel_tol = 1e-15`, so every `cv-train` claim raised
  "manifest cross-validation budget differs from its fixed job allocation"
  before any private read, was swallowed by the privacy boundary as
  `public-preflight-unavailable`, and the ServerApp's all-rounds gate then
  refused the job. The manifest serializer now writes doubles with 17
  significant digits (`digits = I(17)`), which round-trips the exact double
  (worst case one ulp, ~2.2e-16 relative, inside the guard's tolerance); the
  same change removes the latent fragility for the holdout budget split. No
  privacy gate was relaxed: the release guard, its tolerance, the all-rounds
  cross-validation requirement, and fail-closed semantics are unchanged, and
  the byte-verified `dsflower_runner` is unchanged from 0.4.1 (the sticky
  noise identity, including the runner hash, is preserved). A regression test
  pins the release-guard-strength round-trip of every staged budget field.
  Found by the run-at-pin utility campaign's live SuperLink/SuperNode
  cross-validation harness (heart cohort, `pytorch_logreg`, 3 sites, 3 folds,
  epsilon 4), which no packaged suite reached at 0.4.1.
