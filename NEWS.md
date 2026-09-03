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
