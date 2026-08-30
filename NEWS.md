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
