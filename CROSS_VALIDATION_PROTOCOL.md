# Federated cross-validation protocol

The client exports this protocol as `ds.flower.cross_validate()`. It is a
dedicated metrics-only job; it does not wrap repeated fit or validation calls.
The default is three folds, while values from 2 through 10 are supported.

## Public job contract

One dedicated Flower app owns one cross-validation job. Its canonical resampling
contract contains the protocol version, `K`, and the common node-owned privacy
unit; its SHA-256 is carried through the manifest and final `cv.json`. The wider
node manifest independently pins the task/metric layout, model and training
recipe, and fixed job-level privacy allocation. Neither contract contains an
analyst seed, timestamp or retry counter.
The node derives one stable secret-keyed score from the resampling version, unit
semantics and row ordinal or canonical patient identifier, then maps that score
to one of `K` folds. Model, training and metric parameters do not enter this
assignment domain, so comparing HPO/model recipes cannot reroll the folds; `K`
changes only the deterministic score-to-fold mapping. The full job hash still
pins `K` and every execution parameter. A patient can belong to exactly one
fold. Recreating the same contract under the same custodial secret recreates the
same folds and DP noise without SQLite or memoization.
This is a node-local unit contract. A person duplicated across sites requires a
separately governed linkage/split mechanism if federation-wide fold consistency
is required; the app must not infer or export such linkage.

## Required execution

For each fold `f = 1..K`, the ServerApp performs a real, independently
initialized federated training:

1. Every node trains only on units whose assigned fold is not `f`.
2. Every configured round must complete on the exact roster.
3. The final fold aggregate is sent once to those nodes for prediction only on
   units assigned to `f`.
4. Each node adds the bounded sufficient statistics to namespaced
   `Context.state` records carried by Flower's in-memory node runtime and
   acknowledges without returning a vector, prediction, metric or model. The
   records are bound to the public CV-job hash, contract, layout and fold
   digests; they never use a file or database.
5. The fold model is discarded in memory and is never persisted or returned.

After all folds finish, each node makes exactly one DP release of its accumulated
OOF sufficient-statistic vector. The ServerApp pools one vector per node in
memory and publishes only the resulting task-level metrics. It returns no fold
scores, per-node values, OOF predictions or fold models. Selecting a final model
requires a separate explicit `fit`; cross-validation itself returns metrics only.

## Privacy and failure contract

The custodian-provided epsilon/delta pair is the total CV-job budget. The node
reserves 80 percent for training and assigns `0.8 / K` of the pair to each fold;
the remaining 20 percent is used by the single OOF release. This conservative
allocation remains valid when replace-one adjacency changes a unit's fold.
Larger `K` therefore gives each DP-SGD training less budget and can reduce
utility. This accounting exists only inside the active job. There is no
lifetime/global/resource budget, call counter, rate limit or operation-catalog
authorization.

The app is all-or-nothing. Any roster drift, missing training round, failed fold,
invalid acknowledgement, accumulator loss or malformed final vector prevents a
metrics artifact. Intermediate state is RAM-only and is consumed before the
final reply or by every abort message the node receives. A node unreachable
during abort can retain only its in-memory record until that runtime exits; it
cannot persist or return it. Restart means deterministic recomputation of the
entire job. The implementation must bind fold index and the
exact train tensors into each training's semantic PRF identity while binding the
canonical OOF sufficient vector, layout and noise scale into the final release.

## Verified invariants

The implementation tests all of the following:

- `K` genuine federated training cycles and held-out evaluation on every fold;
- patient/row assignment invariance and train/test disjointness;
- one OOF vector per node and no earlier private evaluation release;
- no persisted/returned fold model, prediction, fold metric or node transcript;
- exact fixed job-budget composition and deterministic retry semantics;
- all-or-nothing output under failures at every fold boundary; and
- byte-identical server/client runners in the coordinated pre-promotion release
  check; each copy then runs its platform matrix independently.
