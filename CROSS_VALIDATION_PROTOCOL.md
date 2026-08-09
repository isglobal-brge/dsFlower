# Federated cross-validation protocol (design; not yet an API)

This document fixes the protocol that a future `cross_validate(recipe, spec)`
operation must implement. It is intentionally not exported yet: wrapping the
existing fit and validation calls would expose fold artifacts and would not be
honest out-of-fold validation.

## Public job contract

One dedicated Flower app owns one cross-validation job. Its canonical contract
contains the resampling protocol version, `K`, task/metric layout, model and
training recipe, common node-owned privacy unit, and the fixed job-level privacy
allocation. It contains no analyst seed, run token, timestamp or retry counter.
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
4. Each node adds the bounded sufficient statistics to an in-memory OOF
   accumulator and acknowledges without returning a vector, prediction, metric
   or model.
5. The fold model is discarded in memory and is never persisted or returned.

After all folds finish, each node makes exactly one DP release of its accumulated
OOF sufficient-statistic vector. The ServerApp pools one vector per node in
memory and publishes only the resulting task-level metrics. It returns no fold
scores, per-node values, OOF predictions or fold models. Selecting a final model
requires a separate explicit `fit`; cross-validation itself returns metrics only.

## Privacy and failure contract

The custodian-provided epsilon/delta pair is the total CV-job budget. A fixed,
server-owned allocation composes all `K` training mechanisms plus the single OOF
release; no fold or client parameter may expand it. This accounting exists only
inside the active job. There is no lifetime/global/resource budget, call counter,
rate limit or operation-catalog authorization.

The app is all-or-nothing. Any roster drift, missing training round, failed fold,
invalid acknowledgement, accumulator loss or malformed final vector prevents a
metrics artifact. Intermediate state is RAM-only and restart means deterministic
recomputation of the entire job. The implementation must bind fold index and the
exact train tensors into each training's semantic PRF identity while binding the
canonical OOF sufficient vector, layout and noise scale into the final release.

## Release gate

The client API remains unavailable until tests demonstrate all of the following:

- `K` genuine federated training cycles and held-out evaluation on every fold;
- patient/row assignment invariance and train/test disjointness;
- one OOF vector per node and no earlier private evaluation release;
- no persisted/returned fold model, prediction, fold metric or node transcript;
- exact fixed job-budget composition and deterministic retry semantics;
- all-or-nothing output under failures at every fold boundary; and
- byte-identical server/client runners on every supported platform.
