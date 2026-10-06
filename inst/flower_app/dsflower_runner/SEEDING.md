# Deterministic release identity in 0.7.1

Version `dsflower-semantic-randomness-v3` separates a public semantic request
identity R from a node-private content binding B. The trusted runner validates
closed schemas and constructs R before opening private tables, images or raw
statistic state. R contains the mechanism, effective public selections/model,
initial and incoming model content, training controls, local strategy, node
privacy policy, operation/round/fold, and runtime fingerprint. Raw epsilon and
delta remain semantic even when their calibrated scales round to the same value.
Private population sizes and computed sampling/noise geometry belong in B.

B binds the complete selected source-unit multiset, the actual effective tensors,
validated private execution geometry, and the subset role/assignment digest.
Different source inputs that pool, bin or predict to the same sufficient vector
still have different bindings. Unselected columns and filesystem locations do
not enter either layer. Unit digests, IDs, exact counts and assignments remain
node-local; no new equality status is returned to an analyst.

```text
R = SHA256(frame("request-v3", canonical_public_json))
B = SHA256(frame("data-binding-v1", canonical_private_json))
K = HMAC-SHA256(node_secret,
    frame("dsflower/semantic-prf/v3", R) || frame("data-binding", B))
```

Frames carry explicit lengths. Typed `RequestIdentity` and `DataBinding` objects
are required to derive K. Labeled subkeys separate Gaussian noise, sampling and
training. Adaptive tree stages bind stage coordinates, prior public DP state and
current statistics below K, avoiding a cycle in parent-key derivation. XGBoost
receives its separate fixed-point native noise subkey; its accountant and sampler
are unchanged. Runtime identity reuses the existing measured dependency/backend
facts and includes all runner contents, plus the verified native bundle where
used. CPU/GPU, package, bundle or runner changes can create a new release.

## Canonical inputs and public initialisation

Ordered feature/target roles, model operands, tensor axes, label vocabularies,
bounds, loss parameters and patient grouping are semantic. Aliases, data symbols,
handles, session/run/message IDs, paths, timestamps, archive packaging, physical
row order and pure server aggregation controls are excluded. Authorization and
whole-job integrity checks still verify their original transport pins.

Before computation, selected rows are encoded with typed scalar frames and
ordered using a node-secret HMAC plus a canonical byte tie-break. Identical rows
retain multiplicity and stable per-content occurrence tokens. All visits for one
canonical patient form one unit; visits are ordered before floating reductions.
Patient IDs retain `trim-utf8-v2` semantics and are not renameable nuisances.
Sequence/time/channel axes remain ordered. Images and masks bind decoded content
and selection-driving identifiers, so relocating or repacking equal assets does
not redraw noise. Source records and final tensors are both bound for neural,
Hook, tree, association, validation, holdout and CV releases.

Every trusted neural server path constructs its default initial model from the
canonical public model specification under an isolated public seed: tabular,
structured/sequence, vision heads, AFT/discrete-hazard survival and random
segmentation. Python, NumPy and Torch RNG state is restored; Torch CPU construction
uses `torch.random.fork_rng`. All CV folds start from the same model. Existing
frozen-encoder profiles retain their established initialization semantics.
There is no node-side recomputation or expected-tensor comparison for round-one
training arrays, including checkpoint-initialized paths: existing
shape, dtype and value admission remains, and incoming contents remain bound in
R together with the initial-model hash. Approved public checkpoints retain their
independent content/admission verification.

Hook `initial_arrays` executes on the server inside an isolated Python/NumPy/Torch
RNG context seeded from its public initialization contract. Arbitrary code can
still use OS entropy or other nondeterministic inputs: a changed initializer
output creates a new release per run because incoming array contents remain in
R. No node-side initializer sandbox or initialization cache is added in 0.7.1.

## Assignments and replay

Holdout and CV assignment uses a separate unit-local HMAC domain. It depends on
a canonical patient ID, or the selected row content plus duplicate occurrence,
not R, B, model, hyperparameters, dataset alias or run. Changing a row can move
that row between folds; unaffected tokens retain their assignments. Changing
selected columns can change all row tokens. Holdout fractions threshold the same
score and are nested; CV fold counts remap that score. No row-ordinal fallback is
accepted. Parent source and assignment bindings are checked across job phases.

Hook sample-and-aggregate uses fixed custodian-pinned block count and unit-local
buckets. Its execution key and child seeds depend on public R and block index,
not the full private dataset. Empty blocks remain. Replacing one unit affects at
most its old/new blocks, preserving the existing `min(2C,4C/k)` bound. Holdout
uses the absent-unit sensitivity for both rows and patients; pooled CV OOF keeps
its existing replacement bound. See `PRIVATE_VALIDATION_CV.md` in each package.

All already-private server contributions are ordered by canonical content hash
with a byte tie-break before sums or ensembles. Reply arrival order and ephemeral
Flower node IDs cannot alter aggregate model/vector bytes.

Each admitted Hook uses its existing durable first-release cache, now keyed in
the v3 domain. The final validated/clipped update remains separately bound to its
noise substream. Stored entries contain final released arrays/constant metrics;
no master or noise key is persisted. Active entries stay pinned; concurrent
requests serialize. Cross-run replay lasts only while an entry is retained.
Eviction/tombstone policy is unchanged in 0.7.1; no indefinite replay claim is made.
A cache hit returns final bytes, including any already-applied Hook FedProx step.

FedProx uses public `mu` in `[0,1]`. Positive neural mu requires every scheduled
`learning_rate * mu <= 1`; its proximal step follows the DP optimizer and L1 prox.
Hook relaxation is one post-gate step with eta=1, not per-step instrumentation of
arbitrary Hook optimizers. Zero is normalized to FedAvg with no extra arithmetic
on supported neural/Hook paths. Trees (all five engines), association and
standalone validation reject raw FedProx, including zero.

## Migration and residual scope

Drain active jobs and upgrade both packages/runners together. Restage requests
under v3: there is no mixed v2/v3 fallback. Preserve the node secret and retained
Hook cache on persistent storage. Missing secrets are provisioned as before;
existing malformed, wrong-owner or wrong-mode secret files fail closed with a
custodian recovery instruction. There is no new initialization marker. Deliberate
secret replacement/loss starts a new release domain and is not a free replay.

Secret-keyed, domain-separated streams reproduce the same release for an exact
semantic replay. Incoming model contents and all effective private inputs remain
bound. Omitting either would allow different exact updates to share noise and
can make differencing cancel that noise.

Content equality also affects the joint distribution of related releases. If a
permitted private-data transformation is a no-op, its release can coincide exactly
with the original; otherwise the changed data select a different stream. For
high-dimensional unrounded outputs this can provide a strong equality test.
Finite or clamped outputs may collide, so equal bytes are not a universal proof
that private datasets are identical. No claim of differential privacy for an
unrestricted adaptive transcript of such equality tests is made. 0.7.1 does not
implement neighborhood anchoring or private-data provenance admission. Changing a
key, runtime, contract or custodial snapshot is a new release, not a free replay.
