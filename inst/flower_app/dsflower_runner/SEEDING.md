# Deterministic release identity and neighbourhood replay in 0.7.2

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
R. No node-side initializer sandbox or initialization cache is added.

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
requests serialize. R admission freezes the custodian cache settings without
reserving capacity; only would-be-fresh anchors reserve the complete public run
horizon before Hook execution. Exact/near replays bypass inner-cache capacity
admission. The exact Hook cache's eviction/tombstone policy is unchanged.
The outer neighbourhood store decides first and permanently retains the complete
released payload, including any already-applied Hook FedProx step. A near match
never calls the Hook or applies FedProx a second time.

FedProx uses public `mu` in `[0,1]`. Positive neural mu requires every scheduled
`learning_rate * mu <= 1`; its proximal step follows the DP optimizer and L1 prox.
Hook relaxation is one post-gate step with eta=1, not per-step instrumentation of
arbitrary Hook optimizers. Zero is normalized to FedAvg with no extra arithmetic
on supported neural/Hook paths. Trees (all five engines), association and
standalone validation reject raw FedProx, including zero.

## Neighbourhood replay

Version 0.7.2 mitigates the equality oracle described in
[isglobal-brge/dsFlower#7](https://github.com/isglobal-brge/dsFlower/issues/7)
with immutable neighbourhood anchors. For each public request R (including its
incoming model and round), the node scans **all** retained anchors and returns
the complete stored payload of the **oldest** anchor at distance `d < k`. If none
is eligible, it computes the unchanged v3 content-bound release and appends one
new anchor. Near inputs never become anchors. Newer anchors cannot displace an
older eligible anchor, so replay is stable without per-input bindings.

Distance counts unordered canonical privacy units with multiplicity:
`d = max(|M| - c, |N| - c)`, where `c = sum(min(M[t], N[t]))`. One insertion,
deletion or replacement counts once; in patient mode a patient's complete
selected records form one unit. The default `k` is the node's `nfilter.subset`
(or its `default.` option), otherwise 3, with a floor of 2. Custodians can set
`dsflower.neighbourhood_k` (or `default.dsflower.neighbourhood_k`); the effective
value is frozen per R. Analysts cannot change it or force refresh.

An analyst can no longer test a one-unit difference against an existing release
by obtaining a fresh answer inside that anchor's neighbourhood. This is a
**mitigation, not transcript DP**: the hard `k-1`/`k` boundary and boundaries
between anchors still distinguish some one-unit neighbours. An input must be
at least k from **every** anchor to receive a fresh release. Small updates can
therefore return stale models or statistics; uncertainty intervals do not
include this staleness. No hit/near/fresh status, distance or anchor identifier is
returned. Timing and availability remain outside the guarantee.

The rule covers every neural DP-SGD round, gated Hook release, all five native
tree engines, private validation, holdout, CV fold training and OOF output, and
association. Each round/fold/evaluation has its own R. Completed federation
trajectories replay when the same incoming public models and eligible anchor
choices recur; this is not a universal neighbouring-world transcript claim.
Calibration, sensitivities, accounting, FedProx and the fresh R/B/K identity
remain unchanged. Conditional DP mechanisms compose under their existing
assumptions; no lifetime privacy budget is introduced.

## Migration, state and residual scope

The permanent node-local store defaults to `<node-secret-path>.neighbourhood`.
First release initializes it automatically and pins its random UUID at
`<node-secret-path>.neighbourhood-id`, beside the secret rather than inside the
store. The store uses owner-only directories/files (`0700`/`0600`), keyed unit
fingerprints, MAC-authenticated records and complete payload bytes; it never
stores raw source records or noise seeds. Records are verified before decoding,
and a per-R lock serializes selection and durable anchor commit before release.
There is no eviction, expiry or per-attempt ticket.

The default limits are 256 anchors per R and 64 GiB of store capacity. Fresh
commits must fit both logical retained bytes (payloads, fingerprints and record
overhead) and SQLite allocated pages plus fixed state headroom. Provision extra
physical disk space for transient SQLite journals and filesystem allocation slack. Only an
input that would create a new anchor is refused at a limit, with one stable
error; exact and near replays remain available. A refusal substitutes for a
fresh release and reveals the same fact that the input is far from every anchor.
The shared byte cap also exposes a weak cross-user aggregate signal about prior
store growth. These resource settings are not privacy parameters. Increase
capacity with all retained state intact; never delete anchors to make space.

On Windows, the key parent and store directory require private inheritable
ACLs: the service identity and trusted system/administrator principals may
access the state; other grants, including read access, are rejected. Reparse
points and hard links fail closed. SQLite uses its Windows VFS for durable
commits; the first UUID pin is flushed and published without replacement using
write-through publication. POSIX nodes retain file/directory fsync and flock.
The Windows adapter has portable contract tests; Windows service and crash
validation remains outstanding.

Missing or corrupt established state, missing original keys, unsafe permissions
or a mismatched UUID fail closed. The permanent
`<node-secret-path>.neighbourhood-id.lock` also detects loss of both the store
and local UUID pin. Optional `dsflower.neighbourhood_store_id` externally pins
the UUID and detects loss of all local store markers; without it, losing the
store, UUID pin and initialization lock together can resemble first use. Stop
all workers before restoring a consistent backup of the secret, store, UUID pin,
permanent locks and retained Hook cache. MACs do not detect rollback to an older
complete valid snapshot: avoiding rollback remains a custodian/storage assumption.
Runtime upgrades create new public request domains, so drain jobs and upgrade
both packages together; retain old state and treat new domains as additional
releases. Existing per-release DP does not prove the private anchor-selection
transcript DP.

Fresh computation retains the complete content binding B and v3 noise subkeys.
The anchor selector uses separate node-private fingerprint and integrity subkeys
and canonical source-unit records, including full parent units for holdout/CV.
It does not derive distance from the whole-data B digest or from pooled outputs.
With k=3, let Q remove two units from A, then let B remove three units from A.
Q selects A; after B becomes a fresh anchor Q still selects older A even though
B is nearer. No per-input response binding is needed and Q never becomes an
anchor. An existing R retains its frozen k when the custodian changes the option;
only new public identities use the new value.

Equal bytes are not proof of equal private inputs: nearby inputs deliberately
share a release, and finite/clamped outputs can also collide. A public model,
key, runtime or contract change creates a new request domain. No provenance-only
fast path, analyst refresh nonce or general adaptive transcript-DP claim is added.
