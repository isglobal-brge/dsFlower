# Gated envelope and durable release cache

Status: design review stopped before implementation. Neither feature is shipped.
Baseline: dsFlower `eef214a`, dsFlowerClient `0837265`; semantic identity
`dsflower-semantic-randomness-v2`.

## Required release contract

For every admitted gated training round, the administrator would pin a public
duration D, by installed Hook digest or a default. The origin would be admission
of that round at the trusted node boundary, before private loading or cache
lookup. Release would occur at that origin plus D, including on cache hits.
Early completion would wait. An overrun, child crash, invalid update or exhausted
child quota would contribute a zero update through the existing calibrated
mechanism. Neither a failure metric nor an earlier/later reply would distinguish
these outcomes. The configured child resource quotas would remain in force.

This is stronger than a child timeout followed by minimum-duration padding.
All private loading, identity hashing, partitioning, update validation, noise,
durable persistence and reply preparation must fit behind the same release
boundary. Cleanup must be isolated from both that boundary and later rounds.
A timer running in a process that the Hook can starve or kill is insufficient.

The existing `dp_egress_time_pad` path cannot be relabelled as a deadline. A
future implementation should replace it for admitted Hooks with the stronger
boundary and reject conflicting legacy/new settings. Keeping the legacy path
as a fallback must not count as satisfying the new gate.

## Rounds, sample-and-aggregate and Flower

Each round needs its own fixed schedule; a previous round's completion cannot
shorten the next round's budget. The round coordinate and incoming public model
remain part of the semantic identity. Retries use the same duration, even when
all released bytes are already cached.

Sample-and-aggregate needs independent, public block slots. One block overrunning
its slot must zero only that block. A global cutoff that zeros the entire round,
or skips subsequent blocks because an earlier block ran slowly, can allow one
changed record to suppress unrelated block contributions. The existing
`min(2C, 4C/k)` argument does not justify that replacement. Independent slots
must also cover each block's input/output processing without shared pressure
from one block changing another block's outcome.

Flower can carry a locally scheduled reply without changing round semantics.
The coordinator must explicitly set a round timeout greater than the largest
node deadline plus public dispatch/transport allowances. Message TTL must cover
that timeout, and the outer R run timeout must cover every round and setup.
Insufficient public timeout/TTL can be rejected before admission. Merely raising
these timeouts does not make transport independent of private execution.

The canonical ServerApp currently calls `strategy.start` without a timeout at
`inst/flower_app/dsflower_runner/server_app.py:780`. Installed Flower 1.31.0
defaults to 3,600 seconds per round, while the administrator's existing pad can
reach 230,405 seconds. The R client also defaults to a 3,600-second whole-run
timeout. These are fixable coordination gaps, not a proof that Flower cannot
support scheduled replies.

## Permitted unavailability

Demonstrably public, pre-private admission conditions could emit unavailability
under the requested contract:

- Hook execution disabled, or missing sandbox/resource/deadline attestation;
- invalid public model, public configuration, installed Hook integrity or
  unsupported runtime;
- insufficient public Flower timeout/TTL or an administratively closed run;
- unsafe/missing owner-controlled cache state, or insufficient capacity for a
  reservation computed solely from public model bounds and round count,
  established before private execution with resources protected from Hooks.

Private-data parsing errors, child failures, resource pressure caused by a Hook,
post-execution cache write failures and loss of the reply process cannot simply
be added to this list. Disk-full, out-of-memory and missed heartbeats are not
intrinsically independent of private data. The current deployment does not
isolate them sufficiently to make that assertion. External outages also require
an explicit fault model; this design does not promise a reply from a dead node.
Infrastructure failure after private work could also qualify if its independence
were established by that fault model and enforceable isolation. Its position in
the execution sequence alone neither proves nor disproves independence.

## Durable cache design

The cache would apply only to gated Hooks. Declarative tracks retain their
existing strict deterministic kernels and existing behavior; no optional cache
would be added to them in this change.

Use a domain-separated key derived from the existing Hook master seed, such as
`seeding.sub_seed(master, "gated-release-cache-key/v1")`. The v2 master already
binds the effective private arrays and unit identifiers, request selections,
installed Hook digest, public model, application parameters, privacy policy,
round and numerical runtime. Run tokens, message IDs, paths and timestamps stay
out of the key. Do not store the master or noise keys themselves. A changed
selection or data digest must miss even if an old in-memory reply exists.
The effective new deadline and block/failure policy must also enter the Hook's
semantic configuration because they can change its update; adding these inputs
does not require changing the v2 encoder label. Cache paths and storage capacity
remain operational settings and do not become noise-reroll axes.

Persist the final noised arrays with exact dtype, shape and bytes, and constant
reply metrics, using a bounded non-pickle encoding. Persist the noised-zero
outcome in exactly the same way. A retry reconstructs that content without
executing the Hook. Flower transport metadata must still identify the current
request; byte identity means released arrays and metric content, not the entire
transport envelope.

The node administrator would select a persistent protected directory, outside
staging and outside Hook mounts, and a byte capacity. Require owner-only
permissions (`0700` directories, `0600` files), safe ownership, regular files and
no symlinks. Analyst configuration, nested application parameters and manifest
overrides must reject deadline and cache controls. Atomic replacement or a
transaction, durable synchronization, and per-key exclusion are necessary so
concurrent identical requests cannot choose different releases.

Reserve the complete public worst-case storage budget at run admission. Pin
entries used by an active run until authoritative cleanup closes the run and
rejects further messages. Evict only unpinned entries, oldest first; never evict
live entries to admit another run. Active-run capacity exhaustion must reject
admission before private work. Crash recovery must retain uncertain run pins.
This guarantees retention throughout the run, with cross-run replay only while
an entry remains retained. Indefinite replay after eviction is impossible for a
nondeterministic application without retaining its release or refusing new
identities when storage fills; it must not be claimed.

The durable cache must integrate with `release_guard`: its existing durable
coordinate ledger rejects an older/restarted round once the single in-memory
reply slot advances. A cache added only inside `gated_local_update` would not
be reached for those retries. Gated requests must also bypass the current
early in-memory replay until their data identity is verified. A different
payload for an already claimed coordinate remains forbidden.
Bind a committed coordinate to the semantic cache key as well as the public
payload: changed data or selection must not authorize a second release at that
coordinate. Such a cache miss needs a newly admitted run/coordinate.

Cache replay is deterministic post-processing of the first released mechanism
output. It does not supply a second noisy observation and prevents fresh
application randomness from turning an identical retry into another release.
It leaves clipping, sensitivity, noise distribution and accounting unchanged.
It does not make distinct requests free or create a cross-query privacy budget.
This argument assumes the cache lookup, storage failure and hit/miss timing
are not exposed as additional private-dependent transcript signals.

## Why implementation stops

The present runtime cannot enforce the complete contract above:

1. `client_app.py:1260` starts padding before private loading and hashing, but
   that work runs in the trusted Flower process without an independent deadline
   controller. `_run_isolated` bounds `Popen.wait`; input serialization, output
   parsing and recursive cleanup are outside that wait. Padding at
   `client_app.py:1282` precedes reply encoding at line 1309. Parent-side private
   exceptions become `execution-unavailable` at line 1318.
2. `ARCHITECTURE.md` places the SuperNode and Hook descendants under shared
   cgroup limits. `R/runtime.R` passes attestation flags but does not establish a
   separately reserved release process. A data-dependent allocation or load in
   a Hook can affect the process that must send its noised-zero substitute.
   Existing quota values alone cannot guarantee survival or dispatch latency.
3. The selected reply must be durably committed before release. Returning while
   its write remains pending can produce a different reply after restart;
   waiting for an unbounded filesystem synchronization can miss the deadline.
   In the present shared-resource deployment, a post-private write failure cannot
   simply be called public unavailability. A
   precommitted fallback additionally needs a crash-consistent decision protocol
   preventing a late success commit from replacing an already released zero.
4. Returning from the ClientApp is not the end of the Flower release path.
   Installed Flower 1.31.0 subsequently hashes/serializes reply objects and
   transfers them through ClientAppIO and SuperNode. SuperLink can generate
   node-unavailable or expired-message errors independently of the runner's
   metrics. A local timer cannot turn those errors into the node's calibrated,
   durably chosen zero release. Ignoring missing replies or inserting a public
   unchanged model at the coordinator would violate the existing complete-round
   and mechanism contracts.

The installed sources establishing the last point are
`flwr/supernode/runtime/run_clientapp.py` (`push_message`),
`flwr/supernode/start_client_internal.py`,
`flwr/server/superlink/linkstate/utils.py` (node/message availability), and
`flwr/app/message/message.py` (actual construction time and remaining TTL).

A viable extension needs a protected release service spanning durable storage
and Flower submission, private work in independently constrained workers,
independent block slots, a safely prepared data identity/fallback, and an
explicit enforceable resource/storage/transport fault model. These are new
deployment guarantees, not consequences of the existing attestation or a larger
timeout. This review does not claim fixed deadlines are inherently impossible
in Flower. We stop rather than implement a timer-and-cache approximation: a
complete implementation would require the additional deployment guarantees
above, which cannot be established from the present controls. Production code,
architecture claims, versions and feature release notes remain unchanged.

## Validation required before a future feature release

Exercise the actual Flower boundary for fast/slow Hooks, each independent block
overrun, crash, invalid output, quota exhaustion, delayed cleanup, hashing,
serialization and persistence. Verify one fixed release schedule, identical
metric content and the existing noised-zero mechanism, including faults after
private work. Test cache hits with a nondeterministic Hook across process
restarts and earlier rounds, changed data/selections, concurrent identical
requests, crash points around commit, protected permissions and active-run
eviction. Run both complete R and Python package suites on clean checkouts and
the runner sync check. Unit tests mocking away the emitter, quotas or storage
cannot establish the deployment guarantee.
