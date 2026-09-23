# Deterministic release identity

The semantic contract is `dsflower-semantic-randomness-v2`. A custodial HMAC key
binds each release to its mechanism, effective public configuration, privacy
policy, release coordinates, public model arrays, effective private tensors or
sufficient statistics, and execution profile. Private-array hashing is retained.

Every node release also carries a `request-selection` block. The trusted caller
builds it with `seeding.request_selection(node_manifest)`, never from a Flower
config field or analyst-provided free text. Positive field selection excludes
tokens, message IDs, timestamps, staging files, asset filesystem roots, arbitrary
extra fields and private counts. Ordered feature/target/vocabulary lists remain
ordered, including survival time/event roles. Selected image/mask asset aliases
and their column metadata are included; unrelated assets are excluded.
The selected manifest JSON is represented by its canonical SHA256 digest, so
large ordered schemas do not exhaust the outer key encoder's size limits.
The server preserves the selected source operand and descriptor identity as
protected `request-source` metadata; transient Flower handles remain excluded.

The block covers column roles, patient/unit policy and canonicalization,
vocabularies/bounds, model/loss/imaging/segmentation pins, validation and
association contracts, holdout/CV geometry and assignment contracts, and pinned
aggregation settings. Native adapters additionally bind their validated public
schema, effective engine parameters and contribution policy, excluding resource
ceilings and operational data scope. Native CV binds its validated operation and
fold. Hook keys additionally bind the node-verified uploaded package hash; final
noise remains bound to the validated clipped update through `bind_seed`.
Pooled CV release keys also bind the ordered public fold-model digests retained
in existing node RAM; private statistic hashes do not enter this public block.

Public segmentation initialization additionally binds `segmentation-decoder-init`,
`segmentation-public-manifest-sha256` and `segmentation-public-checkpoint-sha256`
in both the neural config and request selection. These are admitted and pinned
by the node; the manifest hash binds the complete provenance. The existing
encoder and patient-image-selection pins retain their meanings. Explicit
`random` is canonicalized to the omitted default. Registry paths and the public
checkpoint transport payload do not enter the seed contract.
As with every runner update, the source-bound execution fingerprint changes;
this preserves the default mechanism and request contract, not an older
runner's exact deterministic realization.

Identical selections and identical effective inputs replay byte-identically
within the same node-key/runtime domain. Different selected columns derive
different keys even if their private contents coincide. Validation and
association therefore cannot identify a request solely by an equal sufficient
statistic. Public model arrays also distinguish validation requests whose
predictions happen to agree. Hash/PRF separation has the usual computational
collision assumption; it is not a mathematical claim that finite keys are
collision-free.

The v2 change is confined to key derivation. Noise distribution, calibration,
accounting, sampling and clipping algorithms, and training paths are unchanged.
Earlier-runner evidence remains valid as measurements of the same mechanism.
Its exact random realizations are not v2 known answers. Holdout/CV record-local
assignment HMACs are separate from release keys and are unchanged.
