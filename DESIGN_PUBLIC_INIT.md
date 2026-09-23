# Public initialisation routes — dsFlower 0.7.0

Token: `FLOWER_PUBLIC_INIT_2026-09-23`. This design is written before implementation,
against the binding audit `thesis-tasks/RESOURCES_ANALYSIS.md` §§1, 3 and 5.
It supersedes the earlier draft in this workspace. Both repositories start at
v0.6.0 and use `feat/public-initialisation-routes`.

## Scope and invariants

Replace `public:<id>` and its registry beside the node secret with two explicitly
admitted routes for the trusted segmentation decoder contract. The ordinary
public-model channel still carries initial arrays. No changes to the DP mechanism,
accountant, clipping, sampling, training algorithms, release cache or identity v2.
Content canonicalisation has its own explicit version. No resource bytes,
credentials or storage locators appear in node status. C1 also removes ordinary
vision-backbone weight downloads; missing verified encoder material fails closed.

## Shared bundle and scientific identity

Resources name a ZIP archive. Analyst local input may be a ZIP or a directory
with the same closed root-level roster. The manifest schema is
`dsflower-public-initialisation-bundle/v1`, derived from the existing segmentation
manifest: model/feature contract, role `segmentation_decoder`, decoder variant and
canonical decoder-spec digest, dataset provenance and licence qualification,
pretraining protocol SHA-256, creation metadata, exact checkpoint file size/SHA,
ordered per-tensor shapes/dtypes/SHA values, and exact frozen encoder artifact
`encoder.pth` with its pinned full SHA-256. All original-manifest, provenance,
protocol, licence, mirror-metadata and audit evidence files are required and bound
by byte size and digest. `checkpoint.npz` uses ordered, little-endian C-order
float32 tensors and safe NPY/NPZ parsing. Encoder loading uses `weights_only=True`
from verified bytes and constructs the trusted architecture with `weights=None`.

Archive validation precedes tensor parsing: bounded archive/member/total sizes,
bounded member count and decompression, exact closed roster, no duplicate names,
directories, traversal, symlinks, special files or trailing unlisted artifacts.
Reject duplicate/nonfinite JSON and malformed manifest structure. No pickle NPZ.
The three BUSI historical manifests and evidence remain byte-for-byte reference
records under `inst/extdata/`; original checkpoint binaries remain absent. An
operational bundle uses a new envelope around those preserved records and the
recovered originals, including the pinned encoder. Never replace the evaluated
weights by retraining or reserialization.

Canonical content identity `dsflower-public-initialisation-identity/v1` binds
contract/decoder, canonical manifest scientific content, provenance/evidence
hashes, checkpoint bytes, tensor order/schema/hashes and frozen encoder. Exclude
resource names, symbols, URLs, credentials, cache paths, ZIP metadata/packaging,
artifact filenames and administrative labels/timestamps. Repacking or registering
an alias cannot change the noise draw. Changed scientific content must change it.
The canonical manifest digest and origin join request selection; `public_arrays`
continues binding the actual incoming arrays. Archive SHA is a transport admission
pin, not scientific request identity.

## Route (a): analyst-declared public material

`decoder_init = "client:<path-to-bundle>"` is interpreted locally by dsFlowerClient.
The client invokes the shared trusted verifier, declares manifest/content digests,
and supplies a bounded public bundle through policy-gated chunked admission before
node preparation, including the
encoder and evidence needed for verification before private staging. This is
explicit public-material ingress under the analyst policy, never resource admission.
Local paths never enter node configuration. The node verifies and snapshots the
bundle in its protected checkpoint cache; no file is placed beside the node secret.
The coordinator initializes the ordinary Flower public arrays from its local copy.
The trusted runner repeats snapshot verification before private access and hashes
and compares the first-round incoming tensors with the admitted checkpoint.
Manifest/release records say `initialisation = "analyst-declared"` and contain the
canonical digests and public provenance. Later rounds use normal federated updates.

## Route (b): custodian-registered resource

The unique strict resolver format is
`dsflower-checkpoint-v1:<64-lowercase-hex-archive-SHA256>`. The pin comes solely from
the registered descriptor; no extra descriptor field is required by Armadillo.
Register `CheckpointResourceResolver` and `CheckpointResourceClient` on namespace
load. Reuse resourcer file transport with bounded acquisition and HTTP-success
checks, immediately at client construction while credentials/tokens are live.
Support file and HTTP(S); S3 requires an explicitly installed file getter/backend
because resourcer 1.5.0 has no built-in S3 getter. Document and fail clearly when
that extension is absent, rather than silently claiming transport support.

Snapshot even local files: download/copy into a service-owned private acquisition
directory, hash against the descriptor, validate everything, then atomically publish
into a digest-addressed protected cache (0700 directories/0600 files), outside
analyst output stores, Hook mounts and the node-secret registry. Retain no resource
credentials. Reject generic table conversion. Failed/expired acquisition requires
native reassignment; there is no network or random-initialisation fallback.

The analyst assigns the authorized resource with `datashield.assign.resource`,
then uses `datashield.assign.expr(..., quote(flowerCheckpointInitDS("CKPT_R")))`
to assign a session-bound opaque typed handle, e.g. `CKPT`. The method accepts
only the assigned checkpoint client; raw descriptors/lists/URLs/paths are refused.
Preparation accepts `decoder_init = "resource:CKPT"`, resolves the immutable handle
in that session and re-verifies its snapshot before private staging. Rebinding the
resource symbol cannot change an existing admission. Handles end with the session;
registry ACL revocation affects new assignment, and server policy is rechecked at
preparation. Explicit cache tampering/removal causes admission/execution failure.

The coordinator obtains its weights independently through researcher-local
`public_checkpoint_file`, checked against every node's admitted public identity.
That file grants no node authority. Status returns only identity, provenance and
tensor geometry. The runner reopens/re-verifies protected artifacts before private
access and checks first-round tensor equality. Manifest/release records say
`initialisation = "resource:<canonical-manifest-digest>"`.

## Custodian policy and encoders

`dsflower.public_initialisation` is `analyst_or_resource` by default, or
`resource_only`, or `none`. An override
`dsflower.public_initialisation.pytorch_resnet18_segmentation` applies to that
contract; honor DataSHIELD `default.dsflower.*` fallback. Reject invalid values
and analyst attempts to set policy/server-owned pins. Report effective policy in
public capabilities/policy/status and run manifests; enforce before private access.
The policy gates named public-initialisation routes, not existing random decoder
semantics. The frozen encoder always requires a verified source.

Ordinary vision ResNet-18/50 and DenseNet-121 constructors use `weights=None` and
load only a full-SHA verified custodian-preseeded Torch-cache file. Resource-admitted
encoder snapshots are implemented for segmentation. Preserve trusted
architecture and feature preprocessing. Do not download weights on a cache miss.
Segmentation public routes use their bundled encoder, never an implicit Torch-cache
fallback; random segmentation may use the existing explicitly preseeded, full-SHA
verified encoder contract.

## Delivery and verification

Implement server R admission/policy, shared Python validator/runner changes, and
client R local verification/transport in parallel after this design. Remove the
installer and registry production authority. Update architecture, READMEs,
reference-bundle instructions, NEWS, generated API documentation and exact recipes
for Opal file/HTTPS resources, Armadillo project upload plus HTTPS descriptor, and
DSLite resource assignment. Bump both packages to 0.7.0.

Tests must cover both routes through DSLite and two short DP segmentation rounds;
policy denials before private access; digest/geometry failures; unassigned/forged
handles and URL/path rejection; archive bounds/traversal; changed/missing snapshot;
no checkpoint-byte/status leakage; canonical identity alias/repack invariance and
changed-content sensitivity; manifest/release provenance; offline vision loading.
Run complete R and Python suites on clean committed checkouts, standalone DP safety
and runner sync; record exact counts, skips and limitations. Keep mirrored runners
byte-identical. Commit ordinary messages without tags/pushes/thesis edits. Write
`PUBLIC_INIT_CONFIRMATION.md` with recipes, implementation summary/hash and results.
Real Opal/Armadillo procedures are documented, not represented as executed tests.
