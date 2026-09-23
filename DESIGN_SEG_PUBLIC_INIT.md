# Public decoder initialisation for segmentation

Task: `FLOWER_SEGINIT_2026-09-23`. Design written before implementation.

The registry entry `pytorch_resnet18_segmentation` gains `decoder_init`, with
`"random"` as its backward-compatible default and `"public:<checkpoint-id>"`
as the explicit public initialisation route. IDs are bounded ASCII identifiers;
paths, URLs and uploaded weights are not accepted. Decoder architecture remains
an independent, validated parameter. A checkpoint must match that architecture.
The existing `segmentation-checkpoint-sha256` continues to identify the frozen
ImageNet encoder and `segmentation-selection` continues to identify patient image
selection; neither existing pin is repurposed.

## Custodian registry and admission

Administrators install public files under
`dirname(node_secret_path)/segmentation-public-checkpoints/<checkpoint-id>/`.
The node's protected state supplies the trust root. Registry directories/files
must be regular, owned by the node account or root, protected against group/other
writes, and free of symlink/reparse traversal. No runtime downloading occurs.
The R option `dsflower.segmentation_public_checkpoints` is a named character
vector mapping admitted IDs to the exact SHA-256 of their registry manifests;
its default is empty. Selecting an absent ID fails before private staging.
Changing a manifest therefore requires an explicit custodian policy update.

Each `manifest.json` uses `dsflower-segmentation-public-checkpoint/v1` and records
`checkpoint_id`, `model_id`, `decoder`, `feature_contract`, `encoder_sha256`,
dataset provenance, licence declaration and scope, a checkpoint file/digest/size,
ordered tensor names/shapes/dtypes/digests, and named evidence file/digest/size
records for the original pretraining manifest, protocol, dataset provenance,
audit and licence. Dataset archive digests attest the historical public input;
the archive is not a runtime input. The installed evidence bytes, manifest,
checkpoint and all loaded tensors are verified. Hashing and loading consume
the same bounded bytes. NPZ loading disables pickle and checks its exact tensor
roster, float32 shapes, finiteness and per-tensor raw-byte hashes.

The R node invokes the trusted verifier during public admission, before resolving
private descriptors or reading private metadata. Only the node writes resolved
manifest/checkpoint identities and the provenance block into the run manifest;
analysts cannot supply these fields. The trusted runner re-verifies the protected
registry and compares it with the node-pinned provenance before private access.
Malformed policy, missing files, digest mismatches and incompatible decoders fail
closed. There is no random fallback for an admitted public request.

## Execution and evidence

Wire selection is `segmentation-decoder-init`. Public requests additionally bind
the registry manifest SHA-256, checkpoint SHA-256 and verified provenance into
the node's request selection and neural seed contract. Omitted/default random
selection is normalised away to preserve the existing default semantic contract.
The checkpoint ID and content hashes distinguish public initialisers, even when
their tensor values happen to coincide. Operational paths never select seeds.
The existing execution fingerprint hashes runner source, so installing this
updated runner changes its deterministic streams, including random-init runs.
Backward compatibility means the default request/selection contract and training
mechanism stay unchanged, not that an older runner's exact noise is reproduced.

After admission, the existing node status response carries the verified public
NPZ bytes and provenance (never private data or trained weights). The client
requires every selected node to report the same identity and bytes, then supplies
this bounded public payload to its ServerApp. The ServerApp verifies the tensor
digests and initializes its global model from those bytes before constructing the
strategy. This is required for adaptive/server-momentum strategies whose first
update is relative to the initial global model. It also supports custodian-added
checkpoints without requiring a separate analyst installation. The transport
payload cannot replace any node-owned pin and does not enter seed contracts.

At the first training round the node loads the verified decoder weights in place
of the analyst server's initial arrays. Subsequent rounds retain the validated
incoming federated model, while preserving and checking the original public
checkpoint identity. This makes enablement independent of analyst campaign
hooks. A successful node release records public checkpoint provenance and its
round alongside a digest of the released arrays in the run's protected directory.
The run manifest contains the same provenance block. Release-cache mechanics
remain outside this change.

The encoder remains frozen and hash-pinned. The DP contract, accountant, clipping,
sampler, optimiser and training loop are unchanged: private budget adapts the
public decoder. The model remains `vetted = FALSE`; this route does not establish
new utility or licensing claims.

## BUSI bundle and verification

Package the three v5 epochs60 narrow-decoder checkpoints for seeds 20260919,
20260920 and 20260921, with original evidence preserved and registry manifests.
Each decoder has 9,521 float32 parameters, so the checkpoints are small enough
for an installable bundle. Recover only the exact hash-matching original bytes;
do not retrain substitutes. Preserve the Kaggle mirror's CC0 declaration and its
rights-attribution limitation, original author attribution, dataset version,
pretraining protocol and frozen encoder identity. Document an administrator-only
installation and allowlist procedure, without campaign tooling.

Tests cover allowlist admission, malformed/untrusted inputs, digest and tensor
mismatches before private access, checkpoint-specific semantic seeds, actual
first-round loading/subsequent-round continuation, provenance in manifests and
release records, and backward-compatible random initialisation. Run complete R
and Python package suites from clean committed checkouts and compare both runner
trees byte-for-byte. Keep package versions unchanged; commit locally with no tag,
push or thesis edits. Record exact counts, hashes and remaining limitations in
`../SEGINIT_CONFIRMATION.md`.
