# Public BUSI decoder checkpoint bundle

This directory supplies the registry manifests, policy pins and original public
evidence for the three narrow-decoder checkpoints used by the v5 segmentation
evaluation after 60 public BUSI epochs.
The registry IDs are `busi-v5-epochs60-seed20260919`,
`busi-v5-epochs60-seed20260920`, and `busi-v5-epochs60-seed20260921`.
Each decoder has 9,521 float32 parameters in six tensors. The checkpoint files
contain decoder weights only; the frozen ImageNet ResNet-18 encoder remains the
existing node-installed, hash-pinned checkpoint.

**Availability:** the three original `checkpoint.npz` files are not included in
this checkout. Their sole confirmed source is the original evaluation host,
which was stopped during packaging on 2026-09-23. No public checkpoint download
URL has been established. The omission is an access limitation, not a package
size limitation: each file is expected to be 39,510 bytes. The fetch-and-verify
procedure below requires access to that archive (or an exact preserved copy).
It has not been completed against the original binaries in this checkout.
The registry fails closed until those files are installed; these manifests do
not by themselves provide usable public initialisation.

## Provenance and scope

The original dataset is BUSI, attributed to Al-Dhabyani W, Gomaa M, Khaled H,
Fahmy A, *Dataset of breast ultrasound images*, Data in Brief 2020;28:104863,
<https://doi.org/10.1016/j.dib.2019.104863>. Public pretraining used Arya Shah's
Kaggle mirror, version 1 dated 2021-03-14, with archive SHA-256
`7fffc86a517934da55021f66641d81383058d26a30adb0ffab3780ca7e52d57d`.

The preserved licence is **CC0 1.0 as declared by that mirror**, with the
original qualification that this is not an assertion about original rights.
`provenance.json`, `mirror-metadata.json`, and `licence.txt` preserve the recorded
declaration, attribution and source. No dataset images or masks are included.
BUSI's patient mapping was unavailable: pretraining used all 780 images without
a patient-level privacy or held-out utility claim. Within-BUSI duplicates were
retained; cross-dataset near-duplicate screening was not claimed.

`protocol.md` is the unchanged v5 preregistration, SHA-256
`28a713f8ffe6f7bb7b74e7d59d13d1be265d5738b43503c60e6581a67e0c5820`.
Its historical statements about campaign-only loading and non-redistribution
describe that evaluation; this bundle implements the subsequently requested
node-registry route. `original-manifest.json` preserves the original checkpoint,
tensor, encoder, protocol, dataset, provenance and public-audit hashes.
`audit.json` preserves the original public input census and mask-normalisation
records. The new `manifest.json` binds those evidence bytes and the checkpoint
to one registry ID. Its SHA-256 appears in the bundle's `allowlist.json`.

Public pretraining spent no private budget. A dsFlower run spends its unchanged
DP budget adapting the public decoder. Selecting a checkpoint does not promote
the contract to `vetted = TRUE`, establish new utility, or change the accountant,
clipping, sampler, loss, frozen encoder, or training schedule.

## Administrator installation

Install dsFlower **0.6.0** from the reviewed source. Run the installer as the
node service account, after retrieving all three original NPZ files into
`$HOME/dsflower-public-checkpoints/epochs60` with their original
`seed20260919.npz`, `seed20260920.npz` and `seed20260921.npz` names.
The node's existing protected state, Python runtime and frozen encoder must
already be set up. Use the same `DSFLOWER_NODE_SECRET_FILE` setting as the
running node if it overrides the package default. The registry location is
fixed beside that secret; it is never supplied by an analyst.

From this release checkout, the exact verification and installation command is:

```sh
Rscript --vanilla \
  "$HOME/Documents/GitHub/dsflower-fix/dsFlower/tools/install-segmentation-public-checkpoints.R" \
  "$HOME/dsflower-public-checkpoints/epochs60"
```

Use the corresponding absolute source-checkout path when installing on another
node. The helper requires the installed package version to be 0.6.0 and installs
all three IDs. It verifies the package's manifest pins, original file sizes and
SHA-256 values, evidence files, individual tensors and frozen encoder before
publishing entries in the protected node registry. Existing entries are never
overwritten. It reads public artifacts only; it neither downloads files nor
opens private data.

The helper prints an `options(dsflower.segmentation_public_checkpoints = ...)`
statement after successful installation. Persist only the IDs that this
custodian admits in the node's startup configuration. Installing the files does
not itself enable policy, and shell-process options cannot configure a running
node. No manifest or allowlist pin needs to change when the original bytes are
recovered.

On POSIX the protected state, registry and checkpoint directories must be owned
by root or the node account and must not be writable by group or others. Files
must be regular files with the same ownership/write protections. Links and
reparse points are rejected. Run the helper as the node service account so it
can validate the existing secret and own the installed `0600` files. Apply the
node's existing private-directory ACL policy on Windows. Treat `allowlist.json` as part of the reviewed package;
changing a manifest requires a deliberate update of its custodian pin.

Installation alone does not admit a checkpoint. The policy defaults to an
empty named character vector. Enabling one ID does not enable the other two.
Runtime admission verifies the manifest, every installed evidence file, NPZ,
each tensor and the frozen encoder before private staging. The runner repeats
verification before using private inputs. It never downloads public artifacts
or silently falls back to random initialisation.

## Selecting the route

An analyst selects an admitted checkpoint in the ordinary contract constructor:

```r
model <- dsFlowerClient::ds.flower.model.pytorch_resnet18_segmentation(
  decoder = "narrow",
  decoder_init = "public:busi-v5-epochs60-seed20260919"
)
```

The default remains `decoder_init = "random"`. A public checkpoint must match
the independently selected decoder architecture. Unknown, uninstalled,
unallowlisted or mismatching IDs fail before private access. At round one the
trusted node installs the verified public decoder weights; subsequent rounds
continue from the incoming federated weights under the same pinned identity.
Verified public bytes and provenance are returned through node status so that
the client also initialises its Flower strategy from the same checkpoint;
the participating nodes must agree on that provenance. This hand-off happens
automatically in the ordinary client submission path. The request/seed contract,
run manifest, protected node release record and saved client release metadata
retain the registry ID, manifest and checkpoint hashes and the provenance
block. The trusted node independently verifies its installed copy.

## Recovery of the original archive bytes

The immutable source artifacts are
`/workspace/segmentation/v5/public-pretraining/epochs60/seed20260919.npz`,
`seed20260920.npz`, and `seed20260921.npz` on the original evaluation host.
That host is the custodian's existing RunPod `thesis-dsflower-2`
(`7g8bohjp2o5ufh`); it is currently stopped and its former SSH endpoint must not
be assumed current. An administrator with authorised archive access can use its
current SSH endpoint or configured SSH alias. This procedure never starts a
remote machine or spends compute automatically.

```sh
set -e
umask 077
: "${SEGINIT_ARCHIVE_HOST:?Set the authorised archive SSH user/host or alias}"
: "${SEGINIT_ARCHIVE_PORT:?Set its current SSH port}"
SEGINIT_FETCH_DIR="$HOME/dsflower-public-checkpoints/epochs60"
mkdir -p "$SEGINIT_FETCH_DIR"
for seed in 20260919 20260920 20260921; do
  scp -P "$SEGINIT_ARCHIVE_PORT" \
    "$SEGINIT_ARCHIVE_HOST:/workspace/segmentation/v5/public-pretraining/epochs60/seed${seed}.npz" \
    "$SEGINIT_FETCH_DIR/seed${seed}.npz"
done
```

Alternatively, copy those same files from a preserved local archive and pass
that absolute directory as the installer's sole argument. The helper copies
each verified original to its ID's `checkpoint.npz` and requires these exact
SHA-256 values:

| Seed | Checkpoint SHA-256 |
| --- | --- |
| 20260919 | `b6db994dbb3a922b7b62e60a97a6dac5ce56757015d8e3756b1788e08673d311` |
| 20260920 | `b49b39a40ac00287ba5292f053aafff4c8ef042d09ac9c3a51d88ef2d94e07ed` |
| 20260921 | `54a83fc0751d6a3fba2f461db5a9b1e06ef7b828beed30bc0fc2fc7530452abb` |

The manifests' `size_bytes = 39510` is **source-derived, not a measurement of
the unavailable originals**. The preserved pretraining source uses
`np.savez(path, **{str(i): a for i, a in enumerate(arrays)})`: uncompressed arrays
with numeric keys `0` to `5`. The v5 runtime records NumPy 2.4.6. Serialising
the recorded six float32 shapes with that version gives 38,084 tensor bytes,
768 NPY header bytes, 330 ZIP local-header bytes, 306 central-directory bytes
and 22 end-record bytes: 39,510 bytes irrespective of tensor values. The
installation still requires the original checkpoint SHA-256, every tensor
digest and this expected size; it does not accept synthetic arrays. If a
restored artifact contradicts any pin, stop and reconcile the original
evidence instead of changing the allowlist or bypassing validation.

The six tensor hashes must also match the preserved original manifest. Do not
retrain substitutes or regenerate NPZ containers and present them as the same
checkpoint bytes. Neither training nor campaign tooling is needed to install
or use the bundle.

## Reserved source-bundle locations

The reviewer can add the recovered original bytes to these existing source
bundle directories after successful verification:

```text
inst/extdata/segmentation-public-checkpoints/
  busi-v5-epochs60-seed20260919/checkpoint.npz
  busi-v5-epochs60-seed20260920/checkpoint.npz
  busi-v5-epochs60-seed20260921/checkpoint.npz
```

These are reserved filenames, not placeholder binary files. The manifests,
allowlist and original evidence are already present and must remain unchanged.
Copy each original `seed<seed>.npz` byte-for-byte to its matching
`checkpoint.npz`; do not recompress or reserialise it. Packaging these bytes and
verifying their original hashes remain outstanding until recovery. Installing
the protected node registry with the command above does not modify this source
checkout or add recovered binaries to Git.
