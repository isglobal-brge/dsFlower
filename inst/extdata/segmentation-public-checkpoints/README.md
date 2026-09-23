# Public BUSI decoder checkpoint bundle

This directory preserves historical manifests, manifest pins and original public
evidence for the three narrow-decoder checkpoints used by the v5 segmentation
evaluation after 60 public BUSI epochs.
The historical checkpoint IDs are `busi-v5-epochs60-seed20260919`,
`busi-v5-epochs60-seed20260920`, and `busi-v5-epochs60-seed20260921`.
Each decoder has 9,521 float32 parameters in six tensors. The checkpoint files
contain decoder weights only. A complete 0.7.0 bundle must also include the
exact frozen ImageNet ResNet-18 encoder as `encoder.pth`.

**Availability:** the three original `checkpoint.npz` files are not included in
this checkout. Their sole confirmed source is the original evaluation host,
which was stopped during packaging on 2026-09-23. No public checkpoint download
URL has been established. The omission is an access limitation, not a package
size limitation: each file is expected to be 39,510 bytes. The fetch-and-verify
procedure below requires access to that archive (or an exact preserved copy).
It has not been completed against the original binaries in this checkout.
Both initialisation routes fail closed without complete verified bundles; these manifests do
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
describe that evaluation; neither the retired 0.6.0 registry nor the new product
routes change that history. `original-manifest.json` preserves the original checkpoint,
tensor, encoder, protocol, dataset, provenance and public-audit hashes.
`audit.json` preserves the original public input census and mask-normalisation
records. The preserved 0.6.0 `manifest.json` binds those evidence bytes and the
checkpoint. Its historical SHA-256 appears in `allowlist.json`.

Public pretraining spent no private budget. A dsFlower run spends its unchanged
DP budget adapting the public decoder. Selecting a checkpoint does not promote
the contract to `vetted = TRUE`, establish new utility, or change the accountant,
clipping, sampler, loss, frozen encoder, or training schedule.

## Build a complete 0.7.0 bundle, then declare or register it

These are historical 0.6.0 reference manifests and evidence, preserved byte-for-byte.
They are not a runtime registry or an admission allowlist in 0.7.0. The old installer
and `public:<id>` selector have been removed. `allowlist.json` preserves historical
manifest pins only; no current node policy reads it.

After recovering each original, copy it byte-for-byte to its reference directory's
`checkpoint.npz`. Supply the original pinned ResNet-18 encoder file as well:
46,830,571 bytes, SHA-256
`f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec`.
From the dsFlower source checkout, use the trusted runtime:

```sh
/path/to/trusted/python tools/build-public-initialisation-bundle.py \
  inst/extdata/segmentation-public-checkpoints/busi-v5-epochs60-seed20260919 \
  /custodian/public/resnet18-f37072fd.pth \
  /custodian/approved/busi_bundle.zip --creator 'Institution model custodian'
```

The helper creates a new versioned envelope while preserving checkpoint, tensor,
encoder and evidence hashes. It reads the original manifest without changing it.
The resulting closed ZIP includes `encoder.pth`, `checkpoint.npz`, a new
`manifest.json` and all six evidence artifacts. The canonical identity is independent
of ZIP packaging, labels and creation timestamps. Bundle construction grants no
server permission.

For institutional admission, register the bundle with format
`dsflower-checkpoint-v1:<archive-SHA256>` through native Opal/Armadillo/DSLite resource
administration. The analyst assigns it, calls `flowerCheckpointInitDS()`, and selects
`decoder_init="resource:CKPT"`. The analyst independently supplies a matching local
`public_checkpoint_file` for the coordinator; nodes never export checkpoint bytes.
For declared research material use `decoder_init="client:<local-bundle>"`, subject
to the custodian's `dsflower.public_initialisation` policy. Exact commands and cache
requirements are in [the public initialisation guide](../../../PUBLIC_INITIALISATION.md).

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

Alternatively, recover those same files from a preserved local archive. Copy each
original to its reference directory's `checkpoint.npz`. Verification requires these
exact SHA-256 values:

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
bundle builder still requires the original checkpoint SHA-256, every tensor
digest and this expected size; it does not accept synthetic arrays. If a
restored artifact contradicts any pin, stop and reconcile the original
evidence instead of changing the recorded pins or bypassing validation.

The six tensor hashes must also match the preserved original manifest. Do not
retrain substitutes or regenerate NPZ containers and present them as the same
checkpoint bytes. Neither training nor campaign tooling is needed to build
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
verifying their original hashes remain outstanding until recovery. The bundle
helper does not modify the reference evidence or add recovered binaries to Git.
