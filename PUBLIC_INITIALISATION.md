# Public initialisation in 0.7.0

`client:<local-bundle>` supports research iteration with declared public material.
`resource:<handle-symbol>` supports curated institutional checkpoints, large
artifacts and nodes that admit no analyst-supplied material. Both routes use the
same complete, digest-bound bundle and the normal initial public model arrays.
The node verifies before private staging and the trusted runner verifies again
before private access, including equality of the first round's incoming tensors.
The DP mechanism, training algorithm, release cache and identity v2 are unchanged.

## Bundle and migration from 0.6.0

A bundle ZIP contains only root-level regular files: `manifest.json`, the exact
`checkpoint.npz`, `encoder.pth`, and every named original-manifest, provenance,
licence, protocol, mirror-metadata and audit evidence file. The versioned envelope
records dataset/licence attribution and qualification, protocol digest, creation
metadata, decoder/feature contract, checkpoint bytes and every ordered tensor's
shape, dtype and SHA-256. It includes the exact pinned encoder. The validator
rejects unknown files, duplicate names, links, traversal, invalid JSON, unsafe
array headers, archives/decompressed totals above 64 MiB, excessive member sizes and mismatched hashes before tensor admission.

The three [BUSI reference records](inst/extdata/segmentation-public-checkpoints/README.md)
remain unchanged, and their original decoder binaries remain absent. Recover the
originals and copy each byte-for-byte to its reference directory's
`checkpoint.npz`; never retrain or reserialize a replacement. Wrap one recovered
reference with the exact encoder using the trusted node Python environment:

```sh
/path/to/trusted/python tools/build-public-initialisation-bundle.py \
  inst/extdata/segmentation-public-checkpoints/busi-v5-epochs60-seed20260919 \
  /custodian/public/resnet18-f37072fd.pth \
  /custodian/approved/busi_bundle.zip --creator 'Institution model custodian'
```

This offline helper validates and builds a bundle; it grants no node admission.
The old `public:<id>` selector, secret-adjacent registry, allowlist and installer
are retired. A native `file:` resource replaces offline installation. Canonical
identity is versioned separately from identity v2: it binds scientific content
and contract digests, excluding ZIP packaging, resource names, symbols, locations,
credentials and administrative creation labels. Repacking identical contents
therefore does not create a new noise draw.

## Custodian server configuration

Install dsFlower 0.7.0 on each R server profile and its trusted Python runtime.
Persist options in administrator-controlled server startup/profile configuration:

```r
options(dsflower.checkpoint_cache_dir = "/var/lib/dsflower/checkpoints")
options(dsflower.public_initialisation = "analyst_or_resource") # default
# Curated resources only:
options(dsflower.public_initialisation = "resource_only")
# Or refuse both public routes for this contract:
options(dsflower.public_initialisation.pytorch_resnet18_segmentation = "none")
```

Valid values are `analyst_or_resource`, `resource_only`, `none`. Per-contract
options override the global value; DataSHIELD `default.dsflower.*` options are
honored. These govern the explicit public routes; random decoder initialisation
still requires the contract's verified frozen encoder. Policy is visible in
public policy/status and run manifests. Analyst policy overrides are rejected.

Configure a service-owned checkpoint cache outside analyst output directories,
node secret storage and Hook mounts. Cache directories/files are protected
0700/0600; even local resources are verified snapshots. Resource acquisition is
immediate, bounded and digest-checked. It retains no resource credentials.
Missing or changed material fails closed, with no network fallback during training.
Session handles expire with their session; platform ACL revocation controls new
assignment. Change server policy to refuse new preparations using existing handles.
An already prepared run retains its pinned policy and content. Cache removal or
mutation causes later verification to fail, rather than switching initialization.

## Opal custodian: register a file or HTTPS bundle

Projects, approved bundle, profile and permissions must already exist. Run as a
project/resource administrator with administration rights:

```r
o <- opalr::opal.login(
  username = Sys.getenv("OPAL_CUSTODIAN_USER"),
  password = Sys.getenv("OPAL_CUSTODIAN_PASSWORD"),
  url = "https://opal.example"
)
bundle_sha256 <- digest::digest(
  file = "/custodian/approved/busi_bundle.zip", algo = "sha256"
)
opalr::opal.resource_create(
  o, project = "PublicModels", name = "busi",
  url = "https://models.example/busi_bundle.zip",
  format = paste0("dsflower-checkpoint-v1:", bundle_sha256),
  package = "dsFlower"
)
opalr::opal.resource_perm_add(
  o, project = "PublicModels", resource = "busi",
  subject = "segmentation_analysts", type = "group", permission = "view"
)
```

For an offline R node use `url = "file:///custodian/approved/busi_bundle.zip"`.
The expected digest is in the custodian's format string, never an analyst run
parameter. Public HTTPS needs no credentials; a private mirror uses the separate
`identity`/`secret` descriptor arguments. These are
[Opal resource administration APIs](https://github.com/obiba/opalr/blob/3.6.1/R/opal.resource.R).

## Armadillo custodian: storage upload and HTTPS descriptor

On the audited Armadillo v5.17.4 flow, use native project storage. Authorized
storage writes require the deployment's data-manager/superuser role. Upload the
single complete bundle; the resource token is scoped and short-lived:

```sh
curl --fail-with-body \
  -H "Authorization: Bearer ${ARMADILLO_CUSTODIAN_TOKEN}" \
  -F 'object=checkpoints/busi_bundle.zip' \
  -F 'file=@/custodian/approved/busi_bundle.zip' \
  'https://armadillo.example/storage/projects/publicmodels/objects'
```

Then register the descriptor:

```r
MolgenisArmadillo::armadillo.login("https://armadillo.example")
bundle_sha256 <- digest::digest(
  file = "/custodian/approved/busi_bundle.zip", algo = "sha256"
)
res <- resourcer::newResource(
  name = "busi",
  url = paste0(
    "https://armadillo.example/storage/projects/publicmodels/",
    "objects/checkpoints%2Fbusi_bundle.zip"
  ),
  format = paste0("dsflower-checkpoint-v1:", bundle_sha256)
)
MolgenisArmadillo::armadillo.upload_resource(
  project = "publicmodels", folder = "checkpoints", resource = res, name = "busi"
)
```

Keep the expected archive hash in `format`: Armadillo reconstructs descriptors
and does not preserve arbitrary extra fields or external credentials. Assignment
rewrites the object URL to its raw-file endpoint and supplies a resource-scoped
Bearer token. The checkpoint client acquires immediately, then discards credentials;
expired acquisition requires reassignment. These are documented procedures based
on the [native upload API](https://github.com/molgenis/molgenis-r-armadillo/blob/v3.0.0/R/resource.R)
and [audited assignment implementation](https://github.com/molgenis/molgenis-service-armadillo/blob/v5.17.4/armadillo/src/main/java/org/molgenis/armadillo/command/impl/CommandsImpl.java).
Verify them on the deployed platform version; no live Opal/Armadillo test is claimed.

## DSLite custodian fixture

DSLite uses the native resource-assignment API, but both roles share an R process;
it simulates the interface, not adversarial platform isolation.

```r
library(resourcer)
library(dsFlower)
bundle_sha256 <- digest::digest(
  file = "/absolute/fixture/busi_bundle.zip", algo = "sha256"
)
res <- resourcer::newResource(
  name = "busi", url = "file:///absolute/fixture/busi_bundle.zip",
  format = paste0("dsflower-checkpoint-v1:", bundle_sha256)
)
dslite.server <- DSLite::newDSLiteServer(
  resources = list(PublicModels.busi = res),
  config = DSLite::defaultDSConfiguration(include = c("dsBase", "dsFlower")),
  strict = TRUE
)
conns <- list(node = DSI::dsConnect(
  DSLite::DSLite(), name = "node", url = "dslite.server"
))
```

Use a registered `https://` URL in the same descriptor for HTTPS acquisition.
The strict format resolver supports file and HTTP(S) transports through resourcer.
Resourcer 1.5.0 has no built-in S3 getter: `s3://` requires a custodian-installed
registered S3 file getter implementing
`downloadFileBounded(resource, destination, max_bytes, timeout)`. It must stream into
the supplied private destination with the supplied acquisition limits; an absent
or generic unbounded getter fails explicitly. A private HTTPS
endpoint or the Armadillo storage recipe is the built-in alternative.

## Analyst calls

For route (a), no checkpoint resource assignment is required:

```r
model <- dsFlowerClient::ds.flower.model.pytorch_resnet18_segmentation(
  decoder = "narrow", decoder_init = "client:/analyst/public/busi_bundle.zip"
)
result <- dsFlowerClient::ds.flower.fit(
  conns, symbol = "images", target = "mask_path", model = model,
  task = "segmentation", data_kind = "image", rounds = 2L,
  output_dir = "declared-public-model"
)
```

For route (b), after normal DataSHIELD login, assign the registered name and
admit it as a session-bound checkpoint handle. For Opal or the DSLite fixture:

```r
DSI::datashield.assign.resource(
  conns, symbol = "CKPT_R", resource = "PublicModels.busi"
)
```

For Armadillo use its slash namespace, without the stored descriptor's `.rds` suffix:

```r
DSI::datashield.assign.resource(
  conns, symbol = "CKPT_R", resource = "publicmodels/checkpoints/busi"
)
```

Then on either platform:

```r
DSI::datashield.assign.expr(
  conns, symbol = "CKPT", expr = quote(flowerCheckpointInitDS("CKPT_R"))
)
model <- dsFlowerClient::ds.flower.model.pytorch_resnet18_segmentation(
  decoder = "narrow", decoder_init = "resource:CKPT"
)
result <- dsFlowerClient::ds.flower.fit(
  conns, symbol = "images", target = "mask_path", model = model,
  public_checkpoint_file = "/analyst/public/checkpoint.npz",
  task = "segmentation", data_kind = "image", rounds = 2L,
  output_dir = "curated-public-model"
)
```

The example assumes an already admitted image/table symbol and the contract's
required patient, image and mask roles. Different site names can be supplied as
`resource = list(node1 = "PublicModels.busi", node2 = "Models.busi")`.
`CKPT_R` is an assigned typed resource client; `CKPT` is its opaque admission handle.
Raw paths, URLs, lists, manifests and analyst digest pins cannot authorize route
(b). The analyst independently obtains public weights; the local coordinator copy
is checked against the node-admitted identity and grants no node permission.
Status returns identity/provenance/geometry only. Manifests and release records
report `analyst-declared` or `resource:<canonical-manifest-digest>`.

## Ordinary vision encoder provisioning

Runtime model constructors use `weights=None` and safe loading of exact verified
bytes; training does not call torchvision's weight-download path. A custodian may
preseed the trusted runtime's normal Torch checkpoint cache with these official
IMAGENET1K_V1 files before analysis:

| File | Bytes | Full SHA-256 |
| --- | ---: | --- |
| `resnet18-f37072fd.pth` | 46830571 | `f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec` |
| `resnet50-0676ba61.pth` | 102530333 | `0676ba61b6795bbe1773cffd859882e5e297624d384b6993f7c9e683e722fb8a` |
| `densenet121-a639ec97.pth` | 32342954 | `a639ec97d7c33b07ae66f0b5fb7d0192f95a3b11b7576c66c0126c2a727c4395` |

The filenames follow the official [ResNet source](https://docs.pytorch.org/vision/stable/_modules/torchvision/models/resnet.html)
and [DenseNet source](https://docs.pytorch.org/vision/stable/_modules/torchvision/models/densenet.html).
Full hashes were verified during development; a missing, changed or unpinned file
fails closed. Public segmentation bundles always carry their own pinned encoder
snapshot and never fall back to a separately found cache file.
