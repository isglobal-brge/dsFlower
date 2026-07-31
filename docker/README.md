# dsFlower demo rock image

A ready-to-run **DataSHIELD rock node with dsFlower preinstalled** — server-enforced
differential privacy for federated learning, with the PyTorch (CPU) Flower runtime baked
in so the first federated run is instant.

Source release image: **`davidsarrat/dsflower-rock:0.4.0`**. Deploy and derive
other images from an immutable digest, not from a mutable tag.

## What's inside

- an explicitly selected `datashield/rock-base` image (the Dockerfile has no
  default, so an omitted or empty base reference fails the build)
- the `dsFlower` R package installed into `/var/lib/rock/R/library`
- a baked CPU PyTorch/Flower/Opacus venv at `/var/lib/dsflower/venvs/pytorch`

## Build

```bash
# from the package root, produce the source tarball into this directory
cd dsFlower
R CMD build .                       # or: tar --no-xattrs -czf x.tar.gz dsFlower
mv dsFlower_*.tar.gz docker/dsFlower.tar.gz

# build (native linux/amd64 for the federation hosts)
docker build \
  --build-arg ROCK_BASE_IMAGE='datashield/rock-base@sha256:<reviewed-digest>' \
  -t davidsarrat/dsflower-rock:0.4.0 docker/
docker push davidsarrat/dsflower-rock:0.4.0
```

`ROCK_BASE_IMAGE` should be the digest recorded during base-image review. A tag
can still be supplied for local experimentation, but it is mutable and therefore
does not make a reproducible or auditable production build.

## Run as an Opal rock

Point Opal at this image instead of `datashield/rock-base` (via your orchestrator — a
rock profile, docker-compose service, or Coolify service). It exposes the rock API on
`8085` and inherits the base entrypoint, so it is a drop-in replacement.

One-time, register dsFlower's DataSHIELD methods on the Opal it serves (admin):

```r
library(opalr)
o <- opal.login("administrator", Sys.getenv("OPAL_ADMIN_PW"), url = "https://<opal>")
dsadmin.set_package_methods(o, "dsFlower")   # registers flowerFeatureStatsDS, etc.
opal.logout(o)
```

Then researchers use `dsFlowerClient` against the federation — see the
[Method registration + live federation](https://isglobal-brge.github.io/dsFlowerClient/articles/method-registration-live-federation.html)
walkthrough.

## Persistent privacy state

The container image is replaceable; the privacy identity is not. Production
deployments must preserve these two server-owned resources across restarts,
rescheduling and image upgrades:

- the directory containing the SQLite ledger, normally
  `/var/lib/dsflower/privacy/`;
- the private Tier-2 upload spool, normally `/var/lib/dsflower/appstore/`, if
  verified uploads must survive container replacement;
- the same 256-bit node secret, normally `/var/lib/dsflower/node_secret`, or a
  secret-manager path configured through `DSFLOWER_NODE_SECRET_FILE` /
  `dsflower.node_secret_path`.

Do not mount all of `/var/lib/dsflower` over this image: that would hide the baked
`venvs/` directory. Mount the privacy and appstore subdirectories separately and
inject the secret file. The mounted privacy directory must be owned by the Rock
process UID and must not be writable by group or other users (`0700` is the
recommended mode); the ledger itself is enforced as `0600`. The secret file must
also be owned by the Rock process UID with exact mode `0600`; its real parent may
be owned by that UID or root, but must not be writable by group or other users. A
ledger and secret
form one node identity: do not clone
them to a second node, restore only one of them, or roll either one back. Backups
must retain both consistently. dsFlower deliberately declares no Docker `VOLUME`,
because the correct persistent-volume and secret wiring belongs to the
Rock/orchestrator deployment.

## Notes

- **Torch backend is auto-detected, not forced.** The baked venv reflects the *build*
  host: built on a CPU host it bakes the CPU torch build (~1.7 GB; the right small
  default for GPU-less nodes). It is not pinned, so if the image runs on a GPU host the
  runtime resolves to GPU and (re)provisions the CUDA venv there — build on a
  GPU-visible host to bake CUDA directly.
- The image is large (~7 GB on CPU): the rock base is ~5.6 GB and the FL runtime adds
  ~1.7 GB (much larger with a CUDA build).
- Provisioning records exact resolved Python distributions in
  `.dsflower_versions.txt`; server capabilities expose its SHA-256 together with
  the Flower, Torch and Opacus versions. This is evidence of what was installed,
  not a pre-install lock. For reproducible builds, set `DSFLOWER_PYTHON_LOCK` to
  a complete requirements file containing hashes for every transitive artifact;
  dsFlower then invokes `uv pip install --require-hashes -r ...` and binds the
  lock SHA-256 into the venv readiness marker. Enable
  `DSFLOWER_REQUIRE_PYTHON_LOCK=true` to reject an omitted lock. Set
  `DSFLOWER_PYTHON_VERSION` to an exact patch release as well; the default `3.11`
  is a compatibility selector rather than an interpreter lock.
