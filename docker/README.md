# dsFlower demo rock image

A ready-to-run **DataSHIELD rock node with dsFlower preinstalled** — server-enforced
differential privacy for federated learning, with the PyTorch (CPU) Flower runtime baked
in so the first federated run is instant.

Source release image: **`davidsarrat/dsflower-rock:0.4.1`**. Deploy and derive
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
  -t davidsarrat/dsflower-rock:0.4.1 docker/
docker push davidsarrat/dsflower-rock:0.4.1
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

The container image is replaceable; privacy state is runtime state. Production
deployments must preserve these server-owned resources across restarts,
rescheduling and image upgrades:

- the directory containing the SQLite ledger, normally
  `/var/lib/dsflower/privacy/`;
- the private Tier-2 upload spool, normally `/var/lib/dsflower/appstore/`, if
  verified uploads must survive container replacement;
- the runtime-generated 256-bit node secret, normally
  `/var/lib/dsflower/privacy/noise_root`, or a secret-manager path configured
  through `DSFLOWER_NODE_SECRET_FILE`.

Do not mount all of `/var/lib/dsflower` over this image: that would hide the baked
`venvs/` directory. Mount the privacy and appstore subdirectories separately.
The existing Rock entrypoint is preserved. Its service-start hook initializes
the ledger and seed as the `rock` UID after mounts are ready only when both state
paths were explicitly provided as environment variables; otherwise the first
session initializes them after profile options are available. Both Docker builds
fail if either file was created during installation, so no deployment can inherit
a seed baked into an image. If the runtime mount is temporarily unusable, Rock
still starts; private dsFlower calls retry and fail closed until it is repaired.

The mounted privacy directory must be owned by the Rock
process UID and must not be writable by group or other users (`0700` is the
recommended mode); the ledger itself is enforced as `0600`. The secret file must
also be owned by the Rock process UID with exact mode `0600`; its real parent may
be owned by that UID or root, but must not be writable by group or other users.
If a service-owned regular seed is missing, malformed or has unsafe permissions,
dsFlower atomically generates a new one and records a new key epoch without
changing the ledger budget. Unsafe symlinks/ownership remain fail-closed. Ledger
loss or rollback is not recoverable this way and can invalidate lifetime DP, so
the ledger requires durable backup. Do not clone a complete node state to a
concurrent node. dsFlower deliberately declares no Docker `VOLUME`, because the
correct persistent-volume wiring belongs to the Rock/orchestrator deployment.

Set both `DSFLOWER_NODE_SECRET_FILE` and `DSFLOWER_PRIVACY_LEDGER_PATH` to opt in
to pre-service bootstrap. With neither (or only one), the wrapper does not guess:
Opal and Armadillo inject profile R options only after creating a session, so
bootstrap is deferred to `flowerInitDS()`. R-option precedence is preserved for
existing ledger deployments, and a conflicting ledger ENV and R option is
rejected instead of silently selecting different accounting state. The node-key
ENV is authoritative over its R option, so a stale key path cannot block
regeneration.

This runtime contract is connector-neutral, but persistence is an orchestrator
property. A profile manager that removes and recreates Rock without reattaching
the same durable volume cannot preserve lifetime accounting. In that setup,
including currently managed Armadillo Docker profiles without volume support,
run this Rock as an externally managed Compose/Kubernetes service (or add named
volume support to the profile manager) and point the profile at it.

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
