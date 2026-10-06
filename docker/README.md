# dsFlower demo rock image

A ready-to-run **DataSHIELD rock node with dsFlower preinstalled** — server-enforced
differential privacy for federated learning, with the PyTorch (CPU) Flower runtime baked
in so the first federated run is instant.

Target tag for this source release: **`davidsarrat/dsflower-rock:0.4.3`**.
Deploy and derive other images from an immutable digest, not from a mutable tag.

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
  -t davidsarrat/dsflower-rock:0.4.3 docker/
docker push davidsarrat/dsflower-rock:0.4.3
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
dsadmin.set_package_methods(o, "dsFlower")   # registers the current assign/aggregate methods
opal.logout(o)
```

Then researchers use `dsFlowerClient` against the federation — see the
[Method registration + live federation](https://isglobal-brge.github.io/dsFlowerClient/articles/method-registration-live-federation.html)
walkthrough.

## Persistent privacy state

The container image is replaceable. Production deployments should preserve the
runtime-generated 256-bit node secret, normally
`/var/lib/dsflower/privacy/noise_root`, or provide a secret-manager path through
`DSFLOWER_NODE_SECRET_FILE`. Version 0.7.2 also requires the permanent
neighbourhood store (default `<node-secret-path>.neighbourhood`), local UUID pin
(`<node-secret-path>.neighbourhood-id`) and initialization lock
(`<node-secret-path>.neighbourhood-id.lock`). Keep these and the key in one
consistent backup, along with permanent store locks; stop workers before restore.
First release automatically initializes absent first-use state. Existing local
markers or an externally configured `dsflower.neighbourhood_store_id` prevent
silent replacement of missing established state. An external UUID pin also
detects complete local state loss; MACs alone do not detect whole-snapshot rollback.
Preserve the private HookApp upload spool at
`/var/lib/dsflower/appstore/` separately if verified uploads must survive
container replacement. Gated Hooks also require their durable release cache,
by default `/var/lib/dsflower/privacy/release-cache`, to survive replacement for
fresh-path exact replay of nondeterministic applications. Neighbourhood anchors
decide first and retain complete releases without eviction. Administrator profile options
`dsflower.release_cache_dir` and `dsflower.release_cache_bytes` select its path
and logical capacity (default 1 GiB); `default.dsflower.*` fallbacks are supported.
Provision additional space for SQLite journals and filesystem allocation overhead.

Do not mount all of `/var/lib/dsflower` over this image: that would hide the baked
`venvs/` directory. Mount the privacy and appstore subdirectories separately.
The existing Rock entrypoint is preserved. Its service-start hook initializes
the secret as the `rock` UID after mounts are ready when its path is explicitly
provided as an environment variable; otherwise the first session initializes it
after profile options are available. Both Docker builds fail if the secret was
created during installation, so no deployment can inherit a key baked into an
image. If the runtime mount is temporarily unusable, Rock still starts; private
dsFlower calls retry and generate or validate the key when the path is usable.

The mounted privacy directory must be owned by the Rock
process UID and must not be writable by group or other users (`0700` is the
recommended mode). The secret file must be owned by the Rock process UID with
exact mode `0600`; its real parent may
be owned by that UID or root, but must not be writable by group or other users.
A missing seed is generated only before neighbourhood state is established.
A missing established seed, malformed existing seed, unsafe permissions, symlinks
or foreign ownership fail closed pending custodian recovery. Restore the original
key and matching state; replacing them creates an additional release domain. Do not clone the same secret
to concurrent nodes. dsFlower deliberately declares no Docker `VOLUME`, because
the correct persistent-volume wiring belongs to the Rock/orchestrator deployment.

Cache directories require exact mode `0700` and cache files exact mode `0600`,
owned by the service UID, with no symlinks or nonregular files. Keep the cache
outside staging and all Hook mounts. After source canonicalization and
neighbourhood lookup, only would-be-fresh anchors reserve the complete public
worst-case Hook run capacity before child execution. Live exact-cache entries
remain pinned until authoritative run cleanup. Crash recovery retains uncertain
pins, so restore the same state and complete cleanup instead of deleting entries.
Automatic cleanup confirms worker shutdown before closing an existing inner
reservation. An unreserved replay-only run then requires no close record or
capacity charge. The administrator's explicit `release_cache.py close` keeps its
unconditional tombstone behavior, including protection against late admission.
The inner Hook cache can evict unpinned entries, while the outer neighbourhood
store permanently retains released payloads and serves exact/near replays even
when the inner cache is full. Neither store provides a timing-DP guarantee.

The neighbourhood defaults are k from the subset filter (otherwise 3, floor 2),
256 anchors per public request and 64 GiB store capacity. Custodians may override
`dsflower.neighbourhood_k`, `neighbourhood_max_anchors`, `neighbourhood_store_bytes`
and `neighbourhood_state_dir`, with `default.dsflower.*` fallbacks. Existing
requests retain frozen k. Caps refuse only would-be-fresh inputs and expose a
weak cross-user aggregate storage signal. Small updates deliberately return stale
answers, and the hard k-1/k boundary remains: this is mitigation of
[dsFlower#7](https://github.com/isglobal-brge/dsFlower/issues/7), not transcript DP.

Set `DSFLOWER_NODE_SECRET_FILE` to opt in to pre-service bootstrap. Without it,
the wrapper does not guess: Opal and Armadillo inject profile R options only
after creating a session, so bootstrap is deferred to `flowerInitDS()`. The
node-key environment variable is authoritative over its R option, so a stale
option cannot select another key path.

This runtime contract is connector-neutral, but persistence is an orchestrator
property. Reattach the key, store, UUID pin and permanent locks across Rock
replacement. Partial state loss fails closed. Losing every local marker can look
like first use unless the UUID is pinned externally. Use externally managed
Compose/Kubernetes volumes or a secret manager with matching persistent release
state; do not regenerate state to retry an analysis.

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
