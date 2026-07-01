# dsFlower demo rock image

A ready-to-run **DataSHIELD rock node with dsFlower preinstalled** — server-enforced
differential privacy for federated learning, with the PyTorch (CPU) Flower runtime baked
in so the first federated run is instant.

Published image: **`davidsarrat/dsflower-rock`** (`:0.2.4`, `:latest`).

## What's inside

- `datashield/rock-base:latest` (the official DataSHIELD rock compute node)
- the `dsFlower` R package installed into `/var/lib/rock/R/library`
- a baked CPU PyTorch/Flower/Opacus venv at `/var/lib/dsflower/venvs/pytorch`

## Build

```bash
# from the package root, produce the source tarball into this directory
cd dsFlower
R CMD build .                       # or: tar --no-xattrs -czf x.tar.gz dsFlower
mv dsFlower_*.tar.gz docker/dsFlower.tar.gz

# build (native linux/amd64 for the federation hosts)
docker build -t davidsarrat/dsflower-rock:0.2.4 -t davidsarrat/dsflower-rock:latest docker/
docker push davidsarrat/dsflower-rock:0.2.4 && docker push davidsarrat/dsflower-rock:latest
```

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

## Notes

- **Torch backend is auto-detected, not forced.** The baked venv reflects the *build*
  host: built on a CPU host it bakes the CPU torch build (~1.7 GB; the right small
  default for GPU-less nodes). It is not pinned, so if the image runs on a GPU host the
  runtime resolves to GPU and (re)provisions the CUDA venv there — build on a
  GPU-visible host to bake CUDA directly.
- The image is large (~7 GB on CPU): the rock base is ~5.6 GB and the FL runtime adds
  ~1.7 GB (much larger with a CUDA build).
