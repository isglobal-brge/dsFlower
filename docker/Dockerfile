# dsFlower demo rock image
# ---------------------------------------------------------------------------
# A DataSHIELD "rock" compute node with dsFlower preinstalled: server-enforced
# differential privacy for federated learning. Extends the official DataSHIELD
# rock base with the dsFlower R package AND a baked PyTorch (CPU) Flower runtime,
# so a demo node is ready to train with zero manual install and no multi-GB
# first-run provisioning.
#
# Build (from this directory; place the dsFlower source tarball here first):
#   cd dsFlower && R CMD build . && mv dsFlower_*.tar.gz docker/dsFlower.tar.gz
#   docker build -t davidsarrat/dsflower-rock:0.2.4 -t davidsarrat/dsflower-rock:latest docker/
#
# After deploying the image as an Opal rock, register its DataSHIELD methods once:
#   opalr::dsadmin.set_package_methods(opal, "dsFlower")
# ---------------------------------------------------------------------------
FROM datashield/rock-base:latest

LABEL org.opencontainers.image.title="dsflower-rock" \
      org.opencontainers.image.description="DataSHIELD rock node with dsFlower (server-enforced DP federated learning) preinstalled" \
      org.opencontainers.image.source="https://github.com/isglobal-brge/dsFlower" \
      org.opencontainers.image.version="0.2.4" \
      org.opencontainers.image.licenses="MIT"

COPY dsFlower.tar.gz /tmp/dsFlower.tar.gz

# 1) dsFlower + any missing R deps into the rock's R library.
RUN R -e 'p<-c("arrow","digest","jsonlite","processx","rappdirs"); m<-p[!vapply(p,requireNamespace,logical(1),quietly=TRUE)]; if(length(m)) install.packages(m, repos="https://cloud.r-project.org")' \
 && R CMD INSTALL --library=/var/lib/rock/R/library /tmp/dsFlower.tar.gz \
 && rm -f /tmp/dsFlower.tar.gz

# 2) Bake the Flower runtime venv exactly as the node provisions it at run time, so the
#    first federated run is instant instead of a multi-GB download. The torch backend is
#    AUTO-DETECTED, not forced: on a CPU build host this bakes the CPU build (the right,
#    small default for GPU-less nodes); it is NOT pinned, so if the image is deployed on
#    a GPU host the runtime resolves to GPU and (re)provisions the CUDA venv there. Build
#    on a GPU-visible host to bake CUDA directly.
RUN R -e '.libPaths(c("/var/lib/rock/R/library", .libPaths())); dsFlower:::.ensure_python_env("pytorch")'

# 3) Hand the R library + runtime to the rock service user (uid 10041) so the R
#    server (which runs as `rock`) can read/execute them and manage its runtime state.
RUN chown -R rock:rock /var/lib/rock/R/library/dsFlower /var/lib/dsflower

# ENTRYPOINT / CMD / exposed port are inherited from datashield/rock-base — do NOT
# override them, or the rock server will not start correctly.
