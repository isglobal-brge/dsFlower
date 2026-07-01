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

# Install dsFlower (+ any missing R deps), bake the Flower runtime venv, hand everything to
# the rock service user (uid 10041), and DROP the uv wheel cache -- all in ONE layer. Doing
# it in one RUN matters: a separate `chown -R` layer would re-store the whole ~1.7 GB venv
# (doubling it), and the uv cache (~1.6 GB of wheels) is useless at runtime once the venv
# exists. So the first federated run is still instant, but the image stays ~3 GB smaller.
# Torch backend is AUTO-DETECTED (CPU on a CPU build host -> the small portable default),
# never pinned, so a GPU host resolves to GPU and provisions the CUDA venv there.
RUN R -e 'p<-c("arrow","digest","jsonlite","processx","rappdirs"); m<-p[!vapply(p,requireNamespace,logical(1),quietly=TRUE)]; if(length(m)) install.packages(m, repos="https://cloud.r-project.org")' \
 && R CMD INSTALL --library=/var/lib/rock/R/library /tmp/dsFlower.tar.gz \
 && R -e '.libPaths(c("/var/lib/rock/R/library", .libPaths())); dsFlower:::.ensure_python_env("pytorch")' \
 && chown -R rock:rock /var/lib/rock/R/library/dsFlower /var/lib/dsflower \
 && rm -rf /tmp/dsFlower.tar.gz /var/lib/rock/.cache/uv /root/.cache/uv

# ENTRYPOINT / CMD / exposed port are inherited from datashield/rock-base — do NOT
# override them, or the rock server will not start correctly.
