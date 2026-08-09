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
#   docker build --build-arg ROCK_BASE_IMAGE='datashield/rock-base@sha256:<digest>' \
#     -t davidsarrat/dsflower-rock:0.4.1 docker/
#
# After deploying the image as an Opal rock, register its DataSHIELD methods once:
#   opalr::dsadmin.set_package_methods(opal, "dsFlower")
# ---------------------------------------------------------------------------
# No mutable default: the build must name an administrator-reviewed base, and
# production builds should use its sha256 digest.
ARG ROCK_BASE_IMAGE
FROM ${ROCK_BASE_IMAGE}

ARG ROCK_BASE_IMAGE
ARG DSFLOWER_VERSION=0.4.1

LABEL org.opencontainers.image.title="dsflower-rock" \
      org.opencontainers.image.description="DataSHIELD rock node with dsFlower (server-enforced DP federated learning) preinstalled" \
      org.opencontainers.image.source="https://github.com/isglobal-brge/dsFlower" \
      org.opencontainers.image.version="${DSFLOWER_VERSION}" \
      org.opencontainers.image.base.name="${ROCK_BASE_IMAGE}" \
      org.opencontainers.image.licenses="MIT"

COPY dsFlower.tar.gz /tmp/dsFlower.tar.gz

# Install dsFlower (+ any missing R deps), bake the Flower runtime venv, hand everything to
# the rock service user (uid 10041), and DROP the uv wheel cache -- all in ONE layer. Doing
# it in one RUN matters: a separate `chown -R` layer would re-store the whole ~1.7 GB venv
# (doubling it), and the uv cache (~1.6 GB of wheels) is useless at runtime once the venv
# exists. So the first federated run is still instant, but the image stays ~3 GB smaller.
# Torch backend is AUTO-DETECTED (CPU on a CPU build host -> the small portable default),
# never pinned, so a GPU host resolves to GPU and provisions the CUDA venv there.
RUN R -e 'p<-c("arrow","digest","filelock","jsonlite","processx"); m<-p[!vapply(p,requireNamespace,logical(1),quietly=TRUE)]; if(length(m)) install.packages(m, repos="https://cloud.r-project.org")' \
 && R CMD INSTALL --library=/var/lib/rock/R/library /tmp/dsFlower.tar.gz \
 && R -e '.libPaths(c("/var/lib/rock/R/library", .libPaths())); dsFlower:::.ensure_python_env("pytorch")' \
 && chown -R rock:rock /var/lib/rock/R/library/dsFlower /var/lib/dsflower \
 && install -d -o rock -g rock -m 0700 /var/lib/dsflower/privacy \
 && rm -rf /tmp/dsFlower.tar.gz /var/lib/rock/.cache/uv /root/.cache/uv \
 && test ! -e "${DSFLOWER_NODE_SECRET_FILE:-/var/lib/dsflower/privacy/noise_root}" \
 && test ! -L "${DSFLOWER_NODE_SECRET_FILE:-/var/lib/dsflower/privacy/noise_root}" \
 && test ! -e /var/lib/dsflower/privacy/noise_root \
 && test ! -L /var/lib/dsflower/privacy/noise_root \
 && test ! -e /var/lib/dsflower/node_secret \
 && test ! -L /var/lib/dsflower/node_secret \
 && R -e '.libPaths(c("/var/lib/rock/R/library", .libPaths())); p<-c(Sys.getenv("DSFLOWER_NODE_SECRET_FILE",unset=""),dsFlower:::.dsf_option("node_secret_path",""),"/var/lib/dsflower/privacy/noise_root"); p<-as.character(p); p<-p[!is.na(p)&nzchar(p)]; if(any(file.exists(p)|vapply(p,dsFlower:::.path_is_symlink,logical(1)))) stop("noise root was created during image build")'

# Keep the upstream entrypoint/CMD untouched. Its final service hook already runs
# as `rock`, after Rock has prepared its mounts. The wrapper bootstraps early only
# with an explicit node-key path; otherwise the first session uses profile options.
# This code is never run at build.
RUN grep -Eq 'exec[[:space:]]+gosu[[:space:]]+rock[[:space:]]+/opt/obiba/bin/start.sh' /docker-entrypoint.sh \
 && test -x /opt/obiba/bin/start.sh \
 && mv /opt/obiba/bin/start.sh /opt/obiba/bin/start-rock-upstream.sh
COPY --chmod=0755 dsflower-rock-start.sh /opt/obiba/bin/start.sh
