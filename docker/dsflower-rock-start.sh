#!/usr/bin/env bash
set -u

# /docker-entrypoint.sh invokes this hook through gosu, so an explicitly
# configured bootstrap runs as the Rock service UID after runtime mounts are
# ready. Profile R options do not exist until Opal/Armadillo creates a session;
# without an explicit process-level key path, defer safely to flowerInitDS.
secret_path="${DSFLOWER_NODE_SECRET_FILE:-}"

if [[ -n "${secret_path}" ]]; then
  if ! /usr/bin/Rscript --vanilla -e '
.libPaths(c(Sys.getenv("ROCK_LIB", "/var/lib/rock/R/library"), .libPaths()))
getFromNamespace(".privacy_runtime_bootstrap", "dsFlower")()
'; then
    echo "[dsFlower] WARNING: privacy bootstrap failed; private operations will retry it in-session." >&2
  fi
else
  echo "[dsFlower] Privacy bootstrap deferred to the first session so DataSHIELD profile options remain authoritative."
fi

exec /opt/obiba/bin/start-rock-upstream.sh "$@"
