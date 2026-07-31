"""Atomic release guard for the server-owned lifetime privacy reservation."""

import json
import os
import re
import sqlite3


_TOKEN_RE = re.compile(r"^run_[0-9a-f]{32}$")


def _manifest(context):
    manifest_dir = context.node_config.get("manifest-dir")
    if not manifest_dir:
        manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR")
    if not manifest_dir:
        raise RuntimeError("privacy guard has no manifest directory")
    path = os.path.join(manifest_dir, "manifest.json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _message_id(msg):
    metadata = getattr(msg, "metadata", None)
    value = str(getattr(metadata, "message_id", "") or "")
    if not value:
        value = "group:" + str(getattr(metadata, "group_id", "") or "")
    if not value or value == "group:":
        raise RuntimeError("privacy guard requires a Flower message identifier")
    if len(value) > 512 or "\x00" in value:
        raise RuntimeError("invalid Flower message identifier")
    return value


def _same_number(a, b):
    return float(a) == float(b)


def claim_release(context, msg):
    """Reserve one run-local release before any private computation.

    Returns a dict with ``status`` in {``new``, ``replay``, ``noop``}.  Messages
    beyond the server-pinned horizon are not rejected: callers return the incoming
    public arrays unchanged, which is independent of node data.
    """
    manifest = _manifest(context)
    if manifest.get("privacy-reserved") is not True:
        raise RuntimeError("run has no committed privacy reservation")
    token = str(manifest.get("run_token", ""))
    if not _TOKEN_RE.fullmatch(token):
        raise RuntimeError("invalid privacy run token in manifest")
    expected_max = int(manifest.get("privacy-max-releases", 0))
    if expected_max < 1:
        raise RuntimeError("invalid privacy release horizon in manifest")

    db_path = os.environ.get("DSFLOWER_PRIVACY_LEDGER_PATH", "")
    if not db_path or not os.path.isabs(db_path) or os.path.islink(db_path):
        raise RuntimeError("trusted privacy-ledger path is missing or unsafe")
    if not os.path.isfile(db_path):
        raise RuntimeError("privacy ledger does not exist")

    message_id = _message_id(msg)
    con = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    try:
        con.execute("PRAGMA busy_timeout = 10000")
        con.execute("PRAGMA foreign_keys = ON")
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT domain, allocation_index, epsilon, delta, max_releases, "
            "claimed_releases FROM privacy_reservations WHERE run_token = ?",
            (token,),
        ).fetchone()
        if row is None:
            raise RuntimeError("privacy reservation is absent from the ledger")
        domain, allocation_index, epsilon, delta, max_releases, claimed = row
        if str(domain) != str(manifest.get("privacy-domain", "")):
            raise RuntimeError("manifest/ledger privacy domain mismatch")
        if int(allocation_index) != int(manifest.get("privacy-allocation-index", -1)):
            raise RuntimeError("manifest/ledger allocation mismatch")
        if int(max_releases) != expected_max:
            raise RuntimeError("manifest/ledger release horizon mismatch")
        if not _same_number(epsilon, manifest.get("privacy-epsilon", -1)) or not _same_number(
            delta, manifest.get("privacy-delta", -1)
        ):
            raise RuntimeError("manifest/ledger epsilon or delta mismatch")

        prior = con.execute(
            "SELECT release_index FROM privacy_release_claims "
            "WHERE run_token = ? AND message_id = ?",
            (token, message_id),
        ).fetchone()
        if prior is not None:
            con.execute("COMMIT")
            return {
                "status": "replay",
                "run_token": token,
                "allocation_index": int(allocation_index),
                "release_index": int(prior[0]),
                "epsilon": float(epsilon),
                "delta": float(delta),
                "max_releases": int(max_releases),
                "message_id": message_id,
            }

        enabled = bool(manifest.get("privacy-release-enabled", False))
        if int(claimed) >= int(max_releases) or not enabled or epsilon <= 0 or delta <= 0:
            con.execute("COMMIT")
            return {
                "status": "noop",
                "run_token": token,
                "allocation_index": int(allocation_index),
                "release_index": None,
                "epsilon": float(epsilon),
                "delta": float(delta),
                "max_releases": int(max_releases),
                "message_id": message_id,
            }

        release_index = int(claimed) + 1
        changed = con.execute(
            "UPDATE privacy_reservations SET claimed_releases = claimed_releases + 1 "
            "WHERE run_token = ? AND claimed_releases = ? "
            "AND claimed_releases < max_releases",
            (token, int(claimed)),
        ).rowcount
        if changed != 1:
            raise RuntimeError("privacy release claim lost its atomic race")
        con.execute(
            "INSERT INTO privacy_release_claims "
            "(run_token, message_id, release_index, created_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            (token, message_id, release_index),
        )
        con.execute("COMMIT")
        return {
            "status": "new",
            "run_token": token,
            "allocation_index": int(allocation_index),
            "release_index": release_index,
            "epsilon": float(epsilon),
            "delta": float(delta),
            "max_releases": int(max_releases),
            "message_id": message_id,
        }
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        con.close()


def release_id(claim):
    """Domain-separated identity used by deterministic randomness."""
    if claim.get("release_index") is None:
        raise RuntimeError("a no-op has no private release identity")
    return "%s:%d:%d" % (
        claim["run_token"],
        int(claim["allocation_index"]),
        int(claim["release_index"]),
    )
