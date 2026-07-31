"""Release-ledger and deterministic-CSPRNG security invariants.

Run with:
    python3 dsFlower/inst/python/tests/test_release_guard.py
"""

import json
import math
import os
import sqlite3
import stat
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock


RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import release_guard
import seeding


class _Context:
    def __init__(self, manifest_dir):
        self.node_config = {"manifest-dir": manifest_dir}


class _Metadata:
    def __init__(self, message_id):
        self.message_id = message_id
        self.group_id = ""


class _Message:
    def __init__(self, message_id):
        self.metadata = _Metadata(message_id)


def _schema(con):
    con.executescript("""
        CREATE TABLE privacy_policy (
          domain TEXT PRIMARY KEY, total_epsilon REAL, total_delta REAL,
          decay REAL, policy_hash TEXT, next_index INTEGER);
        CREATE TABLE privacy_reservations (
          run_token TEXT PRIMARY KEY, domain TEXT, allocation_index INTEGER,
          epsilon REAL, delta REAL, max_releases INTEGER,
          claimed_releases INTEGER DEFAULT 0, created_at TEXT);
        CREATE TABLE privacy_release_claims (
          run_token TEXT, message_id TEXT, release_index INTEGER,
          created_at TEXT, PRIMARY KEY(run_token, message_id),
          UNIQUE(run_token, release_index));
    """)


class ReleaseGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.db = os.path.join(self.root, "ledger.sqlite")
        self.token = "run_" + "a" * 32
        con = sqlite3.connect(self.db)
        _schema(con)
        con.execute(
            "INSERT INTO privacy_policy VALUES (?,?,?,?,?,?)",
            ("node", 3.0, 1e-5, 0.5, "hash", 2),
        )
        con.execute(
            "INSERT INTO privacy_reservations VALUES (?,?,?,?,?,?,?,?)",
            (self.token, "node", 1, 1.5, 5e-6, 2, 0, "now"),
        )
        con.commit()
        con.close()
        self.manifest = {
            "run_token": self.token,
            "privacy-reserved": True,
            "privacy-release-enabled": True,
            "privacy-domain": "node",
            "privacy-allocation-index": 1,
            "privacy-epsilon": 1.5,
            "privacy-delta": 5e-6,
            "privacy-max-releases": 2,
        }
        self._write_manifest()
        self.old_ledger = os.environ.get("DSFLOWER_PRIVACY_LEDGER_PATH")
        os.environ["DSFLOWER_PRIVACY_LEDGER_PATH"] = self.db
        self.context = _Context(self.root)

    def tearDown(self):
        if self.old_ledger is None:
            os.environ.pop("DSFLOWER_PRIVACY_LEDGER_PATH", None)
        else:
            os.environ["DSFLOWER_PRIVACY_LEDGER_PATH"] = self.old_ledger
        self.tmp.cleanup()

    def _write_manifest(self):
        with open(os.path.join(self.root, "manifest.json"), "w", encoding="utf-8") as fh:
            json.dump(self.manifest, fh)

    def test_new_replay_and_nonblocking_horizon(self):
        first = release_guard.claim_release(self.context, _Message("m1"))
        replay = release_guard.claim_release(self.context, _Message("m1"))
        second = release_guard.claim_release(self.context, _Message("m2"))
        excess = release_guard.claim_release(self.context, _Message("m3"))
        self.assertEqual((first["status"], first["release_index"]), ("new", 1))
        self.assertEqual((replay["status"], replay["release_index"]), ("replay", 1))
        self.assertEqual((second["status"], second["release_index"]), ("new", 2))
        self.assertEqual(excess["status"], "noop")
        con = sqlite3.connect(self.db)
        claimed = con.execute(
            "SELECT claimed_releases FROM privacy_reservations WHERE run_token=?",
            (self.token,),
        ).fetchone()[0]
        con.close()
        self.assertEqual(claimed, 2)

    def test_manifest_cannot_override_ledger(self):
        self.manifest["privacy-epsilon"] = 9.0
        self._write_manifest()
        with self.assertRaisesRegex(RuntimeError, "manifest/ledger"):
            release_guard.claim_release(self.context, _Message("m1"))

    def test_even_tiny_manifest_budget_drift_fails_closed(self):
        self.manifest["privacy-epsilon"] = math.nextafter(1.5, math.inf)
        self._write_manifest()
        with self.assertRaisesRegex(RuntimeError, "manifest/ledger"):
            release_guard.claim_release(self.context, _Message("m1"))

    def test_concurrent_claims_never_cross_horizon(self):
        outcomes = []
        lock = threading.Lock()

        def claim(i):
            result = release_guard.claim_release(self.context, _Message("c%d" % i))
            with lock:
                outcomes.append(result["status"])

        threads = [threading.Thread(target=claim, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("new"), 2)
        self.assertEqual(outcomes.count("noop"), 10)


class SeedDerivationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.secret = os.path.join(self.tmp.name, "node_secret")
        with open(self.secret, "w", encoding="ascii") as fh:
            fh.write("42" * 32 + "\n")
        os.chmod(self.secret, stat.S_IRUSR | stat.S_IWUSR)
        self.old_secret = os.environ.get("DSFLOWER_NODE_SECRET_FILE")
        os.environ["DSFLOWER_NODE_SECRET_FILE"] = self.secret

    def _write_secret(self, content):
        with open(self.secret, "wb") as fh:
            fh.write(content)
        os.chmod(self.secret, 0o600)

    def tearDown(self):
        if self.old_secret is None:
            os.environ.pop("DSFLOWER_NODE_SECRET_FILE", None)
        else:
            os.environ["DSFLOWER_NODE_SECRET_FILE"] = self.old_secret
        self.tmp.cleanup()

    def test_key_is_data_independent_and_release_scoped(self):
        cfg = {"loss-name": "mse", "run-token": "ignored"}
        a = seeding.master_seed(cfg, [[1]], [0], "r:1")
        b = seeding.master_seed(cfg, [[999]], [7], "r:1")
        c = seeding.master_seed(cfg, [[1]], [0], "r:2")
        d = seeding.master_seed({"future-private-field": "different"},
                                [[999]], [7], "r:1")
        self.assertEqual(a, b)
        self.assertEqual(a, d)
        self.assertNotEqual(a, c)

    def test_stream_is_reproducible_but_domain_separated(self):
        master = seeding.master_seed({}, None, None, "r:1")
        one = seeding.np_rng(seeding.sub_seed(master, "noise")).normal(size=32)
        replay = seeding.np_rng(seeding.sub_seed(master, "noise")).normal(size=32)
        other = seeding.np_rng(seeding.sub_seed(master, "shuffle")).normal(size=32)
        self.assertTrue((one == replay).all())
        self.assertFalse((one == other).all())

    def test_secret_accepts_only_optional_lf_or_crlf(self):
        expected = bytes.fromhex("42" * 32)
        for terminator in (b"", b"\n", b"\r\n"):
            with self.subTest(terminator=terminator):
                self._write_secret(b"42" * 32 + terminator)
                self.assertEqual(seeding._node_secret(), expected)

    def test_secret_rejects_permissive_whitespace_and_malformed_hex(self):
        invalid = (
            b" 42" * 32,
            b"42" * 32 + b" ",
            b"42" * 32 + b"\r",
            b"42" * 32 + b"\n\n",
            b"42" * 31 + b"4",
            b"42" * 32 + b"0",
            b"gg" * 32,
        )
        for content in invalid:
            with self.subTest(content=content):
                self._write_secret(content)
                with self.assertRaises(RuntimeError):
                    seeding._node_secret()

    def test_secret_requires_exact_mode_regular_file_and_no_symlink(self):
        os.chmod(self.secret, 0o640)
        with self.assertRaisesRegex(RuntimeError, "0600"):
            seeding._node_secret()
        os.chmod(self.secret, 0o600)

        link = os.path.join(self.tmp.name, "secret-link")
        os.symlink(self.secret, link)
        with mock.patch.dict(
                os.environ, {"DSFLOWER_NODE_SECRET_FILE": link}):
            with self.assertRaisesRegex(RuntimeError, "regular file"):
                seeding._node_secret()

        with mock.patch.dict(
                os.environ, {"DSFLOWER_NODE_SECRET_FILE": self.tmp.name}):
            with self.assertRaisesRegex(RuntimeError, "regular file"):
                seeding._node_secret()

    def test_secret_requires_euid_owner_and_same_opened_inode(self):
        info = os.lstat(self.secret)

        wrong_owner = SimpleNamespace(
            st_mode=info.st_mode, st_uid=os.geteuid() + 1,
            st_dev=info.st_dev, st_ino=info.st_ino,
        )
        with mock.patch.object(seeding.os, "fstat", return_value=wrong_owner):
            with self.assertRaisesRegex(RuntimeError, "owned by"):
                seeding._node_secret()

        swapped = SimpleNamespace(
            st_mode=info.st_mode, st_uid=os.geteuid(),
            st_dev=info.st_dev, st_ino=info.st_ino + 1,
        )
        with mock.patch.object(seeding.os, "fstat", return_value=swapped):
            with self.assertRaisesRegex(RuntimeError, "changed while opening"):
                seeding._node_secret()


if __name__ == "__main__":
    unittest.main()
