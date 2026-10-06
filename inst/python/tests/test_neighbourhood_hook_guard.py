"""Near Hook replies retain staged-input integrity without cache admission."""
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np
from flwr.common import ArrayRecord, ConfigRecord, RecordDict

RUNNER = Path(__file__).resolve().parents[2] / "flower_app"
sys.path.insert(0, str(RUNNER))
from dsflower_runner import neighbourhood, release_cache, release_guard


class HookInputIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.staging = self.root / "run"
        self.staging.mkdir(mode=0o700)
        self.manifest = {
            "run_token": "run_" + "a" * 32,
            "privacy-adjacency": "replace_one",
            "privacy-policy-sha256": "1" * 64,
            "privacy-epsilon": 1.5,
            "privacy-delta": 5e-6,
            "num-server-rounds": 2,
            "dp-track": "egress",
        }
        (self.staging / "manifest.json").write_text(json.dumps(self.manifest))
        self.context = self.context_for_restart()

    def tearDown(self):
        self.temp.cleanup()

    def context_for_restart(self):
        return SimpleNamespace(node_config={"manifest-dir": str(self.staging)}, state=RecordDict())

    def claim(self, round_index=1, context=None):
        message = SimpleNamespace(
            metadata=SimpleNamespace(message_id="message-" + str(round_index)),
            content=RecordDict({"config": ConfigRecord({"server-round": round_index}),
                "arrays": ArrayRecord(numpy_ndarrays=[np.zeros(2, dtype=np.float32)])}))
        return release_guard.claim_release(context or self.context, message)

    def records(self):
        fixed = release_guard._fixed_manifest(self.context)
        with sqlite3.connect(fixed["ledger_path"]) as connection:
            return connection.execute("SELECT coordinate, request_id, semantic_key FROM hook_inputs").fetchall()

    def test_first_near_input_replays_without_hook_cache_and_pins_mutation(self):
        store = neighbourhood.NeighbourhoodStore(str(self.root / "anchors"),
                 secret=b"s" * 32, pin_path=str(self.root / "uuid"))
        anchor = SimpleNamespace(records=(b"one", b"two", b"three"))
        with store.release(b"r" * 32, anchor) as slot:
            slot.commit(b"b" * 32, b"anchor-payload")
        claim = self.claim()
        with mock.patch.object(release_cache.ReleaseCache, "from_env",
                               side_effect=AssertionError("near reply attempted cache admission")):
            release_guard.claim_hook_input(self.context, claim, "c" * 64)
            with store.release(b"r" * 32, SimpleNamespace(records=(b"one", b"two"))) as slot:
                self.assertEqual(slot.cached, b"anchor-payload")
        self.assertEqual(self.records(), [(claim["coordinate"], claim["request_id"], "c" * 64)])
        self.assertFalse((self.root / "cache").exists())
        with self.assertRaisesRegex(RuntimeError, "different semantic identity"):
            release_guard.claim_hook_input(self.context, claim, "d" * 64)
        with sqlite3.connect(store.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM anchors").fetchone()[0], 1)

    def test_restarted_hook_coordinate_keeps_exact_input_pin(self):
        claim = self.claim()
        release_guard.claim_hook_input(self.context, claim, "c" * 64)
        restarted = self.context_for_restart()
        repeated = self.claim(context=restarted)
        release_guard.claim_hook_input(restarted, repeated, "c" * 64)
        with self.assertRaisesRegex(RuntimeError, "different semantic identity"):
            release_guard.claim_hook_input(restarted, repeated, "d" * 64)
        self.assertEqual(len(self.records()), 1)

    def test_distinct_admitted_rounds_keep_distinct_pins_and_repeats_do_not_grow(self):
        first, second = self.claim(1), self.claim(2)
        for _ in range(5):
            release_guard.claim_hook_input(self.context, first, "c" * 64)
            release_guard.claim_hook_input(self.context, second, "d" * 64)
        self.assertEqual(len(self.records()), 2)
        self.assertEqual({row[0] for row in self.records()}, {"claim:train:0:1", "claim:train:0:2"})

    def test_unknown_public_claim_and_invalid_key_are_rejected(self):
        claim = self.claim()
        for changes in ({"request_id": "a" * 64}, {"run_fingerprint": "b" * 64},
                        {"coordinate": "claim:train:0:2", "release_index": 2}):
            with self.assertRaises(RuntimeError):
                release_guard.claim_hook_input(self.context, dict(claim, **changes), "c" * 64)
        for invalid in (b"s" * 32, "short", "z" * 64):
            with self.assertRaises(RuntimeError):
                release_guard.claim_hook_input(self.context, claim, invalid)

    def test_missing_or_unsafe_public_ledger_is_not_recreated_or_chmodded(self):
        claim = self.claim()
        path = Path(release_guard._fixed_manifest(self.context)["ledger_path"])
        path.chmod(0o644)
        with self.assertRaises(RuntimeError):
            release_guard.claim_hook_input(self.context, claim, "c" * 64)
        self.assertEqual(path.stat().st_mode & 0o777, 0o644)
        path.unlink()
        with self.assertRaises(RuntimeError):
            release_guard.claim_hook_input(self.context, claim, "c" * 64)
        self.assertFalse(path.exists())

    def test_corrupt_input_rows_cannot_expand_beyond_public_claims(self):
        claim = self.claim()
        release_guard.claim_hook_input(self.context, claim, "c" * 64)
        path = release_guard._fixed_manifest(self.context)["ledger_path"]
        with sqlite3.connect(path) as connection:
            connection.execute("INSERT INTO hook_inputs VALUES (?, ?, ?)",
                               ("claim:train:0:500", "a" * 64, "d" * 64))
        with self.assertRaisesRegex(RuntimeError, "ledger is invalid"):
            release_guard.claim_hook_input(self.context, claim, "c" * 64)

    def test_cache_read_only_open_check_has_no_admission_charge(self):
        cache = release_cache.ReleaseCache(str(self.root / "cache"), 1)
        run = "a" * 64
        for _ in range(5):
            cache.check_run_open(run)
        with cache._transaction() as connection:
            for table in ("runs", "claims", "entries", "pins"):
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)
        # A prior administrative closure remains authoritative at any capacity.
        bigger = release_cache.ReleaseCache(str(self.root / "cache"), 1024 * 1024)
        bigger.close_run(run)
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            cache.check_run_open(run)


if __name__ == "__main__":
    unittest.main()
