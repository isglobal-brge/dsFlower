"""Short tests for permanent anchors, safe state and complete byte replay."""
from collections import Counter
import hashlib
import multiprocessing
import os
import sqlite3
import stat
import struct
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np

RUNNER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "flower_app", "dsflower_runner"))
sys.path.insert(0, RUNNER)
import canonical_units
import neighbourhood as nbr
import seeding

SECRET = b"s" * 32
REQUEST = b"r" * 32


def units(records):
    return SimpleNamespace(records=tuple(str(record).encode() for record in records))


def open_store(root, **kwargs):
    return nbr.NeighbourhoodStore(os.path.join(root, "anchors"), secret=SECRET,
                                 pin_path=os.path.join(root, "uuid"), **kwargs)


def binding(records):
    return hashlib.sha256(repr(sorted(records)).encode()).digest()


def _worker(root, records, start, results):
    try:
        store = open_store(root)
        start.wait(10)
        with store.release(REQUEST, units(records)) as slot:
            fresh = slot.cached is None
            if fresh:
                time.sleep(0.05)
                slot.commit(binding(records), os.urandom(32))
            results.put((fresh, slot.cached))
    except BaseException as exc:
        results.put(("error", repr(exc)))


def _crash_worker(root, commit):
    store = open_store(root)
    with store.release(REQUEST, units(range(10))) as slot:
        if commit:
            slot.commit(binding(range(10)), b"committed-before-egress")
        os._exit(27)


class NeighbourhoodStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = os.path.realpath(self.temp.name)
        self.store = open_store(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def answer(self, records, answer=b"first", request=REQUEST, store=None):
        with (store or self.store).release(request, units(records)) as slot:
            if slot.cached is None:
                slot.commit(binding(records), answer)
            return slot.cached

    def rows(self):
        with sqlite3.connect(self.store.database) as db:
            return db.execute("SELECT COUNT(*) FROM anchors").fetchone()[0]

    def mutate(self, sql, parameters=()):
        with sqlite3.connect(self.store.database) as db:
            db.execute(sql, parameters)

    def test_distance_counts_replacement_once_and_duplicates(self):
        for left, right, expected in [([], [], 0), ([1, 1], [1], 1),
                ([1, 2], [1, 3], 1), ([1, 2, 3], [4, 5], 3),
                ([1, 1, 2], [1, 2, 2], 1), ([1], [1, 2, 3], 2)]:
            self.assertEqual(nbr.distance(Counter(left), Counter(right)), expected)
            self.assertEqual(nbr.distance(Counter(right), Counter(left)), expected)
        with self.assertRaises(ValueError):
            nbr.distance({"a": 0}, {})

    def test_fingerprints_are_separate_private_hmac_with_multiplicity(self):
        source = units(["private", "private", "other"])
        actual = nbr.unit_fingerprints(source, self.store._unit_key)
        token = __import__("hmac").new(self.store._unit_key,
                  nbr._frame(b"canonical-record-v1", b"private"), hashlib.sha256).hexdigest()
        self.assertEqual(actual[token], 2)
        self.assertNotIn(hashlib.sha256(b"private").hexdigest(), actual)
        self.assertNotEqual(actual, nbr.unit_fingerprints(source, b"x" * 32))

    def test_canonical_row_reorder_and_signed_zero_have_same_units(self):
        a = canonical_units.canonicalize_units([[1.0], [-0.0], [1.0]], secret=SECRET)
        b = canonical_units.canonicalize_units([[1.0], [1.0], [0.0]], secret=SECRET)
        self.assertEqual(nbr.unit_fingerprints(a, self.store._unit_key),
                         nbr.unit_fingerprints(b, self.store._unit_key))
        self.assertEqual(sum(nbr.unit_fingerprints(a, self.store._unit_key).values()), 3)

    def test_whole_patient_changes_many_members_as_one_unit(self):
        a = canonical_units.canonicalize_units([[1], [2], [3], [4]],
                    unit_ids=["a", " a ", "a", "b"], secret=SECRET)
        b = canonical_units.canonicalize_units([[11], [12], [13], [4]],
                    unit_ids=["a", "a", "a", "b"], secret=SECRET)
        self.assertEqual(len(a.records), 2)
        self.assertEqual(len(b.records), 2)
        self.assertEqual(nbr.distance(nbr.unit_fingerprints(a, self.store._unit_key),
                                     nbr.unit_fingerprints(b, self.store._unit_key)), 1)

    def test_oldest_eligible_stays_stable_when_new_closer_anchor_added(self):
        a, q, b = list(range(10)), list(range(8)), list(range(7))
        self.assertEqual(self.answer(a, b"A"), b"A")
        self.assertEqual(self.answer(q, b"Q-must-never-run"), b"A")
        self.assertEqual(self.answer(b, b"B"), b"B")
        self.assertEqual(self.answer(q, b"Q-must-still-not-run"), b"A")
        self.assertEqual(self.rows(), 2)
        self.assertEqual(self.answer(q, store=open_store(self.root)), b"A")

    def test_near_inputs_never_anchor_and_boundary_is_explicit(self):
        self.answer(range(12), b"A")
        self.assertEqual(self.answer(range(11), b"unused"), b"A")
        self.assertEqual(self.answer(range(10), b"unused"), b"A")
        self.assertEqual(self.rows(), 1)
        self.assertEqual(self.answer(range(9), b"B"), b"B")
        self.assertEqual(self.answer(range(8), b"unused"), b"B")
        self.assertEqual(self.answer(range(7), b"unused"), b"B")
        self.assertEqual(self.answer(range(6), b"C"), b"C")
        self.assertEqual(self.rows(), 3)

    def test_every_anchor_scanned_even_after_exact_or_near_first_hit(self):
        self.answer(range(10), b"A")
        self.answer(range(7), b"B")
        self.answer(range(4), b"C")
        for records in (range(10), range(9)):
            with mock.patch.object(nbr, "distance", wraps=nbr.distance) as measured:
                self.assertEqual(self.answer(records), b"A")
                self.assertEqual(measured.call_count, 3)

    def test_fresh_requires_distance_k_from_all_anchors(self):
        self.answer(range(10), b"A")
        self.answer(range(7), b"B")
        self.assertEqual(self.answer(range(5), b"cannot-create-C"), b"B")
        self.assertEqual(self.rows(), 2)

    def test_request_domains_and_round_coordinates_do_not_coalesce(self):
        self.answer(range(10), b"round-one", request=b"1" * 32)
        self.assertEqual(self.answer(range(9), b"round-two", request=b"2" * 32), b"round-two")
        self.assertEqual(self.rows(), 2)

    def test_k_is_frozen_for_used_request_only(self):
        self.answer(range(10), b"A")
        changed = open_store(self.root, k=8)
        self.assertEqual(self.answer(range(7), b"B", store=changed), b"B")
        self.answer(range(10), b"new-R", request=b"z" * 32, store=changed)
        self.assertEqual(self.answer(range(4), b"unused", request=b"z" * 32, store=changed), b"new-R")

    def test_floor_two(self):
        minimal = open_store(self.root, k=0)
        self.answer(range(10), b"A", store=minimal)
        self.assertEqual(self.answer(range(9), b"unused", store=minimal), b"A")
        self.assertEqual(self.answer(range(8), b"B", store=minimal), b"B")

    def test_max_anchor_capacity_never_charges_exact_or_near_replay(self):
        limited = open_store(self.root, max_anchors=1)
        self.answer(range(10), b"A", store=limited)
        for _ in range(10):
            self.assertEqual(self.answer(range(10), store=limited), b"A")
            self.assertEqual(self.answer(range(8), store=limited), b"A")
        with self.assertRaisesRegex(nbr.CapacityError, nbr.CAPACITY_ERROR):
            self.answer(range(7), b"new", store=limited)
        self.assertEqual(self.rows(), 1)

    def test_byte_cap_refuses_only_fresh_even_after_limit_lowered(self):
        self.answer(range(10), b"A")
        limited = open_store(self.root, capacity_bytes=0)
        for records in (range(10), range(8)):
            self.assertEqual(self.answer(records, store=limited), b"A")
        with self.assertRaises(nbr.CapacityError):
            self.answer(range(7), store=limited)
        self.assertEqual(self.rows(), 1)

    def test_payload_over_cap_not_published_and_capacity_can_expand(self):
        limited = open_store(self.root, capacity_bytes=nbr._BASE_BYTES + 32)
        with self.assertRaises(nbr.CapacityError):
            self.answer(range(10), store=limited)
        self.assertEqual(self.rows(), 0)
        self.assertEqual(self.answer(range(10), b"after-increase"), b"after-increase")

    def test_sqlite_page_allocation_is_included_in_store_byte_cap(self):
        limited = open_store(self.root, capacity_bytes=nbr._BASE_BYTES + 10000)
        with self.assertRaises(nbr.CapacityError):
            self.answer([1], b"small", store=limited)
        self.assertEqual(self.rows(), 0)
        self.assertEqual(self.answer([1], b"fits-after-increase"), b"fits-after-increase")

    def test_payload_roundtrips_complete_exact_bits_and_types_after_restart(self):
        payload = ([np.array([[1.0, -0.0]], dtype=">f4"), np.arange(3, dtype=np.uint64),
                    np.array(np.nan)], 7, {"float": -0.0, "nan": float("nan"), "none": None,
                    "bytes": b"artifact\x00", "unicode": "α", "bool": True, "metrics": (1, 0.4)})
        original = nbr.encode_payload(payload)
        released = self.answer(range(10), payload)
        replay = self.answer(range(9), store=open_store(self.root))
        self.assertEqual(nbr.encode_payload(released), original)
        self.assertEqual(nbr.encode_payload(replay), original)
        self.assertEqual(struct.pack(">d", replay[2]["float"]), struct.pack(">d", -0.0))
        self.assertEqual(replay[0][0].dtype, payload[0][0].dtype)
        self.assertIsInstance(replay, tuple)
        self.assertIsInstance(replay[0], list)

    def test_payload_rejects_executable_objects_and_dtype(self):
        for payload in (object(), np.array([object()], dtype=object), {1: "nonstring"}):
            with self.assertRaises(ValueError):
                nbr.encode_payload(payload)

    def test_commit_requires_same_request_binding_and_active_slot(self):
        with self.store.release(REQUEST, units(range(10))) as slot:
            wrong = seeding.DataBinding(b"{}", b"b" * 32, b"x" * 32)
            with self.assertRaises(ValueError):
                slot.commit(wrong, b"answer")
            slot.commit(seeding.DataBinding(b"{}", b"b" * 32, REQUEST), b"answer")
            with self.assertRaises(RuntimeError):
                slot.commit(b"b" * 32, b"again")
        with self.assertRaises(RuntimeError):
            slot.commit(b"b" * 32, b"late")

    def test_concurrent_near_inputs_publish_one_complete_answer(self):
        ctx = multiprocessing.get_context("spawn")
        start, results = ctx.Event(), ctx.Queue()
        workers = [ctx.Process(target=_worker, args=(self.root, list(range(n)), start, results))
                   for n in (10, 9, 10, 9)]
        for process in workers:
            process.start()
        start.set()
        try:
            answers = [results.get(timeout=25) for _ in workers]
        finally:
            for process in workers:
                process.join(timeout=25)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
        self.assertEqual([p.exitcode for p in workers], [0] * len(workers))
        self.assertFalse(any(a[0] == "error" for a in answers), answers)
        self.assertEqual(sum(a[0] for a in answers), 1)
        self.assertEqual(len({a[1] for a in answers}), 1)
        self.assertEqual(self.rows(), 1)

    def test_independent_request_computation_holds_no_global_transaction(self):
        # Pick a distinct lock shard to avoid conservative shard collisions.
        request = b"o" * 32
        import hmac
        shard = lambda r: int(hmac.new(self.store._index_key, nbr._frame(b"request", r), hashlib.sha256).hexdigest()[:8], 16) % nbr._LOCK_SHARDS
        while shard(request) == shard(REQUEST):
            request = hashlib.sha256(request).digest()
        done, errors = threading.Event(), []
        def other():
            try:
                self.answer(range(10), b"other", request=request)
                done.set()
            except BaseException as exc:
                errors.append(exc)
        with self.store.release(REQUEST, units(range(10))) as slot:
            thread = threading.Thread(target=other, daemon=True)
            thread.start()
            self.assertTrue(done.wait(5), errors)
            slot.commit(binding(range(10)), b"first")
        thread.join(5)
        self.assertFalse(errors)

    def test_crash_before_commit_has_no_anchor_after_commit_replays(self):
        ctx = multiprocessing.get_context("spawn")
        for committed in (False, True):
            child = ctx.Process(target=_crash_worker, args=(self.root, committed))
            child.start()
            child.join(15)
            if child.is_alive():
                child.terminate()
                child.join(5)
            self.assertEqual(child.exitcode, 27)
            self.assertEqual(self.rows(), int(committed))
        self.assertEqual(self.answer(range(9), store=open_store(self.root)), b"committed-before-egress")

    def test_payload_mac_is_verified_before_decoder(self):
        self.answer(range(10))
        self.mutate("UPDATE anchors SET payload=?", (b"not-safe-to-decode",))
        with mock.patch.object(nbr, "decode_payload", side_effect=AssertionError("decoded before MAC")):
            with self.assertRaises(nbr.StateError):
                self.answer(range(10))

    def test_every_anchor_corruption_is_detected_even_when_first_is_eligible(self):
        self.answer(range(10), b"A")
        self.answer(range(7), b"B")
        self.mutate("UPDATE anchors SET units=? WHERE sequence=2", (b"{}",))
        with self.assertRaises(nbr.StateError):
            self.answer(range(10))

    def test_transport_verify_authenticates_all_anchors_without_decoding(self):
        self.answer(range(10), b"A", request=b"a" * 32)
        self.answer(range(10), b"B", request=b"b" * 32)
        with mock.patch.object(nbr, "decode_payload", side_effect=AssertionError("transport decoded payload")):
            self.store.verify()
            self.mutate("UPDATE anchors SET payload=? WHERE payload != ?", (b"corrupt", nbr.encode_payload(b"A")))
            with self.assertRaises(nbr.StateError):
                self.store.verify()

    def test_deleted_anchor_is_detected_by_authenticated_request_head(self):
        self.answer(range(10), b"A")
        self.answer(range(7), b"B")
        self.mutate("DELETE FROM anchors WHERE sequence=2")
        with self.assertRaises(nbr.StateError):
            self.answer(range(10))

    def test_deleted_request_and_anchors_detected_by_global_manifest(self):
        self.answer(range(10))
        self.mutate("DELETE FROM anchors")
        self.mutate("DELETE FROM requests")
        with self.assertRaises(nbr.StateError):
            open_store(self.root)

    def test_replaced_valid_anchor_mac_is_bound_to_request_and_sequence(self):
        self.answer(range(10), b"A")
        self.answer(range(7), b"B")
        self.mutate("UPDATE anchors SET binding=(SELECT binding FROM anchors WHERE sequence=1), units=(SELECT units FROM anchors WHERE sequence=1), payload=(SELECT payload FROM anchors WHERE sequence=1), mac=(SELECT mac FROM anchors WHERE sequence=1) WHERE sequence=2")
        with self.assertRaises(nbr.StateError):
            self.answer(range(7))

    def test_missing_established_database_never_recreated(self):
        self.answer(range(10))
        os.unlink(self.store.database)
        for operation in (lambda: open_store(self.root), lambda: self.answer(range(10))):
            with self.assertRaises(nbr.StateError):
                operation()
        self.assertFalse(os.path.exists(self.store.database))

    def test_retained_initialization_lock_prevents_recreation_after_state_loss(self):
        import shutil
        self.answer(range(10))
        shutil.rmtree(self.store.directory)
        os.unlink(self.store.pin_path)
        self.assertTrue(os.path.exists(self.store.init_lock))
        with self.assertRaises(nbr.StateError):
            open_store(self.root)
        self.assertFalse(os.path.exists(self.store.directory))
        self.assertFalse(os.path.exists(self.store.pin_path))

    def test_missing_established_pin_never_recreated(self):
        self.answer(range(10))
        os.unlink(self.store.pin_path)
        with self.assertRaises(nbr.StateError):
            open_store(self.root)
        self.assertFalse(os.path.exists(self.store.pin_path))

    def test_missing_established_lock_never_recreated(self):
        lock = self.store._lock_path(0)
        os.unlink(lock)
        with self.assertRaises(nbr.StateError):
            open_store(self.root)
        self.assertFalse(os.path.exists(lock))

    def test_uuid_external_pin_and_key_mismatch_fail_closed(self):
        self.answer(range(10))
        matched = open_store(self.root, expected_uuid=self.store.store_uuid)
        self.assertEqual(self.answer(range(9), store=matched), b"first")
        with self.assertRaises(nbr.StateError):
            open_store(self.root, expected_uuid="11111111-1111-1111-1111-111111111111")
        with self.assertRaises(nbr.StateError):
            nbr.NeighbourhoodStore(self.store.directory, secret=b"z" * 32, pin_path=self.store.pin_path)
        with tempfile.TemporaryDirectory(dir=self.root) as new:
            with self.assertRaises(nbr.StateError):
                open_store(new, expected_uuid=self.store.store_uuid)
            self.assertFalse(os.path.exists(os.path.join(new, "anchors")))

    def test_files_owner_only_no_symlinks_hardlinks_or_extra_files(self):
        self.answer(range(10))
        self.assertEqual(stat.S_IMODE(os.stat(self.store.directory).st_mode), 0o700)
        for name in os.listdir(self.store.directory):
            self.assertEqual(stat.S_IMODE(os.stat(os.path.join(self.store.directory, name)).st_mode), 0o600)
        os.chmod(self.store.database, 0o640)
        with self.assertRaises(nbr.StateError):
            open_store(self.root)
        os.chmod(self.store.database, 0o600)
        linked = os.path.join(self.root, "db-link")
        os.link(self.store.database, linked)
        with self.assertRaises(nbr.StateError):
            open_store(self.root)
        os.unlink(linked)
        os.symlink(self.store.database, os.path.join(self.store.directory, "extra"))
        with self.assertRaises(nbr.StateError):
            open_store(self.root)

    def test_from_env_defaults_are_specific_to_node_key_file(self):
        paths = [os.path.join(self.root, "node-one"), os.path.join(self.root, "node-two")]
        for path in paths:
            with open(path, "w") as stream:
                stream.write(SECRET.hex())
            os.chmod(path, 0o600)
        stores = []
        clean = {key: value for key, value in os.environ.items() if not key.startswith("DSFLOWER_NEIGHBOURHOOD_")}
        for path in paths:
            with mock.patch.dict(os.environ, dict(clean, DSFLOWER_NODE_SECRET_FILE=path), clear=True):
                stores.append(nbr.NeighbourhoodStore.from_env())
        self.assertNotEqual(stores[0].store_uuid, stores[1].store_uuid)
        self.assertEqual(stores[0].directory, paths[0] + ".neighbourhood")
        self.assertEqual(stores[1].pin_path, paths[1] + ".neighbourhood-id")


if __name__ == "__main__":
    unittest.main()
