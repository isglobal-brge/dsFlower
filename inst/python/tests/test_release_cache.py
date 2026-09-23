"""Durable gated cache: exact releases, concurrency, reservations and custody."""

import multiprocessing
import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import numpy as np

RUNNER = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "..", "..", "flower_app", "dsflower_runner"))
sys.path.insert(0, RUNNER)
import release_cache
import seeding


METRICS = {"num-examples": 1, "hook-executed": 1}
RUN = "a" * 64
REQUEST = "b" * 64
KEY = "c" * 64
COORD = "claim:train:0:1"


def _concurrent_release(directory, start, ready, results):
    ready.put(True)
    start.wait()
    try:
        cache = release_cache.ReleaseCache(directory, 1024 * 1024)
        with cache.release(RUN, COORD, REQUEST, KEY) as slot:
            computed = slot.cached is None
            if computed:
                # Deliberately independent of all seeded RNGs. The sleep keeps
                # the exclusion window open while the second process arrives.
                arrays = [np.frombuffer(os.urandom(64), dtype=np.uint32).copy()]
                time.sleep(0.1)
                slot.commit(arrays, METRICS)
            arrays, metrics = slot.cached
            results.put((computed, arrays[0].tobytes(), metrics))
    except BaseException as exc:
        results.put(("error", repr(exc)))


def _crash_release(directory, run, key, commit):
    cache = release_cache.ReleaseCache(directory, 1024 * 1024)
    with cache.release(run, COORD, REQUEST, key) as slot:
        if commit:
            slot.commit([np.asarray([123.5], dtype=np.float32)], METRICS)
        os._exit(29)


class ReleaseCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = os.path.realpath(self.temp.name)
        self.directory = os.path.join(self.root, "cache")
        self.cache = release_cache.ReleaseCache(self.directory, 1024 * 1024)

    def tearDown(self):
        self.temp.cleanup()

    def _reserve(self, run=RUN, rounds=1, entry_bytes=4096):
        self.cache.reserve_run(run, rounds, entry_bytes)

    def _commit(self, run=RUN, key=KEY, arrays=None, coordinate=COORD):
        if arrays is None:
            arrays = [np.asarray([3.5, -0.0], dtype=np.float32)]
        with self.cache.release(run, coordinate, REQUEST, key) as slot:
            self.assertIsNone(slot.cached)
            slot.commit(arrays, METRICS)
        return arrays

    def test_key_is_the_domain_separated_v2_master_subkey(self):
        master = bytes(range(32))
        self.assertEqual(release_cache.cache_key(master),
                         seeding.sub_seed(master, "gated-release-cache-key/v1").hex())
        self.assertNotEqual(release_cache.cache_key(master), master.hex())
        self.assertNotEqual(release_cache.cache_key(master),
                            seeding.sub_seed(master, "egress").hex())

    def test_exact_dtype_shape_bytes_metrics_survive_reconstruction(self):
        self._reserve(rounds=2)
        arrays = [np.asarray([[1.25, -0.0]], dtype=">f4"),
                  np.arange(4, dtype=np.int64), np.asarray(7.25, dtype=np.float64)]
        self._commit(arrays=arrays)
        self._commit(key="d" * 64, coordinate="claim:train:0:2")
        restarted = release_cache.ReleaseCache(self.directory, 1024 * 1024)
        with restarted.release(RUN, COORD, REQUEST, KEY) as slot:
            replay, metrics = slot.cached
            self.assertEqual(metrics, METRICS)
            for original, restored in zip(arrays, replay):
                self.assertEqual(original.dtype, restored.dtype)
                self.assertEqual(original.shape, restored.shape)
                self.assertEqual(original.tobytes(), restored.tobytes())
            with self.assertRaisesRegex(RuntimeError, "committed twice"):
                slot.commit(arrays, METRICS)

    def test_released_zero_outcome_is_persisted_as_any_other_release(self):
        self._reserve()
        arrays = self._commit(arrays=[np.zeros((2, 2), dtype=np.float32)])
        with self.cache.release(RUN, COORD, REQUEST, KEY) as slot:
            self.assertEqual(slot.cached[0][0].tobytes(), arrays[0].tobytes())
            self.assertEqual(slot.cached[1], METRICS)

    def test_changed_semantics_miss_in_new_run_but_cannot_reroll_coordinate(self):
        self._reserve()
        self._commit()
        with self.assertRaisesRegex(RuntimeError, "different semantic identity"):
            with self.cache.release(RUN, COORD, REQUEST, "d" * 64):
                self.fail("changed semantics reused a coordinate")
        self._reserve(run="e" * 64)
        with self.cache.release("e" * 64, COORD, REQUEST, "d" * 64) as slot:
            self.assertIsNone(slot.cached)
            slot.commit([np.ones(2, dtype=np.float32)], METRICS)

    def test_public_payload_cannot_change_a_bound_coordinate(self):
        self._reserve()
        self._commit()
        with self.assertRaisesRegex(RuntimeError, "different semantic identity"):
            with self.cache.release(RUN, COORD, "d" * 64, KEY):
                self.fail("changed public payload reused a coordinate")

    def test_interrupted_intent_retains_reservation_and_fails_closed(self):
        self._reserve()
        with self.cache.release(RUN, COORD, REQUEST, KEY) as slot:
            self.assertIsNone(slot.cached)
        restarted = release_cache.ReleaseCache(self.directory, 1024 * 1024)
        with self.assertRaisesRegex(RuntimeError, "no durable exact reply"):
            with restarted.release(RUN, COORD, REQUEST, KEY):
                self.fail("interrupted private work was executed again")
        with restarted._transaction() as connection:
            self.assertEqual(connection.execute("SELECT run, entry_key FROM pins").fetchall(),
                             [(RUN, KEY)])

    def test_concurrent_processes_choose_one_nondeterministic_release(self):
        self._reserve()
        context = multiprocessing.get_context("spawn")
        start, ready, results = context.Event(), context.Queue(), context.Queue()
        workers = [context.Process(target=_concurrent_release,
                                   args=(self.directory, start, ready, results))
                   for _ in range(4)]
        for worker in workers:
            worker.start()
        try:
            for _ in workers:
                self.assertTrue(ready.get(timeout=20))
            start.set()
            outcomes = [results.get(timeout=20) for _ in workers]
        finally:
            start.set()
            for worker in workers:
                worker.join(timeout=20)
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
        self.assertEqual([worker.exitcode for worker in workers], [0] * 4)
        self.assertFalse(any(result[0] == "error" for result in outcomes), outcomes)
        self.assertEqual(sum(result[0] for result in outcomes), 1)
        self.assertTrue(all(result[1:] == outcomes[0][1:] for result in outcomes))

    def test_admission_reserves_all_rounds_before_a_release(self):
        with self.assertRaisesRegex(RuntimeError, "capacity.*before private work"):
            self.cache.reserve_run(RUN, 1)
        with self.cache._transaction() as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 0)
        with self.assertRaisesRegex(RuntimeError, "unreserved"):
            with self.cache.release(RUN, COORD, REQUEST, KEY):
                self.fail("unreserved private release")
        self._reserve(rounds=2)
        self._reserve(rounds=2)
        with self.assertRaisesRegex(RuntimeError, "reservation changed"):
            self._reserve(rounds=1)

    def test_active_pins_survive_restart_and_admission_evicts_only_oldest_unpinned(self):
        self._reserve(run="1" * 64)
        self._commit(run="1" * 64, key="1" * 64)
        self.cache.close_run("1" * 64)
        self._reserve(run="2" * 64)
        self._commit(run="2" * 64, key="2" * 64)
        self.cache.close_run("2" * 64)
        self._reserve()
        self._commit()
        with self.cache._transaction() as connection:
            usage = self.cache._usage(connection)
            oldest_bytes = connection.execute(
                "SELECT length(payload) FROM entries WHERE entry_key=?", ("1" * 64,)
            ).fetchone()[0]
        # Leave precisely enough space after deleting just the oldest reply.
        new_run_charge = release_cache._metadata_bytes(1) + 4096
        smaller = release_cache.ReleaseCache(
            self.directory, usage + new_run_charge - oldest_bytes)
        smaller.reserve_run("3" * 64, 1, 4096)
        with smaller._transaction() as connection:
            retained = {row[0] for row in connection.execute("SELECT entry_key FROM entries")}
        self.assertEqual(retained, {"2" * 64, KEY})
        with smaller.release(RUN, COORD, REQUEST, KEY) as slot:
            self.assertIsNotNone(slot.cached)
        with self.assertRaisesRegex(RuntimeError, "capacity.*before private work"):
            smaller.reserve_run("4" * 64, 1, 4096)
        with smaller._transaction() as connection:
            # Failed admission also rolls back attempted unpinned evictions.
            self.assertEqual({row[0] for row in connection.execute("SELECT entry_key FROM entries")},
                             retained)

    def test_cache_hit_is_pinned_by_each_active_run(self):
        self._reserve()
        self._commit()
        self._reserve(run="d" * 64)
        with self.cache.release("d" * 64, COORD, REQUEST, KEY) as slot:
            self.assertIsNotNone(slot.cached)
        self.cache.close_run(RUN)
        with self.cache._transaction() as connection:
            self.assertEqual(connection.execute("SELECT run FROM pins").fetchall(), [("d" * 64,)])
        with self.cache.release("d" * 64, COORD, REQUEST, KEY) as slot:
            self.assertIsNotNone(slot.cached)

    def test_quota_changes_do_not_revoke_existing_run_reservations(self):
        old_limit = (release_cache._STORE_BYTES
                     + release_cache._metadata_bytes(1) + 4096)
        original = release_cache.ReleaseCache(self.directory, old_limit)
        original.reserve_run(RUN, 1, 4096)
        with original.release(RUN, COORD, REQUEST, KEY) as slot:
            slot.commit([np.ones(2, dtype=np.float32)], METRICS)
        larger = release_cache.ReleaseCache(self.directory, 1024 * 1024)
        larger.reserve_run("d" * 64, 10, 4096)
        # An older supervisor can still replay and close its own run using its
        # original receipt, despite newer active reservations at the new quota.
        original.reserve_run(RUN, 1, 4096)
        with original.release(RUN, COORD, REQUEST, KEY) as slot:
            self.assertIsNotNone(slot.cached)
        smaller = release_cache.ReleaseCache(self.directory, 1)
        smaller.reserve_run(RUN, 1, 4096)
        original.close_run(RUN)
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            original.reserve_run(RUN, 1, 4096)

    def test_authoritative_close_rejects_all_further_messages(self):
        self._reserve()
        self._commit()
        self.cache.close_run(RUN)
        self.cache.close_run(RUN)
        self.cache.close_run("f" * 64)
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            self._reserve()
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            with self.cache.release(RUN, COORD, REQUEST, KEY):
                self.fail("closed run replied")

    def test_close_before_reserve_persists_tombstone_for_late_admission(self):
        self.cache.close_run(RUN)
        restarted = release_cache.ReleaseCache(self.directory, 1024 * 1024)
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            restarted.reserve_run(RUN, 1, 4096)

    def test_close_waits_for_active_release_and_then_rejects_late_messages(self):
        self._reserve()
        entered, completed = threading.Event(), threading.Event()
        errors = []

        def close():
            entered.set()
            try:
                self.cache.close_run(RUN)
                completed.set()
            except BaseException as exc:
                errors.append(exc)

        with self.cache.release(RUN, COORD, REQUEST, KEY) as slot:
            worker = threading.Thread(target=close, daemon=True)
            worker.start()
            self.assertTrue(entered.wait(5))
            self.assertFalse(completed.wait(0.1))
            slot.commit([np.asarray([1], dtype=np.float32)], METRICS)
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors)
        self.assertTrue(completed.is_set())
        with self.assertRaisesRegex(RuntimeError, "administratively closed"):
            self._reserve()

    def test_process_crash_replays_committed_bytes_and_retains_uncertain_pins(self):
        context = multiprocessing.get_context("spawn")
        for index, commit in enumerate((False, True), 1):
            run = str(index) * 64
            key = str(index + 2) * 64
            self._reserve(run=run)
            worker = context.Process(target=_crash_release,
                                     args=(self.directory, run, key, commit))
            worker.start()
            worker.join(timeout=15)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
            self.assertEqual(worker.exitcode, 29)
            restarted = release_cache.ReleaseCache(self.directory, 1024 * 1024)
            if commit:
                with restarted.release(run, COORD, REQUEST, key) as slot:
                    self.assertEqual(slot.cached[0][0].tobytes(),
                                     np.asarray([123.5], dtype=np.float32).tobytes())
            else:
                with self.assertRaisesRegex(RuntimeError, "no durable exact reply"):
                    with restarted.release(run, COORD, REQUEST, key):
                        self.fail("crashed private work reran")
            with restarted._transaction() as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM pins WHERE run=? AND entry_key=?", (run, key)
                ).fetchone()[0], 1)

    def test_all_persistent_files_and_directory_are_owner_only(self):
        self._reserve()
        self._commit()
        self.cache.close_run(RUN)
        self.assertEqual(stat.S_IMODE(os.stat(self.directory).st_mode), 0o700)
        for name in os.listdir(self.directory):
            info = os.lstat(os.path.join(self.directory, name))
            self.assertTrue(stat.S_ISREG(info.st_mode))
            self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
            self.assertEqual(info.st_uid, os.geteuid())
            self.assertEqual(info.st_nlink, 1)

    def test_unsafe_directory_file_symlink_and_mount_overlap_rejected(self):
        os.chmod(self.directory, 0o755)
        with self.assertRaisesRegex(RuntimeError, "0700"):
            release_cache.ReleaseCache(self.directory, 1024 * 1024)
        os.chmod(self.directory, 0o700)
        os.chmod(self.cache.database, 0o640)
        with self.assertRaisesRegex(RuntimeError, "0600"):
            release_cache.ReleaseCache(self.directory, 1024 * 1024)
        os.chmod(self.cache.database, 0o600)
        link = os.path.join(self.root, "alias")
        os.symlink(self.directory, link)
        with self.assertRaisesRegex(RuntimeError, "symlinks"):
            release_cache.ReleaseCache(link, 1024 * 1024)
        with self.assertRaisesRegex(RuntimeError, "outside staging"):
            release_cache.ReleaseCache(self.directory, 1024 * 1024,
                                       forbidden_dirs=[self.root])
        os.rename(self.cache.database, self.cache.database + ".saved")
        os.symlink(self.cache.database + ".saved", self.cache.database)
        with self.assertRaisesRegex(RuntimeError, "unsafe|unexpected"):
            release_cache.ReleaseCache(self.directory, 1024 * 1024)

    def test_nonconstant_metrics_object_arrays_and_oversized_entries_rejected(self):
        self._reserve(entry_bytes=256)
        with self.cache.release(RUN, COORD, REQUEST, KEY) as slot:
            with self.assertRaisesRegex(RuntimeError, "constant release metrics"):
                slot.commit([np.ones(1)], {"num-examples": 2, "hook-executed": 1})
            with self.assertRaisesRegex(RuntimeError, "numeric arrays"):
                slot.commit([np.asarray([object()], dtype=object)], METRICS)
            with self.assertRaisesRegex(RuntimeError, "public reservation"):
                slot.commit([np.ones(100)], METRICS)
            slot.commit([np.ones(1)], METRICS)
        with self.assertRaisesRegex(RuntimeError, "committed twice"):
            slot.commit([np.ones(1)], METRICS)

    def test_nonregular_and_hardlinked_state_files_are_rejected(self):
        pipe = os.path.join(self.directory, "pipe")
        os.mkfifo(pipe, 0o600)
        with self.assertRaisesRegex(RuntimeError, "0600"):
            release_cache._safe_file(pipe)
        os.unlink(pipe)
        link = os.path.join(self.root, "database-link")
        os.link(self.cache.database, link)
        with self.assertRaisesRegex(RuntimeError, "0600"):
            release_cache.ReleaseCache(self.directory, 1024 * 1024)

    @unittest.skipUnless(sys.platform == "darwin", "macOS extended ACL invariant")
    def test_macos_directory_read_acl_cannot_override_owner_only_mode(self):
        subprocess.run(["chmod", "+a", "everyone allow read", self.directory],
                       check=True, capture_output=True)
        try:
            self.assertEqual(stat.S_IMODE(os.stat(self.directory).st_mode), 0o700)
            with self.assertRaisesRegex(RuntimeError, "ACLs"):
                release_cache.ReleaseCache(self.directory, 1024 * 1024)
        finally:
            subprocess.run(["chmod", "-N", self.directory], check=True, capture_output=True)

    @unittest.skipUnless(sys.platform == "darwin", "macOS extended ACL invariant")
    def test_macos_file_read_acl_is_rejected_in_process_and_isolated_cli(self):
        subprocess.run(["chmod", "+a", "everyone allow read", self.cache.database],
                       check=True, capture_output=True)
        try:
            self.assertEqual(stat.S_IMODE(os.stat(self.cache.database).st_mode), 0o600)
            with self.assertRaisesRegex(RuntimeError, "ACLs"):
                release_cache.ReleaseCache(self.directory, 1024 * 1024)
            env = dict(os.environ, DSFLOWER_RELEASE_CACHE_DIR=self.directory,
                       DSFLOWER_RELEASE_CACHE_BYTES=str(256 * 1024 * 1024))
            command = [sys.executable, "-I", os.path.join(RUNNER, "release_cache.py"),
                       "reserve", "--run-token", "run_" + "1" * 32, "--rounds", "1"]
            result = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("ACLs", result.stderr)
        finally:
            subprocess.run(["chmod", "-N", self.cache.database],
                           check=True, capture_output=True)

    @unittest.skipUnless(sys.platform == "darwin", "macOS ancestor ACL invariant")
    def test_macos_parent_deny_only_acl_is_safe_but_replacement_grant_is_rejected(self):
        subprocess.run(["chmod", "+a", "everyone deny delete_child", self.root],
                       check=True, capture_output=True)
        try:
            release_cache.ReleaseCache(self.directory, 1024 * 1024)
        finally:
            subprocess.run(["chmod", "-N", self.root], check=True, capture_output=True)
        subprocess.run(["chmod", "+a", "everyone allow delete_child", self.root],
                       check=True, capture_output=True)
        try:
            with self.assertRaisesRegex(RuntimeError, "ACLs"):
                release_cache.ReleaseCache(self.directory, 1024 * 1024)
        finally:
            subprocess.run(["chmod", "-N", self.root], check=True, capture_output=True)

    def test_corruption_fails_closed_without_recomputing(self):
        self._reserve()
        self._commit()
        with self.cache._transaction() as connection:
            connection.execute("UPDATE entries SET payload=?", (b"corrupt",))
        with self.assertRaisesRegex(RuntimeError, "corrupt"):
            with self.cache.release(RUN, COORD, REQUEST, KEY):
                self.fail("corrupt cache reran private application")

    def test_isolated_administrator_cli_reserves_and_closes(self):
        env = dict(os.environ, DSFLOWER_RELEASE_CACHE_DIR=self.directory,
                   DSFLOWER_RELEASE_CACHE_BYTES=str(256 * 1024 * 1024))
        token = "run_" + "1" * 32
        command = [sys.executable, "-I", os.path.join(RUNNER, "release_cache.py")]
        for action in (["reserve", "--run-token", token, "--rounds", "2"],
                       ["close", "--run-token", token]):
            result = subprocess.run(command + action, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        result = subprocess.run(command + ["reserve", "--run-token", token, "--rounds", "2"],
                                env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("administratively closed", result.stderr)


if __name__ == "__main__":
    unittest.main()
