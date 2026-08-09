"""Stateless release guard and deterministic-CSPRNG security invariants.

Run with:
    python3 dsFlower/inst/python/tests/test_release_guard.py
"""

import json
import os
import stat
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
from flwr.common import ArrayRecord, ConfigRecord, RecordDict


RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import release_guard
import seeding


class _Context:
    def __init__(self, manifest_dir):
        self.node_config = {"manifest-dir": manifest_dir}
        self.state = RecordDict()


class _Metadata:
    def __init__(self, message_id):
        self.message_id = message_id
        self.group_id = ""


class _Message:
    def __init__(self, message_id, server_round=1, values=(1.0, 2.0),
                 include_config=True):
        self.metadata = _Metadata(message_id)
        records = {
            "arrays": ArrayRecord(numpy_ndarrays=[
                np.asarray(values, dtype=np.float32)])
        }
        if include_config:
            records["config"] = ConfigRecord({"server-round": server_round})
        self.content = RecordDict(records)


class ReleaseGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.token = "run_" + "a" * 32
        self.manifest = {
            "run_token": self.token,
            "privacy-adjacency": "replace_one",
            "privacy-policy-sha256": "1" * 64,
            "privacy-epsilon": 1.5,
            "privacy-delta": 5e-6,
            "num-server-rounds": 2,
        }
        self._write_manifest()
        self.context = _Context(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def _write_manifest(self):
        with open(os.path.join(self.root, "manifest.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(self.manifest, handle)

    def _cache(self, claim, arrays=(9.0, 8.0), include_arrays=True):
        self.context.state["dsflower-last-release-meta"] = ConfigRecord({
            "message-id": claim["message_id"],
            "request-id": claim["request_id"],
            "release-index": claim["release_index"],
        })
        if include_arrays:
            self.context.state["dsflower-last-release"] = ArrayRecord(
                numpy_ndarrays=[np.asarray(arrays, dtype=np.float32)])

    def test_exact_cached_request_replays_but_message_id_is_not_identity(self):
        first = release_guard.claim_release(self.context, _Message("m1"))
        self.assertEqual((first["status"], first["release_index"]), ("new", 1))
        self._cache(first)

        same_message = release_guard.claim_release(
            self.context, _Message("m1"))
        different_message = release_guard.claim_release(
            self.context, _Message("m2"))
        self.assertEqual(same_message["status"], "replay")
        self.assertEqual(different_message["status"], "replay")
        self.assertEqual(first["request_id"], different_message["request_id"])

    def test_lost_reply_cache_recomputes_instead_of_blocking(self):
        first = release_guard.claim_release(self.context, _Message("m1"))
        self._cache(first, include_arrays=False)
        retried = release_guard.claim_release(self.context, _Message("m1"))
        self.assertEqual(retried["status"], "new")

    def test_payload_collision_for_provisional_round_fails_before_private_work(self):
        first = release_guard.claim_release(self.context, _Message("m1"))
        self._cache(first)
        with self.assertRaisesRegex(RuntimeError, "does not match request payload"):
            release_guard.claim_release(
                self.context, _Message("m1", values=(1.0, 3.0)))
        with self.assertRaisesRegex(RuntimeError, "does not match request payload"):
            release_guard.claim_release(
                self.context, _Message("another-id", values=(1.0, 3.0)))

    def test_round_is_exact_and_bounded_by_the_manifest(self):
        for bad in (True, False, 0, 3, 1.0, "1"):
            with self.subTest(server_round=bad):
                with self.assertRaisesRegex(RuntimeError, "server-round"):
                    release_guard.claim_release(
                        _Context(self.root), _Message("m", server_round=bad))
        with self.assertRaisesRegex(RuntimeError, "missing.*ConfigRecord"):
            release_guard.claim_release(
                _Context(self.root), _Message("m", include_config=False))

        second = release_guard.claim_release(
            _Context(self.root), _Message("m", server_round=2))
        self.assertEqual(second["release_index"], 2)

    def test_manifest_rounds_and_policy_contract_are_fixed(self):
        self.manifest["num-server-rounds"] = 0
        self._write_manifest()
        with self.assertRaisesRegex(RuntimeError, "num-server-rounds"):
            release_guard.claim_release(self.context, _Message("m1"))

        self.manifest["num-server-rounds"] = 2
        self.manifest["privacy-policy-sha256"] = "not-a-policy-hash"
        self._write_manifest()
        with self.assertRaisesRegex(RuntimeError, "canonical stateless"):
            release_guard.claim_release(self.context, _Message("m1"))

    def test_runtime_needs_no_persistent_accounting_state(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            claim = release_guard.claim_release(
                _Context(self.root), _Message("m1"))
        self.assertEqual(claim["status"], "new")

    def test_stateless_claims_do_not_exhaust(self):
        for index in range(1000):
            claim = release_guard.claim_release(
                _Context(self.root), _Message("call-%d" % index))
            self.assertEqual(claim["status"], "new")
            self.assertEqual(claim["num_rounds"], 2)

class SeedDerivationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.secret = os.path.join(self.tmp.name, "node_secret")
        with open(self.secret, "w", encoding="ascii") as handle:
            handle.write("42" * 32 + "\n")
        os.chmod(self.secret, stat.S_IRUSR | stat.S_IWUSR)
        self.old_secret = os.environ.get("DSFLOWER_NODE_SECRET_FILE")
        os.environ["DSFLOWER_NODE_SECRET_FILE"] = self.secret

    def _write_secret(self, content):
        with open(self.secret, "wb") as handle:
            handle.write(content)
        os.chmod(self.secret, 0o600)

    def tearDown(self):
        if self.old_secret is None:
            os.environ.pop("DSFLOWER_NODE_SECRET_FILE", None)
        else:
            os.environ["DSFLOWER_NODE_SECRET_FILE"] = self.old_secret
        self.tmp.cleanup()

    @staticmethod
    def _contract():
        return (
            {"loss-name": "mse", "optimizer": {"name": "sgd", "lr": 0.1}},
            {"policy_hash": "1" * 64, "epsilon": 1.0,
             "delta": 1e-6, "clipping_norm": 1.0},
        )

    def test_key_is_semantic_sticky_and_operational_metadata_is_ignored(self):
        cfg, privacy = self._contract()
        public = [np.asarray([[1.0]], dtype=np.float32)]
        private = [np.asarray([[2.0]], dtype=np.float32),
                   np.asarray([0], dtype=np.int64)]
        a = seeding.master_seed(
            "neural-dpsgd/v1", cfg, privacy, 1,
            public_arrays=public, private_arrays=private)
        replay = seeding.master_seed(
            "neural-dpsgd/v1", dict(reversed(list(cfg.items()))),
            dict(reversed(list(privacy.items()))), 1,
            public_arrays=public, private_arrays=private)
        noisy_cfg_a = dict(cfg, **{
            "run-token": "run-a", "message-id": "message-a",
            "manifest-path": "/tmp/one", "staged-at": "yesterday"})
        noisy_cfg_b = dict(cfg, **{
            "run-token": "run-b", "message-id": "message-b",
            "manifest-path": "/srv/two", "staged-at": "today"})
        selected_a = seeding.select_config(noisy_cfg_a, cfg)
        selected_b = seeding.select_config(noisy_cfg_b, cfg)
        operational_change = seeding.master_seed(
            "neural-dpsgd/v1", selected_b, privacy, 1,
            public_arrays=public, private_arrays=private)
        self.assertEqual(a, replay)
        self.assertEqual(selected_a, selected_b)
        self.assertEqual(a, operational_change)

    def test_every_effective_semantic_axis_changes_the_key(self):
        cfg, privacy = self._contract()
        public = [np.asarray([[1.0]], dtype=np.float32)]
        private = [np.asarray([[2.0]], dtype=np.float32),
                   np.asarray([0], dtype=np.int64)]

        def derive(mechanism="neural-dpsgd/v1", config=cfg,
                   policy=privacy, round_index=1, public_arrays=public,
                   private_arrays=private, unit_ids=None):
            return seeding.master_seed(
                mechanism, config, policy, round_index,
                public_arrays=public_arrays, private_arrays=private_arrays,
                unit_ids=unit_ids)

        base = derive()
        changed = (
            derive(mechanism="validation-gaussian/v1"),
            derive(config={**cfg, "loss-name": "huber"}),
            derive(policy={**privacy, "policy_hash": "2" * 64}),
            derive(round_index=2),
            derive(public_arrays=[np.asarray([[1.5]], dtype=np.float32)]),
            derive(private_arrays=[np.asarray([[2.5]], dtype=np.float32),
                                   private[1]]),
            derive(private_arrays=[private[0], np.asarray([1], dtype=np.int64)]),
            derive(unit_ids=["patient-1"]),
        )
        self.assertTrue(all(value != base for value in changed))
        self.assertEqual(len(set(changed)), len(changed))

    def test_runtime_fingerprint_is_part_of_the_semantic_identity(self):
        cfg, privacy = self._contract()
        with mock.patch.object(
                seeding, "_runtime_fingerprint",
                return_value={"backend": "cpu", "version": "one"}):
            first = seeding.master_seed(
                "neural-dpsgd/v1", cfg, privacy, 1)
        with mock.patch.object(
                seeding, "_runtime_fingerprint",
                return_value={"backend": "cpu", "version": "two"}):
            second = seeding.master_seed(
                "neural-dpsgd/v1", cfg, privacy, 1)
        self.assertNotEqual(first, second)

    def test_oversized_unit_ids_totalize_instead_of_failing(self):
        cfg, privacy = self._contract()
        oversized = seeding.master_seed(
            "neural-dpsgd/v1", cfg, privacy, 1,
            unit_ids=["x" * (seeding._MAX_UNIT_ID_BYTES + 1)])
        sentinel = seeding.master_seed(
            "neural-dpsgd/v1", cfg, privacy, 1,
            unit_ids=["__dsflower_missing_patient_unit__"])
        self.assertEqual(oversized, sentinel)

    def test_semantic_digest_has_a_cross_platform_golden_contract(self):
        cfg, privacy = self._contract()
        with mock.patch.object(
                seeding, "_runtime_fingerprint",
                return_value={"backend": "test", "version": "1"}):
            digest = seeding._semantic_digest(
                "neural-dpsgd/v1", cfg, privacy, 2,
                public_arrays=(np.asarray([[1, -0.0]], dtype=">f4"),),
                private_arrays=(np.asarray([[2.5]], dtype="<f4"),
                                np.asarray([3], dtype="<i8")),
                unit_ids=["p1"])
        self.assertEqual(
            digest.hex(),
            "92ff80aa5026b03062a7885021760409183dc3f436f7fca9a2e464b1779d34d0")

    def test_stream_is_reproducible_but_domain_separated(self):
        cfg, privacy = self._contract()
        master = seeding.master_seed(
            "neural-dpsgd/v1", cfg, privacy, 1)
        one = seeding.np_rng(seeding.sub_seed(master, "noise")).normal(size=32)
        replay = seeding.np_rng(seeding.sub_seed(master, "noise")).normal(size=32)
        other = seeding.np_rng(seeding.sub_seed(master, "shuffle")).normal(size=32)
        self.assertTrue((one == replay).all())
        self.assertFalse((one == other).all())

    def test_torch_determinism_is_strict_not_warning_only(self):
        calls = []
        cudnn = SimpleNamespace(benchmark=True, deterministic=False)
        fake_torch = SimpleNamespace(
            manual_seed=lambda value: None,
            cuda=SimpleNamespace(
                is_available=lambda: False,
                manual_seed_all=lambda value: None),
            use_deterministic_algorithms=lambda *args, **kwargs:
                calls.append((args, kwargs)),
            backends=SimpleNamespace(cudnn=cudnn),
        )
        with mock.patch.dict(sys.modules, {"torch": fake_torch}):
            seeding.seed_torch(b"s" * 32)

        self.assertEqual(calls, [((True,), {})])
        self.assertFalse(cudnn.benchmark)
        self.assertTrue(cudnn.deterministic)

    def test_bound_hook_noise_is_sticky_only_for_the_same_update(self):
        cfg, privacy = self._contract()
        master = seeding.master_seed("hook-output/v1", cfg, privacy, 1)
        update = [np.asarray([1.0, 2.0], dtype=np.float64)]
        replay = [np.asarray([1.0, 2.0], dtype=np.float64)]
        changed = [np.asarray([1.0, 3.0], dtype=np.float64)]
        self.assertEqual(
            seeding.bind_seed(master, "hook-update", update),
            seeding.bind_seed(master, "hook-update", replay))
        self.assertNotEqual(
            seeding.bind_seed(master, "hook-update", update),
            seeding.bind_seed(master, "hook-update", changed))

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

    def test_windows_secret_branch_avoids_unix_only_apis(self):
        with mock.patch.object(seeding.os, "name", "nt"), \
                mock.patch.object(
                    seeding.os, "geteuid", create=True,
                    side_effect=AssertionError("geteuid is Unix-only")), \
                mock.patch.object(
                    seeding.os, "O_NOFOLLOW", None, create=True):
            self.assertEqual(
                seeding._node_secret(), bytes.fromhex("42" * 32))

    def test_windows_metadata_rejects_reparse_and_missing_file_identity(self):
        regular = SimpleNamespace(
            st_file_attributes=0, st_reparse_tag=0,
            st_dev=1, st_ino=2)
        reparse = SimpleNamespace(
            st_file_attributes=0x400, st_reparse_tag=0xA000000C,
            st_dev=1, st_ino=2)
        zero_identity = SimpleNamespace(st_dev=0, st_ino=0)

        self.assertFalse(seeding._is_windows_reparse_point(regular))
        self.assertTrue(seeding._is_windows_reparse_point(reparse))
        with self.assertRaisesRegex(RuntimeError, "stable.*identity"):
            seeding._same_file_identity(
                zero_identity, zero_identity, windows=True)

    @unittest.skipIf(os.name == "nt", "Unix ownership/mode invariant")
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

    @unittest.skipIf(os.name == "nt", "Unix ownership/mode invariant")
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

    @unittest.skipIf(os.name == "nt", "Unix ownership/mode invariant")
    def test_secret_requires_trusted_and_stable_parent(self):
        parent = os.path.dirname(self.secret)
        original_mode = stat.S_IMODE(os.lstat(parent).st_mode)
        try:
            os.chmod(parent, 0o770)
            with self.assertRaisesRegex(RuntimeError, "parent.*writable"):
                seeding._node_secret()
        finally:
            os.chmod(parent, original_mode)

        parent_info = os.lstat(parent)
        wrong_owner = SimpleNamespace(
            st_mode=parent_info.st_mode, st_uid=os.geteuid() + 1,
            st_dev=parent_info.st_dev, st_ino=parent_info.st_ino,
        )
        real_lstat = os.lstat

        def lstat_with_wrong_parent(path):
            return wrong_owner if path == parent else real_lstat(path)

        with mock.patch.object(
                seeding.os, "lstat", side_effect=lstat_with_wrong_parent):
            with self.assertRaisesRegex(RuntimeError, "parent.*owned"):
                seeding._node_secret()

        if os.geteuid() != 0:
            root_owned = SimpleNamespace(
                st_mode=parent_info.st_mode, st_uid=0,
                st_dev=parent_info.st_dev, st_ino=parent_info.st_ino,
            )

            def lstat_with_root_parent(path):
                return root_owned if path == parent else real_lstat(path)

            with mock.patch.object(
                    seeding.os, "lstat", side_effect=lstat_with_root_parent):
                self.assertEqual(seeding._node_secret(), bytes.fromhex("42" * 32))

        swapped_parent = SimpleNamespace(
            st_mode=parent_info.st_mode, st_uid=os.geteuid(),
            st_dev=parent_info.st_dev, st_ino=parent_info.st_ino + 1,
        )
        parent_reads = 0

        def lstat_with_parent_swap(path):
            nonlocal parent_reads
            if path == parent:
                parent_reads += 1
                if parent_reads > 1:
                    return swapped_parent
            return real_lstat(path)

        with mock.patch.object(
                seeding.os, "lstat", side_effect=lstat_with_parent_swap):
            with self.assertRaisesRegex(RuntimeError, "parent changed"):
                seeding._node_secret()


if __name__ == "__main__":
    unittest.main()
