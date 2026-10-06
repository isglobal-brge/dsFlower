"""Tests for the common sufficient-vector Gaussian tree release."""

import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import math
import os
import sys
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import tree_release, seeding
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_forest_adapter import _manifest
from tree_release_kat_support import (CONTRACT, EXECUTION, capture_record,
                                      environment_key, release, request)


class JointGaussianReleaseTests(unittest.TestCase):
    def _release(self, value, layout=None, releases=1,
                 epsilon=1.0, delta=1.0e-6,
                 sensitivity=math.sqrt(2.0),
                 mechanism="test-tree-gaussian/v1",
                 execution="test-tree-release-v1"):
        return release(value, layout, releases, epsilon, delta, sensitivity,
                       mechanism, execution)

    def test_replay_and_canonical_layout_are_exact(self):
        raw = np.asarray([[1, 2], [3, 4]], dtype=np.int64)
        first, sigma = self._release(raw, {"b": 2, "a": 1})
        replay, replay_sigma = self._release(
            raw.astype(">f8"), {"a": 1, "b": 2})
        np.testing.assert_array_equal(first, replay)
        self.assertEqual(sigma, replay_sigma)
        self.assertGreater(sigma, 0.0)

    def test_sufficient_vector_layout_and_composition_bind_noise(self):
        raw = np.asarray([1.0, 2.0, 3.0, 4.0])
        first, sigma = self._release(raw)
        changed, _ = self._release(raw + np.asarray([1.0, 0.0, 0.0, 0.0]))
        relabeled, _ = self._release(raw, {"cells": 4, "release_index": 1})
        remechanized, _ = self._release(
            raw, mechanism="test-tree-gaussian/v2")
        reexecuted, _ = self._release(
            raw, execution="test-tree-release-v2")
        composed, composed_sigma = self._release(raw, releases=4)
        self.assertFalse(np.array_equal(first - raw, changed - (raw + [1, 0, 0, 0])))
        self.assertFalse(np.array_equal(first, relabeled))
        self.assertFalse(np.array_equal(first, remechanized))
        self.assertFalse(np.array_equal(first, reexecuted))
        self.assertFalse(np.array_equal(first, composed))
        self.assertGreater(composed_sigma, sigma)

    def test_raw_calibration_policy_rekeys_even_when_sigma_is_equal(self):
        raw = np.asarray([4.0, 3.0, 2.0, 1.0])
        with mock.patch.object(
                tree_release.dp_harness, "compute_output_sigma",
                return_value=3.25) as calibrate:
            first, sigma = self._release(
                raw, epsilon=0.5, delta=1.0e-5,
                sensitivity=math.sqrt(2.0), releases=1)
            replay, replay_sigma = self._release(
                raw, epsilon=4.0, delta=1.0e-8,
                sensitivity=8.0, releases=17)
        self.assertEqual(calibrate.call_count, 2)
        self.assertEqual(sigma, replay_sigma)
        self.assertNotEqual(first.tobytes(), replay.tobytes())
        self.assertNotEqual((first - raw).tobytes(), (replay - raw).tobytes())

    def test_raw_policy_nextafter_rekeys_even_when_sigma_is_equal(self):
        raw = np.asarray([4.0, 3.0, 2.0, 1.0])
        first, sigma = self._release(raw, delta=1.0e-6)
        adjacent_delta = math.nextafter(1.0e-6, math.inf)
        replay, replay_sigma = self._release(raw, delta=adjacent_delta)
        self.assertEqual(sigma, replay_sigma)
        self.assertNotEqual(first.tobytes(), replay.tobytes())

    def test_numeric_profile_known_answer_for_supported_matrix(self):
        path = Path(__file__).parent / "fixtures/tree-release-kat-v3.json"
        self.assertTrue(path.is_file(),
                        "Missing final v3 KAT fixture; run tools/generate-tree-release-kat.py after source freeze")
        fixture = json.loads(path.read_text())
        self.assertEqual(fixture["contract"], CONTRACT)
        identity = request()
        runtime = json.loads(identity.canonical_json)["runtime"]
        numeric = tree_release.numeric_execution_profile()
        key = environment_key(runtime, numeric)
        matches = [record for record in fixture["profiles"]
                   if environment_key(record["runtime"], record["numeric_profile"]) == key]
        if not matches:
            self.skipTest("actual full runtime has not been verified for this KAT: " + key)
        self.assertEqual(len(matches), 1, "duplicate verified runtime profiles")
        expected = matches[0]
        self.assertEqual(runtime, expected["runtime"],
                         "Runner source changed; deliberately regenerate the v3 KAT after review")
        actual = capture_record()
        # Calibration is unchanged by the identity migration; retain its
        # independent pre-v3 known answer as well as the recorded profile.
        self.assertEqual(actual["sigma_hex"], "0x1.e439944d8cd2fp+2")
        self.assertEqual(actual, expected)

    def test_actual_complete_runner_hash_is_bound_and_source_changes_rekey(self):
        def complete_hash(directory):
            digest = hashlib.sha256()
            paths = [path for path in directory.rglob("*") if path.is_file()
                     and "__pycache__" not in path.relative_to(directory).parts
                     and path.suffix not in (".pyc", ".pyo")]
            for path in sorted(paths, key=lambda item: item.relative_to(directory).as_posix()):
                digest.update(path.relative_to(directory).as_posix().encode() + b"\n")
                digest.update(path.read_bytes())
                digest.update(b"\x00")
            return digest.hexdigest()

        actual = json.loads(request().canonical_json)["runtime"]
        directory = Path(seeding.__file__).resolve().parent
        self.assertEqual(actual["runner_sha256"], complete_hash(directory))
        self.assertEqual(actual["runner_sha256"], seeding._runtime_fingerprint(False)["runner_sha256"])
        # Execute an isolated real copy, then alter a non-Python runner file.
        # This tests the complete hash without mocking any runtime fact or
        # writing into either package's trusted production directory.
        name = "_tree_kat_runner_copy"
        with tempfile.TemporaryDirectory() as temporary:
            copied = Path(temporary) / "runner"
            shutil.copytree(directory, copied, ignore=shutil.ignore_patterns("__pycache__"))
            spec = importlib.util.spec_from_file_location(
                name, copied / "__init__.py", submodule_search_locations=[str(copied)])
            package = importlib.util.module_from_spec(spec)
            sys.modules[name] = package
            try:
                spec.loader.exec_module(package)
                copied_release = importlib.import_module(name + ".tree_release")
                copied_seeding = importlib.import_module(name + ".seeding")
                manifest = _manifest(trees=2, depth=1)
                first = copied_release.native_request_identity(manifest, execution_fingerprint=EXECUTION)
                self.assertEqual(json.loads(first.canonical_json)["runtime"]["runner_sha256"],
                                 complete_hash(copied))
                (copied / "kat-source-witness.txt").write_bytes(b"public runner source change")
                copied_seeding._runtime_fingerprint.cache_clear()
                changed = copied_release.native_request_identity(manifest, execution_fingerprint=EXECUTION)
                self.assertEqual(json.loads(changed.canonical_json)["runtime"]["runner_sha256"],
                                 complete_hash(copied))
                self.assertNotEqual(first.sha256, changed.sha256)
            finally:
                for key in list(sys.modules):
                    if key == name or key.startswith(name + "."):
                        del sys.modules[key]

    def test_kat_environment_selection_excludes_only_the_runner_hash(self):
        runtime = json.loads(request().canonical_json)["runtime"]
        numeric = tree_release.numeric_execution_profile()
        first = environment_key(runtime, numeric)
        self.assertEqual(first, environment_key(dict(runtime, runner_sha256="0" * 64), numeric))
        for field, value in runtime.items():
            if field == "runner_sha256":
                continue
            with self.subTest(runtime_field=field):
                changed = dict(runtime)
                changed[field] = {"changed": value}
                self.assertNotEqual(first, environment_key(changed, numeric))
        for field, value in numeric.items():
            with self.subTest(numeric_field=field):
                changed = dict(numeric)
                changed[field] = {"changed": value}
                self.assertNotEqual(first, environment_key(runtime, changed))

    def test_malformed_or_nonfinite_vectors_fail_closed(self):
        for value in (
                np.asarray([], dtype=np.float64),
                np.asarray([float("nan")]),
                np.asarray([object()], dtype=object)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self._release(value)
        with self.assertRaises(ValueError):
            self._release(np.asarray([1.0]), layout=[])


if __name__ == "__main__":
    unittest.main()
