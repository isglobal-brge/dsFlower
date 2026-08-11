"""Tests for the common sufficient-vector Gaussian tree release."""

import hashlib
import math
import os
import sys
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import tree_release


class JointGaussianReleaseTests(unittest.TestCase):
    def _release(self, value, layout=None, releases=1,
                 epsilon=1.0, delta=1.0e-6,
                 sensitivity=math.sqrt(2.0),
                 mechanism="test-tree-gaussian/v1",
                 execution="test-tree-release-v1"):
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=bytes(range(32))):
            return tree_release.joint_gaussian_release(
                value, mechanism=mechanism,
                layout=({"cells": 4, "release_index": 0}
                        if layout is None else layout),
                epsilon=epsilon, delta=delta, sensitivity=sensitivity,
                num_releases=releases,
                execution_fingerprint=execution)

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

    def test_calibration_inputs_are_not_reroll_axes_when_sigma_is_equal(self):
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
        self.assertEqual(first.tobytes(), replay.tobytes())
        self.assertEqual((first - raw).tobytes(), (replay - raw).tobytes())

    def test_raw_policy_nextafter_is_not_a_reroll_axis_when_sigma_is_equal(self):
        raw = np.asarray([4.0, 3.0, 2.0, 1.0])
        first, sigma = self._release(raw, delta=1.0e-6)
        adjacent_delta = math.nextafter(1.0e-6, math.inf)
        replay, replay_sigma = self._release(raw, delta=adjacent_delta)
        self.assertEqual(sigma, replay_sigma)
        np.testing.assert_array_equal(first, replay)

    def test_numeric_profile_known_answer_for_supported_matrix(self):
        expected = {
            ("darwin", "arm64", "2.4.6"):
                "be65850d33e5a992f54e3a003be63c6315558b001bc1028088649a19cb4e0610",
            ("darwin", "x86_64", "2.4.6"):
                "31efb146d544fc01556ae140510578c716e7b6c9ab10a7e047c51e382b678bf3",
            ("linux", "x86_64", "2.4.6"):
                "cdc473e7ec8b242e451b4a52398d01884178ff8ecbafb731fa98b113864ebe27",
            ("windows", "amd64", "2.4.6"):
                "6b5b0b77bafeca529f7b60ff960d0a9284f21bd8c6dbdfac10d904de088476b2",
        }
        profile = tree_release.numeric_execution_profile()
        key = (profile["system"], profile["machine"], profile["numpy"])
        if key not in expected:
            self.skipTest("numeric profile is not in the release matrix: %r" % (key,))
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=bytes(range(32))):
            released, sigma = tree_release.joint_gaussian_release(
                np.asarray([0.0, 1.0, 2.0, 3.0]),
                mechanism="tree-release-kat/v1",
                layout={"coordinates": 4, "release_index": 0},
                epsilon=1.0, delta=1.0e-6,
                sensitivity=math.sqrt(2.0), num_releases=1,
                execution_fingerprint="tree-release-kat-adapter-v1")
        digest = hashlib.sha256(np.ascontiguousarray(
            released, dtype="<f8").tobytes()).hexdigest()
        self.assertEqual(sigma.hex(), "0x1.e439944d8cd2fp+2")
        self.assertEqual(digest, expected[key])

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
