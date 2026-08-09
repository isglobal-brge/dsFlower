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
                 epsilon=1.0, delta=1.0e-6):
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=bytes(range(32))):
            return tree_release.joint_gaussian_release(
                value, mechanism="test-tree-gaussian/v1",
                layout=({"cells": 4, "release_index": 0}
                        if layout is None else layout),
                epsilon=epsilon, delta=delta, sensitivity=math.sqrt(2.0),
                num_releases=releases,
                execution_fingerprint="test-tree-release-v1")

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
        composed, composed_sigma = self._release(raw, releases=4)
        self.assertFalse(np.array_equal(first - raw, changed - (raw + [1, 0, 0, 0])))
        self.assertFalse(np.array_equal(first, relabeled))
        self.assertFalse(np.array_equal(first, composed))
        self.assertGreater(composed_sigma, sigma)

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
                "af85ba0eabd436e08eb57596cffef9d8cd82fe5f702fb94af4d191e8414871ef",
            ("darwin", "x86_64", "2.4.6"):
                "9f50a426a942428383532a3e9bb13c0060bd2ba19b59d987d03a1231ec742b4c",
            ("linux", "x86_64", "2.4.6"):
                "4edb2f0e26371273c9fb6e7e095fcc2c2f15d5d1b6c06aa40504eb31d91ec93f",
            ("windows", "amd64", "2.4.6"):
                "a221d81fddfe9e6c8751b6e4fd084c2d068f2b0be24f8c486b2d869d4c558171",
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
