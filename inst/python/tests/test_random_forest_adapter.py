"""Privacy, artifact, utility and cost tests for adaptive DP Random Forest."""

import copy
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import forest_accounting
from dsflower_runner import forest_predictor as predictor
from dsflower_runner import random_forest_adapter as adapter
from dsflower_runner import xgboost_adapter as tree_data
from dsflower_runner.forest_sanitizer import sanitize_forest_json


def _typed(kind, value):
    return {"type": kind, "value": value}


def _schema(task="binary_classification", features=3):
    names = ["x%d" % index for index in range(features)]
    target = (
        {"name": "outcome", "kind": "binary", "levels": [
            {"type": "string", "value": "control"},
            {"type": "string", "value": "case"},
        ], "lower": 0.0, "upper": 1.0}
        if task == "binary_classification" else
        {"name": "outcome", "kind": "continuous", "levels": None,
         "lower": -2.0, "upper": 2.0}
    )
    core = {
        "version": 1,
        "features": names,
        "lower": [-2.0] * features,
        "upper": [2.0] * features,
        "cuts": [[-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5]
                 for _ in names],
        "target": target,
    }
    raw = json.dumps(
        core, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    return dict(core, sha256=hashlib.sha256(raw).hexdigest())


def _manifest(task="binary_classification", *, trees=12, depth=3,
              max_features=2, unit="row", features=3, epsilon=12.0):
    schema = _schema(task, features)
    return {
        "contract_version": 1,
        "mode": "native-tight",
        "engine": "random_forest",
        "task": task,
        "public_schema": schema,
        "engine_params": {
            "max_depth": _typed("int", depth),
            "max_features": _typed("int", max_features),
            "n_estimators": _typed("int", trees),
        },
        "privacy": {
            "mechanism": "dp-forest-v1",
            "epsilon": epsilon,
            "delta": 1.0e-6,
            "unit": unit,
            "adjacency": "replace_one",
            "unit_canonicalization": "trim-utf8-v2",
            "contribution_strategy": "one-record-per-unit-v1",
            "max_rows_per_unit": 1,
            "mechanism_params": {
                "candidate_schedule": _typed(
                    "string",
                    forest_accounting.RANDOM_FOREST_CANDIDATE_PROFILE),
                "histogram_release": _typed(
                    "string",
                    forest_accounting.RANDOM_FOREST_HISTOGRAM_PROFILE),
                "leaf_release": _typed(
                    "string", forest_accounting.RANDOM_FOREST_LEAF_PROFILE),
                "partition": _typed(
                    "string",
                    forest_accounting.RANDOM_FOREST_PARTITION_PROFILE),
                "transcript": _typed(
                    "string",
                    forest_accounting.RANDOM_FOREST_TRANSCRIPT_PROFILE),
            },
        },
        "data_scope": {
            "snapshot_hash": "a" * 64,
            "cohort_hash": "b" * 64,
            "schema_hash": schema["sha256"],
        },
        "resources": {
            "threads": 4,
            "memory_mib": 4096,
            "wall_seconds": 900,
            "max_rows": 1_000_000,
            "max_features": 64,
            "max_trees": 512,
            "max_depth": 12,
            "max_bins": 16,
            "max_artifact_bytes": 16 * 1024 * 1024,
        },
    }


def _rehash_schema(manifest):
    core = {
        key: value for key, value in manifest["public_schema"].items()
        if key != "sha256"
    }
    raw = json.dumps(
        core, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    manifest["public_schema"]["sha256"] = digest
    manifest["data_scope"]["schema_hash"] = digest
    return manifest


def _key(value=bytes(range(32))):
    return mock.patch("dsflower_runner.seeding._node_secret",
                      return_value=value)


def _train(manifest, X, y, unit_ids=None):
    with _key():
        prepared = adapter.prepare_random_forest_training(
            manifest, X, y, unit_ids=unit_ids)
        return adapter.train_random_forest(prepared)


class RandomForestAccountingTests(unittest.TestCase):
    def test_replace_one_sensitivities_and_fixed_composition_are_pinned(self):
        split, leaf = forest_accounting.random_forest_sensitivities(
            "binary_classification", 8)
        self.assertEqual(split, 4.0)
        self.assertEqual(leaf, math.sqrt(2.0))
        split, leaf = forest_accounting.random_forest_sensitivities(
            "regression", 9)
        self.assertEqual(split, 6.0)
        self.assertEqual(leaf, 2.0)
        profile = adapter.canonical_random_forest_profile(
            _manifest(trees=8, depth=4, max_features=3))
        self.assertEqual(profile["num_releases"], 5)
        self.assertGreater(profile["split_sigma"], profile["leaf_sigma"])
        self.assertEqual(profile["split_sensitivity"], math.sqrt(6.0))

    def test_old_and_new_units_in_different_trees_nodes_reach_the_bound(self):
        for task, expected in (
                ("binary_classification", 2.0),
                ("regression", 2.0 * math.sqrt(2.0))):
            manifest = _manifest(
                task, trees=8, depth=2, max_features=2, features=3)
            profile = adapter.canonical_random_forest_profile(manifest)
            rows = np.asarray([
                [-1.8, -1.8, -1.8], [-1.2, -0.8, -0.2],
                [-0.2, 0.2, 0.8], [0.2, 0.8, 1.2],
                [0.8, 1.2, 1.8], [1.8, 1.8, 1.8],
            ])
            target = np.ones(rows.shape[0]) * (
                1.0 if task == "binary_classification" else 2.0)
            materialized = adapter.materialize_random_forest_units(
                manifest, rows, target)
            with _key():
                candidates, assignment_key = adapter._public_schedule(
                    profile, rows.shape[1])
                try:
                    assignments = adapter._tree_assignments(
                        materialized._binned_features,
                        materialized._target_units,
                        profile["n_estimators"], assignment_key)
                finally:
                    assignment_key[:] = b"\x00" * len(assignment_key)
            pair = next(
                (left, right)
                for left in range(rows.shape[0])
                for right in range(left + 1, rows.shape[0])
                if assignments[left] != assignments[right]
            )
            level_candidates = candidates[:, 1:3, :]
            old = adapter._level_histograms(
                materialized._binned_features[pair[0]:pair[0] + 1],
                materialized._target_units[pair[0]:pair[0] + 1],
                assignments[pair[0]:pair[0] + 1],
                np.asarray([0], dtype=np.uint32), level_candidates,
                manifest, profile)
            new = adapter._level_histograms(
                materialized._binned_features[pair[1]:pair[1] + 1],
                materialized._target_units[pair[1]:pair[1] + 1],
                assignments[pair[1]:pair[1] + 1],
                np.asarray([1], dtype=np.uint32), level_candidates,
                manifest, profile)
            self.assertAlmostEqual(
                float(np.linalg.norm(new - old)), expected, places=12)

            old_leaf = adapter._leaf_histograms(
                assignments[pair[0]:pair[0] + 1],
                np.asarray([0], dtype=np.uint32),
                materialized._target_units[pair[0]:pair[0] + 1],
                manifest, profile)
            new_leaf = adapter._leaf_histograms(
                assignments[pair[1]:pair[1] + 1],
                np.asarray([3], dtype=np.uint32),
                materialized._target_units[pair[1]:pair[1] + 1],
                manifest, profile)
            leaf_expected = (math.sqrt(2.0)
                             if task == "binary_classification" else 2.0)
            self.assertAlmostEqual(
                float(np.linalg.norm(new_leaf - old_leaf)),
                leaf_expected, places=12)

    def test_profile_is_exact_and_physical_caps_fail_closed(self):
        manifest = _manifest()
        profile = adapter.canonical_random_forest_profile(manifest)
        self.assertEqual(profile["engine"], "random_forest")
        cases = []
        unknown = copy.deepcopy(manifest)
        unknown["engine_params"]["criterion"] = _typed("string", "gini")
        cases.append(unknown)
        analyst_seed = copy.deepcopy(manifest)
        analyst_seed["engine_params"]["seed"] = _typed("int", 7)
        cases.append(analyst_seed)
        wrong_pin = copy.deepcopy(manifest)
        wrong_pin["privacy"]["mechanism_params"]["partition"]["value"] = \
            "bootstrap/private-indices"
        cases.append(wrong_pin)
        too_many_candidates = copy.deepcopy(manifest)
        too_many_candidates["engine_params"]["max_features"]["value"] = 4
        cases.append(too_many_candidates)
        for changed in cases:
            with self.subTest(case=len(cases)), self.assertRaises(ValueError):
                adapter.canonical_random_forest_profile(changed)

        too_large = _manifest(
            trees=512, depth=12, max_features=3, features=3)
        with self.assertRaisesRegex(ValueError, "transcript"):
            adapter.canonical_random_forest_profile(too_large)

    def test_public_preflight_rejects_before_private_copy(self):
        manifest = _manifest(trees=4, depth=2, max_features=2)
        manifest["resources"]["max_rows"] = 10
        X = np.broadcast_to(np.zeros((1, 3)), (11, 3))
        y = np.broadcast_to(np.zeros(1), (11,))
        with mock.patch.object(
                tree_data, "_numeric_array",
                side_effect=AssertionError("private copy reached")):
            with self.assertRaisesRegex(ValueError, "ceiling"):
                adapter.materialize_random_forest_units(manifest, X, y)

        memory_limited = _manifest(
            trees=8, depth=3, max_features=3, features=3)
        memory_limited["resources"]["memory_mib"] = 128
        X = np.broadcast_to(np.zeros((1, 3)), (1_000_000, 3))
        y = np.broadcast_to(np.zeros(1), (1_000_000,))
        with mock.patch.object(
                tree_data, "_numeric_array",
                side_effect=AssertionError("private copy reached")):
            with self.assertRaisesRegex(ValueError, "memory"):
                adapter.materialize_random_forest_units(
                    memory_limited, X, y)


class RandomForestTranscriptTests(unittest.TestCase):
    def setUp(self):
        self.X = np.asarray([
            [-1.7, -0.8, -0.2], [-0.8, -0.2, 0.2],
            [-0.2, 0.2, 0.8], [0.2, 0.8, 1.2],
            [0.8, 1.2, 1.7], [float("nan"), float("inf"), -float("inf")],
        ], dtype=np.float64)
        self.y = np.asarray([0, 0, 0, 1, 1, 0], dtype=np.float64)

    def test_replay_permutation_same_bins_and_resources_are_exact(self):
        manifest = _manifest(trees=8, depth=3, max_features=2)
        first = _train(manifest, self.X, self.y)
        order = np.asarray([5, 2, 0, 4, 1, 3])
        np.testing.assert_array_equal(
            first, _train(manifest, self.X[order], self.y[order]))

        equivalent = self.X.copy()
        equivalent[:5, 0] = [-1.8, -0.7, -0.1, 0.1, 0.7]
        equivalent[:5, 1] = [-0.9, -0.1, 0.1, 0.7, 1.1]
        equivalent[:5, 2] = [-0.1, 0.1, 0.7, 1.1, 1.6]
        np.testing.assert_array_equal(
            first, _train(manifest, equivalent, self.y))

        wider = copy.deepcopy(manifest)
        wider["resources"].update(
            threads=8, memory_mib=8192, wall_seconds=1800,
            max_rows=2_000_000, max_features=128)
        np.testing.assert_array_equal(
            first, _train(wider, self.X, self.y))

    def test_schema_names_ids_and_scope_are_not_reroll_axes(self):
        manifest = _manifest(trees=6, depth=2, max_features=2, unit="patient")
        X = np.asarray([
            [-1.4, -0.9, -0.4], [-1.2, -0.7, -0.2],
            [0.6, 0.8, 1.2], [0.8, 1.0, 1.4],
        ])
        y = np.asarray([0.0, 0.0, 1.0, 1.0])
        first = json.loads(_train(manifest, X, y, ["a", "a", "b", "b"]))

        nominal = copy.deepcopy(manifest)
        nominal["public_schema"]["features"] = ["u", "v", "w"]
        nominal["public_schema"]["target"]["name"] = "renamed"
        nominal["data_scope"]["snapshot_hash"] = "c" * 64
        nominal["data_scope"]["cohort_hash"] = "d" * 64
        _rehash_schema(nominal)
        replay = json.loads(_train(
            nominal, X, y, ["renamed-1", "renamed-1",
                            "renamed-2", "renamed-2"]))
        self.assertNotEqual(
            first["public_schema_sha256"], replay["public_schema_sha256"])
        self.assertEqual(first["trees"], replay["trees"])

    def test_duplicate_assignment_is_content_addressed_not_occurrence_rank(self):
        manifest = _manifest(trees=11, depth=2, max_features=2)
        materialized = adapter.materialize_random_forest_units(
            manifest,
            np.asarray([[-0.2, 0.2, 0.8]] * 4 + [[1.2, -1.2, 0.2]]),
            np.asarray([1.0] * 5))
        profile = adapter.canonical_random_forest_profile(manifest)
        with _key():
            _candidates, key = adapter._public_schedule(profile, 3)
            try:
                assignments = adapter._tree_assignments(
                    materialized._binned_features,
                    materialized._target_units, 11, key)
            finally:
                key[:] = b"\x00" * len(key)
        self.assertEqual(len(set(assignments[:4].tolist())), 1)
        order = np.asarray([4, 2, 0, 3, 1])
        with _key():
            _candidates, key = adapter._public_schedule(profile, 3)
            try:
                replay = adapter._tree_assignments(
                    materialized._binned_features[order],
                    materialized._target_units[order], 11, key)
            finally:
                key[:] = b"\x00" * len(key)
        np.testing.assert_array_equal(assignments[order], replay)

    def test_fixed_transcript_and_splits_use_released_histograms_only(self):
        manifest = _manifest(trees=3, depth=3, max_features=2)
        calls = []

        def zero_release(value, **kwargs):
            calls.append((np.asarray(value).shape, copy.deepcopy(kwargs["layout"])))
            sigma = (adapter.canonical_random_forest_profile(manifest)[
                "leaf_sigma"] if kwargs["layout"]["level"] == 3 else
                adapter.canonical_random_forest_profile(manifest)["split_sigma"])
            return np.zeros_like(value, dtype=np.float64), sigma

        with _key(), mock.patch.object(
                adapter.tree_release, "joint_gaussian_release",
                side_effect=zero_release):
            left = adapter.train_random_forest(
                adapter.prepare_random_forest_training(
                    manifest, self.X, self.y))
            right = adapter.train_random_forest(
                adapter.prepare_random_forest_training(
                    manifest, self.X, 1.0 - self.y))
        self.assertEqual(left, right)
        self.assertEqual(len(calls), 2 * (3 + 1))
        self.assertEqual(
            [layout["release_index"] for _shape, layout in calls[:4]],
            [0, 1, 2, 3])
        self.assertTrue(all(
            layout["transcript"] ==
            forest_accounting.RANDOM_FOREST_TRANSCRIPT_PROFILE
            for _shape, layout in calls))

    def test_changed_statistics_or_secret_changes_model_without_files(self):
        manifest = _manifest(trees=6, depth=2, max_features=2)
        with tempfile.TemporaryDirectory() as directory:
            before = os.listdir(directory)
            old = os.getcwd()
            try:
                os.chdir(directory)
                first = _train(manifest, self.X, self.y)
                changed = self.y.copy()
                changed[0] = 1.0
                self.assertNotEqual(first, _train(manifest, self.X, changed))
                with _key(b"z" * 32):
                    other = adapter.train_random_forest(
                        adapter.prepare_random_forest_training(
                            manifest, self.X, self.y))
                self.assertNotEqual(first, other)
            finally:
                os.chdir(old)
            self.assertEqual(before, os.listdir(directory))

    def test_empty_input_runs_the_same_public_transcript(self):
        manifest = _manifest(trees=3, depth=2, max_features=2)
        calls = []
        original = adapter.tree_release.joint_gaussian_release

        def observe(value, **kwargs):
            calls.append(kwargs["layout"]["release_index"])
            return original(value, **kwargs)

        with _key(), mock.patch.object(
                adapter.tree_release, "joint_gaussian_release",
                side_effect=observe):
            artifact = adapter.train_random_forest(
                adapter.prepare_random_forest_training(
                    manifest, np.empty((0, 3)), np.empty((0,))))
        self.assertEqual(calls, [0, 1, 2])
        model = json.loads(artifact)
        self.assertEqual(len(model["trees"]), 3)
        self.assertTrue(all(
            math.isfinite(value)
            for tree in model["trees"] for value in tree["leaf_values"]))

    def test_prepared_state_is_frozen_one_shot_and_cleared_on_failure(self):
        manifest = _manifest(trees=4, depth=2, max_features=2)
        with _key():
            prepared = adapter.prepare_random_forest_training(
                manifest, self.X, self.y)
            self.assertNotIn(str(len(self.X)), repr(prepared))
            with self.assertRaises(AttributeError):
                prepared._used = True
            adapter.train_random_forest(prepared)
            self.assertEqual(prepared._binned.size, 0)
            with self.assertRaisesRegex(ValueError, "prepared"):
                adapter.train_random_forest(prepared)

            broken = adapter.prepare_random_forest_training(
                manifest, self.X, self.y)
            broken._targets.setflags(write=True)
            broken._targets[0] = 1 - broken._targets[0]
            with self.assertRaisesRegex(ValueError, "prepared"):
                adapter.train_random_forest(broken)

            failed = adapter.prepare_random_forest_training(
                manifest, self.X, self.y)
            with mock.patch.object(
                    adapter.tree_release, "joint_gaussian_release",
                    side_effect=RuntimeError("release failed")):
                with self.assertRaisesRegex(RuntimeError, "release failed"):
                    adapter.train_random_forest(failed)
            self.assertEqual(failed._binned.size, 0)
            with self.assertRaisesRegex(ValueError, "prepared"):
                adapter.train_random_forest(failed)


class RandomForestArtifactTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest(trees=6, depth=2, max_features=2)
        self.X = np.asarray([
            [-1.0, 0.0, -0.5], [1.0, 0.0, 0.5],
            [0.1, 1.0, 1.5], [float("nan"), float("inf"), -float("inf")],
        ])
        self.y = np.asarray([0.0, 1.0, 1.0, 0.0])
        self.artifact = _train(self.manifest, self.X, self.y)

    def test_artifact_is_bounded_data_only_and_strictly_sanitized(self):
        sanitized, digest = adapter.sanitize_random_forest_artifact(
            self.manifest, self.artifact)
        self.assertEqual(sanitized, self.artifact)
        self.assertEqual(digest, hashlib.sha256(self.artifact).hexdigest())
        text = self.artifact.decode("ascii")
        for forbidden in (
                "outcome", '"x0"', "count", "sigma", "epsilon",
                "candidate", "histogram", "history", "path", "log"):
            self.assertNotIn(forbidden, text)

        profile = adapter.canonical_random_forest_profile(self.manifest)
        arguments = adapter._sanitizer_arguments(self.manifest, profile)
        root = json.loads(self.artifact)
        bad = copy.deepcopy(root)
        bad["debug"] = "private"
        with self.assertRaises(ValueError):
            sanitize_forest_json(
                json.dumps(bad, sort_keys=True,
                           separators=(",", ":")).encode("ascii"),
                **arguments)
        bad = copy.deepcopy(root)
        bad["trees"][0]["cut_indices"][0] = 999
        with self.assertRaises(ValueError):
            sanitize_forest_json(
                json.dumps(bad, sort_keys=True,
                           separators=(",", ":")).encode("ascii"),
                **arguments)

    def test_ensemble_predictor_is_finite_and_rechecks_every_member(self):
        ensemble, digest = adapter.build_random_forest_ensemble(
            self.manifest, [self.artifact, self.artifact])
        self.assertEqual(digest, hashlib.sha256(ensemble).hexdigest())
        parsed = predictor.parse_forest_ensemble(ensemble, self.manifest)
        self.assertEqual(parsed.task, "binary")
        self.assertEqual(parsed.num_models, 2)
        values = parsed.predict(self.X)
        self.assertTrue(all(
            math.isfinite(value) and 0.0 <= value <= 1.0
            for value in values))

        container = json.loads(ensemble)
        container["models"][0]["trees"][0]["leaf_values"][0] = 2.0
        with self.assertRaises(ValueError):
            predictor.parse_forest_ensemble(
                json.dumps(container, sort_keys=True,
                           separators=(",", ":")).encode("ascii"),
                self.manifest)

    def test_stdlib_predictor_parses_both_forest_engines_without_numpy(self):
        random_ensemble = adapter.build_random_forest_ensemble(
            self.manifest, [self.artifact])[0]
        extra_manifest = copy.deepcopy(self.manifest)
        extra_manifest["engine"] = "extra_trees"
        del extra_manifest["engine_params"]["max_features"]
        extra_manifest["privacy"]["mechanism_params"] = {
            "leaf_release": _typed(
                "string", forest_accounting.LEAF_RELEASE_PROFILE),
            "topology": _typed(
                "string", forest_accounting.TOPOLOGY_PROFILE),
        }
        extra_member = json.loads(self.artifact)
        extra_member["engine"] = "extra_trees"
        extra_ensemble = json.dumps({
            "aggregation": "mean_prediction",
            "contract": adapter.ENSEMBLE_CONTRACT,
            "engine": "extra_trees",
            "models": [extra_member],
            "public_schema_sha256":
                extra_manifest["public_schema"]["sha256"],
            "task": "binary",
            "version": 1,
        }, ensure_ascii=True, allow_nan=False, sort_keys=True,
            separators=(",", ":")).encode("ascii")
        code = """
import builtins, sys
sys.path.insert(0, %r)
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'numpy' or name.startswith('numpy.'):
        raise ImportError('blocked training dependency')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from dsflower_runner import forest_predictor
assert forest_predictor.ENSEMBLE_CONTRACT == 'dsflower-forest-ensemble-v1'
rf = forest_predictor.parse_forest_ensemble(%r, %r)
et = forest_predictor.parse_forest_ensemble(%r, %r)
assert rf.task == et.task == 'binary'
assert len(rf.predict([[-1.0, 0.0, 0.5]])) == 1
assert len(et.predict([[-1.0, 0.0, 0.5]])) == 1
""" % (os.path.abspath(FLOWER_APP), random_ensemble, self.manifest,
       extra_ensemble, extra_manifest)
        completed = subprocess.run(
            [sys.executable, "-I", "-S", "-c", code],
            check=False, capture_output=True, text=True, timeout=20)
        self.assertEqual(
            completed.returncode, 0,
            msg=completed.stdout + completed.stderr)


class RandomForestUtilityAndCostTests(unittest.TestCase):
    def test_binary_and_regression_have_plausible_signal(self):
        rng = np.random.default_rng(20260811)
        X = rng.uniform(-2.0, 2.0, size=(10_000, 8))
        held_out = rng.uniform(-2.0, 2.0, size=(3000, 8))
        binary_y = (X[:, 0] + 0.35 * X[:, 1] > 0.0).astype(np.float64)
        held_out_binary = (
            held_out[:, 0] + 0.35 * held_out[:, 1] > 0.0).astype(np.float64)
        binary_manifest = _manifest(
            trees=8, depth=4, max_features=3, features=8, epsilon=3.0)
        binary_model = adapter.build_random_forest_ensemble(
            binary_manifest,
            [_train(binary_manifest, X, binary_y)])[0]
        binary_prediction = np.asarray(predictor.predict_forest_ensemble(
            binary_model, binary_manifest, held_out))
        accuracy = float(np.mean(
            (binary_prediction >= 0.5) == held_out_binary))
        self.assertGreater(accuracy, 0.78)

        regression_y = np.clip(
            0.9 * X[:, 0] - 0.25 * X[:, 1], -2.0, 2.0)
        held_out_regression = np.clip(
            0.9 * held_out[:, 0] - 0.25 * held_out[:, 1], -2.0, 2.0)
        regression_manifest = _manifest(
            "regression", trees=4, depth=4, max_features=3,
            features=8, epsilon=3.0)
        regression_model = adapter.build_random_forest_ensemble(
            regression_manifest,
            [_train(regression_manifest, X, regression_y)])[0]
        regression_prediction = np.asarray(predictor.predict_forest_ensemble(
            regression_model, regression_manifest, held_out))
        rmse = float(np.sqrt(np.mean(
            (regression_prediction - held_out_regression) ** 2)))
        baseline = float(np.sqrt(np.mean(
            (held_out_regression - np.mean(held_out_regression)) ** 2)))
        self.assertLess(rmse, 0.75 * baseline)

    def test_training_and_dependency_light_prediction_are_bounded_in_time(self):
        rng = np.random.default_rng(17)
        X = rng.uniform(-2.0, 2.0, size=(100_000, 8))
        y = (X[:, 0] - X[:, 1] > 0.0).astype(np.float64)
        manifest = _manifest(
            trees=16, depth=4, max_features=3, features=8,
            epsilon=100.0)
        started = time.monotonic()
        artifact = _train(manifest, X, y)
        training_seconds = time.monotonic() - started
        ensemble = adapter.build_random_forest_ensemble(
            manifest, [artifact])[0]
        started = time.monotonic()
        values = predictor.predict_forest_ensemble(
            ensemble, manifest, X[:2000])
        prediction_seconds = time.monotonic() - started
        self.assertEqual(len(values), 2000)
        self.assertLess(training_seconds, 30.0)
        self.assertLess(prediction_seconds, 5.0)


if __name__ == "__main__":
    unittest.main()
