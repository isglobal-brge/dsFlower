"""Security, determinism and utility tests for private ExtraTrees."""

import copy
import hashlib
import json
import math
import os
import sys
import time
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import forest_accounting
from dsflower_runner import forest_adapter as adapter
from dsflower_runner import forest_predictor as predictor
from dsflower_runner import native_tree_request
from dsflower_runner import validation
from dsflower_runner.forest_sanitizer import sanitize_forest_json


def _typed(kind, value):
    return {"type": kind, "value": value}


def _schema(task="binary_classification", features=2):
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
        "cuts": [[-1.0, -0.5, 0.0, 0.5, 1.0] for _ in names],
        "target": target,
    }
    raw = json.dumps(
        core, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    return dict(core, sha256=hashlib.sha256(raw).hexdigest())


def _manifest(task="binary_classification", *, trees=16, depth=4,
              unit="row", features=2, epsilon=3.0):
    schema = _schema(task, features)
    return {
        "contract_version": 1,
        "mode": "native-tight",
        "engine": "extra_trees",
        "task": task,
        "public_schema": schema,
        "engine_params": {
            "max_depth": _typed("int", depth),
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
                "leaf_release": _typed(
                    "string", forest_accounting.LEAF_RELEASE_PROFILE),
                "topology": _typed(
                    "string", forest_accounting.TOPOLOGY_PROFILE),
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


def _key():
    return mock.patch(
        "dsflower_runner.seeding._node_secret",
        return_value=bytes(range(32)))


def _public_request(task="binary", *, trees=16, depth=4):
    manifest = _manifest(
        "binary_classification" if task == "binary" else "regression",
        trees=trees, depth=depth)
    schema = manifest["public_schema"]
    return {
        "contract": native_tree_request.REQUEST_CONTRACT,
        "engine": "extra_trees",
        "mode": "native-tight",
        "parameters": [
            {"name": "max_depth", "type": "integer", "value": depth},
            {"name": "n_estimators", "type": "integer", "value": trees},
        ],
        "public_schema": schema,
        "resources": {
            "max_features": 2, "max_trees": trees, "max_depth": depth,
            "max_bins": 6, "max_threads": 4, "memory_mb": 4096,
            "timeout_seconds": 900,
        },
        "task": task,
    }


def _train(manifest, X, y, unit_ids=None):
    with _key():
        prepared = adapter.prepare_extra_trees_training(
            manifest, X, y, unit_ids=unit_ids)
        return adapter.train_extra_trees(prepared)


class ExtraTreesAccountingTests(unittest.TestCase):
    def test_public_request_bridge_injects_exact_forest_mechanisms(self):
        request = _public_request()
        manifest = native_tree_request.backend_manifest(
            request, epsilon=2.0, delta=1.0e-6, unit="row",
            unit_canonicalization="trim-utf8-v2", gradient_clip=999.0,
            snapshot_hash="a" * 64, cohort_hash="b" * 64)
        self.assertEqual(manifest["engine"], "extra_trees")
        self.assertEqual(manifest["privacy"]["mechanism"], "dp-forest-v1")
        self.assertEqual(frozenset(manifest["privacy"]["mechanism_params"]),
                         frozenset(("leaf_release", "topology")))
        self.assertNotIn("base_score", manifest["engine_params"])
        self.assertNotIn("max_bin", manifest["engine_params"])
        self.assertEqual(
            adapter.canonical_extra_trees_profile(manifest)["n_estimators"],
            16)

        random_request = copy.deepcopy(request)
        random_request["engine"] = "random_forest"
        random_request["parameters"].append(
            {"name": "max_features", "type": "integer", "value": 1})
        random_manifest = native_tree_request.backend_manifest(
            random_request, epsilon=2.0, delta=1.0e-6, unit="row",
            unit_canonicalization="trim-utf8-v2", gradient_clip=1.0,
            snapshot_hash="a" * 64, cohort_hash="b" * 64)
        self.assertEqual(random_manifest["engine"], "random_forest")
        self.assertEqual(
            frozenset(random_manifest["privacy"]["mechanism_params"]),
            frozenset(("candidate_schedule", "histogram_release",
                       "leaf_release", "partition", "transcript")))

    def test_joint_vector_sensitivity_and_sigma_are_pinned(self):
        self.assertEqual(
            forest_accounting.leaf_vector_sensitivity(
                "binary_classification", 8), 4.0)
        self.assertEqual(
            forest_accounting.leaf_vector_sensitivity("regression", 9), 6.0)
        profile = adapter.canonical_extra_trees_profile(
            _manifest(trees=8, depth=3))
        self.assertEqual(profile["num_releases"], 1)
        self.assertEqual(profile["sensitivity"], 4.0)
        self.assertGreater(profile["sigma"], 0.0)
        self.assertEqual(
            profile["release_coordinates"], 2 * 8 * (1 << 3))

    def test_profile_is_exact_and_adapter_is_engine_specific(self):
        manifest = _manifest()
        profile = adapter.canonical_extra_trees_profile(manifest)
        self.assertEqual(profile["engine"], "extra_trees")
        for section, name, value in (
                ("engine_params", "seed", _typed("int", 7)),
                ("engine_params", "criterion", _typed("string", "gini")),
                ("privacy", "topology", _typed("string", "private-best-split"))):
            changed = copy.deepcopy(manifest)
            if section == "privacy":
                changed[section]["mechanism_params"][name] = value
            else:
                changed[section][name] = value
            with self.subTest(name=name), self.assertRaises(ValueError):
                adapter.canonical_extra_trees_profile(changed)
        forest = copy.deepcopy(manifest)
        forest["engine"] = "random_forest"
        with self.assertRaisesRegex(ValueError, "extra_trees"):
            adapter.canonical_extra_trees_profile(forest)

    def test_algorithm_and_physical_caps_reject_without_truncation(self):
        for trees, depth in ((513, 2), (2, 13)):
            manifest = _manifest(trees=trees, depth=depth)
            manifest["resources"]["max_trees"] = 10_000
            manifest["resources"]["max_depth"] = 32
            with self.subTest(trees=trees, depth=depth), self.assertRaises(ValueError):
                adapter.canonical_extra_trees_profile(manifest)
        manifest = _manifest(trees=2, depth=2)
        manifest["resources"]["max_rows"] = 2
        X = np.zeros((3, 2), dtype=np.float64)
        with self.assertRaisesRegex(ValueError, "ceiling"):
            adapter.materialize_forest_units(manifest, X, np.zeros(3))


class ExtraTreesStickyTests(unittest.TestCase):
    def test_empty_input_is_replayable_and_predictable(self):
        manifest = _manifest(trees=3, depth=2, features=2)
        features = np.empty((0, 2), dtype=np.float64)
        target = np.empty((0,), dtype=np.float64)
        first = _train(manifest, features, target)
        second = _train(manifest, features, target)
        self.assertEqual(first, second)
        ensemble, _digest = adapter.build_extra_trees_ensemble(
            manifest, [first])
        model = predictor.parse_forest_ensemble(ensemble, manifest)
        predictions = model.predict([[0.0, 0.0]])
        self.assertEqual(len(predictions), 1)
        self.assertTrue(math.isfinite(predictions[0]))

    def setUp(self):
        self.X = np.asarray([
            [-1.7, -0.8], [-0.2, 0.2], [0.2, -0.1], [1.7, 0.8],
            [float("nan"), float("inf")],
        ], dtype=np.float64)
        self.y = np.asarray([0, 0, 1, 1, 0], dtype=np.float64)

    def test_replay_permutation_same_bins_and_resources_are_exact(self):
        manifest = _manifest(trees=8, depth=3)
        first = _train(manifest, self.X, self.y)
        order = np.asarray([4, 2, 0, 3, 1])
        replay = _train(manifest, self.X[order], self.y[order])
        np.testing.assert_array_equal(first, replay)

        equivalent = self.X.copy()
        equivalent[:4, 0] = [-1.9, -0.1, 0.1, 1.9]
        same_bins = _train(manifest, equivalent, self.y)
        np.testing.assert_array_equal(first, same_bins)

        wider = copy.deepcopy(manifest)
        wider["resources"].update(
            threads=8, memory_mib=8192, wall_seconds=1800,
            max_rows=2_000_000, max_features=128)
        np.testing.assert_array_equal(first, _train(wider, self.X, self.y))

        nominal = copy.deepcopy(manifest)
        nominal["public_schema"]["features"] = ["renamed_a", "renamed_b"]
        nominal["public_schema"]["lower"] = [-3.0, -3.0]
        nominal["public_schema"]["upper"] = [3.0, 3.0]
        nominal["public_schema"]["cuts"] = [
            [-1.2, -0.6, 0.1, 0.6, 1.2],
            [-1.2, -0.6, 0.1, 0.6, 1.2],
        ]
        nominal["public_schema"]["target"]["name"] = "renamed_outcome"
        nominal["public_schema"]["target"]["levels"] = [
            {"type": "string", "value": "negative"},
            {"type": "string", "value": "positive"},
        ]
        _rehash_schema(nominal)
        nominal_model = json.loads(_train(nominal, self.X, self.y))
        original_model = json.loads(first)
        self.assertNotEqual(
            nominal_model["public_schema_sha256"],
            original_model["public_schema_sha256"])
        self.assertEqual(nominal_model["trees"], original_model["trees"])

    def test_patient_materialization_is_order_invariant_and_one_record_per_unit(self):
        manifest = _manifest(trees=6, depth=2, unit="patient")
        X = np.asarray([
            [-1.0, 0.0], [-0.5, 0.5], [1.0, 0.0], [1.5, -0.5],
        ])
        y = np.asarray([0.0, 1.0, 1.0, 1.0])
        units = ["a", "a", "b", "b"]
        first = _train(manifest, X, y, units)
        order = np.asarray([3, 1, 2, 0])
        replay = _train(
            manifest, X[order], y[order], [units[index] for index in order])
        np.testing.assert_array_equal(first, replay)
        materialized = adapter.materialize_forest_units(
            manifest, X, y, unit_ids=units)
        self.assertEqual(materialized.target.shape, (2,))
        self.assertFalse(materialized.target.flags.writeable)
        self.assertNotIn("rows", repr(materialized))

    def test_changed_sufficient_vector_or_secret_changes_release(self):
        manifest = _manifest(trees=8, depth=3)
        first = _train(manifest, self.X, self.y)
        changed_y = self.y.copy()
        changed_y[0] = 1.0
        self.assertNotEqual(first, _train(manifest, self.X, changed_y))
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=b"z" * 32):
            other = adapter.train_extra_trees(
                adapter.prepare_extra_trees_training(
                    manifest, self.X, self.y))
        self.assertNotEqual(first, other)

    def test_prepared_release_is_one_shot(self):
        with _key():
            prepared = adapter.prepare_extra_trees_training(
                _manifest(trees=2, depth=2), self.X, self.y)
            with self.assertRaises(AttributeError):
                prepared._used = True
            adapter.train_extra_trees(prepared)
            with self.assertRaisesRegex(ValueError, "prepared"):
                adapter.train_extra_trees(prepared)

    def test_prepared_release_revalidates_every_mutable_component(self):
        manifest = _manifest(trees=2, depth=2)
        mutations = (
            ("profile", lambda value: value._profile.__setitem__(
                "max_depth", 1)),
            ("canonical", lambda value: value._canonical[
                "resources"].__setitem__("max_rows", 9)),
            ("topology", lambda value: value._topology[0][
                "features"].__setitem__(
                    0, 1 - value._topology[0]["features"][0])),
            ("statistics", lambda value: (
                value._stats.setflags(write=True),
                value._stats.__setitem__((0, 0, 0), 999.0),
            )),
        )
        with _key():
            for name, mutate in mutations:
                prepared = adapter.prepare_extra_trees_training(
                    manifest, self.X, self.y)
                mutate(prepared)
                with self.subTest(mutation=name), self.assertRaisesRegex(
                        ValueError, "prepared"):
                    adapter.train_extra_trees(prepared)

    def test_prepared_type_cannot_be_constructed_without_factory_token(self):
        with self.assertRaisesRegex(TypeError, "prepare_extra_trees_training"):
            adapter.PreparedExtraTreesTraining(None, {}, {}, [], np.zeros(1))


class ExtraTreesArtifactTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest(trees=4, depth=2)
        X = np.asarray([[-1.0, 0.0], [1.0, 0.0], [0.1, 1.0]])
        y = np.asarray([0.0, 1.0, 1.0])
        self.artifact = _train(self.manifest, X, y)

    def test_artifact_is_canonical_bounded_and_contains_no_metadata_channels(self):
        sanitized, digest = adapter.sanitize_extra_trees_artifact(
            self.manifest, self.artifact)
        self.assertEqual(sanitized, self.artifact)
        self.assertEqual(digest, hashlib.sha256(self.artifact).hexdigest())
        text = self.artifact.decode("ascii")
        for forbidden in ("outcome", '"x0"', "count", "sigma", "epsilon",
                          "history", "path", "log"):
            self.assertNotIn(forbidden, text)

    def test_sanitizer_rejects_unknown_fields_bad_topology_and_leaf_values(self):
        profile = adapter.canonical_extra_trees_profile(self.manifest)
        args = adapter._sanitizer_arguments(self.manifest, profile)
        root = json.loads(self.artifact)
        cases = []
        unknown = copy.deepcopy(root)
        unknown["debug"] = "private"
        cases.append(unknown)
        bad_cut = copy.deepcopy(root)
        bad_cut["trees"][0]["cut_indices"][0] = 999
        cases.append(bad_cut)
        bad_leaf = copy.deepcopy(root)
        bad_leaf["trees"][0]["leaf_values"][0] = float("nan")
        cases.append(bad_leaf)
        bad_direction = copy.deepcopy(root)
        bad_direction["trees"][0]["default_left"][0] = 1
        cases.append(bad_direction)
        for value in cases:
            with self.subTest(case=len(cases)), self.assertRaises(ValueError):
                sanitize_forest_json(
                    json.dumps(value, allow_nan=True).encode("ascii"), **args)

    def test_ensemble_predictor_rechecks_every_member(self):
        ensemble, digest = adapter.build_extra_trees_ensemble(
            self.manifest, [self.artifact, self.artifact])
        self.assertEqual(digest, hashlib.sha256(ensemble).hexdigest())
        parsed = predictor.parse_forest_ensemble(ensemble, self.manifest)
        self.assertEqual(parsed.task, "binary")
        self.assertEqual(parsed.num_models, 2)
        values = parsed.predict([
            [-1.0, 0.0], [1.0, 0.0], [float("nan"), float("inf")]])
        self.assertEqual(len(values), 3)
        self.assertTrue(all(math.isfinite(value) and 0 <= value <= 1
                            for value in values))
        with self.assertRaisesRegex(ValueError, "not canonical"):
            predictor.parse_forest_ensemble(
                json.dumps(json.loads(ensemble), indent=2).encode("ascii"),
                self.manifest)

        container = json.loads(ensemble)
        container["models"][0]["trees"][0]["leaf_values"][0] = 2.0
        with self.assertRaises(ValueError):
            predictor.parse_forest_ensemble(
                json.dumps(container, sort_keys=True,
                           separators=(",", ":")).encode("ascii"),
                self.manifest)

    def test_predictions_feed_the_common_private_validation_vector(self):
        ensemble = adapter.build_extra_trees_ensemble(
            self.manifest, [self.artifact])[0]
        rows = [[-1.0, 0.0], [1.0, 0.0], [0.1, 1.0]]
        predictions = predictor.predict_forest_ensemble(
            ensemble, self.manifest, rows)
        layout = validation.validation_layout(
            "classification", n_classes=2, bins=16)
        with _key():
            first, sigma = validation.private_validation_vector(
                np.asarray([0.0, 1.0, 1.0]), np.asarray(predictions), layout,
                epsilon=1.0, delta=1.0e-6)
            replay, replay_sigma = validation.private_validation_vector(
                np.asarray([0.0, 1.0, 1.0]), np.asarray(predictions), layout,
                epsilon=1.0, delta=1.0e-6)
        np.testing.assert_array_equal(first, replay)
        self.assertEqual(sigma, replay_sigma)
        self.assertEqual(first.shape, (32,))

    def test_regression_predictions_are_bounded_and_finite(self):
        manifest = _manifest("regression", trees=8, depth=3)
        X = np.linspace(-2.0, 2.0, 100).reshape(50, 2)
        y = np.clip(X[:, 0], -2.0, 2.0)
        artifact = _train(manifest, X, y)
        ensemble = adapter.build_extra_trees_ensemble(
            manifest, [artifact])[0]
        values = predictor.predict_forest_ensemble(
            ensemble, manifest,
            [[-2.0, -2.0], [0.0, 0.0], [2.0, 2.0], [float("nan"), 0.0]])
        self.assertTrue(all(math.isfinite(value) and -2 <= value <= 2
                            for value in values))


class ExtraTreesPlausibilityTests(unittest.TestCase):
    def test_binary_signal_and_throughput_are_plausible(self):
        rows = 20_000
        rng = np.random.default_rng(20260810)
        X = rng.uniform(-2.0, 2.0, size=(rows, 2))
        y = (X[:, 0] > 0.0).astype(np.float64)
        manifest = _manifest(trees=32, depth=4, epsilon=3.0)
        started = time.monotonic()
        artifact = _train(manifest, X, y)
        elapsed = time.monotonic() - started
        ensemble = adapter.build_extra_trees_ensemble(
            manifest, [artifact])[0]
        model = predictor.parse_forest_ensemble(ensemble, manifest)
        sample = X[:2000]
        predicted = np.asarray(model.predict(sample)) >= 0.5
        accuracy = float(np.mean(predicted == (y[:2000] >= 0.5)))
        throughput = rows / max(elapsed, 1.0e-9)
        self.assertGreaterEqual(accuracy, 0.75)
        self.assertGreaterEqual(throughput, 5_000.0)


if __name__ == "__main__":
    unittest.main()
