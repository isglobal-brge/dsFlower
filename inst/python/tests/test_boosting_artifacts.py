"""Fail-closed tests for safe LightGBM and CatBoost projections."""

import copy
import hashlib
import json
import math
import os
import sys
import unittest


FLOWER_APP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import boosting_profile  # noqa: E402
from dsflower_runner import catboost_artifact  # noqa: E402
from dsflower_runner import lightgbm_artifact  # noqa: E402
from dsflower_runner import native_tree_contract  # noqa: E402


def _typed(kind, value):
    return {"type": kind, "value": value}


def _schema(task="binary_classification"):
    target = (
        {
            "name": "outcome", "kind": "binary",
            "levels": [
                {"type": "string", "value": "control"},
                {"type": "string", "value": "case"},
            ],
            "lower": 0.0, "upper": 1.0,
        }
        if task == "binary_classification"
        else {
            "name": "outcome", "kind": "continuous", "levels": None,
            "lower": -4.0, "upper": 6.0,
        }
    )
    core = {
        "version": 1,
        "features": ["marker", "age"],
        "lower": [-5.0, 0.0],
        "upper": [5.0, 100.0],
        "cuts": [[-1.0, 0.0, 1.0], [20.0, 50.0, 80.0]],
        "target": target,
    }
    wire = json.dumps(
        core, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return dict(core, sha256=hashlib.sha256(wire).hexdigest())


def _manifest(engine="lightgbm", task="binary_classification"):
    schema = _schema(task)
    base_score = 0.0 if task == "binary_classification" else 1.0
    if engine == "lightgbm":
        parameters = {
            "base_score": _typed("float", base_score),
            "lambda_l1": _typed("float", 0.0),
            "lambda_l2": _typed("float", 1.0),
            "learning_rate": _typed("float", 0.1),
            "max_bin": _typed("int", 4),
            "max_delta_step": _typed("float", 2.0),
            "max_depth": _typed("int", 2),
            "min_data_in_leaf": _typed("int", 1),
            "min_gain_to_split": _typed("float", 0.0),
            "num_leaves": _typed("int", 2),
            "num_iterations": _typed("int", 1),
        }
    else:
        parameters = {
            "base_score": _typed("float", base_score),
            "border_count": _typed("int", 3),
            "depth": _typed("int", 2),
            "iterations": _typed("int", 1),
            "l2_leaf_reg": _typed("float", 1.0),
            "learning_rate": _typed("float", 0.1),
            "max_delta_step": _typed("float", 2.0),
        }
    return {
        "contract_version": 1,
        "mode": "native-tight",
        "engine": engine,
        "task": task,
        "public_schema": schema,
        "engine_params": parameters,
        "privacy": {
            "mechanism": "dp-histogram-v1",
            "epsilon": 1.0,
            "delta": 1e-6,
            "unit": "row",
            "adjacency": "replace_one",
            "unit_canonicalization": "trim-utf8-v2",
            "contribution_strategy": "one-record-per-unit-v1",
            "max_rows_per_unit": 1,
            "mechanism_params": {
                "gradient_clip": _typed("float", 1.0),
                "hessian_clip": _typed("float", 1.0),
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
            "max_features": 128,
            "max_trees": 100,
            "max_depth": 16,
            "max_bins": 256,
            "max_artifact_bytes": 4 * 1024 * 1024,
        },
    }


def _public_request(engine="lightgbm", task="binary_classification"):
    manifest = _manifest(engine, task)
    omitted = ({"base_score", "max_bin"} if engine == "lightgbm"
               else {"base_score", "border_count"})
    type_map = {"float": "number", "int": "integer"}
    parameters = [{
        "name": name,
        "type": type_map[record["type"]],
        "value": record["value"],
    } for name, record in sorted(manifest["engine_params"].items())
        if name not in omitted]
    resources = manifest["resources"]
    return {
        "contract": native_tree_contract.REQUEST_CONTRACT
        if hasattr(native_tree_contract, "REQUEST_CONTRACT") else
        "dsflower-native-tree-request-v1",
        "engine": engine,
        "mode": "native-tight",
        "parameters": parameters,
        "public_schema": manifest["public_schema"],
        "resources": {
            "max_features": resources["max_features"],
            "max_trees": resources["max_trees"],
            "max_depth": resources["max_depth"],
            "max_bins": resources["max_bins"],
            "max_threads": resources["threads"],
            "memory_mb": resources["memory_mib"],
            "timeout_seconds": resources["wall_seconds"],
        },
        "task": "binary" if task == "binary_classification" else "regression",
    }


def _lightgbm_model(task="binary_classification"):
    return {
        "base_score": 0.0 if task == "binary_classification" else 1.0,
        "contract": "dsflower-lightgbm-safe-model-v1",
        "engine": "lightgbm",
        "public_schema_sha256": _schema(task)["sha256"],
        "task": "binary" if task == "binary_classification" else "regression",
        "trees": [{
            "default_left": [True, False, False],
            "leaf_values": [0.0, -0.1, 0.1],
            "left_children": [1, -1, -1],
            "right_children": [2, -1, -1],
            "split_cut_indices": [0, 0, 0],
            "split_indices": [0, 0, 0],
        }],
        "version": 1,
    }


def _catboost_model(task="binary_classification"):
    return {
        "base_score": 0.0 if task == "binary_classification" else 1.0,
        "contract": "dsflower-catboost-safe-model-v1",
        "engine": "catboost",
        "public_schema_sha256": _schema(task)["sha256"],
        "task": "binary" if task == "binary_classification" else "regression",
        "trees": [{
            "leaf_values": [-0.15, -0.05, 0.05, 0.15],
            "splits": [
                {"cut_index": 0, "default_left": True, "feature_index": 0},
                {"cut_index": 1, "default_left": False, "feature_index": 1},
            ],
        }],
        "version": 1,
    }


def _json(value, canonical=False):
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False,
        sort_keys=canonical, separators=(",", ":"),
    ).encode("ascii")


class ProfileTests(unittest.TestCase):
    def test_public_requests_enrich_exact_boosting_profiles(self):
        from dsflower_runner import native_tree_request
        for engine, profile in (
                ("lightgbm", boosting_profile.lightgbm_profile),
                ("catboost", boosting_profile.catboost_profile)):
            manifest = native_tree_request.backend_manifest(
                _public_request(engine), epsilon=2.0, delta=1.0e-6,
                unit="row", unit_canonicalization="trim-utf8-v2",
                gradient_clip=1.0, snapshot_hash="a" * 64,
                cohort_hash="b" * 64)
            with self.subTest(engine=engine):
                self.assertEqual(manifest["engine"], engine)
                self.assertEqual(manifest["privacy"]["mechanism"],
                                 "dp-histogram-v1")
                self.assertEqual(manifest["engine_params"]["base_score"]["value"],
                                 0.0)
                self.assertEqual(profile(manifest)["engine"], engine)

    def test_exact_lightgbm_and_catboost_profiles(self):
        lightgbm = boosting_profile.lightgbm_profile(_manifest("lightgbm"))
        self.assertEqual(lightgbm["trees"], 1)
        self.assertEqual(lightgbm["max_depth"], 2)
        self.assertEqual(lightgbm["base_score"], 0.0)
        self.assertGreater(lightgbm["leaf_abs_cap"], 0.2)

        catboost = boosting_profile.catboost_profile(_manifest("catboost"))
        self.assertEqual(catboost["trees"], 1)
        self.assertEqual(catboost["max_depth"], 2)
        self.assertEqual(catboost["base_score"], 0.0)
        self.assertGreater(catboost["leaf_abs_cap"], 0.2)

    def test_profiles_reject_unknown_missing_wrong_and_out_of_range_fields(self):
        for engine, function, depth_name in (
                ("lightgbm", boosting_profile.lightgbm_profile, "max_depth"),
                ("catboost", boosting_profile.catboost_profile, "depth")):
            with self.subTest(engine=engine):
                manifest = _manifest(engine)
                manifest["engine_params"]["verbose"] = _typed("int", 1)
                with self.assertRaises(ValueError):
                    function(manifest)

                manifest = _manifest(engine)
                del manifest["engine_params"]["learning_rate"]
                with self.assertRaisesRegex(ValueError, "exact"):
                    function(manifest)

                manifest = _manifest(engine)
                manifest["engine_params"][depth_name] = _typed("float", 2.0)
                with self.assertRaisesRegex(ValueError, "type"):
                    function(manifest)

                manifest = _manifest(engine)
                manifest["engine_params"]["learning_rate"]["value"] = 0.0
                with self.assertRaisesRegex(ValueError, "range"):
                    function(manifest)

    def test_profiles_bind_public_bins_task_base_and_mechanism(self):
        lightgbm = _manifest("lightgbm")
        lightgbm["engine_params"]["max_bin"]["value"] = 5
        with self.assertRaisesRegex(ValueError, "cut geometry"):
            boosting_profile.lightgbm_profile(lightgbm)

        catboost = _manifest("catboost")
        catboost["engine_params"]["border_count"]["value"] = 4
        with self.assertRaisesRegex(ValueError, "cut geometry"):
            boosting_profile.catboost_profile(catboost)

        regression = _manifest("lightgbm", "regression")
        regression["engine_params"]["base_score"]["value"] = 0.0
        with self.assertRaisesRegex(ValueError, "base_score"):
            boosting_profile.lightgbm_profile(regression)

        mechanism = _manifest("catboost")
        mechanism["privacy"]["mechanism_params"]["extra"] = _typed(
            "float", 1.0)
        with self.assertRaisesRegex(ValueError, "mechanism"):
            boosting_profile.catboost_profile(mechanism)

        collapsed = _manifest("catboost")
        collapsed["public_schema"]["cuts"][0][1] = \
            collapsed["public_schema"]["cuts"][0][0] + 1e-10
        core = {
            key: collapsed["public_schema"][key]
            for key in ("version", "features", "lower", "upper", "cuts",
                        "target")
        }
        collapsed["public_schema"]["sha256"] = hashlib.sha256(
            json.dumps(
                core, ensure_ascii=False, allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        collapsed["data_scope"]["schema_hash"] = \
            collapsed["public_schema"]["sha256"]
        with self.assertRaisesRegex(ValueError, "float32"):
            boosting_profile.catboost_profile(collapsed)

    def test_native_training_gate_is_explicitly_closed(self):
        for engine, payload in (
                ("lightgbm", b"tree\nversion=v4\n"),
                ("catboost", b"CBM1untrusted")):
            with self.subTest(engine=engine):
                self.assertFalse(
                    boosting_profile.training_capability(engine)["available"])
                with self.assertRaisesRegex(ValueError, "verified native"):
                    boosting_profile.reject_unverified_native_artifact(
                        engine, payload)

    def test_common_result_contract_rejects_native_prefixes_and_accepts_only_safe_projection(self):
        for engine, module, model, native_prefix in (
                ("lightgbm", lightgbm_artifact, _lightgbm_model,
                 b"tree\nversion=v4\n"),
                ("catboost", catboost_artifact, _catboost_model,
                 b"CBM1untrusted")):
            manifest = _manifest(engine)
            safe, _ = module.sanitize_model(_json(model()), manifest)
            invocation = native_tree_contract.invocation_identity(manifest)

            def result(payload):
                return {
                    "contract_version": 1,
                    "status": "ok",
                    "invocation_id": invocation,
                    "engine": engine,
                    "mode": "native-tight",
                    "artifact": {
                        "kind": "model",
                        "format": native_tree_contract.expected_artifact_format(
                            manifest, "model"),
                        "size_bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    },
                    "sanitization": (
                        native_tree_contract.artifact_sanitization_metadata(
                            manifest, "model")),
                }

            with self.subTest(engine=engine):
                validated = native_tree_contract.validate_backend_result(
                    result(safe), manifest, artifact_bytes=safe)
                self.assertEqual(validated["artifact"]["size_bytes"], len(safe))
                with self.assertRaises(ValueError):
                    native_tree_contract.validate_backend_result(
                        result(native_prefix), manifest,
                        artifact_bytes=native_prefix)


class LightGBMArtifactTests(unittest.TestCase):
    def test_projection_is_sanitized_canonical_and_predictable(self):
        manifest = _manifest("lightgbm")
        sanitized, digest = lightgbm_artifact.sanitize_model(
            _json(_lightgbm_model()), manifest)
        self.assertEqual(sanitized, _json(json.loads(sanitized), canonical=True))
        self.assertEqual(digest, hashlib.sha256(sanitized).hexdigest())
        self.assertNotIn(b"outcome", sanitized)
        self.assertNotIn(b"marker", sanitized)

        ensemble = lightgbm_artifact.build_ensemble([sanitized], manifest)
        predictor = lightgbm_artifact.parse_ensemble(ensemble, manifest)
        self.assertEqual(predictor.num_models, 1)
        self.assertAlmostEqual(
            predictor.predict_one([-2.0, 50.0]),
            1.0 / (1.0 + math.exp(0.1)),
        )
        self.assertAlmostEqual(
            predictor.predict_one([0.0, 50.0]),
            1.0 / (1.0 + math.exp(-0.1)),
        )
        self.assertAlmostEqual(
            predictor.predict_one([float("nan"), 50.0]),
            1.0 / (1.0 + math.exp(0.1)),
        )

    def test_projection_rejects_native_loader_input_and_model_channels(self):
        manifest = _manifest("lightgbm")
        with self.assertRaises(ValueError):
            lightgbm_artifact.sanitize_model(b"tree\nversion=v4\n", manifest)

        for field, value in (
                ("feature_names", ["private"]),
                ("training_history", {"loss": [0.1]}),
                ("path", "/private/model")):
            model = _lightgbm_model()
            model[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "shape"):
                lightgbm_artifact.sanitize_model(_json(model), manifest)

    def test_projection_rejects_bad_topology_cuts_depth_and_leaf_bounds(self):
        manifest = _manifest("lightgbm")
        cases = []
        cyclic = _lightgbm_model()
        cyclic["trees"][0]["left_children"][0] = 0
        cases.append(cyclic)
        cut = _lightgbm_model()
        cut["trees"][0]["split_cut_indices"][0] = 99
        cases.append(cut)
        leaf = _lightgbm_model()
        leaf["trees"][0]["leaf_values"][1] = 0.3
        cases.append(leaf)
        orphan = _lightgbm_model()
        orphan["trees"][0]["left_children"] += [-1]
        orphan["trees"][0]["right_children"] += [-1]
        orphan["trees"][0]["default_left"] += [False]
        orphan["trees"][0]["split_indices"] += [0]
        orphan["trees"][0]["split_cut_indices"] += [0]
        orphan["trees"][0]["leaf_values"] += [0.0]
        cases.append(orphan)
        for index, model in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(ValueError):
                lightgbm_artifact.sanitize_model(_json(model), manifest)


class CatBoostArtifactTests(unittest.TestCase):
    def test_oblivious_projection_is_sanitized_and_predictable(self):
        manifest = _manifest("catboost")
        sanitized, digest = catboost_artifact.sanitize_model(
            _json(_catboost_model()), manifest)
        self.assertEqual(sanitized, _json(json.loads(sanitized), canonical=True))
        self.assertEqual(digest, hashlib.sha256(sanitized).hexdigest())

        ensemble = catboost_artifact.build_ensemble([sanitized], manifest)
        predictor = catboost_artifact.parse_ensemble(ensemble, manifest)
        expected = 1.0 / (1.0 + math.exp(-0.05))
        self.assertAlmostEqual(predictor.predict_one([-2.0, 60.0]), expected)
        missing_expected = 1.0 / (1.0 + math.exp(-0.05))
        self.assertAlmostEqual(
            predictor.predict_one([float("nan"), 60.0]), missing_expected)

    def test_oblivious_projection_rejects_native_categorical_and_bad_geometry(self):
        manifest = _manifest("catboost")
        with self.assertRaises(ValueError):
            catboost_artifact.sanitize_model(b"CBM1untrusted", manifest)

        categorical = _catboost_model()
        categorical["cat_features"] = [0]
        with self.assertRaisesRegex(ValueError, "shape"):
            catboost_artifact.sanitize_model(_json(categorical), manifest)

        bad_leaf_count = _catboost_model()
        bad_leaf_count["trees"][0]["leaf_values"].pop()
        with self.assertRaisesRegex(ValueError, "leaf"):
            catboost_artifact.sanitize_model(_json(bad_leaf_count), manifest)

        bad_cut = _catboost_model()
        bad_cut["trees"][0]["splits"][0]["cut_index"] = -1
        with self.assertRaisesRegex(ValueError, "cut"):
            catboost_artifact.sanitize_model(_json(bad_cut), manifest)

        bad_leaf = _catboost_model()
        bad_leaf["trees"][0]["leaf_values"][0] = -0.3
        with self.assertRaisesRegex(ValueError, "leaf"):
            catboost_artifact.sanitize_model(_json(bad_leaf), manifest)

    def test_duplicate_keys_and_noncanonical_ensemble_fail_closed(self):
        manifest = _manifest("catboost")
        raw = _json(_catboost_model())
        duplicate = raw[:-1] + b',"version":1}'
        with self.assertRaisesRegex(ValueError, "JSON"):
            catboost_artifact.sanitize_model(duplicate, manifest)

        sanitized, _ = catboost_artifact.sanitize_model(raw, manifest)
        ensemble = catboost_artifact.build_ensemble([sanitized], manifest)
        noncanonical = json.dumps(json.loads(ensemble), indent=2).encode("ascii")
        with self.assertRaisesRegex(ValueError, "canonical"):
            catboost_artifact.parse_ensemble(noncanonical, manifest)


if __name__ == "__main__":
    unittest.main()
