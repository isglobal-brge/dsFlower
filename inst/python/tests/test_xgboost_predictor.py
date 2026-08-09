"""Known-answer and fail-closed tests for the pure-Python XGBoost predictor."""

import copy
import hashlib
import importlib.util
import json
import math
import os
import struct
import sys
import unittest
from fractions import Fraction


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import xgboost_predictor as predictor
from dsflower_runner.xgboost_sanitizer import sanitize_xgboost_json


def _typed(kind, value):
    return {"type": kind, "value": value}


def _schema(task):
    target = (
        {
            "name": "outcome", "kind": "binary",
            "levels": [
                {"type": "number", "value": 0.0},
                {"type": "number", "value": 1.0},
            ],
            "lower": 0.0, "upper": 1.0,
        }
        if task == "binary_classification"
        else {
            "name": "outcome", "kind": "continuous", "levels": None,
            "lower": 0.0, "upper": 4.0,
        }
    )
    core = {
        "version": 1,
        "features": ["marker", "age"],
        "lower": [-5.0, 0.0],
        "upper": [5.0, 100.0],
        "cuts": [[-1.0, 0.0, 1.0], [12.0, 16.0]],
        "target": target,
    }
    wire = json.dumps(
        core, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    return dict(core, sha256=hashlib.sha256(wire).hexdigest())


def _manifest(task="binary_classification"):
    schema = _schema(task)
    return {
        "contract_version": 1,
        "mode": "native-tight",
        "engine": "xgboost",
        "task": task,
        "public_schema": schema,
        "engine_params": {
            "base_score": _typed(
                "float", 0.5 if task == "binary_classification" else 2.0),
            "learning_rate": _typed("float", 0.1),
            "max_bin": _typed("int", 4),
            "max_delta_step": _typed("float", 10.0),
            "max_depth": _typed("int", 1),
            "min_child_weight": _typed("float", 1.0),
            "min_split_loss": _typed("float", 0.0),
            "num_boost_round": _typed("int", 1),
            "reg_alpha": _typed("float", 0.0),
            "reg_lambda": _typed("float", 1.0),
        },
        "privacy": {
            "mechanism": "dp-histogram-v1",
            "epsilon": 1.0,
            "delta": 1.0e-6,
            "unit": "patient",
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
            "max_rows": 1000,
            "max_features": 8,
            "max_trees": 20,
            "max_depth": 8,
            "max_bins": 16,
            "max_artifact_bytes": 2 * 1024 * 1024,
        },
    }


def _next_float32(value):
    value = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    bits = bits + 1 if value >= 0.0 else bits - 1
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def _float32(value):
    return struct.unpack(">f", struct.pack(">f", float(value)))[0]


def _raw_model(*, task, left_leaf, right_leaf, default_left):
    base_score = 0.5 if task == "binary_classification" else 2.0
    objective = (
        "binary:logistic"
        if task == "binary_classification" else "reg:squarederror")
    tree = {
        "base_weights": [0.0, left_leaf, right_leaf],
        "categories": [],
        "categories_nodes": [],
        "categories_segments": [],
        "categories_sizes": [],
        "default_left": [default_left, 0, 0],
        "id": 0,
        "left_children": [1, -1, -1],
        "loss_changes": [0.0, 0.0, 0.0],
        "parents": [2_147_483_647, 0, 0],
        "right_children": [2, -1, -1],
        "split_conditions": [
            _next_float32(0.0), float(left_leaf), float(right_leaf)],
        "split_indices": [0, 0, 0],
        "split_type": [0, 0, 0],
        "sum_hessian": [1.0, 0.5, 0.5],
        "tree_param": {
            "num_deleted": "0", "num_feature": "2", "num_nodes": "3",
            "size_leaf_vector": "1",
        },
    }
    return {
        "learner": {
            "attributes": {},
            "feature_names": [],
            "feature_types": [],
            "gradient_booster": {
                "model": {
                    "cats": {
                        "enc": [], "feature_segments": [], "sorted_idx": []},
                    "gbtree_model_param": {
                        "num_parallel_tree": "1", "num_trees": "1"},
                    "iteration_indptr": [0, 1],
                    "tree_info": [0],
                    "trees": [tree],
                },
                "name": "gbtree",
            },
            "learner_model_param": {
                "base_score": "[" + format(base_score, ".17g") + "]",
                "boost_from_average": "0",
                "num_class": "0",
                "num_feature": "2",
                "num_target": "1",
            },
            "objective": {
                "name": objective,
                "reg_loss_param": {"scale_pos_weight": "1"},
            },
        },
        "version": [3, 4, 0],
    }


def _member(*, task="binary_classification", left=-0.2, right=0.4,
            default_left=1, additional_trees=()):
    model = _raw_model(
        task=task, left_leaf=left, right_leaf=right,
        default_left=default_left)
    tree_specs = ((left, right, default_left),) + tuple(additional_trees)
    if len(tree_specs) > 1:
        booster = model["learner"]["gradient_booster"]["model"]
        template = booster["trees"][0]
        trees = []
        for tree_id, (tree_left, tree_right, tree_default) in enumerate(
                tree_specs):
            tree = copy.deepcopy(template)
            tree["id"] = tree_id
            tree["default_left"][0] = tree_default
            tree["base_weights"][1:] = [tree_left, tree_right]
            tree["split_conditions"][1:] = [tree_left, tree_right]
            trees.append(tree)
        booster["trees"] = trees
        booster["gbtree_model_param"]["num_trees"] = str(len(trees))
        booster["iteration_indptr"] = list(range(len(trees) + 1))
        booster["tree_info"] = [0] * len(trees)
    encoded = json.dumps(model, separators=(",", ":")).encode("ascii")
    return sanitize_xgboost_json(
        encoded,
        expected_task=task,
        expected_features=2,
        expected_trees=len(tree_specs),
        expected_max_depth=1,
        public_cuts=((-1.0, 0.0, 1.0), (12.0, 16.0)),
        expected_base_score=(0.5 if task == "binary_classification" else 2.0),
        max_total_nodes=3 * len(tree_specs),
        max_artifact_bytes=2 * 1024 * 1024,
        numeric_abs_cap=1.0e12,
        leaf_abs_cap=1.000001,
    )[0]


def _ensemble(manifest, members):
    ordered = sorted(
        ((hashlib.sha256(member).hexdigest(), member) for member in members),
        key=lambda item: (item[0], item[1]),
    )
    value = {
        "aggregation": "mean_prediction",
        "contract": "dsflower-xgboost-ensemble-v1",
        "engine": "xgboost",
        "models": [json.loads(member.decode("ascii"))
                   for _digest, member in ordered],
        "public_schema_sha256": manifest["public_schema"]["sha256"],
        "task": ("binary" if manifest["task"] == "binary_classification"
                 else "regression"),
        "version": 1,
    }
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _sigmoid(value):
    if value >= 0.0:
        return 1.0 / (1.0 + math.exp(-value))
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


class XGBoostPredictorKnownAnswerTests(unittest.TestCase):
    def test_binary_below_equal_above_nan_and_duplicate_models(self):
        manifest = _manifest()
        member = _member(default_left=1)
        artifact = _ensemble(manifest, [member, member])
        parsed = predictor.parse_xgboost_ensemble(artifact, manifest)
        self.assertEqual(parsed.task, "binary")
        self.assertEqual(parsed.num_features, 2)
        self.assertEqual(parsed.num_models, 2)

        tiny = _next_float32(0.0)
        rows = [
            [-tiny, 20.0], [0.0, 20.0], [tiny, 20.0],
            [float("nan"), 20.0], [float("-inf"), 20.0],
            [float("inf"), 20.0], [-1.0e300, 20.0], [1.0e300, 20.0],
            [Fraction(0, 1), Fraction(20, 1)],
        ]
        expected = [_sigmoid(_float32(-0.2)), _sigmoid(_float32(-0.2)),
                    _sigmoid(_float32(0.4)), _sigmoid(_float32(-0.2)),
                    _sigmoid(_float32(-0.2)), _sigmoid(_float32(0.4)),
                    _sigmoid(_float32(-0.2)), _sigmoid(_float32(0.4)),
                    _sigmoid(_float32(-0.2))]
        self.assertEqual(parsed.predict(rows), expected)
        self.assertEqual(
            predictor.predict_xgboost_ensemble(artifact, manifest, rows),
            expected)
        if importlib.util.find_spec("numpy") is not None:
            import numpy as np
            self.assertEqual(
                parsed.predict_one([np.float32(0.0), np.int64(20)]),
                expected[1])

    def test_ensemble_means_predictions_not_margins(self):
        manifest = _manifest()
        first = _member(left=-0.2, right=0.4)
        second = _member(left=0.6, right=-0.1)
        artifact = _ensemble(manifest, [first, second])
        predictions = predictor.predict_xgboost_ensemble(
            artifact, manifest, [[0.0, 20.0], [_next_float32(0.0), 20.0]])
        self.assertAlmostEqual(
            predictions[0], (_sigmoid(-0.2) + _sigmoid(0.6)) / 2.0)
        self.assertAlmostEqual(
            predictions[1], (_sigmoid(0.4) + _sigmoid(-0.1)) / 2.0)

    def test_regression_adds_public_base_score_and_uses_default_right(self):
        manifest = _manifest("regression")
        manifest["engine_params"]["num_boost_round"]["value"] = 2
        member = _member(
            task="regression", left=-0.5, right=0.25, default_left=0,
            additional_trees=((0.125, -0.75, 1),))
        artifact = _ensemble(manifest, [member])
        tiny = _next_float32(0.0)
        predictions = predictor.predict_xgboost_ensemble(artifact, manifest, [
            [-tiny, 20.0], [0.0, 20.0], [tiny, 20.0],
            [float("nan"), 20.0],
        ])
        self.assertEqual(predictions, [1.625, 1.625, 1.5, 2.375])


class XGBoostPredictorRejectionTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest()
        self.member = _member()
        self.artifact = _ensemble(self.manifest, [self.member])

    def test_duplicate_unknown_noncanonical_and_empty_shapes_fail(self):
        duplicate = self.artifact.replace(
            b'{"aggregation":',
            b'{"aggregation":"mean_prediction","aggregation":', 1)
        with self.assertRaisesRegex(ValueError, "JSON"):
            predictor.parse_xgboost_ensemble(duplicate, self.manifest)

        for mutation in ("unknown", "empty", "whitespace"):
            value = json.loads(self.artifact)
            if mutation == "unknown":
                value["training_log"] = "private"
                candidate = json.dumps(
                    value, sort_keys=True, separators=(",", ":")).encode()
            elif mutation == "empty":
                value["models"] = []
                candidate = json.dumps(
                    value, sort_keys=True, separators=(",", ":")).encode()
            else:
                candidate = self.artifact + b"\n"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                predictor.parse_xgboost_ensemble(candidate, self.manifest)

    def test_cap_schema_task_member_and_order_mismatches_fail(self):
        capped = copy.deepcopy(self.manifest)
        capped["resources"]["max_artifact_bytes"] = len(self.artifact) - 1
        with self.assertRaisesRegex(ValueError, "cap"):
            predictor.parse_xgboost_ensemble(self.artifact, capped)

        for field, replacement in (
                ("public_schema_sha256", "f" * 64),
                ("task", "regression")):
            value = json.loads(self.artifact)
            value[field] = replacement
            candidate = json.dumps(
                value, sort_keys=True, separators=(",", ":")).encode()
            with self.subTest(field=field), self.assertRaises(ValueError):
                predictor.parse_xgboost_ensemble(candidate, self.manifest)

        value = json.loads(self.artifact)
        value["models"][0]["learner"]["private_metric"] = 1.0
        malformed = json.dumps(
            value, sort_keys=True, separators=(",", ":")).encode()
        with self.assertRaises(ValueError):
            predictor.parse_xgboost_ensemble(malformed, self.manifest)

        second = _member(left=0.1, right=0.2)
        ordered = json.loads(_ensemble(self.manifest, [self.member, second]))
        ordered["models"].reverse()
        reversed_artifact = json.dumps(
            ordered, sort_keys=True, separators=(",", ":")).encode()
        with self.assertRaisesRegex(ValueError, "ordered"):
            predictor.parse_xgboost_ensemble(
                reversed_artifact, self.manifest)

    def test_manifest_profile_and_prediction_rows_are_fail_closed(self):
        unknown = copy.deepcopy(self.manifest)
        unknown["engine_params"]["subsample"] = _typed("float", 0.5)
        with self.assertRaisesRegex(ValueError, "parameter profile"):
            predictor.parse_xgboost_ensemble(self.artifact, unknown)

        parsed = predictor.parse_xgboost_ensemble(
            self.artifact, self.manifest)
        for row in ([0.0], [0.0, True], [0.0, complex(1.0)], "bad"):
            with self.subTest(row=row), self.assertRaises(ValueError):
                parsed.predict_one(row)
        with self.assertRaises(ValueError):
            parsed.predict("bad")

    @unittest.skipUnless(importlib.util.find_spec("xgboost") is not None,
                         "xgboost is not installed")
    def test_parity_with_pinned_xgboost_when_available(self):
        import numpy as np
        import xgboost as xgb

        if xgb.__version__ != "3.4.0":
            self.skipTest("prediction parity requires XGBoost 3.4.0")
        tiny = np.nextafter(np.float32(0.0), np.float32(np.inf))
        rows = np.asarray([
            [-tiny, 20.0], [0.0, 20.0], [tiny, 20.0],
            [np.nan, 20.0],
        ], dtype=np.float32)
        regression_manifest = _manifest("regression")
        regression_manifest["engine_params"]["num_boost_round"]["value"] = 2
        for task, manifest, members in (
            ("binary", self.manifest, [
                self.member,
                _member(left=0.6, right=-0.1, default_left=1),
            ]),
            ("regression", regression_manifest, [
                _member(task="regression", left=-0.5, right=0.25,
                        default_left=0,
                        additional_trees=((0.125, -0.75, 1),)),
                _member(task="regression", left=0.3, right=-0.75,
                        default_left=1,
                        additional_trees=((-0.2, 0.4, 0),)),
            ]),
        ):
            expected = np.zeros(rows.shape[0], dtype=np.float64)
            for member in members:
                model = xgb.Booster()
                model.load_model(bytearray(member))
                expected += model.predict(xgb.DMatrix(rows))
            expected /= len(members)
            actual = predictor.predict_xgboost_ensemble(
                _ensemble(manifest, members), manifest, rows.tolist())
            with self.subTest(task=task):
                np.testing.assert_allclose(
                    actual, expected, rtol=1e-7, atol=1e-8)


if __name__ == "__main__":
    unittest.main()
