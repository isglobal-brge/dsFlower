"""Security tests for the pinned XGBoost prediction-artifact sanitizer."""

import copy
import importlib.util
import json
import os
import struct
import sys
import unittest


RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import xgboost_sanitizer as sanitizer


def _public_threshold(value):
    value = struct.unpack(">f", struct.pack(">f", float(value)))[0]
    bits = struct.unpack(">I", struct.pack(">f", value))[0]
    bits = bits + 1 if value >= 0.0 else bits - 1
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def _tree():
    return {
        "base_weights": [0.3, -0.2, 0.2],
        "categories": [],
        "categories_nodes": [],
        "categories_segments": [],
        "categories_sizes": [],
        "default_left": [0, 0, 0],
        "id": 0,
        "left_children": [1, -1, -1],
        "loss_changes": [0.5, 0.0, 0.0],
        "parents": [2_147_483_647, 0, 0],
        "right_children": [2, -1, -1],
        "split_conditions": [_public_threshold(0.0), -0.1, 0.1],
        "split_indices": [0, 0, 0],
        "split_type": [0, 0, 0],
        "sum_hessian": [2.0, 1.0, 1.0],
        "tree_param": {
            "num_deleted": "0",
            "num_feature": "2",
            "num_nodes": "3",
            "size_leaf_vector": "1",
        },
    }


def _model():
    return {
        "learner": {
            "attributes": {},
            "feature_names": [],
            "feature_types": [],
            "gradient_booster": {
                "model": {
                    "cats": {
                        "enc": [], "feature_segments": [], "sorted_idx": [],
                    },
                    "gbtree_model_param": {
                        "num_parallel_tree": "1", "num_trees": "1",
                    },
                    "iteration_indptr": [0, 1],
                    "tree_info": [0],
                    "trees": [_tree()],
                },
                "name": "gbtree",
            },
            "learner_model_param": {
                "base_score": "[5E-1]",
                "boost_from_average": "0",
                "num_class": "0",
                "num_feature": "2",
                "num_target": "1",
            },
            "objective": {
                "name": "binary:logistic",
                "reg_loss_param": {"scale_pos_weight": "1"},
            },
        },
        "version": [3, 4, 0],
    }


def _bytes(value):
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _sanitize(value, *, task="binary_classification", base_score=0.5):
    return sanitizer.sanitize_xgboost_json(
        _bytes(value),
        expected_task=task,
        expected_features=2,
        expected_trees=1,
        expected_max_depth=1,
        public_cuts=((-1.0, 0.0, 1.0), (12.0, 16.0)),
        expected_base_score=base_score,
        max_total_nodes=7,
        max_artifact_bytes=100_000,
    )


class XGBoostSanitizerTests(unittest.TestCase):
    def test_valid_model_is_canonical_idempotent_and_prediction_only(self):
        encoded, digest = _sanitize(_model())
        parsed = json.loads(encoded)
        tree = parsed["learner"]["gradient_booster"]["model"]["trees"][0]
        self.assertEqual(tree["base_weights"], [0.0, 0.0, 0.0])
        self.assertEqual(tree["loss_changes"], [0.0, 0.0, 0.0])
        self.assertEqual(tree["sum_hessian"], [0.0, 0.0, 0.0])
        self.assertEqual(tree["split_conditions"][0], _public_threshold(0.0))
        self.assertRegex(digest, r"^[0-9a-f]{64}$")
        self.assertEqual(encoded, _sanitize(parsed)[0])
        self.assertNotIn(b"training_history", encoded)
        self.assertNotIn(b"patient", encoded)

    def test_names_attributes_unknown_fields_and_categories_are_closed(self):
        for mutation in ("name", "attribute", "unknown", "category"):
            value = _model()
            if mutation == "name":
                value["learner"]["feature_names"] = ["patient_marker"]
            elif mutation == "attribute":
                value["learner"]["attributes"] = {"history": "private"}
            elif mutation == "unknown":
                value["learner"]["training_log"] = "private"
            else:
                value["learner"]["gradient_booster"]["model"]["trees"][0][
                    "categories"] = [1]
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    _sanitize(value)

    def test_only_public_thresholds_and_bounded_leaf_weights_pass(self):
        for threshold in (0.0, 0.25, _public_threshold(
                _public_threshold(0.0))):
            value = _model()
            value["learner"]["gradient_booster"]["model"]["trees"][0][
                "split_conditions"][0] = threshold
            with self.subTest(threshold=threshold), self.assertRaisesRegex(
                    ValueError, "public cut"):
                _sanitize(value)

        value = _model()
        value["learner"]["gradient_booster"]["model"]["trees"][0][
            "split_conditions"][1] = 1.0e7
        with self.assertRaisesRegex(ValueError, "leaf"):
            _sanitize(value)

    def test_topology_depth_and_public_schedule_are_exact(self):
        for mutation in ("parent", "back_edge", "one_child", "duplicate_child"):
            value = _model()
            tree = value["learner"]["gradient_booster"]["model"]["trees"][0]
            if mutation == "parent":
                tree["parents"][2] = 1
            elif mutation == "back_edge":
                tree["left_children"][0] = 0
            elif mutation == "one_child":
                tree["right_children"][0] = -1
            else:
                tree["right_children"][0] = 1
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    _sanitize(value)

        value = _model()
        value["learner"]["gradient_booster"]["model"][
            "gbtree_model_param"]["num_trees"] = "2"
        with self.assertRaisesRegex(ValueError, "geometry"):
            _sanitize(value)

        value = _model()
        model = value["learner"]["gradient_booster"]["model"]
        second = copy.deepcopy(model["trees"][0])
        second["id"] = 1
        model["trees"].append(second)
        with self.assertRaisesRegex(ValueError, "geometry"):
            _sanitize(value)

        value = _model()
        tree = value["learner"]["gradient_booster"]["model"]["trees"][0]
        tree.update({
            "base_weights": [0.0] * 7,
            "default_left": [0] * 7,
            "left_children": [1, 3, 5, -1, -1, -1, -1],
            "loss_changes": [0.0] * 7,
            "parents": [2_147_483_647, 0, 0, 1, 1, 2, 2],
            "right_children": [2, 4, 6, -1, -1, -1, -1],
            "split_conditions": [
                _public_threshold(0.0), _public_threshold(0.0),
                _public_threshold(0.0), -0.1, 0.1, -0.2, 0.2,
            ],
            "split_indices": [0] * 7,
            "split_type": [0] * 7,
            "sum_hessian": [0.0] * 7,
        })
        tree["tree_param"]["num_nodes"] = "7"
        with self.assertRaisesRegex(ValueError, "depth"):
            _sanitize(value)

    def test_leaf_fields_cannot_be_auxiliary_channels(self):
        for field, replacement in (
                ("default_left", 1),
                ("split_indices", 1),
                ("split_conditions", 1.0e7)):
            value = _model()
            value["learner"]["gradient_booster"]["model"]["trees"][0][
                field][1] = replacement
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "leaf"):
                    _sanitize(value)

    def test_objective_base_score_and_version_are_pinned(self):
        for field, replacement in (
                ("objective", "reg:squarederror"),
                ("base_score", "[6E-1]")):
            value = _model()
            if field == "objective":
                value["learner"]["objective"]["name"] = replacement
            else:
                value["learner"]["learner_model_param"][field] = replacement
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _sanitize(value)
        value = _model()
        value["version"] = [3, 5, 0]
        with self.assertRaisesRegex(ValueError, "version"):
            _sanitize(value)

        for location in ("version", "tree_id", "iteration", "tree_info"):
            value = _model()
            if location == "version":
                value["version"][2] = False
            elif location == "tree_id":
                value["learner"]["gradient_booster"]["model"]["trees"][0][
                    "id"] = False
            elif location == "iteration":
                value["learner"]["gradient_booster"]["model"][
                    "iteration_indptr"][0] = False
            else:
                value["learner"]["gradient_booster"]["model"][
                    "tree_info"][0] = False
            with self.subTest(location=location), self.assertRaises(ValueError):
                _sanitize(value)

        value = _model()
        value["learner"]["objective"]["custom_gradient"] = "callback"
        with self.assertRaisesRegex(ValueError, "shape"):
            _sanitize(value)

    def test_regression_profile_is_supported_without_private_base_estimation(self):
        value = _model()
        value["learner"]["learner_model_param"]["base_score"] = "[0E0]"
        value["learner"]["objective"]["name"] = "reg:squarederror"
        encoded, _digest = _sanitize(
            value, task="regression", base_score=0.0)
        parsed = json.loads(encoded)
        self.assertEqual(parsed["learner"]["objective"]["name"],
                         "reg:squarederror")
        self.assertEqual(parsed["learner"]["learner_model_param"]["base_score"],
                         "[0]")

    def test_duplicate_keys_nonfinite_values_and_oversize_are_rejected(self):
        raw = _bytes(_model())
        duplicate = raw.replace(
            b'{"learner":', b'{"version":[3,4,0],"learner":', 1)
        with self.assertRaisesRegex(ValueError, "invalid"):
            sanitizer.sanitize_xgboost_json(
                duplicate,
                expected_task="binary_classification",
                expected_features=2,
                expected_trees=1,
                expected_max_depth=1,
                public_cuts=((-1.0, 0.0, 1.0), (12.0, 16.0)),
                expected_base_score=0.5,
                max_total_nodes=7,
                max_artifact_bytes=100_000,
            )
        nonfinite = raw.replace(b'"sum_hessian":[2.0',
                                b'"sum_hessian":[NaN', 1)
        with self.assertRaisesRegex(ValueError, "invalid"):
            sanitizer.sanitize_xgboost_json(
                nonfinite,
                expected_task="binary_classification",
                expected_features=2,
                expected_trees=1,
                expected_max_depth=1,
                public_cuts=((-1.0, 0.0, 1.0), (12.0, 16.0)),
                expected_base_score=0.5,
                max_total_nodes=7,
                max_artifact_bytes=100_000,
            )
        with self.assertRaisesRegex(ValueError, "byte cap"):
            sanitizer.sanitize_xgboost_json(
                raw,
                expected_task="binary_classification",
                expected_features=2,
                expected_trees=1,
                expected_max_depth=1,
                public_cuts=((-1.0, 0.0, 1.0), (12.0, 16.0)),
                expected_base_score=0.5,
                max_total_nodes=7,
                max_artifact_bytes=10,
            )

    def test_auxiliary_native_statistics_must_still_be_finite(self):
        for field in ("base_weights", "loss_changes", "sum_hessian"):
            value = copy.deepcopy(_model())
            value["learner"]["gradient_booster"]["model"]["trees"][0][
                field][0] = float("nan")
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    _sanitize(value)

        value = _model()
        tree = value["learner"]["gradient_booster"]["model"]["trees"][0]
        tree["base_weights"][0] = -1e30
        tree["loss_changes"][0] = 1e30
        tree["sum_hessian"][0] = 1e30
        encoded, _digest = _sanitize(value)
        sanitized = json.loads(encoded)["learner"]["gradient_booster"][
            "model"]["trees"][0]
        self.assertEqual(sanitized["base_weights"], [0.0, 0.0, 0.0])
        self.assertEqual(sanitized["loss_changes"], [0.0, 0.0, 0.0])
        self.assertEqual(sanitized["sum_hessian"], [0.0, 0.0, 0.0])

    @unittest.skipUnless(importlib.util.find_spec("xgboost") is not None,
                         "xgboost is not installed")
    def test_prediction_parity_with_pinned_upstream_when_available(self):
        import numpy as np
        import xgboost as xgb

        if xgb.__version__ != "3.4.0":
            self.skipTest("prediction parity requires pinned XGBoost 3.4.0")
        raw = _bytes(_model())
        sanitized, _digest = _sanitize(_model())
        original = xgb.Booster()
        safe = xgb.Booster()
        original.load_model(bytearray(raw))
        safe.load_model(bytearray(sanitized))
        just_below = np.nextafter(np.float32(0.0), np.float32(-np.inf))
        just_above = np.nextafter(np.float32(0.0), np.float32(np.inf))
        matrix = xgb.DMatrix(np.asarray([
            [just_below, 12.0], [0.0, 14.0], [just_above, 15.0], [2.0, 16.0],
            [np.nan, 13.0],
        ], dtype=np.float32))
        original_predictions = original.predict(matrix)
        safe_predictions = safe.predict(matrix)
        np.testing.assert_array_equal(original_predictions, safe_predictions)
        self.assertEqual(original_predictions[0], original_predictions[1])
        self.assertNotEqual(original_predictions[1], original_predictions[2])
        self.assertEqual(original_predictions[2], original_predictions[4])


if __name__ == "__main__":
    unittest.main()
