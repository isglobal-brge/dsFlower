"""Engine dispatch, portable sidecar and pure-engine round trips."""

import base64
import hashlib
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd


TESTS = os.path.dirname(os.path.abspath(__file__))
FLOWER_APP = os.path.join(TESTS, "..", "..", "flower_app")
sys.path.insert(0, TESTS)
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import native_tree_engine  # noqa: E402
from dsflower_runner import native_tree_client_app as client_app  # noqa: E402
from dsflower_runner import native_tree_request  # noqa: E402
from dsflower_runner import native_tree_server_app as server_app  # noqa: E402
from test_boosting_adapter import _data  # noqa: E402
from test_boosting_artifacts import _public_request as boosting_request  # noqa: E402
from test_forest_adapter import _public_request as forest_request  # noqa: E402


def _manifest(request):
    return native_tree_request.backend_manifest(
        request, epsilon=3.0, delta=1.0e-6, unit="row",
        unit_canonicalization="trim-utf8-v2", gradient_clip=1.0,
        snapshot_hash="a" * 64, cohort_hash="b" * 64)


def _wire(request):
    raw = json.dumps(
        request, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii"), hashlib.sha256(raw).hexdigest()


def _node_manifest(request, request_b64, request_sha256):
    schema = request["public_schema"]
    return {
        "data_type": "tabular", "data_file": "train.csv",
        "data_format": "csv", "dp-track": "native_tree",
        "num-server-rounds": 1, "target-preencoded": True,
        "target_column": schema["target"]["name"],
        "feature_columns": schema["features"],
        "feature-bounds": {
            "lower": schema["lower"], "upper": schema["upper"]},
        "target-levels": {
            "type": "character", "values": ["control", "case"]},
        "task-type": "classification", "num-classes": 2,
        "dp-unit": "row", "patient_column": None,
        "patient-id-canonicalization": "trim-utf8-v2", "n_units": 8,
        "privacy-adjacency": "replace_one", "privacy-epsilon": 3.0,
        "privacy-delta": 1.0e-6, "privacy-clipping_norm": 1.0,
        "privacy-policy-sha256": "a" * 64,
        "native-tree-request-b64": request_b64,
        "native-tree-request-sha256": request_sha256,
    }


class _Grid:
    def __init__(self, context):
        self.context = context

    @staticmethod
    def get_node_ids():
        return [7]

    def send_and_receive(self, messages, timeout):
        return [client_app.train(message, self.context) for message in messages]


class NativeTreeEngineTests(unittest.TestCase):
    def test_release_specs_preserve_xgboost_and_pin_pure_v2(self):
        xgboost = native_tree_engine.release_spec("xgboost")
        self.assertEqual(xgboost["model_file"],
                         "model.xgboost-ensemble.json")
        self.assertEqual(xgboost["profile_version"], 1)
        for engine in ("extra_trees", "lightgbm", "catboost"):
            spec = native_tree_engine.release_spec(engine)
            with self.subTest(engine=engine):
                self.assertEqual(spec["engine"], engine)
                self.assertEqual(spec["profile_version"], 2)
                self.assertEqual(
                    spec["profile_contract"],
                    "dsflower-native-tree-prediction-profile-v2")

    def test_pure_engines_train_ensemble_predict_without_xgboost_bundle(self):
        features, target = _data()
        cases = (
            ("extra_trees", forest_request(trees=2, depth=1)),
            ("lightgbm", boosting_request("lightgbm")),
            ("catboost", boosting_request("catboost")),
        )
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=bytes(range(32))):
            for engine, request in cases:
                manifest = _manifest(request)
                artifact = native_tree_engine.train_model(
                    manifest, features, target)
                ensemble, digest = native_tree_engine.build_ensemble(
                    manifest, [artifact])
                predictor = native_tree_engine.parse_ensemble(
                    manifest, ensemble)
                predictions = np.asarray(predictor.predict(features))
                with self.subTest(engine=engine):
                    self.assertFalse(native_tree_engine.requires_xgboost_bundle(
                        engine))
                    self.assertEqual(
                        hashlib.sha256(ensemble).hexdigest(), digest)
                    self.assertEqual(predictions.shape, target.shape)
                    self.assertTrue(np.all(np.isfinite(predictions)))

    def test_v2_sidecar_is_canonical_bound_and_engine_specific(self):
        request = boosting_request("catboost")
        import base64
        import json
        request_bytes = json.dumps(
            request, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        request_b64 = base64.b64encode(request_bytes).decode("ascii")
        request_sha256 = hashlib.sha256(request_bytes).hexdigest()
        artifact = b'{"safe":true}'
        profile = native_tree_engine.build_prediction_profile(
            request, request_b64, request_sha256, artifact,
            hashlib.sha256(artifact).hexdigest())
        spec = native_tree_engine.validate_prediction_profile(
            profile, request, request_b64, request_sha256, artifact)
        self.assertEqual(spec["engine"], "catboost")
        self.assertEqual(spec["profile_version"], 2)
        with self.assertRaises(ValueError):
            native_tree_engine.validate_prediction_profile(
                profile, request, request_b64, request_sha256,
                artifact + b" ")

    def test_pure_engines_complete_one_flower_round_and_reopen(self):
        cases = (
            forest_request(trees=2, depth=1),
            boosting_request("lightgbm"),
            boosting_request("catboost"),
        )
        for request in cases:
            engine = request["engine"]
            request_b64, request_sha256 = _wire(request)
            spec = native_tree_engine.release_spec(engine)
            schema = request["public_schema"]
            rows = {}
            for index, feature in enumerate(schema["features"]):
                lower = float(schema["lower"][index])
                upper = float(schema["upper"][index])
                rows[feature] = np.linspace(lower, upper, 8)
            rows[schema["target"]["name"]] = [0, 0, 0, 0, 1, 1, 1, 1]
            with self.subTest(engine=engine), \
                    tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results:
                pd.DataFrame(rows).to_csv(
                    os.path.join(root, "train.csv"), index=False)
                with open(os.path.join(root, "manifest.json"), "w",
                          encoding="utf-8") as handle:
                    json.dump(_node_manifest(
                        request, request_b64, request_sha256), handle)
                cfg = {
                    "dp-track": "native_tree", "num-server-rounds": 1,
                    "min-train-nodes": 1, "round-timeout": 10,
                    "results-dir": results,
                    "native-tree-request-b64": request_b64,
                    "native-tree-request-sha256": request_sha256,
                }
                context = SimpleNamespace(
                    node_config={"manifest-dir": root}, run_config=cfg)
                with mock.patch(
                        "dsflower_runner.seeding._node_secret",
                        return_value=bytes(range(32))):
                    server_app.main(
                        _Grid(context), SimpleNamespace(run_config=cfg))
                model_path = os.path.join(results, spec["model_file"])
                profile_path = os.path.join(results, spec["profile_file"])
                self.assertTrue(os.path.isfile(model_path))
                self.assertTrue(os.path.isfile(profile_path))
                with open(model_path, "rb") as handle:
                    ensemble = handle.read()
                with open(profile_path, "rb") as handle:
                    profile = handle.read()
                native_tree_engine.validate_prediction_profile(
                    profile, request, request_b64, request_sha256, ensemble)
                predictor = native_tree_engine.parse_ensemble(
                    native_tree_request.public_backend_manifest(request),
                    ensemble)
                self.assertEqual(len(predictor.predict(
                    pd.DataFrame(rows)[schema["features"]].to_numpy())), 8)


if __name__ == "__main__":
    unittest.main()
