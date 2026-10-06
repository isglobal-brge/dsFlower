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
from flwr.common import RecordDict

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
from test_random_forest_adapter import _manifest as rf_manifest  # noqa: E402


def _dump_manifest(manifest, handle):
    from v3_test_support import source_sidecar
    directory = os.path.dirname(handle.name)
    frame = pd.read_csv(os.path.join(directory, manifest["data_file"]))
    source_sidecar(directory, manifest, frame)
    json.dump(manifest, handle)


def random_forest_request(*, trees=2, depth=1, max_features=1):
    manifest = rf_manifest(
        trees=trees, depth=depth, max_features=max_features, features=2)
    return {
        "contract": native_tree_request.REQUEST_CONTRACT,
        "engine": "random_forest",
        "mode": "native-tight",
        "parameters": [
            {"name": "max_depth", "type": "integer", "value": depth},
            {"name": "max_features", "type": "integer",
             "value": max_features},
            {"name": "n_estimators", "type": "integer", "value": trees},
        ],
        "public_schema": manifest["public_schema"],
        "resources": {
            "max_features": 2, "max_trees": trees, "max_depth": depth,
            "max_bins": 8, "max_threads": 4, "memory_mb": 4096,
            "timeout_seconds": 900,
        },
        "task": "binary",
    }


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
        "semantic-randomness-contract": "dsflower-semantic-randomness-v3",
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
        for engine in (
                "extra_trees", "random_forest", "lightgbm", "catboost"):
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
            ("random_forest", random_forest_request()),
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

    def test_real_pure_training_fails_closed_without_custodial_root(self):
        features, target = _data()
        manifest = _manifest(forest_request(trees=1, depth=1))
        with mock.patch.dict(
                os.environ, {"DSFLOWER_NODE_SECRET_FILE": ""}, clear=False), \
                self.assertRaisesRegex(RuntimeError, "node-secret path"):
            native_tree_engine.train_model(manifest, features, target)

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

    def test_client_forwards_public_identity_full_source_and_subset_for_every_pure_engine(self):
        from dsflower_runner import resampling, task, tree_release
        from test_native_tree_flower_app import _cv_manifest, _cv_run_config, _run_config
        cases = (forest_request(trees=2, depth=1), random_forest_request(),
                 boosting_request("lightgbm"), boosting_request("catboost"))
        for request in cases:
            for mode in ("ordinary", "holdout", "cv"):
                with self.subTest(engine=request["engine"], mode=mode), tempfile.TemporaryDirectory() as root:
                    encoded, digest = _wire(request)
                    manifest = _node_manifest(request, encoded, digest)
                    config = _run_config(encoded, digest, nodes=1)
                    options = {}
                    if mode == "holdout":
                        contract = resampling.holdout_contract(500_000, "row")
                        manifest.update(resampling.manifest_fields(contract))
                        manifest.update({"holdout-validation-bins": 4,
                            "privacy-training-epsilon": 2.4, "privacy-training-delta": 8e-7,
                            "privacy-holdout-epsilon": .6, "privacy-holdout-delta": 2e-7,
                            "run_token": "run_" + "3" * 32})
                        config = _run_config(encoded, digest, nodes=1, holdout=contract)
                        options["holdout"] = dict(contract, bins=4)
                    elif mode == "cv":
                        parent, contract = _cv_manifest(encoded, digest, folds=2, nodes=1)
                        manifest.update({k: v for k, v in parent.items()
                            if k.startswith(("cv-", "privacy-")) or k == "run_token"})
                        manifest["num-features"] = len(request["public_schema"]["features"])
                        config = _cv_run_config(encoded, digest, folds=2, nodes=1)
                        options.update(cross_validation=dict(contract, bins=4, job_sha256="c" * 64), fold=1)
                    schema = request["public_schema"]
                    frame = pd.DataFrame({name: np.linspace(lower, upper, 8)
                        for name, lower, upper in zip(schema["features"], schema["lower"], schema["upper"])})
                    frame[schema["target"]["name"]] = [0, 0, 0, 0, 1, 1, 1, 1]
                    frame.to_csv(os.path.join(root, "train.csv"), index=False)
                    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as handle:
                        _dump_manifest(manifest, handle)
                    context = SimpleNamespace(node_config={"manifest-dir": root}, run_config=config, state=RecordDict())
                    public_requests, private_sources = [], []
                    original_request, original_load = tree_release.native_request_identity, task.load_native_tree_data
                    def public(*args, **kwargs):
                        self.assertFalse(private_sources, "public R must precede source loading")
                        result = original_request(*args, **kwargs)
                        public_requests.append(result)
                        return result
                    def private(*args, **kwargs):
                        result = original_load(*args, **kwargs)
                        private_sources.append(result[-1])
                        return result
                    with (mock.patch("dsflower_runner.seeding._node_secret", return_value=bytes(range(32))),
                          mock.patch.object(tree_release, "native_request_identity", side_effect=public),
                          mock.patch.object(task, "load_native_tree_data", side_effect=private),
                          mock.patch.object(native_tree_engine, "train_model", wraps=native_tree_engine.train_model) as fit,
                          mock.patch.object(resampling, "holdout_mask_from_context", return_value=np.array([True] * 4 + [False] * 4)),
                          mock.patch.object(resampling, "cross_validation_folds_from_context", return_value=np.array([1] * 4 + [2] * 4))):
                        message = server_app._request_messages((1,), encoded, digest, **options)[0]
                        reply = client_app.train(message, context)
                    self.assertEqual(reply.content["metrics"]["available"], 1)
                    fit.assert_called_once()
                    self.assertEqual(len(public_requests), 1)
                    self.assertEqual(len(private_sources), 1)
                    self.assertIs(fit.call_args.kwargs["request_identity"], public_requests[0])
                    self.assertIs(fit.call_args.kwargs["source_units"], private_sources[0])
                    self.assertEqual(len(private_sources[0].records), 8)
                    actual_request = json.loads(public_requests[0].canonical_json)
                    self.assertEqual(actual_request["operation"], "cv-train" if mode == "cv" else "train")
                    self.assertEqual(actual_request["coordinate"]["fold"], 1 if mode == "cv" else None)
                    if mode == "ordinary":
                        self.assertIsNone(fit.call_args.kwargs["subset"])
                    else:
                        self.assertEqual(fit.call_args.kwargs["subset"]["role"], "train")
                        self.assertRegex(fit.call_args.kwargs["subset"]["assignment_sha256"], r"^[0-9a-f]{64}$")
                        self.assertEqual(len(fit.call_args.args[1]), 4)
                    if mode == "ordinary":
                        # A fresh Flower Context and message/node ID must preserve
                        # actual sanitized bytes, independently of R result metadata.
                        fresh = SimpleNamespace(node_config={"manifest-dir": root}, run_config=config, state=RecordDict())
                        with mock.patch("dsflower_runner.seeding._node_secret", return_value=bytes(range(32))):
                            replay = client_app.train(server_app._request_messages((987,), encoded, digest)[0], fresh)
                        self.assertEqual(replay.content["metrics"]["available"], 1)
                        self.assertEqual(reply.content["arrays"].to_numpy_ndarrays()[0].tobytes(),
                                         replay.content["arrays"].to_numpy_ndarrays()[0].tobytes())

    def test_pure_engines_complete_one_flower_round_and_reopen(self):
        cases = (
            forest_request(trees=2, depth=1),
            random_forest_request(),
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
                    _dump_manifest(_node_manifest(
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
