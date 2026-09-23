"""Distinct native requests must not share keys on identical private tensors."""

import copy
import os
import sys
import unittest
from unittest import mock

import numpy as np


TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS)
sys.path.insert(0, os.path.join(TESTS, "..", "..", "flower_app"))

from dsflower_runner import native_tree_engine, seeding, xgboost_adapter
from dsflower_runner import native_tree_client_app
from test_boosting_adapter import _training_manifest
from test_forest_adapter import _manifest as forest_manifest, _rehash_schema
from test_random_forest_adapter import _manifest as random_forest_manifest
from test_xgboost_adapter import _manifest as xgboost_manifest
import test_xgboost_adapter as xgboost_tests


class NativeRequestSelectionTests(unittest.TestCase):
    def _manifest(self, engine):
        if engine == "extra_trees":
            manifest = forest_manifest(trees=2, depth=1)
        elif engine == "random_forest":
            manifest = random_forest_manifest(
                trees=2, depth=1, max_features=1, features=2)
        elif engine == "xgboost":
            manifest = xgboost_manifest()
        else:
            manifest = _training_manifest(engine, trees=2)
        manifest["privacy"]["unit"] = "patient"
        return manifest

    def _release(self, manifest, selection=None):
        features = np.zeros((4, 2), dtype=np.float64)
        target = np.asarray([0, 1, 0, 1], dtype=np.float64)
        units = ["a", "b", "c", "d"]
        if manifest["privacy"]["unit"] == "row":
            units = None
        keys = []
        original = seeding.master_seed

        def capture(*args, **kwargs):
            key = original(*args, **kwargs)
            keys.append((args[0], key))
            return key

        with mock.patch.object(seeding, "_node_secret", return_value=b"s" * 32), \
                mock.patch.object(seeding, "master_seed", side_effect=capture):
            if manifest["engine"] == "xgboost":
                prepared = xgboost_adapter.prepare_xgboost_training(
                    manifest, features, target, unit_ids=units,
                    native_bundle=xgboost_tests.XGBoostPrfAndBoundaryTests._bundle("f" * 64),
                    request_selection=selection)
                artifact = bytes(prepared._noise_key)
            else:
                artifact = native_tree_engine.train_model(
                    manifest, features, target, unit_ids=units,
                    request_selection=selection)
        return keys, artifact

    def test_identical_tensors_and_selection_replay_for_every_engine(self):
        for engine in ("extra_trees", "random_forest", "xgboost", "lightgbm", "catboost"):
            with self.subTest(engine=engine):
                manifest = self._manifest(engine)
                selection = {"target_column": "outcome", "patient_column": "patient"}
                self.assertEqual(self._release(manifest, selection),
                                 self._release(copy.deepcopy(manifest), selection))

    def test_schema_and_unit_selections_separate_every_engine_key(self):
        for engine in ("extra_trees", "random_forest", "xgboost", "lightgbm", "catboost"):
            manifest = self._manifest(engine)
            first, _ = self._release(manifest)
            changes = (
                ("target-name", lambda m: m["public_schema"]["target"].update(name="other")),
                ("feature-identity", lambda m: m["public_schema"]["features"].__setitem__(0, "other")),
                ("feature-order", lambda m: m["public_schema"]["features"].reverse()),
                ("target-vocabulary", lambda m: m["public_schema"]["target"].update(levels=[
                    {"type": "string", "value": "negative"},
                    {"type": "string", "value": "positive"}])),
                ("feature-lower", lambda m: m["public_schema"]["lower"].__setitem__(0, -6.0)),
                ("feature-upper", lambda m: m["public_schema"]["upper"].__setitem__(0, 101.0)),
                ("public-cut", lambda m: m["public_schema"]["cuts"][0].__setitem__(
                    0, m["public_schema"]["cuts"][0][0] + 0.01)),
                ("unit-policy", lambda m: m["privacy"].update(unit="row")),
            )
            for label, change in changes:
                with self.subTest(engine=engine, selection=label):
                    changed = copy.deepcopy(manifest)
                    change(changed)
                    _rehash_schema(changed)
                    second, _ = self._release(changed)
                    self.assertTrue(set(first).isdisjoint(second))

    def test_node_patient_column_and_fold_coordinates_reach_every_engine(self):
        baseline = {"target_column": "outcome", "patient_column": "patient",
                    "cv-folds": 3, "cv-assignment": "keyed-unit-v1"}
        for engine in ("extra_trees", "random_forest", "xgboost", "lightgbm", "catboost"):
            manifest = self._manifest(engine)
            selection = native_tree_client_app._request_selection(baseline, "cv-train", 1)
            first, _ = self._release(manifest, selection)
            variants = [
                native_tree_client_app._request_selection(baseline, "cv-train", 2),
                native_tree_client_app._request_selection(baseline, "train"),
                native_tree_client_app._request_selection(
                    dict(baseline, patient_column="other_patient"), "cv-train", 1),
            ]
            for changed in variants:
                with self.subTest(engine=engine, selection=changed):
                    second, _ = self._release(manifest, changed)
                    self.assertTrue(set(first).isdisjoint(second))

    def test_boosting_parameters_separate_identical_first_stage_statistics(self):
        for engine in ("lightgbm", "catboost"):
            with self.subTest(engine=engine):
                manifest = self._manifest(engine)
                first, _ = self._release(manifest)
                changed = copy.deepcopy(manifest)
                changed["engine_params"]["learning_rate"]["value"] = 0.25
                second, _ = self._release(changed)
                self.assertNotEqual(first[0], second[0])


if __name__ == "__main__":
    unittest.main()
