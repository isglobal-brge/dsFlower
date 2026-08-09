"""Fail-closed tests for the server-owned native XGBoost adapter."""

import copy
import hashlib
import json
import math
import os
import sys
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import seeding
from dsflower_runner import xgboost_adapter as adapter
from dsflower_runner import xgboost_bundle


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
        else {"name": "outcome", "kind": "continuous",
              "levels": None, "lower": -10.0, "upper": 10.0}
    )
    core = {
        "version": 1,
        "features": ["age", "marker"],
        "lower": [0.0, -5.0],
        "upper": [100.0, 5.0],
        "cuts": [[18.0, 40.0, 65.0], [-1.0, 0.0, 1.0]],
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
            "base_score": _typed("float", 0.5 if task == "binary_classification" else 0.0),
            "learning_rate": _typed("float", 0.1),
            "max_bin": _typed("int", 4),
            "max_delta_step": _typed("float", 10.0),
            "max_depth": _typed("int", 3),
            "min_child_weight": _typed("float", 1.0),
            "min_split_loss": _typed("float", 0.0),
            "num_boost_round": _typed("int", 4),
            "reg_alpha": _typed("float", 0.0),
            "reg_lambda": _typed("float", 1.0),
        },
        "privacy": {
            "mechanism": "dp-histogram-v1",
            "epsilon": 1.0,
            "delta": 1e-6,
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


def _data():
    return (
        np.asarray([[65.0, 1.0], [18.0, -1.0], [40.0, 0.0]],
                   dtype=np.float64),
        np.asarray([1.0, 0.0, 1.0], dtype=np.float64),
        [" patient-c ", "patient-a", "patient-b"],
    )


class XGBoostManifestTests(unittest.TestCase):
    def test_exact_parameter_profile_is_canonical_and_server_pinned(self):
        profile = adapter.canonical_xgboost_profile(_manifest())
        self.assertEqual(profile["num_boost_round"], 4)
        self.assertEqual(profile["max_depth"], 3)
        self.assertEqual(profile["max_bin"], 4)
        self.assertEqual(profile["base_score"], 0.5)
        self.assertEqual(profile["objective"], "binary:logistic")
        self.assertEqual(profile["fixed_point_bits"], 20)
        self.assertEqual(profile["fixed_point_scale"], 1 << 20)
        self.assertGreater(profile["root_noise_scale"], 0)
        self.assertGreater(profile["level_noise_scale"], 0)
        self.assertEqual(profile["releases"], 12)

        for name, value in (
                ("objective", _typed("string", "reg:squarederror")),
                ("custom_objective", _typed("string", "callback")),
                ("subsample", _typed("float", 0.5)),
                ("colsample_bytree", _typed("float", 0.5)),
                ("early_stopping_rounds", _typed("int", 2)),
                ("monotone_constraints", _typed("int_list", [0, 1])),
                ("seed", _typed("int", 7)),
                ("tree_method", _typed("string", "hist"))):
            manifest = _manifest()
            manifest["engine_params"][name] = value
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    adapter.canonical_xgboost_profile(manifest)

    def test_shape_is_semantic_and_resources_only_reject(self):
        for parameter, resource in (
                ("num_boost_round", "max_trees"),
                ("max_depth", "max_depth"),
                ("max_bin", "max_bins")):
            manifest = _manifest()
            manifest["engine_params"][parameter]["value"] = (
                manifest["resources"][resource] + 1)
            with self.subTest(parameter=parameter):
                with self.assertRaisesRegex(ValueError, "resource"):
                    adapter.canonical_xgboost_profile(manifest)

        manifest = _manifest()
        manifest["engine_params"]["max_bin"]["value"] = 5
        with self.assertRaisesRegex(ValueError, "public cuts"):
            adapter.canonical_xgboost_profile(manifest)

        manifest = _manifest()
        manifest["engine_params"]["max_depth"]["value"] = 31
        manifest["resources"]["max_depth"] = 32
        with self.assertRaisesRegex(ValueError, "native XGBoost"):
            adapter.canonical_xgboost_profile(manifest)

    def test_base_score_constraints_and_clips_are_exact(self):
        manifest = _manifest()
        manifest["engine_params"]["base_score"]["value"] = 0.6
        with self.assertRaisesRegex(ValueError, "base_score"):
            adapter.canonical_xgboost_profile(manifest)

        regression = _manifest("regression")
        self.assertEqual(
            adapter.canonical_xgboost_profile(regression)["base_score"], 0.0)

        collapsed = _manifest("regression")
        collapsed["public_schema"]["target"]["lower"] = 1.0
        collapsed["public_schema"]["target"]["upper"] = 1.0 + 1e-12
        core = {name: value for name, value in collapsed["public_schema"].items()
                if name != "sha256"}
        wire = json.dumps(
            core, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        collapsed["public_schema"]["sha256"] = hashlib.sha256(wire).hexdigest()
        collapsed["data_scope"]["schema_hash"] = collapsed[
            "public_schema"]["sha256"]
        with self.assertRaisesRegex(ValueError, "target bounds"):
            adapter.canonical_xgboost_profile(collapsed)

        for mutation in ("missing", "extra", "nonpositive"):
            manifest = _manifest()
            params = manifest["privacy"]["mechanism_params"]
            if mutation == "missing":
                del params["gradient_clip"]
            elif mutation == "extra":
                params["sampler"] = _typed("string", "custom")
            else:
                params["hessian_clip"]["value"] = 0.0
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    adapter.canonical_xgboost_profile(manifest)

        for name in ("max_delta_step", "reg_lambda"):
            manifest = _manifest()
            manifest["engine_params"][name]["value"] = 0.0
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    adapter.canonical_xgboost_profile(manifest)

        manifest = _manifest()
        manifest["privacy"]["delta"] = 0.0
        with self.assertRaises(ValueError):
            adapter.canonical_xgboost_profile(manifest)

        regression = _manifest("regression")
        regression["privacy"]["mechanism_params"]["hessian_clip"]["value"] = 0.5
        with self.assertRaisesRegex(ValueError, "hessian_clip"):
            adapter.canonical_xgboost_profile(regression)

        for section, name in (
                ("engine_params", "reg_lambda"),
                ("privacy", "gradient_clip")):
            manifest = _manifest()
            if section == "engine_params":
                manifest[section][name]["value"] = 1e-100
            else:
                manifest[section]["mechanism_params"][name]["value"] = 1e-100
            with self.subTest(section=section, name=name), self.assertRaises(ValueError):
                adapter.canonical_xgboost_profile(manifest)


class XGBoostMaterializationTests(unittest.TestCase):
    def test_patient_units_are_record_ordered_and_immutable(self):
        X, y, units = _data()
        materialized = adapter.materialize_xgboost_units(
            _manifest(), X, y, unit_ids=units)
        expected = sorted(
            (tuple(row.astype(np.float32)), np.float32(label))
            for row, label in zip(X, y))
        actual = sorted(
            (tuple(row), label) for row, label in zip(
                materialized.features, materialized.target))
        self.assertEqual(actual, expected)
        self.assertEqual(materialized.privacy_unit, "patient")
        self.assertFalse(materialized.features.flags.writeable)
        self.assertFalse(materialized.target.flags.writeable)
        self.assertNotIn("rows", repr(materialized))

        permutation = np.asarray([2, 0, 1])
        replay = adapter.materialize_xgboost_units(
            _manifest(), X[permutation], y[permutation],
            unit_ids=[units[index] for index in permutation])
        np.testing.assert_array_equal(
            materialized.features, replay.features)
        np.testing.assert_array_equal(materialized.target, replay.target)
        self.assertEqual(replay.privacy_unit, "patient")

        unicode_order = adapter.materialize_xgboost_units(
            _manifest(), X, y, unit_ids=["é", "z", "a"])
        self.assertEqual(unicode_order.privacy_unit, "patient")
        np.testing.assert_array_equal(
            unicode_order.features, materialized.features)
        np.testing.assert_array_equal(unicode_order.target, materialized.target)

    def test_patient_duplicates_and_invalid_ids_aggregate_without_oracle(self):
        X = np.asarray([
            [10.0, np.nan], [30.0, 1.0], [70.0, 3.0], [90.0, 5.0],
        ], dtype=np.float64)
        y = np.asarray([1.0, 0.0, 1.0, np.nan], dtype=np.float64)
        units = ["same", " same ", None, "<NA>"]
        with mock.patch.object(
                adapter.xgboost_accounting,
                "validate_fixed_point_unit_geometry",
                wraps=adapter.xgboost_accounting.validate_fixed_point_unit_geometry
        ) as geometry:
            materialized = adapter.materialize_xgboost_units(
                _manifest(), X, y, unit_ids=units)
        self.assertEqual(materialized.features.shape, (2, 2))
        self.assertEqual(geometry.call_args.args[0], 2)
        np.testing.assert_allclose(
            materialized.features,
            np.asarray([[20.0, 1.0], [80.0, 4.0]], dtype=np.float32))
        np.testing.assert_array_equal(
            materialized.target, np.asarray([0.0, 0.0], dtype=np.float32))

        permutation = np.asarray([3, 0, 2, 1])
        replay = adapter.materialize_xgboost_units(
            _manifest(), X[permutation], y[permutation],
            unit_ids=[units[index] for index in permutation])
        self.assertEqual(
            materialized.features.tobytes(), replay.features.tobytes())
        self.assertEqual(materialized.target.tobytes(), replay.target.tobytes())

        all_invalid = adapter.materialize_xgboost_units(
            _manifest(), X[:3], np.asarray([1.0, 0.0, 1.0]),
            unit_ids=["", None, "x" * 5000])
        self.assertEqual(all_invalid.features.shape, (1, 2))
        self.assertEqual(all_invalid.target.tolist(), [1.0])

        numeric_ids = adapter.materialize_xgboost_units(
            _manifest(), X[:2], np.asarray([0.0, 1.0]), unit_ids=[7, "7"])
        self.assertEqual(numeric_ids.features.shape[0], 1)
        self.assertEqual(numeric_ids.target.tolist(), [0.0])

    def test_only_structurally_misaligned_patient_ids_fail(self):
        X, y, _ = _data()
        for units in (["one", "two"], np.asarray([["one", "two", "three"]])):
            with self.subTest(units=units):
                with self.assertRaises(ValueError):
                    adapter.materialize_xgboost_units(
                        _manifest(), X, y, unit_ids=units)

    def test_row_profile_forbids_identifiers_and_keeps_one_row_per_unit(self):
        manifest = _manifest()
        manifest["privacy"]["unit"] = "row"
        X, y, _ = _data()
        materialized = adapter.materialize_xgboost_units(manifest, X, y)
        self.assertEqual(materialized.privacy_unit, "row")
        with self.assertRaisesRegex(ValueError, "row-level"):
            adapter.materialize_xgboost_units(
                manifest, X, y, unit_ids=["a", "b", "c"])

    def test_row_units_have_order_independent_canonical_binary_records(self):
        manifest = _manifest()
        manifest["privacy"]["unit"] = "row"
        X = np.asarray([
            [np.nan, -0.0], [18.0, -1.0], [18.0, -1.0],
            [65.0, 1.0], [0.0, 0.0],
        ], dtype=np.float32)
        y = np.asarray([1.0, 0.0, 0.0, 1.0, -0.0], dtype=np.float32)
        permutation = np.asarray([3, 1, 4, 0, 2])
        first = adapter.materialize_xgboost_units(manifest, X, y)
        replay = adapter.materialize_xgboost_units(
            manifest, X[permutation], y[permutation])
        self.assertEqual(first.features.tobytes(), replay.features.tobytes())
        self.assertEqual(first.target.tobytes(), replay.target.tobytes())
        self.assertEqual(first.features.shape[0], 5)
        duplicate = np.all(
            first.features == np.asarray([18.0, -1.0], dtype=np.float32),
            axis=1) & (first.target == np.float32(0.0))
        self.assertEqual(int(duplicate.sum()), 2)
        record_keys = []
        for bins, outcome in zip(first._binned_features, first.target):
            normalized_outcome = np.asarray([outcome], dtype="<f4")
            normalized_outcome[
                normalized_outcome == np.float32(0.0)] = np.float32(0.0)
            record_keys.append(
                np.asarray(bins, dtype="<u4").tobytes() +
                normalized_outcome.tobytes())
        self.assertEqual(record_keys, sorted(record_keys))

        variant = X.copy()
        variant.view(np.uint32)[0, 0] = np.uint32(0x7FA12345)
        different_nan = adapter.materialize_xgboost_units(
            manifest, variant, y)
        self.assertEqual(first.features.tobytes(),
                         different_nan.features.tobytes())
        self.assertEqual(first.target.tobytes(),
                         different_nan.target.tobytes())

    def test_private_numeric_values_totalize_but_structural_shapes_fail(self):
        X, y, units = _data()
        missing = X.copy(); missing[0, 0] = np.nan
        materialized = adapter.materialize_xgboost_units(
            _manifest(), missing, y, unit_ids=units)
        self.assertEqual(int(np.isnan(materialized.features[:, 0]).sum()), 1)

        private_values = np.asarray([
            [np.inf, -np.inf], [-np.inf, np.inf],
            [101.0, -6.0], [np.nan, np.nan],
        ], dtype=np.float64)
        private_target = np.asarray(
            [0.5, np.nan, np.inf, -np.inf], dtype=np.float64)
        totalized = adapter.materialize_xgboost_units(
            _manifest(), private_values, private_target,
            unit_ids=["a", "b", "c", "d"])
        self.assertTrue(bool(np.all(
            np.isnan(totalized.features) |
            ((totalized.features >= np.asarray([0.0, -5.0])) &
             (totalized.features <= np.asarray([100.0, 5.0]))))))
        self.assertEqual(int(np.isnan(totalized.features).sum()), 2)
        self.assertEqual(sorted(totalized.target.tolist()), [0.0, 0.0, 0.0, 1.0])

        regression = adapter.materialize_xgboost_units(
            _manifest("regression"), X[:3],
            np.asarray([-np.inf, np.nan, np.inf]),
            unit_ids=["a", "b", "c"])
        self.assertEqual(sorted(regression.target.tolist()), [-10.0, 0.0, 10.0])

        cases = [
            (X[:, :1], y, units, "feature"),
            (X, y[:2], units, "row"),
        ]
        for bad_x, bad_y, bad_units, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    adapter.materialize_xgboost_units(
                        _manifest(), bad_x, bad_y, unit_ids=bad_units)

        manifest = _manifest()
        manifest["resources"]["max_rows"] = 2
        with mock.patch.object(
                adapter, "_numeric_array",
                side_effect=AssertionError("copied before preflight")) as copy_array:
            with self.assertRaisesRegex(ValueError, "row ceiling"):
                adapter.materialize_xgboost_units(
                    manifest, X, y, unit_ids=units)
            copy_array.assert_not_called()

        budget = _manifest()["resources"]["memory_mib"] * 1024 * 1024
        with mock.patch.object(
                adapter, "_native_training_peak_bytes", return_value=budget):
            admitted = adapter.materialize_xgboost_units(
                _manifest(), X, y, unit_ids=units)
            self.assertEqual(admitted.features.shape[1], 2)

        with mock.patch.object(
                adapter, "_native_training_peak_bytes",
                return_value=budget + 1), mock.patch.object(
                    adapter, "_numeric_array",
                    side_effect=AssertionError("copied before full preflight")) \
                as copy_array:
            with self.assertRaisesRegex(ValueError, "complete native training"):
                adapter.materialize_xgboost_units(
                    _manifest(), X, y, unit_ids=units)
            copy_array.assert_not_called()

    def test_complete_native_peak_pins_core_sampler_and_resident_buffers(self):
        manifest = _manifest()
        profile = adapter.canonical_xgboost_profile(manifest)
        observed = adapter._native_training_peak_bytes(
            3, 2, "patient", profile, manifest["resources"])
        frontier = 4
        cells = frontier * ((3 + 2) + (3 + 2))
        coordinates = 2 * (cells + 1)
        cuts = 6
        core = (
            16 * cells + 32 * coordinates + 64 * frontier + 4 * 3 +
            16 * 3 + 4 * cuts + 1024 * 1024)
        materialization = adapter._materialization_peak_bytes(3, 2, "patient")
        dataset = 3 * (24 * 2 + 96) + 8 * 4
        artifact = 8 * manifest["resources"]["max_artifact_bytes"]
        subtotal = core + materialization + dataset + artifact
        self.assertEqual(
            observed, subtotal + subtotal // 4 + 16 * 1024 * 1024)

    def test_empty_and_all_invalid_inputs_become_trainable_sentinel_units(self):
        for task in ("binary_classification", "regression"):
            with self.subTest(task=task):
                empty = adapter.materialize_xgboost_units(
                    _manifest(task), np.empty((0, 2), dtype=np.float64),
                    np.empty((0,), dtype=np.float64), unit_ids=[])
                self.assertEqual(empty.features.shape, (1, 2))
                self.assertTrue(bool(np.all(np.isnan(empty.features))))
                self.assertEqual(empty.target.tolist(), [0.0])

        invalid = adapter.materialize_xgboost_units(
            _manifest(),
            np.asarray([[np.nan, np.inf], [np.nan, -np.inf]]),
            np.asarray([np.nan, np.inf]), unit_ids=[None, ""])
        self.assertEqual(invalid.features.shape, (1, 2))
        self.assertTrue(bool(np.isnan(invalid.features[0, 0])))
        self.assertEqual(invalid.features[0, 1], np.float32(0.0))
        self.assertEqual(invalid.target.tolist(), [0.0])


class XGBoostPrfAndBoundaryTests(unittest.TestCase):
    @staticmethod
    def _bundle(digest):
        return xgboost_bundle.TrustedXGBoostBundle(
            token=xgboost_bundle._CONSTRUCTION_TOKEN,
            bundle_sha256=digest,
            xgboost=object(), dp_primitives=object(),
        )

    def _prepare(self, manifest=None, X=None, y=None, units=None,
                 bundle_sha256="f" * 64):
        if X is None:
            X, y, units = _data()
        with mock.patch.object(seeding, "_node_secret", return_value=b"k" * 32):
            return adapter.prepare_xgboost_training(
                manifest or _manifest(), X, y,
                native_bundle=self._bundle(bundle_sha256), unit_ids=units)

    def test_prf_is_sticky_private_bound_and_resource_independent(self):
        first = self._prepare()
        replay = self._prepare()
        self.assertEqual(first._noise_key, replay._noise_key)
        self.assertEqual(len(first._noise_key), 32)
        self.assertNotIn(first._noise_key.hex(), repr(first))
        self.assertNotIn("semantic", repr(first).lower())
        public_view = json.dumps({
            "manifest": first.manifest,
            "native_parameters": first.native_parameters,
            "profile": first.profile,
        }, sort_keys=True)
        self.assertNotIn("semantic_query", public_view)
        self.assertNotIn("noise_key", public_view)

        manifest = _manifest()
        manifest["resources"].update({
            "threads": 1,
            "memory_mib": 8192,
            "wall_seconds": 1800,
            "max_rows": 2000,
            "max_features": 16,
            "max_trees": 100,
            "max_depth": 16,
            "max_bins": 32,
            "max_artifact_bytes": 4 * 1024 * 1024,
        })
        self.assertEqual(
            first._noise_key, self._prepare(manifest=manifest)._noise_key)

        renamed_scope = _manifest()
        renamed_scope["data_scope"]["snapshot_hash"] = "c" * 64
        renamed_scope["data_scope"]["cohort_hash"] = "d" * 64
        self.assertEqual(
            first._noise_key,
            self._prepare(manifest=renamed_scope)._noise_key)

        renamed_levels = copy.deepcopy(_manifest())
        renamed_levels["public_schema"]["target"]["levels"] = [
            {"type": "string", "value": "negative"},
            {"type": "string", "value": "positive"},
        ]
        schema_core = {
            name: value for name, value in
            renamed_levels["public_schema"].items() if name != "sha256"
        }
        schema_wire = json.dumps(
            schema_core, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        renamed_levels["public_schema"]["sha256"] = hashlib.sha256(
            schema_wire).hexdigest()
        renamed_levels["data_scope"]["schema_hash"] = renamed_levels[
            "public_schema"]["sha256"]
        renamed_prepared = self._prepare(manifest=renamed_levels)
        self.assertEqual(first.profile, renamed_prepared.profile)
        self.assertEqual(first._noise_key, renamed_prepared._noise_key)

        wider_features = copy.deepcopy(_manifest())
        wider_features["public_schema"]["lower"][0] = -1.0
        wider_features["public_schema"]["upper"][0] = 101.0
        schema_core = {
            name: value for name, value in
            wider_features["public_schema"].items() if name != "sha256"
        }
        schema_wire = json.dumps(
            schema_core, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        wider_features["public_schema"]["sha256"] = hashlib.sha256(
            schema_wire).hexdigest()
        wider_features["data_scope"]["schema_hash"] = wider_features[
            "public_schema"]["sha256"]
        self.assertNotEqual(
            first.profile, self._prepare(manifest=wider_features).profile)
        self.assertEqual(
            first._noise_key,
            self._prepare(manifest=wider_features)._noise_key)

        regression = _manifest("regression")
        regression_first = self._prepare(manifest=regression)
        wider_target = copy.deepcopy(regression)
        wider_target["public_schema"]["target"]["lower"] = -11.0
        wider_target["public_schema"]["target"]["upper"] = 11.0
        schema_core = {
            name: value for name, value in
            wider_target["public_schema"].items() if name != "sha256"
        }
        schema_wire = json.dumps(
            schema_core, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        wider_target["public_schema"]["sha256"] = hashlib.sha256(
            schema_wire).hexdigest()
        wider_target["data_scope"]["schema_hash"] = wider_target[
            "public_schema"]["sha256"]
        regression_wider = self._prepare(manifest=wider_target)
        self.assertEqual(
            regression_first.profile["base_score"],
            regression_wider.profile["base_score"])
        self.assertEqual(
            regression_first._noise_key, regression_wider._noise_key)

        same_native = _manifest()
        same_native["engine_params"]["learning_rate"]["value"] = \
            math.nextafter(0.1, math.inf)
        self.assertEqual(
            first._noise_key, self._prepare(manifest=same_native)._noise_key)

        changed_native = _manifest()
        changed_native["engine_params"]["learning_rate"]["value"] = 0.11
        self.assertNotEqual(
            first._noise_key, self._prepare(manifest=changed_native)._noise_key)

        X, y, units = _data()
        same_bin_x = X.copy(); same_bin_x[0, 0] = 64.0
        self.assertEqual(
            first._noise_key,
            self._prepare(X=same_bin_x, y=y, units=units)._noise_key)
        changed_bin_x = X.copy(); changed_bin_x[0, 0] = 39.0
        self.assertNotEqual(
            first._noise_key,
            self._prepare(X=changed_bin_x, y=y, units=units)._noise_key)
        changed_units = list(units)
        changed_units[0] = "different-patient"
        self.assertEqual(
            first._noise_key,
            self._prepare(X=X, y=y, units=changed_units)._noise_key)

        with mock.patch.object(
                seeding, "_runtime_fingerprint",
                side_effect=AssertionError("unrelated runtime fingerprint used")):
            self.assertEqual(first._noise_key, self._prepare()._noise_key)
        with mock.patch.object(
                adapter, "EXECUTION_PROFILE",
                "dsflower-xgboost-execution-v2-test"):
            self.assertNotEqual(first._noise_key, self._prepare()._noise_key)

        permutation = np.asarray([2, 0, 1])
        self.assertEqual(
            first._noise_key,
            self._prepare(
                X=X[permutation], y=y[permutation],
                units=[units[index] for index in permutation])._noise_key)
        missing_x = X.copy(); missing_x[0, 0] = np.nan
        zero_x = X.copy(); zero_x[0, 0] = 0.0
        self.assertNotEqual(
            self._prepare(X=missing_x, y=y, units=units)._noise_key,
            self._prepare(X=zero_x, y=y, units=units)._noise_key)
        changed_manifest = _manifest()
        changed_manifest["privacy"]["epsilon"] = 2.0
        self.assertNotEqual(
            first._noise_key,
            self._prepare(manifest=changed_manifest)._noise_key)

        same_pins = _manifest()
        same_pins["privacy"]["epsilon"] = math.nextafter(1.0, math.inf)
        self.assertEqual(
            first.profile["root_noise_scale"],
            self._prepare(manifest=same_pins).profile["root_noise_scale"])
        self.assertEqual(
            first._noise_key, self._prepare(manifest=same_pins)._noise_key)

        self.assertNotEqual(
            first._noise_key,
            self._prepare(bundle_sha256="e" * 64)._noise_key)
        X, y, units = _data()
        for untrusted in (None, object(), "f" * 64):
            with self.subTest(untrusted=type(untrusted).__name__), \
                    mock.patch.object(
                        seeding, "_node_secret",
                        side_effect=AssertionError("secret touched before bundle")), \
                    self.assertRaisesRegex(ValueError, "verified native"):
                adapter.prepare_xgboost_training(
                    _manifest(), X, y, unit_ids=units,
                    native_bundle=untrusted)

    def test_row_prf_is_permutation_and_nan_payload_invariant(self):
        manifest = _manifest()
        manifest["privacy"]["unit"] = "row"
        X = np.asarray([
            [np.nan, -0.0], [18.0, -1.0], [18.0, -1.0],
            [65.0, 1.0], [0.0, 0.0],
        ], dtype=np.float32)
        y = np.asarray([1.0, 0.0, 0.0, 1.0, -0.0], dtype=np.float32)
        first = self._prepare(manifest=manifest, X=X, y=y, units=None)
        permutation = np.asarray([3, 1, 4, 0, 2])
        replay = self._prepare(
            manifest=manifest, X=X[permutation], y=y[permutation],
            units=None)
        self.assertEqual(first._noise_key, replay._noise_key)
        np.testing.assert_array_equal(first.features, replay.features)
        np.testing.assert_array_equal(first.target, replay.target)

        alternate_nan = X.copy()
        alternate_nan.view(np.uint32)[0, 0] = np.uint32(0x7FA12345)
        self.assertEqual(
            first._noise_key,
            self._prepare(
                manifest=manifest, X=alternate_nan, y=y,
                units=None)._noise_key)

        changed = X.copy()
        changed[1, 0] = np.float32(19.0)
        self.assertNotEqual(
            first._noise_key,
            self._prepare(
                manifest=manifest, X=changed, y=y,
                units=None)._noise_key)

    def test_patient_prf_binds_only_effective_bounded_records(self):
        raw_x = np.asarray([
            [10.0, -5.0], [30.0, 5.0], [60.0, 0.0], [70.0, 1.0],
        ], dtype=np.float64)
        raw_y = np.asarray([0.0, 1.0, 1.0, 1.0], dtype=np.float64)
        raw_units = ["patient", " patient ", None, "<NA>"]
        first = self._prepare(X=raw_x, y=raw_y, units=raw_units)

        effective = self._prepare(
            X=np.asarray([[20.0, 0.0], [65.0, 0.5]], dtype=np.float64),
            y=np.asarray([0.0, 1.0], dtype=np.float64),
            units=["effective-a", "effective-b"])
        self.assertEqual(first._noise_key, effective._noise_key)
        self.assertNotIn("rows", repr(first))

        permutation = np.asarray([3, 1, 0, 2])
        replay = self._prepare(
            X=raw_x[permutation], y=raw_y[permutation],
            units=[raw_units[index] for index in permutation])
        self.assertEqual(first._noise_key, replay._noise_key)
        np.testing.assert_array_equal(first.features, replay.features)
        np.testing.assert_array_equal(first.target, replay.target)

        duplicate_x = np.vstack((raw_x, np.asarray([[20.0, 0.0]])))
        duplicate_y = np.append(raw_y, 0.0)
        duplicate_units = raw_units + ["patient"]
        duplicate = self._prepare(
            X=duplicate_x, y=duplicate_y, units=duplicate_units)
        self.assertEqual(first._noise_key, duplicate._noise_key)

        equivalent_x = raw_x.copy()
        equivalent_x[0, 0] = 11.0
        equivalent_x[1, 0] = 29.0
        equivalent_units = ["patient", "patient", "", "x" * 5000]
        equivalent = self._prepare(
            X=equivalent_x, y=raw_y, units=equivalent_units)
        self.assertEqual(first._noise_key, equivalent._noise_key)

        outlier_x = raw_x.copy()
        outlier_x[3, 0] = np.inf
        clipped_x = raw_x.copy()
        clipped_x[3, 0] = 100.0
        self.assertEqual(
            self._prepare(
                X=outlier_x, y=raw_y, units=raw_units)._noise_key,
            self._prepare(
                X=clipped_x, y=raw_y, units=raw_units)._noise_key)

        changed_x = raw_x.copy()
        changed_x[1, 1] = 1.0
        self.assertNotEqual(
            first._noise_key,
            self._prepare(
                X=changed_x, y=raw_y, units=raw_units)._noise_key)

    def test_training_boundary_delegates_only_a_prepared_verified_request(self):
        prepared = self._prepare()
        with mock.patch.object(
                adapter.xgboost_native, "train",
                return_value=b'{"safe":true}') as train:
            self.assertEqual(
                adapter.train_xgboost_native(prepared), b'{"safe":true}')
        train.assert_called_once_with(prepared)
        with self.assertRaises(ValueError):
            adapter.train_xgboost_native(object())
        with self.assertRaises(TypeError):
            adapter.train_xgboost_native(prepared, native_status="forged")

    def test_prepared_public_views_cannot_change_the_native_request(self):
        prepared = self._prepare()
        profile = prepared.profile
        profile["max_depth"] = 30
        parameters = prepared.native_parameters
        parameters["updater"] = "grow_histmaker"
        self.assertEqual(prepared.profile["max_depth"], 3)
        self.assertEqual(
            prepared.native_parameters["updater"],
            "grow_dsflower_dp_hist")
        with self.assertRaises(ValueError):
            prepared.features[0, 0] = np.float32(1.0)
        with self.assertRaises(AttributeError):
            prepared.profile = profile
        with self.assertRaises(AttributeError):
            prepared.num_boost_round = 999

    def test_no_seed_round_or_callbacks_cross_native_parameters(self):
        prepared = self._prepare()
        forbidden = {
            "seed", "random_state", "noise_key", "round", "server_round",
            "callback", "custom_objective", "eval_metric", "eval_set",
        }
        self.assertFalse(forbidden.intersection(prepared.native_parameters))
        self.assertEqual(prepared.num_boost_round, 4)
        self.assertEqual(prepared.native_parameters["updater"],
                         "grow_dsflower_dp_hist")


class XGBoostEnsembleTests(unittest.TestCase):
    @staticmethod
    def _artifact(leaf, task="binary_classification"):
        try:
            from .test_xgboost_sanitizer import (
                _model, _bytes, _public_threshold)
        except ImportError:  # Direct execution from this test directory.
            from test_xgboost_sanitizer import (
                _model, _bytes, _public_threshold)
        model = _model()
        if task == "regression":
            model["learner"]["learner_model_param"]["base_score"] = "[0E0]"
            model["learner"]["objective"]["name"] = "reg:squarederror"
        tree = model["learner"]["gradient_booster"]["model"]["trees"][0]
        tree["split_conditions"][0] = _public_threshold(18.0)
        tree["split_conditions"][1] = -abs(float(leaf))
        tree["split_conditions"][2] = abs(float(leaf))
        return _bytes(model)

    def test_container_is_canonical_order_independent_and_has_no_node_ids(self):
        manifest = _manifest()
        manifest["engine_params"]["num_boost_round"]["value"] = 1
        manifest["engine_params"]["max_depth"]["value"] = 1
        first = self._artifact(0.1)
        second = self._artifact(0.2)
        encoded, digest = adapter.build_xgboost_ensemble(
            manifest, [second, first])
        replay, replay_digest = adapter.build_xgboost_ensemble(
            manifest, [first, second])
        self.assertEqual(encoded, replay)
        self.assertEqual(digest, replay_digest)
        self.assertEqual(hashlib.sha256(encoded).hexdigest(), digest)
        parsed = json.loads(encoded)
        self.assertEqual(set(parsed), {
            "aggregation", "contract", "engine", "models",
            "public_schema_sha256", "task", "version"})
        self.assertEqual(parsed["contract"], "dsflower-xgboost-ensemble-v1")
        self.assertEqual(parsed["engine"], "xgboost")
        self.assertEqual(parsed["aggregation"], "mean_prediction")
        self.assertEqual(
            parsed["public_schema_sha256"],
            manifest["public_schema"]["sha256"])
        self.assertEqual(parsed["task"], "binary")
        self.assertEqual(parsed["version"], 1)
        self.assertEqual(len(parsed["models"]), 2)
        self.assertNotIn(b"node_id", encoded.lower())
        self.assertNotIn(b"semantic_query", encoded.lower())
        self.assertNotIn(b"pickle", encoded.lower())

        regression = _manifest("regression")
        regression["engine_params"]["num_boost_round"]["value"] = 1
        regression["engine_params"]["max_depth"]["value"] = 1
        regression_encoded, _digest = adapter.build_xgboost_ensemble(
            regression, [self._artifact(0.1, "regression")])
        self.assertEqual(json.loads(regression_encoded)["task"], "regression")

    def test_empty_or_unsanitizable_member_fails(self):
        with self.assertRaises(ValueError):
            adapter.build_xgboost_ensemble(_manifest(), [])
        with self.assertRaises(ValueError):
            adapter.build_xgboost_ensemble(_manifest(), [b"pickle"])

        manifest = _manifest()
        manifest["engine_params"]["num_boost_round"]["value"] = 1
        manifest["engine_params"]["max_depth"]["value"] = 1
        manifest["engine_params"]["max_delta_step"]["value"] = 1.0
        with self.assertRaisesRegex(ValueError, "leaf"):
            adapter.build_xgboost_ensemble(manifest, [self._artifact(0.2)])


if __name__ == "__main__":
    unittest.main()
