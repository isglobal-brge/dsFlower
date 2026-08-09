"""Lifecycle tests for the trusted XGBoost ctypes execution boundary."""

import ctypes as ct
import math
import os
import sys
import threading
import time
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import xgboost_bundle
from dsflower_runner import xgboost_native as native


class _Function:
    def __init__(self, callback=lambda *_args: 0):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *arguments):
        return self.callback(*arguments)


class _FakeXGBoost:
    def __init__(self, *, raw=b"native-json", fail_update=None,
                 slow_updates=False):
        self.events = []
        self.parameters = []
        self.updates = []
        self.features = None
        self.labels = None
        self.context = None
        self.raw = raw
        self.fail_update = fail_update
        self.slow_updates = slow_updates
        self.active = 0
        self.max_active = 0
        self.active_lock = threading.Lock()
        self.buffer = ct.create_string_buffer(raw)

        def create_matrix(values, rows, columns, missing, output):
            self.events.append("matrix:create")
            count = int(rows) * int(columns)
            self.features = [float(values[index]) for index in range(count)]
            self.missing = float(missing.value)
            ct.cast(output, ct.POINTER(ct.c_void_p))[0] = 101
            return 0

        def global_config(config):
            self.events.append("global:config")
            self.global_config = config
            return 0

        def set_info(_matrix, name, values, length):
            self.events.append("matrix:labels")
            self.asserted_label_name = name
            self.labels = [float(values[index]) for index in range(int(length))]
            return 0

        def set_context(pointer):
            self.events.append("context:set")
            context = ct.cast(
                pointer, ct.POINTER(native.PrivacyContext)).contents
            self.context = {
                "abi": context.abi_version,
                "key": bytes(context.noise_key[:context.noise_key_size]),
                "mechanism": context.mechanism_id,
                "task": context.task_id,
                "objective": context.objective,
                "trees": context.max_trees,
                "depth": context.max_depth,
                "cuts": list(context.cut_values[:context.cut_values_size]),
                "cut_ptrs": list(context.cut_ptrs[:context.cut_ptrs_size]),
                "fixed": context.fixed_point_scale,
                "root": context.root_noise_scale,
                "level": context.level_noise_scale,
            }
            return 0

        def ready(output):
            self.events.append("context:ready")
            ct.cast(output, ct.POINTER(ct.c_int))[0] = 1
            return 0

        def create_booster(_matrices, _length, output):
            self.events.append("booster:create")
            ct.cast(output, ct.POINTER(ct.c_void_p))[0] = 202
            return 0

        def set_parameter(_booster, name, value):
            self.parameters.append((name.decode("ascii"), value.decode("ascii")))
            return 0

        def update(_booster, iteration, _matrix):
            self.events.append("booster:update")
            self.updates.append(iteration)
            with self.active_lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                if self.slow_updates:
                    time.sleep(0.01)
                return -1 if iteration == self.fail_update else 0
            finally:
                with self.active_lock:
                    self.active -= 1

        def save(_booster, config, length, output):
            self.events.append("booster:save")
            self.save_config = config
            ct.cast(length, ct.POINTER(ct.c_uint64))[0] = len(self.raw)
            ct.cast(output, ct.POINTER(ct.c_char_p))[0] = ct.cast(
                self.buffer, ct.c_char_p)
            return 0

        def clear():
            self.events.append("context:clear")
            return 0

        def free_booster(_booster):
            self.events.append("booster:free")
            return 0

        def free_matrix(_matrix):
            self.events.append("matrix:free")
            return 0

        self.XGBSetGlobalConfig = _Function(global_config)
        self.XGDMatrixCreateFromMat = _Function(create_matrix)
        self.XGDMatrixSetFloatInfo = _Function(set_info)
        self.XGBDsFlowerSetPrivacyContext = _Function(set_context)
        self.XGBDsFlowerPrivacyContextReady = _Function(ready)
        self.XGBoosterCreate = _Function(create_booster)
        self.XGBoosterSetParam = _Function(set_parameter)
        self.XGBoosterUpdateOneIter = _Function(update)
        self.XGBoosterSaveModelToBuffer = _Function(save)
        self.XGBDsFlowerClearPrivacyContext = _Function(clear)
        self.XGBoosterFree = _Function(free_booster)
        self.XGDMatrixFree = _Function(free_matrix)


def _trusted_bundle(library, digest="a" * 64):
    trusted = object.__new__(xgboost_bundle.TrustedXGBoostBundle)
    trusted._bundle_sha256 = digest
    trusted._xgboost = library
    trusted._dp_primitives = object()
    trusted._sealed = True
    return trusted


class _Prepared:
    def __init__(self, library):
        self._native_bundle = _trusted_bundle(library)
        self._native_bundle_sha256 = self._native_bundle.bundle_sha256
        self._noise_key = bytearray(range(1, 33))
        self._features = np.asarray([
            [math.nan, 2.0], [0.0, 3.0], [1.0, 4.0],
        ], dtype=np.float32)
        self._target = np.asarray([-1.0, 0.0, 1.0], dtype=np.float32)
        self._profile = {
            "feature_lower": [-2.0, 1.0],
            "feature_upper": [2.0, 5.0],
            "public_cuts": [[0.0, 1.0], [2.0]],
            "gradient_clip": 1.0,
            "hessian_clip": 1.0,
            "objective": "reg:squarederror",
            "target_lower": -1.0,
            "target_upper": 1.0,
            "base_score": 0.0,
            "leaf_abs_cap": 1.0,
            "num_boost_round": 2,
            "max_depth": 3,
            "max_bin": 3,
            "learning_rate": 0.1,
            "max_delta_step": 1.0,
            "min_child_weight": 1.0,
            "min_split_loss": 0.0,
            "reg_alpha": 0.0,
            "reg_lambda": 1.0,
            "fixed_point_scale": 1024,
            "root_noise_scale": 11,
            "level_noise_scale": 13,
        }
        self._manifest = {
            "task": "regression",
            "privacy": {"unit": "patient"},
            "public_schema": {"features": ["feature_0", "feature_1"]},
            "resources": {"threads": 2, "max_artifact_bytes": 1024},
        }
        self._native_parameters = native._fixed_parameters(
            self._manifest, self._profile)
        self._request_sha256 = native.request_sha256(
            self._manifest, self._profile, self._native_parameters)
        self._num_boost_round = 2
        self._sealed = True

    @property
    def manifest(self):
        return self._manifest

    @property
    def native_parameters(self):
        return self._native_parameters


class NativeXGBoostTests(unittest.TestCase):
    def test_full_lifecycle_preserves_nan_and_returns_only_sanitized_bytes(self):
        library = _FakeXGBoost()
        prepared = _Prepared(library)
        original_key = bytes(prepared._noise_key)
        with mock.patch.object(
                native, "sanitize_xgboost_json",
                return_value=(b'{"canonical":"safe"}', "f" * 64)) as sanitizer:
            result = native.train(prepared)
        self.assertEqual(result, b'{"canonical":"safe"}')
        sanitizer_arguments = native.fixed_sanitizer_arguments(
            prepared._manifest, prepared._profile)
        sanitizer.assert_called_once_with(
            library.raw, **sanitizer_arguments)
        self.assertTrue(math.isnan(library.features[0]))
        self.assertTrue(math.isnan(library.missing))
        self.assertEqual(library.labels, [-1.0, 0.0, 1.0])
        self.assertEqual(library.context, {
            "abi": 3,
            "key": original_key,
            "mechanism": b"xgboost/fixed-point-discrete/v1",
            "task": b"regression",
            "objective": b"reg:squarederror",
            "trees": 2,
            "depth": 3,
            "cuts": [0.0, 1.0, 2.0],
            "cut_ptrs": [0, 2, 3],
            "fixed": 1024,
            "root": 11,
            "level": 13,
        })
        self.assertEqual(library.updates, [0, 1])
        self.assertEqual(library.parameters, sorted(library.parameters))
        self.assertEqual(library.save_config, b'{"format":"json"}')
        self.assertEqual(library.global_config, b'{"verbosity":0}')
        self.assertEqual(library.events[-3:], [
            "context:clear", "booster:free", "matrix:free"])
        self.assertEqual(prepared._noise_key, bytearray(32))

    def test_native_and_sanitizer_failures_always_cleanup_and_are_bounded(self):
        for failure in ("update", "sanitizer"):
            library = _FakeXGBoost(fail_update=0 if failure == "update" else None)
            prepared = _Prepared(library)
            sanitizer = (
                mock.Mock(side_effect=ValueError("private/native/path"))
                if failure == "sanitizer" else mock.Mock()
            )
            with self.subTest(failure=failure), mock.patch.object(
                    native, "sanitize_xgboost_json", sanitizer), \
                    self.assertRaises(native.NativeXGBoostError) as raised:
                native.train(prepared)
            self.assertIn(raised.exception.code, {
                "internal_error", "resource_exhausted"})
            self.assertNotIn("private", str(raised.exception))
            self.assertIsNone(raised.exception.__cause__)
            self.assertEqual(library.events[-3:], [
                "context:clear", "booster:free", "matrix:free"])
            self.assertEqual(prepared._noise_key, bytearray(32))

    def test_every_ffi_stage_failure_wipes_the_key_and_stays_bounded(self):
        stages = (
            "XGBSetGlobalConfig",
            "XGDMatrixCreateFromMat",
            "XGDMatrixSetFloatInfo",
            "XGBDsFlowerSetPrivacyContext",
            "XGBDsFlowerPrivacyContextReady",
            "XGBoosterCreate",
            "XGBoosterSetParam",
            "XGBoosterUpdateOneIter",
            "XGBoosterSaveModelToBuffer",
        )
        for stage in stages:
            library = _FakeXGBoost()
            setattr(library, stage, _Function(lambda *_args: -1))
            prepared = _Prepared(library)
            with self.subTest(stage=stage), self.assertRaises(
                    native.NativeXGBoostError) as raised:
                native.train(prepared)
            self.assertIn(raised.exception.code, {
                "invalid_input", "internal_error"})
            self.assertEqual(prepared._noise_key, bytearray(32))
            self.assertEqual(
                str(raised.exception), "native XGBoost training failed")

    def test_early_untrusted_handle_and_cleanup_errors_still_wipe(self):
        library = _FakeXGBoost()
        prepared = _Prepared(library)
        prepared._native_bundle = object()
        with self.assertRaises(native.NativeXGBoostError):
            native.train(prepared)
        self.assertEqual(prepared._noise_key, bytearray(32))

        library = _FakeXGBoost()
        prepared = _Prepared(library)
        prepared._native_bundle = _trusted_bundle(library, "b" * 64)
        with self.assertRaises(native.NativeXGBoostError):
            native.train(prepared)
        self.assertEqual(prepared._noise_key, bytearray(32))

        library = _FakeXGBoost()
        library.XGBDsFlowerClearPrivacyContext = _Function(
            lambda: (_ for _ in ()).throw(RuntimeError("private")))
        library.XGBoosterFree = _Function(
            lambda *_args: (_ for _ in ()).throw(RuntimeError("private")))
        prepared = _Prepared(library)
        with mock.patch.object(
                native, "sanitize_xgboost_json",
                return_value=(b"safe", "f" * 64)):
            self.assertEqual(native.train(prepared), b"safe")
        self.assertEqual(prepared._noise_key, bytearray(32))

    def test_artifact_cap_is_checked_before_sanitization(self):
        library = _FakeXGBoost(raw=b"x" * 2048)
        prepared = _Prepared(library)
        with mock.patch.object(native, "sanitize_xgboost_json") as sanitizer, \
                self.assertRaises(native.NativeXGBoostError) as raised:
            native.train(prepared)
        self.assertEqual(raised.exception.code, "resource_exhausted")
        sanitizer.assert_not_called()
        self.assertEqual(prepared._noise_key, bytearray(32))

    def test_prepared_training_is_one_shot(self):
        library = _FakeXGBoost()
        prepared = _Prepared(library)
        with mock.patch.object(
                native, "sanitize_xgboost_json",
                return_value=(b"safe", "f" * 64)):
            self.assertEqual(native.train(prepared), b"safe")
            with self.assertRaises(native.NativeXGBoostError) as raised:
                native.train(prepared)
        self.assertEqual(raised.exception.code, "invalid_input")

    def test_parameter_map_is_exact_and_type_strict_before_ffi(self):
        for mutation in (
                "unknown", "changed", "bool_as_int", "coherent_profile"):
            library = _FakeXGBoost()
            prepared = _Prepared(library)
            if mutation == "unknown":
                prepared._native_parameters["callback"] = "forbidden"
            elif mutation == "changed":
                prepared._native_parameters["updater"] = "grow_histmaker"
            elif mutation == "bool_as_int":
                prepared._native_parameters["nthread"] = True
            else:
                prepared._profile["learning_rate"] = 0.2
                prepared._native_parameters["learning_rate"] = 0.2
            with self.subTest(mutation=mutation), self.assertRaises(
                    native.NativeXGBoostError) as raised:
                native.train(prepared)
            self.assertEqual(raised.exception.code, "invalid_input")
            self.assertEqual(library.events, [])
            self.assertEqual(prepared._noise_key, bytearray(32))

    def test_process_library_context_is_mutex_serialized(self):
        library = _FakeXGBoost(slow_updates=True)
        prepared = [_Prepared(library), _Prepared(library)]
        results = []

        def worker(item):
            try:
                results.append(native.train(item))
            except Exception as exc:  # pragma: no cover - asserted below
                results.append(exc)

        with mock.patch.object(
                native, "sanitize_xgboost_json",
                return_value=(b"safe", "f" * 64)):
            threads = [threading.Thread(target=worker, args=(item,))
                       for item in prepared]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(results, [b"safe", b"safe"])
        self.assertEqual(library.max_active, 1)


if __name__ == "__main__":
    unittest.main()
