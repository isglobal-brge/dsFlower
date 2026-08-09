"""Minimal ctypes execution boundary for the verified XGBoost DP fork."""

from __future__ import annotations

import ctypes as ct
import hashlib
import hmac
import json
import math
import threading

import numpy as np

from .xgboost_bundle import is_verified_bundle
from .xgboost_sanitizer import sanitize_xgboost_json


_PRIVACY_CONTEXT_MECHANISM = b"xgboost/fixed-point-discrete/v1"
_GLOBAL_CONFIG = b'{"verbosity":0}'
_NUMERIC_ABS_CAP = 1.0e12
_SAVE_JSON_CONFIG = b'{"format":"json"}'
_ERROR_CODES = frozenset((
    "invalid_input", "resource_exhausted", "unsupported", "internal_error",
))
_TRAINING_LOCK = threading.Lock()


class NativeXGBoostError(RuntimeError):
    """Bounded execution error with no native text, path or private value."""

    def __init__(self, code="internal_error"):
        self.code = code if code in _ERROR_CODES else "internal_error"
        super().__init__("native XGBoost training failed")


class PrivacyContext(ct.Structure):
    """Exact XGBDsFlowerPrivacyContext ABI v3 layout."""

    _fields_ = [
        ("struct_size", ct.c_uint32),
        ("abi_version", ct.c_uint32),
        ("noise_key", ct.POINTER(ct.c_ubyte)),
        ("noise_key_size", ct.c_size_t),
        ("mechanism_id", ct.c_char_p),
        ("privacy_unit", ct.c_char_p),
        ("adjacency", ct.c_char_p),
        ("unit_canonicalization", ct.c_char_p),
        ("contribution_strategy", ct.c_char_p),
        ("gradient_clip", ct.c_double),
        ("hessian_clip", ct.c_double),
        ("max_rows_per_unit", ct.c_uint64),
        ("dmatrix", ct.c_void_p),
        ("task_id", ct.c_char_p),
        ("objective", ct.c_char_p),
        ("target_lower_bound", ct.c_double),
        ("target_upper_bound", ct.c_double),
        ("base_score", ct.c_double),
        ("max_trees", ct.c_uint64),
        ("max_depth", ct.c_uint64),
        ("feature_lower_bounds", ct.POINTER(ct.c_double)),
        ("feature_lower_bounds_size", ct.c_size_t),
        ("feature_upper_bounds", ct.POINTER(ct.c_double)),
        ("feature_upper_bounds_size", ct.c_size_t),
        ("cut_ptrs", ct.POINTER(ct.c_uint64)),
        ("cut_ptrs_size", ct.c_size_t),
        ("cut_values", ct.POINTER(ct.c_double)),
        ("cut_values_size", ct.c_size_t),
        ("fixed_point_scale", ct.c_uint64),
        ("root_noise_scale", ct.c_uint64),
        ("level_noise_scale", ct.c_uint64),
    ]


def _configure_api(library):
    library.XGBSetGlobalConfig.argtypes = [ct.c_char_p]
    library.XGBSetGlobalConfig.restype = ct.c_int
    library.XGBDsFlowerSetPrivacyContext.argtypes = [ct.POINTER(PrivacyContext)]
    library.XGBDsFlowerSetPrivacyContext.restype = ct.c_int
    library.XGBDsFlowerClearPrivacyContext.argtypes = []
    library.XGBDsFlowerClearPrivacyContext.restype = ct.c_int
    library.XGBDsFlowerPrivacyContextReady.argtypes = [ct.POINTER(ct.c_int)]
    library.XGBDsFlowerPrivacyContextReady.restype = ct.c_int
    library.XGDMatrixCreateFromMat.argtypes = [
        ct.POINTER(ct.c_float), ct.c_uint64, ct.c_uint64, ct.c_float,
        ct.POINTER(ct.c_void_p),
    ]
    library.XGDMatrixCreateFromMat.restype = ct.c_int
    library.XGDMatrixSetFloatInfo.argtypes = [
        ct.c_void_p, ct.c_char_p, ct.POINTER(ct.c_float), ct.c_uint64,
    ]
    library.XGDMatrixSetFloatInfo.restype = ct.c_int
    library.XGDMatrixFree.argtypes = [ct.c_void_p]
    library.XGDMatrixFree.restype = ct.c_int
    library.XGBoosterCreate.argtypes = [
        ct.POINTER(ct.c_void_p), ct.c_uint64, ct.POINTER(ct.c_void_p),
    ]
    library.XGBoosterCreate.restype = ct.c_int
    library.XGBoosterSetParam.argtypes = [
        ct.c_void_p, ct.c_char_p, ct.c_char_p,
    ]
    library.XGBoosterSetParam.restype = ct.c_int
    library.XGBoosterUpdateOneIter.argtypes = [
        ct.c_void_p, ct.c_int, ct.c_void_p,
    ]
    library.XGBoosterUpdateOneIter.restype = ct.c_int
    library.XGBoosterSaveModelToBuffer.argtypes = [
        ct.c_void_p, ct.c_char_p, ct.POINTER(ct.c_uint64),
        ct.POINTER(ct.c_char_p),
    ]
    library.XGBoosterSaveModelToBuffer.restype = ct.c_int
    library.XGBoosterFree.argtypes = [ct.c_void_p]
    library.XGBoosterFree.restype = ct.c_int


def _call(function, *arguments, code="internal_error"):
    try:
        status = function(*arguments)
    except Exception:
        raise NativeXGBoostError(code) from None
    if status != 0:
        raise NativeXGBoostError(code)


def _parameter_text(value):
    if isinstance(value, bool):
        return b"1" if value else b"0"
    if type(value) is int:
        return str(value).encode("ascii")
    if type(value) is float and math.isfinite(value):
        return repr(value).encode("ascii")
    if isinstance(value, str) and value and value.isascii():
        return value.encode("ascii")
    raise NativeXGBoostError("invalid_input")


def _fixed_parameters(manifest, profile):
    """Reconstruct the sole native parameter map accepted by this ABI."""
    return {
        "base_score": profile["base_score"],
        "booster": "gbtree",
        "boost_from_average": 0,
        "colsample_bylevel": 1.0,
        "colsample_bynode": 1.0,
        "colsample_bytree": 1.0,
        "device": "cpu",
        "disable_default_eval_metric": 1,
        "grow_policy": "depthwise",
        "learning_rate": profile["learning_rate"],
        "max_bin": profile["max_bin"],
        "max_delta_step": profile["max_delta_step"],
        "max_depth": profile["max_depth"],
        "max_leaves": 0,
        "min_child_weight": profile["min_child_weight"],
        "min_split_loss": profile["min_split_loss"],
        "multi_strategy": "one_output_per_tree",
        "nthread": manifest["resources"]["threads"],
        "num_class": 0,
        "num_parallel_tree": 1,
        "num_target": 1,
        "objective": profile["objective"],
        "process_type": "default",
        "reg_alpha": profile["reg_alpha"],
        "reg_lambda": profile["reg_lambda"],
        "sampling_method": "uniform",
        "subsample": 1.0,
        "tree_method": "hist",
        "updater": "grow_dsflower_dp_hist",
        "validate_parameters": True,
        "verbosity": 0,
    }


def _exact_parameters(actual, expected):
    return type(actual) is dict and actual.keys() == expected.keys() and all(
        type(actual[name]) is type(value) and actual[name] == value
        for name, value in expected.items()
    )


def request_sha256(manifest, profile, parameters):
    """Seal every non-array value consumed by the native execution path."""
    wire = json.dumps(
        {"manifest": manifest, "parameters": parameters, "profile": profile},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(wire).hexdigest()


def fixed_sanitizer_arguments(manifest, profile):
    """Derive the sanitizer contract from the sealed native request only."""
    theoretical_nodes = profile["num_boost_round"] * (
        (1 << (profile["max_depth"] + 1)) - 1)
    byte_ceiling = manifest["resources"]["max_artifact_bytes"]
    return {
        "expected_task": manifest["task"],
        "expected_features": len(manifest["public_schema"]["features"]),
        "expected_trees": profile["num_boost_round"],
        "expected_max_depth": profile["max_depth"],
        "public_cuts": profile["public_cuts"],
        "expected_base_score": profile["base_score"],
        "max_total_nodes": min(theoretical_nodes, byte_ceiling),
        "max_artifact_bytes": byte_ceiling,
        "numeric_abs_cap": _NUMERIC_ABS_CAP,
        "leaf_abs_cap": profile["leaf_abs_cap"],
    }


def _context(prepared, matrix, key_buffer):
    profile = prepared._profile
    manifest = prepared._manifest
    lower_values = tuple(float(value) for value in profile["feature_lower"])
    upper_values = tuple(float(value) for value in profile["feature_upper"])
    cuts = []
    cut_ptr_values = [0]
    for feature in profile["public_cuts"]:
        cuts.extend(float(value) for value in feature)
        cut_ptr_values.append(len(cuts))
    lower = (ct.c_double * len(lower_values))(*lower_values)
    upper = (ct.c_double * len(upper_values))(*upper_values)
    cut_ptrs = (ct.c_uint64 * len(cut_ptr_values))(*cut_ptr_values)
    cut_values = (ct.c_double * len(cuts))(*cuts)
    task = manifest["task"].encode("ascii")
    objective = profile["objective"].encode("ascii")
    privacy_unit = manifest["privacy"]["unit"].encode("ascii")
    context = PrivacyContext(
        ct.sizeof(PrivacyContext),
        3,
        ct.cast(key_buffer, ct.POINTER(ct.c_ubyte)),
        len(key_buffer),
        _PRIVACY_CONTEXT_MECHANISM,
        privacy_unit,
        b"replace_one",
        b"trim-utf8-v2",
        b"one-record-per-unit-v1",
        float(profile["gradient_clip"]),
        float(profile["hessian_clip"]),
        1,
        matrix,
        task,
        objective,
        float(profile["target_lower"]),
        float(profile["target_upper"]),
        float(profile["base_score"]),
        int(profile["num_boost_round"]),
        int(profile["max_depth"]),
        lower,
        len(lower_values),
        upper,
        len(upper_values),
        cut_ptrs,
        len(cut_ptr_values),
        cut_values,
        len(cuts),
        int(profile["fixed_point_scale"]),
        int(profile["root_noise_scale"]),
        int(profile["level_noise_scale"]),
    )
    keepalive = (
        key_buffer, lower, upper, cut_ptrs, cut_values, task, objective,
        privacy_unit,
    )
    return context, keepalive


def _best_effort(function, *arguments):
    try:
        function(*arguments)
    except Exception:
        pass


def _wipe(value):
    if isinstance(value, bytearray):
        value[:] = b"\x00" * len(value)


def _train_once(prepared):
    bundle = getattr(prepared, "_native_bundle", None)
    noise_key = getattr(prepared, "_noise_key", None)
    manifest = getattr(prepared, "_manifest", None)
    profile = getattr(prepared, "_profile", None)
    parameters = getattr(prepared, "_native_parameters", None)
    bundle_sha256 = getattr(prepared, "_native_bundle_sha256", None)
    request_digest = getattr(prepared, "_request_sha256", None)
    if getattr(prepared, "_sealed", False) is not True or \
            not is_verified_bundle(bundle) or \
            not isinstance(noise_key, bytearray) or len(noise_key) != 32 or \
            not any(noise_key) or type(manifest) is not dict or \
            type(profile) is not dict or \
            not isinstance(bundle_sha256, str) or \
            not hmac.compare_digest(bundle.bundle_sha256, bundle_sha256):
        raise NativeXGBoostError("invalid_input")
    try:
        actual_digest = request_sha256(manifest, profile, parameters)
        expected_parameters = _fixed_parameters(manifest, profile)
        sanitizer_arguments = fixed_sanitizer_arguments(manifest, profile)
    except (KeyError, TypeError, ValueError):
        _wipe(noise_key)
        raise NativeXGBoostError("invalid_input") from None
    if not isinstance(request_digest, str) or not hmac.compare_digest(
            request_digest, actual_digest) or \
            not _exact_parameters(parameters, expected_parameters):
        _wipe(noise_key)
        raise NativeXGBoostError("invalid_input")
    library = bundle._xgboost
    features = getattr(prepared, "_features", None)
    target = getattr(prepared, "_target", None)
    if type(features) is not np.ndarray or features.dtype != np.float32 or \
            features.ndim != 2 or not features.flags.c_contiguous or \
            type(target) is not np.ndarray or target.dtype != np.float32 or \
            target.ndim != 1 or not target.flags.c_contiguous or \
            target.shape[0] != features.shape[0]:
        _wipe(noise_key)
        raise NativeXGBoostError("invalid_input")

    matrix = ct.c_void_p()
    booster = ct.c_void_p()
    context_attempted = False
    key_buffer = (ct.c_ubyte * len(noise_key)).from_buffer(noise_key)
    with _TRAINING_LOCK:
        try:
            _configure_api(library)
            _call(library.XGBSetGlobalConfig, _GLOBAL_CONFIG)
            _call(
                library.XGDMatrixCreateFromMat,
                features.ctypes.data_as(ct.POINTER(ct.c_float)),
                features.shape[0], features.shape[1], ct.c_float(math.nan),
                ct.byref(matrix), code="invalid_input",
            )
            _call(
                library.XGDMatrixSetFloatInfo, matrix, b"label",
                target.ctypes.data_as(ct.POINTER(ct.c_float)), target.shape[0],
                code="invalid_input",
            )
            context, keepalive = _context(prepared, matrix, key_buffer)
            if not keepalive:
                raise NativeXGBoostError()
            context_attempted = True
            _call(library.XGBDsFlowerSetPrivacyContext, ct.byref(context))
            ready = ct.c_int(0)
            _call(library.XGBDsFlowerPrivacyContextReady, ct.byref(ready))
            if ready.value != 1:
                raise NativeXGBoostError()

            matrices = (ct.c_void_p * 1)(matrix)
            _call(library.XGBoosterCreate, matrices, 1, ct.byref(booster))
            for name, value in sorted(parameters.items()):
                _call(
                    library.XGBoosterSetParam, booster,
                    name.encode("ascii"), _parameter_text(value),
                    code="invalid_input",
                )
            for iteration in range(profile["num_boost_round"]):
                _call(
                    library.XGBoosterUpdateOneIter,
                    booster, iteration, matrix,
                )

            length = ct.c_uint64(0)
            pointer = ct.c_char_p()
            _call(
                library.XGBoosterSaveModelToBuffer,
                booster, _SAVE_JSON_CONFIG, ct.byref(length), ct.byref(pointer),
            )
            byte_cap = sanitizer_arguments.get("max_artifact_bytes")
            if type(byte_cap) is not int or length.value < 1 or \
                    length.value > byte_cap or not pointer:
                raise NativeXGBoostError("resource_exhausted")
            raw = ct.string_at(pointer, length.value)
            try:
                sanitized, _digest = sanitize_xgboost_json(
                    raw, **sanitizer_arguments)
            except Exception:
                raise NativeXGBoostError("internal_error") from None
            return sanitized
        finally:
            # The context owns a native copy of the key.  Clear it before
            # releasing handles, then erase the sole Python derived-key buffer
            # regardless of which native or sanitizer operation failed.
            if context_attempted:
                _best_effort(library.XGBDsFlowerClearPrivacyContext)
            if booster.value:
                _best_effort(library.XGBoosterFree, booster)
            if matrix.value:
                _best_effort(library.XGDMatrixFree, matrix)
            _wipe(noise_key)


def train(prepared):
    """Train once and return only canonical sanitized XGBoost JSON bytes.

    ``prepared`` must carry the opaque bundle produced by
    :func:`load_verified_xgboost_bundle`.  The function is deliberately
    one-shot: its derived key is erased on success and on every failure.
    """
    try:
        return _train_once(prepared)
    except NativeXGBoostError:
        raise
    except Exception:
        raise NativeXGBoostError("internal_error") from None
    finally:
        _wipe(getattr(prepared, "_noise_key", None))


__all__ = [
    "NativeXGBoostError",
    "PrivacyContext",
    "fixed_sanitizer_arguments",
    "request_sha256",
    "train",
]
