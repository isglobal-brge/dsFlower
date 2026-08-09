#!/usr/bin/env python3
"""Exercise the curated deterministic fixed-point DP histogram core."""

from __future__ import annotations

import ctypes as ct
import importlib.util
import json
import math
import os
import struct
import sys
from pathlib import Path

from context_abi_smoke import (
    PrivacyContext,
    configure_api,
    last_error,
    make_matrix,
    set_labels,
    valid_context,
)


def configure_core_api(lib: ct.CDLL) -> None:
    configure_api(lib)
    lib.XGBoosterSaveModelToBuffer.argtypes = [
        ct.c_void_p,
        ct.c_char_p,
        ct.POINTER(ct.c_uint64),
        ct.POINTER(ct.c_char_p),
    ]
    lib.XGBoosterSerializeToBuffer.argtypes = [
        ct.c_void_p,
        ct.POINTER(ct.c_uint64),
        ct.POINTER(ct.c_char_p),
    ]
    lib.XGBoosterSaveJsonConfig.argtypes = [
        ct.c_void_p,
        ct.POINTER(ct.c_uint64),
        ct.POINTER(ct.c_char_p),
    ]
    lib.XGBoosterLoadModelFromBuffer.argtypes = [
        ct.c_void_p,
        ct.c_void_p,
        ct.c_uint64,
    ]
    lib.XGBoosterPredictFromDMatrix.argtypes = [
        ct.c_void_p,
        ct.c_void_p,
        ct.c_char_p,
        ct.POINTER(ct.POINTER(ct.c_uint64)),
        ct.POINTER(ct.c_uint64),
        ct.POINTER(ct.POINTER(ct.c_float)),
    ]


def make_booster(lib: ct.CDLL, matrix: ct.c_void_p, objective: bytes) -> ct.c_void_p:
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    for name, value in (
        (b"booster", b"gbtree"),
        (b"objective", objective),
        (b"base_score", b"0.5"),
        (b"boost_from_average", b"0"),
        (b"max_depth", b"3"),
        (b"max_delta_step", b"1"),
        (b"lambda", b"1"),
        (b"eta", b"0.3"),
        (b"nthread", b"1"),
        (b"updater", b"grow_dsflower_dp_hist"),
    ):
        assert lib.XGBoosterSetParam(booster, name, value) == 0
    return booster


def make_matrix_with_missing(lib: ct.CDLL) -> ct.c_void_p:
    features = (ct.c_float * 4)(math.nan, 0.0, 2.0, 3.0)
    matrix = ct.c_void_p()
    assert lib.XGDMatrixCreateFromMat(
        features, 4, 1, ct.c_float(math.nan), ct.byref(matrix)
    ) == 0
    set_labels(lib, matrix, (0.0, 0.0, 1.0, 1.0))
    return matrix


def copy_buffer(
    lib: ct.CDLL, function: str, booster: ct.c_void_p, config: bytes | None = None
) -> bytes:
    length = ct.c_uint64()
    pointer = ct.c_char_p()
    call = getattr(lib, function)
    if config is None:
        status = call(booster, ct.byref(length), ct.byref(pointer))
    else:
        status = call(booster, config, ct.byref(length), ct.byref(pointer))
    assert status == 0, last_error(lib)
    return ct.string_at(pointer, length.value)


def predictions(lib: ct.CDLL, booster: ct.c_void_p, matrix: ct.c_void_p) -> list[float]:
    shape = ct.POINTER(ct.c_uint64)()
    dimensions = ct.c_uint64()
    result = ct.POINTER(ct.c_float)()
    config = (
        b'{"type":0,"training":false,"iteration_begin":0,'
        b'"iteration_end":0,"strict_shape":true}'
    )
    assert (
        lib.XGBoosterPredictFromDMatrix(
            booster,
            matrix,
            config,
            ct.byref(shape),
            ct.byref(dimensions),
            ct.byref(result),
        )
        == 0
    ), last_error(lib)
    count = 1
    for index in range(dimensions.value):
        count *= shape[index]
    return [float(result[index]) for index in range(count)]


def context_for(
    matrix: ct.c_void_p,
    task: bytes,
    objective: bytes,
    *,
    max_trees: int,
    key_byte: int = 81,
) -> tuple[PrivacyContext, dict[str, object]]:
    context, keepalive = valid_context(matrix, task=task, objective=objective)
    context.max_trees = max_trees
    keepalive["key"][:] = bytes([key_byte]) * 32  # type: ignore[index]
    return context, keepalive


def assert_safe_serialization(
    lib: ct.CDLL, booster: ct.c_void_p, key: bytes
) -> bytes:
    model = copy_buffer(
        lib, "XGBoosterSaveModelToBuffer", booster, b'{"format":"json"}'
    )
    snapshot = copy_buffer(lib, "XGBoosterSerializeToBuffer", booster)
    config = copy_buffer(lib, "XGBoosterSaveJsonConfig", booster)
    parsed = json.loads(model)
    assert isinstance(parsed, dict)
    forbidden = (
        key,
        b"a" * 64,
        b"one-record-per-unit-v1",
        b"xgboost/fixed-point-discrete/v1",
        struct.pack("<4f", 0.0, 1.0, 2.0, 3.0),
    )
    for artifact in (model, snapshot, config):
        for token in forbidden:
            assert token not in artifact, f"private context token escaped in {artifact[:16]!r}"
    return model


def assert_objective(lib: ct.CDLL, task: bytes, objective: bytes) -> None:
    matrix = make_matrix(lib)
    booster = ct.c_void_p()
    try:
        context, keepalive = context_for(
            matrix, task, objective, max_trees=1, key_byte=81
        )
        assert keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        booster = make_booster(lib, matrix, objective)
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == 0, last_error(lib)
        assert_safe_serialization(lib, booster, bytes([81]) * 32)
        values = predictions(lib, booster, matrix)
        assert len(values) == 4 and all(math.isfinite(value) for value in values)
        if task == b"binary_classification":
            assert all(0.0 <= value <= 1.0 for value in values)
        else:
            assert all(-1.0 <= value <= 4.0 for value in values)
        assert lib.XGBoosterUpdateOneIter(booster, 1, matrix) == -1
        assert b"context-bound total tree count" in last_error(lib), last_error(lib)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        if booster:
            assert lib.XGBoosterFree(booster) == 0
        assert lib.XGDMatrixFree(matrix) == 0


def train_model_bytes(lib: ct.CDLL, matrix: ct.c_void_p, key_byte: int) -> bytes:
    context, keepalive = context_for(
        matrix, b"regression", b"reg:squarederror", max_trees=1, key_byte=key_byte
    )
    assert keepalive
    assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
    booster = make_booster(lib, matrix, b"reg:squarederror")
    try:
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == 0, last_error(lib)
        return assert_safe_serialization(lib, booster, bytes([key_byte]) * 32)
    finally:
        assert lib.XGBoosterFree(booster) == 0
        assert lib.XGBDsFlowerClearPrivacyContext() == 0


def assert_sticky_recomputation(lib: ct.CDLL) -> None:
    matrix = make_matrix(lib)
    try:
        first = train_model_bytes(lib, matrix, 82)
        second = train_model_bytes(lib, matrix, 82)
        assert first == second, "identical semantic training must be byte-identical"
    finally:
        assert lib.XGDMatrixFree(matrix) == 0


def float32(value: float) -> float:
    return struct.unpack(">f", struct.pack(">f", value))[0]


def assert_private_root_oracle(primitives: ct.CDLL | None, tree: dict) -> None:
    # Root total followed by the five public bins: <=0.5, <=1.5, <=2.5,
    # overflow, and missing. Each pair is (gradient, hessian) at Q=1024.
    raw = (0, 4096, 512, 1024, 0, 0, -512, 1024, -512, 1024, 512, 1024)
    expected_private = (1, 4096, 512, 1023, 1, -1, -512, 1025,
                        -513, 1024, 513, 1022)
    if primitives is not None:
        add_noise = primitives.dsflower_dp_add_discrete_gaussian_i64
        add_noise.argtypes = [
            ct.POINTER(ct.c_int64),
            ct.c_size_t,
            ct.c_uint64,
            ct.POINTER(ct.c_ubyte),
            ct.c_size_t,
            ct.POINTER(ct.c_ubyte),
            ct.c_size_t,
            ct.POINTER(ct.c_int64),
        ]
        add_noise.restype = ct.c_int32
        input_values = (ct.c_int64 * len(raw))(*raw)
        output_values = (ct.c_int64 * len(raw))()
        key = (ct.c_ubyte * 32)(*([83] * 32))
        domain = (
            b"dsflower/xgboost/dp-hist/release/v1\0"
            b"\x01"
            + struct.pack(">Q", 0)
            + struct.pack(">I", 0)
        )
        domain_values = (ct.c_ubyte * len(domain)).from_buffer_copy(domain)
        assert add_noise(
            input_values,
            len(raw),
            1,
            key,
            len(key),
            domain_values,
            len(domain),
            output_values,
        ) == 0
        assert tuple(output_values) == expected_private

    # Projecting the KAT hessians to the private root total and applying the
    # pinned gain formula selects cut 1 with missing assigned left.
    cut_bits = struct.unpack(">I", struct.pack(">f", 1.5))[0]
    expected_threshold = struct.unpack(">f", struct.pack(">I", cut_bits + 1))[0]
    assert tree["left_children"][0] != -1
    assert tree["split_indices"][0] == 0
    assert tree["default_left"][0] == 1
    assert float32(tree["split_conditions"][0]) == expected_threshold


def routed_prediction(tree: dict, value: float) -> tuple[float, list[int]]:
    node = 0
    path: list[int] = []
    while tree["left_children"][node] != -1:
        path.append(node)
        if math.isnan(value):
            go_left = tree["default_left"][node] == 1
        else:
            go_left = value < float(tree["split_conditions"][node])
        node = (tree["left_children"] if go_left else tree["right_children"])[node]
    return float32(0.5 + float(tree["split_conditions"][node])), path


def load_sanitizer():
    sanitizer_path = (
        Path(__file__).resolve().parents[3]
        / "inst"
        / "flower_app"
        / "dsflower_runner"
        / "xgboost_sanitizer.py"
    )
    spec = importlib.util.spec_from_file_location(
        "dsflower_native_test_xgboost_sanitizer", sanitizer_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_sanitizer_prediction_parity(
    lib: ct.CDLL,
    matrix: ct.c_void_p,
    model: bytes,
    native_predictions: list[float],
) -> None:
    sanitizer = load_sanitizer()
    sanitized, digest = sanitizer.sanitize_xgboost_json(
        model,
        expected_task="regression",
        expected_features=1,
        expected_trees=1,
        expected_max_depth=3,
        public_cuts=((0.5, 1.5, 2.5),),
        expected_base_score=0.5,
        max_total_nodes=15,
        max_artifact_bytes=1_000_000,
        leaf_abs_cap=1.0,
    )
    assert len(digest) == 64 and sanitized != model
    safe = ct.c_void_p()
    assert lib.XGBoosterCreate(None, 0, ct.byref(safe)) == 0
    try:
        buffer = ct.create_string_buffer(sanitized)
        assert lib.XGBoosterLoadModelFromBuffer(
            safe, ct.cast(buffer, ct.c_void_p), len(sanitized)
        ) == 0, last_error(lib)
        safe_predictions = predictions(lib, safe, matrix)
        assert [struct.pack(">f", value) for value in safe_predictions] == [
            struct.pack(">f", value) for value in native_predictions
        ]
    finally:
        assert lib.XGBoosterFree(safe) == 0


def assert_missing_value_path(lib: ct.CDLL, primitives: ct.CDLL | None) -> None:
    matrix = make_matrix_with_missing(lib)
    booster = ct.c_void_p()
    try:
        context, keepalive = context_for(
            matrix, b"regression", b"reg:squarederror", max_trees=1, key_byte=83
        )
        assert keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        booster = make_booster(lib, matrix, b"reg:squarederror")
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == 0, last_error(lib)
        values = predictions(lib, booster, matrix)
        assert len(values) == 4 and all(math.isfinite(value) for value in values)
        model = assert_safe_serialization(lib, booster, bytes([83]) * 32)
        tree = json.loads(model)["learner"]["gradient_booster"]["model"]["trees"][0]
        assert_private_root_oracle(primitives, tree)
        routed = [
            routed_prediction(tree, value) for value in (math.nan, 0.0, 2.0, 3.0)
        ]
        assert routed[0][1] and tree["default_left"][routed[0][1][0]] == 1
        assert [struct.pack(">f", value) for value in values] == [
            struct.pack(">f", expected) for expected, _path in routed
        ]
        assert_sanitizer_prediction_parity(lib, matrix, model, values)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        if booster:
            assert lib.XGBoosterFree(booster) == 0
        assert lib.XGDMatrixFree(matrix) == 0


def assert_cross_booster_tree_bound(lib: ct.CDLL) -> None:
    matrix = make_matrix(lib)
    boosters: list[ct.c_void_p] = []
    try:
        context, keepalive = context_for(
            matrix, b"regression", b"reg:squarederror", max_trees=2
        )
        assert keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        boosters = [
            make_booster(lib, matrix, b"reg:squarederror") for _ in range(3)
        ]
        assert lib.XGBoosterUpdateOneIter(boosters[0], 0, matrix) == 0, last_error(lib)
        assert lib.XGBoosterUpdateOneIter(boosters[1], 0, matrix) == 0, last_error(lib)
        assert lib.XGBoosterUpdateOneIter(boosters[2], 0, matrix) == -1
        assert b"context-bound total tree count" in last_error(lib), last_error(lib)
        assert lib.XGBoosterUpdateOneIter(boosters[0], 1, matrix) == -1
        assert b"context-bound total tree count" in last_error(lib), last_error(lib)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        for booster in boosters:
            assert lib.XGBoosterFree(booster) == 0
        assert lib.XGDMatrixFree(matrix) == 0


def assert_failed_preflight_does_not_consume_tree(lib: ct.CDLL) -> None:
    matrix = make_matrix(lib)
    bad = ct.c_void_p()
    good = ct.c_void_p()
    exhausted = ct.c_void_p()
    try:
        context, keepalive = context_for(
            matrix, b"regression", b"reg:squarederror", max_trees=1
        )
        assert keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        bad = make_booster(lib, matrix, b"reg:squarederror")
        assert lib.XGBoosterSetParam(bad, b"subsample", b"0.5") == 0
        assert lib.XGBoosterUpdateOneIter(bad, 0, matrix) == -1
        assert b"train.subsample" in last_error(lib), last_error(lib)

        good = make_booster(lib, matrix, b"reg:squarederror")
        assert lib.XGBoosterUpdateOneIter(good, 0, matrix) == 0, last_error(lib)
        exhausted = make_booster(lib, matrix, b"reg:squarederror")
        assert lib.XGBoosterUpdateOneIter(exhausted, 0, matrix) == -1
        assert b"context-bound total tree count" in last_error(lib), last_error(lib)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        for booster in (bad, good, exhausted):
            if booster:
                assert lib.XGBoosterFree(booster) == 0
        assert lib.XGDMatrixFree(matrix) == 0


def main(library: str, primitives_library: str | None = None) -> None:
    dll_directory = None
    primitives = None
    if primitives_library is not None:
        if os.name == "nt":
            dll_directory = os.add_dll_directory(
                os.path.dirname(os.path.abspath(primitives_library))
            )
        primitives = ct.CDLL(primitives_library)
    lib = ct.CDLL(library)
    try:
        configure_core_api(lib)
        assert lib.XGBSetGlobalConfig(b'{"verbosity":0}') == 0
        status = ct.c_char_p()
        assert lib.XGBDsFlowerPrivacyScaffoldStatus(ct.byref(status)) == 0
        assert status.value == b"bundle-core:fixed-point-discrete-v1:internal-only"

        assert_objective(lib, b"regression", b"reg:squarederror")
        assert_objective(lib, b"binary_classification", b"binary:logistic")
        assert_sticky_recomputation(lib)
        assert_missing_value_path(lib, primitives)
        assert_cross_booster_tree_bound(lib)
        assert_failed_preflight_does_not_consume_tree(lib)
        print("XGBoost dsFlower deterministic fixed-point test core: ok")
    finally:
        if dll_directory is not None:
            dll_directory.close()


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        raise SystemExit(f"usage: {sys.argv[0]} LIBXGBOOST [DP_PRIMITIVES_LIBRARY]")
    main(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else None)
