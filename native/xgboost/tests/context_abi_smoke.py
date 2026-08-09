#!/usr/bin/env python3
"""Exercise ABI v3 and prove that native training remains fail closed."""

from __future__ import annotations

import ctypes as ct
import gc
import math
import sys
import threading


class PrivacyContext(ct.Structure):
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


def configure_api(lib: ct.CDLL) -> None:
    lib.XGBGetLastError.restype = ct.c_char_p
    lib.XGBSetGlobalConfig.argtypes = [ct.c_char_p]
    lib.XGBSetGlobalConfig.restype = ct.c_int
    lib.XGBDsFlowerSetPrivacyContext.argtypes = [ct.POINTER(PrivacyContext)]
    lib.XGBDsFlowerClearPrivacyContext.argtypes = []
    lib.XGBDsFlowerPrivacyContextReady.argtypes = [ct.POINTER(ct.c_int)]
    lib.XGBDsFlowerPrivacyScaffoldStatus.argtypes = [ct.POINTER(ct.c_char_p)]
    lib.XGDMatrixCreateFromMat.argtypes = [
        ct.POINTER(ct.c_float),
        ct.c_uint64,
        ct.c_uint64,
        ct.c_float,
        ct.POINTER(ct.c_void_p),
    ]
    lib.XGDMatrixSetFloatInfo.argtypes = [
        ct.c_void_p,
        ct.c_char_p,
        ct.POINTER(ct.c_float),
        ct.c_uint64,
    ]
    lib.XGDMatrixSetStrFeatureInfo.argtypes = [
        ct.c_void_p,
        ct.c_char_p,
        ct.POINTER(ct.c_char_p),
        ct.c_uint64,
    ]
    lib.XGBoosterCreate.argtypes = [
        ct.POINTER(ct.c_void_p),
        ct.c_uint64,
        ct.POINTER(ct.c_void_p),
    ]
    lib.XGBoosterSetParam.argtypes = [ct.c_void_p, ct.c_char_p, ct.c_char_p]
    lib.XGBoosterUpdateOneIter.argtypes = [ct.c_void_p, ct.c_int, ct.c_void_p]
    lib.XGBoosterBoostOneIter.argtypes = [
        ct.c_void_p,
        ct.c_void_p,
        ct.POINTER(ct.c_float),
        ct.POINTER(ct.c_float),
        ct.c_uint64,
    ]
    lib.XGBoosterFree.argtypes = [ct.c_void_p]
    lib.XGDMatrixFree.argtypes = [ct.c_void_p]


def make_matrix(lib: ct.CDLL) -> ct.c_void_p:
    features = (ct.c_float * 4)(0.0, 1.0, 2.0, 3.0)
    matrix = ct.c_void_p()
    assert lib.XGDMatrixCreateFromMat(
        features, 4, 1, ct.c_float(math.nan), ct.byref(matrix)
    ) == 0
    set_labels(lib, matrix, (0.0, 0.0, 1.0, 1.0))
    return matrix


def set_labels(
    lib: ct.CDLL, matrix: ct.c_void_p, values: tuple[float, float, float, float]
) -> None:
    labels = (ct.c_float * 4)(*values)
    assert lib.XGDMatrixSetFloatInfo(matrix, b"label", labels, 4) == 0


def valid_context(
    matrix: ct.c_void_p,
    *,
    task: bytes = b"regression",
    objective: bytes = b"reg:squarederror",
) -> tuple[PrivacyContext, dict[str, object]]:
    key = (ct.c_ubyte * 32)(*range(32))
    lower = (ct.c_double * 1)(-1.0)
    upper = (ct.c_double * 1)(4.0)
    cut_ptrs = (ct.c_uint64 * 2)(0, 3)
    cuts = (ct.c_double * 3)(0.5, 1.5, 2.5)
    task_buffer = ct.create_string_buffer(task)
    objective_buffer = ct.create_string_buffer(objective)
    context = PrivacyContext(
        ct.sizeof(PrivacyContext),
        3,
        key,
        32,
        b"xgboost/fixed-point-discrete/v1",
        b"patient",
        b"replace_one",
        b"trim-utf8-v2",
        b"one-record-per-unit-v1",
        1.0,
        1.0,
        1,
        matrix,
        ct.cast(task_buffer, ct.c_char_p),
        ct.cast(objective_buffer, ct.c_char_p),
        0.0,
        1.0 if task == b"binary_classification" else 3.0,
        0.5,
        5,
        3,
        lower,
        1,
        upper,
        1,
        cut_ptrs,
        2,
        cuts,
        3,
        1024,
        1,
        1,
    )
    keepalive: dict[str, object] = {
        "key": key,
        "lower": lower,
        "upper": upper,
        "cut_ptrs": cut_ptrs,
        "cuts": cuts,
        "task": task_buffer,
        "objective": objective_buffer,
    }
    return context, keepalive


def last_error(lib: ct.CDLL) -> bytes:
    return lib.XGBGetLastError() or b""


def assert_ready(lib: ct.CDLL, expected: int) -> None:
    ready = ct.c_int(-1)
    assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
    assert ready.value == expected


def assert_set_rejected(
    lib: ct.CDLL, context: PrivacyContext, keepalive: object, expected: bytes
) -> None:
    assert keepalive is not None
    assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == -1
    assert expected in last_error(lib), last_error(lib)
    assert_ready(lib, 0)


def assert_update_rejected(
    lib: ct.CDLL,
    matrix: ct.c_void_p,
    expected: bytes,
    *,
    objective: bytes = b"reg:squarederror",
    base_score: bytes = b"0.5",
    max_depth: bytes = b"3",
    extra_params: tuple[tuple[bytes, bytes], ...] = (),
) -> None:
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    try:
        assert lib.XGBoosterSetParam(booster, b"objective", objective) == 0
        assert lib.XGBoosterSetParam(booster, b"base_score", base_score) == 0
        assert lib.XGBoosterSetParam(booster, b"max_depth", max_depth) == 0
        assert lib.XGBoosterSetParam(booster, b"boost_from_average", b"0") == 0
        assert lib.XGBoosterSetParam(
            booster, b"updater", b"grow_dsflower_dp_hist"
        ) == 0
        for name, value in extra_params:
            assert lib.XGBoosterSetParam(booster, name, value) == 0
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == -1
        assert expected in last_error(lib), last_error(lib)
    finally:
        assert lib.XGBoosterFree(booster) == 0


def assert_external_gradient_rejected(lib: ct.CDLL, matrix: ct.c_void_p) -> None:
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    gradients = (ct.c_float * 4)(0.0, 0.0, 0.0, 0.0)
    hessians = (ct.c_float * 4)(1.0, 1.0, 1.0, 1.0)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    try:
        for name, value in (
            (b"objective", b"reg:squarederror"),
            (b"base_score", b"0.5"),
            (b"max_depth", b"3"),
            (b"boost_from_average", b"0"),
            (b"updater", b"grow_dsflower_dp_hist"),
        ):
            assert lib.XGBoosterSetParam(booster, name, value) == 0
        assert lib.XGBoosterBoostOneIter(
            booster, matrix, gradients, hessians, 4
        ) == -1
        assert b"rejects custom, external" in last_error(lib), last_error(lib)
    finally:
        assert lib.XGBoosterFree(booster) == 0


def assert_context_swap_rejected(lib: ct.CDLL, matrix: ct.c_void_p) -> None:
    first, first_keepalive = valid_context(matrix)
    assert first_keepalive
    assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(first)) == 0
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    try:
        for name, value in (
            (b"objective", b"reg:squarederror"),
            (b"base_score", b"0.5"),
            (b"max_depth", b"3"),
            (b"boost_from_average", b"0"),
            (b"updater", b"grow_dsflower_dp_hist"),
        ):
            assert lib.XGBoosterSetParam(booster, name, value) == 0
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == -1
        assert b"fail-closed scaffold" in last_error(lib), last_error(lib)
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        second, second_keepalive = valid_context(matrix)
        assert second_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(second)) == 0
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == -1
        assert b"changed since updater configuration" in last_error(lib), last_error(lib)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        assert lib.XGBoosterFree(booster) == 0


def assert_cross_thread_context_swap_rejected(
    lib: ct.CDLL, matrix: ct.c_void_p
) -> None:
    first, first_keepalive = valid_context(matrix)
    assert first_keepalive
    assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(first)) == 0
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    try:
        for name, value in (
            (b"objective", b"reg:squarederror"),
            (b"base_score", b"0.5"),
            (b"max_depth", b"3"),
            (b"boost_from_average", b"0"),
            (b"updater", b"grow_dsflower_dp_hist"),
        ):
            assert lib.XGBoosterSetParam(booster, name, value) == 0
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == -1
        assert b"fail-closed scaffold" in last_error(lib), last_error(lib)
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        worker_result: list[tuple[int, bytes]] = []

        def update_on_new_thread() -> None:
            second, second_keepalive = valid_context(matrix)
            assert second_keepalive
            assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(second)) == 0
            result = lib.XGBoosterUpdateOneIter(booster, 0, matrix)
            worker_result.append((result, last_error(lib)))
            assert lib.XGBDsFlowerClearPrivacyContext() == 0

        worker = threading.Thread(target=update_on_new_thread)
        worker.start()
        worker.join()
        assert worker_result and worker_result[0][0] == -1
        assert b"changed since updater configuration" in worker_result[0][1]
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        assert lib.XGBoosterFree(booster) == 0


def main(library: str) -> None:
    lib = ct.CDLL(library)
    configure_api(lib)
    assert lib.XGBSetGlobalConfig(b'{"verbosity":0}') == 0

    status = ct.c_char_p()
    assert lib.XGBDsFlowerPrivacyScaffoldStatus(ct.byref(status)) == 0
    assert status.value == b"scaffold-only:no-dp-histogram-privatization"
    assert_ready(lib, 0)

    matrix = make_matrix(lib)
    other_matrix = make_matrix(lib)
    try:
        assert_update_rejected(lib, matrix, b"requires an installed privacy context")

        zero_key_context, zero_keepalive = valid_context(matrix)
        zero_keepalive["key"][:] = bytes(32)  # type: ignore[index]
        assert_set_rejected(lib, zero_key_context, zero_keepalive, b"must not be all zero")

        malformed_pins = (
            ("privacy_unit", b"household", b"privacy_unit must be row or patient"),
            ("adjacency", b"add_remove", b"adjacency must be replace_one"),
            (
                "unit_canonicalization",
                b"trim-utf8-v1",
                b"unit_canonicalization must be trim-utf8-v2",
            ),
            (
                "contribution_strategy",
                b"unbounded",
                b"contribution_strategy must be one-record-per-unit-v1",
            ),
            ("max_rows_per_unit", 2, b"max_rows_per_unit must equal one"),
        )
        for field, value, expected_error in malformed_pins:
            malformed, malformed_keepalive = valid_context(matrix)
            setattr(malformed, field, value)
            assert_set_rejected(lib, malformed, malformed_keepalive, expected_error)

        for field, value, expected_error in (
            ("max_trees", 0, b"max_trees must be between one and 10000"),
            ("max_trees", 10001, b"max_trees must be between one and 10000"),
            ("max_depth", 0, b"max_depth must be between one and 30"),
            ("max_depth", 31, b"max_depth must be between one and 30"),
            ("fixed_point_scale", 0, b"fixed_point_scale must be a power of two"),
            ("fixed_point_scale", 3, b"fixed_point_scale must be a power of two"),
            ("fixed_point_scale", 2**32, b"fixed_point_scale must be a power of two"),
            ("root_noise_scale", 0, b"exact noise scales must be"),
            ("level_noise_scale", 2**53 + 1, b"exact noise scales must be"),
            ("gradient_clip", math.inf, b"positive finite float values"),
            ("gradient_clip", 1e300, b"positive finite float values"),
            ("hessian_clip", 0.0, b"positive finite float values"),
        ):
            malformed, malformed_keepalive = valid_context(matrix)
            setattr(malformed, field, value)
            assert_set_rejected(lib, malformed, malformed_keepalive, expected_error)

        bad_task, bad_task_keepalive = valid_context(
            matrix, task=b"regression", objective=b"binary:logistic"
        )
        assert_set_rejected(lib, bad_task, bad_task_keepalive, b"supports only")

        bad_binary_bounds, bad_binary_bounds_keepalive = valid_context(
            matrix,
            task=b"binary_classification",
            objective=b"binary:logistic",
        )
        bad_binary_bounds.target_upper_bound = 2.0
        assert_set_rejected(
            lib,
            bad_binary_bounds,
            bad_binary_bounds_keepalive,
            b"binary target bounds must equal zero and one",
        )

        bad_base, bad_base_keepalive = valid_context(
            matrix,
            task=b"binary_classification",
            objective=b"binary:logistic",
        )
        bad_base.base_score = 1.0
        assert_set_rejected(lib, bad_base, bad_base_keepalive, b"base_score")

        collapsed_base, collapsed_base_keepalive = valid_context(
            matrix,
            task=b"binary_classification",
            objective=b"binary:logistic",
        )
        collapsed_base.base_score = 1e-100
        assert_set_rejected(
            lib, collapsed_base, collapsed_base_keepalive, b"base_score"
        )

        missing_cuts, missing_cuts_keepalive = valid_context(matrix)
        missing_cuts.cut_values = ct.POINTER(ct.c_double)()
        missing_cuts.cut_values_size = 0
        assert_set_rejected(
            lib, missing_cuts, missing_cuts_keepalive, b"public cuts must contain"
        )

        missing_cut_ptrs, missing_cut_ptrs_keepalive = valid_context(matrix)
        missing_cut_ptrs.cut_ptrs = ct.POINTER(ct.c_uint64)()
        assert_set_rejected(
            lib,
            missing_cut_ptrs,
            missing_cut_ptrs_keepalive,
            b"cut_ptrs must contain exactly",
        )

        collapsed_cuts, collapsed_cuts_keepalive = valid_context(matrix)
        almost_one = math.nextafter(1.0, 2.0)
        collapsed_storage = (ct.c_double * 2)(1.0, almost_one)
        collapsed_ptrs = (ct.c_uint64 * 2)(0, 2)
        collapsed_cuts.cut_values = collapsed_storage
        collapsed_cuts.cut_values_size = 2
        collapsed_cuts.cut_ptrs = collapsed_ptrs
        collapsed_cuts_keepalive.update(
            {"collapsed_cuts": collapsed_storage, "collapsed_ptrs": collapsed_ptrs}
        )
        assert_set_rejected(
            lib, collapsed_cuts, collapsed_cuts_keepalive, b"after float conversion"
        )

        collapsed_bounds, collapsed_bounds_keepalive = valid_context(matrix)
        collapsed_lower = (ct.c_double * 1)(1.0)
        collapsed_upper = (ct.c_double * 1)(almost_one)
        collapsed_bounds.feature_lower_bounds = collapsed_lower
        collapsed_bounds.feature_upper_bounds = collapsed_upper
        collapsed_bounds_keepalive.update(
            {"collapsed_lower": collapsed_lower, "collapsed_upper": collapsed_upper}
        )
        assert_set_rejected(
            lib, collapsed_bounds, collapsed_bounds_keepalive, b"feature bounds"
        )

        weighted_matrix = make_matrix(lib)
        try:
            feature_weights = (ct.c_float * 1)(1.0)
            assert lib.XGDMatrixSetFloatInfo(
                weighted_matrix, b"feature_weights", feature_weights, 1
            ) == 0
            weighted_context, weighted_keepalive = valid_context(weighted_matrix)
            assert_set_rejected(
                lib, weighted_context, weighted_keepalive, b"row/feature weights"
            )
        finally:
            assert lib.XGDMatrixFree(weighted_matrix) == 0

        named_matrix = make_matrix(lib)
        try:
            names = (ct.c_char_p * 1)(b"private_feature_name")
            assert lib.XGDMatrixSetStrFeatureInfo(
                named_matrix, b"feature_name", names, 1
            ) == 0
            named_context, named_keepalive = valid_context(named_matrix)
            assert_set_rejected(
                lib, named_context, named_keepalive, b"does not accept feature names"
            )
        finally:
            assert lib.XGDMatrixFree(named_matrix) == 0

        context, keepalive = valid_context(matrix)
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        assert_ready(lib, 1)

        other_thread_ready: list[int] = []

        def read_other_thread() -> None:
            thread_ready = ct.c_int(-1)
            assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(thread_ready)) == 0
            other_thread_ready.append(thread_ready.value)

        worker = threading.Thread(target=read_other_thread)
        worker.start()
        worker.join()
        assert other_thread_ready == [0], "privacy context must be thread-local"

        keepalive["key"][:] = bytes(32)  # type: ignore[index]
        keepalive["lower"][0] = 99.0  # type: ignore[index]
        keepalive["upper"][0] = -99.0  # type: ignore[index]
        keepalive["cut_ptrs"][1] = 0  # type: ignore[index]
        keepalive["cuts"][0] = math.nan  # type: ignore[index]
        keepalive["task"][0] = b"x"  # type: ignore[index]
        keepalive["objective"][0] = b"x"  # type: ignore[index]
        gc.collect()
        assert_update_rejected(lib, matrix, b"fail-closed scaffold")

        replacement, replacement_keepalive = valid_context(matrix)
        assert replacement_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(replacement)) == -1
        assert b"already installed" in last_error(lib)
        assert lib.XGBDsFlowerClearPrivacyContext() == 0
        assert_ready(lib, 0)

        label_context, label_keepalive = valid_context(matrix)
        assert label_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(label_context)) == 0
        set_labels(lib, matrix, (1.0, 0.0, 1.0, 1.0))
        assert_update_rejected(lib, matrix, b"labels changed after privacy context binding")
        assert lib.XGBDsFlowerClearPrivacyContext() == 0
        set_labels(lib, matrix, (0.0, 0.0, 1.0, 1.0))

        wrong_matrix_context, wrong_matrix_keepalive = valid_context(matrix)
        assert wrong_matrix_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(wrong_matrix_context)) == 0
        assert_update_rejected(
            lib, other_matrix, b"does not match the context-bound DMatrix"
        )
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        depth_context, depth_keepalive = valid_context(matrix)
        assert depth_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(depth_context)) == 0
        assert_update_rejected(lib, matrix, b"exact depth", max_depth=b"2")
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        for parameter, expected_error in (
            ((b"boost_from_average", b"1"), b"must be explicitly disabled"),
            ((b"num_parallel_tree", b"2"), b"single-output, one-tree"),
            ((b"num_target", b"2"), b"single-output, one-tree"),
        ):
            shape_context, shape_keepalive = valid_context(matrix)
            assert shape_keepalive
            assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(shape_context)) == 0
            assert_update_rejected(
                lib, matrix, expected_error, extra_params=(parameter,)
            )
            assert lib.XGBDsFlowerClearPrivacyContext() == 0

        external_context, external_keepalive = valid_context(matrix)
        assert external_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(external_context)) == 0
        assert_external_gradient_rejected(lib, matrix)
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        assert_context_swap_rejected(lib, matrix)
        assert_cross_thread_context_swap_rejected(lib, matrix)

        objective_context, objective_keepalive = valid_context(matrix)
        assert objective_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(objective_context)) == 0
        assert_update_rejected(
            lib,
            matrix,
            b"objective task does not match",
            objective=b"binary:logistic",
        )
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        binary_context, binary_keepalive = valid_context(
            matrix,
            task=b"binary_classification",
            objective=b"binary:logistic",
        )
        assert binary_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(binary_context)) == 0
        assert_update_rejected(
            lib, matrix, b"fail-closed scaffold", objective=b"binary:logistic"
        )
        assert lib.XGBDsFlowerClearPrivacyContext() == 0

        temporary_matrix = make_matrix(lib)
        owned_context, owned_keepalive = valid_context(temporary_matrix)
        assert owned_keepalive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(owned_context)) == 0
        assert lib.XGDMatrixFree(temporary_matrix) == 0
        temporary_matrix = ct.c_void_p()
        gc.collect()
        assert_ready(lib, 1)
        assert lib.XGBDsFlowerClearPrivacyContext() == 0
        assert_ready(lib, 0)
    finally:
        lib.XGBDsFlowerClearPrivacyContext()
        assert lib.XGDMatrixFree(other_matrix) == 0
        assert lib.XGDMatrixFree(matrix) == 0

    print("XGBoost dsFlower ABI v3 context/updater fail-closed smoke: ok")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} LIBXGBOOST")
    main(sys.argv[1])
