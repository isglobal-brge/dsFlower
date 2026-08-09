#!/usr/bin/env python3
"""Exercise the scaffold ABI and prove that both training paths fail closed."""

from __future__ import annotations

import ctypes as ct
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
        ("noise_key_id", ct.c_char_p),
        ("privacy_epoch", ct.c_char_p),
        ("node_id_hash", ct.c_char_p),
        ("dataset_snapshot_hash", ct.c_char_p),
        ("cohort_hash", ct.c_char_p),
        ("query_hash", ct.c_char_p),
        ("mechanism_config_hash", ct.c_char_p),
        ("allocation_hash", ct.c_char_p),
        ("privacy_unit", ct.c_char_p),
        ("adjacency", ct.c_char_p),
        ("unit_canonicalization", ct.c_char_p),
        ("contribution_strategy", ct.c_char_p),
        ("epsilon", ct.c_double),
        ("delta", ct.c_double),
        ("gradient_clip", ct.c_double),
        ("hessian_clip", ct.c_double),
        ("max_rows_per_unit", ct.c_uint64),
    ]


def configure_api(lib: ct.CDLL) -> None:
    lib.XGBGetLastError.restype = ct.c_char_p
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
    lib.XGBoosterCreate.argtypes = [
        ct.POINTER(ct.c_void_p),
        ct.c_uint64,
        ct.POINTER(ct.c_void_p),
    ]
    lib.XGBoosterSetParam.argtypes = [ct.c_void_p, ct.c_char_p, ct.c_char_p]
    lib.XGBoosterUpdateOneIter.argtypes = [ct.c_void_p, ct.c_int, ct.c_void_p]
    lib.XGBoosterFree.argtypes = [ct.c_void_p]
    lib.XGDMatrixFree.argtypes = [ct.c_void_p]


def valid_context() -> tuple[PrivacyContext, ct.Array[ct.c_ubyte]]:
    key = (ct.c_ubyte * 32)(*range(32))
    digest = b"a" * 64
    context = PrivacyContext(
        ct.sizeof(PrivacyContext),
        1,
        key,
        32,
        b"dsflower.xgboost.dp_hist.v0-scaffold",
        digest,
        b"epoch-1",
        digest,
        digest,
        digest,
        digest,
        digest,
        digest,
        b"patient",
        b"replace_one",
        b"trim-utf8-v2",
        b"one-record-per-unit-v1",
        1.0,
        1e-6,
        1.0,
        1.0,
        1,
    )
    return context, key


def last_error(lib: ct.CDLL) -> bytes:
    return lib.XGBGetLastError() or b""


def assert_update_rejected(lib: ct.CDLL, matrix: ct.c_void_p, expected: bytes) -> None:
    booster = ct.c_void_p()
    matrices = (ct.c_void_p * 1)(matrix)
    assert lib.XGBoosterCreate(matrices, 1, ct.byref(booster)) == 0
    try:
        assert lib.XGBoosterSetParam(
            booster, b"objective", b"reg:squarederror"
        ) == 0
        assert lib.XGBoosterSetParam(
            booster, b"updater", b"grow_dsflower_dp_hist"
        ) == 0
        assert lib.XGBoosterUpdateOneIter(booster, 0, matrix) == -1
        assert expected in last_error(lib), last_error(lib)
    finally:
        assert lib.XGBoosterFree(booster) == 0


def main(library: str) -> None:
    lib = ct.CDLL(library)
    configure_api(lib)

    status = ct.c_char_p()
    assert lib.XGBDsFlowerPrivacyScaffoldStatus(ct.byref(status)) == 0
    assert status.value == b"scaffold-only:no-dp-histogram-privatization"

    ready = ct.c_int(-1)
    assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
    assert ready.value == 0

    features = (ct.c_float * 4)(0.0, 1.0, 2.0, 3.0)
    labels = (ct.c_float * 4)(0.0, 0.0, 1.0, 1.0)
    matrix = ct.c_void_p()
    assert lib.XGDMatrixCreateFromMat(
        features, 4, 1, ct.c_float(math.nan), ct.byref(matrix)
    ) == 0
    try:
        assert lib.XGDMatrixSetFloatInfo(matrix, b"label", labels, 4) == 0
        assert_update_rejected(lib, matrix, b"complete server-authoritative privacy context")

        zero_key_context, zero_key = valid_context()
        zero_key[:] = bytes(32)
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(zero_key_context)) == -1
        assert b"must not be all zero" in last_error(lib)

        invalid, invalid_key = valid_context()
        invalid.query_hash = b"A" * 64
        assert invalid_key  # keep borrowed storage alive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(invalid)) == -1
        assert b"lowercase SHA-256" in last_error(lib)
        assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
        assert ready.value == 0

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
            malformed, malformed_key = valid_context()
            setattr(malformed, field, value)
            assert malformed_key  # keep borrowed storage alive
            assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(malformed)) == -1
            assert expected_error in last_error(lib)
            assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
            assert ready.value == 0

        context, key = valid_context()
        assert key  # keep borrowed storage alive
        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == 0
        assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
        assert ready.value == 1

        other_thread_ready: list[int] = []

        def read_other_thread() -> None:
            thread_ready = ct.c_int(-1)
            assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(thread_ready)) == 0
            other_thread_ready.append(thread_ready.value)

        worker = threading.Thread(target=read_other_thread)
        worker.start()
        worker.join()
        assert other_thread_ready == [0], "privacy context must be thread-local"

        assert lib.XGBDsFlowerSetPrivacyContext(ct.byref(context)) == -1
        assert b"already installed" in last_error(lib)
        assert_update_rejected(lib, matrix, b"fail-closed scaffold")
    finally:
        assert lib.XGBDsFlowerClearPrivacyContext() == 0
        assert lib.XGDMatrixFree(matrix) == 0

    assert lib.XGBDsFlowerPrivacyContextReady(ct.byref(ready)) == 0
    assert ready.value == 0
    print("XGBoost dsFlower context/updater fail-closed smoke: ok")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} LIBXGBOOST")
    main(sys.argv[1])
