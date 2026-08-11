"""Bounded differentially-private binary epidemiologic association.

The trusted profile maps every protected row or patient to exactly one cell of
a public 3x3 exposure/outcome table: reference, positive, or unknown on each
axis.  One joint Gaussian release leaves a node.  Pooled tables and descriptive
association measures are ordinary post-processing of those releases.
"""

import math
import numbers

import numpy as np

from . import tree_release


ASSOCIATION_CONTRACT = "dsflower-binary-association-3x3/v1"
RESULT_CONTRACT = "dsflower-binary-association-result/v1"
MECHANISM = "binary-association-joint-gaussian/v1"
EXECUTION_PROFILE = "dsflower-binary-association-execution/v1"
SENSITIVITY = math.sqrt(2.0)
CELL_LABELS = ("reference", "positive", "unknown")
_MAX_EXACT_COUNT = 1 << 53
_MAX_EXPECTED_NODES = 1_000_000
_LEVEL_TEXT_BYTES = 4096


def _privacy_unit(value):
    unit = str(value).lower()
    if unit not in ("row", "patient"):
        raise ValueError("association privacy unit must be row or patient")
    return unit


def association_layout(privacy_unit):
    """Return the complete effective release layout for one node."""
    unit = _privacy_unit(privacy_unit)
    return {
        "cells": 9,
        "contract": ASSOCIATION_CONTRACT,
        "order": "exposure-major/outcome-minor",
        "shape": [3, 3],
        "unit_semantics": (
            "row-one-hot/v1" if unit == "row"
            else "patient-ever-positive/v1"),
    }


def _private_vector(value, name):
    if not isinstance(value, (list, tuple, np.ndarray)):
        raise ValueError("%s must be one vector" % name)
    try:
        array = np.asarray(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("%s must be one vector" % name) from exc
    if array.ndim != 1:
        raise ValueError("%s must be one vector" % name)
    return array


def _canonical_levels(value, name):
    if not isinstance(value, (list, tuple, np.ndarray)):
        raise ValueError("%s must contain exactly two public levels" % name)
    if isinstance(value, np.ndarray) and value.ndim != 1:
        raise ValueError("%s must contain exactly two public levels" % name)
    items = list(value)
    if len(items) != 2:
        raise ValueError("%s must contain exactly two public levels" % name)
    if all(isinstance(item, (bool, np.bool_)) for item in items):
        kind = "boolean"
        canonical = tuple(bool(item) for item in items)
    elif all(isinstance(item, str) for item in items):
        kind = "string"
        canonical = tuple(items)
        try:
            valid = all(
                bool(item) and len(item.encode("utf-8", errors="strict"))
                <= _LEVEL_TEXT_BYTES for item in canonical)
        except UnicodeError as exc:
            raise ValueError("%s contains invalid public text" % name) from exc
        if not valid:
            raise ValueError("%s contains invalid public text" % name)
    elif all(isinstance(item, numbers.Real)
             and not isinstance(item, (bool, np.bool_)) for item in items):
        kind = "number"
        canonical = tuple(float(item) for item in items)
        if not all(math.isfinite(item) for item in canonical):
            raise ValueError("%s must contain finite public numbers" % name)
    else:
        raise ValueError(
            "%s levels must share one string, boolean, or numeric type" % name)
    if canonical[0] == canonical[1]:
        raise ValueError("%s public levels must be distinct" % name)
    return kind, canonical


def _coded_value(value, kind, levels):
    """Totalize one private value; invalid values always become unknown."""
    try:
        if kind == "string":
            if not isinstance(value, str):
                return 2
            value.encode("utf-8", errors="strict")
            candidate = value
        elif kind == "boolean":
            if not isinstance(value, (bool, np.bool_)):
                return 2
            candidate = bool(value)
        else:
            if (not isinstance(value, numbers.Real)
                    or isinstance(value, (bool, np.bool_))):
                return 2
            candidate = float(value)
            if not math.isfinite(candidate):
                return 2
    except (TypeError, ValueError, OverflowError, UnicodeError):
        return 2
    if candidate == levels[0]:
        return 0
    if candidate == levels[1]:
        return 1
    return 2


def _encode_binary(value, levels, name):
    array = _private_vector(value, name)
    kind, canonical = _canonical_levels(levels, "%s_levels" % name)
    codes = np.full(array.shape, 2, dtype=np.uint8)

    # Common staged dtypes stay entirely in NumPy. Object arrays retain a total
    # scalar fallback so malformed private values never become an error bit.
    if kind == "number" and array.dtype.kind in "iuf":
        finite = np.isfinite(array)
        codes[finite & (array == canonical[0])] = 0
        codes[finite & (array == canonical[1])] = 1
        return codes
    if kind == "boolean" and array.dtype.kind == "b":
        codes[array == canonical[0]] = 0
        codes[array == canonical[1]] = 1
        return codes
    if kind == "string" and array.dtype.kind == "U":
        codes[array == canonical[0]] = 0
        codes[array == canonical[1]] = 1
        return codes

    for index, item in enumerate(array):
        codes[index] = _coded_value(item, kind, canonical)
    return codes


def _patient_codes(codes, inverse, units):
    # Priority implements positive > reference > unknown without depending on
    # row order or visit count. Every patient still selects exactly one cell.
    priority = np.asarray((1, 2, 0), dtype=np.uint8)[codes]
    reduced = np.zeros(units, dtype=np.uint8)
    np.maximum.at(reduced, inverse, priority)
    return np.asarray((2, 0, 1), dtype=np.uint8)[reduced]


def _patient_inverse(unit_ids, rows):
    if unit_ids is None or not isinstance(
            unit_ids, (list, tuple, np.ndarray)):
        raise ValueError(
            "patient unit identifiers must align one-to-one with rows")
    if isinstance(unit_ids, np.ndarray) and unit_ids.ndim != 1:
        raise ValueError(
            "patient unit identifiers must align one-to-one with rows")
    if len(unit_ids) != rows:
        raise ValueError(
            "patient unit identifiers must align one-to-one with rows")
    from . import task as task_module
    raw = np.asarray(unit_ids)
    if raw.ndim != 1:
        raise ValueError(
            "patient unit identifiers must align one-to-one with rows")
    if raw.dtype.kind == "U":
        # Staged identifiers are Unicode. Canonicalize unique spellings once,
        # then merge spellings that map to the same protected unit.
        unique_raw, raw_inverse = np.unique(raw, return_inverse=True)
        canonical_unique = np.asarray([
            task_module._canonical_patient_id(value) for value in unique_raw
        ], dtype=str)
        _unique, canonical_inverse = np.unique(
            canonical_unique, return_inverse=True)
        return canonical_inverse[raw_inverse], int(_unique.size)
    canonical = np.asarray([
        task_module._canonical_patient_id(value) for value in raw
    ], dtype=str)
    _unique, inverse = np.unique(canonical, return_inverse=True)
    return inverse, int(_unique.size)


def association_sufficient_vector(
        outcome, exposure, *, outcome_levels, exposure_levels,
        privacy_unit, unit_ids=None):
    """Return the canonical 3x3 sufficient vector before DP release."""
    outcome_codes = _encode_binary(outcome, outcome_levels, "outcome")
    exposure_codes = _encode_binary(exposure, exposure_levels, "exposure")
    if outcome_codes.shape != exposure_codes.shape:
        raise ValueError("association outcome and exposure lengths differ")

    unit = _privacy_unit(privacy_unit)
    rows = int(outcome_codes.size)
    if unit == "row":
        if unit_ids is not None:
            raise ValueError(
                "row-level association must not carry patient identifiers")
    else:
        inverse, units = _patient_inverse(unit_ids, rows)
        outcome_codes = _patient_codes(outcome_codes, inverse, units)
        exposure_codes = _patient_codes(exposure_codes, inverse, units)

    cells = exposure_codes.astype(np.int64) * 3 + outcome_codes
    vector = np.bincount(cells, minlength=9)[:9].astype(np.float64)
    vector = np.ascontiguousarray(vector, dtype=np.float64)
    vector.setflags(write=False)
    return vector


def _canonical_sufficient_vector(value):
    array = np.asarray(value)
    if array.dtype.hasobject or array.dtype.kind not in "iuf" or \
            array.shape != (9,):
        raise ValueError("association sufficient vector must have nine cells")
    canonical = np.array(array, dtype="<f8", order="C", copy=True)
    if (not bool(np.all(np.isfinite(canonical)))
            or bool(np.any(canonical < 0.0))
            or bool(np.any(canonical > _MAX_EXACT_COUNT))
            or bool(np.any(canonical != np.floor(canonical)))):
        raise ValueError("association sufficient vector contains invalid counts")
    canonical[canonical == 0.0] = 0.0
    return canonical


def private_association_vector(
        sufficient, *, privacy_unit, epsilon, delta):
    """Apply the sole sticky joint Gaussian release for one node."""
    raw = _canonical_sufficient_vector(sufficient)
    released, sigma = tree_release.joint_gaussian_release(
        raw, mechanism=MECHANISM, layout=association_layout(privacy_unit),
        epsilon=epsilon, delta=delta, sensitivity=SENSITIVITY,
        num_releases=1, execution_fingerprint=EXECUTION_PROFILE)
    vector = np.ascontiguousarray(released, dtype=np.float64).reshape(9)
    if not bool(np.all(np.isfinite(vector))):
        raise RuntimeError("private association release is non-finite")
    return vector, float(sigma)


def _released_vector(value):
    array = np.asarray(value)
    if array.dtype.hasobject or array.dtype.kind not in "iuf" or \
            array.shape != (9,):
        raise ValueError("private association vector has invalid geometry")
    checked = np.array(array, dtype=np.float64, order="C", copy=True)
    if not bool(np.all(np.isfinite(checked))):
        raise ValueError("private association vector must be finite")
    checked[checked == 0.0] = 0.0
    return checked


def _safe_sum(values):
    with np.errstate(over="ignore", invalid="ignore"):
        total = np.sum(np.asarray(values, dtype=np.longdouble),
                       dtype=np.longdouble)
    limit = np.longdouble(np.finfo(np.float64).max)
    if not np.isfinite(total) or total < 0.0 or total > limit:
        return None
    return float(total)


def _safe_ratio(numerator, denominator):
    if numerator is None or denominator is None or not denominator > 0.0:
        return None
    value = np.longdouble(numerator) / np.longdouble(denominator)
    limit = np.longdouble(np.finfo(np.float64).max)
    if not np.isfinite(value) or value < -limit or value > limit:
        return None
    return float(value)


def _safe_odds_ratio(event_exposed, no_event_exposed,
                     event_unexposed, no_event_unexposed):
    cells = (event_exposed, no_event_exposed,
             event_unexposed, no_event_unexposed)
    if any(not value > 0.0 for value in cells):
        return None
    log_value = (math.log(event_exposed) + math.log(no_event_unexposed)
                 - math.log(no_event_exposed) - math.log(event_unexposed))
    try:
        value = math.exp(log_value)
    except OverflowError:
        return None
    return value if math.isfinite(value) else None


def association_postprocess(released):
    """Project one pooled DP table and derive finite descriptive measures."""
    vector = _released_vector(released)
    table = np.maximum(vector.reshape(3, 3), 0.0)
    table[table == 0.0] = 0.0

    no_event_unexposed, event_unexposed = table[0, 0], table[0, 1]
    no_event_exposed, event_exposed = table[1, 0], table[1, 1]
    unexposed = _safe_sum((no_event_unexposed, event_unexposed))
    exposed = _safe_sum((no_event_exposed, event_exposed))
    prevalence_unexposed = _safe_ratio(event_unexposed, unexposed)
    prevalence_exposed = _safe_ratio(event_exposed, exposed)
    difference = (None if prevalence_unexposed is None
                  or prevalence_exposed is None else
                  float(prevalence_exposed - prevalence_unexposed))
    measures = {
        "odds_ratio": _safe_odds_ratio(
            event_exposed, no_event_exposed,
            event_unexposed, no_event_unexposed),
        "prevalence_difference": difference,
        "prevalence_exposed": prevalence_exposed,
        "prevalence_ratio": _safe_ratio(
            prevalence_exposed, prevalence_unexposed),
        "prevalence_unexposed": prevalence_unexposed,
    }
    return {"measures": measures, "table_dp": table.tolist()}


def _pooled_vector(vectors):
    checked = [_released_vector(value) for value in vectors]
    if not checked:
        raise ValueError("no private association vectors are available")
    stacked = np.stack(checked, axis=0).astype(np.float64, copy=False)
    scale = np.max(np.abs(stacked), axis=0)
    normalized = np.divide(
        stacked, scale, out=np.zeros_like(stacked), where=scale > 0.0)
    wide = (np.sum(normalized.astype(np.longdouble), axis=0,
                   dtype=np.longdouble) * scale.astype(np.longdouble))
    limit = np.longdouble(np.finfo(np.float64).max)
    pooled = np.asarray(np.clip(wide, -limit, limit), dtype=np.float64)
    if not bool(np.all(np.isfinite(pooled))):
        raise RuntimeError("pooled private association vector overflowed")
    return pooled


def _expected_nodes(value):
    if isinstance(value, (bool, np.bool_)) or not isinstance(
            value, numbers.Integral) or not 1 <= int(value) <= _MAX_EXPECTED_NODES:
        raise ValueError("expected association node count is invalid")
    return int(value)


def _base_result(expected_nodes, privacy_unit):
    layout = association_layout(privacy_unit)
    return {
        "available": False,
        "contract": RESULT_CONTRACT,
        "n_nodes": expected_nodes,
        "pooled_only": True,
        "privacy": {
            "adjacency": "replace-one",
            "mechanism": MECHANISM,
            "scope": "per-job-node-dp",
            "sticky": True,
        },
        "schema": 1,
        "unit_semantics": layout["unit_semantics"],
    }


def build_pooled_association_result(
        vectors, sigmas, *, expected_nodes, privacy_unit):
    """Build an all-or-nothing pooled result without any per-node field."""
    expected = _expected_nodes(expected_nodes)
    result = _base_result(expected, privacy_unit)
    if (not isinstance(vectors, (list, tuple))
            or not isinstance(sigmas, (list, tuple))
            or len(vectors) != expected or len(sigmas) != expected):
        return result
    try:
        checked_sigmas = [float(value) for value in sigmas]
        if any(not math.isfinite(value) or value <= 0.0
               for value in checked_sigmas):
            return result
        pooled = _pooled_vector(vectors)
        pooled_sigma = 0.0
        for sigma in checked_sigmas:
            pooled_sigma = math.hypot(pooled_sigma, sigma)
        if not math.isfinite(pooled_sigma) or pooled_sigma <= 0.0:
            return result
        postprocessed = association_postprocess(pooled)
    except (TypeError, ValueError, OverflowError, RuntimeError):
        return result

    result.update(postprocessed)
    result["available"] = True
    result["noise_sd_pooled"] = float(pooled_sigma)
    return result


__all__ = [
    "ASSOCIATION_CONTRACT", "CELL_LABELS", "EXECUTION_PROFILE",
    "MECHANISM", "RESULT_CONTRACT", "SENSITIVITY",
    "association_layout", "association_postprocess",
    "association_sufficient_vector", "build_pooled_association_result",
    "private_association_vector",
]
