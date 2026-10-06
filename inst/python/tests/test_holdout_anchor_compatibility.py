"""Numeric holdout layout upgrades keep durable anchors in separate domains."""

import hashlib
import json
import os
import sqlite3
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "flower_app")))
from dsflower_runner import canonical_units, neighbourhood, seeding, validation


def _inputs(n, patient):
    # Parent records include both training and test units. Two visits still
    # count as one source unit when exercising whole-patient neighbourhoods.
    index = np.repeat(np.arange(n), 2 if patient else 1)
    target = (index % 4).astype(float) / 4
    prediction = ((index + 1) % 4).astype(float) / 4
    ids = np.asarray([str(i) for i in index]) if patient else None
    units = canonical_units.canonicalize_arrays(index[:, None], target, ids)
    test = index % 2 == 1
    assignment = hashlib.sha256(b"".join(units.records)).hexdigest()
    return target[test], prediction[test], None if ids is None else ids[test], units, assignment


def _request(layout, patient):
    return validation.build_validation_request(
        layout, 1.0, 1e-5, operation="holdout-evaluate",
        manifest={"dp-unit": "patient" if patient else "row",
                  "patient_column": "patient_id" if patient else None},
        target_bounds=(0.0, 1.0),
        public_arrays=(np.asarray([0.25, 0.5], dtype=np.float64),))


def _release(layout, request, n, patient):
    y, prediction, ids, units, assignment = _inputs(n, patient)
    return validation.private_validation_vector(
        y, prediction, layout, epsilon=1.0, delta=1e-5,
        target_bounds=(0.0, 1.0), unit_ids=ids,
        include_zero_neighbor=True, request_identity=request,
        source_units=units,
        subset={"role": "test", "assignment_sha256": assignment})


def _stored_payloads():
    # Reopen the production persistent store and inspect its serialized values;
    # no in-memory replay or synthetic cache stands in for the anchor boundary.
    store = neighbourhood.NeighbourhoodStore.from_env()
    store.verify()
    with sqlite3.connect(store.database) as connection:
        return [neighbourhood.decode_payload(row[0]) for row in
                connection.execute("SELECT payload FROM anchors")]


@pytest.mark.parametrize("task_name", ["regression", "count"])
@pytest.mark.parametrize("patient", [False, True])
def test_v4_anchor_persists_shifted_vector_and_replays_exact_and_removed_unit(task_name, patient):
    layout = validation.numeric_holdout_layout(task_name)
    legacy = validation.validation_layout(task_name)
    request = _request(layout, patient)
    y, prediction, ids, _, _ = _inputs(8, patient)
    expected = validation.validation_sufficient_vector(
        y, prediction, legacy, target_bounds=(0.0, 1.0), unit_ids=ids)
    expected[1:] -= expected[0] / 4
    noise = np.linspace(-0.25, 0.25, layout["size"])
    with mock.patch.object(seeding, "np_rng", return_value=SimpleNamespace(
            normal=mock.Mock(return_value=noise))) as draws:
        released, sigma = _release(layout, request, 8, patient)
    assert draws.call_count == 1
    # Count deviance summation may round differently when the constant shift
    # happens per contribution; persistence and replay below must be bit exact.
    np.testing.assert_allclose(released, expected + noise, rtol=0, atol=1e-15)
    original_bytes = released.tobytes()
    stored = _stored_payloads()
    assert len(stored) == 1
    assert stored[0][0].tobytes() == released.tobytes()
    assert stored[0][1] == sigma

    metrics = validation.validation_metrics(released, layout, target_bounds=(0.0, 1.0))
    with mock.patch.object(seeding, "np_rng", side_effect=AssertionError("replayed anchor drew noise")):
        for n in (8, 7):
            replay, replay_sigma = _release(layout, request, n, patient)
            assert replay.tobytes() == released.tobytes()
            assert replay_sigma == sigma
            assert validation.validation_metrics(replay, layout, target_bounds=(0.0, 1.0)) == metrics
    stored_after = _stored_payloads()
    assert len(stored_after) == 1
    assert stored_after[0][0].tobytes() == original_bytes


@pytest.mark.parametrize("task_name", ["regression", "count"])
def test_legacy_anchor_cannot_satisfy_v4_holdout_with_identical_runtime(task_name):
    legacy = validation.validation_layout(task_name)
    shifted = validation.numeric_holdout_layout(task_name)
    runtime = dict(seeding._runtime_fingerprint())
    runtime["runner_sha256"] = "7" * 64
    with mock.patch.object(seeding, "_runtime_fingerprint", return_value=runtime):
        old_request = _request(legacy, False)
        new_request = _request(shifted, False)
    old_identity = json.loads(old_request.canonical_json)
    new_identity = json.loads(new_request.canonical_json)
    assert old_identity["runtime"] == new_identity["runtime"]
    assert old_identity["evaluation"]["layout_version"] == "validation-vector-v3"
    assert new_identity["evaluation"]["layout_version"] == "validation-vector-v4"
    assert old_request.digest != new_request.digest
    # Under the same executable profile, only the layout discriminator differs.
    old_identity["evaluation"]["layout_version"] = "validation-vector-v4"
    old_identity["mechanism"]["profile"]["parameters"]["version"] = "validation-vector-v4"
    assert old_identity == new_identity

    with mock.patch.object(seeding, "np_rng", wraps=seeding.np_rng) as draws:
        old_vector, old_sigma = _release(legacy, old_request, 8, False)
        new_vector, new_sigma = _release(shifted, new_request, 7, False)
    assert draws.call_count == 2
    assert old_vector.tobytes() != new_vector.tobytes()
    assert new_sigma < old_sigma
    assert len(_stored_payloads()) == 2
    with mock.patch.object(seeding, "np_rng", side_effect=AssertionError("replayed anchor drew noise")):
        for layout, request, vector, sigma in (
                (legacy, old_request, old_vector, old_sigma),
                (shifted, new_request, new_vector, new_sigma)):
            replay, replay_sigma = _release(layout, request, 7, False)
            assert replay.tobytes() == vector.tobytes()
            assert replay_sigma == sigma
