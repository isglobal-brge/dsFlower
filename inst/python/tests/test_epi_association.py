"""Tests for the bounded binary epidemiologic association foundation."""

import builtins
import json
import math
import os
import sys
import time
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import epi_association


class AssociationSufficientVectorTests(unittest.TestCase):
    def test_contract_and_exhaustive_replace_one_sensitivity(self):
        row = epi_association.association_layout("row")
        patient = epi_association.association_layout("PATIENT")
        self.assertEqual(row, {
            "cells": 9,
            "contract": "dsflower-binary-association-3x3/v1",
            "order": "exposure-major/outcome-minor",
            "shape": [3, 3],
            "unit_semantics": "row-one-hot/v1",
        })
        self.assertEqual(
            patient["unit_semantics"], "patient-ever-positive/v1")

        contributions = np.eye(9, dtype=np.float64)
        observed = max(
            np.linalg.norm(left - right)
            for left in contributions for right in contributions)
        self.assertEqual(observed, math.sqrt(2.0))
        self.assertEqual(observed, epi_association.SENSITIVITY)

    def test_row_table_totalizes_unknown_values_explicitly(self):
        vector = epi_association.association_sufficient_vector(
            np.asarray([0, 1, None, 9, np.nan], dtype=object),
            np.asarray([0, 1, 1, None, 0], dtype=object),
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="row")
        np.testing.assert_array_equal(
            vector, np.asarray([1, 0, 1, 0, 1, 1, 0, 0, 1],
                               dtype=np.float64))
        self.assertFalse(vector.flags.writeable)

    def test_patient_reducer_is_ever_positive_then_reference_then_unknown(self):
        vector = epi_association.association_sufficient_vector(
            np.asarray(["no", "yes", "?", "no", "?", "?"]),
            np.asarray([
                "control", "control", "treated", "control", "?", "?"]),
            outcome_levels=("no", "yes"),
            exposure_levels=("control", "treated"),
            privacy_unit="patient",
            unit_ids=np.asarray(["a", "a", "a", "b", "b", "c"]))
        np.testing.assert_array_equal(
            vector, np.asarray([1, 0, 0, 0, 1, 0, 0, 0, 1],
                               dtype=np.float64))
        self.assertEqual(float(vector.sum()), 3.0)

    def test_patient_result_ignores_row_order_and_identifier_spelling(self):
        outcome = np.asarray([0, 1, 0, 2, 1, 9, 0])
        exposure = np.asarray([0, 0, 1, 1, 1, 9, 0])
        ids = np.asarray(["a", "a", "b", "b", "c", "c", "d"])
        permutation = np.asarray([6, 3, 1, 5, 0, 4, 2])
        relabeled = np.asarray(["z", "z", "x", "x", "q", "q", "r"])
        first = epi_association.association_sufficient_vector(
            outcome, exposure, outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="patient", unit_ids=ids)
        second = epi_association.association_sufficient_vector(
            outcome[permutation], exposure[permutation],
            outcome_levels=(0.0, 1.0), exposure_levels=(0.0, 1.0),
            privacy_unit="patient", unit_ids=relabeled[permutation])
        np.testing.assert_array_equal(first, second)

    def test_canonical_identifier_aliases_stay_one_protected_unit(self):
        vector = epi_association.association_sufficient_vector(
            np.asarray([0, 1, 0]), np.asarray([0, 1, 0]),
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="patient",
            unit_ids=np.asarray([" patient-a", "patient-a\t", "patient-b"]))
        expected = np.zeros(9)
        expected[0] = 1.0
        expected[4] = 1.0
        np.testing.assert_array_equal(vector, expected)
        self.assertEqual(float(vector.sum()), 2.0)

    def test_empty_row_and_patient_cohorts_have_the_fixed_geometry(self):
        row = epi_association.association_sufficient_vector(
            np.asarray([], dtype=np.float64),
            np.asarray([], dtype=np.float64),
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="row")
        patient = epi_association.association_sufficient_vector(
            np.asarray([], dtype=np.float64),
            np.asarray([], dtype=np.float64),
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="patient", unit_ids=np.asarray([], dtype=str))
        np.testing.assert_array_equal(row, np.zeros(9))
        np.testing.assert_array_equal(patient, np.zeros(9))

    def test_public_and_structural_input_errors_fail_closed(self):
        valid = dict(
            outcome=np.asarray([0, 1]), exposure=np.asarray([0, 1]),
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="row")
        cases = [
            {**valid, "outcome": np.asarray([[0, 1]])},
            {**valid, "exposure": np.asarray([0])},
            {**valid, "outcome_levels": (0, 0)},
            {**valid, "exposure_levels": (False, 1)},
            {**valid, "privacy_unit": "record"},
            {**valid, "unit_ids": np.asarray(["a", "b"])},
            {**valid, "privacy_unit": "patient", "unit_ids": ["a"]},
        ]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                epi_association.association_sufficient_vector(**case)

    def test_one_million_visits_reduce_to_one_patient_quickly(self):
        rows = 1_000_000
        outcome = np.zeros(rows, dtype=np.uint8)
        exposure = np.zeros(rows, dtype=np.uint8)
        outcome[-1] = 1
        exposure[-1] = 1
        unit_ids = np.full(rows, "p", dtype="U1")
        started = time.monotonic()
        vector = epi_association.association_sufficient_vector(
            outcome, exposure, outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="patient", unit_ids=unit_ids)
        elapsed = time.monotonic() - started
        expected = np.zeros(9)
        expected[4] = 1.0
        np.testing.assert_array_equal(vector, expected)
        self.assertLess(elapsed, 20.0)


class AssociationReleaseTests(unittest.TestCase):
    @staticmethod
    def _release(vector, *, unit="row", secret=None):
        secret = bytes(range(32)) if secret is None else secret
        with mock.patch(
                "dsflower_runner.seeding._node_secret",
                return_value=secret):
            return epi_association.private_association_vector(
                vector, privacy_unit=unit, epsilon=1.0, delta=1.0e-6)

    def test_effective_vector_replay_is_byte_exact(self):
        first_raw = epi_association.association_sufficient_vector(
            [0, 1, 1, 0], [0, 1, 0, 1],
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit="row")
        replay_raw = epi_association.association_sufficient_vector(
            np.asarray([0.0, 0.0, 1.0, 1.0]),
            np.asarray([1.0, 0.0, 0.0, 1.0]),
            outcome_levels=(0.0, 1.0), exposure_levels=(0.0, 1.0),
            privacy_unit="row")
        np.testing.assert_array_equal(first_raw, replay_raw)
        first, sigma = self._release(first_raw)
        replay, replay_sigma = self._release(replay_raw.astype(">f8"))
        np.testing.assert_array_equal(first, replay)
        self.assertEqual(sigma, replay_sigma)

    def test_data_secret_and_unit_semantics_bind_noise(self):
        raw = np.asarray([2, 1, 0, 3, 4, 0, 0, 0, 1], dtype=np.float64)
        first, _ = self._release(raw)
        changed = raw.copy()
        changed[0] += 1
        data_changed, _ = self._release(changed)
        secret_changed, _ = self._release(raw, secret=b"z" * 32)
        unit_changed, _ = self._release(raw, unit="patient")
        self.assertFalse(np.array_equal(first - raw, data_changed - changed))
        self.assertFalse(np.array_equal(first, secret_changed))
        self.assertFalse(np.array_equal(first, unit_changed))

    def test_empty_cohort_still_reaches_the_gaussian_mechanism(self):
        released, sigma = self._release(np.zeros(9, dtype=np.float64))
        self.assertEqual(released.shape, (9,))
        self.assertTrue(np.all(np.isfinite(released)))
        self.assertGreater(sigma, 0.0)
        self.assertFalse(np.array_equal(released, np.zeros(9)))

    def test_release_rejects_forged_sufficient_vectors(self):
        cases = [
            np.zeros(8),
            np.asarray([0.5] + [0.0] * 8),
            np.asarray([-1.0] + [0.0] * 8),
            np.asarray([float("nan")] + [0.0] * 8),
            np.asarray([object()] * 9, dtype=object),
        ]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self._release(value)

    def test_release_and_postprocessing_do_not_open_or_create_files(self):
        raw = np.asarray([2, 1, 0, 3, 4, 0, 0, 0, 1], dtype=np.float64)
        with mock.patch.object(
                builtins, "open", side_effect=AssertionError("file I/O")):
            released, _ = self._release(raw)
            result = epi_association.association_postprocess(released)
        json.dumps(result, allow_nan=False)


class AssociationPostprocessingTests(unittest.TestCase):
    def test_known_table_produces_descriptive_association_measures(self):
        released = np.asarray([
            80, 20, 5,
            60, 40, 3,
            2, 1, 4,
        ], dtype=np.float64)
        result = epi_association.association_postprocess(released)
        self.assertEqual(result["table_dp"], released.reshape(3, 3).tolist())
        measures = result["measures"]
        self.assertAlmostEqual(measures["prevalence_unexposed"], 0.2)
        self.assertAlmostEqual(measures["prevalence_exposed"], 0.4)
        self.assertAlmostEqual(measures["prevalence_difference"], 0.2)
        self.assertAlmostEqual(measures["prevalence_ratio"], 2.0)
        self.assertAlmostEqual(measures["odds_ratio"], 8.0 / 3.0)

    def test_negative_noise_is_projected_and_zero_denominators_are_null(self):
        result = epi_association.association_postprocess(-np.ones(9))
        self.assertEqual(result["table_dp"], np.zeros((3, 3)).tolist())
        self.assertTrue(all(value is None
                            for value in result["measures"].values()))
        json.dumps(result, allow_nan=False)

    def test_extreme_finite_input_never_emits_nan_or_infinity(self):
        maximum = np.finfo(np.float64).max
        result = epi_association.association_postprocess(
            np.full(9, maximum, dtype=np.float64))
        json.dumps(result, allow_nan=False)
        for value in result["measures"].values():
            self.assertTrue(value is None or math.isfinite(value))

    def test_pooled_builder_is_complete_or_contains_no_statistics(self):
        first = np.asarray([80, 20, -2, 60, 40, -1, 0, 0, 0],
                           dtype=np.float64)
        second = np.asarray([40, 10, 1, 30, 20, 2, 0, 0, 0],
                            dtype=np.float64)
        result = epi_association.build_pooled_association_result(
            [first, second], [2.0, 3.0], expected_nodes=2,
            privacy_unit="patient")
        self.assertTrue(result["available"])
        self.assertTrue(result["pooled_only"])
        self.assertEqual(result["n_nodes"], 2)
        self.assertEqual(
            result["unit_semantics"], "patient-ever-positive/v1")
        self.assertAlmostEqual(result["noise_sd_pooled"], math.sqrt(13.0))
        self.assertNotIn("per_node", result)
        self.assertNotIn("vectors", result)
        json.dumps(result, allow_nan=False)

        unavailable_cases = [
            ([first], [2.0]),
            ([first, np.full(9, np.nan)], [2.0, 3.0]),
            ([first, second], [2.0, 0.0]),
            ([first, np.zeros(8)], [2.0, 3.0]),
            ([first, second, second], [2.0, 3.0, 3.0]),
        ]
        for vectors, sigmas in unavailable_cases:
            with self.subTest(vectors=len(vectors), sigmas=sigmas):
                unavailable = (
                    epi_association.build_pooled_association_result(
                        vectors, sigmas, expected_nodes=2,
                        privacy_unit="row"))
                self.assertFalse(unavailable["available"])
                for forbidden in (
                        "table_dp", "measures", "noise_sd_pooled", "per_node"):
                    self.assertNotIn(forbidden, unavailable)
                json.dumps(unavailable, allow_nan=False)

    def test_expected_node_count_is_publicly_bounded(self):
        for value in (0, True, 1.5, 1_000_001):
            with self.subTest(value=value), self.assertRaises(ValueError):
                epi_association.build_pooled_association_result(
                    [], [], expected_nodes=value, privacy_unit="row")


if __name__ == "__main__":
    unittest.main()
