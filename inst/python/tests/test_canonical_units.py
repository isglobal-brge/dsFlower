"""Canonical source units: execution permutation and private binding invariants."""
import hashlib
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import canonical_units as cu, resampling, task

SECRET = b"u" * 32


class CanonicalUnitsTests(unittest.TestCase):
    def setUp(self):
        self.secret = mock.patch("dsflower_runner.seeding._node_secret", return_value=SECRET)
        self.secret.start()
        self.addCleanup(self.secret.stop)

    def test_rows_shuffle_and_duplicates_have_identical_bytes(self):
        rows = [[1.0, 0.0], [2.0, 1.0], [1.0, 0.0], [3.0, 1.0]]
        first = cu.canonicalize_units(rows)
        for perm in ([3, 1, 0, 2], [2, 0, 3, 1]):
            other = cu.canonicalize_units([rows[i] for i in perm])
            self.assertEqual(first.records, other.records)
            self.assertEqual(first.unit_tokens, other.unit_tokens)
            self.assertEqual(first.multiset_digest, other.multiset_digest)
            self.assertEqual([rows[i] for i in first.row_permutation],
                             [[rows[i] for i in perm][j] for j in other.row_permutation])
        self.assertEqual(len(first.records), 4)
        self.assertEqual(len(set(first.unit_tokens)), 4)

    def test_patient_and_within_patient_permutations_match(self):
        X = np.array([[1, 2], [3, 4], [5, 6], [7, 8]], dtype=float)
        y = np.array([0, 1, 0, 1], dtype=float)
        ids = np.array([" p1", "p2", "p1 ", "p2"])
        first = cu.canonicalize_arrays(X, y, ids)
        p = [3, 2, 0, 1]
        other = cu.canonicalize_arrays(X[p], y[p], ids[p])
        self.assertEqual(first.records, other.records)
        self.assertEqual(first.unit_ids, other.unit_ids)
        np.testing.assert_array_equal(X[first.row_permutation], X[p][other.row_permutation])
        self.assertEqual([part.stop-part.start for part in first.unit_slices], [2, 2])
        regroup = cu.canonicalize_arrays(X, y, ["p1", "p1", "p2", "p2"])
        self.assertNotEqual(first.multiset_digest, regroup.multiset_digest)

    def test_pooling_reduction_is_order_stable(self):
        from dsflower_runner.client_app import _pool_by_patient
        X = np.array([[1.e16], [-1.e16], [1.], [7.], [-2.]], dtype=np.float64)
        y = np.array([0., 1., 0., 1., 0.], dtype=np.float64)
        ids = np.array(["p", "p", "p", "q", "q"])
        first = _pool_by_patient(X, y, ids, "mse")
        for p in ([4, 3, 2, 1, 0], [0, 2, 4, 1, 3]):
            other = _pool_by_patient(X[p], y[p], ids[p], "mse")
            for a, b in zip(first, other):
                self.assertEqual(a.tobytes(), b.tobytes())
            self.assertEqual(cu.source_units(*first).multiset_digest,
                             cu.source_units(*other).multiset_digest)

    def test_duplicate_multiplicity_changes_binding(self):
        first = cu.canonicalize_units([[1.], [2.]])
        other = cu.canonicalize_units([[1.], [1.], [2.]])
        self.assertNotEqual(first.multiset_digest, other.multiset_digest)

    def test_hmac_collision_uses_content_tiebreak(self):
        with mock.patch.object(cu, "_order_key", return_value=b"x" * 32):
            first = cu.canonicalize_units([[2.], [1.], [2.]])
            other = cu.canonicalize_units([[2.], [2.], [1.]])
        self.assertEqual(first.records, tuple(sorted(first.records)))
        self.assertEqual(first.records, other.records)
        self.assertEqual(first.unit_tokens, other.unit_tokens)

    def test_occurrence_tokens_preserve_other_content_classes(self):
        first = cu.canonicalize_units([[1.], [1.], [2.], [3.]])
        other = cu.canonicalize_units([[1.], [2.], [2.], [3.]])
        self.assertEqual(len(set(first.unit_tokens) - set(other.unit_tokens)), 1)
        self.assertEqual(len(set(other.unit_tokens) - set(first.unit_tokens)), 1)

    def test_sequence_time_axis_not_sorted(self):
        X = np.arange(24).reshape(3, 4, 2)
        y = np.array([0, 1, 0])
        units = cu.canonicalize_arrays(X, y)
        for actual in X[units.row_permutation]:
            self.assertTrue(any(np.array_equal(actual, row) for row in X))
        swapped = cu.canonicalize_arrays(X[:, ::-1], y)
        self.assertNotEqual(units.multiset_digest, swapped.multiset_digest)

    def test_endian_signed_zero_and_invalid_value_encoding(self):
        self.assertEqual(cu.encode_scalar(-0.), cu.encode_scalar(0.))
        self.assertEqual(cu.array_record(np.array([1., -0.], dtype=">f8")),
                         cu.array_record(np.array([1., 0.], dtype="<f8")))
        tags = [None, True, 1, 1., "1", float("nan"), float("inf"), -float("inf")]
        self.assertEqual(len(set(map(cu.encode_scalar, tags))), len(tags))
        self.assertEqual(cu.encode_scalar(1), cu.frame("int64", struct.pack("<q", 1)))
        self.assertEqual(cu.encode_scalar(1.), cu.frame("float64", struct.pack("<d", 1.)))
        self.assertEqual(cu.encode_numeric("1.0"), cu.encode_numeric(1))
        self.assertNotEqual(cu.encode_numeric("bad-one"), cu.encode_numeric("bad-two"))

    def test_full_raw_source_binding_survives_equal_totalized_tensors(self):
        with tempfile.TemporaryDirectory() as directory:
            from types import SimpleNamespace
            context = SimpleNamespace(node_config={"manifest-dir": directory})
            manifest = {"data_file": "data.csv", "target_column": "y",
                        "feature_columns": ["x"], "dp-unit": "row",
                        "num-classes": 2, "task-type": "classification"}
            arrays = []
            for value in (float("inf"), float("nan")):
                source = pd.DataFrame({"x": [value], "y": [0.]})
                with mock.patch.object(task, "_load_manifest", return_value=manifest), \
                        mock.patch.object(task, "_read_staged_frame", return_value=source):
                    arrays.append(task.load_data(context, include_canonical_units=True))
            self.assertEqual(arrays[0][0].tobytes(), arrays[1][0].tobytes())
            self.assertNotEqual(arrays[0][2].multiset_digest, arrays[1][2].multiset_digest)

    def test_images_paths_and_repacking_are_nuisance(self):
        from PIL import Image, PngImagePlugin
        from dsflower_runner.vision import canonical_image_record
        with tempfile.TemporaryDirectory() as directory:
            pixels = np.arange(36, dtype=np.uint8).reshape(3, 4, 3)
            a, b, c = [os.path.join(directory, name+".png") for name in ("a", "b", "c")]
            Image.fromarray(pixels).save(a, compress_level=0)
            meta = PngImagePlugin.PngInfo(); meta.add_text("timestamp", "unrelated")
            Image.fromarray(pixels).save(b, compress_level=9, pnginfo=meta)
            Image.fromarray(pixels + 1).save(c)
            self.assertEqual(canonical_image_record(a), canonical_image_record(b))
            self.assertNotEqual(canonical_image_record(a), canonical_image_record(c))

    def test_row_content_assignments_are_stable_and_unit_local(self):
        rows = [[float(i), float(i % 2)] for i in range(100)]
        first = cu.canonicalize_units(rows)
        other = cu.canonicalize_units(list(reversed(rows)))
        contract = resampling.holdout_contract(400000, "row")
        args = dict(n_rows=100, assignment_tokens=first.row_tokens)
        mask = resampling.holdout_mask(contract, **args)
        np.testing.assert_array_equal(mask, resampling.holdout_mask(
            contract, n_rows=100, assignment_tokens=other.row_tokens))
        larger = resampling.holdout_mask(resampling.holdout_contract(500000, "row"), **args)
        self.assertTrue(np.all(~mask | larger))
        changed = cu.canonicalize_units(rows[:-1] + [[-1., 1.]])
        folds = resampling.cross_validation_folds(resampling.cross_validation_contract(5, "row"), **args)
        changed_folds = resampling.cross_validation_folds(resampling.cross_validation_contract(5, "row"),
            n_rows=100, assignment_tokens=changed.row_tokens)
        by_token = dict(zip(first.row_tokens, folds))
        for token, fold in zip(changed.row_tokens, changed_folds):
            if token in by_token:
                self.assertEqual(fold, by_token[token])
        with self.assertRaisesRegex(ValueError, "requires canonical"):
            resampling.holdout_mask(contract, n_rows=100)

class ContentPartitionSensitivityTests(unittest.TestCase):
    def test_absent_row_and_patient_mean_need_stricter_holdout_bounds(self):
        from dsflower_runner import validation
        for task_name, absent, replacement in (("regression", np.sqrt(5.), 2.),
                                                ("count", np.sqrt(6.), np.sqrt(5.)),
                                                ("segmentation", 2., np.sqrt(3.))):
            with self.subTest(layout=task_name):
                layout = validation.validation_layout(task_name)
                if task_name == "segmentation":
                    target = np.ones((2, 1, 128, 128))
                    prediction = np.ones_like(target)
                    contribution = validation.validation_contributions(target, prediction, layout)
                else:
                    contribution = validation.validation_contributions(
                        np.ones(2), np.zeros(2), layout, target_bounds={"lower": 0., "upper": 1.})
                self.assertAlmostEqual(np.linalg.norm(contribution[0]), absent)
                self.assertAlmostEqual(np.linalg.norm(contribution.mean(axis=0)), absent)
                self.assertGreater(absent, replacement)
                self.assertEqual(validation._validation_release_sensitivity(
                    layout, include_zero_neighbor=False), replacement)
                self.assertEqual(validation._validation_release_sensitivity(
                    layout, include_zero_neighbor=True), absent)


if __name__ == "__main__":
    unittest.main()
