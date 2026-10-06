"""Actual fixed-bucket payload witnesses for Hook bounded-change sensitivity."""
import sys
from pathlib import Path
import unittest
from unittest import mock

import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import canonical_units as cu, seeding, tier2_lib


class HookPartitionTests(unittest.TestCase):
    @staticmethod
    def blocks(X, ids=None, k=7):
        y = np.asarray(X)[:, 0] % 2
        units = cu.canonicalize_arrays(X, y, ids, secret=b"u" * 32)
        X, y = X[units.row_permutation], y[units.row_permutation]
        seed = seeding.sub_seed(b"e" * 32, "partition")
        if ids is None:
            blocks = tier2_lib._row_content_blocks(units.row_tokens, k, seed)
        else:
            blocks, _ = tier2_lib._patient_row_blocks(
                np.asarray(ids)[units.row_permutation], len(X), k, seed)
        return [(X[rows].tobytes(), y[rows].tobytes()) for rows in blocks], blocks, X

    def test_one_replacement_changes_at_most_two_blocks(self):
        for patient in (False, True):
            for duplicate in (False, True):
                with self.subTest(patient=patient, duplicate=duplicate):
                    X = np.arange(90, dtype=float).reshape(30, 3)
                    if duplicate:
                        X[:3] = X[3]
                    ids = np.repeat(np.arange(10), 3).astype(str) if patient else None
                    other = X.copy()
                    other[:3 if patient else 1] = -7
                    before, _, _ = self.blocks(X, ids)
                    after, _, _ = self.blocks(other, ids)
                    changed = sum(a != b for a, b in zip(before, after))
                    self.assertLessEqual(changed, 2)
                    self.assertGreater(changed, 0)
                    # Child seeds contain block coordinate + public execution
                    # key only; unchanged blocks see byte-identical input.
                    for i, (a, b) in enumerate(zip(before, after)):
                        if a == b:
                            self.assertEqual(seeding.sub_seed(b"e" * 32, "child/%d" % i),
                                             seeding.sub_seed(b"e" * 32, "child/%d" % i))

    def test_unaffected_blocks_keep_exact_inputs_after_shuffle(self):
        X = np.arange(90, dtype=float).reshape(30, 3)
        for ids in (None, np.repeat(np.arange(10), 3).astype(str)):
            before, _, _ = self.blocks(X, ids)
            p = np.random.default_rng(7).permutation(len(X))
            after, _, _ = self.blocks(X[p], None if ids is None else ids[p])
            self.assertEqual(before, after)

    def test_block_count_empty_blocks_and_mean_bound(self):
        X = np.arange(6, dtype=float).reshape(2, 3)
        other = X.copy(); other[0] = -9
        for k in (2, 7, 64):
            before, blocks, ordered = self.blocks(X, k=k)
            after, other_blocks, changed = self.blocks(other, k=k)
            self.assertEqual(len(before), k)
            self.assertEqual(len(after), k)
            if k > 2:
                self.assertTrue(any(len(rows) == 0 for rows in blocks))
            # Arbitrary clipped block functional, including empty zero deltas.
            def mean_output(values, indices):
                return sum((np.array([np.tanh(values[idx].sum()), 0.])
                            if len(idx) else np.zeros(2) for idx in indices),
                           start=np.zeros(2)) / k
            difference = np.linalg.norm(mean_output(ordered, blocks)
                                        - mean_output(changed, other_blocks))
            self.assertLessEqual(difference, min(2., 4. / k))

    def test_real_gate_keeps_empty_buckets_as_zero_deltas(self):
        X, y = np.array([[1.], [2.]]), np.array([0., 1.])
        old = [np.array([3.], dtype=np.float64)]
        captured = []
        def aggregate(blocks, incoming, **kwargs):
            captured.extend(blocks)
            return incoming
        policy = {"clipping_norm": 1., "epsilon": 1., "delta": 1e-6,
                  "sample_aggregate": True, "sa_blocks": 8}
        with (mock.patch.object(seeding, "_node_secret", return_value=b"u" * 32),
              mock.patch.object(tier2_lib, "hook_execution_caps", return_value={"full": True}),
              mock.patch.object(tier2_lib, "_pinned_user_package", return_value="/public/app.py"),
              mock.patch.object(tier2_lib, "_run_isolated", return_value=[np.array([4.])]) as child,
              mock.patch.object(tier2_lib.dp_harness, "sample_and_aggregate", side_effect=aggregate)):
            tier2_lib.gated_local_update("app", old, X, y, {}, policy,
                seed=b"n" * 32, execution_seed=b"e" * 32, pad_release=False)
        self.assertEqual(len(captured), 8)
        self.assertLessEqual(child.call_count, 2)
        self.assertGreaterEqual(sum(np.array_equal(update[0], old[0]) for update in captured), 6)
        for call in child.call_args_list:
            self.assertGreater(len(call.args[3]), 0)


if __name__ == "__main__":
    unittest.main()
