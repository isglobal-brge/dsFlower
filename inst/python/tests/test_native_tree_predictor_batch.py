"""Differential and bounded-cost tests for native-tree batch prediction."""

import math
import os
import subprocess
import sys
import time
import unittest
from unittest import mock

import numpy as np


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import boosting_artifact
from dsflower_runner import forest_predictor
from dsflower_runner import xgboost_predictor


_BATCH_ROWS = 4096


def _complete_tree(rng, features, depth, cuts):
    internal = (1 << depth) - 1
    nodes = (1 << (depth + 1)) - 1
    left = tuple(2 * node + 1 if node < internal else -1
                 for node in range(nodes))
    right = tuple(2 * node + 2 if node < internal else -1
                  for node in range(nodes))
    defaults = tuple(bool(value) for value in rng.integers(0, 2, nodes))
    split_features = tuple(int(value) for value in rng.integers(
        0, features, nodes))
    cut_indices = tuple(int(value) for value in rng.integers(
        0, cuts, nodes))
    leaves = tuple(float(value) for value in rng.normal(0.0, 0.1, nodes))
    return left, right, defaults, split_features, cut_indices, leaves


def _forest_model(rng, task, *, features=5, depth=3, trees=3, models=2):
    bounds = tuple((-3.0, 3.0) for _ in range(features))
    cuts = tuple(tuple(float(value) for value in np.linspace(-2.5, 2.5, 7))
                 for _ in range(features))
    forests = []
    for _model in range(models):
        forest = []
        for _tree in range(trees):
            left, right, defaults, split_features, cut_indices, leaves = \
                _complete_tree(rng, features, depth, len(cuts[0]))
            del left, right
            forest.append((split_features, cut_indices, defaults,
                           leaves[(1 << depth) - 1:]))
        forests.append(tuple(forest))
    return forest_predictor.ForestEnsemble(
        forest_predictor._CONSTRUCTION_TOKEN, task, features, depth,
        bounds, cuts, tuple(forests))


def _boosting_model(rng, engine, task, *, features=5, depth=3,
                    trees=3, models=2):
    bounds = tuple((-3.0, 3.0) for _ in range(features))
    cuts = tuple(tuple(float(value) for value in np.linspace(-2.5, 2.5, 7))
                 for _ in range(features))
    members = []
    for _model in range(models):
        member = []
        for _tree in range(trees):
            if engine == "lightgbm":
                member.append(_complete_tree(
                    rng, features, depth, len(cuts[0])))
            else:
                splits = tuple((
                    int(rng.integers(0, features)),
                    int(rng.integers(0, len(cuts[0]))),
                    bool(rng.integers(0, 2)),
                ) for _level in range(depth))
                leaves = tuple(float(value) for value in rng.normal(
                    0.0, 0.1, 1 << depth))
                member.append((splits, leaves))
        members.append((0.125, tuple(member)))
    profile = {
        "engine": engine,
        "task": task,
        "feature_bounds": bounds,
        "cuts": cuts,
    }
    return boosting_artifact.BoostingEnsemble(
        boosting_artifact._CONSTRUCTION_TOKEN, profile, tuple(members))


def _xgboost_model(rng, task, *, features=5, depth=3,
                   trees=3, models=2):
    bounds = tuple((-3.0, 3.0) for _ in range(features))
    cuts = tuple(tuple(float(value) for value in np.linspace(-2.5, 2.5, 7))
                 for _ in range(features))
    members = []
    for _model in range(models):
        member = []
        for _tree in range(trees):
            left, right, defaults, split_features, cut_indices, leaves = \
                _complete_tree(rng, features, depth, len(cuts[0]))
            internal = (1 << depth) - 1
            conditions = tuple(
                cuts[split_features[node]][cut_indices[node]]
                if node < internal else leaves[node]
                for node in range(len(left)))
            member.append((left, right, defaults, split_features, conditions))
        members.append(tuple(member))
    base_score = 0.5 if task == "binary" else 0.125
    return xgboost_predictor.XGBoostEnsemble(
        xgboost_predictor._CONSTRUCTION_TOKEN, task, features, base_score,
        bounds, tuple(members))


def _engines(seed, task):
    rng = np.random.default_rng(seed)
    return {
        "extra_trees": _forest_model(rng, task),
        "random_forest": _forest_model(rng, task),
        "lightgbm": _boosting_model(rng, "lightgbm", task),
        "catboost": _boosting_model(rng, "catboost", task),
        "xgboost": _xgboost_model(rng, task),
    }


def _rows(seed, size, features=5):
    rng = np.random.default_rng(seed)
    rows = rng.normal(size=(size, features)).astype(np.float64)
    if size:
        rows[0, 0] = np.nan
        rows[0, 1] = np.inf
        rows[0, 2] = -np.inf
        rows[0, 3] = 3.0
        rows[0, 4] = -3.0
    return rows


class NativeTreeBatchParityTests(unittest.TestCase):
    def test_all_engines_tasks_and_batch_boundaries_are_bit_exact(self):
        for task in ("binary", "regression"):
            for engine, model in _engines(20260811, task).items():
                for size in (0, 1, 4095, 4096, 4097):
                    with self.subTest(task=task, engine=engine, rows=size):
                        rows = _rows(size + 31, size)
                        expected = [model.predict_one(row) for row in rows]
                        with mock.patch.object(
                                type(model), "predict_one",
                                side_effect=AssertionError(
                                    "numeric ndarray used the scalar path")):
                            actual = model.predict(rows)
                        self.assertEqual(actual, expected)

    def test_fast_paths_never_materialize_more_than_one_bounded_block(self):
        rows = _rows(17, _BATCH_ROWS + 1)
        cases = (
            (forest_predictor, "_predict_numpy_block",
             _forest_model(np.random.default_rng(1), "binary")),
            (boosting_artifact, "_predict_numpy_block",
             _boosting_model(np.random.default_rng(2), "lightgbm", "binary")),
            (boosting_artifact, "_predict_numpy_block",
             _boosting_model(np.random.default_rng(3), "catboost", "binary")),
            (xgboost_predictor, "_predict_numpy_block",
             _xgboost_model(np.random.default_rng(4), "binary")),
        )
        for module, helper, model in cases:
            with self.subTest(module=module.__name__, engine=getattr(
                    model, "_engine", "tree")):
                original = getattr(module, helper)
                sizes = []

                def observed(instance, block, compiled, numpy):
                    sizes.append(int(block.shape[0]))
                    return original(instance, block, compiled, numpy)

                with mock.patch.object(module, helper, side_effect=observed):
                    model.predict(rows)
                self.assertEqual(sizes, [_BATCH_ROWS, 1])
                self.assertLessEqual(max(sizes), _BATCH_ROWS)

    def test_production_float32_and_integer_matrices_are_bit_exact(self):
        base = _rows(23, 257)
        matrices = (
            base.astype(np.float32),
            np.rint(np.nan_to_num(
                base, nan=0.0, posinf=4.0, neginf=-4.0)).astype(np.int64),
        )
        for task in ("binary", "regression"):
            for engine, model in _engines(24, task).items():
                for rows in matrices:
                    with self.subTest(
                            task=task, engine=engine, dtype=str(rows.dtype)):
                        self.assertEqual(
                            model.predict(rows),
                            [model.predict_one(row) for row in rows])

    def test_non_numeric_and_malformed_arrays_keep_the_scalar_contract(self):
        invalid = (
            np.ones((1, 5), dtype=np.bool_),
            np.asarray([["1", "2", "3", "4", "5"]], dtype=object),
            np.asarray([1.0, 2.0, 3.0]),
        )
        for engine, model in _engines(9, "binary").items():
            for rows in invalid:
                with self.subTest(engine=engine, dtype=rows.dtype,
                                  shape=rows.shape):
                    with self.assertRaises((TypeError, ValueError)) as expected:
                        [model.predict_one(row) for row in rows]
                    with self.assertRaises(type(expected.exception)) as actual:
                        model.predict(rows)
                    self.assertEqual(
                        str(actual.exception), str(expected.exception))

    def test_dependency_light_fallback_stays_stdlib_only(self):
        code = """
import sys
sys.path.insert(0, %r)
from dsflower_runner import boosting_artifact as ba
from dsflower_runner import forest_predictor as fp
from dsflower_runner import xgboost_predictor as xp
assert 'numpy' not in sys.modules
bounds = ((-1.0, 1.0),)
cuts = ((0.0,),)
forest = fp.ForestEnsemble(fp._CONSTRUCTION_TOKEN, 'binary', 1, 1,
    bounds, cuts, ((((0,), (0,), (True,), (0.25, 0.75)),),))
boost = ba.BoostingEnsemble(ba._CONSTRUCTION_TOKEN,
    {'engine':'catboost','task':'binary','feature_bounds':bounds,'cuts':cuts},
    ((0.0, ((((0, 0, True),), (0.0, 1.0)),)),))
xgb = xp.XGBoostEnsemble(xp._CONSTRUCTION_TOKEN, 'binary', 1, 0.5,
    bounds, ((((1, -1, -1), (2, -1, -1), (True, False, False),
               (0, 0, 0), (0.0, 0.0, 1.0)),),))
assert forest.predict([[0.0]]) == [0.25]
assert len(boost.predict([[0.0]])) == 1
assert len(xgb.predict([[0.0]])) == 1
assert 'numpy' not in sys.modules
""" % FLOWER_APP
        completed = subprocess.run(
            [sys.executable, "-I", "-S", "-c", code],
            check=False, capture_output=True, text=True, timeout=20)
        self.assertEqual(completed.returncode, 0,
                         msg=completed.stdout + completed.stderr)

    def test_list_fallback_does_not_import_available_numpy(self):
        code = """
import importlib.util
import sys
assert importlib.util.find_spec('numpy') is not None
assert 'numpy' not in sys.modules
sys.path.insert(0, %r)
from dsflower_runner import boosting_artifact as ba
from dsflower_runner import forest_predictor as fp
from dsflower_runner import xgboost_predictor as xp
bounds = ((-1.0, 1.0),)
cuts = ((0.0,),)
forest = fp.ForestEnsemble(fp._CONSTRUCTION_TOKEN, 'binary', 1, 1,
    bounds, cuts, ((((0,), (0,), (True,), (0.25, 0.75)),),))
boost = ba.BoostingEnsemble(ba._CONSTRUCTION_TOKEN,
    {'engine':'catboost','task':'binary','feature_bounds':bounds,'cuts':cuts},
    ((0.0, ((((0, 0, True),), (0.0, 1.0)),)),))
xgb = xp.XGBoostEnsemble(xp._CONSTRUCTION_TOKEN, 'binary', 1, 0.5,
    bounds, ((((1, -1, -1), (2, -1, -1), (True, False, False),
               (0, 0, 0), (0.0, 0.0, 1.0)),),))
assert forest.predict([[0.0]]) == [0.25]
assert len(boost.predict([[0.0]])) == 1
assert len(xgb.predict([[0.0]])) == 1
assert 'numpy' not in sys.modules
""" % FLOWER_APP
        completed = subprocess.run(
            [sys.executable, "-c", code], check=False, capture_output=True,
            text=True, timeout=20)
        self.assertEqual(completed.returncode, 0,
                         msg=completed.stdout + completed.stderr)

    def test_benchmark_reports_relative_speedup_without_wall_clock_gate(self):
        cases = {
            "extra_trees": _forest_model(
                np.random.default_rng(41), "binary", features=8, depth=4,
                trees=16, models=2),
            "random_forest": _forest_model(
                np.random.default_rng(42), "binary", features=8, depth=4,
                trees=16, models=2),
            "lightgbm": _boosting_model(
                np.random.default_rng(43), "lightgbm", "binary", features=8,
                depth=4, trees=16, models=2),
            "catboost": _boosting_model(
                np.random.default_rng(44), "catboost", "binary", features=8,
                depth=4, trees=16, models=2),
            "xgboost": _xgboost_model(
                np.random.default_rng(45), "binary", features=8, depth=4,
                trees=16, models=2),
        }
        rows = _rows(46, 3000, features=8)
        for engine, model in cases.items():
            started = time.perf_counter()
            expected = [model.predict_one(row) for row in rows]
            scalar = time.perf_counter() - started
            started = time.perf_counter()
            actual = model.predict(rows)
            batched = time.perf_counter() - started
            self.assertEqual(actual, expected)
            speedup = scalar / max(batched, sys.float_info.min)
            self.assertTrue(math.isfinite(speedup) and speedup > 0.0)
            print("native-tree %s batch benchmark: scalar=%.6fs batch=%.6fs "
                  "speedup=%.2fx" % (engine, scalar, batched, speedup))


if __name__ == "__main__":
    unittest.main()
