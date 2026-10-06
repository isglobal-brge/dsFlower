"""All server arithmetic consumes public content order, never transport order."""
import itertools
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np
from flwr.common import ArrayRecord, Message, MetricRecord, RecordDict
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import (aggregation, server_app, native_tree_server_app,
                             native_tree_validation_server_app, epi_association,
                             association_server_app)


class AggregationOrderTests(unittest.TestCase):
    @staticmethod
    def reply(value, node_id, sigma=None):
        request = Message(content=RecordDict(), dst_node_id=node_id, message_type="train")
        metrics = {"num-examples": 1, "available": 1}
        if sigma is not None:
            metrics["noise-sd"] = sigma
        return Message(content=RecordDict({
            "arrays": ArrayRecord(numpy_ndarrays=[np.asarray(value, dtype=np.float64)]),
            "metrics": MetricRecord(metrics),
        }), reply_to=request)

    def test_neural_aggregate_arrivals_and_transient_node_ids_are_nuisance(self):
        values = [np.array([1.e16, 2., 3.]), np.array([-1.e16, 3., 4.]),
                  np.array([1., 4., 5.])]
        outputs = []
        for permutation in itertools.permutations(range(3)):
            strategy = server_app._build_strategy({"strategy": "fedavg"}, min_nodes=3)
            replies = [self.reply(values[i], 800 + 13*j) for j, i in enumerate(permutation)]
            result, _ = strategy.aggregate_train(1, replies)
            outputs.append(result.to_numpy_ndarrays()[0].tobytes())
        self.assertEqual(len(set(outputs)), 1)

    def test_all_vector_poolers_are_exactly_arrival_independent(self):
        values = [np.full(9, 1.e20), np.full(9, -1.e20), np.full(9, 1.),
                  np.full(9, 1.)]  # duplicates must retain their contribution
        for pool in (lambda xs: server_app._pool_private_vectors(xs, 9),
                     native_tree_server_app._stable_pool,
                     native_tree_validation_server_app._stable_pool,
                     epi_association._pooled_vector):
            results = {pool([values[i] for i in perm]).tobytes()
                       for perm in itertools.permutations(range(4))}
            self.assertEqual(len(results), 1)
        self.assertEqual(server_app._pool_private_vectors([values[2], values[3]], 9)[0], 2.)

    def test_association_collectors_preserve_vector_sigma_pairing(self):
        vectors = [np.full(9, i + .25) for i in range(3)]
        sigmas = [1., 2., 3.]
        results = []
        for permutation in itertools.permutations(range(3)):
            ids = [90, 30, 70]
            replies = [self.reply(vectors[i], ids[j], sigma=sigmas[i])
                       for j, i in enumerate(permutation)]
            grid = SimpleNamespace(send_and_receive=lambda *a, **k: replies)
            with mock.patch.object(association_server_app, "_request_messages", return_value=[]):
                arrays, noise = association_server_app._collect_releases(grid, ids, {}, 30.)
            results.append(tuple((array.tobytes(), sigma) for array, sigma in zip(arrays, noise)))
        self.assertEqual(len(set(results)), 1)

    def test_content_hash_collisions_use_canonical_byte_tiebreak(self):
        values = [np.array([2.]), np.array([1.]), np.array([1.])]
        class Hash:
            def digest(self): return b"x" * 32
        with mock.patch.object(aggregation.hashlib, "sha256", return_value=Hash()):
            ordered = sorted(values, key=aggregation.vector_key)
            other = sorted(reversed(values), key=aggregation.vector_key)
        self.assertEqual([x.tobytes() for x in ordered], [x.tobytes() for x in other])
        self.assertEqual(len(ordered), 3)


if __name__ == "__main__":
    unittest.main()
