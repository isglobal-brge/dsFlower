"""End-to-end and fail-closed tests for the dedicated association apps."""

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
from flwr.common import ArrayRecord, Message, MetricRecord, RecordDict


TESTS = os.path.dirname(os.path.abspath(__file__))
FLOWER_APP = os.path.join(TESTS, "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import epi_association, seeding, task
from dsflower_runner import association_parquet
from dsflower_runner import association_client_app as client_app
from dsflower_runner import association_server_app as server_app


_PINS = {
    "association-contract": epi_association.ASSOCIATION_CONTRACT,
    "association-contract-sha256": "a" * 64,
    "association-job-sha256": "b" * 64,
    "association-n-nodes": 2,
    "association-privacy-unit": "row",
    "association-unit-semantics": "row-one-hot/v1",
}


def _manifest(rows=4):
    return {
        "data_type": "tabular", "data_file": "train.csv",
        "data_format": "csv", "dp-track": "association",
        "num-server-rounds": 1, "target-preencoded": True,
        "association-preencoded": True,
        "target_column": "outcome", "feature_columns": ["exposure"],
        "user-module": None, "dp-unit": "row", "patient_column": None,
        "patient-id-canonicalization": "trim-utf8-v2",
        "n_units": rows, "n_samples": rows,
        "privacy-adjacency": "replace_one", "privacy-epsilon": 1.0,
        "privacy-delta": 1.0e-6, "privacy-clipping_norm": 1.0,
        "privacy-policy-sha256": "c" * 64,
        **_PINS,
    }


def _write_node(root, frame=None):
    if frame is None:
        frame = pd.DataFrame({
            "outcome": [0, 1, 1, 2], "exposure": [0, 0, 1, 2]})
    frame.to_csv(os.path.join(root, "train.csv"), index=False)
    manifest = _manifest(len(frame))
    with open(os.path.join(root, "manifest.json"), "w",
              encoding="utf-8") as handle:
        json.dump(manifest, handle)
    return manifest


def _config(results_dir):
    return {
        "dp-track": "association", "num-server-rounds": 1,
        "min-train-nodes": 2, "round-timeout": 1,
        "results-dir": results_dir, **_PINS,
    }


class _Grid:
    def __init__(self, context, node_ids=(11, 22), duplicate=False,
                 reverse=False):
        self.context = context
        self.node_ids = tuple(node_ids)
        self.duplicate = duplicate
        self.reverse = reverse
        self.messages = []

    def get_node_ids(self):
        return list(self.node_ids)

    def send_and_receive(self, messages, timeout):
        self.messages = list(messages)
        replies = [client_app.train(message, self.context)
                   for message in self.messages]
        if self.duplicate:
            replies[1] = client_app.train(self.messages[0], self.context)
        return list(reversed(replies)) if self.reverse else replies


class AssociationClientTests(unittest.TestCase):
    def test_pin_tamper_and_unexpected_fields_fail_before_private_read(self):
        cases = (
            ("typed node count", {"association-n-nodes": True}),
            ("job digest", {"association-job-sha256": "0" * 64}),
            ("roster", {"min-train-nodes": 1}),
            ("unexpected field", {"association-reroll": "forbidden"}),
        )
        for label, changed in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results_dir:
                _write_node(root)
                cfg = _config(results_dir)
                message = server_app._request_messages((1,), cfg)[0]
                cfg.update(changed)
                context = SimpleNamespace(
                    node_config={"manifest-dir": root}, run_config=cfg)
                with mock.patch.object(
                        task, "load_association_data",
                        side_effect=AssertionError("private data was read")) as load:
                    reply = client_app.train(message, context)
                load.assert_not_called()
                self.assertEqual(dict(reply.content["metrics"]), {
                    "available": 0, "noise-sd": 0.0, "num-examples": 1})

    def test_loader_totalizes_codes_and_release_is_one_fixed_transcript(self):
        frame = pd.DataFrame({
            "outcome": ["0", "1", "bad", "3"],
            "exposure": ["1", "0", "2", "-1"],
        })
        with tempfile.TemporaryDirectory() as root, \
                tempfile.TemporaryDirectory() as results_dir:
            manifest = _write_node(root, frame)
            cfg = _config(results_dir)
            context = SimpleNamespace(
                node_config={"manifest-dir": root}, run_config=cfg)
            outcome, exposure, units = task.load_association_data(
                context, manifest=manifest)
            np.testing.assert_array_equal(outcome, [0, 1, 2, 2])
            np.testing.assert_array_equal(exposure, [1, 0, 2, 2])
            self.assertIsNone(units)

            message = server_app._request_messages((1,), cfg)[0]
            original_load = task.load_association_data
            original_release = epi_association.private_association_vector
            with (mock.patch.object(
                    task, "load_association_data", wraps=original_load) as load,
                  mock.patch.object(
                    epi_association, "private_association_vector",
                    wraps=original_release) as release,
                  mock.patch.object(
                    seeding, "_node_secret", return_value=b"s" * 32)):
                reply = client_app.train(message, context)
            load.assert_called_once()
            release.assert_called_once()
            vector = reply.content["arrays"].to_numpy_ndarrays()[0]
            self.assertEqual(vector.dtype, np.dtype(np.float64))
            self.assertEqual(vector.shape, (9,))
            self.assertTrue(np.all(np.isfinite(vector)))
            metrics = dict(reply.content["metrics"])
            self.assertEqual(metrics["available"], 1)
            self.assertGreater(metrics["noise-sd"], 0.0)

    def test_isolation_guard_rejects_wider_or_uploaded_code(self):
        with mock.patch.dict(
                sys.modules, {"dsflower_runner.client_app": object()},
                clear=False):
            with self.assertRaisesRegex(RuntimeError, "not isolated"):
                client_app._assert_association_process_isolated()
        with mock.patch.dict(os.environ, {
                "DSFLOWER_PINNED_APP_DIR": "/uploaded/app"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "uploaded code"):
                client_app._assert_association_process_isolated()


class AssociationServerTests(unittest.TestCase):
    def test_complete_release_is_pooled_only_and_reply_order_stable(self):
        outputs = []
        for reverse in (False, True):
            with tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results_dir:
                _write_node(root)
                cfg = _config(results_dir)
                context = SimpleNamespace(
                    node_config={"manifest-dir": root}, run_config=cfg)
                grid = _Grid(context, reverse=reverse)
                with mock.patch.object(
                        seeding, "_node_secret", return_value=b"s" * 32):
                    server_app.main(grid, SimpleNamespace(run_config=cfg))
                with open(os.path.join(results_dir, server_app.RESULT_FILE),
                          "rb") as handle:
                    outputs.append(handle.read())
                released = json.loads(outputs[-1])
                self.assertTrue(released["available"])
                self.assertEqual(
                    released["contract"], epi_association.RESULT_CONTRACT)
                self.assertEqual(released["association_contract"],
                                 epi_association.ASSOCIATION_CONTRACT)
                self.assertEqual(released["association_contract_sha256"],
                                 "a" * 64)
                self.assertEqual(released["association_job_sha256"], "b" * 64)
                self.assertEqual(released["n_nodes"], 2)
                self.assertEqual(released["unit_semantics"], "row-one-hot/v1")
                self.assertNotIn("per_node", released)
                self.assertNotIn("vectors", released)
                self.assertEqual(set(os.listdir(results_dir)), {
                    server_app.RESULT_FILE, server_app.HISTORY_FILE})
                with open(os.path.join(results_dir, server_app.HISTORY_FILE),
                          encoding="ascii") as handle:
                    self.assertEqual(json.load(handle), [
                        {"available": True, "round": 1}])
                if os.name != "nt":
                    mode = stat.S_IMODE(os.stat(os.path.join(
                        results_dir, server_app.RESULT_FILE)).st_mode)
                    self.assertEqual(mode, 0o600)
                self.assertEqual(len(grid.messages), 2)
        self.assertEqual(outputs[0], outputs[1])

    def test_extra_missing_or_duplicate_roster_publishes_no_statistics(self):
        cases = (
            ("extra", (1, 2, 3), False),
            ("missing", (11,), False),
            ("duplicate reply", (11, 22), True),
        )
        for label, node_ids, duplicate in cases:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as root, \
                    tempfile.TemporaryDirectory() as results_dir:
                _write_node(root)
                cfg = _config(results_dir)
                context = SimpleNamespace(
                    node_config={"manifest-dir": root}, run_config=cfg)
                grid = _Grid(context, node_ids=node_ids, duplicate=duplicate)
                clocks = (mock.patch.object(
                    server_app.time, "monotonic", side_effect=(0.0, 2.0))
                    if label == "missing" else mock.patch.object(
                        server_app.time, "monotonic",
                        wraps=server_app.time.monotonic))
                with (clocks, mock.patch.object(
                        seeding, "_node_secret", return_value=b"s" * 32)):
                    server_app.main(grid, SimpleNamespace(run_config=cfg))
                with open(os.path.join(results_dir, server_app.RESULT_FILE),
                          encoding="ascii") as handle:
                    released = json.load(handle)
                self.assertFalse(released["available"])
                self.assertEqual(released["association_contract_sha256"],
                                 "a" * 64)
                self.assertEqual(released["association_job_sha256"], "b" * 64)
                for forbidden in (
                        "table_dp", "measures", "noise_sd_pooled", "per_node"):
                    self.assertNotIn(forbidden, released)

    def test_invalid_vector_or_sigma_is_not_accepted_as_a_node_release(self):
        with tempfile.TemporaryDirectory() as results_dir:
            cfg = _config(results_dir)
            request = server_app._request_messages((11,), cfg)[0]

            def reply(vector, sigma):
                return Message(content=RecordDict({
                    "arrays": ArrayRecord(numpy_ndarrays=[
                        np.asarray(vector, dtype=np.float64)]),
                    "metrics": MetricRecord({
                        "available": 1, "noise-sd": float(sigma),
                        "num-examples": 1}),
                }), reply_to=request)

            for label, candidate in (
                    ("short vector", reply(np.zeros(8), 1.0)),
                    ("non-finite vector", reply(
                        np.asarray([np.nan] + [0.0] * 8), 1.0)),
                    ("zero sigma", reply(np.zeros(9), 0.0)),
                    ("non-finite sigma", reply(np.zeros(9), np.inf))):
                with self.subTest(case=label), self.assertRaises(RuntimeError):
                    server_app._release_from_reply(candidate)

    def test_history_is_the_last_commit_marker(self):
        with tempfile.TemporaryDirectory() as results_dir:
            contract = server_app._run_contract(_config(results_dir))
            unavailable = server_app._result(contract, [], [])
            original = server_app._atomic_write

            def fail_history(path, payload):
                if path.endswith(server_app.HISTORY_FILE):
                    raise OSError("commit marker failure")
                return original(path, payload)

            with mock.patch.object(
                    server_app, "_atomic_write", side_effect=fail_history):
                with self.assertRaisesRegex(OSError, "commit marker"):
                    server_app._save_result(results_dir, unavailable)
            self.assertFalse(os.path.exists(os.path.join(
                results_dir, server_app.RESULT_FILE)))
            self.assertFalse(os.path.exists(os.path.join(
                results_dir, server_app.HISTORY_FILE)))


class AssociationParquetTests(unittest.TestCase):
    def test_selected_dictionary_expansion_is_rejected_before_r_decode(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = 50_000
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source.parquet")
            destination = os.path.join(root, "projection.parquet")
            pq.write_table(pa.table({
                "outcome": ["x" * 4096] * rows,
                "exposure": [0] * rows,
                "unused": ["private"] * rows,
            }), source, compression="gzip", use_dictionary=True)
            self.assertLess(os.path.getsize(source), 1024 * 1024)
            with open(source, "rb") as handle:
                dictionary_table = pq.ParquetFile(
                    handle, read_dictionary=["outcome"]).read(
                        columns=["outcome"], use_threads=False)
            self.assertTrue(pa.types.is_dictionary(
                dictionary_table.column("outcome").chunk(0).type))

            with self.assertRaisesRegex(ValueError, "physical cap"):
                association_parquet.materialize_bounded_projection(
                    source, destination, ["outcome", "exposure"],
                    max_rows=rows, max_bytes=8 * 1024 * 1024)
            self.assertFalse(os.path.lexists(destination))

    def test_projection_uses_only_selected_columns_and_exact_metadata(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source.parquet")
            destination = os.path.join(root, "projection.parquet")
            nested = pa.StructArray.from_arrays([
                pa.array([1, 2, 3]), pa.array([4, 5, 6]),
                pa.array([7, 8, 9])], names=["a", "b", "c"])
            table = pa.Table.from_arrays([
                nested, pa.array(["no", "yes", None]),
                pa.array([0, 1, 2])],
                names=["unused_nested", "outcome", "exposure"])
            pq.write_table(
                table, source, compression="gzip", use_dictionary=True)
            result = association_parquet.materialize_bounded_projection(
                source, destination, ["outcome", "exposure"],
                max_rows=10, max_bytes=1024 * 1024)
            self.assertEqual(result["contract"],
                             "dsflower-association-parquet-projection/v1")
            self.assertEqual(result["rows"], 3)
            self.assertGreater(result["materialized_bytes"], 0)
            self.assertEqual(len(result["sha256"]), 64)
            self.assertEqual(os.path.getsize(destination),
                             result["file_bytes"])
            projected = pq.read_table(destination)
            self.assertEqual(projected.column_names,
                             ["outcome", "exposure"])
            self.assertEqual(projected.num_rows, 3)
            if os.name != "nt":
                self.assertEqual(
                    stat.S_IMODE(os.stat(destination).st_mode), 0o600)


class AssociationRuntimeProbeTests(unittest.TestCase):
    def test_fresh_subprocess_is_dependency_light_and_operational(self):
        code = "\n".join((
            "import sys",
            "sys.path.insert(0, sys.argv[1])",
            "from dsflower_runner import association_runtime_probe as probe",
            "from dsflower_runner import association_client_app as client_entry",
            "from dsflower_runner import association_server_app as server_entry",
            "from flwr.clientapp import ClientApp",
            "from flwr.serverapp import ServerApp",
            "assert isinstance(client_entry.app, ClientApp)",
            "assert isinstance(server_entry.app, ServerApp)",
            "assert probe.probe_association_runtime()",
            "assert not any(name == 'torch' or name.startswith('torch.') "
            "or name.startswith('dsflower_runner.xgboost') for name in sys.modules)",
            "sys.stdout.write('available')",
        ))
        environment = os.environ.copy()
        for key in ("PYTHONPATH", "PYTHONSTARTUP", "PYTHONINSPECT",
                    "DSFLOWER_MANIFEST_DIR", "DSFLOWER_PINNED_APP_DIR"):
            environment.pop(key, None)
        completed = subprocess.run(
            [sys.executable, "-I", "-c", code, FLOWER_APP],
            capture_output=True, text=True, timeout=30, env=environment,
            check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "available")


if __name__ == "__main__":
    unittest.main()
