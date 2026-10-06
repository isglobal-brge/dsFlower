"""Node-side raw source sidecars prevent pre-totalization content collisions."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import canonical_units, source_projection, task


class SourceProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.context = SimpleNamespace(node_config={"manifest-dir": self.temp.name})
        self.manifest = {"data_type": "tabular", "data_file": "train.csv",
                         "feature_columns": ["x"], "target_column": "y",
                         "dp-unit": "row", "num-classes": 2,
                         "task-type": "classification", "patient_column": None,
                         "semantic-randomness-contract": "dsflower-semantic-randomness-v3"}
        self.secret = mock.patch("dsflower_runner.seeding._node_secret", return_value=b"s" * 32)
        self.secret.start(); self.addCleanup(self.secret.stop)

    def write(self, source, ids=None):
        effective = pd.DataFrame({"x": np.zeros(len(source)), "y": np.zeros(len(source))})
        if ids is not None:
            effective["id"] = ids
            self.manifest.update({"dp-unit": "patient", "patient_column": "id",
                                  "patient-id-canonicalization": "trim-utf8-v2"})
        data = self.directory / "train.csv"
        effective.to_csv(data, index=False)
        path = self.directory / "source-projection.jsonl"
        header = {"schema": "dsflower-source-projection-v1", "columns": ["x", "y"],
                  "patient_column": self.manifest["patient_column"]}
        records = [{"values": [value, {"type": "number", "value": "0"}],
                    "patient_id": None if ids is None else ids[i]}
                   for i, value in enumerate(source)]
        path.write_text("\n".join(json.dumps(value, separators=(",", ":"))
                                   for value in [header, *records]) + "\n")
        path.chmod(0o600)
        self.manifest.update({"source_projection_file": path.name,
            "source_projection_schema": header["schema"],
            "source_projection_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_effective_sha256": hashlib.sha256(data.read_bytes()).hexdigest()})
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest))
        return effective

    def load(self):
        return task.load_data(self.context, include_unit_ids=True, include_canonical_units=True)

    def test_equal_effective_tensors_distinct_raw_invalids_change_binding(self):
        results = []
        for value in ({"type": "missing"}, {"type": "nan"}, {"type": "posinf"},
                      {"type": "neginf"}, {"type": "utf8", "value": "invalid-a"},
                      {"type": "utf8", "value": "invalid-b"}):
            self.write([value]); results.append(self.load())
        self.assertEqual(len({result[0].tobytes() for result in results}), 1)
        self.assertEqual(len({result[-1].multiset_digest for result in results}), len(results))

    def test_raw_patient_units_and_invalid_rows_shuffle_exactly(self):
        values = [{"type": "utf8", "value": "a"}, {"type": "nan"},
                  {"type": "utf8", "value": "a"}, {"type": "posinf"}]
        ids = ["p", "q", "p", "p"]
        self.write(values, ids); first = self.load()
        p = [3, 2, 1, 0]
        self.write([values[i] for i in p], [ids[i] for i in p]); second = self.load()
        self.assertEqual(first[-1].records, second[-1].records)
        self.assertEqual(first[-1].multiset_digest, second[-1].multiset_digest)
        for a, b in zip(first[:2], second[:2]): self.assertEqual(a.tobytes(), b.tobytes())

    def test_selected_boolean_csv_and_parquet_aliases_share_source_record(self):
        normalizers = {"x": lambda value: {"FALSE": 0, "TRUE": 1}.get(value, value)}
        for logical, number, text in ((False, "0", "FALSE"), (True, "1", "TRUE")):
            records = []
            for cell in ({"type": "bool", "value": logical},
                         {"type": "number", "value": number},
                         {"type": "utf8", "value": text}):
                effective = self.write([cell])
                records.append(source_projection.records(
                    self.context, self.manifest, effective, ["x", "y"],
                    normalizers=normalizers))
            self.assertEqual(records[0], records[1])
            self.assertEqual(records[0], records[2])

    def test_supervised_native_and_association_keep_the_same_full_source_multiset(self):
        values = [{"type": "nan"}, {"type": "utf8", "value": "invalid"},
                  {"type": "nan"}, {"type": "posinf"}]
        loaders = (task.load_data, task.load_native_tree_data, task.load_association_data)
        for ids in (None, ["a", "b", "a", "a"]):
            with self.subTest(privacy_unit="row" if ids is None else "patient"):
                self.write(values, ids)
                before = [loader(self.context, include_canonical_units=True)[-1]
                          for loader in loaders]
                self.assertEqual(len({units.multiset_digest for units in before}), 1)
                p = [3, 2, 1, 0]
                self.write([values[i] for i in p], None if ids is None else [ids[i] for i in p])
                after = [loader(self.context, include_canonical_units=True)[-1]
                         for loader in loaders]
                for first, second in zip(before, after):
                    self.assertEqual(first.multiset_digest, second.multiset_digest)
                    self.assertEqual(first.records, second.records)
                    self.assertEqual(first.row_tokens, second.row_tokens)
                # Removing an equal-valued source row changes multiplicity even
                # though all effective feature/outcome/exposure values are zero.
                self.write(values[1:], None if ids is None else ids[1:])
                removed = [loader(self.context, include_canonical_units=True)[-1]
                           for loader in loaders]
                self.assertTrue(all(a.multiset_digest != b.multiset_digest
                                    for a, b in zip(before, removed)))

    def test_sidecar_and_effective_integrity_pins_fail_closed(self):
        for filename in ("train.csv", "source-projection.jsonl"):
            with self.subTest(filename=filename):
                self.write([{"type": "nan"}])
                with open(self.directory / filename, "a") as handle: handle.write("\n")
                with self.assertRaisesRegex(ValueError, "changed after pinning"):
                    self.load()
        self.write([{"type": "nan"}])
        (self.directory / "source-projection.jsonl").chmod(0o644)
        with self.assertRaisesRegex(ValueError, "owner-only"):
            self.load()

    def test_source_schema_and_missing_v3_projection_fail_closed(self):
        effective = self.write([{"type": "nan"}])
        no_source = {k: v for k, v in self.manifest.items() if not k.startswith("source_")}
        with self.assertRaisesRegex(ValueError, "missing.*projection"):
            source_projection.records(self.context, no_source, effective, ["x", "y"])
        with self.assertRaisesRegex(ValueError, "schema differs"):
            source_projection.records(self.context, self.manifest, effective, ["y", "x"])
        for value in ({"type": "number", "value": "nan"},
                      {"type": "nan", "value": "forged"},
                      {"type": "bool", "value": 1}):
            with self.assertRaises(ValueError): source_projection._cell(value)


if __name__ == "__main__": unittest.main()
