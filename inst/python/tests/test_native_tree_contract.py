"""Security and canonicalization tests for the internal native-tree ABI.

Run with:
    python3 dsFlower/inst/python/tests/test_native_tree_contract.py
"""

import copy
import hashlib
import json
import os
import sys
import unittest


RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import native_tree_contract as contract


def _typed(kind, value):
    return {"type": kind, "value": value}


def _public_schema(cuts=True, task="binary_classification"):
    target = (
        {"name": "outcome", "kind": "binary", "lower": 0.0, "upper": 1.0}
        if task == "binary_classification"
        else {
            "name": "outcome", "kind": "continuous",
            "lower": -10.0, "upper": 10.0,
        }
    )
    core = {
        "version": 1,
        "features": ["age", "marker"],
        "lower": [0.0, -5.0],
        "upper": [100.0, 5.0],
        "cuts": [[18.0, 40.0, 65.0], [-1.0, 0.0, 1.0]] if cuts else None,
        "target": target,
    }
    wire = json.dumps(
        core, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    return dict(core, sha256=hashlib.sha256(wire).hexdigest())


def _manifest(mode="native-tight", engine="xgboost",
              task="binary_classification"):
    mechanism = "dp-synopsis-v1" if mode == "synopsis-flex" else (
        "dp-forest-v1" if engine in ("random_forest", "extra_trees")
        else "dp-histogram-v1"
    )
    return {
        "contract_version": 1,
        "mode": mode,
        "engine": engine,
        "task": task,
        "public_schema": _public_schema(
            cuts=mode == "native-tight", task=task
        ),
        "engine_params": {
            "learning_rate": _typed("float", 0.1),
            "max_depth": _typed("int", 6),
            "monotone_constraints": _typed("int_list", [0, 1, -1]),
        },
        "privacy": {
            "mechanism": mechanism,
            "epsilon": 1.0,
            "delta": 1e-6,
            "unit": "patient",
            "adjacency": "replace_one",
            "unit_canonicalization": "trim-utf8-v2",
            "contribution_strategy": "one-record-per-unit-v1",
            "max_rows_per_unit": 1,
            "mechanism_params": {
                "gradient_clip": _typed("float", 1.0),
                "hessian_clip": _typed("float", 1.0),
            },
        },
        "data_scope": {
            "snapshot_hash": "a" * 64,
            "cohort_hash": "b" * 64,
            "schema_hash": _public_schema(
                cuts=mode == "native-tight", task=task
            )["sha256"],
        },
        "resources": {
            "threads": 4,
            "memory_mib": 4096,
            "wall_seconds": 900,
            "max_rows": 1_000_000,
            "max_features": 1024,
            "max_trees": 1000,
            "max_depth": 16,
            "max_bins": 1024,
            "max_artifact_bytes": 16 * 1024 * 1024,
        },
    }


class ManifestTests(unittest.TestCase):
    def test_manifest_is_canonical_and_order_independent(self):
        manifest = _manifest()
        reordered = dict(reversed(list(manifest.items())))
        reordered["engine_params"] = dict(
            reversed(list(manifest["engine_params"].items()))
        )
        first = contract.canonical_engine_manifest(manifest)
        second = contract.canonical_engine_manifest(reordered)
        self.assertEqual(first, second)
        self.assertEqual(
            contract.canonical_manifest_bytes(first),
            contract.canonical_manifest_bytes(second),
        )
        self.assertEqual(list(first["engine_params"]), sorted(first["engine_params"]))

    def test_typed_values_are_strict_and_finite(self):
        cases = [
            ("max_depth", _typed("int", True)),
            ("learning_rate", _typed("float", float("nan"))),
            ("labels", _typed("string_list", ["ok", "bad\nvalue"])),
            ("unknown", {"type": "int", "value": 1, "extra": False}),
        ]
        for name, value in cases:
            with self.subTest(name=name):
                manifest = _manifest()
                manifest["engine_params"] = {name: value}
                with self.assertRaises(ValueError):
                    contract.canonical_engine_manifest(manifest)

    def test_errors_never_echo_parameter_values(self):
        manifest = _manifest()
        secret = "TOP-SECRET-VALUE\n"
        manifest["engine_params"] = {"label": _typed("string", secret)}
        with self.assertRaises(ValueError) as raised:
            contract.canonical_engine_manifest(manifest)
        self.assertNotIn(secret, str(raised.exception))

    def test_backend_control_and_io_parameters_are_rejected(self):
        for name in (
            "seed", "nthread", "device", "updater", "objective",
            "model_file", "snapshot_path", "callbacks", "machine_list_file",
            "max_rows_per_unit", "unit_canonicalization", "privacy_epsilon",
        ):
            with self.subTest(name=name):
                manifest = _manifest()
                manifest["engine_params"] = {name: _typed("int", 1)}
                with self.assertRaisesRegex(ValueError, "reserved"):
                    contract.canonical_engine_manifest(manifest)

        synopsis = _manifest(mode="synopsis-flex")
        synopsis["engine_params"] = {
            "objective": _typed("string", "binary:logistic"),
            "eval_metric": _typed("string", "auc"),
            "early_stopping_rounds": _typed("int", 10),
            "seed": _typed("int", 7),
        }
        self.assertEqual(
            len(contract.canonical_engine_manifest(synopsis)["engine_params"]),
            4,
        )
        synopsis["engine_params"] = {"callbacks": _typed("string", "unsafe")}
        with self.assertRaisesRegex(ValueError, "reserved"):
            contract.canonical_engine_manifest(synopsis)

        synopsis["engine_params"] = {
            "custom_objective_fn": _typed("string", "unsafe")
        }
        with self.assertRaisesRegex(ValueError, "reserved"):
            contract.canonical_engine_manifest(synopsis)

        synopsis = _manifest(mode="synopsis-flex")
        synopsis["privacy"]["mechanism_params"] = {"seed": _typed("int", 7)}
        with self.assertRaisesRegex(ValueError, "reserved"):
            contract.canonical_engine_manifest(synopsis)

    def test_manifest_and_resource_caps_are_strict(self):
        for name, limit in contract.RESOURCE_HARD_CAPS.items():
            manifest = _manifest()
            manifest["resources"][name] = limit + 1
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "resource limit"):
                    contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        manifest["resources"]["threads"] = True
        with self.assertRaisesRegex(ValueError, "resource limit"):
            contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        manifest["unexpected"] = "not accepted"
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        manifest["engine_params"]["max_depth"]["value"] = 17
        manifest["resources"]["max_depth"] = 16
        with self.assertRaisesRegex(ValueError, "resource limit"):
            contract.canonical_engine_manifest(manifest)

    def test_mode_engine_and_mechanism_combinations_are_pinned(self):
        manifest = _manifest(mode="synopsis-flex")
        manifest["privacy"]["mechanism"] = "dp-histogram-v1"
        with self.assertRaisesRegex(ValueError, "mechanism"):
            contract.canonical_engine_manifest(manifest)

    def test_public_schema_is_hashed_bounded_and_tight_cuts_are_mandatory(self):
        manifest = _manifest()
        canonical = contract.canonical_engine_manifest(manifest)
        self.assertEqual(
            canonical["public_schema"]["sha256"],
            "a24299d5ccba8a1af70f0c2d5afa06937d9632a75bc69d20d3e1520ec96d5733",
        )

        tampered = copy.deepcopy(manifest)
        tampered["public_schema"]["upper"][0] = 101.0
        with self.assertRaisesRegex(ValueError, "schema digest"):
            contract.canonical_engine_manifest(tampered)

        missing_cuts = copy.deepcopy(manifest)
        missing_cuts["public_schema"] = _public_schema(cuts=False)
        missing_cuts["data_scope"]["schema_hash"] = missing_cuts[
            "public_schema"
        ]["sha256"]
        with self.assertRaisesRegex(ValueError, "public cuts"):
            contract.canonical_engine_manifest(missing_cuts)

        synopsis = _manifest(mode="synopsis-flex")
        self.assertIsNone(
            contract.canonical_engine_manifest(synopsis)["public_schema"]["cuts"]
        )

        mismatch = _manifest()
        mismatch["data_scope"]["schema_hash"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "schema identities"):
            contract.canonical_engine_manifest(mismatch)

        bad_target = _manifest()
        bad_target["public_schema"]["target"]["upper"] = 2.0
        core = {
            key: bad_target["public_schema"][key]
            for key in ("version", "features", "lower", "upper", "cuts", "target")
        }
        bad_target["public_schema"]["sha256"] = hashlib.sha256(
            json.dumps(
                core, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        bad_target["data_scope"]["schema_hash"] = bad_target[
            "public_schema"
        ]["sha256"]
        with self.assertRaisesRegex(ValueError, "binary task"):
            contract.canonical_engine_manifest(bad_target)

        duplicate_target = _manifest()
        duplicate_target["public_schema"]["target"]["name"] = "age"
        with self.assertRaisesRegex(ValueError, "target name"):
            contract.canonical_engine_manifest(duplicate_target)

    def test_privacy_unit_is_server_pinned_but_supports_rows_and_patients(self):
        manifest = _manifest()
        manifest["privacy"]["unit"] = "row"
        self.assertEqual(
            contract.canonical_engine_manifest(manifest)["privacy"]["unit"],
            "row",
        )
        manifest["privacy"]["adjacency"] = "add-remove"
        with self.assertRaisesRegex(ValueError, "adjacency"):
            contract.canonical_engine_manifest(manifest)

        for field, value in (
            ("unit_canonicalization", "raw"),
            ("contribution_strategy", "unbounded"),
            ("max_rows_per_unit", 2),
        ):
            manifest = _manifest()
            manifest["privacy"][field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        del manifest["privacy"]["max_rows_per_unit"]
        with self.assertRaisesRegex(ValueError, "missing required fields"):
            contract.canonical_engine_manifest(manifest)

        manifest = _manifest(engine="random_forest")
        manifest["privacy"]["mechanism"] = "dp-histogram-v1"
        with self.assertRaisesRegex(ValueError, "mechanism"):
            contract.canonical_engine_manifest(manifest)


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = bytes(range(32))

    def test_synopsis_identity_shares_engines_and_hpo_but_not_tasks(self):
        base = _manifest(mode="synopsis-flex")
        query_id = contract.semantic_query_identity(self.root, base)

        changed_params = copy.deepcopy(base)
        changed_params["engine_params"]["learning_rate"]["value"] = 0.9
        self.assertEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_params),
        )

        changed_resources = copy.deepcopy(base)
        changed_resources["resources"]["threads"] = 1
        self.assertEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_resources),
        )

        changed_engine = copy.deepcopy(base)
        changed_engine["engine"] = "lightgbm"
        self.assertEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_engine),
        )

        changed_task = _manifest(mode="synopsis-flex", task="regression")
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_task),
        )

        changed_mechanism = copy.deepcopy(base)
        changed_mechanism["privacy"]["mechanism_params"]["gradient_clip"]["value"] = 2.0
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_mechanism),
        )

        changed_scope = copy.deepcopy(base)
        changed_scope["data_scope"]["snapshot_hash"] = "e" * 64
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_scope),
        )

    def test_native_tight_identity_includes_engine_task_and_mechanism_parameters(self):
        base = _manifest()
        query_id = contract.semantic_query_identity(self.root, base)

        changed_engine = copy.deepcopy(base)
        changed_engine["engine_params"]["max_depth"]["value"] = 7
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_engine),
        )

        changed_mechanism = copy.deepcopy(base)
        changed_mechanism["privacy"]["mechanism_params"]["hessian_clip"]["value"] = 2.0
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_mechanism),
        )

        changed_unit = copy.deepcopy(base)
        changed_unit["privacy"]["unit"] = "row"
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_unit),
        )

        changed_task = _manifest(task="regression")
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_task),
        )

        changed_allocation = copy.deepcopy(base)
        changed_allocation["privacy"]["epsilon"] = 2.0
        changed_allocation["privacy"]["delta"] = 2e-6
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(self.root, changed_allocation),
        )
        self.assertNotEqual(
            contract.invocation_identity(base),
            contract.invocation_identity(changed_allocation),
        )

    def test_identity_is_keyed_and_invocation_binds_every_field(self):
        manifest = _manifest(mode="synopsis-flex")
        query_id = contract.semantic_query_identity(self.root, manifest)
        self.assertRegex(query_id, r"^sq1s_[0-9a-f]{64}$")
        self.assertNotIn(self.root.hex(), query_id)
        self.assertNotEqual(
            query_id,
            contract.semantic_query_identity(b"x" * 32, manifest),
        )
        with self.assertRaisesRegex(ValueError, "exactly 32 bytes"):
            contract.semantic_query_identity(b"short", manifest)

        changed = copy.deepcopy(manifest)
        changed["engine_params"]["max_depth"]["value"] = 9
        self.assertNotEqual(
            contract.invocation_identity(manifest),
            contract.invocation_identity(changed),
        )

        native_id = contract.semantic_query_identity(self.root, _manifest())
        self.assertRegex(native_id, r"^sq1n_[0-9a-f]{64}$")
        changed = copy.deepcopy(manifest)
        changed["resources"]["threads"] = 2
        self.assertNotEqual(
            contract.invocation_identity(manifest),
            contract.invocation_identity(changed),
        )


class ResultTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest()
        self.query_id = contract.semantic_query_identity(b"q" * 32, self.manifest)
        self.invocation_id = contract.invocation_identity(self.manifest)
        self.artifact = b'{"learner":{"attributes":{}}}'
        self.metadata = contract.artifact_sanitization_metadata(
            self.manifest, self.query_id, "model"
        )
        self.result = {
            "contract_version": 1,
            "status": "ok",
            "invocation_id": self.invocation_id,
            "semantic_query_id": self.query_id,
            "engine": "xgboost",
            "mode": "native-tight",
            "artifact": {
                "kind": "model",
                "format": "xgboost-json-v1",
                "size_bytes": len(self.artifact),
                "sha256": hashlib.sha256(self.artifact).hexdigest(),
            },
            "sanitization": self.metadata,
        }

    def test_success_is_bound_to_request_and_artifact_bytes(self):
        validated = contract.validate_backend_result(
            self.result,
            self.manifest,
            expected_query_id=self.query_id,
            artifact_bytes=self.artifact,
        )
        self.assertEqual(validated, self.result)

        for field, value in (("size_bytes", 1), ("sha256", "0" * 64)):
            result = copy.deepcopy(self.result)
            result["artifact"][field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    contract.validate_backend_result(
                        result,
                        self.manifest,
                        expected_query_id=self.query_id,
                        artifact_bytes=self.artifact,
                    )

    def test_result_rejects_logs_paths_extra_fields_and_bad_attestation(self):
        for location, name, value in (
            ((), "logs", "raw row: secret"),
            (("artifact",), "path", "/private/data/model"),
            (("sanitization",), "debug", "secret"),
        ):
            result = copy.deepcopy(self.result)
            target = result
            for component in location:
                target = target[component]
            target[name] = value
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "unsupported fields"):
                    contract.validate_backend_result(
                        result,
                        self.manifest,
                        expected_query_id=self.query_id,
                        artifact_bytes=self.artifact,
                    )

        result = copy.deepcopy(self.result)
        result["sanitization"]["contains_raw_records"] = True
        with self.assertRaisesRegex(ValueError, "sanitization"):
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=self.artifact,
            )

    def test_result_rejects_wrong_ids_format_encoding_and_oversize(self):
        result = copy.deepcopy(self.result)
        result["invocation_id"] = "inv1_" + "0" * 64
        with self.assertRaisesRegex(ValueError, "invocation"):
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=self.artifact,
            )

        result = copy.deepcopy(self.result)
        result["artifact"]["format"] = "pickle"
        with self.assertRaisesRegex(ValueError, "format"):
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=self.artifact,
            )

        malformed = b"not-json"
        result = copy.deepcopy(self.result)
        result["artifact"]["size_bytes"] = len(malformed)
        result["artifact"]["sha256"] = hashlib.sha256(malformed).hexdigest()
        with self.assertRaisesRegex(ValueError, "encoding"):
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=malformed,
            )

        manifest = copy.deepcopy(self.manifest)
        manifest["resources"]["max_artifact_bytes"] = 1
        result = copy.deepcopy(self.result)
        result["invocation_id"] = contract.invocation_identity(manifest)
        with self.assertRaisesRegex(ValueError, "artifact size"):
            contract.validate_backend_result(
                result,
                manifest,
                expected_query_id=self.query_id,
                artifact_bytes=self.artifact,
            )

    def test_error_result_has_only_a_bounded_code_and_no_artifact(self):
        result = {
            "contract_version": 1,
            "status": "error",
            "invocation_id": self.invocation_id,
            "error_code": "resource_exhausted",
        }
        self.assertEqual(
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=None,
            ),
            result,
        )
        result["message"] = "private backend detail"
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            contract.validate_backend_result(
                result,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=None,
            )

        version = {
            "contract_version": 1.0,
            "status": "error",
            "invocation_id": self.invocation_id,
            "error_code": "internal_error",
        }
        with self.assertRaisesRegex(ValueError, "version"):
            contract.validate_backend_result(
                version,
                self.manifest,
                expected_query_id=self.query_id,
                artifact_bytes=None,
            )

    def test_synopsis_metadata_distinguishes_dp_artifact_and_postprocessing(self):
        manifest = _manifest(mode="synopsis-flex")
        query_id = contract.semantic_query_identity(b"s" * 32, manifest)
        synopsis = contract.artifact_sanitization_metadata(
            manifest, query_id, "synopsis"
        )
        model = contract.artifact_sanitization_metadata(
            manifest, query_id, "model"
        )
        self.assertEqual(synopsis["privacy_basis"], "dp-synopsis")
        self.assertEqual(model["privacy_basis"], "dp-synopsis-postprocessing")
        for metadata in (synopsis, model):
            self.assertFalse(metadata["contains_raw_records"])
            self.assertFalse(metadata["contains_unnoised_statistics"])
            self.assertFalse(metadata["contains_backend_logs"])
            self.assertFalse(metadata["contains_target_name"])


if __name__ == "__main__":
    unittest.main()
