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
        {
            "name": "outcome", "kind": "binary",
            "levels": [
                {"type": "string", "value": "control"},
                {"type": "string", "value": "case"},
            ],
            "lower": 0.0, "upper": 1.0,
        }
        if task == "binary_classification"
        else {
            "name": "outcome", "kind": "continuous",
            "levels": None, "lower": -10.0, "upper": 10.0,
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


def _manifest(engine="xgboost", task="binary_classification"):
    mechanism = (
        "dp-forest-v1" if engine in ("random_forest", "extra_trees")
        else "dp-histogram-v1"
    )
    return {
        "contract_version": 1,
        "mode": "native-tight",
        "engine": engine,
        "task": task,
        "public_schema": _public_schema(task=task),
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
            "schema_hash": _public_schema(task=task)["sha256"],
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
            "eval_metric", "early_stopping_rounds", "custom_objective_fn",
        ):
            with self.subTest(name=name):
                manifest = _manifest()
                manifest["engine_params"] = {name: _typed("int", 1)}
                with self.assertRaisesRegex(ValueError, "reserved"):
                    contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        manifest["privacy"]["mechanism_params"] = {"seed": _typed("int", 7)}
        with self.assertRaisesRegex(ValueError, "reserved"):
            contract.canonical_engine_manifest(manifest)

    def test_manifest_and_resource_caps_are_strict(self):
        self.assertEqual(
            contract.RESOURCE_HARD_CAPS["max_artifact_bytes"],
            64 * 1024 * 1024,
        )
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

    def test_mode_and_mechanism_are_pinned(self):
        manifest = _manifest()
        manifest["mode"] = "unsupported"
        with self.assertRaisesRegex(ValueError, "mode"):
            contract.canonical_engine_manifest(manifest)

        manifest = _manifest()
        manifest["privacy"]["mechanism"] = "dp-forest-v1"
        with self.assertRaisesRegex(ValueError, "mechanism"):
            contract.canonical_engine_manifest(manifest)

    def test_public_schema_is_hashed_bounded_and_tight_cuts_are_mandatory(self):
        manifest = _manifest()
        canonical = contract.canonical_engine_manifest(manifest)
        self.assertEqual(
            canonical["public_schema"]["sha256"],
            "77a6e8d46a174381b8b4da168b833b2ee75f09f8ca8ac55f2c954be642ba9073",
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

    def test_binary_target_levels_are_ordered_typed_and_schema_bound(self):
        base = _manifest()
        canonical = contract.canonical_engine_manifest(base)
        self.assertEqual(canonical["public_schema"]["target"]["levels"], [
            {"type": "string", "value": "control"},
            {"type": "string", "value": "case"},
        ])

        for levels in (
                None,
                [{"type": "string", "value": "case"}],
                [
                    {"type": "string", "value": "case"},
                    {"type": "string", "value": "case"},
                ],
                [
                    {"type": "number", "value": True},
                    {"type": "number", "value": 1.0},
                ]):
            changed = _manifest()
            changed["public_schema"]["target"]["levels"] = levels
            with self.subTest(levels=levels), self.assertRaises(ValueError):
                contract.canonical_engine_manifest(changed)

        regression = _manifest(task="regression")
        regression["public_schema"]["target"]["levels"] = [
            {"type": "number", "value": 0.0},
            {"type": "number", "value": 1.0},
        ]
        with self.assertRaisesRegex(ValueError, "must be null"):
            contract.canonical_engine_manifest(regression)

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


class InvocationIdentityTests(unittest.TestCase):
    def test_invocation_identity_binds_every_public_field(self):
        manifest = _manifest()
        invocation_id = contract.invocation_identity(manifest)
        self.assertRegex(invocation_id, r"^inv1_[0-9a-f]{64}$")

        changed = copy.deepcopy(manifest)
        changed["engine_params"]["max_depth"]["value"] = 9
        self.assertNotEqual(
            invocation_id,
            contract.invocation_identity(changed),
        )

        changed = copy.deepcopy(manifest)
        changed["resources"]["threads"] = 2
        self.assertNotEqual(
            invocation_id,
            contract.invocation_identity(changed),
        )


class ResultTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest()
        self.invocation_id = contract.invocation_identity(self.manifest)
        self.artifact = b'{"learner":{"attributes":{}}}'
        self.metadata = contract.artifact_sanitization_metadata(
            self.manifest, "model"
        )
        self.result = {
            "contract_version": 1,
            "status": "ok",
            "invocation_id": self.invocation_id,
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
            artifact_bytes=self.artifact,
        )
        self.assertEqual(validated, self.result)
        self.assertNotIn("semantic_query_id", validated)
        self.assertNotIn("semantic_query_id", validated["sanitization"])

        for field, value in (("size_bytes", 1), ("sha256", "0" * 64)):
            result = copy.deepcopy(self.result)
            result["artifact"][field] = value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    contract.validate_backend_result(
                        result,
                        self.manifest,
                        artifact_bytes=self.artifact,
                    )

    def test_result_rejects_logs_paths_extra_fields_and_bad_attestation(self):
        for location, name, value in (
            ((), "logs", "raw row: secret"),
            ((), "semantic_query_id", "sq1n_" + "0" * 64),
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
                        artifact_bytes=self.artifact,
                    )

        result = copy.deepcopy(self.result)
        result["sanitization"]["contains_raw_records"] = True
        with self.assertRaisesRegex(ValueError, "sanitization"):
            contract.validate_backend_result(
                result,
                self.manifest,
                artifact_bytes=self.artifact,
            )

    def test_result_rejects_wrong_ids_format_encoding_and_oversize(self):
        result = copy.deepcopy(self.result)
        result["invocation_id"] = "inv1_" + "0" * 64
        with self.assertRaisesRegex(ValueError, "invocation"):
            contract.validate_backend_result(
                result,
                self.manifest,
                artifact_bytes=self.artifact,
            )

        result = copy.deepcopy(self.result)
        result["artifact"]["format"] = "pickle"
        with self.assertRaisesRegex(ValueError, "format"):
            contract.validate_backend_result(
                result,
                self.manifest,
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
                artifact_bytes=None,
            ),
            result,
        )
        result["message"] = "private backend detail"
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            contract.validate_backend_result(
                result,
                self.manifest,
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
                artifact_bytes=None,
            )

    def test_only_direct_dp_model_artifacts_are_supported(self):
        metadata = contract.artifact_sanitization_metadata(
            self.manifest, "model"
        )
        self.assertEqual(metadata["privacy_basis"], "direct-dp-training")
        self.assertFalse(metadata["contains_raw_records"])
        self.assertFalse(metadata["contains_unnoised_statistics"])
        self.assertFalse(metadata["contains_backend_logs"])
        self.assertFalse(metadata["contains_target_name"])
        with self.assertRaisesRegex(ValueError, "model artifacts"):
            contract.artifact_sanitization_metadata(
                self.manifest, "statistics"
            )
        with self.assertRaisesRegex(ValueError, "artifact kind"):
            contract.expected_artifact_format(self.manifest, "statistics")


if __name__ == "__main__":
    unittest.main()
