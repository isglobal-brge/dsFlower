"""Strict bridge from the public R request to the trusted tree ABI."""

import base64
import hashlib
import hmac
import json
import math
import re

from . import native_tree_contract as tree_contract


REQUEST_CONTRACT = "dsflower-native-tree-request-v1"
_REQUEST_FIELDS = frozenset((
    "contract", "engine", "mode", "parameters", "public_schema",
    "resources", "task",
))
_RESOURCE_FIELDS = frozenset((
    "max_features", "max_trees", "max_depth", "max_bins", "max_threads",
    "memory_mb", "timeout_seconds",
))
_PARAMETER_FIELDS = frozenset(("name", "type", "value"))
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_TYPE_MAP = {
    "boolean": "bool",
    "integer": "int",
    "number": "float",
    "string": "string",
    "boolean_array": "bool_list",
    "integer_array": "int_list",
    "number_array": "float_list",
    "string_array": "string_list",
}


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("native-tree request has duplicate JSON keys")
        result[key] = value
    return result


def _exact(value, fields, where):
    if not isinstance(value, dict) or frozenset(value) != fields:
        raise ValueError("%s has an unsupported shape" % where)
    return value


def parse_request_wire(request_b64, request_sha256):
    """Decode one exact canonical analyst request and verify its digest."""
    if not isinstance(request_b64, str) or not request_b64:
        raise ValueError("native-tree request is not bounded canonical base64")
    try:
        encoded = request_b64.encode("ascii", errors="strict")
    except UnicodeEncodeError as exc:
        raise ValueError(
            "native-tree request is not bounded canonical base64") from exc
    if len(encoded) > 4 * math.ceil(tree_contract.MANIFEST_MAX_BYTES / 3):
        raise ValueError("native-tree request is not bounded canonical base64")
    if not isinstance(request_sha256, str) or not _HEX_64_RE.fullmatch(
            request_sha256):
        raise ValueError("native-tree request digest is invalid")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "native-tree request is not bounded canonical base64") from exc
    if not 1 <= len(raw) <= tree_contract.MANIFEST_MAX_BYTES or \
            base64.b64encode(raw).decode("ascii") != request_b64 or \
            not hmac.compare_digest(hashlib.sha256(raw).hexdigest(),
                                    request_sha256):
        raise ValueError("native-tree request wire does not match its digest")
    try:
        request = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_object_without_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")),
        )
        canonical = json.dumps(
            request, ensure_ascii=False, allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("native-tree request JSON is invalid") from exc
    if canonical != raw:
        raise ValueError("native-tree request JSON is not canonical")
    request = _exact(request, _REQUEST_FIELDS, "native-tree request")
    if request["contract"] != REQUEST_CONTRACT:
        raise ValueError("native-tree request contract is unsupported")
    return request


def _typed_parameters(records):
    if not isinstance(records, list):
        raise ValueError("native-tree parameters must be an array")
    parameters = {}
    for record in records:
        record = _exact(record, _PARAMETER_FIELDS, "native-tree parameter")
        name = record["name"]
        source_type = record["type"]
        if not isinstance(name, str) or name in parameters or \
                source_type not in _TYPE_MAP:
            raise ValueError("native-tree parameter is invalid or duplicated")
        parameters[name] = {
            "type": _TYPE_MAP[source_type],
            "value": record["value"],
        }
    return parameters


def backend_manifest(request, *, epsilon, delta, unit,
                     unit_canonicalization, gradient_clip,
                     snapshot_hash, cohort_hash):
    """Enrich a public request with trusted, non-public node state."""
    request = _exact(request, _REQUEST_FIELDS, "native-tree request")
    if request["contract"] != REQUEST_CONTRACT or \
            request["engine"] not in ("xgboost", "extra_trees") or \
            request["mode"] != "native-tight" or \
            request["task"] not in ("binary", "regression"):
        raise ValueError("native-tree request is unsupported")
    resources = _exact(
        request["resources"], _RESOURCE_FIELDS, "native-tree resources")
    parameters = _typed_parameters(request["parameters"])
    schema = request["public_schema"]
    if not isinstance(schema, dict):
        raise ValueError("native-tree public schema is invalid")
    cuts = schema.get("cuts")
    target = schema.get("target")
    if not isinstance(cuts, list) or not cuts or any(
            not isinstance(feature, list) or not feature for feature in cuts):
        raise ValueError("native-tree public cuts are incomplete")
    if not isinstance(target, dict):
        raise ValueError("native-tree public target is invalid")
    task = ("binary_classification" if request["task"] == "binary"
            else "regression")
    if task == "binary_classification":
        base_score = 0.5
    else:
        lower = target.get("lower")
        upper = target.get("upper")
        if isinstance(lower, bool) or isinstance(upper, bool) or not isinstance(
                lower, (int, float)) or not isinstance(upper, (int, float)):
            raise ValueError("native-tree target bounds are invalid")
        base_score = float(lower) + (float(upper) - float(lower)) / 2.0
    engine = request["engine"]
    if engine == "xgboost":
        parameters["base_score"] = {
            "type": "float", "value": float(base_score)}
        parameters["max_bin"] = {
            "type": "int", "value": max(len(feature) + 1 for feature in cuts),
        }
        mechanism = "dp-histogram-v1"
        mechanism_params = {
            "gradient_clip": {
                "type": "float", "value": float(gradient_clip),
            },
            "hessian_clip": {"type": "float", "value": 1.0},
        }
    else:
        # Keep request parsing and the local XGBoost predictor stdlib-only.
        # Forest accounting depends on NumPy through the shared DP harness and
        # is needed only when enriching an ExtraTrees training request.
        from . import forest_accounting
        mechanism = "dp-forest-v1"
        mechanism_params = {
            "leaf_release": {
                "type": "string",
                "value": forest_accounting.LEAF_RELEASE_PROFILE,
            },
            "topology": {
                "type": "string",
                "value": forest_accounting.TOPOLOGY_PROFILE,
            },
        }
    result = {
        "contract_version": tree_contract.CONTRACT_VERSION,
        "mode": "native-tight",
        "engine": engine,
        "task": task,
        "public_schema": schema,
        "engine_params": parameters,
        "privacy": {
            "mechanism": mechanism,
            "epsilon": epsilon,
            "delta": delta,
            "unit": unit,
            "adjacency": "replace_one",
            "unit_canonicalization": unit_canonicalization,
            "contribution_strategy": "one-record-per-unit-v1",
            "max_rows_per_unit": 1,
            "mechanism_params": mechanism_params,
        },
        "data_scope": {
            "snapshot_hash": snapshot_hash,
            "cohort_hash": cohort_hash,
            "schema_hash": schema.get("sha256"),
        },
        "resources": {
            "threads": resources["max_threads"],
            "memory_mib": resources["memory_mb"],
            "wall_seconds": resources["timeout_seconds"],
            "max_rows": tree_contract.RESOURCE_HARD_CAPS["max_rows"],
            "max_features": resources["max_features"],
            "max_trees": resources["max_trees"],
            "max_depth": resources["max_depth"],
            "max_bins": resources["max_bins"],
            "max_artifact_bytes": tree_contract.RESOURCE_HARD_CAPS[
                "max_artifact_bytes"],
        },
    }
    return tree_contract.canonical_engine_manifest(result)


def public_backend_manifest(request):
    """Public-only manifest used for coordinator-side re-sanitization."""
    schema = request.get("public_schema") if isinstance(request, dict) else None
    schema_hash = schema.get("sha256") if isinstance(schema, dict) else None
    return backend_manifest(
        request, epsilon=1.0, delta=1.0e-6, unit="row",
        unit_canonicalization="trim-utf8-v2", gradient_clip=1.0,
        snapshot_hash=schema_hash, cohort_hash=schema_hash,
    )


__all__ = [
    "REQUEST_CONTRACT",
    "backend_manifest",
    "parse_request_wire",
    "public_backend_manifest",
]
