"""Internal, data-only ABI contract for trusted native tree backends.

This module does not expose or invoke a backend.  It defines the boundary that a
future node-owned adapter must satisfy before it can touch private data:

* the public manifest is typed, finite, canonical and bounded;
* backend/process controls, paths, callbacks and client-provided seeds are not
  parameters;
* resource limits are explicit and may only reject work, never silently alter a
  privacy mechanism;
* a keyed semantic identity makes memoization stable without disclosing the
  node's persistent identity root (which is separate from its noise root); and
* the result channel contains only fixed metadata and a digest of a separately
  transported, non-executable model/synopsis artifact.  It has no free-form log,
  path or exception field.

This is the server-enriched backend ABI, not the analyst-facing R request ABI.
"Public" below means declared non-private schema and engine parameters; the
privacy contract and opaque data-scope digests are injected and pinned by the
server before this validator is called.

An attestation is not an artifact sanitizer by itself.  Only a trusted,
engine-specific adapter may create it, after parsing and rewriting its native
artifact with the corresponding allowlist.  Untrusted code must never call the
metadata constructor as a way to bless bytes.
"""

import hashlib
import hmac
import json
import math
import re


CONTRACT_VERSION = 1
MANIFEST_MAX_BYTES = 64 * 1024
MAX_PUBLIC_PARAMETERS = 128
MAX_MECHANISM_PARAMETERS = 64
MAX_PARAMETER_NAME_BYTES = 64
MAX_STRING_BYTES = 512
MAX_LIST_ITEMS = 256
MAX_TOTAL_PUBLIC_CUTS = 16_384
MAX_INTEGER = (1 << 53) - 1
MAX_FLOAT_ABS = 1.0e12

# These are hard protocol ceilings, not suggested defaults.  A server may pin
# lower limits.  A backend must reject a request that crosses either set; it must
# never truncate rows/trees/bins because doing so would change semantic identity.
RESOURCE_HARD_CAPS = {
    "threads": 64,
    "memory_mib": 65536,
    "wall_seconds": 21600,
    "max_rows": 10_000_000,
    "max_features": 8192,
    "max_trees": 10_000,
    "max_depth": 32,
    "max_bins": 65_536,
    "max_artifact_bytes": 512 * 1024 * 1024,
}

_RESOURCE_MINIMUMS = {
    "threads": 1,
    "memory_mib": 128,
    "wall_seconds": 1,
    "max_rows": 1,
    "max_features": 1,
    "max_trees": 1,
    "max_depth": 1,
    "max_bins": 2,
    "max_artifact_bytes": 1,
}

_MODES = frozenset(("native-tight", "synopsis-flex"))
_ENGINES = frozenset((
    "xgboost", "lightgbm", "catboost", "random_forest", "extra_trees",
))
_FOREST_ENGINES = frozenset(("random_forest", "extra_trees"))
_TASKS = frozenset((
    "binary_classification",
    "regression",
))

_PARAMETER_TYPES = frozenset((
    "bool", "int", "float", "string",
    "bool_list", "int_list", "float_list", "string_list",
))
_PARAMETER_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_INVOCATION_ID_RE = re.compile(r"^inv1_[0-9a-f]{64}$")
_QUERY_ID_RE = re.compile(r"^sq1[ns]_[0-9a-f]{64}$")

# These controls are node-owned.  Passing them through an engine's generic
# parameter interface could bypass a DP updater, load data/models, execute a
# callback/plugin, choose a seed, open a network listener or emit private logs.
_RESERVED_PARAMETER_NAMES = frozenset((
    "adjacency",
    "allow_writing_files",
    "callbacks",
    "config",
    "custom_objective",
    "data",
    "device",
    "devices",
    "forcedsplits_filename",
    "gpu_device_id",
    "input_model",
    "logging_level",
    "machine_list_file",
    "machines",
    "max_rows_per_unit",
    "model_file",
    "n_jobs",
    "nthread",
    "num_threads",
    "output_model",
    "patient_column",
    "snapshot_file",
    "task_type",
    "thread_count",
    "train_dir",
    "unit",
    "unit_canonicalization",
    "updater",
    "verbose",
    "verbosity",
))
_TIGHT_RESERVED_PARAMETER_NAMES = frozenset((
    "custom_eval", "custom_metric", "early_stop", "early_stop_round",
    "early_stop_rounds", "early_stopping", "early_stopping_round",
    "early_stopping_rounds", "eval_fn", "eval_metric", "eval_set", "evals",
    "feval", "fobj", "loss_function", "metric", "metrics", "objective",
    "objective_fn", "random_seed", "random_state", "seed", "tree_method",
    "use_best_model",
))
_RESERVED_PARAMETER_PARTS = frozenset((
    "callback", "code", "host", "log", "machine", "network", "plugin",
    "port", "script", "socket",
))
_RESERVED_PARAMETER_SUFFIXES = ("_dir", "_file", "_filename", "_path")

_SUCCESS_FIELDS = frozenset((
    "contract_version", "status", "invocation_id", "semantic_query_id",
    "engine", "mode", "artifact", "sanitization",
))
_ERROR_FIELDS = frozenset((
    "contract_version", "status", "invocation_id", "error_code",
))
_ERROR_CODES = frozenset((
    "invalid_input", "resource_exhausted", "unsupported", "internal_error",
))
_ARTIFACT_FIELDS = frozenset((
    "kind", "format", "size_bytes", "sha256",
))
_SANITIZATION_FIELDS = frozenset((
    "profile",
    "privacy_basis",
    "semantic_query_id",
    "contains_raw_records",
    "contains_unnoised_statistics",
    "contains_feature_names",
    "contains_target_name",
    "contains_training_history",
    "contains_backend_logs",
    "contains_paths",
    "contains_executable_payload",
))

_MODEL_FORMATS = {
    "xgboost": "xgboost-json-v1",
    "lightgbm": "lightgbm-text-v1",
    "catboost": "catboost-cbm-v1",
    "random_forest": "dsflower-forest-json-v1",
    "extra_trees": "dsflower-forest-json-v1",
}
_SYNOPSIS_FORMAT = "dsflower-synopsis-json-v1"
_RESOURCE_PARAMETER_ALIASES = {
    "max_trees": frozenset((
        "iterations", "n_estimators", "num_boost_round", "num_iterations",
        "num_trees",
    )),
    "max_depth": frozenset(("depth", "max_depth")),
    "max_bins": frozenset(("border_count", "max_bin", "max_bins")),
    "max_features": frozenset(("max_features",)),
}


def _mapping(value, where):
    if not isinstance(value, dict):
        raise ValueError("%s must be an object" % where)
    return value


def _exact_fields(value, expected, where):
    actual = frozenset(value)
    if actual - expected:
        raise ValueError("%s has unsupported fields" % where)
    if expected - actual:
        raise ValueError("%s is missing required fields" % where)


def _canonical_json(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _bounded_float(value, where, *, lower, upper, lower_open=False,
                   upper_open=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s must be a finite number" % where)
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("%s must be a finite number" % where)
    lower_ok = result > lower if lower_open else result >= lower
    upper_ok = result < upper if upper_open else result <= upper
    if not lower_ok or not upper_ok:
        raise ValueError("%s is outside the supported range" % where)
    return result


def _safe_string(value, where):
    if not isinstance(value, str):
        raise ValueError("%s has the wrong declared type" % where)
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("%s must contain printable ASCII" % where) from exc
    if len(encoded) > MAX_STRING_BYTES or any(byte < 0x20 or byte > 0x7e
                                               for byte in encoded):
        raise ValueError("%s must contain bounded printable ASCII" % where)
    return value


def _typed_scalar(kind, value, where):
    if kind == "bool":
        if not isinstance(value, bool):
            raise ValueError("%s has the wrong declared type" % where)
        return value
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("%s has the wrong declared type" % where)
        if value < -MAX_INTEGER or value > MAX_INTEGER:
            raise ValueError("%s is outside the supported range" % where)
        return int(value)
    if kind == "float":
        if not isinstance(value, float) or not math.isfinite(value):
            raise ValueError("%s has the wrong declared type" % where)
        if abs(value) > MAX_FLOAT_ABS:
            raise ValueError("%s is outside the supported range" % where)
        return float(value)
    if kind == "string":
        return _safe_string(value, where)
    raise ValueError("parameter has an unsupported declared type")


def _reserved_parameter(name, mode):
    if name in _RESERVED_PARAMETER_NAMES or name.endswith(
            _RESERVED_PARAMETER_SUFFIXES):
        return True
    if name.startswith("custom_objective_"):
        return True
    if name.startswith(("contribution_", "dp_", "privacy_", "unit_")):
        return True
    if mode == "native-tight" and name in _TIGHT_RESERVED_PARAMETER_NAMES:
        return True
    return bool(_RESERVED_PARAMETER_PARTS.intersection(name.split("_")))


def _typed_parameters(value, where, max_count, mode):
    params = _mapping(value, where)
    if len(params) > max_count:
        raise ValueError("%s exceeds the parameter-count cap" % where)
    if any(not isinstance(name, str) or not _PARAMETER_NAME_RE.fullmatch(name)
           for name in params):
        raise ValueError("parameter name is invalid")
    canonical = {}
    for name in sorted(params):
        if len(name.encode("ascii")) > MAX_PARAMETER_NAME_BYTES:
            raise ValueError("parameter name is invalid")
        if _reserved_parameter(name, mode):
            raise ValueError("parameter name is reserved for the trusted backend")
        item = _mapping(params[name], "typed parameter")
        _exact_fields(item, frozenset(("type", "value")), "typed parameter")
        kind = item["type"]
        if not isinstance(kind, str) or kind not in _PARAMETER_TYPES:
            raise ValueError("parameter has an unsupported declared type")
        raw_value = item["value"]
        if kind.endswith("_list"):
            if not isinstance(raw_value, list) or len(raw_value) > MAX_LIST_ITEMS:
                raise ValueError("typed parameter list exceeds its cap or is not a list")
            scalar_kind = kind[:-5]
            normalized = [
                _typed_scalar(scalar_kind, element, "typed parameter list item")
                for element in raw_value
            ]
        else:
            normalized = _typed_scalar(kind, raw_value, "typed parameter")
        canonical[name] = {"type": kind, "value": normalized}
    return canonical


def _canonical_privacy(value, mode, engine):
    privacy = _mapping(value, "privacy contract")
    fields = frozenset((
        "mechanism", "epsilon", "delta", "unit", "adjacency",
        "unit_canonicalization", "contribution_strategy", "max_rows_per_unit",
        "mechanism_params",
    ))
    _exact_fields(privacy, fields, "privacy contract")
    mechanism = privacy["mechanism"]
    if mode == "synopsis-flex":
        expected = "dp-synopsis-v1"
    elif engine in _FOREST_ENGINES:
        expected = "dp-forest-v1"
    else:
        expected = "dp-histogram-v1"
    if mechanism != expected:
        raise ValueError("privacy mechanism does not match mode and engine")
    if privacy["unit"] not in ("row", "patient") or \
            privacy["adjacency"] != "replace_one":
        raise ValueError("privacy unit or adjacency is unsupported")
    if privacy["unit_canonicalization"] != "trim-utf8-v2" or \
            privacy["contribution_strategy"] != "one-record-per-unit-v1":
        raise ValueError("privacy unit canonicalization or contribution strategy is unsupported")
    if type(privacy["max_rows_per_unit"]) is not int or \
            privacy["max_rows_per_unit"] != 1:
        raise ValueError("privacy contribution bound must be exactly one row per unit")
    return {
        "mechanism": mechanism,
        "epsilon": _bounded_float(
            privacy["epsilon"], "epsilon", lower=0.0, upper=100.0,
            lower_open=True,
        ),
        "delta": _bounded_float(
            privacy["delta"], "delta", lower=0.0, upper=1.0,
            upper_open=True,
        ),
        "unit": privacy["unit"],
        "adjacency": "replace_one",
        "unit_canonicalization": "trim-utf8-v2",
        "contribution_strategy": "one-record-per-unit-v1",
        "max_rows_per_unit": 1,
        "mechanism_params": _typed_parameters(
            privacy["mechanism_params"],
            "mechanism parameters",
            MAX_MECHANISM_PARAMETERS,
            "native-tight",
        ),
    }


def _public_number_list(value, where, max_items=None):
    if not isinstance(value, list) or not value or \
            (max_items is not None and len(value) > max_items):
        raise ValueError("%s must be a non-empty numeric array" % where)
    result = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError("%s must contain only finite numbers" % where)
        number = float(item)
        if not math.isfinite(number) or abs(number) > MAX_FLOAT_ABS:
            raise ValueError("%s must contain only bounded finite numbers" % where)
        result.append(0.0 if number == 0.0 else number)
    return result


def _public_feature_name(value):
    if not isinstance(value, str) or not value or "/" in value or "\\" in value:
        raise ValueError("public feature name is invalid")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("public feature name is invalid UTF-8") from exc
    if len(encoded) > 256 or any(ord(char) < 0x20 or ord(char) == 0x7f
                                 for char in value):
        raise ValueError("public feature name is invalid")
    return value


def _public_schema_json(core):
    """Canonical schema encoding shared with the public R request boundary."""
    return json.dumps(
        core,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_target(value, task, features):
    target = _mapping(value, "public target")
    fields = frozenset(("name", "kind", "lower", "upper"))
    _exact_fields(target, fields, "public target")
    name = _public_feature_name(target["name"])
    if name in features:
        raise ValueError("public target name must differ from every feature")
    kind = target["kind"]
    if kind not in ("binary", "continuous"):
        raise ValueError("public target kind is unsupported")
    lower = _bounded_float(
        target["lower"], "public target lower bound",
        lower=-MAX_FLOAT_ABS, upper=MAX_FLOAT_ABS,
    )
    upper = _bounded_float(
        target["upper"], "public target upper bound",
        lower=-MAX_FLOAT_ABS, upper=MAX_FLOAT_ABS,
    )
    lower = 0.0 if lower == 0.0 else lower
    upper = 0.0 if upper == 0.0 else upper
    if task == "binary_classification":
        if kind != "binary" or lower != 0.0 or upper != 1.0:
            raise ValueError("binary task requires target bounds [0, 1]")
    elif kind != "continuous" or lower >= upper:
        raise ValueError("regression task requires finite increasing target bounds")
    return {
        "name": name,
        "kind": kind,
        "lower": lower,
        "upper": upper,
    }


def _canonical_public_schema(value, mode, task, resources):
    schema = _mapping(value, "public schema")
    fields = frozenset((
        "version", "features", "lower", "upper", "cuts", "target", "sha256",
    ))
    _exact_fields(schema, fields, "public schema")
    if type(schema["version"]) is not int or schema["version"] != 1:
        raise ValueError("unsupported public schema version")
    raw_features = schema["features"]
    if not isinstance(raw_features, list) or not raw_features or \
            len(raw_features) > resources["max_features"]:
        raise ValueError("public feature count exceeds its resource cap")
    features = [_public_feature_name(item) for item in raw_features]
    if len(set(features)) != len(features):
        raise ValueError("public feature names must be unique")
    lower = _public_number_list(
        schema["lower"], "public lower bounds", resources["max_features"]
    )
    upper = _public_number_list(
        schema["upper"], "public upper bounds", resources["max_features"]
    )
    if len(lower) != len(features) or len(upper) != len(features) or any(
            lo >= hi for lo, hi in zip(lower, upper)):
        raise ValueError("public bounds must match features and satisfy lower < upper")

    raw_cuts = schema["cuts"]
    if raw_cuts is None:
        if mode == "native-tight":
            raise ValueError("native-tight requires complete public cuts")
        cuts = None
    else:
        if not isinstance(raw_cuts, list) or len(raw_cuts) != len(features):
            raise ValueError("public cuts must contain one array per feature")
        cuts = []
        total_cuts = 0
        for index, raw_feature_cuts in enumerate(raw_cuts):
            one = _public_number_list(
                raw_feature_cuts,
                "public feature cuts",
                resources["max_bins"] - 1,
            )
            total_cuts += len(one)
            if total_cuts > MAX_TOTAL_PUBLIC_CUTS:
                raise ValueError("public cuts exceed their total resource cap")
            if len(one) >= resources["max_bins"] or any(
                    right <= left for left, right in zip(one, one[1:])) or any(
                        cut <= lower[index] or cut >= upper[index] for cut in one):
                raise ValueError(
                    "public cuts must be increasing, bounded and within max_bins"
                )
            cuts.append(one)

    target = _canonical_target(schema["target"], task, features)
    core = {
        "version": 1,
        "features": features,
        "lower": lower,
        "upper": upper,
        "cuts": cuts,
        "target": target,
    }
    actual = hashlib.sha256(_public_schema_json(core)).hexdigest()
    advertised = schema["sha256"]
    if not isinstance(advertised, str) or not _HEX_64_RE.fullmatch(advertised) or \
            not hmac.compare_digest(advertised, actual):
        raise ValueError("public schema digest does not match canonical content")
    return dict(core, sha256=actual)


def _canonical_data_scope(value):
    scope = _mapping(value, "data scope")
    fields = frozenset((
        "snapshot_hash", "cohort_hash", "schema_hash", "privacy_epoch_hash",
    ))
    _exact_fields(scope, fields, "data scope")
    canonical = {}
    for name in sorted(fields):
        item = scope[name]
        if not isinstance(item, str) or not _HEX_64_RE.fullmatch(item.lower()):
            raise ValueError("data-scope identities must be SHA-256/HMAC digests")
        canonical[name] = item.lower()
    return canonical


def _canonical_resources(value):
    resources = _mapping(value, "resource contract")
    expected = frozenset(RESOURCE_HARD_CAPS)
    _exact_fields(resources, expected, "resource contract")
    canonical = {}
    for name in sorted(expected):
        item = resources[name]
        if isinstance(item, bool) or not isinstance(item, int):
            raise ValueError("resource limit must be an integer within its cap")
        if item < _RESOURCE_MINIMUMS[name] or item > RESOURCE_HARD_CAPS[name]:
            raise ValueError("resource limit is outside its hard cap")
        canonical[name] = int(item)
    return canonical


def _validate_parameter_resource_caps(parameters, resources):
    for resource_name, aliases in _RESOURCE_PARAMETER_ALIASES.items():
        for name in aliases.intersection(parameters):
            record = parameters[name]
            if record["type"] not in ("int", "float"):
                continue
            value = record["value"]
            if value < 1 or value > resources[resource_name]:
                raise ValueError(
                    "engine parameter exceeds its canonical resource limit"
                )


def canonical_engine_manifest(value):
    """Validate and return the canonical data-only native-engine manifest.

    Engine adapters must apply their own *narrower* per-engine parameter schema
    before invocation.  This common layer guarantees type fidelity and excludes
    generic execution/I/O controls; it does not claim every syntactically valid
    parameter is implemented by every backend.
    """
    manifest = _mapping(value, "engine manifest")
    fields = frozenset((
        "contract_version", "mode", "engine", "task", "public_schema",
        "engine_params", "privacy", "data_scope", "resources",
    ))
    _exact_fields(manifest, fields, "engine manifest")
    if type(manifest["contract_version"]) is not int or \
            manifest["contract_version"] != CONTRACT_VERSION:
        raise ValueError("unsupported native-tree contract version")
    mode = manifest["mode"]
    engine = manifest["engine"]
    task = manifest["task"]
    if not isinstance(mode, str) or mode not in _MODES:
        raise ValueError("unsupported native-tree mode")
    if not isinstance(engine, str) or engine not in _ENGINES:
        raise ValueError("unsupported native-tree engine")
    if not isinstance(task, str) or task not in _TASKS:
        raise ValueError("unsupported native-tree task")
    resources = _canonical_resources(manifest["resources"])
    public_schema = _canonical_public_schema(
        manifest["public_schema"], mode, task, resources
    )
    data_scope = _canonical_data_scope(manifest["data_scope"])
    if not hmac.compare_digest(
            public_schema["sha256"], data_scope["schema_hash"]):
        raise ValueError("public and data-scope schema identities do not match")
    engine_params = _typed_parameters(
        manifest["engine_params"],
        "engine parameters",
        MAX_PUBLIC_PARAMETERS,
        mode,
    )
    _validate_parameter_resource_caps(engine_params, resources)
    canonical = {
        "contract_version": CONTRACT_VERSION,
        "mode": mode,
        "engine": engine,
        "task": task,
        "public_schema": public_schema,
        "engine_params": engine_params,
        "privacy": _canonical_privacy(manifest["privacy"], mode, engine),
        "data_scope": data_scope,
        "resources": resources,
    }
    if len(_canonical_json(canonical)) > MANIFEST_MAX_BYTES:
        raise ValueError("canonical engine manifest exceeds its byte cap")
    return canonical


def canonical_manifest_bytes(value):
    """Return the sole JSON encoding allowed on the backend ABI."""
    return _canonical_json(canonical_engine_manifest(value))


def invocation_identity(value):
    """Unkeyed binding for one exact public invocation, including resources."""
    digest = hashlib.sha256(canonical_manifest_bytes(value)).hexdigest()
    return "inv1_" + digest


def semantic_query_identity(identity_root, value):
    """Return a domain-separated HMAC identity for sticky DP randomness.

    A native-tight identity includes every engine parameter because it can change
    the sequence/sensitivity of private accesses.  In synopsis-flex mode only
    the task, schema, dataset scope and synopsis/privacy mechanism remain bound.
    The engine and ``engine_params`` are downstream postprocessing and are
    omitted so one cached DP synopsis can serve XGBoost, LightGBM, CatBoost and
    local HPO for that task.  Resource ceilings are non-semantic in both modes:
    implementations must reject at a ceiling and never silently clip work to it.

    ``identity_root`` is a persistent node-internal HMAC key dedicated to
    identities.  It must not be the DP noise root, and the returned identifier
    must never be exposed through the analyst-facing protocol.  Epsilon/delta
    are deliberately absent because cache lookup precedes privacy reservation;
    the ledger binds a committed artifact to the active policy separately.
    """
    if not isinstance(identity_root, (bytes, bytearray, memoryview)) or \
            len(identity_root) < 32:
        raise ValueError("identity root must be at least 32 bytes")
    manifest = canonical_engine_manifest(value)
    material = {
        "identity_version": 1,
        "contract_version": manifest["contract_version"],
        "mode": manifest["mode"],
        "task": manifest["task"],
        "public_schema": manifest["public_schema"],
        # epsilon/delta describe the later accountant allocation.  Identity is
        # claimed before that reservation so cache lookup cannot depend on them;
        # the durable ledger separately binds a replay to its policy hash.
        "privacy": {
            "mechanism": manifest["privacy"]["mechanism"],
            "unit": manifest["privacy"]["unit"],
            "adjacency": manifest["privacy"]["adjacency"],
            "unit_canonicalization": manifest["privacy"]["unit_canonicalization"],
            "contribution_strategy": manifest["privacy"]["contribution_strategy"],
            "max_rows_per_unit": manifest["privacy"]["max_rows_per_unit"],
            "mechanism_params": manifest["privacy"]["mechanism_params"],
        },
        "data_scope": manifest["data_scope"],
    }
    if manifest["mode"] == "native-tight":
        material["engine"] = manifest["engine"]
        material["engine_params"] = manifest["engine_params"]
        prefix = "sq1n_"
    else:
        prefix = "sq1s_"
    digest = hmac.new(
        bytes(identity_root),
        b"dsflower/native-tree/semantic-query/v1\x00" + _canonical_json(material),
        hashlib.sha256,
    ).hexdigest()
    return prefix + digest


def _validate_query_id(value, mode):
    prefix = "sq1n_" if mode == "native-tight" else "sq1s_"
    if not isinstance(value, str) or not _QUERY_ID_RE.fullmatch(value) or \
            not value.startswith(prefix):
        raise ValueError("semantic query identity is invalid for this mode")
    return value


def artifact_sanitization_metadata(manifest, semantic_query_id, artifact_kind):
    """Create the only attestation shape accepted from a trusted sanitizer."""
    canonical = canonical_engine_manifest(manifest)
    query_id = _validate_query_id(semantic_query_id, canonical["mode"])
    if canonical["mode"] == "native-tight":
        if artifact_kind != "model":
            raise ValueError("native-tight backends may only release model artifacts")
        basis = "direct-dp-training"
    elif artifact_kind == "synopsis":
        basis = "dp-synopsis"
    elif artifact_kind == "model":
        basis = "dp-synopsis-postprocessing"
    else:
        raise ValueError("unsupported artifact kind")
    return {
        "profile": "dsflower-tree-artifact-v1",
        "privacy_basis": basis,
        "semantic_query_id": query_id,
        "contains_raw_records": False,
        "contains_unnoised_statistics": False,
        "contains_feature_names": False,
        "contains_target_name": False,
        "contains_training_history": False,
        "contains_backend_logs": False,
        "contains_paths": False,
        "contains_executable_payload": False,
    }


def expected_artifact_format(manifest, artifact_kind):
    """Return the single non-Pickle artifact encoding allowed by the ABI."""
    canonical = canonical_engine_manifest(manifest)
    if artifact_kind == "synopsis":
        if canonical["mode"] != "synopsis-flex":
            raise ValueError("native-tight backends cannot release a synopsis")
        return _SYNOPSIS_FORMAT
    if artifact_kind != "model":
        raise ValueError("unsupported artifact kind")
    return _MODEL_FORMATS[canonical["engine"]]


def _json_object_without_duplicate_keys(artifact):
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(
            artifact.decode("utf-8"),
            object_pairs_hook=object_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError,
            RecursionError) as exc:
        raise ValueError("artifact encoding is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("artifact encoding is invalid")


def _validate_artifact_encoding(artifact_format, artifact):
    if artifact_format in (
        "xgboost-json-v1", "dsflower-forest-json-v1",
        "dsflower-synopsis-json-v1",
    ):
        _json_object_without_duplicate_keys(artifact)
        return
    if artifact_format == "lightgbm-text-v1":
        try:
            text = artifact.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("artifact encoding is invalid") from exc
        if not text.startswith("tree\n") or "\x00" in text or any(
                ord(char) < 0x20 and char not in "\n\r\t" for char in text):
            raise ValueError("artifact encoding is invalid")
        return
    if artifact_format == "catboost-cbm-v1":
        if not artifact.startswith(b"CBM1"):
            raise ValueError("artifact encoding is invalid")
        return
    raise ValueError("artifact format is unsupported")


def validate_backend_result(result, manifest, *, expected_query_id,
                            artifact_bytes):
    """Validate a backend response and bind it to the separately returned bytes.

    Failure responses intentionally carry a bounded code only.  Detailed native
    diagnostics belong in a node-admin-only sink outside this protocol and must
    never be reflected to the analyst or copied into an artifact.
    """
    canonical = canonical_engine_manifest(manifest)
    response = _mapping(result, "backend result")
    status = response.get("status")
    if status == "error":
        _exact_fields(response, _ERROR_FIELDS, "backend result")
        if type(response["contract_version"]) is not int or \
                response["contract_version"] != CONTRACT_VERSION:
            raise ValueError("backend result contract version mismatch")
        expected_invocation = invocation_identity(canonical)
        if not isinstance(response["invocation_id"], str) or not \
                _INVOCATION_ID_RE.fullmatch(response["invocation_id"]) or not \
                hmac.compare_digest(response["invocation_id"], expected_invocation):
            raise ValueError("backend result invocation identity mismatch")
        if not isinstance(response["error_code"], str) or \
                response["error_code"] not in _ERROR_CODES:
            raise ValueError("backend result error code is unsupported")
        if artifact_bytes is not None:
            raise ValueError("failed backend result must not include an artifact")
        return dict(response)

    if status != "ok":
        raise ValueError("backend result status is unsupported")
    _exact_fields(response, _SUCCESS_FIELDS, "backend result")
    if type(response["contract_version"]) is not int or \
            response["contract_version"] != CONTRACT_VERSION:
        raise ValueError("backend result contract version mismatch")

    expected_invocation = invocation_identity(canonical)
    invocation_id = response["invocation_id"]
    if not isinstance(invocation_id, str) or not _INVOCATION_ID_RE.fullmatch(
            invocation_id) or not hmac.compare_digest(
                invocation_id, expected_invocation):
        raise ValueError("backend result invocation identity mismatch")

    query_id = _validate_query_id(expected_query_id, canonical["mode"])
    returned_query_id = response["semantic_query_id"]
    if not isinstance(returned_query_id, str) or not hmac.compare_digest(
            returned_query_id, query_id):
        raise ValueError("backend result semantic query identity mismatch")
    if response["engine"] != canonical["engine"] or \
            response["mode"] != canonical["mode"]:
        raise ValueError("backend result engine or mode mismatch")

    artifact_meta = _mapping(response["artifact"], "artifact metadata")
    _exact_fields(artifact_meta, _ARTIFACT_FIELDS, "artifact metadata")
    kind = artifact_meta["kind"]
    expected_format = expected_artifact_format(canonical, kind)
    if artifact_meta["format"] != expected_format:
        raise ValueError("artifact format does not match the contract")
    advertised_size = artifact_meta["size_bytes"]
    if isinstance(advertised_size, bool) or not isinstance(advertised_size, int) or \
            advertised_size < 1:
        raise ValueError("artifact size is invalid")
    size_cap = min(
        canonical["resources"]["max_artifact_bytes"],
        RESOURCE_HARD_CAPS["max_artifact_bytes"],
    )
    if advertised_size > size_cap:
        raise ValueError("artifact size exceeds its resource cap")
    if not isinstance(artifact_bytes, (bytes, bytearray, memoryview)):
        raise ValueError("successful backend result is missing artifact bytes")
    if len(artifact_bytes) != advertised_size:
        raise ValueError("artifact size does not match returned bytes")
    artifact = bytes(artifact_bytes)
    _validate_artifact_encoding(expected_format, artifact)
    advertised_hash = artifact_meta["sha256"]
    actual_hash = hashlib.sha256(artifact).hexdigest()
    if not isinstance(advertised_hash, str) or not _HEX_64_RE.fullmatch(
            advertised_hash) or not hmac.compare_digest(advertised_hash, actual_hash):
        raise ValueError("artifact digest does not match returned bytes")

    sanitization = _mapping(response["sanitization"], "sanitization metadata")
    _exact_fields(sanitization, _SANITIZATION_FIELDS, "sanitization metadata")
    expected_sanitization = artifact_sanitization_metadata(
        canonical, query_id, kind
    )
    if sanitization != expected_sanitization:
        raise ValueError("artifact sanitization attestation is invalid")
    return {
        "contract_version": CONTRACT_VERSION,
        "status": "ok",
        "invocation_id": invocation_id,
        "semantic_query_id": returned_query_id,
        "engine": canonical["engine"],
        "mode": canonical["mode"],
        "artifact": dict(artifact_meta),
        "sanitization": dict(sanitization),
    }


__all__ = [
    "CONTRACT_VERSION",
    "MANIFEST_MAX_BYTES",
    "RESOURCE_HARD_CAPS",
    "artifact_sanitization_metadata",
    "canonical_engine_manifest",
    "canonical_manifest_bytes",
    "expected_artifact_format",
    "invocation_identity",
    "semantic_query_identity",
    "validate_backend_result",
]
