"""Stateless, semantic deterministic randomness for private releases.

The randomness contract needs only a custodial 256-bit node key. HMAC-SHA256 binds
each master key to the effective mechanism, public configuration, privacy
policy, server round, incoming public arrays, and effective private inputs.
Operational identities (run tokens, message ids, paths and timestamps) never
enter this contract. Recomputing the same semantic identity produces the same
stream without a historical budget. Gated Hooks separately persist their first
released output so application nondeterminism cannot create a second release.

Numeric DP noise uses a ChaCha20 stream rather than NumPy's statistical PCG.
Its unpredictability is computational and depends on keeping the node key
secret.  Sub-keys domain-separate initialization, sampling, training, and noise.
"""

import hashlib
import hmac
import importlib.metadata
import json
import math
import os
import platform
import re
import stat
import sys
from functools import lru_cache


_SECRET_ENV = "DSFLOWER_NODE_SECRET_FILE"
_MASK63 = 0x7FFF_FFFF_FFFF_FFFF
_MASK31 = 0x7FFF_FFFF
_MAX_CONFIG_BYTES = 1 << 20
_MAX_CONFIG_DEPTH = 16
_MAX_CONFIG_ITEMS = 131072
_MAX_ARRAYS = 512
_MAX_ARRAY_NDIM = 16
_MAX_UNIT_ID_BYTES = 4096
_HASH_CHUNK_BYTES = 1 << 20
_POLICY_HASH = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)


def _is_windows_reparse_point(info):
    """Return whether a Windows stat result names a reparse point."""
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    reparse_tag = int(getattr(info, "st_reparse_tag", 0) or 0)
    return bool(attributes & _WINDOWS_REPARSE_POINT or reparse_tag)


def _same_file_identity(before, after, *, windows):
    """Compare stable file identities and fail closed if Windows omits them."""
    try:
        before_identity = (int(before.st_dev), int(before.st_ino))
        after_identity = (int(after.st_dev), int(after.st_ino))
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "dsFlower cannot establish a stable node-secret file identity"
        ) from exc
    if windows and (before_identity == (0, 0) or after_identity == (0, 0)):
        raise RuntimeError(
            "dsFlower cannot establish a stable node-secret file identity"
        )
    return before_identity == after_identity


def _node_secret():
    """Read and validate the dedicated node key; never fall back silently."""
    path = os.environ.get(_SECRET_ENV, "")
    if not path or not os.path.isabs(path):
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe")

    windows = os.name == "nt"
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if not windows and nofollow is None:
        raise RuntimeError("this platform cannot safely open the dsFlower node secret")
    try:
        parent = os.path.dirname(path)
        parent_info = os.lstat(parent)
        path_info = os.lstat(path)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    if not stat.S_ISDIR(parent_info.st_mode):
        raise RuntimeError("dsFlower node-secret parent must be a real directory")
    if not stat.S_ISREG(path_info.st_mode):
        raise RuntimeError("dsFlower node secret must be a regular file")
    if windows:
        if (_is_windows_reparse_point(parent_info)
                or _is_windows_reparse_point(path_info)):
            raise RuntimeError(
                "dsFlower node-secret path must not contain a reparse point"
            )
    else:
        euid = os.geteuid()
        if parent_info.st_uid not in (euid, 0):
            raise RuntimeError(
                "dsFlower node-secret parent must be owned by the node EUID or root"
            )
        if stat.S_IMODE(parent_info.st_mode) & 0o022:
            raise RuntimeError(
                "dsFlower node-secret parent must not be writable by group or other users"
            )

    if windows:
        flags = (os.O_RDONLY | getattr(os, "O_BINARY", 0)
                 | getattr(os, "O_NOINHERIT", 0))
    else:
        flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    try:
        info = os.fstat(fd)
        parent_after = os.lstat(parent)
        if (not stat.S_ISDIR(parent_after.st_mode)
                or not _same_file_identity(
                    parent_info, parent_after, windows=windows)):
            raise RuntimeError("dsFlower node-secret parent changed while opening")
        if (not stat.S_ISREG(info.st_mode)
                or not _same_file_identity(path_info, info, windows=windows)):
            raise RuntimeError("dsFlower node-secret path changed while opening")
        if windows:
            if (_is_windows_reparse_point(parent_after)
                    or _is_windows_reparse_point(info)):
                raise RuntimeError(
                    "dsFlower node-secret path must not contain a reparse point"
                )
        else:
            if parent_after.st_uid not in (euid, 0):
                raise RuntimeError(
                    "dsFlower node-secret parent must be owned by the node EUID or root"
                )
            if stat.S_IMODE(parent_after.st_mode) & 0o022:
                raise RuntimeError(
                    "dsFlower node-secret parent must not be writable by group or other users"
                )
            if info.st_uid != euid:
                raise RuntimeError("dsFlower node secret must be owned by the node EUID")
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise RuntimeError("dsFlower node secret must have mode 0600")
        with os.fdopen(fd, "rb", closefd=False) as fh:
            # 64 hex bytes plus an optional LF/CRLF. Reading one byte beyond
            # that maximum makes oversized or multiply-terminated files fail
            # without permissive whitespace stripping.
            encoded = fh.read(67)
    finally:
        os.close(fd)

    if len(encoded) == 64:
        hex_bytes = encoded
    elif len(encoded) == 65 and encoded.endswith(b"\n"):
        hex_bytes = encoded[:-1]
    elif len(encoded) == 66 and encoded.endswith(b"\r\n"):
        hex_bytes = encoded[:-2]
    else:
        raise RuntimeError(
            "dsFlower node secret must contain exactly 64 hex characters"
        )
    if any(byte not in b"0123456789abcdefABCDEF" for byte in hex_bytes):
        raise RuntimeError("dsFlower node secret is not valid hex")
    return bytes.fromhex(hex_bytes.decode("ascii"))


def select_config(config, keys):
    """Copy only caller-declared effective public fields.

    Positive selection is deliberate: transport metadata such as run tokens,
    message ids, paths and staging timestamps cannot accidentally become a
    reroll axis when new fields are added to a Flower ``ConfigRecord``.
    """
    if not isinstance(config, dict):
        raise RuntimeError("semantic configuration must be an object")
    return {key: config[key] for key in sorted(set(keys)) if key in config}


@lru_cache(maxsize=2)
def _runtime_fingerprint(probe_torch_backend=True):
    """Public execution facts that can change deterministic model arithmetic."""
    packages = {}
    for name in ("cryptography", "numpy", "opacus", "torch", "torchvision"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "absent"

    backend = {"kind": "cpu"}
    try:
        if not probe_torch_backend:
            raise ImportError("this mechanism has a fixed CPU execution profile")
        import torch
        if torch.cuda.is_available():
            backend = {
                "kind": "cuda",
                "cuda": str(getattr(torch.version, "cuda", None)),
                "cudnn": int(torch.backends.cudnn.version() or 0),
                "device": str(torch.cuda.get_device_name(0)),
                "capability": [int(value) for value in
                               torch.cuda.get_device_capability(0)],
            }
    except (ImportError, RuntimeError):
        backend = {"kind": "unavailable" if probe_torch_backend else "cpu"}

    runner = hashlib.sha256()
    runner_dir = os.path.dirname(os.path.abspath(__file__))
    paths = []
    for root, dirs, files in os.walk(runner_dir):
        dirs[:] = [name for name in dirs if name != "__pycache__"]
        for name in files:
            if not name.endswith((".pyc", ".pyo")):
                paths.append(os.path.relpath(os.path.join(root, name), runner_dir).replace(os.sep, "/"))
    for name in sorted(paths):
        with open(os.path.join(runner_dir, name), "rb") as handle:
            runner.update(name.encode("utf-8") + b"\n")
            runner.update(handle.read())
            runner.update(b"\x00")

    return {
        "python": "%d.%d.%d" % sys.version_info[:3],
        "implementation": platform.python_implementation(),
        "system": platform.system().lower(),
        "machine": platform.machine().lower(),
        "packages": packages,
        "backend": backend,
        "runner_sha256": runner.hexdigest(),
    }


def _normalize_json(value, depth, state):
    if depth > _MAX_CONFIG_DEPTH:
        raise RuntimeError("semantic configuration is nested too deeply")
    state[0] += 1
    if state[0] > _MAX_CONFIG_ITEMS:
        raise RuntimeError("semantic configuration has too many items")
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise RuntimeError("semantic configuration must be finite")
        return 0.0 if value == 0.0 else value
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        return [_normalize_json(item, depth + 1, state) for item in value]
    if isinstance(value, dict):
        out = {}
        for key in sorted(value):
            if not isinstance(key, str):
                raise RuntimeError("semantic configuration keys must be strings")
            key.encode("utf-8", errors="strict")
            out[key] = _normalize_json(value[key], depth + 1, state)
        return out
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return _normalize_json(value.item(), depth, state)
    except ImportError:
        pass
    raise RuntimeError("semantic configuration contains an unsupported value")


def _canonical_json(value):
    normalized = _normalize_json(value, 0, [0])
    encoded = json.dumps(
        normalized, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_CONFIG_BYTES:
        raise RuntimeError("semantic configuration is too large")
    return encoded


def _frame_header(hasher, label, payload_size):
    label = str(label).encode("utf-8", errors="strict")
    hasher.update(len(label).to_bytes(4, "big"))
    hasher.update(label)
    hasher.update(int(payload_size).to_bytes(8, "big"))


def _frame(hasher, label, payload):
    payload = bytes(payload)
    _frame_header(hasher, label, len(payload))
    hasher.update(payload)


def _array_metadata(array):
    return _canonical_json({
        "dtype": array.dtype.str,
        "shape": [int(dim) for dim in array.shape],
    })


def _update_array(hasher, label, value):
    import numpy as np

    array = np.asarray(value)
    if (array.dtype.hasobject or array.dtype.kind not in "biuf"
            or array.ndim > _MAX_ARRAY_NDIM):
        raise RuntimeError("semantic arrays must be bounded real numeric tensors")
    if array.dtype.kind == "f" and not bool(np.all(np.isfinite(array))):
        raise RuntimeError("semantic arrays must be finite")
    canonical_dtype = array.dtype.newbyteorder("<")
    canonical = np.ascontiguousarray(array.astype(canonical_dtype, copy=False))
    _frame(hasher, label + "/meta", _array_metadata(canonical))
    raw = memoryview(canonical.reshape(-1)).cast("B")
    _frame_header(hasher, label + "/bytes", len(raw))
    if canonical.dtype.kind != "f":
        for start in range(0, len(raw), _HASH_CHUNK_BYTES):
            hasher.update(raw[start:start + _HASH_CHUNK_BYTES])
        return

    # Normalize -0 to +0 one bounded chunk at a time.  This avoids a second
    # dataset-sized allocation while preserving numerical semantic equality.
    flat = canonical.reshape(-1)
    step = max(1, _HASH_CHUNK_BYTES // int(canonical.dtype.itemsize))
    for start in range(0, flat.size, step):
        chunk = flat[start:start + step]
        if bool(np.any(chunk == 0)):
            chunk = chunk.copy()
            chunk[chunk == 0] = 0.0
        hasher.update(memoryview(np.ascontiguousarray(chunk)).cast("B"))


def _update_arrays(hasher, namespace, arrays):
    arrays = list(arrays or ())
    if len(arrays) > _MAX_ARRAYS:
        raise RuntimeError("semantic release has too many arrays")
    _frame(hasher, namespace + "/count", len(arrays).to_bytes(4, "big"))
    for index, value in enumerate(arrays):
        _update_array(hasher, "%s/%d" % (namespace, index), value)


def _update_unit_ids(hasher, unit_ids):
    if unit_ids is None:
        _frame(hasher, "unit-ids/present", b"0")
        return
    try:
        count = len(unit_ids)
        values = iter(unit_ids)
    except TypeError as exc:
        raise RuntimeError(
            "semantic privacy-unit identifiers must be one-dimensional") from exc
    _frame(hasher, "unit-ids/present", b"1")
    _frame(hasher, "unit-ids/count", int(count).to_bytes(8, "big"))
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise RuntimeError("semantic privacy-unit identifiers must be strings")
        encoded = value.encode("utf-8", errors="strict")
        if len(encoded) > _MAX_UNIT_ID_BYTES:
            encoded = b"__dsflower_missing_patient_unit__"
        _frame(hasher, "unit-ids/%d" % index, encoded)


# ---------------------------------------------------------------------------
# Version 3: public request identity and private content binding are distinct.
# These types never contain a secret. Only release_key() crosses that boundary.
# ---------------------------------------------------------------------------
from dataclasses import dataclass

SEMANTIC_CONTRACT = "dsflower-semantic-randomness-v3"
_GEOMETRY_FIELDS = frozenset((
    "n_staged_rows", "n_privacy_units", "accounting_n_units", "sample_rate",
    "steps_per_round", "expected_batch_size", "total_steps", "noise_multiplier",
    "output_sigma", "block_count", "vector_size", "fixed_point_scale"))
_SCHEMAS = {
    "coordinate": "round fold",
    "selection": "layout features targets patient_column privacy_unit unit_canonicalization target_levels target_bounds feature_bounds target_encoding association_encoding image_roles preprocessing",
    "preprocessing": "version numeric_totalization patient_pooling row_order image_selection image_decoder image_size extractor_profile encoder_sha256 mask_vocabulary segmentation_preprocessing",
    "model": "family spec input_shape output_shape loss loss_parameters engine_contract",
    "initialisation": "mode version spec_sha256 initial_model_sha256 checkpoint_sha256 manifest_sha256 encoder_sha256 public_origin admission_policy_sha256",
    "public_arrays": "schema sha256",
    "training": "num_rounds local_epochs batch_size learning_rate optimizer scheduler",
    "strategy": "local_rule mu eta_rule",
    "privacy": "policy_version policy_sha256 epsilon delta unit unit_canonicalization patient_column adjacency clipping_norm accountant_profile sampling_profile budget_allocation hook_sample_aggregate hook_block_count_policy numeric_bounds_profile",
    "budget_allocation": "kind training_fraction evaluation_fraction folds fold_epsilon fold_delta evaluation_epsilon evaluation_delta",
    "evaluation": "kind layout_version task bins bounds survival_horizons survival_nll_bound artifact_format artifact_sha256 profile_sha256 public_schema_sha256 resampling fold_model_sha256",
    "resampling": "version method assignment privacy_unit unit_canonicalization test_numerator test_denominator folds",
    "application": "entrypoint package_sha256 app_params task num_classes initialisation_contract_sha256 egress_schema execution_profile",
    "execution_profile": "sandbox_profile egress_timeout egress_memory_mb egress_file_mb egress_processes threads",
    "image_roles": "image_column mask_column sample_id_column subject_id_column mask_empty_column asset_types",
}


def _object(name, **values):
    fields = _SCHEMAS[name].split()
    if set(values) - set(fields):
        raise ValueError("unknown " + name + " identity fields")
    return {key: values.get(key) for key in fields}


def _exact(value, fields, name):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ValueError("unknown or missing " + name + " identity fields")


def _digest_frame(label, payload):
    digest = hashlib.sha256()
    _frame(digest, label, payload)
    return digest.digest()


def _framed_bytes(label, payload):
    label = label.encode("utf-8")
    payload = bytes(payload)
    return len(label).to_bytes(4, "big") + label + len(payload).to_bytes(8, "big") + payload


@dataclass(frozen=True)
class RequestIdentity:
    canonical_json: bytes
    digest: bytes

    @property
    def sha256(self):
        return self.digest.hex()


@dataclass(frozen=True)
class DataBinding:
    canonical_json: bytes
    digest: bytes
    request_digest: bytes

    @property
    def sha256(self):
        return self.digest.hex()


def decode_json(value):
    """Reject duplicate object keys as well as NaN/Infinity in public contracts."""
    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate semantic JSON key")
            result[key] = item
        return result
    def constant(_):
        raise ValueError("nonfinite semantic JSON number")
    return json.loads(value, object_pairs_hook=pairs, parse_constant=constant)


def public_array_identity(arrays):
    import numpy as np
    arrays = list(arrays)
    digest = hashlib.sha256()
    _update_arrays(digest, "public-arrays", arrays)
    schema = [{"name": str(index), "dtype": np.asarray(a).dtype.newbyteorder("<").str,
               "shape": list(np.asarray(a).shape)} for index, a in enumerate(arrays)]
    return {"schema": schema, "sha256": digest.hexdigest()}


def _validate_request(value):
    required = {"contract", "operation", "mechanism", "runtime", *_SCHEMAS.keys()} - {
        "preprocessing", "budget_allocation", "resampling", "execution_profile", "image_roles"}
    _exact(value, required, "request")
    if value["contract"] != SEMANTIC_CONTRACT:
        raise ValueError("unsupported semantic randomness contract")
    if value["operation"] not in ("train", "validate", "holdout-evaluate", "cv-train", "cv-oof-release", "association"):
        raise ValueError("invalid semantic operation")
    for name in ("coordinate", "selection", "model", "initialisation", "public_arrays", "strategy", "privacy"):
        _exact(value[name], _SCHEMAS[name].split(), name)
    for name in ("training", "evaluation", "application"):
        if value[name] is not None:
            _exact(value[name], _SCHEMAS[name].split(), name)
    for parent, name in (("selection", "preprocessing"), ("selection", "image_roles"),
                         ("privacy", "budget_allocation"), ("evaluation", "resampling"),
                         ("application", "execution_profile")):
        obj = value[parent]
        if obj is not None and obj[name] is not None:
            _exact(obj[name], _SCHEMAS[name].split(), name)
    coordinate = value["coordinate"]
    if type(coordinate["round"]) is not int or coordinate["round"] < 1:
        raise ValueError("semantic round must be a positive integer")
    if coordinate["fold"] is not None and (type(coordinate["fold"]) is not int or coordinate["fold"] < 1):
        raise ValueError("semantic fold must be a positive integer")
    mechanism = value["mechanism"]
    _exact(mechanism, ("name", "version", "profile"), "mechanism")
    if mechanism["name"] not in ("neural-dpsgd", "hook-output-perturbation", "hook-sample-aggregate",
                                 "native-extra-trees", "native-random-forest", "native-lightgbm", "native-catboost",
                                 "native-xgboost", "private-validation-vector", "association-vector") or mechanism["version"] != "v3":
        raise ValueError("invalid v3 mechanism")
    _exact(mechanism["profile"], ("id", "parameters"), "mechanism profile")
    runtime = value["runtime"]
    _exact(runtime, ("python", "implementation", "system", "machine", "packages", "backend",
                     "runner_sha256", "engine_profile", "native_bundle_sha256"), "runtime")
    _exact(runtime["packages"], ("cryptography", "numpy", "opacus", "torch", "torchvision"), "runtime packages")
    selection = value["selection"]
    for target in selection["targets"]:
        _exact(target, ("role", "column"), "target role")
    _exact(selection["target_encoding"], ("kind", "preencoded", "invalid_rule"), "target encoding")
    if type(selection["target_encoding"]["preencoded"]) is not bool:
        raise ValueError("preencoded must be boolean")
    if selection["association_encoding"] is not None:
        _exact(selection["association_encoding"], ("preencoded", "levels", "unknown_code"), "association encoding")
    if selection["feature_bounds"] is not None:
        _exact(selection["feature_bounds"], ("lower", "upper"), "feature bounds")
    for array in value["public_arrays"]["schema"]:
        _exact(array, ("name", "dtype", "shape"), "array schema")
        if any(type(dim) is not int or dim < 0 for dim in array["shape"]):
            raise ValueError("invalid array dimensions")
    init = value["initialisation"]
    if value["public_arrays"]["schema"] and init["initial_model_sha256"] is None:
        raise ValueError("initial model content identity is required")
    training = value["training"]
    if training is not None:
        optimizer = training["optimizer"]
        active = {"sgd": "momentum nesterov", "adam": "beta1 beta2 eps amsgrad",
                  "adamw": "beta1 beta2 eps amsgrad", "rmsprop": "momentum eps alpha"}
        if optimizer["name"] not in active:
            raise ValueError("invalid optimizer identity")
        _exact(optimizer, ["name", "weight_decay", "l1_penalty"] + active[optimizer["name"]].split(), "optimizer")
        scheduler = training["scheduler"]
        active = {"none": "", "step": "step_size gamma", "exponential": "gamma", "cosine": "min_lr"}
        if scheduler["name"] not in active:
            raise ValueError("invalid scheduler identity")
        _exact(scheduler, ["name"] + active[scheduler["name"]].split(), "scheduler")
    strategy = value["strategy"]
    if type(strategy["mu"]) not in (int, float) or not 0 <= strategy["mu"] <= 1:
        raise ValueError("invalid local strategy mu")
    if strategy["mu"] == 0:
        if strategy != {"local_rule": "none", "mu": 0.0, "eta_rule": None}:
            raise ValueError("noncanonical zero local strategy")
    elif strategy["local_rule"] not in ("fedprox-step", "fedprox-gated-update"):
        raise ValueError("invalid local strategy")
    # Encoding also enforces finite scalars and bounded public JSON.
    _canonical_json(value)


def _identity_number(value, name, *, integer=False, nullable=False, minimum=None):
    if value is None and nullable:
        return
    if (type(value) not in ((int,) if integer else (int, float))
            or not math.isfinite(value) or minimum is not None and value < minimum):
        raise ValueError("invalid " + name + " identity number")


def _identity_string(value, name, *, nullable=False):
    if value is None and nullable:
        return
    if not isinstance(value, str) or not value:
        raise ValueError("invalid " + name + " identity string")


def _identity_shape(value, name, *, nullable=False, allow_zero=False):
    if value is None and nullable:
        return
    if not isinstance(value, list) or any(type(v) is not int or v < (0 if allow_zero else 1) for v in value):
        raise ValueError("invalid " + name + " identity shape")


def _identity_array_schema(schema):
    import numpy as np
    if not isinstance(schema, list):
        raise ValueError("array identity schema must be an ordered list")
    for index, item in enumerate(schema):
        _exact(item, ("name", "dtype", "shape"), "array schema")
        if item["name"] != str(index) or not isinstance(item["dtype"], str):
            raise ValueError("array identity schema must have canonical names and dtypes")
        try:
            dtype = np.dtype(item["dtype"])
        except TypeError as exc:
            raise ValueError("invalid array identity dtype") from exc
        if dtype.kind not in "biuf" or dtype.str != dtype.newbyteorder("<").str or dtype.str != item["dtype"]:
            raise ValueError("invalid array identity dtype")
        _identity_shape(item["shape"], "array", allow_zero=True)


def _canonical_levels(levels):
    """The existing R type aliases denote the same ordered public scalar list."""
    if levels is None:
        return None
    aliases = {"character": "string", "logical": "boolean", "numeric": "number"}
    if isinstance(levels, dict):
        _exact(levels, ("type", "values"), "level descriptor")
        if not isinstance(levels["values"], list):
            raise ValueError("public levels must be an ordered list")
        kind = aliases.get(levels["type"], levels["type"])
        levels = [{"type": kind, "value": item} for item in levels["values"]]
    if not isinstance(levels, list):
        raise ValueError("public levels must be an ordered list")
    result = []
    for item in levels:
        if not isinstance(item, dict):
            item = {"type": "boolean" if type(item) is bool else ("number" if type(item) in (int, float) else "string"), "value": item}
        _exact(item, ("type", "value"), "target level")
        kind, raw = aliases.get(item["type"], item["type"]), item["value"]
        if kind == "number":
            _identity_number(raw, "target level")
            raw = float(raw)
        elif kind == "boolean":
            if type(raw) is not bool:
                raise ValueError("boolean target level has the wrong type")
        elif kind == "string":
            if not isinstance(raw, str):
                raise ValueError("string target level has the wrong type")
        else:
            raise ValueError("invalid target level type")
        result.append({"type": kind, "value": raw})
    return result


def _validate_canonical_model_spec(spec):
    """Check the emitted AST ABI without reapplying raw symbolic/literal caps.

    The producer has already validated the submitted spec. Resolved @in/@out
    dimensions may legitimately exceed the smaller cap on raw literal widths.
    """
    from . import model_spec
    if not isinstance(spec, dict) or spec.get("kind") not in ("sequential", "graph"):
        raise ValueError("invalid canonical model AST")
    graph = spec["kind"] == "graph"
    _exact(spec, ("kind", "nodes", "output") if graph else ("kind", "layers"), "canonical model AST")
    entries = spec["nodes" if graph else "layers"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= model_spec._MAX_LAYERS:
        raise ValueError("invalid canonical model AST size")
    names, parents = {"@in"}, {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError("invalid canonical model layer")
        op = entry.get("op")
        if not isinstance(op, str) or op not in model_spec._OPS and (not graph or op not in model_spec._GRAPH_OPS):
            raise ValueError("invalid canonical model operation")
        fields = set(model_spec._OP_FIELDS.get(op, ()))
        if op == "upsample":
            fields.discard("mode")  # nearest is the sole admitted implementation
        _exact(entry, {"op"} | fields | ({"name", "in"} if graph else set()), "canonical model layer")
        for field in fields:
            item = entry[field]
            if field == "bias":
                if type(item) is not bool:
                    raise ValueError("canonical bias flag must be boolean")
            elif field in ("shape", "output_size", "dims"):
                _identity_shape(item, "canonical " + field, allow_zero=field == "dims")
            else:
                real = field in ("negative_slope", "p", "scale", "shift")
                _identity_number(item, "canonical " + field, integer=not real)
        if graph:
            name, inputs = entry["name"], entry["in"]
            if name != "n%d" % index or not isinstance(inputs, list) or not inputs or any(
                    not isinstance(parent, str) or parent not in names for parent in inputs):
                raise ValueError("canonical graph labels/edges are invalid")
            names.add(name)
            parents[name] = inputs
    if graph:
        if spec["output"] != "n%d" % (len(entries) - 1):
            raise ValueError("canonical graph output must be final")
        reachable = set()
        def visit(name):
            if name == "@in" or name in reachable:
                return
            reachable.add(name)
            for parent in parents[name]:
                visit(parent)
        visit(spec["output"])
        if len(reachable) != len(entries):
            raise ValueError("canonical graph contains unused nodes")


def _validate_nested_slots(value):
    """Every nested object is closed; only admitted Hook app_params is free JSON."""
    mechanism = value["mechanism"]
    _identity_string(mechanism["profile"]["id"], "mechanism profile")
    profile = mechanism["profile"]["parameters"]
    name = mechanism["name"]
    if name == "neural-dpsgd":
        for item in profile.values():
            _identity_string(item, "neural mechanism profile")
    elif name.startswith("hook-"):
        for field in ("gate_profile", "block_assignment_profile"):
            _identity_string(profile[field], "Hook mechanism profile")
    elif name == "private-validation-vector":
        from .validation import _effective_validation_layout
        if _effective_validation_layout(profile) != profile:
            raise ValueError("validation identity must use its effective public layout")
    elif name == "association-vector":
        from .epi_association import association_layout
        if not any(profile == association_layout(unit) for unit in ("row", "patient")):
            raise ValueError("invalid association mechanism profile")
    elif name.startswith("native-"):
        from . import native_tree_contract
        engine = value["model"]["engine_contract"]
        if engine["engine"].replace("_", "-") != name[len("native-"):]:
            raise ValueError("native engine and mechanism disagree")
        schema = engine["public_schema"]
        native_tree_contract._canonical_public_schema(schema, engine["task"],
            native_tree_contract.RESOURCE_HARD_CAPS)
        for field in ("contract_version", "mode", "task"):
            if isinstance(engine[field], (dict, list)):
                raise ValueError("invalid native engine scalar")
    runtime = value["runtime"]
    for name in ("python", "implementation", "system", "machine"):
        _identity_string(runtime[name], "runtime " + name)
    for package in runtime["packages"].values():
        _identity_string(package, "runtime package")
    backend = runtime["backend"]
    if not isinstance(backend, dict) or backend.get("kind") not in ("cpu", "cuda", "unavailable"):
        raise ValueError("invalid runtime backend")
    fields = ("kind", "cuda", "cudnn", "device", "capability") if backend["kind"] == "cuda" else ("kind",)
    _exact(backend, fields, "runtime backend")
    if backend["kind"] == "cuda":
        for field in ("cuda", "device"):
            _identity_string(backend[field], "runtime " + field)
        _identity_number(backend["cudnn"], "runtime cudnn", integer=True, minimum=0)
        _identity_shape(backend["capability"], "CUDA capability", allow_zero=True)
        if len(backend["capability"]) != 2:
            raise ValueError("invalid CUDA capability")
    engine = runtime["engine_profile"]
    if isinstance(engine, dict):
        _exact(engine, ("contract", "native_bundle_sha256"), "native runtime profile")
        _identity_string(engine["contract"], "native runtime contract")
        if engine["native_bundle_sha256"] != runtime["native_bundle_sha256"]:
            raise ValueError("native runtime bundle identities disagree")
    elif engine is not None:
        _identity_string(engine, "runtime engine profile")
    if not isinstance(engine, dict) and runtime["native_bundle_sha256"] is not None:
        raise ValueError("native bundle digest requires its native runtime profile")
    for field in ("runner_sha256", "native_bundle_sha256"):
        digest = runtime[field]
        if digest is not None and (not isinstance(digest, str) or _POLICY_HASH.fullmatch(digest) is None):
            raise ValueError("invalid runtime digest")
    if runtime["runner_sha256"] is None:
        raise ValueError("runtime runner digest is required")
    selection = value["selection"]
    if selection["layout"] not in ("tabular", "structured", "sequence", "image", "survival", "segmentation", "association"):
        raise ValueError("invalid source selection layout")
    if not isinstance(selection["features"], list):
        raise ValueError("selected features must be an ordered list")
    for column in selection["features"]:
        _identity_string(column, "feature column")
    if not isinstance(selection["targets"], list):
        raise ValueError("selected targets must be an ordered list")
    for role in selection["targets"]:
        if role["role"] not in ("target", "time", "event", "outcome", "exposure", "mask"):
            raise ValueError("invalid target role")
        _identity_string(role["column"], "target column")
    for field in ("kind", "invalid_rule"):
        _identity_string(selection["target_encoding"][field], "target encoding")
    _identity_string(selection["unit_canonicalization"], "unit canonicalization")
    _identity_string(selection["patient_column"], "patient column", nullable=True)
    if selection["privacy_unit"] not in ("row", "patient"):
        raise ValueError("invalid source privacy unit")
    if _canonical_levels(selection["target_levels"]) != selection["target_levels"]:
        raise ValueError("target levels must use canonical typed scalars")
    for bounds in (selection["target_bounds"], *(selection["feature_bounds"].values() if selection["feature_bounds"] is not None else ())):
        if bounds is not None:
            if not isinstance(bounds, list):
                raise ValueError("numeric bounds must be an ordered list")
            for number in bounds:
                _identity_number(number, "numeric bound")
    encoding = selection["association_encoding"]
    if encoding is not None:
        if type(encoding["preencoded"]) is not bool or type(encoding["unknown_code"]) is not int or encoding["unknown_code"] != 2:
            raise ValueError("invalid association encoding")
        _exact(encoding["levels"], ("outcome", "exposure"), "association levels")
        for levels in encoding["levels"].values():
            if _canonical_levels(levels) != levels:
                raise ValueError("association levels must use canonical typed scalars")
    prep = selection["preprocessing"]
    for field in set(prep) - {"image_size", "mask_vocabulary"}:
        _identity_string(prep[field], "preprocessing " + field, nullable=True)
    if prep["image_size"] is not None:
        _identity_number(prep["image_size"], "image size", integer=True, minimum=1)
    if prep["mask_vocabulary"] is not None:
        if not isinstance(prep["mask_vocabulary"], list):
            raise ValueError("mask vocabulary must be an ordered list")
        for label in prep["mask_vocabulary"]:
            _identity_number(label, "mask vocabulary", integer=True, minimum=0)
    roles = selection["image_roles"]
    if roles is not None:
        for field in set(roles) - {"asset_types"}:
            _identity_string(roles[field], "image role", nullable=True)
        for asset in roles["asset_types"].values():
            if asset is not None:
                for scalar in asset.values():
                    _identity_string(scalar, "asset type", nullable=True)
    model = value["model"]
    _identity_string(model["loss"], "model loss", nullable=True)
    for field, parameter in model["loss_parameters"].items():
        if field not in ("survival", "segmentation"):
            _identity_number(parameter, "loss parameter")
        elif field == "survival":
            for key, scalar in parameter.items():
                if key == "edges":
                    if not isinstance(scalar, list):
                        raise ValueError("survival edges must be an ordered list")
                    for edge in scalar:
                        _identity_number(edge, "survival edge")
                elif key in ("time_unit", "time_origin", "distribution"):
                    _identity_string(scalar, "survival " + key)
                else:
                    _identity_number(scalar, "survival " + key)
        else:
            for key in ("alpha", "smooth"):
                _identity_number(parameter[key], "segmentation " + key)
            for key in ("selection", "preprocessing"):
                _identity_string(parameter[key], "segmentation " + key)
            _identity_shape(parameter["output_shape"], "segmentation output")
            _identity_shape(parameter["mask_vocabulary"], "mask vocabulary", allow_zero=True)
    if model["family"] != value["mechanism"]["name"]:
        raise ValueError("model family and mechanism disagree")
    for field in ("input_shape", "output_shape"):
        _identity_shape(model[field], "model " + field, nullable=True)
    if model["spec"] is not None:
        if not model["input_shape"] or not model["output_shape"]:
            raise ValueError("model spec requires public dimensions")
        _validate_canonical_model_spec(model["spec"])
    if not model["family"].startswith("native-") and model["engine_contract"] is not None:
        raise ValueError("non-native model has native engine fields")
    _identity_array_schema(value["public_arrays"]["schema"])
    training = value["training"]
    if training is not None:
        for field in ("num_rounds", "local_epochs", "batch_size"):
            _identity_number(training[field], "training " + field, integer=True, minimum=1)
        _identity_number(training["learning_rate"], "training learning rate", minimum=0)
        for field, scalar in training["optimizer"].items():
            if field == "name":
                continue
            if field in ("nesterov", "amsgrad"):
                if type(scalar) is not bool:
                    raise ValueError("optimizer flag must be boolean")
            else:
                _identity_number(scalar, "optimizer " + field, minimum=0)
        for field, scalar in training["scheduler"].items():
            if field != "name":
                _identity_number(scalar, "scheduler " + field, integer=field == "step_size", minimum=0)
    application = value["application"]
    if application is not None:
        _identity_array_schema(application["egress_schema"])
        for field in ("entrypoint", "package_sha256", "task"):
            _identity_string(application[field], "application " + field)
        _identity_number(application["num_classes"], "application classes", integer=True, minimum=1)
        execution = application["execution_profile"]
        _identity_string(execution["sandbox_profile"], "sandbox profile")
        for field in set(execution) - {"sandbox_profile"}:
            _identity_number(execution[field], "Hook execution " + field, integer=True, nullable=True, minimum=1)
    evaluation = value["evaluation"]
    if evaluation is not None:
        if evaluation["kind"] != value["operation"]:
            raise ValueError("evaluation kind and operation disagree")
        for field in ("bins",):
            _identity_number(evaluation[field], "evaluation " + field, nullable=True, integer=True, minimum=1)
        for field in ("bounds", "survival_horizons"):
            items = evaluation[field]
            if items is not None:
                if not isinstance(items, list):
                    raise ValueError("evaluation numeric profile must be an ordered list")
                for item in items:
                    _identity_number(item, "evaluation " + field)
        _identity_number(evaluation["survival_nll_bound"], "evaluation survival NLL", nullable=True, minimum=0)
        for field in ("layout_version", "task", "artifact_format"):
            _identity_string(evaluation[field], "evaluation " + field, nullable=True)
    for obj in (value["initialisation"], value["privacy"], runtime,
                application or {}, evaluation or {}):
        for field, digest in obj.items():
            if field.endswith("sha256") and field != "fold_model_sha256" and digest is not None:
                if not isinstance(digest, str) or _POLICY_HASH.fullmatch(digest) is None:
                    raise ValueError("invalid semantic digest")
    # Remaining leaves outside app_params must be scalar/list values, never an
    # accidental arbitrary map hidden in a nominal scalar field.
    for obj, exempt in ((value["initialisation"], set()), (value["privacy"], {"budget_allocation"}),
                        (value["coordinate"], set()), (value["strategy"], set()),
                        (value["privacy"]["budget_allocation"], set()),
                        (application or {}, {"app_params", "egress_schema", "execution_profile"}),
                        ((evaluation or {}).get("resampling") or {}, set())):
        if any(isinstance(item, (dict, list)) for field, item in obj.items() if field not in exempt):
            raise ValueError("unexpected structured semantic scalar")


def _validate_nested_request(value):
    """Close the nested public slots; admitted app_params alone is free JSON."""
    mechanism = value["mechanism"]
    name, profile = mechanism["name"], mechanism["profile"]["parameters"]
    model = value["model"]
    if name == "neural-dpsgd":
        _exact(profile, ("accountant_profile", "sampling_profile", "numeric_bounds_profile"), "neural profile")
    elif name.startswith("hook-"):
        _exact(profile, ("gate_profile", "block_assignment_profile", "blocks"), "Hook profile")
        if type(profile["blocks"]) is not int or not 1 <= profile["blocks"] <= 64:
            raise ValueError("invalid public Hook block count")
    elif name == "private-validation-vector":
        task = profile.get("task")
        keys = {"task", "size", "sensitivity"}
        if task == "binary": keys |= {"bins"}
        elif task in ("multiclass", "ordinal"): keys |= {"bins", "classes"}
        elif task == "multilabel": keys |= {"bins", "labels"}
        elif task == "survival": keys |= {"horizons", "nll_bound"}
        elif task == "segmentation": keys |= {"max_pixels"}
        elif task not in ("regression", "count"):
            raise ValueError("invalid validation profile task")
        _exact(profile, keys, "validation profile")
    elif name == "association-vector":
        _exact(profile, ("cells", "contract", "order", "shape", "unit_semantics"), "association profile")
    elif name.startswith("native-"):
        engine = model["engine_contract"]
        _exact(engine, ("contract_version", "mode", "engine", "task", "public_schema", "engine_params"), "native engine contract")
        schemas = {
            "extra_trees": ("max_depth n_estimators", "leaf_release topology"),
            "random_forest": ("max_depth max_features n_estimators", "candidate_schedule histogram_release leaf_release partition transcript"),
            "lightgbm": ("base_score lambda_l1 lambda_l2 learning_rate max_bin max_delta_step max_depth min_data_in_leaf min_gain_to_split num_leaves num_iterations", "gradient_clip hessian_clip"),
            "catboost": ("base_score border_count depth iterations l2_leaf_reg learning_rate max_delta_step", "gradient_clip hessian_clip"),
            "xgboost": ("base_score learning_rate max_bin max_delta_step max_depth min_child_weight min_split_loss num_boost_round reg_alpha reg_lambda", "gradient_clip hessian_clip"),
        }
        if engine["engine"] not in schemas:
            raise ValueError("unknown native identity engine")
        params, controls = schemas[engine["engine"]]
        _exact(engine["engine_params"], params.split(), "native engine parameters")
        _exact(profile, controls.split(), "native mechanism parameters")
        for obj in (engine["engine_params"], profile):
            for pin in obj.values():
                _exact(pin, ("type", "value"), "typed native parameter")
                tag, item = pin["type"], pin["value"]
                if tag not in ("int", "float", "bool", "string") or (
                    tag == "int" and type(item) is not int or
                    tag == "float" and type(item) not in (int, float) or
                    tag == "bool" and type(item) is not bool or
                    tag == "string" and type(item) is not str):
                    raise ValueError("native parameter value disagrees with type")
        schema = engine["public_schema"]
        _exact(schema, ("version", "features", "lower", "upper", "cuts", "target", "sha256"), "native public schema")
        _exact(schema["target"], ("name", "kind", "levels", "lower", "upper"), "native target")
        for level in schema["target"]["levels"] or []:
            _exact(level, ("type", "value"), "native target level")
    loss = model["loss"]
    fields = {"negbin_nll": {"nb_dispersion"}, "gamma_nll": {"gamma_shape"},
              "huber": {"huber_delta"}, "quantile": {"quantile_level"},
              "aft_weibull_nll": {"survival"}, "aft_lognormal_nll": {"survival"},
              "discrete_hazard_nll": {"survival"}, "segmentation_bce_dice": {"segmentation"}}
    _exact(model["loss_parameters"], fields.get(loss, set()), "loss parameters")
    if "survival" in model["loss_parameters"]:
        keys = {"schema_version", "time_unit", "time_origin", "t_min", "horizon"}
        keys |= {"edges"} if loss == "discrete_hazard_nll" else {"time_scale", "distribution", "dispersion"}
        _exact(model["loss_parameters"]["survival"], keys, "survival loss")
    if "segmentation" in model["loss_parameters"]:
        _exact(model["loss_parameters"]["segmentation"], ("alpha", "smooth", "selection", "preprocessing", "output_shape", "mask_vocabulary"), "segmentation loss")
    roles = value["selection"]["image_roles"]
    if roles is not None:
        _exact(roles["asset_types"], ("image", "mask"), "asset roles")
        for asset in roles["asset_types"].values():
            if asset is not None:
                _exact(asset, ("type", "kind"), "asset type")
    for obj in (value["initialisation"], value["public_arrays"]):
        for key, digest in obj.items():
            if (key.endswith("sha256") and digest is not None
                    and (not isinstance(digest, str) or _POLICY_HASH.fullmatch(digest) is None)):
                raise ValueError("invalid public identity digest")
    evaluation = value["evaluation"]
    if evaluation is not None and evaluation["fold_model_sha256"] is not None:
        for index, item in enumerate(evaluation["fold_model_sha256"], 1):
            _exact(item, ("fold", "model_sha256"), "fold model")
            if item["fold"] != index or _POLICY_HASH.fullmatch(item["model_sha256"]) is None:
                raise ValueError("public fold models must be complete and ordered")


def build_request_identity(*, operation, mechanism, runtime, coordinate, selection,
                           model, initialisation, public_arrays, training, strategy,
                           privacy, evaluation, application):
    value = dict(contract=SEMANTIC_CONTRACT, operation=operation, mechanism=mechanism,
                 runtime=runtime, coordinate=coordinate, selection=selection, model=model,
                 initialisation=initialisation, public_arrays=public_arrays, training=training,
                 strategy=strategy, privacy=privacy, evaluation=evaluation, application=application)
    _validate_request(value)
    _validate_nested_request(value)
    _validate_nested_slots(value)
    encoded = _canonical_json(value)
    return RequestIdentity(encoded, _digest_frame("request-v3", encoded))


def bind_private_data(request, units, *, effective_tensors=(), geometry=None, subset=None):
    if not isinstance(request, RequestIdentity):
        raise TypeError("private binding requires a typed v3 public request")
    units_digest = getattr(units, "multiset_digest", None)
    if not isinstance(units_digest, str) or _POLICY_HASH.fullmatch(units_digest) is None:
        raise ValueError("private binding requires complete canonical source units")
    geometry = dict(geometry or {})
    if set(geometry) - _GEOMETRY_FIELDS:
        raise ValueError("unknown private geometry field")
    counts = {"n_staged_rows", "n_privacy_units", "accounting_n_units", "steps_per_round", "expected_batch_size", "total_steps", "block_count", "vector_size", "fixed_point_scale"}
    for field, number in geometry.items():
        _identity_number(number, "private geometry " + field, integer=field in counts, nullable=True, minimum=0)
        if number is not None and field not in counts:
            geometry[field] = float(number)
    geometry = {key: geometry.get(key) for key in sorted(_GEOMETRY_FIELDS)}
    subset = {"role": "all", "assignment_sha256": None} if subset is None else dict(subset)
    _exact(subset, ("role", "assignment_sha256"), "private subset")
    if subset["role"] not in ("all", "train", "test", "oof"):
        raise ValueError("invalid private subset role")
    assignment = subset["assignment_sha256"]
    if assignment is not None and (not isinstance(assignment, str) or _POLICY_HASH.fullmatch(assignment) is None):
        raise ValueError("invalid private assignment digest")
    digest = hashlib.sha256()
    _update_arrays(digest, "effective-tensors", effective_tensors)
    encoded = _canonical_json({"version": "dsflower-private-binding-v1", "kind": "content",
        "units_sha256": units_digest, "effective_tensors_sha256": digest.hexdigest(),
        "geometry": geometry, "subset": subset})
    return DataBinding(encoded, _digest_frame("data-binding-v1", encoded), request.digest)


def release_key(request, binding):
    if not isinstance(request, RequestIdentity) or not isinstance(binding, DataBinding):
        raise TypeError("release_key requires typed v3 request and private binding")
    if binding.request_digest != request.digest:
        raise ValueError("private binding belongs to a different public request")
    return hmac.new(_node_secret(), _framed_bytes("dsflower/semantic-prf/v3", request.digest)
                    + _framed_bytes("data-binding", binding.digest), hashlib.sha256).digest()


def public_execution_key(request, label):
    if not isinstance(request, RequestIdentity):
        raise TypeError("public execution randomness requires a typed request")
    return hmac.new(_node_secret(), _framed_bytes("dsflower/public-execution/v3", request.digest)
                    + _framed_bytes("label", str(label).encode()), hashlib.sha256).digest()


def _public_runtime(execution_fingerprint=None):
    runtime = dict(_runtime_fingerprint(execution_fingerprint is None))
    runtime["engine_profile"] = execution_fingerprint
    runtime["native_bundle_sha256"] = (execution_fingerprint.get("native_bundle_sha256")
        if isinstance(execution_fingerprint, dict) else None)
    return runtime


def _effective_training(cfg, pins):
    def get(wire, pin, default):
        return pins.get(pin, cfg.get(wire, default))
    opt = dict(get("optimizer", "optimizer", {}))
    name = str(opt.get("name", cfg.get("optimizer-name", "sgd"))).lower()
    optimizer = dict(name=name, weight_decay=float(opt.get("weight_decay", cfg.get("weight-decay", 0.0))),
                     l1_penalty=float(opt.get("l1_penalty", cfg.get("l1-penalty", 0.0))))
    defaults = {"momentum": 0.0, "nesterov": False, "beta1": 0.9, "beta2": 0.999,
                "eps": 1e-8, "amsgrad": False, "alpha": 0.99}
    active = {"sgd": ("momentum", "nesterov"), "adam": ("beta1", "beta2", "eps", "amsgrad"),
              "adamw": ("beta1", "beta2", "eps", "amsgrad"), "rmsprop": ("momentum", "eps", "alpha")}
    for key in active[name]:
        wire = "optimizer-" + ("rmsprop-alpha" if key == "alpha" else key)
        value = opt.get("rmsprop_alpha" if key == "alpha" else key, cfg.get(wire, defaults[key]))
        optimizer[key] = value if key in ("nesterov", "amsgrad") else float(value)
    sched = dict(get("scheduler", "scheduler", {}))
    name = str(sched.get("name", cfg.get("scheduler-name", "none"))).lower()
    scheduler = {"name": name}
    for key in {"none": (), "step": ("step_size", "gamma"), "exponential": ("gamma",), "cosine": ("min_lr",)}[name]:
        default = {"step_size": 1, "gamma": 0.1, "min_lr": 0.0}[key]
        value = sched.get(key, cfg.get("scheduler-" + key.replace("_", "-"), default))
        scheduler[key] = int(value) if key == "step_size" else float(value)
    return _object("training", num_rounds=int(get("num-server-rounds", "num_rounds", 1)),
        local_epochs=int(get("local-epochs", "local_epochs", 1)), batch_size=int(get("batch-size", "batch_size", 32)),
        learning_rate=float(get("learning-rate", "learning_rate", 0.01)), optimizer=optimizer, scheduler=scheduler)


def _selection_projection(manifest):
    m = manifest
    features = m.get("feature_columns", [])
    if isinstance(features, str):
        features = [features]
    loss = m.get("loss-name")
    layout = "segmentation" if loss == "segmentation_bce_dice" else ("survival" if m.get("task-type") == "survival" else ("image" if m.get("data_type", m.get("data-kind")) == "image" else "tabular"))
    if m.get("dp-track") == "association":
        layout = "association"
    target = m.get("target_column")
    targets = [] if target is None else [{"role": "target", "column": item} for item in (target if isinstance(target, list) else [target])]
    if layout == "association":
        targets = [{"role": "outcome", "column": target}, {"role": "exposure", "column": features[0] if features else None}]
    if layout == "survival":
        targets = [{"role": role, "column": col} for role, col in zip(("time", "event"), (target if isinstance(target, list) else [target]))]
    image_roles = None
    if layout in ("image", "segmentation"):
        assets = m.get("assets", {})
        def asset(name):
            obj = assets.get(m.get(name + "_asset", name + "s"), {})
            return {"type": obj.get("type"), "kind": obj.get("kind")}
        image_roles = _object("image_roles", image_column=m.get("image_path_col", assets.get(m.get("image_asset", "images"), {}).get("path_col")), mask_column=m.get("mask_path_col", assets.get(m.get("mask_asset", "masks"), {}).get("path_col")),
            sample_id_column=m.get("sample_id_col"), subject_id_column=m.get("subject_id_col"), mask_empty_column=m.get("mask_empty_col"),
            asset_types={"image": asset("image"), "mask": asset("mask") if layout == "segmentation" else None})
    bounds = m.get("feature-bounds")
    if bounds is not None and isinstance(bounds, dict):
        def vector(value):
            return [float(item) for item in (value if isinstance(value, (list, tuple)) else [value])]
        bounds = {"lower": vector(bounds.get("lower")), "upper": vector(bounds.get("upper"))}
    target_bounds = m.get("target-bounds")
    if target_bounds is not None:
        target_bounds = ([float(target_bounds["lower"]), float(target_bounds["upper"])]
            if isinstance(target_bounds, dict) else [float(item) for item in target_bounds])
    levels = _canonical_levels(m.get("target-levels"))
    image = layout in ("image", "segmentation")
    vocabulary = m.get("mask-vocabulary") if layout == "segmentation" else None
    if isinstance(vocabulary, str):
        vocabulary = [int(value) for value in vocabulary.split(",")]
    preprocessing = _object("preprocessing", version="source-units-v1", numeric_totalization="bounded-totalization-v1",
        patient_pooling="ordered-mean-mode-v1", row_order="keyed-unit-content-v1", image_selection=m.get("segmentation-selection") if layout == "segmentation" else None,
        image_decoder="canonical-decoded-v1" if image_roles else None, image_size=m.get("image-size") if image else None,
        extractor_profile=m.get("vision-extractor-profile") if image else None,
        encoder_sha256=m.get("public-initialisation-encoder-sha256", m.get("segmentation-checkpoint-sha256")) if image else None,
        mask_vocabulary=vocabulary, segmentation_preprocessing=m.get("segmentation-preprocessing") if layout == "segmentation" else None)
    unit = m.get("dp-unit", m.get("privacy_unit", "row"))
    return _object("selection", layout=layout, features=list(features), targets=targets, patient_column=m.get("patient_column"),
        privacy_unit=unit,
        unit_canonicalization="trim-utf8-v2" if unit == "patient" else "row-content-occurrence-v1",
        target_levels=levels, target_bounds=None if layout == "survival" else target_bounds,
        feature_bounds=bounds, target_encoding={"kind": m.get("task-type", "classification"),
            "preencoded": bool(m.get("target-preencoded", False)), "invalid_rule": "zero-contribution-v1"},
        association_encoding=({"preencoded": bool(m.get("association-preencoded", False)),
            "levels": {"outcome": _canonical_levels(m.get("association-outcome-levels")), "exposure": _canonical_levels(m.get("association-exposure-levels"))}, "unknown_code": 2} if layout == "association" else None),
        image_roles=image_roles, preprocessing=preprocessing)


def request_selection(manifest):
    """Project public column/asset roles; authorization aliases stay in manifest."""
    return _selection_projection(manifest)


def request_identity(mechanism, config, privacy, round_index=1, *, public_arrays=(), execution_fingerprint=None, manifest=None):
    """Trusted adapter from admitted execution pins to the single closed v3 schema.

    No private loader, census, sufficient statistic, source locator or whole-job
    hash is consulted here. Callers pass computed geometry to bind_private_data.
    """
    config = dict(config)
    cfg = dict(manifest or {})
    cfg.update(config.get("run", {}))
    pins = config.get("pins", {})
    selection = config.get("request-selection")
    if selection is None:
        selection = request_selection(cfg)
    else:
        _exact(selection, _SCHEMAS["selection"].split(), "selection")
    operation = config.get("operation", "train")
    name = mechanism.split("/")[0]
    aliases = {"validation-sufficient": "private-validation-vector", "validation": "private-validation-vector",
               "association": "association-vector", "hook-output": "hook-output-perturbation"}
    name = aliases.get(name, name)
    if "validation" in name:
        name = "private-validation-vector"
        operation = config.get("operation", "validate")
    if "association" in name:
        name = "association-vector"
        operation = "association"
    engine = config.get("engine_contract")
    if engine is not None:
        name = "native-" + engine["engine"].replace("_", "-")
    hook = config.get("hook")
    arrays = public_array_identity(public_arrays)
    loss = pins.get("loss_name", cfg.get("loss-name"))
    spec = None
    init_spec_hash = None
    if cfg.get("model-spec-b64"):
        from . import model_spec, initialisation
        spec = model_spec.canonical_spec(cfg)
        init_spec_hash = initialisation.initialisation_spec_sha256(cfg)
    params = {}
    for selected_loss, field in (("negbin_nll", "nb-dispersion"), ("gamma_nll", "gamma-shape"), ("huber", "huber-delta"), ("quantile", "quantile-level")):
        if loss == selected_loss:
            params[field.replace("-", "_")] = float(cfg[field])
    if loss in ("aft_weibull_nll", "aft_lognormal_nll", "discrete_hazard_nll"):
        from . import survival
        params["survival"] = survival.config_from_run(cfg, loss)
    if loss == "segmentation_bce_dice":
        params["segmentation"] = {key: cfg.get("segmentation-" + key.replace("_", "-")) for key in ("alpha", "smooth", "selection", "preprocessing", "output_shape")}
        params["segmentation"]["mask_vocabulary"] = cfg.get("mask-vocabulary")
    output_shape = None
    if loss is not None:
        if loss == "segmentation_bce_dice":
            output_shape = [int(value) for value in str(cfg.get("segmentation-output-shape", "1,128,128")).split(",")]
            params["segmentation"]["output_shape"] = output_shape
            params["segmentation"]["mask_vocabulary"] = [int(value) for value in cfg["mask-vocabulary"].split(",")]
            params["segmentation"]["alpha"] = float(params["segmentation"]["alpha"])
            params["segmentation"]["smooth"] = float(params["segmentation"]["smooth"])
        else:
            # This projection is also used by dependency-light native model
            # validation, which must not import Torch merely to name a shape.
            width = 1
            if loss in ("cross_entropy", "hinge"):
                width = max(2, int(cfg.get("num-classes", 2)))
            elif loss == "ordinal":
                width = max(1, int(cfg.get("num-classes", 2)) - 1)
            elif loss == "multilabel_bce":
                width = int(cfg["num-labels"])
            elif loss == "discrete_hazard_nll":
                width = len(params["survival"]["edges"]) - 1
            output_shape = [width]
    model = _object("model", family=name, spec=spec,
        input_shape=[int(cfg["num-features"])] if cfg.get("num-features") is not None else None,
        output_shape=output_shape,
        loss=loss, loss_parameters=params, engine_contract=engine)
    checkpoint = cfg.get("public-initialisation-checkpoint-sha256")
    init = _object("initialisation", mode=("hook-pinned" if hook else ("public-checkpoint" if checkpoint else ("spec-seeded" if spec else "none"))),
        version="dsflower-public-init-v1", spec_sha256=init_spec_hash,
        initial_model_sha256=config.get("initial-model-sha256", cfg.get("initial-model-sha256", arrays["sha256"] if arrays["schema"] else None)),
        checkpoint_sha256=checkpoint, manifest_sha256=cfg.get("public-initialisation-manifest-sha256"),
        encoder_sha256=cfg.get("public-initialisation-encoder-sha256", cfg.get("segmentation-checkpoint-sha256") if loss == "segmentation_bce_dice" else None), public_origin=cfg.get("public-initialisation-origin"),
        admission_policy_sha256=cfg.get("public-initialisation-admission-policy-sha256"))
    training = None
    if operation in ("train", "cv-train") and not name.startswith("native-") and not hook:
        training = _effective_training(cfg, pins)
    elif operation in ("holdout-evaluate", "cv-oof-release") and cfg.get("num-server-rounds"):
        training = _effective_training(cfg, pins)
    from .strategy import canonical_local_strategy
    strategy_cfg = dict(cfg)
    strategy_cfg.update({k: v for k, v in config.items() if k == "strategy" or k.startswith("strategy-")})
    strategy_track = "egress" if hook else ("neural" if name == "neural-dpsgd" else name)
    if (operation in ("holdout-evaluate", "cv-oof-release")
            and cfg.get("dp-track") == "neural"):
        # This release is part of the admitted training job; FedProx belongs to
        # its training identity even though the final mechanism is validation.
        strategy_track = "neural"
    strategy = canonical_local_strategy(strategy_cfg, strategy_track)
    unit = privacy.get("unit", privacy.get("privacy_unit", selection["privacy_unit"]))
    cv = cfg.get("cv-folds")
    holdout = cfg.get("resampling-method")
    budget = _object("budget_allocation", kind="cv" if cv else ("holdout" if holdout else "training"),
        training_fraction=0.8 if cv or holdout else None, evaluation_fraction=0.2 if cv or holdout else None,
        folds=cv, fold_epsilon=privacy.get("epsilon") if cv and operation == "cv-train" else None,
        fold_delta=privacy.get("delta") if cv and operation == "cv-train" else None,
        evaluation_epsilon=privacy.get("epsilon") if operation in ("holdout-evaluate", "cv-oof-release") else None,
        evaluation_delta=privacy.get("delta") if operation in ("holdout-evaluate", "cv-oof-release") else None)
    def policy_real(field):
        number = privacy.get(field)
        _identity_number(number, "privacy " + field, nullable=field == "clipping_norm")
        return None if number is None else float(number)
    policy = _object("privacy", policy_version="dsflower-policy-v3", epsilon=policy_real("epsilon"), delta=policy_real("delta"),
        unit=unit, unit_canonicalization="trim-utf8-v2" if unit == "patient" else "row-content-occurrence-v1", patient_column=selection["patient_column"],
        adjacency=privacy.get("adjacency", "replace_one"), clipping_norm=policy_real("clipping_norm"),
        accountant_profile="node-accountant-v1", sampling_profile="secure-poisson-v1" if name == "neural-dpsgd" else "fixed-release-v1",
        budget_allocation=budget, hook_sample_aggregate=bool(privacy.get("sample_aggregate", True)) if hook else None,
        hook_block_count_policy=int(privacy.get("sa_blocks", 8)) if hook and privacy.get("sample_aggregate", True) else (1 if hook else None),
        numeric_bounds_profile="bounded-numeric-v1")
    for field, number in budget.items():
        if field not in ("kind", "folds") and number is not None:
            _identity_number(number, "budget " + field)
            budget[field] = float(number)
    policy["policy_sha256"] = hashlib.sha256(_canonical_json({k: v for k, v in policy.items() if k != "policy_sha256"})).hexdigest()
    layout = config.get("layout")
    evaluation = None
    if layout is not None or cv or holdout or operation in ("validate", "holdout-evaluate", "cv-oof-release", "association"):
        layout = layout or {}
        resampling = None
        if cv or holdout:
            prefix = "cv-" if cv else "resampling-"
            resampling = _object("resampling", version=cfg.get(prefix + "version"), method=cfg.get(prefix + "method"),
                assignment=cfg.get(prefix + "assignment"), privacy_unit=cfg.get(prefix + "privacy-unit"),
                unit_canonicalization=cfg.get(prefix + "unit-canonicalization"), test_numerator=cfg.get("resampling-test-numerator"),
                test_denominator=cfg.get("resampling-test-denominator"), folds=cv)
        evaluation = _object("evaluation", kind=operation, layout_version=layout.get("version", layout.get("contract", "validation-vector-v3")),
            task=layout.get("task", cfg.get("validation-task")), bins=layout.get("bins", cfg.get("validation-bins")), bounds=selection["target_bounds"],
            survival_horizons=layout.get("horizons", cfg.get("validation-survival-horizons")), survival_nll_bound=layout.get("nll_bound", cfg.get("validation-survival-nll-bound")),
            artifact_format=cfg.get("validation-artifact-format"), artifact_sha256=cfg.get("validation-artifact-sha256"),
            profile_sha256=cfg.get("validation-profile-sha256"), public_schema_sha256=cfg.get("validation-public-schema-sha256"),
            resampling=resampling, fold_model_sha256=config.get("fold_model_sha256"))
    application = None
    if hook:
        application = _object("application", entrypoint=config.get("module"), package_sha256=config.get("module-sha256"),
            app_params=hook["app_params"], task=hook["task"], num_classes=hook["num_classes"],
            initialisation_contract_sha256=init_spec_hash, egress_schema=arrays["schema"],
            execution_profile=_object("execution_profile", sandbox_profile="node-egress-v1", threads=1,
                **{key: privacy.get(key) for key in ("egress_timeout", "egress_memory_mb", "egress_file_mb", "egress_processes")}))
        training = _effective_training({"num-server-rounds": hook["num_rounds"]}, {})
    profile = config.get("mechanism-profile")
    if profile is None:
        if name == "neural-dpsgd":
            profile = {key: policy[key] for key in ("accountant_profile", "sampling_profile", "numeric_bounds_profile")}
        elif hook:
            profile = {"gate_profile": "output-gate-v3", "block_assignment_profile": "unit-local-buckets-v1", "blocks": policy["hook_block_count_policy"]}
        else:
            profile = layout or {}
    return build_request_identity(operation=operation, mechanism={"name": name, "version": "v3", "profile": {"id": mechanism, "parameters": profile}},
        runtime=_public_runtime(execution_fingerprint), coordinate={"round": int(round_index), "fold": config.get("fold")},
        selection=selection, model=model, initialisation=init, public_arrays=arrays, training=training, strategy=strategy,
        privacy=policy, evaluation=evaluation, application=application)


def bind_seed(master, label, arrays):
    """Bind a sub-key to validated pre-noise tensors.

    Hook children are untrusted and may be nondeterministic.  Binding the final
    noise key to their validated update prevents the same noise vector from
    being reused over two different statistics while retaining sticky retries
    when the update is identical.
    """
    if not isinstance(master, (bytes, bytearray)) or len(master) != 32:
        raise RuntimeError("invalid dsFlower master release key")
    digest = hashlib.sha256()
    _frame(digest, "contract", b"dsflower-bound-subkey-v1")
    _frame(digest, "label", str(label).encode("utf-8", errors="strict"))
    _update_arrays(digest, "bound-arrays", arrays)
    return hmac.new(
        bytes(master), b"dsflower/bound-subkey/v1\x00" + digest.digest(),
        hashlib.sha256).digest()


def sub_seed(master, label):
    """Derive an independent 256-bit sub-key for one randomness axis."""
    if not isinstance(master, (bytes, bytearray)) or len(master) != 32:
        raise RuntimeError("invalid dsFlower master release key")
    return hmac.new(bytes(master),
                    b"dsflower/subkey/v2\x00" + str(label).encode("utf-8"),
                    hashlib.sha256).digest()


def _seed_int(seed):
    if not isinstance(seed, (bytes, bytearray)) or len(seed) < 8:
        raise RuntimeError("invalid deterministic seed")
    return int.from_bytes(bytes(seed)[:8], "big")


class SecureNumpyRng:
    """Small NumPy-compatible facade backed by a keyed ChaCha20 stream.

    ``normal`` uses Box-Muller and sums four independent samples divided by two.
    This preserves N(0, sigma^2) while applying the same 2n floating-point
    hardening principle used by Opacus secure mode.
    """

    def __init__(self, key):
        if not isinstance(key, (bytes, bytearray)) or len(key) != 32:
            raise RuntimeError("ChaCha20 RNG needs a 256-bit key")
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms
        except Exception as exc:
            raise RuntimeError(
                "cryptography is required for dsFlower's secure DP RNG"
            ) from exc
        nonce = hmac.new(bytes(key), b"dsflower/chacha20/nonce/v2",
                         hashlib.sha256).digest()[:16]
        self._stream = Cipher(algorithms.ChaCha20(bytes(key), nonce), mode=None).encryptor()

    def _bytes(self, n):
        return self._stream.update(b"\x00" * int(n))

    def _uniform(self, n):
        import numpy as np

        raw = np.frombuffer(self._bytes(8 * int(n)), dtype="<u8")
        # Exactly 53 random mantissa bits, strictly inside (0, 1).
        return ((raw >> np.uint64(11)).astype(np.float64) + 0.5) / float(1 << 53)

    def normal(self, loc=0.0, scale=1.0, size=None):
        import numpy as np

        shape = () if size is None else ((size,) if isinstance(size, int) else tuple(size))
        count = int(np.prod(shape, dtype=np.int64)) if shape else 1
        hardened_count = 4 * count
        pairs = (hardened_count + 1) // 2
        u1 = self._uniform(pairs)
        u2 = self._uniform(pairs)
        radius = np.sqrt(-2.0 * np.log(u1))
        angle = 2.0 * np.pi * u2
        samples = np.empty(2 * pairs, dtype=np.float64)
        samples[0::2] = radius * np.cos(angle)
        samples[1::2] = radius * np.sin(angle)
        samples = samples[:hardened_count].reshape(4, count).sum(axis=0) / 2.0
        samples = float(loc) + float(scale) * samples
        return samples.reshape(shape) if shape else float(samples[0])

    def _randbelow(self, upper):
        upper = int(upper)
        if upper <= 0:
            raise ValueError("upper must be positive")
        limit = (1 << 64) - ((1 << 64) % upper)
        while True:
            value = int.from_bytes(self._bytes(8), "little")
            if value < limit:
                return value % upper

    def bernoulli_mask_one_in(self, denominator, size):
        """Return ``size`` independent Bernoulli(1/denominator) draws.

        Rejection before the modulo removes modulo bias, so the inclusion
        probability is exactly the reciprocal requested by the DP accountant
        (within the ChaCha20 pseudorandom-stream model), rather than a rounded
        floating-point threshold.
        """
        import operator
        import numpy as np

        try:
            denominator = operator.index(denominator)
            size = operator.index(size)
        except TypeError as exc:
            raise ValueError("denominator and size must be integers") from exc
        if denominator <= 0 or denominator >= (1 << 64):
            raise ValueError("denominator must be in [1, 2^64)")
        if size < 0:
            raise ValueError("size must be non-negative")

        raw = np.frombuffer(self._bytes(8 * size), dtype="<u8")
        out = np.empty(size, dtype=np.bool_)
        remainder = (1 << 64) % denominator
        if remainder == 0:
            accepted = np.ones(size, dtype=np.bool_)
        else:
            cutoff = np.uint64((1 << 64) - remainder)
            accepted = raw < cutoff
        divisor = np.uint64(denominator)
        out[accepted] = (raw[accepted] % divisor) == 0
        # At most denominator-1 words out of 2^64 reach this path.  Retry them
        # individually with the same unbiased primitive instead of allocating
        # another full-size buffer.
        for index in np.flatnonzero(~accepted):
            out[index] = self._randbelow(denominator) == 0
        return out

    def permutation(self, n):
        import numpy as np

        out = np.arange(int(n))
        for i in range(len(out) - 1, 0, -1):
            j = self._randbelow(i + 1)
            out[i], out[j] = out[j], out[i]
        return out


def np_rng(seed):
    return SecureNumpyRng(seed)


def seed_torch(seed):
    """Seed common ML RNGs and request portable deterministic Torch kernels."""
    import random
    import numpy as np
    import torch

    value = _seed_int(seed)
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    torch.manual_seed(value & _MASK63)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(value & _MASK63)
    np.random.seed(value & _MASK31)
    random.seed(value & _MASK31)
    torch.use_deterministic_algorithms(True)
    cudnn = getattr(torch.backends, "cudnn", None)
    if cudnn is not None:
        cudnn.benchmark = False
        cudnn.deterministic = True
