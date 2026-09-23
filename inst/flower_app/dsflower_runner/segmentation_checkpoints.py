"""Digest-bound public initialisation bundles; no resource-byte status export.

The R service admits a verified protected snapshot before staging. The trusted
runner repeats admission before opening private data. Only the local coordinator
helper returns decoder bytes, from the analyst's independently supplied file.
"""

import argparse
import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import tempfile
import zipfile

import numpy as np

SCHEMA = "dsflower-public-initialisation-bundle/v1"
IDENTITY_VERSION = "dsflower-public-initialisation-identity/v1"
INIT_KEY = "segmentation-decoder-init"
MANIFEST_KEY = "public-initialisation-manifest-sha256"
CHECKPOINT_KEY = "public-initialisation-checkpoint-sha256"
PROVENANCE_KEY = "public-initialisation-provenance"
ORIGIN_KEY = "public-initialisation-origin"
DIRECTORY_KEY = "public-initialisation-directory"
POLICY_KEY = "public-initialisation-policy"
TRANSPORT_KEY = "segmentation-public-initialization-b64"
IDENTITY_KEYS = (MANIFEST_KEY, CHECKPOINT_KEY, ORIGIN_KEY)
ENCODER_KEY = "public-initialisation-encoder-sha256"
VERSION_KEY = "public-initialisation-identity-version"
NODE_KEYS = (MANIFEST_KEY, CHECKPOINT_KEY, PROVENANCE_KEY, ORIGIN_KEY, DIRECTORY_KEY, POLICY_KEY, ENCODER_KEY, VERSION_KEY)
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_EVIDENCE = {"original_manifest", "protocol", "provenance", "audit", "licence", "mirror_metadata"}
_MAX_FILE = 2 * 1024 * 1024
_MAX_BUNDLE = 64 * 1024 * 1024
_ENCODER_SIZE = 46_830_571


def checkpoint_id(config):
    """Return a canonical origin; paths/session selectors are consumed by R."""
    value = config.get(INIT_KEY, "random")
    if value == "random":
        return None
    if type(value) is not str or value not in ("client", "resource"):
        raise ValueError("segmentation decoder_init must be random, client or resource after admission")
    return value


def _sha(value):
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError("public checkpoint requires a canonical SHA-256")
    return value


def _protected(path, directory=False):
    # Reuse the runner's existing POSIX/Windows ACL and reparse-point checks.
    # This does not load or enable a native tree runtime.
    from .xgboost_bundle import _secure_metadata, BundleVerificationError
    try:
        metadata = _secure_metadata(Path(path), directory=directory)
        if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != (0o700 if directory else 0o600):
            raise ValueError("public checkpoint snapshot permissions must be 0700/0600")
        return metadata
    except BundleVerificationError as exc:
        raise ValueError("public checkpoint paths must be regular, custodian-owned and protected") from exc


def _read(path, limit, expected_sha=None, expected_size=None, protected=True):
    before = _protected(path) if protected else os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("public checkpoint source must be a regular file")
    if before.st_size < 1 or before.st_size > limit:
        raise ValueError("public checkpoint file exceeds its size bound")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as handle:
        after = os.fstat(handle.fileno())
        if ((before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
                or not stat.S_ISREG(after.st_mode)):
            raise ValueError("public checkpoint file changed while opening")
        data = handle.read(limit + 1)
    final = _protected(path) if protected else os.lstat(path)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns):
        raise ValueError("public checkpoint file changed while reading")
    if len(data) > limit or (expected_size is not None and len(data) != expected_size):
        raise ValueError("public checkpoint file size mismatch")
    if expected_sha is not None and hashlib.sha256(data).hexdigest() != _sha(expected_sha):
        raise ValueError("public checkpoint file digest mismatch")
    return data


def _json(data):
    def unique(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError("public checkpoint JSON has duplicate fields")
            obj[key] = value
        return obj
    def nonfinite(_):
        raise ValueError("public checkpoint JSON is nonfinite")
    value = json.loads(data, object_pairs_hook=unique, parse_constant=nonfinite)
    if not isinstance(value, dict):
        raise ValueError("public checkpoint JSON must be an object")
    json.dumps(value, allow_nan=False)  # also rejects finite-syntax overflow
    return value


def _tensor_shapes(decoder):
    if decoder == "pointwise":
        return [(1, 128, 1, 1), (1,)]
    if decoder not in ("current", "narrow"):
        raise ValueError("public checkpoint decoder is unsupported")
    a, b = (32, 16) if decoder == "current" else (8, 4)
    return [(a, 128, 3, 3), (a,), (b, a, 3, 3), (b,), (1, b, 1, 1), (1,)]


def _decode_arrays(payload, manifest):
    """Check the full envelope and NPY headers before NumPy allocates arrays."""
    checkpoint = manifest["checkpoint"]
    if (type(checkpoint.get("size_bytes")) is not int
            or not 0 < len(payload) <= _MAX_FILE
            or len(payload) != checkpoint["size_bytes"]
            or hashlib.sha256(payload).hexdigest() != _sha(checkpoint["sha256"])):
        raise ValueError("public checkpoint file digest or size mismatch")
    shapes = _manifest_tensor_shapes(manifest)
    tensors = manifest["tensors"]
    if not isinstance(tensors, list) or len(tensors) != len(shapes):
        raise ValueError("public checkpoint tensor roster mismatch")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        entries = archive.infolist()
        expected_names = [str(i) + ".npy" for i in range(len(shapes))]
        if sorted(item.filename for item in entries) != sorted(expected_names):
            raise ValueError("public checkpoint NPZ tensor roster mismatch")
        for item in entries:
            index = int(item.filename[:-4])
            expected_bytes = int(np.prod(shapes[index])) * 4
            if item.file_size > expected_bytes + 4096:
                raise ValueError("public checkpoint NPZ tensor exceeds size bound")
            with archive.open(item) as member:
                data = member.read(expected_bytes + 4097)
            if len(data) != item.file_size:
                raise ValueError("public checkpoint NPZ tensor size mismatch")
            stream = io.BytesIO(data)
            version = np.lib.format.read_magic(stream)
            if version == (1, 0):
                shape, fortran, dtype = np.lib.format.read_array_header_1_0(
                    stream, max_header_size=4096)
            elif version == (2, 0):
                shape, fortran, dtype = np.lib.format.read_array_header_2_0(
                    stream, max_header_size=4096)
            else:
                raise ValueError("public checkpoint NPY version is unsupported")
            if (shape != shapes[index] or np.dtype(dtype).str != "<f4" or fortran
                    or item.file_size - stream.tell() != expected_bytes):
                raise ValueError("public checkpoint NPY tensor contract mismatch")
    arrays = []
    with np.load(io.BytesIO(payload), allow_pickle=False) as archive:
        for i, (item, shape) in enumerate(zip(tensors, shapes)):
            if (not isinstance(item, dict)
                    or set(item) != {"name", "shape", "dtype", "sha256"}
                    or item["name"] != str(i) or item["dtype"] != "float32"
                    or not isinstance(item["shape"], list)
                    or any(type(dim) is not int for dim in item["shape"])
                    or tuple(item["shape"]) != shape):
                raise ValueError("public checkpoint tensor contract mismatch")
            array = archive[item["name"]]
            if not np.isfinite(array).all() or np.any(np.abs(array) > 1.0e6):
                raise ValueError("public checkpoint tensor values are invalid")
            if hashlib.sha256(array.tobytes(order="C")).hexdigest() != _sha(item["sha256"]):
                raise ValueError("public checkpoint tensor digest mismatch")
            arrays.append(np.array(array, copy=True, order="C"))
    return arrays


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _record(record, limit):
    if (not isinstance(record, dict) or set(record) != {"file", "sha256", "size_bytes"}
            or not isinstance(record["file"], str) or _FILE.fullmatch(record["file"]) is None
            or type(record["size_bytes"]) is not int or not 0 < record["size_bytes"] <= limit):
        raise ValueError("public checkpoint artifact record is invalid")
    _sha(record["sha256"])
    return record


def _clean_spec(spec):
    clean = copy.deepcopy(spec)
    for layer in clean.get("layers", []):
        if layer.get("op") == "upsample" and layer.get("mode") == "nearest":
            layer.pop("mode")
    return clean


def _manifest_tensor_shapes(manifest):
    if manifest.get("role") != "tabular_model":
        return _tensor_shapes(manifest["decoder"])
    from . import model_spec
    cfg = manifest["model_config"]
    loss = cfg["loss-name"]
    model = model_spec.build_from_spec(
        manifest["model_spec"], cfg["num-features"],
        model_spec.output_width(loss, cfg), num_labels=cfg["num-labels"],
        output_limit=model_spec.output_limit_for_loss(loss))
    return [tuple(value.shape) for value in model.state_dict().values()]


_TABULAR_LOSS_PARAMETERS = {
    "negbin_nll": ("nb-dispersion", 1.0, 1e-6, 1e12),
    "gamma_nll": ("gamma-shape", 1.0, 1e-6, 1e12),
    "huber": ("huber-delta", 1.0, 1e-6, 1e6),
    "quantile": ("quantile-level", 0.5, 0.0, 1.0),
}


def _effective_tabular_model_config(config):
    """Canonical trusted loss constants; omitted and explicit defaults agree."""
    effective = dict(config)
    parameter = _TABULAR_LOSS_PARAMETERS.get(config.get("loss-name"))
    if parameter is not None:
        key, default, lower, upper = parameter
        value = config.get(key, default)
        if (type(value) not in (int, float) or not np.isfinite(value)
                or not lower <= value <= upper
                or (key == "quantile-level" and value in (lower, upper))):
            raise ValueError("public checkpoint loss parameter is invalid: " + key)
        effective[key] = float(value)
    return effective


def _validate_tabular_manifest(manifest, requested_spec=None):
    required = {"schema_version", "checkpoint_id", "model_id", "role",
                "model_spec", "model_spec_sha256", "model_config", "feature_contract",
                "dataset", "licence", "checkpoint", "tensors", "evidence",
                "pretraining_protocol_sha256", "creation"}
    if (set(manifest) != required or manifest["schema_version"] != SCHEMA
            or manifest["model_id"] != "declarative_neural"
            or not isinstance(manifest["checkpoint_id"], str)
            or not 0 < len(manifest["checkpoint_id"]) <= 128):
        raise ValueError("public checkpoint manifest violates the tabular contract")
    spec = manifest["model_spec"]
    if (not isinstance(spec, dict)
            or manifest["model_spec_sha256"] != hashlib.sha256(_canonical(spec)).hexdigest()
            or (requested_spec is not None and spec != requested_spec)):
        raise ValueError("public checkpoint does not match the requested model spec")
    cfg = manifest["model_config"]
    required_cfg = {"loss-name", "num-features", "num-classes", "num-labels"}
    survival_losses = {"aft_weibull_nll", "aft_lognormal_nll", "discrete_hazard_nll"}
    losses = {"bce_logits", "cross_entropy", "hinge", "ordinal", "multilabel_bce",
              "mse", "huber", "quantile", "poisson_nll", "negbin_nll", "gamma_nll"}
    if isinstance(cfg, dict) and cfg.get("loss-name") in survival_losses:
        required_cfg.add("survival-config-b64")
    parameter = _TABULAR_LOSS_PARAMETERS.get(cfg.get("loss-name")) if isinstance(cfg, dict) else None
    allowed_cfg = required_cfg | ({parameter[0]} if parameter is not None else set())
    if (not isinstance(cfg, dict) or not required_cfg <= set(cfg) <= allowed_cfg
            or cfg["loss-name"] not in losses | survival_losses
            or any(type(cfg[key]) is not int or not 1 <= cfg[key] <= 65536
                   for key in ("num-features", "num-classes", "num-labels"))
            or not 2 <= cfg["num-classes"] <= 1024
            or not 2 <= cfg["num-labels"] <= 1024):
        raise ValueError("public checkpoint model geometry is invalid")
    _effective_tabular_model_config(cfg)
    if cfg["loss-name"] in survival_losses:
        from . import survival
        encoded = cfg["survival-config-b64"]
        if not isinstance(encoded, str) or len(encoded) > 65536:
            raise ValueError("public checkpoint survival configuration is invalid")
        decoded = _json(base64.b64decode(encoded, validate=True))
        survival.validate_survival_config(decoded, cfg["loss-name"])
    if cfg["loss-name"] in {"bce_logits", "multilabel_bce", *survival_losses} and cfg["num-classes"] != 2:
        raise ValueError("public checkpoint binary/survival class geometry is invalid")
    feature = manifest["feature_contract"]
    if (not isinstance(feature, dict) or set(feature) != {
            "features", "feature_lower", "feature_upper", "target_levels", "target_bounds"}
            or not isinstance(feature["features"], list)
            or len(feature["features"]) != cfg["num-features"]
            or any(not isinstance(v, str) or not v for v in feature["features"])
            or len(set(feature["features"])) != cfg["num-features"]):
        raise ValueError("public checkpoint feature contract is invalid")
    lower, upper = feature["feature_lower"], feature["feature_upper"]
    if lower is not None or upper is not None:
        if (not isinstance(lower, list) or not isinstance(upper, list)
                or len(lower) != cfg["num-features"] or len(upper) != len(lower)
                or any(type(v) not in (int, float) or not np.isfinite(v) for v in lower + upper)
                or any(a >= b for a, b in zip(lower, upper))):
            raise ValueError("public checkpoint feature bounds are invalid")
    levels, bounds = feature["target_levels"], feature["target_bounds"]
    if levels is not None and (not isinstance(levels, list) or len(levels) < 2
            or any(type(v) not in (str, int, float, bool) for v in levels)
            or len({_canonical(v) for v in levels}) != len(levels)):
        raise ValueError("public checkpoint target levels are invalid")
    if bounds is not None and (not isinstance(bounds, dict) or set(bounds) != {"lower", "upper"}
            or any(type(v) not in (int, float) or not np.isfinite(v) for v in bounds.values())
            or bounds["lower"] >= bounds["upper"]):
        raise ValueError("public checkpoint target bounds are invalid")
    creation = manifest["creation"]
    if (not isinstance(creation, dict) or set(creation) != {"created_at", "creator"}
            or any(not isinstance(v, str) or not v.strip() or len(v) > 256 for v in creation.values())
            or any(not isinstance(manifest[k], dict) or not manifest[k] for k in ("dataset", "licence"))):
        raise ValueError("public checkpoint provenance is invalid")
    checkpoint = _record(manifest["checkpoint"], _MAX_FILE)
    if checkpoint["file"] != "checkpoint.npz":
        raise ValueError("public checkpoint filename is invalid")
    evidence = manifest["evidence"]
    if not isinstance(evidence, dict) or set(evidence) != _EVIDENCE:
        raise ValueError("public checkpoint evidence roster mismatch")
    records = [checkpoint, *[_record(r, _MAX_FILE) for r in evidence.values()]]
    if (manifest["pretraining_protocol_sha256"] != evidence["protocol"]["sha256"]
            or len({"manifest.json", *(r["file"] for r in records)}) != 1 + len(records)):
        raise ValueError("public checkpoint evidence identity mismatch")
    shapes = _manifest_tensor_shapes(manifest)
    tensors = manifest["tensors"]
    if not isinstance(tensors, list) or len(tensors) != len(shapes):
        raise ValueError("public checkpoint tensor roster mismatch")
    for i, (tensor, shape) in enumerate(zip(tensors, shapes)):
        if (not isinstance(tensor, dict) or set(tensor) != {"name", "shape", "dtype", "sha256"}
                or tensor["name"] != str(i) or tensor["dtype"] != "float32"
                or not isinstance(tensor["shape"], list)
                or any(type(dim) is not int for dim in tensor["shape"])
                or tuple(tensor["shape"]) != shape):
            raise ValueError("public checkpoint tensor contract mismatch")
        _sha(tensor["sha256"])
    return records


def verify_model_config(manifest, config, node_manifest=None):
    """Bind generic public tensors to the trusted loss and feature geometry."""
    if manifest.get("role") == "tabular_model":
        effective = _effective_tabular_model_config(manifest["model_config"])
        supplied = _effective_tabular_model_config(config)
        for key, value in effective.items():
            if key == "survival-config-b64":
                from . import survival
                same = survival.config_from_run(config, config["loss-name"]) == survival.config_from_run(
                    manifest["model_config"], config["loss-name"])
            else:
                same = supplied.get(key) == value
            if not same:
                raise ValueError("public checkpoint model contract mismatch: " + key)
        if node_manifest is not None:
            schema = manifest["feature_contract"]
            bounds = node_manifest.get("feature-bounds") or {}
            levels = node_manifest.get("target-levels")
            if isinstance(levels, dict):
                levels = levels.get("values")
            checks = {"features": node_manifest.get("feature_columns"),
                      "feature_lower": bounds.get("lower"), "feature_upper": bounds.get("upper"),
                      "target_levels": levels, "target_bounds": node_manifest.get("target-bounds")}
            if any(schema[key] != value for key, value in checks.items()):
                raise ValueError("public checkpoint feature/target contract mismatch")


def _validate_manifest(manifest, decoder_spec=None):
    if manifest.get("role") == "tabular_model":
        return _validate_tabular_manifest(manifest, decoder_spec)
    from . import segmentation
    required = {"schema_version", "checkpoint_id", "model_id", "decoder",
                "feature_contract", "encoder_sha256", "dataset", "licence",
                "checkpoint", "tensors", "evidence", "role", "encoder",
                "decoder_spec_sha256", "pretraining_protocol_sha256", "creation"}
    if (set(manifest) != required or manifest["schema_version"] != SCHEMA
            or manifest["model_id"] != "pytorch_resnet18_segmentation"
            or manifest["role"] != "segmentation_decoder"
            or manifest["feature_contract"] != segmentation.PROFILE
            or manifest["encoder_sha256"] != segmentation.CHECKPOINT_SHA256
            or not isinstance(manifest["checkpoint_id"], str)
            or not 0 < len(manifest["checkpoint_id"]) <= 128):
        raise ValueError("public checkpoint manifest violates the segmentation contract")
    creation = manifest["creation"]
    if (not isinstance(creation, dict) or set(creation) != {"created_at", "creator"}
            or any(not isinstance(v, str) or not v.strip() or len(v) > 256
                   for v in creation.values())):
        raise ValueError("public checkpoint creation metadata is invalid")
    expected_spec = segmentation.decoder_spec(manifest["decoder"])
    if (manifest["decoder_spec_sha256"] != hashlib.sha256(_canonical(expected_spec)).hexdigest()
            or (decoder_spec is not None and _clean_spec(decoder_spec) != expected_spec)):
        raise ValueError("public checkpoint does not match the requested decoder")
    for key in ("dataset", "licence"):
        if not isinstance(manifest[key], dict) or not manifest[key]:
            raise ValueError("public checkpoint requires dataset provenance and licence")
    checkpoint = _record(manifest["checkpoint"], _MAX_FILE)
    encoder = _record(manifest["encoder"], _MAX_BUNDLE)
    if (checkpoint["file"] != "checkpoint.npz" or encoder["file"] != "encoder.pth"
            or encoder["sha256"] != segmentation.CHECKPOINT_SHA256
            or encoder["size_bytes"] != _ENCODER_SIZE):
        raise ValueError("public checkpoint frozen encoder contract mismatch")
    evidence = manifest["evidence"]
    if not isinstance(evidence, dict) or set(evidence) != _EVIDENCE:
        raise ValueError("public checkpoint evidence roster mismatch")
    for record in evidence.values():
        _record(record, _MAX_FILE)
    if manifest["pretraining_protocol_sha256"] != evidence["protocol"]["sha256"]:
        raise ValueError("public checkpoint pretraining protocol digest mismatch")
    records = [checkpoint, encoder, *evidence.values()]
    names = ["manifest.json", *(record["file"] for record in records)]
    if len(names) != len(set(names)):
        raise ValueError("public checkpoint artifact filenames must be distinct")
    # Check tensor headers here, before NPZ/tensor parsing.
    shapes = _manifest_tensor_shapes(manifest)
    tensors = manifest["tensors"]
    if not isinstance(tensors, list) or len(tensors) != len(shapes):
        raise ValueError("public checkpoint tensor roster mismatch")
    for i, (tensor, shape) in enumerate(zip(tensors, shapes)):
        if (not isinstance(tensor, dict) or set(tensor) != {"name", "shape", "dtype", "sha256"}
                or tensor["name"] != str(i) or tensor["dtype"] != "float32"
                or not isinstance(tensor["shape"], list)
                or any(type(dim) is not int for dim in tensor["shape"])
                or tuple(tensor["shape"]) != shape):
            raise ValueError("public checkpoint tensor contract mismatch")
        _sha(tensor["sha256"])
    return records


def canonical_manifest_sha256(manifest):
    """Versioned scientific identity, independent of resource/ZIP/name metadata."""
    scientific = copy.deepcopy(manifest)
    scientific.pop("creation")
    scientific.pop("checkpoint_id")
    if scientific.get("role") == "tabular_model":
        scientific["model_config"] = _effective_tabular_model_config(scientific["model_config"])
    if scientific.get("role") == "tabular_model" and "survival-config-b64" in scientific["model_config"]:
        from . import survival
        config = scientific["model_config"]
        config["survival-config"] = survival.config_from_run(config, config["loss-name"])
        config.pop("survival-config-b64")
    for record in (scientific["checkpoint"],
                   *([scientific["encoder"]] if "encoder" in scientific else []),
                   *scientific["evidence"].values()):
        record.pop("file")
    return hashlib.sha256(_canonical({"identity_version": IDENTITY_VERSION,
                                      "manifest": scientific})).hexdigest()


def _summary(manifest):
    return {"identity_version": IDENTITY_VERSION,
            "provenance": {"manifest_sha256": canonical_manifest_sha256(manifest),
                           "manifest": manifest},
            "checkpoint_sha256": manifest["checkpoint"]["sha256"],
            "encoder_sha256": manifest.get("encoder_sha256", hashlib.sha256(b"").hexdigest()),
            "tensor_schema": copy.deepcopy(manifest["tensors"])}


def _verify_evidence(manifest, contents):
    evidence = manifest["evidence"]
    original = _json(contents[evidence["original_manifest"]["file"]])
    if (original.get("checkpoint_sha256") != manifest["checkpoint"]["sha256"]
            or original.get("tensor_sha256") != [item["sha256"] for item in manifest["tensors"]]
            or original.get("encoder_sha256") != manifest.get("encoder_sha256")
            or original.get("decoder") != manifest.get("decoder")
            or (manifest.get("role") == "tabular_model" and
                original.get("model_spec_sha256") != manifest["model_spec_sha256"])
            or original.get("privacy") != "public_nonprivate"):
        raise ValueError("public checkpoint pretraining evidence disagrees with manifest")
    for key in ("protocol", "provenance", "audit"):
        if original.get(key + "_sha256") != evidence[key]["sha256"]:
            raise ValueError("public checkpoint pretraining evidence digest mismatch")
    provenance = _json(contents[evidence["provenance"]["file"]])
    _sha(provenance.get("sha256"))
    for key in ("dataset", "release", "dataset_url", "licence", "attribution"):
        if not isinstance(provenance.get(key), str) or not provenance[key].strip():
            raise ValueError("public checkpoint dataset provenance is incomplete")
    if (manifest["dataset"] != provenance
            or original.get("dataset_sha256") != provenance.get("sha256")
            or original.get("dataset") != provenance.get("dataset")
            or provenance.get("licence_sha256") != evidence["licence"]["sha256"]
            or provenance.get("metadata_sha256") != evidence["mirror_metadata"]["sha256"]
            or manifest["licence"].get("declaration") != provenance.get("licence")
            or not isinstance(manifest["licence"].get("scope"), str)
            or not manifest["licence"]["scope"].strip()):
        raise ValueError("public checkpoint dataset or licence provenance mismatch")


def _verify_contents(contents, decoder_spec=None):
    manifest = _json(contents["manifest.json"])
    records = _validate_manifest(manifest, decoder_spec)
    if set(contents) != {"manifest.json", *(r["file"] for r in records)}:
        raise ValueError("public checkpoint bundle roster is not closed")
    for record in records:
        data = contents[record["file"]]
        if (len(data) != record["size_bytes"] or
                hashlib.sha256(data).hexdigest() != record["sha256"]):
            raise ValueError("public checkpoint artifact digest or size mismatch")
    _verify_evidence(manifest, contents)
    arrays = _decode_arrays(contents[manifest["checkpoint"]["file"]], manifest)
    return arrays, _summary(manifest)


def _read_contents(path, *, protected=False, decoder_spec=None, expected_bundle_sha256=None):
    path = Path(path)
    if path.is_dir():
        if expected_bundle_sha256 is not None:
            raise ValueError("registered public checkpoint resources require a ZIP archive")
        if protected:
            _protected(path, directory=True)
        elif path.is_symlink():
            raise ValueError("public checkpoint source cannot be a symlink")
        manifest_bytes = _read(path / "manifest.json", 65536, protected=protected)
        manifest = _json(manifest_bytes)
        records = _validate_manifest(manifest, decoder_spec)
        if set(p.name for p in path.iterdir()) != {"manifest.json", *(r["file"] for r in records)}:
            raise ValueError("public checkpoint bundle roster is not closed")
        contents = {"manifest.json": manifest_bytes}
        for record in records:
            contents[record["file"]] = _read(path / record["file"], record["size_bytes"],
                                            record["sha256"], record["size_bytes"], protected)
        return contents, None
    payload = _read(path, _MAX_BUNDLE, protected=protected)
    digest = hashlib.sha256(payload).hexdigest()
    if expected_bundle_sha256 is not None and digest != _sha(expected_bundle_sha256):
        raise ValueError("public checkpoint registered bundle digest mismatch")
    # Bound the central directory before ZipFile allocates its entry objects.
    # Small checkpoint bundles never need ZIP64 or multi-disk containers.
    eocd = payload.rfind(b"PK\x05\x06", max(0, len(payload) - 65557))
    if eocd < 0 or len(payload) - eocd < 22:
        raise ValueError("public checkpoint archive footer is invalid")
    _, disk, directory_disk, disk_count, total_count, directory_size, directory_offset, comment_size = struct.unpack(
        "<4s4H2LH", payload[eocd:eocd + 22])
    if (disk or directory_disk or disk_count != total_count or not 1 <= total_count <= 16
            or directory_size > 16384 or directory_offset + directory_size != eocd
            or eocd + 22 + comment_size != len(payload)
            or payload[:4] != b"PK\x03\x04"):
        raise ValueError("public checkpoint archive directory bounds are invalid")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        entries = archive.infolist()
        if len(entries) != total_count or not 1 <= len(entries) <= 16 or sum(e.file_size for e in entries) > _MAX_BUNDLE:
            raise ValueError("public checkpoint archive exceeds decompression bounds")
        names = [e.filename for e in entries]
        if len(set(names)) != len(names) or "manifest.json" not in names:
            raise ValueError("public checkpoint archive roster is invalid")
        for entry in entries:
            mode = entry.external_attr >> 16
            if (not _FILE.fullmatch(entry.filename) or entry.is_dir()
                    or (stat.S_IFMT(mode) not in (0, stat.S_IFREG))
                    or entry.flag_bits & 1
                    or entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
                    or not 0 < entry.file_size <= _MAX_BUNDLE
                    or entry.file_size > max(4096, entry.compress_size * 512)):
                raise ValueError("public checkpoint archive entry is unsafe or exceeds bounds")
        manifest_entry = archive.getinfo("manifest.json")
        if manifest_entry.file_size > 65536:
            raise ValueError("public checkpoint manifest exceeds size bound")
        manifest_bytes = archive.read(manifest_entry)
        records = _validate_manifest(_json(manifest_bytes), decoder_spec)
        expected = {r["file"]: r for r in records}
        if set(names) != {"manifest.json", *expected}:
            raise ValueError("public checkpoint archive roster is not closed")
        # Complete member size/roster validation precedes any tensor parsing.
        for name, record in expected.items():
            if archive.getinfo(name).file_size != record["size_bytes"]:
                raise ValueError("public checkpoint archive artifact size mismatch")
        contents = {"manifest.json": manifest_bytes}
        for name in expected:
            contents[name] = archive.read(name)
    return contents, digest


def inspect_bundle(path, decoder_spec=None):
    contents, _ = _read_contents(path, decoder_spec=decoder_spec)
    return _verify_contents(contents, decoder_spec)[1]


def pack_bundle(directory, output_file):
    contents, _ = _read_contents(directory)
    _verify_contents(contents)
    with zipfile.ZipFile(output_file, "x", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(contents):
            archive.writestr(name, contents[name])
    return inspect_bundle(output_file)


def admit_bundle(path, cache_root, expected_bundle_sha256=None, decoder_spec=None):
    """Verify all public bytes, then atomically publish a private snapshot."""
    contents, archive_digest = _read_contents(path, decoder_spec=decoder_spec,
                                               expected_bundle_sha256=expected_bundle_sha256)
    _, summary = _verify_contents(contents, decoder_spec)
    root = Path(cache_root)
    if not root.is_absolute():
        raise ValueError("public checkpoint protected cache must be absolute")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _protected(root, directory=True)
    # Storage identity need not be the scientific/noise identity.
    storage_digest = archive_digest or hashlib.sha256(_canonical(summary)).hexdigest()
    destination = root / storage_digest
    if not destination.exists():
        temporary = Path(tempfile.mkdtemp(prefix=".admitting-", dir=root))
        try:
            for name, data in contents.items():
                fd = os.open(temporary / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as output:
                    output.write(data)
                    output.flush()
                    os.fsync(output.fileno())
            try:
                os.rename(temporary, destination)
            except OSError:
                if not destination.exists():
                    raise
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
    result = verify_snapshot(destination, summary["provenance"]["manifest_sha256"], decoder_spec)
    result["bundle_sha256"] = archive_digest
    return result


def verify_snapshot(snapshot_directory, manifest_sha256, decoder_spec=None):
    path = Path(snapshot_directory)
    if not path.is_absolute():
        raise ValueError("public checkpoint snapshot must be absolute")
    _protected(path.parent, directory=True)
    contents, _ = _read_contents(path, protected=True, decoder_spec=decoder_spec)
    _, summary = _verify_contents(contents, decoder_spec)
    if summary["provenance"]["manifest_sha256"] != _sha(manifest_sha256):
        raise ValueError("public checkpoint canonical manifest digest mismatch")
    return dict(summary, snapshot_directory=str(path))


def coordinator_payload(checkpoint_file, summary, decoder_spec=None):
    """LOCAL analyst file only. This helper must never back node status."""
    manifest = summary["provenance"]["manifest"]
    _validate_manifest(manifest, decoder_spec)
    expected = _summary(manifest)
    if any(summary.get(key) != value for key, value in expected.items()):
        raise ValueError("public checkpoint summary identity mismatch")
    if Path(checkpoint_file).is_dir() or Path(checkpoint_file).suffix.lower() == ".zip":
        local = client_payload(checkpoint_file, decoder_spec)
        if local["provenance"]["manifest_sha256"] != expected["provenance"]["manifest_sha256"]:
            raise ValueError("local coordinator bundle differs from node-admitted identity")
        return dict(expected, local_arrays_b64=local["local_arrays_b64"])
    payload = _read(Path(checkpoint_file), _MAX_FILE, manifest["checkpoint"]["sha256"],
                    manifest["checkpoint"]["size_bytes"], protected=False)
    _decode_arrays(payload, manifest)
    return dict(expected, local_arrays_b64=base64.b64encode(payload).decode("ascii"))


def client_payload(path, decoder_spec=None):
    """Produce coordinator arrays from an independently supplied LOCAL bundle."""
    contents, _ = _read_contents(path, decoder_spec=decoder_spec)
    _, summary = _verify_contents(contents, decoder_spec)
    return dict(summary, local_arrays_b64=base64.b64encode(contents["checkpoint.npz"]).decode("ascii"))


def server_initialization(config):
    from . import model_spec
    selected = checkpoint_id(config)
    encoded = config.get(TRANSPORT_KEY)
    if selected is None:
        if encoded is not None:
            raise ValueError("random decoder cannot carry public initialization bytes")
        return None
    if not isinstance(encoded, str) or len(encoded) > 4 * _MAX_FILE:
        raise ValueError("public decoder requires a bounded local coordinator payload")
    payload = _json(base64.b64decode(encoded, validate=True))
    expected_keys = set(_summary(payload["provenance"]["manifest"])) | {"local_arrays_b64"}
    if set(payload) != expected_keys:
        raise ValueError("public initialization payload fields are invalid")
    manifest = payload["provenance"]["manifest"]
    _validate_manifest(manifest, model_spec.read_spec(config))
    verify_model_config(manifest, config)
    if any(payload[key] != value for key, value in _summary(manifest).items()):
        raise ValueError("public initialization payload identity mismatch")
    return _decode_arrays(base64.b64decode(payload["local_arrays_b64"], validate=True), manifest)


def verify_node_checkpoint(config, manifest):
    from . import model_spec
    selected = checkpoint_id(manifest)
    if checkpoint_id(config) != selected:
        raise ValueError("Flower public checkpoint selection differs from node manifest")
    if selected is None:
        if any(key in manifest or key in config for key in NODE_KEYS if key != POLICY_KEY):
            raise ValueError("random decoder cannot carry public checkpoint provenance")
        return None, None
    for key in NODE_KEYS:
        if key not in manifest or config.get(key) != manifest[key]:
            raise ValueError("public checkpoint requires node-owned provenance pins")
    origin = "analyst-declared" if selected == "client" else "resource"
    policy = manifest[POLICY_KEY]
    if (manifest[ORIGIN_KEY] != origin or policy not in ("analyst_or_resource", "resource_only")
            or (selected == "client" and policy != "analyst_or_resource")):
        raise ValueError("public checkpoint origin violates node policy")
    directory = Path(manifest[DIRECTORY_KEY])
    if not directory.is_absolute():
        raise ValueError("public checkpoint snapshot must be absolute")
    _protected(directory.parent, directory=True)
    contents, _ = _read_contents(directory, protected=True,
                                 decoder_spec=model_spec.read_spec(config))
    arrays, summary = _verify_contents(contents, model_spec.read_spec(config))
    verify_model_config(summary["provenance"]["manifest"], config, manifest)
    if (summary["provenance"]["manifest_sha256"] != manifest[MANIFEST_KEY]
            or summary["checkpoint_sha256"] != manifest[CHECKPOINT_KEY]
            or summary["encoder_sha256"] != manifest[ENCODER_KEY]
            or summary["identity_version"] != manifest[VERSION_KEY]
            or summary != manifest[PROVENANCE_KEY]):
        raise ValueError("public checkpoint provenance differs from node manifest")
    return arrays, summary


def record_release(context, manifest, round_index, arrays, fold=None):
    if checkpoint_id(manifest) is None:
        return
    from . import task
    directory = task._get_manifest_dir(context)
    initialisation = ("analyst-declared" if manifest[ORIGIN_KEY] == "analyst-declared"
                      else "resource:" + manifest[MANIFEST_KEY])
    record = {"schema_version": "dsflower-public-initialisation-release/v1",
              "run_token": manifest["run_token"], "round": int(round_index),
              "initialisation": initialisation,
              "public_initialisation_policy": manifest[POLICY_KEY],
              "public_initialisation": manifest[PROVENANCE_KEY],
              "released_tensors": [
                  {"shape": list(a.shape), "dtype": str(a.dtype),
                   "sha256": hashlib.sha256(a.tobytes(order="C")).hexdigest()} for a in arrays]}
    if fold is not None:
        if type(fold) is not int or not 1 <= fold <= 10:
            raise ValueError("public checkpoint release fold is invalid")
        record["fold"] = fold
    filename = ("segmentation-public-init-release-fold-%02d-%06d.json" % (fold, round_index)
                if fold is not None else "segmentation-public-init-release-%06d.json" % round_index)
    fd, temporary = tempfile.mkstemp(prefix=".segmentation-release-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(_canonical(record))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, os.path.join(directory, filename))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("inspect", "local", "admit", "verify", "pack"))
    parser.add_argument("path")
    parser.add_argument("destination", nargs="?")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--decoder-spec-b64")
    args = parser.parse_args()
    spec = (_json(base64.b64decode(args.decoder_spec_b64, validate=True))
            if args.decoder_spec_b64 else None)
    if args.operation == "inspect":
        result = inspect_bundle(args.path, spec)
    elif args.operation == "local":
        result = client_payload(args.path, spec)
    elif args.operation == "admit":
        result = admit_bundle(args.path, args.destination, args.expected_sha256, spec)
    elif args.operation == "verify":
        result = verify_snapshot(args.path, args.manifest_sha256, spec)
    else:
        result = pack_bundle(args.path, args.destination)
    print(json.dumps(result, allow_nan=False, sort_keys=True))


if __name__ == "__main__":
    main()
