"""Custodian-installed, provenance-bound public segmentation decoders.

This module reads public artifacts only. Admission also runs it before R stages
private metadata. A checkpoint is never fetched or deserialized with pickle.
"""

import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import zipfile

import numpy as np

SCHEMA = "dsflower-segmentation-public-checkpoint/v1"
INIT_KEY = "segmentation-decoder-init"
MANIFEST_KEY = "segmentation-public-manifest-sha256"
CHECKPOINT_KEY = "segmentation-public-checkpoint-sha256"
PROVENANCE_KEY = "segmentation-public-provenance"
TRANSPORT_KEY = "segmentation-public-initialization-b64"
IDENTITY_KEYS = (INIT_KEY, MANIFEST_KEY, CHECKPOINT_KEY)
NODE_KEYS = (MANIFEST_KEY, CHECKPOINT_KEY, PROVENANCE_KEY)
_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_EVIDENCE = {"original_manifest", "protocol", "provenance", "audit", "licence",
             "mirror_metadata"}
_MAX_FILE = 2 * 1024 * 1024


def checkpoint_id(config):
    value = config.get(INIT_KEY, "random")
    if value == "random":
        return None
    if (not isinstance(value, str) or not value.startswith("public:")
            or _ID.fullmatch(value[7:]) is None):
        raise ValueError("segmentation decoder_init must be random or public:<checkpoint-id>")
    return value[7:]


def _sha(value):
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError("public checkpoint requires a canonical SHA-256")
    return value


def _protected(path, directory=False):
    # Reuse the runner's existing POSIX/Windows ACL and reparse-point checks.
    # This does not load or enable a native tree runtime.
    from .xgboost_bundle import _secure_metadata, BundleVerificationError
    try:
        return _secure_metadata(Path(path), directory=directory)
    except BundleVerificationError as exc:
        raise ValueError("public checkpoint paths must be regular, custodian-owned and protected") from exc


def _read(path, limit, expected_sha=None, expected_size=None):
    before = _protected(path)
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
    final = _protected(path)
    if (before.st_dev, before.st_ino) != (final.st_dev, final.st_ino):
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


def _artifact(directory, record):
    if (not isinstance(record, dict)
            or set(record) != {"file", "sha256", "size_bytes"}
            or not isinstance(record["file"], str)
            or _FILE.fullmatch(record["file"]) is None
            or type(record["size_bytes"]) is not int
            or not 0 < record["size_bytes"] <= _MAX_FILE):
        raise ValueError("public checkpoint artifact record is invalid")
    return _read(directory / record["file"], _MAX_FILE,
                 record["sha256"], record["size_bytes"])


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
    shapes = _tensor_shapes(manifest["decoder"])
    tensors = manifest["tensors"]
    if not isinstance(tensors, list) or len(tensors) != len(shapes):
        raise ValueError("public checkpoint tensor roster mismatch")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        entries = archive.infolist()
        expected_names = [str(i) + ".npy" for i in range(len(shapes))]
        if sorted(item.filename for item in entries) != expected_names:
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


def verify_checkpoint(registry_root, checkpoint_id, manifest_sha256, decoder_spec=None):
    """Return verified arrays and provenance, using only bounded public bytes."""
    from . import segmentation

    if not isinstance(checkpoint_id, str) or _ID.fullmatch(checkpoint_id) is None:
        raise ValueError("invalid public checkpoint id")
    root = Path(registry_root)
    if not root.is_absolute() or root.name != "segmentation-public-checkpoints":
        raise ValueError("public checkpoint registry must be under protected node state")
    # R canonicalises the secret's parent. Reject links throughout the registry
    # itself; the parent is the existing protected node-state trust root.
    _protected(root.parent, directory=True)
    _protected(root, directory=True)
    directory = root / checkpoint_id
    _protected(directory, directory=True)
    manifest = _json(_read(directory / "manifest.json", 65536, manifest_sha256))
    required = {"schema_version", "checkpoint_id", "model_id", "decoder",
                "feature_contract", "encoder_sha256", "dataset", "licence",
                "checkpoint", "tensors", "evidence"}
    if (set(manifest) != required or manifest["schema_version"] != SCHEMA
            or manifest["checkpoint_id"] != checkpoint_id
            or manifest["model_id"] != "pytorch_resnet18_segmentation"
            or manifest["feature_contract"] != segmentation.PROFILE
            or manifest["encoder_sha256"] != segmentation.CHECKPOINT_SHA256):
        raise ValueError("public checkpoint manifest violates the segmentation contract")
    for key in ("dataset", "licence"):
        if not isinstance(manifest[key], dict) or not manifest[key]:
            raise ValueError("public checkpoint requires dataset provenance and licence")
    if decoder_spec is not None:
        clean = copy.deepcopy(decoder_spec)
        for layer in clean.get("layers", []):
            if layer.get("op") == "upsample" and layer.get("mode") == "nearest":
                layer.pop("mode")
        if clean != segmentation.decoder_spec(manifest["decoder"]):
            raise ValueError("public checkpoint does not match the requested decoder")
    tensors = manifest["tensors"]
    evidence = manifest["evidence"]
    if not isinstance(evidence, dict) or set(evidence) != _EVIDENCE:
        raise ValueError("public checkpoint evidence roster mismatch")
    files = [manifest["checkpoint"]["file"], "manifest.json"]
    verified = {}
    for key, record in evidence.items():
        verified[key] = _artifact(directory, record)
        files.append(record["file"])
    if len(files) != len(set(files)):
        raise ValueError("public checkpoint artifact filenames must be distinct")
    original = _json(verified["original_manifest"])
    if (original.get("checkpoint_sha256") != manifest["checkpoint"]["sha256"]
            or original.get("tensor_sha256") != [item["sha256"] for item in tensors]
            or original.get("encoder_sha256") != manifest["encoder_sha256"]
            or original.get("decoder") != manifest["decoder"]
            or original.get("privacy") != "public_nonprivate"):
        raise ValueError("public checkpoint pretraining evidence disagrees with manifest")
    for key in ("protocol", "provenance", "audit"):
        if original.get(key + "_sha256") != evidence[key]["sha256"]:
            raise ValueError("public checkpoint pretraining evidence digest mismatch")
    provenance = _json(verified["provenance"])
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
    payload = _artifact(directory, manifest["checkpoint"])
    arrays = _decode_arrays(payload, manifest)
    return arrays, {"manifest_sha256": manifest_sha256, "manifest": manifest}


def initialization_payload(registry_root, checkpoint_id, manifest_sha256, decoder_spec=None):
    """Public-only status payload for the ordinary researcher-side ServerApp."""
    _, provenance = verify_checkpoint(
        registry_root, checkpoint_id, manifest_sha256, decoder_spec)
    payload = _artifact(Path(registry_root) / checkpoint_id,
                        provenance["manifest"]["checkpoint"])
    return {"provenance": provenance,
            "checkpoint_base64": base64.b64encode(payload).decode("ascii")}


def server_initialization(config):
    """Verify a public status payload; it grants no node-side authorization."""
    from . import model_spec, segmentation
    selected = checkpoint_id(config)
    encoded = config.get(TRANSPORT_KEY)
    if selected is None:
        if encoded is not None:
            raise ValueError("random decoder cannot carry public initialization bytes")
        return None
    if not isinstance(encoded, str) or len(encoded) > 4 * _MAX_FILE:
        raise ValueError("public decoder requires a bounded node status payload")
    status = _json(base64.b64decode(encoded, validate=True))
    if set(status) != {"provenance", "checkpoint_base64"}:
        raise ValueError("public initialization payload fields are invalid")
    provenance = status["provenance"]
    if not isinstance(provenance, dict) or set(provenance) != {"manifest_sha256", "manifest"}:
        raise ValueError("public initialization provenance is invalid")
    _sha(provenance["manifest_sha256"])
    manifest = provenance["manifest"]
    if (manifest.get("schema_version") != SCHEMA
            or manifest.get("checkpoint_id") != selected
            or manifest.get("model_id") != "pytorch_resnet18_segmentation"
            or manifest.get("feature_contract") != segmentation.PROFILE
            or manifest.get("encoder_sha256") != segmentation.CHECKPOINT_SHA256):
        raise ValueError("public initialization payload violates segmentation contract")
    spec = model_spec.read_spec(config)
    for layer in spec.get("layers", []):
        if layer.get("op") == "upsample" and layer.get("mode") == "nearest":
            layer.pop("mode")
    if spec != segmentation.decoder_spec(manifest["decoder"]):
        raise ValueError("public checkpoint does not match the requested decoder")
    return _decode_arrays(base64.b64decode(status["checkpoint_base64"], validate=True), manifest)


def verify_node_checkpoint(config, manifest):
    """Resolve exclusively from the node secret's parent and node-authored pins."""
    from . import model_spec
    selected = checkpoint_id(manifest)
    if checkpoint_id(config) != selected:
        raise ValueError("Flower public checkpoint selection differs from node manifest")
    if selected is None:
        if any(key in manifest or key in config for key in NODE_KEYS):
            raise ValueError("random decoder cannot carry public checkpoint provenance")
        return None, None
    for key in NODE_KEYS:
        if key not in manifest or config.get(key) != manifest[key]:
            raise ValueError("public checkpoint requires node-owned provenance pins")
    secret = os.environ.get("DSFLOWER_NODE_SECRET_FILE", "")
    if not secret or not os.path.isabs(secret):
        raise ValueError("public checkpoint registry has no protected node state")
    arrays, provenance = verify_checkpoint(
        Path(secret).parent / "segmentation-public-checkpoints", selected,
        manifest[MANIFEST_KEY], model_spec.read_spec(config))
    if (provenance != manifest[PROVENANCE_KEY]
            or provenance["manifest"]["checkpoint"]["sha256"] != manifest[CHECKPOINT_KEY]):
        raise ValueError("public checkpoint provenance differs from node manifest")
    return arrays, provenance


def record_release(context, manifest, round_index, arrays):
    """Store public provenance with the successful DP release, never private data."""
    if checkpoint_id(manifest) is None:
        return
    from . import task
    directory = task._get_manifest_dir(context)
    record = {"schema_version": "dsflower-segmentation-public-release/v1",
              "run_token": manifest["run_token"], "round": int(round_index),
              "decoder_init": manifest[INIT_KEY],
              "public_initialisation": manifest[PROVENANCE_KEY],
              "released_tensors": [
                  {"shape": list(a.shape), "dtype": str(a.dtype),
                   "sha256": hashlib.sha256(a.tobytes(order="C")).hexdigest()}
                  for a in arrays]}
    payload = json.dumps(record, sort_keys=True, allow_nan=False).encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=".segmentation-release-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, os.path.join(
            directory, "segmentation-public-init-release-%06d.json" % round_index))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
