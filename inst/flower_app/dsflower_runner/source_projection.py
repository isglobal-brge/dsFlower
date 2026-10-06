"""Read the node-private pre-totalization projection written during R staging."""
import hashlib
import json
import math
import os
import re
import stat

from .canonical_units import encode_row

_SCHEMA = "dsflower-source-projection-v1"
_FIELDS = ("source_projection_file", "source_projection_schema",
           "source_projection_sha256", "source_effective_sha256")


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("private source projection contains duplicate fields")
        result[key] = value
    return result


def _json(line):
    try:
        return json.loads(line, object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("private source projection is malformed") from exc


def _digest(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cell(value):
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ValueError("private source projection has an invalid scalar")
    tag = value["type"]
    if tag in ("missing", "nan", "posinf", "neginf"):
        if set(value) != {"type"}:
            raise ValueError("private source projection has unexpected scalar fields")
        return {"missing": None, "nan": float("nan"), "posinf": float("inf"),
                "neginf": -float("inf")}[tag]
    if set(value) != {"type", "value"}:
        raise ValueError("private source projection has unexpected scalar fields")
    item = value["value"]
    if tag == "utf8" and isinstance(item, str):
        item.encode("utf-8", errors="strict")
        return item
    if tag == "bool" and type(item) is bool:
        return item
    if tag == "number" and isinstance(item, str):
        try:
            number = float(item)
        except ValueError as exc:
            raise ValueError("private source projection numeric scalar is invalid") from exc
        if math.isfinite(number):
            return number
    raise ValueError("private source projection has an invalid scalar")


def records(context, manifest, effective, columns, *, normalizers=None):
    """Return source row records aligned with the verified effective staging file.

    Legacy direct-loader fixtures without a contract may supply raw frames.
    Every v3 R-staged manifest pins the source/effective pair and fails closed on
    missing/changed private sidecar bytes. No checksum participates in public R.
    """
    supplied = [field in manifest for field in _FIELDS]
    if not any(supplied):
        if manifest.get("semantic-randomness-contract") == "dsflower-semantic-randomness-v3":
            raise ValueError("v3 staged input is missing its private source projection")
        return None
    if not all(supplied) or manifest["source_projection_schema"] != _SCHEMA:
        raise ValueError("private source projection contract is incomplete")
    name = manifest["source_projection_file"]
    if (not isinstance(name, str) or not name or name != os.path.basename(name)
            or name in (".", "..") or "\\" in name):
        raise ValueError("private source projection has an unsafe location")
    from . import task
    directory = task._get_manifest_dir(context)
    path = os.path.join(directory, name)
    try:
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or (os.name == "posix" and (
                info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600)):
            raise ValueError("private source projection requires owner-only protected storage")
        source_hash, effective_hash = manifest[_FIELDS[2]], manifest[_FIELDS[3]]
        if any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
               for value in (source_hash, effective_hash)):
            raise ValueError("private source projection hash pin is invalid")
        data_name = manifest.get("data_file") or manifest.get("samples_file")
        if (_digest(path) != source_hash
                or _digest(os.path.join(directory, data_name)) != effective_hash):
            raise ValueError("private source projection or staged data changed after pinning")
        with open(path, "r", encoding="utf-8", errors="strict") as handle:
            header = _json(handle.readline())
            if (not isinstance(header, dict)
                    or set(header) != {"schema", "columns", "patient_column"}
                    or header["schema"] != _SCHEMA or header["columns"] != list(columns)
                    or header["patient_column"] != manifest.get("patient_column")):
                raise ValueError("private source projection schema differs from selected roles")
            ids = task._load_patient_ids(effective, manifest)
            result = []
            for index, line in enumerate(handle):
                record = _json(line)
                if (index >= len(effective) or not isinstance(record, dict)
                        or set(record) != {"values", "patient_id"}
                        or not isinstance(record["values"], list)
                        or len(record["values"]) != len(columns)):
                    raise ValueError("private source projection row geometry is invalid")
                expected_id = None if ids is None else str(ids[index])
                if record["patient_id"] != expected_id:
                    raise ValueError("private source projection patient roster changed")
                values = [_cell(value) for value in record["values"]]
                if normalizers:
                    values = [normalizers[column](value) if column in normalizers else value
                              for column, value in zip(columns, values)]
                result.append(encode_row(values, numeric=True))
            if len(result) != len(effective):
                raise ValueError("private source projection row count changed")
            return result
    except OSError as exc:
        raise ValueError("private source projection is unavailable; custodian must restage the input") from exc
