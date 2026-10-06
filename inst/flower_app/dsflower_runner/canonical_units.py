"""Node-private source-unit encoding, multiset binding and stable execution order.

The ordering key is unit-local: it contains neither a request identity nor a
whole-dataset digest. Records and tokens must never be returned to an analyst.
"""
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import hmac
import math
import struct
import importlib
import importlib.util
import os

import numpy as np


_VERSION = b"dsflower-canonical-units-v1"


def _sibling(name):
    """Resolve only a runner sibling, including standalone Hook entry points."""
    if __package__:
        return importlib.import_module("." + name, __package__)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trusted_imports.py")
    spec = importlib.util.spec_from_file_location("_dsflower_trusted_import_bootstrap", path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.load_sibling(name, __file__)


def frame(tag, value=b""):
    tag = tag.encode("utf-8") if isinstance(tag, str) else bytes(tag)
    value = bytes(value)
    return struct.pack(">Q", len(tag)) + tag + struct.pack(">Q", len(value)) + value


def encode_scalar(value):
    """Typed, length-framed scalar, with endian/negative-zero normalization."""
    if value is None or value is np.ma.masked:
        return frame("missing")
    # pandas missing scalars cannot safely be converted to bool.
    if type(value).__name__ in ("NAType", "NaTType"):
        return frame("missing")
    if isinstance(value, (bool, np.bool_)):
        return frame("bool", b"\x01" if value else b"\x00")
    if isinstance(value, (int, np.integer)):
        if not -(1 << 63) <= int(value) < (1 << 63):
            raise ValueError("canonical integer is outside int64")
        return frame("int64", struct.pack("<q", int(value)))
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if math.isnan(number):
            return frame("nan")
        if math.isinf(number):
            return frame("posinf" if number > 0 else "neginf")
        return frame("float64", struct.pack("<d", 0.0 if number == 0 else number))
    if isinstance(value, str):
        return frame("utf8", value.encode("utf-8", errors="strict"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Encoded image/content records, never a filesystem pathname.
        return frame("record", value)
    raise TypeError("unsupported canonical source scalar")


def encode_numeric(value):
    """Apply the loader's numeric coercion without losing invalid source values.

    CSV and Parquet physical numeric representations share float64 semantics.
    Invalid text remains bound even if execution totalizes it to a safe value.
    """
    if value is None or type(value).__name__ in ("NAType", "NaTType"):
        return encode_scalar(None)
    if isinstance(value, str) and not value.strip():
        return encode_scalar(None)
    try:
        return encode_scalar(float(value))
    except (TypeError, ValueError, OverflowError):
        return encode_scalar(value)


def encode_row(values, *, numeric=False):
    encoder = encode_numeric if numeric else encode_scalar
    fields = tuple(encoder(value) for value in values)
    return frame("row", struct.pack(">Q", len(fields)) + b"".join(fields))


def array_record(value):
    """Digest decoded tensor content including original shape and numeric values.

    Iterate bounded chunks so image binding never serializes a complete cohort.
    Nonfinite tags are distinct and numeric coercion is float64, matching table
    source encoding, while tensor dimensions remain semantic.
    """
    array = np.asarray(value)
    if array.dtype.kind not in "biuf":
        raise TypeError("canonical tensor must be numeric")
    digest = hashlib.sha256()
    digest.update(frame("decoded-array-v1", struct.pack(">Q", array.ndim)))
    digest.update(frame("dtype", array.dtype.newbyteorder("<").str.encode("ascii")))
    digest.update(frame("shape", b"".join(struct.pack(">Q", int(n)) for n in array.shape)))
    # Numeric content is streamed in canonical LE float64 chunks. Nonfinite
    # IEEE representations are normalized rather than retaining NaN payloads.
    for offset in range(0, array.size, 131072):
        chunk = np.array(array.flat[offset:offset + 131072], dtype="<f8", copy=True)
        chunk[chunk == 0] = 0.0
        chunk[np.isnan(chunk)] = np.nan
        digest.update(chunk.tobytes(order="C"))
    return frame("decoded-array-sha256", digest.digest())


@dataclass(frozen=True)
class CanonicalUnits:
    records: tuple
    row_permutation: np.ndarray
    unit_slices: tuple
    unit_tokens: tuple
    multiset_digest: str
    row_tokens: tuple
    unit_ids: tuple | None = None


class CanonicalArray(np.ndarray):
    """An internal array carrying its full source binding through array views."""
    def __array_finalize__(self, parent):
        self.canonical_units = getattr(parent, "canonical_units", None)


def attach_units(value, units):
    result = np.asarray(value).view(CanonicalArray)
    result.canonical_units = units
    return result


def source_units(*values):
    found = [getattr(value, "canonical_units", None) for value in values]
    found = [units for units in found if units is not None]
    if not found:
        return None
    if any(units.multiset_digest != found[0].multiset_digest for units in found[1:]):
        raise ValueError("private arrays have inconsistent source bindings")
    return found[0]


def _order_key(secret, record):
    return hmac.new(secret, frame("dsflower/unit-order/v1", record), hashlib.sha256).digest()


def occurrence_token(record, occurrence):
    return frame("row-content-occurrence-v1", record + struct.pack(">Q", occurrence))


def canonicalize_units(rows, *, unit_ids=None, secret=None,
                       unit_canonicalization=None):
    """Encode whole units and return the one permutation used by all loaders.

    ``rows`` is an iterable of ordered scalar roles or already encoded records.
    Duplicate rows remain a multiset; only occurrences within an equal-content
    class are numbered, so a replacement cannot retokenize unrelated units.
    """
    if secret is None:
        secret = _sibling("seeding")._node_secret()
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise ValueError("canonical unit ordering requires a 256-bit node secret")
    keyed_order = lru_cache(maxsize=65536)(lambda record: _order_key(secret, record))
    encoded = [bytes(row) if isinstance(row, (bytes, bytearray)) else encode_row(row)
               for row in rows]
    unit = "row" if unit_ids is None else "patient"
    canonicalization = unit_canonicalization or (
        "row-content-occurrence-v1" if unit == "row" else "trim-utf8-v2")
    if canonicalization != ("row-content-occurrence-v1" if unit == "row" else "trim-utf8-v2"):
        raise ValueError("unsupported privacy-unit canonicalization")
    header = frame("unit-type", unit.encode()) + frame("unit-canonicalization", canonicalization.encode())
    if unit_ids is None:
        members = [(frame("unit", header + struct.pack(">Q", 1) + record), [index], None)
                   for index, record in enumerate(encoded)]
    else:
        _canonical_patient_id = _sibling("task")._canonical_patient_id
        ids = np.asarray(unit_ids)
        if ids.ndim != 1 or len(ids) != len(encoded):
            raise ValueError("canonical patient identifiers must match source rows")
        grouped = {}
        canonical_id = lru_cache(maxsize=65536)(_canonical_patient_id)
        for index, value in enumerate(ids):
            grouped.setdefault(canonical_id(value), []).append(index)
        members = []
        for patient, indices in grouped.items():
            indices.sort(key=lambda index: (keyed_order(encoded[index]), encoded[index]))
            record = frame("unit", header + frame("patient-id", patient.encode("utf-8"))
                           + struct.pack(">Q", len(indices))
                           + b"".join(frame("member", encoded[index]) for index in indices))
            members.append((record, indices, patient))
    members.sort(key=lambda item: (keyed_order(item[0]), item[0]))
    records, permutation, slices, tokens, row_tokens, patients = [], [], [], [], [], []
    occurrences = {}
    for record, indices, patient in members:
        records.append(record)
        offset = len(permutation)
        permutation.extend(indices)
        slices.append(slice(offset, len(permutation)))
        occurrence = occurrences.get(record, 0)
        occurrences[record] = occurrence + 1
        token = (occurrence_token(record, occurrence) if patient is None
                 else frame("patient-id", patient.encode("utf-8")))
        tokens.append(token)
        row_tokens.extend([token] * len(indices))
        if patient is not None:
            patients.append(patient)
    digest = hashlib.sha256(frame(_VERSION, struct.pack(">Q", len(records))))
    # Binding is canonical independently of keyed execution ordering.
    for record in sorted(records):
        digest.update(frame("unit", record))
    return CanonicalUnits(tuple(records), np.asarray(permutation, dtype=np.intp),
                          tuple(slices), tuple(tokens), digest.hexdigest(),
                          tuple(row_tokens), tuple(patients) if unit_ids is not None else None)


def canonicalize_arrays(X, y, unit_ids=None, *, secret=None):
    """Canonical source view for direct trusted numeric-engine entry points."""
    X, y = np.asarray(X), np.asarray(y)
    if X.ndim < 1 or y.ndim < 1 or len(X) != len(y):
        raise ValueError("canonical arrays must have matching example axes")
    return canonicalize_units(
        (encode_row((array_record(X[i]), array_record(y[i]))) for i in range(len(X))),
        unit_ids=unit_ids, secret=secret)


def canonicalize_frame(frame_data, columns, *, unit_ids=None, secret=None):
    """Bind selected numeric roles only; table symbols/paths are never inputs."""
    selected = frame_data[list(columns)]
    encode = lru_cache(maxsize=65536)(lambda row: encode_row(row, numeric=True))
    return canonicalize_units(
        (encode(row) for row in selected.itertuples(index=False, name=None)),
        unit_ids=unit_ids, secret=secret)
