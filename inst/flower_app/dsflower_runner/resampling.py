"""Stateless node-owned resampling.

Assignments are a PRF of the fixed semantic contract and one protected unit.
The analyst cannot choose a seed and operational run identities are absent, so
retrying or recreating the same contract yields the same partition without any
database or history.
"""

import hashlib
import hmac
import json
import math
import re

import numpy as np

from . import seeding


_VERSION = "dsflower-resampling-v1"
_METHOD = "holdout"
_ASSIGNMENT = "hmac-sha256-threshold-v1"
_CV_VERSION = "dsflower-cross-validation-v1"
_CV_METHOD = "cross_validation"
_CV_ASSIGNMENT = "hmac-sha256-score-v1"
_DENOMINATOR = 1_000_000
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CONTRACT_FIELDS = frozenset({
    "assignment", "method", "privacy_unit", "sha256",
    "test_denominator", "test_numerator", "unit_canonicalization",
    "version",
})
_CV_CONTRACT_FIELDS = frozenset({
    "assignment", "folds", "method", "privacy_unit", "sha256",
    "unit_canonicalization", "version",
})


def _exact_int(value, label, lower, upper):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("%s must be an exact integer" % label)
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("%s must be an exact integer" % label) from exc
    if (not math.isfinite(number) or number != math.floor(number)
            or not lower <= number <= upper):
        raise ValueError("%s is outside its supported range" % label)
    return int(number)


def _payload(test_numerator, privacy_unit):
    numerator = _exact_int(
        test_numerator, "holdout test numerator", 1, _DENOMINATOR - 1)
    unit = str(privacy_unit).lower()
    if unit not in ("row", "patient"):
        raise ValueError("holdout privacy unit must be row or patient")
    return {
        "assignment": _ASSIGNMENT,
        "method": _METHOD,
        "privacy_unit": unit,
        "test_denominator": _DENOMINATOR,
        "test_numerator": numerator,
        "unit_canonicalization": (
            "trim-utf8-v2" if unit == "patient" else "row-ordinal-v1"),
        "version": _VERSION,
    }


def _wire(payload):
    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")


def holdout_contract(test_numerator, privacy_unit):
    payload = _payload(test_numerator, privacy_unit)
    return {**payload, "sha256": hashlib.sha256(_wire(payload)).hexdigest()}


def validate_holdout_contract(value):
    if (not isinstance(value, dict)
            or frozenset(value.keys()) != _CONTRACT_FIELDS):
        raise ValueError("holdout contract has an unexpected field or seed axis")
    expected = holdout_contract(
        value.get("test_numerator"), value.get("privacy_unit"))
    supplied_hash = value.get("sha256")
    if (not isinstance(supplied_hash, str)
            or _SHA256_RE.fullmatch(supplied_hash) is None
            or supplied_hash != expected["sha256"]):
        raise ValueError("holdout contract SHA-256 is invalid")
    if value != expected:
        raise ValueError("holdout contract differs from its canonical form")
    return expected


def manifest_fields(contract):
    value = validate_holdout_contract(dict(contract))
    return {
        "resampling-version": value["version"],
        "resampling-method": value["method"],
        "resampling-assignment": value["assignment"],
        "resampling-test-numerator": value["test_numerator"],
        "resampling-test-denominator": value["test_denominator"],
        "resampling-privacy-unit": value["privacy_unit"],
        "resampling-unit-canonicalization": value["unit_canonicalization"],
        "resampling-contract-sha256": value["sha256"],
    }


def contract_from_manifest(manifest):
    if not isinstance(manifest, dict):
        raise ValueError("holdout manifest must be an object")
    contract = {
        "version": manifest.get("resampling-version"),
        "method": manifest.get("resampling-method"),
        "assignment": manifest.get("resampling-assignment"),
        "test_numerator": manifest.get("resampling-test-numerator"),
        "test_denominator": manifest.get("resampling-test-denominator"),
        "privacy_unit": manifest.get("resampling-privacy-unit"),
        "unit_canonicalization": manifest.get(
            "resampling-unit-canonicalization"),
        "sha256": manifest.get("resampling-contract-sha256"),
    }
    value = validate_holdout_contract(contract)
    if manifest.get("dp-unit") != value["privacy_unit"]:
        raise ValueError("holdout contract differs from the pinned privacy unit")
    expected_canonicalization = manifest.get("patient-id-canonicalization")
    if value["privacy_unit"] == "patient":
        if expected_canonicalization != value["unit_canonicalization"]:
            raise ValueError(
                "holdout contract differs from patient canonicalization")
    elif manifest.get("patient_column") not in (None, ""):
        raise ValueError("row holdout must not pin a patient column")
    return value


def _cv_payload(folds, privacy_unit):
    k = _exact_int(folds, "cross-validation folds", 2, 10)
    unit = str(privacy_unit).lower()
    if unit not in ("row", "patient"):
        raise ValueError("cross-validation privacy unit must be row or patient")
    return {
        "assignment": _CV_ASSIGNMENT,
        "folds": k,
        "method": _CV_METHOD,
        "privacy_unit": unit,
        "unit_canonicalization": (
            "trim-utf8-v2" if unit == "patient" else "row-ordinal-v1"),
        "version": _CV_VERSION,
    }


def cross_validation_contract(folds, privacy_unit):
    payload = _cv_payload(folds, privacy_unit)
    return {**payload, "sha256": hashlib.sha256(_wire(payload)).hexdigest()}


def validate_cross_validation_contract(value):
    if (not isinstance(value, dict)
            or frozenset(value.keys()) != _CV_CONTRACT_FIELDS):
        raise ValueError(
            "cross-validation contract has an unexpected field or seed axis")
    expected = cross_validation_contract(
        value.get("folds"), value.get("privacy_unit"))
    supplied_hash = value.get("sha256")
    if (not isinstance(supplied_hash, str)
            or _SHA256_RE.fullmatch(supplied_hash) is None
            or supplied_hash != expected["sha256"]):
        raise ValueError("cross-validation contract SHA-256 is invalid")
    if value != expected:
        raise ValueError(
            "cross-validation contract differs from its canonical form")
    return expected


def cross_validation_manifest_fields(contract):
    value = validate_cross_validation_contract(dict(contract))
    return {
        "cv-version": value["version"],
        "cv-method": value["method"],
        "cv-assignment": value["assignment"],
        "cv-folds": value["folds"],
        "cv-privacy-unit": value["privacy_unit"],
        "cv-unit-canonicalization": value["unit_canonicalization"],
        "cv-contract-sha256": value["sha256"],
    }


def cross_validation_contract_from_manifest(manifest):
    if not isinstance(manifest, dict):
        raise ValueError("cross-validation manifest must be an object")
    contract = {
        "version": manifest.get("cv-version"),
        "method": manifest.get("cv-method"),
        "assignment": manifest.get("cv-assignment"),
        "folds": manifest.get("cv-folds"),
        "privacy_unit": manifest.get("cv-privacy-unit"),
        "unit_canonicalization": manifest.get("cv-unit-canonicalization"),
        "sha256": manifest.get("cv-contract-sha256"),
    }
    value = validate_cross_validation_contract(contract)
    if manifest.get("dp-unit") != value["privacy_unit"]:
        raise ValueError(
            "cross-validation contract differs from the pinned privacy unit")
    expected_canonicalization = manifest.get("patient-id-canonicalization")
    if value["privacy_unit"] == "patient":
        if expected_canonicalization != value["unit_canonicalization"]:
            raise ValueError(
                "cross-validation contract differs from patient canonicalization")
    elif manifest.get("patient_column") not in (None, ""):
        raise ValueError("row cross-validation must not pin a patient column")
    return value


def _assignment_domain_hash(contract):
    # The fraction is a threshold, not a fresh randomization axis.  Excluding
    # only its numerator makes different requested fractions nested instead of
    # giving the analyst a partition reroll; every other assignment semantic is
    # still domain-separated.
    assignment_domain = {
        key: contract[key] for key in (
            "assignment", "method", "privacy_unit", "test_denominator",
            "unit_canonicalization", "version")
    }
    return hashlib.sha256(_wire(assignment_domain)).digest()


def _unit_digest(secret, domain_hash, token):
    message = (b"dsflower/holdout-unit/v1\x00" + domain_hash
               + len(token).to_bytes(4, "big") + token)
    return hmac.new(secret, message, hashlib.sha256).digest()


def holdout_mask(contract, *, n_rows, unit_ids=None):
    value = validate_holdout_contract(dict(contract))
    rows = _exact_int(n_rows, "holdout row count", 0, (1 << 63) - 1)
    unit = value["privacy_unit"]
    if unit == "row":
        if unit_ids is not None:
            raise ValueError("row holdout does not accept patient identifiers")
        tokens = (b"row\x00" + index.to_bytes(8, "big")
                  for index in range(rows))
    else:
        if unit_ids is None:
            raise ValueError("patient holdout requires unit identifiers")
        ids = np.asarray(unit_ids)
        if ids.ndim != 1 or ids.shape[0] != rows:
            raise ValueError("holdout unit identifiers must match row count")
        from . import task
        tokens = (
            b"patient\x00" + task._canonical_patient_id(item).encode(
                "utf-8", errors="strict") for item in ids)

    threshold = (int(value["test_numerator"]) * (1 << 256)
                 // int(value["test_denominator"]))
    # Validate/open the custodial root once per partition, never once per row.
    # Patient tokens are memoized only within this call so repeated records for
    # one unit cost one HMAC without creating persistent state.
    secret = seeding._node_secret()
    domain_hash = _assignment_domain_hash(value)
    if unit == "row":
        assigned = (
            int.from_bytes(_unit_digest(secret, domain_hash, token), "big")
            < threshold for token in tokens)
    else:
        memo = {}

        def patient_assigned(token):
            if token not in memo:
                memo[token] = (
                    int.from_bytes(
                        _unit_digest(secret, domain_hash, token), "big")
                    < threshold)
            return memo[token]

        assigned = (patient_assigned(token) for token in tokens)
    return np.fromiter(assigned, dtype=np.bool_, count=rows)


def holdout_mask_from_context(context, *, n_rows, unit_ids=None):
    from . import task
    manifest = task._load_manifest(context)
    contract = contract_from_manifest(manifest)
    return holdout_mask(contract, n_rows=n_rows, unit_ids=unit_ids)


def cross_validation_folds(contract, *, n_rows, unit_ids=None):
    value = validate_cross_validation_contract(dict(contract))
    rows = _exact_int(
        n_rows, "cross-validation row count", 0, (1 << 63) - 1)
    unit = value["privacy_unit"]
    if unit == "row":
        if unit_ids is not None:
            raise ValueError(
                "row cross-validation does not accept patient identifiers")
        tokens = (b"row\x00" + index.to_bytes(8, "big")
                  for index in range(rows))
    else:
        if unit_ids is None:
            raise ValueError(
                "patient cross-validation requires unit identifiers")
        ids = np.asarray(unit_ids)
        if ids.ndim != 1 or ids.shape[0] != rows:
            raise ValueError(
                "cross-validation unit identifiers must match row count")
        from . import task
        tokens = (
            b"patient\x00" + task._canonical_patient_id(item).encode(
                "utf-8", errors="strict") for item in ids)

    assignment_domain = {
        key: value[key] for key in (
            "assignment", "method", "privacy_unit",
            "unit_canonicalization", "version")
    }
    domain_hash = hashlib.sha256(_wire(assignment_domain)).digest()
    secret = seeding._node_secret()
    k = int(value["folds"])
    memo = {}

    def assigned(token):
        if token not in memo:
            message = (b"dsflower/cv-unit/v1\x00" + domain_hash
                       + len(token).to_bytes(4, "big") + token)
            score = int.from_bytes(
                hmac.new(secret, message, hashlib.sha256).digest(), "big")
            memo[token] = (score * k // (1 << 256)) + 1
        return memo[token]

    return np.fromiter(
        (assigned(token) for token in tokens), dtype=np.int16, count=rows)


def cross_validation_folds_from_context(context, *, n_rows, unit_ids=None):
    from . import task
    manifest = task._load_manifest(context)
    contract = cross_validation_contract_from_manifest(manifest)
    return cross_validation_folds(
        contract, n_rows=n_rows, unit_ids=unit_ids)


__all__ = [
    "contract_from_manifest", "cross_validation_contract",
    "cross_validation_contract_from_manifest", "cross_validation_folds",
    "cross_validation_folds_from_context", "cross_validation_manifest_fields",
    "holdout_contract", "holdout_mask", "holdout_mask_from_context",
    "manifest_fields", "validate_cross_validation_contract",
    "validate_holdout_contract",
]
