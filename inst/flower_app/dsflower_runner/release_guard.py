"""Sticky release identity and exact-request replay guard.

The node-written manifest pins one training's privacy contract and Flower's
per-message ConfigRecord supplies its public round coordinate. A message
identifier is used only to find an in-memory reply cache; it never selects
privacy randomness or authorizes a release.  Every release coordinate is
atomically claimed in the run's private staging directory before private work
begins and mirrored into Flower's NodeState.  Once the single reply cache
advances, an older coordinate fails closed rather than recomputing a second
private release.
"""

import hashlib
import json
import math
import os
import re
import sqlite3

from flwr.common import ArrayRecord, ConfigRecord


_TOKEN_RE = re.compile(r"^run_[0-9a-f]{32}$")
_MAX_ROUNDS = 500
_MAX_ARRAYS = 256
_MAX_SERIALIZED_ARRAY_BYTES = 64 * 1024 * 1024 + _MAX_ARRAYS * 4096
_CACHE_META_KEY = "dsflower-last-release-meta"
_CACHE_ARRAYS_KEY = "dsflower-last-release"
_CLAIM_LEDGER_KEY = "dsflower-release-claim-ledger-v1"
_CLAIM_LEDGER_VERSION = "dsflower-release-claim-ledger-v1"
_CLAIM_LEDGER_FILENAME = ".dsflower-release-claim-ledger-v1.sqlite3"
_REQUEST_ID_RE = re.compile(r"^[0-9a-f]{64}$")


def _manifest_dir(context):
    manifest_dir = context.node_config.get("manifest-dir")
    if not manifest_dir:
        manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR")
    if not manifest_dir:
        raise RuntimeError("privacy guard has no manifest directory")
    return manifest_dir


def _manifest(context, manifest_dir=None):
    if manifest_dir is None:
        manifest_dir = _manifest_dir(context)
    path = os.path.join(manifest_dir, "manifest.json")
    with open(path, encoding="utf-8") as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise RuntimeError("privacy manifest must be a JSON object")
    return value


def _exact_int(value, label, lower, upper):
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError("%s must be an exact integer" % label)
    if value < lower or value > upper:
        raise RuntimeError("%s is outside [%d, %d]" % (label, lower, upper))
    return value


def _finite_float(value, label, lower, upper):
    if isinstance(value, bool):
        raise RuntimeError("%s must be a finite number" % label)
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError("%s must be a finite number" % label) from exc
    if not math.isfinite(number) or number <= lower or number > upper:
        raise RuntimeError("%s is outside its valid range" % label)
    return number


def _fixed_manifest(context):
    manifest_dir = _manifest_dir(context)
    manifest = _manifest(context, manifest_dir)
    token = str(manifest.get("run_token", ""))
    if not _TOKEN_RE.fullmatch(token):
        raise RuntimeError("invalid privacy run token in manifest")
    policy_hash = manifest.get("privacy-policy-sha256")
    if (not isinstance(policy_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", policy_hash) is None):
        raise RuntimeError("manifest has no canonical stateless privacy policy")
    if manifest.get("privacy-adjacency") != "replace_one":
        raise RuntimeError("manifest must pin replace-one privacy adjacency")

    num_rounds = _exact_int(
        manifest.get("num-server-rounds"),
        "manifest num-server-rounds", 1, _MAX_ROUNDS)

    epsilon = _finite_float(
        manifest.get("privacy-epsilon"), "manifest privacy-epsilon",
        0.0, 10.0)
    delta = _finite_float(
        manifest.get("privacy-delta"), "manifest privacy-delta",
        0.0, 1.0e-3)
    holdout = manifest.get("resampling-contract-sha256") is not None
    cross_validation = manifest.get("cv-contract-sha256") is not None
    if holdout and cross_validation:
        raise RuntimeError("manifest cannot combine holdout and cross-validation")
    budgets = {
        "train": (epsilon, delta),
    }
    if holdout:
        try:
            from . import resampling
        except ImportError:  # direct module tests
            import resampling
        try:
            resampling.contract_from_manifest(manifest)
        except Exception as exc:
            raise RuntimeError("manifest has no valid holdout contract") from exc
        expected = {
            "privacy-training-epsilon": epsilon * 0.8,
            "privacy-training-delta": delta * 0.8,
            "privacy-holdout-epsilon": epsilon - epsilon * 0.8,
            "privacy-holdout-delta": delta - delta * 0.8,
        }
        actual = {}
        for key, value in expected.items():
            actual[key] = _finite_float(
                manifest.get(key), "manifest %s" % key, 0.0,
                10.0 if key.endswith("epsilon") else 1.0e-3)
            if not math.isclose(actual[key], value, rel_tol=1.0e-15,
                                abs_tol=0.0):
                raise RuntimeError(
                    "manifest holdout budget differs from its fixed job allocation")
        budgets["train"] = (
            actual["privacy-training-epsilon"],
            actual["privacy-training-delta"])
        budgets["holdout-evaluate"] = (
            actual["privacy-holdout-epsilon"],
            actual["privacy-holdout-delta"])
    cv_folds = 0
    if cross_validation:
        try:
            from . import resampling
        except ImportError:  # direct module tests
            import resampling
        try:
            contract = resampling.cross_validation_contract_from_manifest(
                manifest)
        except Exception as exc:
            raise RuntimeError(
                "manifest has no valid cross-validation contract") from exc
        cv_folds = int(contract["folds"])
        expected = {
            "privacy-cv-training-epsilon": epsilon * 0.8,
            "privacy-cv-training-delta": delta * 0.8,
            "privacy-cv-fold-epsilon": epsilon * 0.8 / cv_folds,
            "privacy-cv-fold-delta": delta * 0.8 / cv_folds,
            "privacy-cv-oof-epsilon": epsilon - epsilon * 0.8,
            "privacy-cv-oof-delta": delta - delta * 0.8,
        }
        actual = {}
        for key, value in expected.items():
            actual[key] = _finite_float(
                manifest.get(key), "manifest %s" % key, 0.0,
                10.0 if key.endswith("epsilon") else 1.0e-3)
            if not math.isclose(actual[key], value, rel_tol=1.0e-15,
                                abs_tol=0.0):
                raise RuntimeError(
                    "manifest cross-validation budget differs from its fixed "
                    "job allocation")
        budgets = {
            "cv-train": (
                actual["privacy-cv-fold-epsilon"],
                actual["privacy-cv-fold-delta"]),
            "cv-accumulate": (0.0, 0.0),
            "cv-release": (
                actual["privacy-cv-oof-epsilon"],
                actual["privacy-cv-oof-delta"]),
            "cv-abort": (0.0, 0.0),
        }
    max_claims = (cv_folds * (num_rounds + 1) + 2
                  if cross_validation else
                  num_rounds + (1 if holdout else 0))
    return {
        "num_rounds": num_rounds,
        "policy_hash": policy_hash,
        "run_fingerprint": hashlib.sha256(
            b"dsflower/release-ledger/run/v1\x00" + token.encode("ascii")
        ).hexdigest(),
        "budgets": budgets,
        "holdout": holdout,
        "cross_validation": cross_validation,
        "cv_folds": cv_folds,
        "max_claims": max_claims,
        "ledger_path": os.path.join(manifest_dir, _CLAIM_LEDGER_FILENAME),
    }


def _message_config(msg):
    content = getattr(msg, "content", None)
    try:
        config = content["config"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError("train message is missing its ConfigRecord") from exc
    if not isinstance(config, ConfigRecord):
        raise RuntimeError("train message config must be a ConfigRecord")
    return config


def _server_round(msg, num_rounds):
    config = _message_config(msg)
    try:
        value = config["server-round"]
    except KeyError as exc:
        raise RuntimeError("train ConfigRecord is missing server-round") from exc
    return _exact_int(value, "server-round", 1, num_rounds)


def _operation(msg, fixed):
    config = _message_config(msg)
    value = config.get("dsflower-operation", "train")
    if value == "holdout-evaluate" and not fixed["holdout"]:
        raise RuntimeError("holdout evaluation requires a pinned holdout contract")
    if not isinstance(value, str) or value not in fixed["budgets"]:
        raise RuntimeError("train message has an unsupported release operation")
    return value


def _fold_coordinate(msg, fixed, operation, round_index):
    config = _message_config(msg)
    supplied = config.get("dsflower-fold")
    if not fixed["cross_validation"]:
        if supplied is not None:
            raise RuntimeError(
                "fold coordinate requires a cross-validation contract")
        if (operation == "holdout-evaluate"
                and round_index != fixed["num_rounds"]):
            raise RuntimeError(
                "holdout evaluation requires the final training round")
        return 0
    folds = int(fixed["cv_folds"])
    fold = _exact_int(
        supplied, "cross-validation fold", 1, folds + 1)
    if operation in ("cv-train", "cv-accumulate") and fold > folds:
        raise RuntimeError("cross-validation fold is outside the training folds")
    if operation == "cv-accumulate" and round_index != fixed["num_rounds"]:
        raise RuntimeError(
            "cross-validation accumulation requires the final training round")
    if operation == "cv-release":
        if fold != folds + 1:
            raise RuntimeError("cross-validation final release fold is invalid")
        if round_index != fixed["num_rounds"]:
            raise RuntimeError(
                "cross-validation release requires the final training round")
    if operation == "cv-abort":
        if fold != folds + 1 or round_index != fixed["num_rounds"]:
            raise RuntimeError("cross-validation abort coordinate is invalid")
    return fold


def _message_id(msg):
    """Return a bounded cache hint; an absent identifier simply disables it."""
    metadata = getattr(msg, "metadata", None)
    value = str(getattr(metadata, "message_id", "") or "")
    if not value:
        group = str(getattr(metadata, "group_id", "") or "")
        value = ("group:" + group) if group else ""
    if len(value) > 512 or "\x00" in value:
        return ""
    return value


def _frame(digest, value):
    raw = value if isinstance(value, bytes) else str(value).encode("utf-8")
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def _request_id(msg, operation, fold, round_index):
    """Hash the exact bounded public ArrayRecord and its round coordinate."""
    content = getattr(msg, "content", None)
    try:
        arrays = content["arrays"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError("train message is missing its ArrayRecord") from exc
    if not isinstance(arrays, ArrayRecord):
        raise RuntimeError("train message arrays must be an ArrayRecord")
    entries = list(arrays.items())
    if not 1 <= len(entries) <= _MAX_ARRAYS:
        raise RuntimeError("train ArrayRecord exceeds the public array-count cap")

    digest = hashlib.sha256(b"dsflower/public-request/v1\x00")
    _frame(digest, operation)
    digest.update(int(fold).to_bytes(8, "big"))
    digest.update(int(round_index).to_bytes(8, "big"))
    total = 0
    for key, item in entries:
        data = getattr(item, "data", None)
        if (getattr(item, "stype", None) != "numpy.ndarray"
                or not isinstance(data, bytes)):
            raise RuntimeError("train ArrayRecord needs NumPy tensor encoding")
        total += len(data)
        if total > _MAX_SERIALIZED_ARRAY_BYTES:
            raise RuntimeError("train ArrayRecord exceeds the public model-size cap")
        shape = getattr(item, "shape", None)
        if not isinstance(shape, (list, tuple)):
            raise RuntimeError("train ArrayRecord has invalid shape metadata")
        _frame(digest, key)
        _frame(digest, getattr(item, "dtype", ""))
        _frame(digest, ",".join(str(value) for value in shape))
        _frame(digest, data)
    return digest.hexdigest()


def _cached_status(context, message_id, operation, fold, round_index,
                   request_id):
    state = getattr(context, "state", None)
    if state is None or not hasattr(state, "get"):
        return "new"
    meta = state.get(_CACHE_META_KEY)
    if meta is None or not hasattr(meta, "get"):
        return "new"

    cached_message = str(meta.get("message-id", "") or "")
    cached_request = str(meta.get("request-id", "") or "")
    cached_operation = str(meta.get("operation", "train") or "")
    cached_fold = int(meta.get("fold", 0) or 0)
    cached_round = meta.get("release-index")
    exact = (cached_request == request_id
             and cached_operation == operation
             and cached_fold == fold
             and cached_round == round_index)
    if exact and state.get(_CACHE_ARRAYS_KEY) is not None:
        return "replay"

    # A message-id collision or a second payload for the same provisional round
    # must fail before private data is read.  Message ids do not establish
    # identity; they only make these accidental/malicious cache collisions visible.
    if ((message_id and cached_message == message_id)
            or (cached_request and cached_operation == operation
                and cached_fold == fold
                and cached_round == round_index)):
        if not exact:
            raise RuntimeError("cached round identity does not match request payload")
    return "new"


def _claim_key(operation, fold, round_index):
    return "claim:%s:%d:%d" % (operation, int(fold), int(round_index))


def _claim_ledger(context, fixed):
    """Load and validate the bounded NodeState ledger projection."""
    state = getattr(context, "state", None)
    if (state is None or not hasattr(state, "get")
            or not hasattr(state, "__setitem__")):
        raise RuntimeError("privacy guard has no persistent per-run state")
    binding = {
        "version": _CLAIM_LEDGER_VERSION,
        "run-fingerprint": fixed["run_fingerprint"],
        "policy-hash": fixed["policy_hash"],
    }
    ledger = state.get(_CLAIM_LEDGER_KEY)
    if ledger is None:
        return state, ConfigRecord(binding)
    if not isinstance(ledger, ConfigRecord):
        raise RuntimeError("privacy release claim ledger is invalid")
    if any(ledger.get(key) != value for key, value in binding.items()):
        raise RuntimeError("privacy release claim ledger binding changed")
    claims = {key: value for key, value in ledger.items()
              if key not in binding}
    if (len(claims) > fixed["max_claims"]
            or any(not key.startswith("claim:")
                   or not isinstance(value, str)
                   or _REQUEST_ID_RE.fullmatch(value) is None
                   for key, value in claims.items())):
        raise RuntimeError("privacy release claim ledger is invalid")
    return state, ledger


def _durable_claim(fixed, ledger, key, request_id):
    """Atomically claim a coordinate across ClientApp processes and restarts."""
    binding = {
        "version": _CLAIM_LEDGER_VERSION,
        "run-fingerprint": fixed["run_fingerprint"],
        "policy-hash": fixed["policy_hash"],
    }
    connection = None
    try:
        connection = sqlite3.connect(
            fixed["ledger_path"], timeout=30.0, isolation_level=None)
        if os.name != "nt":
            os.chmod(fixed["ledger_path"], 0o600)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS ledger ("
            "ledger_key TEXT PRIMARY KEY, ledger_value TEXT NOT NULL"
            ") WITHOUT ROWID")
        stored = dict(connection.execute(
            "SELECT ledger_key, ledger_value FROM ledger"))
        if not stored:
            connection.executemany(
                "INSERT INTO ledger (ledger_key, ledger_value) VALUES (?, ?)",
                binding.items())
            stored.update(binding)
        if any(stored.get(name) != value
               for name, value in binding.items()):
            raise RuntimeError("privacy release claim ledger binding changed")

        claims = {name: value for name, value in stored.items()
                  if name not in binding}
        if (len(claims) > fixed["max_claims"]
                or any(not name.startswith("claim:")
                       or not isinstance(value, str)
                       or _REQUEST_ID_RE.fullmatch(value) is None
                       for name, value in claims.items())):
            raise RuntimeError("privacy release claim ledger is invalid")

        for name, value in ledger.items():
            if name in binding:
                continue
            durable_value = stored.get(name)
            if durable_value is not None and durable_value != value:
                raise RuntimeError("privacy release claim ledger diverged")
            if durable_value is None:
                connection.execute(
                    "INSERT INTO ledger (ledger_key, ledger_value) VALUES (?, ?)",
                    (name, value))
                stored[name] = value

        claimed_request = stored.get(key)
        claim_count = len(stored) - len(binding)
        if claim_count > fixed["max_claims"]:
            raise RuntimeError("privacy release claim ledger is invalid")
        if claimed_request is None:
            if claim_count >= fixed["max_claims"]:
                raise RuntimeError("privacy release claim ledger is exhausted")
            connection.execute(
                "INSERT INTO ledger (ledger_key, ledger_value) VALUES (?, ?)",
                (key, request_id))
            stored[key] = request_id
        connection.commit()
        return ConfigRecord(stored), claimed_request
    except RuntimeError:
        if connection is not None:
            connection.rollback()
        raise
    except (OSError, sqlite3.Error) as exc:
        if connection is not None:
            connection.rollback()
        raise RuntimeError(
            "privacy release coordinate could not be claimed") from exc
    finally:
        if connection is not None:
            connection.close()


def _sticky_status(context, fixed, message_id, operation, fold, round_index,
                   request_id):
    """Claim one coordinate before private work or replay its exact last reply."""
    state, ledger = _claim_ledger(context, fixed)
    key = _claim_key(operation, fold, round_index)
    cache_status = _cached_status(
        context, message_id, operation, fold, round_index, request_id)
    updated, claimed_request = _durable_claim(
        fixed, ledger, key, request_id)
    try:
        state[_CLAIM_LEDGER_KEY] = updated
    except Exception as exc:
        raise RuntimeError("privacy release coordinate could not be claimed") from exc
    persisted = state.get(_CLAIM_LEDGER_KEY)
    if (not isinstance(persisted, ConfigRecord)
            or persisted.get(key) != updated.get(key)):
        raise RuntimeError("privacy release coordinate could not be claimed")

    if claimed_request is not None:
        if claimed_request != request_id:
            raise RuntimeError(
                "claimed release coordinate does not match request payload")
        if cache_status == "replay":
            return "replay"
        raise RuntimeError(
            "release coordinate was already claimed and its exact reply is unavailable")
    return cache_status


def claim_release(context, msg):
    """Reserve one sticky round claim, optionally replaying the exact last reply."""
    fixed = _fixed_manifest(context)
    num_rounds = fixed["num_rounds"]
    operation = _operation(msg, fixed)
    epsilon, delta = fixed["budgets"][operation]
    round_index = _server_round(msg, num_rounds)
    fold = _fold_coordinate(msg, fixed, operation, round_index)
    request_id = _request_id(msg, operation, fold, round_index)
    message_id = _message_id(msg)
    status = _sticky_status(
        context, fixed, message_id, operation, fold, round_index, request_id)
    return {
        "status": status,
        "operation": operation,
        "fold": fold,
        "release_index": round_index,
        "epsilon": epsilon,
        "delta": delta,
        "num_rounds": num_rounds,
        "message_id": message_id,
        "request_id": request_id,
        "policy_hash": fixed["policy_hash"],
    }
