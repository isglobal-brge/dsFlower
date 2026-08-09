"""Dedicated trusted ClientApp for one complete native DP-XGBoost training."""

import os
import re
import sys


_FORBIDDEN_MODULES = (
    "torch", "opacus", "dsflower_runner.client_app",
    "dsflower_runner.egress_child", "dsflower_runner.tier2_lib",
)


def _assert_native_process_isolated():
    """Fail if the dedicated native process has admitted a wider runner."""
    if os.environ.get("DSFLOWER_PINNED_APP_DIR"):
        raise RuntimeError("native-tree process cannot admit uploaded code")
    for name in tuple(sys.modules):
        if any(name == forbidden or name.startswith(forbidden + ".")
               for forbidden in _FORBIDDEN_MODULES):
            raise RuntimeError("native-tree process is not isolated")


# Production SuperNodes always carry the node-owned manifest directory.  Keep
# ordinary test discovery importable, while enforcing isolation before any
# third-party/native module enters a real ClientApp process.
if os.environ.get("DSFLOWER_MANIFEST_DIR"):
    _assert_native_process_isolated()


import numpy as np
from flwr.clientapp import ClientApp
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from . import native_tree_request, task, xgboost_adapter, xgboost_bundle


app = ClientApp()
_BUNDLE_ENV = "DSFLOWER_XGBOOST_BUNDLE_ROOT"
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_UNAVAILABLE = b'{"status":"unavailable","version":1}'
_MESSAGE_CONFIG_FIELDS = frozenset((
    "native-tree-request-b64", "native-tree-request-sha256", "server-round",
))


def _load_node_bundle_once():
    root = os.environ.get(_BUNDLE_ENV, "")
    if not root or not os.path.isabs(root):
        return None
    try:
        return xgboost_bundle.load_verified_xgboost_bundle(root)
    except Exception:
        return None


_NATIVE_BUNDLE = _load_node_bundle_once()


def _reply(msg, artifact, available):
    encoded = np.frombuffer(bytes(artifact), dtype=np.uint8).copy()
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[encoded]),
        "metrics": MetricRecord({
            "available": int(bool(available)),
            "num-examples": 1,
        }),
    }), reply_to=msg)


def _unavailable_reply(msg):
    return _reply(msg, _UNAVAILABLE, False)


def _exact_message_config(msg):
    if frozenset(msg.content.keys()) != frozenset(("config",)):
        raise ValueError("native-tree request message has an invalid shape")
    config = msg.content["config"]
    if not isinstance(config, ConfigRecord) or \
            frozenset(config.keys()) != _MESSAGE_CONFIG_FIELDS or \
            type(config["server-round"]) is not int or \
            config["server-round"] != 1:
        raise ValueError("native-tree request message has an invalid config")
    return config


def _validate_public_manifest(request, manifest):
    schema = request["public_schema"]
    if manifest.get("data_type") != "tabular" or \
            manifest.get("dp-track") != "native_tree" or \
            type(manifest.get("num-server-rounds")) is not int or \
            manifest["num-server-rounds"] != 1 or \
            manifest.get("target-preencoded") is not True or \
            manifest.get("user-module") not in (None, "") or \
            manifest.get("feature_columns") != schema["features"] or \
            manifest.get("target_column") != schema["target"]["name"]:
        raise ValueError("node manifest differs from the native-tree request")
    bounds = manifest.get("feature-bounds")
    if not isinstance(bounds, dict) or \
            bounds.get("lower") != schema["lower"] or \
            bounds.get("upper") != schema["upper"]:
        raise ValueError("node feature bounds differ from the request")

    target = schema["target"]
    if request["task"] == "binary":
        levels = manifest.get("target-levels")
        level_types = {
            "character": "string", "logical": "boolean", "numeric": "number",
        }
        expected_type = (level_types.get(levels.get("type"))
                         if isinstance(levels, dict) else None)
        expected_values = levels.get("values") if isinstance(levels, dict) else None
        request_levels = target.get("levels")
        if manifest.get("task-type") != "classification" or \
                not isinstance(expected_values, list) or \
                not isinstance(request_levels, list) or \
                [item.get("type") for item in request_levels] != \
                [expected_type] * len(request_levels) or \
                [item.get("value") for item in request_levels] != expected_values:
            raise ValueError("node target levels differ from the request")
    else:
        bounds = manifest.get("target-bounds")
        if manifest.get("task-type") != "regression" or \
                not isinstance(bounds, dict) or \
                bounds.get("lower") != target.get("lower") or \
                bounds.get("upper") != target.get("upper"):
            raise ValueError("node target bounds differ from the request")


def _node_privacy(manifest):
    policy_hash = manifest.get("privacy-policy-sha256")
    if not isinstance(policy_hash, str) or not _HEX_64_RE.fullmatch(policy_hash):
        raise ValueError("node privacy policy is not pinned")
    if manifest.get("privacy-adjacency") != "replace_one" or \
            manifest.get("dp-unit") not in ("row", "patient") or \
            manifest.get("patient-id-canonicalization") != "trim-utf8-v2":
        raise ValueError("node privacy unit is not pinned")
    values = []
    for key in ("privacy-epsilon", "privacy-delta",
                "privacy-clipping_norm"):
        value = manifest.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("node privacy parameters are invalid")
        values.append(float(value))
    epsilon, delta, gradient_clip = values
    return {
        "epsilon": epsilon,
        "delta": delta,
        "unit": manifest["dp-unit"],
        "unit_canonicalization": "trim-utf8-v2",
        "gradient_clip": gradient_clip,
    }


def _pinned_request(msg, context):
    message_config = _exact_message_config(msg)
    manifest = task._load_manifest(context)
    run_config = dict(context.run_config)
    request_b64 = manifest.get("native-tree-request-b64")
    request_sha256 = manifest.get("native-tree-request-sha256")
    if run_config.get("dp-track") != "native_tree" or \
            type(run_config.get("num-server-rounds")) is not int or \
            run_config["num-server-rounds"] != 1 or \
            run_config.get("native-tree-request-b64") != request_b64 or \
            run_config.get("native-tree-request-sha256") != request_sha256 or \
            message_config["native-tree-request-b64"] != request_b64 or \
            message_config["native-tree-request-sha256"] != request_sha256:
        raise ValueError("Flower request differs from the node manifest pin")
    request = native_tree_request.parse_request_wire(
        request_b64, request_sha256)
    _validate_public_manifest(request, manifest)
    privacy = _node_privacy(manifest)
    # Validate the complete public/accounting profile before opening private data.
    schema_hash = request["public_schema"]["sha256"]
    preflight = native_tree_request.backend_manifest(
        request, snapshot_hash=schema_hash, cohort_hash=schema_hash,
        **{key: privacy[key] for key in (
            "epsilon", "delta", "unit", "unit_canonicalization",
            "gradient_clip")})
    xgboost_adapter.canonical_xgboost_profile(preflight)
    return request, manifest, privacy


@app.train()
def train(msg: Message, context: Context) -> Message:
    try:
        request, node_manifest, privacy = _pinned_request(msg, context)
        if not xgboost_bundle.is_verified_bundle(_NATIVE_BUNDLE):
            return _unavailable_reply(msg)
        features, target, unit_ids = task.load_native_tree_data(
            context, manifest=node_manifest)
        # The generic ABI requires opaque scope fields, but the native adapter
        # deliberately excludes them from sticky randomness and binds the
        # canonical effective tensors itself.  Reuse the public schema pin here:
        # no redundant O(N) pass and no second private identity channel.
        scope_hash = request["public_schema"]["sha256"]
        manifest = native_tree_request.backend_manifest(
            request, snapshot_hash=scope_hash, cohort_hash=scope_hash,
            **{key: privacy[key] for key in (
                "epsilon", "delta", "unit", "unit_canonicalization",
                "gradient_clip")})
        prepared = xgboost_adapter.prepare_xgboost_training(
            manifest, features, target, native_bundle=_NATIVE_BUNDLE,
            unit_ids=unit_ids)
        native_artifact = xgboost_adapter.train_xgboost_native(prepared)
        artifact, _digest = xgboost_adapter.sanitize_xgboost_artifact(
            manifest, native_artifact)
        return _reply(msg, artifact, True)
    except Exception:
        # Never expose paths, native diagnostics, private counts or exceptions.
        return _unavailable_reply(msg)


__all__ = ["app", "train"]
