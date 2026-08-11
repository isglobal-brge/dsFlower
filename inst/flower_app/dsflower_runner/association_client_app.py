"""Dedicated trusted ClientApp for one private binary association release."""

import math
import os
import re
import sys


_FORBIDDEN_MODULES = (
    "torch", "opacus", "dsflower_runner.client_app",
    "dsflower_runner.server_app", "dsflower_runner.egress_child",
    "dsflower_runner.association_server_app",
    "dsflower_runner.tier2_lib", "dsflower_runner.native_tree_client_app",
    "dsflower_runner.native_tree_server_app",
    "dsflower_runner.native_tree_validation_client_app",
    "dsflower_runner.native_tree_validation_server_app",
    "dsflower_runner.xgboost_adapter", "dsflower_runner.xgboost_bundle",
    "dsflower_runner.xgboost_native",
)


def _assert_association_process_isolated():
    """Fail if the dependency-light association process admitted wider code."""
    if os.environ.get("DSFLOWER_PINNED_APP_DIR"):
        raise RuntimeError("association cannot admit uploaded code")
    for name in tuple(sys.modules):
        if any(name == forbidden or name.startswith(forbidden + ".")
               for forbidden in _FORBIDDEN_MODULES):
            raise RuntimeError("association process is not isolated")


if os.environ.get("DSFLOWER_MANIFEST_DIR"):
    _assert_association_process_isolated()


import numpy as np
from flwr.clientapp import ClientApp
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from . import epi_association, task


app = ClientApp()
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_PIN_FIELDS = frozenset((
    "association-contract", "association-contract-sha256",
    "association-job-sha256", "association-n-nodes",
    "association-privacy-unit", "association-unit-semantics",
))
_MESSAGE_CONFIG_FIELDS = _PIN_FIELDS | frozenset(("server-round",))


def _reply(msg, vector, sigma, available):
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[
            np.asarray(vector, dtype=np.float64).reshape(9)]),
        "metrics": MetricRecord({
            "available": int(bool(available)),
            "noise-sd": float(sigma),
            "num-examples": 1,
        }),
    }), reply_to=msg)


def _unavailable_reply(msg):
    return _reply(msg, np.zeros(9, dtype=np.float64), 0.0, False)


def _exact_message_config(msg):
    if frozenset(msg.content.keys()) != frozenset(("config",)):
        raise ValueError("association request message has an invalid shape")
    config = msg.content["config"]
    if not isinstance(config, ConfigRecord) or \
            frozenset(config.keys()) != _MESSAGE_CONFIG_FIELDS or \
            type(config["server-round"]) is not int or \
            config["server-round"] != 1:
        raise ValueError("association request message has an invalid config")
    return config


def _validate_public_contract(manifest, config):
    if manifest.get("association-contract") != \
            epi_association.ASSOCIATION_CONTRACT:
        raise ValueError("association contract literal is invalid")
    for key in ("association-contract-sha256", "association-job-sha256"):
        if not isinstance(manifest.get(key), str) or \
                not _HEX_64_RE.fullmatch(manifest[key]):
            raise ValueError("association digest pin is invalid")
    nodes = manifest.get("association-n-nodes")
    if type(nodes) is not int or not 1 <= nodes <= 65_536:
        raise ValueError("association node count is invalid")
    unit = manifest.get("association-privacy-unit")
    layout = epi_association.association_layout(unit)
    if manifest.get("association-unit-semantics") != \
            layout["unit_semantics"] or manifest.get("dp-unit") != unit:
        raise ValueError("association privacy unit is invalid")

    target = manifest.get("target_column")
    features = manifest.get("feature_columns")
    patient = manifest.get("patient_column")
    if manifest.get("data_type") != "tabular" or \
            manifest.get("dp-track") != "association" or \
            type(manifest.get("num-server-rounds")) is not int or \
            manifest["num-server-rounds"] != 1 or \
            manifest.get("target-preencoded") is not True or \
            manifest.get("association-preencoded") is not True or \
            manifest.get("user-module") not in (None, "") or \
            not isinstance(target, str) or not target or \
            not isinstance(features, list) or len(features) != 1 or \
            not isinstance(features[0], str) or not features[0] or \
            features[0] == target or patient in (target, features[0]):
        raise ValueError("node manifest differs from association pins")
    if unit == "row":
        if patient not in (None, ""):
            raise ValueError("row association cannot pin a patient column")
    elif not isinstance(patient, str) or not patient or \
            manifest.get("patient-id-canonicalization") != "trim-utf8-v2":
        raise ValueError("patient association has invalid unit identifiers")
    if config["association-n-nodes"] != nodes:
        raise ValueError("association message node count is invalid")


def _pinned_contract(msg, context):
    message_config = _exact_message_config(msg)
    manifest = task._load_manifest(context)
    raw_config = dict(context.run_config)
    if raw_config.get("dp-track") != "association" or \
            type(raw_config.get("num-server-rounds")) is not int or \
            raw_config["num-server-rounds"] != 1:
        raise ValueError("Flower association config is not one exact release")
    run_config = task.load_pinned_run_config(context)
    for key in _PIN_FIELDS:
        if key not in manifest or key not in run_config or \
                type(run_config[key]) is not type(manifest[key]) or \
                run_config[key] != manifest[key] or \
                type(message_config[key]) is not type(manifest[key]) or \
                message_config[key] != manifest[key]:
            raise ValueError("Flower association config differs from manifest pin")
    nodes = manifest.get("association-n-nodes")
    if type(raw_config.get("min-train-nodes")) is not int or \
            raw_config["min-train-nodes"] != nodes or \
            not 1 <= raw_config["min-train-nodes"] <= 65_536:
        raise ValueError("Flower association roster differs from manifest pin")
    _validate_public_contract(manifest, message_config)
    return manifest, run_config


@app.train()
def train(msg: Message, context: Context) -> Message:
    try:
        node_manifest, cfg = _pinned_contract(msg, context)
        privacy = task.load_privacy_config(context)
        outcome, exposure, unit_ids = task.load_association_data(
            context, manifest=node_manifest)
        sufficient = epi_association.association_sufficient_vector(
            outcome, exposure, outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit=cfg["association-privacy-unit"], unit_ids=unit_ids)
        released, sigma = epi_association.private_association_vector(
            sufficient, privacy_unit=cfg["association-privacy-unit"],
            epsilon=privacy["epsilon"], delta=privacy["delta"])
        if not math.isfinite(sigma) or sigma <= 0.0:
            raise RuntimeError("association noise scale is invalid")
        return _reply(msg, released, sigma, True)
    except Exception:
        # Never expose paths, parser diagnostics, private counts or exceptions.
        return _unavailable_reply(msg)


__all__ = ["app", "train"]
