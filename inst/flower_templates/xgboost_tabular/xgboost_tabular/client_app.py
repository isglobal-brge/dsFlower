"""Flower ClientApp for Federated XGBoost."""

import json
import os

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import xgboost as xgb

from .task import load_data, load_privacy_config
from .privacy_utils import bucket_count


class FlowerClient(NumPyClient):
    def __init__(self, X_train, y_train, privacy_config,
                 max_depth=6, eta=0.3, objective="binary:logistic",
                 local_rounds=10):
        self.dtrain = xgb.DMatrix(X_train, label=y_train)
        self.X_train = X_train
        self.y_train = y_train
        self.privacy_config = privacy_config
        self.params = {
            "max_depth": max_depth,
            "eta": eta,
            "objective": objective,
            "verbosity": 0,
        }
        self.local_rounds = local_rounds
        self.model = None

    def get_parameters(self, config):
        if self.model is None:
            return [np.array([], dtype=np.uint8)]
        model_bytes = self.model.save_raw(raw_format="json")
        return [np.frombuffer(model_bytes, dtype=np.uint8)]

    def set_parameters(self, parameters):
        if len(parameters) == 0 or len(parameters[0]) == 0:
            self.model = None
            return
        model_bytes = parameters[0].tobytes()
        self.model = xgb.Booster(params=self.params)
        self.model.load_model(bytearray(model_bytes))

    def fit(self, parameters, config):
        self.set_parameters(parameters)

        if self.model is None:
            self.model = xgb.train(
                self.params, self.dtrain,
                num_boost_round=self.local_rounds,
            )
        else:
            self.model = xgb.train(
                self.params, self.dtrain,
                num_boost_round=self.local_rounds,
                xgb_model=self.model,
            )

        new_weights = self.get_parameters(config)

        n_examples = len(self.y_train)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return new_weights, n_examples, {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)

        if not self.privacy_config.get("allow_per_node_metrics", True):
            n_examples = len(self.y_train)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n_examples = bucket_count(n_examples)
            return 0.0, n_examples, {}

        if self.model is None:
            return float("inf"), len(self.y_train), {}

        preds = self.model.predict(self.dtrain)
        if "binary" in self.params.get("objective", ""):
            labels = (preds > 0.5).astype(int)
            accuracy = float(np.mean(labels == self.y_train))
            loss = float(-np.mean(
                self.y_train * np.log(np.clip(preds, 1e-7, 1)) +
                (1 - self.y_train) * np.log(np.clip(1 - preds, 1e-7, 1))
            ))
        else:
            accuracy = 0.0
            loss = float(np.mean((preds - self.y_train) ** 2))

        n_examples = len(self.y_train)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return loss, n_examples, {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)
    privacy_config = load_privacy_config(context)

    max_depth = int(cfg.get("max_depth", 6))
    eta = float(cfg.get("eta", 0.3))
    objective = cfg.get("objective", "binary:logistic")
    local_rounds = int(cfg.get("local_rounds", 10))

    return FlowerClient(
        X, y, privacy_config,
        max_depth=max_depth,
        eta=eta,
        objective=objective,
        local_rounds=local_rounds,
    )


def _needs_secagg():
    manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
    if not manifest_dir:
        return False
    try:
        with open(os.path.join(manifest_dir, "manifest.json")) as f:
            return json.load(f).get("require_secure_aggregation", False) is True
    except (OSError, json.JSONDecodeError, KeyError):
        return False


if _needs_secagg():
    from flwr.client.mod import secaggplus_mod
    app = ClientApp(client_fn=client_fn, mods=[secaggplus_mod])
else:
    app = ClientApp(client_fn=client_fn)
