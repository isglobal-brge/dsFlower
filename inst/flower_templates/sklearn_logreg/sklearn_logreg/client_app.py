"""Flower ClientApp for Federated Logistic Regression."""

import json
import os

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score

from .task import load_data, load_privacy_config
from .privacy_utils import dp_sgd_linear, bucket_count


def _resolve_classes(y, n_classes=None):
    """Resolve the class vector used by all clients.

    Binary clinical tasks can yield one-class shards after stratified splits.
    Scikit-learn still needs to see both labels when fitting a classifier, so
    the global class set is inferred from the declared number of classes when
    available and otherwise falls back to the finite labels observed locally.
    """
    y = np.asarray(y)
    finite = np.isfinite(y)
    labels = np.unique(y[finite])
    if n_classes is not None and int(n_classes) > 1:
        return np.arange(int(n_classes), dtype=y.dtype)
    if labels.size == 1 and labels[0] in (0, 1):
        return np.array([0, 1], dtype=y.dtype)
    return labels


def _fit_with_class_anchors(model, X, y, classes):
    """Fit a sklearn classifier while preserving global class shape."""
    y = np.asarray(y)
    finite = np.isfinite(y)
    X_fit = X[finite]
    y_fit = y[finite]
    if y_fit.size == 0:
        raise ValueError("No finite target values available for sklearn_logreg")

    sample_weight = np.ones(y_fit.shape[0], dtype=float)
    observed = set(np.unique(y_fit).tolist())
    missing = [c for c in classes if c not in observed]
    if missing:
        anchor = np.mean(X_fit, axis=0, keepdims=True)
        X_fit = np.vstack([X_fit, np.repeat(anchor, len(missing), axis=0)])
        y_fit = np.concatenate([y_fit, np.asarray(missing, dtype=y_fit.dtype)])
        sample_weight = np.concatenate([sample_weight, np.zeros(len(missing))])

    model.fit(X_fit, y_fit, sample_weight=sample_weight)


class FlowerClient(NumPyClient):
    def __init__(self, X, y, privacy_config, penalty="l2", C=1.0, max_iter=100,
                 n_classes=None):
        self.X = X
        self.y = y
        self.privacy_config = privacy_config
        self.classes = _resolve_classes(y, n_classes)
        self.model = LogisticRegression(
            penalty=penalty, C=C, max_iter=max_iter, warm_start=True
        )
        _fit_with_class_anchors(self.model, X, y, self.classes)

    def get_parameters(self, config):
        return [self.model.coef_, self.model.intercept_]

    def set_parameters(self, parameters):
        # Weights may arrive fp16-compressed; sklearn fits in fp64.
        self.model.coef_ = np.asarray(parameters[0], dtype=np.float64)
        self.model.intercept_ = np.asarray(parameters[1], dtype=np.float64)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        pc = self.privacy_config
        multiclass = len(self.classes) > 2
        n_outputs = len(self.classes) if multiclass else 1

        # Encode labels: 0..K-1 (multiclass) or 0/1 (binary; positive = last class).
        if multiclass:
            idx = {c: i for i, c in enumerate(self.classes)}
            y_enc = np.array([idx.get(v, 0) for v in self.y], dtype=np.int64)
        else:
            y_enc = (np.asarray(self.y) == self.classes[-1]).astype(np.float32)

        # Formal per-example DP-SGD (Opacus) via a torch linear surrogate,
        # warm-started from the global model. Replaces sklearn's non-DP fit so
        # this template gets real (epsilon, delta)-DP + the distributed sqrt(N)
        # noise split, identical in spirit to the PyTorch templates.
        coef, intercept = dp_sgd_linear(
            np.asarray(self.X, dtype=np.float32), y_enc, n_outputs,
            epsilon=pc["epsilon"], delta=pc["delta"],
            clipping_norm=pc["clipping_norm"],
            lr=float(config.get("learning_rate", 0.5)),
            local_epochs=int(config.get("local_epochs", 5)),
            batch_size=int(config.get("batch_size", 64)),
            num_rounds=int(config.get("num-server-rounds", 1)),
            num_clients=int(pc.get("num_clients", 1)),
            distributed=bool(pc.get("require_secure_aggregation", False)),
            multiclass=multiclass,
            init_coef=self.model.coef_, init_intercept=self.model.intercept_,
        )
        self.model.coef_ = coef
        self.model.intercept_ = intercept

        n_examples = len(self.X)
        if not pc.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)
        return self.get_parameters(config), n_examples, {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)

        # Suppress per-node metrics if required
        if not self.privacy_config.get("allow_per_node_metrics", True):
            n_examples = len(self.X)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n_examples = bucket_count(n_examples)
            return 0.0, n_examples, {}

        y_pred = self.model.predict_proba(self.X)
        loss = log_loss(self.y, y_pred, labels=self.classes)
        accuracy = accuracy_score(self.y, self.model.predict(self.X))

        n_examples = len(self.X)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return loss, n_examples, {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)
    privacy_config = load_privacy_config(context)

    penalty = cfg.get("penalty", "l2")
    C = float(cfg.get("C", 1.0))
    max_iter = int(cfg.get("max_iter", 100))
    n_classes = cfg.get("n_classes", cfg.get("num_classes", 2))

    return FlowerClient(X, y, privacy_config,
                        penalty=penalty, C=C, max_iter=max_iter,
                        n_classes=n_classes)


def _needs_secagg():
    """Check if secure aggregation is required by reading manifest at module load.

    Reads from DSFLOWER_MANIFEST_DIR env var (set by the server before
    launching the SuperNode). This runs at import time so the ClientApp
    is constructed with the correct mods before any Flower communication.
    """
    manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
    if not manifest_dir:
        return False
    manifest_path = os.path.join(manifest_dir, "manifest.json")
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        return manifest.get("require_secure_aggregation", False) is True
    except (OSError, json.JSONDecodeError, KeyError):
        return False


# Build ClientApp with SecAgg+ client mod when required.
# Both server-side (SecAggPlusWorkflow) AND client-side (secaggplus_mod)
# must be enabled for secure aggregation to function.
if _needs_secagg():
    from flwr.client.mod import secaggplus_mod
    app = ClientApp(client_fn=client_fn, mods=[secaggplus_mod])
else:
    app = ClientApp(client_fn=client_fn)
