"""Flower ClientApp for Federated SGD Classifier."""

import json
import os

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import log_loss, accuracy_score, hinge_loss

from .task import load_data, load_privacy_config
from .privacy_utils import clip_weights, add_gaussian_noise, compute_sigma, bucket_count


def _resolve_classes(y, n_classes=None):
    y = np.asarray(y)
    finite = np.isfinite(y)
    labels = np.unique(y[finite])
    if n_classes is not None and int(n_classes) > 1:
        return np.arange(int(n_classes), dtype=y.dtype)
    if labels.size == 1 and labels[0] in (0, 1):
        return np.array([0, 1], dtype=y.dtype)
    return labels


def _fit_with_class_anchors(model, X, y, classes):
    y = np.asarray(y)
    finite = np.isfinite(y)
    X_fit = X[finite]
    y_fit = y[finite]
    if y_fit.size == 0:
        raise ValueError("No finite target values available for sklearn_sgd")

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
    def __init__(self, X, y, privacy_config, loss="log_loss", alpha=0.0001,
                 lr_schedule="optimal", penalty="l2", l1_ratio=0.15,
                 eta0=0.01, max_iter=1000, n_classes=None):
        self.X = X
        self.y = y
        self.privacy_config = privacy_config
        self.classes = _resolve_classes(y, n_classes)
        kw = dict(loss=loss, alpha=alpha, learning_rate=lr_schedule,
                  penalty=penalty, warm_start=True, max_iter=max_iter,
                  tol=1e-4, random_state=42)
        if lr_schedule in ("constant", "invscaling", "adaptive"):
            kw["eta0"] = eta0
        if penalty == "elasticnet":
            kw["l1_ratio"] = l1_ratio
        self.model = SGDClassifier(**kw)
        _fit_with_class_anchors(self.model, X, y, self.classes)

    def get_parameters(self, config):
        return [self.model.coef_, self.model.intercept_]

    def set_parameters(self, parameters):
        self.model.coef_ = parameters[0]
        self.model.intercept_ = parameters[1]

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        old_weights = [p.copy() for p in parameters]

        _fit_with_class_anchors(self.model, self.X, self.y, self.classes)
        new_weights = self.get_parameters(config)

        # Update-level clipping/noise (NOT patient-level DP-SGD)
        if self.privacy_config.get("privacy_mode") == "dp":
            cn = self.privacy_config["clipping_norm"]
            eps = self.privacy_config["epsilon"]
            delta = self.privacy_config["delta"]
            n = self.privacy_config["n_samples"]

            new_weights = clip_weights(new_weights, old_weights, cn)
            sigma = compute_sigma(eps, delta, cn, n)
            new_weights = add_gaussian_noise(new_weights, old_weights, sigma, cn)

        n_examples = len(self.X)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return new_weights, n_examples, {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)

        if not self.privacy_config.get("allow_per_node_metrics", True):
            n_examples = len(self.X)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n_examples = bucket_count(n_examples)
            return 0.0, n_examples, {}

        if hasattr(self.model, "predict_proba"):
            y_pred_proba = self.model.predict_proba(self.X)
            loss = log_loss(self.y, y_pred_proba, labels=self.classes)
        else:
            scores = self.model.decision_function(self.X)
            if np.ndim(scores) == 1:
                y_signed = np.where(self.y > 0, 1, -1)
                loss = hinge_loss(y_signed, scores)
            else:
                loss = hinge_loss(self.y, scores)
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

    loss = cfg.get("loss", "log_loss")
    alpha = float(cfg.get("alpha", 0.0001))
    lr_schedule = cfg.get("lr_schedule", "optimal")
    penalty = cfg.get("penalty", "l2")
    l1_ratio = float(cfg.get("l1_ratio", 0.15))
    eta0 = float(cfg.get("eta0", 0.01))
    max_iter = int(cfg.get("max_iter", 1000))
    n_classes = cfg.get("n_classes", cfg.get("num_classes", 2))

    return FlowerClient(X, y, privacy_config,
                        loss=loss, alpha=alpha, lr_schedule=lr_schedule,
                        penalty=penalty, l1_ratio=l1_ratio,
                        eta0=eta0, max_iter=max_iter, n_classes=n_classes)


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
