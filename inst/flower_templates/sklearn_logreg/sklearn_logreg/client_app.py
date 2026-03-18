"""Flower ClientApp for Federated Logistic Regression."""

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score

from .task import load_data, load_privacy_config
from .privacy_utils import clip_weights, add_gaussian_noise, compute_sigma, bucket_count


class FlowerClient(NumPyClient):
    def __init__(self, X, y, privacy_config, penalty="l2", C=1.0, max_iter=100):
        self.X = X
        self.y = y
        self.privacy_config = privacy_config
        self.model = LogisticRegression(
            penalty=penalty, C=C, max_iter=max_iter, warm_start=True
        )
        # Initialize model with at least one sample per class
        classes = np.unique(y)
        init_idx = [np.where(y == c)[0][0] for c in classes]
        self.model.fit(X[init_idx], y[init_idx])

    def get_parameters(self, config):
        return [self.model.coef_, self.model.intercept_]

    def set_parameters(self, parameters):
        self.model.coef_ = parameters[0]
        self.model.intercept_ = parameters[1]

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        old_weights = [p.copy() for p in parameters]

        self.model.fit(self.X, self.y)
        new_weights = self.get_parameters(config)

        # Apply local DP if required
        if self.privacy_config.get("privacy_mode") == "dp":
            cn = self.privacy_config["clipping_norm"]
            eps = self.privacy_config["epsilon"]
            delta = self.privacy_config["delta"]
            n = self.privacy_config["n_samples"]

            new_weights = clip_weights(new_weights, old_weights, cn)
            sigma = compute_sigma(eps, delta, cn, n)
            new_weights = add_gaussian_noise(new_weights, old_weights, sigma, cn)

        # Bucket num_examples if required
        n_examples = len(self.X)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return new_weights, n_examples, {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)

        # Suppress per-node metrics if required
        if not self.privacy_config.get("allow_per_node_metrics", True):
            n_examples = len(self.X)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n_examples = bucket_count(n_examples)
            return 0.0, n_examples, {}

        y_pred = self.model.predict_proba(self.X)
        loss = log_loss(self.y, y_pred, labels=np.unique(self.y))
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

    return FlowerClient(X, y, privacy_config,
                        penalty=penalty, C=C, max_iter=max_iter)


# Build app with optional SecAgg+ mod
def _build_app():
    try:
        from .task import load_privacy_config as _lpc
        # We cannot call load_privacy_config at module level (no context),
        # so SecAgg is enabled via run_config instead
        pass
    except Exception:
        pass
    return ClientApp(client_fn=client_fn)


app = ClientApp(client_fn=client_fn)
