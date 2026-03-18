"""Flower ClientApp for Federated Logistic Regression."""

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, accuracy_score

from .task import load_data


class FlowerClient(NumPyClient):
    def __init__(self, X, y, penalty="l2", C=1.0, max_iter=100):
        self.X = X
        self.y = y
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
        self.model.fit(self.X, self.y)
        return self.get_parameters(config), len(self.X), {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)
        y_pred = self.model.predict_proba(self.X)
        loss = log_loss(self.y, y_pred, labels=np.unique(self.y))
        accuracy = accuracy_score(self.y, self.model.predict(self.X))
        return loss, len(self.X), {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)

    penalty = cfg.get("penalty", "l2")
    C = float(cfg.get("C", 1.0))
    max_iter = int(cfg.get("max_iter", 100))

    return FlowerClient(X, y, penalty=penalty, C=C, max_iter=max_iter)


app = ClientApp(client_fn=client_fn)
