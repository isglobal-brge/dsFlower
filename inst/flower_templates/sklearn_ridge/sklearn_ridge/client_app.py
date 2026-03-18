"""Flower ClientApp for Federated Ridge Classifier."""

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
from sklearn.linear_model import RidgeClassifier
from sklearn.metrics import accuracy_score

from .task import load_data


class FlowerClient(NumPyClient):
    def __init__(self, X, y, alpha=1.0):
        self.X = X
        self.y = y
        self.model = RidgeClassifier(alpha=alpha)
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
        y_pred = self.model.predict(self.X)
        accuracy = accuracy_score(self.y, y_pred)
        # Ridge doesn't have predict_proba, use decision function as proxy
        loss = float(np.mean((self.model.decision_function(self.X) -
                              self.y) ** 2))
        return loss, len(self.X), {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)
    alpha = float(cfg.get("alpha", 1.0))
    return FlowerClient(X, y, alpha=alpha)


app = ClientApp(client_fn=client_fn)
