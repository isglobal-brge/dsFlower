"""Flower ClientApp for Federated XGBoost (Histogram Protocol)."""

import json
import os

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np

from .task import load_data, load_privacy_config
from .privacy_utils import bucket_count

SCALE_FACTOR = 1_000_000  # Quantization scale for SecAgg+ (int64)


class SecureXGBClient(NumPyClient):
    def __init__(self, X, y, privacy_config, n_bins=64,
                 learning_rate=0.3, reg_lambda=1.0, objective="binary:logistic"):
        self.X = X.astype(np.float64)
        self.y = y.astype(np.float64)
        self.privacy_config = privacy_config
        self.n_bins = n_bins
        self.learning_rate = learning_rate
        self.reg_lambda = reg_lambda
        self.objective = objective
        self.n_features = X.shape[1]

        # State maintained across rounds
        self.bin_edges = None  # (n_features, n_bins-1)
        self.predictions = np.zeros(len(y), dtype=np.float64)
        self.node_assignments = np.zeros(len(y), dtype=np.int32)
        self.trees = []  # List of tree dicts for final model

    def get_parameters(self, config):
        # Return predictions (used for initialization)
        return [self.predictions.astype(np.float32)]

    def set_parameters(self, parameters):
        pass  # Parameters are handled per-phase in fit()

    def fit(self, parameters, config):
        phase = config.get("phase", "histogram")

        if phase == "bin_edges":
            return self._handle_bin_edges(parameters, config)
        elif phase == "histogram":
            return self._handle_histogram(parameters, config)
        elif phase == "split":
            return self._handle_split(parameters, config)
        elif phase == "leaf":
            return self._handle_leaf(parameters, config)
        else:
            return [np.array([], dtype=np.float32)], len(self.y), {}

    def _compute_gradients(self):
        """Compute first and second order gradients for the objective."""
        if self.objective == "binary:logistic":
            probs = 1.0 / (1.0 + np.exp(-self.predictions))
            grad = probs - self.y
            hess = probs * (1.0 - probs)
        else:  # squared error for regression
            grad = self.predictions - self.y
            hess = np.ones_like(grad)
        return grad, hess

    def _handle_bin_edges(self, parameters, config):
        """Receive global bin edges from server."""
        if len(parameters) > 0 and len(parameters[0]) > 0:
            flat = parameters[0].astype(np.float64)
            self.bin_edges = flat.reshape(self.n_features, -1)
        else:
            # Compute local bin edges as fallback
            edges = []
            for f in range(self.n_features):
                col = self.X[:, f]
                percentiles = np.linspace(0, 100, self.n_bins + 1)[1:-1]
                edges.append(np.percentile(col, percentiles))
            self.bin_edges = np.array(edges)

        # Return local bin edges for server to merge
        local_edges = []
        for f in range(self.n_features):
            col = self.X[:, f]
            percentiles = np.linspace(0, 100, self.n_bins + 1)[1:-1]
            local_edges.append(np.percentile(col, percentiles))
        local_edges = np.array(local_edges, dtype=np.float32)

        return [local_edges.ravel()], len(self.y), {}

    def _handle_histogram(self, parameters, config):
        """Compute and return quantized histograms for SecAgg+."""
        node_id = int(config.get("node_id", "0"))

        # Update bin edges if provided
        if len(parameters) > 0 and len(parameters[0]) > 0:
            flat = parameters[0].astype(np.float64)
            if len(flat) == self.n_features * (self.n_bins - 1):
                self.bin_edges = flat.reshape(self.n_features, -1)

        grad, hess = self._compute_gradients()
        mask = self.node_assignments == node_id

        if not np.any(mask):
            # No samples in this node - return zero histograms
            n_hist_values = self.n_features * self.n_bins * 2  # grad + hess
            return [np.zeros(n_hist_values, dtype=np.int64)], len(self.y), {}

        X_node = self.X[mask]
        grad_node = grad[mask]
        hess_node = hess[mask]

        # Compute histograms
        grad_hist = np.zeros((self.n_features, self.n_bins), dtype=np.float64)
        hess_hist = np.zeros((self.n_features, self.n_bins), dtype=np.float64)

        for f in range(self.n_features):
            bin_idx = np.digitize(X_node[:, f], self.bin_edges[f])
            bin_idx = np.clip(bin_idx, 0, self.n_bins - 1)
            for b in range(self.n_bins):
                in_bin = bin_idx == b
                grad_hist[f, b] = grad_node[in_bin].sum()
                hess_hist[f, b] = hess_node[in_bin].sum()

        # Quantize for SecAgg+ (int64)
        grad_quantized = np.round(grad_hist * SCALE_FACTOR).astype(np.int64)
        hess_quantized = np.round(hess_hist * SCALE_FACTOR).astype(np.int64)

        # Flatten: [grad_f0_b0, ..., grad_fN_bM, hess_f0_b0, ..., hess_fN_bM]
        flat = np.concatenate([grad_quantized.ravel(), hess_quantized.ravel()])

        n_examples = int(mask.sum())
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return [flat], n_examples, {}

    def _handle_split(self, parameters, config):
        """Apply split decision to node assignments."""
        if len(parameters) == 0 or len(parameters[0]) == 0:
            return [np.array([], dtype=np.float32)], len(self.y), {}

        split_info = parameters[0].astype(np.float64)
        feature_idx = int(split_info[0])
        threshold = float(split_info[1])
        node_id = int(split_info[2])
        left_child = int(split_info[3])
        right_child = int(split_info[4])

        mask = self.node_assignments == node_id
        if np.any(mask):
            indices = np.where(mask)[0]
            go_left = self.X[indices, feature_idx] <= threshold
            self.node_assignments[indices[go_left]] = left_child
            self.node_assignments[indices[~go_left]] = right_child

        return [np.array([], dtype=np.float32)], len(self.y), {}

    def _handle_leaf(self, parameters, config):
        """Apply leaf values to update predictions."""
        if len(parameters) == 0 or len(parameters[0]) == 0:
            return self.get_parameters(config), len(self.y), {}

        leaf_values = parameters[0].astype(np.float64)

        for node_id in range(len(leaf_values)):
            mask = self.node_assignments == node_id
            if np.any(mask):
                self.predictions[mask] += self.learning_rate * leaf_values[node_id]

        # Reset for next tree
        self.node_assignments[:] = 0

        return self.get_parameters(config), len(self.y), {}

    def evaluate(self, parameters, config):
        if not self.privacy_config.get("allow_per_node_metrics", True):
            n = len(self.y)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n = bucket_count(n)
            return 0.0, n, {}

        if self.objective == "binary:logistic":
            probs = 1.0 / (1.0 + np.exp(-self.predictions))
            preds = (probs > 0.5).astype(int)
            accuracy = float(np.mean(preds == self.y))
            loss = float(-np.mean(
                self.y * np.log(np.clip(probs, 1e-7, 1)) +
                (1 - self.y) * np.log(np.clip(1 - probs, 1e-7, 1))
            ))
        else:
            accuracy = 0.0
            loss = float(np.mean((self.predictions - self.y) ** 2))

        n = len(self.y)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n = bucket_count(n)

        return loss, n, {"accuracy": accuracy}


def client_fn(context: Context) -> SecureXGBClient:
    cfg = context.run_config
    X, y = load_data(context)
    privacy_config = load_privacy_config(context)

    n_bins = int(cfg.get("n_bins", 64))
    learning_rate = float(cfg.get("eta", 0.3))
    reg_lambda = float(cfg.get("reg_lambda", 1.0))
    objective = cfg.get("objective", "binary:logistic")

    return SecureXGBClient(
        X, y, privacy_config,
        n_bins=n_bins,
        learning_rate=learning_rate,
        reg_lambda=reg_lambda,
        objective=objective,
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


# SecAgg+ is enabled when the server trust profile requires it.
if _needs_secagg():
    from flwr.client.mod import secaggplus_mod
    app = ClientApp(client_fn=client_fn, mods=[secaggplus_mod])
else:
    app = ClientApp(client_fn=client_fn)
