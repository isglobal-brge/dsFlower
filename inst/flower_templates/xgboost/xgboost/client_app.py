"""Flower ClientApp for Federated XGBoost (Histogram Protocol)."""

import json
import math
import os

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np

from .task import load_data, load_privacy_config, _get_manifest_dir
from .privacy_utils import bucket_count


class SecureXGBClient(NumPyClient):
    def __init__(self, X, y, privacy_config, n_bins=64,
                 learning_rate=0.3, reg_lambda=1.0, objective="binary:logistic",
                 num_class=2, manifest_dir=None):
        self.X = X.astype(np.float64)
        self.y = y.astype(np.float64)
        self.privacy_config = privacy_config
        self.n_bins = n_bins
        self.learning_rate = learning_rate
        self.reg_lambda = reg_lambda
        self.objective = objective
        # Multiclass softmax boosting keeps one output column per class; binary
        # and regression keep a single output. Each class is boosted by its own
        # trees, but every per-class gradient histogram still flows through the
        # SAME Secure Aggregation path -- the server only ever sees cross-site
        # sums, never any single site's per-class histogram.
        self.multiclass = str(objective).startswith("multi")
        self.n_classes = int(num_class) if self.multiclass else 2
        self.n_outputs = self.n_classes if self.multiclass else 1
        self.n_features = X.shape[1]
        # Batched multiclass packs every class's gradient/hessian histogram into
        # ONE secure round (vector widened by n_outputs), so all K per-class
        # stumps are built per boosting round instead of one per round. Mirrors
        # the server's `batched` flag; both gate on SecAgg + multiclass.
        self.batched = bool(
            privacy_config.get("require_secure_aggregation", False)
        ) and self.n_outputs > 1
        outputs = self.n_outputs if self.batched else 1
        self.vector_length = self.n_features * self.n_bins * 2 * outputs

        # State maintained across rounds
        self.bin_edges = None  # (n_features, n_bins-1)
        self.predictions = np.zeros((len(y), self.n_outputs), dtype=np.float64)
        self.node_assignments = np.zeros(len(y), dtype=np.int32)
        self.trees = []  # List of tree dicts for final model
        self.state_path = None
        self.last_applied_tree = -1
        if manifest_dir:
            self.state_path = os.path.join(manifest_dir, "xgboost_state.npz")
            self._load_state()

    def _load_state(self):
        """Restore per-run client state across Flower ClientApp invocations."""
        if not self.state_path or not os.path.exists(self.state_path):
            return
        try:
            state = np.load(self.state_path, allow_pickle=False)
            predictions = state["predictions"] if "predictions" in state.files else None
            node_assignments = (
                state["node_assignments"] if "node_assignments" in state.files else None
            )
            bin_edges = state["bin_edges"] if "bin_edges" in state.files else None
            if predictions is not None and predictions.shape[0] == len(self.y):
                pred = predictions.astype(np.float64)
                if pred.ndim == 1:
                    pred = pred.reshape(-1, 1)
                if pred.shape[1] == self.n_outputs:
                    self.predictions = pred
            if node_assignments is not None and len(node_assignments) == len(self.y):
                self.node_assignments = node_assignments.astype(np.int32)
            if bin_edges is not None and bin_edges.size:
                self.bin_edges = bin_edges.astype(np.float64)
            if "last_applied_tree" in state.files:
                self.last_applied_tree = int(state["last_applied_tree"])
        except Exception:
            # State can be rebuilt from the protocol messages. A corrupt cache
            # should not abort the run; it simply starts this client state fresh.
            self.predictions = np.zeros((len(self.y), self.n_outputs), dtype=np.float64)
            self.node_assignments = np.zeros(len(self.y), dtype=np.int32)
            self.bin_edges = None
            self.last_applied_tree = -1

    def _save_state(self):
        """Persist per-run state needed by the multi-phase histogram protocol."""
        if not self.state_path:
            return
        bin_edges = self.bin_edges
        if bin_edges is None:
            bin_edges = np.array([], dtype=np.float64)
        np.savez(
            self.state_path,
            predictions=self.predictions.astype(np.float64),
            node_assignments=self.node_assignments.astype(np.int32),
            bin_edges=bin_edges.astype(np.float64),
            last_applied_tree=np.array(self.last_applied_tree, dtype=np.int32),
        )

    def get_parameters(self, config):
        # SecAgg+ expects a stable parameter shape across secure rounds. The
        # histogram vector is the largest vector used by the protocol, so all
        # control phases use the same padded shape.
        return [np.zeros(self.vector_length, dtype=np.float32)]

    def set_parameters(self, parameters):
        pass  # Parameters are handled per-phase in fit()

    def _pad_vector(self, values):
        out = np.zeros(self.vector_length, dtype=np.float32)
        values = np.asarray(values, dtype=np.float32).ravel()
        n = min(len(values), self.vector_length)
        if n:
            out[:n] = values[:n]
        return out

    def _ack_vector(self):
        out = np.zeros(self.vector_length, dtype=np.float32)
        out[0] = 1.0
        return out

    def _fit_num_examples(self, n):
        """Return the weighting factor used by Flower for fit aggregation."""
        if self.privacy_config.get("require_secure_aggregation", False):
            # Flower SecAgg+ computes a weighted average over masked vectors.
            # Histogram boosting needs sums, not sample-size weighted averages.
            # Giving every client weight 1 lets the strategy recover the sum by
            # summing the identical aggregate vector passed back by the workflow.
            return 1
        if not self.privacy_config.get("allow_exact_num_examples", True):
            return bucket_count(int(n))
        return int(n)

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
            return [self._ack_vector()], self._fit_num_examples(len(self.y)), {}

    def _softmax(self):
        z = self.predictions - self.predictions.max(axis=1, keepdims=True)
        expz = np.exp(z)
        return expz / expz.sum(axis=1, keepdims=True)

    def _compute_gradients(self, class_idx=0):
        """First/second-order gradients for the active output.

        Multiclass softmax: for class k, grad = p_k - 1{y==k}, hess = p_k(1-p_k);
        each class's gradients are histogrammed and SecAgg-summed independently.
        """
        if self.multiclass:
            probs = self._softmax()
            p = probs[:, class_idx]
            grad = p - (self.y == class_idx).astype(np.float64)
            hess = p * (1.0 - p)
        elif self.objective == "binary:logistic":
            probs = 1.0 / (1.0 + np.exp(-self.predictions[:, 0]))
            grad = probs - self.y
            hess = probs * (1.0 - probs)
        else:  # squared error for regression
            grad = self.predictions[:, 0] - self.y
            hess = np.ones_like(grad)
        return grad, hess

    def _maybe_apply_previous_stump(self, config):
        """Apply the previous boosted stump exactly once per client state."""
        tree_raw = config.get("apply_tree_id")
        if tree_raw is None:
            return
        tree_id = int(tree_raw)
        if tree_id <= self.last_applied_tree:
            return

        class_idx = int(float(config.get("apply_class_idx", 0)))
        feature_idx = int(float(config.get("apply_feature", -1)))
        threshold = float(config.get("apply_threshold", 0.0))
        left_leaf = float(config.get("apply_left_leaf", 0.0))
        right_leaf = float(config.get("apply_right_leaf", left_leaf))

        if feature_idx < 0:
            leaf = np.full(len(self.y), left_leaf, dtype=np.float64)
        else:
            leaf = np.where(self.X[:, feature_idx] <= threshold, left_leaf, right_leaf)
        self.predictions[:, class_idx] += self.learning_rate * leaf
        self.last_applied_tree = tree_id
        self.node_assignments[:] = 0
        self._save_state()

    def _apply_pending_updates(self, config):
        """Apply all K per-class stumps from the previous batched round, once.

        Each update advances one class's predictions; idempotent via
        last_applied_tree so a resent config never double-applies.
        """
        raw = config.get("apply_updates")
        if not raw:
            return
        try:
            updates = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        max_tree = self.last_applied_tree
        for u in updates:
            tree_id = int(u.get("tree_id", -1))
            if tree_id <= self.last_applied_tree:
                continue
            class_idx = int(u.get("class_idx", 0))
            feature_idx = int(u.get("feature", -1))
            threshold = float(u.get("threshold", 0.0))
            left_leaf = float(u.get("left_leaf", 0.0))
            right_leaf = float(u.get("right_leaf", left_leaf))
            if feature_idx < 0:
                leaf = np.full(len(self.y), left_leaf, dtype=np.float64)
            else:
                leaf = np.where(
                    self.X[:, feature_idx] <= threshold, left_leaf, right_leaf
                )
            self.predictions[:, class_idx] += self.learning_rate * leaf
            max_tree = max(max_tree, tree_id)
        self.last_applied_tree = max_tree
        self.node_assignments[:] = 0
        self._save_state()

    def _maybe_add_update_noise(self, flat):
        """Add Gaussian noise to bounded histogram contributions.

        For binary-logistic histogram boosting, each row contributes bounded
        gradient and Hessian terms to one bin per feature. The profile's
        clipping_norm is treated here as the declared L2 sensitivity bound for
        a single row's histogram contribution, not as a norm cap on the whole
        site-level aggregate histogram. Clipping the aggregate would destroy
        the split signal before secure aggregation.
        """
        if self.privacy_config.get("privacy_mode") != "dp_update_level":
            return flat.astype(np.float32)

        sensitivity = float(self.privacy_config.get("clipping_norm", 1.0))
        epsilon = float(self.privacy_config.get("epsilon", 1.0))
        delta = float(self.privacy_config.get("delta", 1e-5))
        if sensitivity <= 0 or epsilon <= 0 or delta <= 0:
            raise ValueError("epsilon, delta, and clipping_norm must be positive")

        sigma = math.sqrt(2.0 * math.log(1.25 / delta)) * (sensitivity / epsilon)
        noise = np.random.normal(0.0, sigma, size=flat.shape)
        return (flat.astype(np.float64) + noise).astype(np.float32)

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
        self._save_state()

        return [self._pad_vector(local_edges.ravel())], self._fit_num_examples(len(self.y)), {}

    def _handle_histogram(self, parameters, config):
        """Compute and return quantized histograms for SecAgg+."""
        if str(config.get("secure_stump_boost", "false")).lower() == "true":
            if self.batched:
                self._apply_pending_updates(config)
            else:
                self._maybe_apply_previous_stump(config)
            self.node_assignments[:] = 0

        node_id = int(config.get("node_id", "0"))
        class_idx = int(float(config.get("class_idx", 0)))

        # Update bin edges if provided
        if len(parameters) > 0 and len(parameters[0]) > 0:
            flat = parameters[0].astype(np.float64)
            edge_len = self.n_features * (self.n_bins - 1)
            if len(flat) >= edge_len:
                self.bin_edges = flat[:edge_len].reshape(self.n_features, -1)

        if self.batched:
            return self._batched_histograms(node_id)

        grad, hess = self._compute_gradients(class_idx)
        mask = self.node_assignments == node_id

        if not np.any(mask):
            # No samples in this node - return zero histograms
            return [
                np.zeros(self.vector_length, dtype=np.float32)
            ], self._fit_num_examples(0), {}

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

        # Flatten: [grad_f0_b0, ..., grad_fN_bM, hess_f0_b0, ..., hess_fN_bM]
        flat = np.concatenate([grad_hist.ravel(), hess_hist.ravel()]).astype(np.float32)
        flat = self._maybe_add_update_noise(flat)

        return [flat], self._fit_num_examples(mask.sum()), {}

    def _batched_histograms(self, node_id):
        """All K classes' grad/hess histograms packed into one widened vector.

        Layout per class c: grad_hist(F*B) then hess_hist(F*B), concatenated
        c=0..K-1. The server unpacks the same layout after SecAgg summation.
        """
        mask = self.node_assignments == node_id
        n = int(mask.sum())
        if n == 0:
            return [np.zeros(self.vector_length, dtype=np.float32)], \
                self._fit_num_examples(0), {}

        X_node = self.X[mask]
        # Bin index per (sample, feature) -- shared across all classes.
        bin_idx = np.empty((n, self.n_features), dtype=np.int64)
        for f in range(self.n_features):
            bi = np.digitize(X_node[:, f], self.bin_edges[f])
            bin_idx[:, f] = np.clip(bi, 0, self.n_bins - 1)

        fb = self.n_features * self.n_bins
        per_class = fb * 2
        out = np.zeros(self.vector_length, dtype=np.float64)
        for c in range(self.n_outputs):
            grad, hess = self._compute_gradients(c)
            grad_node = grad[mask]
            hess_node = hess[mask]
            grad_hist = np.zeros((self.n_features, self.n_bins), dtype=np.float64)
            hess_hist = np.zeros((self.n_features, self.n_bins), dtype=np.float64)
            for f in range(self.n_features):
                bi = bin_idx[:, f]
                grad_hist[f] = np.bincount(bi, weights=grad_node, minlength=self.n_bins)[:self.n_bins]
                hess_hist[f] = np.bincount(bi, weights=hess_node, minlength=self.n_bins)[:self.n_bins]
            off = c * per_class
            out[off:off + fb] = grad_hist.ravel()
            out[off + fb:off + per_class] = hess_hist.ravel()

        out = self._maybe_add_update_noise(out)
        return [out.astype(np.float32)], self._fit_num_examples(n), {}

    def _handle_split(self, parameters, config):
        """Apply split decision to node assignments."""
        if len(parameters) == 0 or len(parameters[0]) == 0:
            return [self._ack_vector()], self._fit_num_examples(len(self.y)), {}

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
        self._save_state()

        return [self._ack_vector()], self._fit_num_examples(len(self.y)), {}

    def _handle_leaf(self, parameters, config):
        """Apply leaf values to update predictions."""
        if len(parameters) == 0 or len(parameters[0]) == 0:
            return [self._ack_vector()], self._fit_num_examples(len(self.y)), {}

        class_idx = int(float(config.get("class_idx", 0)))
        leaf_values = parameters[0].astype(np.float64)

        for node_id in range(len(leaf_values)):
            mask = self.node_assignments == node_id
            if np.any(mask):
                self.predictions[mask, class_idx] += self.learning_rate * leaf_values[node_id]

        # Reset for next tree
        self.node_assignments[:] = 0
        self._save_state()

        return [self._ack_vector()], self._fit_num_examples(len(self.y)), {}

    def evaluate(self, parameters, config):
        if not self.privacy_config.get("allow_per_node_metrics", True):
            n = len(self.y)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n = bucket_count(n)
            return 0.0, n, {}

        if self.multiclass:
            probs = self._softmax()
            preds = np.argmax(probs, axis=1)
            accuracy = float(np.mean(preds == self.y))
            idx = self.y.astype(int)
            p_true = probs[np.arange(len(self.y)), idx]
            loss = float(-np.mean(np.log(np.clip(p_true, 1e-7, 1))))
        elif self.objective == "binary:logistic":
            probs = 1.0 / (1.0 + np.exp(-self.predictions[:, 0]))
            preds = (probs > 0.5).astype(int)
            accuracy = float(np.mean(preds == self.y))
            loss = float(-np.mean(
                self.y * np.log(np.clip(probs, 1e-7, 1)) +
                (1 - self.y) * np.log(np.clip(1 - probs, 1e-7, 1))
            ))
        else:
            accuracy = 0.0
            loss = float(np.mean((self.predictions[:, 0] - self.y) ** 2))

        n = len(self.y)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n = bucket_count(n)

        return loss, n, {"accuracy": accuracy}


def client_fn(context: Context) -> SecureXGBClient:
    cfg = context.run_config
    X, y = load_data(context)
    privacy_config = load_privacy_config(context)
    manifest_dir = _get_manifest_dir(context)

    n_bins = int(cfg.get("n_bins", 64))
    learning_rate = float(cfg.get("eta", 0.3))
    reg_lambda = float(cfg.get("reg_lambda", 1.0))
    objective = cfg.get("objective", "binary:logistic")
    num_class = int(cfg.get("num_class", 2))

    return SecureXGBClient(
        X, y, privacy_config,
        n_bins=n_bins,
        learning_rate=learning_rate,
        reg_lambda=reg_lambda,
        objective=objective,
        num_class=num_class,
        manifest_dir=manifest_dir,
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
