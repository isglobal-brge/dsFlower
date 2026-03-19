"""Flower ServerApp for Secure Federated XGBoost (Histogram Protocol).

Implements a custom Strategy that orchestrates a multi-phase histogram
aggregation protocol. The server never sees individual client gradients --
only the SecAgg+ aggregated sums of quantized histograms.

Protocol per tree:
  1. bin_edges   - Collect local percentile edges, compute global edges.
  2. histogram   - For each node to split: collect quantized grad/hess
                   histograms, find best split from aggregated sums.
  3. split       - Broadcast split decision; clients update node assignments.
  4. leaf        - After all splits done, broadcast leaf values; clients
                   update predictions.

Round budget:
  1 round for bin_edges
  Per tree: up to (2^max_depth - 1) histogram rounds + same number of split
  rounds + 1 leaf round = 2 * max_internal_nodes + 1 rounds per tree.
  With max_depth=3 (7 internal nodes max): 15 rounds per tree.
  Total: 1 + n_trees * (2 * max_internal_nodes + 1).
"""

import json
import logging
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
from flwr.common import (
    Context,
    FitIns,
    FitRes,
    Parameters,
    Scalar,
    ndarrays_to_parameters,
    parameters_to_ndarrays,
)
from flwr.server import ServerApp, ServerAppComponents, ServerConfig
from flwr.server.client_manager import ClientManager
from flwr.server.client_proxy import ClientProxy
from flwr.server.strategy import Strategy

logger = logging.getLogger("xgboost_secure_horizontal.server")

SCALE_FACTOR = 1_000_000  # Must match client


class SecureXGBoostStrategy(Strategy):
    """Custom strategy for histogram-based secure federated XGBoost."""

    def __init__(
        self,
        n_trees: int = 10,
        max_depth: int = 3,
        n_bins: int = 64,
        reg_lambda: float = 1.0,
        learning_rate: float = 0.3,
        min_child_weight: float = 1.0,
        objective: str = "binary:logistic",
        n_features: int = 0,
        results_dir: str = "/tmp/dsflower_results",
        min_available_clients: int = 2,
        min_fit_clients: int = 2,
        allow_per_node_metrics: bool = True,
    ):
        super().__init__()
        self.n_trees = n_trees
        self.max_depth = max_depth
        self.n_bins = n_bins
        self.reg_lambda = reg_lambda
        self.learning_rate = learning_rate
        self.min_child_weight = min_child_weight
        self.objective = objective
        self.n_features = n_features
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.min_available_clients = min_available_clients
        self.min_fit_clients = min_fit_clients
        self.allow_per_node_metrics = allow_per_node_metrics

        # State machine
        self.current_tree = 0
        self.global_bin_edges = None  # (n_features, n_bins-1)

        # Tree construction state
        self.nodes_to_split = deque()  # Queue of (node_id, depth) to process
        self.next_node_id = 0  # Counter for assigning node IDs
        self.tree_splits = {}  # node_id -> {feature, threshold, left, right}
        self.tree_leaf_values = {}  # node_id -> leaf_value
        self.node_grad_sums = {}  # node_id -> (G, H) for leaf computation
        self.current_split_node = None  # Node being processed this round

        # Phase tracking: the phase for the NEXT configure_fit call
        self.phase = "bin_edges"
        # After aggregate_fit processes results, it may change the phase

        # All completed trees (for final model export)
        self.completed_trees = []
        self.history = []

        # Maximum internal nodes in a tree of depth max_depth
        self.max_internal_nodes = (2 ** max_depth) - 1

    def initialize_parameters(
        self, client_manager: ClientManager
    ) -> Optional[Parameters]:
        return None

    def configure_fit(
        self, server_round: int, parameters: Parameters,
        client_manager: ClientManager
    ) -> List[Tuple[ClientProxy, FitIns]]:
        clients = client_manager.sample(
            num_clients=self.min_fit_clients,
            min_num_clients=self.min_fit_clients,
        )

        config: Dict[str, Scalar] = {"phase": self.phase}
        fit_params = parameters

        if self.phase == "bin_edges":
            # Send empty params; clients will return local bin edges
            fit_params = ndarrays_to_parameters([np.array([], dtype=np.float32)])

        elif self.phase == "histogram":
            node_id = self._get_current_histogram_node()
            config["node_id"] = str(node_id)
            # Send global bin edges
            if self.global_bin_edges is not None:
                fit_params = ndarrays_to_parameters(
                    [self.global_bin_edges.ravel().astype(np.float32)]
                )
            else:
                fit_params = ndarrays_to_parameters(
                    [np.array([], dtype=np.float32)]
                )

        elif self.phase == "split":
            # Send split decision for the node we just decided
            if self.current_split_node is not None:
                split = self.tree_splits[self.current_split_node]
                split_arr = np.array([
                    split["feature"],
                    split["threshold"],
                    self.current_split_node,
                    split["left"],
                    split["right"],
                ], dtype=np.float64)
                fit_params = ndarrays_to_parameters(
                    [split_arr.astype(np.float32)]
                )
            else:
                fit_params = ndarrays_to_parameters(
                    [np.array([], dtype=np.float32)]
                )

        elif self.phase == "leaf":
            # Send leaf values for all nodes
            max_node = self.next_node_id
            leaf_values = np.zeros(max_node, dtype=np.float64)
            for nid, val in self.tree_leaf_values.items():
                if nid < max_node:
                    leaf_values[nid] = val
            fit_params = ndarrays_to_parameters(
                [leaf_values.astype(np.float32)]
            )

        fit_ins = FitIns(parameters=fit_params, config=config)
        return [(client, fit_ins) for client in clients]

    def aggregate_fit(
        self, server_round: int,
        results: List[Tuple[ClientProxy, FitRes]],
        failures: List[Union[Tuple[ClientProxy, FitRes], BaseException]],
    ) -> Tuple[Optional[Parameters], Dict[str, Scalar]]:
        if not results:
            return None, {}

        if self.phase == "bin_edges":
            return self._aggregate_bin_edges(results)
        elif self.phase == "histogram":
            return self._aggregate_histogram(results)
        elif self.phase == "split":
            return self._aggregate_split(results)
        elif self.phase == "leaf":
            return self._aggregate_leaf(results)

        return None, {}

    def _aggregate_bin_edges(self, results):
        """Merge local bin edges from all clients into global edges."""
        all_edges = []
        for _, fit_res in results:
            ndarrays = parameters_to_ndarrays(fit_res.parameters)
            if len(ndarrays) > 0 and len(ndarrays[0]) > 0:
                all_edges.append(ndarrays[0].astype(np.float64))

        if all_edges:
            # Infer n_features from the first client response
            n_edges_per_feature = self.n_bins - 1
            if self.n_features == 0:
                self.n_features = len(all_edges[0]) // n_edges_per_feature

            # Average edges across clients
            stacked = np.stack(
                [e.reshape(self.n_features, n_edges_per_feature) for e in all_edges]
            )
            self.global_bin_edges = np.mean(stacked, axis=0)

        # Start building the first tree
        self.current_tree = 0
        self._start_new_tree()
        self.phase = "histogram"

        return None, {}

    def _start_new_tree(self):
        """Reset state for building a new tree."""
        self.nodes_to_split = deque()
        self.next_node_id = 1  # root is node 0
        self.tree_splits = {}
        self.tree_leaf_values = {}
        self.node_grad_sums = {}
        self.current_split_node = None

        # Start with root node (id=0, depth=0)
        self.nodes_to_split.append((0, 0))

    def _get_current_histogram_node(self):
        """Return the node_id for the current histogram request."""
        if self.nodes_to_split:
            return self.nodes_to_split[0][0]
        return 0

    def _aggregate_histogram(self, results):
        """Sum quantized histograms and find the best split."""
        if not self.nodes_to_split:
            # No more nodes - move to leaf phase
            self.phase = "leaf"
            return None, {}

        node_id, depth = self.nodes_to_split[0]

        # Sum histograms across clients (SecAgg+ already did this for us
        # in secure mode; in non-secure mode we do it here)
        aggregated = None
        for _, fit_res in results:
            ndarrays = parameters_to_ndarrays(fit_res.parameters)
            if len(ndarrays) > 0 and len(ndarrays[0]) > 0:
                arr = ndarrays[0].astype(np.float64)
                if aggregated is None:
                    aggregated = arr.copy()
                else:
                    aggregated += arr

        if aggregated is None or len(aggregated) == 0:
            # No data - make this a leaf
            self.nodes_to_split.popleft()
            self.tree_leaf_values[node_id] = 0.0
            self._advance_after_node_processed()
            return None, {}

        # Dequantize
        n_hist = self.n_features * self.n_bins
        grad_flat = aggregated[:n_hist] / SCALE_FACTOR
        hess_flat = aggregated[n_hist:] / SCALE_FACTOR

        grad_hist = grad_flat.reshape(self.n_features, self.n_bins)
        hess_hist = hess_flat.reshape(self.n_features, self.n_bins)

        # Total G and H for this node
        G_total = grad_hist.sum()
        H_total = hess_hist.sum()
        self.node_grad_sums[node_id] = (G_total, H_total)

        # Find best split
        best_gain = 0.0
        best_feature = -1
        best_bin = -1

        base_score = (G_total ** 2) / (H_total + self.reg_lambda)

        for f in range(self.n_features):
            G_left = 0.0
            H_left = 0.0
            for b in range(self.n_bins - 1):  # Don't split on last bin
                G_left += grad_hist[f, b]
                H_left += hess_hist[f, b]
                G_right = G_total - G_left
                H_right = H_total - H_left

                if H_left < self.min_child_weight or H_right < self.min_child_weight:
                    continue

                gain = 0.5 * (
                    (G_left ** 2) / (H_left + self.reg_lambda)
                    + (G_right ** 2) / (H_right + self.reg_lambda)
                    - base_score
                )

                if gain > best_gain:
                    best_gain = gain
                    best_feature = f
                    best_bin = b

        self.nodes_to_split.popleft()

        if best_gain <= 0 or depth >= self.max_depth:
            # Make this a leaf
            leaf_value = -G_total / (H_total + self.reg_lambda)
            self.tree_leaf_values[node_id] = leaf_value
            self._advance_after_node_processed()
            return None, {}

        # Record split
        left_child = self.next_node_id
        right_child = self.next_node_id + 1
        self.next_node_id += 2

        # Compute threshold from bin edge
        threshold = float(self.global_bin_edges[best_feature, best_bin])

        self.tree_splits[node_id] = {
            "feature": best_feature,
            "threshold": threshold,
            "left": left_child,
            "right": right_child,
            "gain": best_gain,
        }

        # Queue children for splitting (depth + 1)
        if depth + 1 < self.max_depth:
            self.nodes_to_split.append((left_child, depth + 1))
            self.nodes_to_split.append((right_child, depth + 1))
        else:
            # At max depth - these become leaves; we need their histograms
            # to compute leaf values. Queue them for histogram computation,
            # they will become leaves automatically (depth >= max_depth).
            self.nodes_to_split.append((left_child, depth + 1))
            self.nodes_to_split.append((right_child, depth + 1))

        # Move to split phase to broadcast this decision
        self.current_split_node = node_id
        self.phase = "split"
        return None, {}

    def _aggregate_split(self, results):
        """Split broadcast acknowledged. Move to next histogram or leaf."""
        self.current_split_node = None
        self._advance_after_node_processed()
        return None, {}

    def _advance_after_node_processed(self):
        """Decide next phase after a node has been processed."""
        if self.nodes_to_split:
            self.phase = "histogram"
        else:
            # All nodes processed - assign leaf values to any split children
            # that were not yet assigned
            self._compute_remaining_leaf_values()
            self.phase = "leaf"

    def _compute_remaining_leaf_values(self):
        """For nodes that were split but whose children have no histogram
        data yet, infer leaf values from the parent split."""
        # Any node that appears as a child in tree_splits but is not itself
        # in tree_splits or tree_leaf_values gets a zero leaf value.
        all_child_nodes = set()
        for split in self.tree_splits.values():
            all_child_nodes.add(split["left"])
            all_child_nodes.add(split["right"])

        for child_id in all_child_nodes:
            if child_id not in self.tree_splits and child_id not in self.tree_leaf_values:
                # Check if we have grad/hess sums from histogram
                if child_id in self.node_grad_sums:
                    G, H = self.node_grad_sums[child_id]
                    self.tree_leaf_values[child_id] = -G / (H + self.reg_lambda)
                else:
                    self.tree_leaf_values[child_id] = 0.0

    def _aggregate_leaf(self, results):
        """Leaf values applied. Save this tree and move to next."""
        # Build tree structure for export
        tree_dict = {
            "tree_id": self.current_tree,
            "splits": {},
            "leaves": {},
        }
        for nid, split in self.tree_splits.items():
            tree_dict["splits"][str(nid)] = {
                "feature": split["feature"],
                "threshold": split["threshold"],
                "left": split["left"],
                "right": split["right"],
                "gain": split.get("gain", 0.0),
            }
        for nid, val in self.tree_leaf_values.items():
            tree_dict["leaves"][str(nid)] = val

        self.completed_trees.append(tree_dict)

        self.history.append({
            "tree": self.current_tree,
            "n_splits": len(self.tree_splits),
            "n_leaves": len(self.tree_leaf_values),
        })

        self.current_tree += 1

        if self.current_tree < self.n_trees:
            self._start_new_tree()
            self.phase = "histogram"
        else:
            # All trees done - save model
            self._save_model()
            self._save_history()
            self.phase = "done"

        return None, {}

    def _save_model(self):
        """Save the completed model as JSON."""
        model = {
            "model_type": "xgboost_secure_horizontal",
            "n_trees": len(self.completed_trees),
            "max_depth": self.max_depth,
            "learning_rate": self.learning_rate,
            "reg_lambda": self.reg_lambda,
            "objective": self.objective,
            "n_features": self.n_features,
            "n_bins": self.n_bins,
            "trees": self.completed_trees,
        }
        if self.global_bin_edges is not None:
            model["bin_edges"] = self.global_bin_edges.tolist()

        path = self.results_dir / "global_model.json"
        with open(path, "w") as f:
            json.dump(model, f, indent=2)
        logger.info("Saved model to %s", path)

    def _save_history(self):
        """Save training history."""
        path = self.results_dir / "history.json"
        with open(path, "w") as f:
            json.dump(self.history, f, indent=2)

    # --- Strategy interface methods ---

    def configure_evaluate(
        self, server_round: int, parameters: Parameters,
        client_manager: ClientManager
    ) -> List[Tuple[ClientProxy, FitIns]]:
        # Only evaluate after all trees are built
        if self.phase == "done" or (
            self.phase == "leaf" and self.current_tree >= self.n_trees - 1
        ):
            clients = client_manager.sample(
                num_clients=self.min_fit_clients,
                min_num_clients=self.min_fit_clients,
            )
            eval_ins = FitIns(
                parameters=ndarrays_to_parameters([np.array([], dtype=np.float32)]),
                config={},
            )
            return [(client, eval_ins) for client in clients]
        return []

    def aggregate_evaluate(
        self, server_round: int, results, failures
    ) -> Tuple[Optional[float], Dict[str, Scalar]]:
        if not results:
            return None, {}

        total_loss = 0.0
        total_examples = 0
        accuracies = []

        for _, eval_res in results:
            total_loss += eval_res.loss * eval_res.num_examples
            total_examples += eval_res.num_examples
            if "accuracy" in eval_res.metrics:
                accuracies.append(eval_res.metrics["accuracy"])

        avg_loss = total_loss / max(total_examples, 1)
        metrics: Dict[str, Scalar] = {}
        if accuracies:
            metrics["accuracy"] = float(np.mean(accuracies))

        if not self.allow_per_node_metrics:
            metrics = {}

        return avg_loss, metrics

    def evaluate(
        self, server_round: int, parameters: Parameters
    ) -> Optional[Tuple[float, Dict[str, Scalar]]]:
        return None

    def num_fit_clients(self, num_available_clients: int) -> Tuple[int, int]:
        return max(self.min_fit_clients, 2), self.min_available_clients

    def num_evaluation_clients(self, num_available_clients: int) -> Tuple[int, int]:
        return max(self.min_fit_clients, 2), self.min_available_clients


def _compute_num_rounds(n_trees, max_depth):
    """Compute the total number of server rounds needed.

    Round budget:
      1 round for bin_edges
      Per tree: worst case 2 * (2^max_depth - 1) + 1 rounds
        = (2^max_depth - 1) histogram rounds
        + (2^max_depth - 1) split rounds
        + 1 leaf round
    """
    max_internal_nodes = (2 ** max_depth) - 1
    rounds_per_tree = 2 * max_internal_nodes + 1
    return 1 + n_trees * rounds_per_tree


def server_fn(context: Context) -> ServerAppComponents:
    """Configure the server for secure histogram-based XGBoost."""
    cfg = context.run_config

    n_trees = int(cfg.get("n_trees", 10))
    max_depth = int(cfg.get("max_depth", 3))
    n_bins = int(cfg.get("n_bins", 64))
    reg_lambda = float(cfg.get("reg_lambda", 1.0))
    learning_rate = float(cfg.get("eta", 0.3))
    min_child_weight = float(cfg.get("min_child_weight", 1.0))
    objective = cfg.get("objective", "binary:logistic")
    n_features = int(cfg.get("n_features", 0))
    results_dir = cfg.get("results-dir", "/tmp/dsflower_results")
    require_secagg = str(cfg.get("require-secure-aggregation", "false")).lower() == "true"
    allow_per_node_metrics = str(
        cfg.get("allow-per-node-metrics", "true")
    ).lower() == "true"

    num_rounds = _compute_num_rounds(n_trees, max_depth)

    strategy = SecureXGBoostStrategy(
        n_trees=n_trees,
        max_depth=max_depth,
        n_bins=n_bins,
        reg_lambda=reg_lambda,
        learning_rate=learning_rate,
        min_child_weight=min_child_weight,
        objective=objective,
        n_features=n_features,
        results_dir=results_dir,
        min_available_clients=int(cfg.get("strategy-min_available_clients", 2)),
        min_fit_clients=int(cfg.get("strategy-min_fit_clients", 2)),
        allow_per_node_metrics=allow_per_node_metrics,
    )

    config = ServerConfig(num_rounds=num_rounds)

    if require_secagg:
        from flwr.server.workflow import SecAggPlusWorkflow
        num_clients = int(cfg.get("strategy-min_available_clients", 2))
        return ServerAppComponents(
            strategy=strategy, config=config,
            workflow=SecAggPlusWorkflow(
                num_shares=num_clients,
                reconstruction_threshold=num_clients,
            ),
        )

    return ServerAppComponents(strategy=strategy, config=config)


app = ServerApp(server_fn=server_fn)
