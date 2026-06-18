"""Flower ServerApp for Federated XGBoost (Histogram Protocol).

Implements a custom Strategy that orchestrates a multi-phase histogram
aggregation protocol. In secure profiles, SecAgg+ protects individual client
histograms so the server sees only aggregated sums of quantized histograms.
Trusted demo profiles can run the same protocol without SecAgg+ for end-to-end
validation.

Trusted-profile protocol per tree:
  1. bin_edges   - Collect local percentile edges, compute global edges.
  2. histogram   - For each node to split: collect quantized grad/hess
                   histograms, find best split from aggregated sums.
  3. split       - Broadcast split decision; clients update node assignments.
  4. leaf        - After all splits done, broadcast leaf values; clients
                   update predictions.

Secure-profile protocol:
  SecAgg+ workflows require a stable fit shape and do not tolerate the
  variable control phases used by the full tree builder. For clinical profiles
  the template therefore runs a boosted-stump protocol: each secure round
  aggregates one fixed-shape root histogram, the server derives one split and
  two leaf values, and the next round's config tells clients to apply the
  previous stump before computing the next encrypted histogram.

Round budget:
  1 round for bin_edges
  Per tree: all node histograms, internal-node split broadcasts, and one leaf
  broadcast. With max_depth=3 this is 15 histogram rounds, 7 split rounds and
  1 leaf round per tree.
"""

import json
import logging
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
from flwr.common import (
    Context,
    EvaluateIns,
    EvaluateRes,
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

logger = logging.getLogger("xgboost.server")


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
        num_class: int = 2,
        n_features: int = 0,
        results_dir: str = "/tmp/dsflower_results",
        min_available_clients: int = 2,
        min_fit_clients: int = 2,
        allow_per_node_metrics: bool = True,
        evaluation_only: bool = False,
        secure_single_round: bool = False,
        secure_stump_boost: bool = False,
        fixed_bin_range: float = 4.0,
        batch_multiclass: bool = False,
    ):
        super().__init__()
        self.multiclass = str(objective).startswith("multi")
        self.n_classes = int(num_class) if self.multiclass else 2
        self.n_outputs = self.n_classes if self.multiclass else 1
        # One tree per output (class) per boosting round; total = rounds x outputs.
        # Every per-class tree is built from a SecAgg-summed histogram, so the
        # privacy guarantee is identical to the binary case, applied K times.
        self.n_boost_rounds = n_trees
        self.n_trees = n_trees * self.n_outputs
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
        self.evaluation_only = evaluation_only
        self.secure_stump_boost = secure_stump_boost or secure_single_round
        self.secure_single_round = secure_single_round
        self.fixed_bin_range = fixed_bin_range
        self.pending_stump_update = None
        # Batched multiclass: build all K per-class stumps in ONE secure round
        # instead of K sequential rounds. The histogram vector carries every
        # class's gradient/hessian sums at once (still a single SecAgg-summed
        # vector, so the privacy guarantee is unchanged), cutting the number of
        # secure round-trips by a factor of K. Binary/regression (n_outputs == 1)
        # is the degenerate 1-class case and behaves exactly as before.
        #
        # OPT-IN (default off): the protocol is validated in-process but the
        # widened SecAgg+ vector has not yet passed end-to-end federated testing
        # over the DSI tunnel (large vectors stress the relay; see notes). Until
        # then the default path stays the per-class-per-round one. Enable with
        # the `batch_multiclass` model param once federated-validated.
        self.batched = (
            self.secure_stump_boost and self.n_outputs > 1 and bool(batch_multiclass)
        )
        self.pending_stump_updates = []

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

        if self.secure_stump_boost:
            self.global_bin_edges = self._fixed_bin_edges()
            self._start_new_tree()
            self.phase = "histogram"

    def _fixed_parameters(self, values=None) -> Parameters:
        """Return a single fixed-shape parameter vector for SecAgg+ rounds."""
        outputs = self.n_outputs if self.batched else 1
        length = max(1, int(self.n_features) * int(self.n_bins) * 2 * outputs)
        arr = np.zeros(length, dtype=np.float32)
        if values is not None:
            values = np.asarray(values, dtype=np.float32).ravel()
            n = min(len(values), length)
            if n:
                arr[:n] = values[:n]
        return ndarrays_to_parameters([arr])

    def _fixed_bin_edges(self):
        """Use deterministic bins for one-round secure histogram aggregation."""
        if self.n_features <= 0:
            return None
        edges = np.linspace(
            -self.fixed_bin_range,
            self.fixed_bin_range,
            self.n_bins + 1,
            dtype=np.float64,
        )[1:-1]
        return np.tile(edges, (self.n_features, 1))

    def _record_completed_tree(self):
        """Export the current in-memory tree into the completed tree list."""
        tree_dict = {
            "tree_id": self.current_tree,
            "class_idx": int(self.current_tree % self.n_outputs),
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

        # The active output/class for the tree currently being built. Cycles
        # 0..K-1 across trees so each class is boosted in turn.
        config: Dict[str, Scalar] = {
            "phase": self.phase,
            "class_idx": str(self.current_tree % self.n_outputs),
        }
        fit_params = parameters

        if self.phase == "bin_edges":
            # Send empty params; clients will return local bin edges
            fit_params = ndarrays_to_parameters([np.array([], dtype=np.float32)])

        elif self.phase == "histogram":
            node_id = self._get_current_histogram_node()
            config["node_id"] = str(node_id)
            if self.secure_stump_boost:
                config["secure_stump_boost"] = "true"
                config["tree_id"] = str(self.current_tree)
                if self.batched:
                    # All K per-class updates from the previous round, applied
                    # together before clients compute this round's histograms.
                    if self.pending_stump_updates:
                        config["apply_updates"] = json.dumps(self.pending_stump_updates)
                elif self.pending_stump_update is not None:
                    update = self.pending_stump_update
                    config["apply_tree_id"] = str(update["tree_id"])
                    config["apply_class_idx"] = str(update.get("class_idx", 0))
                    config["apply_feature"] = str(update["feature"])
                    config["apply_threshold"] = str(update["threshold"])
                    config["apply_left_leaf"] = str(update["left_leaf"])
                    config["apply_right_leaf"] = str(update["right_leaf"])
            # Send global bin edges
            if self.global_bin_edges is not None:
                if self.secure_stump_boost:
                    fit_params = self._fixed_parameters(self.global_bin_edges.ravel())
                else:
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
            edge_len = self.n_features * n_edges_per_feature
            stacked = np.stack([
                e[:edge_len].reshape(self.n_features, n_edges_per_feature)
                for e in all_edges
            ])
            self.global_bin_edges = np.mean(stacked, axis=0)

        # Start building the first tree
        self.current_tree = 0
        self._start_new_tree()
        self.phase = "histogram"

        return self._fixed_parameters(
            None if self.global_bin_edges is None else self.global_bin_edges.ravel()
        ), {}

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
            return self._fixed_parameters(), {}

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

        n_hist = self.n_features * self.n_bins

        # Batched multiclass: aggregated carries all K classes' histograms.
        # Build every per-class stump from this single secure round.
        if self.batched:
            self.nodes_to_split.popleft()
            return self._aggregate_secure_stumps_batched(aggregated, n_hist)

        if aggregated is None or len(aggregated) == 0:
            # No data - make this a leaf
            self.nodes_to_split.popleft()
            self.tree_leaf_values[node_id] = 0.0
            self._advance_after_node_processed()
            return self._fixed_parameters(), {}

        grad_flat = aggregated[:n_hist]
        hess_flat = aggregated[n_hist:(2 * n_hist)]

        grad_hist = grad_flat.reshape(self.n_features, self.n_bins)
        hess_hist = hess_flat.reshape(self.n_features, self.n_bins)

        best_gain, best_feature, best_bin, G_total, H_total = self._best_split(
            grad_hist, hess_hist
        )
        self.node_grad_sums[node_id] = (G_total, H_total)

        self.nodes_to_split.popleft()

        if self.secure_stump_boost:
            return self._aggregate_secure_stump(
                node_id=node_id,
                depth=depth,
                grad_hist=grad_hist,
                hess_hist=hess_hist,
                G_total=G_total,
                H_total=H_total,
                best_gain=best_gain,
                best_feature=best_feature,
                best_bin=best_bin,
            )

        if best_gain <= 0 or depth >= self.max_depth:
            # Make this a leaf
            leaf_value = -G_total / (H_total + self.reg_lambda)
            self.tree_leaf_values[node_id] = leaf_value
            if self.secure_single_round:
                self._record_completed_tree()
                self.current_tree += 1
                self._save_model()
                self._save_history()
                self.phase = "done"
                return self._fixed_parameters(), {}
            self._advance_after_node_processed()
            return self._fixed_parameters(), {}

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

        if self.secure_single_round:
            G_left = float(grad_hist[best_feature, :(best_bin + 1)].sum())
            H_left = float(hess_hist[best_feature, :(best_bin + 1)].sum())
            G_right = float(G_total - G_left)
            H_right = float(H_total - H_left)
            self.tree_leaf_values[left_child] = -G_left / (H_left + self.reg_lambda)
            self.tree_leaf_values[right_child] = -G_right / (H_right + self.reg_lambda)
            self._record_completed_tree()
            self.current_tree += 1
            self._save_model()
            self._save_history()
            self.phase = "done"
            return self._fixed_parameters(), {}

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
        split = self.tree_splits[node_id]
        split_arr = np.array([
            split["feature"],
            split["threshold"],
            node_id,
            split["left"],
            split["right"],
        ], dtype=np.float32)
        return self._fixed_parameters(split_arr), {}

    def _aggregate_secure_stump(
        self,
        node_id,
        depth,
        grad_hist,
        hess_hist,
        G_total,
        H_total,
        best_gain,
        best_feature,
        best_bin,
    ):
        """Build exactly one split-or-leaf tree from a secure root histogram."""
        self.nodes_to_split.clear()

        if best_gain <= 0 or depth >= self.max_depth or best_feature < 0:
            leaf_value = -G_total / (H_total + self.reg_lambda)
            self.tree_leaf_values[node_id] = leaf_value
            update = {
                "tree_id": self.current_tree,
                "class_idx": int(self.current_tree % self.n_outputs),
                "feature": -1,
                "threshold": 0.0,
                "left_leaf": float(leaf_value),
                "right_leaf": float(leaf_value),
            }
        else:
            left_child = 1
            right_child = 2
            threshold = float(self.global_bin_edges[best_feature, best_bin])

            G_left = float(grad_hist[best_feature, :(best_bin + 1)].sum())
            H_left = float(hess_hist[best_feature, :(best_bin + 1)].sum())
            G_right = float(G_total - G_left)
            H_right = float(H_total - H_left)
            left_leaf = -G_left / (H_left + self.reg_lambda)
            right_leaf = -G_right / (H_right + self.reg_lambda)

            self.tree_splits[node_id] = {
                "feature": int(best_feature),
                "threshold": threshold,
                "left": left_child,
                "right": right_child,
                "gain": float(best_gain),
            }
            self.tree_leaf_values[left_child] = float(left_leaf)
            self.tree_leaf_values[right_child] = float(right_leaf)
            update = {
                "tree_id": self.current_tree,
                "class_idx": int(self.current_tree % self.n_outputs),
                "feature": int(best_feature),
                "threshold": threshold,
                "left_leaf": float(left_leaf),
                "right_leaf": float(right_leaf),
            }

        self._record_completed_tree()
        self.current_tree += 1
        self.pending_stump_update = update

        if self.current_tree < self.n_trees:
            self._start_new_tree()
            self.phase = "histogram"
        else:
            self._save_model()
            self._save_history()
            self.phase = "done"

        return self._fixed_parameters(), {
            "secure_histogram_tree": float(self.current_tree),
            "secure_stump_gain": float(best_gain),
        }

    def _best_split(self, grad_hist, hess_hist):
        """Find the best (feature, bin) split for one node's histogram."""
        G_total = float(grad_hist.sum())
        H_total = float(hess_hist.sum())
        best_gain, best_feature, best_bin = 0.0, -1, -1
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
                    best_gain, best_feature, best_bin = gain, f, b
        return best_gain, best_feature, best_bin, G_total, H_total

    def _build_stump(self, tree_id, class_idx, grad_hist, hess_hist,
                     G_total, H_total, best_gain, best_feature, best_bin):
        """Build one boosted stump, record it as tree `tree_id`, return its
        apply-update (the leaf values clients use to advance their predictions)."""
        splits, leaves = {}, {}
        if best_gain <= 0 or best_feature < 0:
            leaf_value = -G_total / (H_total + self.reg_lambda)
            leaves["0"] = float(leaf_value)
            update = {
                "tree_id": tree_id, "class_idx": class_idx, "feature": -1,
                "threshold": 0.0, "left_leaf": float(leaf_value),
                "right_leaf": float(leaf_value),
            }
        else:
            threshold = float(self.global_bin_edges[best_feature, best_bin])
            G_left = float(grad_hist[best_feature, :(best_bin + 1)].sum())
            H_left = float(hess_hist[best_feature, :(best_bin + 1)].sum())
            G_right = G_total - G_left
            H_right = H_total - H_left
            left_leaf = -G_left / (H_left + self.reg_lambda)
            right_leaf = -G_right / (H_right + self.reg_lambda)
            splits["0"] = {
                "feature": int(best_feature), "threshold": threshold,
                "left": 1, "right": 2, "gain": float(best_gain),
            }
            leaves["1"] = float(left_leaf)
            leaves["2"] = float(right_leaf)
            update = {
                "tree_id": tree_id, "class_idx": class_idx,
                "feature": int(best_feature), "threshold": threshold,
                "left_leaf": float(left_leaf), "right_leaf": float(right_leaf),
            }
        self.completed_trees.append({
            "tree_id": tree_id, "class_idx": class_idx,
            "splits": splits, "leaves": leaves,
        })
        self.history.append({
            "tree": tree_id, "n_splits": len(splits), "n_leaves": len(leaves),
        })
        return update

    def _aggregate_secure_stumps_batched(self, aggregated, n_hist):
        """Build all K per-class stumps from one widened secure histogram.

        `aggregated` packs, per class c: grad_hist(F*B) then hess_hist(F*B).
        Each class's split is decided independently from its own SecAgg-summed
        histogram; the server never sees any single site's contribution.
        """
        per_class = n_hist * 2
        if aggregated is None:
            aggregated = np.zeros(per_class * self.n_outputs, dtype=np.float64)
        base = self.current_tree
        updates = []
        for c in range(self.n_outputs):
            off = c * per_class
            grad_hist = aggregated[off:off + n_hist].reshape(self.n_features, self.n_bins)
            hess_hist = aggregated[off + n_hist:off + per_class].reshape(self.n_features, self.n_bins)
            bg, bf, bb, G_total, H_total = self._best_split(grad_hist, hess_hist)
            updates.append(self._build_stump(
                base + c, c, grad_hist, hess_hist, G_total, H_total, bg, bf, bb
            ))
        self.current_tree += self.n_outputs
        self.pending_stump_updates = updates

        if self.current_tree < self.n_trees:
            self._start_new_tree()
            self.phase = "histogram"
        else:
            self._save_model()
            self._save_history()
            self.phase = "done"

        return self._fixed_parameters(), {
            "secure_batched_round": float(self.current_tree // self.n_outputs),
        }

    def _aggregate_split(self, results):
        """Split broadcast acknowledged. Move to next histogram or leaf."""
        self.current_split_node = None
        self._advance_after_node_processed()
        return self._fixed_parameters([1.0]), {}

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
        self._record_completed_tree()
        self.current_tree += 1

        if self.current_tree < self.n_trees:
            self._start_new_tree()
            self.phase = "histogram"
        else:
            # All trees done - save model
            self._save_model()
            self._save_history()
            self.phase = "done"

        return self._fixed_parameters([1.0]), {}

    def _save_model(self):
        """Save the completed model as JSON."""
        if self.evaluation_only:
            return
        model = {
            "model_type": "xgboost",
            "n_trees": len(self.completed_trees),
            "max_depth": self.max_depth,
            "learning_rate": self.learning_rate,
            "reg_lambda": self.reg_lambda,
            "objective": self.objective,
            "multiclass": self.multiclass,
            "n_classes": self.n_classes,
            "n_outputs": self.n_outputs,
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
    ) -> List[Tuple[ClientProxy, EvaluateIns]]:
        # Only evaluate after all trees are built
        if self.phase == "done" or (
            self.phase == "leaf" and self.current_tree >= self.n_trees - 1
        ):
            clients = client_manager.sample(
                num_clients=self.min_fit_clients,
                min_num_clients=self.min_fit_clients,
            )
            eval_ins = EvaluateIns(
                parameters=ndarrays_to_parameters([np.array([], dtype=np.float32)]),
                config={},
            )
            return [(client, eval_ins) for client in clients]
        return []

    def aggregate_evaluate(
        self,
        server_round: int,
        results: List[Tuple[ClientProxy, EvaluateRes]],
        failures: List[Union[Tuple[ClientProxy, EvaluateRes], BaseException]],
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

        self.history.append({
            "round": server_round,
            "loss": avg_loss,
            "n_clients": len(results),
            "n_failures": len(failures),
            **metrics,
        })
        self._save_history()

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
      Per tree: worst case histograms for every node in the full binary tree,
      split broadcasts for internal nodes, and one leaf broadcast:
        = (2^(max_depth + 1) - 1) histogram rounds
        + (2^max_depth - 1) split rounds
        + 1 leaf round
    """
    max_internal_nodes = (2 ** max_depth) - 1
    max_total_nodes = (2 ** (max_depth + 1)) - 1
    rounds_per_tree = max_total_nodes + max_internal_nodes + 1
    return 1 + n_trees * rounds_per_tree


def _compute_secure_num_rounds(n_trees):
    """One fixed-shape secure histogram aggregation per boosted stump."""
    return max(1, int(n_trees))


def server_fn(context: Context) -> ServerAppComponents:
    """Configure the server for histogram-based XGBoost."""
    cfg = context.run_config

    n_trees = int(cfg.get("n_trees", 10))
    max_depth = int(cfg.get("max_depth", 3))
    n_bins = int(cfg.get("n_bins", 64))
    reg_lambda = float(cfg.get("reg_lambda", 1.0))
    learning_rate = float(cfg.get("eta", 0.3))
    min_child_weight = float(cfg.get("min_child_weight", 1.0))
    objective = cfg.get("objective", "binary:logistic")
    num_class = int(cfg.get("num_class", 2))
    n_outputs = num_class if str(objective).startswith("multi") else 1
    n_features = int(cfg.get("n_features", 0))
    results_dir = cfg.get("results-dir", "/tmp/dsflower_results")
    require_secagg = str(cfg.get("require-secure-aggregation", "false")).lower() == "true"
    allow_per_node_metrics = str(
        cfg.get("allow-per-node-metrics", "true")
    ).lower() == "true"
    fixed_client_sampling = str(cfg.get("fixed-client-sampling", "false")).lower() == "true"
    evaluation_only = str(cfg.get("evaluation-only", "false")).lower() == "true"
    secure_stump_boost = require_secagg
    if secure_stump_boost and max_depth != 1:
        logger.warning(
            "Secure XGBoost profiles use boosted stumps; requested max_depth=%s "
            "will be treated as max_depth=1.",
            max_depth,
        )
        max_depth = 1
    secure_single_round = (
        secure_stump_boost and n_trees == 1 and max_depth == 1 and n_outputs == 1
    )

    # Round budget. Batched multiclass builds all K per-class stumps in ONE
    # secure round, so it needs only n_trees (boosting) rounds instead of
    # n_trees x n_outputs. Binary/regression keep n_trees rounds either way.
    # Opt-in (default off) until federated SecAgg+ validation lands.
    batch_multiclass = str(cfg.get("batch_multiclass", "false")).lower() == "true"
    batched = secure_stump_boost and n_outputs > 1 and batch_multiclass
    if secure_stump_boost:
        num_rounds = _compute_secure_num_rounds(n_trees if batched else n_trees * n_outputs)
    else:
        num_rounds = _compute_num_rounds(n_trees * n_outputs, max_depth)

    min_available = int(cfg.get("strategy-min_available_clients", 2))
    min_fit = int(cfg.get("strategy-min_fit_clients", 2))
    if fixed_client_sampling:
        min_fit = min_available

    strategy = SecureXGBoostStrategy(
        n_trees=n_trees,
        max_depth=max_depth,
        n_bins=n_bins,
        reg_lambda=reg_lambda,
        learning_rate=learning_rate,
        min_child_weight=min_child_weight,
        objective=objective,
        num_class=num_class,
        n_features=n_features,
        results_dir=results_dir,
        min_available_clients=min_available,
        min_fit_clients=min_fit,
        allow_per_node_metrics=allow_per_node_metrics,
        evaluation_only=evaluation_only,
        secure_single_round=secure_single_round,
        secure_stump_boost=secure_stump_boost,
        fixed_bin_range=float(cfg.get("fixed-bin-range", 4.0)),
        batch_multiclass=batch_multiclass,
    )

    config = ServerConfig(num_rounds=num_rounds)

    return ServerAppComponents(strategy=strategy, config=config)


def _requires_secagg(context: Context) -> bool:
    return str(context.run_config.get("require-secure-aggregation", "false")).lower() == "true"


def _run_server_app(grid, context: Context) -> None:
    components = server_fn(context)
    if _requires_secagg(context):
        from flwr.server.compat import LegacyContext
        from flwr.server.workflow import DefaultWorkflow, SecAggPlusWorkflow

        cfg = context.run_config
        num_clients = int(cfg.get("strategy-min_available_clients", 2))
        if num_clients < 3:
            raise RuntimeError("SecAgg+ requires at least three available clients.")
        legacy_context = LegacyContext(
            context,
            config=components.config,
            strategy=components.strategy,
            client_manager=components.client_manager,
        )
        workflow = DefaultWorkflow(
            fit_workflow=SecAggPlusWorkflow(
                num_shares=num_clients,
                reconstruction_threshold=max(2, num_clients - 1),
                max_weight=10.0,
                clipping_range=float(cfg.get("secagg-clipping-range", 4096.0)),
                quantization_range=int(cfg.get("secagg-quantization-range", 16777216)),
                modulus_range=int(cfg.get("secagg-modulus-range", 4294967296)),
            )
        )
        workflow(grid, legacy_context)
        return

    from flwr.server.compat import start_grid
    start_grid(
        server=components.server,
        config=components.config,
        strategy=components.strategy,
        client_manager=components.client_manager,
        grid=grid,
    )


app = ServerApp()
app.main()(_run_server_app)
