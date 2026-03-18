"""Flower ServerApp with multi-strategy support and privacy-aware aggregation."""

import json
import os
from pathlib import Path

import numpy as np
from flwr.common import Context, ndarrays_to_parameters, parameters_to_ndarrays
from flwr.server import ServerApp, ServerAppComponents, ServerConfig
from flwr.server.strategy import FedAvg, FedProx, FedAdam, FedAdagrad

_STRATEGY_MAP = {
    "FedAvg": FedAvg,
    "FedProx": FedProx,
    "FedAdam": FedAdam,
    "FedAdagrad": FedAdagrad,
}


_ADAPTIVE_STRATEGIES = {FedAdam, FedAdagrad}


def _xgboost_bagging_aggregate(results):
    """Aggregate XGBoost models by merging their trees (bagging).

    Each client sends a single numpy array containing the raw JSON bytes of
    its XGBoost Booster model.  Standard FedAvg cannot work here because
    averaging raw bytes is meaningless and the arrays may have different
    lengths.

    Instead we parse each client's JSON model, collect all trees, and
    combine them into a single model.  The resulting global model contains
    every tree from every client.
    """
    if not results:
        return None

    all_trees = []
    base_model = None

    for fit_res_parameters, _num_examples in results:
        ndarrays = parameters_to_ndarrays(fit_res_parameters)
        if len(ndarrays) == 0 or len(ndarrays[0]) == 0:
            continue
        model_bytes = ndarrays[0].tobytes()
        model_json = json.loads(model_bytes)

        if base_model is None:
            base_model = model_json

        # XGBoost JSON format: model_json["learner"]["gradient_booster"]
        #   ["model"]["trees"] is a list of tree dicts.
        try:
            trees = (
                model_json["learner"]["gradient_booster"]["model"]["trees"]
            )
            all_trees.extend(trees)
        except (KeyError, TypeError):
            # Fallback: if the JSON structure is unexpected, just use the
            # first valid model as-is.
            if base_model is None:
                base_model = model_json
            continue

    if base_model is None:
        return None

    # Merge all trees into the base model and update tree count metadata.
    try:
        gbtree = base_model["learner"]["gradient_booster"]["model"]
        gbtree["trees"] = all_trees
        # Renumber tree IDs sequentially
        for idx, tree in enumerate(all_trees):
            tree["id"] = idx
        gbtree["gbtree_model_param"]["num_trees"] = str(len(all_trees))

        # Also update iteration_indptr which tracks trees per boosting
        # iteration. After bagging we treat every tree as one iteration.
        gbtree["tree_info"] = [0] * len(all_trees)
        gbtree["iteration_indptr"] = list(range(len(all_trees) + 1))
    except (KeyError, TypeError):
        pass

    merged_bytes = json.dumps(base_model).encode("utf-8")
    merged_array = np.frombuffer(merged_bytes, dtype=np.uint8).copy()
    return ndarrays_to_parameters([merged_array])


def _make_save_strategy(base_cls, results_dir, num_rounds,
                         allow_per_node_metrics=True, **kwargs):
    """Create a SaveModelStrategy that subclasses the given Flower strategy."""

    class _Strategy(base_cls):
        """Dynamic strategy that saves global model weights and metrics."""

        def __init__(self, results_dir, num_rounds, allow_per_node_metrics=True,
                     **kw):
            # FedAdam/FedAdagrad require initial_parameters but we don't
            # have model params at server init time. Pass a dummy value and
            # override initialize_parameters to fetch from a real client.
            if base_cls in _ADAPTIVE_STRATEGIES and "initial_parameters" not in kw:
                kw["initial_parameters"] = ndarrays_to_parameters([])
                self._needs_client_init = True
            else:
                self._needs_client_init = False
            super().__init__(**kw)
            self.results_dir = Path(results_dir)
            self.results_dir.mkdir(parents=True, exist_ok=True)
            self.num_rounds = num_rounds
            self.allow_per_node_metrics = allow_per_node_metrics
            self.history = []

        def initialize_parameters(self, client_manager):
            """Override to request real params from a client for adaptive strategies."""
            if self._needs_client_init:
                # Clear the dummy initial_parameters so Flower requests from a client.
                # This also forces FedAdam/FedAdagrad to initialize their internal
                # state (current_weights, m_t, v_t) from the real client params.
                self.initial_parameters = None
                return None
            return super().initialize_parameters(client_manager)

        def aggregate_fit(self, server_round, results, failures):
            """Aggregate XGBoost models via tree bagging instead of FedAvg.

            XGBoost models are serialized as raw JSON bytes.  Averaging
            those bytes (as FedAvg does) is invalid -- the arrays may differ
            in length and byte-level averaging destroys the JSON structure.

            Instead we parse each client's model, merge their trees, and
            return a single combined model.
            """
            if not results:
                return None, {}

            # Extract (Parameters, num_examples) pairs from results.
            params_and_counts = [
                (fit_res.parameters, fit_res.num_examples)
                for _, fit_res in results
            ]

            aggregated_parameters = _xgboost_bagging_aggregate(
                params_and_counts
            )

            aggregated_metrics = {}
            if aggregated_parameters is not None:
                weights = parameters_to_ndarrays(aggregated_parameters)
                self._save_weights(weights, server_round)
            return aggregated_parameters, aggregated_metrics

        def aggregate_evaluate(self, server_round, results, failures):
            loss, metrics = super().aggregate_evaluate(
                server_round, results, failures
            )
            self.history.append({
                "round": server_round,
                "loss": float(loss) if loss is not None else None,
                "n_clients": len(results),
                "n_failures": len(failures),
            })
            if server_round == self.num_rounds:
                self._save_history()

            if not self.allow_per_node_metrics:
                metrics = {}

            return loss, metrics

        def _save_weights(self, weights, server_round):
            data = {str(i): w.tolist() for i, w in enumerate(weights)}
            data["__shapes__"] = [list(w.shape) for w in weights]
            data["__round__"] = server_round
            path = self.results_dir / "global_model.json"
            with open(path, "w") as f:
                json.dump(data, f)

        def _save_history(self):
            path = self.results_dir / "history.json"
            with open(path, "w") as f:
                json.dump(self.history, f)

    return _Strategy(
        results_dir=results_dir,
        num_rounds=num_rounds,
        allow_per_node_metrics=allow_per_node_metrics,
        **kwargs,
    )


def server_fn(context: Context) -> ServerAppComponents:
    """Configure the server with the requested aggregation strategy."""
    cfg = context.run_config

    num_rounds = int(cfg.get("num-server-rounds", 5))
    results_dir = cfg.get("results-dir", "/tmp/dsflower_results")
    require_secagg = str(cfg.get("require-secure-aggregation", "false")).lower() == "true"
    allow_per_node_metrics = str(cfg.get("allow-per-node-metrics", "true")).lower() == "true"

    strategy_name = cfg.get("strategy", "FedAvg")
    base_cls = _STRATEGY_MAP.get(strategy_name)
    if base_cls is None:
        raise ValueError(
            f"Unknown strategy '{strategy_name}'. "
            f"Supported: {', '.join(_STRATEGY_MAP)}"
        )

    strategy_kwargs = {
        "fraction_fit": float(cfg.get("strategy-fraction_fit", 1.0)),
        "fraction_evaluate": float(cfg.get("strategy-fraction_evaluate", 1.0)),
        "min_fit_clients": int(cfg.get("strategy-min_fit_clients", 2)),
        "min_available_clients": int(cfg.get("strategy-min_available_clients", 2)),
    }

    if strategy_name == "FedProx":
        strategy_kwargs["proximal_mu"] = float(cfg.get("strategy-proximal_mu", 0.1))
    elif strategy_name in ("FedAdam", "FedAdagrad"):
        strategy_kwargs["eta"] = float(cfg.get("strategy-eta", 0.01))
        strategy_kwargs["tau"] = float(cfg.get("strategy-tau", 1e-3))

    strategy = _make_save_strategy(
        base_cls,
        results_dir=results_dir,
        num_rounds=num_rounds,
        allow_per_node_metrics=allow_per_node_metrics,
        **strategy_kwargs,
    )

    config = ServerConfig(num_rounds=num_rounds)

    if require_secagg:
        from flwr.server.workflow import SecAggPlusWorkflow, DefaultWorkflow
        return ServerAppComponents(
            strategy=strategy, config=config,
            workflow=SecAggPlusWorkflow()
        )

    return ServerAppComponents(strategy=strategy, config=config)


app = ServerApp(server_fn=server_fn)
