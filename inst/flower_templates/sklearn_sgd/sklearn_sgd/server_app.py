"""Flower ServerApp with multi-strategy support and privacy-aware aggregation."""

import json
import os
from pathlib import Path

import numpy as np
from flwr.common import Context, parameters_to_ndarrays
from flwr.server import ServerApp, ServerAppComponents, ServerConfig
from flwr.server.strategy import FedAvg, FedProx, FedAdam, FedAdagrad

_STRATEGY_MAP = {
    "FedAvg": FedAvg,
    "FedProx": FedProx,
    "FedAdam": FedAdam,
    "FedAdagrad": FedAdagrad,
}


def _make_save_strategy(base_cls, results_dir, num_rounds,
                         allow_per_node_metrics=True, **kwargs):
    """Create a SaveModelStrategy that subclasses the given Flower strategy."""

    class _Strategy(base_cls):
        """Dynamic strategy that saves global model weights and metrics."""

        def __init__(self, results_dir, num_rounds, allow_per_node_metrics=True,
                     **kw):
            super().__init__(**kw)
            self.results_dir = Path(results_dir)
            self.results_dir.mkdir(parents=True, exist_ok=True)
            self.num_rounds = num_rounds
            self.allow_per_node_metrics = allow_per_node_metrics
            self.history = []

        def aggregate_fit(self, server_round, results, failures):
            aggregated_parameters, aggregated_metrics = super().aggregate_fit(
                server_round, results, failures
            )
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
