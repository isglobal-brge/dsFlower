"""Flower ServerApp with weight saving."""

import json
import os
from pathlib import Path

import numpy as np
from flwr.common import Context, parameters_to_ndarrays
from flwr.server import ServerApp, ServerAppComponents, ServerConfig
from flwr.server.strategy import FedAvg


class SaveModelStrategy(FedAvg):
    """FedAvg that saves global model weights and metrics after each round."""

    def __init__(self, results_dir, num_rounds, **kwargs):
        super().__init__(**kwargs)
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.num_rounds = num_rounds
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


def server_fn(context: Context) -> ServerAppComponents:
    """Configure the server."""
    cfg = context.run_config

    num_rounds = int(cfg.get("num-server-rounds", 5))
    results_dir = cfg.get("results-dir", "/tmp/dsflower_results")

    strategy = SaveModelStrategy(
        results_dir=results_dir,
        num_rounds=num_rounds,
        fraction_fit=float(cfg.get("strategy-fraction_fit", 1.0)),
        fraction_evaluate=float(cfg.get("strategy-fraction_evaluate", 1.0)),
        min_fit_clients=int(cfg.get("strategy-min_fit_clients", 2)),
        min_available_clients=int(cfg.get("strategy-min_available_clients", 2)),
    )

    config = ServerConfig(num_rounds=num_rounds)
    return ServerAppComponents(strategy=strategy, config=config)


app = ServerApp(server_fn=server_fn)
