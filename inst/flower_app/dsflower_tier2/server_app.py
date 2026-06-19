"""Tier-2 trusted runner ServerApp (researcher side) — new Message API FedAvg.

Initializes the global model from the uploaded app's initial_arrays(), runs FedAvg
over the already-DP-gated client updates, and saves the result.
"""

import json
import os

import numpy as np

from flwr.serverapp import Grid, ServerApp
from flwr.serverapp.strategy import FedAvg
from flwr.common import ArrayRecord, Context

from .tier2_lib import load_user_module


app = ServerApp()


@app.main()
def main(grid: Grid, context: Context) -> None:
    cfg = context.run_config
    num_rounds = int(cfg.get("num-server-rounds", 1))
    n_features = int(cfg.get("num-features", 0))
    if n_features <= 0:
        raise ValueError("num-features must be set in the run config.")
    user_module = str(cfg.get("user-module", ""))
    if not user_module:
        raise ValueError("Tier-2 run requires 'user-module' in the run config.")
    min_nodes = int(cfg.get("min-train-nodes", 2))

    user_mod = load_user_module(user_module)
    init_arrays = [np.asarray(a, dtype=np.float32)
                   for a in user_mod.initial_arrays(dict(cfg), n_features)]
    initial = ArrayRecord(numpy_ndarrays=init_arrays)

    strategy = FedAvg(
        fraction_train=1.0,
        fraction_evaluate=0.0,
        min_train_nodes=min_nodes,
        min_evaluate_nodes=0,
        min_available_nodes=min_nodes,
    )
    result = strategy.start(grid=grid, initial_arrays=initial, num_rounds=num_rounds)
    _save_results(cfg, result)


def _save_results(cfg, result) -> None:
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)

    final_arrays = result.arrays.to_numpy_ndarrays()
    if not final_arrays:
        raise RuntimeError(
            "No client updates were aggregated (all ClientApps failed); "
            "nothing to save. Check the node-side ClientApp logs.")

    np.savez(os.path.join(results_dir, "model.npz"), *final_arrays)
    # also a JSON the R relay's watchdog recognizes as a completed model artifact
    with open(os.path.join(results_dir, "global_model.json"), "w") as f:
        json.dump({"model_type": "tier2", "arrays": [a.tolist() for a in final_arrays]}, f)

    num_rounds = int(cfg.get("num-server-rounds", 1))
    history = [{"round": r, "n_failures": 0} for r in range(1, num_rounds + 1)]
    with open(os.path.join(results_dir, "history.json"), "w") as f:
        json.dump(history, f)
