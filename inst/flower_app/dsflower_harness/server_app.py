"""Tier-1 harness ServerApp (researcher side) — new Message API FedAvg.

Initializes the global model from the run config (same architecture the nodes
build), runs FedAvg over the already-DP client updates, and writes the final
model + history into results-dir for the R relay's artifact watchdog to collect.
No Secure Aggregation: the mean of locally-private updates is post-processing, so
the per-node DP guarantee carries through to the aggregate.
"""

import json
import os

import torch

from flwr.serverapp import Grid, ServerApp
from flwr.serverapp.strategy import FedAvg
from flwr.common import ArrayRecord, Context

from .model_zoo import build_model, get_torch_params, set_torch_params


app = ServerApp()


def _build_initial_model(cfg):
    """Build the global model the nodes will train. For an image run this is the
    small HEAD over frozen-backbone features (feature_dim fixed by the backbone, so
    the ServerApp needs no images); for tabular it is the configured model."""
    if str(cfg.get("data-kind", "")).lower() == "image":
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        n_classes = int(cfg.get("num-classes", 2))
        return vision.build_head(vision.feature_dim_for(backbone), n_classes)
    n_features = int(cfg.get("num-features", 0))
    if n_features <= 0:
        raise ValueError(
            "num-features must be set in the run config "
            "(the researcher passes len(feature_columns))."
        )
    return build_model(cfg, input_dim=n_features)


@app.main()
def main(grid: Grid, context: Context) -> None:
    cfg = context.run_config
    num_rounds = int(cfg.get("num-server-rounds", 1))
    min_nodes = int(cfg.get("min-train-nodes", 2))

    model = _build_initial_model(cfg)
    initial = ArrayRecord(numpy_ndarrays=get_torch_params(model))

    strategy = FedAvg(
        fraction_train=1.0,
        fraction_evaluate=0.0,
        min_train_nodes=min_nodes,
        min_evaluate_nodes=0,
        min_available_nodes=min_nodes,
    )
    result = strategy.start(
        grid=grid,
        initial_arrays=initial,
        num_rounds=num_rounds,
    )

    _save_results(cfg, model, result)


def _save_results(cfg, model, result) -> None:
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)

    final_arrays = result.arrays.to_numpy_ndarrays()
    if not final_arrays:
        # All client updates failed -> nothing aggregated. Raise so the run is
        # reported as a failure (the R relay detects the ServerApp exception)
        # rather than silently succeeding with an empty model.
        raise RuntimeError(
            "No client updates were aggregated (all ClientApps failed); "
            "nothing to save. Check the node-side ClientApp logs."
        )

    set_torch_params(model, final_arrays)
    torch.save(model.state_dict(), os.path.join(results_dir, "model.pt"))

    # One row per round (1..num_rounds) so the relay's artifact watchdog always
    # sees the final round, even when aggregated metrics are absent (we suppress
    # per-node metrics, so train_metrics_clientapp may be empty by design).
    per_round = dict(result.train_metrics_clientapp or {})
    num_rounds = int(cfg.get("num-server-rounds", 1))
    history = []
    for rnd in range(1, num_rounds + 1):
        row = {"round": rnd, "n_failures": 0}
        mrec = per_round.get(rnd)
        if mrec is not None:
            try:
                if "num-examples" in mrec:
                    row["n_examples"] = mrec["num-examples"]
            except Exception:
                pass
        history.append(row)
    with open(os.path.join(results_dir, "history.json"), "w") as f:
        json.dump(history, f)
