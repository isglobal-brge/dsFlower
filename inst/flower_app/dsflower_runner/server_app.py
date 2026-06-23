"""dsFlower unified ServerApp (researcher side) — new Message API.

UNTRUSTED by design: this runs on the researcher's machine. It performs NO DP
work and never sees raw per-node data — only already-private node releases, which
it post-processes. Dispatch is on the run config's ``dp-track`` (the node
re-checks its OWN manifest-pinned track, so a mismatch fails closed node-side):

  * neural / egress — FedAvg over the already-DP client updates (the mean of
    locally-private updates is post-processing, so the per-node guarantee carries
    to the aggregate; no Secure Aggregation needed).
  * trees — a bespoke one-round summation loop over the Grid: collect each node's
    already-noised DP-GBDT booster and BAG them (concatenate trees, scale weights
    by 1/M). Never FedAvg (it would average mismatched boosters).

Final artifacts go to results-dir for the R relay's watchdog: model.pt + history
(neural/egress), or booster.json + history (trees).
"""

import importlib
import json
import os

import numpy as np
import torch

from flwr.serverapp import Grid, ServerApp
from flwr.serverapp.strategy import FedAvg
from flwr.common import ArrayRecord, Context, RecordDict

from .params import get_torch_params, set_torch_params

app = ServerApp()


# --------------------------------------------------------------------------- #
# Initial model (neural) — import the client's build_model for the array shapes.
# --------------------------------------------------------------------------- #

def _build_initial_model(cfg):
    """Build the global model the nodes will train, from the client-shipped
    build_model (bundled in the FAB). Researcher-side + untrusted: this only seeds
    the initial array SHAPES (random init); the node enforces all DP + hardening."""
    mod = importlib.import_module(str(cfg["model-module"]))
    bcfg = dict(cfg)
    if str(cfg.get("data-kind", "")).lower() == "image":
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        bcfg["feature-dim"] = int(vision.feature_dim_for(backbone))
    else:
        n = int(cfg.get("num-features", 0))
        if n <= 0:
            raise ValueError("num-features must be set in the run config "
                             "(the researcher passes len(feature_columns)).")
        bcfg["num-features"] = n
    model = mod.build_model(bcfg)
    if not isinstance(model, torch.nn.Module):
        raise ValueError("build_model must return a torch.nn.Module")
    return model


# --------------------------------------------------------------------------- #
# Trees — bespoke one-round bagging over already-noised boosters.
# --------------------------------------------------------------------------- #

def _array_to_booster(arr):
    return json.loads(bytes(np.asarray(arr, dtype=np.uint8)).decode("utf-8"))


def _bag_boosters(boosters):
    """Bag M already-DP boosters: concatenate their trees, scale every leaf weight
    by 1/M (so prediction = mean of the per-node ensembles). Pure post-processing
    of already-private releases over disjoint row sets."""
    m = len(boosters)
    merged = {k: v for k, v in boosters[0].items() if k != "trees"}
    merged["n_boosters"] = m
    merged["trees"] = []
    for b in boosters:
        for tree in b.get("trees", []):
            merged["trees"].append({
                "feat": tree["feat"], "thr": tree["thr"],
                "w": [float(w) / m for w in tree["w"]],
            })
    return merged


def _run_trees(grid, cfg):
    min_nodes = int(cfg.get("min-train-nodes", 2))
    node_ids = [n for n in grid.get_node_ids()]
    if len(node_ids) < min_nodes:
        raise RuntimeError("only %d node(s) available, need >= %d for the trees run"
                           % (len(node_ids), min_nodes))
    # Each node trains its FULL local DP-GBDT in one round (the booster's n_trees is
    # the node-internal boosting; one message exchange suffices). No init model.
    empty = RecordDict({"arrays": ArrayRecord(numpy_ndarrays=[np.zeros(1, dtype=np.float64)])})
    messages = [grid.create_message(empty, "train", nid, group_id="dsflower-trees")
                for nid in node_ids]
    replies = grid.send_and_receive(
        messages, timeout=float(cfg.get("round-timeout", 3600)))

    boosters = []
    for r in replies:
        try:
            if r.has_error():
                continue
            arrays = r.content["arrays"].to_numpy_ndarrays()
            if arrays:
                boosters.append(_array_to_booster(arrays[0]))
        except Exception:
            continue
    if not boosters:
        raise RuntimeError(
            "no DP-GBDT boosters returned (all ClientApps failed); check node logs.")
    return _bag_boosters(boosters)


def _save_trees(cfg, booster):
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "booster.json"), "w") as f:
        json.dump(booster, f)
    with open(os.path.join(results_dir, "history.json"), "w") as f:
        json.dump([{"round": 1, "n_failures": 0,
                    "n_trees": len(booster.get("trees", [])),
                    "n_boosters": booster.get("n_boosters", 1)}], f)


# --------------------------------------------------------------------------- #
# Neural / egress — FedAvg over already-private updates.
# --------------------------------------------------------------------------- #

def _initial_arrays(cfg, track):
    if track == "egress":
        from . import tier2_lib
        user_mod = tier2_lib.load_user_module(str(cfg["user-module"]))
        n = int(cfg.get("num-features", 0))
        arrays = [np.asarray(a, dtype=np.float64)
                  for a in user_mod.initial_arrays(dict(cfg), n)]
        return None, ArrayRecord(numpy_ndarrays=arrays)
    model = _build_initial_model(cfg)
    return model, ArrayRecord(numpy_ndarrays=get_torch_params(model))


def _run_fedavg(grid, cfg, track):
    num_rounds = int(cfg.get("num-server-rounds", 1))
    min_nodes = int(cfg.get("min-train-nodes", 2))
    model, initial = _initial_arrays(cfg, track)
    strategy = FedAvg(
        fraction_train=1.0, fraction_evaluate=0.0,
        min_train_nodes=min_nodes, min_evaluate_nodes=0,
        min_available_nodes=min_nodes)
    result = strategy.start(grid=grid, initial_arrays=initial, num_rounds=num_rounds)
    _save_results(cfg, model, result)


def _save_results(cfg, model, result):
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)
    final_arrays = result.arrays.to_numpy_ndarrays()
    if not final_arrays:
        raise RuntimeError(
            "No client updates were aggregated (all ClientApps failed); nothing to "
            "save. Check the node-side ClientApp logs.")
    if model is not None:
        set_torch_params(model, final_arrays)
        torch.save(model.state_dict(), os.path.join(results_dir, "model.pt"))
    else:  # egress: no torch model, save raw arrays
        np.savez(os.path.join(results_dir, "model.npz"), *final_arrays)

    num_rounds = int(cfg.get("num-server-rounds", 1))
    per_round = dict(result.train_metrics_clientapp or {})
    history = []
    for rnd in range(1, num_rounds + 1):
        row = {"round": rnd, "n_failures": 0}
        mrec = per_round.get(rnd)
        if mrec is not None and "num-examples" in mrec:
            row["n_examples"] = mrec["num-examples"]
        history.append(row)
    with open(os.path.join(results_dir, "history.json"), "w") as f:
        json.dump(history, f)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

@app.main()
def main(grid: Grid, context: Context) -> None:
    cfg = context.run_config
    track = str(cfg.get("dp-track", "neural")).lower()
    if track == "trees":
        _save_trees(cfg, _run_trees(grid, cfg))
    else:  # neural or egress
        _run_fedavg(grid, cfg, track)
