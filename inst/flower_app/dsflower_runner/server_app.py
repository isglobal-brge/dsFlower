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

import json
import os

import numpy as np
import torch

from flwr.serverapp import Grid, ServerApp
from flwr.serverapp.strategy import FedAvg
from flwr.common import ArrayRecord, Context, Message, RecordDict

from .params import get_torch_params, set_torch_params

app = ServerApp()


# --------------------------------------------------------------------------- #
# Initial model (neural) — node-built from the researcher's spec (DATA, no code).
# --------------------------------------------------------------------------- #

def _build_initial_model(cfg):
    """Seed the global model's array SHAPES from the researcher's declarative spec
    (DATA, node-built by model_spec). Researcher-side + untrusted: random init only;
    the nodes rebuild from the same spec and enforce all DP + hardening. No
    researcher code is imported here."""
    try:
        import model_spec
    except ImportError:
        from . import model_spec
    if str(cfg.get("data-kind", "")).lower() == "image":
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        in_dim = int(vision.feature_dim_for(backbone))
    else:
        in_dim = int(cfg.get("num-features", 0))
        if in_dim <= 0:
            raise ValueError("num-features must be set in the run config "
                             "(the researcher passes len(feature_columns)).")
    spec = model_spec.read_spec(cfg)
    loss_name = str(cfg.get("loss-name", "bce_logits"))
    out_dim = model_spec.output_width(loss_name, cfg)
    num_labels = int(cfg["num-labels"]) if cfg.get("num-labels") is not None else None
    model = model_spec.build_from_spec(spec, in_dim=in_dim, out_dim=out_dim,
                                       num_labels=num_labels)
    if not isinstance(model, torch.nn.Module):
        raise ValueError("build_from_spec must return a torch.nn.Module")
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
    import time
    min_nodes = int(cfg.get("min-train-nodes", 2))
    timeout = float(cfg.get("round-timeout", 600))
    # SuperNodes dial in shortly AFTER the run starts. FedAvg waits for them
    # internally; the manual trees loop must too -- otherwise get_node_ids()
    # returns empty at t=0 and the run aborts (and, with the tunnel pump driven by
    # the client run wait-loop, can leave the run hanging). Poll until enough connect.
    node_ids, waited = [], 0.0
    while waited < timeout:
        node_ids = [n for n in grid.get_node_ids()]
        if len(node_ids) >= min_nodes:
            break
        time.sleep(2.0)
        waited += 2.0
    if len(node_ids) < min_nodes:
        raise RuntimeError("only %d node(s) connected after %ds, need >= %d"
                           % (len(node_ids), int(waited), min_nodes))
    # Each node trains its FULL local DP-GBDT in one round (the booster's n_trees is
    # the node-internal boosting; one message exchange suffices). No init model.
    empty = RecordDict({"arrays": ArrayRecord(numpy_ndarrays=[np.zeros(1, dtype=np.float64)])})
    # Build messages with the modern Message constructor (mirrors FedAvg's
    # _construct_messages) -- NOT the deprecated Grid.create_message, whose shim
    # builds a message the 1.3x SuperNodes don't route (the round-trip then hangs).
    messages = [Message(content=empty, message_type="train",
                        dst_node_id=nid, group_id="dsflower-trees")
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
