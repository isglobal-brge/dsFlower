"""dsFlower unified trusted ClientApp (node side) — always-on enforced DP.

The researcher ships only a model SPEC (a declarative nn.Module architecture, or
an XGBoost data-spec) + hyperparameters -- DATA, never code, so nothing the
researcher submits executes in this interpreter; this trusted, node-resident
harness owns every DP-critical step, so the guarantee cannot be bypassed. The
node-written, tamper-proof manifest pins the enforced-DP TRACK and all privacy +
sampling parameters; the client run config can only request, never weaken them.

Tracks, dispatched on the manifest's pinned ``dp-track``:
  * neural — Opacus DP-SGD over a client nn.Module (tabular, or an image sub-mode
    that trains only a head on FROZEN-backbone features). Per-sample clip + noise;
    the loss is harness-owned; the released state_dict is stash-gated.
  * trees  — enforced DP-GBDT (S-GBDT mechanism): random-split trees with the full
    Gaussian noise added node-side to each leaf histogram (local DP), then a
    booster the untrusted ServerApp bags by post-processing.
  * egress — the labelled-weaker fallback: the client's own local_update, wrapped
    in output-perturbation DP (whole-update clip + Gaussian noise). Admission-gated.
"""

import json

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from flwr.clientapp import ClientApp
from flwr.common import ArrayRecord, Context, Message, MetricRecord, RecordDict

from .task import (load_data, load_image_collection, is_image_run,
                   load_privacy_config, load_dp_track, load_run_pins,
                   load_gbdt_spec, load_tabular_patient_ids)
from .params import get_torch_params, set_torch_params, load_user_model

try:
    import dp_harness
    import dp_gbdt
except ImportError:
    from . import dp_harness, dp_gbdt


app = ClientApp()


# --------------------------------------------------------------------------- #
# Neural track (Opacus DP-SGD)
# --------------------------------------------------------------------------- #

def _prep_target(y, loss_name):
    """Target tensor shaped for the harness-owned loss."""
    if loss_name == "cross_entropy":
        return torch.from_numpy(y).long()              # [N], multi-logit output
    if loss_name == "multilabel_bce":
        return torch.from_numpy(y).float()             # [N, L]
    return torch.from_numpy(y).float().unsqueeze(1)    # [N, 1] (bce/mse/poisson)


def _dp_fit(model, X, y, pcfg, pins, msg, n_staged):
    """Opacus DP-SGD with the harness-owned loss + manifest-pinned sampling/horizon.
    Every input to the noise calibration (clip C, epsilon, delta, batch size, local
    epochs, rounds, sample count) is authoritative from the manifest, never the
    client run config -- so the client cannot stretch the composition horizon while
    the ledger debits a fixed budget."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    loss_name = pins["loss_name"]
    batch_size = int(pins["batch_size"])
    local_epochs = int(pins["local_epochs"])
    num_rounds = int(pins["num_rounds"])

    model = model.to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=float(pins["learning_rate"]))
    dataset = TensorDataset(torch.from_numpy(X).float(), _prep_target(y, loss_name))
    # Anti-shrink: the manifest n_samples is the server-recorded count of the STAGED
    # (pre-pool) frame; assert it matches so the client can't shrink the accountant
    # denominator. The accountant below uses len(dataset) (POST-pool = per-patient DP
    # unit), the correct DP n; n_staged is pre-pool and equals the manifest, so legit
    # per-patient pooling (which reduces len(dataset)) does not trip this check.
    if pcfg.get("n_samples") and int(pcfg["n_samples"]) != int(n_staged):
        raise RuntimeError("staged sample count != manifest n_samples (fail closed)")
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model, optimizer, trainloader, _engine = dp_harness.make_private_dpsgd(
        model, optimizer, trainloader,
        clipping_norm=pcfg["clipping_norm"], epsilon=pcfg["epsilon"],
        delta=pcfg["delta"], local_epochs=local_epochs, num_rounds=num_rounds,
        n_samples=len(dataset), batch_size=batch_size)

    set_torch_params(model, msg.content["arrays"].to_numpy_ndarrays())

    criterion = dp_harness.loss_from_allowlist(loss_name)   # node allowlist, mean reduction
    model.train()
    for _ in range(local_epochs):
        for xb, yb in trainloader:
            xb, yb = xb.to(device), yb.to(device)
            clean = [p.detach().clone() for p in model.parameters()]
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            # Defense-in-depth for the param-stash (A1): undo ANY in-place write the
            # forward made to the leaf params BEFORE the optimizer steps, so weights
            # evolve ONLY via the (noised) DP-SGD update, never a forward side effect.
            # (assert_stock_architecture already forbids custom forwards; this guards
            # a missed vector -- the grad_sample Opacus uses is untouched by .data.)
            with torch.no_grad():
                for p, c in zip(model.parameters(), clean):
                    p.copy_(c)
            optimizer.step()
    # RELEASE-TIME gate (the load-time assert_releasable is NOT enough on its own:
    # a buffer / frozen param / new parameter registered lazily inside the
    # researcher's forward appears only AFTER load, and Opacus noises ONLY the
    # gradients of the parameters that existed when make_private was called -- so a
    # lazily-stashed tensor would ship raw + un-noised in the state_dict). Re-assert
    # releasability on the ACTUAL post-training module AND require its released
    # key-set to be EXACTLY the set validated at load. Fail closed on any drift.
    released = getattr(model, "_module", model)
    dp_harness.assert_releasable(released)
    expected = getattr(released, "_dsflower_release_keys", None)
    current = tuple(n for n, _ in torch.nn.Module.named_parameters(released))
    if expected is None or current != tuple(expected):
        raise RuntimeError(
            "released tensor set changed during training (weight-stash channel: a "
            "lazily-added buffer or parameter); refusing to release un-noised data.")
    return get_torch_params(model), len(dataset)


def _assert_label_range(y, n_classes):
    """The head width is fixed by num-classes; verify the labels fit it. Generic
    message (no exact counts -> no disclosure)."""
    if not len(y):
        raise RuntimeError("no labelled samples to train on")
    max_label = int(np.nanmax(y))
    n_distinct = int(np.unique(y[np.isfinite(y)]).size)
    if int(n_classes) <= 2:
        if max_label > 1 or n_distinct > 2:
            raise RuntimeError(
                "label/num-classes mismatch: a binary model but the labels are not "
                "in {0,1}. Set the model's n_classes to your class count.")
    elif max_label >= int(n_classes):
        raise RuntimeError(
            "label/num-classes mismatch: a label exceeds num-classes. Set the "
            "model's n_classes to at least your class count.")


def _pool_by_patient(X, y, groups):
    """Per-PATIENT DP: mean-pool features per patient (one DP example per patient).
    Returns (X_pooled, y_pooled, True) when labels are patient-level; (None,None,False)
    when any patient mixes labels (pooling would corrupt them, so keep per-row)."""
    g = [("" if gv is None else str(gv)) for gv in groups]
    g = np.asarray([gv if (gv and gv.lower() != "nan") else f"__row_{i}"
                    for i, gv in enumerate(g)], dtype=object)
    Xp, yp = [], []
    for key in dict.fromkeys(g.tolist()):
        m = g == key
        labs = np.unique(y[m])
        if labs.size > 1:
            return None, None, False
        Xp.append(X[m].mean(axis=0))
        yp.append(labs[0])
    return np.stack(Xp), np.asarray(yp, dtype=y.dtype), True


def _train_neural(msg, context, cfg, pcfg, pins):
    manifest_image = is_image_run(context)
    cfg_image = str(cfg.get("data-kind", "")).lower() == "image"
    if manifest_image != cfg_image:
        raise RuntimeError(
            "data-kind mismatch: run config says "
            + ("image" if cfg_image else "tabular")
            + " but this node's data is "
            + ("an image collection" if manifest_image else "tabular")
            + ". Use a vision model for imaging collections, a tabular model otherwise.")
    n_classes = int(pins["n_classes"])

    if manifest_image:
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        image_size = int(cfg.get("image-size", 224))
        paths, y, groups = load_image_collection(context)
        n_staged = len(y)                      # pre-pool staged count (== manifest n_samples)
        _assert_label_range(y, n_classes)
        encoder, feat_dim = vision.build_backbone(backbone)
        read = vision.read_image_3d if vision.is_3d_backbone(backbone) else vision.read_image_2d
        try:
            images = [read(p, image_size) for p in paths]      # the ONLY pass over pixels
        except Exception as e:
            raise RuntimeError(f"failed to read an image in the collection: {e}")
        if not images:
            raise RuntimeError("no images could be read from the collection")
        X = vision.extract_features(encoder, images)           # frozen, no grad
        del images
        if not np.all(np.isfinite(X)):
            raise RuntimeError("non-finite features (corrupt backbone weights or inputs?)")
        if groups is not None:
            Xp, yp, pooled = _pool_by_patient(X, y, groups)
            if pooled:
                X, y = Xp, yp
        # The trainable head is node-built from the researcher's spec, with the
        # frozen-backbone feature dim as @in (default spec = nn.Linear(feat_dim, @out)).
        model = load_user_model(cfg, feat_dim, pins["loss_name"])
    else:
        X, y = load_data(context)
        n_staged = len(y)                      # pre-pool staged count (== manifest n_samples)
        _assert_label_range(y, n_classes)
        groups = load_tabular_patient_ids(context)
        if groups is not None:
            Xp, yp, pooled = _pool_by_patient(X, y, groups)
            if pooled:
                X, y = Xp, yp
        model = load_user_model(cfg, X.shape[1], pins["loss_name"])

    # Defense in depth: probe per-sample independence before training. The model is
    # node-built from the allowlisted spec vocabulary (all per-sample-safe), so this
    # passes by construction -- but it stays as a fail-closed backstop against a
    # build_from_spec bug ever admitting a layer that couples samples (x - x.mean(0),
    # batch attention, cdist(x, x)) and silently breaking the per-sample DP-SGD
    # sensitivity bound. Cheap; a Linear/ReLU head passes it trivially.
    import copy
    k = min(8, len(X))
    xb = torch.from_numpy(X[:k]).float()
    yb = _prep_target(y[:k], pins["loss_name"])
    # Probe a DEEPCOPY: the probe wraps the model with Opacus to read per-sample
    # gradients, which leaves grad-sample hooks behind; the real model must reach
    # make_private clean (else Opacus raises "Trying to add hooks twice").
    dp_harness.per_sample_independence_probe(
        copy.deepcopy(model), dp_harness.loss_from_allowlist(pins["loss_name"]), xb, yb)

    return _dp_fit(model, X, y, pcfg, pins, msg, n_staged)


# --------------------------------------------------------------------------- #
# Trees track (enforced DP-GBDT, local DP node-side)
# --------------------------------------------------------------------------- #

def _booster_to_array(booster):
    """Serialize the booster (our own format) to a uint8 array for transport."""
    return np.frombuffer(json.dumps(booster).encode("utf-8"), dtype=np.uint8)


def _train_trees(context, pcfg):
    spec = load_gbdt_spec(context)                 # validated, manifest-authoritative
    X, y = load_data(context)
    pids = load_tabular_patient_ids(context)       # per-patient DP unit when present
    booster = dp_gbdt.fit_dp_gbdt(
        X, y, objective=spec["objective"], depth=spec["max_depth"],
        n_trees=spec["n_trees"], learning_rate=spec["learning_rate"],
        reg_lambda=spec["reg_lambda"], feature_ranges=spec["feature_ranges"],
        n_bins=spec["n_bins"], run_token=spec["run_token"],
        epsilon=pcfg["epsilon"], delta=pcfg["delta"], patient_ids=pids)
    n = len(y) if pids is None else int(np.unique(pids).size)
    return [_booster_to_array(booster)], n


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

@app.train()
def train(msg: Message, context: Context) -> Message:
    cfg = dict(context.run_config)
    pcfg = load_privacy_config(context)
    track = load_dp_track(context)                 # node-pinned, NOT trusted from pyproject

    if track == "trees":
        new_arrays, n = _train_trees(context, pcfg)
    elif track == "egress":
        from . import tier2_lib
        X, y = load_data(context)
        user_mod = tier2_lib.load_user_module(str(cfg["user-module"]))
        old = msg.content["arrays"].to_numpy_ndarrays()
        new_arrays = tier2_lib.gated_local_update(user_mod, old, X, y, cfg, pcfg)
        n = len(y)
    else:  # neural (tabular or image)
        pins = load_run_pins(context)
        new_arrays, n = _train_neural(msg, context, cfg, pcfg, pins)

    # Disclosure backstop: bucket the released sample count; no per-node metrics.
    reply = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=new_arrays),
        "metrics": MetricRecord({"num-examples": dp_harness.bucket_count(n)}),
    })
    return Message(content=reply, reply_to=msg)
