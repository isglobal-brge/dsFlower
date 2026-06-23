"""Tier-1 trusted ClientApp (node side) — always-on DP-SGD, new Message API.

Runs on each DataSHIELD node's SuperNode. The researcher supplies only the model
architecture + hyperparameters (run config) and the DP budget (tamper-proof
manifest); this trusted harness owns the training loop, so per-example DP is
applied unconditionally and cannot be bypassed by the submitted app.

Two data paths, dispatched on the staged manifest's data_type:
  * tabular: build the model, DP-SGD over the raw features.
  * image (dsImaging collection): extract features with a FROZEN backbone (no
    grad), then DP-SGD only the small head on the features. The frozen backbone
    never enters the trainable graph (so no BatchNorm problem) and the released
    update is a low-dim feature-space head gradient — the disclosure vector for
    images, which Opacus noises and which is far harder to invert into pixels than
    a full-network gradient (dsFlower has no Secure Aggregation).
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from flwr.clientapp import ClientApp
from flwr.common import ArrayRecord, Context, Message, MetricRecord, RecordDict

from .task import load_data, load_image_collection, is_image_run, load_privacy_config
from .model_zoo import build_model, get_torch_params, set_torch_params

try:
    import dp_harness
except ImportError:
    from . import dp_harness


app = ClientApp()


def _dp_fit(model, X, y, pcfg, cfg, n_classes, msg):
    """Opacus DP-SGD fit shared by the tabular + image (head) paths. Trains on the
    GPU when one is available (model + batches on the same device), else CPU."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    lr = float(cfg.get("learning-rate", 0.01))
    batch_size = int(cfg.get("batch-size", 32))
    local_epochs = int(cfg.get("local-epochs", 1))
    num_rounds = int(cfg.get("num-server-rounds", 1))
    multiclass = int(n_classes) > 2

    model = model.to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    y_t = torch.from_numpy(y).long() if multiclass else torch.from_numpy(y).float()
    dataset = TensorDataset(torch.from_numpy(X).float(), y_t)
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model, optimizer, trainloader, _engine = dp_harness.make_private_dpsgd(
        model, optimizer, trainloader,
        clipping_norm=pcfg["clipping_norm"], epsilon=pcfg["epsilon"],
        delta=pcfg["delta"], local_epochs=local_epochs, num_rounds=num_rounds,
        n_samples=len(dataset), batch_size=batch_size)

    set_torch_params(model, msg.content["arrays"].to_numpy_ndarrays())

    criterion = nn.CrossEntropyLoss() if multiclass else nn.BCEWithLogitsLoss()
    model.train()
    for _ in range(local_epochs):
        for xb, yb in trainloader:
            xb, yb = xb.to(device), yb.to(device)
            optimizer.zero_grad()
            out = model(xb)
            loss = criterion(out, yb) if multiclass else criterion(out, yb.unsqueeze(1))
            loss.backward()
            optimizer.step()
    return get_torch_params(model), len(dataset)


def _assert_label_range(y, n_classes):
    """The head width is fixed by num-classes; verify the labels fit it. Generic
    message (no exact counts -> no disclosure): binary BCE would SILENTLY mislearn
    a stray label (e.g. 1-indexed {1,2}); multiclass CE would throw cryptically."""
    if not len(y):
        raise RuntimeError("no labelled samples to train on")
    max_label = int(np.nanmax(y))
    n_distinct = int(np.unique(y[np.isfinite(y)]).size)
    if int(n_classes) <= 2:
        if max_label > 1 or n_distinct > 2:
            raise RuntimeError(
                "label/num-classes mismatch: a binary model but the labels are "
                "not in {0,1}. Set the model's n_classes to your class count.")
    elif max_label >= int(n_classes):
        raise RuntimeError(
            "label/num-classes mismatch: a label exceeds num-classes. Set the "
            "model's n_classes to at least your class count.")


def _pool_by_patient(X, y, groups):
    """Per-PATIENT DP: mean-pool features per patient (one DP example per patient).
    Returns (X_pooled, y_pooled, True) when labels are patient-level (one label per
    patient); returns (None, None, False) when any patient mixes labels (slice-level
    labels -> pooling would corrupt them, so keep per-image). A missing patient id
    becomes a singleton (that image trains as its own per-image unit)."""
    g = [("" if gv is None else str(gv)) for gv in groups]
    g = np.asarray([gv if (gv and gv.lower() != "nan") else f"__img_{i}"
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


@app.train()
def train(msg: Message, context: Context) -> Message:
    cfg = dict(context.run_config)
    pcfg = load_privacy_config(context)

    # The authoritative data kind is the node-side manifest. If the researcher's
    # run config disagrees (e.g. a tabular model was picked for an imaging
    # collection, or vice versa), the ServerApp built a mismatched initial model;
    # fail with a CLEAR message here rather than a cryptic load_state_dict shape
    # error. The ServerApp can only see the run config (it has no node manifest),
    # so this node-side cross-check is where the two signals first meet.
    manifest_image = is_image_run(context)
    cfg_image = str(cfg.get("data-kind", "")).lower() == "image"
    if manifest_image != cfg_image:
        raise RuntimeError(
            "data-kind mismatch: the run config says "
            + ("image" if cfg_image else "tabular")
            + " but this node's data is "
            + ("an image collection" if manifest_image else "tabular")
            + ". Use a vision model (pytorch_resnet18 / pytorch_densenet121) for "
            "imaging collections, or a tabular model otherwise.")

    if manifest_image:
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        n_classes = int(cfg.get("num-classes", 2))
        image_size = int(cfg.get("image-size", 224))

        # Vision batch-size floor (anti gradient-inversion: batches < 32 are "not
        # safe"). Clamp UP, never below; this complements the admission min-count.
        floor = int(cfg.get("vision-batch-floor", 32))
        if int(cfg.get("batch-size", 32)) < floor:
            cfg["batch-size"] = floor

        paths, y, groups = load_image_collection(context)   # paths + labels + patient ids
        _assert_label_range(y, n_classes)

        encoder, feat_dim = vision.build_backbone(backbone)
        read = vision.read_image_3d if vision.is_3d_backbone(backbone) else vision.read_image_2d
        try:
            images = [read(p, image_size) for p in paths]    # the ONLY pass over pixels
        except Exception as e:
            raise RuntimeError(f"failed to read an image in the collection: {e}")
        if not images:
            raise RuntimeError("no images could be read from the collection")
        X = vision.extract_features(encoder, images)     # frozen, no grad -> features
        del images
        if not np.all(np.isfinite(X)):
            raise RuntimeError(
                "non-finite features extracted (corrupt backbone weights or inputs?)")

        # Per-PATIENT DP: when a patient column is present and labels are
        # patient-level, mean-pool features per patient so the DP example IS the
        # patient -> the formal DP unit matches the per-patient admission unit
        # (closes the group-privacy gap). Slice-level labels keep per-image.
        if groups is not None:
            Xp, yp, pooled = _pool_by_patient(X, y, groups)
            if pooled:
                X, y = Xp, yp

        model = vision.build_head(feat_dim, n_classes)   # the only trainable/communicated module
        new_arrays, n = _dp_fit(model, X, y, pcfg, cfg, n_classes, msg)
    else:
        X, y = load_data(context)
        _assert_label_range(y, 2)                        # tabular harness models are binary
        model = build_model(cfg, input_dim=X.shape[1])
        new_arrays, n = _dp_fit(model, X, y, pcfg, cfg, n_classes=2, msg=msg)

    # Disclosure backstop: bucket the released sample count; no per-node metrics.
    n_examples = dp_harness.bucket_count(n)
    reply = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=new_arrays),
        "metrics": MetricRecord({"num-examples": n_examples}),
    })
    return Message(content=reply, reply_to=msg)
