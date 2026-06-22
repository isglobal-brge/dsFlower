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
    """Opacus DP-SGD fit shared by the tabular + image (head) paths."""
    lr = float(cfg.get("learning-rate", 0.01))
    batch_size = int(cfg.get("batch-size", 32))
    local_epochs = int(cfg.get("local-epochs", 1))
    num_rounds = int(cfg.get("num-server-rounds", 1))
    multiclass = int(n_classes) > 2

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
            optimizer.zero_grad()
            out = model(xb)
            loss = criterion(out, yb) if multiclass else criterion(out, yb.unsqueeze(1))
            loss.backward()
            optimizer.step()
    return get_torch_params(model), len(dataset)


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

        paths, y = load_image_collection(context)        # paths + labels only

        # The head width is fixed by num-classes (config); verify the labels fit it.
        # Without this, binary BCE would SILENTLY mislearn a stray label>=2, and
        # multiclass CE would throw a cryptic index error deep in the loop.
        n_distinct = int(np.unique(y).size) if len(y) else 0
        max_label = int(y.max()) if len(y) else 0
        if n_classes <= 2:
            if n_distinct > 2 or max_label > 1:
                raise RuntimeError(
                    f"num-classes={n_classes} (binary) but the labels have "
                    f"{n_distinct} distinct values (max {max_label}); set the "
                    "model's n_classes to match your label set.")
        elif max_label >= n_classes:
            raise RuntimeError(
                f"num-classes={n_classes} but a label value {max_label} was found; "
                "set the model's n_classes to at least the number of distinct labels.")

        encoder, feat_dim = vision.build_backbone(backbone)
        read = vision.read_image_3d if vision.is_3d_backbone(backbone) else vision.read_image_2d
        images = [read(p, image_size) for p in paths]    # the ONLY pass over pixels
        X = vision.extract_features(encoder, images)     # frozen, no grad -> features
        del images
        model = vision.build_head(feat_dim, n_classes)   # the only trainable/communicated module
        new_arrays, n = _dp_fit(model, X, y, pcfg, cfg, n_classes, msg)
    else:
        X, y = load_data(context)
        model = build_model(cfg, input_dim=X.shape[1])
        new_arrays, n = _dp_fit(model, X, y, pcfg, cfg, n_classes=2, msg=msg)

    # Disclosure backstop: bucket the released sample count; no per-node metrics.
    n_examples = dp_harness.bucket_count(n)
    reply = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=new_arrays),
        "metrics": MetricRecord({"num-examples": n_examples}),
    })
    return Message(content=reply, reply_to=msg)
