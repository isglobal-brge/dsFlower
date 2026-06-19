"""Tier-1 trusted ClientApp (node side) — always-on DP-SGD, new Message API.

Runs on each DataSHIELD node's SuperNode. The researcher supplies only the model
architecture + hyperparameters (run config) and the DP budget (tamper-proof
manifest); this trusted harness owns the training loop, so per-example DP is
applied unconditionally and cannot be bypassed by the submitted app.
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from flwr.clientapp import ClientApp
from flwr.common import ArrayRecord, Context, Message, MetricRecord, RecordDict

from .task import load_data, load_privacy_config
from .model_zoo import build_model, get_torch_params, set_torch_params

# Prefer the node-resident trusted DP harness; fall back to the bundled copy so
# the FAB still runs where the node module isn't on the path (local/dev).
try:
    import dp_harness
except ImportError:
    from . import dp_harness


app = ClientApp()


@app.train()
def train(msg: Message, context: Context) -> Message:
    cfg = context.run_config

    X, y = load_data(context)
    pcfg = load_privacy_config(context)

    lr = float(cfg.get("learning-rate", 0.01))
    batch_size = int(cfg.get("batch-size", 32))
    local_epochs = int(cfg.get("local-epochs", 1))
    num_rounds = int(cfg.get("num-server-rounds", 1))

    model = build_model(cfg, input_dim=X.shape[1])
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    dataset = TensorDataset(torch.from_numpy(X), torch.from_numpy(y))
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    # ALWAYS-ON DP-SGD: Opacus wraps model/optimizer/loader (per-sample clipping
    # + Gaussian noise). The model is privatized BEFORE the global weights are
    # loaded so the wrapped module's state_dict aligns with the server's.
    model, optimizer, trainloader, _engine = dp_harness.make_private_dpsgd(
        model, optimizer, trainloader,
        clipping_norm=pcfg["clipping_norm"],
        epsilon=pcfg["epsilon"],
        delta=pcfg["delta"],
        local_epochs=local_epochs,
        num_rounds=num_rounds,
        n_samples=len(dataset),
        batch_size=batch_size,
    )

    set_torch_params(model, msg.content["arrays"].to_numpy_ndarrays())

    criterion = nn.BCEWithLogitsLoss()
    model.train()
    for _ in range(local_epochs):
        for xb, yb in trainloader:
            optimizer.zero_grad()
            output = model(xb)
            loss = criterion(output, yb.unsqueeze(1))
            loss.backward()
            optimizer.step()

    new_arrays = get_torch_params(model)

    # Disclosure backstop: bucket the released sample count (also the FedAvg
    # weight); never emit per-node metrics or an exact count.
    n_examples = dp_harness.bucket_count(len(dataset))

    reply = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=new_arrays),
        "metrics": MetricRecord({"num-examples": n_examples}),
    })
    return Message(content=reply, reply_to=msg)
