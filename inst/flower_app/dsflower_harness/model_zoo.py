"""Model construction for the dsFlower Tier-1 trusted harness.

The harness builds the model architecture from the run config (distributed with
the FAB, identical on the ServerApp that initializes the global model and on the
ClientApp that trains it). Only DP-compatible architectures are offered — no
BatchNorm (which couples samples and breaks Opacus' per-sample sensitivity bound).

MVP scope: binary classification with logistic regression and an MLP. More model
families (multiclass, sklearn surrogate, xgboost DP-GBDT, survival) layer on once
the spine is validated federated.
"""

from collections import OrderedDict


def build_model(cfg, input_dim):
    """Build a torch nn.Module from the run config + input dimension."""
    import torch.nn as nn

    name = str(cfg.get("model", cfg.get("model-name", "logreg"))).lower()
    input_dim = int(input_dim)
    if input_dim <= 0:
        raise ValueError(f"input_dim must be positive, got {input_dim}")

    if name in ("logreg", "logistic", "logistic_regression", "linear"):
        return nn.Linear(input_dim, 1)
    if name in ("mlp", "mlp_classifier"):
        hidden = int(cfg.get("hidden-dim", 64))
        return nn.Sequential(
            nn.Linear(input_dim, hidden),
            nn.ReLU(),
            nn.Linear(hidden, 1),
        )
    raise ValueError(
        f"Unsupported model '{name}' (Tier-1 harness MVP supports: logreg, mlp)"
    )


def get_torch_params(model):
    """Extract parameters as a list of numpy arrays.

    Unwraps Opacus' GradSampleModule (``._module``) so the state_dict keys/shapes
    match the un-wrapped model the ServerApp initialized.
    """
    module = getattr(model, "_module", model)
    return [v.detach().cpu().numpy() for v in module.state_dict().values()]


def set_torch_params(model, arrays):
    """Load a list of numpy arrays into the model (Opacus-aware)."""
    import torch

    module = getattr(model, "_module", model)
    keys = list(module.state_dict().keys())
    if len(keys) != len(arrays):
        raise ValueError(
            f"parameter count mismatch: model has {len(keys)} tensors, "
            f"received {len(arrays)}"
        )
    sd = OrderedDict((k, torch.tensor(v)) for k, v in zip(keys, arrays))
    module.load_state_dict(sd, strict=True)
