"""Parameter (de)serialization + trusted client-model loading for dsflower_runner.

``get_torch_params`` / ``set_torch_params`` were the only non-catalog part of the
deleted ``model_zoo`` — they are trusted plumbing (the Opacus-unwrapping
positional state_dict contract), kept here. ``load_user_model`` is the neural
track's gate: it imports the client-shipped ``build_model`` (architecture ONLY)
behind the integrity hook and enforces the DP-compatibility + no-stash invariants
before any training.
"""

import importlib
from collections import OrderedDict


def get_torch_params(model):
    """Model parameters as a list of numpy arrays.

    Unwraps Opacus' GradSampleModule (``._module``) so the state_dict keys/shapes
    match the un-wrapped model the ServerApp initialized. Safe to release ONLY
    after ``dp_harness.assert_releasable`` has confirmed there are no buffers and
    no frozen parameters (so every entry here is a DP-SGD-trained, noised tensor).
    """
    module = getattr(model, "_module", model)
    return [v.detach().cpu().numpy() for v in module.state_dict().values()]


def set_torch_params(model, arrays):
    """Load a list of numpy arrays into the model (Opacus-aware), strict match."""
    import torch

    module = getattr(model, "_module", model)
    keys = list(module.state_dict().keys())
    if len(keys) != len(arrays):
        raise ValueError(
            "parameter count mismatch: model has %d tensors, received %d"
            % (len(keys), len(arrays)))
    sd = OrderedDict((k, torch.tensor(v)) for k, v in zip(keys, arrays))
    module.load_state_dict(sd, strict=True)


def load_user_model(cfg, input_dim, *, input_key="num-features", allow_custom=False):
    """Import the client-shipped model module and build its nn.Module.

    The client supplies ONLY ``build_model(cfg) -> nn.Module`` (the forward graph);
    never the training loop, the loss, or the optimizer — those are harness-owned.
    The module name comes from ``cfg["model-module"]`` (node-verified to equal the
    single uploaded package basename), and is imported behind the integrity hook,
    which has already verified the package bytes against the node-computed pin.

    Hardening gates, in order (all fail closed):
      1. ``build_model`` exists and returns an ``nn.Module``.
      2. Opacus ``ModuleValidator`` — REJECT (never ``.fix()``) DP-incompatible
         layers (BatchNorm etc.) that would couple samples.
      3. ``assert_releasable`` — no buffers, no frozen parameters, so the released
         state_dict cannot carry un-noised raw data.
    The DEFAULT path emits vetted, stash-free architectures; ``allow_custom`` (a
    custodian opt-in) additionally triggers the per-sample-independence probe at
    the call site before training.
    """
    import torch
    from opacus.validators import ModuleValidator
    try:
        import dp_harness
    except ImportError:
        from . import dp_harness

    module_name = str(cfg["model-module"])
    mod = importlib.import_module(module_name)
    build = getattr(mod, "build_model", None)
    if not callable(build):
        raise ValueError(
            "model module '%s' must define build_model(cfg) -> nn.Module"
            % module_name)

    bcfg = dict(cfg)
    bcfg[input_key] = int(input_dim)
    model = build(bcfg)
    if not isinstance(model, torch.nn.Module):
        raise ValueError("build_model must return a torch.nn.Module")
    if not ModuleValidator.is_valid(model):
        raise ValueError(
            "model is not DP-compatible (Opacus ModuleValidator): %s"
            % ModuleValidator.validate(model, strict=False))
    dp_harness.assert_releasable(model)
    return model
