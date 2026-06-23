"""Parameter (de)serialization + trusted client-model loading for dsflower_runner.

``get_torch_params`` / ``set_torch_params`` were the only non-catalog part of the
deleted ``model_zoo`` — they are trusted plumbing (the Opacus-unwrapping
positional state_dict contract), kept here. ``load_user_model`` is the neural
track's gate: it imports the client-shipped ``build_model`` (architecture ONLY)
behind the integrity hook and enforces the DP-compatibility + no-stash invariants
before any training.
"""

import importlib


def get_torch_params(model):
    """Trainable parameters as numpy arrays, read via the UNBOUND stock
    ``nn.Module.named_parameters`` -- deliberately NOT ``module.state_dict()``.

    state_dict() runs any registered ``_state_dict_hooks``; a researcher hook there
    can REWRITE the DP-noised weights with captured raw data AT RELEASE (after
    DP-SGD, after the param-restore) and the value-blind release gates never see it.
    An instance override of ``named_parameters`` could do the same. Calling the
    stock CLASS method on the (assert_stock_architecture-vetted) module reads the
    real DP-SGD-trained ``_parameters`` storage directly and ignores any instance
    tampering. No buffers exist (assert_releasable), so this is the full releasable
    set; ``._module`` Opacus-unwraps.
    """
    import torch.nn as nn
    module = getattr(model, "_module", model)
    return [p.detach().cpu().numpy() for _, p in nn.Module.named_parameters(module)]


def set_torch_params(model, arrays):
    """Load numpy arrays into the trainable parameters via the stock
    ``nn.Module.named_parameters`` -- NOT ``load_state_dict`` (which runs
    ``_load_state_dict_pre_hooks``). In-place copy into the real param storage.
    """
    import torch
    import torch.nn as nn
    module = getattr(model, "_module", model)
    params = list(nn.Module.named_parameters(module))
    if len(params) != len(arrays):
        raise ValueError(
            "parameter count mismatch: model has %d params, received %d"
            % (len(params), len(arrays)))
    with torch.no_grad():
        for (_, p), a in zip(params, arrays):
            p.copy_(torch.as_tensor(a, dtype=p.dtype, device=p.device))


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
    dp_harness.assert_stock_architecture(model)   # no researcher forward (root gate)
    dp_harness.assert_releasable(model)
    # Snapshot the EXACT releasable key-set now (post-validation, pre-training).
    # The release gate in client_app._dp_fit re-checks this immediately before
    # shipping: a buffer / frozen param / new parameter registered lazily DURING
    # the researcher's forward (i.e. after this gate, where Opacus would never noise
    # it) grows the state_dict and is rejected before it can leave the node.
    model._dsflower_release_keys = tuple(
        n for n, _ in torch.nn.Module.named_parameters(model))
    return model
