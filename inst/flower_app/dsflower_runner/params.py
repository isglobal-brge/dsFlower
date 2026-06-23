"""Parameter (de)serialization + trusted node-side model construction.

``get_torch_params`` / ``set_torch_params`` are trusted plumbing (the
Opacus-unwrapping positional contract). ``load_user_model`` builds the neural
model NODE-SIDE from the researcher's declarative spec (see ``model_spec``): no
researcher code runs in this interpreter, so the trusted release path cannot be
subverted by import-time monkeypatching of torch globals. The stock-architecture
+ releasable invariants are re-checked as defense in depth before any training.
"""


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


def load_user_model(cfg, input_dim, loss_name):
    """Build the neural model NODE-SIDE from the researcher's declarative spec.

    The researcher ships ONLY a spec (DATA, decoded + built by ``model_spec``);
    never source, a training loop, a loss, or an optimizer -- all harness-owned.
    ``input_dim`` (@in) and the loss-derived output width (@out) are node-decided,
    so the researcher controls only the hidden structure. Because no researcher
    code runs here, the trusted release path is safe by construction; the gates
    below are defense in depth -- a node-built module from the stock allowlist
    passes them trivially, but they fail closed on any builder bug:

      1. ``build_from_spec`` -- only allowlisted, per-sample-DP-safe stock layers.
      2. Opacus ``ModuleValidator`` -- REJECT (never ``.fix()``) any DP-incompatible
         layer (BatchNorm etc.) that would couple samples.
      3. ``assert_stock_architecture`` / ``assert_releasable`` -- no researcher
         forward, no buffers, no frozen params (nothing to carry un-noised data).
    """
    import torch
    from opacus.validators import ModuleValidator
    try:
        import dp_harness
    except ImportError:
        from . import dp_harness
    try:
        import model_spec
    except ImportError:
        from . import model_spec

    spec = model_spec.read_spec(cfg)
    out_dim = model_spec.output_width(loss_name, cfg)
    num_labels = int(cfg["num-labels"]) if cfg.get("num-labels") is not None else None
    model = model_spec.build_from_spec(
        spec, in_dim=int(input_dim), out_dim=out_dim, num_labels=num_labels)

    if not isinstance(model, torch.nn.Module):
        raise ValueError("build_from_spec must return a torch.nn.Module")
    if not ModuleValidator.is_valid(model):
        raise ValueError(
            "model is not DP-compatible (Opacus ModuleValidator): %s"
            % ModuleValidator.validate(model, strict=False))
    dp_harness.assert_stock_architecture(model)   # node-built => stock (belt + suspenders)
    dp_harness.assert_releasable(model)
    # Snapshot the EXACT releasable key-set now (post-build, pre-training). The
    # release gate in client_app._dp_fit re-checks it immediately before shipping,
    # so nothing registered lazily during forward can grow the released state_dict.
    model._dsflower_release_keys = tuple(
        n for n, _ in torch.nn.Module.named_parameters(model))
    return model
