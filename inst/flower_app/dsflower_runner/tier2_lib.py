"""Trusted Tier-2 runner library (node-resident).

The researcher's uploaded app provides a narrow, reviewable interface (exfiltration
-scanned + hash-verified at install, framework-agnostic):

    initial_arrays(cfg: dict, input_dim: int) -> list[np.ndarray]
        # the starting global model parameters (ServerApp uses this to init)
    local_update(global_arrays: list[np.ndarray], X, y, cfg: dict) -> list[np.ndarray]
        # load the global model, train however you like, return new parameters

The app trains FREELY (any framework / optimizer / loss / loop). This trusted
runner then applies the DP GATE — output perturbation (McMahan et al. 2018): it
clips the weight delta (new - global) to L2 norm C and adds Gaussian noise
calibrated to (epsilon, delta). The guarantee holds for ANY local_update: even if
the app returns parameters that memorize a record, the RELEASED update is
(epsilon, delta)-DP because the trusted runner — not the app — bounds the
sensitivity and adds the noise. DP parameters come from the server-written,
tamper-proof manifest, never from the app.
"""

import importlib

import numpy as np

try:
    import dp_harness            # node-resident trusted copy
except ImportError:
    from . import dp_harness     # bundled fallback


_REQUIRED_HOOKS = ("initial_arrays", "local_update")


def load_user_module(module_name):
    """Import the uploaded app module and confirm it exposes the interface."""
    mod = importlib.import_module(module_name)
    missing = [h for h in _REQUIRED_HOOKS if not callable(getattr(mod, h, None))]
    if missing:
        raise ValueError(
            "Uploaded Tier-2 app '%s' is missing required hook(s): %s. It must "
            "define initial_arrays(cfg, input_dim) and "
            "local_update(global_arrays, X, y, cfg)." % (module_name, ", ".join(missing))
        )
    return mod


def _as_f64_list(weights):
    return [np.asarray(w, dtype=np.float64) for w in weights]


def gated_local_update(user_mod, global_arrays, X, y, cfg, pcfg):
    """Run the app's training from the global model, then apply the DP gate."""
    old = _as_f64_list(global_arrays)
    new = _as_f64_list(user_mod.local_update([o.copy() for o in old], X, y, cfg))

    if len(new) != len(old):
        raise ValueError(
            "Tier-2 app returned %d weight tensors but the global model has %d; "
            "the update shape must match." % (len(new), len(old)))
    for nw, ow in zip(new, old):
        if nw.shape != ow.shape:
            raise ValueError(
                "Tier-2 app weight shape %s != global %s." % (nw.shape, ow.shape))

    gated = dp_harness.output_perturbation(
        new, old,
        clipping_norm=pcfg["clipping_norm"],
        epsilon=pcfg["epsilon"],
        delta=pcfg["delta"],
    )
    return [g.astype(np.float32) for g in gated]
