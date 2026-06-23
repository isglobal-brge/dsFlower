"""dsFlower DP harness — BUNDLED FALLBACK COPY.

The CANONICAL, trusted copy is node-resident at dsFlower/inst/python/dp_harness.py;
client_app.py imports that one preferentially (so the node controls the DP code and
it cannot be weakened by the uploaded FAB) and falls back to this bundled copy only
when the node-resident module is not importable, keeping the FAB self-contained for
local/dev runs. Keep the two byte-identical; M3's content-hash verification will
formalize the trust relationship. See ARCHITECTURE.md §5, §7.
"""

import math

import numpy as np


def calibrate_noise_multiplier(epsilon, delta, sample_rate, total_epochs):
    """Noise multiplier for target (epsilon, delta) over all DP-SGD steps.

    Prefers the PRV accountant (tighter than RDP), then RDP, then the analytic
    Gaussian mechanism. Calibrated over TOTAL local epochs (num_rounds *
    local_epochs) since RDP/PRV compose over steps, not rounds.
    """
    if epsilon <= 0 or delta <= 0:
        raise ValueError("epsilon and delta must be positive")
    sample_rate = min(1.0, max(1e-12, float(sample_rate)))
    total_epochs = max(1, int(total_epochs))
    try:
        from opacus.accountants.utils import get_noise_multiplier
        for accountant in ("prv", "rdp"):
            try:
                return float(get_noise_multiplier(
                    target_epsilon=float(epsilon),
                    target_delta=float(delta),
                    sample_rate=sample_rate,
                    epochs=total_epochs,
                    accountant=accountant,
                ))
            except Exception:
                continue
    except Exception:
        pass
    return math.sqrt(2.0 * math.log(1.25 / float(delta))) / float(epsilon)


def make_private_dpsgd(model, optimizer, trainloader, clipping_norm,
                       epsilon, delta, local_epochs, num_rounds=1,
                       noise_multiplier=None, n_samples=None, batch_size=None):
    """Wrap model/optimizer/dataloader with Opacus for per-example DP-SGD."""
    from opacus import PrivacyEngine
    from opacus.validators import ModuleValidator

    if not ModuleValidator.is_valid(model):
        raise ValueError(
            "Model is not DP-compatible (Opacus ModuleValidator). DP-incompatible "
            "layers (e.g. BatchNorm) break the per-sample sensitivity bound; this "
            "must be rejected in validation, not silently rewritten."
        )

    if noise_multiplier is None:
        n = int(n_samples) if n_samples else len(trainloader.dataset)
        bs = int(batch_size) if batch_size else (
            getattr(trainloader, "batch_size", None) or max(1, n))
        sample_rate = float(bs) / max(1, n)
        noise_multiplier = calibrate_noise_multiplier(
            epsilon=epsilon, delta=delta, sample_rate=sample_rate,
            total_epochs=max(1, int(num_rounds)) * max(1, int(local_epochs)),
        )

    privacy_engine = PrivacyEngine()
    model, optimizer, trainloader = privacy_engine.make_private(
        module=model,
        optimizer=optimizer,
        data_loader=trainloader,
        noise_multiplier=noise_multiplier,
        max_grad_norm=float(clipping_norm),
    )
    return model, optimizer, trainloader, privacy_engine


def compute_output_sigma(epsilon, delta, clipping_norm):
    """Analytic Gaussian-mechanism noise scale for a single bounded release."""
    if epsilon <= 0 or delta <= 0 or clipping_norm <= 0:
        raise ValueError("epsilon, delta, and clipping_norm must be positive")
    return math.sqrt(2.0 * math.log(1.25 / float(delta))) * (
        float(clipping_norm) / float(epsilon))


def clip_update(new_weights, old_weights, clipping_norm):
    """Clip the global L2 norm of the weight delta (new - old) to clipping_norm."""
    delta = [np.asarray(w) - np.asarray(o)
             for w, o in zip(new_weights, old_weights)]
    flat = np.concatenate([d.ravel() for d in delta]) if delta else np.array([])
    l2 = float(np.linalg.norm(flat))
    if l2 > clipping_norm and l2 > 0:
        scale = clipping_norm / l2
        delta = [d * scale for d in delta]
    return [np.asarray(o) + d for o, d in zip(old_weights, delta)]


def add_gaussian_noise(weights, old_weights, sigma, clipping_norm):
    """Add N(0, (sigma*clipping_norm)^2) noise to the (clipped) weight delta."""
    out = []
    for w, o in zip(weights, old_weights):
        w = np.asarray(w); o = np.asarray(o)
        delta = w - o
        noise = np.random.normal(0.0, sigma * float(clipping_norm), size=delta.shape)
        out.append(o + delta + noise.astype(delta.dtype))
    return out


def output_perturbation(new_weights, old_weights, clipping_norm, epsilon, delta):
    """Tier-2 DP in one call: clip the update to C, then add calibrated noise."""
    clipped = clip_update(new_weights, old_weights, clipping_norm)
    sigma = compute_output_sigma(epsilon, delta, clipping_norm)
    return add_gaussian_noise(clipped, old_weights, sigma, clipping_norm)


def bucket_count(n):
    """Round a count to the nearest power of two; counts <= 3 (the nfilter.subset
    default) are SUPPRESSED to 0 so a near-empty node never leaks an exact small
    size. Mirrors the R .bucket_count suppression (policy.R)."""
    n = int(n)
    if n <= 3:
        return 0
    return int(2 ** round(math.log2(n)))


def is_bn_key(key):
    """True if a state_dict key belongs to a BatchNorm-like layer."""
    indicators = (".bn", "batch_norm", ".norm", "running_mean",
                  "running_var", "num_batches_tracked")
    k = key.lower()
    return any(ind in k for ind in indicators)
