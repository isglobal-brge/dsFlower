"""Privacy utilities for update-level noise in federated learning.

WARNING: This implements UPDATE-LEVEL obfuscation (weight delta clipping +
Gaussian noise), NOT patient-level DP-SGD. This provides meaningful
protection of individual node weight updates but does NOT constitute
formal per-example differential privacy.

Patient-level DP-SGD is handled separately through Opacus and the
high_sensitivity_dp server profile. Templates that have not been validated for
per-example gradients are blocked by policy before training starts.

All privacy enforcement reads from manifest.json (server-written,
tamper-proof), NOT from pyproject.toml.
"""

import math

import numpy as np


def clip_weights(weights, old_weights, clipping_norm):
    """Clip the L2 norm of the weight delta (new - old).

    Args:
        weights: List of numpy arrays (new weights after training).
        old_weights: List of numpy arrays (weights before training).
        clipping_norm: Maximum L2 norm for the delta.

    Returns:
        List of numpy arrays with clipped delta applied to old_weights.
    """
    delta = [w - o for w, o in zip(weights, old_weights)]
    flat = np.concatenate([d.ravel() for d in delta])
    l2_norm = float(np.linalg.norm(flat))

    if l2_norm > clipping_norm:
        scale = clipping_norm / l2_norm
        delta = [d * scale for d in delta]

    return [o + d for o, d in zip(old_weights, delta)]


def add_gaussian_noise(weights, old_weights, sigma, clipping_norm):
    """Add calibrated Gaussian noise to the weight delta.

    Args:
        weights: List of numpy arrays (clipped weights).
        old_weights: List of numpy arrays (weights before training).
        sigma: Noise scale (standard deviation multiplier).
        clipping_norm: Clipping norm (used to scale noise).

    Returns:
        List of numpy arrays with noise added.
    """
    noisy = []
    for w, o in zip(weights, old_weights):
        delta = w - o
        noise = np.random.normal(0, sigma * clipping_norm, size=delta.shape)
        noisy.append(o + delta + noise.astype(delta.dtype))
    return noisy


def compute_sigma(epsilon, delta, clipping_norm, n_samples):
    """Compute noise scale via the Gaussian mechanism.

    Uses the analytic Gaussian mechanism formula:
        sigma = sqrt(2 * ln(1.25 / delta)) * (clipping_norm / epsilon)

    Args:
        epsilon: Privacy budget.
        delta: Privacy failure probability.
        clipping_norm: L2 clipping norm.
        n_samples: Number of training samples (for reference).

    Returns:
        Float noise scale (sigma).
    """
    if epsilon <= 0 or delta <= 0 or clipping_norm <= 0:
        raise ValueError("epsilon, delta, and clipping_norm must be positive")
    return math.sqrt(2.0 * math.log(1.25 / delta)) * (clipping_norm / epsilon)


def bucket_count(n):
    """Bucket a count using power-of-two rounding.

    Args:
        n: Exact count.

    Returns:
        Bucketed count as int.
    """
    if n <= 0:
        return 0
    if n < 4:
        return int(n)
    return int(2 ** round(math.log2(n)))


def make_private_opacus(model, optimizer, trainloader, clipping_norm,
                        epsilon, delta, epochs, noise_multiplier=None,
                        num_rounds=1, num_clients=1, distributed=False,
                        n_samples=None, batch_size=None):
    """Wrap model/optimizer/dataloader with Opacus PrivacyEngine for DP-SGD.

    Provides REAL per-example differential privacy. Opacus validates the model
    and replaces incompatible modules (BatchNorm -> GroupNorm, LSTM -> DPLSTM).

    The noise scale is calibrated to the target (epsilon, delta) over ALL
    federated steps (num_rounds * local epochs) via Opacus' RDP accountant.

    Distributed DP: when ``distributed`` is set (>=3 nodes under Secure
    Aggregation), each node adds only its 1/sqrt(N) share of the noise; the
    SecAgg-summed aggregate then carries the full target noise, giving
    central-DP-quality utility without a trusted aggregator. With <3 nodes each
    node adds the full noise (local DP): same target epsilon, more noise.
    """
    from opacus import PrivacyEngine
    from opacus.validators import ModuleValidator

    if not ModuleValidator.is_valid(model):
        model = ModuleValidator.fix(model)
        optimizer = type(optimizer)(model.parameters(), **optimizer.defaults)

    privacy_engine = PrivacyEngine()

    if noise_multiplier is None:
        try:
            from opacus.accountants.utils import get_noise_multiplier
            n = int(n_samples) if n_samples else len(trainloader.dataset)
            bs = int(batch_size) if batch_size else (
                getattr(trainloader, "batch_size", None) or max(1, n))
            sample_rate = min(1.0, float(bs) / max(1, n))
            total_epochs = max(1, int(num_rounds)) * max(1, int(epochs))
            noise_multiplier = get_noise_multiplier(
                target_epsilon=float(epsilon),
                target_delta=float(delta),
                sample_rate=sample_rate,
                epochs=total_epochs,
                accountant="rdp",
            )
        except Exception:
            # Fallback: analytic Gaussian mechanism (conservative single-shot).
            noise_multiplier = math.sqrt(2.0 * math.log(1.25 / float(delta))) / float(epsilon)

    # Distributed-DP noise split: variances add under the SecAgg sum, so each of
    # N nodes adds sigma/sqrt(N) and the aggregate carries sigma.
    if distributed and int(num_clients) >= 3:
        noise_multiplier = noise_multiplier / math.sqrt(int(num_clients))

    model, optimizer, trainloader = privacy_engine.make_private(
        module=model,
        optimizer=optimizer,
        data_loader=trainloader,
        noise_multiplier=noise_multiplier,
        max_grad_norm=clipping_norm,
    )

    return model, optimizer, trainloader, privacy_engine


# --- FedBN helpers ---

def is_bn_key(key):
    """Check if a state_dict key belongs to a BatchNorm layer."""
    bn_indicators = (".bn", "batch_norm", ".norm", "running_mean",
                     "running_var", "num_batches_tracked")
    key_lower = key.lower()
    return any(ind in key_lower for ind in bn_indicators)


def get_parameters_fedbn(model, exclude_bn=False):
    """Extract model parameters, optionally excluding BatchNorm."""
    import numpy as np
    module = getattr(model, '_module', model)
    sd = module.state_dict()
    if not exclude_bn:
        return [val.cpu().numpy() for val in sd.values()]
    return [val.cpu().numpy() for key, val in sd.items() if not is_bn_key(key)]


def set_parameters_fedbn(model, parameters, exclude_bn=False):
    """Set model parameters, optionally skipping BatchNorm (keeping local)."""
    import torch
    from collections import OrderedDict
    module = getattr(model, '_module', model)
    sd = module.state_dict()
    if not exclude_bn:
        new_sd = OrderedDict()
        for key, val in zip(sd.keys(), parameters):
            new_sd[key] = torch.tensor(val)
        module.load_state_dict(new_sd, strict=True)
    else:
        non_bn_keys = [k for k in sd.keys() if not is_bn_key(k)]
        new_sd = OrderedDict(sd)  # start with current (preserves BN)
        for key, val in zip(non_bn_keys, parameters):
            new_sd[key] = torch.tensor(val)
        module.load_state_dict(new_sd, strict=True)
