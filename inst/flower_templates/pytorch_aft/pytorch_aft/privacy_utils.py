"""Privacy utilities for update-level noise in federated learning.

WARNING: This implements UPDATE-LEVEL obfuscation (weight delta clipping +
Gaussian noise), NOT patient-level DP-SGD. This provides meaningful
protection of individual node weight updates but does NOT constitute
formal per-example differential privacy.

For formal patient-level DP, templates should use Opacus DP-SGD with
per-example gradient clipping. That is a planned upgrade (see secure_dp
profile documentation in policy.R).

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
                        epsilon, delta, epochs, noise_multiplier=None):
    """Wrap model/optimizer/dataloader with Opacus PrivacyEngine for DP-SGD.

    This provides REAL per-example differential privacy, not update-level noise.
    Opacus validates the model architecture and automatically replaces
    incompatible modules (e.g. BatchNorm -> GroupNorm, LSTM -> DPLSTM).

    Args:
        model: PyTorch module.
        optimizer: PyTorch optimizer (will be recreated if model is fixed).
        trainloader: DataLoader for training data.
        clipping_norm: Maximum per-example gradient L2 norm.
        epsilon: Target privacy budget (used to derive noise_multiplier
            when noise_multiplier is not provided).
        delta: Privacy failure probability.
        epochs: Number of training epochs (for accountant reference).
        noise_multiplier: Explicit noise scale. If None, derived from
            epsilon/delta via the Gaussian mechanism formula.

    Returns:
        Tuple of (model, optimizer, trainloader, privacy_engine).
    """
    from opacus import PrivacyEngine
    from opacus.validators import ModuleValidator

    if not ModuleValidator.is_valid(model):
        model = ModuleValidator.fix(model)
        optimizer = type(optimizer)(model.parameters(), **optimizer.defaults)

    privacy_engine = PrivacyEngine()

    if noise_multiplier is None:
        noise_multiplier = math.sqrt(2.0 * math.log(1.25 / delta)) / epsilon

    model, optimizer, trainloader = privacy_engine.make_private(
        module=model,
        optimizer=optimizer,
        data_loader=trainloader,
        noise_multiplier=noise_multiplier,
        max_grad_norm=clipping_norm,
    )

    return model, optimizer, trainloader, privacy_engine
