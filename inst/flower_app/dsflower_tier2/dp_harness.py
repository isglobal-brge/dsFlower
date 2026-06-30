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
    # PRV/RDP both failed (should not happen with opacus installed). Do NOT fall
    # back to the single-shot analytic Gaussian sigma: applied per step over the
    # many DP-SGD steps it UNDER-noises (composes to >> the target epsilon) -> a
    # privacy violation. Fail closed rather than train with a wrong guarantee.
    raise RuntimeError(
        "Could not calibrate DP-SGD noise via the PRV/RDP accountant. Refusing to "
        "train rather than risk under-noising; ensure opacus is installed and "
        "(epsilon, delta, sample_rate, epochs) are valid.")


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


def _std_normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def compute_output_sigma(epsilon, delta, clipping_norm):
    """Minimal Gaussian std for an (epsilon, delta)-DP release of L2 sensitivity
    `clipping_norm`, via the ANALYTIC Gaussian mechanism (Balle & Wang 2018). Exact for ALL
    epsilon -- the classic sqrt(2 ln(1.25/delta))*S/epsilon bound only holds for epsilon<=1
    and under-noises above it (at epsilon=10, delta=1e-5 it leaks ~2.3x the target delta).
    sigma scales linearly in the sensitivity, preserving the sample-and-aggregate 2C/k
    ratio."""
    if epsilon <= 0 or delta <= 0 or clipping_norm <= 0:
        raise ValueError("epsilon, delta, and clipping_norm must be positive")

    def _delta_of_sigma(sigma):
        a = float(clipping_norm) / (2.0 * sigma)
        b = float(epsilon) * sigma / float(clipping_norm)
        return _std_normal_cdf(a - b) - math.exp(float(epsilon)) * _std_normal_cdf(-a - b)

    lo, hi = 1e-12, max(float(clipping_norm), 1.0)
    while _delta_of_sigma(hi) > delta:
        hi *= 2.0
        if hi > 1e15:
            break
    if _delta_of_sigma(hi) > delta:   # FAIL CLOSED: refuse rather than release under-noised
        raise ValueError(
            "cannot achieve (epsilon=%g, delta=%g) at sensitivity %g: no sigma brackets "
            "the target delta" % (epsilon, delta, clipping_norm))
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if _delta_of_sigma(mid) > delta:
            lo = mid
        else:
            hi = mid
    return hi


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


def add_gaussian_noise(weights, old_weights, std):
    """Add N(0, std^2) noise to the (already clipped) weight delta. `std` is the FULL
    Gaussian-mechanism standard deviation (sensitivity folded in by the caller); drawn
    from a fresh OS-entropy generator, never the shared global np.random (predictable
    noise would void DP)."""
    out = []
    for w, o in zip(weights, old_weights):
        w = np.asarray(w); o = np.asarray(o)
        delta = w - o
        noise = np.random.default_rng().normal(0.0, float(std), size=delta.shape)
        out.append(o + delta + noise.astype(delta.dtype))
    return out


def output_perturbation(new_weights, old_weights, clipping_norm, epsilon, delta):
    """Tier-2 / universal-floor DP in one call: clip the update to C, then add Gaussian
    noise calibrated to the L2 SENSITIVITY of a C-clipped release = 2*C (the C-ball
    DIAMETER, since arbitrary code's update is not a sum of per-record bounded terms),
    NOT C. (A prior version used C and re-multiplied the clip in add_gaussian_noise --
    a double-C masked only because C defaults to 1.)"""
    clipped = clip_update(new_weights, old_weights, clipping_norm)
    std = compute_output_sigma(epsilon, delta, 2.0 * clipping_norm)   # full std for sensitivity 2C
    return add_gaussian_noise(clipped, old_weights, std)


def sample_and_aggregate(block_updates, old_weights, clipping_norm, epsilon, delta):
    """Improved universal floor (Nissim-Raskhodnikova-Smith sample-and-aggregate): given
    the user's black-box update computed INDEPENDENTLY on each of k DISJOINT, data-
    independent blocks of the private data, release the clip-and-average aggregate under
    the Gaussian mechanism.

    Soundness + utility: each record lives in exactly ONE block, so under replace-one
    adjacency one record perturbs exactly ONE block update. Every block delta is clipped
    into the C-ball, so one block moves by at most the diameter 2C; the k-block MEAN moves
    by at most 2C/k. The released L2 sensitivity is 2C/k -- a k-fold reduction vs the plain
    floor's 2C (k-times-smaller noise at the SAME epsilon, delta), still an ordinary
    per-round Gaussian release in the ledger. The CALLER must build the partition
    independently of the data values and map failed/non-finite blocks to a zero delta
    (both leak-safe: zero is inside the C-ball; a data-independent partition encodes
    nothing)."""
    k = len(block_updates)
    if k < 1:
        raise ValueError("sample_and_aggregate needs at least one block update")
    old = [np.asarray(o, dtype=np.float64) for o in old_weights]
    mean_delta = [np.zeros_like(o) for o in old]
    for bu in block_updates:
        clipped = clip_update(bu, old, clipping_norm)          # old + delta, ||delta||_2 <= C
        for i, (c, o) in enumerate(zip(clipped, old)):
            mean_delta[i] = mean_delta[i] + (np.asarray(c) - o)
    mean_delta = [md / float(k) for md in mean_delta]
    mean_new = [o + md for o, md in zip(old, mean_delta)]
    std = compute_output_sigma(epsilon, delta, 2.0 * float(clipping_norm) / float(k))  # sensitivity 2C/k
    return add_gaussian_noise(mean_new, old, std)


def bucket_count(n):
    """Round a count to the nearest power of two (counts < 4 are exact)."""
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
