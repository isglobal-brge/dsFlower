"""dsFlower DP harness (node-side, trusted) — always-on differential privacy.

This module is the framework's privacy core. It is installed with the dsFlower
node package and runs as TRUSTED code: the researcher never ships it, so the DP
guarantee cannot be disabled or weakened by uploaded app code.

Design (dsFlower 2.0):
  * DP is ALWAYS applied — there are no privacy profiles and no "off" path.
  * Node-side central DP — each data node is the trusted curator for its local
    dataset. There is no Secure Aggregation. Each node adds the full noise
    calibrated to the target (epsilon, delta); the aggregate is the mean
    of already-private updates (post-processing, so the guarantee composes).
  * Two enforcement tiers (see ARCHITECTURE.md §5):
      - Tier 1 (model submission): `make_private_dpsgd` runs Opacus DP-SGD with
        per-sample gradient clipping + Gaussian noise; tight DP, good utility.
      - Tier 2 (arbitrary app):    `output_perturbation` hard-clips the whole
        weight delta to L2 norm C and adds Gaussian noise; coarse but holds for
        ANY update.
  * DP-SGD calibration uses the PRV accountant when available (tighter than RDP
    ⇒ less noise for the same epsilon), falling back to RDP and then failing
    closed.  Tier-2 output perturbation uses its separate, closed-form
    RDP-calibrated Gaussian bound.

All privacy parameters MUST come from the server-written, tamper-proof
manifest.json — never from client-controlled pyproject config.
"""

import math
from functools import lru_cache

import numpy as np

try:
    from .seeding import SecureNumpyRng
except ImportError:  # Direct execution by the standalone regression script.
    import sys
    if "_dsftrusted_seeding" in sys.modules:
        SecureNumpyRng = sys.modules["_dsftrusted_seeding"].SecureNumpyRng
    else:
        from seeding import SecureNumpyRng


# Public, data-independent saturation bound for numeric model releases.  It is
# far outside useful model ranges but prevents IEEE-754 overflow from becoming
# a bypass around the finite-output gate.  Clamping a DP result is post-processing.
MAX_RELEASE_ABS = 1.0e6
MAX_PARAMETER_ABS = 1.0e6


def _require_secure_rng(rng, purpose):
    if not isinstance(rng, SecureNumpyRng):
        raise RuntimeError(
            "%s requires an explicit release-scoped SecureNumpyRng" % purpose)
    if not callable(getattr(rng, "normal", None)):
        raise RuntimeError("the secure RNG must provide normal()")
    return rng


# --------------------------------------------------------------------------- #
# Tier 1 — Opacus DP-SGD (per-sample gradient clipping + Gaussian noise)
# --------------------------------------------------------------------------- #

def totalize_grad_samples(parameters, clipping_norm):
    """Coordinate-totalise Opacus grad_sample tensors before its global L2 clip."""
    import torch

    try:
        bound = float(clipping_norm)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError(
            "DP-SGD clipping norm must be finite and positive") from exc
    if not math.isfinite(bound) or bound <= 0.0:
        raise RuntimeError("DP-SGD clipping norm must be finite and positive")
    for parameter in parameters:
        grad_sample = getattr(parameter, "grad_sample", None)
        if grad_sample is None:
            raise RuntimeError(
                "DP-SGD parameter has no per-sample gradient; refusing training")
        samples = grad_sample if isinstance(grad_sample, list) else [grad_sample]
        if not samples or any(not isinstance(value, torch.Tensor) for value in samples):
            raise RuntimeError(
                "DP-SGD per-sample gradient has an invalid representation")
        with torch.no_grad():
            for value in samples:
                value.nan_to_num_(nan=0.0, posinf=bound, neginf=-bound)
                value.clamp_(-bound, bound)
                if not bool(torch.isfinite(value).all()):
                    raise RuntimeError(
                        "DP-SGD per-sample gradient could not be totalized")

class _SecurePoissonBatchSampler:
    """Poisson batches driven only by the node's ChaCha20 release stream."""

    def __init__(self, *, num_samples, steps, rng):
        self.num_samples = int(num_samples)
        self.steps = int(steps)
        if self.num_samples < 1 or self.steps < 1:
            raise ValueError("secure Poisson sampling needs samples and steps")
        if not callable(getattr(rng, "bernoulli_mask_one_in", None)):
            raise RuntimeError("secure Poisson sampling needs a ChaCha20 RNG")
        self.rng = rng
        self.sample_rate = 1.0 / float(self.steps)

    def __len__(self):
        return self.steps

    def __iter__(self):
        for _ in range(self.steps):
            mask = self.rng.bernoulli_mask_one_in(
                self.steps, self.num_samples)
            yield np.flatnonzero(mask).tolist()


def _make_secure_poisson_loader(trainloader, *, steps_per_epoch,
                                secure_sampling_rng):
    """Replace the canonical tensor loader with exact ChaCha Poisson batches."""
    from opacus.data_loader import (dtype_safe, shape_safe,
                                    wrap_collate_with_empty)
    from torch.utils.data import DataLoader, IterableDataset

    dataset = trainloader.dataset
    if isinstance(dataset, IterableDataset):
        raise ValueError("secure Poisson sampling needs an indexed dataset")
    if len(dataset) < 1:
        raise ValueError("DP-SGD needs a non-empty dataset")

    # The trusted runner builds a TensorDataset, as did the Opacus loader this
    # replaces.  Pin the same empty-batch shapes up front so a first empty draw
    # is valid as well.
    sample = dataset[0]
    if not isinstance(sample, (list, tuple)) or not sample:
        raise ValueError("secure DP-SGD expects a non-empty tuple/list sample")
    sample_empty_shapes = [(0, *shape_safe(value)) for value in sample]
    dtypes = [dtype_safe(value) for value in sample]
    collate_fn = wrap_collate_with_empty(
        collate_fn=trainloader.collate_fn,
        sample_empty_shapes=sample_empty_shapes,
        dtypes=dtypes,
    )
    batch_sampler = _SecurePoissonBatchSampler(
        num_samples=len(dataset), steps=steps_per_epoch,
        rng=secure_sampling_rng,
    )
    return DataLoader(
        dataset=dataset,
        batch_sampler=batch_sampler,
        num_workers=trainloader.num_workers,
        collate_fn=collate_fn,
        pin_memory=trainloader.pin_memory,
        timeout=trainloader.timeout,
        worker_init_fn=trainloader.worker_init_fn,
        multiprocessing_context=trainloader.multiprocessing_context,
        generator=None,
        prefetch_factor=trainloader.prefetch_factor,
        persistent_workers=trainloader.persistent_workers,
    )


def _replace_one_to_add_remove_budget(epsilon, delta):
    """Convert a bounded/replace-one target to an add/remove target for Opacus.

    A replacement is one removal followed by one addition. If the underlying
    mechanism is (epsilon0, delta0)-DP for add/remove neighbours, two-step group
    privacy gives (2*epsilon0, (1+exp(epsilon0))*delta0)-DP for replace-one.
    """
    try:
        epsilon = float(epsilon)
        delta = float(delta)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("epsilon and delta must be finite numeric values") from exc
    if (not math.isfinite(epsilon) or epsilon <= 0.0
            or not math.isfinite(delta) or not 0.0 < delta < 1.0):
        raise ValueError("require finite epsilon > 0 and finite 0 < delta < 1")
    epsilon0 = epsilon / 2.0
    try:
        delta0 = delta / (1.0 + math.exp(epsilon0))
    except OverflowError as exc:
        raise ValueError("epsilon is too large for bounded-DP calibration") from exc
    if not math.isfinite(delta0) or delta0 <= 0.0:
        raise ValueError("bounded-DP conversion produced an invalid add/remove delta")
    return epsilon0, delta0


def calibrate_noise_multiplier(epsilon, delta, sample_rate, total_epochs,
                               total_steps=None):
    """Noise multiplier for a replace-one target over all DP-SGD steps.

    Prefers the PRV accountant (tighter than RDP), then RDP, and fails closed if
    neither can calibrate the full run. Opacus accounts add/remove neighbours, so
    the node's bounded/replace-one target is converted to the two-step
    group-privacy target before calling it. The noise is
    calibrated over the TOTAL number of local epochs (num_rounds * local_epochs)
    because RDP/PRV compose over steps, independent of how many federated rounds
    those steps are spread across — this is what makes one-shot training cost the
    same epsilon as many rounds.
    """
    epsilon0, delta0 = _replace_one_to_add_remove_budget(epsilon, delta)
    sample_rate = min(1.0, max(1e-12, float(sample_rate)))
    total_epochs = max(1, int(total_epochs))
    if total_steps is not None and int(total_steps) < 1:
        raise ValueError("total_steps must be positive")
    try:
        from opacus.accountants.utils import get_noise_multiplier
        for accountant in ("prv", "rdp"):
            try:
                horizon = ({"steps": int(total_steps)} if total_steps is not None
                           else {"epochs": total_epochs})
                return float(get_noise_multiplier(
                    target_epsilon=epsilon0,
                    target_delta=delta0,
                    sample_rate=sample_rate,
                    accountant=accountant,
                    **horizon,
                ))
            except Exception:
                continue
    except Exception:
        pass
    # PRV/RDP both failed (should not happen with opacus installed). Do NOT fall
    # back to a single-shot output-perturbation Gaussian sigma: applied per step over the
    # many DP-SGD steps it UNDER-noises (composes to >> the target epsilon) -> a
    # privacy violation. Fail closed rather than train with a wrong guarantee.
    raise RuntimeError(
        "Could not calibrate DP-SGD noise via the PRV/RDP accountant. Refusing to "
        "train rather than risk under-noising; ensure opacus is installed and "
        "(epsilon, delta, sample_rate, epochs) are valid.")


@lru_cache(maxsize=128)
def _cached_noise_multiplier(epsilon, delta, sample_rate, total_epochs,
                             total_steps, opacus_version):
    """Memoize exact accountant results for an immutable public run horizon.

    ``opacus_version`` is deliberately part of the key: it prevents a result
    calibrated by one accountant implementation from being reused after an
    in-process test/development reload with another version.
    """
    return calibrate_noise_multiplier(
        epsilon=epsilon, delta=delta, sample_rate=sample_rate,
        total_epochs=total_epochs, total_steps=total_steps)


def make_private_dpsgd(model, optimizer, trainloader, clipping_norm,
                       epsilon, delta, local_epochs, num_rounds=1,
                       noise_multiplier=None, n_samples=None, batch_size=None,
                       noise_generator=None, secure_noise_rng=None,
                       secure_sampling_rng=None):
    """Wrap model/optimizer/dataloader with Opacus for per-example DP-SGD.

    Returns (model, optimizer, trainloader, privacy_engine). The sensitivity
    bound is enforced by Opacus' per-sample clipping (max_grad_norm), so the DP
    guarantee holds for ANY forward-pass module — the researcher supplies only
    the architecture, never the training loop.

    Opacus' ModuleValidator must already have passed in the validation pipeline
    (DP-incompatible layers such as BatchNorm couple samples and break the
    per-sample-gradient sensitivity bound). We assert validity here as a
    backstop and do NOT silently fix() the architecture.
    """
    from opacus import PrivacyEngine, __version__ as opacus_version
    from opacus.validators import ModuleValidator

    if not ModuleValidator.is_valid(model):
        raise ValueError(
            "Model is not DP-compatible (Opacus ModuleValidator). DP-incompatible "
            "layers (e.g. BatchNorm) break the per-sample sensitivity bound; this "
            "must be rejected in validation, not silently rewritten."
        )

    # The canonical loader's length pins both the configured Poisson rate and
    # the number of optimizer/accountant steps.  Sampling itself must not fall
    # back to torch.Generator (MT19937); that would truncate the release key and
    # would not provide the cryptographic threat model used for DP noise.
    steps_per_epoch = len(trainloader)
    if steps_per_epoch < 1:
        raise ValueError("DP-SGD needs a non-empty data loader")
    if secure_sampling_rng is None:
        raise RuntimeError("DP-SGD requires the node ChaCha20 sampling RNG")
    if not isinstance(secure_sampling_rng, SecureNumpyRng):
        raise RuntimeError(
            "DP-SGD sampling requires an explicit release-scoped SecureNumpyRng")
    secure_noise_rng = _require_secure_rng(secure_noise_rng, "DP-SGD noise")
    if noise_generator is not None:
        raise RuntimeError("DP-SGD does not accept an alternate noise generator")
    trainloader = _make_secure_poisson_loader(
        trainloader,
        steps_per_epoch=steps_per_epoch,
        secure_sampling_rng=secure_sampling_rng,
    )

    if noise_multiplier is None:
        # The secure sampler includes every row independently with rate exactly
        # 1/steps_per_epoch.  Calibrate against that rate and the exact number of
        # steps; using epochs with batch_size/n can under-count at ceil boundaries.
        total_epochs = max(1, int(num_rounds)) * max(1, int(local_epochs))
        sample_rate = 1.0 / float(steps_per_epoch)
        noise_multiplier = _cached_noise_multiplier(
            float(epsilon), float(delta), float(sample_rate),
            int(total_epochs), int(steps_per_epoch * total_epochs),
            str(opacus_version))

    privacy_engine = PrivacyEngine()
    # DP noise is replaced below by a ChaCha20 stream derived from the node secret
    # and a unique accountant release identity. It is independent of private data.
    model, optimizer, trainloader = privacy_engine.make_private(
        module=model,
        optimizer=optimizer,
        data_loader=trainloader,
        noise_multiplier=noise_multiplier,
        max_grad_norm=float(clipping_norm),
        noise_generator=noise_generator,
        # The supplied loader already is genuine Poisson sampling.  False here
        # only prevents Opacus from replacing its ChaCha sampler; Opacus still
        # attaches its accountant hook with q=1/len(trainloader).
        poisson_sampling=False,
    )
    if not isinstance(trainloader.batch_sampler, _SecurePoissonBatchSampler):
        raise RuntimeError("Opacus replaced the trusted Poisson sampler")
    # Modern torchcsprng wheels do not exist for current PyTorch/Python.  Keep
    # Opacus' clipping/accounting, but replace only DPOptimizer.add_noise with
    # a trusted ChaCha20-backed Gaussian source.  Four independent Gaussian
    # draws are summed / 2 (the floating-point hardening used by Opacus secure
    # mode), then copied to the parameter device.
    import types
    import torch
    from opacus.optimizers.optimizer import (
        _check_processed_flag, _mark_as_processed)

    original_clip_and_accumulate = optimizer.clip_and_accumulate

    def _clip_and_accumulate_finite(self):
        totalize_grad_samples(self.params, self.max_grad_norm)
        return original_clip_and_accumulate()

    def _add_secure_noise(self):
        for param in self.params:
            _check_processed_flag(param.summed_grad)
            if not bool(torch.isfinite(param.summed_grad).all()):
                raise RuntimeError(
                    "Opacus produced a non-finite clipped gradient; refusing release")
            values = secure_noise_rng.normal(
                0.0,
                self.noise_multiplier * self.max_grad_norm,
                size=tuple(param.summed_grad.shape),
            )
            noise = torch.as_tensor(
                values,
                dtype=param.summed_grad.dtype,
                device=param.summed_grad.device,
            )
            private_grad = param.summed_grad + noise
            if not bool(torch.isfinite(private_grad).all()):
                raise RuntimeError(
                    "DP-SGD noise addition produced a non-finite gradient")
            param.grad = private_grad.view_as(param)
            _mark_as_processed(param.summed_grad)

    optimizer.clip_and_accumulate = types.MethodType(
        _clip_and_accumulate_finite, optimizer)
    optimizer.add_noise = types.MethodType(_add_secure_noise, optimizer)
    return model, optimizer, trainloader, privacy_engine


# --------------------------------------------------------------------------- #
# Tier 2 — output perturbation (clip the whole update + Gaussian noise)
# --------------------------------------------------------------------------- #

def resolve_dp_track(run_config, manifest_track):
    """Server-DERIVED, unforgeable DP routing: choose the enforced-DP mechanism from
    WHAT was actually submitted, never from a client-stated preference. An uploaded
    user-module (arbitrary foreign code) ALWAYS gets the output-perturbation floor
    ('egress'), never DP-SGD ('neural') -- a client cannot route
    its own code to a tighter mechanism. For node-built artifacts the node-pinned
    manifest track applies (declarative spec -> neural). Anything
    unrecognized fails closed to the universal floor. This is the single, testable
    routing decision; client_app.train() calls it (the neural track additionally only
    ever runs the hash-verified harness, so foreign code cannot impersonate it)."""
    if run_config.get("user-module"):
        return "egress"
    if manifest_track in ("neural", "egress", "validation"):
        return manifest_track
    return "egress"


def compute_output_sigma(epsilon, delta, clipping_norm, num_releases=1):
    """Conservative per-release Gaussian std for a composed transcript.

    Calibration uses the closed-form RDP guarantee, avoiding subtraction of two
    nearly equal Gaussian tails (which can under-noise at small epsilon/delta in
    ordinary double precision).  For ``z = sensitivity / sigma`` and
    ``L = log(1/delta)``, Gaussian RDP converted at its optimal Renyi order gives

        epsilon_bound = z^2/2 + z*sqrt(2L).

    For ``R`` releases with the same sensitivity and noise scale, total RDP gives
    ``epsilon_bound = R*z^2/2 + z*sqrt(2*R*L)``. Solving the same quadratic for
    ``z*sqrt(R)`` makes the per-release standard deviation ``sqrt(R)`` times the
    single-release value. This is valid for every finite epsilon > 0 and
    0 < delta < 1, and is intentionally conservative relative to exact optimal
    Gaussian calibration.
    """
    try:
        epsilon = float(epsilon)
        delta = float(delta)
        sensitivity = float(clipping_norm)
        releases_float = float(num_releases)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "epsilon, delta, clipping_norm, and num_releases must be numeric"
        ) from exc
    if (not math.isfinite(epsilon) or epsilon <= 0.0
            or not math.isfinite(delta) or not 0.0 < delta < 1.0
            or not math.isfinite(sensitivity) or sensitivity <= 0.0
            or not math.isfinite(releases_float) or releases_float < 1.0
            or releases_float != math.floor(releases_float)
            or releases_float > 1_000_000):
        raise ValueError(
            "require epsilon > 0, 0 < delta < 1, clipping_norm > 0, and "
            "integer num_releases in [1, 1000000]"
        )
    releases = int(releases_float)

    root_log = math.sqrt(2.0 * -math.log(delta))
    positive_root = math.hypot(root_log, math.sqrt(2.0 * epsilon))
    z = (2.0 * epsilon) / (positive_root + root_log)
    sigma = sensitivity * math.sqrt(float(releases)) / z
    if not math.isfinite(sigma) or sigma <= 0.0:
        raise ValueError("Gaussian RDP calibration is outside the numeric range")
    # Leave a tiny explicit safety margin for the independent floating-point
    # evaluation of the RDP inequality at extreme policy values.
    return math.nextafter(sigma * (1.0 + 1.0e-12), math.inf)


def clip_update(new_weights, old_weights, clipping_norm):
    """Validate and clip one untrusted global update in float64.

    Count/shape mismatches are rejected.  A numeric candidate containing NaN,
    infinity, or a subtraction overflow maps deterministically to the zero
    delta.  Finite extreme values are clipped with a scaled norm calculation
    that never squares their original magnitude.
    """
    try:
        clipping_norm = float(clipping_norm)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("clipping_norm must be finite and positive") from exc
    if not math.isfinite(clipping_norm) or clipping_norm <= 0.0:
        raise ValueError("clipping_norm must be finite and positive")
    if not isinstance(old_weights, (list, tuple)) or not old_weights:
        raise ValueError("old_weights must be a non-empty list/tuple")
    if (not isinstance(new_weights, (list, tuple))
            or len(new_weights) != len(old_weights)):
        raise ValueError("candidate weight count does not match old_weights")

    old = []
    candidates = []
    invalid_candidate = False
    for index, (raw_new, raw_old) in enumerate(zip(new_weights, old_weights)):
        old_array = np.asarray(raw_old)
        new_array = np.asarray(raw_new)
        if old_array.dtype.kind not in "biuf" or old_array.size < 1:
            raise ValueError("old weight %d is not a non-empty real array" % index)
        if new_array.dtype.kind not in "biuf":
            raise ValueError("candidate weight %d is not a real array" % index)
        if new_array.shape != old_array.shape:
            raise ValueError("candidate weight %d has the wrong shape" % index)
        old_array = np.asarray(old_array, dtype=np.float64)
        new_array = np.asarray(new_array, dtype=np.float64)
        if not bool(np.all(np.isfinite(old_array))):
            raise ValueError("old weight %d contains non-finite values" % index)
        if not bool(np.all(np.isfinite(new_array))):
            invalid_candidate = True
        old.append(old_array)
        candidates.append(new_array)

    if invalid_candidate:
        return [value.copy() for value in old]

    with np.errstate(over="ignore", invalid="ignore"):
        deltas = [new - previous
                  for new, previous in zip(candidates, old)]
    if any(not bool(np.all(np.isfinite(delta))) for delta in deltas):
        return [value.copy() for value in old]

    max_abs = max(float(np.max(np.abs(delta))) for delta in deltas)
    if max_abs == 0.0:
        return [value.copy() for value in old]
    scaled_sq = sum(float(np.sum(np.square(delta / max_abs), dtype=np.float64))
                    for delta in deltas)
    scaled_norm = math.sqrt(scaled_sq)
    scale = 1.0
    # Compare without forming max_abs * scaled_norm, which itself can overflow.
    if max_abs > clipping_norm / scaled_norm:
        scale = (clipping_norm / max_abs) / scaled_norm
    clipped = [previous + delta * scale
               for previous, delta in zip(old, deltas)]
    if any(not bool(np.all(np.isfinite(value))) for value in clipped):
        return [value.copy() for value in old]
    return clipped


def add_gaussian_noise(weights, old_weights, std, rng=None):
    """Add N(0, std^2) noise to the (already clipped) weight delta. `std` is the FULL
    RDP-calibrated Gaussian-mechanism standard deviation, with the sensitivity
    already folded in by the caller.

    `rng` is the trusted release-scoped CSPRNG. A predictable or private-data-derived
    seed would void the mechanism assumptions, so callers must use the node-secret
    derivation in ``seeding.py``."""
    rng = _require_secure_rng(rng, "private output perturbation")
    try:
        std = float(std)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("noise std must be finite and non-negative") from exc
    if not math.isfinite(std) or std < 0.0:
        raise ValueError("noise std must be finite and non-negative")
    if (not isinstance(weights, (list, tuple))
            or not isinstance(old_weights, (list, tuple))
            or not old_weights or len(weights) != len(old_weights)):
        raise ValueError("weight count does not match old_weights")

    out = []
    for index, (raw_weight, raw_old) in enumerate(zip(weights, old_weights)):
        weight = np.asarray(raw_weight)
        old = np.asarray(raw_old)
        if (weight.dtype.kind not in "biuf" or old.dtype.kind not in "biuf"
                or weight.shape != old.shape or weight.size < 1):
            raise ValueError("weight %d does not match old_weights" % index)
        weight = np.asarray(weight, dtype=np.float64)
        old = np.asarray(old, dtype=np.float64)
        if (not bool(np.all(np.isfinite(weight)))
                or not bool(np.all(np.isfinite(old)))):
            raise ValueError("noise input %d contains non-finite values" % index)
        noise = np.asarray(
            rng.normal(0.0, std, size=weight.shape), dtype=np.float64)
        if noise.shape != weight.shape or not bool(np.all(np.isfinite(noise))):
            raise RuntimeError("secure Gaussian RNG returned an invalid draw")

        # Absolute clipping before noise is 1-Lipschitz, so it cannot enlarge the
        # already bounded sensitivity.  Clipping again after noise is ordinary DP
        # post-processing.  Limiting noise to +/-2B before the addition is exactly
        # equivalent in the saturated tails and avoids an intermediate overflow.
        base = np.clip(weight, -MAX_RELEASE_ABS, MAX_RELEASE_ABS)
        safe_noise = np.clip(noise, -2.0 * MAX_RELEASE_ABS,
                             2.0 * MAX_RELEASE_ABS)
        released = np.clip(base + safe_noise,
                           -MAX_RELEASE_ABS, MAX_RELEASE_ABS)
        if not bool(np.all(np.isfinite(released))):
            raise RuntimeError("Gaussian post-processing produced non-finite output")
        out.append(np.asarray(released, dtype=np.float64))
    return out


def output_perturbation(new_weights, old_weights, clipping_norm, epsilon, delta,
                        rng=None, num_releases=1):
    """Tier-2 / universal-floor DP in one call: clip the update to C, then add Gaussian
    noise calibrated to the L2 SENSITIVITY of a C-clipped release, which is 2*C -- NOT C.
    Two adjacent datasets each yield an update inside the C-ball, so they can differ by
    up to the ball's diameter 2C; for ARBITRARY code the update is not a sum of
    per-record bounded terms, so the per-record bound is the diameter, not C. (DP-SGD's
    per-sample-gradient SUM is sensitivity C and is accounted separately by Opacus; this
    floor is the only release where the 2C diameter applies.)"""
    clipped = clip_update(new_weights, old_weights, clipping_norm)
    std = compute_output_sigma(
        epsilon, delta, 2.0 * clipping_norm, num_releases=num_releases)
    return add_gaussian_noise(clipped, old_weights, std, rng=rng)


def sample_and_aggregate(block_updates, old_weights, clipping_norm, epsilon, delta,
                         rng=None, num_releases=1):
    """Improved universal floor (Nissim-Raskhodnikova-Smith sample-and-aggregate): given
    the user's black-box update computed INDEPENDENTLY on each of k DISJOINT, data-
    independent blocks of the private data, release the clip-and-average aggregate under
    the Gaussian mechanism.

    Why it is sound AND tighter than `output_perturbation`: each privacy unit (a patient
    when patient IDs are pinned, otherwise one row) lives in exactly ONE block, so under
    replace-one adjacency a fixed-ID unit perturbs one block. We do not assume the patient
    roster/identifier is public or fixed, however: replacing a unit may remove it from one
    keyed block and add it to another. At most TWO block outputs can therefore change, each
    by the C-ball diameter 2C. The mean sensitivity is conservatively
    min(2C, 4C/k), where the outer 2C is the diameter of the mean's own C-ball. Thus k=2
    claims no amplification; k>2 can still improve utility. It composes as an ordinary
    per-round Gaussian release in the training's RDP/PRV accountant.

    The CALLER must (1) build the block partition independently of feature/label VALUES
    (a random row permutation, or a keyed assignment that keeps each patient's rows
    together -- never sorting/stratifying on model inputs) and
    (2) map any failed/non-finite block to a zero delta. Both are leak-safe: a zero delta
    is inside the C-ball, so it cannot escape the conservative multi-block bound, and
    the partition cannot encode feature/label values."""
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
    sensitivity = min(
        2.0 * float(clipping_norm),
        4.0 * float(clipping_norm) / float(k),
    )
    std = compute_output_sigma(
        epsilon, delta, sensitivity, num_releases=num_releases)
    return add_gaussian_noise(mean_new, old, std, rng=rng)


# --------------------------------------------------------------------------- #
# Disclosure backstop (deterministic, independent of DP)
# --------------------------------------------------------------------------- #

def bucket_count(n):
    """Round a count to the nearest power of two (counts < 4 are exact).

    Used so released sample counts (num_examples) never leak an exact node size.
    """
    n = int(n)
    if n <= 3:
        return 0
    return int(2 ** round(math.log2(n)))


# --------------------------------------------------------------------------- #
# Neural submission hardening — close raw-data exfiltration via the state_dict.
#
# DP-SGD only noises the GRADIENTS of trainable parameters. Anything a malicious
# architecture stashes OUTSIDE that path — a registered buffer, a frozen
# parameter, an in-place ``param.data`` write in forward — would otherwise be
# released verbatim by get_torch_params and bypass the noise entirely. The
# default path emits only vetted, stash-free architectures (logreg / MLP /
# linear heads); these node-side gates enforce that invariant so a custom-code
# submission cannot smuggle raw data out through the released weights.
# --------------------------------------------------------------------------- #

def assert_releasable(model):
    """Reject a model whose released state_dict could carry un-noised raw data.

    The released artifact is get_torch_params(model) = state_dict().values(); for
    the (epsilon, delta) guarantee to cover ALL of it, every released tensor must
    be a DP-SGD-trained parameter. We require: (1) NO registered buffers (a
    buffer is released but never receives a noised gradient -> a stash channel);
    (2) EVERY parameter trainable (a frozen parameter is released but never
    noised). A vetted logreg / MLP / linear head has neither.
    """
    buffers = [name for name, _ in model.named_buffers()]
    if buffers:
        raise ValueError(
            "model registers buffers %r: buffers are released in the state_dict "
            "but never receive DP noise (a raw-data stash channel). Submit a "
            "buffer-free architecture, or use the egress fallback." % buffers[:8])
    frozen = [name for name, p in model.named_parameters() if not p.requires_grad]
    if frozen:
        raise ValueError(
            "model has non-trainable parameters %r: they are released but never "
            "DP-noised (a raw-data stash channel). Every parameter must be "
            "trainable on the DP-SGD track." % frozen[:8])


# Vetted NODE-OWNED classes the researcher NEVER supplies (specs are DATA, not code):
# the model_spec graph interpreter. Since the researcher can only name allowlisted ops,
# the only non-torch.nn class ever instantiated on the DP-SGD path is this trusted,
# node-built interpreter. Admitted by EXACT name+module (not isinstance -> no subclass
# smuggling). The Opacus DP layers (DPLSTM/DPGRU/DPMultiheadAttention) will be added
# here when wired, with their hook/cell_type tolerance handled explicitly.
_VETTED_NODE_CLASSES = frozenset({"FiniteClamp", "GraphModule", "RecurrentBlock"})

# Exact Opacus DP-layer classes the node may instantiate (DP-friendly RNN replacements).
# Admitted by exact module + name (the researcher submits only op-enums, never classes,
# so no subclass smuggling). RecurrentBlock SANITIZES their state_dict hooks + cell_type
# at build, so they still pass the strict no-hooks / no-instance-override checks below;
# here we only allow their CLASS ORIGIN.
_VETTED_OPACUS_CLASSES = frozenset({
    "DPLSTM", "DPGRU", "DPRNN", "DPLSTMCell", "DPGRUCell", "DPRNNCell",
    "RNNLinear", "SequenceBias",
})


def _is_node_owned_class(cls):
    """True iff cls is a stock torch.nn layer, an EXACT vetted node-owned class, or an
    EXACT vetted Opacus DP-layer class."""
    mod = cls.__module__
    if mod.startswith("torch.nn"):
        return True
    if mod.rsplit(".", 1)[-1] == "model_spec" and cls.__name__ in _VETTED_NODE_CLASSES:
        return True
    if mod.startswith("opacus.layers") and cls.__name__ in _VETTED_OPACUS_CLASSES:
        return True
    return False


def assert_stock_architecture(model):
    """ROOT defense: the researcher's model object is UNTRUSTED, so allow only a pure
    composition of stock torch.nn layers with NO researcher-injected behaviour at all.
    Successive red-team passes each found a different injection facet -- a custom
    forward (param-.data stash / sample coupling), a lazy buffer, backward/state_dict
    hooks, and an instance ``named_modules`` override that substitutes raw data for
    the noised weights at the release read -- so the gate is exhaustive over the
    object's surface: stock class, NO callable in the instance __dict__ (no method
    override of any kind), NO hooks, stock param containers + stock nn.Parameter/Tensor
    params (no tensor-subclass). The first-party generators (nn.Sequential / nn.Linear
    / nn.ReLU / ...) pass; a genuinely custom model must use the egress track (whole-
    update output perturbation assumes nothing about the model). Call at LOAD.

    CRUCIAL: traverse the RAW ``_modules`` storage, never ``model.modules()`` /
    ``named_modules`` -- those route through the very instance-overridable methods we
    are validating, so the gate itself must not call them (else the override hides).
    """
    import torch
    import torch.nn as nn
    from collections import OrderedDict
    _STOCK_DICT = (dict, OrderedDict)

    def _walk(m):
        yield m
        kids = getattr(m, "_modules", None)
        if type(kids) not in _STOCK_DICT:
            raise ValueError("module %r has a non-stock _modules container (a "
                             "traversal-subversion channel)." % type(m).__name__)
        for child in kids.values():
            if child is not None:
                yield from _walk(child)

    for m in _walk(model):
        cls = type(m)
        if not _is_node_owned_class(cls):
            raise ValueError(
                "non-stock module %r on the DP-SGD track: only stock torch.nn layers and "
                "node-owned vetted classes (%s) are allowed (a custom class is researcher "
                "code). Build from the allowlist, or use the egress track."
                % (cls.__name__, ", ".join(sorted(_VETTED_NODE_CLASSES))))
        # No instance-level method override: a stock module stores only params /
        # buffers / submodules / config in its __dict__, never a callable. Any callable
        # there is a method override -- `forward` (stash / sample-coupling) or
        # `named_modules`/`named_parameters`/`parameters`/... which the release path
        # traverses through, letting it substitute raw data for the noised weights.
        overrides = sorted(a for a, v in vars(m).items() if callable(v))
        if overrides:
            raise ValueError(
                "module %r has instance-level method override(s) %r (a stash / "
                "release-substitution channel); not allowed on the DP-SGD track."
                % (cls.__name__, overrides))
        # No hooks of any kind: a backward hook CAPTURES the raw input Opacus stashes
        # during backward; a state_dict hook REWRITES the noised weights at release.
        hook_attrs = sorted(a for a, v in vars(m).items() if a.endswith("_hooks") and v)
        if hook_attrs:
            raise ValueError(
                "module %r has registered hooks %r: a data-capture (backward) / "
                "release-rewrite (state_dict) channel; not allowed on the DP-SGD "
                "track." % (cls.__name__, hook_attrs))
        # Every parameter must be a STOCK nn.Parameter wrapping a STOCK Tensor: a
        # Parameter/tensor SUBCLASS could intercept .detach()/.cpu()/.numpy() via
        # __torch_function__ and return raw data at the release read.
        pdict = getattr(m, "_parameters", None)
        if type(pdict) not in _STOCK_DICT:
            raise ValueError("module %r has a non-stock _parameters container."
                             % cls.__name__)
        for pname, p in pdict.items():
            if p is not None and (type(p) is not nn.Parameter
                                  or type(p.data) is not torch.Tensor):
                raise ValueError(
                    "module %r parameter %r is not a stock nn.Parameter/Tensor (a "
                    "tensor-subclass exfil channel); not allowed." % (cls.__name__, pname))


_LOSS_ALLOWLIST = {
    "bce_logits":     ("BCEWithLogitsLoss", {}),
    "cross_entropy":  ("CrossEntropyLoss", {}),
    "mse":            ("MSELoss", {}),
    "poisson_nll":    ("PoissonNLLLoss", {"log_input": True}),
    "multilabel_bce": ("BCEWithLogitsLoss", {}),
    "hinge":          ("MultiMarginLoss", {}),  # linear SVM (multiclass margin), per-sample
    "ordinal":        ("BCEWithLogitsLoss", {}),  # ordinal regression via K-1 cumulative tasks (CORN)
}


def _negbin_nll_factory(cfg):
    """Negative-binomial (NB2) negative log-likelihood, log-link, PER-SAMPLE.
    pred = log-mean [N,1]; target = non-negative counts [N,1]. The dispersion
    'size' r (variance = mu + mu^2 / r) is a harmless modelling hyperparameter
    read from the run config: it shapes the loss but NOT the DP guarantee --
    per-sample gradients are clipped to C and noised regardless of the loss, so a
    hostile r can only hurt the client's own fit, never privacy. Mean reduction
    (Opacus calibrates noise assuming it). Decomposes per sample -> DP-SGD-safe."""
    r = float(cfg.get("nb-dispersion", 1.0))
    if not math.isfinite(r) or not 1.0e-6 <= r <= 1.0e12:
        raise ValueError("nb-dispersion must be in [1e-6, 1e12], got %r" % (r,))
    log_r, lgamma_r = math.log(r), math.lgamma(r)

    def negbin_nll(pred, target):
        import torch
        z = pred.reshape(-1)                       # log-mean (log-link)
        y = target.reshape(-1).to(z.dtype)         # non-negative counts
        log_r_plus_mu = torch.logaddexp(torch.full_like(z, log_r), z)   # log(r + exp(z)), overflow-safe
        ll = (torch.lgamma(y + r) - lgamma_r - torch.lgamma(y + 1.0)
              - r * torch.nn.functional.softplus(z - log_r)   # r*log(r/(r+mu)); stable in both limits
              + y * (z - log_r_plus_mu))
        return (-ll).mean()
    return negbin_nll


def _gamma_nll_factory(cfg):
    """Gamma GLM negative log-likelihood, log-link, PER-SAMPLE. pred = log-mean
    [N,1]; target = strictly-positive continuous [N,1] (cost, concentration, length
    of stay). The shape k (variance = mu^2 / k) is a harmless run-config hyperparameter
    -- it shapes the loss, never the clip/noise, so it is no DP lever. Mean reduction;
    decomposes per sample -> DP-SGD-safe. At k=1 this is the exponential NLL z + y*exp(-z)."""
    k = float(cfg.get("gamma-shape", 1.0))
    if not math.isfinite(k) or not 1.0e-6 <= k <= 1.0e12:
        raise ValueError("gamma-shape must be in [1e-6, 1e12], got %r" % (k,))
    lgamma_k, log_k = math.lgamma(k), math.log(k)

    def gamma_nll(pred, target):
        import torch
        z = pred.reshape(-1)                       # log-mean (log-link)
        y = target.reshape(-1).to(z.dtype)         # strictly positive continuous
        ll = ((k - 1.0) * torch.log(y) - k * y * torch.exp(-z)
              - k * (z - log_k) - lgamma_k)
        return (-ll).mean()
    return gamma_nll


def _huber_factory(cfg):
    """Stock per-sample Huber regression loss with a pinned public transition."""
    delta = float(cfg.get("huber-delta", 1.0))
    if not math.isfinite(delta) or not 1.0e-6 <= delta <= 1.0e6:
        raise ValueError("huber-delta must be in [1e-6, 1e6], got %r" % (delta,))
    import torch.nn as nn
    return nn.HuberLoss(delta=delta, reduction="mean")


def _quantile_factory(cfg):
    """Per-sample pinball loss for one public conditional quantile."""
    quantile = float(cfg.get("quantile-level", 0.5))
    if not math.isfinite(quantile) or not 0.0 < quantile < 1.0:
        raise ValueError("quantile-level must be in (0, 1), got %r"
                         % (quantile,))

    def quantile_loss(pred, target):
        import torch
        residual = target.reshape(-1) - pred.reshape(-1)
        return torch.maximum(
            quantile * residual, (quantile - 1.0) * residual).mean()
    return quantile_loss


# Custom TRUSTED per-sample losses (node code, never client code): name -> factory(cfg).
# Each MUST decompose per sample with mean reduction so the DP-SGD sensitivity bound
# holds; enforced by per_sample_independence_probe + the DP safety suite. Hyperparams
# come from the run config but can only shape the loss, never the clip/noise -> no DP
# lever. This is how tight DP is GROWN (vetted node losses), not by trusting client code.
_CUSTOM_LOSS_FACTORY = {
    "negbin_nll": _negbin_nll_factory,
    "gamma_nll": _gamma_nll_factory,
    "huber": _huber_factory,
    "quantile": _quantile_factory,
}


def loss_from_allowlist(name, cfg=None):
    """Instantiate a per-sample-decomposable loss from the node allowlist, with
    reduction='mean'. The loss is NEVER taken from client code: Opacus computes
    per-sample gradients via backward hooks but never inspects the loss, so a
    sample-coupling loss (contrastive / Cox partial-likelihood / a hand-rolled
    ``loss/batch.mean()``) yields well-formed but WRONG per-sample gradients that
    silently defeat the clip-to-C sensitivity bound. Mean reduction is required
    because Opacus calibrates the noise assuming it. Stock losses come from the
    allowlist; vetted custom per-sample losses from _CUSTOM_LOSS_FACTORY (cfg
    supplies only DP-irrelevant shape hyperparameters)."""
    import torch.nn as nn
    if name in _LOSS_ALLOWLIST:
        cls_name, kw = _LOSS_ALLOWLIST[name]
        return getattr(nn, cls_name)(reduction="mean", **kw)
    if name in _CUSTOM_LOSS_FACTORY:
        return _CUSTOM_LOSS_FACTORY[name](cfg or {})
    raise ValueError("loss '%s' is not on the node allowlist %r"
                     % (name, sorted(list(_LOSS_ALLOWLIST) + list(_CUSTOM_LOSS_FACTORY))))


def per_sample_independence_probe(model, criterion, x_sample, y_sample):
    """Best-effort gate for CUSTOM uploaded forwards (default path is vetted code,
    so this only runs when a custodian opts in). Perturb one row's input and
    assert only that row's per-sample gradient changes; a forward that couples
    samples in plain tensor ops (x - x.mean(0), batch-wise attention, cdist(x,x))
    passes ModuleValidator's layer-type denylist yet breaks the per-sample bound.
    Necessary-not-sufficient (data-dependent coupling can still hide), so the
    conservative route for untrusted forwards remains the egress track. Raises on
    detected coupling; fails closed if the probe itself cannot run."""
    import torch
    from opacus import GradSampleModule

    gs = GradSampleModule(model)

    def per_sample_grad(x, y):
        # Seed identically each pass so stochastic layers (dropout) draw the SAME mask,
        # isolating the row-i perturbation; without this, dropout's per-pass randomness
        # would look like cross-sample coupling and false-positive a valid model.
        torch.manual_seed(0)
        gs.zero_grad(set_to_none=True)
        loss = criterion(gs(x), y)
        loss.backward()
        for p in gs.parameters():
            g = getattr(p, "grad_sample", None)
            if g is not None:
                return g.detach().clone()
        raise ValueError("per-sample-independence probe could not read a "
                         "grad_sample; refusing the custom model (fail closed).")

    # The probe's seeding must NOT leak into training (DP noise must stay random):
    # snapshot the global RNG and restore it once the probe is done.
    rng_state = torch.get_rng_state()
    try:
        g0 = per_sample_grad(x_sample, y_sample)
        n = x_sample.shape[0]
        # Perturb EVERY row (not just row 0): a forward that couples via the mean of the
        # OTHER rows (e.g. x - x[1:].mean(0)) leaves row 0 invariant but is exposed the
        # moment any other row is perturbed. For each i, ONLY row i's per-sample gradient
        # may change; if perturbing row i moves another row's gradient, samples couple.
        for i in range(n):
            x2 = x_sample.clone()
            x2[i] = x2[i] + 1.0
            gi = per_sample_grad(x2, y_sample)
            if n > 1:
                other = torch.arange(n) != i
                if not torch.allclose(g0[other], gi[other], atol=1e-5):
                    raise ValueError(
                        "submitted forward graph COUPLES samples (perturbing row %d "
                        "changed another row's per-sample gradient): its DP-SGD "
                        "guarantee would be wrong. Use a per-sample architecture or "
                        "the egress fallback." % i)
    finally:
        torch.set_rng_state(rng_state)


# --------------------------------------------------------------------------- #
# FedBN helpers (keep BatchNorm statistics local; never released)
# --------------------------------------------------------------------------- #

def is_bn_key(key):
    """True if a state_dict key belongs to a BatchNorm-like layer."""
    indicators = (".bn", "batch_norm", ".norm", "running_mean",
                  "running_var", "num_batches_tracked")
    k = key.lower()
    return any(ind in k for ind in indicators)
