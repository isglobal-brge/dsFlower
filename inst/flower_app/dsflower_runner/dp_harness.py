"""dsFlower DP harness (node-side, trusted) — always-on differential privacy.

This module is the framework's privacy core. It is installed with the dsFlower
node package and runs as TRUSTED code: the researcher never ships it, so the DP
guarantee cannot be disabled or weakened by uploaded app code.

Design (dsFlower 2.0):
  * DP is ALWAYS applied — there are no privacy profiles and no "off" path.
  * Local DP only — there is no Secure Aggregation. Each node adds the full
    noise calibrated to the target (epsilon, delta); the aggregate is the mean
    of already-private updates (post-processing, so the guarantee composes).
  * Two enforcement tiers (see ARCHITECTURE.md §5):
      - Tier 1 (model submission): `make_private_dpsgd` runs Opacus DP-SGD with
        per-sample gradient clipping + Gaussian noise; tight DP, good utility.
      - Tier 2 (arbitrary app):    `output_perturbation` hard-clips the whole
        weight delta to L2 norm C and adds Gaussian noise; coarse but holds for
        ANY update.
  * Accounting uses the PRV accountant when available (tighter than RDP ⇒ less
    noise for the same epsilon), falling back to RDP, then to the analytic
    Gaussian mechanism.

All privacy parameters MUST come from the server-written, tamper-proof
manifest.json — never from client-controlled pyproject config.
"""

import math

import numpy as np


# --------------------------------------------------------------------------- #
# Tier 1 — Opacus DP-SGD (per-sample gradient clipping + Gaussian noise)
# --------------------------------------------------------------------------- #

def calibrate_noise_multiplier(epsilon, delta, sample_rate, total_epochs):
    """Noise multiplier for the target (epsilon, delta) over all DP-SGD steps.

    Prefers the PRV accountant (tighter than RDP), then RDP, then the analytic
    Gaussian mechanism as a conservative single-shot fallback. The noise is
    calibrated over the TOTAL number of local epochs (num_rounds * local_epochs)
    because RDP/PRV compose over steps, independent of how many federated rounds
    those steps are spread across — this is what makes one-shot training cost the
    same epsilon as many rounds.
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
    # Analytic Gaussian mechanism (conservative, single-shot).
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


# --------------------------------------------------------------------------- #
# Tier 2 — output perturbation (clip the whole update + Gaussian noise)
# --------------------------------------------------------------------------- #

def compute_output_sigma(epsilon, delta, clipping_norm):
    """Analytic Gaussian-mechanism noise scale for a single bounded release.

    sigma = sqrt(2 * ln(1.25 / delta)) * (clipping_norm / epsilon).

    For multi-round Tier-2 training the caller must compose epsilon over rounds
    (DP-FedAvg, McMahan et al. 2018) before passing the per-round epsilon here.
    """
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
        if not cls.__module__.startswith("torch.nn"):
            raise ValueError(
                "non-stock module %r on the DP-SGD track: only stock torch.nn modules "
                "are allowed (a custom class is researcher code). Build from torch.nn "
                "layers, or use the egress track." % cls.__name__)
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
}


def loss_from_allowlist(name):
    """Instantiate a per-sample-decomposable loss from the node allowlist, with
    reduction='mean'. The loss is NEVER taken from client code: Opacus computes
    per-sample gradients via backward hooks but never inspects the loss, so a
    sample-coupling loss (contrastive / Cox partial-likelihood / a hand-rolled
    ``loss/batch.mean()``) yields well-formed but WRONG per-sample gradients that
    silently defeat the clip-to-C sensitivity bound. Mean reduction is required
    because Opacus calibrates the noise assuming it."""
    import torch.nn as nn
    if name not in _LOSS_ALLOWLIST:
        raise ValueError("loss '%s' is not on the node allowlist %r"
                         % (name, sorted(_LOSS_ALLOWLIST)))
    cls_name, kw = _LOSS_ALLOWLIST[name]
    return getattr(nn, cls_name)(reduction="mean", **kw)


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
