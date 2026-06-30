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


def _take_rows(D, idx):
    """Row-subset X or y by integer positions, preserving a pandas object if used."""
    if hasattr(D, "iloc"):
        return D.iloc[idx]
    return np.asarray(D)[idx]


def _safe_block_update(user_mod, old, Xb, yb, cfg):
    """Run the user's update on ONE block, defensively. Any exception, wrong shape, or
    non-finite output collapses to a zero delta (the global model unchanged). This is
    leak-safe -- a zero delta is inside the C-ball, so a data-dependent block failure can
    neither crash/bias the release nor escape the 2C/k sensitivity bound -- and sound."""
    try:
        nw = _as_f64_list(user_mod.local_update([o.copy() for o in old], Xb, yb, cfg))
        if (len(nw) == len(old)
                and all(a.shape == o.shape for a, o in zip(nw, old))
                and all(bool(np.all(np.isfinite(a))) for a in nw)):
            return nw
    except Exception:
        pass
    return [o.copy() for o in old]


def _choose_blocks(n, pcfg):
    """Number of sample-and-aggregate blocks, from SERVER policy + the row count ONLY
    (never from the observed updates). Under replace-one adjacency n is invariant, so
    routing on it is sound; the noise scale implies at most a coarse multiple-of-min_block
    RANGE of n, never the exact private count. Returns 1 to signal the plain floor.

    DEFAULT OFF: the 2C/k gain is sound only if each block update is an INDEPENDENT
    function of its block. For untrusted black-box code sharing this process, a stateful
    local_update could carry state across the sequential block calls and make one record
    affect many blocks -> true sensitivity 2C, not 2C/k. Enabling it soundly requires
    per-block PROCESS isolation (a fresh interpreter per block), which this FL runtime
    cannot add safely today (fork/subprocess in the client segfaults here). So SAA is an
    opt-in for trusted/stateless egress (dsflower.dp_sample_aggregate=TRUE); the plain 2C
    floor remains the universal default."""
    if not bool(pcfg.get("sample_aggregate", False)):
        return 1
    min_block = max(1, int(pcfg.get("sa_min_block", 64)))
    max_blocks = max(1, int(pcfg.get("sa_max_blocks", 8)))
    return max(1, min(max_blocks, int(n) // min_block))


def gated_local_update(user_mod, global_arrays, X, y, cfg, pcfg):
    """Run the app's training from the global model, then apply the DP gate. The NODE (not
    the app) picks the mechanism + parameters: the sample-and-aggregate floor (sensitivity
    2C/k) when there are enough rows for k>=2 blocks, else the plain output-perturbation
    floor (2C). The app's cfg never controls DP."""
    old = _as_f64_list(global_arrays)
    n = int(len(X))
    k = _choose_blocks(n, pcfg)

    if k >= 2:
        # Data-INDEPENDENT random partition into k disjoint blocks: a fresh-entropy
        # permutation of row INDICES (never sorted/stratified by a feature or label, so
        # the partition cannot encode the data). One record lands in exactly one block.
        perm = np.random.default_rng().permutation(n)
        block_updates = [
            _safe_block_update(user_mod, old, _take_rows(X, idx), _take_rows(y, idx), cfg)
            for idx in np.array_split(perm, k)
        ]
        gated = dp_harness.sample_and_aggregate(
            block_updates, old,
            clipping_norm=pcfg["clipping_norm"],
            epsilon=pcfg["epsilon"],
            delta=pcfg["delta"],
        )
    else:
        # Plain output-perturbation floor (sensitivity 2C). validate-or-zero here too: a
        # data-dependent exception, wrong shape, or non-finite output collapses to a zero
        # delta rather than a visible client failure or a non-finite release (both could
        # leak a data predicate). The whole update is clipped into the C-ball regardless,
        # so the 2C bound holds for ANY code (state within one run cannot escape the clip).
        new = _safe_block_update(user_mod, old, X, y, cfg)
        gated = dp_harness.output_perturbation(
            new, old,
            clipping_norm=pcfg["clipping_norm"],
            epsilon=pcfg["epsilon"],
            delta=pcfg["delta"],
        )
    return [g.astype(np.float32) for g in gated]
