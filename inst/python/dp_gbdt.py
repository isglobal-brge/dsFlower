"""dsFlower DP-GBDT engine (node-side, trusted) — enforced differential privacy
for gradient-boosted decision trees, via the S-GBDT-style local-DP mechanism
(Nuradha et al., "Frugal Differentially Private GBDT", arXiv:2309.12041).

This is the TREES track's privacy core. Like ``dp_harness.py`` it is installed
with the dsFlower node package and runs as TRUSTED code: the researcher ships
only a *spec* (objective, depth, learning rate, ...), never this module, so the
DP guarantee cannot be disabled or weakened by uploaded app code.

Design invariants (identical posture to ``dp_harness.py``):
  * LOCAL DP only — there is NO Secure Aggregation. Each node adds the FULL
    Gaussian noise to its own per-leaf gradient/hessian histogram node-side,
    BEFORE any booster bytes leave the SuperNode. The federation aggregate (sum
    of already-noised histograms over disjoint rows) is post-processing, so the
    guarantee composes and the untrusted researcher-side ServerApp performs no
    DP work and sees no raw sums.
  * TOTALLY-RANDOM splits — each internal node's (feature, threshold) is drawn
    from a PUBLIC PRNG seeded by (run_token, tree_index, node_index). No data
    flows into split selection, so split selection costs ZERO privacy budget and
    every node grows the identical structure for a given tree (same seed ⇒ leaf
    ℓ means the same routing predicate federation-wide).
  * The ONLY data-touching release per tree is the leaf histogram
    S_t = (G_leaf, H_leaf) in R^{2L}. One individual (a patient under per-patient
    pooling, else a row) routes to exactly ONE leaf, so the replace-one L2
    sensitivity is Δ₂ = sqrt((2·g*)² + (h*)²) — the worst case is a *same-leaf*
    swap, where the gradient coordinate swings by up to 2·g* and the hessian by
    up to h*.
  * Leaf weights are the Newton step computed from the NOISED histogram
    (post-processing of S̃_t — no separate, separately-charged leaf release).
  * Accounting: each S̃_t is the Gaussian mechanism at noise multiplier σ ⇒
    (α, α/(2σ²))-RDP. There are exactly K = T_total releases per contributing
    record (depth does NOT multiply K — one release per tree, not per level),
    composed SEQUENTIALLY by RDP addition. σ is calibrated ONCE up front for
    T_total. The accountant mirrors privacy_ledger.R::.rdp_gaussian /
    .rdp_to_dp byte-for-byte (the trees venv has no opacus) and FAILS CLOSED if
    it cannot solve — never an analytic per-release fallback.

All privacy parameters MUST come from the server-written, tamper-proof manifest.
"""

import hashlib
import math

import numpy as np

# --------------------------------------------------------------------------- #
# Accountant — pure-numpy RDP, mirroring privacy_ledger.R exactly.
# --------------------------------------------------------------------------- #

# Mirror privacy_ledger.R::.RDP_ORDERS exactly so the node-side calibration and
# the R-side ledger agree on what "(epsilon, delta)" means.
_RDP_ORDERS = (1.25, 1.5, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0)

# Objective allowlist: each maps to FINITE per-instance gradient/hessian clip
# bounds (g*, h*). Only objectives whose gradient AND hessian are provably
# bounded are DP-safe without an admin-set clip; unbounded-gradient objectives
# (e.g. reg:squarederror) are REJECTED here, exactly as Opacus' ModuleValidator
# rejects BatchNorm on the neural track.
#   binary:logistic — p = sigmoid(F) in (0,1); g = p - y in (-1,1) ⇒ g* = 1;
#                      h = p(1-p) in (0, 1/4] ⇒ h* = 1/4.
_OBJECTIVE_CLIP = {
    "binary:logistic": (1.0, 0.25),
}


def clip_bounds(objective):
    """Return finite (g*, h*) gradient/hessian clip bounds for an allowlisted
    objective; reject anything else (fail closed)."""
    obj = str(objective)
    if obj not in _OBJECTIVE_CLIP:
        raise ValueError(
            "DP-GBDT objective %r is not on the bounded-gradient allowlist %r; "
            "unbounded-gradient objectives need an admin-set clip and are "
            "refused rather than trained with a wrong sensitivity."
            % (obj, sorted(_OBJECTIVE_CLIP)))
    return _OBJECTIVE_CLIP[obj]


def replace_one_sensitivity(g_star, h_star):
    """Replace-one L2 sensitivity Δ₂ of the per-leaf (Σg, Σh) 2-vector.

    Worst case is a SAME-leaf swap (row i → i' routes to the same leaf): the
    gradient coordinate swings by up to 2·g* and the hessian by up to h*, so
    Δ₂ = sqrt((2·g*)² + (h*)²). The smaller sqrt(2)·sqrt(g*²+h*²) only covers a
    DIFFERENT-leaf swap and under-noises; replace-one adjacency requires the
    larger same-leaf constant.
    """
    return math.sqrt((2.0 * float(g_star)) ** 2 + float(h_star) ** 2)


def _rdp_gaussian(alpha, sigma):
    """RDP of the Gaussian mechanism at order alpha, noise multiplier sigma
    (= noise_std / sensitivity). Mirrors privacy_ledger.R::.rdp_gaussian."""
    if sigma <= 0:
        return math.inf
    return alpha / (2.0 * sigma * sigma)


def _rdp_to_dp(rdp_vec, orders, delta):
    """RDP → (epsilon, delta) conversion. Mirrors privacy_ledger.R::.rdp_to_dp:
    epsilon(delta) = min over alpha of [rdp(alpha) + log(1/delta)/(alpha-1)]."""
    if delta <= 0 or delta >= 1:
        return math.inf
    return min(r + math.log(1.0 / delta) / (a - 1.0)
               for r, a in zip(rdp_vec, orders))


def gbdt_epsilon(sigma, delta, t_total, orders=_RDP_ORDERS):
    """(epsilon, delta) achieved by ``t_total`` sequential Gaussian releases at
    noise multiplier ``sigma`` (RDP composes by addition)."""
    rdp_vec = [t_total * _rdp_gaussian(a, sigma) for a in orders]
    return _rdp_to_dp(rdp_vec, orders, delta)


def calibrate_gbdt_sigma(epsilon, delta, t_total, orders=_RDP_ORDERS):
    """Smallest Gaussian noise multiplier σ s.t. ``t_total`` composed releases
    achieve (epsilon, delta)-DP. RDP-primary, pure-numpy, FAILS CLOSED.

    Returns σ (= noise_std / sensitivity). The caller draws N(0, (σ·Δ₂)²) on each
    of the 2L histogram coordinates. ε is monotonically decreasing in σ, so we
    bracket then bisect. Raises RuntimeError rather than returning an
    under-noising fallback.
    """
    if not (epsilon > 0) or not (0 < delta < 1):
        raise ValueError("require epsilon > 0 and 0 < delta < 1")
    t_total = max(1, int(t_total))

    def eps_at(sigma):
        return gbdt_epsilon(sigma, delta, t_total, orders)

    lo, hi = 1.0e-3, 1.0
    # Grow hi until enough noise meets the budget.
    bracketed = False
    for _ in range(64):
        if eps_at(hi) <= epsilon:
            bracketed = True
            break
        hi *= 2.0
    if not bracketed:
        raise RuntimeError(
            "DP-GBDT noise calibration could not bracket a multiplier for "
            "(epsilon=%g, delta=%g, T_total=%d); refusing to train rather than "
            "under-noise." % (epsilon, delta, t_total))
    if eps_at(lo) <= epsilon:
        # Even negligible noise already satisfies a (very loose) budget.
        sigma = lo
    else:
        for _ in range(100):
            mid = 0.5 * (lo + hi)
            if eps_at(mid) <= epsilon:
                hi = mid
            else:
                lo = mid
        sigma = hi  # hi-side always satisfies the budget
    if not math.isfinite(sigma) or sigma <= 0:
        raise RuntimeError("DP-GBDT noise calibration failed; refusing to train.")
    return float(sigma)


# --------------------------------------------------------------------------- #
# Link function helpers (binary:logistic).
# --------------------------------------------------------------------------- #

def _sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -60.0, 60.0)))


def _logit(p):
    p = min(1.0 - 1e-9, max(1e-9, float(p)))
    return math.log(p / (1.0 - p))


# --------------------------------------------------------------------------- #
# Fixed, data-independent structure: bins + totally-random trees.
# --------------------------------------------------------------------------- #

def _seed(run_token, *parts):
    """Deterministic 63-bit seed from the run token + integer parts. Identical on
    every node (run_token is manifest-pinned), so trees match federation-wide."""
    key = (str(run_token) + ":" + ":".join(str(int(p)) for p in parts)).encode()
    return int(hashlib.sha256(key).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF


def random_tree(run_token, tree_index, depth, feature_ranges, n_bins):
    """Complete binary tree of ``depth``; each internal node's (feature,
    threshold) is drawn from a PUBLIC PRNG seeded by (run_token, tree_index,
    node_index). Data-independent ⇒ zero split budget, identical on every node.

    Returns (feat[n_internal], thr[n_internal]) where n_internal = 2^depth - 1
    and node k's children are 2k+1 (left, x<thr) and 2k+2 (right, x>=thr).
    Thresholds lie on bin boundaries inside the manifest-pinned [lo, hi] range.
    """
    depth = int(depth)
    n_internal = (1 << depth) - 1
    n_features = len(feature_ranges)
    n_bins = max(2, int(n_bins))
    feat = np.empty(n_internal, dtype=np.int64)
    thr = np.empty(n_internal, dtype=np.float64)
    for node in range(n_internal):
        rng = np.random.default_rng(_seed(run_token, tree_index, node))
        j = int(rng.integers(0, n_features))
        lo, hi = float(feature_ranges[j][0]), float(feature_ranges[j][1])
        if not (hi > lo):
            hi = lo + 1.0
        b = int(rng.integers(1, n_bins))           # bin boundary 1..n_bins-1
        feat[node] = j
        thr[node] = lo + (hi - lo) * (b / float(n_bins))
    return feat, thr


def route_to_leaf(X, feat, thr, depth):
    """Leaf index in [0, 2^depth) for each row of X following the complete-tree
    structure (root 0; children of k are 2k+1/2k+2; go right iff x[feat] >= thr)."""
    X = np.asarray(X, dtype=np.float64)
    n = X.shape[0]
    depth = int(depth)
    node = np.zeros(n, dtype=np.int64)
    rows = np.arange(n)
    for _ in range(depth):
        j = feat[node]
        t = thr[node]
        go_right = X[rows, j] >= t
        node = 2 * node + 1 + go_right.astype(np.int64)
    return node - ((1 << depth) - 1)


# --------------------------------------------------------------------------- #
# Per-patient pooling (DP unit = patient). Mirrors the neural _pool_by_patient.
# --------------------------------------------------------------------------- #

def pool_by_patient(X, y, patient_ids):
    """Collapse each patient's rows into ONE row (mean features, majority/first
    label) so the DP unit is the patient: one patient then routes to one leaf and
    contributes one clamped (g, h), bounding its replace-one sensitivity to Δ₂.
    Returns (X_pooled, y_pooled)."""
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64).ravel()
    pid = np.asarray(patient_ids).ravel()
    uniq = np.unique(pid)
    Xp = np.empty((len(uniq), X.shape[1]), dtype=np.float64)
    yp = np.empty(len(uniq), dtype=np.float64)
    for k, u in enumerate(uniq):
        m = pid == u
        Xp[k] = X[m].mean(axis=0)
        # one outcome per patient: round the mean label (ties → 1) to {0,1}.
        yp[k] = 1.0 if y[m].mean() >= 0.5 else 0.0
    return Xp, yp


# --------------------------------------------------------------------------- #
# Prediction (numpy; no xgboost dependency — the booster is our own format).
# --------------------------------------------------------------------------- #

def predict_margin(booster, X):
    """Raw margin F(x) = base_margin + Σ_t w_t[leaf_t(x)] over the ensemble."""
    X = np.asarray(X, dtype=np.float64)
    F = np.full(X.shape[0], float(booster["base_margin"]), dtype=np.float64)
    depth = int(booster["depth"])
    for tree in booster["trees"]:
        feat = np.asarray(tree["feat"], dtype=np.int64)
        thr = np.asarray(tree["thr"], dtype=np.float64)
        w = np.asarray(tree["w"], dtype=np.float64)
        leaf = route_to_leaf(X, feat, thr, depth)
        F = F + w[leaf]
    return F


def predict_proba(booster, X):
    """sigmoid(margin) for binary:logistic."""
    return _sigmoid(predict_margin(booster, X))


# --------------------------------------------------------------------------- #
# Federated primitives — node-side release + server-side post-processing.
# --------------------------------------------------------------------------- #

def node_noised_histogram(X, y, current_margin, feat, thr, depth, *,
                          sigma, delta2, g_star, h_star, n_leaves):
    """ONE round, node-side: compute this node's per-leaf (G, H) histogram for the
    given (already public) tree structure, then add the FULL Gaussian noise
    LOCALLY before returning — the local-DP release. ``current_margin`` is F(x_i)
    from the global booster grown so far (post-processing; DP-safe).

    Returns the noised 2L vector S̃ = concat(G̃, H̃). The caller (ClientApp) ships
    this; it never ships raw sums.
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64).ravel()
    p = _sigmoid(np.asarray(current_margin, dtype=np.float64))
    g = np.clip(p - y, -g_star, g_star)
    h = np.clip(p * (1.0 - p), 0.0, h_star)
    leaf = route_to_leaf(X, feat, thr, depth)
    G = np.zeros(n_leaves, dtype=np.float64)
    H = np.zeros(n_leaves, dtype=np.float64)
    np.add.at(G, leaf, g)
    np.add.at(H, leaf, h)
    noise_std = float(sigma) * float(delta2)
    G_tilde = G + np.random.normal(0.0, noise_std, size=n_leaves)
    H_tilde = H + np.random.normal(0.0, noise_std, size=n_leaves)
    return np.concatenate([G_tilde, H_tilde])


def grow_tree_from_histograms(summed_hist, n_leaves, reg_lambda, learning_rate):
    """Server-side post-processing: given the SUM of already-noised histograms
    across nodes (S̃ = [ΣG̃, ΣH̃]), compute the Newton leaf weights. No DP work —
    the noise was added node-side; summing private releases over disjoint rows is
    post-processing.

        w_ℓ = -η · G̃_ℓ / max(λ, H̃_ℓ + λ)        (denominator guard)
    """
    s = np.asarray(summed_hist, dtype=np.float64)
    G = s[:n_leaves]
    H = s[n_leaves:2 * n_leaves]
    lam = float(reg_lambda)
    return (-float(learning_rate) * G / np.maximum(lam, H + lam)).astype(np.float64)


# --------------------------------------------------------------------------- #
# Single-process loop — used by unit tests and the degenerate single-node case.
# --------------------------------------------------------------------------- #

def fit_dp_gbdt(X, y, *, objective, depth, n_trees, learning_rate, reg_lambda,
                feature_ranges, n_bins, run_token, epsilon, delta,
                base_score=0.5, patient_ids=None):
    """Train a DP-GBDT booster in one process with LOCAL DP. Equivalent to the
    federated loop with a single shard: each tree is one Gaussian release, σ is
    calibrated once for T_total = n_trees. Returns the booster dict (our own
    serializable format). Used by tests and the single-node path; the multi-node
    path drives ``node_noised_histogram`` / ``grow_tree_from_histograms`` round
    by round through Flower instead.
    """
    if patient_ids is not None:
        X, y = pool_by_patient(X, y, patient_ids)
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64).ravel()
    n, n_features = X.shape
    if len(feature_ranges) != n_features:
        raise ValueError("feature_ranges length %d != n_features %d"
                         % (len(feature_ranges), n_features))
    g_star, h_star = clip_bounds(objective)
    delta2 = replace_one_sensitivity(g_star, h_star)
    depth = int(depth)
    n_leaves = 1 << depth
    t_total = max(1, int(n_trees))
    sigma = calibrate_gbdt_sigma(epsilon, delta, t_total)   # ONE σ up front

    base_margin = _logit(float(base_score))
    booster = {"objective": objective, "depth": depth, "n_bins": int(n_bins),
               "base_margin": base_margin, "learning_rate": float(learning_rate),
               "feature_ranges": [[float(a), float(b)] for a, b in feature_ranges],
               "sigma": float(sigma), "delta2": float(delta2),
               "epsilon": float(epsilon), "delta": float(delta), "trees": []}
    F = np.full(n, base_margin, dtype=np.float64)
    debits = 0
    for t in range(t_total):
        feat, thr = random_tree(run_token, t, depth, feature_ranges, n_bins)
        s_tilde = node_noised_histogram(
            X, y, F, feat, thr, depth, sigma=sigma, delta2=delta2,
            g_star=g_star, h_star=h_star, n_leaves=n_leaves)
        debits += 1
        w = grow_tree_from_histograms(s_tilde, n_leaves, reg_lambda, learning_rate)
        booster["trees"].append({"feat": feat.tolist(), "thr": thr.tolist(),
                                  "w": w.tolist()})
        leaf = route_to_leaf(X, feat, thr, depth)
        F = F + w[leaf]
    assert debits == t_total, "DP-GBDT release count != T_total; refusing release"
    return booster
