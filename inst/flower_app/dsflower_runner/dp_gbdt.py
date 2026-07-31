"""dsFlower DP-GBDT engine (node-side, trusted) — enforced differential privacy
for gradient-boosted decision trees, via an S-GBDT-style curator-side mechanism
(Nuradha et al., "Frugal Differentially Private GBDT", arXiv:2309.12041).

This is the TREES track's privacy core. Like ``dp_harness.py`` it is installed
with the dsFlower node package and runs as TRUSTED code: the researcher ships
only a *spec* (objective, depth, learning rate, ...), never this module, so the
DP guarantee cannot be disabled or weakened by uploaded app code.

Design invariants (identical posture to ``dp_harness.py``):
  * NODE-SIDE CENTRAL DP — each node curates its local dataset and there is NO
    Secure Aggregation. Each node adds the FULL
    Gaussian noise to its own per-leaf gradient/hessian histogram node-side,
    BEFORE any booster bytes leave the SuperNode. The federation server bags the
    already-private local boosters; this is post-processing, so it performs no
    DP work and sees no raw sums.
  * TOTALLY-RANDOM splits — each internal node's (feature, threshold) is drawn
    from a PUBLIC PRNG seeded by (run_token, tree_index, node_index). No data
    flows into split selection, so split selection costs ZERO privacy budget.
    Run tokens are node-local; the deployed server bags complete local boosters,
    so tree structures do not need to align across nodes.
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
    T_total with the closed continuous-order Gaussian RDP bound.

All privacy parameters MUST come from the server-written, tamper-proof manifest.
"""

import hashlib
import math

import numpy as np

try:
    from .seeding import SecureNumpyRng
except ImportError:  # Direct execution by the standalone regression script.
    from seeding import SecureNumpyRng

# --------------------------------------------------------------------------- #
# Accountant — pure-numpy RDP composed across all tree releases.
# --------------------------------------------------------------------------- #

# Absolute engine caps. The manifest applies tighter admin caps, but these checks
# live beside the allocations they protect so direct/library calls cannot bypass
# them and evaluate ``1 << depth`` or build an unbounded booster first.
_MAX_DEPTH = 10
_MAX_TREES = 200
_MAX_BINS = 64
_MAX_FEATURES = 65_536
_MAX_RUN_TOKEN_CHARS = 1024

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


def _require_secure_noise_rng(rng):
    if not isinstance(rng, SecureNumpyRng):
        raise RuntimeError(
            "DP-GBDT requires an explicit release-scoped SecureNumpyRng"
        )
    if not callable(getattr(rng, "normal", None)):
        raise RuntimeError("DP-GBDT secure RNG must provide normal()")
    return rng


def _bounded_int(value, name, lo, hi):
    if (isinstance(value, (bool, np.bool_))
            or not isinstance(value, (int, np.integer))):
        raise ValueError("%s must be an int in [%d, %d], got %r"
                         % (name, lo, hi, value))
    value = int(value)
    if value < lo or value > hi:
        raise ValueError("%s must be an int in [%d, %d], got %r"
                         % (name, lo, hi, value))
    return value


def _finite_float(value, name, *, positive=False):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("%s must be a finite number, got %r" % (name, value))
    if not math.isfinite(value) or (positive and value <= 0.0):
        suffix = " and > 0" if positive else ""
        raise ValueError("%s must be finite%s, got %r" % (name, suffix, value))
    return value


def _finite_xy(X, y):
    """Normalize and reject non-finite private data before any DP mechanism."""
    try:
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64).ravel()
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("X and y must be numeric arrays") from exc
    if X.ndim != 2 or X.shape[0] < 1 or X.shape[1] < 1:
        raise ValueError("X must be a non-empty 2D array")
    if X.shape[1] > _MAX_FEATURES:
        raise ValueError("X has %d features (cap %d)" % (X.shape[1], _MAX_FEATURES))
    if y.shape != (X.shape[0],):
        raise ValueError("y length %d != X rows %d" % (y.size, X.shape[0]))
    if not np.all(np.isfinite(X)) or not np.all(np.isfinite(y)):
        raise ValueError("X and y must contain only finite values")
    return X, y


def _validated_feature_ranges(feature_ranges, n_features=None):
    if not isinstance(feature_ranges, (list, tuple)) or not feature_ranges:
        raise ValueError("feature_ranges must be a non-empty list")
    if len(feature_ranges) > _MAX_FEATURES:
        raise ValueError("feature_ranges has %d entries (cap %d)"
                         % (len(feature_ranges), _MAX_FEATURES))
    if n_features is not None and len(feature_ranges) != int(n_features):
        raise ValueError("feature_ranges length %d != n_features %d"
                         % (len(feature_ranges), n_features))
    result = []
    for j, bounds in enumerate(feature_ranges):
        if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            raise ValueError("feature_ranges[%d] must be [lower, upper]" % j)
        lo = _finite_float(bounds[0], "feature_ranges[%d].lower" % j)
        hi = _finite_float(bounds[1], "feature_ranges[%d].upper" % j)
        span = hi - lo
        if not (hi > lo and math.isfinite(span)):
            raise ValueError("feature_ranges[%d] must be finite with lower < upper "
                             "and a finite span" % j)
        result.append((lo, hi))
    return result


def _validated_run_token(run_token):
    if (not isinstance(run_token, str) or not run_token
            or len(run_token) > _MAX_RUN_TOKEN_CHARS):
        raise ValueError("run_token must be a non-empty string of at most %d characters"
                         % _MAX_RUN_TOKEN_CHARS)
    return run_token


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


def gbdt_epsilon(sigma, delta, t_total):
    """(epsilon, delta) achieved by ``t_total`` sequential Gaussian releases at
    noise multiplier ``sigma``, minimized over every real Renyi order > 1.

    For one Gaussian, RDP(alpha)=alpha/(2*sigma^2). Composing T releases and
    minimizing ``T*alpha/(2*sigma^2) + log(1/delta)/(alpha-1)`` gives the closed
    bound below. Unlike a finite order grid, it remains valid and useful for
    arbitrarily small positive epsilon in the supported policy range.
    """
    try:
        sigma = float(sigma)
        delta = float(delta)
        releases = float(t_total)
    except (TypeError, ValueError, OverflowError):
        return math.inf
    if (not math.isfinite(sigma) or sigma <= 0.0
            or not math.isfinite(delta) or not 0.0 < delta < 1.0
            or not math.isfinite(releases) or releases < 1.0
            or releases != math.floor(releases)):
        return math.inf
    inv_sigma = 1.0 / sigma
    return (releases * inv_sigma * inv_sigma / 2.0
            + inv_sigma * math.sqrt(
                2.0 * releases * math.log(1.0 / delta)))


def calibrate_gbdt_sigma(epsilon, delta, t_total):
    """Conservative closed-form multiplier for the composed Gaussian transcript."""
    epsilon = _finite_float(epsilon, "epsilon", positive=True)
    delta = _finite_float(delta, "delta")
    if not (0 < delta < 1):
        raise ValueError("require epsilon > 0 and 0 < delta < 1")
    t_total = _bounded_int(t_total, "t_total", 1, _MAX_TREES)

    root_log = math.sqrt(2.0 * -math.log(delta))
    positive_root = math.hypot(root_log, math.sqrt(2.0 * epsilon))
    y = (2.0 * epsilon) / (positive_root + root_log)
    sigma = math.sqrt(float(t_total)) / y
    if not math.isfinite(sigma) or sigma <= 0:
        raise RuntimeError("DP-GBDT noise calibration failed; refusing to train.")
    return math.nextafter(float(sigma) * (1.0 + 1.0e-12), math.inf)


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
    a retry of this node's manifest-pinned run."""
    key = (str(run_token) + ":" + ":".join(str(int(p)) for p in parts)).encode()
    return int(hashlib.sha256(key).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF


def random_tree(run_token, tree_index, depth, feature_ranges, n_bins):
    """Complete binary tree of ``depth``; each internal node's (feature,
    threshold) is drawn from a PUBLIC PRNG seeded by (run_token, tree_index,
    node_index). Data-independent ⇒ zero split budget and exact retryability
    within one node run.

    Returns (feat[n_internal], thr[n_internal]) where n_internal = 2^depth - 1
    and node k's children are 2k+1 (left, x<thr) and 2k+2 (right, x>=thr).
    The live federated path bags local boosters, so independent node run tokens
    may intentionally produce different public structures.
    Thresholds lie on bin boundaries inside the manifest-pinned [lo, hi] range.
    """
    run_token = _validated_run_token(run_token)
    tree_index = _bounded_int(tree_index, "tree_index", 0, _MAX_TREES - 1)
    depth = _bounded_int(depth, "depth", 1, _MAX_DEPTH)
    n_bins = _bounded_int(n_bins, "n_bins", 2, _MAX_BINS)
    feature_ranges = _validated_feature_ranges(feature_ranges)
    n_internal = (1 << depth) - 1
    n_features = len(feature_ranges)
    feat = np.empty(n_internal, dtype=np.int64)
    thr = np.empty(n_internal, dtype=np.float64)
    for node in range(n_internal):
        rng = np.random.default_rng(_seed(run_token, tree_index, node))
        j = int(rng.integers(0, n_features))
        lo, hi = feature_ranges[j]
        b = int(rng.integers(1, n_bins))           # bin boundary 1..n_bins-1
        feat[node] = j
        thr[node] = lo + (hi - lo) * (b / float(n_bins))
        if not math.isfinite(thr[node]):
            raise RuntimeError("random-tree threshold is non-finite; refusing to train")
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
    X, y = _finite_xy(X, y)
    pid = np.asarray(patient_ids).ravel()
    if pid.shape != (X.shape[0],):
        raise ValueError("patient_ids length %d != X rows %d" % (pid.size, X.shape[0]))
    for value in pid:
        if value is None or (isinstance(value, (float, np.floating))
                             and not math.isfinite(float(value))):
            raise ValueError("patient_ids must not contain missing values")
    try:
        uniq = np.unique(pid)
    except (TypeError, ValueError) as exc:
        raise ValueError("patient_ids must have a consistently comparable type") from exc
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
                          sigma, delta2, g_star, h_star, n_leaves, rng):
    """ONE round, node-side: compute this node's per-leaf (G, H) histogram for the
    given (already public) tree structure, then add the FULL Gaussian noise
    on-node before returning — the curator-side DP release. ``current_margin`` is F(x_i)
    from the global booster grown so far (post-processing; DP-safe).

    Returns the noised 2L vector S̃ = concat(G̃, H̃). The caller (ClientApp) ships
    this; it never ships raw sums.
    """
    rng = _require_secure_noise_rng(rng)
    X, y = _finite_xy(X, y)
    depth = _bounded_int(depth, "depth", 1, _MAX_DEPTH)
    n_leaves = _bounded_int(n_leaves, "n_leaves", 2, 1 << _MAX_DEPTH)
    if n_leaves != 1 << depth:
        raise ValueError("n_leaves must equal 2^depth")
    n_internal = n_leaves - 1
    try:
        if len(feat) != n_internal or len(thr) != n_internal:
            raise ValueError("feat/thr must have length %d" % n_internal)
        raw_feat = np.asarray(feat)
        feat = np.asarray(feat, dtype=np.int64)
        thr = np.asarray(thr, dtype=np.float64)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("feat/thr must be numeric vectors of length %d" % n_internal) from exc
    if (raw_feat.shape != (n_internal,) or feat.shape != (n_internal,)
            or not np.issubdtype(raw_feat.dtype, np.integer)):
        raise ValueError("feat must be an integer vector of length %d" % n_internal)
    if thr.shape != (n_internal,) or not np.all(np.isfinite(thr)):
        raise ValueError("thr must be a finite vector of length %d" % n_internal)
    if np.any(feat < 0) or np.any(feat >= X.shape[1]):
        raise ValueError("tree feature index is outside X's columns")
    try:
        current_margin = np.asarray(current_margin, dtype=np.float64).ravel()
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("current_margin must be numeric") from exc
    if (current_margin.shape != (X.shape[0],)
            or not np.all(np.isfinite(current_margin))):
        raise ValueError("current_margin must be finite with one value per X row")
    sigma = _finite_float(sigma, "sigma", positive=True)
    delta2 = _finite_float(delta2, "delta2", positive=True)
    g_star = _finite_float(g_star, "g_star", positive=True)
    h_star = _finite_float(h_star, "h_star", positive=True)
    noise_std = sigma * delta2
    if not math.isfinite(noise_std):
        raise ValueError("sigma * delta2 must be finite")

    p = _sigmoid(current_margin)
    g = np.clip(p - y, -g_star, g_star)
    h = np.clip(p * (1.0 - p), 0.0, h_star)
    leaf = route_to_leaf(X, feat, thr, depth)
    G = np.zeros(n_leaves, dtype=np.float64)
    H = np.zeros(n_leaves, dtype=np.float64)
    np.add.at(G, leaf, g)
    np.add.at(H, leaf, h)
    if not np.all(np.isfinite(G)) or not np.all(np.isfinite(H)):
        raise RuntimeError("DP-GBDT raw histogram overflowed; refusing to run the mechanism")
    G_noise = np.asarray(rng.normal(0.0, noise_std, size=n_leaves),
                         dtype=np.float64)
    H_noise = np.asarray(rng.normal(0.0, noise_std, size=n_leaves),
                         dtype=np.float64)
    if (G_noise.shape != (n_leaves,) or H_noise.shape != (n_leaves,)
            or not np.all(np.isfinite(G_noise)) or not np.all(np.isfinite(H_noise))):
        raise RuntimeError("DP-GBDT noise generator returned a non-finite/invalid draw")
    G_tilde = G + G_noise
    H_tilde = H + H_noise
    released = np.concatenate([G_tilde, H_tilde])
    if not np.all(np.isfinite(released)):
        raise RuntimeError("DP-GBDT noised histogram is non-finite; refusing release")
    return released


def grow_tree_from_histograms(summed_hist, n_leaves, reg_lambda, learning_rate):
    """Server-side post-processing: given the SUM of already-noised histograms
    across nodes (S̃ = [ΣG̃, ΣH̃]), compute the Newton leaf weights. No DP work —
    the noise was added node-side; summing private releases over disjoint rows is
    post-processing.

        w_ℓ = -η · G̃_ℓ / max(λ, H̃_ℓ + λ)        (denominator guard)
    """
    n_leaves = _bounded_int(n_leaves, "n_leaves", 2, 1 << _MAX_DEPTH)
    try:
        if len(summed_hist) != 2 * n_leaves:
            raise ValueError("wrong histogram length")
        s = np.asarray(summed_hist, dtype=np.float64)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("summed_hist must be numeric") from exc
    if s.shape != (2 * n_leaves,) or not np.all(np.isfinite(s)):
        raise ValueError("summed_hist must be a finite vector of length %d"
                         % (2 * n_leaves))
    G = s[:n_leaves]
    H = s[n_leaves:2 * n_leaves]
    lam = _finite_float(reg_lambda, "reg_lambda", positive=True)
    eta = _finite_float(learning_rate, "learning_rate", positive=True)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        numerator = -eta * G
        shifted_h = H + lam
        denominator = np.maximum(lam, shifted_h)
        w = (numerator / denominator).astype(np.float64)
    if (not np.all(np.isfinite(numerator))
            or not np.all(np.isfinite(shifted_h))):
        raise RuntimeError("DP-GBDT leaf-weight arithmetic overflowed; refusing release")
    if not np.all(np.isfinite(w)):
        raise RuntimeError("DP-GBDT leaf weights are non-finite; refusing release")
    return w


# --------------------------------------------------------------------------- #
# Single-process loop — used by unit tests and the degenerate single-node case.
# --------------------------------------------------------------------------- #

def fit_dp_gbdt(X, y, *, objective, depth, n_trees, learning_rate, reg_lambda,
                feature_ranges, n_bins, run_token, epsilon, delta,
                noise_rng, base_score=0.5, patient_ids=None):
    """Train a DP-GBDT booster with trusted-curator DP at one node: each tree is one
    Gaussian release, σ is calibrated once for T_total = n_trees. Returns the
    booster dict (our own serializable format).

    DEPLOYED FEDERATION = BAGGING: every node runs this full local fit on its own
    rows and the server BAGS the M boosters (server_app._bag_boosters: concat trees,
    scale leaf weights by 1/M). The ``node_noised_histogram`` / ``grow_tree_from_histograms``
    primitives below implement the alternative HISTOGRAM-SUMMING design (one summed
    Newton step per tree across all rows). It was evaluated and NOT adopted: at equal
    per-node privacy its utility is empirically equal-or-worse than bagging (bagging
    already averages M per-leaf estimates -> the same ~sqrt(M) noise reduction), while
    it costs T sequential Flower round-trips. The primitives are kept for tests and as
    a reference implementation, but the live path is bagging.

    ``noise_rng`` is deliberately mandatory and type-checked. Production and
    direct callers share the same CSPRNG contract; tests that need failure
    doubles subclass ``SecureNumpyRng`` explicitly instead of activating an
    insecure fallback.
    """
    objective = str(objective)
    g_star, h_star = clip_bounds(objective)
    depth = _bounded_int(depth, "depth", 1, _MAX_DEPTH)
    t_total = _bounded_int(n_trees, "n_trees", 1, _MAX_TREES)
    n_bins = _bounded_int(n_bins, "n_bins", 2, _MAX_BINS)
    learning_rate = _finite_float(learning_rate, "learning_rate", positive=True)
    reg_lambda = _finite_float(reg_lambda, "reg_lambda", positive=True)
    epsilon = _finite_float(epsilon, "epsilon", positive=True)
    delta = _finite_float(delta, "delta")
    if not (0.0 < delta < 1.0):
        raise ValueError("require 0 < delta < 1")
    base_score = _finite_float(base_score, "base_score")
    if not (0.0 <= base_score <= 1.0):
        raise ValueError("base_score must be in [0, 1]")
    run_token = _validated_run_token(run_token)
    noise_rng = _require_secure_noise_rng(noise_rng)

    # This gate intentionally precedes patient pooling and every noise draw. Legacy
    # staged data may retain missing values (drop_missing=FALSE); NaN/Inf must fail
    # closed instead of becoming data-dependent routing or a non-finite release.
    X, y = _finite_xy(X, y)
    if patient_ids is not None:
        X, y = pool_by_patient(X, y, patient_ids)
    n, n_features = X.shape
    feature_ranges = _validated_feature_ranges(feature_ranges, n_features)
    delta2 = replace_one_sensitivity(g_star, h_star)
    n_leaves = 1 << depth
    sigma = calibrate_gbdt_sigma(epsilon, delta, t_total)   # ONE σ up front

    base_margin = _logit(base_score)
    if not math.isfinite(base_margin):
        raise RuntimeError("DP-GBDT base margin is non-finite; refusing to train")
    booster = {"objective": objective, "depth": depth, "n_bins": n_bins,
               "base_margin": base_margin, "learning_rate": learning_rate,
               "feature_ranges": [[a, b] for a, b in feature_ranges],
               "sigma": sigma, "delta2": delta2,
               "epsilon": epsilon, "delta": delta, "trees": []}
    F = np.full(n, base_margin, dtype=np.float64)
    # ONE release-scoped CSPRNG drives every tree's leaf noise.
    debits = 0
    for t in range(t_total):
        feat, thr = random_tree(run_token, t, depth, feature_ranges, n_bins)
        s_tilde = node_noised_histogram(
            X, y, F, feat, thr, depth, sigma=sigma, delta2=delta2,
            g_star=g_star, h_star=h_star, n_leaves=n_leaves, rng=noise_rng)
        if not np.all(np.isfinite(s_tilde)):
            raise RuntimeError("DP-GBDT histogram is non-finite; refusing release")
        debits += 1
        w = grow_tree_from_histograms(s_tilde, n_leaves, reg_lambda, learning_rate)
        if not np.all(np.isfinite(w)):
            raise RuntimeError("DP-GBDT leaf weights are non-finite; refusing release")
        leaf = route_to_leaf(X, feat, thr, depth)
        next_F = F + w[leaf]
        if not np.all(np.isfinite(next_F)):
            raise RuntimeError("DP-GBDT margin is non-finite; refusing release")
        booster["trees"].append({"feat": feat.tolist(), "thr": thr.tolist(),
                                  "w": w.tolist()})
        F = next_F
    assert debits == t_total, "DP-GBDT release count != T_total; refusing release"
    return booster
