"""Unit + federated-simulation tests for dp_gbdt.py (pure numpy).

Run:  python3 dsFlower/inst/python/tests/test_dp_gbdt.py
The DP-GBDT engine is pure numpy, so these run off-node for fast iteration and
double as the node-side regression suite.
"""
import json
import math
import os
import sys

RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)
import numpy as np
import dp_gbdt as G

ok = fail = 0


def check(name, cond):
    global ok, fail
    if cond:
        ok += 1
        print(f"  PASS  {name}")
    else:
        fail += 1
        print(f"  FAIL  {name}")


def rejects(fn):
    try:
        fn()
        return False
    except Exception:
        return True


def raises_value_error(fn):
    """Distinguish a precondition rejection from a later allocator/RNG failure."""
    try:
        fn()
        return False
    except ValueError:
        return True
    except Exception:
        return False


def auc(y, p):
    y = np.asarray(y)
    n1 = int(y.sum()); n0 = len(y) - n1
    if n1 == 0 or n0 == 0:
        return 0.5
    ranks = np.argsort(np.argsort(p)) + 1
    return (ranks[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


def _test_secure_rng(seed):
    """Explicit deterministic CSPRNG injection for this regression script."""
    return G.SecureNumpyRng(int(seed).to_bytes(32, "big"))


# --------------------------------------------------------------------------- #
print("== sensitivity / allowlist ==")
d2 = G.replace_one_sensitivity(1.0, 0.25)
check("replace_one(1,0.25)=sqrt(4+1/16)~2.0156", abs(d2 - math.sqrt(4 + 1 / 16)) < 1e-9)
check("replace_one > sqrt(2)*sqrt(g^2+h^2) (the under-noising constant)",
      d2 > math.sqrt(2) * math.sqrt(1 + 0.0625))
check("binary:logistic -> (1,0.25)", G.clip_bounds("binary:logistic") == (1.0, 0.25))
try:
    G.clip_bounds("reg:squarederror"); check("regression requires target bounds", False)
except ValueError:
    check("regression requires target bounds", True)

reg_geometry = G.regression_geometry((0.0, 1.0))
check("bounded regression derives (g*,h*)=(1,1)",
      G.clip_bounds("reg:squarederror", (0.0, 1.0)) == (1.0, 1.0))
check("unit-range regression sensitivity covers same/different leaf at 2",
      abs(reg_geometry["delta2"] - 2.0) < 1e-12)
narrow_delta = G.regression_replace_one_sensitivity((-0.1, 0.1))
check("narrow gradients still pay Hessian routing sensitivity",
      abs(narrow_delta - math.sqrt(2.02)) < 1e-12 and narrow_delta > 0.2)
asym_geometry = G.regression_geometry(
    (0.0, 2.0), margin_bounds=(1.0, 3.0))
check("asymmetric gradient support uses the larger cross-leaf geometry",
      asym_geometry["gradient_bounds"] == (-1.0, 3.0)
      and abs(asym_geometry["delta2"] - math.sqrt(20.0)) < 1e-12)

# Exhaust the interval endpoints and both routing cases. This independently
# checks the closed form used to calibrate the Gaussian mechanism.
observed = 0.0
for g0 in asym_geometry["gradient_bounds"]:
    for g1 in asym_geometry["gradient_bounds"]:
        observed = max(observed, abs(g0 - g1))
        observed = max(observed, math.sqrt(g0 * g0 + g1 * g1 + 2.0))
check("regression sensitivity equals exhaustive endpoint geometry",
      abs(observed - asym_geometry["delta2"]) < 1e-12)
check("explicit gradient clip tightens the public gradient support",
      G.regression_geometry((0.0, 10.0), gradient_clip=0.5)["gradient_bounds"]
      == (-0.5, 0.5))
check("oversized gradient clip normalizes to the public raw-gradient bound",
      G.regression_geometry((0.0, 1.0), gradient_clip=1e100)["gradient_clip"]
      == 1.0)
check("unknown objective remains fail-closed", raises_value_error(
      lambda: G.clip_bounds("reg:absoluteerror", (0.0, 1.0))))

print("== accountant: calibrate achieves <= target epsilon, tight ==")
for eps in (0.5, 1.0, 3.0, 8.0):
    for T in (1, 5, 10, 50):
        sigma = G.calibrate_gbdt_sigma(eps, 1e-5, T)
        achieved = G.gbdt_epsilon(sigma, 1e-5, T)
        check(f"eps={eps} T={T}: {achieved:.4f}<= {eps} & tight (sigma={sigma:.3f})",
              achieved <= eps + 1e-6 and achieved >= 0.5 * eps - 1e-9)
check("more releases => more epsilon",
      G.gbdt_epsilon(5., 1e-5, 20) > G.gbdt_epsilon(5., 1e-5, 10) > G.gbdt_epsilon(5., 1e-5, 1))
check("more noise => less epsilon", G.gbdt_epsilon(2., 1e-5, 10) < G.gbdt_epsilon(1., 1e-5, 10))
edge_sigma = G.calibrate_gbdt_sigma(1e-6, 1e-12, G._MAX_TREES)
check("continuous-order RDP supports the numerical policy edge",
      G.gbdt_epsilon(edge_sigma, 1e-12, G._MAX_TREES) <= 1e-6)

print("== calibrate fails closed ==")
for bad in [(-1., 1e-5, 10), (math.nan, 1e-5, 10),
            (math.inf, 1e-5, 10), (1., 0., 10), (1., 1.0, 10),
            (1., math.nan, 10), (1., 1e-5, 0),
            (1., 1e-5, G._MAX_TREES + 1)]:
    try:
        G.calibrate_gbdt_sigma(*bad); check(f"reject {bad}", False)
    except (ValueError, RuntimeError):
        check(f"fail-closed {bad}", True)

print("== random trees: deterministic + data-independent ==")
ranges = [(0., 1.)] * 3
f1, t1 = G.random_tree("tok", 0, 3, ranges, 16)
f2, t2 = G.random_tree("tok", 0, 3, ranges, 16)
check("same (token,t) => identical", np.array_equal(f1, f2) and np.allclose(t1, t2))
check("different t => different", not np.array_equal(f1, G.random_tree("tok", 1, 3, ranges, 16)[0]))
check("different token => different", not np.array_equal(f1, G.random_tree("X", 0, 3, ranges, 16)[0]))
check("n_internal == 2^depth - 1", len(f1) == 7)
leaf = G.route_to_leaf(np.random.RandomState(0).rand(200, 3), f1, t1, 3)
check("leaves in [0,2^depth)", leaf.min() >= 0 and leaf.max() < 8)

# Prove the depth cap fires before np.empty: the sentinel would turn a missing
# pre-allocation guard into an AssertionError without risking an OOM in CI.
_real_empty = G.np.empty
_alloc_calls = []
def _forbidden_empty(*args, **kwargs):
    _alloc_calls.append(args)
    raise AssertionError("allocation reached")
G.np.empty = _forbidden_empty
try:
    _prealloc_rejected = raises_value_error(
        lambda: G.random_tree("tok", 0, 62, ranges, 16))
finally:
    G.np.empty = _real_empty
check("oversized depth rejected before tree allocation",
      _prealloc_rejected and not _alloc_calls)
check("oversized n_bins rejected", raises_value_error(
      lambda: G.random_tree("tok", 0, 3, ranges, G._MAX_BINS + 1)))
_real_empty = G.np.empty
_alloc_calls = []
G.np.empty = _forbidden_empty
try:
    _feature_cap_rejected = raises_value_error(lambda: G.random_tree(
        "tok", 0, 3, [(0.0, 1.0)] * (G._MAX_FEATURES + 1), 16))
finally:
    G.np.empty = _real_empty
check("feature-range count cap fires before tree arrays",
      _feature_cap_rejected and not _alloc_calls)
for bad_ranges in ([(math.nan, 1.0)] * 3, [(0.0, math.inf)] * 3,
                   [(1.0, 0.0)] * 3, [(-1e308, 1e308)] * 3):
    check("non-finite/invalid feature ranges rejected", raises_value_error(
          lambda br=bad_ranges: G.random_tree("tok", 0, 3, br, 16)))

# --------------------------------------------------------------------------- #
print("== single-node fit: utility + noise scaling ==")
rs = np.random.RandomState(42)
n = 1000
X = rs.rand(n, 4)
y = ((X[:, 0] + X[:, 1]) > 1.0).astype(float)
fr = [(0., 1.)] * 4

class _BombRng(G.SecureNumpyRng):
    def __init__(self):
        pass

    def normal(self, *args, **kwargs):
        raise AssertionError("DP mechanism was reached")


def _guarded_fit(X_arg=X, y_arg=y, **overrides):
    args = dict(objective="binary:logistic", depth=3, n_trees=2,
                learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr,
                n_bins=16, run_token="guard", epsilon=2.0, delta=1e-5,
                noise_rng=_BombRng())
    args.update(overrides)
    return G.fit_dp_gbdt(X_arg, y_arg, **args)


def _guarded_reg_fit(X_arg=X, y_arg=y, **overrides):
    args = dict(objective="reg:squarederror", depth=3, n_trees=2,
                learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr,
                n_bins=16, run_token="reg-guard", epsilon=2.0, delta=1e-5,
                target_bounds=(0.0, 1.0), margin_bounds=(0.0, 1.0),
                gradient_clip=0.5, noise_rng=_BombRng())
    args.update(overrides)
    return G.fit_dp_gbdt(X_arg, y_arg, **args)


print("== finite-data + bounded-spec gates precede the DP mechanism ==")
for label, X_bad, y_bad in (
        ("NaN X", np.where(np.arange(X.size).reshape(X.shape) == 0, math.nan, X), y),
        ("Inf X", np.where(np.arange(X.size).reshape(X.shape) == 0, math.inf, X), y),
        ("NaN y", X, np.where(np.arange(y.size) == 0, math.nan, y)),
        ("Inf y", X, np.where(np.arange(y.size) == 0, math.inf, y))):
    check(label + " rejected before noise draw",
          raises_value_error(lambda xb=X_bad, yb=y_bad: _guarded_fit(xb, yb)))
check("NaN is rejected before patient pooling/noise", raises_value_error(
      lambda: _guarded_fit(np.where(np.arange(X.size).reshape(X.shape) == 0,
                                    math.nan, X), y,
                           patient_ids=np.repeat(np.arange(125), 8))))
for field, value in (("depth", G._MAX_DEPTH + 1),
                     ("n_trees", G._MAX_TREES + 1),
                     ("n_bins", G._MAX_BINS + 1)):
    check("oversized %s rejected before noise draw" % field,
          raises_value_error(lambda f=field, v=value: _guarded_fit(**{f: v})))
for field, value in (("learning_rate", math.nan), ("learning_rate", math.inf),
                     ("reg_lambda", 0.0), ("reg_lambda", math.inf),
                     ("epsilon", math.inf), ("delta", math.nan),
                     ("base_score", math.nan)):
    check("non-finite/invalid %s rejected before noise draw" % field,
          raises_value_error(lambda f=field, v=value: _guarded_fit(**{f: v})))
check("feature-range count mismatch rejected before noise draw", raises_value_error(
      lambda: _guarded_fit(feature_ranges=fr[:-1])))
check("non-finite feature range rejected before noise draw", raises_value_error(
      lambda: _guarded_fit(feature_ranges=[(0.0, math.inf)] * 4)))
for bad_bounds in (None, (0.0, 0.0), (1.0, 0.0), (math.nan, 1.0),
                   (0.0, math.inf), (-G._MAX_PUBLIC_BOUND_ABS - 1.0, 1.0),
                   "0,1"):
    check("invalid regression target bounds rejected before noise draw",
          raises_value_error(
              lambda bounds=bad_bounds: _guarded_reg_fit(
                  target_bounds=bounds)))
for bad_bounds in ((0.0, 0.0), (1.0, 0.0), (math.nan, 1.0),
                   (0.0, math.inf), (0.0, G._MAX_PUBLIC_BOUND_ABS + 1.0)):
    check("invalid regression margin bounds rejected before noise draw",
          raises_value_error(
              lambda bounds=bad_bounds: _guarded_reg_fit(
                  margin_bounds=bounds)))
for bad_clip in (0.0, -1.0, math.nan, math.inf, True):
    check("invalid regression gradient clip rejected before noise draw",
          raises_value_error(
              lambda clip=bad_clip: _guarded_reg_fit(gradient_clip=clip)))
check("fit refuses a missing secure RNG",
      rejects(lambda: _guarded_fit(noise_rng=None)))
check("fit refuses an explicitly injected statistical PRNG",
      rejects(lambda: _guarded_fit(noise_rng=np.random.default_rng(7))))

probe_feat, probe_thr = G.random_tree("rng-guard", 0, 2, fr, 16)
check("histogram primitive refuses a missing secure RNG", rejects(
      lambda: G.node_noised_histogram(
          X, y, np.zeros(n), probe_feat, probe_thr, 2,
          sigma=1.0, delta2=1.0, g_star=1.0, h_star=0.25,
          n_leaves=4, rng=None)))
check("histogram primitive refuses a statistical PRNG", rejects(
      lambda: G.node_noised_histogram(
          X, y, np.zeros(n), probe_feat, probe_thr, 2,
          sigma=1.0, delta2=1.0, g_star=1.0, h_star=0.25,
          n_leaves=4, rng=np.random.default_rng(8))))

reg_probe = G.regression_geometry(
    (0.0, 1.0), margin_bounds=(0.0, 1.0), gradient_clip=0.2)
check("regression histogram refuses an understated sensitivity before noise",
      raises_value_error(lambda: G.node_noised_histogram(
          X, y, np.zeros(n), probe_feat, probe_thr, 2,
          sigma=1.0, delta2=reg_probe["delta2"] - 1e-6,
          g_star=reg_probe["g_star"], h_star=reg_probe["h_star"],
          n_leaves=4, rng=_BombRng(), objective="reg:squarederror",
          target_bounds=(0.0, 1.0), margin_bounds=(0.0, 1.0),
          gradient_clip=0.2)))

class _ZeroRng(G.SecureNumpyRng):
    def __init__(self):
        pass

    def normal(self, loc, scale, size):
        return np.zeros(size, dtype=np.float64)


probe_X = np.array([[0.0], [1.0], [0.25], [0.75]])
probe_y = np.array([-5.0, 5.0, 0.75, 0.25])
probe_margin = np.array([-10.0, 10.0, 10.0, -10.0])
probe_release = G.node_noised_histogram(
    probe_X, probe_y, probe_margin, np.array([0]), np.array([0.5]), 1,
    sigma=1.0, delta2=reg_probe["delta2"],
    g_star=reg_probe["g_star"], h_star=reg_probe["h_star"],
    n_leaves=2, rng=_ZeroRng(), objective="reg:squarederror",
    target_bounds=(0.0, 1.0), margin_bounds=(0.0, 1.0),
    gradient_clip=0.2)
check("regression clips targets, margins and gradients before histogramming",
      np.allclose(probe_release, np.array([0.2, -0.2, 2.0, 2.0])))

b_hi = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=3, n_trees=30,
                     learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=32,
                     run_token="hi", epsilon=8.0, delta=1e-5,
                     noise_rng=_test_secure_rng(1))
auc_hi = auc(y, G.predict_proba(b_hi, X))
check(f"high-eps beats chance (AUC={auc_hi:.3f})", auc_hi > 0.6)
b_lo = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=3, n_trees=30,
                     learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=32,
                     run_token="lo", epsilon=0.1, delta=1e-5,
                     noise_rng=_test_secure_rng(2))
check(f"tiny-eps ~ chance ({auc(y, G.predict_proba(b_lo, X)):.3f} < {auc_hi:.3f})",
      auc(y, G.predict_proba(b_lo, X)) < auc_hi)
check("tiny-eps sigma >> high-eps sigma", b_lo["sigma"] > b_hi["sigma"])
check("returned booster has only finite thresholds/weights",
      all(np.all(np.isfinite(tree["thr"])) and np.all(np.isfinite(tree["w"]))
          for tree in b_hi["trees"]))
check("entire returned booster serializes with allow_nan=False",
      not rejects(lambda: json.dumps(b_hi, allow_nan=False)))

# --------------------------------------------------------------------------- #
print("== bounded squared-error regression ==")
rs_reg = np.random.RandomState(314)
X_reg = rs_reg.rand(2400, 4)
y_reg = np.clip(0.1 + 0.55 * X_reg[:, 0] + 0.25 * X_reg[:, 1]
                + rs_reg.normal(0.0, 0.03, len(X_reg)), 0.0, 1.0)
reg_args = dict(
    objective="reg:squarederror", depth=3, n_trees=50,
    learning_rate=0.2, reg_lambda=2.0, feature_ranges=fr, n_bins=32,
    run_token="reg-utility", epsilon=12.0, delta=1e-5,
    target_bounds=(0.0, 1.0), margin_bounds=(0.0, 1.0),
    gradient_clip=0.5, base_score=0.5)
b_reg = G.fit_dp_gbdt(
    X_reg, y_reg, noise_rng=_test_secure_rng(9001), **reg_args)
p_reg = G.predict_value(b_reg, X_reg)
baseline_mse = float(np.mean((y_reg - y_reg.mean()) ** 2))
reg_mse = float(np.mean((y_reg - p_reg) ** 2))
check("bounded regression beats the constant baseline",
      reg_mse < 0.8 * baseline_mse)
check("regression predictions obey public margin bounds",
      bool(np.all(p_reg >= 0.0) and np.all(p_reg <= 1.0)))
check("regression composes all tree releases within target epsilon",
      G.gbdt_epsilon(b_reg["sigma"], b_reg["delta"],
                     len(b_reg["trees"])) <= b_reg["epsilon"] + 1e-9)
b_reg_retry = G.fit_dp_gbdt(
    X_reg, y_reg, noise_rng=_test_secure_rng(9001), **reg_args)
check("bounded regression is byte-deterministic for the same release seed",
      json.dumps(b_reg, sort_keys=True, separators=(",", ":"))
      == json.dumps(b_reg_retry, sort_keys=True, separators=(",", ":")))

# Public clipping is total: out-of-domain private labels cannot select a
# success/error path and are equivalent to their clipped representation.
y_reg_outside = y_reg.copy()
y_reg_outside[:4] = (-100.0, 100.0, -1.0, 2.0)
clip_args = dict(reg_args)
clip_args.update(n_trees=3, run_token="reg-target-clip")
b_reg_outside = G.fit_dp_gbdt(
    X_reg, y_reg_outside, noise_rng=_test_secure_rng(77), **clip_args)
b_reg_clipped = G.fit_dp_gbdt(
    X_reg, np.clip(y_reg_outside, 0.0, 1.0),
    noise_rng=_test_secure_rng(77), **clip_args)
check("out-of-bound regression targets equal their public clipped form",
      json.dumps(b_reg_outside, sort_keys=True)
      == json.dumps(b_reg_clipped, sort_keys=True))
check("bounded regression booster serializes without NaN/Inf",
      not rejects(lambda: json.dumps(b_reg, allow_nan=False)))
default_base_args = dict(reg_args)
default_base_args.update(n_trees=1, run_token="reg-public-base",
                         target_bounds=(10.0, 20.0),
                         margin_bounds=(10.0, 20.0))
default_base_args.pop("base_score")
b_reg_default_base = G.fit_dp_gbdt(
    X_reg, y_reg + 10.0, noise_rng=_test_secure_rng(78),
    **default_base_args)
check("regression default base score is the public margin midpoint",
      b_reg_default_base["base_margin"] == 15.0)

class _InfRng(G.SecureNumpyRng):
    def __init__(self):
        pass

    def normal(self, loc, scale, size):
        return np.full(size, math.inf)

check("non-finite mechanism draw is refused, never serialized in a booster",
      rejects(lambda: G.fit_dp_gbdt(
          X, y, objective="binary:logistic", depth=2, n_trees=1,
          learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=16,
          run_token="bad-noise", epsilon=2.0, delta=1e-5,
          noise_rng=_InfRng())))
check("non-finite aggregated histogram cannot produce leaf weights",
      raises_value_error(lambda: G.grow_tree_from_histograms(
          np.array([0.0] * 7 + [math.inf]), 4, 1.0, 0.3)))
check("finite extreme leaf weights are saturated to the public model cap",
      np.array_equal(
          G.grow_tree_from_histograms(
              np.array([-1.0e12, 1.0e12, 0.0, 0.0,
                        0.0, 0.0, 0.0, 0.0]),
              4, 1.0e-6, 1.0),
          np.array([1.0e6, -1.0e6, 0.0, 0.0])))
check("finite-but-overflowing leaf arithmetic is refused",
      rejects(lambda: G.grow_tree_from_histograms(
          np.array([0.0] * 4 + [np.finfo(np.float64).max] * 4),
          4, np.finfo(np.float64).max, 0.3)))

# --------------------------------------------------------------------------- #
print("== FEDERATED SIMULATION: 3 disjoint shards, synchronized histogram-sum ==")
# Mirrors the deployed flow: each ROUND, every shard computes F from the global
# booster-so-far and releases its NODE-SIDE-noised leaf histogram; the (untrusted)
# server SUMS the already-noised histograms (post-processing) and grows one tree.
# Each shard releases T_total histograms -> sigma calibrated for T_total per shard.
shards = np.array_split(rs.permutation(n), 3)
depth, T_total, lam, eta, n_bins = 3, 30, 1.0, 0.3, 32
eps, delta = 6.0, 1e-5
g_star, h_star = G.clip_bounds("binary:logistic")
delta2 = G.replace_one_sensitivity(g_star, h_star)
sigma = G.calibrate_gbdt_sigma(eps, delta, T_total)
L = 1 << depth
booster = {"objective": "binary:logistic", "depth": depth, "n_bins": n_bins,
           "base_margin": G._logit(float(y.mean())), "learning_rate": eta,
           "feature_ranges": [[0., 1.]] * 4, "sigma": sigma, "delta2": delta2,
           "epsilon": eps, "delta": delta, "trees": []}
shard_rngs = [_test_secure_rng(100 + i) for i in range(len(shards))]
for t in range(T_total):
    feat, thr = G.random_tree("fed-tok", t, depth, fr, n_bins)   # identical on all shards
    summed = np.zeros(2 * L)
    for idx, shard_rng in zip(shards, shard_rngs):
        Fs = G.predict_margin(booster, X[idx])                   # from global booster so far
        summed += G.node_noised_histogram(X[idx], y[idx], Fs, feat, thr, depth,
                                           sigma=sigma, delta2=delta2,
                                           g_star=g_star, h_star=h_star, n_leaves=L,
                                           rng=shard_rng)
    w = G.grow_tree_from_histograms(summed, L, lam, eta)         # untrusted server: post-processing
    booster["trees"].append({"feat": feat.tolist(), "thr": thr.tolist(), "w": w.tolist()})
auc_fed = auc(y, G.predict_proba(booster, X))
check(f"federated (3 shards, eps={eps}) beats chance (AUC={auc_fed:.3f} > 0.6)", auc_fed > 0.6)
check("federated AUC within reason of single-node", abs(auc_fed - auc_hi) < 0.25)
check("federated booster has T_total trees", len(booster["trees"]) == T_total)
# privacy: each shard released exactly T_total noised histograms
check("per-shard release count == T_total (sigma calibrated for it)",
      G.gbdt_epsilon(sigma, delta, T_total) <= eps + 1e-6)

print("== per-patient pooling ==")
pids = np.repeat(np.arange(125), 8)
Xp, yp = G.pool_by_patient(X, y, pids)
check("pooled to 125 patient-rows", Xp.shape == (125, 4))
Xp_reg, yp_reg = G.pool_by_patient(
    np.array([[0.0], [1.0], [2.0], [3.0]]),
    np.array([0.1, 0.3, 0.8, 1.2]), np.array(["a", "a", "b", "b"]),
    objective="reg:squarederror", target_bounds=(0.0, 1.0))
check("regression pools one bounded mean outcome per patient",
      np.allclose(Xp_reg.ravel(), (0.5, 2.5))
      and np.allclose(yp_reg, (0.2, 0.9)))
bp = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=2, n_trees=10,
                   learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=16,
                   run_token="pat", epsilon=5.0, delta=1e-5, patient_ids=pids,
                   noise_rng=_test_secure_rng(3))
check("per-patient fit non-degenerate", len(bp["trees"]) == 10)

print(f"\n=== {ok} passed, {fail} failed ===")
sys.exit(1 if fail else 0)
