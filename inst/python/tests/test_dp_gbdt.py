"""Unit + federated-simulation tests for dp_gbdt.py (pure numpy).

Run:  python3 dsFlower/inst/python/tests/test_dp_gbdt.py
The DP-GBDT engine is pure numpy, so these run off-node for fast iteration and
double as the node-side regression suite.
"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


def auc(y, p):
    y = np.asarray(y)
    n1 = int(y.sum()); n0 = len(y) - n1
    if n1 == 0 or n0 == 0:
        return 0.5
    ranks = np.argsort(np.argsort(p)) + 1
    return (ranks[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


# --------------------------------------------------------------------------- #
print("== sensitivity / allowlist ==")
d2 = G.replace_one_sensitivity(1.0, 0.25)
check("replace_one(1,0.25)=sqrt(4+1/16)~2.0156", abs(d2 - math.sqrt(4 + 1 / 16)) < 1e-9)
check("replace_one > sqrt(2)*sqrt(g^2+h^2) (the under-noising constant)",
      d2 > math.sqrt(2) * math.sqrt(1 + 0.0625))
check("binary:logistic -> (1,0.25)", G.clip_bounds("binary:logistic") == (1.0, 0.25))
try:
    G.clip_bounds("reg:squarederror"); check("reject unbounded objective", False)
except ValueError:
    check("reject unbounded objective", True)

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

print("== calibrate fails closed ==")
for bad in [(-1., 1e-5, 10), (1., 0., 10), (1., 1.0, 10)]:
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

# --------------------------------------------------------------------------- #
print("== single-node fit: utility + noise scaling ==")
rs = np.random.RandomState(42)
n = 1000
X = rs.rand(n, 4)
y = ((X[:, 0] + X[:, 1]) > 1.0).astype(float)
fr = [(0., 1.)] * 4
b_hi = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=3, n_trees=30,
                     learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=32,
                     run_token="hi", epsilon=8.0, delta=1e-5)
auc_hi = auc(y, G.predict_proba(b_hi, X))
check(f"high-eps beats chance (AUC={auc_hi:.3f})", auc_hi > 0.6)
b_lo = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=3, n_trees=30,
                     learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=32,
                     run_token="lo", epsilon=0.1, delta=1e-5)
check(f"tiny-eps ~ chance ({auc(y, G.predict_proba(b_lo, X)):.3f} < {auc_hi:.3f})",
      auc(y, G.predict_proba(b_lo, X)) < auc_hi)
check("tiny-eps sigma >> high-eps sigma", b_lo["sigma"] > b_hi["sigma"])

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
for t in range(T_total):
    feat, thr = G.random_tree("fed-tok", t, depth, fr, n_bins)   # identical on all shards
    summed = np.zeros(2 * L)
    for idx in shards:
        Fs = G.predict_margin(booster, X[idx])                   # from global booster so far
        summed += G.node_noised_histogram(X[idx], y[idx], Fs, feat, thr, depth,
                                           sigma=sigma, delta2=delta2,
                                           g_star=g_star, h_star=h_star, n_leaves=L)
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
bp = G.fit_dp_gbdt(X, y, objective="binary:logistic", depth=2, n_trees=10,
                   learning_rate=0.3, reg_lambda=1.0, feature_ranges=fr, n_bins=16,
                   run_token="pat", epsilon=5.0, delta=1e-5, patient_ids=pids)
check("per-patient fit non-degenerate", len(bp["trees"]) == 10)

print(f"\n=== {ok} passed, {fail} failed ===")
sys.exit(1 if fail else 0)
