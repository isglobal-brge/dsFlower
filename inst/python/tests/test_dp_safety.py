"""DP safety / no-leak regression suite for the trusted runner (dp_harness, tier2_lib).

Run:  python3 dsFlower/inst/python/tests/test_dp_safety.py
Imports the REAL runner modules (the byte-identical code the nodes execute), so
this doubles as the node-side regression suite. Off-node for fast iteration.

TRUST MODEL (the fail-closed adaptive DP gateway)
-------------------------------------------------
For ANY submission the node grants the tightest DP it can GUARANTEE BY
CONSTRUCTION, never by trusting/validating the submitted code:

  * DP-SGD (per-sample clip + Gaussian noise)  -- granted ONLY for models the
    node BUILDS from the coupling-free op/loss allowlist (Tier-1 spec). Sound by
    construction: the node owns the architecture, loss and optimizer; a sample's
    gradient cannot depend on its batch peers. Defense in depth: ModuleValidator
    + per_sample_independence_probe + assert_releasable.
  * Output-perturbation (clip the whole update + Gaussian noise)  -- the UNIVERSAL
    floor for everything else: arbitrary uploaded code (Tier-2), couplng losses,
    non-allowlist ops. Valid for ANY function; the app never controls the release.

Why NOT tight DP for arbitrary code: research (Opacus docs; "Finding Private
Bugs", ICLR'23) shows static validation is a NON-EXHAUSTIVE denylist (a custom
`x - x.mean(0)` passes validation yet breaks per-sample independence) and DP-SGD
bugs do NOT fail loudly. So untrusted code can only get the sound floor; tight DP
is grown by ENRICHING the vetted allowlist, not by trusting submissions. These
tests assert that every leak vector is closed, fail-closed.
"""
import math
import os
import sys

RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import numpy as np
import torch
import torch.nn as nn
import dp_harness as dh
import model_spec
import tier2_lib

torch.manual_seed(0)
np.random.seed(0)
ok = fail = 0


def check(name, cond):
    global ok, fail
    if cond:
        ok += 1; print(f"  PASS  {name}")
    else:
        fail += 1; print(f"  FAIL  {name}")


def rejects(fn):
    """True iff fn() raises (the fail-closed behaviour we want)."""
    try:
        fn(); return False
    except Exception:
        return True


# --------------------------------------------------------------------------- #
print("== output-perturbation floor: analytic Gaussian + sensitivity bound ==")
C, eps, delta = 1.0, 1.0, 1e-5
sigma = dh.compute_output_sigma(eps, delta, C)
def _analytic_delta(s, ep, sens):
    a, b = sens / (2 * s), ep * s / sens
    return (0.5 * (1 + math.erf((a - b) / math.sqrt(2)))
            - math.exp(ep) * 0.5 * (1 + math.erf((-a - b) / math.sqrt(2))))
check("compute_output_sigma returns the analytic-Gaussian sigma meeting the target delta",
      _analytic_delta(sigma, eps, C) <= delta * 1.001)
check("compute_output_sigma rejects eps<=0", rejects(lambda: dh.compute_output_sigma(0, delta, C)))

old = [np.zeros((4, 3), np.float32), np.zeros(3, np.float32)]
huge = [np.full((4, 3), 1e6, np.float32), np.full(3, 1e6, np.float32)]   # "raw data" delta
clipped = dh.clip_update(huge, old, C)
cd = np.concatenate([(c - o).ravel() for c, o in zip(clipped, old)])
check("clip_update bounds ||delta|| <= C", np.linalg.norm(cd) <= C + 1e-5)

out = dh.output_perturbation(huge, old, clipping_norm=C, epsilon=eps, delta=delta)
rel = np.concatenate([(o2 - o1).ravel() for o2, o1 in zip(out, old)])
check("output-perturbation DESTROYS raw-data exfil (1e6 -> O(sigma))",
      np.max(np.abs(rel)) < 50)

# The floor's L2 sensitivity is the C-ball DIAMETER = 2C (arbitrary code), so the noise
# std must be 2*sigma(C), NOT sigma(C). Test at C=1 AND C=4: C=4 also catches the
# double-C regression (a prior version multiplied the clip in twice -> std ~ C^2).
for Cx in (1.0, 4.0):
    s1 = dh.compute_output_sigma(eps, delta, Cx)                  # k*Cx/eps (sensitivity Cx)
    z = [np.zeros(300000, np.float32)]
    emp = float(np.std(dh.output_perturbation(z, z, clipping_norm=Cx, epsilon=eps, delta=delta)[0]))
    check("floor noise std == 2*sigma(C) for C=%g (2C sensitivity, no double-C)" % Cx,
          abs(emp - 2.0 * s1) < 0.05 * (2.0 * s1))

# --------------------------------------------------------------------------- #
print("== loss allowlist: only stock per-sample-decomposable losses ==")
for nm in ("bce_logits", "cross_entropy", "mse", "poisson_nll", "multilabel_bce"):
    check(f"loss_from_allowlist('{nm}') ok", dh.loss_from_allowlist(nm) is not None)
check("loss_from_allowlist rejects unknown 'cox_partial'",
      rejects(lambda: dh.loss_from_allowlist("cox_partial")))

# --------------------------------------------------------------------------- #
print("== per-sample independence probe: catches what static validation misses ==")
class Clean(nn.Module):
    def __init__(s): super().__init__(); s.lin = nn.Linear(3, 2)
    def forward(s, x): return s.lin(x)

class MeanCouple(nn.Module):           # x - x.mean(0): passes Opacus denylist, breaks DP
    def __init__(s): super().__init__(); s.lin = nn.Linear(3, 2)
    def forward(s, x): return s.lin(x - x.mean(0, keepdim=True))

class AttnCouple(nn.Module):           # softmax(x @ x.T): couples the whole batch
    def __init__(s): super().__init__(); s.lin = nn.Linear(3, 2)
    def forward(s, x): return s.lin(torch.softmax(x @ x.t(), 1) @ x)

x = torch.randn(8, 3); y = torch.randint(0, 2, (8,))
ce = nn.CrossEntropyLoss()
check("clean model passes the probe",
      not rejects(lambda: dh.per_sample_independence_probe(Clean(), ce, x, y)))
check("x - x.mean(0) coupling REJECTED",
      rejects(lambda: dh.per_sample_independence_probe(MeanCouple(), ce, x, y)))
check("batch-attention coupling REJECTED",
      rejects(lambda: dh.per_sample_independence_probe(AttnCouple(), ce, x, y)))

# --------------------------------------------------------------------------- #
print("== assert_releasable: no un-noised stash channels ==")
class Buf(nn.Module):
    def __init__(s):
        super().__init__(); s.lin = nn.Linear(3, 1)
        s.register_buffer("stash", torch.zeros(64))
class Frozen(nn.Module):
    def __init__(s):
        super().__init__(); s.lin = nn.Linear(3, 1)
        s.stash = nn.Parameter(torch.zeros(64), requires_grad=False)
check("clean model is releasable", not rejects(lambda: dh.assert_releasable(Clean())))
check("buffer stash channel REJECTED", rejects(lambda: dh.assert_releasable(Buf())))
check("frozen-param stash channel REJECTED", rejects(lambda: dh.assert_releasable(Frozen())))

# --------------------------------------------------------------------------- #
print("== Tier-2 gate (gated_local_update): adversarial apps cannot leak ==")
pcfg = {"clipping_norm": 1.0, "epsilon": 1.0, "delta": 1e-5}
g = [np.zeros(20, np.float32)]
Xraw = (np.random.randn(8) * 7).astype(np.float32)

class Exfil:                            # smuggle raw data into the returned weights
    def initial_arrays(s, cfg, d): return [np.zeros(d, np.float32)]
    def local_update(s, gg, X, yy, cfg):
        w = [a.copy() for a in gg]
        n = min(X.size, w[0].size)
        w[0].ravel()[:n] = X.ravel()[:n] * 1e6
        return w
class WrongShape:
    def initial_arrays(s, cfg, d): return [np.zeros(d, np.float32)]
    def local_update(s, gg, X, yy, cfg): return [np.zeros(21, np.float32)]

ex = tier2_lib.gated_local_update(Exfil(), g, Xraw, np.zeros(8), {}, pcfg)
exrel = np.concatenate([(o - gg).ravel() for o, gg in zip(ex, g)])
check("Tier-2 exfil via weights DESTROYED by the gate (1e6 raw -> O(sigma))",
      np.max(np.abs(exrel)) < 100)
_ws = tier2_lib.gated_local_update(WrongShape(), g, Xraw, np.zeros(8), {}, pcfg)
check("Tier-2 shape-mismatch smuggling NEUTRALIZED (validate-or-zero -> global shape, no raw escape)",
      len(_ws) == len(g) and all(a.shape == o.shape for a, o in zip(_ws, g))
      and all(np.all(np.isfinite(a)) for a in _ws))

# --------------------------------------------------------------------------- #
print("== custom loss factory: negative-binomial NLL (per-sample, DP-SGD-safe) ==")
nb = dh.loss_from_allowlist("negbin_nll", {"nb-dispersion": 2.0})
check("loss_from_allowlist('negbin_nll', cfg) returns a callable", callable(nb))
# Numerics, INDEPENDENT cross-check: NB2 -> exact Poisson NLL as dispersion -> inf.
# float64 so the check probes the FORMULA's limit, not float32 cancellation at huge r.
zc = torch.tensor([0.2, 0.8, 1.5]).reshape(-1, 1).double()
yc = torch.tensor([0.0, 2.0, 4.0]).reshape(-1, 1).double()
pois_exact = float((zc.exp() - yc * zc + torch.lgamma(yc + 1.0)).mean())
nb_big = dh.loss_from_allowlist("negbin_nll", {"nb-dispersion": 1e7})
check("negbin_nll -> exact Poisson NLL as dispersion -> inf",
      abs(float(nb_big(zc, yc)) - pois_exact) < 1e-2)
check("negbin_nll rejects dispersion <= 0 (fail closed)",
      rejects(lambda: dh.loss_from_allowlist("negbin_nll", {"nb-dispersion": 0.0})))
check("negbin_nll rejects non-finite dispersion (fail closed)",
      rejects(lambda: dh.loss_from_allowlist("negbin_nll", {"nb-dispersion": float("inf")})))
class CountHead(nn.Module):
    def __init__(s): super().__init__(); s.lin = nn.Linear(3, 1)
    def forward(s, x): return s.lin(x)
xct = torch.randn(8, 3); yct = torch.randint(0, 6, (8, 1)).float()
check("negbin_nll model passes the per-sample independence probe",
      not rejects(lambda: dh.per_sample_independence_probe(CountHead(), nb, xct, yct)))

# --------------------------------------------------------------------------- #
print("== custom loss factory: gamma NLL (per-sample, DP-SGD-safe) ==")
gm = dh.loss_from_allowlist("gamma_nll", {"gamma-shape": 1.0})
check("loss_from_allowlist('gamma_nll', cfg) returns a callable", callable(gm))
# Numerics, INDEPENDENT: gamma(shape=1) == exponential NLL z + y*exp(-z).
zg = torch.tensor([0.3, 1.1]).reshape(-1, 1).double()
yg = torch.tensor([0.5, 2.0]).reshape(-1, 1).double()
expo = float((zg + yg * torch.exp(-zg)).mean())
check("gamma_nll(shape=1) == exponential NLL", abs(float(gm(zg, yg)) - expo) < 1e-9)
check("gamma_nll rejects shape <= 0 (fail closed)",
      rejects(lambda: dh.loss_from_allowlist("gamma_nll", {"gamma-shape": -1.0})))
class PosHead(nn.Module):
    def __init__(s): super().__init__(); s.lin = nn.Linear(3, 1)
    def forward(s, x): return s.lin(x)
check("gamma_nll model passes the per-sample independence probe",
      not rejects(lambda: dh.per_sample_independence_probe(
          PosHead(), gm, torch.randn(8, 3), torch.rand(8, 1) + 0.1)))

# --------------------------------------------------------------------------- #
print("== ordinal (CORN): node-decided K-1 width + stock per-sample BCE ==")
check("ordinal output_width = K-1", model_spec.output_width("ordinal", {"num-classes": 4}) == 3)
check("ordinal degenerate (K=2) width = 1", model_spec.output_width("ordinal", {"num-classes": 2}) == 1)
check("ordinal loss is stock BCEWithLogitsLoss",
      type(dh.loss_from_allowlist("ordinal")).__name__ == "BCEWithLogitsLoss")

# --------------------------------------------------------------------------- #
print("== conv ops: node-shaped CNN, per-sample, stock + Opacus grad_sample ==")
import copy as _copy
_cnn_spec = {"kind": "sequential", "layers": [
    {"op": "reshape", "shape": [1, 8, 8]},
    {"op": "conv2d", "out_channels": 8, "kernel_size": 3, "padding": 1}, {"op": "relu"},
    {"op": "maxpool2d", "kernel_size": 2},
    {"op": "adaptiveavgpool2d", "output_size": [1, 1]}, {"op": "flatten"},
    {"op": "linear", "out": "@out"}]}
_cnn = model_spec.build_from_spec(_cnn_spec, 64, 3)
check("CNN spec builds to a stock module with output width == out_dim",
      tuple(_cnn(torch.randn(4, 64)).shape) == (4, 3))
check("CNN has no buffers (assert_releasable holds)",
      not rejects(lambda: dh.assert_releasable(_cnn)))
check("CNN passes the per-sample independence probe",
      not rejects(lambda: dh.per_sample_independence_probe(
          _copy.deepcopy(_cnn), nn.CrossEntropyLoss(),
          torch.randn(8, 64), torch.randint(0, 3, (8,)))))
check("conv2d on a flat (un-reshaped) input REJECTED", rejects(
    lambda: model_spec.build_from_spec({"kind": "sequential", "layers": [
        {"op": "conv2d", "out_channels": 8}, {"op": "flatten"},
        {"op": "linear", "out": "@out"}]}, 64, 2)))
check("reshape that changes element count REJECTED", rejects(
    lambda: model_spec.build_from_spec({"kind": "sequential", "layers": [
        {"op": "reshape", "shape": [1, 8, 8]}, {"op": "flatten"},
        {"op": "linear", "out": "@out"}]}, 30, 1)))
check("conv out_channels over cap REJECTED", rejects(
    lambda: model_spec.build_from_spec({"kind": "sequential", "layers": [
        {"op": "reshape", "shape": [1, 8, 8]}, {"op": "conv2d", "out_channels": 99999},
        {"op": "flatten"}, {"op": "linear", "out": "@out"}]}, 64, 2)))

# --------------------------------------------------------------------------- #
print("== adaptive routing: the SERVER picks the DP mechanism, unforgeably ==")
check("declarative spec -> neural (DP-SGD, tight)", dh.resolve_dp_track({}, "neural") == "neural")
check("gbdt spec -> trees (DP-GBDT)", dh.resolve_dp_track({}, "trees") == "trees")
check("explicit egress -> egress (output-perturbation floor)", dh.resolve_dp_track({}, "egress") == "egress")
check("uploaded code requesting NEURAL -> FORCED to the floor (cannot be fooled)",
      dh.resolve_dp_track({"user-module": "evil"}, "neural") == "egress")
check("uploaded code requesting TREES -> FORCED to the floor",
      dh.resolve_dp_track({"user-module": "evil"}, "trees") == "egress")
check("unrecognized track -> fail-closed to the floor",
      dh.resolve_dp_track({}, "weird") == "egress")

# --------------------------------------------------------------------------- #
print("== typed graph (DAG): residual/skip/concat, per-sample, gate-admitted ==")
import copy as _cp2
_resnet = {"kind": "graph", "output": "out", "nodes": [
    {"name": "img", "op": "reshape", "in": ["@in"], "shape": [1, 8, 8]},
    {"name": "c1", "op": "conv2d", "in": ["img"], "out_channels": 4, "kernel_size": 3, "padding": 1},
    {"name": "r1", "op": "relu", "in": ["c1"]},
    {"name": "c2", "op": "conv2d", "in": ["r1"], "out_channels": 4, "kernel_size": 3, "padding": 1},
    {"name": "res", "op": "add", "in": ["c1", "c2"]},          # residual skip
    {"name": "pool", "op": "adaptiveavgpool2d", "in": ["res"], "output_size": [1, 1]},
    {"name": "flat", "op": "flatten", "in": ["pool"]},
    {"name": "out", "op": "linear", "in": ["flat"], "out": "@out"}]}
check("DAG ResNet block (residual conv) builds, output width == out_dim",
      tuple(model_spec.build_from_spec(_resnet, 64, 3)(torch.randn(4, 64)).shape) == (4, 3))
check("DAG GraphModule admitted by assert_stock_architecture",
      not rejects(lambda: dh.assert_stock_architecture(model_spec.build_from_spec(_resnet, 64, 3))))
check("DAG ResNet block has no buffers (releasable)",
      not rejects(lambda: dh.assert_releasable(model_spec.build_from_spec(_resnet, 64, 3))))
check("DAG ResNet block passes the per-sample independence probe",
      not rejects(lambda: dh.per_sample_independence_probe(
          _cp2.deepcopy(model_spec.build_from_spec(_resnet, 64, 3)), nn.CrossEntropyLoss(),
          torch.randn(8, 64), torch.randint(0, 3, (8,)))))
check("DAG concat (2 branches) builds with summed feature width",
      tuple(model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
          {"name": "b1", "op": "linear", "in": ["@in"], "out": 8},
          {"name": "b2", "op": "linear", "in": ["@in"], "out": 8},
          {"name": "cat", "op": "concat", "in": ["b1", "b2"], "axis": 0},
          {"name": "out", "op": "linear", "in": ["cat"], "out": "@out"}]}, 16, 2)(
          torch.randn(4, 16)).shape) == (4, 2))
check("DAG add with mismatched per-sample shapes REJECTED", rejects(
    lambda: model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
        {"name": "h1", "op": "linear", "in": ["@in"], "out": 8},
        {"name": "h2", "op": "linear", "in": ["@in"], "out": 16},
        {"name": "bad", "op": "add", "in": ["h1", "h2"]},
        {"name": "out", "op": "linear", "in": ["bad"], "out": "@out"}]}, 16, 2)))
check("DAG forward-referenced input (non-topological) REJECTED", rejects(
    lambda: model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
        {"name": "a", "op": "relu", "in": ["b"]},
        {"name": "b", "op": "linear", "in": ["@in"], "out": 4},
        {"name": "out", "op": "linear", "in": ["a"], "out": "@out"}]}, 8, 2)))

# --------------------------------------------------------------------------- #
print("== advanced graph ops: attention / broadcast / upsample (per-sample) ==")
_tx = {"kind": "graph", "output": "out", "nodes": [
    {"name": "x", "op": "reshape", "in": ["@in"], "shape": [8, 8]},
    {"name": "q", "op": "linear", "in": ["x"], "out": 8},
    {"name": "k", "op": "linear", "in": ["x"], "out": 8},
    {"name": "v", "op": "linear", "in": ["x"], "out": 8},
    {"name": "kt", "op": "transpose", "in": ["k"], "dims": [0, 1]},
    {"name": "sc", "op": "matmul", "in": ["q", "kt"]},
    {"name": "a", "op": "softmax", "in": ["sc"], "axis": 1},
    {"name": "ctx", "op": "matmul", "in": ["a", "v"]},
    {"name": "res", "op": "add", "in": ["x", "ctx"]},
    {"name": "n", "op": "layernorm", "in": ["res"]},
    {"name": "flat", "op": "flatten", "in": ["n"]},
    {"name": "out", "op": "linear", "in": ["flat"], "out": "@out"}]}
check("transformer attention block (matmul/softmax/transpose) builds, admitted, width ok",
      (not rejects(lambda: dh.assert_stock_architecture(model_spec.build_from_spec(_tx, 64, 3))))
      and tuple(model_spec.build_from_spec(_tx, 64, 3)(torch.randn(4, 64)).shape) == (4, 3))
check("attention block per-sample-safe (attention over TOKENS, not the batch)",
      not rejects(lambda: dh.per_sample_independence_probe(
          _cp2.deepcopy(model_spec.build_from_spec(_tx, 64, 3)), nn.CrossEntropyLoss(),
          torch.randn(8, 64), torch.randint(0, 3, (8,)))))
check("broadcast mul [4,1,1] x [4,4,4] (squeeze-excitation) builds", tuple(
    model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
        {"name": "i", "op": "reshape", "in": ["@in"], "shape": [4, 4, 4]},
        {"name": "sq", "op": "adaptiveavgpool2d", "in": ["i"], "output_size": [1, 1]},
        {"name": "sc", "op": "mul", "in": ["i", "sq"]},
        {"name": "f", "op": "flatten", "in": ["sc"]},
        {"name": "out", "op": "linear", "in": ["f"], "out": "@out"}]}, 64, 2)(
        torch.randn(4, 64)).shape) == (4, 2))
check("upsample (U-Net decoder path) builds", tuple(
    model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
        {"name": "i", "op": "reshape", "in": ["@in"], "shape": [1, 4, 4]},
        {"name": "u", "op": "upsample", "in": ["i"], "scale_factor": 2},
        {"name": "f", "op": "flatten", "in": ["u"]},
        {"name": "out", "op": "linear", "in": ["f"], "out": "@out"}]}, 16, 2)(
        torch.randn(4, 16)).shape) == (4, 2))

# --------------------------------------------------------------------------- #
print("== recurrent (LSTM/GRU) via sanitized Opacus DP-RNN -- gate stays strict ==")
_lstm = {"kind": "graph", "output": "out", "nodes": [
    {"name": "x", "op": "reshape", "in": ["@in"], "shape": [8, 8]},
    {"name": "h", "op": "lstm", "in": ["x"], "hidden": 16},
    {"name": "out", "op": "linear", "in": ["h"], "out": "@out"}]}
check("LSTM sequence DAG builds, output width == out_dim",
      tuple(model_spec.build_from_spec(_lstm, 64, 3)(torch.randn(4, 64)).shape) == (4, 3))
check("LSTM admitted (RecurrentBlock + sanitized DPLSTM, no buffers)",
      (not rejects(lambda: dh.assert_stock_architecture(model_spec.build_from_spec(_lstm, 64, 3))))
      and not rejects(lambda: dh.assert_releasable(model_spec.build_from_spec(_lstm, 64, 3))))
check("LSTM per-sample-safe (recurrence over TIME, not the batch)",
      not rejects(lambda: dh.per_sample_independence_probe(
          _cp2.deepcopy(model_spec.build_from_spec(_lstm, 64, 3)), nn.CrossEntropyLoss(),
          torch.randn(8, 64), torch.randint(0, 3, (8,)))))
_hooked = nn.Sequential(nn.Linear(3, 1))
_hooked[0].register_forward_hook(lambda m, i, o: o)
check("gate STILL rejects ANY module carrying a hook (sanitize did NOT weaken it)",
      rejects(lambda: dh.assert_stock_architecture(_hooked)))

# --------------------------------------------------------------------------- #
print("== improved floor: sample-and-aggregate (NRS) -- sensitivity 2C/k, server-routed ==")
_saC, _saK = 4.0, 5
_saOld = [np.zeros(10), np.zeros(3)]
_saRng = np.random.default_rng(0)
def _sa_ru(scale):
    return [_saOld[0] + _saRng.normal(0, scale, 10), _saOld[1] + _saRng.normal(0, scale, 3)]
def _sa_clipmean(blocks):
    md = [np.zeros_like(o) for o in _saOld]
    for bu in blocks:
        cl = dh.clip_update(bu, _saOld, _saC)
        for i, (c, o) in enumerate(zip(cl, _saOld)):
            md[i] = md[i] + (np.asarray(c) - o)
    return [m / len(blocks) for m in md]
_saWorst = 0.0
for _ in range(1000):
    _saA = [_sa_ru(10.0) for _ in range(_saK)]
    _saB = [(_saA[i] if i != 2 else _sa_ru(50.0)) for i in range(_saK)]
    _dA, _dB = _sa_clipmean(_saA), _sa_clipmean(_saB)
    _saWorst = max(_saWorst, float(np.linalg.norm(
        np.concatenate([(a - b).ravel() for a, b in zip(_dA, _dB)]))))
check("sample-and-aggregate L2 sensitivity <= 2C/k for ANY one-block neighbor",
      _saWorst <= 2 * _saC / _saK + 1e-9)
_saEps, _saDelta = 1.0, 1e-5
_saZero = [[o.copy() for o in _saOld] for _ in range(_saK)]
_saRel = np.array([np.concatenate([a.ravel() for a in
                   dh.sample_and_aggregate(_saZero, _saOld, _saC, _saEps, _saDelta)])
                   for _ in range(3000)])
_saTheo = dh.compute_output_sigma(_saEps, _saDelta, 2 * _saC / _saK)  # analytic Gaussian, sens 2C/k
check("sample-and-aggregate noise std matches the analytic-Gaussian sigma for sensitivity 2C/k",
      abs(_saRel.std() - _saTheo) / _saTheo < 0.12)
# analytic Gaussian must actually MEET the target delta at high epsilon (the classic
# sqrt(2 ln(1.25/delta)) S/eps bound leaks ~2.3x here -- the bug this fixes).
_agS = dh.compute_output_sigma(10.0, 1e-5, 2.0)
_agA, _agB = 2.0 / (2 * _agS), 10.0 * _agS / 2.0
_agDelta = (0.5 * (1 + math.erf((_agA - _agB) / math.sqrt(2)))
            - math.exp(10.0) * 0.5 * (1 + math.erf((-_agA - _agB) / math.sqrt(2))))
check("analytic Gaussian sigma meets the (eps=10, delta=1e-5) target (classic bound would leak)",
      _agDelta <= 1e-5 * 1.001)

class _SaFakeMod:
    def __init__(self): self.seen = []
    def local_update(self, g, X, y, cfg):
        self.seen.append(np.asarray(X).ravel().copy()); return [g[0] + 0.1, g[1] + 0.1]
_saPcfg = dict(clipping_norm=_saC, epsilon=2.0, delta=1e-5,
               sample_aggregate=True, sa_min_block=10, sa_max_blocks=8)  # explicit opt-in (default OFF)
_saX = np.arange(100).reshape(100, 1).astype(float); _saY = np.arange(100).astype(float)
_saFm = _SaFakeMod(); tier2_lib.gated_local_update(_saFm, _saOld, _saX, _saY, {}, _saPcfg)
check("server routes n=100 to k=8 disjoint blocks covering every row exactly once",
      len(_saFm.seen) == 8 and
      np.array_equal(np.sort(np.concatenate(_saFm.seen)), np.sort(_saX.ravel())))
check("block partition is RANDOM (not contiguous) -> data-independent",
      not all(np.array_equal(np.sort(s), np.arange(int(s.min()), int(s.min()) + len(s)))
              for s in _saFm.seen))
_saFm2 = _SaFakeMod(); tier2_lib.gated_local_update(_saFm2, _saOld, _saX[:8], _saY[:8], {}, _saPcfg)
_saFm3 = _SaFakeMod(); tier2_lib.gated_local_update(_saFm3, _saOld, _saX, _saY, {},
                                                    dict(_saPcfg, sample_aggregate=False))
_saFm4 = _SaFakeMod(); tier2_lib.gated_local_update(  # no SAA keys at all -> default OFF
    _saFm4, _saOld, _saX, _saY, {}, dict(clipping_norm=_saC, epsilon=2.0, delta=1e-5))
check("small n, policy-off, AND default (no key) all fall back to the plain 2C floor",
      len(_saFm2.seen) == 1 and len(_saFm3.seen) == 1 and len(_saFm4.seen) == 1)

class _SaBadMod:
    def local_update(self, g, X, y, cfg):
        if int(np.asarray(X)[0, 0]) % 2 == 0:
            raise RuntimeError("data-dependent boom")
        return [g[0] * np.nan, g[1]]
check("validate-or-zero: data-dependent crash/NaN blocks -> finite release (no crash/leak)",
      all(np.all(np.isfinite(a)) for a in
          tier2_lib.gated_local_update(_SaBadMod(), _saOld, _saX, _saY, {}, _saPcfg)))

# --------------------------------------------------------------------------- #
print(f"\n== DP safety suite: {ok} passed, {fail} failed ==")
sys.exit(1 if fail else 0)
