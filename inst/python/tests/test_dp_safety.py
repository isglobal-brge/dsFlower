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
from unittest import mock as _mock

RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "flower_app", "dsflower_runner")
sys.path.insert(0, RUNNER)

import numpy as np
import torch
import torch.nn as nn
import dp_harness as dh
import model_spec
import seeding
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


def raises_value_error(fn):
    """True only for an explicit precondition rejection, not a later OOM/sentinel."""
    try:
        fn(); return False
    except ValueError:
        return True
    except Exception:
        return False


def _secure_rng(value):
    return seeding.np_rng(int(value).to_bytes(32, "big", signed=False))


# Tier-2 egress now runs uploads OUT-OF-PROCESS, so adversarial "apps" must be real
# importable packages (by name), never in-process objects. _mkmod writes one exact package
# initializer; the isolated child loads that path directly rather than resolving sys.path.
import tempfile as _tempfile
import textwrap as _textwrap
_T2DIR = _tempfile.mkdtemp(prefix="dpsafe_mods_")
sys.path.insert(0, _T2DIR)


def _mkmod(name, body):
    package = os.path.join(_T2DIR, name)
    os.mkdir(package)
    with open(os.path.join(package, "__init__.py"), "w") as f:
        f.write("import numpy as np\n" + _textwrap.dedent(body))
    return name


# --------------------------------------------------------------------------- #
print("== output-perturbation floor: composed Gaussian RDP + sensitivity bound ==")
C, eps, delta = 1.0, 1.0, 1e-5
sigma = dh.compute_output_sigma(eps, delta, C)
def _rdp_epsilon_bound(s, de, sens, releases=1):
    z = sens / s
    return releases * z * z / 2 + z * math.sqrt(
        2 * releases * math.log(1 / de))
check("compute_output_sigma meets the closed Gaussian RDP bound",
      _rdp_epsilon_bound(sigma, delta, C) <= eps)
check("Gaussian RDP calibration is safe across the full numeric policy range",
      all(_rdp_epsilon_bound(
              dh.compute_output_sigma(e, d, 2.0, num_releases=r), d, 2.0, r) <= e
          for e in (1e-6, 0.5, 1.0, 3.0, 10.0)
          for d in (1e-3, 1e-5, 1e-12) for r in (1, 3, 500)))
check("composed per-release sigma scales exactly as sqrt(R)",
      math.isclose(
          dh.compute_output_sigma(eps, delta, C, num_releases=9),
          3 * dh.compute_output_sigma(eps, delta, C),
          rel_tol=2e-15, abs_tol=0.0))
check("compute_output_sigma rejects eps<=0", rejects(lambda: dh.compute_output_sigma(0, delta, C)))
check("compute_output_sigma rejects a non-integer release horizon",
      rejects(lambda: dh.compute_output_sigma(eps, delta, C, num_releases=1.5)))

old = [np.zeros((4, 3), np.float32), np.zeros(3, np.float32)]
huge = [np.full((4, 3), 1e6, np.float32), np.full(3, 1e6, np.float32)]   # "raw data" delta
clipped = dh.clip_update(huge, old, C)
cd = np.concatenate([(c - o).ravel() for c, o in zip(clipped, old)])
check("clip_update bounds ||delta|| <= C", np.linalg.norm(cd) <= C + 1e-5)

out = dh.output_perturbation(
    huge, old, clipping_norm=C, epsilon=eps, delta=delta,
    rng=_secure_rng(100))
rel = np.concatenate([(o2 - o1).ravel() for o2, o1 in zip(out, old)])
check("output-perturbation DESTROYS raw-data exfil (1e6 -> O(sigma))",
      np.max(np.abs(rel)) < 50)

# The floor's L2 sensitivity is the C-ball DIAMETER = 2C (arbitrary code), so the noise
# std must be 2*sigma(C), NOT sigma(C). Test at C=1 AND C=4: C=4 also catches the
# double-C regression (a prior version multiplied the clip in twice -> std ~ C^2).
for Cx in (1.0, 4.0):
    s1 = dh.compute_output_sigma(eps, delta, Cx)                  # k*Cx/eps (sensitivity Cx)
    z = [np.zeros(300000, np.float32)]
    emp = float(np.std(dh.output_perturbation(
        z, z, clipping_norm=Cx, epsilon=eps, delta=delta,
        rng=_secure_rng(int(1000 * Cx)))[0]))
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
print("== Tier-2 gate (ISOLATED gated_local_update): adversarial uploads cannot leak ==")
pcfg = {
    "clipping_norm": 1.0, "epsilon": 1.0, "delta": 1e-5,
    "egress_timeout": 30, "hook_enabled": True, "sample_aggregate": False,
}
g = [np.zeros(20, np.float32)]
Xraw = (np.random.randn(8) * 7).astype(np.float32); yraw = np.zeros(8)
_mkmod("t2_exfil", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg):
            w = [np.asarray(a).copy() for a in g]; X = np.asarray(X)
            n = min(X.size, w[0].size); w[0].ravel()[:n] = X.ravel()[:n] * 1e6
            return w
        """)
_mkmod("t2_monkey", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg):
            try:                       # try to disable the DP from inside (in-process attack)
                import dp_harness
                dp_harness.add_gaussian_noise = lambda *a, **k: list(a[0])
                dp_harness.compute_output_sigma = lambda *a, **k: 0.0
            except Exception: pass
            return [np.asarray(g[0]) + 1e6]
        """)
_mkmod("t2_wrongshape", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg): return [np.zeros(21, np.float32)]
        """)
_mkmod("t2_crash", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg): raise RuntimeError("data-dependent boom")
        """)
_mkmod("t2_huge", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg): return [np.zeros(5_000_000, np.float64)]
        """)
_mkmod("t2_count", """
        def initial_arrays(cfg, d): return [np.zeros(d, np.float32)]
        def local_update(g, X, y, cfg): return [np.asarray(g[0]), np.zeros(3)]
        """)
_test_caps = {"subprocess": True, "net_lock": True, "fs_isolation": True,
              "bwrap": None}
_hook_cfg = {
    "app_params": {}, "round_index": 1, "num_rounds": 1,
    "task": "classification", "num_classes": 2,
}
_pinned_file = lambda name: os.path.join(_T2DIR, name, "__init__.py")
with _mock.patch.object(tier2_lib, "hook_execution_caps", return_value=_test_caps), \
        _mock.patch.object(tier2_lib, "_pinned_user_package", side_effect=_pinned_file):
    _seed = b"t" * 32
    _ex = tier2_lib.gated_local_update(
        "t2_exfil", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 exfil via weights DESTROYED by the gate (1e6 raw -> O(sigma))",
          np.max(np.abs(np.concatenate([(o - gg).ravel()
                                        for o, gg in zip(_ex, g)]))) < 100)
    _mk = tier2_lib.gated_local_update(
        "t2_monkey", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 in-process MONKEYPATCH defeated by isolation (parent DP intact, release bounded)",
          np.max(np.abs(np.concatenate([(o - gg).ravel()
                                        for o, gg in zip(_mk, g)]))) < 100)
    check("isolation: the parent NEVER imported the untrusted upload (absent from sys.modules)",
          "t2_monkey" not in sys.modules and "t2_exfil" not in sys.modules)
    check("trusted Hook loader shares the exact SecureNumpyRng class with its DP gate",
          isinstance(tier2_lib.seeding.np_rng(b"k" * 32),
                     tier2_lib.dp_harness.SecureNumpyRng))
    _ws = tier2_lib.gated_local_update(
        "t2_wrongshape", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 shape-mismatch NEUTRALIZED (validate-or-zero -> noisy global shape)",
          len(_ws) == len(g) and all(a.shape == o.shape for a, o in zip(_ws, g))
          and all(np.all(np.isfinite(a)) for a in _ws))
    _cr = tier2_lib.gated_local_update(
        "t2_crash", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 crashing/data-dependent upload -> finite NOISY release",
          all(np.all(np.isfinite(a)) for a in _cr)
          and all(a.shape == o.shape for a, o in zip(_cr, g))
          and any(not np.array_equal(a, o) for a, o in zip(_cr, g)))
    _hr = tier2_lib.gated_local_update(
        "t2_huge", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 oversized result rejected by the size cap -> noisy zero update (no parent OOM)",
          len(_hr) == len(g) and all(a.shape == o.shape for a, o in zip(_hr, g)))
    _wc = tier2_lib.gated_local_update(
        "t2_count", g, Xraw, yraw, _hook_cfg, pcfg,
        seed=_seed, hook_caps=_test_caps)
    check("Tier-2 wrong array-count rejected before load -> noisy zero update",
          len(_wc) == len(g) and all(np.all(np.isfinite(a)) for a in _wc))
    check("gated_local_update refuses a non-str module (the node never imports an object)",
          rejects(lambda: tier2_lib.gated_local_update(
              object(), g, Xraw, yraw, _hook_cfg, pcfg, seed=_seed,
              hook_caps=_test_caps)))
import time as _time
_t0 = _time.monotonic()
_disabled = tier2_lib.gated_local_update(
    "t2_crash", g, Xraw, yraw, _hook_cfg,
    dict(pcfg, egress_time_pad=2.0))
check("HookApp stays disabled when the timing envelope is shorter than timeout+guard",
      _time.monotonic() - _t0 < 1.9
      and all(np.array_equal(a, b) for a, b in zip(_disabled, g)))
check("seccomp network-lock helper is present + callable (no-op off-Linux)",
      callable(getattr(__import__("egress_child"), "_install_seccomp_no_net", None)))

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
print("== robust Huber regression (public parameter, per-sample DP-SGD) ==")
hb = dh.loss_from_allowlist("huber", {"huber-delta": 0.5})
hp = torch.tensor([[0.0], [2.0], [-3.0]])
hy = torch.tensor([[0.0], [0.0], [1.0]])
reference = nn.HuberLoss(delta=0.5, reduction="mean")(hp, hy)
check("huber uses the exact public delta", torch.equal(hb(hp, hy), reference))
check("changing huber-delta changes the applied loss",
      float(hb(hp, hy)) != float(dh.loss_from_allowlist(
          "huber", {"huber-delta": 2.0})(hp, hy)))
check("huber rejects an invalid public delta",
      rejects(lambda: dh.loss_from_allowlist("huber", {"huber-delta": 0.0})))
check("huber model passes the per-sample independence probe",
      not rejects(lambda: dh.per_sample_independence_probe(
          PosHead(), hb, torch.randn(8, 3), torch.randn(8, 1))))
check("huber has scalar width and the wide regression output domain",
      model_spec.output_width("huber", {"num-classes": 2}) == 1
      and model_spec.output_limit_for_loss("huber") == model_spec._MAX_ACTIVATION_ABS)

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
print("== hostile model specs: reject before large constructors/forwards ==")
import base64 as _base64
_real_b64decode = _base64.b64decode
_decode_calls = []
def _forbidden_decode(*args, **kwargs):
    _decode_calls.append(args)
    raise AssertionError("decoder reached")
_base64.b64decode = _forbidden_decode
try:
    _oversized_b64_rejected = raises_value_error(lambda: model_spec.read_spec(
        {"model-spec-b64": "A" * (model_spec._MAX_SPEC_B64_BYTES + 4)}))
finally:
    _base64.b64decode = _real_b64decode
check("oversized encoded spec rejected before base64 decode",
      _oversized_b64_rejected and not _decode_calls)
import json as _json
_real_json_loads = _json.loads
_json_calls = []
def _forbidden_json_loads(*args, **kwargs):
    _json_calls.append(args)
    raise AssertionError("JSON parser reached")
_json.loads = _forbidden_json_loads
try:
    _large_decoded = _base64.b64encode(
        b" " * (model_spec._MAX_SPEC_JSON_BYTES + 1)).decode("ascii")
    _oversized_json_rejected = raises_value_error(lambda: model_spec.read_spec(
        {"model-spec-b64": _large_decoded}))
finally:
    _json.loads = _real_json_loads
check("oversized decoded spec rejected before JSON parse",
      _oversized_json_rejected and not _json_calls)

_real_linear = model_spec.nn.Linear
_linear_calls = []
def _forbidden_linear(*args, **kwargs):
    _linear_calls.append(args)
    raise AssertionError("Linear constructor reached")
model_spec.nn.Linear = _forbidden_linear
try:
    _huge_linear_rejected = raises_value_error(lambda: model_spec.build_from_spec(
        {"kind": "sequential", "layers": [
            {"op": "linear", "out": model_spec._MAX_WIDTH},
            {"op": "linear", "out": "@out"}]},
        model_spec._MAX_WIDTH, 2))
finally:
    model_spec.nn.Linear = _real_linear
check("huge linear parameter product rejected before constructor",
      _huge_linear_rejected and not _linear_calls)

_real_conv1d = model_spec.nn.Conv1d
_conv_calls = []
def _forbidden_conv1d(*args, **kwargs):
    _conv_calls.append(args)
    raise AssertionError("Conv1d constructor reached")
model_spec.nn.Conv1d = _forbidden_conv1d
try:
    _huge_conv_rejected = raises_value_error(lambda: model_spec.build_from_spec(
        {"kind": "sequential", "layers": [
            {"op": "reshape", "shape": [4096, 1]},
            {"op": "conv1d", "out_channels": 4096, "kernel_size": 1},
            {"op": "flatten"}, {"op": "linear", "out": "@out"}]}, 4096, 2))
finally:
    model_spec.nn.Conv1d = _real_conv1d
check("huge convolution parameter product rejected before constructor",
      _huge_conv_rejected and not _conv_calls)

_linear_calls = []
def _cheap_linear(*args, **kwargs):
    _linear_calls.append(args)
    return nn.Identity()
model_spec.nn.Linear = _cheap_linear
try:
    _cumulative_rejected = raises_value_error(lambda: model_spec.build_from_spec(
        {"kind": "sequential", "layers": [
            {"op": "linear", "out": 2000}, {"op": "linear", "out": 2000},
            {"op": "linear", "out": "@out"}]}, 2000, 2))
finally:
    model_spec.nn.Linear = _real_linear
check("cumulative parameter budget rejects before the overflowing constructor",
      _cumulative_rejected and len(_linear_calls) == 1)
check("oversized activation shape rejected before a model forward", raises_value_error(
      lambda: model_spec.build_from_spec({"kind": "sequential", "layers": [
          {"op": "reshape", "shape": [1, 64, 64]},
          {"op": "upsample", "scale_factor": 64}, {"op": "flatten"},
          {"op": "linear", "out": "@out"}]}, 4096, 2)))
check("oversized node-owned input dim rejected before constructor", raises_value_error(
      lambda: model_spec.build_from_spec({"kind": "sequential", "layers": [
          {"op": "linear", "out": "@out"}]}, model_spec._MAX_DIM + 1, 2)))
check("non-finite graph affine constants rejected", raises_value_error(
      lambda: model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
          {"name": "bad", "op": "affine", "in": ["@in"], "scale": math.nan},
          {"name": "out", "op": "linear", "in": ["bad"], "out": "@out"}]}, 4, 2)))
check("oversized graph affine constants rejected", raises_value_error(
      lambda: model_spec.build_from_spec({"kind": "graph", "output": "out", "nodes": [
          {"name": "bad", "op": "affine", "in": ["@in"],
           "scale": model_spec._MAX_PUBLIC_SCALAR_ABS + 1.0},
          {"name": "out", "op": "linear", "in": ["bad"], "out": "@out"}]}, 4, 2)))
_wide_regression = model_spec.build_from_spec(
    {"kind": "sequential", "layers": [{"op": "linear", "out": "@out"}]},
    1, 1, output_limit=model_spec.output_limit_for_loss("mse"))
with torch.no_grad():
    _wide_regression[0].weight.fill_(model_spec._MAX_PUBLIC_SCALAR_ABS)
    _wide_regression[0].bias.zero_()
_wide_value = _wide_regression(torch.ones(1, 1)).item()
check("direct MSE regression keeps a wide finite output domain",
      model_spec._MAX_OUTPUT_ABS < _wide_value <= model_spec._MAX_ACTIVATION_ABS)

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

_total_div = {"kind": "graph", "output": "out", "nodes": [
    {"name": "ratio", "op": "div", "in": ["@in", "@in"]},
    {"name": "out", "op": "linear", "in": ["ratio"], "out": "@out"}]}
_total_div_model = model_spec.build_from_spec(_total_div, 3, 1)
_total_div_x = torch.tensor([[0.0, 1.0, -1.0], [1.0, 2.0, 3.0]])
check("DAG division is total at zero and emits only finite bounded logits",
      bool(torch.isfinite(_total_div_model(_total_div_x)).all())
      and bool((_total_div_model(_total_div_x).abs() <= model_spec._MAX_OUTPUT_ABS).all()))
check("totalized DAG division remains per-sample independent",
      not rejects(lambda: dh.per_sample_independence_probe(
          _cp2.deepcopy(_total_div_model), nn.MSELoss(),
          _total_div_x.repeat(4, 1), torch.zeros(8, 1))))

_total_arithmetic = {"kind": "graph", "output": "out", "nodes": [
    {"name": "product", "op": "mul", "in": ["@in", "@in"]},
    {"name": "scaled", "op": "affine", "in": ["product"],
     "scale": model_spec._MAX_PUBLIC_SCALAR_ABS,
     "shift": model_spec._MAX_PUBLIC_SCALAR_ABS},
    {"name": "out", "op": "linear", "in": ["scaled"], "out": "@out"}]}
_total_arithmetic_out = model_spec.build_from_spec(
    _total_arithmetic, 3, 1)(torch.full((4, 3), model_spec._MAX_ACTIVATION_ABS))
check("DAG extreme finite arithmetic saturates instead of failing",
      bool(torch.isfinite(_total_arithmetic_out).all())
      and bool((_total_arithmetic_out.abs() <= model_spec._MAX_OUTPUT_ABS).all()))

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
print("== improved floor: sample-and-aggregate -- conservative multi-block sensitivity ==")
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
_saNoise = _secure_rng(991)
_saRel = np.array([np.concatenate([a.ravel() for a in
                   dh.sample_and_aggregate(
                       _saZero, _saOld, _saC, _saEps, _saDelta, rng=_saNoise)])
                   for _ in range(3000)])
_saSens = min(2 * _saC, 4 * _saC / _saK)
_saTheo = dh.compute_output_sigma(_saEps, _saDelta, _saSens)
check("sample-and-aggregate noise matches min(2C,4C/k) conservative sensitivity",
      abs(_saRel.std() - _saTheo) / _saTheo < 0.12)
# The strict safety margin keeps the independently recomputed RDP inequality on
# the safe side even at the smallest supported epsilon/delta and 500 releases.
_edge_eps, _edge_delta, _edge_r = 1e-6, 1e-12, 500
_edge_sigma = dh.compute_output_sigma(
    _edge_eps, _edge_delta, 2.0, num_releases=_edge_r)
check("Gaussian RDP sigma is strictly safe at the numerical policy edge",
      _rdp_epsilon_bound(
          _edge_sigma, _edge_delta, 2.0, _edge_r) <= _edge_eps)

# Mechanism selection is the NODE's automatic, capability-gated decision (NOT a researcher
# opt-in). _choose_blocks is the pure server-side rule, tested directly.
_saPol = dict(sa_blocks=8)
check("SAA is GATED OFF without a full sandbox (subprocess-only) -> plain 2C floor",
      tier2_lib._choose_blocks(_saPol, False) == 1)
check("with a full sandbox the node uses the fixed public block count",
      tier2_lib._choose_blocks(_saPol, True) == 8)
check("custodian governance off-switch honoured even with a full sandbox",
      tier2_lib._choose_blocks(dict(_saPol, sample_aggregate=False), True) == 1)
check("subprocess isolation (the monkeypatch fix) is universally available",
      tier2_lib.sandbox_caps()["subprocess"] is True)
check("SAA full-sandbox gate requires real net+fs isolation (not subprocess alone)",
      tier2_lib._full_sandbox_ok({"subprocess": True, "net_lock": False, "fs_isolation": False})
      is False)

# --------------------------------------------------------------------------- #
print(f"\n== DP safety suite: {ok} passed, {fail} failed ==")
sys.exit(1 if fail else 0)
