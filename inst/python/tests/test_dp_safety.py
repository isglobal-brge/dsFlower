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
check("sigma = sqrt(2 ln(1.25/delta)) * C/eps",
      abs(sigma - math.sqrt(2 * math.log(1.25 / delta)) * (C / eps)) < 1e-9)
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
check("Tier-2 shape-mismatch smuggling REJECTED",
      rejects(lambda: tier2_lib.gated_local_update(WrongShape(), g, Xraw, np.zeros(8), {}, pcfg)))

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
print(f"\n== DP safety suite: {ok} passed, {fail} failed ==")
sys.exit(1 if fail else 0)
