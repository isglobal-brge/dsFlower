"""Trusted Tier-2 runner library (node-resident).

The researcher's uploaded app provides a narrow, reviewable interface (exfiltration-scanned
+ hash-verified at install, framework-agnostic):

    initial_arrays(cfg: dict, input_dim: int) -> list[np.ndarray]
        # starting global model parameters; the ServerApp (researcher's own machine) uses it
    local_update(global_arrays, X, y, cfg) -> list[np.ndarray]
        # train however you like on the node's PRIVATE data, return new parameters

PROCESS-ISOLATED TRUST BOUNDARY. On the data node the untrusted `local_update` is run in a
FRESH, separate interpreter (`egress_child.py`); the trusted PARENT never imports or executes
the upload. The child can only ever hand back plain numeric arrays (loaded with
allow_pickle=False), which the parent validates and then privatises itself -- clip the delta
to the C-ball and add analytic-Gaussian noise. So the upload cannot monkeypatch the DP
harness / NumPy / the RNG, cannot leak via a crash/traceback, and (with a sufficient
sandbox) cannot carry state across sample-and-aggregate blocks. DP parameters always come
from the server-written manifest, never from the app.

Mechanism selection is the NODE's automatic, server-authoritative decision -- never the
researcher's: the plain 2C output-perturbation floor universally, and the sample-and-aggregate
2C/k floor when (a) the platform provides a sandbox strong enough to GUARANTEE per-block
independence and (b) policy thresholds say it helps.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile

import numpy as np

try:
    import dp_harness            # node-resident trusted copy
except ImportError:
    from . import dp_harness     # bundled fallback


_REQUIRED_HOOKS = ("initial_arrays", "local_update")
_CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "egress_child.py")
_FSIZE_LIMIT = 1024 * 1024 * 1024     # 1 GiB cap on child file writes
_DEFAULT_TIMEOUT = 900                 # wall-clock seconds per child run


def load_user_module(module_name):
    """Import the uploaded app + confirm the interface. SERVER/CLIENT-side ONLY (the
    ServerApp runs on the researcher's own machine, for initial_arrays). The data NODE never
    calls this -- it runs the upload out-of-process via gated_local_update."""
    import importlib
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


def _sanitize_cfg(cfg):
    """Hand the untrusted child only inert scalar cfg values; strip DP params, the module
    pointer, and anything non-scalar (no secrets, no paths, no objects)."""
    out = {}
    for k, v in dict(cfg or {}).items():
        ks = str(k)
        if ks == "user-module" or ks.startswith(("privacy-", "dp_", "dp-")):
            continue
        if v is None or isinstance(v, (str, int, float, bool)):
            out[ks] = v
    return out


# --------------------------------------------------------------------------- #
# Sandbox capability preflight (subprocess is universal; the rest is platform-gated)
# --------------------------------------------------------------------------- #

def _bwrap_works(path):
    """True iff bubblewrap can actually unshare the network here (userns enabled)."""
    try:
        r = subprocess.run([path, "--unshare-net", "--ro-bind", "/", "/", "true"],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=10)
        return r.returncode == 0
    except Exception:
        return False


def sandbox_caps():
    """What isolation the platform provides RIGHT NOW. subprocess + parent-side DP (the
    monkeypatch fix) is universal; cross-block net+fs isolation (needed to make
    sample-and-aggregate sound against malicious code) is offered only where a namespace
    sandbox (bubblewrap) genuinely works."""
    caps = {"subprocess": True, "linux": sys.platform.startswith("linux"),
            "rlimit": False, "bwrap": None, "net_lock": False, "fs_isolation": False}
    try:
        import resource  # noqa: F401
        caps["rlimit"] = True
    except Exception:
        pass
    tool = shutil.which("bwrap")
    if tool and _bwrap_works(tool):
        caps["bwrap"] = tool
        caps["net_lock"] = True
        caps["fs_isolation"] = True
    return caps


def _full_sandbox_ok(caps):
    """Sample-and-aggregate's 2C/k bound needs GUARANTEED per-block independence, i.e. no
    cross-block state via shared network or filesystem."""
    return bool(caps.get("subprocess") and caps.get("net_lock") and caps.get("fs_isolation"))


def _wrap_sandbox(cmd, caps, td):
    """Wrap the child in bubblewrap (private net + read-only fs + writable only its temp dir)
    when available. Otherwise run it plain: the parent still applies ALL DP, and network is
    best-effort (the child's Python sockets are neutered + the container egress policy is the
    production boundary)."""
    if caps.get("bwrap") and caps.get("net_lock"):
        return [caps["bwrap"], "--unshare-all", "--ro-bind", "/", "/",
                "--bind", td, td, "--proc", "/proc", "--dev", "/dev",
                "--die-with-parent", "--", *cmd]
    return cmd


def _killpg(p):
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass


def _run_isolated(module_name, old, X, y, cfg, caps, timeout):
    """Run the untrusted local_update on (X, y) in a FRESH interpreter. Returns f64 arrays,
    or None on ANY failure (crash, timeout, wrong count/shape, non-finite, unreadable). The
    parent never imports or executes the upload; the result is loaded allow_pickle=False so
    it can never execute code here."""
    td = tempfile.mkdtemp(prefix="dsf_egress_")
    try:
        inp = os.path.join(td, "in.npz")
        outd = os.path.join(td, "out")
        cfgf = os.path.join(td, "cfg.json")
        np.savez(inp, **{("g_%03d" % i): np.asarray(o, np.float64) for i, o in enumerate(old)},
                 X=np.asarray(X), y=np.asarray(y))
        with open(cfgf, "w") as f:
            json.dump(_sanitize_cfg(cfg), f)
        syspath = os.pathsep.join(p for p in sys.path if p)
        base = [sys.executable, "-B", "-E", "-s", _CHILD,
                "--in", inp, "--out", outd, "--cfg", cfgf,
                "--module", str(module_name), "--syspath", syspath]
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "TMPDIR": td,
               "DSF_RLIMIT_CPU": str(int(timeout)), "DSF_RLIMIT_FSIZE": str(_FSIZE_LIMIT),
               "DSF_NO_NET": "1"}
        cmd = _wrap_sandbox(base, caps, td)
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, env=env, start_new_session=True)
        except Exception:
            return None
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _killpg(p)
            return None
        finally:
            _killpg(p)  # reap any lingering grandchildren in the process group
        if p.returncode != 0:
            return None
        okf = os.path.join(outd, "_ok")
        if not os.path.exists(okf):
            return None
        try:
            nout = int(open(okf).read())
        except Exception:
            return None
        res = []
        for i in range(nout):
            wf = os.path.join(outd, "w_%d.npy" % i)
            if not os.path.exists(wf):
                return None
            try:
                a = np.load(wf, allow_pickle=False)
            except Exception:
                return None
            res.append(np.asarray(a, np.float64))
        return res
    finally:
        shutil.rmtree(td, ignore_errors=True)


def _validate(res, old):
    """Authoritative parent-side check: exact array count + shapes matching `old`, all
    finite. Anything else -> None (caller maps to a zero delta)."""
    if res is None or len(res) != len(old):
        return None
    for a, o in zip(res, old):
        if a.shape != o.shape or not bool(np.all(np.isfinite(a))):
            return None
    return res


def _choose_blocks(n, pcfg, full_sandbox):
    """Server-managed ADAPTIVE mechanism selection (never a researcher choice): use
    sample-and-aggregate (k>=2) only when the FULL sandbox guarantees per-block independence
    AND policy thresholds say it applies; otherwise the plain 2C floor. k is derived from
    the PUBLIC row count only -- sound because n is invariant under replace-one adjacency.
    `sample_aggregate` here is a CUSTODIAN governance switch (default on where sound), not a
    researcher/analyst knob."""
    if not full_sandbox:
        return 1
    if not bool(pcfg.get("sample_aggregate", True)):
        return 1
    min_block = max(1, int(pcfg.get("sa_min_block", 64)))
    max_blocks = max(1, int(pcfg.get("sa_max_blocks", 8)))
    return max(1, min(max_blocks, int(n) // min_block))


def gated_local_update(module_name, global_arrays, X, y, cfg, pcfg):
    """Run the upload out-of-process from the global model, then apply the DP gate in the
    trusted parent. The NODE picks the mechanism: sample-and-aggregate (2C/k) when the
    platform can guarantee block independence and policy says so, else the plain 2C floor.
    `module_name` (not an imported module) is passed so the parent never imports the upload.

    Back-compat: if a pre-isolation caller still passes an imported module object, fall back
    to its __name__ so older ClientApps keep working."""
    if not isinstance(module_name, str):
        module_name = getattr(module_name, "__name__", None) or str(module_name)
    old = _as_f64_list(global_arrays)
    n = int(len(X))
    caps = sandbox_caps()
    k = _choose_blocks(n, pcfg, _full_sandbox_ok(caps))
    timeout = int(pcfg.get("egress_timeout", _DEFAULT_TIMEOUT))

    if k >= 2:
        # Data-INDEPENDENT random partition into k disjoint blocks (fresh-entropy permutation
        # of row INDICES, never by feature/label). One record lands in exactly one block, and
        # each block runs in its OWN isolated interpreter -> genuine independence.
        perm = np.random.default_rng().permutation(n)
        block_updates = []
        for idx in np.array_split(perm, k):
            r = _validate(_run_isolated(module_name, old, _take_rows(X, idx),
                                        _take_rows(y, idx), cfg, caps, timeout), old)
            block_updates.append(r if r is not None else [o.copy() for o in old])
        gated = dp_harness.sample_and_aggregate(
            block_updates, old,
            clipping_norm=pcfg["clipping_norm"],
            epsilon=pcfg["epsilon"],
            delta=pcfg["delta"],
        )
    else:
        r = _validate(_run_isolated(module_name, old, X, y, cfg, caps, timeout), old)
        new = r if r is not None else [o.copy() for o in old]   # validate-or-zero
        gated = dp_harness.output_perturbation(
            new, old,
            clipping_norm=pcfg["clipping_norm"],
            epsilon=pcfg["epsilon"],
            delta=pcfg["delta"],
        )
    return [g.astype(np.float32) for g in gated]
