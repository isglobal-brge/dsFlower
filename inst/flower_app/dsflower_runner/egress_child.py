"""Isolated Tier-2 worker -- runs the UNTRUSTED researcher `local_update` in a FRESH
interpreter, fully separate from the trusted parent.

Why this file exists: the parent (the ClientApp on the data node) applies ALL differential
privacy -- clip to the C-ball, aggregate, add analytic-Gaussian noise. The untrusted upload
must therefore never share the parent's process: in-process it could monkeypatch the DP
harness / NumPy / the RNG before the gate runs, or carry state across sample-and-aggregate
blocks and break the 2C/k independence. Here the child only ever produces a plain numeric
weight array, written as .npy; the parent reads it back with allow_pickle=False (so the
result can never execute code) and validates it. Anything the child does to its OWN
interpreter is irrelevant.

Runs in the CHILD only. Portable: the hardening below is all best-effort and Linux-gated, so
the same file imports and runs on Linux/macOS/Windows (advanced sandboxing simply no-ops
where unavailable). The parent decides, by capability preflight, what guarantees hold.
"""

import argparse
import json
import os
import sys


def _harden():
    """Best-effort, never-fatal child hardening. Resource limits + a Python-level network
    neuter. NOT a security boundary on its own (a determined child can evade Python-level
    blocks); the airtight layers are the parent (which never runs user code), the container
    egress policy, and -- where available -- namespace/seccomp sandboxing applied by the
    parent's launcher."""
    try:
        import resource
        cpu = int(os.environ.get("DSF_RLIMIT_CPU", "0"))
        if cpu > 0:
            resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu + 2))
        asb = int(os.environ.get("DSF_RLIMIT_AS", "0"))
        if asb > 0:
            resource.setrlimit(resource.RLIMIT_AS, (asb, asb))
        fsz = int(os.environ.get("DSF_RLIMIT_FSIZE", "0"))
        if fsz > 0:
            resource.setrlimit(resource.RLIMIT_FSIZE, (fsz, fsz))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except Exception:
        pass
    if os.environ.get("DSF_NO_NET") == "1":
        try:
            import socket

            def _blocked(*a, **k):
                raise OSError("network disabled in the DP egress sandbox")

            socket.socket = _blocked              # type: ignore[assignment]
            socket.create_connection = _blocked   # type: ignore[assignment]
            if hasattr(socket, "create_server"):
                socket.create_server = _blocked   # type: ignore[assignment]
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--cfg", dest="cfg", required=True)
    ap.add_argument("--module", dest="module", required=True)
    ap.add_argument("--syspath", dest="syspath", default="")
    args = ap.parse_args()

    _harden()

    import numpy as np

    for p in args.syspath.split(os.pathsep):
        if p and p not in sys.path:
            sys.path.insert(0, p)

    data = np.load(args.inp, allow_pickle=False)
    gkeys = sorted(k for k in data.files if k.startswith("g_"))
    old = [data[k] for k in gkeys]
    X = data["X"]
    y = data["y"]
    with open(args.cfg) as f:
        cfg = json.load(f)

    import importlib
    mod = importlib.import_module(args.module)            # re-verified by sitecustomize
    res = mod.local_update([np.asarray(o).copy() for o in old], X, y, cfg)
    res = [np.asarray(w, dtype=np.float64) for w in res]  # arrays only -> .npy

    os.makedirs(args.out, exist_ok=True)
    for i, w in enumerate(res):
        np.save(os.path.join(args.out, "w_%d.npy" % i), w, allow_pickle=False)
    # success sentinel written LAST + atomically: the parent treats its absence as failure
    tmp = os.path.join(args.out, "_ok.tmp")
    with open(tmp, "w") as f:
        f.write(str(len(res)))
    os.replace(tmp, os.path.join(args.out, "_ok"))


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException:
        # never surface a (possibly data-dependent) traceback; the parent maps a missing
        # success sentinel to a zero delta.
        os._exit(3)
