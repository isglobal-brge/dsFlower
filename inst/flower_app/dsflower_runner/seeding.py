"""Deterministic-noise seeding for the enforced-DP tracks.

Repeating the SAME query (same data + same config) on a node yields the SAME model,
so an attacker cannot average many identical runs to cancel the DP noise. The master
seed is HMAC(node_secret, canonical_config || data_fingerprint): the per-node secret
(created once at install, never released) makes the noise UNPREDICTABLE to the analyst,
while (config, data) make it reproducible across identical repeats.

If no node secret is present every RNG falls back to fresh OS entropy -- DP is always
safe (just non-deterministic). Determinism is best-effort, never at the cost of the
guarantee: a missing/unreadable secret can only make noise MORE random, never known.
"""
import hashlib
import hmac
import json
import os

_SECRET_ENV = "DSFLOWER_NODE_SECRET_FILE"
_SECRET_DEFAULT = "/var/lib/dsflower/node_secret"
_VOLATILE_CFG_KEYS = frozenset(("results-dir", "run-token", "run_token"))
_MASK63 = 0x7FFF_FFFF_FFFF_FFFF
_MASK31 = 0x7FFF_FFFF


def _node_secret():
    """Per-node secret bytes (created at install). None -> OS-entropy fallback."""
    path = os.environ.get(_SECRET_ENV) or _SECRET_DEFAULT
    try:
        with open(path, "rb") as fh:
            b = fh.read().strip()
        return b or None
    except Exception:
        return None


def canonical_config(cfg):
    """Stable string of the 'query' (model spec + hyperparams + target ...), minus
    volatile keys (results dir / run token) so identical queries map to identical seeds."""
    items = {k: cfg[k] for k in cfg if k not in _VOLATILE_CFG_KEYS}
    return json.dumps(items, sort_keys=True, default=str)


def data_fingerprint(X, y=None):
    """Stable SHA-256 of the training data. Same data -> same fingerprint."""
    import numpy as np
    h = hashlib.sha256()
    h.update(np.ascontiguousarray(np.asarray(X, dtype=np.float64)).tobytes())
    if y is not None:
        h.update(b"|")
        h.update(np.ascontiguousarray(np.asarray(y)).tobytes())
    return h.hexdigest()


def master_seed(cfg, X, y=None):
    """HMAC(node_secret, config || data_fingerprint) -> 64-bit int, or None if no secret."""
    secret = _node_secret()
    if secret is None:
        return None
    msg = (canonical_config(cfg) + "|" + data_fingerprint(X, y)).encode("utf-8")
    return int.from_bytes(hmac.new(secret, msg, hashlib.sha256).digest()[:8], "big")


def sub_seed(master, label):
    """Independent-but-deterministic sub-seed for one randomness axis. None -> None."""
    if master is None:
        return None
    d = hashlib.sha256((str(int(master)) + "|" + label).encode("utf-8")).digest()
    return int.from_bytes(d[:8], "big")


def np_rng(seed):
    """numpy Generator: seeded (deterministic) or fresh OS entropy when seed is None."""
    import numpy as np
    return np.random.default_rng(None if seed is None else int(seed))


def torch_generator(seed, device="cpu"):
    """torch.Generator: seeded (deterministic) or randomly seeded when seed is None."""
    import torch
    g = torch.Generator(device=device)
    if seed is None:
        g.seed()
    else:
        g.manual_seed(int(seed) & _MASK63)
    return g


def seed_torch(seed):
    """Seed the torch / numpy / random GLOBAL RNGs (weight init, dropout, Opacus's
    Poisson sampler and other internals). No-op when seed is None."""
    if seed is None:
        return
    import random
    import numpy as np
    import torch
    torch.manual_seed(int(seed) & _MASK63)
    np.random.seed(int(seed) & _MASK31)
    random.seed(int(seed) & _MASK31)
