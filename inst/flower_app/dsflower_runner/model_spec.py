"""Declarative model spec -> nn.Module, built ENTIRELY node-side from a fixed
allowlist of stock layer constructors.

WHY THIS EXISTS (the central security property)
------------------------------------------------
The researcher ships a JSON *spec* (DATA), never Python source. No researcher code
ever runs in the trusted interpreter. That structurally closes the worst exfil
class we found while red-teaming the source-submission design: a researcher module
imported into the trusted interpreter can, AT IMPORT (before any object-surface
gate runs), monkeypatch global torch internals -- e.g. ``torch.Tensor.numpy`` or
``F.linear`` -- so that the trusted DP-release path (which reads weights via
``p.numpy()``) silently ships RAW DATA while the returned module stays 100% stock
and passes every architecture gate. No object inspection can close that, because
the betrayal is in researcher-mutated GLOBAL state, not in the object.

A spec removes the premise: there is no researcher module to import. The node maps
op-strings to FIXED, locally-held torch constructors (never ``getattr(nn, name)``,
never ``eval``), so a spec can at worst be REJECTED or build a bounded, genuinely
stock module. It can never execute.

WHAT IS ALLOWED (max flexibility within the secure + DP-valid paradigm)
-----------------------------------------------------------------------
Only layers whose forward is independent across the batch dimension, so the DP-SGD
per-sample gradient sensitivity bound holds:
  * ``linear``                          (the workhorse; carries params)
  * pointwise activations: ``relu``, ``gelu``, ``tanh``, ``sigmoid``, ``elu``,
    ``silu``, ``leaky_relu``
  * ``dropout``                         (independent per-sample mask)
  * ``layernorm``                       (normalizes over FEATURES, per-sample;
                                         affine params, NO batch-coupled buffers)
BatchNorm and anything that mixes across the batch are deliberately NOT in the
vocabulary -- they break per-sample DP-SGD (Opacus' ModuleValidator rejects them
too). Residual/branching graphs and convolutions are clean future ops (still DATA,
still node-built); the current vocabulary is the universal feed-forward space and
covers every first-party model (tabular heads + frozen-backbone vision heads).
"""

import torch
import torch.nn as nn

# Hard caps so a hostile spec is bounded -- never an OOM / DoS lever.
_MAX_LAYERS = 64
_MAX_WIDTH = 8192          # cap on a LITERAL linear width (symbolic @out is trusted)
_MAX_DIM = 1 << 20         # sanity bound on any resolved dimension


def output_width(loss_name, cfg):
    """The node-decided output width for the run's PINNED loss. The researcher never
    specifies output width: the spec ends with a linear to the symbolic ``@out`` and
    the node fills it in from the loss it pinned, so a mis-sized head is impossible.
    Mirrors the historical generator logic, but server-authoritative."""
    nc = int(cfg.get("num-classes", 2))
    if loss_name == "cross_entropy":
        return max(2, nc)                      # one logit per class (>=2, softmax-CE)
    if loss_name == "multilabel_bce":
        return int(cfg["num-labels"])          # one independent logit per label
    if loss_name in ("mse", "poisson_nll"):
        return 1                               # scalar regression / log-rate
    return 1 if nc <= 2 else nc                # bce_logits: binary, or one-vs-rest


def read_spec(cfg):
    """Decode the base64 JSON spec from the run config into a plain dict. Base64 so
    the JSON travels intact through TOML without quote-escaping pitfalls."""
    import base64
    import json
    raw = cfg.get("model-spec-b64")
    if not raw:
        raise ValueError("run config missing 'model-spec-b64' (neural track needs a spec)")
    try:
        data = base64.b64decode(str(raw), validate=True)
        spec = json.loads(data.decode("utf-8"))
    except Exception as e:
        raise ValueError("could not decode model spec: %s" % e)
    return spec


# --------------------------------------------------------------------------- #
# Typed, range-checked field parsers (every spec value is hostile until proven).
# --------------------------------------------------------------------------- #

def _resolve_dim(v, dims):
    """A dimension is either a symbolic name (node-resolved) or a positive int."""
    if isinstance(v, str):
        if v not in dims:
            raise ValueError("unknown symbolic dim %r (allowed: %s)"
                             % (v, ", ".join(sorted(dims))))
        return int(dims[v])
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError("dim must be a positive int or a symbolic name, got %r" % (v,))
    if v < 1 or v > _MAX_DIM:
        raise ValueError("dim out of range [1, %d]: %d" % (_MAX_DIM, v))
    return int(v)


def _unit_float(v, name, hi_open=True):
    ok = (not isinstance(v, bool)) and isinstance(v, (int, float)) and \
         (0.0 <= float(v) < 1.0 if hi_open else 0.0 <= float(v) <= 1.0)
    if not ok:
        bound = "[0, 1)" if hi_open else "[0, 1]"
        raise ValueError("%s must be a float in %s, got %r" % (name, bound, v))
    return float(v)


# --------------------------------------------------------------------------- #
# Op builders: (layer_spec, in_features, dims) -> (nn.Module, out_features).
# The running feature dim threads through the sequence. Each builder uses ONLY a
# locally-named stock constructor -- there is no name->class reflection anywhere.
# --------------------------------------------------------------------------- #

def _b_linear(s, d_in, dims):
    out_raw = s.get("out", "@out")
    out = _resolve_dim(out_raw, dims)
    if not isinstance(out_raw, str) and out > _MAX_WIDTH:
        raise ValueError("linear width %d exceeds cap %d" % (out, _MAX_WIDTH))
    bias = s.get("bias", True)
    if not isinstance(bias, bool):
        raise ValueError("linear 'bias' must be a bool, got %r" % (bias,))
    return nn.Linear(d_in, out, bias=bias), out


def _b_relu(s, d, dims):     return nn.ReLU(), d
def _b_gelu(s, d, dims):     return nn.GELU(), d
def _b_tanh(s, d, dims):     return nn.Tanh(), d
def _b_sigmoid(s, d, dims):  return nn.Sigmoid(), d
def _b_elu(s, d, dims):      return nn.ELU(), d
def _b_silu(s, d, dims):     return nn.SiLU(), d


def _b_leaky_relu(s, d, dims):
    return nn.LeakyReLU(_unit_float(s.get("negative_slope", 0.01),
                                    "leaky_relu negative_slope", hi_open=False)), d


def _b_dropout(s, d, dims):
    return nn.Dropout(_unit_float(s.get("p", 0.5), "dropout p")), d


def _b_layernorm(s, d, dims):
    # Normalizes over the feature dim (per-sample): DP-safe, affine params, and --
    # unlike BatchNorm -- no running-stat buffers (so assert_releasable still holds).
    return nn.LayerNorm(d), d


_OPS = {
    "linear": _b_linear,
    "relu": _b_relu, "gelu": _b_gelu, "tanh": _b_tanh, "sigmoid": _b_sigmoid,
    "elu": _b_elu, "silu": _b_silu, "leaky_relu": _b_leaky_relu,
    "dropout": _b_dropout, "layernorm": _b_layernorm,
}


def build_from_spec(spec, in_dim, out_dim, *, num_labels=None):
    """Build a genuinely-stock nn.Module from a declarative spec. No researcher code
    executes. ``in_dim`` (@in) and ``out_dim`` (@out) are node-decided -- the latter
    from the pinned loss -- so the researcher controls only the hidden structure."""
    if not isinstance(spec, dict):
        raise ValueError("spec must be a JSON object, got %s" % type(spec).__name__)
    if spec.get("kind", "sequential") != "sequential":
        raise ValueError("unsupported spec kind %r (only 'sequential')" % (spec.get("kind"),))
    layers = spec.get("layers")
    if not isinstance(layers, list) or not layers:
        raise ValueError("spec.layers must be a non-empty list")
    if len(layers) > _MAX_LAYERS:
        raise ValueError("spec has %d layers (cap %d)" % (len(layers), _MAX_LAYERS))

    dims = {"@in": int(in_dim), "@out": int(out_dim)}
    if num_labels is not None:
        dims["@nlabels"] = int(num_labels)

    modules, d = [], int(in_dim)
    for i, ls in enumerate(layers):
        if not isinstance(ls, dict):
            raise ValueError("layer %d must be an object, got %s" % (i, type(ls).__name__))
        op = ls.get("op")
        if not isinstance(op, str) or op not in _OPS:
            raise ValueError("layer %d has unknown op %r (allowed: %s)"
                             % (i, op, ", ".join(sorted(_OPS))))
        m, d = _OPS[op](ls, d, dims)
        modules.append(m)

    # The head must emit raw logits at the loss-determined width: the final layer is
    # a linear projection onto @out. A trailing activation (would double-apply with
    # the pinned loss) or a wrong width is rejected here, before any training.
    if layers[-1].get("op") != "linear":
        raise ValueError("the final layer must be 'linear' (the head emits logits)")
    if d != int(out_dim):
        raise ValueError("spec output width %d != required %d (end with a linear to @out)"
                         % (d, out_dim))

    return modules[0] if len(modules) == 1 else nn.Sequential(*modules)
