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
    if loss_name in ("cross_entropy", "hinge"):
        return max(2, nc)                      # one logit per class (softmax-CE / margin-SVM)
    if loss_name == "multilabel_bce":
        return int(cfg["num-labels"])          # one independent logit per label
    if loss_name == "ordinal":
        return max(1, nc - 1)                  # K-1 cumulative-threshold logits (CORN)
    if loss_name in ("mse", "poisson_nll", "negbin_nll", "gamma_nll"):
        return 1                               # scalar regression / log-rate / log-mean
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


_MAX_CHANNELS = 4096
_MAX_SPATIAL = 4096


# --------------------------------------------------------------------------- #
# Op builders: (layer_spec, shape_in, dims) -> (nn.Module, shape_out). The state
# threaded through the sequence is a SHAPE tuple (excluding the batch dim):
# ``(features,)`` for flat tabular, ``(C, L)`` / ``(C, H, W)`` after a reshape into
# temporal / spatial form. EVERY op is per-sample (no cross-batch mixing) so the
# DP-SGD per-sample sensitivity bound holds: Opacus implements grad_sample for
# Conv1d/Conv2d, and reshape/pool/flatten are parameterless. Each builder uses ONLY
# a locally-named stock constructor -- no name->class reflection anywhere.
# --------------------------------------------------------------------------- #

def _prod(shape):
    p = 1
    for s in shape:
        p *= int(s)
    return p


def _pos_int(v, name, hi=_MAX_DIM):
    if isinstance(v, bool) or not isinstance(v, int) or v < 1 or v > hi:
        raise ValueError("%s must be an int in [1, %d], got %r" % (name, hi, v))
    return int(v)


def _conv_out(length, k, stride, pad, dilation):
    out = (length + 2 * pad - dilation * (k - 1) - 1) // stride + 1
    if out < 1:
        raise ValueError("a conv/pool op collapses a spatial dim to %d (<1); use a "
                         "smaller kernel/stride or more padding" % out)
    return out


def _require_flat(shape, op):
    if len(shape) != 1:
        raise ValueError("op %r needs a flat input but the running shape is %r "
                         "(add a 'flatten' first)" % (op, shape))


def _require_spatial(shape, op, ndim):
    if len(shape) != ndim + 1:
        raise ValueError("op %r needs a %dD spatial input (channels + %d dims) but the "
                         "running shape is %r (reshape first)" % (op, ndim, ndim, shape))


def _b_linear(s, shape, dims):
    _require_flat(shape, "linear")
    out_raw = s.get("out", "@out")
    out = _resolve_dim(out_raw, dims)
    if not isinstance(out_raw, str) and out > _MAX_WIDTH:
        raise ValueError("linear width %d exceeds cap %d" % (out, _MAX_WIDTH))
    bias = s.get("bias", True)
    if not isinstance(bias, bool):
        raise ValueError("linear 'bias' must be a bool, got %r" % (bias,))
    return nn.Linear(shape[0], out, bias=bias), (out,)


def _b_relu(s, shape, dims):     return nn.ReLU(), shape
def _b_gelu(s, shape, dims):     return nn.GELU(), shape
def _b_tanh(s, shape, dims):     return nn.Tanh(), shape
def _b_sigmoid(s, shape, dims):  return nn.Sigmoid(), shape
def _b_elu(s, shape, dims):      return nn.ELU(), shape
def _b_silu(s, shape, dims):     return nn.SiLU(), shape


def _b_leaky_relu(s, shape, dims):
    return nn.LeakyReLU(_unit_float(s.get("negative_slope", 0.01),
                                    "leaky_relu negative_slope", hi_open=False)), shape


def _b_dropout(s, shape, dims):
    return nn.Dropout(_unit_float(s.get("p", 0.5), "dropout p")), shape


def _b_layernorm(s, shape, dims):
    # Normalizes over the feature dim (per-sample): DP-safe, affine params, and --
    # unlike BatchNorm -- no running-stat buffers (so assert_releasable still holds).
    _require_flat(shape, "layernorm")
    return nn.LayerNorm(shape[0]), shape


def _b_reshape(s, shape, dims):
    # Reshape the flat per-sample vector into (C,L) or (C,H,W) for conv. Pure view,
    # per-sample, parameterless. The element count must be preserved exactly.
    raw = s.get("shape")
    if not isinstance(raw, list) or not (1 <= len(raw) <= 3):
        raise ValueError("reshape 'shape' must be a list of 1..3 positive ints, got %r" % (raw,))
    tgt = tuple(_pos_int(v, "reshape dim", hi=_MAX_SPATIAL) for v in raw)
    if _prod(tgt) != _prod(shape):
        raise ValueError("reshape to %r changes the element count (%d != %d)"
                         % (tgt, _prod(tgt), _prod(shape)))
    if len(tgt) >= 2 and tgt[0] > _MAX_CHANNELS:
        raise ValueError("reshape channel count %d exceeds cap %d" % (tgt[0], _MAX_CHANNELS))
    return nn.Unflatten(1, tgt), tgt


def _b_flatten(s, shape, dims):
    return nn.Flatten(), (_prod(shape),)


def _conv_hparams(s):
    k = _pos_int(s.get("kernel_size", 3), "kernel_size", hi=_MAX_SPATIAL)
    stride = _pos_int(s.get("stride", 1), "stride", hi=_MAX_SPATIAL)
    pad = s.get("padding", 0)
    if isinstance(pad, bool) or not isinstance(pad, int) or pad < 0 or pad > _MAX_SPATIAL:
        raise ValueError("padding must be an int in [0, %d], got %r" % (_MAX_SPATIAL, pad))
    dilation = _pos_int(s.get("dilation", 1), "dilation", hi=_MAX_SPATIAL)
    return k, stride, pad, dilation


def _b_conv1d(s, shape, dims):
    _require_spatial(shape, "conv1d", 1)
    c_in, length = shape
    c_out = _pos_int(s.get("out_channels", c_in), "out_channels", hi=_MAX_CHANNELS)
    k, stride, pad, dil = _conv_hparams(s)
    return (nn.Conv1d(c_in, c_out, k, stride=stride, padding=pad, dilation=dil),
            (c_out, _conv_out(length, k, stride, pad, dil)))


def _b_conv2d(s, shape, dims):
    _require_spatial(shape, "conv2d", 2)
    c_in, h, w = shape
    c_out = _pos_int(s.get("out_channels", c_in), "out_channels", hi=_MAX_CHANNELS)
    k, stride, pad, dil = _conv_hparams(s)
    return (nn.Conv2d(c_in, c_out, k, stride=stride, padding=pad, dilation=dil),
            (c_out, _conv_out(h, k, stride, pad, dil), _conv_out(w, k, stride, pad, dil)))


def _b_maxpool2d(s, shape, dims):
    _require_spatial(shape, "maxpool2d", 2)
    c, h, w = shape
    k = _pos_int(s.get("kernel_size", 2), "kernel_size", hi=_MAX_SPATIAL)
    stride = _pos_int(s.get("stride", k), "stride", hi=_MAX_SPATIAL)
    return (nn.MaxPool2d(k, stride=stride),
            (c, _conv_out(h, k, stride, 0, 1), _conv_out(w, k, stride, 0, 1)))


def _b_adaptiveavgpool2d(s, shape, dims):
    _require_spatial(shape, "adaptiveavgpool2d", 2)
    raw = s.get("output_size", [1, 1])
    if not isinstance(raw, list) or len(raw) != 2:
        raise ValueError("adaptiveavgpool2d 'output_size' must be a list [h, w], got %r" % (raw,))
    h, w = (_pos_int(v, "output_size", hi=_MAX_SPATIAL) for v in raw)
    return nn.AdaptiveAvgPool2d((h, w)), (shape[0], h, w)


_OPS = {
    "linear": _b_linear,
    "relu": _b_relu, "gelu": _b_gelu, "tanh": _b_tanh, "sigmoid": _b_sigmoid,
    "elu": _b_elu, "silu": _b_silu, "leaky_relu": _b_leaky_relu,
    "dropout": _b_dropout, "layernorm": _b_layernorm,
    "reshape": _b_reshape, "flatten": _b_flatten,
    "conv1d": _b_conv1d, "conv2d": _b_conv2d,
    "maxpool2d": _b_maxpool2d, "adaptiveavgpool2d": _b_adaptiveavgpool2d,
}


def build_from_spec(spec, in_dim, out_dim, *, num_labels=None):
    """Build a genuinely-stock nn.Module from a declarative spec. No researcher code
    executes. ``in_dim`` (@in) and ``out_dim`` (@out) are node-decided -- the latter
    from the pinned loss -- so the researcher controls only the hidden structure. The
    running per-sample SHAPE threads through (flat -> reshape -> conv/pool -> flatten
    -> linear head), and the final shape must equal ``(out_dim,)``."""
    if not isinstance(spec, dict):
        raise ValueError("spec must be a JSON object, got %s" % type(spec).__name__)
    kind = spec.get("kind", "sequential")
    if kind == "graph":
        return build_from_graph(spec, in_dim, out_dim, num_labels=num_labels)
    if kind != "sequential":
        raise ValueError("unsupported spec kind %r (only 'sequential' or 'graph')" % (kind,))
    layers = spec.get("layers")
    if not isinstance(layers, list) or not layers:
        raise ValueError("spec.layers must be a non-empty list")
    if len(layers) > _MAX_LAYERS:
        raise ValueError("spec has %d layers (cap %d)" % (len(layers), _MAX_LAYERS))

    dims = {"@in": int(in_dim), "@out": int(out_dim)}
    if num_labels is not None:
        dims["@nlabels"] = int(num_labels)

    modules, shape = [], (int(in_dim),)
    for i, ls in enumerate(layers):
        if not isinstance(ls, dict):
            raise ValueError("layer %d must be an object, got %s" % (i, type(ls).__name__))
        op = ls.get("op")
        if not isinstance(op, str) or op not in _OPS:
            raise ValueError("layer %d has unknown op %r (allowed: %s)"
                             % (i, op, ", ".join(sorted(_OPS))))
        m, shape = _OPS[op](ls, shape, dims)
        modules.append(m)

    # The head must emit raw logits at the loss-determined width: the final layer is
    # a linear projection onto @out. A trailing activation (would double-apply with
    # the pinned loss) or a wrong width is rejected here, before any training.
    if layers[-1].get("op") != "linear":
        raise ValueError("the final layer must be 'linear' (the head emits logits)")
    if shape != (int(out_dim),):
        raise ValueError("spec output shape %r != required (%d,) (end with a linear to @out)"
                         % (shape, out_dim))

    return modules[0] if len(modules) == 1 else nn.Sequential(*modules)


# --------------------------------------------------------------------------- #
# Typed computation GRAPH (DAG): named tensors, multi-input ops, fan-out -> skip /
# residual / concat. Strictly more expressive than the sequential form (covers
# ResNet / U-Net / DenseNet / multi-branch topologies) while staying DATA: the node
# builds ONE trusted GraphModule interpreter from allowlisted, per-sample-safe ops.
# Every op is samplewise or within-sample-reduce; the batch dim is NEVER reduced or
# mixed (concat/add operate on per-sample axes only). The researcher submits only op
# enums + attributes -- no class paths, no code. assert_stock_architecture admits the
# GraphModule class by exact identity (see dp_harness).
# --------------------------------------------------------------------------- #

class GraphModule(nn.Module):
    """Node-owned executor for a declarative DAG. The graph (a list of node dicts) and
    the output name are DATA attributes; all trainable state lives in the stock
    submodules in ``_mods``. The forward is a FIXED interpreter over allowlisted ops --
    never researcher code -- with no hooks, no instance method overrides and no
    non-stock parameters."""

    def __init__(self, graph, output_name, mods):
        super().__init__()
        self._dsflower_graph = graph              # data (list of {name, op, inputs, ...})
        self._dsflower_output = str(output_name)  # data
        self._mods = nn.ModuleDict(mods)          # stock op modules (params live here)

    def forward(self, x):
        env = {"@in": x}
        for nd in self._dsflower_graph:
            ins = [env[t] for t in nd["inputs"]]
            mod = nd.get("module")
            if mod is not None:                    # single-input param/stock op
                env[nd["name"]] = self._mods[mod](ins[0])
                continue
            op = nd["op"]
            if op == "add":
                t = ins[0]
                for o in ins[1:]:
                    t = t + o
                env[nd["name"]] = t
            elif op == "mul":
                t = ins[0]
                for o in ins[1:]:
                    t = t * o
                env[nd["name"]] = t
            elif op == "concat":
                env[nd["name"]] = torch.cat(ins, dim=nd["axis"] + 1)   # +1: never the batch dim
            else:
                raise ValueError("graph forward: unsupported functional op %r" % (op,))
        return env[self._dsflower_output]


def _g_add(nd, in_shapes):
    if len(in_shapes) < 2 or any(s != in_shapes[0] for s in in_shapes):
        raise ValueError("add needs >=2 inputs of identical per-sample shape, got %r" % (in_shapes,))
    return in_shapes[0]


def _g_mul(nd, in_shapes):
    if len(in_shapes) < 2 or any(s != in_shapes[0] for s in in_shapes):
        raise ValueError("mul needs >=2 inputs of identical per-sample shape, got %r" % (in_shapes,))
    return in_shapes[0]


def _g_concat(nd, in_shapes):
    axis = nd.get("axis", 0)                       # per-sample axis (0 = first non-batch dim)
    if isinstance(axis, bool) or not isinstance(axis, int) or axis < 0:
        raise ValueError("concat 'axis' must be a non-negative per-sample axis, got %r" % (axis,))
    if len(in_shapes) < 2:
        raise ValueError("concat needs >=2 inputs")
    rank = len(in_shapes[0])
    if axis >= rank or any(len(s) != rank for s in in_shapes):
        raise ValueError("concat: equal-rank inputs and axis in range required, got %r axis=%d"
                         % (in_shapes, axis))
    for j in range(rank):
        if j != axis and any(s[j] != in_shapes[0][j] for s in in_shapes):
            raise ValueError("concat: shapes must match except on axis %d, got %r" % (axis, in_shapes))
    out = list(in_shapes[0]); out[axis] = sum(s[axis] for s in in_shapes)
    return tuple(out)


# Multi-input / functional graph ops: (node, [in_shapes]) -> out_shape. All per-sample
# (operate on per-sample axes; the batch dim is never touched).
_GRAPH_OPS = {"add": _g_add, "mul": _g_mul, "concat": _g_concat}


def build_from_graph(spec, in_dim, out_dim, *, num_labels=None):
    """Build a node-owned GraphModule from a declarative DAG. Nodes must be listed in
    TOPOLOGICAL order (every input already defined). Single-input ops come from the
    sequential allowlist ``_OPS``; multi-input/functional ops from ``_GRAPH_OPS``. The
    output tensor must be a final ``linear`` projection to ``@out`` (raw logits at the
    loss-decided width)."""
    nodes = spec.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("graph spec needs a non-empty 'nodes' list")
    if len(nodes) > _MAX_LAYERS:
        raise ValueError("graph has %d nodes (cap %d)" % (len(nodes), _MAX_LAYERS))
    output_name = spec.get("output")
    if not isinstance(output_name, str) or not output_name:
        raise ValueError("graph spec needs an 'output' tensor name")

    dims = {"@in": int(in_dim), "@out": int(out_dim)}
    if num_labels is not None:
        dims["@nlabels"] = int(num_labels)

    shapes = {"@in": (int(in_dim),)}
    graph, mods = [], {}
    for i, nd in enumerate(nodes):
        if not isinstance(nd, dict):
            raise ValueError("graph node %d must be an object" % i)
        name, op, ins = nd.get("name"), nd.get("op"), nd.get("in", [])
        if not isinstance(name, str) or not name or name in shapes:
            raise ValueError("graph node %d needs a unique non-empty 'name' (got %r)" % (i, name))
        if not isinstance(ins, list) or not ins or any(t not in shapes for t in ins):
            raise ValueError("graph node %r has an undefined/forward input (list nodes in "
                             "topological order): %r" % (name, ins))
        if op in _OPS:
            if len(ins) != 1:
                raise ValueError("op %r (node %r) takes exactly 1 input, got %d" % (op, name, len(ins)))
            m, out_shape = _OPS[op](nd, shapes[ins[0]], dims)
            mods[name] = m
            graph.append({"name": name, "op": op, "inputs": ins, "module": name})
        elif op in _GRAPH_OPS:
            out_shape = _GRAPH_OPS[op](nd, [shapes[t] for t in ins])
            graph.append({"name": name, "op": op, "inputs": ins, "axis": int(nd.get("axis", 0))})
        else:
            raise ValueError("graph node %r has unknown op %r (allowed: %s)"
                             % (name, op, ", ".join(sorted(list(_OPS) + list(_GRAPH_OPS)))))
        shapes[name] = out_shape

    if output_name not in shapes:
        raise ValueError("graph 'output' %r is not produced by any node" % (output_name,))
    if shapes[output_name] != (int(out_dim),):
        raise ValueError("graph output shape %r != required (%d,) (end with a linear to @out)"
                         % (shapes[output_name], out_dim))
    if nodes[-1].get("name") != output_name or nodes[-1].get("op") != "linear":
        raise ValueError("the final node must be the 'linear' projection to @out (emits raw logits)")
    return GraphModule(graph, output_name, mods)
