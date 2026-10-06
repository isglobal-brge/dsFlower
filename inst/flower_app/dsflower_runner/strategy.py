"""Public FedProx contract and post-processing of already-private updates."""
import math

MAX_FEDPROX_MU = 1.0
HOOK_PROX_STEP_SIZE = 1.0
NONE = {"local_rule": "none", "mu": 0.0, "eta_rule": None}
_FIELDS = {
    "fedavg": set(), "fedprox": {"strategy-mu"},
    "fedadam": {"strategy-eta", "strategy-eta-l", "strategy-beta-1", "strategy-beta-2", "strategy-tau"},
    "fedyogi": {"strategy-eta", "strategy-eta-l", "strategy-beta-1", "strategy-beta-2", "strategy-tau"},
    "fedadagrad": {"strategy-eta", "strategy-eta-l", "strategy-tau"},
    "fedavgm": {"strategy-server-learning-rate", "strategy-server-momentum"},
}


def canonical_local_strategy(config, track="neural"):
    name = config.get("strategy", "fedavg")
    if not isinstance(name, str):
        raise ValueError("strategy must be a public name")
    name = name.lower()
    if name == "prox": name = "fedprox"
    if name not in _FIELDS:
        raise ValueError("unsupported aggregation strategy")
    present = {key for key in config if str(key).startswith("strategy-")}
    if present - _FIELDS[name]:
        raise ValueError("unknown or inapplicable strategy fields")
    if track == "egress" and name not in ("fedavg", "fedprox"):
        raise ValueError("Hook training supports only FedAvg or FedProx")
    if name != "fedprox":
        return dict(NONE)
    # Reject the raw unsupported family before normalizing zero.
    if track not in ("neural", "egress"):
        raise ValueError("FedProx is unsupported for trees, association and validation")
    mu = config.get("strategy-mu")
    if type(mu) not in (int, float) or not math.isfinite(mu) or not 0 <= mu <= MAX_FEDPROX_MU:
        raise ValueError("FedProx mu must be one finite numeric value in [0, 1]")
    if mu == 0:
        return dict(NONE)
    return {"local_rule": "fedprox-gated-update" if track == "egress" else "fedprox-step",
            "mu": float(mu), "eta_rule": "unit-step" if track == "egress" else "scheduled-optimizer-lr"}


def validate_prox_horizon(pins):
    strategy = pins.get("strategy", NONE)
    mu = strategy["mu"]
    if not mu or strategy["local_rule"] != "fedprox-step":
        return
    base = float(pins["learning_rate"])
    schedule = pins["scheduler"]
    last = int(pins["num_rounds"]) * int(pins["local_epochs"]) - 1
    if schedule["name"] in ("step", "exponential"):
        exponent = last // int(schedule["step_size"]) if schedule["name"] == "step" else last
        try:
            largest = max(base, base * math.pow(float(schedule["gamma"]), exponent))
        except OverflowError:
            largest = math.inf
    elif schedule["name"] == "cosine":
        largest = max(base, float(schedule["min_lr"]))
    else:
        largest = base
    if not math.isfinite(largest) or largest * mu > 1:
        raise ValueError("FedProx requires scheduled learning_rate * mu <= 1 for every step")


def apply_neural_prox(optimizer, reference, strategy):
    """Apply after the DP optimizer and L1 prox; reference is fixed round input."""
    mu = strategy["mu"]
    if not mu:
        return
    import torch
    with torch.no_grad():
        seen = set()
        for group in optimizer.param_groups:
            eta = float(group["lr"])
            if not math.isfinite(eta) or eta < 0 or eta * mu > 1:
                raise RuntimeError("FedProx step violates public stability bound")
            for parameter in group["params"]:
                key = id(parameter)
                if key not in reference or key in seen:
                    raise RuntimeError("FedProx parameter reference changed")
                seen.add(key)
                parameter.add_(parameter - reference[key], alpha=-eta * mu)
        if seen != set(reference):
            raise RuntimeError("FedProx parameter reference is incomplete")


def apply_gated_prox(arrays, incoming, strategy):
    """Hook eta=1 relaxation after its complete DP gate, before cache commit."""
    mu = strategy["mu"]
    if not mu:
        return arrays
    import numpy as np
    if len(arrays) != len(incoming):
        raise RuntimeError("Hook FedProx output geometry changed")
    result = []
    for value, anchor in zip(arrays, incoming):
        value, anchor = np.asarray(value), np.asarray(anchor)
        if value.shape != anchor.shape:
            raise RuntimeError("Hook FedProx output geometry changed")
        result.append((value - mu * (value - anchor)).astype(value.dtype, copy=False))
    return result
