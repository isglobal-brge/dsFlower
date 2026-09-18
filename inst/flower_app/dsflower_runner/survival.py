"""Trusted subject-level survival likelihoods and public prediction semantics.

Every target row is one subject. Invalid rows contribute zero but remain in the
batch mean, sampler and accountant. Dispersion and interval edges are public.
"""

import base64
import json
import math

import numpy as np

SURVIVAL_LOSSES = frozenset((
    "aft_weibull_nll", "aft_lognormal_nll", "discrete_hazard_nll"))
MU_LIMIT = 10.0
DISPERSION_GRID = (0.5, 1.0, 2.0)
TARGET_COLUMNS = ("__survival_time", "__survival_event", "__survival_valid")


def validate_survival_config(config, loss_name=None):
    """Validate public domains before private data is opened; return plain pins."""
    if not isinstance(config, dict):
        raise ValueError("survival-config must be an object")
    hazard = loss_name == "discrete_hazard_nll" or (
        loss_name is None and "edges" in config)
    common = {"schema_version", "time_unit", "time_origin", "t_min", "horizon"}
    expected = common | ({"edges"} if hazard else {
        "time_scale", "distribution", "dispersion"})
    if set(config) != expected:
        raise ValueError("survival-config fields do not match its public schema")
    if (type(config["schema_version"]) is not int
            or config["schema_version"] != 1
            or config["time_unit"] != "days"
            or config["time_origin"] != "baseline"):
        raise ValueError("unsupported survival schema, time unit or origin")
    result = dict(config)
    for key in ("t_min", "horizon") + (() if hazard else ("time_scale",)):
        value = config[key]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or not 1e-6 <= value <= 1e6):
            raise ValueError("survival public times must be in [1e-6, 1e6]")
        result[key] = float(value)
    if result["t_min"] > result["horizon"]:
        raise ValueError("survival t_min exceeds horizon")
    if hazard:
        edges = config["edges"]
        if (not isinstance(edges, list) or not 2 <= len(edges) <= 65
                or any(isinstance(x, bool) or not isinstance(x, (int, float))
                       or not math.isfinite(x) for x in edges)
                or edges[0] != 0 or edges[-1] != result["horizon"]
                or any(b <= a for a, b in zip(edges, edges[1:]))):
            raise ValueError("hazard edges require 1 <= K <= 64 and 0=b0<...<bK=T")
        result["edges"] = [float(x) for x in edges]
    else:
        distribution = config["distribution"]
        dispersion = config["dispersion"]
        if (distribution not in ("weibull", "lognormal")
                or isinstance(dispersion, bool) or dispersion not in DISPERSION_GRID):
            raise ValueError("AFT distribution/dispersion is not on the public grid")
        if (loss_name is not None
                and loss_name != "aft_" + distribution + "_nll"):
            raise ValueError("AFT distribution does not match the pinned loss")
        result["dispersion"] = float(dispersion)
        if (distribution == "weibull" and dispersion * (
                math.log(result["horizon"] / result["time_scale"]) + MU_LIMIT) > 60):
            raise ValueError("AFT public exponential domain exceeds 60")
    if loss_name is not None and loss_name not in SURVIVAL_LOSSES:
        raise ValueError("survival loss is not on the trusted allowlist")
    return result


def config_from_run(cfg, loss_name=None):
    config = cfg.get("survival-config")
    if config is None:
        try:
            config = json.loads(base64.b64decode(
                cfg["survival-config-b64"], validate=True).decode("utf-8"))
        except (KeyError, TypeError, ValueError, UnicodeError) as exc:
            raise ValueError("missing or invalid survival-config-b64") from exc
    return validate_survival_config(config, loss_name)


def period_targets(times, events, valid, config):
    """Pack d[1:K],m[1:K],valid under the interval-end convention."""
    config = validate_survival_config(config, "discrete_hazard_nll")
    edges = np.asarray(config["edges"], dtype=np.float64)
    times = np.asarray(times, dtype=np.float64)
    events = np.asarray(events, dtype=np.float64)
    valid = np.asarray(valid, dtype=np.float64)
    k = len(edges) - 1
    index = np.clip(np.searchsorted(edges[1:], times, side="left"), 0, k-1)
    event = (events == 1) & (valid == 1)
    d = np.zeros((len(times), k), dtype=np.float64)
    d[np.arange(len(times)), index] = event
    completed = edges[1:][None, :] <= times[:, None]
    m = np.where(event[:, None], np.arange(k)[None, :] <= index[:, None],
                 completed) & (valid[:, None] == 1)
    return np.column_stack((d, m, valid)).astype(np.float32)


def loss_factory(loss_name, cfg):
    """Mean subject NLL, including the original-time event-density Jacobian."""
    config = config_from_run(cfg, loss_name)

    def loss(prediction, target):
        import torch
        import torch.nn.functional as functional
        if prediction.ndim != 2 or target.ndim != 2:
            raise ValueError("survival tensors require one leading subject axis")
        if prediction.shape[0] != target.shape[0]:
            raise ValueError("survival subject dimensions differ")
        # Preserve a differentiable zero on empty Poisson draws.
        if prediction.shape[0] == 0:
            return prediction.sum() * 0.0
        if loss_name == "discrete_hazard_nll":
            k = len(config["edges"]) - 1
            if prediction.shape[1] != k or target.shape[1] != 2*k + 1:
                raise ValueError("hazard tensors do not match the public K")
            terms = functional.binary_cross_entropy_with_logits(
                prediction, target[:, :k], reduction="none")
            return (target[:, -1] * (terms * target[:, k:2*k]).sum(1) / k).mean()
        if prediction.shape[1] != 1 or target.shape[1] != 3:
            raise ValueError("AFT requires scalar location and time/event/valid")
        mu = prediction[:, 0].double().clamp(-MU_LIMIT, MU_LIMIT)
        time, event, valid = target.double().unbind(1)
        log_u = torch.log(time / config["time_scale"])
        a = log_u - mu
        dispersion = config["dispersion"]
        if loss_name == "aft_weibull_nll":
            cumulative_hazard = torch.exp(dispersion * a)
            log_density = (math.log(dispersion) - mu
                           + (dispersion - 1.0) * a - cumulative_hazard)
            log_survival = -cumulative_hazard
        else:
            z = a / dispersion
            log_density = (-log_u - math.log(dispersion)
                           - 0.5 * math.log(2.0 * math.pi) - 0.5 * z.square())
            log_survival = torch.special.log_ndtr(-z)
        log_density = log_density - math.log(config["time_scale"])
        return (-valid * (event * log_density + (1.0-event) * log_survival)).mean()

    return loss


def survival_predictions(output, config, times=None):
    """Channel-B prediction on public times, with pinned ranking conventions."""
    import torch
    config = validate_survival_config(config)
    values = torch.as_tensor(output, dtype=torch.float64)
    if values.ndim == 1:
        values = values[:, None]
    requested = np.asarray(
        config.get("edges", [0.0, config["horizon"]]) if times is None else times,
        dtype=np.float64)
    if (requested.ndim != 1 or not np.all(np.isfinite(requested))
            or np.any(requested < 0) or np.any(requested > config["horizon"])):
        raise ValueError("prediction times must be finite and within [0,horizon]")
    if "edges" in config:
        edges = np.asarray(config["edges"], dtype=np.float64)
        if values.ndim != 2 or values.shape[1] != len(edges)-1:
            raise ValueError("hazard prediction width differs from K")
        at_edges = torch.cat((torch.ones((len(values), 1), dtype=values.dtype),
            torch.exp(torch.nn.functional.logsigmoid(-values).cumsum(1))), 1).numpy()
        indices = np.searchsorted(edges, requested, side="right") - 1
        survival = at_edges[:, indices]
        risk = -(at_edges[:, :-1] * np.diff(edges)).sum(1)
        crossed = at_edges <= 0.5
        median = np.where(crossed.any(1), edges[crossed.argmax(1)], np.inf)
    else:
        if values.ndim != 2 or values.shape[1] != 1:
            raise ValueError("AFT prediction requires one location")
        mu = values[:, 0].clamp(-MU_LIMIT, MU_LIMIT)
        log_u = torch.log(torch.as_tensor(requested) / config["time_scale"])
        a = log_u[None, :] - mu[:, None]
        if config["distribution"] == "weibull":
            survival = torch.exp(-torch.exp(config["dispersion"] * a)).numpy()
            median = (config["time_scale"] * torch.exp(mu)
                      * math.log(2.0) ** (1.0/config["dispersion"])).numpy()
        else:
            survival = torch.exp(torch.special.log_ndtr(
                -a/config["dispersion"])).numpy()
            median = (config["time_scale"] * torch.exp(mu)).numpy()
        risk = -mu.numpy()
    return {"times": requested, "survival": survival, "median": median, "risk": risk}
