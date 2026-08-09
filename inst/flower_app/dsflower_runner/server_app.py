"""dsFlower unified ServerApp (researcher side) — new Message API.

UNTRUSTED by design: this runs on the researcher's machine. It performs NO DP
work and never sees raw per-node data — only already-private node releases, which
it post-processes. Dispatch is on the run config's ``dp-track`` (the node
re-checks its OWN manifest-pinned track, so a mismatch fails closed node-side):

  * neural / egress — FedAvg over the already-DP client updates (the mean of
    locally-private updates is post-processing, so the per-node guarantee carries
    to the aggregate; no Secure Aggregation needed).
  * validation — sum one fixed-layout DP sufficient-statistic vector per node
    and save only pooled metrics derived by post-processing.

Final artifacts go to results-dir for the R relay's watchdog: a native model,
a bounded portable weight record and history (neural/egress), or pooled DP
metrics only (validation).
"""

import json
import math
import os
import shutil
import tempfile

import numpy as np
import torch

from flwr.serverapp import Grid, ServerApp
from flwr.serverapp.strategy import (
    FedAvg, FedAdam, FedAdagrad, FedYogi, FedAvgM)
from flwr.common import (ArrayRecord, ConfigRecord, Context, Message,
                         MetricRecord, RecordDict)

from .params import get_torch_params, set_torch_params

app = ServerApp()

# JSON is only the small-model interchange format used by the R client. Native
# model.pt/model.npz remains authoritative for larger models. The conservative
# per-element estimate prevents list conversion from multiplying memory use and
# keeps the generated JSON below the client's default 50 MiB read ceiling.
_PORTABLE_JSON_MAX_BYTES = 50 * 1024**2
_PORTABLE_JSON_BYTES_PER_ELEMENT = 32


def _save_portable_arrays(results_dir, arrays, round_number):
    arrays = [np.asarray(value) for value in arrays]
    if any(not np.issubdtype(value.dtype, np.number)
           or not bool(np.all(np.isfinite(value))) for value in arrays):
        raise RuntimeError("aggregated model contains non-finite parameters")

    estimated_bytes = sum(
        int(value.size) * _PORTABLE_JSON_BYTES_PER_ELEMENT
        + int(value.ndim) * 24 + 64
        for value in arrays
    )
    model_path = os.path.join(results_dir, "global_model.json")
    skipped_path = os.path.join(results_dir, "global_model.skipped.json")
    if estimated_bytes > _PORTABLE_JSON_MAX_BYTES:
        if os.path.exists(model_path):
            os.unlink(model_path)
        with open(skipped_path, "w", encoding="utf-8") as handle:
            json.dump({"reason": "weights_exceed_json_limit"}, handle)
        return

    payload = {str(i): value.tolist() for i, value in enumerate(arrays)}
    payload["__shapes__"] = [list(value.shape) for value in arrays]
    payload["__round__"] = int(round_number)
    temporary = model_path + ".tmp"
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, allow_nan=False, separators=(",", ":"))
        os.replace(temporary, model_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    if os.path.exists(skipped_path):
        os.unlink(skipped_path)


# --------------------------------------------------------------------------- #
# Initial model (neural) — node-built from the researcher's spec (DATA, no code).
# --------------------------------------------------------------------------- #

def _build_initial_model(cfg):
    """Seed the global model's array SHAPES from the researcher's declarative spec
    (DATA, node-built by model_spec). Researcher-side + untrusted: random init only;
    the nodes rebuild from the same spec and enforce all DP + hardening. No
    researcher code is imported here."""
    try:
        import model_spec
    except ImportError:
        from . import model_spec
    if str(cfg.get("data-kind", "")).lower() == "image":
        from . import vision
        backbone = vision.normalize_backbone(cfg.get("backbone", cfg.get("model", "resnet18")))
        in_dim = int(vision.feature_dim_for(backbone))
    else:
        in_dim = int(cfg.get("num-features", 0))
        if in_dim <= 0:
            raise ValueError("num-features must be set in the run config "
                             "(the researcher passes len(feature_columns)).")
    spec = model_spec.read_spec(cfg)
    loss_name = str(cfg.get("loss-name", "bce_logits"))
    out_dim = model_spec.output_width(loss_name, cfg)
    num_labels = int(cfg["num-labels"]) if cfg.get("num-labels") is not None else None
    model = model_spec.build_from_spec(spec, in_dim=in_dim, out_dim=out_dim,
                                       num_labels=num_labels)
    if not isinstance(model, torch.nn.Module):
        raise ValueError("build_from_spec must return a torch.nn.Module")
    return model


# --------------------------------------------------------------------------- #
# Validation — one strict pooled sum of node-private sufficient statistics.
# --------------------------------------------------------------------------- #

def _run_validation(grid, cfg):
    import time
    from . import validation

    layout = validation.layout_from_config(dict(cfg))
    public_arrays = validation.public_model_arrays(dict(cfg))
    expected = int(cfg.get("min-train-nodes", 2))
    timeout = float(cfg.get("round-timeout", 600))
    node_ids, waited = [], 0.0
    while waited < timeout:
        node_ids = list(grid.get_node_ids())
        if len(node_ids) >= expected:
            break
        time.sleep(2.0)
        waited += 2.0
    if len(node_ids) != expected:
        return None, expected, False
    content = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=public_arrays),
        "config": ConfigRecord({"server-round": 1}),
    })
    messages = [Message(
        content=content, message_type="train", dst_node_id=node_id,
        group_id="dsflower-validation") for node_id in node_ids]
    try:
        replies = list(grid.send_and_receive(
            messages, timeout=float(cfg.get("round-timeout", 3600))))
    except Exception:
        return None, expected, False
    try:
        sources = [int(reply.metadata.src_node_id) for reply in replies]
    except Exception:
        return None, expected, False
    if (len(sources) != expected or len(set(sources)) != expected
            or set(sources) != {int(value) for value in node_ids}):
        return None, expected, False
    vectors = []
    for reply in replies:
        if reply.has_error():
            continue
        try:
            metrics = reply.content["metrics"]
            if (int(metrics.get(
                        "public-preflight-unavailable", 0)) == 1
                    or int(metrics.get(
                        "execution-unavailable", 0)) == 1):
                continue
            arrays = reply.content["arrays"].to_numpy_ndarrays()
            if len(arrays) != 1:
                continue
            vector = np.asarray(arrays[0], dtype=np.float64)
            if (vector.shape != (int(layout["size"]),)
                    or not bool(np.all(np.isfinite(vector)))):
                continue
            vectors.append(vector)
        except Exception:
            continue
    if len(replies) != expected or len(vectors) != expected:
        return None, expected, False
    stacked = np.stack(vectors, axis=0).astype(np.float64, copy=False)
    scale = np.max(np.abs(stacked), axis=0)
    normalized = np.divide(
        stacked, scale, out=np.zeros_like(stacked), where=scale > 0.0)
    wide = (np.sum(normalized.astype(np.longdouble), axis=0,
                   dtype=np.longdouble)
            * scale.astype(np.longdouble))
    limit = np.longdouble(np.finfo(np.float64).max)
    pooled = np.asarray(np.clip(wide, -limit, limit), dtype=np.float64)
    bounds = (validation.target_bounds_from_config(dict(cfg))
              if layout["task"] in ("regression", "count") else None)
    metrics = validation.validation_metrics(
        pooled, layout, target_bounds=bounds)
    return metrics, expected, True


def _pool_private_vectors(vectors, size):
    if not vectors:
        raise RuntimeError("no private validation vectors are available")
    checked = []
    for value in vectors:
        vector = np.asarray(value, dtype=np.float64)
        if vector.shape != (int(size),) or not bool(np.all(np.isfinite(vector))):
            raise RuntimeError("private validation vector has invalid geometry")
        checked.append(vector)
    stacked = np.stack(checked, axis=0).astype(np.float64, copy=False)
    scale = np.max(np.abs(stacked), axis=0)
    normalized = np.divide(
        stacked, scale, out=np.zeros_like(stacked), where=scale > 0.0)
    wide = (np.sum(normalized.astype(np.longdouble), axis=0,
                   dtype=np.longdouble)
            * scale.astype(np.longdouble))
    limit = np.longdouble(np.finfo(np.float64).max)
    pooled = np.asarray(np.clip(wide, -limit, limit), dtype=np.float64)
    if not bool(np.all(np.isfinite(pooled))):
        raise RuntimeError("pooled private validation vector overflowed")
    return pooled


def _run_holdout(grid, cfg, final_arrays, training_roster):
    """Evaluate one final aggregate; no per-node value is persisted or returned."""
    import time
    from . import validation

    layout = validation.holdout_layout_from_config(dict(cfg))
    expected = int(cfg.get("min-train-nodes", 2))
    timeout = float(cfg.get("round-timeout", 3600))
    node_ids = list(grid.get_node_ids())
    try:
        pinned_roster = {int(value) for value in training_roster}
        current_roster = {int(value) for value in node_ids}
    except Exception as exc:
        raise RuntimeError("holdout federation roster is invalid") from exc
    if (len(node_ids) != expected or len(current_roster) != expected
            or current_roster != pinned_roster):
        raise RuntimeError("holdout federation roster changed after training")
    content = RecordDict({
        "arrays": final_arrays,
        "config": ConfigRecord({
            "server-round": int(cfg.get("num-server-rounds", 1)),
            "dsflower-operation": "holdout-evaluate",
        }),
    })
    messages = [Message(
        content=content, message_type="train", dst_node_id=node_id,
        group_id="dsflower-holdout-v1") for node_id in node_ids]
    started = time.monotonic()
    replies = list(grid.send_and_receive(messages, timeout=timeout))
    if time.monotonic() - started > timeout + 1.0:
        raise RuntimeError("holdout evaluation exceeded its public timeout")
    try:
        sources = [int(reply.metadata.src_node_id) for reply in replies]
    except Exception as exc:
        raise RuntimeError("holdout replies have no verifiable node identity") from exc
    if (len(replies) != expected or len(set(sources)) != expected
            or set(sources) != {int(value) for value in node_ids}):
        raise RuntimeError("holdout replies do not match the training roster")
    replies = [reply for _, reply in sorted(
        zip(sources, replies), key=lambda item: item[0])]
    vectors = []
    for reply in replies:
        if reply.has_error():
            raise RuntimeError("one or more holdout releases are unavailable")
        metrics = reply.content.get("metrics")
        if (not isinstance(metrics, MetricRecord)
                or int(metrics.get("public-preflight-unavailable", 0)) == 1
                or int(metrics.get("execution-unavailable", 0)) == 1):
            raise RuntimeError("one or more holdout releases are unavailable")
        arrays = reply.content["arrays"].to_numpy_ndarrays()
        if len(arrays) != 1:
            raise RuntimeError("holdout release has invalid geometry")
        vectors.append(arrays[0])
    pooled = _pool_private_vectors(vectors, layout["size"])
    bounds = (validation.holdout_target_bounds_from_config(dict(cfg))
              if layout["task"] in ("regression", "count") else None)
    return validation.validation_metrics(
        pooled, layout, target_bounds=bounds)


def _save_validation(cfg, metrics, n_nodes, available):
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)
    history_path = os.path.join(results_dir, "history.json")
    with open(history_path, "w", encoding="utf-8") as handle:
        json.dump([{"round": 1, "available": bool(available)}], handle,
                  allow_nan=False, separators=(",", ":"))
    payload = {
        "pooled_only": True,
        "privacy": "node-dp-pooled-postprocessing",
        "task": str(cfg.get("validation-task", "")),
        "n_nodes": int(n_nodes),
        "available": bool(available),
    }
    if available:
        payload["metrics"] = metrics
    final = os.path.join(results_dir, "validation.json")
    temporary = final + ".tmp"
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, allow_nan=False,
                      separators=(",", ":"))
        os.replace(temporary, final)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


# --------------------------------------------------------------------------- #
# Neural / egress — FedAvg over already-private updates.
# --------------------------------------------------------------------------- #

def _initial_arrays(cfg, track):
    if track == "egress":
        from . import tier2_lib
        public_cfg = tier2_lib.public_hook_config(dict(cfg), round_index=0)
        user_mod = tier2_lib.load_user_module(str(cfg["user-module"]))
        n = int(cfg.get("num-features", 0))
        arrays = [np.asarray(a, dtype=np.float64)
                  for a in user_mod.initial_arrays(public_cfg, n)]
        return None, ArrayRecord(numpy_ndarrays=arrays)
    model = _build_initial_model(cfg)
    return model, ArrayRecord(numpy_ndarrays=get_torch_params(model))


# Selectable server-side aggregation. Every strategy here runs ONLY on the
# already-DP client updates, on the researcher's SuperLink -- so by the DP
# post-processing theorem the per-node (epsilon, delta) guarantee is unchanged.
# The strategy is pure aggregation, never a privacy knob. FedProx is excluded
# (it needs a node-side proximal term).


class _RequireCompleteTrain:
    """Reject a round unless the configured federation replies successfully."""

    def __init__(self, *, expected_train_nodes,
                 require_hook_executed=False, stable_roster=False, **kwargs):
        self.expected_train_nodes = int(expected_train_nodes)
        self.require_hook_executed = bool(require_hook_executed)
        self.stable_roster = bool(stable_roster)
        self.training_roster = None
        self.available_rounds = set()
        self.unavailable_rounds = set()
        self._last_available_arrays = None
        self._round_input_arrays = {}
        self._round_expected_nodes = {}
        super().__init__(**kwargs)

    def configure_train(self, server_round, arrays, config, grid):
        # Use one immutable-per-round copy rather than the ConfigRecord reused by
        # Flower's strategy loop.  The trusted ClientApp accepts only this exact
        # public coordinate and independently checks it against its local manifest.
        round_config = ConfigRecord(dict(config))
        round_config["server-round"] = int(server_round)
        messages = list(super().configure_train(
            server_round, arrays, round_config, grid))
        for message in messages:
            sent_config = message.content.get("config")
            if (not isinstance(sent_config, ConfigRecord)
                    or sent_config.get("server-round") != int(server_round)):
                raise RuntimeError("Flower did not pin the canonical server round")
        connected = list(grid.get_node_ids())
        destinations = [message.metadata.dst_node_id for message in messages]
        if (len(connected) != self.expected_train_nodes
                or len(messages) != self.expected_train_nodes
                or set(destinations) != set(connected)):
            raise RuntimeError(
                "%d node(s) are connected in round %d, expected exactly %d; "
                "refusing an unexpected federation roster."
                % (len(connected), server_round, self.expected_train_nodes))
        connected_roster = frozenset(int(value) for value in connected)
        if self.stable_roster:
            if self.training_roster is None:
                self.training_roster = connected_roster
            elif connected_roster != self.training_roster:
                raise RuntimeError(
                    "atomic holdout federation roster changed between rounds")
        self._round_input_arrays[int(server_round)] = arrays
        self._round_expected_nodes[int(server_round)] = set(destinations)
        return messages

    def aggregate_train(self, server_round, replies):
        replies = list(replies)
        valid_count = sum(not reply.has_error() for reply in replies)
        if (len(replies) != self.expected_train_nodes
                or valid_count != self.expected_train_nodes):
            self._check_and_log_replies(replies, is_train=True)
            raise RuntimeError(
                "%d of %d configured node(s) returned a valid update in round %d; "
                "refusing to aggregate a degraded federation."
                % (valid_count, self.expected_train_nodes, server_round))
        try:
            sources = [int(reply.metadata.src_node_id) for reply in replies]
        except Exception as exc:
            raise RuntimeError("Training replies have no verifiable node identity") from exc
        expected_nodes = self._round_expected_nodes.get(int(server_round))
        if (len(set(sources)) != self.expected_train_nodes
                or (expected_nodes is not None and set(sources) != expected_nodes)):
            raise RuntimeError(
                "Training replies do not match the configured federation roster.")
        unavailable = []
        for reply in replies:
            try:
                metrics = reply.content["metrics"]
                unavailable.append(
                    int(metrics.get(
                        "public-preflight-unavailable", 0)) == 1
                    or int(metrics.get(
                        "execution-unavailable", 0)) == 1)
            except Exception:
                unavailable.append(False)
        if any(unavailable):
            self.unavailable_rounds.add(int(server_round))
            baseline = self._round_input_arrays.pop(
                int(server_round), self._last_available_arrays)
            self._round_expected_nodes.pop(int(server_round), None)
            if baseline is None:
                raise RuntimeError(
                    "No public round input is available for an unavailable "
                    "release; refusing to expose a client fallback.")
            return baseline, MetricRecord({"available": 0})
        if self.require_hook_executed:
            ready = []
            for reply in replies:
                try:
                    ready.append(int(reply.content["metrics"].get(
                        "hook-executed", 0)) == 1)
                except Exception:
                    ready.append(False)
            if not all(ready):
                raise RuntimeError(
                    "Hook execution is not available on every configured node; "
                    "refusing to report an unchanged model as trained.")
        arrays, metrics = super().aggregate_train(server_round, replies)
        self._round_input_arrays.pop(int(server_round), None)
        self._round_expected_nodes.pop(int(server_round), None)
        if arrays is not None:
            self.available_rounds.add(int(server_round))
            self._last_available_arrays = arrays
        return arrays, metrics


class _StrictFedAvg(_RequireCompleteTrain, FedAvg):
    pass


class _StrictFedAdam(_RequireCompleteTrain, FedAdam):
    pass


class _StrictFedAdagrad(_RequireCompleteTrain, FedAdagrad):
    pass


class _StrictFedYogi(_RequireCompleteTrain, FedYogi):
    pass


class _StrictFedAvgM(_RequireCompleteTrain, FedAvgM):
    pass


_STRATEGIES = {
    "fedavg": _StrictFedAvg, "fedadam": _StrictFedAdam,
    "fedadagrad": _StrictFedAdagrad, "fedyogi": _StrictFedYogi,
    "fedavgm": _StrictFedAvgM,
}


def _build_strategy(cfg, min_nodes, track=None, stable_roster=False):
    name = str(cfg.get("strategy", "fedavg")).lower()
    if name not in _STRATEGIES:
        raise ValueError(f"Unsupported aggregation strategy: {name}")
    common = dict(
        fraction_train=1.0, fraction_evaluate=0.0,
        min_train_nodes=min_nodes, min_evaluate_nodes=0,
        min_available_nodes=min_nodes, weighted_by_key="num-examples")

    def number(key, default, *, zero=False, unit=False):
        value = float(cfg.get(key, default))
        if (not math.isfinite(value) or (value < 0.0 if zero else value <= 0.0)
                or (unit and value >= 1.0)):
            raise ValueError(f"Invalid public strategy parameter: {key}")
        return value

    if name in ("fedadam", "fedyogi"):
        defaults = ((0.1, 0.1) if name == "fedadam" else (0.01, 0.0316))
        specific = dict(
            eta=number("strategy-eta", defaults[0]),
            eta_l=number("strategy-eta-l", defaults[1]),
            beta_1=number("strategy-beta-1", 0.9, zero=True, unit=True),
            beta_2=number("strategy-beta-2", 0.99, zero=True, unit=True),
            tau=number("strategy-tau", 1e-3))
    elif name == "fedadagrad":
        specific = dict(
            eta=number("strategy-eta", 0.1),
            eta_l=number("strategy-eta-l", 0.1),
            tau=number("strategy-tau", 1e-3))
    elif name == "fedavgm":
        specific = dict(
            server_learning_rate=number(
                "strategy-server-learning-rate", 1.0),
            server_momentum=number(
                "strategy-server-momentum", 0.0, zero=True, unit=True))
    else:
        specific = {}
    return _STRATEGIES[name](
        expected_train_nodes=min_nodes,
        require_hook_executed=(track == "egress"),
        stable_roster=stable_roster, **common, **specific)


def _run_fedavg(grid, cfg, track):
    num_rounds = int(cfg.get("num-server-rounds", 1))
    min_nodes = int(cfg.get("min-train-nodes", 2))
    has_holdout = cfg.get("resampling-contract-sha256") is not None
    model, initial = _initial_arrays(cfg, track)
    strategy = _build_strategy(
        cfg, min_nodes, track=track, stable_roster=has_holdout)
    result = strategy.start(grid=grid, initial_arrays=initial, num_rounds=num_rounds)
    holdout_metrics = None
    if has_holdout:
        if track != "neural" or str(cfg.get("data-kind", "")).lower() != "tabular":
            raise RuntimeError("atomic holdout is implemented only for tabular neural runs")
        if strategy.available_rounds != set(range(1, num_rounds + 1)):
            raise RuntimeError("atomic holdout requires every training round")
        holdout_metrics = _run_holdout(
            grid, cfg, result.arrays, strategy.training_roster)
    _save_results(
        cfg, model, result, available_rounds=strategy.available_rounds,
        holdout_metrics=holdout_metrics)


def _save_results(cfg, model, result, available_rounds=None,
                  holdout_metrics=None):
    results_dir = cfg.get("results-dir")
    if not results_dir:
        return
    os.makedirs(results_dir, exist_ok=True)
    if holdout_metrics is not None:
        transaction = tempfile.mkdtemp(prefix=".holdout-", dir=results_dir)
        published = []
        try:
            nested = dict(cfg)
            nested["results-dir"] = transaction
            _save_results(
                nested, model, result, available_rounds=available_rounds)
            _save_holdout(nested, holdout_metrics)
            names = sorted(os.listdir(transaction))
            if "history.json" not in names or "holdout.json" not in names:
                raise RuntimeError("atomic holdout transaction is incomplete")
            for name in [value for value in names if value != "history.json"]:
                destination = os.path.join(results_dir, name)
                if os.path.exists(destination):
                    raise RuntimeError("atomic holdout output already exists")
                os.replace(os.path.join(transaction, name), destination)
                published.append(destination)
            history = os.path.join(results_dir, "history.json")
            if os.path.exists(history):
                raise RuntimeError("atomic holdout output already exists")
            os.replace(os.path.join(transaction, "history.json"), history)
            published.append(history)
        except Exception:
            for path in published:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            raise
        finally:
            shutil.rmtree(transaction, ignore_errors=True)
        return
    final_arrays = result.arrays.to_numpy_ndarrays()
    if not final_arrays:
        raise RuntimeError(
            "No client updates were aggregated (all ClientApps failed); nothing to "
            "save. Check the node-side ClientApp logs.")
    num_rounds = int(cfg.get("num-server-rounds", 1))
    if available_rounds is None:
        available_rounds = set(range(1, num_rounds + 1))
    else:
        available_rounds = {int(value) for value in available_rounds}
    if available_rounds:
        if model is not None:
            set_torch_params(model, final_arrays)
            torch.save(model.state_dict(), os.path.join(results_dir, "model.pt"))
        else:  # egress: no torch model, save raw arrays
            np.savez(os.path.join(results_dir, "model.npz"), *final_arrays)
        _save_portable_arrays(results_dir, final_arrays, num_rounds)

    per_round = dict(result.train_metrics_clientapp or {})
    history = []
    for rnd in range(1, num_rounds + 1):
        row = {"round": rnd, "n_failures": 0,
               "available": rnd in available_rounds}
        mrec = per_round.get(rnd)
        if mrec is not None and "num-examples" in mrec:
            row["n_examples"] = mrec["num-examples"]
        history.append(row)
    with open(os.path.join(results_dir, "history.json"), "w") as f:
        json.dump(history, f)


def _save_holdout(cfg, metrics):
    results_dir = cfg.get("results-dir")
    if not results_dir:
        raise RuntimeError("holdout results directory is missing")
    from . import validation
    layout = validation.holdout_layout_from_config(dict(cfg))
    required_metric = {
        "binary": "accuracy", "multiclass": "accuracy",
        "ordinal": "accuracy", "multilabel": "macro_f1",
        "regression": "mae", "count": "mae",
    }[layout["task"]]
    if (not isinstance(metrics, dict) or required_metric not in metrics
            or any(key in metrics for key in (
                "per_node", "predictions", "folds"))):
        raise RuntimeError("holdout metrics violate the pooled-only contract")
    payload = {
        "pooled_only": True,
        "privacy": "node-dp-pooled-postprocessing",
        "method": "holdout",
        "task": layout["task"],
        "n_nodes": int(cfg.get("min-train-nodes", 0)),
        "metrics": metrics,
    }
    final = os.path.join(results_dir, "holdout.json")
    temporary = final + ".tmp"
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, allow_nan=False, separators=(",", ":"))
        os.replace(temporary, final)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

@app.main()
def main(grid: Grid, context: Context) -> None:
    cfg = context.run_config
    track = str(cfg.get("dp-track", "neural")).lower()
    if track == "validation":
        metrics, n_nodes, available = _run_validation(grid, cfg)
        _save_validation(cfg, metrics, n_nodes, available)
    else:  # neural or egress
        _run_fedavg(grid, cfg, track)
