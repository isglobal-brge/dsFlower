"""Flower ServerApp with multi-strategy support and privacy-aware aggregation."""

import json
import os
from pathlib import Path

import numpy as np
from flwr.common import Context, ndarrays_to_parameters, parameters_to_ndarrays
from flwr.server import ServerApp, ServerAppComponents, ServerConfig
from flwr.server.strategy import FedAvg, FedProx, FedAdam, FedAdagrad

_STRATEGY_MAP = {
    "FedAvg": FedAvg,
    "FedBN": FedAvg,  # Server-side same as FedAvg; BN exclusion is client-side
    "FedProx": FedProx,
    "FedAdam": FedAdam,
    "FedAdagrad": FedAdagrad,
}


_ADAPTIVE_STRATEGIES = {FedAdam, FedAdagrad}


def _make_save_strategy(base_cls, results_dir, num_rounds,
                         allow_per_node_metrics=True, evaluation_only=False,
                         template_name="unknown", save_json_max_bytes=16777216,
                         **kwargs):
    """Create a SaveModelStrategy that subclasses the given Flower strategy."""

    class _Strategy(base_cls):
        """Dynamic strategy that saves global model weights and metrics."""

        def __init__(self, results_dir, num_rounds, allow_per_node_metrics=True,
                     evaluation_only=False, template_name="unknown",
                     save_json_max_bytes=16777216, **kw):
            # FedAdam/FedAdagrad require initial_parameters but we don't
            # have model params at server init time. Pass a dummy value and
            # override initialize_parameters to fetch from a real client.
            if base_cls in _ADAPTIVE_STRATEGIES and "initial_parameters" not in kw:
                kw["initial_parameters"] = ndarrays_to_parameters([])
                self._needs_client_init = True
            else:
                self._needs_client_init = False
            super().__init__(**kw)
            self.results_dir = Path(results_dir)
            self.results_dir.mkdir(parents=True, exist_ok=True)
            self.num_rounds = num_rounds
            self.allow_per_node_metrics = allow_per_node_metrics
            self.evaluation_only = evaluation_only
            self._template_name = template_name
            self.save_json_max_bytes = int(save_json_max_bytes)
            self.history = []

        def initialize_parameters(self, client_manager):
            """Override to request real params from a client for adaptive strategies."""
            if self._needs_client_init:
                # Clear the dummy initial_parameters so Flower requests from a client.
                # This also forces FedAdam/FedAdagrad to initialize their internal
                # state (current_weights, m_t, v_t) from the real client params.
                self.initial_parameters = None
                return None
            return super().initialize_parameters(client_manager)

        def aggregate_fit(self, server_round, results, failures):
            # Lazy-init current_weights for adaptive strategies: the dummy
            # initial_parameters left current_weights empty. On round 1,
            # reconstruct from a FedAvg pass so delta computation works.
            if (self._needs_client_init and
                    hasattr(self, 'current_weights') and
                    len(self.current_weights) == 0):
                fedavg_agg, _ = FedAvg.aggregate_fit(
                    self, server_round, results, failures
                )
                if fedavg_agg is not None:
                    self.current_weights = parameters_to_ndarrays(fedavg_agg)

            aggregated_parameters, aggregated_metrics = super().aggregate_fit(
                server_round, results, failures
            )
            if aggregated_parameters is not None:
                weights = parameters_to_ndarrays(aggregated_parameters)
                self._save_weights(weights, server_round)
            return aggregated_parameters, aggregated_metrics

        def aggregate_evaluate(self, server_round, results, failures):
            loss, metrics = super().aggregate_evaluate(
                server_round, results, failures
            )
            self.history.append({
                "round": server_round,
                "loss": float(loss) if loss is not None else None,
                "n_clients": len(results),
                "n_failures": len(failures),
            })
            if server_round == self.num_rounds:
                self._save_history()

            if not self.allow_per_node_metrics:
                metrics = {}

            return loss, metrics

        def _save_weights(self, weights, server_round):
            if self.evaluation_only:
                return

            # Native framework format is the primary artifact for large
            # vision models. Portable JSON is kept only while reasonably small.
            self._save_native(weights, server_round)

            total_bytes = int(sum(getattr(w, "nbytes", 0) for w in weights))
            if total_bytes > self.save_json_max_bytes:
                path = self.results_dir / "global_model.skipped.json"
                with open(path, "w") as f:
                    json.dump({
                        "reason": "weights_exceed_json_limit",
                        "nbytes": total_bytes,
                        "max_bytes": self.save_json_max_bytes,
                        "round": server_round,
                        "native_artifact": "model.pt",
                    }, f)
                return

            # Portable JSON format for small models.
            data = {str(i): w.tolist() for i, w in enumerate(weights)}
            data["__shapes__"] = [list(w.shape) for w in weights]
            data["__round__"] = server_round
            path = self.results_dir / "global_model.json"
            with open(path, "w") as f:
                json.dump(data, f)

        def _save_native(self, weights, server_round):
            """Save model in native ML framework format."""
            try:
                self._save_pytorch_native(weights, server_round)
                return
            except ImportError:
                pass
            try:
                self._save_sklearn_native(weights, server_round)
                return
            except ImportError:
                pass

        def _save_sklearn_native(self, weights, server_round):
            """Save as sklearn model via joblib."""
            import importlib
            import joblib

            _SKLEARN_MODELS = {
                "sklearn_logreg": ("sklearn.linear_model", "LogisticRegression"),
                "sklearn_ridge": ("sklearn.linear_model", "RidgeClassifier"),
                "sklearn_sgd": ("sklearn.linear_model", "SGDClassifier"),
            }

            template = self._template_name
            entry = _SKLEARN_MODELS.get(template)
            if entry is None:
                # Fallback: save raw numpy arrays
                np.savez(
                    self.results_dir / "model.npz",
                    **{str(i): w for i, w in enumerate(weights)},
                )
                return

            mod_name, cls_name = entry
            mod = importlib.import_module(mod_name)
            ModelClass = getattr(mod, cls_name)

            model = ModelClass()
            coef = np.array(weights[0])
            if coef.ndim == 1:
                coef = coef.reshape(1, -1)
            model.coef_ = coef
            model.intercept_ = np.array(weights[1]).reshape(-1)
            model.classes_ = np.arange(coef.shape[0] + 1) if coef.shape[0] > 1 \
                else np.array([0, 1])
            model.n_features_in_ = coef.shape[1]

            path = self.results_dir / "model.joblib"
            joblib.dump(model, path)

        def _save_pytorch_native(self, weights, server_round):
            """Save as PyTorch state_dict checkpoint."""
            import torch
            from collections import OrderedDict

            # Try to resolve state_dict key names from the template's model
            keys = None
            try:
                import importlib
                template = self._template_name
                client_mod = importlib.import_module(f"{template}.client_app")
                Net = getattr(client_mod, "Net", None)
                if Net is not None:
                    # Build a tiny dummy model to extract key names
                    import inspect
                    sig = inspect.signature(Net.__init__)
                    params = list(sig.parameters.keys())
                    kw = {}
                    for p in params:
                        if p == "self":
                            continue
                        if p in ("input_dim", "input_size", "in_features",
                                 "n_features", "in_channels"):
                            kw[p] = weights[0].shape[-1] if weights[0].ndim >= 2 \
                                else weights[0].shape[0]
                        elif p in ("n_classes", "output_dim", "num_classes"):
                            kw[p] = weights[-2].shape[0] if weights[-2].ndim >= 2 \
                                else 1
                        elif p in ("hidden_size", "hidden_dim"):
                            kw[p] = 64
                        elif p in ("num_layers", "n_layers"):
                            kw[p] = 2
                    dummy = Net(**kw)
                    keys = list(dummy.state_dict().keys())
            except Exception:
                keys = None

            if keys is not None and len(keys) == len(weights):
                state_dict = OrderedDict(
                    {k: torch.tensor(w) for k, w in zip(keys, weights)}
                )
            else:
                state_dict = OrderedDict(
                    {str(i): torch.tensor(w) for i, w in enumerate(weights)}
                )

            checkpoint = {
                "state_dict": state_dict,
                "round": server_round,
                "template": getattr(self, "_template_name", "unknown"),
                "shapes": [list(w.shape) for w in weights],
            }
            path = self.results_dir / "model.pt"
            torch.save(checkpoint, path)

        def _save_history(self):
            path = self.results_dir / "history.json"
            with open(path, "w") as f:
                json.dump(self.history, f)

    return _Strategy(
        results_dir=results_dir,
        num_rounds=num_rounds,
        allow_per_node_metrics=allow_per_node_metrics,
        evaluation_only=evaluation_only,
        template_name=template_name,
        save_json_max_bytes=save_json_max_bytes,
        **kwargs,
    )


def server_fn(context: Context) -> ServerAppComponents:
    """Configure the server with the requested aggregation strategy."""
    cfg = context.run_config

    num_rounds = int(cfg.get("num-server-rounds", 5))
    results_dir = cfg.get("results-dir", "/tmp/dsflower_results")
    template_name = cfg.get("template-name", "unknown")
    require_secagg = str(cfg.get("require-secure-aggregation", "false")).lower() == "true"
    allow_per_node_metrics = str(cfg.get("allow-per-node-metrics", "true")).lower() == "true"
    fixed_client_sampling = str(cfg.get("fixed-client-sampling", "false")).lower() == "true"
    evaluation_only = str(cfg.get("evaluation-only", "false")).lower() == "true"
    save_json_max_bytes = int(cfg.get("save-json-max-bytes", 16777216))

    strategy_name = cfg.get("strategy", "FedAvg")
    base_cls = _STRATEGY_MAP.get(strategy_name)
    if base_cls is None:
        raise ValueError(
            f"Unknown strategy '{strategy_name}'. "
            f"Supported: {', '.join(_STRATEGY_MAP)}"
        )

    strategy_kwargs = {
        "fraction_fit": float(cfg.get("strategy-fraction_fit", 1.0)),
        "fraction_evaluate": float(cfg.get("strategy-fraction_evaluate", 1.0)),
        "min_fit_clients": int(cfg.get("strategy-min_fit_clients", 2)),
        "min_available_clients": int(cfg.get("strategy-min_available_clients", 2)),
    }

    if strategy_name == "FedProx":
        strategy_kwargs["proximal_mu"] = float(cfg.get("strategy-proximal_mu", 0.1))
    elif strategy_name in ("FedAdam", "FedAdagrad"):
        strategy_kwargs["eta"] = float(cfg.get("strategy-eta", 0.01))
        strategy_kwargs["tau"] = float(cfg.get("strategy-tau", 1e-3))

    if fixed_client_sampling:
        strategy_kwargs["fraction_fit"] = 1.0

    strategy = _make_save_strategy(
        base_cls,
        results_dir=results_dir,
        num_rounds=num_rounds,
        allow_per_node_metrics=allow_per_node_metrics,
        evaluation_only=evaluation_only,
        template_name=template_name,
        save_json_max_bytes=save_json_max_bytes,
        **strategy_kwargs,
    )

    config = ServerConfig(num_rounds=num_rounds)

    if require_secagg:
        num_clients = int(cfg.get("strategy-min_available_clients", 2))
        if num_clients >= 3:
            try:
                # Flower >= 1.10: ServerAppComponents accepts workflow
                from flwr.server.workflow import SecAggPlusWorkflow
                return ServerAppComponents(
                    strategy=strategy, config=config,
                    workflow=SecAggPlusWorkflow(
                        num_shares=num_clients,
                        reconstruction_threshold=max(1, num_clients - 1),
                    )
                )
            except TypeError:
                # Flower < 1.10: workflow param not supported in ServerAppComponents.
                # SecAgg protection is provided client-side via secaggplus_mod
                # (the client checks manifest.json and loads the mod at import time).
                import sys
                print(
                    "\nDSFLOWER NOTE: Server-side SecAggPlusWorkflow not supported "
                    "in this Flower version. Client-side secaggplus_mod is active.\n",
                    file=sys.stderr, flush=True,
                )
            except ImportError:
                pass
        else:
            import sys
            print(
                "\nDSFLOWER NOTE: SecAgg+ requires 3+ clients but only "
                f"{num_clients} configured. Running without SecAgg+.\n"
                "Privacy of individual updates is protected by DP-SGD noise "
                "when secure_dp profile is active.\n",
                file=sys.stderr, flush=True,
            )

    return ServerAppComponents(strategy=strategy, config=config)


app = ServerApp(server_fn=server_fn)
