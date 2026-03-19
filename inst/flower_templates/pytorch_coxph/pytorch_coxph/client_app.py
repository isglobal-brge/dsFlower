"""Flower ClientApp for Federated Cox Proportional Hazards."""

import json
import os
from collections import OrderedDict

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from .task import load_data, load_privacy_config
from .privacy_utils import clip_weights, add_gaussian_noise, compute_sigma, bucket_count, make_private_opacus


def _cox_partial_likelihood_loss(log_risk, time, event):
    """Negative partial log-likelihood for Cox PH model.

    Args:
        log_risk: (batch,) predicted log-risk scores.
        time: (batch,) observed times.
        event: (batch,) event indicators (1=event, 0=censored).

    Returns:
        Scalar loss tensor.
    """
    sorted_idx = torch.argsort(time, descending=True)
    log_risk = log_risk[sorted_idx]
    event = event[sorted_idx]

    log_cumsum_risk = torch.logcumsumexp(log_risk, dim=0)
    loss = -torch.sum((log_risk - log_cumsum_risk) * event)
    n_events = event.sum()
    if n_events > 0:
        loss = loss / n_events
    return loss


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, privacy_config,
                 learning_rate=0.01, local_epochs=1, device="cpu",
                 optimizer=None, privacy_engine=None):
        self.model = model.to(device)
        self.trainloader = trainloader
        self.privacy_config = privacy_config
        self.device = device
        self.local_epochs = local_epochs
        self.privacy_engine = privacy_engine
        if optimizer is not None:
            self.optimizer = optimizer
        else:
            self.optimizer = torch.optim.Adam(
                model.parameters(), lr=learning_rate
            )

    def get_parameters(self, config):
        module = getattr(self.model, '_module', self.model)
        return [val.cpu().numpy() for val in module.state_dict().values()]

    def set_parameters(self, parameters):
        module = getattr(self.model, '_module', self.model)
        state_dict = OrderedDict()
        for key, val in zip(module.state_dict().keys(), parameters):
            state_dict[key] = torch.tensor(val)
        module.load_state_dict(state_dict, strict=True)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        privacy_mode = self.privacy_config.get("privacy_mode")
        old_weights = [p.copy() for p in parameters] if privacy_mode == "dp_update_level" else None

        self.model.train()
        for _ in range(self.local_epochs):
            for X_batch, time_batch, event_batch in self.trainloader:
                X_batch = X_batch.to(self.device)
                time_batch = time_batch.to(self.device)
                event_batch = event_batch.to(self.device)
                self.optimizer.zero_grad()
                log_risk = self.model(X_batch).squeeze(1)
                loss = _cox_partial_likelihood_loss(log_risk, time_batch, event_batch)
                loss.backward()
                self.optimizer.step()

        new_weights = self.get_parameters(config)
        fit_metrics = {}

        if privacy_mode == "dp" and self.privacy_engine is not None:
            dp_delta = self.privacy_config["delta"]
            actual_epsilon = self.privacy_engine.get_epsilon(dp_delta)
            fit_metrics["dp_epsilon"] = actual_epsilon

        elif privacy_mode == "dp_update_level":
            cn = self.privacy_config["clipping_norm"]
            eps = self.privacy_config["epsilon"]
            delta = self.privacy_config["delta"]
            n = self.privacy_config["n_samples"]

            new_weights = clip_weights(new_weights, old_weights, cn)
            sigma = compute_sigma(eps, delta, cn, n)
            new_weights = add_gaussian_noise(new_weights, old_weights, sigma, cn)

        n_examples = len(self.trainloader.dataset)
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return new_weights, n_examples, fit_metrics

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)

        if not self.privacy_config.get("allow_per_node_metrics", True):
            n_examples = len(self.trainloader.dataset)
            if not self.privacy_config.get("allow_exact_num_examples", True):
                n_examples = bucket_count(n_examples)
            return 0.0, n_examples, {}

        self.model.eval()
        total_loss = 0.0
        total = 0
        with torch.no_grad():
            for X_batch, time_batch, event_batch in self.trainloader:
                X_batch = X_batch.to(self.device)
                time_batch = time_batch.to(self.device)
                event_batch = event_batch.to(self.device)
                log_risk = self.model(X_batch).squeeze(1)
                batch_loss = _cox_partial_likelihood_loss(log_risk, time_batch, event_batch)
                total_loss += batch_loss.item() * len(X_batch)
                total += len(X_batch)
        avg_loss = total_loss / max(total, 1)

        n_examples = total
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return avg_loss, n_examples, {"loss": avg_loss}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, time, event = load_data(context)
    privacy_config = load_privacy_config(context)

    learning_rate = float(cfg.get("learning_rate", 0.01))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    input_dim = X.shape[1]
    model = nn.Linear(input_dim, 1)

    dataset = TensorDataset(
        torch.from_numpy(X),
        torch.from_numpy(time),
        torch.from_numpy(event),
    )
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    privacy_engine = None

    if privacy_config.get("privacy_mode") == "dp":
        model, optimizer, trainloader, privacy_engine = make_private_opacus(
            model, optimizer, trainloader,
            clipping_norm=privacy_config["clipping_norm"],
            epsilon=privacy_config["epsilon"],
            delta=privacy_config["delta"],
            epochs=local_epochs,
        )

    return FlowerClient(
        model, trainloader, privacy_config,
        learning_rate=learning_rate,
        local_epochs=local_epochs,
        device=device,
        optimizer=optimizer,
        privacy_engine=privacy_engine,
    )


def _needs_secagg():
    manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
    if not manifest_dir:
        return False
    try:
        with open(os.path.join(manifest_dir, "manifest.json")) as f:
            return json.load(f).get("require_secure_aggregation", False) is True
    except (OSError, json.JSONDecodeError, KeyError):
        return False


if _needs_secagg():
    from flwr.client.mod import secaggplus_mod
    app = ClientApp(client_fn=client_fn, mods=[secaggplus_mod])
else:
    app = ClientApp(client_fn=client_fn)
