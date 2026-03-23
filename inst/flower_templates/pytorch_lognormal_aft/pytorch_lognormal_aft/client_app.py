"""Flower ClientApp for Federated Accelerated Failure Time (AFT) Model.

Parametric survival model with log-normal distribution.
Predicts log(T) = X*beta + sigma*epsilon, epsilon ~ N(0,1).
"""

import json
import math
import os
from collections import OrderedDict

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from .task import load_data, load_privacy_config
from .privacy_utils import (
    clip_weights, add_gaussian_noise, compute_sigma, bucket_count,
    make_private_opacus,
)


class AFTModel(nn.Module):
    """AFT model with learnable log-scale parameter."""

    def __init__(self, input_dim):
        super().__init__()
        self.linear = nn.Linear(input_dim, 1)
        self.log_scale = nn.Parameter(torch.tensor(0.0))

    def forward(self, x):
        mu = self.linear(x).squeeze(1)
        return mu, self.log_scale


def _aft_log_normal_loss(mu, log_scale, log_time, event):
    """Negative log-likelihood for log-normal AFT.

    For uncensored (event=1): log f(t) = log phi(z) - log(sigma) - log(t)
    For censored (event=0):   log S(t) = log Phi(-z)
    where z = (log(t) - mu) / sigma, phi = N(0,1) pdf, Phi = N(0,1) cdf.
    """
    sigma = torch.exp(log_scale) + 1e-8
    z = (log_time - mu) / sigma

    # Log pdf of standard normal
    log_phi = -0.5 * z ** 2 - 0.5 * math.log(2 * math.pi)
    # Log survival of standard normal (using log_erfc for numerical stability)
    log_Phi_neg_z = torch.log(0.5 * torch.erfc(z / math.sqrt(2)) + 1e-15)

    # Uncensored: log f(t) = log_phi - log(sigma) - log(t)
    ll_uncensored = log_phi - log_scale - log_time
    # Censored: log S(t) = log(1 - Phi(z)) = log Phi(-z)
    ll_censored = log_Phi_neg_z

    ll = event * ll_uncensored + (1 - event) * ll_censored
    return -ll.mean()


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
        self.optimizer = optimizer or torch.optim.Adam(
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
                mu, log_scale = self.model(X_batch)
                log_time = torch.log(time_batch + 1e-8)
                loss = _aft_log_normal_loss(mu, log_scale, log_time, event_batch)
                loss.backward()
                self.optimizer.step()

        new_weights = self.get_parameters(config)
        fit_metrics = {}

        if privacy_mode == "dp" and self.privacy_engine is not None:
            fit_metrics["dp_epsilon"] = self.privacy_engine.get_epsilon(
                self.privacy_config["delta"])
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
                mu, log_scale = self.model(X_batch)
                log_time = torch.log(time_batch + 1e-8)
                batch_loss = _aft_log_normal_loss(mu, log_scale, log_time,
                                                  event_batch)
                total_loss += batch_loss.item() * len(X_batch)
                total += len(X_batch)
        avg_loss = total_loss / max(total, 1)

        n_examples = total
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return avg_loss, n_examples, {"loss": avg_loss}


def client_fn(context: Context) -> FlowerClient:
    cfg = context.run_config
    X, time, event = load_data(context)
    privacy_config = load_privacy_config(context)

    learning_rate = float(cfg.get("learning_rate", 0.01))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    input_dim = X.shape[1]
    model = AFTModel(input_dim)

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
        learning_rate=learning_rate, local_epochs=local_epochs,
        device=device, optimizer=optimizer, privacy_engine=privacy_engine,
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
