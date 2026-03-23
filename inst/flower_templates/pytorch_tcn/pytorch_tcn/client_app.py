"""Flower ClientApp for Federated PyTorch TCN."""

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
from .privacy_utils import (
    clip_weights, add_gaussian_noise, compute_sigma, bucket_count,
    make_private_opacus, get_parameters_fedbn, set_parameters_fedbn,
)


class _TCNBlock(nn.Module):
    """Single TCN residual block with dilated causal convolution."""
    def __init__(self, in_ch, out_ch, kernel_size, dilation):
        super().__init__()
        padding = (kernel_size - 1) * dilation
        self.conv1 = nn.Conv1d(in_ch, out_ch, kernel_size,
                               padding=padding, dilation=dilation)
        self.bn1 = nn.BatchNorm1d(out_ch)
        self.conv2 = nn.Conv1d(out_ch, out_ch, kernel_size,
                               padding=padding, dilation=dilation)
        self.bn2 = nn.BatchNorm1d(out_ch)
        self.relu = nn.ReLU(inplace=True)
        self.downsample = (nn.Conv1d(in_ch, out_ch, 1)
                           if in_ch != out_ch else None)

    def forward(self, x):
        residual = x
        out = self.relu(self.bn1(self.conv1(x)))
        # Trim to causal output length
        out = out[:, :, :x.size(2)]
        out = self.bn2(self.conv2(out))
        out = out[:, :, :x.size(2)]
        if self.downsample is not None:
            residual = self.downsample(x)
        return self.relu(out + residual)


class TCN(nn.Module):
    """Temporal Convolutional Network with stacked dilated causal convolutions."""
    def __init__(self, n_channels, kernel_size=3, n_layers=4,
                 hidden_dim=64, n_classes=1):
        super().__init__()
        layers = []
        in_ch = n_channels
        for i in range(n_layers):
            dilation = 2 ** i
            layers.append(_TCNBlock(in_ch, hidden_dim, kernel_size, dilation))
            in_ch = hidden_dim
        self.network = nn.Sequential(*layers)
        self.head = nn.Linear(hidden_dim, n_classes)

    def forward(self, x):
        # x: (batch, channels, seq_len)
        out = self.network(x)
        # Global average pooling over time
        out = out.mean(dim=2)
        return self.head(out)


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, privacy_config,
                 learning_rate=0.001, local_epochs=1, device="cpu",
                 optimizer=None, privacy_engine=None, fedbn=False):
        self.model = model.to(device)
        self.trainloader = trainloader
        self.privacy_config = privacy_config
        self.device = device
        self.local_epochs = local_epochs
        self.fedbn = fedbn
        self.criterion = nn.BCEWithLogitsLoss()
        self.privacy_engine = privacy_engine
        if optimizer is not None:
            self.optimizer = optimizer
        else:
            self.optimizer = torch.optim.Adam(
                model.parameters(), lr=learning_rate
            )

    def get_parameters(self, config):
        module = getattr(self.model, '_module', self.model)
        return get_parameters_fedbn(module, exclude_bn=self.fedbn)

    def set_parameters(self, parameters):
        module = getattr(self.model, '_module', self.model)
        set_parameters_fedbn(module, parameters, exclude_bn=self.fedbn)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        privacy_mode = self.privacy_config.get("privacy_mode")
        old_weights = [p.copy() for p in parameters] if privacy_mode == "dp_update_level" else None

        self.model.train()
        for _ in range(self.local_epochs):
            for X_batch, y_batch in self.trainloader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device).unsqueeze(1)
                self.optimizer.zero_grad()
                output = self.model(X_batch)
                loss = self.criterion(output, y_batch)
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
        correct = 0
        total = 0
        with torch.no_grad():
            for X_batch, y_batch in self.trainloader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device).unsqueeze(1)
                output = self.model(X_batch)
                total_loss += self.criterion(output, y_batch).item() * len(X_batch)
                preds = (torch.sigmoid(output) > 0.5).float()
                correct += (preds == y_batch).sum().item()
                total += len(X_batch)
        avg_loss = total_loss / max(total, 1)
        accuracy = correct / max(total, 1)

        n_examples = total
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return avg_loss, n_examples, {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)
    privacy_config = load_privacy_config(context)

    n_channels = int(cfg.get("n_channels", 1))
    kernel_size = int(cfg.get("kernel_size", 3))
    n_layers = int(cfg.get("n_layers", 4))
    learning_rate = float(cfg.get("learning_rate", 0.001))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    # Reshape flat features to (channels, seq_len)
    n_features = X.shape[1]
    seq_len = n_features // n_channels
    X_reshaped = X.reshape(-1, n_channels, seq_len)

    dataset = TensorDataset(
        torch.from_numpy(X_reshaped), torch.from_numpy(y)
    )
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model = TCN(n_channels=n_channels, kernel_size=kernel_size,
                n_layers=n_layers)
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

    strategy = cfg.get("strategy", "FedAvg")
    fedbn = (strategy == "FedBN")

    return FlowerClient(
        model, trainloader, privacy_config,
        learning_rate=learning_rate,
        local_epochs=local_epochs,
        device=device,
        optimizer=optimizer,
        privacy_engine=privacy_engine,
        fedbn=fedbn,
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
