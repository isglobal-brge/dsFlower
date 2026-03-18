"""Flower ClientApp for Federated PyTorch Multi-Class Classifier."""

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
from .privacy_utils import clip_weights, add_gaussian_noise, compute_sigma, bucket_count


def _build_classifier(input_dim, hidden_layers, n_classes):
    """Build a classifier: MLP if hidden_layers given, else Linear."""
    if hidden_layers:
        layers = []
        prev = input_dim
        for h in hidden_layers:
            layers.append(nn.Linear(prev, h))
            layers.append(nn.ReLU())
            prev = h
        layers.append(nn.Linear(prev, n_classes))
        return nn.Sequential(*layers)
    return nn.Linear(input_dim, n_classes)


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, n_classes, privacy_config,
                 learning_rate=0.01, local_epochs=1, device="cpu"):
        self.model = model.to(device)
        self.trainloader = trainloader
        self.n_classes = n_classes
        self.privacy_config = privacy_config
        self.device = device
        self.local_epochs = local_epochs
        self.criterion = nn.CrossEntropyLoss()
        self.optimizer = torch.optim.Adam(
            model.parameters(), lr=learning_rate
        )

    def get_parameters(self, config):
        return [val.cpu().numpy() for val in self.model.state_dict().values()]

    def set_parameters(self, parameters):
        state_dict = OrderedDict()
        for key, val in zip(self.model.state_dict().keys(), parameters):
            state_dict[key] = torch.tensor(val)
        self.model.load_state_dict(state_dict, strict=True)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        old_weights = [p.copy() for p in parameters]

        self.model.train()
        for _ in range(self.local_epochs):
            for X_batch, y_batch in self.trainloader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device).long()
                self.optimizer.zero_grad()
                output = self.model(X_batch)
                loss = self.criterion(output, y_batch)
                loss.backward()
                self.optimizer.step()

        new_weights = self.get_parameters(config)

        if self.privacy_config.get("privacy_mode") == "dp":
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

        return new_weights, n_examples, {}

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
                y_batch = y_batch.to(self.device).long()
                output = self.model(X_batch)
                total_loss += self.criterion(output, y_batch).item() * len(X_batch)
                preds = output.argmax(dim=1)
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

    hidden_str = cfg.get("hidden_layers", "")
    if isinstance(hidden_str, str) and hidden_str.strip():
        hidden_layers = [int(x) for x in hidden_str.split(",")]
    elif isinstance(hidden_str, str):
        hidden_layers = []
    else:
        hidden_layers = [int(x) for x in hidden_str]

    n_classes = int(cfg.get("n_classes", 3))
    learning_rate = float(cfg.get("learning_rate", 0.01))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    input_dim = X.shape[1]
    model = _build_classifier(input_dim, hidden_layers, n_classes)

    dataset = TensorDataset(
        torch.from_numpy(X), torch.from_numpy(y)
    )
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    return FlowerClient(
        model, trainloader, n_classes, privacy_config,
        learning_rate=learning_rate,
        local_epochs=local_epochs,
        device=device
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
