"""Flower ClientApp for Federated PyTorch DenseNet-121."""

import json
import os
from collections import OrderedDict

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision.models import densenet121

from .task import load_image_data, load_privacy_config, ImageDataset
from .privacy_utils import clip_weights, add_gaussian_noise, compute_sigma, bucket_count


def _build_densenet121(n_classes):
    """Build DenseNet-121 with custom classifier."""
    model = densenet121(weights=None)
    model.classifier = nn.Linear(model.classifier.in_features, n_classes)
    return model


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, privacy_config,
                 learning_rate=0.001, local_epochs=1, device="cpu"):
        self.model = model.to(device)
        self.trainloader = trainloader
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
            for images, labels in self.trainloader:
                images = images.to(self.device)
                labels = labels.to(self.device)
                self.optimizer.zero_grad()
                output = self.model(images)
                loss = self.criterion(output, labels)
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
            for images, labels in self.trainloader:
                images = images.to(self.device)
                labels = labels.to(self.device)
                output = self.model(images)
                total_loss += self.criterion(output, labels).item() * len(images)
                preds = output.argmax(dim=1)
                correct += (preds == labels).sum().item()
                total += len(images)
        avg_loss = total_loss / max(total, 1)
        accuracy = correct / max(total, 1)

        n_examples = total
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return avg_loss, n_examples, {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    data_root, samples_df, target_col = load_image_data(context)
    privacy_config = load_privacy_config(context)

    n_classes = int(cfg.get("n_classes", 2))
    learning_rate = float(cfg.get("learning_rate", 0.001))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    dataset = ImageDataset(data_root, samples_df, target_col=target_col)
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model = _build_densenet121(n_classes)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    return FlowerClient(
        model, trainloader, privacy_config,
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
