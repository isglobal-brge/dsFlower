"""Flower ClientApp for Federated PyTorch MLP."""

from collections import OrderedDict

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from .task import load_data


def _build_mlp(input_dim, hidden_layers, output_dim=1):
    """Build an MLP model from layer sizes."""
    layers = []
    prev = input_dim
    for h in hidden_layers:
        layers.append(nn.Linear(prev, h))
        layers.append(nn.ReLU())
        prev = h
    layers.append(nn.Linear(prev, output_dim))
    return nn.Sequential(*layers)


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, learning_rate=0.01,
                 local_epochs=1, device="cpu"):
        self.model = model.to(device)
        self.trainloader = trainloader
        self.device = device
        self.local_epochs = local_epochs
        self.criterion = nn.BCEWithLogitsLoss()
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
        return self.get_parameters(config), len(self.trainloader.dataset), {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)
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
        return avg_loss, total, {"accuracy": accuracy}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    X, y = load_data(context)

    hidden_str = cfg.get("hidden_layers", "64,32")
    if isinstance(hidden_str, str):
        hidden_layers = [int(x) for x in hidden_str.split(",")]
    else:
        hidden_layers = [int(x) for x in hidden_str]

    learning_rate = float(cfg.get("learning_rate", 0.01))
    batch_size = int(cfg.get("batch_size", 32))
    local_epochs = int(cfg.get("local_epochs", 1))

    input_dim = X.shape[1]
    model = _build_mlp(input_dim, hidden_layers)

    dataset = TensorDataset(
        torch.from_numpy(X), torch.from_numpy(y)
    )
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    return FlowerClient(
        model, trainloader,
        learning_rate=learning_rate,
        local_epochs=local_epochs,
        device=device
    )


app = ClientApp(client_fn=client_fn)
