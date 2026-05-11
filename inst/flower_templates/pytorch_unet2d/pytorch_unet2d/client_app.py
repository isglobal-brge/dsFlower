"""Flower ClientApp for Federated U-Net 2D Segmentation."""

import json
import os
from collections import OrderedDict

from flwr.client import ClientApp, NumPyClient
from flwr.common import Context

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from .task import (
    load_image_data, load_privacy_config, SegmentationDataset,
    get_mask_root, get_path_cols,
)
from .privacy_utils import (
    clip_weights, add_gaussian_noise, compute_sigma, bucket_count,
    get_parameters_fedbn, set_parameters_fedbn,
)


class _DoubleConv(nn.Module):
    """Two 3x3 convolutions with BatchNorm and ReLU."""
    def __init__(self, in_ch, out_ch):
        super().__init__()
        self.conv = nn.Sequential(
            nn.Conv2d(in_ch, out_ch, 3, padding=1),
            nn.BatchNorm2d(out_ch),
            nn.ReLU(inplace=True),
            nn.Conv2d(out_ch, out_ch, 3, padding=1),
            nn.BatchNorm2d(out_ch),
            nn.ReLU(inplace=True),
        )

    def forward(self, x):
        return self.conv(x)


class UNet2D(nn.Module):
    """Simple U-Net with 4-level encoder-decoder and skip connections."""
    def __init__(self, n_classes=1, in_channels=3):
        super().__init__()
        # Encoder
        self.enc1 = _DoubleConv(in_channels, 64)
        self.enc2 = _DoubleConv(64, 128)
        self.enc3 = _DoubleConv(128, 256)
        self.enc4 = _DoubleConv(256, 512)
        self.pool = nn.MaxPool2d(2)

        # Bottleneck
        self.bottleneck = _DoubleConv(512, 1024)

        # Decoder
        self.up4 = nn.ConvTranspose2d(1024, 512, 2, stride=2)
        self.dec4 = _DoubleConv(1024, 512)
        self.up3 = nn.ConvTranspose2d(512, 256, 2, stride=2)
        self.dec3 = _DoubleConv(512, 256)
        self.up2 = nn.ConvTranspose2d(256, 128, 2, stride=2)
        self.dec2 = _DoubleConv(256, 128)
        self.up1 = nn.ConvTranspose2d(128, 64, 2, stride=2)
        self.dec1 = _DoubleConv(128, 64)

        self.final = nn.Conv2d(64, n_classes, 1)

    def forward(self, x):
        e1 = self.enc1(x)
        e2 = self.enc2(self.pool(e1))
        e3 = self.enc3(self.pool(e2))
        e4 = self.enc4(self.pool(e3))

        b = self.bottleneck(self.pool(e4))

        d4 = self.dec4(torch.cat([self.up4(b), e4], dim=1))
        d3 = self.dec3(torch.cat([self.up3(d4), e3], dim=1))
        d2 = self.dec2(torch.cat([self.up2(d3), e2], dim=1))
        d1 = self.dec1(torch.cat([self.up1(d2), e1], dim=1))

        return self.final(d1)


class DiceBCELoss(nn.Module):
    """Combined Dice + BCE loss for segmentation."""
    def __init__(self):
        super().__init__()
        self.bce = nn.BCEWithLogitsLoss()

    def forward(self, logits, targets):
        bce_loss = self.bce(logits, targets)

        probs = torch.sigmoid(logits)
        smooth = 1e-6
        intersection = (probs * targets).sum()
        dice_loss = 1 - (2 * intersection + smooth) / (
            probs.sum() + targets.sum() + smooth
        )

        return bce_loss + dice_loss


class FlowerClient(NumPyClient):
    def __init__(self, model, trainloader, privacy_config,
                 learning_rate=0.001, local_epochs=1, device="cpu",
                 fedbn=False):
        self.model = model.to(device)
        self.trainloader = trainloader
        self.privacy_config = privacy_config
        self.device = device
        self.local_epochs = local_epochs
        self.fedbn = fedbn
        self.criterion = DiceBCELoss()
        self.optimizer = torch.optim.Adam(
            model.parameters(), lr=learning_rate
        )

    def get_parameters(self, config):
        return get_parameters_fedbn(self.model, exclude_bn=self.fedbn)

    def set_parameters(self, parameters):
        set_parameters_fedbn(self.model, parameters, exclude_bn=self.fedbn)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        old_weights = [p.copy() for p in parameters]

        self.model.train()
        for _ in range(self.local_epochs):
            for images, masks in self.trainloader:
                images = images.to(self.device)
                masks = masks.to(self.device)
                self.optimizer.zero_grad()
                output = self.model(images)
                loss = self.criterion(output, masks)
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
        total = 0
        with torch.no_grad():
            for images, masks in self.trainloader:
                images = images.to(self.device)
                masks = masks.to(self.device)
                output = self.model(images)
                total_loss += self.criterion(output, masks).item() * len(images)
                total += len(images)
        avg_loss = total_loss / max(total, 1)

        n_examples = total
        if not self.privacy_config.get("allow_exact_num_examples", True):
            n_examples = bucket_count(n_examples)

        return avg_loss, n_examples, {"loss": avg_loss}


def client_fn(context: Context) -> FlowerClient:
    """Create a Flower client."""
    cfg = context.run_config
    data_root, samples_df, target_col = load_image_data(context)
    privacy_config = load_privacy_config(context)

    n_classes = int(cfg.get("n_classes", 1))
    learning_rate = float(cfg.get("learning_rate", 0.001))
    batch_size = int(cfg.get("batch_size", 8))
    local_epochs = int(cfg.get("local_epochs", 1))

    mask_root = get_mask_root(context)
    image_path_col, mask_path_col = get_path_cols(context)
    dataset = SegmentationDataset(
        data_root,
        samples_df,
        target_col=target_col,
        image_path_col=image_path_col,
        mask_root=mask_root,
        mask_path_col=mask_path_col,
    )
    trainloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    strategy = cfg.get("strategy", "FedAvg")
    fedbn = (strategy == "FedBN")

    model = UNet2D(n_classes=n_classes)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    return FlowerClient(
        model, trainloader, privacy_config,
        learning_rate=learning_rate,
        local_epochs=local_epochs,
        device=device,
        fedbn=fedbn
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
