"""Ordinary vision acquisition is offline, full-SHA pinned and fail-closed."""

import hashlib
import io
from pathlib import Path
import sys
from unittest import mock

import pytest
import torch
import torchvision.models as models

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import vision


@pytest.mark.parametrize("name", ["resnet18", "resnet50", "densenet121"])
def test_empty_encoder_cache_never_downloads_or_constructs_random_fallback(name, tmp_path):
    with mock.patch.object(torch.hub, "get_dir", return_value=str(tmp_path)), \
            mock.patch.object(torch.hub, "load_state_dict_from_url", side_effect=AssertionError("network")), \
            mock.patch.object(models, name, side_effect=AssertionError("model construction")):
        with pytest.raises(ValueError, match="custodian must pre-seed"):
            vision.build_backbone(name)


@pytest.mark.parametrize("name", ["resnet18", "resnet50", "densenet121"])
def test_preseed_requires_full_digest_not_torchvision_filename_prefix(name, tmp_path, monkeypatch):
    directory = tmp_path / "checkpoints"
    directory.mkdir()
    filename, size, digest = vision._PRESEEDED_ENCODERS[name]
    (directory / filename).write_bytes(b"substituted cached bytes")
    monkeypatch.setattr(torch.hub, "get_dir", lambda: str(tmp_path))
    with pytest.raises(ValueError, match="verified pretrained encoder"):
        vision.verified_backbone_bytes(name)
    assert len(digest) == 64
    assert len(digest) > len(filename.split("-")[1].split(".")[0])


@pytest.mark.parametrize("name", ["resnet18", "resnet50", "densenet121"])
def test_verified_bytes_construct_only_trusted_architecture_with_weights_none(name):
    constructor = getattr(models, name)
    torch.manual_seed(0)
    fixture = constructor(weights=None)
    storage = io.BytesIO()
    state = fixture.state_dict()
    if name == "densenet121":
        # Exercise the exact legacy naming used by the pinned official artifact.
        import re
        pattern = re.compile(r"^(.*denselayer\d+\.(?:norm|relu|conv))([12]\.(?:weight|bias|running_mean|running_var))$")
        state = {((m.group(1) + "." + m.group(2)) if (m := pattern.match(k)) else k): v
                 for k, v in state.items()}
    torch.save(state, storage)
    with mock.patch.object(vision, "verified_backbone_bytes", return_value=storage.getvalue()), \
            mock.patch.object(models, name, wraps=constructor) as build, \
            mock.patch.object(torch.hub, "load_state_dict_from_url", side_effect=AssertionError("network")):
        model, width = vision.build_backbone(name)
    build.assert_called_once_with(weights=None)
    assert not model.training
    assert all(not p.requires_grad for p in model.parameters())
    assert width == vision.feature_dim_for(name)


def test_changed_encoder_is_rejected_even_with_expected_size(tmp_path, monkeypatch):
    original = b"pinned public encoder bytes"
    filename = "fixture.pth"
    digest = hashlib.sha256(original).hexdigest()
    monkeypatch.setitem(vision._PRESEEDED_ENCODERS, "resnet18", (filename, len(original), digest))
    directory = tmp_path / "checkpoints"
    directory.mkdir()
    (directory / filename).write_bytes(b"X" + original[1:])
    monkeypatch.setattr(torch.hub, "get_dir", lambda: str(tmp_path))
    with pytest.raises(ValueError, match="verified pretrained encoder"):
        vision.verified_backbone_bytes("resnet18")
