"""Focused safety checks for the opt-in full-network vision DP release.

These use the actual public model builders; no training/data boundary is mocked.
Large models are constructed serially and CPU threads are bounded for local runs.
"""

import base64
import gc
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import torch
from opacus.validators import ModuleValidator
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import (client_app, dp_harness, initialisation, params,
                             seeding, task, vision)


@pytest.fixture(autouse=True)
def bounded_cpu_threads():
    previous = torch.get_num_threads()
    torch.set_num_threads(1)
    yield
    torch.set_num_threads(previous)
    gc.collect()


def config(mode="scratch", backbone="resnet18"):
    spec = {"layers": [{"op": "linear"}]}
    result = {
        "model-spec-b64": base64.b64encode(json.dumps(spec).encode()).decode(),
        "data-kind": "image", "backbone": backbone,
        "vision-extractor-profile": vision.extractor_profile_for(backbone),
        "num-features": vision.feature_dim_for(backbone), "num-classes": 2,
        "image-size": 128 if backbone == "densenet121_3d" else 32,
        "loss-name": "bce_logits", "task-type": "classification",
        "batch-size": 2, "local-epochs": 1, "num-server-rounds": 1,
        "learning-rate": .01,
    }
    if mode is not None:
        result["backbone-mode"] = mode
    return result


@pytest.mark.parametrize("backbone", ["resnet18", "densenet121", "resnet18_3d", "densenet121_3d"])
def test_scratch_has_no_buffers_or_batchnorm_and_every_weight_is_releasable(backbone):
    model = initialisation.build_initial_model(config(backbone=backbone))
    assert all(p.requires_grad for p in model.parameters())
    assert not list(model.buffers())
    assert not any(isinstance(m, torch.nn.modules.batchnorm._BatchNorm)
                   for m in model.modules())
    assert any(isinstance(m, torch.nn.GroupNorm) for m in model.modules())
    assert ModuleValidator.is_valid(model)
    dp_harness.assert_releasable(model)
    assert len(params.get_torch_params(model)) == len(model.state_dict())
    assert sum(p.numel() for p in model.parameters()) > 6_000_000


@pytest.mark.parametrize("backbone", ["resnet18", "densenet121"])
def test_scratch_initialisation_is_identical_across_fresh_node_processes(backbone):
    cfg = config(backbone=backbone)
    script = (
        "import json, torch; torch.set_num_threads(1); "
        "from dsflower_runner.initialisation import build_initial_model, hash_public_arrays; "
        "from dsflower_runner.params import get_torch_params; "
        "print(hash_public_arrays(get_torch_params(build_initial_model(json.loads("
        + repr(json.dumps(cfg)) + ")))))"
    )
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2] / "flower_app"))
    outputs = [subprocess.check_output([sys.executable, "-c", script], env=env,
                                      timeout=90) for _ in range(2)]
    assert outputs[0] == outputs[1]


def test_default_and_explicit_frozen_preserve_public_initialisation_without_encoder_io():
    implicit, explicit = config(None), config("frozen")
    with mock.patch.object(vision, "verified_backbone_bytes",
                           side_effect=AssertionError("frozen head initializer read encoder")):
        assert initialisation.initialisation_spec_sha256(implicit) == initialisation.initialisation_spec_sha256(explicit)
        first = params.get_torch_params(initialisation.build_initial_model(implicit))
        second = params.get_torch_params(initialisation.build_initial_model(explicit))
    assert len(first) == 2
    for a, b in zip(first, second):
        np.testing.assert_array_equal(a, b)


def test_frozen_matches_073_byte_for_byte_at_a_fixed_master_seed(tmp_path):
    """Use the actual verified encoder and pre-change runner when supplied.

    Full release identities intentionally bind the entire runner code digest;
    this regression fixes the master seed to isolate the frozen computation.
    """
    baseline = os.environ.get("DSFLOWER_BASELINE_RUNNER_ROOT")
    if not baseline:
        pytest.skip("set DSFLOWER_BASELINE_RUNNER_ROOT to the 0.7.3 flower_app tree")
    cfg = config(None)
    manifest = dict(cfg, data_type="image")
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    paths = []
    generator = np.random.default_rng(19)
    for index in range(3):
        path = tmp_path / ("fixed-%d.png" % index)
        Image.fromarray(generator.integers(0, 256, (32, 32, 3), dtype=np.uint8)).save(path)
        paths.append(str(path))
    script = '''
import json, sys
from types import SimpleNamespace
import numpy as np
import torch
torch.set_num_threads(1)
from dsflower_runner import client_app, initialisation, params, task, vision
cfg, directory, paths = json.loads(sys.argv[1])
initial = initialisation.build_initial_model(cfg)
model = params.load_user_model(cfg, 512, "bce_logits")
params.set_torch_params(model, params.get_torch_params(initial))
encoder, size, is_3d, device = vision.prepare_backbone("resnet18", cfg["vision-extractor-profile"], 512, 32)
X = vision.extract_features_from_paths(encoder, paths, size, is_3d, device=device)
X, y = client_app._pool_by_patient(X, np.array([0., 0., 1.]), np.array(["a", "a", "b"]), "bce_logits")
context = SimpleNamespace(node_config={"manifest-dir": directory})
pins = dict(task.load_run_pins(context), round_index=1)
pcfg = {"epsilon": 4., "delta": 1e-5, "clipping_norm": 1., "n_samples": 3}
arrays, _ = client_app._dp_fit(model, X, y, pcfg, pins, 3, cfg,
    master=b"f" * 32, noise_multiplier=1.25)
print(initialisation.hash_public_arrays([X, *arrays]))
'''
    payload = json.dumps([cfg, str(tmp_path), paths])
    roots = [baseline, str(Path(__file__).resolve().parents[2] / "flower_app")]
    results = [subprocess.check_output([sys.executable, "-c", script, payload],
                   env=dict(os.environ, PYTHONPATH=root), timeout=90).strip()
               for root in roots]
    assert results[0] == results[1]


def test_batchnorm_negative_control_and_trainable_model_patient_independence():
    coupled = torch.nn.Sequential(torch.nn.Conv2d(3, 4, 1), torch.nn.BatchNorm2d(4))
    assert not ModuleValidator.is_valid(coupled)
    model = initialisation.build_initial_model(config())
    model.train()
    torch.manual_seed(19)
    images = torch.randn(2, 3, 32, 32)
    with torch.no_grad():
        first = model(images)[0].clone()
        images[1] = images[1] * 100 + 13
        second = model(images)[0]
    torch.testing.assert_close(first, second, rtol=0, atol=0)


def test_patient_clipping_uses_one_joint_norm_for_every_trainable_parameter():
    model = initialisation.build_initial_model(config())
    parameters = list(model.parameters())
    # One large synthetic patient derivative on every released coordinate.
    for parameter in parameters:
        parameter.grad = torch.full_like(parameter, 2.)
    clipping_norm = .5
    dp_harness.clip_patient_gradients(parameters, clipping_norm)
    joint_norm = torch.stack([p.grad.double().square().sum()
                              for p in parameters]).sum().sqrt().item()
    assert 0 < joint_norm <= clipping_norm + 1e-7
    assert joint_norm == pytest.approx(clipping_norm, rel=1e-6)
    assert all(torch.count_nonzero(p.grad) == p.numel() for p in parameters)
    # Every coordinate has the SAME global clip factor. Layerwise clipping
    # would give different factors and a joint norm strictly greater than C.
    expected_coordinate = clipping_norm / np.sqrt(sum(p.numel() for p in parameters))
    assert all(torch.allclose(p.grad, torch.full_like(p, expected_coordinate))
               for p in parameters)
    parameters[-1].grad = None
    with pytest.raises(RuntimeError, match="every released"):
        dp_harness.clip_patient_gradients(parameters, clipping_norm)


class RecordingSecureRng(seeding.SecureNumpyRng):
    def __init__(self):
        super().__init__(b"v" * 32)
        self.calls = []

    def normal(self, loc=0., scale=1., size=None):
        self.calls.append((loc, scale, size))
        return super().normal(loc, scale, size=size)


def test_noise_covers_every_parameter_even_when_no_patient_is_sampled():
    model = initialisation.build_initial_model(config())
    parameters = list(model.parameters())
    sums = [torch.zeros_like(p) for p in parameters]
    rng = RecordingSecureRng()
    dp_harness.add_private_patient_gradients(
        parameters, sums, clipping_norm=.5, noise_multiplier=1.25,
        expected_batch_size=2, secure_noise_rng=rng)
    assert rng.calls == [(0., .625, tuple(p.shape)) for p in parameters]
    assert all(p.grad is not None and torch.isfinite(p.grad).all()
               and torch.count_nonzero(p.grad) > 0 for p in parameters)
    assert all(torch.count_nonzero(value) == 0 for value in sums)
    # Verify the public expected-batch divisor on the same secure stream.
    replay = RecordingSecureRng()
    for parameter in parameters:
        expected = torch.as_tensor(replay.normal(0., .625, size=tuple(parameter.shape)),
                                   dtype=parameter.dtype) / 2
        torch.testing.assert_close(parameter.grad, expected, rtol=0, atol=0)


@pytest.mark.parametrize("backbone", ["resnet18", "densenet121"])
def test_streamed_whole_patient_gradient_matches_pooled_feature_chain_rule(tmp_path, backbone):
    cfg = config(backbone=backbone)
    model = initialisation.build_initial_model(cfg)
    generator = np.random.default_rng(7)
    paths = []
    for index in range(2):
        path = tmp_path / ("image-%d.png" % index)
        Image.fromarray(generator.integers(0, 256, (32, 32, 3), dtype=np.uint8)).save(path)
        paths.append(str(path))
    criterion = dp_harness.loss_from_allowlist("bce_logits", cfg)
    target = torch.ones(1, 1)
    features = [model.backbone(torch.from_numpy(vision.read_image_2d(path, 32)).unsqueeze(0))
                for path in paths]
    pooled = torch.stack([value.double() for value in features]).mean(0).float()
    criterion(model.head(pooled), target).backward()
    expected = {name: p.grad.detach().clone() for name, p in model.named_parameters()}
    del features, pooled
    client_app._vision_patient_backward(model, paths, target, criterion, 32, False, torch.device("cpu"))
    assert len(expected) == len(model.state_dict())
    for name, parameter in model.named_parameters():
        assert parameter.grad is not None, name
        torch.testing.assert_close(parameter.grad, expected[name], atol=2e-6, rtol=2e-4,
                                   msg=lambda msg: name + ": " + msg)


def test_full_network_and_frozen_head_account_for_the_identical_poisson_schedule(tmp_path):
    from opacus.accountants.prv import PRVAccountant

    cfg = config()
    cfg["batch-size"] = 1
    cfg["local-epochs"] = 2
    manifest = dict(cfg, **{"data_type": "image", "dp-unit": "patient",
                           "allow-trainable-backbone": True})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    context = SimpleNamespace(node_config={"manifest-dir": str(tmp_path)})
    pins = dict(task.load_run_pins(context), round_index=1)
    pcfg = {"epsilon": 4., "delta": 1e-5, "clipping_norm": 1., "n_samples": 2}
    effective = dp_harness.effective_dpsgd_mechanism(
        epsilon=4., delta=1e-5, clipping_norm=1., n_samples=2, batch_size=1,
        local_epochs=2, num_rounds=1)
    path = tmp_path / "public-image.png"
    Image.fromarray(np.full((32, 32, 3), 127, np.uint8)).save(path)
    histories = []
    original = PRVAccountant.step

    def record_step(self, *, noise_multiplier, sample_rate):
        histories[-1].append((noise_multiplier, sample_rate))
        return original(self, noise_multiplier=noise_multiplier, sample_rate=sample_rate)

    with mock.patch.object(PRVAccountant, "step", record_step):
        histories.append([])
        trainable = params.load_user_model(cfg, 512, "bce_logits")
        client_app._dp_fit_vision(trainable, [[str(path)], [str(path)]],
            np.array([0., 1.], np.float32), pcfg, pins, cfg,
            master=b"a" * 32, effective_privacy=effective)
        del trainable
        histories.append([])
        frozen_cfg = dict(cfg, **{"backbone-mode": "frozen"})
        frozen = params.load_user_model(frozen_cfg, 512, "bce_logits")
        client_app._dp_fit(frozen, np.zeros((2, 512), np.float32),
            np.array([0., 1.], np.float32), pcfg, pins, 2, frozen_cfg,
            master=b"a" * 32,
            noise_multiplier=effective["noise_multiplier"])
    assert histories[0] == histories[1]
    assert histories[0] == [(effective["noise_multiplier"], .5)] * 4


@pytest.mark.parametrize("mode", ["finetune", "scratch"])
def test_trainable_request_requires_the_node_manifest_permission(tmp_path, mode):
    cfg = config(mode)
    manifest = dict(cfg, **{"dp-track": "neural", "data_type": "image", "dp-unit": "patient"})
    path = tmp_path / "manifest.json"
    context = SimpleNamespace(node_config={"manifest-dir": str(tmp_path)}, run_config=cfg)
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="custodian|trainable"):
        task.load_pinned_run_config(context)
    manifest["allow-trainable-backbone"] = True
    path.write_text(json.dumps(manifest))
    admitted = task.load_pinned_run_config(context)
    assert admitted["backbone-mode"] == mode
