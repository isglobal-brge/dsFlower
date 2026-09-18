"""Subject-level segmentation gates: math, all parameters, assets and authority."""
import base64
import copy
import json
import os
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import torch
from PIL import Image
from opacus import GradSampleModule
from opacus.validators import ModuleValidator
from torch.utils.data import DataLoader, TensorDataset

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "flower_app"))
from dsflower_runner import (segmentation as seg, client_app, dp_harness,
                             model_spec, params, seeding, task, vision, server_app)

torch.set_num_threads(1)


def config():
    return {"model-spec-b64": base64.b64encode(json.dumps(seg.decoder_spec()).encode()).decode(),
            "task-type": "segmentation", "loss-name": "segmentation_bce_dice",
            "data-kind": "image", "backbone": seg.BACKBONE,
            "vision-extractor-profile": seg.PROFILE, "num-features": seg.FEATURE_DIM,
            "num-classes": 2, "image-size": 128, "segmentation-alpha": .5,
            "segmentation-smooth": 1.0, "mask-vocabulary": "0,255",
            "segmentation-selection": seg.SELECTION,
            "segmentation-preprocessing": seg.PREPROCESSING,
            "segmentation-checkpoint-sha256": seg.CHECKPOINT_SHA256,
            "segmentation-output-shape": "1,128,128"}


def decoder():
    return params.load_user_model(config(), seg.FEATURE_DIM, "segmentation_bce_dice")


def targets(n=3):
    value = torch.zeros(n, 2, 128, 128)
    value[:, 1] = 1
    if n > 1:
        value[1, 0, 30:90, 20:100] = 1
    return value


@pytest.mark.parametrize("alpha", [.5, 1.0])
def test_loss_independent_numeric_reference_empty_and_invalid(alpha):
    logits = torch.stack([torch.full((1, 128, 128), v) for v in (-1., .25, 2.)])
    y = targets()
    y[2, 1] = 0
    observed = seg.loss_factory({"segmentation-alpha": alpha})(logits, y)
    ref = []
    for i in range(3):
        z, target = logits[i].numpy().astype(float), y[i, :1].numpy()
        prob = 1 / (1 + np.exp(-z))
        bce = (np.logaddexp(0, z) - z * target).mean()
        dice = (2 * (prob * target).sum() + 1) / (prob.sum() + target.sum() + 1)
        ref.append(float(y[i, 1, 0, 0]) * (alpha * bce + (1-alpha) * (1-dice)))
    assert float(observed) == pytest.approx(np.mean(ref), rel=1e-6)


def grad_samples(model, x, y, loss):
    wrapped = GradSampleModule(copy.deepcopy(model), loss_reduction="mean")
    loss(wrapped(x), y).backward()
    result = {name.removeprefix("_module."): p.grad_sample.detach().clone()
              for name, p in wrapped.named_parameters()}
    wrapped.remove_hooks()
    return result


@pytest.mark.parametrize("device", ["cpu"] + (["cuda"] if torch.cuda.is_available() else []))
@pytest.mark.parametrize("alpha", [.5, 1.0])
def test_every_parameter_matches_one_subject_and_is_independent(alpha, device):
    torch.manual_seed(19)
    model, x, y = decoder().to(device), torch.randn(3, seg.FEATURE_DIM, device=device), targets().to(device)
    loss = seg.loss_factory({"segmentation-alpha": alpha})
    gradients = grad_samples(model, x, y, loss)
    assert len(gradients) == 6
    for i in range(3):
        model.zero_grad()
        loss(model(x[i:i+1]), y[i:i+1]).backward()
        for name, p in model.named_parameters():
            torch.testing.assert_close(gradients[name][i], p.grad, atol=2e-6, rtol=2e-4)
    changed_x, changed_y = x.clone(), y.clone()
    changed_x[1] += 3
    changed_y[1, 0] = 1 - changed_y[1, 0]
    changed = grad_samples(model, changed_x, changed_y, loss)
    for name in gradients:
        torch.testing.assert_close(gradients[name][[0, 2]], changed[name][[0, 2]])
    assert any(not torch.equal(gradients[name][1], changed[name][1]) for name in gradients)


def test_batch_wide_dice_negative_control_detects_coupling():
    def coupled(z, y):
        p, target = z.sigmoid(), y[:, :1]
        return 1 - (2 * (p * target).sum() + 1) / (p.sum() + target.sum() + 1)
    model, x, y = decoder(), torch.randn(3, seg.FEATURE_DIM), targets()
    before = grad_samples(model, x, y, coupled)
    y[1, 0] = 1 - y[1, 0]
    after = grad_samples(model, x, y, coupled)
    assert any(not torch.allclose(before[k][0], after[k][0]) for k in before)


def test_training_batchnorm_negative_control_detects_coupling():
    model = torch.nn.Sequential(torch.nn.Conv2d(3, 2, 1), torch.nn.BatchNorm2d(2))
    assert not ModuleValidator.is_valid(model)
    x = torch.randn(3, 3, 8, 8)
    before = model(x)[0].detach().clone()
    x[1] += 100
    assert not torch.allclose(before, model(x)[0])


def test_decoder_has_only_dp_trained_parameters_and_spatial_admission_is_narrow():
    model = decoder()
    assert sum(p.numel() for p in model.parameters()) == 41537
    assert not list(model.buffers()) and all(p.requires_grad for p in model.parameters())
    dp_harness.assert_stock_architecture(model)
    dp_harness.assert_releasable(model)
    assert tuple(model(torch.zeros(2, seg.FEATURE_DIM)).shape) == (2, 1, 128, 128)
    with pytest.raises(ValueError):
        model_spec.build_from_spec(seg.decoder_spec(), seg.FEATURE_DIM, 1)
    bad = seg.decoder_spec()
    bad["layers"][-1]["out_channels"] = 2
    with pytest.raises(ValueError):
        model_spec.build_from_spec(bad, seg.FEATURE_DIM, 1, output_shape=seg.OUTPUT_SHAPE)
    assert server_app._build_initial_model(config())(torch.zeros(1, seg.FEATURE_DIM)).shape == (1, 1, 128, 128)


@pytest.mark.parametrize("key,value", [
    ("segmentation-alpha", .6), ("segmentation-smooth", 0),
    ("segmentation-checkpoint-sha256", "0" * 64),
    ("segmentation-selection", "largest-mask"), ("image-size", 256),
    ("num-features", 512), ("mask-vocabulary", "auto"),
    ("segmentation-output-shape", "1,64,64"), ("task-type", "classification"),
    ("validation-bins", 20), ("cv-folds", 3), ("resampling-method", "holdout"),
])
def test_bad_public_pins_rejected_before_private_access(key, value):
    cfg = config()
    cfg[key] = value
    with mock.patch.object(task, "_read_staged_frame", side_effect=AssertionError("private read")):
        with pytest.raises(ValueError):
            seg.validate_config(cfg)


def test_all_invalid_and_empty_draws_have_zero_gradients_but_noise_steps():
    class Draws(seeding.SecureNumpyRng):
        def __init__(self):
            super().__init__(b"a" * 32)
            self.calls = 0
        def bernoulli_mask_one_in(self, denominator, n):
            self.calls += 1
            return np.zeros(n, bool) if self.calls == 1 else np.ones(n, bool)
    model = decoder()
    initial = [p.detach().clone() for p in model.parameters()]
    dataset = TensorDataset(torch.zeros(3, seg.FEATURE_DIM), torch.zeros(3, 2, 128, 128))
    rng = Draws()
    private, optimizer, loader, engine = dp_harness.make_private_dpsgd(
        model, torch.optim.SGD(model.parameters(), lr=.01), DataLoader(dataset, batch_size=2),
        clipping_norm=1, epsilon=4, delta=1e-5, local_epochs=1,
        noise_multiplier=1., secure_sampling_rng=rng, secure_noise_rng=rng)
    loss = seg.loss_factory({})
    lengths = []
    for x, y in loader:
        lengths.append(len(x))
        optimizer.zero_grad()
        loss(private(x), y).backward()
        assert all(torch.count_nonzero(p.grad_sample) == 0 for p in private.parameters())
        optimizer.step()
    assert lengths == [0, 3]
    assert engine.accountant.history == [(1., .5, 2)]
    assert optimizer.expected_batch_size == 1
    assert all(torch.isfinite(p).all() for p in private.parameters())
    assert all(not torch.equal(a, b) for a, b in zip(initial, private.parameters()))


class FixtureEncoder(torch.nn.Module):
    def forward(self, x):
        return F.adaptive_avg_pool2d(x.mean(1, keepdim=True), (16, 16)).repeat(1, 128, 1, 1)


from torch.nn import functional as F


def fixture(tmp_path):
    root = str(tmp_path)
    Image.fromarray(np.full((16, 20, 3), 128, np.uint8)).save(tmp_path / "image.png")
    for name, corner in (("a", 0), ("b", 10)):
        mask = np.zeros((16, 20), np.uint8)
        mask[:, corner:corner+5] = 255
        Image.fromarray(mask).save(tmp_path / (name + ".png"))
    frame = pd.DataFrame({"patient": ["p1", "p1", "p1", "p2", "p3"],
        "image_id": ["b", "a", "a", "a", "a"],
        "path": ["missing.png", "image.png", "image.png", "image.png", "../escape.png"],
        "mask": ["missing.png", "a.png", "b.png", "__dsflower_empty_mask__", "a.png"],
        "empty": [0, 0, 0, 1, 0]})
    frame.to_csv(tmp_path / "samples.csv", index=False)
    manifest = dict(config(), data_type="image", samples_file="samples.csv", target_column="mask",
                    sample_id_col="image_id", mask_empty_col="empty", patient_column="patient",
                    n_samples=5, n_units=3, assets={
                        "images": {"root": root, "path_col": "path"},
                        "masks": {"root": root, "path_col": "mask"}})
    manifest.update({"dp-unit": "patient", "patient-id-canonicalization": "trim-utf8-v2"})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return SimpleNamespace(node_config={"manifest-dir": root}, run_config=config()), frame


def test_subject_selection_union_invalid_and_declared_empty_keep_n(tmp_path):
    context, frame = fixture(tmp_path)
    X, y, subjects, n = seg.load_subject_tensors(context, config(), FixtureEncoder(), "cpu")
    assert n == 5 and len(subjects) == 3
    assert y[:, 1, 0, 0].tolist() == [1, 1, 0]
    assert y[0, 0].sum() == 8192 and y[1, 0].sum() == 0
    assert not X[2].any() and not y[2].any()
    frame.loc[0, "path"] = "image.png"  # unselected image cannot change tensors
    frame.to_csv(tmp_path / "samples.csv", index=False)
    X2, y2, _, _ = seg.load_subject_tensors(context, config(), FixtureEncoder(), "cpu")
    np.testing.assert_array_equal(X, X2)
    np.testing.assert_array_equal(y, y2)
    frame.loc[1, "path"] = "bad.png"  # selected conflict invalidates p1 only
    frame.to_csv(tmp_path / "samples.csv", index=False)
    X3, y3, _, _ = seg.load_subject_tensors(context, config(), FixtureEncoder(), "cpu")
    assert not X3[0].any() and not y3[0].any()
    np.testing.assert_array_equal(X[1:], X3[1:])
    np.testing.assert_array_equal(y[1:], y3[1:])


def test_mask_vocab_geometry_corruption_and_symlink_containment(tmp_path):
    context, _ = fixture(tmp_path)
    for name, array in [("vocab", np.full((16, 20), 7, np.uint8)),
                        ("geometry", np.zeros((5, 5), np.uint8))]:
        path = tmp_path / (name + ".png")
        Image.fromarray(array).save(path)
        assert seg.read_pair(tmp_path / "image.png", [path], [0], "0,255")[2] == 0
    (tmp_path / "corrupt.png").write_text("not an image")
    assert seg.read_pair(tmp_path / "corrupt.png", [tmp_path / "a.png"], [0], "0,255")[2] == 0
    (tmp_path / "escape.png").symlink_to(tmp_path.parent / "outside.png")
    with pytest.raises(ValueError):
        task._resolve_image_path(str(tmp_path), "escape.png")


def test_csv_image_ids_and_validity_are_not_inferred_from_other_subjects(tmp_path):
    context, frame = fixture(tmp_path)
    frame["image_id"] = ["1", "001", "001", "01", "1"]
    frame["empty"] = ["FALSE", "FALSE", "FALSE", "TRUE", "bad"]
    frame.to_csv(tmp_path / "samples.csv", index=False)
    X, y, _, _ = seg.load_subject_tensors(context, config(), FixtureEncoder(), "cpu")
    assert y[:, 1, 0, 0].tolist() == [1, 1, 0]
    frame.loc[4, "empty"] = "FALSE"
    frame.to_csv(tmp_path / "samples.csv", index=False)
    X2, y2, _, _ = seg.load_subject_tensors(context, config(), FixtureEncoder(), "cpu")
    np.testing.assert_array_equal(X, X2)
    np.testing.assert_array_equal(y, y2)


def test_real_manifest_uses_authoritative_data_type():
    manifest = config()
    manifest["data_type"] = manifest.pop("data-kind")
    seg.validate_config(manifest)
    with pytest.raises(ValueError, match="conflicts"):
        seg.validate_config(dict(manifest, **{"data-kind": "tabular"}))


def test_channel_b_empty_convention_and_foreground_strata():
    true = np.zeros((4, 1, 128, 128))
    true[2:, :, :64] = 1
    prob = true.copy()
    prob[1] = 1
    prob[3] = 0
    metrics = seg.channel_b_metrics(prob, true)
    assert metrics == {"mean_dice": .5, "mean_iou": .5,
                       "foreground_dice": .5, "empty_dice": .5,
                       "foreground_iou": .5, "empty_iou": .5}


def test_semantic_identity_binds_every_effective_pin_and_tensor_not_paths():
    cfg, pins = config(), {"batch_size": 16, "round_index": 1}
    x, y = np.zeros((2, seg.FEATURE_DIM), np.float32), targets(2).numpy()
    def digest(c, xx=x, yy=y):
        selected, _ = client_app._neural_seed_contract(c, pins, {})
        return seeding._semantic_digest("seg-test", selected, {"policy_hash": "1" * 64}, 1,
                                        private_arrays=(xx, yy), execution_fingerprint={})
    original = digest(cfg)
    assert digest(dict(cfg, run_token="new", samples_file="/new/path")) == original
    for key in (*seg.PIN_KEYS, "vision-extractor-profile"):
        assert digest(dict(cfg, **{key: str(cfg[key]) + "changed"})) != original
    changed = y.copy()
    changed[0, 1] = 0
    assert digest(cfg, yy=changed) != original
    changed_x = x.copy()
    changed_x[0, 0] = 1
    assert digest(cfg, xx=changed_x) != original


def test_train_path_uses_subject_n_and_retries_identically(tmp_path):
    context, _ = fixture(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.update({"batch-size": 2, "local-epochs": 1, "num-server-rounds": 2,
                     "learning-rate": .01})
    manifest_path.write_text(json.dumps(manifest))
    pins = task.load_run_pins(context)
    pins["round_index"] = 1
    pcfg = {"epsilon": 4., "delta": 1e-5, "clipping_norm": 1., "n_samples": 5}
    model = decoder()
    with mock.patch.object(seg, "prepare_encoder", return_value=(FixtureEncoder(), "cpu")), \
            mock.patch.object(seeding, "_node_secret", return_value=b"s" * 32), \
            mock.patch.object(dp_harness, "effective_dpsgd_mechanism",
                              wraps=dp_harness.effective_dpsgd_mechanism) as mechanism:
        first, n = client_app._train_segmentation(context, config(), pcfg, pins, copy.deepcopy(model))
        second, n2 = client_app._train_segmentation(context, config(), pcfg, pins, copy.deepcopy(model))
    assert n == n2 == 3
    assert all(call.kwargs["n_samples"] == 3 for call in mechanism.call_args_list)
    assert len(first) == len(list(model.parameters())) == 6
    for a, b in zip(first, second):
        np.testing.assert_array_equal(a, b)


def test_checkpoint_and_frozen_batchnorm_profile_when_cache_available():
    path = os.path.join(torch.hub.get_dir(), "checkpoints", "resnet18-f37072fd.pth")
    if not os.path.isfile(path):
        pytest.skip("custodian-pinned checkpoint not seeded in local cache")
    encoder, device = seg.prepare_encoder(config())
    assert not encoder.training and not any(p.requires_grad for p in encoder.parameters())
    before = {name: b.detach().clone() for name, b in encoder.named_buffers()}
    with torch.no_grad():
        x = torch.randn(3, 3, 128, 128, device=device)
        first = encoder(x).clone()
        changed = x.clone()
        changed[1] += 100
        after = encoder(changed)
    torch.testing.assert_close(first[[0, 2]], after[[0, 2]])
    for name, buffer in encoder.named_buffers():
        torch.testing.assert_close(before[name], buffer, rtol=0, atol=0)


def test_encoder_preflight_failure_does_not_read_private_records(tmp_path):
    context, _ = fixture(tmp_path)
    with mock.patch.object(seg, "prepare_encoder", side_effect=ValueError("checkpoint")), \
            mock.patch.object(seg, "load_subject_tensors") as load:
        with pytest.raises(ValueError, match="checkpoint"):
            client_app._train_segmentation(context, config(), {}, {}, decoder())
    load.assert_not_called()


def test_segmentation_pins_full_float32_arithmetic():
    matmul = torch.backends.cuda.matmul.allow_tf32
    cudnn = torch.backends.cudnn.allow_tf32
    try:
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True
        decoder()
        assert torch.backends.cuda.matmul.allow_tf32 is False
        assert torch.backends.cudnn.allow_tf32 is False
        assert torch.get_float32_matmul_precision() == "highest"
    finally:
        torch.backends.cuda.matmul.allow_tf32 = matmul
        torch.backends.cudnn.allow_tf32 = cudnn


def test_encoder_loads_the_exact_verified_bytes_despite_cache_replacement(tmp_path):
    import shutil
    import torchvision.models as models
    source = os.path.join(torch.hub.get_dir(), "checkpoints", "resnet18-f37072fd.pth")
    if not os.path.isfile(source):
        pytest.skip("custodian-pinned checkpoint not seeded in local cache")
    cache = tmp_path / "checkpoints"
    cache.mkdir()
    target = cache / "resnet18-f37072fd.pth"
    shutil.copyfile(source, target)
    expected = torch.load(source, map_location="cpu", weights_only=True)["conv1.weight"]
    original = models.resnet18
    def replace_cache(*, weights):
        assert weights is None  # never let torchvision reopen/fetch its cache
        target.write_bytes(b"replaced after verification")
        return original(weights=None)
    with mock.patch.object(torch.hub, "get_dir", return_value=str(tmp_path)), \
            mock.patch.object(models, "resnet18", side_effect=replace_cache):
        encoder, _ = seg.prepare_encoder(config())
    torch.testing.assert_close(encoder[0].weight.cpu(), expected, rtol=0, atol=0)


def test_two_round_dp_adam_matches_independent_subject_gradient_reference(tmp_path):
    """The production loop must match explicit clipping/noise for every parameter."""
    import hashlib
    import math

    cfg = dict(config(), **{"dp-unit": "patient", "optimizer-name": "adam",
        "learning-rate": .001, "batch-size": 2, "local-epochs": 1,
        "num-server-rounds": 2})
    (tmp_path / "manifest.json").write_text(json.dumps(cfg))
    pins = task.load_run_pins(SimpleNamespace(node_config={"manifest-dir": str(tmp_path)}))
    torch.manual_seed(713)
    x = torch.randn(3, seg.FEATURE_DIM) * 5.
    y = targets()
    y[2, 1] = 0  # Retained invalid subject, including its Poisson inclusion.
    reference = decoder()
    initial = [a.copy() for a in params.get_torch_params(reference)]
    actual = [a.copy() for a in initial]
    mechanism = dp_harness.effective_dpsgd_mechanism(8, 1e-5, 1., 3, 2, 1, 2)
    sigma = mechanism["noise_multiplier"]
    steps = math.ceil(len(x) / 2)
    divisor = max(1, len(x) // steps)
    clipped_subjects = 0
    for rnd in (1, 2):
        master = hashlib.sha256(("public-reference-%d" % rnd).encode()).digest()
        optimizer = torch.optim.Adam(reference.parameters(), lr=.001)
        sampler = seeding.np_rng(seeding.sub_seed(master, "sample"))
        noise = seeding.np_rng(seeding.sub_seed(master, "noise"))
        for _ in range(steps):
            indices = np.flatnonzero(sampler.bernoulli_mask_one_in(steps, len(x)))
            sums = [torch.zeros_like(p) for p in reference.parameters()]
            for i in indices:
                reference.zero_grad()
                z = reference(x[i:i + 1])
                mask = y[i:i + 1, :1]
                bce = torch.nn.functional.binary_cross_entropy_with_logits(z, mask)
                prob = z.sigmoid()
                dice = (2 * (prob * mask).sum() + 1) / (prob.sum() + mask.sum() + 1)
                (y[i, 1, 0, 0] * (.5 * bce + .5 * (1 - dice))).backward()
                gradients = [p.grad.detach().clamp(-1, 1) for p in reference.parameters()]
                norm = torch.stack([g.square().sum() for g in gradients]).sum().sqrt()
                clipped_subjects += int(norm > 1)
                factor = (1 / (norm + 1e-6)).clamp(max=1)
                for total, g in zip(sums, gradients):
                    total.add_(g * factor)
            optimizer.zero_grad()
            for p, total in zip(reference.parameters(), sums):
                sampled = torch.as_tensor(noise.normal(0., sigma, size=tuple(p.shape)), dtype=p.dtype)
                p.grad = (total + sampled) / divisor
            optimizer.step()
        production = decoder()
        params.set_torch_params(production, actual)
        with mock.patch.object(torch.cuda, "is_available", return_value=False):
            actual, n = client_app._dp_fit(production, x.numpy(), y.numpy(),
                {"epsilon": 8, "delta": 1e-5, "clipping_norm": 1., "n_samples": 3},
                dict(pins, round_index=rnd), 3, cfg, master, sigma)
        assert n == 3
        for observed, expected in zip(actual, params.get_torch_params(reference)):
            np.testing.assert_allclose(observed, expected, atol=2e-7, rtol=2e-5)
    assert clipped_subjects > 0
    assert all(not np.array_equal(a, b) for a, b in zip(initial, actual))
