"""FedProx is ordered DP post-processing, with zero a byte-exact identity."""
import copy
import inspect
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import torch
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app" / "dsflower_runner"))
from dsflower_runner import strategy, client_app, dp_harness, params, server_app, task


def local(mu):
    return strategy.canonical_local_strategy({"strategy": "fedprox", "strategy-mu": mu}, "neural")


@pytest.mark.parametrize("bad", [True, "0.1", None, [], float("nan"), float("inf"), -.1, 1.1])
def test_mu_requires_bounded_finite_numeric_scalar(bad):
    with pytest.raises(ValueError): local(bad)


@pytest.mark.parametrize("track", ["native_tree", "xgboost", "association", "validation"])
@pytest.mark.parametrize("mu", [0, .1])
def test_unsupported_tracks_reject_before_zero_normalization(track, mu):
    with pytest.raises(ValueError, match="unsupported"):
        strategy.canonical_local_strategy({"strategy": "fedprox", "strategy-mu": mu}, track)


def test_unknown_and_inactive_strategy_fields_fail_closed():
    for config in ({"strategy": "fedavg", "strategy-mu": 0},
                   {"strategy": "fedprox", "strategy-mu": .1, "strategy-eta": 2},
                   {"strategy": "fedprox"}):
        with pytest.raises(ValueError): strategy.canonical_local_strategy(config)


def pins(mu, l1=.2):
    return {"strategy": local(mu), "loss_name": "mse", "n_classes": 2,
            "batch_size": 2, "local_epochs": 1, "num_rounds": 1, "round_index": 1,
            "learning_rate": .5,
            "optimizer": {"name": "sgd", "weight_decay": 0., "l1_penalty": l1,
                          "momentum": 0., "nesterov": False},
            "scheduler": {"name": "none", "step_size": 1, "gamma": .1, "min_lr": 0.}}


def test_public_horizon_validation_and_zero_bypass():
    p = pins(1)
    p.update(local_epochs=2, num_rounds=2)
    p["scheduler"].update(name="exponential", gamma=2)
    with pytest.raises(ValueError, match="every step"): strategy.validate_prox_horizon(p)
    p["strategy"] = local(0)
    strategy.validate_prox_horizon(p)


def test_after_noise_then_l1_then_prox_and_fixed_round_reference():
    model = torch.nn.Linear(1, 1, bias=False)
    with torch.no_grad(): model.weight.fill_(2.)
    model._dsflower_release_keys = ("weight",)
    seen = []
    def already_private(model, optimizer, loader, **kwargs):
        seen.append(kwargs)
        def step():
            with torch.no_grad():
                model.weight.add_(2.)
        optimizer.step = step
        # Two steps establish that the reference remains the round input.
        batch = (torch.ones(2, 1), torch.zeros(2, 1))
        return model, optimizer, [batch, batch], None
    with mock.patch.object(dp_harness, "make_private_dpsgd", side_effect=already_private), \
         mock.patch.object(dp_harness, "assert_releasable"):
        released, _ = client_app._dp_fit(model, np.ones((2,1), np.float32), np.zeros(2, np.float32),
            {"clipping_norm": 1., "epsilon": 2., "delta": 1e-5}, pins(.5), 2, {}, b"k"*32, 1.)
    # ((2 + 2 - .1) - .25*(3.9-2)) = 3.425; repeat around fixed 2.
    expected = torch.tensor([[2.]], dtype=torch.float32)
    for _ in range(2):
        expected.add_(2)
        expected.copy_(torch.sign(expected)*torch.clamp(expected.abs()-.1, min=0))
        expected.add_(expected - 2, alpha=-.25)
    assert released[0].tobytes() == expected.numpy().tobytes()
    assert seen[0]["epsilon"] == 2. and seen[0]["delta"] == 1e-5 and seen[0]["clipping_norm"] == 1.
    assert "prox" not in inspect.getsource(dp_harness.loss_from_allowlist).lower()


def test_schedule_uses_current_parameter_group_lr():
    first, second = torch.nn.Parameter(torch.tensor([4.])), torch.nn.Parameter(torch.tensor([8.]))
    optimizer = torch.optim.SGD([{"params": [first], "lr": .2}, {"params": [second], "lr": .8}])
    reference = {id(first): torch.tensor([2.]), id(second): torch.tensor([2.])}
    strategy.apply_neural_prox(optimizer, reference, local(.5))
    assert first.item() == pytest.approx(3.8)
    assert second.item() == pytest.approx(5.6)


def test_mu_zero_is_byte_identical_to_fedavg_without_any_arithmetic():
    assert local(0) == strategy.canonical_local_strategy({"strategy": "fedavg"})
    strategy.apply_neural_prox(None, None, local(0))
    arrays = [np.array([-0., 1.], dtype=np.float32)]
    assert strategy.apply_gated_prox(arrays, None, local(0)) is arrays


@pytest.mark.parametrize("sample_aggregate", [False, True])
def test_hook_prox_runs_after_complete_gate_before_dtype_conversion(sample_aggregate, tmp_path, monkeypatch):
    secret = tmp_path / "node-secret"
    secret.write_text("73" * 32)
    secret.chmod(0o600)
    monkeypatch.setenv("DSFLOWER_NODE_SECRET_FILE", str(secret))
    from dsflower_runner import tier2_lib
    incoming = [np.array([2.], dtype=np.float64)]
    gate_result = [np.array([3.123456789], dtype=np.float64)]
    caps = {"subprocess": True, "net_lock": True, "fs_isolation": True}
    policy = {"sample_aggregate": sample_aggregate, "sa_blocks": 2, "hook_enabled": True,
              "clipping_norm": 1., "epsilon": 2., "delta": 1e-5, "egress_time_pad": 0}
    prox = strategy.canonical_local_strategy({"strategy": "fedprox", "strategy-mu": .3}, "egress")
    with mock.patch.object(tier2_lib, "hook_execution_caps", return_value=caps), \
         mock.patch.object(tier2_lib, "_pinned_user_package", return_value="/public/hook.py"), \
         mock.patch.object(tier2_lib, "_run_isolated", return_value=incoming), \
         mock.patch.object(tier2_lib.dp_harness, "output_perturbation", return_value=gate_result), \
         mock.patch.object(tier2_lib.dp_harness, "sample_and_aggregate", return_value=gate_result):
        got = tier2_lib.gated_local_update("hook", incoming, np.ones((2,1)), np.ones(2), {}, policy,
              seed=b"s"*32, execution_seed=b"e"*32, hook_caps=caps, pad_release=False,
              local_strategy=prox)
    expected = (gate_result[0] - .3*(gate_result[0]-incoming[0])).astype(np.float32)
    assert got[0].tobytes() == expected.tobytes()


def test_server_uses_complete_equal_weight_fedavg():
    built = server_app._build_strategy({"strategy": "fedprox", "strategy-mu": .2}, 2, "neural")
    assert isinstance(built, server_app._StrictFedAvg)
    assert built.expected_train_nodes == 2


def test_raw_config_cannot_override_manifest_mu(tmp_path):
    manifest = {"dp-track": "egress", "strategy": "fedprox", "strategy-mu": .2,
                "app-params-b64": "e30=", "app-params-sha256": "a"*64,
                "num-server-rounds": 1, "task-type": "classification", "num-classes": 2,
                "num-features": 1}
    context = SimpleNamespace(run_config=dict(manifest, **{"strategy-mu": .3}), node_config={})
    with mock.patch.object(task, "_load_manifest", return_value=manifest):
        with pytest.raises(ValueError, match="strategy"):
            task.load_pinned_run_config(context)
