"""Focused regressions for exact DP-SGD accounting and finite release gates."""

import base64
import io
import math
import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader, TensorDataset
from flwr.common import ArrayRecord, Message, RecordDict


FLOWER_APP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import (client_app, dp_harness, egress_child,
                             model_spec, params, seeding, task, tier2_lib,
                             vision, server_app)  # noqa: E402


def _hook_wire(app_params=None, rounds=2, task_type="classification",
               num_classes=2):
    raw = json.dumps(
        {} if app_params is None else app_params,
        ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "app-params-b64": base64.b64encode(raw).decode("ascii"),
        "app-params-sha256": hashlib.sha256(raw).hexdigest(),
        "num-server-rounds": rounds,
        "task-type": task_type,
        "num-classes": num_classes,
    }


def _package_hash(package_dir):
    digest = hashlib.sha256()
    entries = []
    for current, dirs, files in os.walk(package_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for filename in files:
            if not filename.endswith((".pyc", ".pyo")):
                full = os.path.join(current, filename)
                rel = os.path.relpath(full, package_dir).replace(os.sep, "/")
                entries.append((rel, full))
    for rel, full in sorted(entries):
        with open(full, "rb") as handle:
            content = handle.read()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\n")
        digest.update(content)
        digest.update(b"\x00")
    return digest.hexdigest()


class ManifestPrivacyContractTests(unittest.TestCase):
    def test_adjacency_and_hook_resource_limits_are_server_pinned(self):
        manifest = {
            "privacy-policy-sha256": "1" * 64,
            "privacy-epsilon": 1.0,
            "privacy-delta": 1e-5,
            "privacy-clipping_norm": 1.0,
            "privacy-adjacency": "replace_one",
            "privacy-sample_aggregate": 0,
            "privacy-sa_blocks": 8,
            "privacy-egress_time_pad": 0,
            "privacy-egress_timeout": 900,
            "privacy-egress_memory_mb": 4096,
            "privacy-egress_file_mb": 512,
            "privacy-egress_processes": 32,
            "privacy-hook_enabled": 0,
            "n_samples": 10,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            path = os.path.join(manifest_dir, "manifest.json")
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)
            cfg = task.load_privacy_config(context)
            self.assertEqual(cfg["adjacency"], "replace_one")
            self.assertEqual(cfg["egress_memory_mb"], 4096)
            self.assertEqual(cfg["egress_file_mb"], 512)
            self.assertEqual(cfg["egress_processes"], 32)

            manifest["privacy-adjacency"] = "add_remove"
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)
            with self.assertRaisesRegex(ValueError, "replace-one"):
                task.load_privacy_config(context)

    def test_hook_child_receives_server_pinned_resource_limits(self):
        captured = {}

        class FailedChild:
            pid = 424242
            returncode = 1

            def wait(self, timeout):
                captured["timeout"] = timeout

        def fake_popen(*args, **kwargs):
            captured["env"] = kwargs["env"]
            return FailedChild()

        pcfg = {
            "egress_memory_mb": 2048,
            "egress_file_mb": 256,
            "egress_processes": 16,
        }
        with mock.patch.object(
                tier2_lib.subprocess, "Popen", side_effect=fake_popen), \
                mock.patch.object(tier2_lib, "_killpg"):
            result = tier2_lib._run_isolated(
                "example_hook", "/unused/__init__.py",
                [np.zeros(2)], np.zeros((1, 1)), np.zeros(1), {
                    "app_params": {}, "round_index": 1, "num_rounds": 1,
                    "task": "classification", "num_classes": 2,
                }, pcfg,
                {}, timeout=7, child_seed=b"c" * 32)

        self.assertIsNone(result)
        self.assertEqual(captured["env"]["DSF_RLIMIT_AS"], str(2048 * 1024 * 1024))
        self.assertEqual(captured["env"]["DSF_RLIMIT_FSIZE"], str(256 * 1024 * 1024))
        self.assertEqual(captured["env"]["DSF_RLIMIT_NPROC"], "16")
        self.assertEqual(captured["env"]["DSF_RLIMIT_CPU"], "7")
        self.assertEqual(captured["env"]["DSF_DETERMINISTIC_SEED"],
                         (b"c" * 32).hex())
        self.assertEqual(captured["env"]["PYTHONHASHSEED"],
                         str(int.from_bytes(b"c" * 4, "big")))
        self.assertEqual(captured["env"]["CUBLAS_WORKSPACE_CONFIG"], ":4096:8")

    def test_hook_requires_independent_resource_isolation_attestation(self):
        pcfg = {
            "hook_enabled": True, "egress_timeout": 30,
            "egress_time_pad": 35, "sample_aggregate": False,
        }
        caps = {
            "subprocess": True, "net_lock": True,
            "fs_isolation": True,
        }
        with mock.patch.dict(os.environ, {
                "DSF_SAA_SANDBOX_OK": "1",
        }, clear=True):
            self.assertIsNone(tier2_lib.hook_execution_caps(pcfg, caps))
        with mock.patch.dict(os.environ, {
                "DSF_SAA_SANDBOX_OK": "1",
                "DSF_HOOK_RESOURCE_ISOLATION_OK": "1",
        }, clear=True):
            self.assertIs(tier2_lib.hook_execution_caps(pcfg, caps), caps)

    def test_sample_aggregate_padding_wraps_the_release_once(self):
        old = [np.zeros(2, dtype=np.float64)]
        pcfg = {
            "hook_enabled": True, "sample_aggregate": True, "sa_blocks": 3,
            "egress_timeout": 1, "egress_time_pad": 10,
            "clipping_norm": 1.0, "epsilon": 1.0, "delta": 1e-5,
        }
        caps = {"subprocess": True, "net_lock": True, "fs_isolation": True}
        with mock.patch.object(
                tier2_lib, "hook_execution_caps", return_value=caps), \
                mock.patch.object(
                    tier2_lib, "_pinned_user_package", return_value="/hook/__init__.py"), \
                mock.patch.object(
                    tier2_lib, "_run_isolated",
                    side_effect=lambda *args, **kwargs: [
                        array.copy() for array in old]) as run, \
                mock.patch.object(
                    tier2_lib.time, "monotonic", side_effect=[100.0, 101.0]), \
                mock.patch.object(tier2_lib.time, "sleep") as sleep:
            result = tier2_lib.gated_local_update(
                "hookpkg", old, np.zeros((6, 1)), np.zeros(6), {}, pcfg,
                seed=b"s" * 32, execution_seed=b"e" * 32,
                hook_caps=caps)

        self.assertEqual(run.call_count, 3)
        self.assertTrue(all(call.args[-1] == 1 for call in run.call_args_list))
        self.assertTrue(all(len(call.kwargs["child_seed"]) == 32
                            for call in run.call_args_list))
        sleep.assert_called_once_with(9.0)
        self.assertEqual(result[0].shape, old[0].shape)

    def test_hook_noise_key_is_bound_to_the_validated_pre_noise_update(self):
        old = [np.zeros(2, dtype=np.float64)]
        pcfg = {
            "hook_enabled": True, "sample_aggregate": False,
            "egress_timeout": 1, "egress_time_pad": 0,
            "clipping_norm": 1.0, "epsilon": 1.0, "delta": 1e-5,
        }
        caps = {"subprocess": True, "net_lock": True, "fs_isolation": True}
        first = [np.asarray([1.0, 0.0])]
        changed = [np.asarray([2.0, 0.0])]
        noises = []

        def capture_noise(new, _old, **kwargs):
            noises.append(kwargs["rng"].normal(size=8))
            return new

        with mock.patch.object(
                tier2_lib, "hook_execution_caps", return_value=caps), \
                mock.patch.object(
                    tier2_lib, "_pinned_user_package",
                    return_value="/hook/__init__.py"), \
                mock.patch.object(
                    tier2_lib, "_run_isolated",
                    side_effect=[first, first, changed]), \
                mock.patch.object(
                    tier2_lib.dp_harness, "output_perturbation",
                    side_effect=capture_noise):
            for _ in range(3):
                tier2_lib.gated_local_update(
                    "hookpkg", old, np.zeros((2, 1)), np.zeros(2), {}, pcfg,
                    seed=b"s" * 32, execution_seed=b"e" * 32,
                    hook_caps=caps, pad_release=False)

        np.testing.assert_array_equal(noises[0], noises[1])
        self.assertFalse(np.array_equal(noises[0], noises[2]))

    def test_isolated_hook_python_and_numpy_training_are_deterministic(self):
        cfg = {
            "app_params": {}, "round_index": 1, "num_rounds": 1,
            "task": "classification", "num_classes": 2,
        }
        pcfg = {
            "egress_memory_mb": 2048, "egress_file_mb": 64,
            "egress_processes": 16,
        }
        old = [np.zeros(2, dtype=np.float64)]
        with tempfile.TemporaryDirectory() as root:
            package = os.path.join(root, "seeded_hook")
            os.makedirs(package)
            module_file = os.path.join(package, "__init__.py")
            with open(module_file, "w", encoding="utf-8") as handle:
                handle.write(
                    "import random\n"
                    "import numpy as np\n"
                    "def local_update(global_arrays, X, y, cfg):\n"
                    "    value = random.random() + np.random.random()\n"
                    "    return [np.full_like(global_arrays[0], value)]\n")
            common = (
                "seeded_hook", module_file, old, np.zeros((2, 1)),
                np.zeros(2), cfg, pcfg, {}, 10,
            )
            first = tier2_lib._run_isolated(
                *common, child_seed=b"a" * 32)
            replay = tier2_lib._run_isolated(
                *common, child_seed=b"a" * 32)
            changed = tier2_lib._run_isolated(
                *common, child_seed=b"b" * 32)

        self.assertIsNotNone(first)
        self.assertIsNotNone(replay)
        self.assertIsNotNone(changed)
        np.testing.assert_array_equal(first[0], replay[0])
        self.assertFalse(np.array_equal(first[0], changed[0]))

    def test_hostile_npy_shape_is_rejected_before_np_load(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "hostile.npy")
            with open(path, "wb") as handle:
                np.lib.format.write_array_header_1_0(handle, {
                    "descr": np.lib.format.dtype_to_descr(np.dtype(np.float64)),
                    "fortran_order": False,
                    "shape": (10**12,),
                })
            with mock.patch.object(
                    tier2_lib.np, "load",
                    side_effect=AssertionError("np.load must not run")) as load:
                with self.assertRaises(ValueError):
                    tier2_lib._load_expected_f64_npy(path, (1,))
            load.assert_not_called()


class NeuralSemanticConfigTests(unittest.TestCase):
    def test_transport_aliases_canonicalize_to_effective_bounds(self):
        bounds = {"lower": [0.0], "upper": [1.0]}
        encoded = base64.b64encode(json.dumps(
            bounds, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")).decode("ascii")
        pins = {"optimizer": "sgd"}
        privacy = {"policy_hash": "1" * 64}

        direct, _ = client_app._neural_seed_contract(
            {"feature-bounds": bounds, "feature-bounds-b64": "ignored-one"},
            pins, privacy)
        changed_alias, _ = client_app._neural_seed_contract(
            {"feature-bounds": bounds, "feature-bounds-b64": "ignored-two"},
            pins, privacy)
        encoded_only, _ = client_app._neural_seed_contract(
            {"feature-bounds-b64": encoded}, pins, privacy)

        self.assertEqual(direct, changed_alias)
        self.assertEqual(direct, encoded_only)

    def test_backbone_makes_model_fallback_inert(self):
        pins = {"optimizer": "sgd"}
        privacy = {"policy_hash": "1" * 64}
        first = client_app._neural_seed_contract(
            {"backbone": "resnet18", "model": "ignored-one"},
            pins, privacy)
        second = client_app._neural_seed_contract(
            {"backbone": "resnet18", "model": "ignored-two"},
            pins, privacy)
        self.assertEqual(first, second)

    def test_resampling_geometry_is_a_sticky_seed_axis_only_when_present(self):
        cfg = {"model-spec-b64": "e30=", "loss-name": "bce_logits"}
        pins = {"round_index": 1, "batch_size": 2}
        privacy = {"policy_hash": "1" * 64, "n_samples": 8}
        public = (np.asarray([0.25, -0.5], dtype=np.float32),)
        private = (np.asarray([[1.0, 2.0]], dtype=np.float32),)

        plain, _ = client_app._neural_seed_contract(cfg, pins, privacy)
        self.assertNotIn("resampling-geometry-n-units", plain)

        def derive(geometry):
            config, effective_privacy = client_app._neural_seed_contract(
                cfg, pins, privacy, geometry_n_units=geometry)
            return seeding.master_seed(
                "neural-dpsgd/v1", config, effective_privacy, 1,
                public_arrays=public, private_arrays=private,
                execution_fingerprint={"runtime": "fixed-test"})

        with mock.patch.object(
                seeding, "_node_secret", return_value=b"\x51" * 32):
            first = derive(4)
            replay = derive(4)
            changed = derive(5)

        self.assertEqual(first, replay)
        self.assertNotEqual(first, changed)


class DpSgdAccountingTests(unittest.TestCase):
    def test_calibration_cache_reuses_only_the_exact_public_horizon(self):
        cached = dp_harness._cached_noise_multiplier
        cached.cache_clear()
        self.addCleanup(cached.cache_clear)

        with mock.patch.object(
                dp_harness, "calibrate_noise_multiplier",
                side_effect=[3.75, 4.25]) as calibrate:
            first = cached(2.0, 1e-5, 0.25, 4, 16, "opacus-test")
            repeated = cached(2.0, 1e-5, 0.25, 4, 16, "opacus-test")
            changed = cached(2.0, 1e-5, 0.25, 4, 17, "opacus-test")

        self.assertEqual(first, 3.75)
        self.assertEqual(repeated, first)
        self.assertEqual(changed, 4.25)
        self.assertEqual(calibrate.call_count, 2)
        self.assertEqual(cached.cache_info().hits, 1)
        self.assertEqual(cached.cache_info().misses, 2)
        self.assertEqual(cached.cache_info().maxsize, 128)

    def test_replace_one_budget_conversion_matches_two_step_group_privacy(self):
        epsilon, delta = 2.4, 1e-5
        epsilon0, delta0 = dp_harness._replace_one_to_add_remove_budget(
            epsilon, delta)

        self.assertEqual(epsilon0, epsilon / 2.0)
        self.assertEqual(delta0, delta / (1.0 + math.exp(epsilon / 2.0)))
        self.assertEqual(2.0 * epsilon0, epsilon)
        self.assertTrue(math.isclose(
            (1.0 + math.exp(epsilon0)) * delta0,
            delta, rel_tol=1e-15, abs_tol=0.0))

    def test_opacus_calibrator_receives_transformed_add_remove_target(self):
        epsilon, delta = 3.0, 2e-5
        expected_epsilon = epsilon / 2.0
        expected_delta = delta / (1.0 + math.exp(expected_epsilon))

        with mock.patch(
                "opacus.accountants.utils.get_noise_multiplier",
                side_effect=[RuntimeError("PRV unavailable"), 7.25]) as get_noise:
            sigma = dp_harness.calibrate_noise_multiplier(
                epsilon, delta, sample_rate=0.25, total_epochs=4,
                total_steps=16)

        self.assertEqual(sigma, 7.25)
        self.assertEqual(get_noise.call_count, 2)
        for call in get_noise.call_args_list:
            kwargs = call.kwargs
            self.assertEqual(kwargs["target_epsilon"], expected_epsilon)
            self.assertEqual(kwargs["target_delta"], expected_delta)
            self.assertEqual(kwargs["sample_rate"], 0.25)
            self.assertEqual(kwargs["steps"], 16)
        self.assertEqual(
            [call.kwargs["accountant"] for call in get_noise.call_args_list],
            ["prv", "rdp"])

    def test_budget_conversion_rejects_non_finite_or_invalid_targets(self):
        for epsilon, delta in (
                (float("nan"), 1e-5), (float("inf"), 1e-5),
                (0.0, 1e-5), (1.0, float("nan")),
                (1.0, float("inf")), (1.0, 0.0), (1.0, 1.0)):
            with self.subTest(epsilon=epsilon, delta=delta):
                with self.assertRaises(ValueError):
                    dp_harness._replace_one_to_add_remove_budget(epsilon, delta)

    def test_calibration_matches_opacus_loader_rate_and_exact_steps(self):
        dp_harness._cached_noise_multiplier.cache_clear()
        self.addCleanup(dp_harness._cached_noise_multiplier.cache_clear)
        n_samples, batch_size = 127, 32
        local_epochs, num_rounds = 2, 3
        dataset = TensorDataset(torch.randn(n_samples, 3), torch.randn(n_samples, 1))
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
        model = torch.nn.Linear(3, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

        with mock.patch.object(
                dp_harness, "calibrate_noise_multiplier", return_value=1.0) as calibrate:
            _, private_optimizer, private_loader, _ = dp_harness.make_private_dpsgd(
                model, optimizer, loader,
                clipping_norm=1.0, epsilon=1.0, delta=1e-5,
                local_epochs=local_epochs, num_rounds=num_rounds,
                n_samples=n_samples, batch_size=batch_size,
                secure_sampling_rng=_ScriptedSamplingRng(),
                secure_noise_rng=_ScriptedSamplingRng(),
            )

        args = calibrate.call_args.kwargs
        steps_per_epoch = len(loader)
        self.assertTrue(math.isclose(
            args["sample_rate"], 1.0 / steps_per_epoch, rel_tol=0.0, abs_tol=0.0))
        self.assertEqual(
            args["total_steps"], steps_per_epoch * local_epochs * num_rounds)
        self.assertEqual(len(private_loader), steps_per_epoch)
        self.assertEqual(
            private_optimizer.expected_batch_size,
            int(n_samples / steps_per_epoch))
        self.assertEqual(private_loader.batch_sampler.num_samples, n_samples)
        self.assertIsInstance(
            private_loader.batch_sampler,
            dp_harness._SecurePoissonBatchSampler,
        )

    def test_missing_secure_sampling_rng_fails_closed(self):
        dataset = TensorDataset(torch.randn(4, 2), torch.randn(4, 1))
        loader = DataLoader(dataset, batch_size=2)
        model = torch.nn.Linear(2, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

        with self.assertRaisesRegex(RuntimeError, "ChaCha20 sampling RNG"):
            dp_harness.make_private_dpsgd(
                model, optimizer, loader,
                clipping_norm=1.0, epsilon=1.0, delta=1e-5,
                local_epochs=1, noise_multiplier=1.0,
            )


class _ScriptedSamplingRng(seeding.SecureNumpyRng):
    def __init__(self, masks=None):
        self.masks = list(masks or [])
        self.calls = []

    def bernoulli_mask_one_in(self, denominator, size):
        self.calls.append((denominator, size))
        if self.masks:
            return np.asarray(self.masks.pop(0), dtype=np.bool_)
        return np.zeros(size, dtype=np.bool_)

    @staticmethod
    def normal(loc=0.0, scale=1.0, size=None):
        return np.zeros(size, dtype=np.float64)


class SecurePoissonSamplingTests(unittest.TestCase):
    def test_missing_or_untrusted_secure_noise_rng_fails_closed(self):
        dataset = TensorDataset(torch.randn(4, 2), torch.randn(4, 1))
        loader = DataLoader(dataset, batch_size=2)
        model = torch.nn.Linear(2, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        for bad in (None, np.random.default_rng(1)):
            with self.subTest(rng=type(bad).__name__), self.assertRaisesRegex(
                    RuntimeError, "SecureNumpyRng"):
                dp_harness.make_private_dpsgd(
                    model, optimizer, loader,
                    clipping_norm=1.0, epsilon=1.0, delta=1e-5,
                    local_epochs=1, noise_multiplier=1.0,
                    secure_sampling_rng=_ScriptedSamplingRng(),
                    secure_noise_rng=bad,
                )

    def test_sampler_uses_exact_reciprocal_api_and_keeps_empty_draw(self):
        rng = _ScriptedSamplingRng([
            [False, False, False],
            [True, False, True],
        ])
        sampler = dp_harness._SecurePoissonBatchSampler(
            num_samples=3, steps=2, rng=rng)

        self.assertEqual(list(sampler), [[], [0, 2]])
        self.assertEqual(rng.calls, [(2, 3), (2, 3)])
        self.assertEqual(sampler.sample_rate, 0.5)

    def test_chacha_reciprocal_draw_rejects_modulo_bias(self):
        # For denominator 3, uint64 max is the sole rejected word. Its retry (6)
        # is selected; the other positions exercise residues 1, 2, and 0.
        rng = object.__new__(seeding.SecureNumpyRng)
        chunks = [
            b"".join(value.to_bytes(8, "little") for value in (
                (1 << 64) - 1, 1, 2, 3)),
            (6).to_bytes(8, "little"),
        ]
        requested = []

        def fake_bytes(size):
            requested.append(size)
            value = chunks.pop(0)
            self.assertEqual(len(value), size)
            return value

        rng._bytes = fake_bytes
        mask = rng.bernoulli_mask_one_in(3, 4)

        np.testing.assert_array_equal(mask, [True, False, False, True])
        self.assertEqual(requested, [32, 8])

    def test_secure_loader_collates_a_first_empty_batch(self):
        dataset = TensorDataset(
            torch.arange(6, dtype=torch.float32).reshape(3, 2),
            torch.arange(3, dtype=torch.float32).reshape(3, 1),
        )
        loader = DataLoader(dataset, batch_size=2, shuffle=False)
        rng = _ScriptedSamplingRng([
            [False, False, False],
            [True, False, True],
        ])
        private_loader = dp_harness._make_secure_poisson_loader(
            loader, steps_per_epoch=2, secure_sampling_rng=rng)

        batches = list(private_loader)
        self.assertEqual(tuple(batches[0][0].shape), (0, 2))
        self.assertEqual(tuple(batches[0][1].shape), (0, 1))
        torch.testing.assert_close(
            batches[1][0], dataset.tensors[0][torch.tensor([0, 2])])
        self.assertEqual(rng.calls, [(2, 3), (2, 3)])

    def test_empty_batch_still_consumes_one_accounted_private_step(self):
        dataset = TensorDataset(torch.randn(3, 2), torch.randn(3, 1))
        loader = DataLoader(dataset, batch_size=2, shuffle=False)
        rng = _ScriptedSamplingRng([
            [False, False, False],
            [True, False, False],
        ])
        model = torch.nn.Linear(2, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        model, optimizer, private_loader, engine = dp_harness.make_private_dpsgd(
            model, optimizer, loader,
            clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            local_epochs=1, noise_multiplier=1.0,
            secure_sampling_rng=rng,
            secure_noise_rng=rng,
        )

        for xb, yb in private_loader:
            optimizer.zero_grad()
            torch.nn.functional.mse_loss(model(xb), yb).backward()
            optimizer.step()

        self.assertEqual(engine.accountant.history, [(1.0, 0.5, 2)])
        self.assertTrue(all(bool(torch.isfinite(p).all())
                            for p in model.parameters()))


class FiniteGateTests(unittest.TestCase):
    def test_private_input_gate_rejects_nan_and_infinity(self):
        good_x = np.ones((4, 2), dtype=np.float32)
        good_y = np.zeros(4, dtype=np.float32)
        client_app._assert_finite_private_inputs(good_x, good_y)

        bad_x = good_x.copy()
        bad_x[0, 0] = np.nan
        with self.assertRaises(RuntimeError):
            client_app._assert_finite_private_inputs(bad_x, good_y)

        bad_y = good_y.copy()
        bad_y[0] = np.inf
        with self.assertRaises(RuntimeError):
            client_app._assert_finite_private_inputs(good_x, bad_y)

    def test_public_bounds_with_subnormal_span_remain_total(self):
        transformed = client_app._apply_feature_bounds(
            np.asarray([[0.0], [np.nextafter(0.0, 1.0)]], dtype=np.float64),
            {"feature-bounds": {
                "lower": [0.0], "upper": [np.nextafter(0.0, 1.0)]}})
        self.assertTrue(bool(np.isfinite(transformed).all()))
        self.assertTrue(bool(
            np.abs(transformed).max() <= dp_harness.MAX_PARAMETER_ABS))

    def test_release_gate_rejects_non_finite_parameter(self):
        model = torch.nn.Linear(2, 1)
        client_app._assert_finite_release(model)
        with torch.no_grad():
            model.weight[0, 0] = float("nan")
        with self.assertRaises(RuntimeError):
            client_app._assert_finite_release(model)

    def test_nonfinite_per_sample_gradients_are_totalized_before_l2_clip(self):
        model = torch.nn.Linear(2, 1)
        model.weight.grad_sample = torch.tensor([
            [[float("nan"), float("inf")]],
            [[-float("inf"), 0.5]],
        ])
        model.bias.grad_sample = [torch.tensor([[float("inf")], [0.25]])]

        dp_harness.totalize_grad_samples(
            model.parameters(), clipping_norm=1.0)

        for parameter in model.parameters():
            values = (parameter.grad_sample if isinstance(parameter.grad_sample, list)
                      else [parameter.grad_sample])
            self.assertTrue(all(bool(torch.isfinite(value).all()) for value in values))
            self.assertTrue(all(bool((value.abs() <= 1.0).all()) for value in values))

    def test_deep_valid_graph_backward_overflow_is_totalized(self):
        from opacus import GradSampleModule

        layers = []
        for index in range(8):
            layers.extend((torch.nn.Linear(1, 1), model_spec.FiniteClamp(
                model_spec._MAX_OUTPUT_ABS if index == 7
                else model_spec._MAX_ACTIVATION_ABS)))
        model = torch.nn.Sequential(*layers)
        with torch.no_grad():
            for module in model.modules():
                if isinstance(module, torch.nn.Linear):
                    module.weight.fill_(model_spec._MAX_PUBLIC_SCALAR_ABS)
                    module.bias.zero_()
        private_model = GradSampleModule(model)
        loss = dp_harness.loss_from_allowlist(
            "gamma_nll", {"gamma-shape": 1.0e12})(
                private_model(torch.zeros(2, 1)),
                torch.full((2, 1), 1.0e6))
        loss.backward()
        self.assertTrue(any(
            not bool(torch.isfinite(parameter.grad_sample).all())
            for parameter in private_model.parameters()))

        dp_harness.totalize_grad_samples(
            private_model.parameters(), clipping_norm=1.0)

        flat = [parameter.grad_sample.reshape(2, -1)
                for parameter in private_model.parameters()]
        per_sample_norm = torch.cat(flat, dim=1).norm(2, dim=1)
        self.assertTrue(bool(torch.isfinite(per_sample_norm).all()))

    def test_opacus_clip_wrapper_totalizes_exactly_once_immediately_pre_clip(self):
        dataset = TensorDataset(torch.zeros(2, 1), torch.zeros(2, 1))
        loader = DataLoader(dataset, batch_size=2, shuffle=False)
        model = torch.nn.Linear(1, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        _, optimizer, _, _ = dp_harness.make_private_dpsgd(
            model, optimizer, loader,
            clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            local_epochs=1, noise_multiplier=1.0,
            secure_sampling_rng=_ScriptedSamplingRng(),
            secure_noise_rng=_ScriptedSamplingRng())
        for parameter in optimizer.params:
            sample_shape = (2,) + tuple(parameter.shape)
            parameter.grad_sample = torch.full(sample_shape, float("inf"))
            parameter.grad_sample.reshape(-1)[0] = float("nan")

        with mock.patch.object(
                dp_harness, "totalize_grad_samples",
                wraps=dp_harness.totalize_grad_samples) as totalize:
            optimizer.clip_and_accumulate()

        totalize.assert_called_once()
        self.assertTrue(all(
            bool(torch.isfinite(parameter.summed_grad).all())
            for parameter in optimizer.params))

    def test_vectorized_patient_pooling_preserves_reference_semantics(self):
        def reference(X, y, groups, loss_name):
            g = np.asarray([
                task._canonical_patient_id(value) for value in groups
            ], dtype=object)
            Xp, yp = [], []
            categorical = (loss_name in client_app._CLASSIFICATION_LOSSES
                           or loss_name == "multilabel_bce")
            for key in dict.fromkeys(g.tolist()):
                mask = g == key
                Xp.append(np.asarray(X[mask], dtype=np.float64).mean(axis=0))
                values = np.asarray(y[mask])
                if values.ndim > 1:
                    pooled = np.asarray(values, dtype=np.float64).mean(axis=0)
                    if categorical:
                        pooled = (pooled >= 0.5).astype(values.dtype)
                elif categorical:
                    labels, counts = np.unique(values, return_counts=True)
                    pooled = labels[np.argmax(counts)]
                else:
                    pooled = np.asarray(values, dtype=np.float64).mean()
                yp.append(pooled)
            return np.stack(Xp), np.asarray(yp, dtype=y.dtype)

        X = np.asarray([
            [1.0, 8.0], [10.0, 2.0], [3.0, 4.0],
            [14.0, 6.0], [5.0, 0.0], [18.0, 10.0],
        ], dtype=np.float32)
        groups = np.asarray(["p2", "p1", "p2", "p1", "p2", "p1"])
        cases = (
            (np.asarray([2, 3, 1, 3, 2, 1], dtype=np.int64),
             "cross_entropy"),
            (np.asarray([1.5, 4.0, 2.5, 8.0, 3.5, 12.0], dtype=np.float32),
             "mse"),
            (np.asarray([
                [0, 1], [1, 0], [1, 0],
                [0, 1], [0, 0], [1, 1],
            ], dtype=np.float32), "multilabel_bce"),
        )
        for y, loss_name in cases:
            with self.subTest(loss_name=loss_name):
                expected_X, expected_y = reference(X, y, groups, loss_name)
                actual_X, actual_y = client_app._pool_by_patient(
                    X, y, groups, loss_name)
                np.testing.assert_allclose(actual_X, expected_X, rtol=0, atol=0)
                np.testing.assert_array_equal(actual_y, expected_y)

    def test_public_hook_arrays_are_bounded_and_finite(self):
        valid = client_app._validate_public_egress_arrays([
            np.zeros((2, 3), dtype=np.float32), np.ones(1, dtype=np.int64)])
        self.assertEqual(len(valid), 2)
        for invalid in (
                [],
                [np.asarray([np.nan])],
                [np.asarray([1e308], dtype=np.float64)],
                [np.asarray([1 + 2j])],
                [np.zeros((1,) * (client_app._MAX_EGRESS_NDIM + 1))],
                [np.zeros(0)]):
            with self.assertRaises(RuntimeError):
                client_app._validate_public_egress_arrays(invalid)
        with mock.patch.object(client_app, "_MAX_EGRESS_ELEMENTS", 2):
            with self.assertRaises(RuntimeError):
                client_app._validate_public_egress_arrays([np.zeros(3)])
        record = ArrayRecord(numpy_ndarrays=[np.zeros(2, dtype=np.float32)])
        next(iter(record.values())).shape = (10**9,)
        with self.assertRaises(RuntimeError):
            client_app._validate_public_egress_arrays(record)


class NumericOverflowTests(unittest.TestCase):
    class _ZeroRng(seeding.SecureNumpyRng):
        def __init__(self):
            pass

        @staticmethod
        def normal(loc=0.0, scale=1.0, size=None):
            return np.zeros(size, dtype=np.float64)

    class _ExtremeRng(seeding.SecureNumpyRng):
        def __init__(self):
            pass

        @staticmethod
        def normal(loc=0.0, scale=1.0, size=None):
            values = np.asarray([1e308, -1e308], dtype=np.float64)
            return values.reshape(size)

    def test_extreme_finite_candidate_is_stably_clipped(self):
        old = [np.zeros(2, dtype=np.float64)]
        candidate = [np.asarray([1e308, -1e308], dtype=np.float64)]

        clipped = dp_harness.clip_update(candidate, old, clipping_norm=1.0)

        np.testing.assert_allclose(
            clipped[0], np.asarray([1.0, -1.0]) / math.sqrt(2.0),
            rtol=1e-15, atol=0.0)
        self.assertTrue(bool(np.all(np.isfinite(clipped[0]))))
        self.assertLessEqual(float(np.linalg.norm(clipped[0])), 1.0 + 1e-15)

    def test_nonfinite_or_overflowing_candidate_maps_to_zero_delta(self):
        old = [np.asarray([0.25, -0.25], dtype=np.float64)]
        for candidate in (
                [np.asarray([np.nan, 1.0], dtype=np.float64)],
                [np.asarray([np.inf, -np.inf], dtype=np.float64)]):
            with self.subTest(candidate=candidate[0]):
                clipped = dp_harness.clip_update(candidate, old, clipping_norm=1.0)
                np.testing.assert_array_equal(clipped[0], old[0])

        max_float = np.finfo(np.float64).max
        clipped = dp_harness.clip_update(
            [np.asarray([max_float])], [np.asarray([-max_float])],
            clipping_norm=1.0)
        np.testing.assert_array_equal(clipped[0], [-max_float])

    def test_count_and_shape_mismatch_are_rejected(self):
        old = [np.zeros(2, dtype=np.float64)]
        with self.assertRaises(ValueError):
            dp_harness.clip_update([], old, clipping_norm=1.0)
        with self.assertRaises(ValueError):
            dp_harness.clip_update([np.zeros(1)], old, clipping_norm=1.0)

    def test_gaussian_release_is_float64_finite_and_saturated(self):
        released = dp_harness.add_gaussian_noise(
            [np.zeros(2)], [np.zeros(2)], std=1.0,
            rng=self._ExtremeRng())

        self.assertEqual(released[0].dtype, np.dtype(np.float64))
        self.assertTrue(bool(np.all(np.isfinite(released[0]))))
        self.assertTrue(bool(np.all(
            np.abs(released[0]) <= dp_harness.MAX_RELEASE_ABS)))

    def test_nan_hook_exploit_releases_only_finite_bounded_postprocessing(self):
        old = [np.zeros(3, dtype=np.float64)]
        candidate = [np.asarray([1e308, -1e308, np.nan])]
        released = dp_harness.output_perturbation(
            candidate, old, clipping_norm=1.0,
            epsilon=1.0, delta=1e-5, rng=self._ZeroRng())

        np.testing.assert_array_equal(released[0], old[0])
        self.assertTrue(bool(np.all(np.isfinite(released[0]))))


class StrictNeuralInitializationTests(unittest.TestCase):
    @staticmethod
    def _model_arrays(model):
        return [parameter.detach().cpu().numpy().copy()
                for parameter in model.parameters()]

    def test_set_torch_params_rejects_broadcast_cast_and_nonfinite(self):
        model = torch.nn.Linear(2, 1)
        valid = self._model_arrays(model)
        params.set_torch_params(model, valid)

        invalid_cases = []
        wrong_shape = [value.copy() for value in valid]
        wrong_shape[0] = np.zeros(1, dtype=valid[0].dtype)
        invalid_cases.append(wrong_shape)
        wrong_dtype = [value.copy() for value in valid]
        wrong_dtype[0] = wrong_dtype[0].astype(np.float64)
        invalid_cases.append(wrong_dtype)
        nonfinite = [value.copy() for value in valid]
        nonfinite[0].flat[0] = np.nan
        invalid_cases.append(nonfinite)

        for arrays in invalid_cases:
            with self.subTest(shapes=[value.shape for value in arrays],
                              dtypes=[value.dtype for value in arrays]):
                with self.assertRaises(ValueError):
                    params.set_torch_params(model, arrays)

    def test_tabular_input_width_comes_from_server_manifest_columns(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular",
                    "feature_columns": ["patient_id", "x", "z"],
                    "patient_column": "patient_id",
                }, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir})
            self.assertEqual(
                client_app._neural_input_dim(context, {}, False), 2)

    def test_prepare_neural_model_uses_the_original_public_seed_contract(self):
        model = torch.nn.Linear(2, 1)
        msg = SimpleNamespace(content=RecordDict({
            "arrays": ArrayRecord(numpy_ndarrays=self._model_arrays(model)),
        }))
        cfg = {"data-kind": "tabular", "model-spec-b64": "e30="}
        pcfg = {"policy_hash": "1" * 64, "n_samples": 4}
        pins = {"round_index": 1, "loss_name": "mse"}
        original_contract = client_app._neural_seed_contract

        with (mock.patch.object(client_app, "is_image_run", return_value=False),
              mock.patch.object(client_app, "_neural_input_dim", return_value=2),
              mock.patch.object(client_app, "_neural_seed_contract",
                                wraps=original_contract) as seed_contract,
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"\x61" * 32),
              mock.patch.object(client_app.seeding, "seed_torch"),
              mock.patch.object(client_app, "load_user_model",
                                return_value=model)):
            prepared, input_dim, manifest_image = client_app._prepare_neural_model(
                msg, None, cfg, pcfg, pins)

        self.assertIs(prepared, model)
        self.assertEqual(input_dim, 2)
        self.assertFalse(manifest_image)
        seed_contract.assert_called_once_with(cfg, pins, pcfg)

    def test_invalid_neural_initial_model_is_rejected_before_private_read(self):
        # Shapes and dtype match a Linear(2,1), but magnitude is hostile.
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[
                    np.full((1, 2), 1e13, dtype=np.float32),
                    np.zeros(1, dtype=np.float32),
                ])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 1, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        pins = {
            "loss_name": "mse", "batch_size": 2, "local_epochs": 1,
            "num_rounds": 1, "n_classes": 2, "learning_rate": 0.01,
        }
        model = torch.nn.Linear(2, 1)
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value={"data-kind": "tabular"}),
              mock.patch.object(client_app, "load_dp_track", return_value="neural"),
              mock.patch.object(client_app, "load_privacy_config", return_value={}),
              mock.patch.object(client_app, "load_run_pins", return_value=pins),
              mock.patch.object(client_app, "is_image_run", return_value=False),
              mock.patch.object(client_app, "_neural_input_dim", return_value=2),
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"m" * 32),
              mock.patch.object(client_app.seeding, "seed_torch"),
              mock.patch.object(client_app, "load_user_model", return_value=model),
              mock.patch.object(client_app, "load_data",
                                side_effect=AssertionError("private read")) as load_data,
              mock.patch.object(client_app, "load_image_collection",
                                side_effect=AssertionError("private pixels")) as load_images):
            reply = client_app.train(msg, context)

        load_data.assert_not_called()
        load_images.assert_not_called()
        self.assertFalse(reply.has_error())
        self.assertEqual(dict(reply.content["metrics"]), {
            "num-examples": 1, "public-preflight-unavailable": 1})
        fallback = reply.content["arrays"].to_numpy_ndarrays()
        self.assertEqual(len(fallback), 1)
        np.testing.assert_array_equal(fallback[0], np.zeros(1, dtype=np.float32))

    def test_neural_horizon_mismatch_fails_before_private_read(self):
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.zeros(1, dtype=np.float32)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 3, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        pins = {
            "loss_name": "mse", "batch_size": 2, "local_epochs": 1,
            "num_rounds": 2, "n_classes": 2, "learning_rate": 0.01,
        }
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value={"data-kind": "tabular"}),
              mock.patch.object(client_app, "load_dp_track", return_value="neural"),
              mock.patch.object(client_app, "load_privacy_config", return_value={}),
              mock.patch.object(client_app, "load_run_pins", return_value=pins),
              mock.patch.object(client_app, "_prepare_neural_model",
                                side_effect=AssertionError("private read")) as prepare):
            reply = client_app.train(msg, context)

        prepare.assert_not_called()
        self.assertFalse(reply.has_error())
        self.assertEqual(dict(reply.content["metrics"]), {
            "num-examples": 1, "public-preflight-unavailable": 1})

    def test_private_values_are_never_used_by_architecture_probe(self):
        X = np.zeros((2, 1), dtype=np.float32)
        y = np.asarray([1.0, 1.0e6], dtype=np.float32)
        pins = {
            "loss_name": "mse", "batch_size": 2, "local_epochs": 1,
            "num_rounds": 1, "n_classes": 2, "learning_rate": 0.01,
        }
        model = torch.nn.Linear(1, 1)
        with (mock.patch.object(client_app, "load_data", return_value=(X, y)),
              mock.patch.object(client_app, "load_tabular_patient_ids",
                                return_value=None),
              mock.patch.object(client_app.task_module,
                                "assert_pinned_unit_count"),
              mock.patch.object(
                  client_app.dp_harness, "per_sample_independence_probe",
                  side_effect=AssertionError("private architecture probe")) as probe,
              mock.patch.object(client_app.seeding, "master_seed",
                                return_value=b"m" * 32) as master_seed,
              mock.patch.object(client_app, "_dp_fit",
                                return_value=([np.zeros(1)], 2)) as fit):
            result = client_app._train_neural(
                SimpleNamespace(), {}, {
                    "clipping_norm": 1.0, "policy_hash": "1" * 64,
                }, {**pins, "round_index": 1}, model,
                input_dim=1, manifest_image=False)

        probe.assert_not_called()
        fit.assert_called_once()
        self.assertEqual(result[1], 2)
        self.assertEqual(master_seed.call_args.args[0], "neural-dpsgd/v1")
        private_arrays = master_seed.call_args.kwargs["private_arrays"]
        np.testing.assert_array_equal(private_arrays[0], X)
        np.testing.assert_array_equal(private_arrays[1], y.astype(np.float32))


class RunPinBoundsTests(unittest.TestCase):
    def test_empty_pinned_privacy_unit_roster_is_structurally_valid(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"n_units": 0}, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            task.assert_pinned_unit_count(context, 0)
            with self.assertRaisesRegex(RuntimeError, "roster changed"):
                task.assert_pinned_unit_count(context, 1)

    def test_training_horizon_pins_have_strict_absolute_bounds(self):
        base = {
            "loss-name": "mse", "batch-size": 32, "local-epochs": 2,
            "num-server-rounds": 3, "num-classes": 2,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            for key, invalid in (
                    ("batch-size", 0),
                    ("batch-size", task._MAX_BATCH_SIZE + 1),
                    ("batch-size", 1.5),
                    ("local-epochs", 0),
                    ("local-epochs", task._MAX_LOCAL_EPOCHS + 1),
                    ("local-epochs", float("nan")),
                    ("num-server-rounds", 0),
                    ("num-server-rounds", task._MAX_SERVER_ROUNDS + 1),
                    ("num-server-rounds", float("inf")),
                    ("num-server-rounds", 1.5)):
                manifest = dict(base)
                manifest[key] = invalid
                with open(os.path.join(manifest_dir, "manifest.json"), "w",
                          encoding="utf-8") as handle:
                    json.dump(manifest, handle)
                with self.assertRaises(ValueError):
                    task.load_run_pins(context)

    def test_learning_rate_must_be_finite_and_positive(self):
        base = {
            "loss-name": "mse", "batch-size": 32, "local-epochs": 2,
            "num-server-rounds": 3, "num-classes": 2,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            for invalid in (
                    0.0, -0.1, task._MAX_LEARNING_RATE + 0.1,
                    float("nan"), float("inf")):
                manifest = dict(base, **{"learning-rate": invalid})
                with open(os.path.join(manifest_dir, "manifest.json"), "w",
                          encoding="utf-8") as handle:
                    json.dump(manifest, handle)
                with self.subTest(learning_rate=invalid):
                    with self.assertRaises(ValueError):
                        task.load_run_pins(context)

            manifest = dict(base, **{"learning-rate": 0.1})
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            ignored = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config={"learning-rate": float("nan")})
            self.assertEqual(task.load_run_pins(ignored)["learning_rate"], 0.1)

    def test_bce_logits_is_restricted_to_binary_heads(self):
        manifest = {
            "loss-name": "bce_logits", "batch-size": 32,
            "local-epochs": 1, "num-server-rounds": 1,
            "num-classes": 3,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            with self.assertRaisesRegex(ValueError, "binary only"):
                task.load_run_pins(context)

    def test_image_config_is_frozen_from_the_node_manifest(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"image-size": 224, "backbone": "resnet18"}, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config={"image-size": 10**9, "backbone": "other"})
            cfg = task.load_pinned_run_config(context)
            self.assertEqual(cfg["image-size"], 224)
            self.assertEqual(cfg["backbone"], "resnet18")

    def test_optimizer_and_scheduler_are_manifest_pinned_and_applied(self):
        manifest = {
            "dp-track": "neural", "loss-name": "mse", "batch-size": 32,
            "local-epochs": 3, "num-server-rounds": 2, "num-classes": 2,
            "learning-rate": 0.02, "weight-decay": 0.03,
            "l1-penalty": 0.04, "optimizer-name": "adamw",
            "optimizer-beta1": 0.8, "optimizer-beta2": 0.95,
            "optimizer-eps": 1e-7, "optimizer-amsgrad": True,
            "scheduler-name": "cosine", "scheduler-min-lr": 0.001,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config=dict(manifest))
            pins = task.load_run_pins(context)
            model = torch.nn.Linear(2, 1)
            optimizer = client_app._build_optimizer(model, pins)
            self.assertIsInstance(optimizer, torch.optim.AdamW)
            self.assertEqual(optimizer.param_groups[0]["betas"], (0.8, 0.95))
            self.assertEqual(optimizer.param_groups[0]["eps"], 1e-7)
            self.assertEqual(optimizer.param_groups[0]["weight_decay"], 0.03)
            self.assertTrue(optimizer.param_groups[0]["amsgrad"])
            self.assertAlmostEqual(
                client_app._scheduled_learning_rate(pins, 0), 0.02)
            self.assertAlmostEqual(
                client_app._scheduled_learning_rate(pins, 5), 0.001)

            growing = dict(manifest)
            growing["scheduler-name"] = "exponential"
            growing["scheduler-gamma"] = 1.1
            growing.pop("scheduler-min-lr")
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(growing, handle)
            context.run_config = dict(growing)
            growing_pins = task.load_run_pins(context)
            self.assertAlmostEqual(
                client_app._scheduled_learning_rate(growing_pins, 1), 0.022)

    def test_incompatible_or_mismatched_optimizer_fields_fail_closed(self):
        manifest = {
            "dp-track": "neural", "loss-name": "mse", "batch-size": 32,
            "local-epochs": 1, "num-server-rounds": 1, "num-classes": 2,
            "learning-rate": 0.01, "optimizer-name": "sgd",
            "optimizer-momentum": 0.1, "optimizer-nesterov": False,
            "optimizer-beta1": 0.8, "scheduler-name": "none",
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            with self.assertRaisesRegex(ValueError, "incompatible"):
                task.load_run_pins(context)

            manifest.pop("optimizer-beta1")
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context.run_config = dict(manifest, **{"optimizer-momentum": 0.9})
            with self.assertRaisesRegex(ValueError, "does not match"):
                task.load_pinned_run_config(context)

    def test_only_the_selected_loss_parameter_is_accepted(self):
        base = {
            "dp-track": "neural", "loss-name": "huber", "batch-size": 32,
            "local-epochs": 1, "num-server-rounds": 1, "num-classes": 2,
            "learning-rate": 0.01, "optimizer-name": "sgd",
            "optimizer-momentum": 0.0, "optimizer-nesterov": False,
            "scheduler-name": "none", "huber-delta": 2.5,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            path = os.path.join(manifest_dir, "manifest.json")
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir}, run_config={})
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(base, handle)
            pins = task.load_run_pins(context)
            self.assertEqual(pins["loss_name"], "huber")

            invalid = dict(base, **{"gamma-shape": 3.0})
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(invalid, handle)
            with self.assertRaisesRegex(ValueError, "incompatible"):
                task.load_run_pins(context)

            missing = dict(base)
            missing.pop("huber-delta")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(missing, handle)
            with self.assertRaisesRegex(ValueError, "missing selected"):
                task.load_run_pins(context)

    def test_quantile_loss_is_pinned_separable_and_bounded(self):
        criterion = dp_harness.loss_from_allowlist(
            "quantile", {"quantile-level": 0.75})
        value = criterion(
            torch.tensor([[0.0], [2.0]]),
            torch.tensor([[1.0], [0.0]]))
        self.assertAlmostEqual(float(value), 0.625)
        for invalid in (0.0, 1.0, float("nan")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                dp_harness.loss_from_allowlist(
                    "quantile", {"quantile-level": invalid})

        manifest = {
            "dp-track": "neural", "loss-name": "quantile",
            "batch-size": 32, "local-epochs": 1,
            "num-server-rounds": 1, "num-classes": 2,
            "learning-rate": 0.01, "optimizer-name": "sgd",
            "optimizer-momentum": 0.0, "optimizer-nesterov": False,
            "scheduler-name": "none", "quantile-level": 0.75,
        }
        with tempfile.TemporaryDirectory() as manifest_dir:
            path = os.path.join(manifest_dir, "manifest.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config=dict(manifest))
            pins = task.load_run_pins(context)
            self.assertEqual(pins["loss_name"], "quantile")


class HookAppPublicConfigTests(unittest.TestCase):
    @staticmethod
    def _wire_bytes(raw):
        return {
            "app-params-b64": base64.b64encode(raw).decode("ascii"),
            "app-params-sha256": hashlib.sha256(raw).hexdigest(),
            "num-server-rounds": 3,
            "task-type": "regression",
            "num-classes": 2,
        }

    def test_recursive_params_and_server_round_metadata_are_exact(self):
        params_value = {
            "alpha": 0.125,
            "enabled": True,
            "labels": ["a", None, 3],
            "optimizer": {"momentum": 0.9, "name": "adam"},
        }
        cfg = _hook_wire(
            params_value, rounds=3, task_type="regression", num_classes=2)
        cfg.update({"round_index": 999, "privacy-epsilon": 1e9})

        public = tier2_lib.public_hook_config(cfg, round_index=2)

        self.assertEqual(public, {
            "app_params": params_value,
            "round_index": 2,
            "num_rounds": 3,
            "task": "regression",
            "num_classes": 2,
        })
        self.assertEqual(tier2_lib._sanitize_cfg(public), public)
        with self.assertRaisesRegex(ValueError, "unknown or missing"):
            tier2_lib._sanitize_cfg(dict(public, epsilon=1000))

    def test_malformed_reserved_nonfinite_and_oversized_params_fail_closed(self):
        cases = []
        good = _hook_wire({"alpha": 1})
        cases.append(("hash", dict(good, **{"app-params-sha256": "0" * 64})))
        cases.append(("duplicate", self._wire_bytes(b'{"a":1,"a":2}')))
        cases.append(("top-array", self._wire_bytes(b"[]")))
        cases.append(("nonfinite", self._wire_bytes(b'{"value":NaN}')))
        cases.append(("utf8", self._wire_bytes(b'{"value":"\xff"}')))
        for key in ("epsilon", "training_epsilon", "noiseMultiplier", "apiToken",
                    "model_path", "requirements", "round_index", "dp-noise"):
            cases.append(("reserved-" + key, _hook_wire({key: 1})))
        cases.append(("path-value", _hook_wire({"model": "folder/model.bin"})))
        nested = {"leaf": 1}
        for _ in range(9):
            nested = {"nested": nested}
        cases.append(("depth", _hook_wire(nested)))
        cases.append(("items", _hook_wire({"values": [0] * 2048})))
        cases.append(("string", _hook_wire({"value": "x" * 4097})))
        cases.append(("bytes", _hook_wire({
            "field%02d" % index: "x" * 4000 for index in range(20)
        })))

        for label, cfg in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    tier2_lib.public_hook_config(cfg, round_index=1)

    def test_manifest_pin_mismatch_is_rejected(self):
        pins = _hook_wire({"alpha": 1})
        pins["num-features"] = 4
        manifest = dict(pins, **{
            "dp-track": "egress", "user-module": "server_pkg",
        })
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump(manifest, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir},
                run_config=dict(pins, **{"user-module": "analyst_pkg"}),
            )
            exact = task.load_pinned_run_config(context)
            self.assertEqual(exact["user-module"], "server_pkg")
            tampering = {
                "app-params-b64": _hook_wire({"alpha": 2})["app-params-b64"],
                "app-params-sha256": "0" * 64,
                "num-server-rounds": 4,
                "task-type": "count",
                "num-classes": 3,
                "num-features": 5,
            }
            for key, value in tampering.items():
                with self.subTest(key=key):
                    context.run_config = dict(pins, **{
                        "user-module": "analyst_pkg", key: value,
                    })
                    with self.assertRaisesRegex(ValueError, "manifest pin"):
                        task.load_pinned_run_config(context)

    def test_invalid_params_fail_before_hook_import_or_private_read(self):
        cfg = _hook_wire({"alpha": 1}, rounds=1)
        cfg.update({
            "app-params-sha256": "0" * 64,
            "user-module": "hookpkg", "num-features": 2,
        })
        with mock.patch.object(
                tier2_lib, "load_user_module",
                side_effect=AssertionError("hook import")) as load_module:
            with self.assertRaisesRegex(ValueError, "pinned hash"):
                server_app._initial_arrays(cfg, "egress")
        load_module.assert_not_called()

        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.zeros(2, dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 1, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value=cfg),
              mock.patch.object(client_app, "load_dp_track", return_value="egress"),
              mock.patch.object(client_app, "load_privacy_config", return_value={}),
              mock.patch.object(client_app, "load_data",
                                side_effect=AssertionError("private read")) as load_data,
              mock.patch.object(tier2_lib, "hook_execution_caps",
                                side_effect=AssertionError("sandbox gate")) as caps):
            reply = client_app.train(msg, context)
        load_data.assert_not_called()
        caps.assert_not_called()
        self.assertFalse(reply.has_error())

    def test_initial_and_local_hooks_receive_same_params_and_owned_rounds(self):
        params_value = {
            "learning_rate": 0.05,
            "layers": [16, 8],
            "optimizer": {"name": "adam", "amsgrad": False},
        }
        cfg = _hook_wire(params_value, rounds=2)
        cfg.update({"user-module": "hookpkg", "num-features": 3})
        seen = {}

        class Hook:
            @staticmethod
            def initial_arrays(public_cfg, input_dim):
                seen["initial"] = public_cfg
                return [np.zeros(input_dim, dtype=np.float64)]

        with mock.patch.object(tier2_lib, "load_user_module", return_value=Hook):
            _model, record = server_app._initial_arrays(cfg, "egress")
        self.assertEqual(record.to_numpy_ndarrays()[0].shape, (3,))

        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.zeros(3, dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 2, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        pcfg = {
            "epsilon": 1.0, "delta": 1e-5, "hook_enabled": True,
            "egress_timeout": 30, "egress_time_pad": 35.0,
        }

        def capture_update(_module, old, _X, _y, public_cfg, _pcfg, **_kwargs):
            seen["local"] = public_cfg
            return old

        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value=cfg),
              mock.patch.object(client_app, "load_dp_track", return_value="egress"),
              mock.patch.object(client_app, "load_privacy_config", return_value=pcfg),
              mock.patch.object(tier2_lib, "hook_execution_caps",
                                return_value={"sandbox": True}),
              mock.patch.object(client_app, "load_data", return_value=(
                  np.zeros((2, 3), dtype=np.float32),
                  np.zeros(2, dtype=np.float32))),
              mock.patch.object(client_app, "load_tabular_patient_ids",
                                return_value=None),
              mock.patch.object(client_app.task_module, "assert_pinned_unit_count"),
              mock.patch.object(tier2_lib, "hook_master_seed",
                                return_value=b"m" * 32),
              mock.patch.object(tier2_lib, "hook_execution_seed",
                                return_value=b"e" * 32),
              mock.patch.object(tier2_lib, "pad_hook_release"),
              mock.patch.object(tier2_lib, "gated_local_update",
                                side_effect=capture_update)):
            reply = client_app.train(msg, context)

        self.assertFalse(reply.has_error())
        self.assertEqual(seen["initial"]["app_params"], seen["local"]["app_params"])
        self.assertEqual(seen["initial"]["round_index"], 0)
        self.assertEqual(seen["local"]["round_index"], 1)
        for field in ("num_rounds", "task", "num_classes"):
            self.assertEqual(seen["initial"][field], seen["local"][field])


class Tier2PinTests(unittest.TestCase):
    def test_exact_verified_initializer_wins_over_same_name_module(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as manifest:
            package = os.path.join(root, "hookpkg")
            os.mkdir(package)
            init_file = os.path.join(package, "__init__.py")
            with open(init_file, "w", encoding="utf-8") as handle:
                handle.write("SOURCE = 'verified-package'\n")
            with open(os.path.join(root, "hookpkg.py"), "w", encoding="utf-8") as handle:
                handle.write("SOURCE = 'unhashed-shadow'\n")
            with open(os.path.join(manifest, "pinned_packages.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"hookpkg": _package_hash(package)}, handle)

            with mock.patch.dict(os.environ, {
                    "DSFLOWER_PINNED_APP_DIR": root,
                    "DSFLOWER_MANIFEST_DIR": manifest,
            }, clear=False):
                pinned_file = tier2_lib._pinned_user_package("hookpkg")
            self.assertEqual(pinned_file, os.path.realpath(init_file))
            module = egress_child._load_pinned_package("hookpkg", pinned_file)
            self.assertEqual(module.SOURCE, "verified-package")
            sys.modules.pop("hookpkg", None)

    def test_namespace_directory_without_initializer_is_rejected(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as manifest:
            package = os.path.join(root, "hookpkg")
            os.mkdir(package)
            with open(os.path.join(package, "code.py"), "w", encoding="utf-8") as handle:
                handle.write("x = 1\n")
            with open(os.path.join(manifest, "pinned_packages.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"hookpkg": _package_hash(package)}, handle)
            with mock.patch.dict(os.environ, {
                    "DSFLOWER_PINNED_APP_DIR": root,
                    "DSFLOWER_MANIFEST_DIR": manifest,
            }, clear=False):
                with self.assertRaises(RuntimeError):
                    tier2_lib._pinned_user_package("hookpkg")

    def test_node_config_uses_only_manifest_pinned_module(self):
        with tempfile.TemporaryDirectory() as manifest:
            with open(os.path.join(manifest, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"user-module": "server_pkg"}, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest},
                run_config={"user-module": "analyst_pkg"},
            )
            self.assertEqual(
                task.load_pinned_run_config(context)["user-module"], "server_pkg")

    def test_disabled_hook_returns_before_loading_private_data(self):
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=[np.zeros(2, dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 2, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        pcfg = {
            "epsilon": 1.0, "delta": 1e-5, "hook_enabled": False,
            "egress_timeout": 30, "egress_time_pad": 0.0,
        }
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value={"user-module": "server_pkg"}),
              mock.patch.object(client_app, "load_dp_track", return_value="egress"),
              mock.patch.object(client_app, "load_privacy_config", return_value=pcfg),
              mock.patch.object(client_app, "load_data",
                                side_effect=AssertionError("private read")) as load_data,
              mock.patch.object(tier2_lib, "hook_execution_caps", return_value=None)):
            reply = client_app.train(msg, context)

        load_data.assert_not_called()
        self.assertFalse(reply.has_error())
        self.assertEqual(dict(reply.content["metrics"]), {
            "num-examples": 1, "hook-executed": 0,
            "public-preflight-unavailable": 1})
        np.testing.assert_array_equal(
            reply.content["arrays"].to_numpy_ndarrays()[0],
            np.zeros(2, dtype=np.float32),
        )

    def test_invalid_public_hook_arrays_fail_before_loading_private_data(self):
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.asarray([np.nan], dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m1", "release_index": 1,
            "num_rounds": 2, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        pcfg = {
            "epsilon": 1.0, "delta": 1e-5, "hook_enabled": True,
            "egress_timeout": 30, "egress_time_pad": 36.0,
        }
        with (mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value={"user-module": "server_pkg"}),
              mock.patch.object(client_app, "load_dp_track", return_value="egress"),
              mock.patch.object(client_app, "load_privacy_config", return_value=pcfg),
              mock.patch.object(client_app, "load_data",
                                side_effect=AssertionError("private read")) as load_data):
            reply = client_app.train(msg, context)

        load_data.assert_not_called()
        self.assertFalse(reply.has_error())
        self.assertEqual(dict(reply.content["metrics"]), {
            "num-examples": 1, "hook-executed": 0,
            "public-preflight-unavailable": 1})
        fallback = reply.content["arrays"].to_numpy_ndarrays()
        self.assertEqual(len(fallback), 1)
        np.testing.assert_array_equal(fallback[0], np.zeros(1, dtype=np.float32))


class ClientAppExceptionBoundaryTests(unittest.TestCase):
    def test_private_conversion_error_never_crosses_flower_boundary(self):
        sentinel = "DSFLOWER_PRIVATE_SENTINEL_7"
        msg = Message(
            content=RecordDict({
                "arrays": ArrayRecord(
                    numpy_ndarrays=[np.zeros(1, dtype=np.float64)])
            }),
            dst_node_id=1,
            message_type="train",
        )
        context = SimpleNamespace(state=RecordDict())
        claim = {
            "status": "new", "message_id": "m-private", "release_index": 1,
            "num_rounds": 1, "run_token": "run_" + "a" * 32,
            "epsilon": 1.0, "delta": 1e-5,
        }
        def fail_on_private_conversion(*_args, **kwargs):
            pd.DataFrame({"private_feature": [sentinel]}).to_numpy(
                dtype=np.float32)

        with self.assertRaises(ValueError) as conversion:
            fail_on_private_conversion()
        self.assertIn(sentinel, str(conversion.exception))

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (redirect_stdout(stdout), redirect_stderr(stderr),
              mock.patch.object(client_app.release_guard, "claim_release",
                                return_value=claim),
              mock.patch.object(client_app, "load_pinned_run_config",
                                return_value={}),
              mock.patch.object(client_app, "load_dp_track", return_value="neural"),
              mock.patch.object(client_app, "load_privacy_config",
                                return_value={"epsilon": 1.0, "delta": 1e-5}),
              mock.patch.object(client_app, "load_run_pins",
                                return_value={"num_rounds": 1}),
              mock.patch.object(client_app, "_prepare_neural_model",
                                return_value=(object(), 1, False)),
              mock.patch.object(client_app, "_train_neural",
                                side_effect=fail_on_private_conversion) as train_neural):
            reply = client_app.train(msg, context)

        train_neural.assert_called_once()
        self.assertFalse(reply.has_error())
        self.assertEqual(dict(reply.content["metrics"]), {
            "num-examples": 1, "execution-unavailable": 1})
        arrays = reply.content["arrays"].to_numpy_ndarrays()
        self.assertEqual(len(arrays), 1)
        np.testing.assert_array_equal(arrays[0], np.zeros(1, dtype=np.float64))

        observable = "\n".join((
            repr(reply), repr(reply.error if reply.has_error() else None),
            repr(reply.content), stdout.getvalue(), stderr.getvalue(),
            repr(arrays),
        ))
        self.assertNotIn(sentinel, observable)


class PatientIdGateTests(unittest.TestCase):
    @staticmethod
    def _patient_manifest():
        return {
            "dp-unit": "patient",
            "patient_column": "patient_id",
            "patient-id-canonicalization": "trim-utf8-v2",
        }

    def test_pinned_patient_ids_totalize_missing_empty_and_nan_strings(self):
        manifest = self._patient_manifest()
        for invalid in (None, "", "  ", "na", "nan", "NaN", "NULL",
                        "<NA>", "NaT"):
            frame = pd.DataFrame({"patient_id": ["p1", invalid]})
            self.assertEqual(
                task._load_patient_ids(frame, manifest).tolist(),
                ["p1", task._MISSING_PATIENT_UNIT],
            )

    def test_oversized_patient_ids_totalize_before_grouping_and_seeding(self):
        oversized = "x" * (task._MAX_PATIENT_ID_BYTES + 1)
        loaded = task._load_patient_ids(
            pd.DataFrame({"patient_id": [oversized]}),
            self._patient_manifest())
        self.assertEqual(loaded.tolist(), [task._MISSING_PATIENT_UNIT])
        self.assertEqual(
            tier2_lib._canonical_patient_id(oversized),
            task._MISSING_PATIENT_UNIT)

    def test_v2_patient_id_fixture_matches_active_runner_components(self):
        em_space_id = "\u2003patient\u2003"
        values = [" \tpatient\r\n", em_space_id, "N/A",
                  task._MISSING_PATIENT_UNIT, None, "<NA>", "NaT"]
        expected = ["patient", em_space_id, "N/A",
                    task._MISSING_PATIENT_UNIT,
                    task._MISSING_PATIENT_UNIT,
                    task._MISSING_PATIENT_UNIT,
                    task._MISSING_PATIENT_UNIT]
        loaded = task._load_patient_ids(
            pd.DataFrame({"patient_id": values}), self._patient_manifest())
        self.assertEqual(loaded.tolist(), expected)
        self.assertEqual(
            [tier2_lib._canonical_patient_id(value) for value in values], expected)

        unsupported = self._patient_manifest()
        unsupported["patient-id-canonicalization"] = "trim-utf8-v1"
        with self.assertRaisesRegex(ValueError, "unsupported"):
            task._load_patient_ids(
                pd.DataFrame({"patient_id": ["p1"]}), unsupported)

    def test_missing_and_literal_sentinel_merge_conservatively(self):
        groups = [task._MISSING_PATIENT_UNIT, None]
        Xp, yp = client_app._pool_by_patient(
            np.asarray([[1.0], [3.0]], np.float32),
            np.asarray([0.0, 1.0], np.float32), groups, "mse")
        self.assertEqual(Xp.shape, (1, 1))
        self.assertEqual(yp.shape, (1,))
        blocks, n_units = tier2_lib._patient_row_blocks(
            groups, 2, 2, b"p" * 32)
        self.assertEqual(n_units, 1)
        self.assertEqual(sorted(np.concatenate(blocks).tolist()), [0, 1])

    def test_valid_pinned_patient_ids_are_returned(self):
        frame = pd.DataFrame({"patient_id": ["p1", "p2"]})
        self.assertEqual(
            task._load_patient_ids(frame, self._patient_manifest()).tolist(),
            ["p1", "p2"],
        )

    def test_row_unit_never_auto_detects_patient_ids(self):
        frame = pd.DataFrame({"patient_id": ["p1", "p2"]})
        self.assertIsNone(task._load_patient_ids(frame, {
            "dp-unit": "row", "patient_column": None,
            "patient-id-canonicalization": "trim-utf8-v2",
        }))

    def test_tabular_and_image_loaders_both_totalize_patient_ids(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            context = SimpleNamespace(node_config={"manifest-dir": manifest_dir})

            pd.DataFrame({
                "patient_id": ["p1", ""], "x": [1.0, 2.0], "y": [0, 1],
            }).to_csv(os.path.join(manifest_dir, "data.csv"), index=False)
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular", "data_file": "data.csv",
                    "data_format": "csv", "patient_column": "patient_id",
                    "dp-unit": "patient",
                    "patient-id-canonicalization": "trim-utf8-v2",
                }, handle)
            self.assertEqual(
                task.load_tabular_patient_ids(context).tolist(),
                ["p1", task._MISSING_PATIENT_UNIT],
            )

            pd.DataFrame({
                "patient_id": ["p1", "nan"], "relative_path": ["a.png", "b.png"],
                "label": [0, 1],
            }).to_csv(os.path.join(manifest_dir, "samples.csv"), index=False)
            for filename in ("a.png", "b.png"):
                with open(os.path.join(manifest_dir, filename), "wb") as handle:
                    handle.write(b"not-decoded-by-this-test")
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "image", "samples_file": "samples.csv",
                    "assets": {"images": {"type": "image_root",
                                             "root": manifest_dir,
                                             "path_col": "relative_path"}},
                    "target_column": "label",
                    "patient_column": "patient_id",
                    "dp-unit": "patient",
                    "patient-id-canonicalization": "trim-utf8-v2",
                }, handle)
            _, _, groups = task.load_image_collection(context)
            self.assertEqual(
                groups.tolist(), ["p1", task._MISSING_PATIENT_UNIT])

    def test_csv_patient_ids_are_lossless_and_match_pinned_roster(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            pd.DataFrame({
                "patient_id": ["001", "1", "N/A"],
                "x": [1.0, 2.0, 3.0], "y": [0, 1, 0],
            }).to_csv(os.path.join(manifest_dir, "data.csv"), index=False)
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "tabular", "data_file": "data.csv",
                    "data_format": "csv", "target_column": "y",
                    "feature_columns": ["x"], "task-type": "classification",
                    "num-classes": 2, "dp-unit": "patient",
                    "patient_column": "patient_id",
                    "patient-id-canonicalization": "trim-utf8-v2",
                    "n_units": 3,
                }, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir})
            ids = task.load_tabular_patient_ids(context)
            self.assertEqual(ids.tolist(), ["001", "1", "N/A"])
            task.assert_pinned_unit_count(context, 3, ids)

    def test_pinned_roster_mismatch_is_structural_failure(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({"n_units": 2}, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir})
            with self.assertRaisesRegex(RuntimeError, "roster changed"):
                task.assert_pinned_unit_count(context, 1)

    def test_image_paths_are_regular_files_contained_by_the_image_root(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            nested = os.path.join(root, "nested")
            os.mkdir(nested)
            valid = os.path.join(nested, "scan.png")
            with open(valid, "wb") as handle:
                handle.write(b"image")
            self.assertEqual(
                task._resolve_image_path(root, "nested/scan.png"),
                os.path.realpath(valid),
            )
            self.assertIsNone(
                task._resolve_image_path(root, task._INVALID_IMAGE))

            for bad in ("../secret", "/absolute", "a/../../secret",
                        r"a\..\secret", "C:/secret", "a//secret", "./secret"):
                with self.subTest(path=bad), self.assertRaises(ValueError):
                    task._resolve_image_path(root, bad)

            outside_file = os.path.join(outside, "private.png")
            with open(outside_file, "wb") as handle:
                handle.write(b"private")
            link = os.path.join(root, "link.png")
            try:
                os.symlink(outside_file, link)
            except (OSError, NotImplementedError):
                return
            with self.assertRaises(ValueError):
                task._resolve_image_path(root, "link.png")

    def test_private_bad_image_paths_and_bytes_become_fixed_zero_records(self):
        with tempfile.TemporaryDirectory() as root:
            corrupt = os.path.join(root, "corrupt.png")
            with open(corrupt, "wb") as handle:
                handle.write(b"not an image")
            pd.DataFrame({
                "relative_path": ["../escape.png", "corrupt.png"],
                "label": ["bad", 1],
            }).to_csv(os.path.join(root, "samples.csv"), index=False)
            with open(os.path.join(root, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_type": "image", "samples_file": "samples.csv",
                    "assets": {"images": {"type": "image_root",
                                             "root": root,
                                             "path_col": "relative_path"}},
                    "target_column": "label",
                    "task-type": "classification", "num-classes": 2,
                    "dp-unit": "row", "patient_column": None,
                    "patient-id-canonicalization": "trim-utf8-v2",
                }, handle)
            context = SimpleNamespace(node_config={"manifest-dir": root})
            paths, labels, groups = task.load_image_collection(context)
            self.assertEqual(paths, [None, os.path.realpath(corrupt)])
            np.testing.assert_array_equal(labels, np.asarray([0, 1], np.float32))
            self.assertIsNone(groups)

            with mock.patch.object(
                    vision, "_read_array",
                    side_effect=AssertionError("sentinel must not be opened")):
                zero = vision.read_image_2d(None, 8)
                named_zero = vision.read_image_2d(task._INVALID_IMAGE, 8)
            self.assertEqual(zero.shape, (3, 8, 8))
            self.assertFalse(bool(np.any(zero)))
            np.testing.assert_array_equal(named_zero, zero)
            corrupt_zero = vision.read_image_2d(corrupt, 8)
            self.assertEqual(corrupt_zero.shape, (3, 8, 8))
            self.assertFalse(bool(np.any(corrupt_zero)))
            volume_zero = vision.read_image_3d(None, 16)
            self.assertEqual(volume_zero.shape, (1, 16, 16, 16))
            self.assertFalse(bool(np.any(volume_zero)))

    def test_invalid_image_size_never_reaches_zero_allocation(self):
        invalid = (-1, True, np.bool_(False),
                   vision._MAX_IMAGE_SIZE + 1, 8.0, "8")
        with mock.patch.object(
                vision.np, "zeros",
                side_effect=AssertionError("np.zeros must not run")) as zeros:
            for reader in (vision.read_image_2d, vision.read_image_3d):
                for value in invalid:
                    with self.subTest(reader=reader.__name__, value=value):
                        with self.assertRaises(ValueError):
                            reader(None, value)
        zeros.assert_not_called()

    def test_oversized_source_is_rejected_before_raster_open(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "oversized.png")
            with open(path, "wb") as handle:
                handle.truncate(vision._MAX_IMAGE_SOURCE_BYTES + 1)
            image_api = SimpleNamespace(open=mock.Mock(
                side_effect=AssertionError("raster decoder must not run")))
            pil = SimpleNamespace(Image=image_api)
            with mock.patch.dict(sys.modules, {"PIL": pil, "PIL.Image": image_api}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            image_api.open.assert_not_called()

    def test_symlink_source_is_rejected_before_raster_open(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "target.png")
            path = os.path.join(root, "link.png")
            with open(target, "wb") as handle:
                handle.write(b"small header stub")
            try:
                os.symlink(target, path)
            except (OSError, NotImplementedError):
                self.skipTest("symlinks are unavailable")
            image_api = SimpleNamespace(open=mock.Mock(
                side_effect=AssertionError("raster decoder must not run")))
            pil = SimpleNamespace(Image=image_api)
            with mock.patch.dict(sys.modules, {"PIL": pil, "PIL.Image": image_api}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            image_api.open.assert_not_called()

    def test_raster_header_limit_precedes_pixel_decode(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "huge.png")
            with open(path, "wb") as handle:
                handle.write(b"small header stub")
            image = mock.MagicMock()
            image.__enter__.return_value = image
            image.height = vision._MAX_IMAGE_AXIS
            image.width = vision._MAX_IMAGE_AXIS
            image.getbands.return_value = ("R", "G", "B")
            image_api = SimpleNamespace(open=mock.Mock(return_value=image))
            pil = SimpleNamespace(Image=image_api)
            with mock.patch.dict(sys.modules, {"PIL": pil, "PIL.Image": image_api}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            image.convert.assert_not_called()

    def test_nifti_header_limit_precedes_voxel_decode(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "huge.nii")
            with open(path, "wb") as handle:
                handle.write(b"small header stub")
            image = SimpleNamespace(
                shape=(vision._MAX_IMAGE_AXIS, vision._MAX_IMAGE_AXIS, 2),
                get_data_dtype=mock.Mock(return_value=np.dtype("f4")),
                get_fdata=mock.Mock(
                    side_effect=AssertionError("voxel decoder must not run")),
            )
            nib = SimpleNamespace(load=mock.Mock(return_value=image))
            with mock.patch.dict(sys.modules, {"nibabel": nib}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            image.get_fdata.assert_not_called()

    def test_nrrd_header_limit_precedes_voxel_decode(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "huge.nrrd")
            with open(path, "wb") as handle:
                handle.write(b"NRRD0005\ntype: float\n\n")
            nrrd = SimpleNamespace(
                read_header=mock.Mock(return_value={
                    "type": "float",
                    "sizes": (vision._MAX_IMAGE_AXIS,
                              vision._MAX_IMAGE_AXIS, 2),
                }),
                read=mock.Mock(
                    side_effect=AssertionError("voxel decoder must not run")),
            )
            with mock.patch.dict(sys.modules, {"nrrd": nrrd}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            nrrd.read.assert_not_called()

    def test_detached_nrrd_is_totalized_without_reading_sidecar(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "detached.nrrd")
            with open(path, "wb") as handle:
                handle.write(b"NRRD0005\ntype: float\n\n")
            nrrd = SimpleNamespace(
                read_header=mock.Mock(return_value={
                    "type": "float", "sizes": (4, 4, 4),
                    "data file": "private.raw",
                }),
                read=mock.Mock(
                    side_effect=AssertionError("sidecar must not be read")),
            )
            with mock.patch.dict(sys.modules, {"nrrd": nrrd}):
                zero = vision.read_image_3d(path, 16)
            nrrd.read.assert_not_called()
            self.assertEqual(zero.shape, (1, 16, 16, 16))
            self.assertFalse(bool(np.any(zero)))

    def test_detached_mhd_is_totalized_without_starting_decoder(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "detached.mhd")
            with open(path, "wb") as handle:
                handle.write(b"ObjectType = Image\nElementDataFile = private.raw\n")
            sitk = SimpleNamespace(ImageFileReader=mock.Mock(
                side_effect=AssertionError("MHD decoder must not run")))
            with mock.patch.dict(sys.modules, {"SimpleITK": sitk}):
                zero = vision.read_image_3d(path, 16)
            sitk.ImageFileReader.assert_not_called()
            self.assertEqual(zero.shape, (1, 16, 16, 16))
            self.assertFalse(bool(np.any(zero)))

    def test_mha_must_embed_its_payload(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "detached.mha")
            with open(path, "wb") as handle:
                handle.write(b"ObjectType = Image\nElementDataFile = private.raw\n")
            sitk = SimpleNamespace(ImageFileReader=mock.Mock(
                side_effect=AssertionError("MHA decoder must not run")))
            with mock.patch.dict(sys.modules, {"SimpleITK": sitk}):
                zero = vision.read_image_3d(path, 16)
            sitk.ImageFileReader.assert_not_called()
            self.assertEqual(zero.shape, (1, 16, 16, 16))
            self.assertFalse(bool(np.any(zero)))

    def test_simpleitk_header_limit_precedes_voxel_decode(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "huge.mha")
            with open(path, "wb") as handle:
                handle.write(b"small header stub")
            reader = mock.MagicMock()
            reader.GetPixelID.return_value = 1
            reader.GetSize.return_value = (
                vision._MAX_IMAGE_AXIS, vision._MAX_IMAGE_AXIS, 2)
            reader.GetNumberOfComponents.return_value = 1
            sitk = SimpleNamespace(
                ImageFileReader=mock.Mock(return_value=reader),
                GetPixelIDValueAsString=mock.Mock(return_value="32-bit float"),
                GetArrayFromImage=mock.Mock(
                    side_effect=AssertionError("voxel decoder must not run")),
            )
            with mock.patch.dict(sys.modules, {"SimpleITK": sitk}):
                with self.assertRaises(ValueError):
                    vision._read_array(path)
            reader.Execute.assert_not_called()
            sitk.GetArrayFromImage.assert_not_called()

    def test_3d_features_stream_in_byte_bounded_batches(self):
        shape = vision._image_record_shape(16, True)
        record_bytes = int(np.prod(shape)) * 4
        observed = []

        class RecordingBackbone(torch.nn.Module):
            def forward(self, batch):
                observed.append((int(batch.shape[0]),
                                 int(batch.numel() * batch.element_size())))
                return batch.mean(dim=(2, 3, 4))

        def read(path, image_size):
            self.assertEqual(image_size, 16)
            return np.full(shape, float(path), dtype=np.float32)

        with mock.patch.object(
                vision, "_MAX_IMAGE_BATCH_BYTES", 2 * record_bytes), \
                mock.patch.object(vision, "read_image_3d", side_effect=read):
            features = vision.extract_features_from_paths(
                RecordingBackbone(), list(range(5)), 16, True,
                device=torch.device("cpu"))

        self.assertEqual([count for count, _ in observed], [2, 2, 1])
        self.assertTrue(all(size <= 2 * record_bytes for _, size in observed))
        np.testing.assert_array_equal(
            features[:, 0], np.arange(5, dtype=np.float32))

    def test_image_larger_than_batch_cap_fails_before_decode(self):
        shape = vision._image_record_shape(16, True)
        record_bytes = int(np.prod(shape)) * 4
        with mock.patch.object(
                vision, "_MAX_IMAGE_BATCH_BYTES", record_bytes - 1), \
                mock.patch.object(
                    vision, "read_image_3d",
                    side_effect=AssertionError("image must not be decoded")) as read:
            with self.assertRaises(ValueError):
                vision.extract_features_from_paths(
                    object(), ["private.mha"], 16, True,
                    device=torch.device("cpu"))
        read.assert_not_called()


class PublicTargetTests(unittest.TestCase):
    def test_staged_public_codes_do_not_depend_on_observed_categories(self):
        manifest = {
            "task-type": "classification", "num-classes": 3,
            "target-levels": {
                "type": "character", "values": ["b", "c", "d"],
            },
        }
        first = task._load_target(pd.Series([0, 1, 2]), manifest)
        second = task._load_target(pd.Series([1, 2, 1]), manifest)
        np.testing.assert_array_equal(first, np.asarray([0, 1, 2], np.float32))
        np.testing.assert_array_equal(second, np.asarray([1, 2, 1], np.float32))

    def test_invalid_classification_values_map_to_public_code_zero(self):
        target = task._load_target(
            pd.Series(["private-a", np.nan, np.inf, 1.5, -1, 2, 1]),
            {"task-type": "classification", "num-classes": 2})
        np.testing.assert_array_equal(
            target, np.asarray([0, 0, 0, 0, 0, 0, 1], np.float32))

    def test_public_bounds_clip_regression_target(self):
        target = task._load_target(pd.Series([-5.0, 5.0, 20.0]), {
            "task-type": "regression",
            "target-bounds": {"lower": 0.0, "upper": 10.0},
        })
        np.testing.assert_array_equal(target, np.asarray([0, 5, 10], np.float32))

    def test_invalid_regression_values_map_to_public_bounds_midpoint(self):
        target = task._load_target(
            pd.Series(["bad", np.nan, np.inf, -np.inf, 4.0]), {
                "task-type": "regression",
                "target-bounds": {"lower": 0.0, "upper": 10.0},
            })
        np.testing.assert_array_equal(
            target, np.asarray([5, 5, 10, 0, 4], np.float32))

    def test_private_feature_values_use_public_defaults_and_finite_clip(self):
        frame = pd.DataFrame({
            "a": ["bad", np.inf, 1.0e20],
            "b": [np.nan, -np.inf, -1.0e20],
        })
        values = task._load_features(frame, {
            "feature-bounds": {"lower": [2.0, -4.0], "upper": [6.0, 2.0]},
        })
        np.testing.assert_array_equal(values, np.asarray([
            [4.0, -1.0], [4.0, -1.0], [1.0e6, -1.0e6],
        ], np.float32))

    def test_python_target_boundary_repeats_server_numeric_contract(self):
        invalid = (
            {"task-type": "regression",
             "target-bounds": {"lower": 0.0, "upper": 1.0e6 + 1.0}},
            {"task-type": "count",
             "target-bounds": {"lower": -1.0, "upper": 10.0}},
            {"task-type": "regression", "loss-name": "gamma_nll",
             "target-bounds": {"lower": 0.0, "upper": 10.0}},
        )
        for manifest in invalid:
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                task._load_target(pd.Series([1.0]), manifest)

    def test_tabular_loader_excludes_pinned_patient_id_from_features(self):
        with tempfile.TemporaryDirectory() as manifest_dir:
            pd.DataFrame({
                "patient_id": ["p1", "p2"], "x": [1.0, 2.0], "y": [0, 1],
            }).to_csv(os.path.join(manifest_dir, "data.csv"), index=False)
            with open(os.path.join(manifest_dir, "manifest.json"), "w",
                      encoding="utf-8") as handle:
                json.dump({
                    "data_file": "data.csv", "data_format": "csv",
                    "target_column": "y",
                    "feature_columns": ["patient_id", "x"],
                    "task-type": "classification", "num-classes": 2,
                    "dp-unit": "patient", "patient_column": "patient_id",
                    "patient-id-canonicalization": "trim-utf8-v2",
                }, handle)
            context = SimpleNamespace(
                node_config={"manifest-dir": manifest_dir})
            X, y = task.load_data(context)
            np.testing.assert_array_equal(X, np.asarray([[1], [2]], np.float32))
            np.testing.assert_array_equal(y, np.asarray([0, 1], np.float32))


class PatientPartitionTests(unittest.TestCase):
    def test_one_patient_never_crosses_blocks_and_has_2c_over_k_sensitivity(self):
        ids = np.asarray(["p1", "p2", "p1", "p3", "p2", "p4"], dtype=object)
        k = 3
        partition_seed = b"p" * 32
        blocks, n_units = tier2_lib._patient_row_blocks(
            ids, len(ids), k, partition_seed)

        self.assertEqual(n_units, 4)
        self.assertEqual(sorted(np.concatenate(blocks).tolist()), list(range(len(ids))))
        for patient_id in set(ids.tolist()):
            containing = [i for i, rows in enumerate(blocks)
                          if any(ids[row] == patient_id for row in rows)]
            self.assertEqual(len(containing), 1)

        base = np.zeros(len(ids), dtype=np.float64)
        neighbor = base.copy()
        base[ids == "p2"] = 100.0
        neighbor[ids == "p2"] = -100.0
        old = [np.zeros(1, dtype=np.float64)]

        def updates(values):
            return [[np.asarray([values[rows].sum()], dtype=np.float64)]
                    for rows in blocks]

        before = updates(base)
        after = updates(neighbor)
        self.assertEqual(sum(not np.array_equal(a[0], b[0])
                             for a, b in zip(before, after)), 1)
        out_before = dp_harness.sample_and_aggregate(
            before, old, clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            rng=client_app.seeding.np_rng(b"n" * 32))
        out_after = dp_harness.sample_and_aggregate(
            after, old, clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            rng=client_app.seeding.np_rng(b"n" * 32))
        distance = float(np.linalg.norm(out_before[0] - out_after[0]))
        self.assertLessEqual(distance, min(2.0, 4.0 / k) + 1e-12)

    def test_changed_patient_id_can_affect_two_blocks_and_uses_4c_over_k(self):
        k = 3
        seed = b"p" * 32
        old_ids = np.asarray(["p1", "p2", "p3"], dtype=object)
        old_blocks, _ = tier2_lib._patient_row_blocks(old_ids, 3, k, seed)
        old_block = next(i for i, rows in enumerate(old_blocks) if 1 in rows)

        new_ids = old_ids.copy()
        for candidate_index in range(1, 100):
            new_ids[1] = "replacement-%d" % candidate_index
            new_blocks, _ = tier2_lib._patient_row_blocks(new_ids, 3, k, seed)
            new_block = next(i for i, rows in enumerate(new_blocks) if 1 in rows)
            if new_block != old_block:
                break
        self.assertNotEqual(old_block, new_block)

        before = [[np.zeros(1)] for _ in range(k)]
        after = [[np.zeros(1)] for _ in range(k)]
        before[old_block] = [np.ones(1)]
        before[new_block] = [np.ones(1)]
        after[old_block] = [-np.ones(1)]
        after[new_block] = [-np.ones(1)]
        old = [np.zeros(1)]
        released_before = dp_harness.sample_and_aggregate(
            before, old, clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            rng=client_app.seeding.np_rng(b"n" * 32))
        released_after = dp_harness.sample_and_aggregate(
            after, old, clipping_norm=1.0, epsilon=1.0, delta=1e-5,
            rng=client_app.seeding.np_rng(b"n" * 32))
        distance = float(np.linalg.norm(released_before[0] - released_after[0]))
        self.assertAlmostEqual(distance, min(2.0, 4.0 / k), places=12)

        # At k=2 the conservative bound is the same 2C as the plain floor.
        self.assertEqual(min(2.0, 4.0 / 2), 2.0)

    def test_patient_partition_totalizes_invalid_but_rejects_misaligned_ids(self):
        blocks, n_units = tier2_lib._patient_row_blocks(
            ["p1", np.nan], 2, 2, b"p" * 32)
        self.assertEqual(n_units, 2)
        self.assertEqual(sorted(np.concatenate(blocks).tolist()), [0, 1])
        with self.assertRaises(RuntimeError):
            tier2_lib._patient_row_blocks(["p1"], 2, 2, b"p" * 32)


if __name__ == "__main__":
    unittest.main()
