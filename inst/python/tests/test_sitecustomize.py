"""Regression tests for the SuperNode parent-process import boundary."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest import mock


HOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sitecustomize.py")


def _load_hook(manifest_dir):
    name = "_dsflower_sitecustomize_test_" + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(name, HOOK)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(os.environ, {"DSFLOWER_MANIFEST_DIR": manifest_dir}, clear=False):
        spec.loader.exec_module(module)
    finder = next(item for item in sys.meta_path
                  if item.__class__.__module__ == name)
    sys.meta_path.remove(finder)
    return module, finder


class ParentImportBoundaryTests(unittest.TestCase):
    def test_generated_torch_template_requires_origin_and_exact_source(self):
        with tempfile.TemporaryDirectory() as root:
            hook, finder = _load_hook(root)
            name = "_remote_module_non_scriptable"
            directory = os.path.join(root, "generated")
            os.mkdir(directory)
            origin = os.path.join(directory, name + ".py")
            source = "VALUE = 42\n"
            Path(origin).write_text(source)
            torch_root = os.path.realpath(os.path.join(root, "installed", "torch"))
            instantiator = SimpleNamespace(
                __file__=os.path.join(torch_root, "distributed/nn/jit/instantiator.py"),
                INSTANTIATED_TEMPLATE_DIR_PATH=directory,
                _TEMP_DIR=SimpleNamespace(name=directory))
            template = SimpleNamespace(
                __file__=os.path.join(torch_root, "distributed/nn/jit/templates/remote_module_template.py"),
                get_remote_module_template=lambda cuda: source)
            modules = {
                "torch": SimpleNamespace(__file__=os.path.join(torch_root, "__init__.py")),
                "torch.distributed.nn.jit.instantiator": instantiator,
                "torch.distributed.nn.jit.templates.remote_module_template": template,
            }
            def make_spec(path=origin):
                return importlib.util.spec_from_file_location(name, path)

            with (mock.patch.dict(sys.modules, modules),
                  mock.patch.object(hook, "_SAFE_PREFIXES", (os.path.dirname(torch_root),))):
                accepted = hook._torch_generated_spec(name, make_spec())
                self.assertIsNotNone(accepted)
                # The loader executes the verified snapshot even if a file or
                # cached bytecode changes between verification and execution.
                Path(origin).write_text("raise AssertionError('substitution')\n")
                module = importlib.util.module_from_spec(accepted)
                accepted.loader.exec_module(module)
                self.assertEqual(module.VALUE, 42)
                self.assertIsNone(hook._torch_generated_spec(name, make_spec()))
                Path(origin).write_text(source)
                self.assertIsNone(hook._torch_generated_spec(name + "_other", make_spec()))
                shadow = os.path.join(root, name + ".py")
                Path(shadow).write_text(source)
                self.assertIsNone(hook._torch_generated_spec(name, make_spec(shadow)))
                for item in (modules["torch"], instantiator, template):
                    with mock.patch.object(item, "__file__", shadow):
                        self.assertIsNone(hook._torch_generated_spec(name, make_spec()))
                with mock.patch.object(instantiator._TEMP_DIR, "name", root):
                    self.assertIsNone(hook._torch_generated_spec(name, make_spec()))
                # A same-name shadow goes through the ordinary default-deny
                # path, and HookApp rejection still precedes this exception.
                with (mock.patch.object(hook._PathFinder, "find_spec", return_value=make_spec(shadow)),
                      mock.patch.object(hook, "_abort", side_effect=RuntimeError("denied")),
                      self.assertRaisesRegex(RuntimeError, "denied")):
                    finder.find_spec(name)
                with (mock.patch.object(hook, "_USER_MODULE", name),
                      mock.patch.object(hook, "_abort", side_effect=RuntimeError("denied")),
                      self.assertRaisesRegex(RuntimeError, "denied")):
                    finder.find_spec(name)

    @unittest.skipUnless(all(importlib.util.find_spec(p) for p in ("torch", "opacus", "flwr")),
                         "requires the PyTorch training environment")
    def test_recurrent_contracts_train_in_fresh_guarded_processes(self):
        # Import dependencies only in a new interpreter: pre-importing Opacus
        # would hide the generated-module regression by caching the module.
        code = r'''
import base64, json, os, sys
import sitecustomize
assert any(isinstance(f, sitecustomize._IntegrityFinder) for f in sys.meta_path)
import numpy as np
import torch
from dsflower_runner import client_app, dp_harness, params
assert "dsflower_runner" in sitecustomize._verified_packages
torch.set_num_threads(2)
spec = {"kind": "graph", "output": "out", "nodes": [
    {"name": "x", "op": "reshape", "in": ["@in"], "shape": [128, 9]},
    {"name": "h", "op": sys.argv[1], "in": ["x"], "hidden": 32},
    {"name": "out", "op": "linear", "in": ["h"], "out": "@out"}]}
cfg = {"model-spec-b64": base64.b64encode(json.dumps(spec).encode()).decode(),
       "num-features": 1152, "num-classes": 6, "loss-name": "cross_entropy"}
model = params.load_user_model(cfg, 1152, "cross_entropy")
pins = {"loss_name": "cross_entropy", "batch_size": 32, "local_epochs": 1,
        "num_rounds": 5, "round_index": 1, "n_classes": 6, "learning_rate": .001,
        "optimizer": {"name": "sgd", "weight_decay": 0, "l1_penalty": 0,
                      "momentum": 0, "nesterov": False}, "scheduler": {"name": "none"}}
pcfg = {"epsilon": 4., "delta": 1e-6, "clipping_norm": 1., "n_samples": 7}
mechanism = dp_harness.effective_dpsgd_mechanism(4., 1e-6, 1., 7, 32, 1, 5)
x = np.zeros((7, 1152), dtype=np.float32)
y = np.arange(7, dtype=np.int64) % 6
arrays, count = client_app._dp_fit(model, x, y, pcfg, pins, 7, cfg,
                                 os.urandom(32), mechanism["noise_multiplier"])
assert count == 7 and all(np.isfinite(a).all() for a in arrays)
print("guarded recurrent DP update passed", sys.argv[1])
'''
        with tempfile.TemporaryDirectory() as root:
            hook, _ = _load_hook(root)
            runner = Path(HOOK).resolve().parent.parent / "flower_app/dsflower_runner"
            Path(root, "pinned_packages.json").write_text(json.dumps({
                "dsflower_runner": hook._hash_package(str(runner))}))
            Path(root, "manifest.json").write_text('{"dp-track": "neural"}')
            env = dict(os.environ, DSFLOWER_MANIFEST_DIR=root,
                       PYTHONPATH=os.pathsep.join((str(Path(HOOK).resolve().parent), str(runner.parent))),
                       OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
            for kind in ("lstm", "gru"):
                with self.subTest(kind=kind):
                    result = subprocess.run([sys.executable, "-c", code, kind], env=env,
                                            capture_output=True, text=True, timeout=180)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn("guarded recurrent DP update passed", result.stdout)

    def test_foreign_packages_require_the_single_pin_map_contract(self):
        with tempfile.TemporaryDirectory() as root:
            package = os.path.join(root, "foreignpkg")
            os.mkdir(package)
            with open(os.path.join(package, "__init__.py"), "w",
                      encoding="utf-8") as fh:
                fh.write("VALUE = 1\n")

            unpinned, _ = _load_hook(root)
            with (mock.patch.object(unpinned, "_abort",
                                    side_effect=RuntimeError("denied")) as abort,
                  self.assertRaisesRegex(RuntimeError, "denied")):
                unpinned._verify_foreign("foreignpkg", package)
            abort.assert_called_once()

            actual = unpinned._hash_package(package)
            with open(os.path.join(root, "pinned_packages.json"), "w",
                      encoding="utf-8") as fh:
                json.dump({"foreignpkg": actual}, fh)
            pinned, _ = _load_hook(root)
            with mock.patch.object(pinned, "_abort") as abort:
                pinned._verify_foreign("foreignpkg", package)
            abort.assert_not_called()

            with open(os.path.join(root, "pinned_packages.json"), "w",
                      encoding="utf-8") as fh:
                json.dump({"foreignpkg": "0" * 64}, fh)
            mismatched, _ = _load_hook(root)
            with (mock.patch.object(mismatched, "_abort",
                                    side_effect=RuntimeError("denied")) as abort,
                  self.assertRaisesRegex(RuntimeError, "denied")):
                mismatched._verify_foreign("foreignpkg", package)
            abort.assert_called_once()

    def test_uploaded_module_is_denied_before_runtime_and_safe_path_exemptions(self):
        for module_name in ("flwr", "numpy", "torch"):
            with self.subTest(module_name=module_name), tempfile.TemporaryDirectory() as root:
                with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
                    json.dump({"user-module": module_name}, fh)
                hook, finder = _load_hook(root)
                with (mock.patch.object(hook, "_abort",
                                        side_effect=RuntimeError("denied")) as abort,
                      mock.patch.object(hook._PathFinder, "find_spec") as path_finder):
                    with self.assertRaisesRegex(RuntimeError, "denied"):
                        finder.find_spec(module_name)
                abort.assert_called_once()
                path_finder.assert_not_called()

    def test_canonical_runner_is_verified_even_under_a_safe_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
                json.dump({}, fh)
            hook, finder = _load_hook(root)
            spec = SimpleNamespace(
                origin="/trusted/site-packages/dsflower_runner/__init__.py",
                submodule_search_locations=["/trusted/site-packages/dsflower_runner"],
            )
            with (mock.patch.object(hook._PathFinder, "find_spec", return_value=spec),
                  mock.patch.object(hook, "_is_foreign", return_value=False),
                  mock.patch.object(hook, "_verify_foreign") as verify):
                finder.find_spec("dsflower_runner")
            verify.assert_called_once_with(
                "dsflower_runner", "/trusted/site-packages/dsflower_runner")

    def test_runtime_name_does_not_exempt_foreign_top_level_code(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
                json.dump({"user-module": "hookpkg"}, fh)
            hook, finder = _load_hook(root)
            spec = SimpleNamespace(
                origin="/uploaded/flwr.py", submodule_search_locations=None)
            with (mock.patch.object(hook._PathFinder, "find_spec", return_value=spec),
                  mock.patch.object(hook, "_is_foreign", return_value=True),
                  mock.patch.object(hook, "_abort",
                                    side_effect=RuntimeError("denied"))):
                with self.assertRaisesRegex(RuntimeError, "denied"):
                    finder.find_spec("flwr")

    def test_flower_clientapp_loader_is_wrapped_after_module_exec(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
                json.dump({"dp-track": "neural"}, fh)
            hook, finder = _load_hook(root)

            calls = []

            def original(*args, **kwargs):
                calls.append((args, kwargs))
                return object()

            class Loader:
                def create_module(self, spec):
                    return None

                def exec_module(self, module):
                    module.load_app = original

            spec = SimpleNamespace(
                origin="/trusted/site-packages/flwr/clientapp/utils.py",
                submodule_search_locations=None,
                loader=Loader(),
            )
            with (mock.patch.object(hook._PathFinder, "find_spec", return_value=spec),
                  mock.patch.object(hook, "_is_foreign", return_value=False)):
                wrapped_spec = finder.find_spec("flwr.clientapp.utils")

            module = SimpleNamespace()
            wrapped_spec.loader.exec_module(module)
            self.assertTrue(module.load_app._dsflower_entrypoint_guard)

            with (mock.patch.object(hook, "_abort",
                                    side_effect=RuntimeError("denied")) as abort):
                with self.assertRaisesRegex(RuntimeError, "denied"):
                    module.load_app("json:loads", ValueError, "/uploaded/fab")
            abort.assert_called_once()
            self.assertEqual(calls, [])

    def test_canonical_clientapp_ref_loads_only_after_hash_pin_activation(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
                json.dump({"dp-track": "neural"}, fh)
            hook, finder = _load_hook(root)
            runner_spec = SimpleNamespace(
                origin="/uploaded/dsflower_runner/__init__.py",
                submodule_search_locations=["/uploaded/dsflower_runner"],
            )
            loaded_app = object()

            def original(ref, *args, **kwargs):
                self.assertEqual(ref, "dsflower_runner.client_app:app")
                finder.find_spec("dsflower_runner")
                return loaded_app

            module = SimpleNamespace(load_app=original)
            hook._install_clientapp_load_guard(module)
            with (mock.patch.object(hook._PathFinder, "find_spec",
                                    return_value=runner_spec),
                  mock.patch.object(hook, "_verify_foreign") as verify):
                result = module.load_app(
                    "dsflower_runner.client_app:app", ValueError, "/uploaded/fab"
                )

            self.assertIs(result, loaded_app)
            verify.assert_called_once_with(
                "dsflower_runner", "/uploaded/dsflower_runner")
            self.assertIn("dsflower_runner", hook._verified_packages)

    def test_node_track_selects_only_its_exact_clientapp_reference(self):
        for track, allowed, denied in (
                ("neural", "dsflower_runner.client_app:app",
                 "dsflower_runner.native_tree_client_app:app"),
                ("native_tree", "dsflower_runner.native_tree_client_app:app",
                 "dsflower_runner.client_app:app"),
                ("native_validation",
                 "dsflower_runner.native_tree_validation_client_app:app",
                 "dsflower_runner.client_app:app"),
                ("association", "dsflower_runner.association_client_app:app",
                 "dsflower_runner.client_app:app")):
            with self.subTest(track=track), tempfile.TemporaryDirectory() as root:
                with open(os.path.join(root, "manifest.json"), "w",
                          encoding="utf-8") as fh:
                    manifest = ({
                        "dp-track": "validation",
                        "validation-model-track": "native_tree",
                    } if track == "native_validation" else {"dp-track": track})
                    json.dump(manifest, fh)
                hook, _finder = _load_hook(root)
                hook._verified_packages.add("dsflower_runner")
                calls = []

                def original(ref, *args, **kwargs):
                    calls.append((ref, args, kwargs))
                    return object()

                module = SimpleNamespace(load_app=original)
                hook._install_clientapp_load_guard(module)
                module.load_app(allowed)
                self.assertEqual(calls, [(allowed, (), {})])
                with (mock.patch.object(
                        hook, "_abort", side_effect=RuntimeError("denied")),
                      self.assertRaisesRegex(RuntimeError, "denied")):
                    module.load_app(denied)


if __name__ == "__main__":
    unittest.main()
