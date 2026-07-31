"""Regression tests for the SuperNode parent-process import boundary."""

import importlib.util
import json
import os
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
                json.dump({}, fh)
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
                json.dump({}, fh)
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


if __name__ == "__main__":
    unittest.main()
