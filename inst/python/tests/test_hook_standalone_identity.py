"""Standalone Hook parents retain typed v3 identity and trusted import origins."""
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


RUNNER = Path(__file__).resolve().parents[2] / "flower_app" / "dsflower_runner"


class StandaloneHookIdentityTests(unittest.TestCase):
    def test_standalone_hook_typed_request_binding_and_shadow_resistance(self):
        code = r'''
            import importlib.util, os, pathlib, sys
            from unittest import mock
            import numpy as np
            root, shadow = map(pathlib.Path, sys.argv[1:])
            for name in ("seeding", "strategy", "task", "canonical_units", "dp_harness"):
                (shadow / (name + ".py")).write_text("raise AssertionError('untrusted shadow executed')")
            (shadow / "dsflower_runner").mkdir()
            (shadow / "dsflower_runner" / "__init__.py").write_text("raise AssertionError('shadow package executed')")
            sys.path.insert(0, str(shadow))
            secret = shadow / "node-secret"
            secret.write_text((b"s" * 32).hex())
            secret.chmod(0o600)
            os.environ["DSFLOWER_NODE_SECRET_FILE"] = str(secret)
            spec = importlib.util.spec_from_file_location("standalone_hook", root / "tier2_lib.py")
            hook = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = hook
            spec.loader.exec_module(hook)
            cfg = {"app_params": {}, "round_index": 1, "num_rounds": 1,
                   "task": "regression", "num_classes": 2}
            policy = {"epsilon": 2., "delta": 1e-6, "clipping_norm": 1.,
                      "sample_aggregate": True, "sa_blocks": 4, "adjacency": "replace_one"}
            manifest = {"data_type": "tabular", "feature_columns": ["x"],
                        "target_column": "y", "dp-unit": "row", "task-type": "regression"}
            arrays = [np.zeros((1, 1))]
            X, y = np.array([[1.], [3.], [1.]]), np.array([0., 1., 0.])
            with mock.patch.object(hook, "_pinned_user_package", return_value="a" * 64):
                request = hook.hook_request_identity("upload", arrays, cfg, policy, manifest=manifest)
                def release(x, target, ids=None):
                    return hook.hook_master_seed("upload", arrays, x, target, cfg, policy,
                                                 unit_ids=ids, request_identity=request)
                first = release(X, y)
                assert first == release(X[::-1], y[::-1])
                assert first != release(X + 1, y)
                assert release(X, y, ["p", "q", "p"]) == release(X[::-1], y[::-1], ["p", "q", "p"])
                execution = hook.hook_execution_seed("upload", arrays, cfg, policy, request_identity=request)
                assert len(first) == len(execution) == 32
                assert first != execution
            # The canonicalizer also supports an explicit standalone import.
            spec = importlib.util.spec_from_file_location("standalone_units", root / "canonical_units.py")
            units = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = units
            spec.loader.exec_module(units)
            assert units.canonicalize_arrays(X, y, ["p", "q", "p"]).multiset_digest
            # Existing poisoned namespace entries must fail before execution.
            trusted = hook._trusted_import("canonical_units")
            old = trusted.__file__
            trusted.__file__ = str(shadow / "canonical_units.py")
            try:
                try:
                    hook._trusted_import("canonical_units")
                except RuntimeError as exc:
                    assert "another path" in str(exc)
                else:
                    raise AssertionError("cached untrusted origin was accepted")
            finally:
                trusted.__file__ = old
        '''
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, "-c", textwrap.dedent(code),
                                     str(RUNNER), directory], text=True,
                                    capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
