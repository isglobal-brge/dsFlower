"""Tests for the node-owned XGBoost bundle trust boundary."""

import ctypes as ct
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


FLOWER_APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "..", "flower_app")
sys.path.insert(0, FLOWER_APP)

from dsflower_runner import xgboost_bundle as bundle


_REAL_CDLL = ct.CDLL


class _Function:
    def __init__(self, callback=lambda *_args: 0):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *arguments):
        return self.callback(*arguments)


class _PrimitiveLibrary:
    def __init__(self, *, abi=2, mechanism=None):
        mechanism = mechanism or bundle.DP_PRIMITIVES_MECHANISM.encode("ascii")
        self.dsflower_dp_primitives_abi_version = _Function(lambda: abi)
        self.dsflower_dp_primitives_mechanism_id = _Function(lambda: mechanism)


class _XGBoostLibrary:
    def __init__(self, *, status=None, status_result=0):
        status = status or bundle.XGBOOST_STATUS.encode("ascii")

        def report(output):
            ct.cast(output, ct.POINTER(ct.c_char_p))[0] = status
            return status_result

        self.XGBDsFlowerPrivacyScaffoldStatus = _Function(report)
        for name in (
                "XGBDsFlowerSetPrivacyContext",
                "XGBDsFlowerClearPrivacyContext",
                "XGBDsFlowerPrivacyContextReady",
                "XGBSetGlobalConfig",
                "XGDMatrixCreateFromMat",
                "XGDMatrixSetFloatInfo",
                "XGDMatrixFree",
                "XGBoosterCreate",
                "XGBoosterSetParam",
                "XGBoosterUpdateOneIter",
                "XGBoosterSaveModelToBuffer",
                "XGBoosterFree"):
            setattr(self, name, _Function())


def _manifest(xgboost_bytes, primitive_bytes):
    system, machine = bundle._expected_platform()
    xgboost_path, primitive_path = bundle._expected_library_paths(system)
    return {
        "schema": bundle.BUNDLE_SCHEMA,
        "bundle_version": 1,
        "platform": {"system": system, "machine": machine},
        "xgboost": {
            "path": xgboost_path,
            "sha256": hashlib.sha256(xgboost_bytes).hexdigest(),
            "privacy_context_abi": 3,
            "status": bundle.XGBOOST_STATUS,
            "mechanism": bundle.XGBOOST_MECHANISM,
        },
        "dp_primitives": {
            "path": primitive_path,
            "sha256": hashlib.sha256(primitive_bytes).hexdigest(),
            "abi": 2,
            "mechanism": bundle.DP_PRIMITIVES_MECHANISM,
        },
        "provenance": {
            "upstream_commit": bundle.EXPECTED_UPSTREAM_COMMIT,
            "upstream_tree": bundle.EXPECTED_UPSTREAM_TREE,
            "patched_tree": bundle.EXPECTED_PATCHED_TREE,
            "patchset_version": bundle.EXPECTED_PATCHSET_VERSION,
        },
    }


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii") + b"\n"


class _BundleDirectory:
    def __init__(self):
        # The loader deliberately rejects shared temp/workspace ACLs.  A child
        # of the service account's home models the node-owned deployment root.
        self.temporary = tempfile.TemporaryDirectory(dir=Path.home())
        self.root = Path(self.temporary.name).resolve()
        if os.name == "nt":
            identity = subprocess.run(
                ["whoami"], check=True, capture_output=True, text=True,
            ).stdout.strip()
            if not identity:
                raise RuntimeError("Windows test identity is unavailable")
            subprocess.run([
                "icacls", str(self.root), "/inheritance:r", "/grant:r",
                "%s:(OI)(CI)F" % identity,
            ], check=True, capture_output=True, text=True)
        self.xgboost_bytes = b"real-xgboost-binary"
        self.primitive_bytes = b"real-dp-primitive"
        self.manifest = _manifest(self.xgboost_bytes, self.primitive_bytes)
        (self.root / "lib").mkdir()
        (self.root / self.manifest["xgboost"]["path"]).write_bytes(
            self.xgboost_bytes)
        (self.root / self.manifest["dp_primitives"]["path"]).write_bytes(
            self.primitive_bytes)
        self.write_manifest()

    def write_manifest(self, raw=None):
        (self.root / bundle.MANIFEST_NAME).write_bytes(
            _canonical(self.manifest) if raw is None else raw)

    def close(self):
        self.temporary.cleanup()


class XGBoostBundleTests(unittest.TestCase):
    def setUp(self):
        self.directory = _BundleDirectory()
        self.xgboost = _XGBoostLibrary()
        self.primitive = _PrimitiveLibrary()

        def load(path, **_kwargs):
            if path is None:
                return _REAL_CDLL(None, **_kwargs)
            return self.xgboost if "xgboost" in os.path.basename(path) \
                else self.primitive

        self.loader = mock.patch.object(bundle.ct, "CDLL", side_effect=load)
        self.cdll = self.loader.start()

    def tearDown(self):
        self.loader.stop()
        self.directory.close()

    def test_exact_bundle_loads_without_search_path_mutation(self):
        environment = dict(os.environ)
        verified = bundle.load_verified_xgboost_bundle(self.directory.root)
        raw = (self.directory.root / bundle.MANIFEST_NAME).read_bytes()
        self.assertIsInstance(verified, bundle.TrustedXGBoostBundle)
        self.assertEqual(verified.bundle_sha256,
                         hashlib.sha256(raw).hexdigest())
        self.assertTrue(bundle.is_verified_bundle(verified))
        with self.assertRaises(AttributeError):
            verified._bundle_sha256 = "0" * 64
        with self.assertRaises(AttributeError):
            verified._xgboost = object()
        self.assertNotIn(str(self.directory.root), repr(verified))
        self.assertEqual(environment, dict(os.environ))
        self.assertTrue(bundle.capability(self.directory.root))
        self.assertTrue(bundle.probe_xgboost_bundle(
            self.directory.root).available)
        loaded = [call.args[0] for call in self.cdll.call_args_list
                  if call.args[0] is not None]
        self.assertTrue(all(os.path.isabs(path) for path in loaded))

    def test_manifest_must_be_exact_canonical_and_complete(self):
        variants = []
        variants.append(_canonical(self.directory.manifest)[:-1])
        pretty = json.dumps(self.directory.manifest, indent=2).encode("ascii") + b"\n"
        variants.append(pretty)
        extra = dict(self.directory.manifest, extra=True)
        variants.append(_canonical(extra))
        duplicate = _canonical(self.directory.manifest).replace(
            b'{"bundle_version":1,', b'{"bundle_version":1,"bundle_version":1,')
        variants.append(duplicate)
        for raw in variants:
            with self.subTest(raw=raw[:30]):
                self.directory.write_manifest(raw)
                with self.assertRaises(bundle.BundleVerificationError):
                    bundle.load_verified_xgboost_bundle(self.directory.root)
        self.assertFalse(bundle.capability(self.directory.root))

    def test_hash_provenance_status_and_abi_mismatch_fail_closed(self):
        xgboost_file = self.directory.root / self.directory.manifest["xgboost"]["path"]
        xgboost_file.write_bytes(b"tampered")
        with self.assertRaises(bundle.BundleVerificationError):
            bundle.load_verified_xgboost_bundle(self.directory.root)
        xgboost_file.write_bytes(self.directory.xgboost_bytes)

        self.directory.manifest["provenance"]["patched_tree"] = "0" * 40
        self.directory.write_manifest()
        with self.assertRaises(bundle.BundleVerificationError):
            bundle.load_verified_xgboost_bundle(self.directory.root)
        self.directory.manifest["provenance"]["patched_tree"] = \
            bundle.EXPECTED_PATCHED_TREE
        self.directory.write_manifest()

        self.xgboost.XGBDsFlowerPrivacyScaffoldStatus = _XGBoostLibrary(
            status=b"secret/native/path/status").XGBDsFlowerPrivacyScaffoldStatus
        error = bundle.probe_xgboost_bundle(self.directory.root)
        self.assertFalse(error.available)
        self.assertEqual(error.error_code, "invalid_bundle")
        self.assertNotIn("secret", repr(error.error_code))

        self.xgboost = _XGBoostLibrary()
        self.primitive = _PrimitiveLibrary(abi=99)
        self.assertFalse(bundle.capability(self.directory.root))

    def test_unexpected_loader_failure_is_bounded_without_native_text(self):
        secret = "native /private/path diagnostic"
        self.primitive.dsflower_dp_primitives_abi_version = _Function(
            lambda: (_ for _ in ()).throw(RuntimeError(secret)))
        with self.assertRaises(bundle.BundleVerificationError) as raised:
            bundle.load_verified_xgboost_bundle(self.directory.root)
        self.assertEqual(raised.exception.code, "internal_error")
        self.assertNotIn(secret, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_extra_files_insecure_modes_and_links_are_rejected(self):
        extra = self.directory.root / "lib" / "unhashed-library.so"
        extra.write_bytes(b"extra")
        with self.assertRaises(bundle.BundleVerificationError):
            bundle.load_verified_xgboost_bundle(self.directory.root)
        extra.unlink()

        if os.name == "posix":
            manifest_file = self.directory.root / bundle.MANIFEST_NAME
            manifest_file.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IWOTH)
            with self.assertRaises(bundle.BundleVerificationError):
                bundle.load_verified_xgboost_bundle(self.directory.root)
            manifest_file.chmod(stat.S_IRUSR | stat.S_IWUSR)

        if hasattr(os, "symlink"):
            primitive_path = self.directory.root / self.directory.manifest[
                "dp_primitives"]["path"]
            target = self.directory.root / "primitive-real"
            target.write_bytes(self.directory.primitive_bytes)
            primitive_path.unlink()
            try:
                primitive_path.symlink_to(target)
            except OSError:
                self.skipTest("symlinks are unavailable")
            with self.assertRaises(bundle.BundleVerificationError):
                bundle.load_verified_xgboost_bundle(self.directory.root)

    @unittest.skipUnless(os.name == "posix", "POSIX owner invariant")
    def test_wrong_posix_owner_is_rejected(self):
        manifest_file = self.directory.root / bundle.MANIFEST_NAME
        actual = os.lstat(manifest_file)
        values = list(actual)
        values[4] = os.geteuid() + 10_000
        wrong_owner = os.stat_result(values)
        with mock.patch.object(bundle.os, "lstat", return_value=wrong_owner), \
                self.assertRaises(bundle.BundleVerificationError):
            bundle._secure_metadata(manifest_file)

    def test_windows_acl_check_is_mandatory_not_best_effort(self):
        manifest_file = self.directory.root / bundle.MANIFEST_NAME
        rejection = bundle.BundleVerificationError("invalid_bundle")
        with mock.patch.object(bundle.os, "name", "nt"), \
                mock.patch.object(
                    bundle, "_windows_secure_acl",
                    side_effect=rejection) as acl, \
                self.assertRaises(bundle.BundleVerificationError):
            bundle._secure_metadata(manifest_file)
        acl.assert_called_once_with(
            manifest_file, require_node_owner=True, parent_chain=False)

    @unittest.skipUnless(os.name == "nt", "Windows DACL invariant")
    def test_windows_real_acl_success_path(self):
        try:
            bundle._secure_parent_chain(self.directory.root)
            bundle._windows_secure_acl(
                self.directory.root, require_node_owner=True,
                parent_chain=False)
            bundle._windows_secure_acl(
                self.directory.root / bundle.MANIFEST_NAME,
                require_node_owner=True, parent_chain=False)
        except bundle.BundleVerificationError:
            details = subprocess.run(
                ["icacls", str(self.directory.root)], check=True,
                capture_output=True, text=True,
            ).stdout
            self.fail("Windows fixture DACL was rejected:\n%s" % details)

    @unittest.skipUnless(
        sys.platform.startswith("linux"), "Linux POSIX ACL invariant")
    def test_linux_posix_acl_uses_the_effective_mode_mask(self):
        manifest_file = self.directory.root / bundle.MANIFEST_NAME
        with mock.patch.object(bundle.os, "listxattr", return_value=[
                "system.posix_acl_access", "system.posix_acl_default"]):
            bundle._secure_metadata(manifest_file)
        with mock.patch.object(
                bundle.os, "listxattr", return_value=["system.nfs4_acl"]), \
                self.assertRaises(bundle.BundleVerificationError):
            bundle._secure_metadata(manifest_file)

    @unittest.skipUnless(
        sys.platform == "darwin", "macOS extended ACL invariant")
    def test_macos_extended_acl_is_rejected(self):
        manifest_file = self.directory.root / bundle.MANIFEST_NAME
        subprocess.run(
            ["chmod", "+a", "everyone allow write", str(manifest_file)],
            check=True, capture_output=True)
        try:
            with self.assertRaises(bundle.BundleVerificationError):
                bundle._secure_metadata(manifest_file)
        finally:
            subprocess.run(
                ["chmod", "-N", str(manifest_file)],
                check=True, capture_output=True)

    @unittest.skipUnless(
        sys.platform == "darwin", "macOS custodial root ACL invariant")
    def test_macos_custodial_root_extended_acl_is_rejected(self):
        subprocess.run(
            ["chmod", "+a", "everyone deny delete", str(self.directory.root)],
            check=True, capture_output=True)
        try:
            with self.assertRaises(bundle.BundleVerificationError):
                bundle.load_verified_xgboost_bundle(self.directory.root)
        finally:
            subprocess.run(
                ["chmod", "-N", str(self.directory.root)],
                check=True, capture_output=True)

    @unittest.skipUnless(
        sys.platform == "darwin", "macOS ancestor ACL invariant")
    def test_macos_parent_deny_delete_is_safe_but_allow_is_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            parent = Path(temporary).resolve()
            child = parent / "bundle"
            child.mkdir()
            subprocess.run(
                ["chmod", "+a", "everyone deny delete_child", str(parent)],
                check=True, capture_output=True)
            try:
                bundle._secure_parent_chain(child)
            finally:
                subprocess.run(
                    ["chmod", "-N", str(parent)],
                    check=True, capture_output=True)
            subprocess.run(
                ["chmod", "+a", "everyone allow delete_child", str(parent)],
                check=True, capture_output=True)
            try:
                with self.assertRaises(bundle.BundleVerificationError):
                    bundle._secure_parent_chain(child)
            finally:
                subprocess.run(
                    ["chmod", "-N", str(parent)],
                    check=True, capture_output=True)

    def test_relative_root_and_direct_handle_construction_are_forbidden(self):
        with self.assertRaises(bundle.BundleVerificationError):
            bundle.load_verified_xgboost_bundle("relative/bundle")
        with self.assertRaises(TypeError):
            bundle.TrustedXGBoostBundle(
                token=object(),
                bundle_sha256="0" * 64, xgboost=None, dp_primitives=None)

    def test_dynamic_loader_override_environment_fails_closed(self):
        for name in (
                "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
                "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH",
                "DYLD_PRINT_LIBRARIES"):
            with self.subTest(name=name), mock.patch.dict(
                    os.environ, {name: "/untrusted/injection"}), \
                    self.assertRaises(bundle.BundleVerificationError):
                bundle.load_verified_xgboost_bundle(self.directory.root)

    @unittest.skipUnless(os.name == "posix", "POSIX parent mode invariant")
    def test_writable_parent_chain_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary).resolve()
            child = parent / "bundle"
            child.mkdir()
            parent.chmod(0o777)
            try:
                with self.assertRaises(bundle.BundleVerificationError):
                    bundle._secure_parent_chain(child)
            finally:
                parent.chmod(0o700)

    def test_all_platform_library_names_are_fixed(self):
        self.assertEqual(bundle._expected_library_paths("linux"), (
            "lib/libxgboost.so", "lib/libdsflower_dp_primitives.so"))
        self.assertEqual(bundle._expected_library_paths("macos"), (
            "lib/libxgboost.dylib", "lib/libdsflower_dp_primitives.dylib"))
        self.assertEqual(bundle._expected_library_paths("windows"), (
            "lib/xgboost.dll", "lib/dsflower_dp_primitives.dll"))


if __name__ == "__main__":
    unittest.main()
