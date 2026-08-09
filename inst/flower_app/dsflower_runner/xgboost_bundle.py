"""Verified loader for the node-owned native XGBoost DP bundle.

The bundle is trusted configuration, not an analyst input.  This module checks
its canonical manifest, provenance, platform, ownership and every binary byte
before any dynamic library is loaded.  It never mutates process search-path
environment variables and never returns native diagnostic strings.  It must run
in the fresh native ClientApp subprocess before any submitted package is
imported; Python/dynamic-loader state is not an isolation boundary.
"""

from __future__ import annotations

import ctypes as ct
from ctypes import wintypes
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat


BUNDLE_SCHEMA = "dsflower-xgboost-bundle-v1"
BUNDLE_VERSION = 1
XGBOOST_PRIVACY_CONTEXT_ABI = 3
DP_PRIMITIVES_ABI = 2
XGBOOST_STATUS = "bundle-core:fixed-point-discrete-v1:internal-only"
XGBOOST_MECHANISM = "xgboost/fixed-point-discrete/v1"
DP_PRIMITIVES_MECHANISM = "cks20-discrete-gaussian-i64-hmac-sha256-v1"
EXPECTED_UPSTREAM_COMMIT = "06335b125dccb859aacef142675506bfb84401b3"
EXPECTED_UPSTREAM_TREE = "bfea7a1cb9cca3156478da0a077bd637d0749dea"
EXPECTED_PATCHSET_VERSION = 3
# Updated atomically with the production patchset.  A different tree is never
# accepted merely because a locally supplied manifest names it.
EXPECTED_PATCHED_TREE = "376740e907457f52c96745e52440a94a9aab4177"

MANIFEST_NAME = "manifest.json"
MANIFEST_MAX_BYTES = 64 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OBJECT_RE = re.compile(r"[0-9a-f]{40}\Z")
_ERROR_CODES = frozenset((
    "unavailable", "invalid_bundle", "unsupported", "internal_error",
))
_CONSTRUCTION_TOKEN = object()
_FORBIDDEN_LOADER_ENV_PREFIXES = ("LD_", "DYLD_")


class BundleVerificationError(RuntimeError):
    """Bounded bundle error that deliberately omits paths/native diagnostics."""

    def __init__(self, code):
        self.code = code if code in _ERROR_CODES else "internal_error"
        super().__init__("native XGBoost bundle verification failed")


class XGBoostBundleProbe:
    """Fixed-shape result suitable for a node capability probe."""

    __slots__ = ("available", "bundle_sha256", "error_code")

    def __init__(self, *, available, bundle_sha256=None, error_code=None):
        self.available = bool(available)
        self.bundle_sha256 = bundle_sha256 if self.available else None
        self.error_code = None if self.available else error_code


class TrustedXGBoostBundle:
    """Frozen result of complete verification and successful ABI probing.

    This is defense in depth inside the trusted runner.  Python object privacy
    is not a sandbox: submitted code must never execute in this process.
    """

    __slots__ = (
        "_bundle_sha256", "_dp_primitives", "_sealed", "_xgboost",
    )

    def __init__(self, *, token, bundle_sha256,
                 xgboost, dp_primitives):
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("trusted bundles are created only by the verifier")
        object.__setattr__(self, "_bundle_sha256", bundle_sha256)
        object.__setattr__(self, "_xgboost", xgboost)
        object.__setattr__(self, "_dp_primitives", dp_primitives)
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False):
            raise AttributeError("trusted native bundle handle is frozen")
        object.__setattr__(self, name, value)

    def __repr__(self):
        return "TrustedXGBoostBundle(bundle_sha256=%r)" % self.bundle_sha256

    @property
    def bundle_sha256(self):
        return self._bundle_sha256


def is_verified_bundle(value):
    return type(value) is TrustedXGBoostBundle and \
        getattr(value, "_sealed", False) is True and \
        isinstance(getattr(value, "_bundle_sha256", None), str) and \
        _SHA256_RE.fullmatch(value._bundle_sha256) is not None and \
        getattr(value, "_xgboost", None) is not None and \
        getattr(value, "_dp_primitives", None) is not None


def _reject(code="invalid_bundle"):
    raise BundleVerificationError(code) from None


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _reject()
        result[key] = value
    return result


def _exact_fields(value, expected):
    if not isinstance(value, dict) or frozenset(value) != frozenset(expected):
        _reject()
    return value


def _expected_platform():
    systems = {"linux": "linux", "darwin": "macos", "windows": "windows"}
    machines = {
        "amd64": "x86_64", "x64": "x86_64", "x86_64": "x86_64",
        "aarch64": "aarch64", "arm64": "aarch64",
    }
    system = systems.get(platform.system().strip().lower())
    machine = machines.get(platform.machine().strip().lower())
    if system is None or machine is None:
        _reject("unsupported")
    return system, machine


def _expected_library_paths(system):
    if system == "linux":
        return "lib/libxgboost.so", "lib/libdsflower_dp_primitives.so"
    if system == "macos":
        return "lib/libxgboost.dylib", "lib/libdsflower_dp_primitives.dylib"
    return "lib/xgboost.dll", "lib/dsflower_dp_primitives.dll"


def _is_reparse_point(metadata):
    flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    attributes = getattr(metadata, "st_file_attributes", 0)
    return bool(attributes & flag)


class _Acl(ct.Structure):
    _fields_ = [
        ("revision", ct.c_ubyte),
        ("reserved", ct.c_ubyte),
        ("size", ct.c_ushort),
        ("ace_count", ct.c_ushort),
        ("reserved2", ct.c_ushort),
    ]


class _AceHeader(ct.Structure):
    _fields_ = [
        ("ace_type", ct.c_ubyte),
        ("ace_flags", ct.c_ubyte),
        ("ace_size", ct.c_ushort),
    ]


class _SidAndAttributes(ct.Structure):
    _fields_ = [("sid", ct.c_void_p), ("attributes", wintypes.DWORD)]


class _TokenUser(ct.Structure):
    _fields_ = [("user", _SidAndAttributes)]


def _windows_current_user_sid(advapi, kernel):
    token = wintypes.HANDLE()
    if not advapi.OpenProcessToken(
            kernel.GetCurrentProcess(), 0x0008, ct.byref(token)):
        _reject()
    try:
        required = wintypes.DWORD(0)
        advapi.GetTokenInformation(token, 1, None, 0, ct.byref(required))
        if required.value < ct.sizeof(_TokenUser):
            _reject()
        storage = ct.create_string_buffer(required.value)
        if not advapi.GetTokenInformation(
                token, 1, storage, required, ct.byref(required)):
            _reject()
        sid = ct.cast(storage, ct.POINTER(_TokenUser)).contents.user.sid
        if not sid or not advapi.IsValidSid(sid):
            _reject()
        length = advapi.GetLengthSid(sid)
        copied = ct.create_string_buffer(length)
        if not advapi.CopySid(length, copied, sid):
            _reject()
        return copied
    finally:
        kernel.CloseHandle(token)


def _windows_well_known_sid(advapi, sid_type):
    size = wintypes.DWORD(68)
    storage = ct.create_string_buffer(size.value)
    if not advapi.CreateWellKnownSid(
            sid_type, None, storage, ct.byref(size)):
        _reject()
    return storage


def _windows_secure_acl(path, *, require_node_owner, parent_chain=False):
    """Reject replaceable Windows paths using owner and DACL inspection."""
    try:
        advapi = ct.WinDLL("advapi32", use_last_error=True)
        kernel = ct.WinDLL("kernel32", use_last_error=True)
    except (AttributeError, OSError):
        _reject("unsupported")

    advapi.GetNamedSecurityInfoW.argtypes = [
        wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD,
        ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_void_p),
        ct.POINTER(ct.c_void_p), ct.POINTER(ct.c_void_p),
        ct.POINTER(ct.c_void_p),
    ]
    advapi.GetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi.GetAce.argtypes = [
        ct.c_void_p, wintypes.DWORD, ct.POINTER(ct.c_void_p)]
    advapi.GetAce.restype = wintypes.BOOL
    advapi.IsValidSid.argtypes = [ct.c_void_p]
    advapi.IsValidSid.restype = wintypes.BOOL
    advapi.EqualSid.argtypes = [ct.c_void_p, ct.c_void_p]
    advapi.EqualSid.restype = wintypes.BOOL
    advapi.GetLengthSid.argtypes = [ct.c_void_p]
    advapi.GetLengthSid.restype = wintypes.DWORD
    advapi.CopySid.argtypes = [wintypes.DWORD, ct.c_void_p, ct.c_void_p]
    advapi.CopySid.restype = wintypes.BOOL
    advapi.CreateWellKnownSid.argtypes = [
        wintypes.DWORD, ct.c_void_p, ct.c_void_p,
        ct.POINTER(wintypes.DWORD)]
    advapi.CreateWellKnownSid.restype = wintypes.BOOL
    advapi.ConvertStringSidToSidW.argtypes = [
        wintypes.LPCWSTR, ct.POINTER(ct.c_void_p)]
    advapi.ConvertStringSidToSidW.restype = wintypes.BOOL
    advapi.OpenProcessToken.argtypes = [
        wintypes.HANDLE, wintypes.DWORD, ct.POINTER(wintypes.HANDLE)]
    advapi.OpenProcessToken.restype = wintypes.BOOL
    advapi.GetTokenInformation.argtypes = [
        wintypes.HANDLE, wintypes.DWORD, ct.c_void_p, wintypes.DWORD,
        ct.POINTER(wintypes.DWORD)]
    advapi.GetTokenInformation.restype = wintypes.BOOL
    kernel.GetCurrentProcess.argtypes = []
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    kernel.LocalFree.argtypes = [ct.c_void_p]
    kernel.LocalFree.restype = ct.c_void_p

    owner = ct.c_void_p()
    dacl = ct.c_void_p()
    descriptor = ct.c_void_p()
    result = advapi.GetNamedSecurityInfoW(
        str(path), 1, 0x00000001 | 0x00000004,
        ct.byref(owner), None, ct.byref(dacl), None, ct.byref(descriptor))
    if result != 0 or not owner.value or not dacl.value:
        if descriptor.value:
            kernel.LocalFree(descriptor)
        _reject()
    try:
        current = _windows_current_user_sid(advapi, kernel)
        local_system = _windows_well_known_sid(advapi, 22)
        administrators = _windows_well_known_sid(advapi, 26)
        owner_rights = _windows_well_known_sid(advapi, 71)
        trusted_installer = ct.c_void_p()
        if not advapi.ConvertStringSidToSidW(
                "S-1-5-80-956008885-3418522649-1831038044-"
                "1853292631-2271478464", ct.byref(trusted_installer)):
            _reject()
        try:
            trusted_owners = (
                current, local_system, administrators, trusted_installer)
            if require_node_owner and not any(
                    advapi.EqualSid(owner, sid) for sid in trusted_owners):
                _reject()
            # OWNER RIGHTS (S-1-3-4) resolves to the already-validated owner;
            # it does not grant an independent principal access to the path.
            trusted_sids = trusted_owners + (owner_rights,)

            acl = ct.cast(dacl, ct.POINTER(_Acl)).contents
            replacement_rights = (
                0x10000000 | 0x40000000 |  # GENERIC_ALL / GENERIC_WRITE
                0x00010000 | 0x00040000 | 0x00080000 |  # delete/DACL/owner
                0x00000040  # delete-child
            )
            content_write_rights = (
                0x00000002 | 0x00000004 | 0x00000010
            )
            write_rights = replacement_rights | (
                0 if parent_chain else content_write_rights)
            allowed_types = frozenset((0, 4, 5, 9, 11))
            for index in range(acl.ace_count):
                ace = ct.c_void_p()
                if not advapi.GetAce(
                        dacl, index, ct.byref(ace)) or not ace.value:
                    _reject()
                header = ct.cast(ace, ct.POINTER(_AceHeader)).contents
                if header.ace_type not in allowed_types or \
                        header.ace_flags & 0x08:  # INHERIT_ONLY_ACE
                    continue
                mask = ct.c_uint32.from_address(ace.value + 4).value
                if not mask & write_rights:
                    continue
                # ACCESS_ALLOWED_ACE has Mask followed immediately by SidStart.
                # Compound/object/callback layouts are variable; writable ones
                # are rejected rather than guessed at.
                if header.ace_type != 0:
                    _reject()
                sid = ct.c_void_p(ace.value + 8)
                if not advapi.IsValidSid(sid) or not any(
                        advapi.EqualSid(sid, trusted)
                        for trusted in trusted_sids):
                    _reject()
        finally:
            if trusted_installer.value:
                kernel.LocalFree(trusted_installer)
    finally:
        kernel.LocalFree(descriptor)


def _reject_extended_acl(path, *, parent_chain=False):
    system = platform.system().strip().lower()
    if system == "darwin":
        library = ct.CDLL(None, use_errno=True)
        library.acl_get_file.argtypes = [ct.c_char_p, ct.c_int]
        library.acl_get_file.restype = ct.c_void_p
        library.acl_free.argtypes = [ct.c_void_p]
        library.acl_free.restype = ct.c_int
        library.acl_get_entry.argtypes = [
            ct.c_void_p, ct.c_int, ct.POINTER(ct.c_void_p)]
        library.acl_get_entry.restype = ct.c_int
        library.acl_get_tag_type.argtypes = [
            ct.c_void_p, ct.POINTER(ct.c_int)]
        library.acl_get_tag_type.restype = ct.c_int
        library.acl_get_permset_mask_np.argtypes = [
            ct.c_void_p, ct.POINTER(ct.c_uint64)]
        library.acl_get_permset_mask_np.restype = ct.c_int
        library.acl_get_flagset_np.argtypes = [
            ct.c_void_p, ct.POINTER(ct.c_void_p)]
        library.acl_get_flagset_np.restype = ct.c_int
        library.acl_get_flag_np.argtypes = [ct.c_void_p, ct.c_uint]
        library.acl_get_flag_np.restype = ct.c_int
        ct.set_errno(0)
        acl = library.acl_get_file(os.fsencode(path), 0x00000100)
        if acl:
            try:
                if not parent_chain:
                    _reject()
                entry = ct.c_void_p()
                entry_id = 0  # ACL_FIRST_ENTRY
                replacement_rights = (
                    (1 << 4) | (1 << 6) | (1 << 12) | (1 << 13))
                while True:
                    ct.set_errno(0)
                    result = library.acl_get_entry(
                        acl, entry_id, ct.byref(entry))
                    if result == -1 and ct.get_errno() == errno.EINVAL:
                        break
                    if result != 0 or not entry.value:
                        _reject()
                    entry_id = -1  # ACL_NEXT_ENTRY
                    tag = ct.c_int()
                    permissions = ct.c_uint64()
                    flags = ct.c_void_p()
                    if library.acl_get_tag_type(
                            entry, ct.byref(tag)) != 0 or \
                            library.acl_get_permset_mask_np(
                                entry, ct.byref(permissions)) != 0 or \
                            library.acl_get_flagset_np(
                                entry, ct.byref(flags)) != 0:
                        _reject()
                    inherit_only = library.acl_get_flag_np(flags, 1 << 8)
                    if inherit_only < 0:
                        _reject()
                    if tag.value == 1 and not inherit_only and \
                            permissions.value & replacement_rights:
                        _reject()
            finally:
                library.acl_free(acl)
            return
        acl_error = ct.get_errno()
        if acl_error not in (0, errno.ENOENT, errno.ENOTSUP,
                             getattr(errno, "EOPNOTSUPP", errno.ENOTSUP)):
            _reject()
    elif system == "linux":
        try:
            names = os.listxattr(path, follow_symlinks=False)
        except (AttributeError, OSError):
            _reject()
        # Linux POSIX ACL write grants are bounded by the group-class mask,
        # which is reflected in st_mode and checked by _secure_metadata.
        # Unknown ACL mechanisms are not assumed to share that invariant.
        posix_acl = {"system.posix_acl_access", "system.posix_acl_default"}
        if any("acl" in name.lower() and name not in posix_acl
               for name in names):
            _reject()
    else:
        _reject("unsupported")


def _secure_metadata(path, *, directory=False, require_node_owner=True,
                     parent_chain=False):
    try:
        metadata = os.lstat(path)
    except OSError:
        _reject("unavailable")
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode) or \
            _is_reparse_point(metadata):
        _reject()
    if os.name == "posix" and hasattr(os, "geteuid"):
        allowed_owners = (0, os.geteuid())
        if metadata.st_uid not in allowed_owners or metadata.st_mode & (
                stat.S_IWGRP | stat.S_IWOTH):
            _reject()
        _reject_extended_acl(path, parent_chain=parent_chain)
    elif os.name == "nt":
        _windows_secure_acl(
            path, require_node_owner=require_node_owner,
            parent_chain=parent_chain)
    return metadata


def _secure_parent_chain(root):
    current = root.parent
    while True:
        _secure_metadata(
            current, directory=True, require_node_owner=True,
            parent_chain=True)
        parent = current.parent
        if parent == current:
            break
        current = parent


def _same_file(first, second):
    return first.st_dev == second.st_dev and first.st_ino == second.st_ino and \
        first.st_size == second.st_size and \
        first.st_mtime_ns == second.st_mtime_ns


def _read_file(path, *, max_bytes=None, expected_sha256=None):
    before = _secure_metadata(path)
    if max_bytes is not None and before.st_size > max_bytes:
        _reject()
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | \
        getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        _reject()
    digest = hashlib.sha256()
    chunks = [] if max_bytes is not None else None
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or not _same_file(before, opened):
            _reject()
        total = 0
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            total += len(block)
            if max_bytes is not None and total > max_bytes:
                _reject()
            digest.update(block)
            if chunks is not None:
                chunks.append(block)
        after = os.fstat(descriptor)
        if total != opened.st_size or not _same_file(opened, after):
            _reject()
    finally:
        os.close(descriptor)
    if expected_sha256 is not None and digest.hexdigest() != expected_sha256:
        _reject()
    return b"".join(chunks) if chunks is not None else digest.hexdigest()


def _canonical_manifest(raw):
    try:
        manifest = json.loads(
            raw.decode("ascii"), object_pairs_hook=_object_without_duplicates)
    except BundleVerificationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        _reject()
    canonical = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii") + b"\n"
    if canonical != raw:
        _reject()
    return manifest


def _validate_manifest(manifest):
    _exact_fields(manifest, (
        "schema", "bundle_version", "platform", "xgboost",
        "dp_primitives", "provenance",
    ))
    if manifest["schema"] != BUNDLE_SCHEMA or \
            type(manifest["bundle_version"]) is not int or \
            manifest["bundle_version"] != BUNDLE_VERSION:
        _reject()
    system, machine = _expected_platform()
    target = _exact_fields(manifest["platform"], ("system", "machine"))
    if target != {"system": system, "machine": machine}:
        _reject("unsupported")

    expected_xgboost_path, expected_dp_path = _expected_library_paths(system)
    xgboost = _exact_fields(manifest["xgboost"], (
        "path", "sha256", "privacy_context_abi", "status", "mechanism",
    ))
    if xgboost.get("path") != expected_xgboost_path or \
            not isinstance(xgboost.get("sha256"), str) or \
            _SHA256_RE.fullmatch(xgboost["sha256"]) is None or \
            type(xgboost.get("privacy_context_abi")) is not int or \
            xgboost["privacy_context_abi"] != XGBOOST_PRIVACY_CONTEXT_ABI or \
            xgboost.get("status") != XGBOOST_STATUS or \
            xgboost.get("mechanism") != XGBOOST_MECHANISM:
        _reject()

    primitive = _exact_fields(manifest["dp_primitives"], (
        "path", "sha256", "abi", "mechanism",
    ))
    if primitive.get("path") != expected_dp_path or \
            not isinstance(primitive.get("sha256"), str) or \
            _SHA256_RE.fullmatch(primitive["sha256"]) is None or \
            type(primitive.get("abi")) is not int or \
            primitive["abi"] != DP_PRIMITIVES_ABI or \
            primitive.get("mechanism") != DP_PRIMITIVES_MECHANISM:
        _reject()

    provenance = _exact_fields(manifest["provenance"], (
        "upstream_commit", "upstream_tree", "patched_tree", "patchset_version",
    ))
    if provenance.get("upstream_commit") != EXPECTED_UPSTREAM_COMMIT or \
            provenance.get("upstream_tree") != EXPECTED_UPSTREAM_TREE or \
            not isinstance(provenance.get("patched_tree"), str) or \
            _GIT_OBJECT_RE.fullmatch(provenance["patched_tree"]) is None or \
            provenance["patched_tree"] != EXPECTED_PATCHED_TREE or \
            type(provenance.get("patchset_version")) is not int or \
            provenance["patchset_version"] != EXPECTED_PATCHSET_VERSION:
        _reject()
    return system


def _root_path(bundle_root):
    if not isinstance(bundle_root, (str, os.PathLike)):
        _reject("unavailable")
    try:
        root = Path(os.fspath(bundle_root))
        if not root.is_absolute() or root.resolve(strict=True) != root:
            _reject()
    except (OSError, RuntimeError, TypeError, ValueError):
        _reject("unavailable")
    _secure_parent_chain(root)
    _secure_metadata(root, directory=True)
    return root


def _verify_tree(root, allowed_files):
    found = set()
    for current, directories, files in os.walk(root, topdown=True,
                                               followlinks=False):
        current_path = Path(current)
        _secure_metadata(current_path, directory=True)
        relative_dir = current_path.relative_to(root).as_posix()
        if relative_dir not in (".", "lib"):
            _reject()
        for name in directories:
            child = current_path / name
            _secure_metadata(child, directory=True)
            if child.relative_to(root).as_posix() != "lib":
                _reject()
        for name in files:
            child = current_path / name
            _secure_metadata(child)
            found.add(child.relative_to(root).as_posix())
    if found != set(allowed_files):
        _reject()


def _configure_primitive_api(library):
    library.dsflower_dp_primitives_abi_version.argtypes = []
    library.dsflower_dp_primitives_abi_version.restype = ct.c_uint32
    library.dsflower_dp_primitives_mechanism_id.argtypes = []
    library.dsflower_dp_primitives_mechanism_id.restype = ct.c_char_p


def _configure_xgboost_probe(library):
    library.XGBDsFlowerPrivacyScaffoldStatus.argtypes = [
        ct.POINTER(ct.c_char_p)]
    library.XGBDsFlowerPrivacyScaffoldStatus.restype = ct.c_int


def _load_libraries(root, xgboost_path, primitive_path, system):
    primitive_file = str(root / primitive_path)
    xgboost_file = str(root / xgboost_path)
    try:
        if system == "windows":
            if not hasattr(os, "add_dll_directory"):
                _reject("unsupported")
            directory = os.add_dll_directory(str(root / "lib"))
            try:
                primitive = ct.CDLL(primitive_file)
                xgboost = ct.CDLL(xgboost_file)
            finally:
                directory.close()
        else:
            mode = getattr(os, "RTLD_NOW", 0) | getattr(os, "RTLD_GLOBAL", 0)
            primitive = ct.CDLL(primitive_file, mode=mode)
            xgboost = ct.CDLL(xgboost_file, mode=mode)
    except BundleVerificationError:
        raise
    except (AttributeError, OSError, TypeError, ValueError):
        _reject("unsupported")
    return xgboost, primitive


def _probe_abis(xgboost, primitive):
    try:
        for symbol in (
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
            getattr(xgboost, symbol)
        _configure_primitive_api(primitive)
        if primitive.dsflower_dp_primitives_abi_version() != DP_PRIMITIVES_ABI:
            _reject()
        mechanism = primitive.dsflower_dp_primitives_mechanism_id()
        if mechanism != DP_PRIMITIVES_MECHANISM.encode("ascii"):
            _reject()
        _configure_xgboost_probe(xgboost)
        status = ct.c_char_p()
        if xgboost.XGBDsFlowerPrivacyScaffoldStatus(ct.byref(status)) != 0 or \
                status.value != XGBOOST_STATUS.encode("ascii"):
            _reject()
    except BundleVerificationError:
        raise
    except (AttributeError, OSError, TypeError, ValueError):
        _reject()


def _load_xgboost_bundle(bundle_root):
    """Verify, load and probe one exact node-owned production bundle."""
    if any(value and name.upper().startswith(
            _FORBIDDEN_LOADER_ENV_PREFIXES)
            for name, value in os.environ.items()):
        _reject()
    root = _root_path(bundle_root)
    manifest_path = root / MANIFEST_NAME
    raw = _read_file(manifest_path, max_bytes=MANIFEST_MAX_BYTES)
    manifest = _canonical_manifest(raw)
    system = _validate_manifest(manifest)
    xgboost_path = manifest["xgboost"]["path"]
    primitive_path = manifest["dp_primitives"]["path"]
    _verify_tree(root, (MANIFEST_NAME, xgboost_path, primitive_path))
    _read_file(root / xgboost_path,
               expected_sha256=manifest["xgboost"]["sha256"])
    _read_file(root / primitive_path,
               expected_sha256=manifest["dp_primitives"]["sha256"])
    xgboost, primitive = _load_libraries(
        root, xgboost_path, primitive_path, system)
    # Detect administrator replacement between byte verification and dlopen.
    _read_file(root / xgboost_path,
               expected_sha256=manifest["xgboost"]["sha256"])
    _read_file(root / primitive_path,
               expected_sha256=manifest["dp_primitives"]["sha256"])
    _probe_abis(xgboost, primitive)
    return TrustedXGBoostBundle(
        token=_CONSTRUCTION_TOKEN,
        bundle_sha256=hashlib.sha256(raw).hexdigest(),
        xgboost=xgboost,
        dp_primitives=primitive,
    )


def probe_xgboost_bundle(bundle_root):
    """Return only bounded capability state; never paths or native errors."""
    try:
        bundle = _load_xgboost_bundle(bundle_root)
        return XGBoostBundleProbe(
            available=True, bundle_sha256=bundle.bundle_sha256)
    except BundleVerificationError as exc:
        return XGBoostBundleProbe(available=False, error_code=exc.code)
    except Exception:
        return XGBoostBundleProbe(
            available=False, error_code="internal_error")


def load_verified_xgboost_bundle(bundle_root):
    """Stable runner API for obtaining an opaque verified bundle handle."""
    try:
        return _load_xgboost_bundle(bundle_root)
    except BundleVerificationError:
        raise
    except Exception:
        raise BundleVerificationError("internal_error") from None


def capability(bundle_root):
    """True only when the complete manifest, bytes, status and ABI probe pass."""
    return probe_xgboost_bundle(bundle_root).available


__all__ = [
    "BUNDLE_SCHEMA",
    "BundleVerificationError",
    "TrustedXGBoostBundle",
    "XGBoostBundleProbe",
    "capability",
    "is_verified_bundle",
    "load_verified_xgboost_bundle",
    "probe_xgboost_bundle",
]
