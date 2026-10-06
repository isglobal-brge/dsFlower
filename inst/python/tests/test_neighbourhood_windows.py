"""Portable Windows-adapter contract tests; not Windows runtime certification."""
import ctypes as ct
import errno
import os
from pathlib import Path
import stat
import sys
import types
import unittest
from unittest import mock

RUNNER = Path(__file__).resolve().parents[2] / "flower_app" / "dsflower_runner"
sys.path.insert(0, str(RUNNER))
import neighbourhood_windows as windows
import release_cache


class WindowsStoreAdapterTests(unittest.TestCase):
    def test_windows_path_normalization_and_cross_drive_mounts(self):
        self.assertEqual(windows.normalize_path("C:/node/privacy/store"), r"C:\node\privacy\store")
        self.assertTrue(windows.overlaps("C:/node", "c:/NODE/privacy/store"))
        self.assertFalse(windows.overlaps("D:/node", "C:/node"))
        self.assertFalse(windows.overlaps("C:/node", "C:/node-extra"))
        for path in ("relative", "C:relative", "\\root-relative", "C:/bad\x00path"):
            with self.assertRaises(RuntimeError):
                windows.normalize_path(path)
        self.assertEqual(windows.normalize_path("//host/share/private"), r"\\host\share\private")

    def test_windows_sqlite_uri_preserves_drive_and_encodes_private_path(self):
        self.assertEqual(windows.sqlite_uri("C:/a b/anchors.sqlite3"),
                         "file:///C:/a%20b/anchors.sqlite3?mode=rw")
        self.assertEqual(windows.sqlite_uri("//host/share/private/anchors.sqlite3"),
                         "file://host/share/private/anchors.sqlite3?mode=rw")

    def test_ancestor_acl_checks_keep_private_directory_and_drive_root(self):
        with mock.patch.object(windows, "_metadata") as checked:
            windows.safe_parent("C:/node/private", private=True)
        self.assertEqual([call.args[0] for call in checked.call_args_list],
                         [r"C:\node\private", r"C:\node", "C:\\"])
        self.assertTrue(checked.call_args_list[0].kwargs["private"])
        self.assertFalse(checked.call_args_list[-1].kwargs["private"])
        self.assertTrue(checked.call_args_list[-1].kwargs["parent_chain"])

    def test_lock_waits_past_ten_contentions_and_unlocks_same_byte(self):
        fake = types.SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0,
            locking=mock.Mock(side_effect=[OSError(errno.EACCES, "busy")] * 12 + [None, None]))
        with (mock.patch.dict(sys.modules, {"msvcrt": fake}),
              mock.patch.object(windows.os, "lseek") as seek,
              mock.patch.object(windows.time, "sleep") as sleep):
            windows.acquire_lock(23)
            windows.release_lock(23)
        self.assertEqual(sleep.call_count, 12)
        self.assertEqual(fake.locking.call_args_list[-1], mock.call(23, fake.LK_UNLCK, 1))
        self.assertEqual(seek.call_args_list[-1], mock.call(23, 0, os.SEEK_SET))

    def test_lock_does_not_retry_invalid_descriptor_or_other_io_failure(self):
        fake = types.SimpleNamespace(LK_NBLCK=2,
            locking=mock.Mock(side_effect=OSError(errno.EBADF, "bad descriptor")))
        with (mock.patch.dict(sys.modules, {"msvcrt": fake}),
              mock.patch.object(windows.os, "lseek"),
              mock.patch.object(windows.time, "sleep") as sleep):
            with self.assertRaises(OSError):
                windows.acquire_lock(23)
        sleep.assert_not_called()

    def test_created_file_rejects_identity_swap_and_closes_descriptor(self):
        first = types.SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_nlink=1,
                                      st_dev=1, st_ino=2)
        swapped = types.SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_nlink=1,
                                        st_dev=1, st_ino=3)
        with (mock.patch.object(windows, "safe_parent"),
              mock.patch.object(windows, "_metadata", side_effect=[first, swapped]),
              mock.patch.object(windows.os, "open", return_value=23),
              mock.patch.object(windows.os, "fstat", return_value=first),
              mock.patch.object(windows.os, "close") as closed):
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                windows.safe_file("C:/private/file", create=True)
        closed.assert_called_once_with(23)

    def test_created_file_unsafe_acl_closes_descriptor_without_repair(self):
        with (mock.patch.object(windows, "safe_parent"),
              mock.patch.object(windows, "_metadata", side_effect=RuntimeError("unsafe ACL")),
              mock.patch.object(windows.os, "open", return_value=23),
              mock.patch.object(windows.os, "close") as closed):
            with self.assertRaisesRegex(RuntimeError, "unsafe ACL"):
                windows.safe_file("C:/private/file", create=True)
        closed.assert_called_once_with(23)

    def test_private_acl_rejects_untrusted_read_grant_public_bundle_accepts(self):
        helper = release_cache._acl_helper()
        acl = helper._Acl()
        acl.ace_count = 1
        ace = ct.create_string_buffer(24)
        ct.cast(ace, ct.POINTER(helper._AceHeader)).contents.ace_type = 0
        ct.c_uint32.from_buffer(ace, 4).value = 1  # FILE_READ_DATA
        def pointer_number(value):
            return value.value if isinstance(value, ct.c_void_p) else ct.addressof(value)
        def security(path, kind, fields, owner, group, dacl, sacl, descriptor):
            ct.cast(owner, ct.POINTER(ct.c_void_p))[0] = ct.c_void_p(1000)
            ct.cast(dacl, ct.POINTER(ct.c_void_p))[0] = ct.c_void_p(ct.addressof(acl))
            ct.cast(descriptor, ct.POINTER(ct.c_void_p))[0] = ct.c_void_p(9000)
            return 0
        def get_ace(dacl, index, output):
            ct.cast(output, ct.POINTER(ct.c_void_p))[0] = ct.c_void_p(ct.addressof(ace))
            return 1
        def sid_string(text, output):
            ct.cast(output, ct.POINTER(ct.c_void_p))[0] = ct.c_void_p(5000)
            return 1
        advapi, kernel = mock.Mock(), mock.Mock()
        advapi.GetNamedSecurityInfoW.side_effect = security
        advapi.GetAce.side_effect = get_ace
        advapi.ConvertStringSidToSidW.side_effect = sid_string
        advapi.IsValidSid.return_value = 1
        advapi.EqualSid.side_effect = lambda left, right: pointer_number(left) == pointer_number(right)
        with (mock.patch.object(helper.ct, "WinDLL", create=True,
                                side_effect=lambda name, **kwargs: advapi if name == "advapi32" else kernel),
              mock.patch.object(helper, "_windows_current_user_sid", return_value=ct.c_void_p(1000)),
              mock.patch.object(helper, "_windows_well_known_sid", side_effect=lambda lib, kind: ct.c_void_p(kind * 100))):
            helper._windows_secure_acl("C:/public/bundle", require_node_owner=True)
            with self.assertRaises(helper.BundleVerificationError):
                helper._windows_secure_acl("C:/private/anchors", require_node_owner=True, private=True)

    def test_pin_flushes_before_nonreplacing_write_through_publication(self):
        events = []
        kernel = mock.Mock()
        kernel.CreateFileW.return_value = 42
        kernel.FlushFileBuffers.side_effect = lambda handle: events.append("flush") or 1
        kernel.MoveFileExW.side_effect = lambda old, new, flags: events.append(("move", flags)) or 1
        fake = types.SimpleNamespace(open_osfhandle=mock.Mock(return_value=23), get_osfhandle=lambda fd: 42)
        with (mock.patch.dict(sys.modules, {"msvcrt": fake}),
              mock.patch.object(windows, "_kernel", return_value=kernel),
              mock.patch.object(windows, "safe_parent"),
              mock.patch.object(windows, "_metadata"),
              mock.patch.object(windows.os, "write", return_value=36),
              mock.patch.object(windows.os, "close"),
              mock.patch.object(windows.os.path, "lexists", return_value=False),
              mock.patch.object(windows.os, "O_BINARY", 0x8000, create=True),
              mock.patch.object(windows.os, "O_NOINHERIT", 0x80, create=True)):
            windows.publish_pin("C:/private/pin", b"a" * 36)
        self.assertEqual(events, ["flush", ("move", 8)])
        self.assertEqual(kernel.CreateFileW.call_args.args[4], 1)  # CREATE_NEW
        self.assertTrue(kernel.CreateFileW.call_args.args[5] & 0x80000000)
        kernel.CloseHandle.assert_not_called()  # CRT descriptor owns the handle


if __name__ == "__main__":
    unittest.main()
