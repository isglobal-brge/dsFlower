"""Windows custody/locking for the permanent neighbourhood store.

SQLite uses its Windows VFS for committed database durability. The first UUID
pin is flushed and published without replacement using MoveFileExW's documented
write-through flag. Win32 does not provide POSIX directory fsync; no successful
payload precedes durable pin publication. Unsafe ACLs, missing file identities,
reparse points and failed durability operations are errors, never fallbacks.
"""
from functools import lru_cache
import errno
import ntpath
import os
from pathlib import PureWindowsPath
import stat
import time
import uuid

try:
    from . import release_cache, seeding
except ImportError:
    import release_cache
    import seeding


def normalize_path(path):
    if not isinstance(path, str) or "\x00" in path or not ntpath.isabs(path):
        raise RuntimeError("Windows neighbourhood state needs an absolute path")
    path = ntpath.normpath(path)
    if not ntpath.splitdrive(path)[0]:
        raise RuntimeError("Windows neighbourhood state needs a drive or UNC root")
    return path


def overlaps(left, right):
    left, right = ntpath.normcase(normalize_path(left)), ntpath.normcase(normalize_path(right))
    if ntpath.splitdrive(left)[0] != ntpath.splitdrive(right)[0]:
        return False
    return ntpath.commonpath((left, right)) in (left, right)


def sqlite_uri(path):
    return PureWindowsPath(normalize_path(path)).as_uri() + "?mode=rw"


def _metadata(path, *, directory=False, parent_chain=False, private=False):
    helper = release_cache._acl_helper()
    result = helper._secure_metadata(path, directory=directory,
                                     require_node_owner=True, parent_chain=parent_chain)
    if private:
        helper._windows_secure_acl(path, require_node_owner=True, private=True)
    if not directory and result.st_nlink != 1:
        raise RuntimeError("Windows neighbourhood files must not be hardlinked")
    return result


def safe_parent(path, *, private=False):
    current = normalize_path(path)
    first = True
    while True:
        _metadata(current, directory=True, parent_chain=not (first and private),
                  private=first and private)
        parent = ntpath.dirname(current)
        if parent == current:
            return
        current, first = parent, False


def safe_directory(path, forbidden_dirs=()):
    path = normalize_path(path)
    for forbidden in forbidden_dirs:
        if forbidden and overlaps(path, os.path.realpath(forbidden)):
            raise RuntimeError("neighbourhood state must be outside staging and Hook mounts")
    safe_parent(ntpath.dirname(path))
    try:
        # The custodian's private parent provides an inheritable private DACL.
        # Validate the actual resulting DACL before writing any sensitive bytes.
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    _metadata(path, directory=True, private=True)
    return path


def safe_file(path, *, create=False):
    path = normalize_path(path)
    safe_parent(ntpath.dirname(path), private=True)
    flags = os.O_RDWR | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOINHERIT", 0)
    created = False
    if create:
        try:
            fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            fd = None
    else:
        fd = None
    if fd is None:
        before = _metadata(path, private=True)
        fd = os.open(path, flags)
    else:
        before = None
    try:
        if before is None:
            before = _metadata(path, private=True)
        info = os.fstat(fd)
        named = _metadata(path, private=True)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or not seeding._same_file_identity(before, info, windows=True)
                or not seeding._same_file_identity(info, named, windows=True)):
            raise RuntimeError("Windows neighbourhood file identity changed while opening")
        if created:
            os.fsync(fd)
        return fd
    except BaseException:
        os.close(fd)
        raise


def acquire_lock(fd):
    import msvcrt
    while True:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            # LK_LOCK itself stops after ten seconds. Nonblocking retries on
            # contention retain the lock for arbitrarily long admitted fits.
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise
            time.sleep(0.1)


def release_lock(fd):
    import msvcrt
    os.lseek(fd, 0, os.SEEK_SET)
    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


@lru_cache(maxsize=1)
def _kernel():
    import ctypes as ct
    from ctypes import wintypes
    kernel = ct.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  ct.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.FlushFileBuffers.argtypes = [wintypes.HANDLE]
    kernel.FlushFileBuffers.restype = wintypes.BOOL
    kernel.MoveFileExW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    kernel.MoveFileExW.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def publish_pin(path, value):
    """Flush and publish a first-use pin; never replace established custody."""
    import ctypes as ct
    import msvcrt
    path = normalize_path(path)
    safe_parent(ntpath.dirname(path), private=True)
    temporary = path + "." + uuid.uuid4().hex + ".tmp"
    kernel = _kernel()
    # CREATE_NEW, inherited private DACL, WRITE_THROUGH, OPEN_REPARSE_POINT.
    handle = kernel.CreateFileW(temporary, 0xC0000000, 0x7, None, 1,
                                0x80000000 | 0x00200000 | 0x80, None)
    if handle == ct.c_void_p(-1).value:
        raise ct.WinError(ct.get_last_error())
    descriptor = None
    try:
        _metadata(temporary, private=True)
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_BINARY | os.O_NOINHERIT)
        handle = None  # the CRT descriptor owns it from this point
        if os.write(descriptor, value) != len(value):
            raise RuntimeError("Windows neighbourhood pin write was incomplete")
        if not kernel.FlushFileBuffers(msvcrt.get_osfhandle(descriptor)):
            raise ct.WinError(ct.get_last_error())
        os.close(descriptor)
        descriptor = None
        # MOVEFILE_WRITE_THROUGH only: no REPLACE_EXISTING or COPY_ALLOWED.
        if not kernel.MoveFileExW(temporary, path, 0x8):
            raise ct.WinError(ct.get_last_error())
        _metadata(path, private=True)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if handle is not None:
            kernel.CloseHandle(handle)
        if os.path.lexists(temporary):
            os.unlink(temporary)
