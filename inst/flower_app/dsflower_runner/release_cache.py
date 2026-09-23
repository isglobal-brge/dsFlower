"""Durable exact replies for gated Hooks, with public admission reservations.

Only released arrays and constant metrics are cached. Keys are independent
subkeys of the v2 semantic master; no master or noise key is stored. SQLite
commits precede replies, and persistent run pins survive process crashes.
The configured quota covers encoded replies and conservative metadata charges;
SQLite journals and filesystem allocation overhead need additional disk space.
"""

import argparse
from contextlib import contextmanager, ExitStack
from functools import lru_cache
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import stat
import struct
import sys
import sysconfig


MAX_ENTRY_BYTES = 65 * 1024 * 1024
_MAX_ARRAY_BYTES = 64 * 1024 * 1024
_MAX_HEADER_BYTES = 1024 * 1024
_MAX_ARRAYS = 256
_MAX_NDIM = 8
_MAX_ELEMENTS = 8_000_000
_STORE_BYTES = 65536
_LOCK_SHARDS = 64
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN = re.compile(r"run_[0-9a-f]{32}\Z")
_COORDINATE = re.compile(r"claim:train:0:([1-9][0-9]{0,2})\Z")
_MAGIC = b"dsflower-release-cache-v1\x00"
_METRICS = {"num-examples": 1, "hook-executed": 1}
_DB_NAME = "releases.sqlite3"


def cache_key(master):
    """Derive the cache identifier without retaining the semantic master."""
    try:
        from . import seeding
    except ImportError:  # Direct module tests; CLI admission never imports it.
        import seeding
    return seeding.sub_seed(master, "gated-release-cache-key/v1").hex()


def run_fingerprint(run_token):
    if not isinstance(run_token, str) or _TOKEN.fullmatch(run_token) is None:
        raise RuntimeError("invalid gated cache run token")
    return hashlib.sha256(
        b"dsflower/release-ledger/run/v1\x00" + run_token.encode("ascii")
    ).hexdigest()


def _hex(value, label):
    if not isinstance(value, str) or _HEX.fullmatch(value) is None:
        raise RuntimeError("invalid gated cache %s" % label)
    return value


def _integer(value, label, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise RuntimeError("invalid gated cache %s" % label)
    return value


def _metadata_bytes(rounds):
    # Closed run tombstones retain their charge: repeated runs cannot grow an
    # unbounded ledger outside the administrator's admission quota.
    return 4096 + 2048 * rounds


@lru_cache(maxsize=1)
def _acl_validator():
    if __package__:
        from . import xgboost_bundle
    else:
        # The administrator CLI runs as ``python -I /trusted/release_cache.py``.
        # Import only the colocated trusted helper, never a sys.path candidate.
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xgboost_bundle.py")
        spec = importlib.util.spec_from_file_location("_dsflower_cache_acl", path)
        xgboost_bundle = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(xgboost_bundle)
    return xgboost_bundle._reject_extended_acl


def _reject_unsafe_acl(path, *, parent_chain=False):
    try:
        _acl_validator()(path, parent_chain=parent_chain)
    except RuntimeError as exc:
        raise RuntimeError("gated cache permissions contain unsafe or unsupported ACLs") from exc


def _safe_file(path, *, create=False):
    flags = (os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
             | getattr(os, "O_CLOEXEC", 0))
    if create:
        flags |= os.O_CREAT
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise RuntimeError("unsafe or unavailable gated cache file") from exc
    try:
        info = os.fstat(fd)
        named = os.lstat(path)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)):
            raise RuntimeError("gated cache files require owner-only mode 0600")
        _reject_unsafe_acl(path)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _safe_directory(path, forbidden_dirs):
    if (os.name != "posix" or not hasattr(os, "O_NOFOLLOW")
            or not hasattr(os, "geteuid")):
        raise RuntimeError("gated cache requires POSIX ownership and file locks")
    if (not isinstance(path, str) or not os.path.isabs(path)
            or os.path.normpath(path) != path):
        raise RuntimeError("gated cache needs a safe absolute directory")
    for forbidden in forbidden_dirs:
        if not forbidden:
            continue
        root = os.path.realpath(forbidden)
        # Reject both ancestors and descendants, including a cache directory
        # which would contain staging or an installed Hook mount.
        if os.path.commonpath((path, root)) in (path, root):
            raise RuntimeError("gated cache must be outside staging and Hook mounts")
    current = os.path.sep
    _reject_unsafe_acl(current, parent_chain=True)
    for component in path.strip(os.path.sep).split(os.path.sep):
        current = os.path.join(current, component)
        if current == path:
            try:
                os.mkdir(path, 0o700)
            except FileExistsError:
                pass
        try:
            info = os.lstat(current)
        except OSError as exc:
            raise RuntimeError("gated cache parent directory is unavailable") from exc
        if not stat.S_ISDIR(info.st_mode):
            raise RuntimeError("gated cache path must contain no symlinks")
        if info.st_uid not in (0, os.geteuid()):
            raise RuntimeError("gated cache path has unsafe ownership")
        if current != path:
            # A root-owned sticky temporary directory cannot rename somebody
            # else's owned child. Other writable ancestors are unsafe.
            sticky_root = info.st_uid == 0 and info.st_mode & stat.S_ISVTX
            if info.st_mode & 0o022 and not sticky_root:
                raise RuntimeError("gated cache parent is writable by other users")
        elif info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise RuntimeError("gated cache directory requires owner-only mode 0700")
        _reject_unsafe_acl(current, parent_chain=current != path)
    return path


def _hook_mounts():
    # Match the runtime/code mounts exposed by tier2_lib._code_dirs without
    # importing the Hook runtime during the administrator's admission CLI.
    paths = sysconfig.get_paths()
    return [sys.prefix, sys.exec_prefix,
            os.path.dirname(os.path.abspath(sys.executable)),
            os.path.dirname(os.path.abspath(__file__)),
            os.environ.get("DSFLOWER_PINNED_APP_DIR"),
            os.environ.get("DSFLOWER_MANIFEST_DIR")] + [
                paths.get(key) for key in ("stdlib", "platstdlib", "purelib", "platlib")]


def _encode(arrays, metrics):
    import numpy as np

    if (not isinstance(metrics, dict) or metrics != _METRICS
            or any(type(value) is not int for value in metrics.values())):
        raise RuntimeError("gated cache accepts only constant release metrics")
    if not isinstance(arrays, (list, tuple)) or not 1 <= len(arrays) <= _MAX_ARRAYS:
        raise RuntimeError("gated cache array count exceeds the public cap")
    metadata, chunks = [], []
    total_bytes = total_elements = 0
    for array in arrays:
        array = np.asarray(array)
        if (array.dtype.kind not in "biuf" or array.dtype.hasobject
                or array.ndim > _MAX_NDIM or array.dtype.fields is not None):
            raise RuntimeError("gated cache requires bounded numeric arrays")
        total_bytes += array.nbytes
        total_elements += array.size
        if total_bytes > _MAX_ARRAY_BYTES or total_elements > _MAX_ELEMENTS:
            raise RuntimeError("gated cache arrays exceed the public model-size cap")
        metadata.append({"dtype": array.dtype.str, "shape": list(array.shape),
                         "bytes": array.nbytes})
        chunks.append(array.tobytes(order="C"))
    header = json.dumps({"arrays": metadata, "metrics": metrics},
                        sort_keys=True, separators=(",", ":")).encode("ascii")
    if len(header) > _MAX_HEADER_BYTES:
        raise RuntimeError("gated cache header exceeds its public cap")
    encoded = _MAGIC + struct.pack(">I", len(header)) + header + b"".join(chunks)
    if len(encoded) > MAX_ENTRY_BYTES:
        raise RuntimeError("gated cache encoding exceeds its public cap")
    return encoded


def _decode(encoded):
    import numpy as np

    if (not isinstance(encoded, bytes) or len(encoded) > MAX_ENTRY_BYTES
            or not encoded.startswith(_MAGIC)
            or len(encoded) < len(_MAGIC) + 4):
        raise RuntimeError("invalid gated cache reply encoding")
    offset = len(_MAGIC) + 4
    length = struct.unpack(">I", encoded[len(_MAGIC):offset])[0]
    if length > _MAX_HEADER_BYTES or offset + length > len(encoded):
        raise RuntimeError("invalid gated cache reply header")
    try:
        header = json.loads(encoded[offset:offset + length])
        offset += length
        if set(header) != {"arrays", "metrics"}:
            raise ValueError("unexpected header")
        metrics = header["metrics"]
        if (metrics != _METRICS or not isinstance(metrics, dict)
                or any(type(value) is not int for value in metrics.values())):
            raise ValueError("nonconstant metrics")
        entries = header["arrays"]
        if not isinstance(entries, list) or not 1 <= len(entries) <= _MAX_ARRAYS:
            raise ValueError("array count")
        arrays, total_elements, total_bytes = [], 0, 0
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"dtype", "shape", "bytes"}:
                raise ValueError("array metadata")
            dtype = np.dtype(entry["dtype"])
            shape = entry["shape"]
            if (dtype.kind not in "biuf" or dtype.hasobject
                    or dtype.fields is not None or not isinstance(shape, list)
                    or len(shape) > _MAX_NDIM
                    or any(type(value) is not int or value < 0
                           or value > _MAX_ELEMENTS for value in shape)):
                raise ValueError("array geometry")
            elements = 1
            for dimension in shape:
                elements *= dimension
            size = elements * dtype.itemsize
            total_elements += elements
            total_bytes += size
            if (type(entry["bytes"]) is not int or entry["bytes"] != size
                    or total_elements > _MAX_ELEMENTS
                    or total_bytes > _MAX_ARRAY_BYTES or offset + size > len(encoded)):
                raise ValueError("array size")
            arrays.append(np.frombuffer(encoded, dtype=dtype, count=elements,
                                        offset=offset).reshape(shape).copy())
            offset += size
        if offset != len(encoded):
            raise ValueError("trailing data")
        return arrays, dict(metrics)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise RuntimeError("invalid gated cache reply encoding") from exc


class ReleaseCache:
    def __init__(self, directory, capacity_bytes, *, forbidden_dirs=()):
        self.capacity_bytes = _integer(capacity_bytes, "capacity", 1, 2**63 - 1)
        self.directory = _safe_directory(directory, list(forbidden_dirs) + _hook_mounts())
        self.database = os.path.join(directory, _DB_NAME)
        with self._lock(_LOCK_SHARDS):
            for name in os.listdir(directory):
                if (name not in (_DB_NAME, _DB_NAME + "-journal")
                        and re.fullmatch(r"lock-[0-9a-f]{2}", name) is None):
                    raise RuntimeError("unexpected file in gated cache directory")
                os.close(_safe_file(os.path.join(directory, name)))
            os.close(_safe_file(self.database, create=True))
            with self._connection() as connection:
                connection.execute("CREATE TABLE IF NOT EXISTS cache_version (version INTEGER)")
                version = connection.execute("SELECT version FROM cache_version").fetchall()
                if not version:
                    connection.execute("INSERT INTO cache_version VALUES (1)")
                elif version != [(1,)]:
                    raise RuntimeError("unsupported gated cache version")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS runs (run TEXT PRIMARY KEY, "
                    "rounds INTEGER NOT NULL, entry_bytes INTEGER NOT NULL, "
                    "closed INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS entries (sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "entry_key TEXT UNIQUE NOT NULL, payload BLOB NOT NULL, digest TEXT NOT NULL)")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS claims (run TEXT NOT NULL, coordinate TEXT NOT NULL, "
                    "request_id TEXT NOT NULL, entry_key TEXT NOT NULL, committed INTEGER NOT NULL, "
                    "PRIMARY KEY (run, coordinate)) WITHOUT ROWID")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS pins (run TEXT NOT NULL, entry_key TEXT NOT NULL, "
                    "PRIMARY KEY (run, entry_key)) WITHOUT ROWID")
            self._sync_directory()

    @classmethod
    def from_env(cls, *, forbidden_dirs=()):
        directory = os.environ.get("DSFLOWER_RELEASE_CACHE_DIR", "")
        raw = os.environ.get("DSFLOWER_RELEASE_CACHE_BYTES", "")
        if re.fullmatch(r"[1-9][0-9]{0,18}", raw) is None:
            raise RuntimeError("administrator gated cache capacity is missing or invalid")
        return cls(directory, int(raw), forbidden_dirs=forbidden_dirs)

    def _sync_directory(self):
        fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @contextmanager
    def _lock(self, shard):
        import fcntl

        _safe_directory(self.directory, ())
        fd = _safe_file(os.path.join(self.directory, "lock-%02x" % shard), create=True)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def _transaction(self):
        # Serialize short metadata operations separately from long per-key
        # leases. This also prevents races validating transient SQLite journals.
        with self._lock(_LOCK_SHARDS):
            with self._connection() as connection:
                yield connection

    @contextmanager
    def _connection(self):
        # The protected, non-symlink directory excludes untrusted path swaps.
        _safe_directory(self.directory, ())
        os.close(_safe_file(self.database))
        journal = self.database + "-journal"
        if os.path.lexists(journal):
            os.close(_safe_file(journal))
        connection = sqlite3.connect(self.database, timeout=60.0, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=EXTRA")
            connection.execute("PRAGMA fullfsync=ON")
            connection.execute("PRAGMA secure_delete=ON")
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _usage(self, connection):
        runs = connection.execute("SELECT rounds, entry_bytes, closed FROM runs").fetchall()
        usage = _STORE_BYTES + sum(
            _metadata_bytes(rounds) + (0 if closed else rounds * entry_bytes)
            for rounds, entry_bytes, closed in runs)
        usage += connection.execute(
            "SELECT COALESCE(SUM(length(payload)), 0) FROM entries e "
            "WHERE NOT EXISTS (SELECT 1 FROM pins p WHERE p.entry_key=e.entry_key)"
        ).fetchone()[0]
        return usage

    def _enforce_capacity(self, connection):
        usage = self._usage(connection)
        while usage > self.capacity_bytes:
            oldest = connection.execute(
                "SELECT e.entry_key, length(e.payload) FROM entries e "
                "WHERE NOT EXISTS (SELECT 1 FROM pins p WHERE p.entry_key=e.entry_key) "
                "ORDER BY sequence LIMIT 1").fetchone()
            if oldest is None:
                raise RuntimeError("gated cache capacity is exhausted before private work")
            connection.execute("DELETE FROM entries WHERE entry_key=?", (oldest[0],))
            usage -= oldest[1]

    def reserve_run(self, run_fingerprint, num_rounds, entry_bytes=MAX_ENTRY_BYTES):
        """Reserve every round's public maximum before loading private inputs."""
        run = _hex(run_fingerprint, "run fingerprint")
        rounds = _integer(num_rounds, "round count", 1, 500)
        entry_bytes = _integer(entry_bytes, "entry reservation", 1, MAX_ENTRY_BYTES)
        with self._transaction() as connection:
            previous = connection.execute(
                "SELECT rounds, entry_bytes, closed FROM runs WHERE run=?", (run,)
            ).fetchone()
            if previous is not None:
                if previous[2]:
                    raise RuntimeError("gated cache run is administratively closed")
                if previous[:2] != (rounds, entry_bytes):
                    raise RuntimeError("gated cache run reservation changed")
                # A previously admitted run keeps its capacity even if the
                # administrator later changes the quota for new admissions.
                return
            else:
                connection.execute("INSERT INTO runs (run, rounds, entry_bytes) VALUES (?, ?, ?)",
                                   (run, rounds, entry_bytes))
            self._enforce_capacity(connection)

    @contextmanager
    def release(self, run_fingerprint, coordinate, request_id, key):
        """Hold per-key exclusion through lookup, Hook execution and commit."""
        run = _hex(run_fingerprint, "run fingerprint")
        key = _hex(key, "semantic key")
        request_id = _hex(request_id, "public request identity")
        coordinate_match = (_COORDINATE.fullmatch(coordinate)
                            if isinstance(coordinate, str) else None)
        if coordinate_match is None:
            raise RuntimeError("invalid gated cache release coordinate")
        with self._lock(int(key[:2], 16) % _LOCK_SHARDS):
            with self._transaction() as connection:
                reserved = connection.execute(
                    "SELECT rounds, entry_bytes, closed FROM runs WHERE run=?", (run,)
                ).fetchone()
                if reserved is None or reserved[2]:
                    raise RuntimeError("gated cache run is unreserved or administratively closed")
                if int(coordinate_match[1]) > reserved[0]:
                    raise RuntimeError("gated cache coordinate exceeds the public reservation")
                previous = connection.execute(
                    "SELECT request_id, entry_key, committed FROM claims WHERE run=? AND coordinate=?",
                    (run, coordinate)).fetchone()
                if previous is not None and previous[:2] != (request_id, key):
                    raise RuntimeError("committed release coordinate has a different semantic identity")
                entry = connection.execute(
                    "SELECT payload, digest FROM entries WHERE entry_key=?", (key,)).fetchone()
                if previous is not None and entry is None:
                    raise RuntimeError("claimed release coordinate has no durable exact reply")
                if entry is not None:
                    if len(entry[0]) > reserved[1] or hashlib.sha256(entry[0]).hexdigest() != entry[1]:
                        raise RuntimeError("gated cache reply is corrupt or exceeds its reservation")
                    cached = _decode(entry[0])
                else:
                    cached = None
                if previous is None:
                    connection.execute(
                        "INSERT INTO claims VALUES (?, ?, ?, ?, ?)",
                        (run, coordinate, request_id, key, int(entry is not None)))
                elif entry is not None:
                    connection.execute("UPDATE claims SET committed=1 WHERE run=? AND coordinate=?",
                                       (run, coordinate))
                connection.execute("INSERT OR IGNORE INTO pins VALUES (?, ?)", (run, key))
            slot = _ReleaseSlot(self, run, coordinate, key, reserved[1], cached)
            try:
                yield slot
            finally:
                slot.active = False

    def close_run(self, run_fingerprint):
        """Authoritatively close after all in-flight releases finish; retain tombstone."""
        run = _hex(run_fingerprint, "run fingerprint")
        with ExitStack() as stack:
            for shard in range(_LOCK_SHARDS):
                stack.enter_context(self._lock(shard))
            with self._transaction() as connection:
                # Cleanup can race an admission whose public reservation has
                # not yet committed. The tombstone also closes that late run.
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO runs (run, rounds, entry_bytes, closed) VALUES (?, 0, 0, 1)",
                    (run,)).rowcount
                connection.execute("UPDATE runs SET closed=1 WHERE run=?", (run,))
                connection.execute("DELETE FROM pins WHERE run=?", (run,))
                if inserted:
                    self._enforce_capacity(connection)


class _ReleaseSlot:
    def __init__(self, cache, run, coordinate, key, entry_bytes, cached):
        self.cache, self.run, self.coordinate, self.key = cache, run, coordinate, key
        self.entry_bytes, self.cached, self.active = entry_bytes, cached, True

    def commit(self, arrays, metrics):
        if not self.active or self.cached is not None:
            raise RuntimeError("gated cache release cannot be committed twice")
        encoded = _encode(arrays, metrics)
        if len(encoded) > self.entry_bytes:
            raise RuntimeError("gated cache reply exceeds its public reservation")
        with self.cache._transaction() as connection:
            row = connection.execute("SELECT closed FROM runs WHERE run=?", (self.run,)).fetchone()
            claim = connection.execute(
                "SELECT entry_key, committed FROM claims WHERE run=? AND coordinate=?",
                (self.run, self.coordinate)).fetchone()
            if row != (0,) or claim != (self.key, 0):
                raise RuntimeError("gated cache release is closed or already committed")
            connection.execute("INSERT INTO entries (entry_key, payload, digest) VALUES (?, ?, ?)",
                               (self.key, encoded, hashlib.sha256(encoded).hexdigest()))
            connection.execute("UPDATE claims SET committed=1 WHERE run=? AND coordinate=?",
                               (self.run, self.coordinate))
        self.cached = _decode(encoded)


def _main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("reserve", "close"))
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--run-token")
    identity.add_argument("--run-fingerprint")
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--forbidden-dir", action="append", default=[])
    args = parser.parse_args()
    try:
        cache = ReleaseCache.from_env(forbidden_dirs=args.forbidden_dir)
        run = run_fingerprint(args.run_token) if args.run_token else args.run_fingerprint
        if args.action == "reserve":
            cache.reserve_run(run, args.rounds)
        else:
            cache.close_run(run)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        parser.exit(1, "gated release cache: %s\n" % exc)


if __name__ == "__main__":
    _main()
