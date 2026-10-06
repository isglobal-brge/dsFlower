"""Permanent, authenticated oldest-eligible neighbourhood release anchors.

The hard boundary is disclosure-control mitigation, not transcript DP. Only a
fresh complete release creates an anchor; retries never consume storage credit.
SQLite commits precede egress. MACs and an authenticated table manifest detect
row corruption/deletion, but not rollback of a whole valid backup. Keep the node
key, UUID pin and store together, and never evict or rebuild established state.
"""
from collections import Counter
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import struct
import uuid

try:
    from . import neighbourhood_windows, release_cache, seeding
except ImportError:
    import neighbourhood_windows
    import release_cache
    import seeding


STATE_ERROR = "dsFlower neighbourhood release state is unavailable; contact the data custodian"
CAPACITY_ERROR = "dsFlower neighbourhood release capacity is exhausted; contact the data custodian"
_VERSION = "dsflower-neighbourhood-v1"
_MAGIC = b"dsflower-neighbourhood-payload-v1\x00"
_BASE_BYTES = 65536
_RECORD_BYTES = 4096
_LOCK_SHARDS = 64
_UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
_HEX = re.compile(r"[0-9a-f]{64}\Z")


class StateError(RuntimeError):
    def __init__(self):
        super().__init__(STATE_ERROR)


class CapacityError(RuntimeError):
    def __init__(self):
        super().__init__(CAPACITY_ERROR)


def _frame(*parts):
    return b"".join(struct.pack(">Q", len(part)) + part for part in parts)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def _integer(value, minimum=1):
    if type(value) is not int or not minimum <= value <= 2**53 - 1:
        raise ValueError("invalid neighbourhood administrator policy")
    return value


def distance(left, right):
    """Counted insert/delete/substitute distance; replacing a unit costs one."""
    if any(type(n) is not int or n < 1 for n in list(left.values()) + list(right.values())):
        raise ValueError("unit multiplicities must be positive integers")
    common = sum(min(n, right.get(token, 0)) for token, n in left.items())
    return max(sum(left.values()) - common, sum(right.values()) - common)


def unit_fingerprints(units, key):
    """HMAC complete 0.7.1 records (whole patients), preserving duplicates."""
    records = getattr(units, "records", None)
    if records is None or not isinstance(key, bytes) or len(key) != 32:
        raise ValueError("neighbourhood requires canonical units and a private subkey")
    result = Counter()
    for record in records:
        if not isinstance(record, bytes):
            raise ValueError("neighbourhood requires canonical unit bytes")
        result[hmac.new(key, _frame(b"canonical-record-v1", record), hashlib.sha256).hexdigest()] += 1
    return result


def encode_payload(value):
    """Deterministic non-executable encoding, including exact tensor/float bits."""
    import numpy as np
    chunks = []

    def encode(item, depth=0):
        if depth > 64:
            raise ValueError("release payload nesting exceeds its limit")
        if item is None:
            return ["null"]
        if isinstance(item, np.ndarray):
            if item.dtype.hasobject or item.dtype.fields is not None or item.dtype.kind not in "biufc":
                raise ValueError("release payload requires numeric arrays")
            raw = item.tobytes(order="C")
            chunks.append(raw)
            return ["array", item.dtype.str, list(item.shape), len(raw)]
        if isinstance(item, np.generic):
            return encode(item.item(), depth)
        if isinstance(item, bool):
            return ["bool", item]
        if isinstance(item, int):
            return ["int", str(item)]
        if isinstance(item, float):
            return ["float", struct.pack(">d", item).hex()]
        if isinstance(item, str):
            return ["str", item]
        if isinstance(item, bytes):
            chunks.append(item)
            return ["bytes", len(item)]
        if isinstance(item, (list, tuple)):
            return ["tuple" if isinstance(item, tuple) else "list",
                    [encode(part, depth + 1) for part in item]]
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            return ["dict", [[key, encode(item[key], depth + 1)] for key in sorted(item)]]
        raise ValueError("unsupported release payload value")

    header = _json(encode(value))
    return _MAGIC + struct.pack(">Q", len(header)) + header + b"".join(chunks)


def decode_payload(encoded):
    """Called only after verifying the complete anchor's MAC."""
    import numpy as np
    if not isinstance(encoded, bytes) or not encoded.startswith(_MAGIC):
        raise ValueError("invalid neighbourhood payload")
    start = len(_MAGIC) + 8
    length = struct.unpack(">Q", encoded[len(_MAGIC):start])[0]
    if start + length > len(encoded):
        raise ValueError("invalid neighbourhood payload header")
    tree = json.loads(encoded[start:start + length])
    offset = start + length

    def consume(length):
        nonlocal offset
        if type(length) is not int or length < 0 or offset + length > len(encoded):
            raise ValueError("invalid neighbourhood payload length")
        result = encoded[offset:offset + length]
        offset += length
        return result

    def decode(item, depth=0):
        if depth > 64 or not isinstance(item, list) or not item:
            raise ValueError("invalid neighbourhood payload tree")
        tag = item[0]
        if tag == "null" and len(item) == 1:
            return None
        if tag == "bool" and len(item) == 2 and type(item[1]) is bool:
            return item[1]
        if tag == "int" and len(item) == 2:
            return int(item[1])
        if tag == "float" and len(item) == 2:
            return struct.unpack(">d", bytes.fromhex(item[1]))[0]
        if tag == "str" and len(item) == 2 and isinstance(item[1], str):
            return item[1]
        if tag == "bytes" and len(item) == 2:
            return consume(item[1])
        if tag == "array" and len(item) == 4:
            dtype = np.dtype(item[1])
            shape = item[2]
            if (dtype.hasobject or dtype.fields is not None or dtype.kind not in "biufc"
                    or not isinstance(shape, list) or len(shape) > 32
                    or any(type(n) is not int or n < 0 for n in shape)):
                raise ValueError("invalid neighbourhood array")
            elements = 1
            for n in shape:
                elements *= n
            if item[3] != elements * dtype.itemsize:
                raise ValueError("invalid neighbourhood array size")
            return np.frombuffer(consume(item[3]), dtype=dtype).reshape(shape).copy()
        if tag in ("list", "tuple") and len(item) == 2 and isinstance(item[1], list):
            values = [decode(part, depth + 1) for part in item[1]]
            return tuple(values) if tag == "tuple" else values
        if tag == "dict" and len(item) == 2 and isinstance(item[1], list):
            result = {}
            for key, part in item[1]:
                if not isinstance(key, str) or key in result:
                    raise ValueError("invalid neighbourhood dictionary")
                result[key] = decode(part, depth + 1)
            return result
        raise ValueError("invalid neighbourhood payload type")

    result = decode(tree)
    if offset != len(encoded):
        raise ValueError("trailing neighbourhood payload bytes")
    return result


def _safe_file(path, *, create=False):
    if os.name == "nt":
        return neighbourhood_windows.safe_file(path, create=create)
    return release_cache._safe_file(path, create=create)


def _safe_directory(path, forbidden_dirs):
    if os.name == "nt":
        return neighbourhood_windows.safe_directory(path, forbidden_dirs)
    return release_cache._safe_directory(path, forbidden_dirs)


def _overlaps(left, right):
    if os.name == "nt":
        return neighbourhood_windows.overlaps(left, right)
    return os.path.commonpath((left, right)) in (left, right)


def _sync_directory(path):
    if os.name == "nt":
        # Windows uses flushed SQLite commits and write-through UUID pin
        # publication. It has no POSIX directory-fsync API to invoke here.
        neighbourhood_windows.safe_parent(path, private=True)
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _safe_parent(path):
    if os.name == "nt":
        neighbourhood_windows.safe_parent(path, private=True)
        return
    if not os.path.isabs(path) or os.path.normpath(path) != path:
        raise StateError()
    current = os.path.sep
    for component in [""] + path.strip(os.path.sep).split(os.path.sep):
        current = os.path.join(current, component)
        info = os.lstat(current)
        sticky_root = info.st_uid == 0 and info.st_mode & stat.S_ISVTX
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, os.geteuid())
                or (info.st_mode & 0o022 and not sticky_root)):
            raise StateError()
        release_cache._reject_unsafe_acl(current, parent_chain=True)


def _check_directory(path):
    # The shared helper may create the final directory; established state may
    # never use that behavior, so reject absence before invoking it.
    if not os.path.isdir(path) or os.path.islink(path):
        raise StateError()
    return _safe_directory(path, ())


@contextmanager
def _file_lock(path, *, create=False):
    if os.name == "nt":
        lock, unlock = neighbourhood_windows.acquire_lock, neighbourhood_windows.release_lock
    else:
        import fcntl
        lock = lambda descriptor: fcntl.flock(descriptor, fcntl.LOCK_EX)
        unlock = lambda descriptor: fcntl.flock(descriptor, fcntl.LOCK_UN)
    try:
        fd = _safe_file(path, create=create)
    except (OSError, RuntimeError) as exc:
        raise StateError() from exc
    acquired = False
    try:
        lock(fd)
        acquired = True
        info, named = os.fstat(fd), os.lstat(path)
        if (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino):
            raise StateError()
        yield
    finally:
        if acquired:
            unlock(fd)
        os.close(fd)


class NeighbourhoodStore:
    def __init__(self, directory, *, secret, pin_path, k=3, max_anchors=256,
                 capacity_bytes=64 * 1024**3, expected_uuid="", forbidden_dirs=()):
        self.k = max(2, _integer(k, 0))
        self.max_anchors = _integer(max_anchors)
        self.capacity_bytes = _integer(capacity_bytes, 0)
        if not isinstance(secret, bytes) or len(secret) != 32:
            raise ValueError("neighbourhood requires a private 256-bit node key")
        self._mac_key = hmac.new(secret, _frame(_VERSION.encode(), b"records"), hashlib.sha256).digest()
        self._unit_key = hmac.new(secret, _frame(_VERSION.encode(), b"units"), hashlib.sha256).digest()
        self._index_key = hmac.new(secret, _frame(_VERSION.encode(), b"request-index"), hashlib.sha256).digest()
        if os.name == "nt":
            directory = neighbourhood_windows.normalize_path(directory)
            pin_path = neighbourhood_windows.normalize_path(pin_path)
        self.directory = directory
        self.pin_path = pin_path
        self.database = os.path.join(directory, "anchors.sqlite3")
        self.init_lock = pin_path + ".lock"
        self.expected_uuid = expected_uuid
        self._forbidden = list(forbidden_dirs) + release_cache._hook_mounts()
        try:
            if expected_uuid and _UUID.fullmatch(expected_uuid) is None:
                raise StateError()
            if (not os.path.isabs(pin_path) or os.path.normpath(pin_path) != pin_path
                    or not os.path.isabs(directory) or os.path.normpath(directory) != directory
                    or _overlaps(directory, pin_path)):
                raise StateError()
            # Validate the pin parent without creating it. The root secret has
            # already been validated by from_env; direct callers get the same
            # no-symlink/owner-only persistence requirement here.
            _safe_parent(os.path.dirname(pin_path))
            for forbidden in self._forbidden:
                if forbidden and _overlaps(pin_path, os.path.realpath(forbidden)):
                    raise StateError()
            lock_preexisted = os.path.lexists(self.init_lock)
            established = (lock_preexisted or os.path.lexists(pin_path)
                           or os.path.lexists(directory) or bool(expected_uuid))
            with _file_lock(self.init_lock, create=not established):
                pin_exists, directory_exists = os.path.lexists(pin_path), os.path.lexists(directory)
                if not pin_exists and not directory_exists and not expected_uuid and not lock_preexisted:
                    self._initialize()
                elif not pin_exists or not directory_exists:
                    raise StateError()
                self.store_uuid = self._read_pin()
                if expected_uuid and expected_uuid != self.store_uuid:
                    raise StateError()
                self._paths()
                with self._transaction() as connection:
                    self._verify(connection)
        except (OSError, sqlite3.Error, RuntimeError, ValueError, TypeError, KeyError) as exc:
            if isinstance(exc, (StateError, CapacityError)):
                raise
            raise StateError() from exc

    @classmethod
    def from_env(cls, *, forbidden_dirs=()):
        secret = seeding._node_secret()
        secret_path = os.environ.get("DSFLOWER_NODE_SECRET_FILE", "")
        if not os.path.isabs(secret_path):
            raise StateError()
        def setting(name, default):
            raw = os.environ.get("DSFLOWER_NEIGHBOURHOOD_" + name, str(default))
            if re.fullmatch(r"[0-9]{1,16}", raw) is None:
                raise ValueError("invalid neighbourhood administrator policy")
            return int(raw)
        return cls(os.environ.get("DSFLOWER_NEIGHBOURHOOD_DIR", secret_path + ".neighbourhood"),
                   secret=secret, pin_path=secret_path + ".neighbourhood-id",
                   k=setting("K", 3), max_anchors=setting("MAX_ANCHORS", 256),
                   capacity_bytes=setting("STORE_BYTES", 64 * 1024**3),
                   expected_uuid=os.environ.get("DSFLOWER_NEIGHBOURHOOD_STORE_ID", ""),
                   forbidden_dirs=forbidden_dirs)

    def _read_pin(self):
        fd = _safe_file(self.pin_path)
        try:
            raw = os.read(fd, 37)
            os.fsync(fd)
        finally:
            os.close(fd)
        result = raw.decode("ascii")
        if _UUID.fullmatch(result) is None:
            raise StateError()
        _sync_directory(os.path.dirname(self.pin_path))
        return result

    def _initialize(self):
        self.store_uuid = str(uuid.uuid4())
        _safe_directory(self.directory, self._forbidden)
        for shard in range(_LOCK_SHARDS + 1):
            os.close(_safe_file(self._lock_path(shard), create=True))
        os.close(_safe_file(self.database, create=True))
        with self._connection() as connection:
            connection.execute("CREATE TABLE header (id INTEGER PRIMARY KEY CHECK(id=1), body BLOB NOT NULL, mac BLOB NOT NULL)")
            connection.execute("CREATE TABLE requests (request_key TEXT PRIMARY KEY, body BLOB NOT NULL, mac BLOB NOT NULL) WITHOUT ROWID")
            connection.execute("CREATE TABLE anchors (request_key TEXT NOT NULL REFERENCES requests(request_key), sequence INTEGER NOT NULL, binding BLOB NOT NULL, units BLOB NOT NULL, payload BLOB NOT NULL, mac BLOB NOT NULL, PRIMARY KEY(request_key, sequence)) WITHOUT ROWID")
            self._write_header(connection, _BASE_BYTES)
        _sync_directory(self.directory)
        if os.name == "nt":
            neighbourhood_windows.publish_pin(self.pin_path, self.store_uuid.encode("ascii"))
            return
        fd = os.open(self.pin_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.write(fd, self.store_uuid.encode("ascii"))
            os.fsync(fd)
        finally:
            os.close(fd)
        _sync_directory(os.path.dirname(self.pin_path))

    def _lock_path(self, shard):
        return os.path.join(self.directory, "lock-%02x" % shard)

    def _paths(self):
        _check_directory(self.directory)
        os.close(_safe_file(self.init_lock))
        _safe_directory(self.directory, self._forbidden)
        if self._read_pin() != self.store_uuid:
            raise StateError()
        allowed = {"anchors.sqlite3", "anchors.sqlite3-journal"}
        allowed.update("lock-%02x" % shard for shard in range(_LOCK_SHARDS + 1))
        for name in os.listdir(self.directory):
            if name not in allowed:
                raise StateError()
            os.close(_safe_file(os.path.join(self.directory, name)))
        for shard in range(_LOCK_SHARDS + 1):
            os.close(_safe_file(self._lock_path(shard)))
        os.close(_safe_file(self.database))

    @contextmanager
    def _connection(self):
        # mode=rw prevents SQLite from silently recreating a deleted database.
        os.close(_safe_file(self.database))
        journal = self.database + "-journal"
        if os.path.lexists(journal):
            os.close(_safe_file(journal))
        from urllib.parse import quote
        uri = (neighbourhood_windows.sqlite_uri(self.database) if os.name == "nt"
               else "file:" + quote(self.database) + "?mode=rw")
        connection = sqlite3.connect(uri, uri=True,
                                     timeout=60.0, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=EXTRA")
            connection.execute("PRAGMA fullfsync=ON")
            connection.execute("PRAGMA secure_delete=ON")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _transaction(self):
        try:
            with _file_lock(self._lock_path(_LOCK_SHARDS)):
                self._paths()
                with self._connection() as connection:
                    yield connection
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, struct.error, RuntimeError) as exc:
            if isinstance(exc, (StateError, CapacityError)):
                raise
            raise StateError() from exc

    def _allocated_bytes(self, connection):
        # Persistent SQLite pages plus conservative pin/lock/schema headroom.
        # DELETE journals are transient durability overhead, documented as
        # additional filesystem headroom rather than retained release state.
        return (_BASE_BYTES + connection.execute("PRAGMA page_count").fetchone()[0]
                * connection.execute("PRAGMA page_size").fetchone()[0])

    def _mac(self, kind, *parts):
        return hmac.new(self._mac_key, _frame(_VERSION.encode(), self.store_uuid.encode(),
                        kind.encode(), *parts), hashlib.sha256).digest()

    def _manifest(self, connection):
        digest = hashlib.sha256()
        for key, body, mac in connection.execute("SELECT request_key, body, mac FROM requests ORDER BY request_key"):
            if not hmac.compare_digest(mac, self._mac("request", key.encode(), body)):
                raise StateError()
            digest.update(_frame(key.encode(), body, mac))
        return digest.hexdigest()

    def _write_header(self, connection, used):
        body = _json({"version": _VERSION, "uuid": self.store_uuid, "used": used,
                      "manifest": self._manifest(connection)})
        connection.execute("INSERT OR REPLACE INTO header VALUES (1, ?, ?)",
                           (body, self._mac("header", body)))

    def _verify(self, connection):
        rows = connection.execute("SELECT id, body, mac FROM header").fetchall()
        if len(rows) != 1 or rows[0][0] != 1:
            raise StateError()
        _, body, mac = rows[0]
        if not hmac.compare_digest(mac, self._mac("header", body)):
            raise StateError()
        header = json.loads(body)
        if (set(header) != {"version", "uuid", "used", "manifest"}
                or header["version"] != _VERSION or header["uuid"] != self.store_uuid
                or type(header["used"]) is not int or header["used"] < _BASE_BYTES
                or header["manifest"] != self._manifest(connection)):
            raise StateError()
        return header

    def _request(self, connection, key):
        row = connection.execute("SELECT body, mac FROM requests WHERE request_key=?", (key,)).fetchone()
        if row is None:
            return None
        body, mac = row
        if not hmac.compare_digest(mac, self._mac("request", key.encode(), body)):
            raise StateError()
        result = json.loads(body)
        if (set(result) != {"k", "count", "head"} or type(result["k"]) is not int
                or result["k"] < 2 or type(result["count"]) is not int or result["count"] < 1
                or not isinstance(result["head"], str) or _HEX.fullmatch(result["head"]) is None):
            raise StateError()
        return result

    def _anchors(self, connection, key, policy, fingerprints):
        # Every anchor is authenticated and compared even after finding an
        # eligible one. The manifest detects missing/reordered whole records.
        head = hashlib.sha256()
        count = 0
        selected = None
        for seq, binding, units, payload, mac in connection.execute(
                "SELECT sequence, binding, units, payload, mac FROM anchors WHERE request_key=? ORDER BY sequence", (key,)):
            count += 1
            if seq != count or not hmac.compare_digest(mac, self._mac("anchor", key.encode(),
                    struct.pack(">Q", seq), binding, units, payload)):
                raise StateError()
            if len(binding) != 32:
                raise StateError()
            head.update(_frame(mac))
            stored = json.loads(units)
            if (not isinstance(stored, dict) or any(not isinstance(t, str) or _HEX.fullmatch(t) is None
                    or type(n) is not int or n < 1 for t, n in stored.items())):
                raise StateError()
            eligible = distance(fingerprints, stored) < policy["k"]
            if eligible and selected is None:
                selected = payload
        if policy is None:
            if count:
                raise StateError()
        elif count != policy["count"] or head.hexdigest() != policy["head"]:
            raise StateError()
        return selected, head

    def verify(self):
        """Authenticate retained state before an existing transport-only retry.

        Such a retry has already pinned its source and does not reselect from a
        newly supplied input, but it must still fail closed after state loss.
        Payloads are authenticated as raw bytes without deserializing them.
        """
        with self._transaction() as connection:
            self._verify(connection)
            for (key,) in connection.execute("SELECT request_key FROM requests ORDER BY request_key").fetchall():
                self._anchors(connection, key, self._request(connection, key), {})
            if connection.execute("SELECT request_key FROM anchors EXCEPT SELECT request_key FROM requests").fetchone():
                raise StateError()

    @contextmanager
    def release(self, request, units):
        digest = getattr(request, "digest", request)
        if isinstance(digest, str) and _HEX.fullmatch(digest):
            digest = bytes.fromhex(digest)
        if not isinstance(digest, bytes) or len(digest) != 32:
            raise ValueError("neighbourhood requires a v3 request digest")
        fingerprints = unit_fingerprints(units, self._unit_key)
        key = hmac.new(self._index_key, _frame(b"request", digest), hashlib.sha256).hexdigest()
        slot = None
        try:
            with _file_lock(self._lock_path(int(key[:8], 16) % _LOCK_SHARDS)):
                with self._transaction() as connection:
                    header = self._verify(connection)
                    policy = self._request(connection, key)
                    selected, _ = self._anchors(connection, key, policy, fingerprints)
                    if selected is None and ((policy is not None and policy["count"] >= self.max_anchors)
                                             or header["used"] >= self.capacity_bytes):
                        raise CapacityError()
                    slot = _Slot(self, key, digest, fingerprints,
                                 None if selected is None else decode_payload(selected))
                # No global SQLite/file lock is retained during fresh training.
                yield slot
        except (OSError, sqlite3.Error) as exc:
            raise StateError() from exc
        finally:
            if slot is not None:
                slot.active = False


class _Slot:
    def __init__(self, store, key, request_digest, fingerprints, cached):
        self.store, self.key, self.request_digest = store, key, request_digest
        self.fingerprints, self.cached = fingerprints, cached
        self.active = True
        self.committed = cached is not None

    def commit(self, binding, payload):
        if not self.active or self.committed:
            raise RuntimeError("neighbourhood release cannot be committed twice or outside its lock")
        digest = getattr(binding, "digest", binding)
        if getattr(binding, "request_digest", self.request_digest) != self.request_digest:
            raise ValueError("neighbourhood binding belongs to another request")
        if isinstance(digest, str) and _HEX.fullmatch(digest):
            digest = bytes.fromhex(digest)
        if not isinstance(digest, bytes) or len(digest) != 32:
            raise ValueError("neighbourhood requires a complete v3 private binding")
        if payload is None:
            raise ValueError("complete neighbourhood release payload cannot be null")
        encoded, units = encode_payload(payload), _json(self.fingerprints)
        charge = _RECORD_BYTES + len(encoded) + len(units) + len(digest)
        store = self.store
        try:
            with store._transaction() as connection:
                header = store._verify(connection)
                policy = store._request(connection, self.key)
                selected, head = store._anchors(connection, self.key, policy, self.fingerprints)
                if selected is not None:
                    raise StateError()
                count = 0 if policy is None else policy["count"]
                if count >= store.max_anchors or header["used"] + charge > store.capacity_bytes:
                    raise CapacityError()
                sequence = count + 1
                mac = store._mac("anchor", self.key.encode(), struct.pack(">Q", sequence), digest, units, encoded)
                head.update(_frame(mac))
                body = _json({"k": store.k if policy is None else policy["k"],
                              "count": sequence, "head": head.hexdigest()})
                connection.execute("INSERT INTO requests VALUES (?, ?, ?) ON CONFLICT(request_key) DO UPDATE SET body=excluded.body, mac=excluded.mac",
                                   (self.key, body, store._mac("request", self.key.encode(), body)))
                connection.execute("INSERT INTO anchors VALUES (?, ?, ?, ?, ?, ?)",
                                   (self.key, sequence, digest, units, encoded, mac))
                store._write_header(connection, header["used"] + charge)
                if store._allocated_bytes(connection) > store.capacity_bytes:
                    raise CapacityError()
            self.cached = decode_payload(encoded)
            self.committed = True
        except (OSError, sqlite3.Error) as exc:
            raise StateError() from exc


@contextmanager
def release(request, units, *, forbidden_dirs=()):
    """Trusted runner entry point, after admission and source canonicalization."""
    with NeighbourhoodStore.from_env(forbidden_dirs=forbidden_dirs).release(request, units) as slot:
        yield slot
