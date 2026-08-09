"""Stateless, semantic deterministic randomness for private releases.

The only persistent state is a custodial 256-bit node key.  HMAC-SHA256 binds
each master key to the effective mechanism, public configuration, privacy
policy, server round, incoming public arrays, and effective private inputs.
Operational identities (run tokens, message ids, paths and timestamps) never
enter this contract.  Recomputing the same semantic release therefore produces
the same stream without a database, counter, cache, or historical budget.

Numeric DP noise uses a ChaCha20 stream rather than NumPy's statistical PCG.
Its unpredictability is computational and depends on keeping the node key
secret.  Sub-keys domain-separate initialization, sampling, training, and noise.
"""

import hashlib
import hmac
import importlib.metadata
import json
import math
import os
import platform
import re
import stat
import sys
from functools import lru_cache


_SECRET_ENV = "DSFLOWER_NODE_SECRET_FILE"
_MASK63 = 0x7FFF_FFFF_FFFF_FFFF
_MASK31 = 0x7FFF_FFFF
_MAX_CONFIG_BYTES = 1 << 20
_MAX_CONFIG_DEPTH = 16
_MAX_CONFIG_ITEMS = 8192
_MAX_ARRAYS = 512
_MAX_ARRAY_NDIM = 16
_MAX_UNIT_ID_BYTES = 4096
_HASH_CHUNK_BYTES = 1 << 20
_POLICY_HASH = re.compile(r"[0-9a-f]{64}\Z")
_WINDOWS_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)


def _is_windows_reparse_point(info):
    """Return whether a Windows stat result names a reparse point."""
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    reparse_tag = int(getattr(info, "st_reparse_tag", 0) or 0)
    return bool(attributes & _WINDOWS_REPARSE_POINT or reparse_tag)


def _same_file_identity(before, after, *, windows):
    """Compare stable file identities and fail closed if Windows omits them."""
    try:
        before_identity = (int(before.st_dev), int(before.st_ino))
        after_identity = (int(after.st_dev), int(after.st_ino))
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "dsFlower cannot establish a stable node-secret file identity"
        ) from exc
    if windows and (before_identity == (0, 0) or after_identity == (0, 0)):
        raise RuntimeError(
            "dsFlower cannot establish a stable node-secret file identity"
        )
    return before_identity == after_identity


def _node_secret():
    """Read and validate the dedicated node key; never fall back silently."""
    path = os.environ.get(_SECRET_ENV, "")
    if not path or not os.path.isabs(path):
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe")

    windows = os.name == "nt"
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if not windows and nofollow is None:
        raise RuntimeError("this platform cannot safely open the dsFlower node secret")
    try:
        parent = os.path.dirname(path)
        parent_info = os.lstat(parent)
        path_info = os.lstat(path)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    if not stat.S_ISDIR(parent_info.st_mode):
        raise RuntimeError("dsFlower node-secret parent must be a real directory")
    if not stat.S_ISREG(path_info.st_mode):
        raise RuntimeError("dsFlower node secret must be a regular file")
    if windows:
        if (_is_windows_reparse_point(parent_info)
                or _is_windows_reparse_point(path_info)):
            raise RuntimeError(
                "dsFlower node-secret path must not contain a reparse point"
            )
    else:
        euid = os.geteuid()
        if parent_info.st_uid not in (euid, 0):
            raise RuntimeError(
                "dsFlower node-secret parent must be owned by the node EUID or root"
            )
        if stat.S_IMODE(parent_info.st_mode) & 0o022:
            raise RuntimeError(
                "dsFlower node-secret parent must not be writable by group or other users"
            )

    if windows:
        flags = (os.O_RDONLY | getattr(os, "O_BINARY", 0)
                 | getattr(os, "O_NOINHERIT", 0))
    else:
        flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    try:
        info = os.fstat(fd)
        parent_after = os.lstat(parent)
        if (not stat.S_ISDIR(parent_after.st_mode)
                or not _same_file_identity(
                    parent_info, parent_after, windows=windows)):
            raise RuntimeError("dsFlower node-secret parent changed while opening")
        if (not stat.S_ISREG(info.st_mode)
                or not _same_file_identity(path_info, info, windows=windows)):
            raise RuntimeError("dsFlower node-secret path changed while opening")
        if windows:
            if (_is_windows_reparse_point(parent_after)
                    or _is_windows_reparse_point(info)):
                raise RuntimeError(
                    "dsFlower node-secret path must not contain a reparse point"
                )
        else:
            if parent_after.st_uid not in (euid, 0):
                raise RuntimeError(
                    "dsFlower node-secret parent must be owned by the node EUID or root"
                )
            if stat.S_IMODE(parent_after.st_mode) & 0o022:
                raise RuntimeError(
                    "dsFlower node-secret parent must not be writable by group or other users"
                )
            if info.st_uid != euid:
                raise RuntimeError("dsFlower node secret must be owned by the node EUID")
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise RuntimeError("dsFlower node secret must have mode 0600")
        with os.fdopen(fd, "rb", closefd=False) as fh:
            # 64 hex bytes plus an optional LF/CRLF. Reading one byte beyond
            # that maximum makes oversized or multiply-terminated files fail
            # without permissive whitespace stripping.
            encoded = fh.read(67)
    finally:
        os.close(fd)

    if len(encoded) == 64:
        hex_bytes = encoded
    elif len(encoded) == 65 and encoded.endswith(b"\n"):
        hex_bytes = encoded[:-1]
    elif len(encoded) == 66 and encoded.endswith(b"\r\n"):
        hex_bytes = encoded[:-2]
    else:
        raise RuntimeError(
            "dsFlower node secret must contain exactly 64 hex characters"
        )
    if any(byte not in b"0123456789abcdefABCDEF" for byte in hex_bytes):
        raise RuntimeError("dsFlower node secret is not valid hex")
    return bytes.fromhex(hex_bytes.decode("ascii"))


def select_config(config, keys):
    """Copy only caller-declared effective public fields.

    Positive selection is deliberate: transport metadata such as run tokens,
    message ids, paths and staging timestamps cannot accidentally become a
    reroll axis when new fields are added to a Flower ``ConfigRecord``.
    """
    if not isinstance(config, dict):
        raise RuntimeError("semantic configuration must be an object")
    return {key: config[key] for key in sorted(set(keys)) if key in config}


@lru_cache(maxsize=1)
def _runtime_fingerprint():
    """Public execution facts that can change deterministic model arithmetic."""
    packages = {}
    for name in ("cryptography", "numpy", "opacus", "torch", "torchvision"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "absent"

    backend = {"kind": "cpu"}
    try:
        import torch
        if torch.cuda.is_available():
            backend = {
                "kind": "cuda",
                "cuda": str(getattr(torch.version, "cuda", None)),
                "cudnn": int(torch.backends.cudnn.version() or 0),
                "device": str(torch.cuda.get_device_name(0)),
                "capability": [int(value) for value in
                               torch.cuda.get_device_capability(0)],
            }
    except (ImportError, RuntimeError):
        backend = {"kind": "unavailable"}

    runner = hashlib.sha256()
    runner_dir = os.path.dirname(os.path.abspath(__file__))
    for name in sorted(value for value in os.listdir(runner_dir)
                       if value.endswith(".py")):
        encoded_name = name.encode("utf-8", errors="strict")
        with open(os.path.join(runner_dir, name), "rb") as handle:
            content = handle.read()
        runner.update(len(encoded_name).to_bytes(4, "big"))
        runner.update(encoded_name)
        runner.update(len(content).to_bytes(8, "big"))
        runner.update(content)

    return {
        "python": "%d.%d.%d" % sys.version_info[:3],
        "implementation": platform.python_implementation(),
        "system": platform.system().lower(),
        "machine": platform.machine().lower(),
        "packages": packages,
        "backend": backend,
        "runner_sha256": runner.hexdigest(),
    }


def _normalize_json(value, depth, state):
    if depth > _MAX_CONFIG_DEPTH:
        raise RuntimeError("semantic configuration is nested too deeply")
    state[0] += 1
    if state[0] > _MAX_CONFIG_ITEMS:
        raise RuntimeError("semantic configuration has too many items")
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise RuntimeError("semantic configuration must be finite")
        return 0.0 if value == 0.0 else value
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        return [_normalize_json(item, depth + 1, state) for item in value]
    if isinstance(value, dict):
        out = {}
        for key in sorted(value):
            if not isinstance(key, str):
                raise RuntimeError("semantic configuration keys must be strings")
            key.encode("utf-8", errors="strict")
            out[key] = _normalize_json(value[key], depth + 1, state)
        return out
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return _normalize_json(value.item(), depth, state)
    except ImportError:
        pass
    raise RuntimeError("semantic configuration contains an unsupported value")


def _canonical_json(value):
    normalized = _normalize_json(value, 0, [0])
    encoded = json.dumps(
        normalized, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_CONFIG_BYTES:
        raise RuntimeError("semantic configuration is too large")
    return encoded


def _frame_header(hasher, label, payload_size):
    label = str(label).encode("utf-8", errors="strict")
    hasher.update(len(label).to_bytes(4, "big"))
    hasher.update(label)
    hasher.update(int(payload_size).to_bytes(8, "big"))


def _frame(hasher, label, payload):
    payload = bytes(payload)
    _frame_header(hasher, label, len(payload))
    hasher.update(payload)


def _array_metadata(array):
    return _canonical_json({
        "dtype": array.dtype.str,
        "shape": [int(dim) for dim in array.shape],
    })


def _update_array(hasher, label, value):
    import numpy as np

    array = np.asarray(value)
    if (array.dtype.hasobject or array.dtype.kind not in "biuf"
            or array.ndim > _MAX_ARRAY_NDIM):
        raise RuntimeError("semantic arrays must be bounded real numeric tensors")
    if array.dtype.kind == "f" and not bool(np.all(np.isfinite(array))):
        raise RuntimeError("semantic arrays must be finite")
    canonical_dtype = array.dtype.newbyteorder("<")
    canonical = np.ascontiguousarray(array.astype(canonical_dtype, copy=False))
    _frame(hasher, label + "/meta", _array_metadata(canonical))
    raw = memoryview(canonical).cast("B")
    _frame_header(hasher, label + "/bytes", len(raw))
    if canonical.dtype.kind != "f":
        for start in range(0, len(raw), _HASH_CHUNK_BYTES):
            hasher.update(raw[start:start + _HASH_CHUNK_BYTES])
        return

    # Normalize -0 to +0 one bounded chunk at a time.  This avoids a second
    # dataset-sized allocation while preserving numerical semantic equality.
    flat = canonical.reshape(-1)
    step = max(1, _HASH_CHUNK_BYTES // int(canonical.dtype.itemsize))
    for start in range(0, flat.size, step):
        chunk = flat[start:start + step]
        if bool(np.any(chunk == 0)):
            chunk = chunk.copy()
            chunk[chunk == 0] = 0.0
        hasher.update(memoryview(np.ascontiguousarray(chunk)).cast("B"))


def _update_arrays(hasher, namespace, arrays):
    arrays = list(arrays or ())
    if len(arrays) > _MAX_ARRAYS:
        raise RuntimeError("semantic release has too many arrays")
    _frame(hasher, namespace + "/count", len(arrays).to_bytes(4, "big"))
    for index, value in enumerate(arrays):
        _update_array(hasher, "%s/%d" % (namespace, index), value)


def _update_unit_ids(hasher, unit_ids):
    if unit_ids is None:
        _frame(hasher, "unit-ids/present", b"0")
        return
    try:
        count = len(unit_ids)
        values = iter(unit_ids)
    except TypeError as exc:
        raise RuntimeError(
            "semantic privacy-unit identifiers must be one-dimensional") from exc
    _frame(hasher, "unit-ids/present", b"1")
    _frame(hasher, "unit-ids/count", int(count).to_bytes(8, "big"))
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise RuntimeError("semantic privacy-unit identifiers must be strings")
        encoded = value.encode("utf-8", errors="strict")
        if len(encoded) > _MAX_UNIT_ID_BYTES:
            encoded = b"__dsflower_missing_patient_unit__"
        _frame(hasher, "unit-ids/%d" % index, encoded)


def _semantic_digest(mechanism, config, privacy, round_index,
                     public_arrays=(), private_arrays=(), unit_ids=None,
                     execution_fingerprint=None):
    if (not isinstance(mechanism, str) or not mechanism
            or len(mechanism.encode("utf-8", errors="strict")) > 128):
        raise RuntimeError("semantic mechanism identifier is invalid")
    if type(round_index) is not int or round_index < 1:
        raise RuntimeError("semantic server round must be a positive integer")
    if not isinstance(privacy, dict):
        raise RuntimeError("semantic privacy policy must be an object")
    policy_hash = privacy.get("policy_hash")
    if not isinstance(policy_hash, str) or _POLICY_HASH.fullmatch(policy_hash) is None:
        raise RuntimeError("semantic privacy policy hash is missing or invalid")

    digest = hashlib.sha256()
    _frame(digest, "contract", b"dsflower-semantic-randomness-v1")
    _frame(digest, "mechanism", mechanism.encode("utf-8"))
    runtime = (_runtime_fingerprint() if execution_fingerprint is None
               else execution_fingerprint)
    _frame(digest, "runtime", _canonical_json(runtime))
    _frame(digest, "server-round", round_index.to_bytes(8, "big"))
    _frame(digest, "public-config", _canonical_json(config))
    _frame(digest, "privacy-policy", _canonical_json(privacy))
    _update_arrays(digest, "public-arrays", public_arrays)
    _update_arrays(digest, "private-arrays", private_arrays)
    _update_unit_ids(digest, unit_ids)
    return digest.digest()


def master_seed(mechanism, config, privacy, round_index, *,
                public_arrays=(), private_arrays=(), unit_ids=None,
                execution_fingerprint=None):
    """Derive one stateless 256-bit key from the effective release semantics."""
    digest = _semantic_digest(
        mechanism, config, privacy, round_index,
        public_arrays=public_arrays, private_arrays=private_arrays,
        unit_ids=unit_ids, execution_fingerprint=execution_fingerprint)
    return hmac.new(
        _node_secret(), b"dsflower/semantic-prf/v1\x00" + digest,
        hashlib.sha256).digest()


def bind_seed(master, label, arrays):
    """Bind a sub-key to validated pre-noise tensors.

    Hook children are untrusted and may be nondeterministic.  Binding the final
    noise key to their validated update prevents the same noise vector from
    being reused over two different statistics while retaining sticky retries
    when the update is identical.
    """
    if not isinstance(master, (bytes, bytearray)) or len(master) != 32:
        raise RuntimeError("invalid dsFlower master release key")
    digest = hashlib.sha256()
    _frame(digest, "contract", b"dsflower-bound-subkey-v1")
    _frame(digest, "label", str(label).encode("utf-8", errors="strict"))
    _update_arrays(digest, "bound-arrays", arrays)
    return hmac.new(
        bytes(master), b"dsflower/bound-subkey/v1\x00" + digest.digest(),
        hashlib.sha256).digest()


def sub_seed(master, label):
    """Derive an independent 256-bit sub-key for one randomness axis."""
    if not isinstance(master, (bytes, bytearray)) or len(master) != 32:
        raise RuntimeError("invalid dsFlower master release key")
    return hmac.new(bytes(master),
                    b"dsflower/subkey/v2\x00" + str(label).encode("utf-8"),
                    hashlib.sha256).digest()


def _seed_int(seed):
    if not isinstance(seed, (bytes, bytearray)) or len(seed) < 8:
        raise RuntimeError("invalid deterministic seed")
    return int.from_bytes(bytes(seed)[:8], "big")


class SecureNumpyRng:
    """Small NumPy-compatible facade backed by a keyed ChaCha20 stream.

    ``normal`` uses Box-Muller and sums four independent samples divided by two.
    This preserves N(0, sigma^2) while applying the same 2n floating-point
    hardening principle used by Opacus secure mode.
    """

    def __init__(self, key):
        if not isinstance(key, (bytes, bytearray)) or len(key) != 32:
            raise RuntimeError("ChaCha20 RNG needs a 256-bit key")
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms
        except Exception as exc:
            raise RuntimeError(
                "cryptography is required for dsFlower's secure DP RNG"
            ) from exc
        nonce = hmac.new(bytes(key), b"dsflower/chacha20/nonce/v2",
                         hashlib.sha256).digest()[:16]
        self._stream = Cipher(algorithms.ChaCha20(bytes(key), nonce), mode=None).encryptor()

    def _bytes(self, n):
        return self._stream.update(b"\x00" * int(n))

    def _uniform(self, n):
        import numpy as np

        raw = np.frombuffer(self._bytes(8 * int(n)), dtype="<u8")
        # Exactly 53 random mantissa bits, strictly inside (0, 1).
        return ((raw >> np.uint64(11)).astype(np.float64) + 0.5) / float(1 << 53)

    def normal(self, loc=0.0, scale=1.0, size=None):
        import numpy as np

        shape = () if size is None else ((size,) if isinstance(size, int) else tuple(size))
        count = int(np.prod(shape, dtype=np.int64)) if shape else 1
        hardened_count = 4 * count
        pairs = (hardened_count + 1) // 2
        u1 = self._uniform(pairs)
        u2 = self._uniform(pairs)
        radius = np.sqrt(-2.0 * np.log(u1))
        angle = 2.0 * np.pi * u2
        samples = np.empty(2 * pairs, dtype=np.float64)
        samples[0::2] = radius * np.cos(angle)
        samples[1::2] = radius * np.sin(angle)
        samples = samples[:hardened_count].reshape(4, count).sum(axis=0) / 2.0
        samples = float(loc) + float(scale) * samples
        return samples.reshape(shape) if shape else float(samples[0])

    def _randbelow(self, upper):
        upper = int(upper)
        if upper <= 0:
            raise ValueError("upper must be positive")
        limit = (1 << 64) - ((1 << 64) % upper)
        while True:
            value = int.from_bytes(self._bytes(8), "little")
            if value < limit:
                return value % upper

    def bernoulli_mask_one_in(self, denominator, size):
        """Return ``size`` independent Bernoulli(1/denominator) draws.

        Rejection before the modulo removes modulo bias, so the inclusion
        probability is exactly the reciprocal requested by the DP accountant
        (within the ChaCha20 pseudorandom-stream model), rather than a rounded
        floating-point threshold.
        """
        import operator
        import numpy as np

        try:
            denominator = operator.index(denominator)
            size = operator.index(size)
        except TypeError as exc:
            raise ValueError("denominator and size must be integers") from exc
        if denominator <= 0 or denominator >= (1 << 64):
            raise ValueError("denominator must be in [1, 2^64)")
        if size < 0:
            raise ValueError("size must be non-negative")

        raw = np.frombuffer(self._bytes(8 * size), dtype="<u8")
        out = np.empty(size, dtype=np.bool_)
        remainder = (1 << 64) % denominator
        if remainder == 0:
            accepted = np.ones(size, dtype=np.bool_)
        else:
            cutoff = np.uint64((1 << 64) - remainder)
            accepted = raw < cutoff
        divisor = np.uint64(denominator)
        out[accepted] = (raw[accepted] % divisor) == 0
        # At most denominator-1 words out of 2^64 reach this path.  Retry them
        # individually with the same unbiased primitive instead of allocating
        # another full-size buffer.
        for index in np.flatnonzero(~accepted):
            out[index] = self._randbelow(denominator) == 0
        return out

    def permutation(self, n):
        import numpy as np

        out = np.arange(int(n))
        for i in range(len(out) - 1, 0, -1):
            j = self._randbelow(i + 1)
            out[i], out[j] = out[j], out[i]
        return out


def np_rng(seed):
    return SecureNumpyRng(seed)


def seed_torch(seed):
    """Seed common ML RNGs and request portable deterministic Torch kernels."""
    import random
    import numpy as np
    import torch

    value = _seed_int(seed)
    os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    torch.manual_seed(value & _MASK63)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(value & _MASK63)
    np.random.seed(value & _MASK31)
    random.seed(value & _MASK31)
    torch.use_deterministic_algorithms(True)
    cudnn = getattr(torch.backends, "cudnn", None)
    if cudnn is not None:
        cudnn.benchmark = False
        cudnn.deterministic = True
