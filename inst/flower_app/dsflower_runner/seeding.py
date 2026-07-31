"""Domain-separated deterministic randomness for private releases.

The server creates a dedicated 256-bit key at runtime.  Every *new* release gets
an independent identity from the persistent accountant; HMAC-SHA256 derives a
separate key for each randomness axis.  Numeric DP noise uses a ChaCha20 stream,
not NumPy's statistical PCG generator.  Its unpredictability is computational
and depends on keeping the node key secret and never reusing a release identity.
Exact Flower message retries are handled by the release guard and never consume
a second stream.
"""

import hashlib
import hmac
import os
import stat


_SECRET_ENV = "DSFLOWER_NODE_SECRET_FILE"
_MASK63 = 0x7FFF_FFFF_FFFF_FFFF
_MASK31 = 0x7FFF_FFFF


def _node_secret():
    """Read and validate the dedicated node key; never fall back silently."""
    path = os.environ.get(_SECRET_ENV, "")
    if not path or not os.path.isabs(path):
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe")

    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RuntimeError("this platform cannot safely open the dsFlower node secret")
    try:
        path_info = os.lstat(path)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    if not stat.S_ISREG(path_info.st_mode):
        raise RuntimeError("dsFlower node secret must be a regular file")

    flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("trusted dsFlower node-secret path is missing or unsafe") from exc
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode)
                or (info.st_dev, info.st_ino) != (path_info.st_dev, path_info.st_ino)):
            raise RuntimeError("dsFlower node-secret path changed while opening")
        if info.st_uid != os.geteuid():
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


def master_seed(cfg, X, y=None, release_id=None):
    """Derive a 256-bit key for exactly one accountant-authorized release.

    ``cfg``, ``X`` and ``y`` remain in the signature for runner ABI
    compatibility, but are intentionally not read: mechanism randomness must
    be structurally independent of every data/configuration field. The
    server-owned release identity alone prevents stream reuse.
    """
    if not release_id:
        raise RuntimeError("private randomness requires a release identity")
    message = b"dsflower/dp/v2\x00" + str(release_id).encode("utf-8")
    return hmac.new(_node_secret(), message, hashlib.sha256).digest()


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
    """Seed data-independent initialization/dropout from a release sub-key."""
    import random
    import numpy as np
    import torch

    value = _seed_int(seed)
    torch.manual_seed(value & _MASK63)
    np.random.seed(value & _MASK31)
    random.seed(value & _MASK31)
