"""dsFlower DSI-tunnel forwarder (node side).

Bridges a local TCP connection (the Flower SuperNode dials this) to a byte spool
that the DataSHIELD tunnel methods drain/fill, so the researcher's R relay can
carry the bytes to/from the SuperLink over DSI. No Tor, no public address: the
SuperNode connects to 127.0.0.1 on its own node.

    SuperNode --TCP--> 127.0.0.1:<listen> --(this)--> up.bin / down.bin (spool)
                                                       ^ drained/filled by DSI

Robust lifecycle:
- Multi-connection: the SuperNode may drop and reconnect (e.g. a gRPC handshake
  that timed out under tunnel latency). Each accepted connection starts a fresh
  stream -- the spool is truncated and a monotonically increasing `gen` is
  published so the relay resets its socket/offsets for the new stream.
- Self-terminating: the relay updates a `relay_hb` heartbeat on every exchange.
  If it goes stale (the researcher's relay died, the client was killed, or the
  tunnel connection was lost), the forwarder exits, which makes its SuperNode
  lose the SuperLink and self-terminate (--max-wait-time). No orphans.

Spool files (per generation; compactable + absolute relay-owned offsets):
  up.bin   : SuperNode -> SuperLink bytes  (this appends; the relay reads)
  down.bin : SuperLink -> SuperNode bytes  (the relay appends; this reads)
  Each .bin starts with an atomic 8-byte big-endian absolute-base header.
  up.ack   : relay's durable absolute acknowledgement for up.bin compaction
  down.ack : relay's durable absolute acknowledgement for down.bin compaction
  gen      : current connection generation (this writes; the relay reads)
  relay_hb : relay heartbeat (the relay touches; this watches for staleness)
"""
import argparse
from contextlib import contextmanager
import math
import os
import socket
import struct
import time

if os.name == "nt":
    import msvcrt
else:
    import fcntl

# Tolerance window for a transient relay/connection loss: the forwarder keeps the
# SuperNode connected and the run recovers (the loss-free relay re-requests from
# its offsets) for up to this long. Only a SUSTAINED loss tears down. Matched to
# the SuperNode --max-wait-time so both tolerate the same window.
RELAY_TTL = float(os.environ.get("DSFLOWER_RELAY_TTL", "180"))  # seconds
SPOOL_MAX_BYTES = int(
    os.environ.get("DSFLOWER_TUNNEL_SPOOL_MAX_BYTES", str(1024**3))
)
IO_CHUNK_BYTES = 65536
SPOOL_HEADER_BYTES = 8
COMPACT_THRESHOLD_BYTES = max(
    IO_CHUNK_BYTES, min(16 * 1024**2, SPOOL_MAX_BYTES // 4)
)

if SPOOL_MAX_BYTES < IO_CHUNK_BYTES:
    raise RuntimeError("DSFLOWER_TUNNEL_SPOOL_MAX_BYTES is too small")


def relay_alive(hb_path):
    try:
        return (time.time() - os.path.getmtime(hb_path)) <= RELAY_TTL
    except OSError:
        # flowerTunnelUpDS seeds this file before spawning us. Its absence means
        # the relay cleaned up or its temporary session disappeared.
        return False


@contextmanager
def generation_fence(lock_path):
    """Share dsFlower's exchange.lock while replacing one byte stream."""
    with open(lock_path, "a+b") as lock_file:
        lock_file.seek(0)
        if os.name == "nt":
            # filelock uses LockFileEx on byte zero on Windows; CRT byte-range
            # locks interoperate with it without adding a Python dependency.
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            # filelock uses an fcntl write lock from byte zero through EOF.
            fcntl.lockf(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            lock_file.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.lockf(lock_file.fileno(), fcntl.LOCK_UN)


def publish_generation(gen_path, gen):
    tmp_path = f"{gen_path}.{os.getpid()}.tmp"
    with open(tmp_path, "w") as f:
        f.write(str(gen))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, gen_path)


def reset_spool(path, base=0):
    with open(path, "wb") as f:
        f.write(struct.pack("!d", float(base)))
        f.flush()
        os.fsync(f.fileno())


def spool_state(path):
    size = os.path.getsize(path)
    if size < SPOOL_HEADER_BYTES:
        raise RuntimeError("invalid tunnel spool header")
    with open(path, "rb") as f:
        header = f.read(SPOOL_HEADER_BYTES)
    base_float = struct.unpack("!d", header)[0]
    if (
        not math.isfinite(base_float)
        or base_float < 0
        or base_float != math.floor(base_float)
        or base_float > 2**53
    ):
        raise RuntimeError("invalid tunnel spool base")
    base = int(base_float)
    retained = size - SPOOL_HEADER_BYTES
    return base, retained, base + retained


def read_published_offset(path):
    try:
        with open(path, "r") as f:
            value = float(f.readline().strip())
        if (
            not math.isfinite(value)
            or value < 0
            or value > 2**53
            or value != math.floor(value)
        ):
            return None
        return int(value)
    except (OSError, ValueError):
        return None


def compact_spool(path, acknowledged, lock_path):
    """Drop an acknowledged prefix with base+payload replaced atomically."""
    base, _, eof = spool_state(path)
    target = min(acknowledged, eof)
    if target - base < COMPACT_THRESHOLD_BYTES:
        return False
    with generation_fence(lock_path):
        base, _, eof = spool_state(path)
        target = min(acknowledged, eof)
        if target - base < COMPACT_THRESHOLD_BYTES:
            return False
        tmp_path = f"{path}.{os.getpid()}.compact"
        try:
            with open(path, "rb") as source, open(tmp_path, "wb") as target_file:
                target_file.write(struct.pack("!d", float(target)))
                source.seek(SPOOL_HEADER_BYTES + target - base)
                while True:
                    chunk = source.read(IO_CHUNK_BYTES)
                    if not chunk:
                        break
                    target_file.write(chunk)
                target_file.flush()
                os.fsync(target_file.fileno())
            os.replace(tmp_path, path)
        finally:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
    return True


def accept_latest(srv):
    """Accept every queued dial and keep only the newest TCP stream."""
    latest = None
    while True:
        try:
            candidate, _ = srv.accept()
        except BlockingIOError:
            return latest
        if latest is not None:
            try:
                latest.close()
            except OSError:
                pass
        latest = candidate


def serve_connection(
    conn, up_path, down_path, gen_path, hb_path, up_ack_path, down_ack_path,
    lock_path, gen, srv
):
    conn.setblocking(False)
    # The R exchange holds this same advisory lock. A reconnect therefore
    # cannot truncate the old generation while an aggregate is appending or
    # reading it, and the published generation changes atomically with reset.
    with generation_fence(lock_path):
        reset_spool(up_path)
        reset_spool(down_path)
        with open(up_ack_path, "w") as f:
            f.write("0")
        with open(down_ack_path, "w") as f:
            f.write("0")
        publish_generation(gen_path, gen)
    down_off = 0
    idle = 0
    last_hb_check = 0.0
    while True:
        moved = False
        # relay liveness (cheap; throttled to ~once/sec)
        now = time.time()
        if now - last_hb_check > 1.0:
            last_hb_check = now
            if not relay_alive(hb_path):
                try:
                    conn.close()
                except OSError:
                    pass
                return False, None   # relay gone -> stop serving + exit
        # A gRPC redial can arrive before the kernel reports the old TCP stream
        # closed. Promote the newest queued connection immediately instead of
        # making it wait behind a half-open generation.
        replacement = accept_latest(srv)
        if replacement is not None:
            try:
                conn.close()
            except OSError:
                pass
            return True, replacement
        # socket -> up.bin. Once the bounded spool is full, stop receiving so
        # TCP applies backpressure instead of allowing unbounded disk growth.
        _, up_size, _ = spool_state(up_path)
        if up_size < SPOOL_MAX_BYTES:
            try:
                data = conn.recv(min(IO_CHUNK_BYTES, SPOOL_MAX_BYTES - up_size))
                if data:
                    with open(up_path, "ab") as f:
                        f.write(data)
                    moved = True
                else:
                    break  # peer closed
            except BlockingIOError:
                pass
            except (ConnectionResetError, OSError):
                break
        # down.bin -> socket
        try:
            down_base, _, down_eof = spool_state(down_path)
        except OSError:
            down_base, down_eof = down_off, down_off
        if down_off < down_base:
            raise RuntimeError("downstream offset precedes compacted spool base")
        if down_eof > down_off:
            with open(down_path, "rb") as f:
                f.seek(SPOOL_HEADER_BYTES + down_off - down_base)
                chunk = f.read(min(IO_CHUNK_BYTES, down_eof - down_off))
            try:
                sent = conn.send(chunk)
                if sent > 0:
                    down_off += sent
                    moved = True
            except BlockingIOError:
                pass
            except (BrokenPipeError, ConnectionResetError, OSError):
                break
        up_ack = read_published_offset(up_ack_path)
        if up_ack is not None and compact_spool(up_path, up_ack, lock_path):
            moved = True
        # Bytes sent to the SuperNode are retained until the relay has observed
        # their DSI ACK. This keeps an ACK-lost replay byte-verifiable instead of
        # accepting an unverifiable offset that has already been compacted.
        down_ack = read_published_offset(down_ack_path)
        if down_ack is not None and compact_spool(
            down_path, min(down_ack, down_off), lock_path
        ):
            moved = True
        if moved:
            idle = 0
        else:
            idle = min(idle + 1, 25)
            time.sleep(0.004 * idle)
    try:
        conn.close()
    except Exception:
        pass
    return True, None   # connection ended normally; keep serving (re-accept)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)   # host:port the SuperNode dials
    ap.add_argument("--spool", required=True)     # shared spool directory
    a = ap.parse_args()

    os.makedirs(a.spool, exist_ok=True)
    up_path = os.path.join(a.spool, "up.bin")
    down_path = os.path.join(a.spool, "down.bin")
    gen_path = os.path.join(a.spool, "gen")
    hb_path = os.path.join(a.spool, "relay_hb")
    up_ack_path = os.path.join(a.spool, "up.ack")
    down_ack_path = os.path.join(a.spool, "down.ack")
    lock_path = os.path.join(a.spool, "exchange.lock")
    for path in (up_path, down_path):
        if not os.path.exists(path) or os.path.getsize(path) < SPOOL_HEADER_BYTES:
            reset_spool(path)
    if not os.path.exists(up_ack_path):
        with open(up_ack_path, "w") as f:
            f.write("0")
    if not os.path.exists(down_ack_path):
        with open(down_ack_path, "w") as f:
            f.write("0")

    host, port_text = a.listen.rsplit(":", 1)
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("listen port must be between 1 and 65535")
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(8)
    srv.setblocking(False)
    open(os.path.join(a.spool, "ready"), "w").close()

    gen = 0
    pending = None
    while True:
        if not relay_alive(hb_path):
            break   # relay gone -> exit (SuperNode will hit --max-wait-time)
        if pending is None:
            try:
                pending = accept_latest(srv)
            except OSError:
                break
            if pending is None:
                time.sleep(0.05)
                continue
        conn, pending = pending, None
        gen += 1
        open(os.path.join(a.spool, "connected"), "w").close()
        keep, pending = serve_connection(
            conn, up_path, down_path, gen_path, hb_path, up_ack_path,
            down_ack_path, lock_path, gen, srv
        )
        if not keep:
            break   # relay went away mid-connection -> exit


if __name__ == "__main__":
    main()
