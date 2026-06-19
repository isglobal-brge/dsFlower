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

Spool files (per generation; append-only + relay-owned offsets):
  up.bin   : SuperNode -> SuperLink bytes  (this appends; the relay reads)
  down.bin : SuperLink -> SuperNode bytes  (the relay appends; this reads)
  gen      : current connection generation (this writes; the relay reads)
  relay_hb : relay heartbeat (the relay touches; this watches for staleness)
"""
import argparse
import os
import socket
import time

# Tolerance window for a transient relay/connection loss: the forwarder keeps the
# SuperNode connected and the run recovers (the loss-free relay re-requests from
# its offsets) for up to this long. Only a SUSTAINED loss tears down. Matched to
# the SuperNode --max-wait-time so both tolerate the same window.
RELAY_TTL = float(os.environ.get("DSFLOWER_RELAY_TTL", "180"))  # seconds


def relay_alive(hb_path):
    try:
        return (time.time() - os.path.getmtime(hb_path)) <= RELAY_TTL
    except OSError:
        # No heartbeat file yet -> assume alive briefly (seeded at flowerTunnelUpDS)
        return True


def serve_connection(conn, up_path, down_path, gen_path, hb_path, gen):
    conn.setblocking(False)
    open(up_path, "wb").close()
    open(down_path, "wb").close()
    with open(gen_path, "w") as f:
        f.write(str(gen))
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
                return False   # relay gone -> stop serving + exit
        # socket -> up.bin
        try:
            data = conn.recv(65536)
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
            sz = os.path.getsize(down_path)
        except OSError:
            sz = down_off
        if sz > down_off:
            with open(down_path, "rb") as f:
                f.seek(down_off)
                chunk = f.read(sz - down_off)
            try:
                conn.sendall(chunk)
                down_off = sz
                moved = True
            except (BrokenPipeError, OSError):
                break
        if moved:
            idle = 0
        else:
            idle = min(idle + 1, 25)
            time.sleep(0.004 * idle)
    try:
        conn.close()
    except Exception:
        pass
    return True   # connection ended normally; keep serving (re-accept)


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
    open(up_path, "ab").close()
    open(down_path, "ab").close()

    host, port = a.listen.rsplit(":", 1)
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(8)
    srv.settimeout(5.0)   # so we can poll relay liveness while waiting to accept
    open(os.path.join(a.spool, "ready"), "w").close()

    gen = 0
    while True:
        if not relay_alive(hb_path):
            break   # relay gone -> exit (SuperNode will hit --max-wait-time)
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        gen += 1
        open(os.path.join(a.spool, "connected"), "w").close()
        keep = serve_connection(conn, up_path, down_path, gen_path, hb_path, gen)
        if not keep:
            break   # relay went away mid-connection -> exit


if __name__ == "__main__":
    main()
