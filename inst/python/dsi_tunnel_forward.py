"""dsFlower DSI-tunnel forwarder (node side).

Bridges a local TCP connection (the Flower SuperNode dials this) to a byte spool
that the DataSHIELD tunnel methods drain/fill, so the researcher's R relay can
carry the bytes to/from the SuperLink over DSI. No Tor, no public address: the
SuperNode connects to 127.0.0.1 on its own node.

    SuperNode --TCP--> 127.0.0.1:<listen> --(this)--> up.bin / down.bin (spool)
                                                       ^ drained/filled by DSI

Multi-connection: the SuperNode may drop and reconnect (e.g. a gRPC handshake
that timed out under tunnel latency). Each accepted connection starts a fresh
stream -- the spool is truncated and a monotonically increasing generation is
published in `gen`. The relay reads `gen` via flowerTunnelExchangeDS and, on a
change, resets its SuperLink socket and byte offsets so the new SuperNode stream
maps to a fresh SuperLink connection.

Spool protocol (per generation; append-only + relay-owned offsets):
  up.bin   : SuperNode -> SuperLink bytes  (this appends; the relay reads)
  down.bin : SuperLink -> SuperNode bytes  (the relay appends; this reads)
  gen      : current connection generation (this writes; the relay reads)
"""
import argparse
import os
import socket
import time


def serve_connection(conn, up_path, down_path, gen_path, gen):
    conn.setblocking(False)
    # Fresh stream for this connection: truncate the spool and publish the
    # generation so the relay resets its offsets/socket for the new SuperNode.
    open(up_path, "wb").close()
    open(down_path, "wb").close()
    with open(gen_path, "w") as f:
        f.write(str(gen))
    down_off = 0
    idle = 0
    while True:
        moved = False
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)   # host:port the SuperNode dials
    ap.add_argument("--spool", required=True)     # shared spool directory
    a = ap.parse_args()

    os.makedirs(a.spool, exist_ok=True)
    up_path = os.path.join(a.spool, "up.bin")
    down_path = os.path.join(a.spool, "down.bin")
    gen_path = os.path.join(a.spool, "gen")
    open(up_path, "ab").close()
    open(down_path, "ab").close()

    host, port = a.listen.rsplit(":", 1)
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(8)
    open(os.path.join(a.spool, "ready"), "w").close()

    gen = 0
    while True:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        gen += 1
        open(os.path.join(a.spool, "connected"), "w").close()
        serve_connection(conn, up_path, down_path, gen_path, gen)
        # loop back to serve a reconnecting SuperNode


if __name__ == "__main__":
    main()
