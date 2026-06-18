"""dsFlower DSI-tunnel forwarder (node side).

Bridges a local TCP connection (the Flower SuperNode dials this) to an
append-only byte spool that the DataSHIELD tunnel methods drain/fill, so the
researcher's R relay can carry the bytes to/from the SuperLink over DSI. No Tor,
no public address: the SuperNode connects to 127.0.0.1 on its own node.

    SuperNode --TCP--> 127.0.0.1:<listen> --(this)--> up.bin / down.bin (spool)
                                                       ^ drained/filled by DSI

Spool protocol (append-only + reader offset, no locking needed):
  up.bin   : SuperNode -> SuperLink bytes  (this appends; flowerTunnelPollDS reads)
  down.bin : SuperLink -> SuperNode bytes  (flowerTunnelPushDS appends; this reads)
"""
import argparse
import os
import socket
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)   # host:port the SuperNode dials
    ap.add_argument("--spool", required=True)     # shared spool directory
    a = ap.parse_args()

    os.makedirs(a.spool, exist_ok=True)
    up_path = os.path.join(a.spool, "up.bin")
    down_path = os.path.join(a.spool, "down.bin")
    open(up_path, "ab").close()
    open(down_path, "ab").close()

    host, port = a.listen.rsplit(":", 1)
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(1)
    # Signal readiness to the R method (which waits for this file).
    open(os.path.join(a.spool, "ready"), "w").close()

    conn, _ = srv.accept()
    conn.setblocking(False)
    open(os.path.join(a.spool, "connected"), "w").close()

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
                # peer closed the connection
                break
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

        # adaptive idle sleep: responsive when active, gentle when quiet
        if moved:
            idle = 0
        else:
            idle = min(idle + 1, 25)
            time.sleep(0.004 * idle)

    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
