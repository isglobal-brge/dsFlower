"""Tiny TCP client to validate the DSI tunnel forwarder (milestone 1b).

Connects to the node-local forwarder, sends msg*nrep, reads that many bytes back
(the researcher's relay echoes them through DSI), and writes them to --out.
"""
import argparse
import socket


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--msg", required=True)
    ap.add_argument("--nrep", type=int, default=1000)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    payload = a.msg.encode() * a.nrep
    s = socket.create_connection(("127.0.0.1", a.port), timeout=60)
    s.sendall(payload)
    got = b""
    while len(got) < len(payload):
        chunk = s.recv(65536)
        if not chunk:
            break
        got += chunk
    with open(a.out, "wb") as f:
        f.write(got)
    s.close()


if __name__ == "__main__":
    main()
