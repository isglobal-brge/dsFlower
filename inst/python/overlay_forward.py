"""dsFlower overlay forwarder.

Listens on a loopback port for the local flower-supernode and forwards each
connection to the SuperLink's overlay (tailnet) address THROUGH the Tailscale
userspace SOCKS5 proxy. This lets the SuperNode reach a SuperLink that lives on
the researcher's machine across NAT, using only outbound connections.

    supernode --TCP--> 127.0.0.1:<listen> --(this)--> SOCKS5 --> <tailnet>:<port>

Usage:
    overlay_forward.py --listen 127.0.0.1:PORT --socks 127.0.0.1:1055 --target 100.x.y.z:9092
"""
import argparse, socket, struct, sys, threading


def pipe(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d:
                break
            b.sendall(d)
    except Exception:
        pass
    finally:
        for s in (a, b):
            try:
                s.close()
            except Exception:
                pass


def socks5_connect(proxy_host, proxy_port, dst_host, dst_port):
    s = socket.create_connection((proxy_host, proxy_port), timeout=30)
    s.sendall(b"\x05\x01\x00")
    if s.recv(2) != b"\x05\x00":
        raise RuntimeError("SOCKS5 no-auth refused")
    # IPv4 if dotted-quad, else domain
    parts = dst_host.split(".")
    is_ip = len(parts) == 4 and all(p.isdigit() for p in parts)
    if is_ip:
        req = b"\x05\x01\x00\x01" + socket.inet_aton(dst_host)
    else:
        h = dst_host.encode()
        req = b"\x05\x01\x00\x03" + bytes([len(h)]) + h
    req += struct.pack("!H", dst_port)
    s.sendall(req)
    rep = s.recv(10)
    if not rep or rep[1] != 0:
        raise RuntimeError("SOCKS5 connect failed rc=%s" % (rep[1] if rep else "none"))
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)   # host:port
    ap.add_argument("--socks", required=True)    # host:port
    ap.add_argument("--target", required=True)   # host:port
    a = ap.parse_args()

    lh, lp = a.listen.rsplit(":", 1)
    ph, pp = a.socks.rsplit(":", 1)
    th, tp = a.target.rsplit(":", 1)
    pp, tp = int(pp), int(tp)

    def handle(c):
        try:
            up = socks5_connect(ph, pp, th, tp)
            threading.Thread(target=pipe, args=(c, up), daemon=True).start()
            pipe(up, c)
        except Exception as e:
            sys.stderr.write("overlay_forward err: %s\n" % e)
            try:
                c.close()
            except Exception:
                pass

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lh, int(lp)))
    srv.listen(64)
    sys.stderr.write("overlay_forward %s -> SOCKS5(%s) -> %s\n" % (a.listen, a.socks, a.target))
    sys.stderr.flush()
    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()


if __name__ == "__main__":
    main()
