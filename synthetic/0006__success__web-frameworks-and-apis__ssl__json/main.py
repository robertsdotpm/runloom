# -*- coding: utf-8 -*-
"""Web frameworks and apis -- a web frameworks and APIs toy using the ssl primitive with a json payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : web frameworks and APIs
  primitive : ssl
  format    : json (json)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
ssl primitive to carry a json payload.  Prints PASS and exits 0 when
healthy; FAIL / hang / crash signals a bug.
"""
import sys
import os
import io
import time
import json
import zlib
import base64
import struct
import csv
import pickle
import hashlib
import tempfile
import shutil
import socket
import ssl
import selectors
import signal
import subprocess
import threading
import queue
import multiprocessing as mp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "src"))
import runloom
import runloom_c

THEME = "web frameworks and APIs"
CATSLUG = "web-frameworks-and-apis"
PRIM = "ssl"
FMT = "json"
TESTTYPE = "success"
NW = 3
NHUB = 8
CERT = os.path.join(HERE, "..", "_assets", "cert.pem")
KEY = os.path.join(HERE, "..", "_assets", "key.pem")
PY = sys.executable


def finish(ok, info=""):
    if ok:
        print("PASS")
    else:
        print("FAIL:", info)
        sys.exit(1)


def make_coordinator(results, n, state):
    """Goroutine that fans in n boolean results over a channel."""
    def coordinator():
        good = 0
        for _ in range(n):
            value, _ = results.recv()
            if value:
                good += 1
        state["good"] = good
    return coordinator


def recvexact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("short read")
        buf += chunk
    return bytes(buf)

# ---- format codec ----
def mk_payload():
    return {"category": THEME, "kind": "json",
            "items": [THEME + "#" + str(i) for i in range(8)],
            "count": 8, "ok": True, "ratio": 0.5}

def encode(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")

def decode(buf):
    return json.loads(bytes(buf).decode("utf-8"))

# ---- body ----
def main():
    runloom.monkey.patch()
    GO = runloom.go
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(CERT, KEY)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    results = runloom.Chan(NW)
    state = {"good": 0}

    def handle(raw):
        try:
            conn = server_ctx.wrap_socket(raw, server_side=True,
                                          do_handshake_on_connect=False)
            conn.do_handshake()
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            body = recvexact(conn, length)
            conn.sendall(struct.pack(">I", len(body)) + body)
            conn.close()
        except Exception:
            try:
                raw.close()
            except OSError:
                pass

    def accept_loop():
        for _ in range(NW):
            raw, _ = listener.accept()
            GO(lambda r=raw: handle(r))

    def client():
        ok = False
        raw = None
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            raw = socket.create_connection(("127.0.0.1", port))
            conn = ctx.wrap_socket(raw, server_hostname="localhost",
                                   do_handshake_on_connect=False)
            conn.do_handshake()
            conn.sendall(struct.pack(">I", len(enc)) + enc)
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            got = recvexact(conn, length)
            ok = decode(got) == payload
            conn.close()
        except Exception:
            ok = False
            if raw is not None:
                try:
                    raw.close()
                except OSError:
                    pass
        results.send(ok)

    def __root():
        GO(make_coordinator(results, NW, state))
        GO(accept_loop)
        for _ in range(NW):
            GO(client)
    runloom.run(NHUB, __root)
    listener.close()
    finish(state["good"] == NW, state)

if __name__ == "__main__":
    main()
