# -*- coding: utf-8 -*-
"""Http clients and networking -- a HTTP clients and networking toy using the sockets primitive with a compressed payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : HTTP clients and networking
  primitive : sockets
  format    : compressed (zlib)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
sockets primitive to carry a compressed payload.  Prints PASS and exits 0 when
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

THEME = "HTTP clients and networking"
CATSLUG = "http-clients-and-networking"
PRIM = "sockets"
FMT = "compressed"
TESTTYPE = "success"
NW = 4
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
    unit = (THEME + " | stream record | ").encode("utf-8")
    return unit * 8000               # ~300 KB pre-compression (exercises heavy offload)

def encode(obj):
    return zlib.compress(obj, 6)     # transports small; decompress reconstructs

def decode(buf):
    return zlib.decompress(bytes(buf))

# ---- body ----
def main():
    runloom.monkey.patch()
    GO = runloom.fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    results = runloom.Chan(NW)
    state = {"good": 0}

    def handle(conn):
        try:
            hdr = recvexact(conn, 4)
            length = struct.unpack(">I", hdr)[0]
            body = recvexact(conn, length)
            conn.sendall(struct.pack(">I", len(body)) + body)
        except Exception:
            pass
        finally:
            conn.close()

    def accept_loop():
        for _ in range(NW):
            conn, _ = listener.accept()
            GO(lambda c=conn: handle(c))

    def client():
        ok = False
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            sock.sendall(struct.pack(">I", len(enc)) + enc)
            hdr = recvexact(sock, 4)
            length = struct.unpack(">I", hdr)[0]
            got = recvexact(sock, length)
            ok = decode(got) == payload
            sock.close()
        except Exception:
            ok = False
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
