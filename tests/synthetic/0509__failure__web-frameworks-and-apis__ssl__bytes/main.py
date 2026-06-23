# -*- coding: utf-8 -*-
"""Web frameworks and apis -- a web frameworks and APIs toy using the ssl primitive with a bytes payload, expecting failure.

Synthetic runloom toy program (auto-generated).
  test type : failure
  category  : web frameworks and APIs
  primitive : ssl
  format    : bytes (raw)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
ssl primitive to carry a bytes payload.  Prints PASS and exits 0 when
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
FMT = "bytes"
TESTTYPE = "failure"
NW = 6
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
    head = (THEME + ":bytes:").encode("utf-8")
    return head + bytes((i * 7 + 11) % 256 for i in range(96))

def encode(obj):
    return bytes(obj)

def decode(buf):
    return bytes(buf)

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
    listener.listen(8)
    port = listener.getsockname()[1]
    state = {}

    def server():
        try:
            conn, _ = listener.accept()
            conn.recv(64)               # plain read -- never speaks TLS
            conn.close()
        except OSError:
            pass
        finally:
            listener.close()

    def client():
        raw = None
        try:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            raw = socket.create_connection(("127.0.0.1", port))
            conn = ctx.wrap_socket(raw, server_hostname="localhost",
                                   do_handshake_on_connect=False)
            conn.do_handshake()
            state["err"] = None
        except OSError as exc:          # ssl.SSLError is an OSError subclass
            state["err"] = type(exc).__name__
        finally:
            if raw is not None:
                try:
                    raw.close()
                except OSError:
                    pass

    def __root():
        GO(server)
        GO(client)
    runloom.run(NHUB, __root)
    finish(bool(state.get("err")), state)

if __name__ == "__main__":
    main()
