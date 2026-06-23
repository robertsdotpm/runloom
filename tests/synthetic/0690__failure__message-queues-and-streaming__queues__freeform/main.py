# -*- coding: utf-8 -*-
"""Message queues and streaming -- a message queues and streaming toy using the queues primitive with a freeform payload, expecting failure.

Synthetic runloom toy program (auto-generated).
  test type : failure
  category  : message queues and streaming
  primitive : queues
  format    : freeform (base64)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
queues primitive to carry a freeform payload.  Prints PASS and exits 0 when
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

THEME = "message queues and streaming"
CATSLUG = "message-queues-and-streaming"
PRIM = "queues"
FMT = "freeform"
TESTTYPE = "failure"
NW = 7
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
    return (THEME + "::b64::").encode("utf-8") + bytes((i * 3) % 256 for i in range(80))

def encode(obj):
    return base64.b64encode(bytes(obj))

def decode(buf):
    return base64.b64decode(bytes(buf))

# ---- body ----
def main():
    runloom.monkey.patch()
    GO = runloom.fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            bounded = queue.Queue(maxsize=1)
            bounded.put_nowait(enc)
            bounded.put_nowait(enc)         # full
            state["err"] = None
        except queue.Full:
            state["err"] = "Full"

    def __root():
        GO(worker)
    runloom.run(NHUB, __root)
    finish(state.get("err") == "Full", state)

if __name__ == "__main__":
    main()
