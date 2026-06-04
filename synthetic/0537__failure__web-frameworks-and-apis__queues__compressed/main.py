# -*- coding: utf-8 -*-
"""Web frameworks and apis -- a web frameworks and APIs toy using the queues primitive with a compressed payload, expecting failure.

Synthetic runloom toy program (auto-generated).
  test type : failure
  category  : web frameworks and APIs
  primitive : queues
  format    : compressed (zlib)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
queues primitive to carry a compressed payload.  Prints PASS and exits 0 when
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
PRIM = "queues"
FMT = "compressed"
TESTTYPE = "failure"
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
    GO = runloom.go
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    state = {}

    def worker():
        try:
            queue.Queue().get_nowait()      # empty
            state["err"] = None
        except queue.Empty:
            state["err"] = "Empty"

    def __root():
        GO(worker)
    runloom.run(NHUB, __root)
    finish(state.get("err") == "Empty", state)

if __name__ == "__main__":
    main()
