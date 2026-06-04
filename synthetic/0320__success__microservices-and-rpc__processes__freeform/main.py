# -*- coding: utf-8 -*-
"""Microservices and rpc -- a microservices and RPC toy using the processes primitive with a freeform payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : microservices and RPC
  primitive : processes
  format    : freeform (csv)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
processes primitive to carry a freeform payload.  Prints PASS and exits 0 when
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

THEME = "microservices and RPC"
CATSLUG = "microservices-and-rpc"
PRIM = "processes"
FMT = "freeform"
TESTTYPE = "success"
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
    return [[THEME, str(i), str(i * i)] for i in range(6)]

def encode(obj):
    out = io.StringIO()
    writer = csv.writer(out)
    for row in obj:
        writer.writerow(row)
    return out.getvalue().encode("utf-8")

def decode(buf):
    rows = list(csv.reader(io.StringIO(bytes(buf).decode("utf-8"))))
    return [r for r in rows if r]

# ---- body ----
CHILD_ECHO = "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"

def main():
    runloom.monkey.patch()
    GO = runloom.go
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker():
        ok = False
        try:
            done = subprocess.run([PY, "-c", CHILD_ECHO], input=enc,
                                  capture_output=True)
            ok = (done.returncode == 0) and (decode(done.stdout) == payload)
        except Exception:
            ok = False
        results.send(ok)

    def __root():
        GO(make_coordinator(results, NW, state))
        for _ in range(NW):
            GO(worker)
    runloom.run(NHUB, __root)
    finish(state["good"] == NW, state)

if __name__ == "__main__":
    main()
