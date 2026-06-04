# -*- coding: utf-8 -*-
"""Concurrency runtimes and synchronization -- a concurrency runtimes and synchronization toy using the disk_files primitive with a json payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : concurrency runtimes and synchronization
  primitive : disk_files
  format    : json (json)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
disk_files primitive to carry a json payload.  Prints PASS and exits 0 when
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

THEME = "concurrency runtimes and synchronization"
CATSLUG = "concurrency-runtimes-and-synchronization"
PRIM = "disk_files"
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
    workdir = tempfile.mkdtemp(prefix="rlsyn_")
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker(idx):
        ok = False
        try:
            path = os.path.join(workdir, "chunk{0}.bin".format(idx))
            with open(path, "wb") as handle:
                handle.write(enc)
            with open(path, "rb") as handle:
                got = handle.read()
            ok = decode(got) == payload
        except Exception:
            ok = False
        results.send(ok)

    def __root():
        GO(make_coordinator(results, NW, state))
        for i in range(NW):
            GO(lambda i=i: worker(i))
    runloom.run(NHUB, __root)
    shutil.rmtree(workdir, ignore_errors=True)
    finish(state["good"] == NW, state)

if __name__ == "__main__":
    main()
