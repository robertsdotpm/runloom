# -*- coding: utf-8 -*-
"""Microservices and rpc -- a microservices and RPC toy using the disk_files primitive with a freeform payload, expecting failure.

Synthetic runloom toy program (auto-generated).
  test type : failure
  category  : microservices and RPC
  primitive : disk_files
  format    : freeform (struct)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
disk_files primitive to carry a freeform payload.  Prints PASS and exits 0 when
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
PRIM = "disk_files"
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
    digest = hashlib.sha256(THEME.encode("utf-8")).digest()
    return tuple(digest[i] for i in range(8))

def encode(obj):
    return struct.pack(">8B", *obj)

def decode(buf):
    return tuple(struct.unpack(">8B", bytes(buf)))

# ---- body ----
def main():
    runloom.monkey.patch()
    GO = runloom.go
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    workdir = tempfile.mkdtemp(prefix="rlsyn_")
    state = {}

    def worker():
        try:
            with open(os.path.join(workdir, "missing", "x.bin"), "rb") as handle:
                handle.read()
            state["err"] = None
        except FileNotFoundError as exc:
            state["err"] = type(exc).__name__

    def __root():
        GO(worker)
    runloom.run(NHUB, __root)
    shutil.rmtree(workdir, ignore_errors=True)
    finish(state.get("err") == "FileNotFoundError", state)

if __name__ == "__main__":
    main()
