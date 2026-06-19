# -*- coding: utf-8 -*-
"""Microservices and rpc -- a microservices and RPC toy using the threads primitive with a bytes payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : microservices and RPC
  primitive : threads
  format    : bytes (raw)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
threads primitive to carry a bytes payload.  Prints PASS and exits 0 when
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
PRIM = "threads"
FMT = "bytes"
TESTTYPE = "success"
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
    # Real OS threads coordinated via the monkey-patched threading.Lock,
    # joined cooperatively from a goroutine.  Correctness rests on the
    # atomicity of list.append under free-threading (not on the lock
    # serialising), so this stays valid under M:N parallelism -- the
    # lock is exercised under contention but never gates the result.
    lock = threading.Lock()
    collected = []
    state = {}

    def thread_body(idx):
        ok = decode(enc) == payload
        with lock:
            collected.append(ok)

    def driver():
        workers = [threading.Thread(target=thread_body, args=(i,))
                   for i in range(NW)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()                 # cooperative join parks the goroutine
        state["count"] = sum(1 for ok in collected if ok)
        state["total"] = len(collected)

    def __root():
        GO(driver)
    runloom.run(NHUB, __root)
    finish(state.get("count") == NW and state.get("total") == NW, state)

if __name__ == "__main__":
    main()
