# -*- coding: utf-8 -*-
"""Http clients and networking -- a HTTP clients and networking toy using the multiprocessing primitive with a json payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : HTTP clients and networking
  primitive : multiprocessing
  format    : json (json)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.go(...) onto 8 hub threads, using the monkey-patched cooperative
multiprocessing primitive to carry a json payload.  Prints PASS and exits 0 when
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
PRIM = "multiprocessing"
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
def mp_echo(in_q, out_q):
    out_q.put(in_q.get())

def main():
    runloom.monkey.patch()
    GO = runloom.go
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    ctx = mp.get_context("spawn")
    in_q = ctx.Queue()
    out_q = ctx.Queue()
    proc = ctx.Process(target=mp_echo, args=(in_q, out_q))
    state = {}

    def driver():
        proc.start()
        in_q.put(enc)
        got = out_q.get()
        proc.join()
        state["ok"] = decode(got) == payload
        state["exit"] = proc.exitcode

    def __root():
        GO(driver)
    runloom.run(NHUB, __root)
    in_q.close()
    out_q.close()
    finish(state.get("ok") is True and state.get("exit") == 0, state)

if __name__ == "__main__":
    main()
