# -*- coding: utf-8 -*-
"""Automation and scraping -- a automation and scraping toy using the dns primitive with a unicode payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : automation and scraping
  primitive : dns
  format    : unicode (utf-8)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
dns primitive to carry a unicode payload.  Prints PASS and exits 0 when
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

THEME = "automation and scraping"
CATSLUG = "automation-and-scraping"
PRIM = "dns"
FMT = "unicode"
TESTTYPE = "success"
NW = 5
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
    return ("「" + THEME + "」 数据 données データ Ωμέγα 🐍🧵 ") * 3

def encode(obj):
    return obj.encode("utf-8")

def decode(buf):
    return bytes(buf).decode("utf-8")

# ---- body ----
def main():
    runloom.monkey.patch()
    GO = runloom.fiber
    payload = mk_payload()
    enc = encode(payload)
    assert decode(enc) == payload
    results = runloom.Chan(NW)
    state = {"good": 0}

    def worker():
        ok = False
        try:
            infos = socket.getaddrinfo("localhost", 80,
                                       proto=socket.IPPROTO_TCP)
            ok = len(infos) >= 1
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
