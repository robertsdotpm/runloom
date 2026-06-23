# -*- coding: utf-8 -*-
"""Automation and scraping -- a automation and scraping toy using the select_selectors primitive with a compressed payload, expecting success.

Synthetic runloom toy program (auto-generated).
  test type : success
  category  : automation and scraping
  primitive : select_selectors
  format    : compressed (zlib)
  scheduler : M:N via runloom.run(8, root), free-threaded 3.13t, GIL off

Exercises runloom's main API -- the root goroutine spawns workers with
runloom.fiber(...) onto 8 hub threads, using the monkey-patched cooperative
select_selectors primitive to carry a compressed payload.  Prints PASS and exits 0 when
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
PRIM = "select_selectors"
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
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    state = {}

    def writer():
        runloom.sleep(0.01)
        right.sendall(struct.pack(">I", len(enc)) + enc)
        right.close()

    def reader():
        sel = selectors.DefaultSelector()
        sel.register(left, selectors.EVENT_READ)
        buf = bytearray()
        length = None
        try:
            while True:
                events = sel.select(timeout=3.0)
                if not events:
                    break
                chunk = left.recv(65536)
                if not chunk:
                    break
                buf += chunk
                if length is None and len(buf) >= 4:
                    length = struct.unpack(">I", bytes(buf[:4]))[0]
                if length is not None and len(buf) >= 4 + length:
                    break
        finally:
            sel.close()
        if length is not None and len(buf) >= 4 + length:
            state["ok"] = decode(bytes(buf[4:4 + length])) == payload
        else:
            state["ok"] = False
        left.close()

    def __root():
        GO(reader)
        GO(writer)
    runloom.run(NHUB, __root)
    finish(state.get("ok") is True, state)

if __name__ == "__main__":
    main()
