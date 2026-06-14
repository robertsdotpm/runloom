"""Adversarial QA: monkey offload surfaces -- heavy CPU-bound stdlib
(hashlib/zlib auto-offload), cooperative subprocess waiting, and selectors.

The offload pool must keep the scheduler RUNNING other fibers while a CPU-bound
or blocking call is offloaded (no stalled hub), and produce correct results.
Runs under monkey.patch() like the other monkey suites.
"""
import hashlib
import sys
import time
import zlib

import runloom.monkey as monkey
monkey.patch()

import selectors
import socket
import subprocess

import pytest

import runloom_c as rc
from adv_util import hang_guard, assert_faster_than


# --------------------------------------------------------------------------
# heavy CPU-bound offload (size-gated)
# --------------------------------------------------------------------------
def test_hashlib_large_offload_correct():
    data = b"x" * (1 << 20)                 # 1 MiB -> above the offload threshold
    expected = hashlib.sha256(data).hexdigest()   # computed here (may itself offload)
    out = {}
    def main():
        out["h"] = hashlib.sha256(data).hexdigest()
    with hang_guard(20, "hashlib offload"):
        rc.go(main); rc.run()
    # ground-truth via a fresh hasher fed in small chunks (stays inline)
    h = hashlib.sha256()
    for i in range(0, len(data), 4096):
        h.update(data[i:i + 4096])
    assert out["h"] == h.hexdigest() == expected


def test_zlib_roundtrip_large():
    data = bytes((i * 7) & 0xFF for i in range(4096)) * 256   # 1 MiB, compressible-ish
    out = {}
    def main():
        comp = zlib.compress(data, 6)
        out["ok"] = zlib.decompress(comp) == data
        out["shrank"] = len(comp) < len(data)
    with hang_guard(20, "zlib roundtrip"):
        rc.go(main); rc.run()
    assert out.get("ok") is True
    assert out.get("shrank") is True


def test_heavy_offload_overlaps_scheduler():
    # A big hash on one fiber must not freeze the scheduler -- a burner fiber
    # keeps making progress while the hash is offloaded.
    big = b"y" * (4 << 20)
    progress = []
    def hasher():
        progress.append("hash-start")
        hashlib.sha512(big).hexdigest()
        progress.append("hash-done")
    def burner():
        for i in range(20):
            progress.append(("burn", i))
            rc.sched_yield()
    def main():
        rc.go(hasher)
        rc.go(burner)
    with hang_guard(20, "heavy overlap"):
        rc.go(main); rc.run()
    done_idx = progress.index("hash-done")
    burns_before = sum(1 for p in progress[:done_idx]
                       if isinstance(p, tuple) and p[0] == "burn")
    assert burns_before >= 5, "scheduler stalled during the offloaded hash (%d burns)" % burns_before


# --------------------------------------------------------------------------
# cooperative subprocess waiting
# --------------------------------------------------------------------------
def test_subprocess_wait_is_cooperative():
    progress = []
    def waiter():
        progress.append("spawn")
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])
        p.wait()
        progress.append(("exit", p.returncode))
    def burner():
        for i in range(10):
            progress.append(("burn", i))
            time.sleep(0.01)                # cooperative (patched)
    def main():
        rc.go(waiter)
        rc.go(burner)
    with hang_guard(20, "subprocess wait"):
        rc.go(main); rc.run()
    assert ("exit", 0) in progress
    exit_idx = progress.index(("exit", 0))
    burns_before = sum(1 for p in progress[:exit_idx]
                       if isinstance(p, tuple) and p[0] == "burn")
    assert burns_before >= 5, "subprocess.wait() blocked the scheduler (%d burns)" % burns_before


def test_subprocess_wait_timeout_then_kill():
    out = {}
    def main():
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        try:
            p.wait(timeout=0.1)
            out["r"] = "no-timeout"
        except subprocess.TimeoutExpired:
            out["r"] = "timeout"
            p.kill()
            p.wait()
    with hang_guard(20, "subprocess timeout"):
        rc.go(main); rc.run()
    assert out.get("r") == "timeout"


# --------------------------------------------------------------------------
# selectors (cooperative DefaultSelector)
# --------------------------------------------------------------------------
def test_selectors_default_selector_cooperative():
    out = {}
    def main():
        sel = selectors.DefaultSelector()
        a, b = socket.socketpair()
        sel.register(a, selectors.EVENT_READ)
        def writer():
            rc.sched_yield(); rc.sched_yield()
            b.send(b"go")
        rc.go(writer)
        events = sel.select(timeout=3)
        out["n"] = len(events)
        out["data"] = a.recv(2)
        sel.close(); a.close(); b.close()
    with hang_guard(20, "selectors"):
        rc.go(main); rc.run()
    assert out.get("n", 0) >= 1
    assert out.get("data") == b"go"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
