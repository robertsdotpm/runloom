"""Leak / resource-balance harness.

Coverage and TSan find wrong behaviour; this finds LEAKED resources -- the bug
class pygo has a documented history of (FD leak in load_interfaces, the
completed-PygoTask task<->driver cycle, the exception refcycle, leaked parkers)
and which the new monkey layer adds fresh surface for (the thread-pool offload
backend, the 60 s DNS cache, subprocess pipes, the cooperative socket/selector
wrappers).

Method: run a workload as a goroutine `iters` times and assert the live-object
population AND the open-fd count return to a post-warmup baseline.  Warmup
absorbs one-time setup (lazy imports, the offload pool, cache priming) so only
*per-iteration* growth -- a real leak -- trips it.  The fd check is the
strongest signal for pygo's history: any descriptor opened per iteration and
not closed shows up immediately.

Usage:
  tools/leak_check.py                 # run the built-in monkey workloads
  from tools.leak_check import check_leak
"""
import gc
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pygo_core


def fd_count():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def _drive(fn):
    box = []
    pygo_core.go(lambda: box.append(fn()), stack_size=8 << 20)
    pygo_core.run()
    return box[0] if box else None


def check_leak(workload, iters=60, warmup=5, name="workload",
               obj_tol=None, fd_tol=0):
    """Run `workload` (a 0-arg callable, executed inside a goroutine) `iters`
    times; return a dict and raise AssertionError on a leak.

    obj_tol defaults to iters//4 (gc.get_objects is intrinsically noisy);
    fd_tol defaults to 0 (descriptors must balance exactly)."""
    if obj_tol is None:
        obj_tol = max(8, iters // 4)

    for _ in range(warmup):
        _drive(workload)
    gc.collect()
    base_obj = len(gc.get_objects())
    base_fd = fd_count()

    for _ in range(iters):
        _drive(workload)
    gc.collect()
    end_obj = len(gc.get_objects())
    end_fd = fd_count()

    obj_growth = end_obj - base_obj
    fd_growth = end_fd - base_fd
    info = {"name": name, "iters": iters, "obj_growth": obj_growth,
            "fd_growth": fd_growth, "base_fd": base_fd, "end_fd": end_fd}
    print("[leak] {0:<22} iters={1:<4} obj_growth={2:<6} fd_growth={3} "
          "(fd {4}->{5})".format(name, iters, obj_growth, fd_growth,
                                 base_fd, end_fd))
    assert fd_growth <= fd_tol, \
        "{0}: leaked {1} fd(s) over {2} iters (fd {3}->{4})".format(
            name, fd_growth, iters, base_fd, end_fd)
    assert obj_growth <= obj_tol, \
        "{0}: leaked ~{1} objects over {2} iters (> tol {3})".format(
            name, obj_growth, iters, obj_tol)
    return info


# ----- built-in monkey-layer workloads -------------------------------------
def _wl_socketpair():
    import socket
    a, b = socket.socketpair()
    try:
        a.sendall(b"x" * 64)
        b.recv(64)
    finally:
        a.close()
        b.close()


def _wl_simplequeue():
    import queue
    q = queue.SimpleQueue()
    for i in range(8):
        q.put(i)
    while not q.empty():
        q.get()


def _wl_file_offload():
    import tempfile
    fd, path = tempfile.mkstemp()
    try:
        os.write(fd, b"hello" * 100)
        os.close(fd)
        with open(path, "rb") as fh:
            fh.read()
    finally:
        os.unlink(path)


def _wl_subprocess():
    import subprocess
    subprocess.run([sys.executable, "-c", "pass"], check=True)


def main():
    import pygo.monkey
    pygo.monkey.patch()
    rc = 0
    workloads = [
        ("socketpair", _wl_socketpair, 80),
        ("simplequeue", _wl_simplequeue, 80),
        ("file_offload", _wl_file_offload, 60),
        ("subprocess", _wl_subprocess, 30),
    ]
    for name, wl, iters in workloads:
        try:
            check_leak(wl, iters=iters, name=name)
        except AssertionError as e:
            print("[leak] LEAK: {0}".format(e))
            rc = 1
    print("[leak]", "no leaks" if rc == 0 else "LEAKS DETECTED")
    return rc


if __name__ == "__main__":
    sys.exit(main())
