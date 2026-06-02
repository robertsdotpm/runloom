"""Regression: many goroutines doing offloaded I/O concurrently must not
deadlock (single-thread AND multi-hub M:N).

Before the _thread.allocate_lock fix this froze the scheduler: file work goes
through tempfile.gettempdir(), whose `_once_lock` was a REAL _thread lock (only
threading.Lock was patched).  Goroutine A held it across the yielding offloaded
probe while goroutine B blocked on the real lock on the same scheduler thread
-> deadlock.  A hung run is caught by run_isolated's per-file timeout.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pygo.monkey
import pygo_core

pygo.monkey.patch()

import tempfile     # imported AFTER patch, so its _once_lock is cooperative


def _filework():
    fd, path = tempfile.mkstemp()
    try:
        os.write(fd, b"z" * 128)
        os.close(fd)
        with open(path, "rb") as fh:
            fh.read()
    finally:
        os.unlink(path)


def test_concurrent_offload_single_thread():
    for _ in range(16):
        pygo_core.go(_filework, stack_size=2 << 20)
    pygo_core.run()
    assert pygo_core._self_check(0) == 0


def test_concurrent_offload_mn():
    pygo_core.mn_init(4)
    for _ in range(16):
        pygo_core.mn_go(_filework)
    pygo_core.mn_run()
    pygo_core.mn_fini()
    assert pygo_core._self_check(0) == 0
