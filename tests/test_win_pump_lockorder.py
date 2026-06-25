"""WSAPoll / select idle-pump GIL<->pool.lock inversion soak -- Windows.

Regression for the AB-BA lock-order bug fixed in netpoll_pump.c.inc: the WSAPoll
and select() idle-pump branches used to hold runloom_pool.lock ACROSS
Py_BEGIN/END_ALLOW_THREADS.  Py_END_ALLOW_THREADS re-attaches (waits out a
pending stop-the-world), while every other pool.lock holder (wait_fd, register,
release_if_idle) takes the lock WITH the thread attached -- so the pump's
(pool.lock -> attach/STW) order inverts everyone else's (attached -> pool.lock):

  pump:      detached, HOLDS pool.lock, blocks re-attaching (a GC STW is pending)
  STW:       waits for every thread to reach a safe point
  hub T:     attached, spins in register() waiting for pool.lock (held by pump)
             -> not at a safe point -> STW never completes -> 3-way deadlock.

There is no deterministic trigger (it is a timing race), so this is a SOAK: many
hubs + heavy concurrent wait_fd/close churn (pool.lock contention) + object
allocation churn (STW pressure) maximise the interleaving.  With the fix the pump
drops pool.lock before the wait, so the workload always completes; without it the
run() eventually wedges and run_isolated's watchdog SIGABRTs the file (a timeout
== the deadlock).  Backend is forced via RUNLOOM_NETPOLL (wsapoll | select); both
were fixed, so the runner exercises each.
"""
import os
import socket
import sys

import pytest

sys.path.insert(0, "src")

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("win"),
    reason="WSAPoll/select idle-pump is the Windows netpoll backend")

import runloom_c          # noqa: E402
import runloom            # noqa: E402

READ = 1
WRITE = 2

HUBS = 4
WORKERS = 64
ITERS = 40          # wait_fd parks per worker per session
SESSIONS = 4        # re-roll the interleaving
DEADLINE_MS = 50    # READ park sits parked this long -> the pump actively polls it

# NOTE: this is a PROBABILISTIC soak, not a deterministic repro -- the AB-BA is a
# timing race, so a clean pass does not *prove* absence (the structural fix --
# the pump dropping pool.lock before the wait -- is the real guarantee; this is
# the dynamic backstop against a future regression).
#
# A FIXED pool of socketpairs is created ONCE and reused: on Windows
# socket.socketpair() is emulated as a loopback-TCP connect, so creating ~10^5
# of them in a loop exhausts ephemeral ports and BLOCKS the hub on the
# synchronous connect -- which masquerades as a pump wedge but is the test, not
# the bug.  Reusing a fixed set still drives the inversion surface: each wait_fd
# re-registers under pool.lock, the parked READ fds keep the pump polling (it
# takes pool.lock across the GIL re-attach), and the allocation churn forces
# biased-refcount/GC stop-the-world -- the third leg of the AB-BA.


def test_backend_is_windows_pump():
    """Guard: the inversion lives in the WSAPoll + select pump branches only."""
    be = runloom_c.netpoll_backend()
    assert be in ("wsapoll", "select"), (
        "force a Windows pump backend: set RUNLOOM_NETPOLL=wsapoll (or select); "
        "got %r" % be)


def test_pump_lockorder_soak():
    """SESSIONS x {run(HUBS): WORKERS goroutines, each parking ITERS times in
    wait_fd(READ, deadline) on its own fixed fd + allocation churn}.  Parked fds
    keep the pump polling (pool.lock held across the GIL re-attach); the re-parks
    contend pool.lock; the churn drives stop-the-world.  Healthy: each session
    finishes in ~2s.  Deadlocked (pre-fix): run() wedges -> run_isolated's
    per-file watchdog SIGABRTs (the timeout == the deadlock)."""
    pairs = [socket.socketpair() for _ in range(WORKERS)]
    for a, b in pairs:
        a.setblocking(False)
        b.setblocking(False)
    fds = [a.fileno() for a, b in pairs]
    backend = [None]
    try:
        for s in range(SESSIONS):
            done = bytearray(WORKERS)    # one writer per slot -> race-free, GIL off

            def worker(done, fd, idx):
                churn = []
                for _ in range(ITERS):
                    try:
                        # READ never fires (no peer write) -> parks to the
                        # deadline, so the pump actively polls it; the return +
                        # next call re-registers under pool.lock.
                        runloom_c.wait_fd(fd, READ, DEADLINE_MS)
                    except Exception:        # noqa: BLE001
                        pass
                    churn.append([object() for _ in range(8)])   # STW pressure
                    if len(churn) > 4:
                        churn.pop(0)
                done[idx] = 1

            def main(done=done):
                backend[0] = runloom_c.netpoll_backend()
                for i in range(WORKERS):
                    runloom.go(worker, done, fds[i], i)

            runloom.run(HUBS, main)
            completed = sum(done)
            assert completed == WORKERS, (
                "session %d/%d: only %d/%d workers finished -- run() wedged "
                "(pump GIL<->pool.lock inversion?) on backend=%s"
                % (s + 1, SESSIONS, completed, WORKERS, backend[0]))
    finally:
        for a, b in pairs:
            try:
                a.close(); b.close()
            except Exception:                # noqa: BLE001
                pass

    assert backend[0] in ("wsapoll", "select"), backend[0]
