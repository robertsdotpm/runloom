"""Regression: os.fork() must not leave a child deadlocked on a balance lock.

A hub holds runloom_g_global_lock (the cross-hub g-slab balance pool lock) for
the microseconds of a freelist splice. If fork() races that window, the child
inherits the lock LOCKED with a dead owner; the next balance-pool access in the
child deadlocks forever. runloom_after_fork_child() -- auto-run via
os.register_at_fork(after_in_child=...) -- must re-init it.

We make the race deterministic: a helper thread HOLDS the lock, then we fork; the
child does nothing but try to acquire that one lock. Without the after-fork reset
the child deadlocks (detected as a wait timeout); with it, the child returns at
once and exits 0.
"""
import os
import sys
import time
import signal
import threading

import runloom            # registers the os.register_at_fork(after_in_child) handler
import runloom_c

_HOLD_NS = 4_000_000_000   # 4s: longer than the child timeout so the lock is held at fork
_CHILD_TIMEOUT = 2.5


def _run_once():
    acquired = threading.Event()

    def _hold():
        acquired.set()
        runloom_c._test_g_balance_hold_ns(_HOLD_NS)   # acquire + sleep + release

    t = threading.Thread(target=_hold, daemon=True)
    t.start()
    acquired.wait()
    time.sleep(0.3)          # the event fires just before the acquire; let it land

    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # CHILD: register_at_fork already ran reset_after_fork(). The lock was
        # inherited from the (dead) holding thread. Probe it.
        try:
            runloom_c._test_g_balance_acquire()       # hangs iff inherited held
            os._exit(0)
        except BaseException:
            os._exit(2)

    deadline = time.time() + _CHILD_TIMEOUT
    while time.time() < deadline:
        wpid, st = os.waitpid(pid, os.WNOHANG)
        if wpid == pid:
            return os.WIFEXITED(st) and os.WEXITSTATUS(st) == 0
        time.sleep(0.02)
    # timed out -> child is deadlocked on the inherited lock
    try:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
    except BaseException:
        pass
    return False


def test_fork_does_not_deadlock_on_balance_lock():
    assert _run_once(), "child deadlocked on the inherited g-slab balance lock"


if __name__ == "__main__":
    ok = _run_once()
    print("RESULT:", "PASS -- child acquired the lock (no deadlock)" if ok
          else "FAIL -- child DEADLOCKED on the inherited g-slab balance lock")
    sys.exit(0 if ok else 1)
