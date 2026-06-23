"""big_100 / 116 -- SIGCHLD storm.

A SIGCHLD handler installed at import (main thread) counts child-state-change
signals.  Goroutines spawn many short-lived subprocesses (via procutil.popen,
which builds the Popen off-goroutine to dodge the nested-offload deadlock) and
reap them with communicate()/wait().  Under M:N, the SIGCHLD handler reenters
the interpreter while the cooperative child-reaping (pidfd wait / selectors
offload) is in flight.  Every child must be reaped with a non-None returncode --
no lost status, no zombie left behind.

Stresses: SIGCHLD reentrancy, subprocess reaping under M:N, no lost child
status, no zombies.
"""
import os
import signal
import subprocess
import sys

import harness
import procutil

# --- SIGCHLD handler installed at IMPORT time, on the main thread.  It MUST be
#     a no-op reaper that does NOT itself waitpid (that would steal the status
#     the cooperative Popen.wait needs); it only counts the signal. -----------
SIG_COUNT = [0]


def on_sigchld(signum, frame):
    SIG_COUNT[0] += 1


try:
    # SA_RESTART so the handler does not turn a blocking syscall into EINTR in
    # a way that breaks the cooperative reaper; we just want to observe it.
    signal.signal(signal.SIGCHLD, on_sigchld)
    HAVE_SIGCHLD = True
except (ValueError, OSError, AttributeError):
    HAVE_SIGCHLD = False        # Windows / no SIGCHLD


def worker(H, wid, rng, state):
    py = state["py"]
    for _ in H.round_range():
        try:
            # A trivial, fast-exiting child.  Alternate between `true` and a
            # no-op python -c so the spawn path varies; both exit 0 promptly.
            if (wid & 1) == 0:
                cmd = ["true"]
            else:
                cmd = [py, "-c", "pass"]
            proc = procutil.popen(cmd, stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL,
                                  running=H.running)
        except OSError:
            break
        try:
            proc.communicate()
        except OSError:
            if not H.running():
                break
            raise
        rc = proc.returncode
        if not H.check(rc is not None,
                       "child not reaped (returncode None) wid={0}".format(wid)):
            return
        if not H.check(rc == 0,
                       "child exited {0} wid={1}".format(rc, wid)):
            return
        H.reaped[wid] += 1              # single writer per slot (race-free)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"py": sys.executable}
    H.reaped = [0] * max(1, H.funcs)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(HAVE_SIGCHLD, "SIGCHLD unavailable on this platform")
    total_reaped = sum(H.reaped)
    H.check(total_reaped > 0, "no children were spawned/reaped")
    # No zombies must remain: every child this process spawned was reaped, so a
    # final non-blocking waitpid(-1) must raise ECHILD (no waitable children).
    leftover = 0
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break                       # ECHILD: no children -> clean
        except OSError:
            break
        if pid == 0:
            break                       # a child still running but not zombie
        leftover += 1
    H.check(leftover == 0,
            "{0} zombie/unreaped children remained".format(leftover))
    H.log("sigchld_handled={0} reaped={1} leftover={2}".format(
        SIG_COUNT[0], total_reaped, leftover))


if __name__ == "__main__":
    harness.main("p116_sigchld_storm", body, setup=setup, post=post,
                 default_funcs=120,
                 describe="SIGCHLD storm from many short-lived subprocesses; "
                          "every child reaped, no zombie, no lost status")
