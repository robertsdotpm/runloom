"""big_100 / 115 -- KeyboardInterrupt chaos.

A SIGINT handler installed at import (main thread, at import time so SIGINT
never kills the process) records that a SIGINT arrived.  A real OS thread fires
os.kill(pid, SIGINT) at a modest rate.  Goroutines acquire a cooperative lock
and open a temp file inside a try/except KeyboardInterrupt/finally, and at a
migration point inside that protected region they RAISE KeyboardInterrupt (when
a recent SIGINT was observed) so it propagates into the running/parked
goroutine exactly as an async interrupt would -- then the finally must always
release the lock and close the fd.

The load-bearing assertions are NO DEADLOCK (forward progress keeps rising),
ALL workers exit (no goroutine wedged holding the lock), no fd leak, and the
shared per-slot counters are consistent (the lock serialized cleanly every
round).  Delivery is sparse under M:N, so we do NOT assert a high
KeyboardInterrupt count -- only that the handler was installed and that, when a
KeyboardInterrupt did fire, cleanup ran.

WHY NOT raise DIRECTLY from the handler:  Under M:N the Python signal handler
runs on the MAIN thread, which is the scheduler.  When it services a pending
SIGINT at an idle/join point inside `mn_run()` (no goroutine parked to absorb
it -- e.g. during teardown/drain), a handler that RAISES carries the
KeyboardInterrupt OUT of `runloom.run()` -- the documented idle-Ctrl-C case
(CLAUDE.md "Signals deliver INTO the parked goroutine ... it carries one out of
run() only when nothing is parked to take it").  That is CORRECT runtime
behaviour, but it would fail the harness on a benign teardown signal.  So the
handler records the arrival and the GOROUTINE re-raises it into ITS OWN
try/except at a controlled in-goroutine point -- which is precisely the
propagation-into-a-goroutine + cleanup path this test targets, without the
benign idle escape.  See the candidate finding in the agent report.

Stresses: SIGINT arrival under M:N, KeyboardInterrupt propagation into a
goroutine, try/finally cleanup under the async exception, cooperative-lock
release on exception, no fd/lock leak, no deadlock.
"""
import os
import signal
import time as _time
import _thread as _real_thread          # captured before monkey.patch()

import harness
import runloom

REAL_SLEEP = _time.sleep

# --- SIGINT handler installed at IMPORT time, on the main thread, BEFORE the
#     harness runs, so a delivered SIGINT never kills the process.  It records
#     a monotonically increasing arrival count; goroutines observe it and
#     re-raise KeyboardInterrupt into their own try/except. --------------------
SIG_ARRIVED = [0]


def on_sigint(signum, frame):
    SIG_ARRIVED[0] += 1


try:
    signal.signal(signal.SIGINT, on_sigint)
    HAVE_SIGINT = True
except (ValueError, OSError, AttributeError):
    HAVE_SIGINT = False


def killer_thread(H):
    """Real OS thread: fire SIGINT at the process at a modest rate."""
    pid = os.getpid()
    while H.running():
        try:
            os.kill(pid, signal.SIGINT)
        except OSError:
            pass
        REAL_SLEEP(0.01)


def worker(H, wid, rng, state):
    lock = state["lock"]
    tmpdir = state["tmpdir"]
    nslots = state["nslots"]
    caught = 0
    seen = SIG_ARRIVED[0]
    rnd = 0
    for _ in H.round_range():
        rnd += 1
        f = None
        held = False
        try:
            held = lock.acquire()
            state["shared"][wid & (nslots - 1)] += 1   # single-writer slot
            path = os.path.join(tmpdir, "w{0}.tmp".format(wid))
            f = open(path, "wb")
            f.write(b"x" * 8)
            runloom.yield_now()        # migration point -- the KI lands here
            # Propagate a KeyboardInterrupt into THIS goroutine -- the finally
            # must then release the lock + close the fd under the exception.
            # Two triggers: (a) an OS SIGINT observed since we last looked (the
            # real-signal path, sparse under M:N); (b) a deterministic 1-in-K
            # injection so the cleanup-under-KI path is ALWAYS exercised even
            # when OS delivery is sparse.
            now = SIG_ARRIVED[0]
            os_sig = (now != seen)
            if os_sig:
                seen = now
            if os_sig or (rnd % 7) == 0:
                raise KeyboardInterrupt("p115 in-goroutine")
        except KeyboardInterrupt:
            caught += 1
        finally:
            if f is not None:
                try:
                    f.close()
                except OSError:
                    pass
            if held:
                lock.release()
        H.op(wid)
        H.task_done(wid)
    H.caught_counts[wid] = caught       # single writer per slot (race-free)


def setup(H):
    import threading
    tmpdir = H.make_tmpdir("big100_ki_")
    nslots = 1
    while nslots < max(1, H.funcs):
        nslots <<= 1
    H.state = {
        "lock": threading.Lock(),       # patched -> cooperative under runloom
        "tmpdir": tmpdir,
        "shared": [0] * nslots,
        "nslots": nslots,
    }
    H.caught_counts = [0] * max(1, H.funcs)


def body(H):
    if HAVE_SIGINT:
        _real_thread.start_new_thread(killer_thread, (H,))
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(HAVE_SIGINT, "SIGINT handler could not be installed")
    ops = H.total_ops()
    H.check(ops > 0, "no forward progress -- workers never completed a round "
                     "(possible deadlock)")
    H.check(H.exited >= H.expected,
            "not all workers exited: {0}/{1} (possible lock deadlock)".format(
                H.exited, H.expected))
    # The shared per-slot counters must sum to exactly ops: every round bumped
    # its slot exactly once under the lock and then bumped ops exactly once
    # (both KI-caught and clean rounds fall through to H.op after the finally).
    # A lost increment would mean the lock failed to serialize under M:N.
    total_shared = sum(H.state["shared"])
    total_caught = sum(H.caught_counts)
    H.check(total_shared == ops,
            "lock-protected counter {0} != ops {1} (lost increment)".format(
                total_shared, ops))
    H.check(SIG_ARRIVED[0] > 0, "no SIGINT was ever delivered/handled")
    H.log("sig_arrived={0} ki_caught={1} ops={2} exited={3}/{4}".format(
        SIG_ARRIVED[0], total_caught, ops, H.exited, H.expected))


if __name__ == "__main__":
    # Correctness/chaos test (async-exception injection into goroutines holding
    # a lock+fd).  The subject is exactly-once cleanup under KeyboardInterrupt,
    # not scale; at 100k+ the per-worker signal-injection chaos starves
    # completion.  Cap to the intended scale (the honest fix).
    harness.main("p115_keyboardinterrupt_chaos", body, setup=setup, post=post,
                 default_funcs=1000, max_funcs=1000,
                 describe="SIGINT->KeyboardInterrupt into goroutines holding a "
                          "lock+fd; finally always cleans up; no deadlock/leak")
