"""big_100 / 307 -- exit WHILE a foreign OS thread is live in a patched primitive.

p209 is the *cooperative* inverse of this program: it shares a patched Lock /
Condition / Queue between goroutines and real OS threads, but it deliberately
WAITS (a deadline loop on threads_done) for every foreign thread to drain its
fixed budget before the run ends.  That tests steady-state foreign-thread safety.
The untested adversarial case is the TEARDOWN edge: exiting WHILE a foreign OS
thread is still actively churning a patched primitive -- parked in a patched
`Condition.wait(timeout)` or holding the cooperative-fallback `Lock` -- at the
exact instant `mn_run()` joins and `mn_fini()` tears down hub / scheduler-TLS
state.

The CLAUDE.md "FOREIGN-OS-THREAD-safe" invariant requires a patched primitive
reached from a non-goroutine thread to DETECT the foreign thread (TLS peek NULL,
no current g) and fall back to REAL-OS blocking -- never park a non-existent
goroutine, never lazily allocate scheduler state.  At teardown that fallback path
reads runloom TLS / hub state that `mn_fini()` is concurrently freeing: a foreign
thread that wakes from its patched `Condition.wait` AFTER the scheduler TLS / hub
array is freed reads now-freed state -> UAF / SIGSEGV, or it pins teardown forever
(the join never completes -> the process hangs).

BUG HUNTED: foreign-OS-thread vs mn_fini teardown race -- a patched primitive's
real-OS fallback touching freed scheduler TLS/hub state at the exit instant, or a
foreign thread parked on a torn-down primitive wedging teardown.

A CHILD runloom program is launched per worker.  The child:
  * captures `_thread` BEFORE monkey.patch(), then patches;
  * spawns M FOREIGN OS threads, each looping FOREVER (never signalled to stop):
    take a patched `Lock`, then `Condition.wait(timeout=0.05)` on shared state
    whose flag is NEVER set.  The SHORT timeout is deliberate (per the spec): the
    thread is genuinely *cycling* through the patched primitive at the exit
    instant -- re-entering the real-OS fallback every ~50ms -- rather than parked
    forever in one timeout-less wait that might never re-touch the fallback path
    during the teardown window;
  * runs a few goroutine WORKERS that all RETURN (so `run()` can join -- a
    goroutine parked forever would hang the clean path, see p134/p135), prints
    DONE-MARKER, then tears the root down via one of two shapes:
      - "return": fall off the end of main() -> `run()` joins the (returned)
        goroutines and the interpreter runs mn_fini WHILE the foreign threads are
        still mid-`Condition.wait`;
      - "os_exit": `os._exit(0)` with the foreign threads truly mid-flight and no
        finalization at all -- the more dangerous variant.
  The foreign threads are `start_new_thread` (daemon-equivalent), so they never
  themselves block process exit for a benign reason.

ORACLE: per child, returncode >= 0 (NO negative crash signal -- a teardown UAF
surfaces as -SIGSEGV / -SIGABRT) AND no TimeoutExpired (a foreign thread parked
on a torn-down primitive would wedge the join / process exit forever) AND
DONE-MARKER reached (proves the foreign threads were actually live and cycling
before teardown).  post() requires at least one CLEAN exit of BOTH the "return"
and the "os_exit" variant, so a regression in either teardown shape is caught.
Runs in subprocess isolation: a UAF must crash a CHILD, not poison the parent
sweep.

Stresses: foreign-OS-thread vs mn_fini teardown race, patched-Condition/Lock
real-OS fallback at the scheduler-GONE boundary, abrupt os._exit on top of live
foreign threads, no segfault / no wedge at exit.

Good TSan / deterministic-M:N-replay target: a foreign thread reading scheduler
TLS as mn_fini frees it is precisely the data race TSan exists to flag, and the
exit-instant interleave is what deterministic replay pins so a green run is
evidence rather than luck.
"""
import os
import subprocess

import harness
import procutil

# Both teardown shapes the child exercises; post() requires a clean observation
# of EACH so a regression in either is caught (the os_exit variant is the more
# dangerous one -- no mn_fini, foreign threads truly mid-flight).
VARIANTS = ("return", "os_exit")

# NOTE: the child script body itself uses .format()/{0} placeholders at RUNTIME,
# so we inject the src path via a sentinel token + .replace() rather than
# CHILD.format(...) (which would try to fill those runtime {0}s at build time).
CHILD = r'''
import sys, os, time
sys.path.insert(0, __SRC_PATH__)
# Capture the REAL thread spawner + real sleep BEFORE monkey.patch() so the
# foreign threads are genuine OS threads (not goroutines) and so the worker's
# settle sleep below is the cooperative one but the foreign threads' fallback is
# the real OS.
import _thread as _rt
REAL_SLEEP = time.sleep
import runloom
import runloom.monkey
runloom.monkey.patch()                    # threading.Lock/Condition now cooperative

MODE = sys.argv[1] if len(sys.argv) > 1 else "return"
NTHREADS = 6                              # modest foreign-OS-thread pool
NWORKERS = 8                             # a few goroutines that all RETURN

# Created AFTER monkey.patch() -> these are the PATCHED (cooperative) primitives.
# The foreign threads reach them from a non-goroutine thread, forcing the
# real-OS-fallback path the invariant is about.
import threading
lock = threading.Lock()
cond = threading.Condition(lock)
ready_flag = [False]                       # NEVER set -> wait() only ever times out
shared = [0]                               # touched under the lock so the Lock is hot
live_threads = [0]                         # how many foreign threads actually started

def foreign_loop(tid):
    # Runs on a REAL OS thread.  Loops FOREVER (never signalled to stop): take the
    # patched Lock, wait on the patched Condition with a SHORT timeout so the
    # thread is continuously re-entering the real-OS fallback -- it is genuinely
    # mid-primitive at whatever instant the root tears down.
    live_threads[0] += 1
    n = 0
    while True:
        try:
            with cond:
                shared[0] += 1            # mutate under the patched Lock
                # Short timeout: returns ~every 50ms, flag never set, so the
                # thread keeps cycling through acquire/wait/release -> it is mid
                # patched-primitive at the exit instant, not parked forever.
                cond.wait(timeout=0.05)
            n += 1
        except Exception:
            # If teardown rips a primitive out from under us, swallow it on the
            # foreign thread -- the ORACLE is the parent observing the process
            # exit cleanly (returncode>=0, no hang), not this thread's bookkeeping.
            return

def worker(wid):
    # A goroutine that does a little cooperative work and RETURNS, so run()'s
    # join completes on the clean ("return") path.
    for _ in range(50):
        with cond:                         # contend the SAME patched primitive
            shared[0] += 1
        runloom.yield_now()

def main():
    # Spawn the foreign OS threads first; they start cycling immediately.
    for tid in range(NTHREADS):
        _rt.start_new_thread(foreign_loop, (tid,))
    # A handful of goroutines that all return.
    for wid in range(NWORKERS):
        runloom.fiber(worker, wid)
    runloom.sleep(0.08)                    # let the foreign threads get mid-wait
    # Prove the foreign threads are actually live and cycling before we tear down.
    sys.stdout.write("DONE-MARKER live={0}\n".format(live_threads[0]))
    sys.stdout.flush()
    if MODE == "os_exit":
        # Foreign threads are RIGHT NOW mid-Condition.wait on a patched primitive;
        # terminate on top of them with no mn_fini, no finalization.
        sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
        os._exit(0)
    # "return": fall off the end -> run() joins the (returned) goroutines and the
    # interpreter runs mn_fini WHILE the foreign threads are still mid-wait on the
    # patched Condition.

runloom.run(4, main)
# Reached only on the "return" variant: run() joined cleanly with foreign threads
# still live in the patched primitive, and the interpreter is now finalizing.
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_fthread_exit_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.replace("__SRC_PATH__", repr(src)))
    # Per-variant clean-exit tallies (single-writer accumulation in post()).
    H.state = {"py": sys.executable, "script": script,
               "clean": {v: [0] * 1024 for v in VARIANTS}}


def worker(H, wid, rng, state):
    clean = state["clean"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        # Alternate the two teardown shapes deterministically per-(wid,round) so
        # both are exercised regardless of funcs/rounds, plus a little jitter.
        variant = VARIANTS[(wid + rng.getrandbits(1)) & 1]
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"], variant],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, env=env,
                                  running=H.running)
        except OSError:
            break
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            # A foreign thread parked on a torn-down primitive wedged the join /
            # process exit -- teardown never completed.
            H.fail("child HUNG at exit-with-live-foreign-thread ({0}) wid={1} "
                   "(teardown wedged by a foreign thread on a torn-down patched "
                   "primitive)".format(variant, wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # ORACLE: exit must be clean.  A teardown UAF (foreign thread waking from
        # a patched Condition after sched TLS is freed) surfaces as a NEGATIVE
        # returncode (-SIGSEGV / -SIGABRT); any nonzero is also a fault here since
        # both shapes are designed to exit 0.
        if not H.check(proc.returncode == 0,
                       "child ({0}) exited {1} wid={2} -- crash/UAF tearing down "
                       "on top of a live foreign thread in a patched primitive? "
                       "stderr={3!r}".format(
                           variant, proc.returncode, wid, err[-300:])):
            return
        # DONE-MARKER proves the foreign threads were actually live and cycling
        # through the patched primitive before the teardown shape ran.
        if not H.check(b"DONE-MARKER" in out,
                       "child ({0}) never reached the live-foreign-thread state "
                       "wid={1}: {2!r}".format(variant, wid, out[:160])):
            return
        clean[variant][slot] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = H.state["clean"]
    totals = {v: sum(clean[v]) for v in VARIANTS}
    H.log("clean_exits {0} (total ops={1}) exited={2}/{3}".format(
        " ".join("{0}={1}".format(v, totals[v]) for v in VARIANTS),
        H.total_ops(), H.exited, H.expected))
    H.check(H.total_ops() > 0,
            "no child exited cleanly with a live foreign thread at exit")
    # Require BOTH teardown shapes observed clean, so a regression in EITHER the
    # mn_fini-join path or the os._exit path is caught (per the spec: post()
    # asserts BOTH variants were observed clean, not just one).
    for v in VARIANTS:
        H.check(totals[v] > 0,
                "no CLEAN exit observed for the '{0}' teardown variant -- that "
                "teardown shape on top of a live foreign thread was never proven "
                "safe (or it crashed/hung every time)".format(v))


if __name__ == "__main__":
    harness.main("p307_foreign_thread_live_at_exit", body, setup=setup, post=post,
                 default_funcs=100,
                 describe="child runloom exits (mn_fini-join AND abrupt os._exit) "
                          "with a foreign OS thread still cycling a patched "
                          "Condition/Lock; returncode>=0, no crash, no hang")
