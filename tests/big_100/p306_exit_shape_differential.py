"""big_100 / 306 -- root-exit shape differential over identical in-flight state.

A runloom root can END in four distinct ways, and each takes a DIFFERENT path
through the M:N teardown (mn_run join / mn_fini / atexit / interpreter
finalization):

  * "return"  -- the root function returns; mn_run joins the still-live (now
    woken) goroutines and run() falls through.
  * "sysexit" -- the root raises SystemExit (sys.exit) out of its body.
  * "raise"   -- an UNHANDLED ordinary exception (RuntimeError) propagates out
    of the root.
  * "osexit"  -- the root calls os._exit(): the interpreter is torn down
    IMMEDIATELY, skipping mn_fini, atexit and finalization, on top of every
    live hub thread and netpoll registration.

p134 only covers return-from-root; p135 only covers return-vs-os._exit for
sockets.  NEITHER exercises SystemExit-out-of-root nor unhandled-exception-out-
of-root, and none does it as a CONTROLLED DIFFERENTIAL where every shape stands
up BYTE-IDENTICAL in-flight state before ending.  That identical state is the
adversarial part: at the shape-dispatch instant the child has

  * 1 accept loop parked in accept() on a bound listener,
  * K goroutines parked in recv() on live socketpairs,
  * K short-sleep-loop goroutines cycling the timer heap, and
  * 1 outstanding runloom.blocking() offload still running on a pool thread.

The bug hunted: a teardown path that frees the hub array (or runs Python
finalization on a live hub thread) BEFORE a parked goroutine's netpoll arm is
released -- or that delivers a stale wake to an already-freed parker -- would
SEGV/SIGABRT or HANG, and it would do so on ONE specific shape only (the four
paths diverge precisely in WHEN the scheduler is torn down relative to the
parked arms / the outstanding offload).

ORACLE (subprocess driver, like p135): the parent picks a shape per worker and
asserts a PER-SHAPE verdict on the child:

  * returncode is EXACTLY the expected value for that shape (here 0 for all
    four on a sound runtime: run() swallows SystemExit / unhandled exceptions
    out of root after printing the traceback, and os._exit(0) is a clean 0), and
  * returncode >= 0 -- ANY SIGSEGV/SIGABRT at teardown surfaces as a NEGATIVE
    returncode (-11 / -6) and fails the check on THAT shape, and
  * the child printed DONE-MARKER (it reached identical in-flight state) and
    MAIN-EXIT (it ran the shape to completion -- only osexit prints MAIN-EXIT
    before exiting; the other three reach it after run() returns), and
  * no TimeoutExpired -- a teardown that hangs the join (a stranded parker that
    its shape never woke, or a wedged offload drain) is H.fail'd.

require_no_lost on the parent pool.

NOTE on returncode semantics measured on this runtime: an unhandled SystemExit
or RuntimeError propagating out of the root is CAUGHT by run() (it prints the
traceback to stderr and returns), so the process still falls through to exit 0;
the differential bite is therefore "every shape exits cleanly (>=0) with its
expected code and no hang" -- a path that diverged by crashing/hanging on just
one shape is exactly what the per-shape verdict isolates.  Only osexit skips
run()'s join entirely (it is the one shape that does NOT wake its parkers).

Stresses: interpreter teardown differential (return / SystemExit / unhandled-
exception / os._exit), mn_run join vs mn_fini, finalization on a live hub,
abrupt exit over parked accept/recv/sleep + an outstanding offload, no segfault
or hang on any shape.

Good TSan / controlled-M:N-replay target: the teardown-vs-parked-arm and the
stale-wake-on-freed-parker orderings are pure memory-ordering races; a data-race
report at the join/free boundary is often the first signal, before the per-shape
returncode oracle even fires.
"""
import os
import subprocess

import harness
import procutil

# Per-shape EXPECTED process returncode on a sound runtime.  run() swallows a
# SystemExit / unhandled exception out of root (prints traceback, returns), so
# all four shapes exit 0; a crash at teardown surfaces as a NEGATIVE code that
# differs from the expected value AND is < 0 (double-caught below).
SHAPES = {
    "return":  0,   # mn_run joins the woken goroutines, run() falls through
    "sysexit": 0,   # SystemExit out of root, caught by run()
    "raise":   0,   # unhandled RuntimeError out of root, caught by run()
    "osexit":  0,   # os._exit(0): immediate, skips mn_fini/finalization
}
SHAPE_ORDER = ("return", "sysexit", "raise", "osexit")

# Identical in-flight state for EVERY shape: K recv-parked + K sleep-loop g's,
# 1 accept loop, 1 outstanding offload.  Kept modest so the child settles fast.
K = 8

CHILD = r'''
import sys, os, socket, threading, time
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                    # cooperative socket I/O on the hubs

SHAPE = sys.argv[1] if len(sys.argv) > 1 else "return"
K = {K}
stop = [False]
lock = threading.Lock()
live = []                                 # every live socket fd (listener, conns, pairs)

def track(s):
    with lock:
        live.append(s)
    return s

def recv_parked(s):
    # Parked forever in recv() until its socket is closed (the shape wakes it).
    try:
        s.recv(64)
    except OSError:
        pass

def sleep_loop():
    # Cycles the timer heap so the timer subsystem is live at the exit instant.
    while not stop[0]:
        runloom.sleep(0.01)

def accept_loop(srv):
    # Parked in accept() until the listener is closed.
    try:
        while not stop[0]:
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            track(conn)
    except OSError:
        pass

def slow_blocking():
    # A genuinely blocking call run on a pool thread via runloom.blocking; the
    # 0.4s real sleep guarantees the offload is STILL OUTSTANDING at the
    # shape-dispatch instant (it must not be trivially drained).
    time.sleep(0.4)
    return 1

def offload_g():
    try:
        runloom.blocking(slow_blocking)
    except Exception:
        pass

def main():
    srv = track(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(64)
    runloom.fiber(accept_loop, srv)
    for _ in range(K):
        a, b = socket.socketpair()
        track(a); track(b)
        runloom.fiber(recv_parked, a)     # parked recv on a live socketpair
        runloom.fiber(sleep_loop)         # short-sleep loop on the timer heap
    runloom.fiber(offload_g)              # one outstanding runloom.blocking offload
    runloom.sleep(0.05)                   # let every arm actually park
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()

    if SHAPE == "osexit":
        # No mn_fini / finalization: terminate ON TOP of every live arm + the
        # outstanding offload.  This is the ONLY shape that does NOT wake parkers.
        sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
        os._exit(0)

    # return / sysexit / raise must WAKE the parked accept/recv (and stop the
    # sleep loops) as part of the shape, else run()'s join hangs for a BENIGN
    # reason and masks the real teardown signal (per the p134 join semantic).
    stop[0] = True
    with lock:
        socks = list(live)
    for s in socks:
        try: s.close()
        except OSError: pass

    if SHAPE == "sysexit":
        sys.exit(0)                       # SystemExit out of root
    if SHAPE == "raise":
        raise RuntimeError("shape=raise: unhandled exception out of root")
    # SHAPE == "return": fall through, run() joins the woken goroutines.

runloom.run(4, main)
# Reached for return / sysexit / raise (run() swallows the exception and
# returns); osexit never gets here.
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_exitshape_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src, K=K))
    H.state = {"py": sys.executable, "script": script,
               "per_shape": [0] * 1024}   # ops attributed by shape index, 1/slot


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        # Pick THIS worker's shape; rotate so every shape is exercised across
        # the pool even at small --funcs.
        shape = SHAPE_ORDER[(wid + rng.getrandbits(2)) % len(SHAPE_ORDER)]
        expected = SHAPES[shape]
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"], shape],
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
            H.fail("child HUNG at root-exit shape={0!r} wid={1} (teardown join "
                   "wedged: a parker its shape never woke, or a stuck offload "
                   "drain)".format(shape, wid))
            return
        except OSError:
            if not H.running():
                break
            raise

        # PER-SHAPE verdict 1: the exact expected returncode for THIS shape.
        # A teardown SEGV/SIGABRT surfaces as a negative signal returncode and
        # both differs from `expected` (caught here) and is < 0 (caught next).
        if not H.check(proc.returncode == expected,
                       "shape={0!r} wid={1}: returncode {2} != expected {3} "
                       "(teardown crash/divergence on this shape only?) "
                       "stderr={4!r}".format(shape, wid, proc.returncode,
                                             expected, err[-200:])):
            return
        # PER-SHAPE verdict 2 (explicit, even though expected==0 here): NO
        # negative-signal returncode -- isolates a -SIGSEGV(-11)/-SIGABRT(-6) at
        # teardown that a future regression could turn `expected` negative for.
        if not H.check(proc.returncode is not None and proc.returncode >= 0,
                       "shape={0!r} wid={1}: crash-signal returncode {2} at "
                       "teardown (SIGSEGV/SIGABRT over live arms?) stderr={3!r}"
                       .format(shape, wid, proc.returncode, err[-200:])):
            return
        # The child reached identical in-flight state...
        if not H.check(b"DONE-MARKER" in out,
                       "shape={0!r} wid={1}: never reached in-flight state: "
                       "{2!r}".format(shape, wid, out[:120])):
            return
        # ...and ran the shape to completion (printed MAIN-EXIT).
        if not H.check(b"MAIN-EXIT" in out,
                       "shape={0!r} wid={1}: never printed MAIN-EXIT (root-exit "
                       "shape did not run to completion): {2!r}".format(
                           shape, wid, out[:200])):
            return
        state["per_shape"][SHAPE_ORDER.index(shape)] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ps = H.state["per_shape"]
    by_shape = {name: ps[i] for i, name in enumerate(SHAPE_ORDER)}
    H.log("clean_exits={0} by_shape={1} exited={2}/{3}".format(
        H.total_ops(), by_shape, H.exited, H.expected))
    H.check(H.total_ops() > 0,
            "no child reached a clean root-exit shape over live in-flight state")
    H.require_no_lost("exit-shape coverage")


if __name__ == "__main__":
    harness.main("p306_exit_shape_differential", body, setup=setup, post=post,
                 default_funcs=100,
                 describe="child runloom ends via return / SystemExit / "
                          "unhandled-exc / os._exit over IDENTICAL in-flight "
                          "state (parked accept+recv+sleep + outstanding "
                          "offload); per-shape expected returncode, >=0, no hang")
