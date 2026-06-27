"""big_100 / 308 -- atexit handlers post-mn_fini: blocking IO + scheduler spawn.

atexit handlers run AFTER `runloom.run()` returns -- i.e. after mn_fini has torn
the M:N scheduler down and the hub threads are gone.  An atexit handler is thus
the WORST possible caller of a monkey-patched cooperative primitive: there is no
live goroutine (TLS peek is NULL) and, worse, the hub array the scheduler used
may already be freed.  The runtime's foreign-OS-thread invariant says such a
caller must fall back to the REAL OS (block on the real kernel primitive, never
park a non-existent g, never lazily alloc sched state, never touch a freed hub
array).  This program drives that invariant from atexit at the scheduler-GONE
boundary.

p79 only registers a trivial print; it never exercises a blocking-IO atexit
handler nor a scheduler-touching (`fiber()`) atexit handler post-mn_fini.

A CHILD runloom program registers four atexit handlers BEFORE run() (so they
fire in REVERSE registration order during interpreter finalization, after the
scheduler is gone):

  * h0 (registered first, runs LAST):  a wall-time-checked patched
    `time.sleep(SLEEP_S)`.  Post-scheduler this MUST fall back to a real kernel
    sleep -- so the handler measures elapsed monotonic time and only prints its
    marker WITH a `dt=` it actually waited.  A broken fallback that returned
    instantly (or tried to park a non-existent g and silently no-op'd) would
    show dt ~= 0; a fallback that deadlocked trying to park would HANG the
    process (caught by the parent's timeout).
  * h1:  acquire + release a patched `threading.Lock`.  Post-scheduler this must
    block/acquire on the real OS lock, not park a non-existent g.
  * h2:  blocking IO on a real socketpair: send 4 bytes, recv them back via a
    patched socket.  Post-scheduler the patched recv must do a real kernel recv,
    not register a netpoll arm on a freed hub.
  * h3 (registered last, runs FIRST):  attempt `runloom.fiber(noop)` with NO live
    scheduler.  The SHARPEST sub-probe: it must take the no-scheduler path
    cleanly -- it must NOT crash and must NOT run the body on a (freed) hub.  We
    prove it took that path by printing a FALLBACK marker: either fiber() raised
    (a clean refusal) OR it returned without the body ever executing on a live
    hub (the no-scheduler no-op).  A handler that "silently worked" by running
    the body on a torn-down hub -- or SIGSEGV'd touching the freed hub array --
    is exactly the bug; both are caught (the former by the body-ran assertion in
    the child -> non-FALLBACK marker, the latter by a crash-signal returncode).

Child main stands up a couple of background goroutines, winds them down, and
returns; interpreter finalization then fires the four atexit handlers.

ORACLE (subprocess driver, like p135 / p306):

  * returncode == 0 AND >= 0 -- a SIGSEGV/SIGABRT from an atexit handler touching
    a freed hub array surfaces as a NEGATIVE signal returncode and fails, and
  * all four ATEXIT-<k> markers present AND in REVERSE registration order
    (3,2,1,0) -- proves every handler ran to completion after the scheduler was
    gone, none hung, and finalization ordering held under the M:N runtime, and
  * a FALLBACK marker from h3 -- proves the post-fini `fiber()` took the
    no-scheduler path (clean raise OR no-op body), NOT a live/freed hub, and
  * h0's marker carries dt >= SLEEP_FLOOR -- proves the blocking sleep handler
    ACTUALLY WAITED on the real OS (a no-op fallback returns instantly and would
    otherwise silently pass), and
  * no TimeoutExpired -- an atexit handler that deadlocked trying to park a
    non-existent g HANGs the process and is H.fail'd.

require_no_lost on the parent pool.

Stresses: atexit / interpreter-finalization ordering post-mn_fini, monkey-patched
blocking primitives (time.sleep / Lock / socket) falling back to the real OS with
the scheduler GONE, runloom.fiber() with no live scheduler, no park of a
non-existent g, no touch of a freed hub array, no crash/hang at the
scheduler-already-gone boundary.

Good TSan / controlled-M:N-replay target: the atexit-vs-mn_fini ordering and the
freed-hub-array read are pure teardown-ordering hazards; a data-race / use-after-
free report at the post-fini spawn is often the first signal, before the marker /
returncode oracle even fires.
"""
import os
import subprocess

import harness
import procutil

# How long the wall-time-checked blocking sleep handler waits, and the floor the
# parent requires it to have actually waited (generous margin below SLEEP_S so a
# real-but-jittery OS sleep never false-fails; a broken no-op fallback returns in
# ~microseconds and is well under the floor).
SLEEP_S = 0.05
SLEEP_FLOOR = 0.02

CHILD = r'''
import sys, os, time, atexit, threading, socket
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                    # patched time.sleep / Lock / socket

SLEEP_S = {sleep_s}

def emit(s):
    sys.stdout.write(s + "\n"); sys.stdout.flush()

# --- atexit handlers, registered h0..h3; they FIRE in reverse: h3,h2,h1,h0. ---
# Each runs AFTER runloom.run() returns -> after mn_fini -> scheduler gone.

def h0_sleep():
    # Wall-time-checked: the patched time.sleep must fall back to a REAL kernel
    # sleep post-scheduler (no live g to park).  Marker carries the measured dt
    # so the parent can prove it actually waited.
    t0 = time.monotonic()
    time.sleep(SLEEP_S)
    dt = time.monotonic() - t0
    emit("ATEXIT-0 dt={{0:.4f}}".format(dt))

def h1_lock():
    # Patched Lock must acquire/release on the real OS, not park a non-existent g.
    lk = threading.Lock()
    lk.acquire()
    try:
        pass
    finally:
        lk.release()
    emit("ATEXIT-1")

def h2_sock():
    # Real blocking socket round-trip through the patched socket: post-scheduler
    # recv must hit the real kernel, not arm netpoll on a freed hub.
    a, b = socket.socketpair()
    try:
        a.sendall(b"ping")
        got = b.recv(4)
        emit("ATEXIT-2 got={{0!r}}".format(got))
    finally:
        a.close(); b.close()

def h3_fiber():
    # SHARPEST probe: runloom.fiber() with NO live scheduler.  Must take the
    # no-scheduler path -- clean raise OR a no-op whose body never runs on a
    # (freed) hub -- never crash, never "silently work" by executing on a hub.
    ran = [False]
    def noop():
        ran[0] = True
    took_fallback = False
    try:
        g = runloom.fiber(noop)
        # If it returned, give any (illegitimate) live-hub execution a real
        # window to run the body.  On a sound runtime there is no scheduler, so
        # the body must NOT run -> ran[0] stays False -> this is the fallback.
        time.sleep(0.02)
        if not ran[0]:
            took_fallback = True            # no-scheduler no-op: clean fallback
    except Exception as e:                   # a clean raise is also the fallback
        took_fallback = True
        emit("FIBER-RAISED {{0}}".format(type(e).__name__))
    if took_fallback:
        emit("FALLBACK")
    else:
        # The body RAN on a torn-down hub -- the targeted bug.
        emit("FIBER-BODY-RAN-ON-DEAD-HUB")
    emit("ATEXIT-3")

atexit.register(h0_sleep)                    # runs LAST
atexit.register(h1_lock)
atexit.register(h2_sock)
atexit.register(h3_fiber)                     # runs FIRST

stop = [False]

def bg():
    n = 0
    while not stop[0] and n < 2000:
        runloom.sleep(0.001); n += 1

def main():
    for _ in range(4):
        runloom.fiber(bg)
    runloom.sleep(0.02)
    stop[0] = True                           # wind the background g's down

runloom.run(4, main)
emit("MAIN-EXIT")                            # printed BEFORE atexit handlers fire
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_atexitblk_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src, sleep_s=SLEEP_S))
    H.state = {"py": sys.executable, "script": script}


# The four markers, in the order they MUST appear in the child's stdout (reverse
# registration order).  ATEXIT-0 carries a trailing "dt=...".
ORDER = [b"ATEXIT-3", b"ATEXIT-2", b"ATEXIT-1", b"ATEXIT-0"]


def _ordered(out, markers):
    """True iff each marker appears AND in the given order (by first index)."""
    last = -1
    for m in markers:
        i = out.find(m)
        if i < 0 or i < last:
            return False
        last = i
    return True


def _sleep_dt(out):
    """Parse h0's measured dt from 'ATEXIT-0 dt=NNNN'; -1.0 if absent/garbled."""
    i = out.find(b"ATEXIT-0 dt=")
    if i < 0:
        return -1.0
    tail = out[i + len(b"ATEXIT-0 dt="):].split(b"\n", 1)[0]
    try:
        return float(tail.strip())
    except ValueError:
        return -1.0


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"]],
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
            H.fail("child HUNG running atexit handlers post-mn_fini wid={0} "
                   "(a handler deadlocked trying to park a non-existent g, or "
                   "blocked forever on a real-OS fallback)".format(wid))
            return
        except OSError:
            if not H.running():
                break
            raise

        # 1) clean exit -- a SIGSEGV/SIGABRT from a handler touching the freed
        #    hub array surfaces as a NEGATIVE signal returncode here.
        if not H.check(proc.returncode == 0,
                       "child exited {0} wid={1} (crash/abort in an atexit "
                       "handler post-mn_fini -- freed hub array?) stderr={2!r}"
                       .format(proc.returncode, wid, err[-200:])):
            return
        if not H.check(proc.returncode is not None and proc.returncode >= 0,
                       "child crash-signal returncode {0} wid={1} at atexit "
                       "(SIGSEGV/SIGABRT over a torn-down hub?) stderr={2!r}"
                       .format(proc.returncode, wid, err[-200:])):
            return
        # 2) MAIN-EXIT before any atexit marker -- atexit really does fire AFTER
        #    run() returns / the scheduler is gone (pins the ordering claim).
        mi = out.find(b"MAIN-EXIT")
        if not H.check(mi >= 0, "child never returned from run() wid={0}: {1!r}"
                       .format(wid, out[:160])):
            return
        first_atexit = min((p for p in (out.find(m) for m in ORDER) if p >= 0),
                           default=-1)
        if not H.check(first_atexit < 0 or mi < first_atexit,
                       "atexit marker preceded MAIN-EXIT wid={0} -- handlers did "
                       "NOT run post-run() as assumed: {1!r}".format(wid,
                                                                     out[:200])):
            return
        # 3) all four markers present AND in reverse registration order.
        if not H.check(_ordered(out, ORDER),
                       "atexit markers missing or out of reverse-registration "
                       "order wid={0}: {1!r}".format(wid, out[:240])):
            return
        # 4) the post-fini fiber() took the no-scheduler path (didn't crash, and
        #    didn't run its body on a torn-down/freed hub).
        if not H.check(b"FALLBACK" in out and
                       b"FIBER-BODY-RAN-ON-DEAD-HUB" not in out,
                       "post-mn_fini runloom.fiber() did NOT take the no-"
                       "scheduler fallback wid={0} (body ran on a torn-down hub "
                       "or touched the freed hub array): {1!r}".format(
                           wid, out[:240])):
            return
        # 5) the blocking sleep handler ACTUALLY waited on the real OS.
        dt = _sleep_dt(out)
        if not H.check(dt >= SLEEP_FLOOR,
                       "post-mn_fini time.sleep fallback did not actually wait "
                       "wid={0}: dt={1} < {2} (no-op/instant return instead of a "
                       "real kernel sleep)".format(wid, dt, SLEEP_FLOOR)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0,
            "no child ran its atexit handlers cleanly post-mn_fini")
    H.log("clean_atexit_runs={0} exited={1}/{2}".format(
        H.total_ops(), H.exited, H.expected))
    H.require_no_lost("atexit-post-fini coverage")


if __name__ == "__main__":
    harness.main("p308_atexit_blocking_and_spawn", body, setup=setup, post=post,
                 default_funcs=100,
                 describe="child runs atexit handlers AFTER mn_fini (scheduler "
                          "gone): wall-timed time.sleep + Lock + socket blocking "
                          "IO fall back to real OS, runloom.fiber() takes the "
                          "no-scheduler path; exit 0, markers in reverse order, "
                          "FALLBACK proven, sleep actually waited")
