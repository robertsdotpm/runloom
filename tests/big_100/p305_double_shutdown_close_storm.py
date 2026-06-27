"""big_100 / 305 -- double / re-entrant close+shutdown STORM racing the join.

Shutdown is usually assumed to happen exactly ONCE.  Real failure modes re-enter
it: a SECOND close-storm (re-closing already-closed fds) and a `shutdown(SHUT_
RDWR)` on every fd race the in-progress join, so a wake can be delivered to a
parker the FIRST close already woke-and-freed.  That is precisely the runtime
invariant from CLAUDE.md:

    A freed `runloom_g` struct never returns to the OS.  `slab_free` retains it
    (refcount 0, magic DEAD); a stale dup-wake reaches `hub_submit`, which reads
    `g->refcount` via `try_incref` -- only sound while the struct is a valid g.
    Freeing -> garbage refcount -> SIGSEGV (arm64).

A double close()+shutdown() of the same fd is the most direct big_100-level
generator of that stale-dup-wake-on-a-DEAD-slot: the first wave wakes-and-frees
the recv-parked goroutine; the second wave, fired BEFORE those woken g's have
been joined, must NOT enqueue a wake against the now-DEAD parker.

A CHILD runloom program builds many socketpairs, each with a recv-parked
goroutine, plus genuine in-flight bytes (a peer goroutine sends a chunk, then
parks in its own recv) so the parked recv has a REAL completion to race the
second wake against.  After printing DONE-MARKER it performs two back-to-back
wind-downs WITHOUT settling between them:

  * wave 1: close() every socket (wakes every parked recv with EBADF/empty).
  * wave 2: immediately close() them ALL again + shutdown(SHUT_RDWR) on each
    (every fd is already closed/woken -> EBADF, swallowed) -- fired before the
    wave-1-woken goroutines have been joined by run().

then returns; run() joins the woken goroutines and the child prints MAIN-EXIT.
High child count + many sockets per child amplifies the race window.

ORACLE (subprocess, per-pid core attribution via run_all): the parent asserts
the child returncode is >= 0 (NOT a crash signal -- a stale-wake UAF surfaces as
-SIGSEGV / -SIGABRT == returncode < 0) AND that both DONE-MARKER and MAIN-EXIT
are present (proving the child reached the close storm and the join then drained,
not a hang).  A timeout => the join wedged on a lost/garbage wake -> H.fail.

This is a regression SENTINEL for a fixed-but-fragile invariant (it has a CBMC
guard, `tools/verify/cbmc/sched_qref_cbmc.c`), so a single green run is NOT
evidence of absence -- it wants MANY child iterations.  Excellent TSan / M:N
controlled-replay target: the refcount race on the freed g is reported before it
becomes an intermittent arm64 SEGV.

Stresses: re-entrant/idempotent teardown, double close + shutdown(SHUT_RDWR),
stale dup-wake on a DEAD/freed parker at the join boundary, try_incref on a
retained g, no SIGSEGV/SIGABRT/hang at shutdown.
"""
import os
import subprocess

import harness
import procutil

# Each child stands up SOCKS_PER_CHILD socketpairs, each with a recv-parked
# goroutine + a peer that sends one chunk then parks in its own recv.  Tuned so
# the child settles fast (DONE-MARKER) yet has a wide parked population for the
# double wave to race; the parent passes it as argv so a sweep can amplify.
SOCKS_PER_CHILD = 200

CHILD = r'''
import sys, os, socket, threading
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                     # cooperative socket I/O on the hubs

K = int(sys.argv[1]) if len(sys.argv) > 1 else 200

stop = [False]
lock = threading.Lock()
live = []                                  # every live socket (both ends of each pair)

def track(s):
    with lock:
        live.append(s)
    return s

def recv_parked(s):
    # Park in recv; the very first recv completes against the peer's in-flight
    # chunk, then we loop and park again with NOTHING in flight -- so at the
    # close storm this goroutine is parked on an empty fd with a real pending
    # netpoll registration, exactly the slot the second wave can stale-wake.
    try:
        while not stop[0]:
            d = s.recv(64)
            if not d:
                break                      # peer closed / shutdown -> woken, return
    except OSError:
        pass                               # close-during-recv: woken, return

def peer(s):
    # One in-flight chunk so the matching recv has a genuine completion to race
    # the second-wave wake against, then park in our own recv too (doubling the
    # parked population the storm must wake exactly once).
    try:
        s.sendall(b"x" * 16)
        while not stop[0]:
            d = s.recv(64)
            if not d:
                break
    except OSError:
        pass

def main():
    for _ in range(K):
        a, b = socket.socketpair()
        track(a); track(b)
        runloom.fiber(recv_parked, a)
        runloom.fiber(peer, b)
    runloom.sleep(0.05)                     # let every pair settle: parked + 1 chunk in flight
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()

    # Re-entrant close STORM racing the join: signal stop, then fire TWO waves
    # back-to-back with NO settle in between, so wave 2 hits parkers wave 1 has
    # woken-but-not-yet-joined.  Each socket is closed twice and shutdown once;
    # every error in the second wave (EBADF on an already-closed fd) is expected
    # and swallowed -- the point is to provoke a stale dup-wake on a DEAD slot,
    # not to do clean teardown.
    stop[0] = True
    with lock:
        socks = list(live)
    # wave 1: close all -> wakes every parked recv.
    for s in socks:
        try: s.close()
        except OSError: pass
    # wave 2 (immediate, no sleep): close again + shutdown(SHUT_RDWR) on every
    # already-closed fd, before run()'s join has drained the wave-1-woken g's.
    for s in socks:
        try: s.close()
        except OSError: pass
        try: s.shutdown(socket.SHUT_RDWR)
        except OSError: pass
    # return -> run() joins the woken goroutines; a stale dup-wake on a freed g
    # would SIGSEGV here, before MAIN-EXIT prints.

runloom.run(4, main)
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_closestorm_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src))
    H.state = {"py": sys.executable, "script": script}


def worker(H, wid, rng, state):
    socks = str(SOCKS_PER_CHILD)
    for _ in H.round_range():
        if not H.running():
            break
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"], socks],
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
            H.fail("child HUNG in double close+shutdown storm (join wedged on a "
                   "lost/garbage wake) wid={0}".format(wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # The stale-dup-wake UAF surfaces as a CRASH SIGNAL: subprocess reports a
        # signal-killed child as returncode < 0 (e.g. -11 SIGSEGV, -6 SIGABRT).
        # A clean exit (the woken goroutines joined and the double wave was
        # idempotent) is returncode == 0.  Anything < 0 is the targeted bug.
        if not H.check(proc.returncode >= 0,
                       "child CRASHED (signal {0}) in double close+shutdown "
                       "storm wid={1} -- stale dup-wake on a freed/DEAD parker? "
                       "stderr={2!r}".format(
                           -proc.returncode, wid, err[-300:])):
            return
        if not H.check(proc.returncode == 0,
                       "child exited {0} (nonzero) in close storm wid={1} "
                       "stderr={2!r}".format(proc.returncode, wid, err[-300:])):
            return
        # DONE-MARKER proves it reached the close storm; MAIN-EXIT proves run()'s
        # join drained the woken goroutines after the double wave (no wedge).
        if not H.check(b"DONE-MARKER" in out,
                       "child never reached the close storm wid={0}: {1!r}"
                       .format(wid, out[:160])):
            return
        if not H.check(b"MAIN-EXIT" in out,
                       "child reached close storm but join never returned "
                       "wid={0}: {1!r}".format(wid, out[:160])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0,
            "no child survived the double close+shutdown storm")
    H.log("clean_storms={0} socks_per_child={1} exited={2}/{3}".format(
        H.total_ops(), SOCKS_PER_CHILD, H.exited, H.expected))


if __name__ == "__main__":
    harness.main("p305_double_shutdown_close_storm", body, setup=setup, post=post,
                 default_funcs=100,
                 describe="child runloom fires a double/re-entrant close+"
                          "shutdown(SHUT_RDWR) storm racing run()'s join over "
                          "many recv-parked goroutines with bytes in flight; "
                          "returncode>=0 (no -SIGSEGV/-SIGABRT) + no hang")
