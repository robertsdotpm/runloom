"""big_100 / 309 -- interpreter exit with live child procs + open pipes +
goroutines parked in pipe reads.

p135 covers live *sockets* at the exit instant; p134 covers socketpairs.  An
anonymous pipe to a real subprocess is a DISTINCT fd class -- no SO_LINGER, no
half-close handshake, and a live child still actively writing into the pipe's
buffer.  Nothing in the corpus sits, at the exit instant, with all three of:

  * N real CHILD PROCESSES still alive (a `python -u -c` dribbler that writes a
    byte + sleeps in a loop forever, so its stdout pipe stays netpoll-armed),
  * their stdout PIPES open and arm-registered on the hubs, and
  * N goroutines PARKED in a cooperative `proc.stdout.read(1)` on those fds.

Two teardown shapes are exercised per child (chosen by the parent worker):

  * "clean": flip a stop flag, CLOSE THE READ ENDS FIRST (which wakes every
    parked pipe-reader -> read() returns b"" / raises -> the reader g returns),
    THEN terminate()+wait() every child, then return from main so mn_run joins
    the woken readers.  This is the ordering under test: close-read-then-reap.
    A wrong order (reap first) can wedge -- a child blocked writing into a now-
    full pipe whose reader is gone never dies, stranding the join.
  * "abrupt": os._exit(0) on top of live children + open pipes + parked readers
    -- the interpreter never runs mn_fini / finalization.  Must terminate
    immediately with status 0, no crash from tearing the process down on top of
    live pipe netpoll registrations.

ORACLE (primary): the parent process's OWN fd count is sampled across iterations
(guarded behind fd_base >= 0, i.e. Linux /proc/self/fd).  A wrong close/reap
order that strands a reader, orphans a child, or leaks a pipe fd shows up as
monotonic parent fd GROWTH -- an auditor goroutine asserts the live fd count
stays bounded, and post() asserts the end-vs-base balance is bounded.

ORACLE (secondary): every child -- clean AND abrupt -- must exit with a
NON-NEGATIVE returncode (no -SIGSEGV/-SIGABRT from tearing down on top of live
pipe arms; clean and abrupt both exit 0) and must reach DONE-MARKER (it actually
stood up the children + parked readers) without hanging (TimeoutExpired -> fail:
the join wedged because a parked pipe-reader was stranded).

Invariant: per-child returncode == 0 + DONE-MARKER reached + no hang; parent fd
count bounded across all iterations (no orphaned child / leaked pipe).

Stresses: interpreter finalization with live anonymous pipes, abrupt os._exit on
top of live pipe netpoll registrations + live children, close-read-then-reap
teardown ordering, parked-pipe-reader wake at shutdown, parent fd-leak balance.

Good TSan / controlled-M:N-replay target: the close-read-wakes-parked-reader vs
mn_run-join ordering at the exit instant is a teardown race; a data-race or a
stranded parker is the first signal, before the parent fd oracle even fires.
"""
import os
import subprocess

import harness
import procutil

# A child of the CHILD: writes one byte then sleeps, forever and unbuffered, so
# its stdout pipe stays netpoll-armed (a child that finishes early leaves nothing
# armed at the exit instant).  -u = unbuffered so the byte actually hits the pipe.
DRIBBLER = ("import sys,time\n"
            "while True:\n"
            "    sys.stdout.write('x'); sys.stdout.flush()\n"
            "    time.sleep(0.01)\n")

CHILD = r'''
import sys, os, subprocess, threading
sys.path.insert(0, {src!r})
import runloom
import runloom_c
import runloom.monkey
runloom.monkey.patch()                     # cooperative pipe read() on the hubs

# Wake a goroutine parked in runloom_c.wait_fd(fd, READ): the SAME primitive the
# cooperative socket close uses (cancel-BEFORE-free, so the parker raises
# OSError(ECANCELED)).  os.close(fd) alone only clears the arm bit and does NOT
# wake a parked pipe reader -- so the clean teardown MUST cancel first or the
# join wedges on a stranded reader.  See src/runloom/monkey/sockets.py.
_cancel_fd = getattr(runloom_c, "netpoll_cancel_fd", None)

MODE = sys.argv[1] if len(sys.argv) > 1 else "clean"
KIDS = int(sys.argv[2]) if len(sys.argv) > 2 else 6
PY   = sys.argv[3] if len(sys.argv) > 3 else sys.executable

DRIBBLER = {dribbler!r}

stop = [False]
lock = threading.Lock()
procs = []                                  # live child Popen objects
rfds = []                                   # raw stdout pipe fds (one per child)
reads = [0]                                 # bytes a reader actually drained

def reader(fd):
    """Park in a COOPERATIVE pipe read on the raw fd until the read end is
    closed (os.read -> b"") or torn out from under us.  The patched os.read
    sets the fd non-blocking and parks the goroutine on the fd's netpoll arm
    (runloom_c.wait_fd), so the hub thread is NOT OS-blocked -- this is the
    'parked pipe-reader' the exit-instant oracle is about.  (proc.stdout.read,
    a C BufferedReader, would instead OS-block the hub via a raw read syscall;
    that starves the scheduler.  os.read on the raw fd is the cooperative path.)"""
    try:
        while not stop[0]:
            d = os.read(fd, 1)              # parks on the pipe fd's netpoll arm
            if not d:
                break                       # read end closed / EOF -> wake-return
            with lock:
                reads[0] += 1
    except (OSError, ValueError):
        pass                                # closed-under-us / torn read -> done

def main():
    # Stand up KIDS real children, each dribbling into its stdout pipe, with one
    # goroutine parked in a cooperative read() per child.
    for _ in range(KIDS):
        try:
            # Build the Popen OFF the goroutine (runloom.blocking runs it on a
            # pool thread where _in_goroutine() is False) to dodge the nested-
            # offload deadlock procutil.py documents as BUG #4 (Popen.__init__
            # -> _pyio FileIO -> offloaded os.fstat, whose wait can lose its
            # wakeup at high concurrent-spawn rates).  KIDS is small here.
            p = runloom.blocking(subprocess.Popen,
                                 [PY, "-u", "-c", DRIBBLER],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        except OSError:
            break
        # DETACH the BufferedReader so it neither buffers ahead of us nor owns
        # the fd: we read + close the raw pipe fd directly so the reader g parks
        # on the netpoll arm and the teardown order is exactly the one we test.
        fd = p.stdout.fileno()
        try:
            p.stdout.detach()              # fd is now ours; reader does os.read
        except (OSError, ValueError):
            pass
        with lock:
            procs.append(p)
            rfds.append(fd)
        runloom.fiber(reader, fd)
    runloom.sleep(0.08)                     # let dribblers write + readers park
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()

    with lock:
        live = list(procs)
        fds = list(rfds)

    if MODE == "abrupt":
        # Children alive, pipes open + netpoll-armed, readers parked on them
        # RIGHT NOW.  Terminate on top of all of it -- no mn_fini, no join, no
        # pipe close.  (Kill the dribblers so they're not stranded as orphans;
        # the test PARENT's fd oracle catches any fd WE leak in the parent, not
        # in this short-lived child.)
        for p in live:
            try: p.kill()
            except OSError: pass
        sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
        os._exit(0)

    # clean: ORDER UNDER TEST -- wake every parked reader FIRST (cancel its
    # wait_fd, then close its read end), THEN terminate + reap the children.
    # Reaping first could block a dribbler writing into a now-full pipe whose
    # reader is gone, wedging the join for a benign-ordering reason.
    stop[0] = True
    for fd in fds:                          # 1) wake parked readers, close reads
        if _cancel_fd is not None:
            try: _cancel_fd(fd)             # raise ECANCELED in the parked reader
            except (OSError, ValueError): pass
        try: os.close(fd)                   # then free fd N (arm bit cleared)
        except OSError: pass
    for p in live:                          # 2) now stop + reap the children
        try: p.kill()
        except OSError: pass
    for p in live:
        try: p.wait()
        except OSError: pass
    # Fall through: mn_run joins the woken (now-returned) reader goroutines.

runloom.run(4, main)
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_exitpipe_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src, dribbler=DRIBBLER))
    # KIDS: real grandchildren per child iteration.  Small -- each is a real
    # process holding a pipe; the point is the EXIT INSTANT, not throughput.
    H.state = {"py": sys.executable, "script": script, "kids": 6}
    # fd_ceiling tracks the high-water parent fd count for the auditor's log.
    H.fd_ceiling = 0


def worker(H, wid, rng, state):
    py = state["py"]
    script = state["script"]
    kids = state["kids"]
    for _ in H.round_range():
        if not H.running():
            break
        mode = "abrupt" if (rng.random() < 0.5) else "clean"
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen(
                [py, script, mode, str(kids), py],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=env, running=H.running)
        except OSError:
            break                           # shutdown cancelled the spawn
        try:
            out, err = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            # A timeout is a STRANDED-READER bug ONLY while the harness is still
            # running.  Once H.running() is False the run has hit its deadline and
            # this child was merely caught mid-flight at over-scale -- a benign
            # drain timeout, NOT a wedged join.  Don't false-positive on scale.
            if not H.running():
                break
            H.fail("child HUNG at exit-with-live-pipes ({0}) wid={1} -- a parked "
                   "pipe-reader was stranded (close/reap order wedged the join)"
                   .format(mode, wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # No crash signal: a UAF/SIGSEGV from tearing down on top of live pipe
        # netpoll arms surfaces as a NEGATIVE returncode (-SIGSEGV/-SIGABRT).
        # Both clean and abrupt must exit exactly 0.
        if not H.check(proc.returncode == 0,
                       "child ({0}) exited {1} wid={2} (crash at exit with live "
                       "pipes + parked readers?) stderr={3!r}".format(
                           mode, proc.returncode, wid, err[-200:])):
            return
        if not H.check(b"DONE-MARKER" in out,
                       "child ({0}) never stood up live pipes/readers wid={1}: "
                       "{2!r}".format(mode, wid, out[:120])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    # PRIMARY ORACLE: the parent's OWN fd count across iterations.  A wrong
    # close/reap order that strands a reader, orphans a grandchild, or leaks a
    # pipe shows as monotonic parent fd growth.  Guard behind fd_base >= 0 so it
    # silently no-ops where /proc/self/fd is unavailable (count_fds() -> -1).
    def auditor():
        base = harness.count_fds()
        if base < 0:
            H.log("fd oracle skipped (no /proc/self/fd on this platform)")
            return
        # Each in-flight child briefly holds up to ~2 pipe fds in the parent
        # (its stdout+stderr) while communicate() drains; bound generously by
        # the concurrent-spawn ceiling, not by H.funcs (children are reaped per
        # iteration, so the parent fd count must NOT grow with funcs).
        ceiling = base + procutil.MAX_CONCURRENT * 4 + 256
        while H.running():
            fds = harness.count_fds()
            if fds < 0:
                break
            H.fd_ceiling = max(H.fd_ceiling, fds)
            H.check(fds < ceiling,
                    "parent fd leak: {0} open (base {1}, ceiling {2}) -- an "
                    "orphaned/unreaped grandchild or an unclosed pipe at child "
                    "exit".format(fds, base, ceiling))
            H.sleep(0.5)
        H.log("parent fd_ceiling={0} base={1}".format(H.fd_ceiling, base))

    H.fiber(auditor)


def post(H):
    H.log("clean_or_abrupt_children_ok={0} exited={1}/{2} fd_base={3} fd_end={4}"
          .format(H.total_ops(), H.exited, H.expected, H.fd_base, H.fd_end))
    H.check(H.total_ops() > 0,
            "no child exited cleanly with live pipes + parked readers")
    # PRIMARY end-state leak balance: the parent fd count must return near its
    # starting point (every grandchild reaped, every pipe closed).  Guarded so
    # it no-ops on non-Linux (fd_base/fd_end == -1 there).
    if H.fd_base >= 0 and H.fd_end >= 0:
        H.check(H.fd_end < H.fd_base + procutil.MAX_CONCURRENT * 4 + 256,
                "parent fd leak across run: end {0} vs base {1} (orphaned "
                "grandchild / leaked pipe at child exit)".format(
                    H.fd_end, H.fd_base))


if __name__ == "__main__":
    # SUBPROCESS program: each worker iteration forks a real child runloom
    # process which itself forks KIDS real grandchildren -- so the real process
    # count is ~funcs*KIDS at peak.  procutil's MAX_CONCURRENT semaphore bounds
    # concurrent child spawns, but a hard funcs ceiling keeps the soak driver
    # from driving this at 1M (which would fork-bomb the box, not test a bug).
    harness.main("p309_exit_pipes_parked_readers", body, setup=setup, post=post,
                 default_funcs=100, max_funcs=300,
                 describe="child runloom exits (clean close-read-then-reap AND "
                          "abrupt os._exit) with live child procs + open pipes + "
                          "parked pipe-readers; returncode 0, no hang, parent "
                          "fd count bounded (leak oracle)")
