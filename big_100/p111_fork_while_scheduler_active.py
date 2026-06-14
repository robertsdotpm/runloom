"""big_100 / 111 -- os.fork() while the M:N scheduler is active.

A goroutine calls os.fork() while OTHER goroutines are running across the hubs.
In the CHILD: immediately runloom_c.reset_after_fork() (drop the inherited hub
threads / scheduler locks that only exist in the parent's address space), write
a known byte to a pipe, and os._exit(EXPECTED).  In the PARENT: os.waitpid the
child, verify the exit status is EXPECTED and read back the byte.

The hazard is the classic fork-in-a-threaded-runtime deadlock: the child
inherits a COPY of every lock the M:N runtime held at fork time, but only the
forking thread survives in the child, so a lock held by a now-vanished hub
thread is held forever -- any child code that touches it deadlocks.
reset_after_fork() must re-init the scheduler state so the child can run the
minimal "write byte + exit" without wedging.

Funcs are kept LOW: each round forks a real process.

Stresses: os.fork under M:N, reset_after_fork, child lock/hub-thread inherit,
fork-deadlock avoidance, child exit-status + pipe-byte conservation.
"""
import os

import harness
import runloom
import runloom_c

# Capture the ORIGINAL blocking os.write/os.read BEFORE the harness calls
# monkey.patch().  The child must use these raw syscalls: after fork the child
# has no working scheduler until reset_after_fork(), and even after it the child
# is a bare process with one thread -- the cooperative os.write/os.read (which
# park a goroutine on the netpoll) must NOT be used there.  (The parent is a
# healthy goroutine and uses the patched cooperative versions for its read.)
RAW_WRITE = os.write
RAW_READ = os.read

EXPECTED = 41          # child's os._exit code
BYTE = b"K"            # the byte the child writes to the pipe


def child_main(wfd):
    """Runs ONLY in the forked child.  Must touch the scheduler as little as
    possible until reset_after_fork() has re-initialized it."""
    try:
        # FIRST thing: drop the parent's inherited hub threads / sched locks.
        runloom_c.reset_after_fork()
    except Exception:
        # Even if reset is unavailable, still try to produce our byte + exit so
        # the parent's invariant can detect the failure precisely.
        pass
    try:
        RAW_WRITE(wfd, BYTE)       # raw blocking write -- no cooperative park
    except OSError:
        os._exit(7)        # distinct failure code: couldn't write the byte
    os._exit(EXPECTED)


def worker(H, wid, rng, state):
    for _ in H.round_range():
        rfd, wfd = os.pipe()
        try:
            pid = os.fork()
        except OSError:
            os.close(rfd)
            os.close(wfd)
            break
        if pid == 0:
            # CHILD.  Close the read end, run the minimal child, never return.
            try:
                os.close(rfd)
            except OSError:
                pass
            child_main(wfd)
            os._exit(99)       # unreachable; child_main always _exit()s
        # PARENT.
        os.close(wfd)          # parent only reads
        try:
            data = os.read(rfd, 1)
        except OSError:
            data = b""
        finally:
            try:
                os.close(rfd)
            except OSError:
                pass
        try:
            _wpid, status = os.waitpid(pid, 0)
        except OSError:
            if not H.running():
                break
            H.fail("waitpid failed for child wid={0}".format(wid))
            return
        exited_ok = os.WIFEXITED(status) and os.WEXITSTATUS(status) == EXPECTED
        if not H.check(exited_ok,
                       "child wid={0} bad status 0x{1:x} (want exit {2})".format(
                           wid, status, EXPECTED)):
            return
        if not H.check(data == BYTE,
                       "child wid={0} pipe byte {1!r}!={2!r}".format(
                           wid, data, BYTE)):
            return
        H.forks[wid] += 1      # single writer per slot (race-free)
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.forks = [0] * max(1, H.funcs)
    H.state = {}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total = sum(H.forks)
    H.check(total > 0, "no fork+child round ever completed (child may have "
                       "deadlocked on an inherited hub lock)")
    H.log("forks_completed={0} exited={1}/{2}".format(
        total, H.exited, H.expected))


if __name__ == "__main__":
    harness.main("p111_fork_while_scheduler_active", body, setup=setup,
                 post=post, default_funcs=60,
                 describe="os.fork() from a goroutine under M:N; child does "
                          "reset_after_fork + write byte + _exit; parent reaps "
                          "and verifies status + byte")
