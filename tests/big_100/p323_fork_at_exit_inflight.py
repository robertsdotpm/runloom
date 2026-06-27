"""big_100 / 323 -- os.fork() from a goroutine AT the teardown instant with
resources IN FLIGHT (parked recv g + open listener + outstanding offload).

p111 forks while the scheduler is active MID-RUN: other goroutines are running,
the child reset_after_fork()s, writes a byte, and _exit()s -- a steady-state
fork test.  The UNTESTED boundary is fork() from a goroutine AT the *teardown /
drain instant*, with live resources outstanding.

os.fork() in an M:N runtime copies ONLY the calling OS thread, so the child
inherits a FROZEN scheduler: every other hub thread is GONE in the child, yet the
child's address space still holds parked-g state, a duplicated open listener +
recv-parked sockets' netpoll fds, AND -- the genuinely new stressor -- the
pid-keyed state of an offload (`runloom_c.blocking(...)`) that is STILL
OUTSTANDING at fork time.  The blockpool keys its in-flight work / wait state by
identity that the child now duplicates while its pool thread does NOT exist in the
child.  If the fork-child does anything COOPERATIVE before exec/_exit (touch the
frozen scheduler -> deadlock on an inherited hub lock / wait on the duplicated
offload that no pool thread will ever complete), OR the PARENT's post-fork
teardown trips over the now-duplicated netpoll fds / pid-keyed offload state, the
process hangs or crashes.

The child therefore does the absolute minimum in the fork-child: reset_after_fork
to drop the inherited hub threads / scheduler+blockpool locks, then a RAW
os._exit(7) -- no cooperative op, no offload wait, no scheduler touch (heeding
p111: the fork child must NOT touch the scheduler before exec/_exit).  The
fork-PARENT (a goroutine in the original process) reaps the fork-child with a RAW
os.waitpid (not the cooperative patched path -- waitpid here must be a real
syscall, not a netpoll park) and asserts WEXITSTATUS == 7, proving the child's
_exit was actually reaped (not just that nobody hung).  The root then RETURNS so
run()/mn_fini drains the still-parked recv goroutines, the open listener, and the
outstanding offload -- the teardown the fork raced.

ORACLE (subprocess driver -- fork misbehavior is contained to a CHILD process, so
a UAF/hang poisons a child, never the parent sweep):

  * outer-child returncode >= 0  (a teardown/offload-duplication UAF surfaces as a
    NEGATIVE crash signal -SIGSEGV/-SIGABRT; >= 0 means no crash);
  * outer-child returncode == 0  (both the fork reap AND the post-fork teardown of
    the in-flight resources completed cleanly);
  * DONE-MARKER present          (proves the child reached the teardown instant
    with the recv g's PARKED, the listener OPEN, and the offload OUTSTANDING --
    i.e. the hazard was actually set up, not skipped);
  * FORKCHILD-OK present         (proves the fork-child's reset+_exit(7) was
    actually REAPED with the right status -- not merely that the parent didn't
    hang);
  * MAIN-EXIT present            (proves run()/mn_fini then drained the in-flight
    resources after the fork -- the teardown the fork raced did NOT wedge);
  * a TimeoutExpired => H.fail   (the fork froze the scheduler and the join/drain
    never completed -- the duplicated offload/netpoll-fd teardown wedged).

post() additionally calls require_no_lost on the parent pool (no parent-side
worker was lost driving the children).

Stresses: os.fork() from a goroutine at the teardown/drain instant; child inherits
a frozen scheduler (hub threads gone) with parked-g + duplicated listener/recv
netpoll fds + an OUTSTANDING pid-keyed offload; reset_after_fork before any
cooperative op; raw waitpid reap; post-fork mn_fini drain of in-flight resources;
no SIGSEGV/SIGABRT, no hang.

Good TSan / deterministic-M:N-replay target: fork copying one OS thread out of N
while an offload's pid-keyed blockpool state is in flight is exactly the
duplicated-state hazard a race detector flags before it becomes an intermittent
teardown SEGV; the exit-instant interleave is what deterministic replay pins.
"""
import os
import subprocess

import harness
import procutil

# K recv-parked goroutines outstanding at the fork instant.  Small: each is a real
# socketpair half held open through teardown; the subject is the fork-at-exit
# boundary, not socket throughput.
RECV_PARKED = int(os.environ.get("P323_RECV", "8"))

# NOTE: the child body uses .format() {0}-style placeholders at RUNTIME (in its own
# print statements), so the src path is injected via a sentinel token + .replace()
# rather than CHILD.format(...), which would try to fill those runtime {0}s now.
CHILD = r'''
import sys, os, socket, time
sys.path.insert(0, __SRC_PATH__)
# Capture the RAW (un-patched) waitpid/_exit and the real time.sleep BEFORE
# monkey.patch().  After fork the child has no working scheduler until
# reset_after_fork(), and the fork-PARENT must reap via a REAL waitpid syscall --
# never a cooperative netpoll park on a torn-down scheduler.  os._exit is already a
# raw syscall; we alias it for symmetry.  REAL_SLEEP is the genuine OS sleep the
# outstanding offload runs on a pool thread (so the offload is truly in flight).
RAW_WAITPID = os.waitpid
RAW_EXIT = os._exit
REAL_SLEEP = time.sleep
import runloom
import runloom_c
import runloom.monkey
runloom.monkey.patch()                     # cooperative socket I/O on the hubs

K = int(sys.argv[1]) if len(sys.argv) > 1 else 8

CHILD_EXIT = 7                             # the fork-child's os._exit code

live = []                                  # every live socket (held open to teardown)
recv_done = [0]                            # recv g's that observed wake (diagnostic)


def slow_fstat(path):
    # Body of the OUTSTANDING offload: a genuinely blocking syscall on a pool
    # thread (runloom_c.blocking parks the calling fiber on the blockpool with
    # pid-keyed wait state).  A short real sleep keeps it IN FLIGHT across the
    # fork + DONE-MARKER instant so the fork duplicates the offload's wait state
    # while its pool thread does NOT exist in the child.
    REAL_SLEEP(path)
    return os.getpid()


def recv_parked(s):
    # Park forever in a cooperative recv on the socketpair half (peer never sends).
    # At the fork instant this g is PARKED on the fd's netpoll arm -- the
    # duplicated-fd state the child inherits frozen.  At teardown the harness's
    # close wakes it (EOF / ECANCELED) and it returns so mn_fini can join.
    try:
        d = s.recv(64)
        if not d:
            recv_done[0] += 1               # single-writer-ish diagnostic only
    except (OSError, ValueError):
        recv_done[0] += 1                   # closed-under-us at teardown -> woken


def offload_holder():
    # A goroutine whose ONLY job is to be parked on an outstanding offload at the
    # fork instant.  runloom.blocking parks here on the blockpool until slow_fstat
    # returns; we spawn it just before DONE-MARKER and never join it before the
    # fork, so the offload's pid-keyed state is live across fork().
    try:
        runloom.blocking(slow_fstat, 0.6)
    except Exception:
        pass                               # torn down at teardown -> done


def fork_at_exit():
    # The crux: from a GOROUTINE, at the teardown instant, os.fork().  The child
    # inherits a frozen scheduler (every other hub thread gone) plus the parked
    # recv g's, the open listener, and the OUTSTANDING offload's pid-keyed state.
    try:
        pid = os.fork()
    except OSError:
        # fork itself refused (rlimit) -- not the bug under test; let the root
        # return so teardown still drains and MAIN-EXIT marks the clean path.
        sys.stdout.write("FORK-REFUSED\n"); sys.stdout.flush()
        return
    if pid == 0:
        # FORK-CHILD.  Touch the scheduler as LITTLE as possible: FIRST drop the
        # inherited hub threads / scheduler + blockpool locks (a lock held by a
        # now-vanished hub thread, or the in-flight offload's wait state, would
        # otherwise wedge any cooperative op here), then RAW _exit -- NO
        # cooperative op, NO offload wait, NO scheduler touch (heeds p111).
        try:
            runloom_c.reset_after_fork()
        except Exception:
            pass                           # even if unavailable, still _exit so the
                                           # parent's WEXITSTATUS oracle is precise
        RAW_EXIT(CHILD_EXIT)
        RAW_EXIT(99)                        # unreachable
    # FORK-PARENT (a healthy goroutine in the original process).  Reap with a RAW
    # waitpid syscall -- never the cooperative path on a scheduler being torn down.
    try:
        _wpid, status = RAW_WAITPID(pid, 0)
    except OSError:
        sys.stdout.write("REAP-FAILED\n"); sys.stdout.flush()
        return
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == CHILD_EXIT:
        # The fork-child's reset_after_fork + _exit(7) was actually reaped with the
        # expected status: proof the child's exit happened, not just no-hang.
        sys.stdout.write("FORKCHILD-OK\n"); sys.stdout.flush()
    else:
        sys.stdout.write("FORKCHILD-BAD 0x{0:x}\n".format(status)); sys.stdout.flush()


def main():
    # Listener OPEN at teardown (its accept-arm fd is among the netpoll fds the
    # fork duplicates).  Bound to an ephemeral loopback port; nobody connects --
    # it just has to be open + (optionally) accept-parked across the fork instant.
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(64)
    srv.setblocking(False)
    live.append(srv)

    # K recv-parked goroutines (each on a socketpair half held open to teardown).
    for _ in range(K):
        a, b = socket.socketpair()
        live.append(a); live.append(b)
        runloom.fiber(recv_parked, a)       # parks in recv; peer b never sends

    # The OUTSTANDING offload: spawn the holder so an offload is in flight (its
    # pid-keyed blockpool wait state is live across the fork).
    runloom.fiber(offload_holder)

    runloom.sleep(0.05)                      # settle: recv g's parked, offload in flight
    # DONE-MARKER proves the hazard is fully set up: listener open, K recv g's
    # PARKED, offload OUTSTANDING -- right before we fork from a goroutine.
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()

    # Fork from a GOROUTINE at the teardown instant, reap the fork-child, then
    # RETURN so run()/mn_fini drains the still-in-flight resources (the teardown
    # the fork raced).  Run it on its OWN goroutine and join via a tiny settle so
    # the fork happens with the recv g's still parked + offload still outstanding.
    runloom.fiber(fork_at_exit)
    runloom.sleep(0.15)                      # let the fork+reap complete before drain

    # Close the live sockets so the parked recv g's wake (EOF/ECANCELED) and the
    # listener closes -- then fall off main(): run() joins the woken g's + the
    # offload completes/cancels, and mn_fini tears the (post-fork) scheduler down.
    for s in live:
        try:
            s.close()
        except OSError:
            pass
    # return -> the teardown the fork raced.  A duplicated-offload / netpoll-fd UAF
    # would SIGSEGV here before MAIN-EXIT; a wedged drain would hang (-> timeout).


runloom.run(4, main)
# Reached only if the fork raced the teardown cleanly: run() joined the recv g's,
# the outstanding offload drained, and mn_fini ran with no crash/wedge.
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_forkexit_"), "child.py")
    body_src = CHILD.replace("__SRC_PATH__", repr(src))
    with open(script, "w") as f:
        f.write(body_src)
    H.state = {"py": sys.executable, "script": script}


def worker(H, wid, rng, state):
    py = state["py"]
    script = state["script"]
    recv = str(RECV_PARKED)
    for _ in H.round_range():
        if not H.running():
            break
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([py, script, recv],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, env=env,
                                  running=H.running)
        except OSError:
            break                           # shutdown cancelled the spawn
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            # A timeout is the fork FREEZING the scheduler: the join/drain of the
            # in-flight resources (duplicated offload / netpoll fds) never
            # completed.  Only a bug while the harness is still running -- once
            # H.running() is False this child was merely caught mid-flight at the
            # deadline (benign over-scale drain), not a wedged join.
            if not H.running():
                break
            H.fail("child HUNG: fork-at-exit froze the scheduler join/drain wid={0} "
                   "(duplicated offload / netpoll-fd teardown wedged)".format(wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # ORACLE 1: no crash signal.  A teardown/offload-duplication UAF surfaces
        # as a NEGATIVE returncode (-SIGSEGV/-SIGABRT); >= 0 means no crash.
        if not H.check(proc.returncode >= 0,
                       "child CRASHED (signal {0}) at fork-from-goroutine-at-exit "
                       "wid={1} -- UAF on the duplicated offload/netpoll-fd state? "
                       "stderr={2!r}".format(
                           -proc.returncode, wid, err[-300:])):
            return
        # ORACLE 2: clean exit (fork reap AND post-fork teardown both completed).
        if not H.check(proc.returncode == 0,
                       "child exited {0} (nonzero) at fork-at-exit wid={1} "
                       "stderr={2!r}".format(proc.returncode, wid, err[-300:])):
            return
        # ORACLE 3: the hazard was actually set up (listener open, recv g's parked,
        # offload outstanding) right before the fork.
        if not H.check(b"DONE-MARKER" in out,
                       "child never reached the fork-at-teardown instant with "
                       "resources in flight wid={0}: {1!r}".format(wid, out[:160])):
            return
        # ORACLE 4 (the load-bearing one): the fork-child's reset+_exit(7) was
        # actually REAPED with the right status -- proves the fork's exec/_exit
        # happened and was reaped, not just that the parent didn't hang.
        if not H.check(b"FORKCHILD-OK" in out,
                       "fork-child was NOT reaped with exit 7 wid={0} (the fork "
                       "from a goroutine at the exit instant did not produce a "
                       "clean child reap): {1!r}".format(wid, out[:200])):
            return
        # ORACLE 5: run()/mn_fini then drained the in-flight resources after the
        # fork -- the teardown the fork raced did not wedge.
        if not H.check(b"MAIN-EXIT" in out,
                       "child reaped the fork-child but the post-fork teardown "
                       "never drained (join wedged on duplicated offload/netpoll "
                       "state) wid={0}: {1!r}".format(wid, out[:200])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0,
            "no child survived fork-from-a-goroutine-at-the-teardown-instant with "
            "resources in flight (every child crashed, hung, or never reaped its "
            "fork-child)")
    # No PARENT-side worker was lost driving the children.
    H.require_no_lost("p323 fork-at-exit driver")
    H.log("clean_fork_at_exit={0} recv_parked_per_child={1} exited={2}/{3}".format(
        H.total_ops(), RECV_PARKED, H.exited, H.expected))


if __name__ == "__main__":
    # Each round forks a REAL process inside a REAL subprocess child, so the scale
    # is intentionally tiny -- cap it (the 1M sweep's --funcs would otherwise spawn
    # ~100k+ child processes and wedge the box).  The subject is the fork-at-exit
    # boundary, not goroutine count.
    harness.main("p323_fork_at_exit_inflight", body, setup=setup, post=post,
                 default_funcs=40, max_funcs=40,
                 describe="child runloom forks from a goroutine AT the teardown "
                          "instant with a parked recv g + open listener + "
                          "OUTSTANDING offload in flight; fork-child "
                          "reset_after_fork + _exit(7) reaped via raw waitpid; "
                          "returncode>=0 + FORKCHILD-OK + MAIN-EXIT, no hang")
