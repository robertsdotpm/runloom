"""Teardown x park-site matrix (item 11 + item 4's teardown axis).

The shutdown-fork-teardown class (12 appendix bugs) has one shape: startup and
steady state get all the coverage, but fini/close/fork must WAKE OR CANCEL every
parked waiter -- epoll/kqueue emit no event for a closed fd, a chan recv blocks
until close, and a fork-child inherits parked fibers on dead hubs.  A teardown
that forgets one waiter strands it forever.

This sweeps each park primitive x each teardown vector and asserts the waiter is
released and the runtime exits cleanly (a strand shows as a timeout).  Each cell
runs in its own subprocess so one strand can't wedge the file.

House style: %/.format, prints kept.
"""
import os
import subprocess
import sys
import textwrap

import pytest

PY = sys.executable
ENV = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")


def run_body(body, timeout=30):
    script = ("import runloom_c as rc, runloom, socket, sys, os\n"
              + textwrap.dedent(body))
    return subprocess.run([PY, "-c", script], env=ENV, capture_output=True,
                          timeout=timeout)


def expect(body, sentinel="OK", timeout=30):
    try:
        p = run_body(body, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail("STRAND: a parked waiter was not released by teardown (hang)")
    assert sentinel.encode() in p.stdout, (p.stdout[-800:], p.stderr[-800:])


# ---- teardown vector: cancel_all_parked() wakes netpoll waiters -------------

def test_cancel_all_parked_wakes_every_netpoll_waiter():
    # N fibers parked on sockets; cancel_all_parked must wake EVERY one with
    # ECANCELED, and run() must then exit.  A forgotten waiter -> timeout.
    expect("""
        import runloom.monkey; runloom.monkey.patch()
        N = 12; woke = [0]
        def body():
            socks = [socket.socketpair() for _ in range(N)]
            for rd, wr in socks:
                rd.setblocking(True)
                def parker(rd=rd):
                    try: rd.recv(4)
                    except OSError: pass
                    woke[0] += 1
                rc.fiber(parker)
            for _ in range(20): rc.sched_yield()
            rc.cancel_all_parked()
            for _ in range(200):
                if woke[0] == N: break
                rc.sched_yield()
        rc.fiber(body); rc.run()
        print("OK" if woke[0] == N else "FAIL woke=%d/%d" % (woke[0], N))
    """)


# ---- teardown vector: chan close wakes every recv waiter --------------------

def test_chan_close_wakes_every_recv_waiter():
    expect("""
        N = 12; woke = [0]
        def body():
            ch = rc.Chan(0)
            def parker():
                v, ok = ch.recv()
                if not ok: woke[0] += 1
            for _ in range(N): rc.fiber(parker)
            for _ in range(20): rc.sched_yield()
            ch.close()
            for _ in range(200):
                if woke[0] == N: break
                rc.sched_yield()
        rc.fiber(body); rc.run()
        print("OK" if woke[0] == N else "FAIL woke=%d/%d" % (woke[0], N))
    """)


# ---- teardown vector: fork-child is usable when the PARENT has a parked fiber -

def test_fork_child_usable_when_parent_has_parked_fiber():
    # The supported fork case is a fork from the MAIN thread (not from inside a
    # running scheduler) -> the child's at-fork handler resets the runtime.  Here
    # the parent's single-thread run() has already parked a fiber on a socket and
    # returned control (via a self-send that lets run() finish), leaving netpoll
    # state initialised; the child then forks and must run a fresh scheduler to
    # completion rather than inherit the parent's netpoll fd / arm cache and hang.
    expect("""
        import runloom.monkey; runloom.monkey.patch()
        # 1) run a workload in the parent that exercises + tears down netpoll
        def warm():
            rd, wr = socket.socketpair(); rd.setblocking(True)
            got = {}
            def parker():
                wr.sendall(b'x'); got['v'] = rd.recv(1)
            rc.fiber(parker)
            while 'v' not in got: rc.sched_yield()
        rc.fiber(warm); rc.run()
        # 2) fork from the main thread; child runs a fresh scheduler
        pid = os.fork()
        if pid == 0:
            ran = [0]
            def child_work():
                for _ in range(50): rc.sched_yield()
                ran[0] = 1
            rc.fiber(child_work); rc.run()
            os._exit(0 if ran[0] == 1 else 7)
        _, st = os.waitpid(pid, 0)
        print("OK" if os.waitstatus_to_exitcode(st) == 0 else
              "FAIL child_rc=%d" % os.waitstatus_to_exitcode(st))
    """)


# ---- teardown vector: mn (multi-hub) run exits after waking parked fibers ----

def test_mn_run_exits_after_parked_fibers_woken():
    # Under M:N, fibers park on a chan across hubs; a producer wakes them, and
    # runloom.run(N) must return (not strand a hub on a parked-but-woken fiber).
    expect("""
        N = 10; done = [0]
        def body():
            ch = rc.Chan(0)
            def consumer():
                v, ok = ch.recv()
                done[0] += 1
            for _ in range(N): rc.mn_fiber(consumer)
            def producer():
                for _ in range(N): ch.send(1)
            rc.mn_fiber(producer)
            while done[0] < N: rc.sched_sleep(0.003)
        runloom.run(4, main_fn=body)
        print("OK" if done[0] == N else "FAIL done=%d/%d" % (done[0], N))
    """)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
