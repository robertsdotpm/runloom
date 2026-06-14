"""M:N deadlock census (Go's checkdead analogue).

runloom_mn_run() polls runloom_mn_pending_global and returns at 0.  If the count
stays > 0 while the whole M:N system is quiescent with no wake source -- every
hub idle, no runnable/stealable work, no sleeper/timer, nothing in flight on
netpoll/blockpool/io_uring/a foreign park -- the remaining fibers are blocked on
a channel/lock/await that nothing can ever wake.  Without the census mn_run
spins its 1 ms poll forever: a silent hang.  With it, mn_run surfaces a
diagnostic (warn, the default) or raises (RUNLOOM_DEADLOCK=raise /
set_deadlock_mode(2)).

Just as important: it must NOT false-fire while a legitimate wake source exists
(a pending timer/sleeper, parked netpoll I/O, or simply busy hubs).  These
negative cases run in RAISE mode too, so a false positive becomes a test
failure rather than a silent misclassification.
"""
import os
# Short quiescent budget so the positive cases resolve in tens of ms rather than
# the 200 ms default.  Read once by the C census on the first mn_run, so it must
# be set before any run().
os.environ.setdefault("RUNLOOM_DEADLOCK_MS", "40")

import pytest
import runloom
import runloom_c


def _with_mode(mode, fn):
    old = runloom_c.get_deadlock_mode()
    runloom_c.set_deadlock_mode(mode)
    try:
        return fn()
    finally:
        runloom_c.set_deadlock_mode(old)


# ---- positive: real deadlocks are detected (raise instead of hang) ----

def test_recv_with_no_sender_is_detected():
    def body():
        def main():
            runloom_c.Chan(0).recv()          # unbuffered, nobody will ever send
        with pytest.raises(RuntimeError):
            runloom.run(2, main)
    _with_mode(2, body)


def test_cyclic_two_fiber_deadlock_is_detected():
    # A waits on chA; B waits on chB; each would unblock the other only after
    # being unblocked first -> a cycle with no entry point.
    def body():
        def main():
            chA = runloom_c.Chan(0)
            chB = runloom_c.Chan(0)

            def fiber_a():
                chA.recv()
                chB.send(1)

            def fiber_b():
                chB.recv()
                chA.send(1)

            runloom.go(fiber_a)
            runloom.go(fiber_b)
        with pytest.raises(RuntimeError):
            runloom.run(2, main)
    _with_mode(2, body)


# ---- negative: legitimate blocking must NOT be flagged ----

def test_sleeper_is_not_a_false_deadlock():
    # A fiber sleeping past the quiescent budget keeps a timer pending; the
    # census must see wakeable work and not fire, even in raise mode.
    def body():
        done = []

        def main():
            runloom_c.sched_sleep(0.15)       # > RUNLOOM_DEADLOCK_MS
            done.append(1)

        runloom.run(2, main)                  # must NOT raise
        assert done == [1]
    _with_mode(2, body)


def test_channel_handoff_is_not_a_false_deadlock():
    # Producer sleeps, then sends; consumer waits on recv.  While the consumer
    # is parked the producer's timer keeps the system non-quiescent -> no fire.
    def body():
        got = []

        def main():
            ch = runloom_c.Chan(0)

            def producer():
                runloom_c.sched_sleep(0.12)
                ch.send(42)

            def consumer():
                val = ch.recv()               # Go-style (value, ok) tuple
                got.append(val[0] if isinstance(val, tuple) else val)

            runloom.go(producer)
            runloom.go(consumer)
        runloom.run(2, main)                  # must NOT raise
        assert got == [42]
    _with_mode(2, body)


def test_busy_workload_no_false_fire():
    # Many compute fibers running to completion -- hubs are busy, never
    # quiescent-with-live-work.
    def body():
        out = bytearray(64)

        def main():
            from runloom.sync import WaitGroup
            wg = WaitGroup()
            wg.add(64)

            def work(i):
                s = 0
                for k in range(3000):
                    s += k * i
                out[i] = s & 0xFF
                wg.done()

            for i in range(64):
                runloom.go(work, i)
            wg.wait()

        runloom.run(4, main)                  # must NOT raise
    _with_mode(2, body)


def test_ping_pong_churn_no_false_fire():
    # Directly stresses the census's false-positive race: two fibers bounce a
    # token over unbuffered channels for many rounds, so at every instant one
    # fiber is running and the other is parked at a rendezvous -- the system is
    # never quiescent.  The run lasts well past RUNLOOM_DEADLOCK_MS, so a census
    # that mis-sampled a transient all-parked window would raise here.
    def body():
        N = 50000
        result = []

        def main():
            up = runloom_c.Chan(0)
            down = runloom_c.Chan(0)

            def pinger():
                for _ in range(N):
                    up.send(1)
                    down.recv()
                result.append("ping-done")

            def ponger():
                for _ in range(N):
                    up.recv()
                    down.send(1)

            runloom.go(pinger)
            runloom.go(ponger)

        runloom.run(2, main)                  # must NOT raise across the churn
        assert result == ["ping-done"]
    _with_mode(2, body)
