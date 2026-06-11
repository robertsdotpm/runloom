"""Regression: a STALE netpoll pending_wake stash must not produce a spurious
wait_fd return.

epoll is LEVEL-triggered, so an undrained byte re-fires the pump while no parker
is linked; that 2nd fire stashes in the process-global pending_wake bitmap.  The
parker the 1st fire woke then DRAINS the byte after wait_fd returns -- and on fd
REUSE the next wait_fd would consume the now-stale stash and return early with no
byte present (a spurious wake).  runloom_fd_pending_wake_consume now ground-truths
every claimed stash against the kernel's ACTUAL readiness (a non-blocking poll,
never a read), so a stale stash is discarded and the goroutine parks correctly.

Both scenarios reproduced reliably before the fix (reuse: 20/20 no-write cycles
returned early; high-fan-in Event: a growing number of waiters woke False) and go
to exactly 0 after.  Driven under M:N run(8) on purpose -- the bug needs the
cross-hub pump timing that the single-thread drain loop does not exercise.
"""
import os
import time

import runloom
import runloom_c

READ = 1


def test_fd_reuse_no_spurious_wait_fd_return():
    """Alternating wake / no-wake cycles that REUSE one pipe fd: a no-write
    cycle must block to its deadline, never return early off a stale stash."""
    box = {"spurious": 0, "cycles": 0}

    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)

        def cycle(do_write):
            t0 = time.monotonic()
            runloom_c.wait_fd(r, READ, 500)        # 0.5s deadline
            dt = time.monotonic() - t0
            try:
                os.read(r, 64)                     # drain (the byte, if any)
            except OSError:
                pass
            if not do_write:
                box["cycles"] += 1
                if dt < 0.4:                       # no byte written, woke early
                    box["spurious"] += 1

        for i in range(40):
            do_write = (i % 2 == 0)
            done = [False]

            def run(do_write=do_write, done=done):
                cycle(do_write)
                done[0] = True

            runloom.go(run)
            runloom.sleep(0.05)
            if do_write:
                os.write(w, b"\x01")
            while not done[0]:                     # serialize cycles on the one fd
                runloom.sleep(0.01)

        os.close(r)
        os.close(w)

    runloom.run(8, main)
    assert box["cycles"] == 20, box
    assert box["spurious"] == 0, box


def test_high_fanin_event_no_spurious_false():
    """500 goroutines wait on one monkey-patched Event; set() must wake them all
    True -- none may time out / wake False off a stale stash.  Repeated trials
    because the stale-stash leak grew across trials before the fix."""
    from runloom import monkey
    monkey.patch()
    import threading

    box = {"false": 0, "total": 0, "trials": 0}

    def main():
        for _ in range(5):
            ev = threading.Event()
            outs = []

            def waiter():
                outs.append(ev.wait(0.5))

            for _ in range(500):
                runloom.go(waiter)
            runloom.sleep(0.1)
            ev.set()
            runloom.sleep(0.4)
            box["total"] += len(outs)
            box["false"] += sum(1 for o in outs if not o)
            box["trials"] += 1

    runloom.run(8, main)
    assert box["trials"] == 5, box
    assert box["total"] == 5 * 500, box
    assert box["false"] == 0, box
