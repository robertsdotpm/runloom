"""Signal-storm robustness (S2).

pygo runs no code in async-signal context (S0: preemption is eval-breaker
based, the Ctrl-C path runs at a safe point). This hammers the signal-delivery
machinery anyway: fire SIGALRM at ~1 kHz while many goroutines do heavy
park/wake (chan ping-pong + sched_sleep), and verify the scheduler neither
crashes, hangs, nor returns wrong results. Run it standalone, and under the
whole-ext TSan harness (tools/run_sanitizers_ext.sh) to catch a race on the
signal/netpoll wake edge.
"""
import os
import signal
import sys

sys.path.insert(0, "src")
import pygo_core

fires = [0]


def on_alarm(signum, frame):
    fires[0] += 1


def main():
    if not hasattr(signal, "setitimer"):
        print("SKIP: no setitimer on this platform")
        return 0
    signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)   # 1 kHz storm
    try:
        results = []
        N = 3000

        # heavy park/wake: 32 ping-pong pairs, each bouncing N times, plus
        # sleepers parking on the timer -- all while signals rain down.
        # NB: pygo_core.go(fn, stack_size) does NOT forward args (the 2nd
        # positional is stack_size) -- capture everything via closures.
        def make_pinger(a, b):
            def pinger():
                for i in range(N):
                    a.send(i)
                    b.recv()
                results.append(N)
            return pinger

        def make_ponger(a, b):
            def ponger():
                for _ in range(N):
                    v, _ = a.recv()
                    b.send(v)
            return ponger

        def sleeper():
            for _ in range(50):
                pygo_core.sched_sleep(0.0005)

        for _ in range(32):
            a, b = pygo_core.Chan(), pygo_core.Chan()
            pygo_core.go(make_pinger(a, b))
            pygo_core.go(make_ponger(a, b))
        for _ in range(32):
            pygo_core.go(sleeper)
        pygo_core.run()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    ok_pairs = sum(results)
    expected = 32 * N
    print("signal fires: %d   ping-pong roundtrips completed: %d / %d"
          % (fires[0], ok_pairs, expected))
    if ok_pairs != expected:
        print("FAIL: lost/duplicated work under the signal storm")
        return 1
    if fires[0] == 0:
        print("WARN: no signals delivered (itimer didn't fire?)")
    print("OK: scheduler survived the signal storm with correct results")
    return 0


if __name__ == "__main__":
    sys.exit(main())
