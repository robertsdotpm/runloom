"""Stress test for the claim: runloom_netpoll_signal_wake claims ARMED parkers
and abandons them, so a fiber mid-wait_fd (link->commit window) at signal time
gets a spurious instant timeout (wait_fd returns 0).

Setup:
  - MAIN thread: runloom loop with one fiber parked forever on recv();
    a repeating SIGALRM (every 4 ms) whose handler raises _Sig.  The parked
    fiber catches _Sig and re-parks.  This drives the scheduler-grab
    signal_wake path over and over while the shared default parker pool
    contains other threads' parkers.
  - BACKGROUND thread: its own runloom loop with 24 fibers, each looping:
    recv(timeout=10s) on a socketpair whose peer gets a byte within ~15 ms
    from a feeder thread.  Every iteration passes through wait_fd's
    ARMED window (link -> epoll_ctl -> commit CAS).  If signal_wake ever
    claims one of these ARMED parkers and abandons it, that fiber's recv
    raises socket.timeout essentially instantly (elapsed << 10 s) -> BUG.

Expected (claim FALSE / owner filter works): zero timeouts, all signals
delivered into the main parked fiber.
"""
import sys
import time
import threading
import signal
import socket

import runloom.monkey as monkey
monkey.patch()
import runloom_c

DURATION = 6.0
NFIBERS = 24

errors = []
sig_caught = [0]
bg_iters = [0]
stop = threading.Event()


class _Sig(Exception):
    pass


def handler(signum, frame):
    raise _Sig


# ---------------- background loop (own thread, own sched) ----------------
def bg_thread():
    pairs = [socket.socketpair() for _ in range(NFIBERS)]

    def feeder():
        while not stop.is_set():
            for _, wr in pairs:
                try:
                    wr.send(b"x")
                except OSError:
                    return
            time.sleep(0.015)

    ft = threading.Thread(target=feeder, daemon=True)
    ft.start()

    def make_fiber(i):
        rd, _ = pairs[i]
        rd.settimeout(10.0)

        def body():
            while not stop.is_set():
                t0 = time.monotonic()
                try:
                    data = rd.recv(16)
                except socket.timeout:
                    el = time.monotonic() - t0
                    errors.append(
                        "SPURIOUS TIMEOUT fiber=%d elapsed=%.4fs (timeout=10s)"
                        % (i, el))
                    return
                except OSError:
                    return
                if not data:
                    return
                bg_iters[0] += 1
        return body

    for i in range(NFIBERS):
        runloom_c.fiber(make_fiber(i))
    runloom_c.run()
    for rd, wr in pairs:
        rd.close()
        wr.close()


# ---------------- main loop: parked fiber absorbing raising signals -------
def main():
    bt = threading.Thread(target=bg_thread, daemon=True)
    bt.start()
    time.sleep(0.3)  # let background parkers populate the shared pool

    rd, wr = socket.socketpair()
    rd.settimeout(30.0)

    def victim():
        deadline = time.monotonic() + DURATION
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            try:
                rd.recv(16)   # never-ready: only signals wake it
                errors.append("main recv returned data unexpectedly")
                return
            except _Sig:
                sig_caught[0] += 1
            except socket.timeout:
                el = time.monotonic() - t0
                errors.append(
                    "MAIN fiber spurious timeout elapsed=%.4fs (timeout=30s)"
                    % el)
                return
        signal.setitimer(signal.ITIMER_REAL, 0)

    old = signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, 0.05, 0.004)
    try:
        runloom_c.fiber(victim)
        try:
            runloom_c.run()
        except _Sig:
            # idle-loop fallback delivery (no parker eligible at that instant)
            pass
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)

    stop.set()
    wr.send(b"z")  # wake main victim if still parked (it isn't; loop ended)
    time.sleep(0.3)

    print("signals caught in fiber:", sig_caught[0])
    print("background recv iterations:", bg_iters[0])
    if errors:
        print("ERRORS (%d):" % len(errors))
        for e in errors[:20]:
            print("  ", e)
        print("CLAIM REPRODUCED")
        sys.exit(1)
    print("no spurious timeouts -- claim NOT reproduced")


if __name__ == "__main__":
    main()
