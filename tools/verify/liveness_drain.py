#!/usr/bin/env python3
"""liveness_drain.py -- dynamic liveness / drain-mode oracle
(TigerBeetle freeze-and-assert-drain; the QA steal-list's biggest
under-weighted item).

pygo verifies liveness only at the MODEL level (TLA+ _live.cfg, Coq); there is no
dynamic oracle proving the REAL runtime makes forward progress.  This is it:

  Phase 1 (chaos):  drive a spawn/park/wake/steal-heavy workload with RUNLOOM_DELAY
                    seeded scheduler perturbation active -- widened windows where
                    a lost wake / livelock / stalled-hub bug strands a goroutine.
  Phase 2 (freeze): after a chaos window, _delay_freeze() stops ALL injection.
                    The runtime must now PROVE it recovers.
  Phase 3 (drain):  EVERY goroutine must complete.  We measure forward progress
                    directly -- one completion slot per goroutine -- rather than
                    relying on run() hanging (the runtime's deadlock detector
                    reaps a fully-stuck run, so a hang is not a reliable signal).
                    Any goroutine whose slot never gets set is a liveness
                    violation (lost wake / livelock / stalled hub), exactly the
                    stall-recovery roadmap (Groups A/B/C).

This is the "stop faulting, now PROVE liveness" phase that BUGGIFY's recovery
deadline and the Sometimes() asserts only gesture at.

  PYTHONPATH=src python tools/verify/liveness_drain.py [--seed N] [--workers W]
             [--hubs H] [--per P] [--chaos S] [--deadline S] [--teeth]

--teeth strands one goroutine (parks forever) to prove the oracle FIRES: reaching
a clean DRAINED/PASS with --teeth would mean the oracle is vacuous.
"""
import argparse
import os
import sys
import threading
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=0xC0FFEE)
    ap.add_argument("--workers", type=int, default=1200)
    ap.add_argument("--hubs", type=int, default=4)
    ap.add_argument("--per", type=int, default=300)
    ap.add_argument("--chaos", type=float, default=0.3)      # chaos window (s)
    ap.add_argument("--deadline", type=float, default=40.0)  # hard drain deadline (s)
    ap.add_argument("--teeth", action="store_true")
    ap.add_argument("--buggify", action="store_true",
                    help="use BUGGIFY (per-seed random fault subset, ~25%% firing) "
                         "instead of RUNLOOM_DELAY (all sites, every hit)")
    args = ap.parse_args()

    # Chaos on: seeded scheduler perturbation (the C layer reads the env lazily).
    # BUGGIFY activates a different ~half-of-sites subset per seed (FoundationDB);
    # RUNLOOM_DELAY perturbs every site every hit.  Both are stopped by
    # _delay_freeze() at the drain deadline.
    os.environ["RUNLOOM_BUGGIFY" if args.buggify else "RUNLOOM_DELAY"] = str(args.seed)
    os.environ.setdefault("RUNLOOM_DELAY_MAX_NS", "5000")

    import runloom
    import runloom_c as rc
    from runloom.sync import WaitGroup

    nch = max(4, args.workers // 50)
    # slot layout: [0,workers)=producers, then nch consumers, then closer, [+teeth]
    n_prod, n_cons, n_close = args.workers, nch, 1
    n_teeth = 1 if args.teeth else 0
    ntotal = n_prod + n_cons + n_close + n_teeth
    done = bytearray(ntotal)          # one completion slot per goroutine (no shared slot)

    def main_fn():
        chans = [rc.Chan(0) for _ in range(nch)]
        wg = WaitGroup()
        wg.add(n_prod)
        base_cons = n_prod
        base_close = n_prod + n_cons

        def consumer(ci, ch):
            try:
                while True:
                    _v, ok = ch.recv()
                    if not ok:
                        break
            finally:
                done[base_cons + ci] = 1

        def producer(pi, ch):
            try:
                for k in range(args.per):
                    ch.send(k)
            finally:
                wg.done()
                done[pi] = 1

        def closer():
            try:
                wg.wait()                 # all producers done...
                for ch in chans:
                    ch.close()            # ...so consumers drain and exit
            finally:
                done[base_close] = 1

        def stuck():                      # teeth: never completes
            try:
                rc.Chan(0).recv()         # nobody sends/closes
            finally:
                done[ntotal - 1] = 1      # (unreachable in a healthy run)

        for ci, ch in enumerate(chans):
            rc.mn_fiber((lambda ci=ci, ch=ch: consumer(ci, ch)))
        for pi in range(n_prod):
            ch = chans[pi % nch]
            rc.mn_fiber((lambda pi=pi, ch=ch: producer(pi, ch)))
        rc.mn_fiber(closer)
        if args.teeth:
            rc.mn_fiber(stuck)

    # Watchdog: run() did not drain within the deadline.  For a real workload
    # that is a LIVENESS VIOLATION (some goroutine stranded -- lost wake /
    # livelock / stalled hub the runtime did NOT self-recover from); diagnose +
    # fail (exit 2).  In --teeth mode a hang is the INTENDED proof the oracle
    # fires, so report it and exit 0.
    def on_deadline():
        healthy = sum(done[:ntotal - 1]) if args.teeth else sum(done)
        htot = ntotal - n_teeth
        if args.teeth:
            sys.stderr.write("\nliveness_drain[teeth]: oracle FIRED -- run did not "
                             "drain within %.1fs (%d/%d healthy done, stranded "
                             "goroutine stuck as intended)\n"
                             % (args.deadline, healthy, htot))
        else:
            sys.stderr.write("\n=== LIVENESS VIOLATION: not drained within %.1fs "
                             "(%d/%d drained) ===\n" % (args.deadline, healthy, htot))
        try:
            sys.stderr.write("deadlocked=%d fibers=%d chaos_active=%s\n" % (
                rc.count_deadlocked(), rc.fiber_count(), rc._delay_active()))
            for h in rc.mn_hub_states():
                pend = h.get("pending") if isinstance(h, dict) else None
                if pend:                       # only the hub(s) still holding work
                    sys.stderr.write("  STUCK hub %r\n" % (h,))
        except Exception as e:  # noqa: BLE001
            sys.stderr.write("(diag failed: %r)\n" % (e,))
        sys.stderr.flush()
        os._exit(0 if args.teeth else 2)

    # Timers set HERE (before runloom.run) so they re-establish intact in the
    # TLBC re-exec child.  _delay_freeze() is atomic / thread-safe.
    freeze_t = threading.Timer(args.chaos, rc._delay_freeze)
    wd = threading.Timer(args.deadline, on_deadline)
    freeze_t.daemon = wd.daemon = True
    freeze_t.start()
    wd.start()

    t0 = time.monotonic()
    err = None
    try:
        runloom.run(args.hubs, main_fn)
    except BaseException as e:  # noqa: BLE001
        err = e
    dt = time.monotonic() - t0
    wd.cancel()
    freeze_t.cancel()

    completed = sum(done)
    # expected: every goroutine EXCEPT a deliberately-stranded teeth one.
    healthy_total = ntotal - n_teeth
    healthy_done = sum(done[:ntotal - 1]) if args.teeth else completed
    stranded = healthy_total - healthy_done

    dl = 0
    try:
        dl = rc.count_deadlocked()
    except Exception:  # noqa: BLE001
        pass

    print("liveness_drain: completed=%d/%d in %.2fs (chaos froze at %.1fs, "
          "deadlocked=%d)%s" % (
              completed, ntotal, dt, args.chaos, dl,
              (" err=%r" % (err,)) if err is not None else ""))

    if args.teeth:
        # Oracle must NOTICE the stranded goroutine: either the healthy set fully
        # drained while the teeth slot stayed unset (stranded detected), or run()
        # raised / the watchdog killed us.
        if done[ntotal - 1] == 0 and (stranded == 0):
            print("liveness_drain[teeth]: oracle FIRED -- stranded goroutine "
                  "detected (%d/%d healthy drained, teeth slot unset)"
                  % (healthy_done, healthy_total))
            return 0
        if err is not None:
            print("liveness_drain[teeth]: oracle FIRED via runtime raise: %r" % (err,))
            return 0
        print("liveness_drain[teeth]: FAIL -- oracle vacuous (nothing stranded)")
        return 1

    try:
        rc._self_check()
    except Exception as e:  # noqa: BLE001
        print("liveness_drain: FAIL -- _self_check: %r" % (e,))
        return 1
    if err is not None:
        print("liveness_drain: FAIL -- run raised under chaos: %r" % (err,))
        return 1
    if stranded != 0 or completed != ntotal:
        print("liveness_drain: FAIL -- %d goroutine(s) never drained (liveness "
              "violation)" % (ntotal - completed))
        return 1
    if dl != 0:
        print("liveness_drain: FAIL -- %d goroutine(s) deadlocked" % dl)
        return 1
    print("liveness_drain: PASS -- all %d goroutines drained (forward progress "
          "held under chaos+freeze)" % ntotal)
    return 0


if __name__ == "__main__":
    sys.exit(main())
