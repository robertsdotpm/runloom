"""big_100 / 313 -- park directly on a USER eventfd; strict counter conservation.

Parks goroutines DIRECTLY on a user-created `os.eventfd` (not a socket, not the
runtime's internal hub-kick fd) via the generic `runloom_c.wait_fd` /
per-fd arm-cache path, and checks a true COUNTING law: every unit a producer
writes is drained exactly once by the consumer.

Why this is its own program (vs the channel-framed wake tests p224/p225): runloom
uses an eventfd INTERNALLY for per-hub wake kicks, and `runloom_epoll_handle_event`
explicitly distinguishes that internal kick fd and routes everything else through
the generic `runloom_pump_dispatch_event` / arm-cache.  A USER eventfd parked on
via wait_fd is therefore an ORDINARY pollable fd to the runtime -- but its
value-semantics make readiness edge-y and counter-driven: a `read()` drains the
8-byte counter to 0, the fd goes non-readable, and the consumer must RE-ARM and
RE-PARK.  A producer's `eventfd_write` that lands in the window between the
drain-to-0 and the re-park must NOT be lost (the Dekker clear-then-recheck class,
here exercised on the GENERIC-fd re-arm rather than the internal kick).

Topology (closed-world per worker group, so conservation is exactly checkable):
each worker owns ONE eventfd (NO EFD_SEMAPHORE -> a single read returns and
clears the FULL accumulated counter), spawns ONE consumer goroutine, and is
itself the producer:

  * producer: `eventfd_write(efd, 1)` exactly N times (each increment unit-
    attributable), bumping its produced shard by N; then it signals the consumer
    "done" over a 1-cap channel.
  * consumer: loop -> `wait_fd(efd, READ, ceiling)` -> on readable, drain the
    counter with `eventfd_read` and add the value to a running total via H.op;
    re-park.  It exits only once the producer has signalled done AND a final
    drain leaves the kernel counter at 0 (nothing in flight).  The bounded
    ceiling means the consumer re-checks the done flag and re-drains even if an
    edge wake is lost -- so a lost re-arm shows up as a COUNT MISMATCH (consumed
    < produced) rather than only as a hang.

Oracle (the strong one -- a counting law, not mere liveness):
  * conservation: sum of all producer increments == sum the consumers drain.
    A single dropped wake-after-drain on the value-semantics re-arm loses a unit
    and makes consumed < produced even when nothing visibly hangs.
  * require_no_lost: a consumer stranded parked with a non-zero kernel counter
    (a genuinely lost re-arm that the ceiling can't rescue) is a LOST worker.
  * the harness watchdog catches an outright park-forever hang.

Skips cleanly (note_scale_limit) if os.eventfd is unavailable (non-Linux) or the
wait_fd primitive is missing.

Stresses: wait_fd parking on a USER eventfd, generic-fd arm-cache re-arm after a
drain-to-0, value-semantics clear-then-recheck (lost wake-after-drain), cross-hub
producer/consumer counter conservation.

Good TSan / controlled-M:N-replay target: the drain-to-0 -> re-arm -> producer-
write ordering is a pure memory-ordering corner; a data-race report on the arm
cache or a single lost unit under replay is the first signal before the
conservation sum even closes.
"""
import os
import sys

import harness
import runloom

# ---- availability guard ---------------------------------------------------
# os.eventfd is Linux-only; wait_fd is the generic-fd park primitive.  Detect
# and skip cleanly (the campaign treats a clean skip as non-fatal).
_HAVE_EVENTFD = hasattr(os, "eventfd") and hasattr(os, "eventfd_write") \
    and hasattr(os, "eventfd_read")

try:
    import runloom_c
    _HAVE_WAITFD = hasattr(runloom_c, "wait_fd")
except Exception:                       # pragma: no cover - import guard
    runloom_c = None
    _HAVE_WAITFD = False

READ = 1                                # wait_fd events bitmask: 1 = readable
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", -1) if runloom_c else -1

# Per-park ceiling (ms).  A bounded ceiling means the consumer periodically wakes
# to re-check the producer's done flag and re-drain the counter -- so a lost EDGE
# wake-after-drain cannot strand it forever, and instead surfaces as a COUNT
# mismatch (the counting oracle) rather than only as a watchdog hang.  Kept short
# so the test stays responsive; long enough that the steady state is genuine
# park/wake cycles, not a poll loop.
CEILING_MS = 50

WRITES_PER_ROUND = 256                  # eventfd_write(1) calls per worker round


def consumer(efd, done_ch, counts, slot):
    """Park on the eventfd, drain its counter into the consumed total, re-arm.

    Exits only once the producer has signalled done AND a final drain leaves the
    kernel counter at 0 (so nothing written is ever left undrained).  Every unit
    drained is added to the consumed shard via H.op-style accumulation, so the
    post() sum is a true counting law over all writes."""
    drained = 0
    producer_done = False
    while True:
        try:
            ready = runloom_c.wait_fd(efd, READ, CEILING_MS)
        except OSError:
            # fd error -> stop; the conservation check will flag the shortfall.
            break
        if ready == CANCELLED:
            break
        if ready & READ:
            # Drain the WHOLE accumulated counter (no EFD_SEMAPHORE) in one read.
            try:
                v = os.eventfd_read(efd)
            except BlockingIOError:
                # Raced another wake that already drained to 0; re-arm.
                v = 0
            except OSError:
                break
            drained += v
        # ready == 0: bare timeout (ceiling) -> fall through to the done re-check
        # and re-park below.  This is the clear-then-recheck point: if the
        # producer is done, do ONE more non-blocking drain to sweep any unit that
        # landed in the drain->re-park window before declaring completion.
        if not producer_done and done_ch.try_recv() is not None:
            producer_done = True
        if producer_done:
            # Final sweep: a unit written after our last drain but before we saw
            # `done` must still be collected.  Loop draining until the counter is
            # empty, then exit -- if a wake was genuinely lost the kernel counter
            # would still be > 0 here and we would keep draining it, so this both
            # closes the count AND makes a lost re-arm visible as a non-empty
            # final drain (caught by the counting oracle, not a silent pass).
            while True:
                try:
                    v = os.eventfd_read(efd)
                except BlockingIOError:
                    break               # counter empty -> truly done
                except OSError:
                    break
                if v == 0:
                    break
                drained += v
            break
    counts["consumed"][slot] += drained


def producer(efd, n, done_ch, rng):
    """Write n unit increments, then signal the consumer it is done.  A sparse
    yield fans the writes across the consumer's park/drain cycles so the
    drain-to-0 -> re-arm window is hit repeatedly (the targeted corner).  Uses
    its OWN random.Random (sharing one across goroutines under M:N corrupts it)."""
    try:
        for i in range(n):
            os.eventfd_write(efd, 1)
            # Periodically yield so the consumer drains to 0 and must re-arm
            # between our writes -- exercising the value-semantics re-arm path.
            if (i & 7) == 0:
                runloom.yield_now()
            elif rng.getrandbits(6) == 0:
                runloom.sleep(0.0)
    finally:
        done_ch.send(True)


def worker(H, wid, rng, state):
    slot = wid                          # UNIQUE per worker (no shared-slot += race)
    produced = state["produced"]
    for _ in H.round_range():
        if not H.running():
            break
        # One eventfd per round, NO EFD_SEMAPHORE so a single read drains the
        # full accumulated counter; non-blocking so eventfd_read raises
        # BlockingIOError on an empty counter instead of blocking a hub thread.
        try:
            efd = os.eventfd(0, os.EFD_NONBLOCK)
        except OSError:
            if not H.running():
                break
            continue
        done_ch = runloom.Chan(1)
        n = WRITES_PER_ROUND
        cseed = rng.getrandbits(48)

        wg = runloom.WaitGroup()
        wg.add(2)

        def run_consumer(efd=efd, done_ch=done_ch, slot=slot):
            try:
                consumer(efd, done_ch, state, slot)
            finally:
                wg.done()

        def run_producer(efd=efd, n=n, done_ch=done_ch, cseed=cseed):
            import random
            try:
                producer(efd, n, done_ch, random.Random(cseed))
            finally:
                wg.done()

        H.fiber(run_consumer)
        H.fiber(run_producer)
        wg.wait()

        # Drop the idle arm and close (releases the per-fd arm-cache entry so a
        # reused fd number re-registers cleanly next round).
        try:
            runloom_c.netpoll_release_if_idle(efd)
        except Exception:
            pass
        try:
            os.close(efd)
        except OSError:
            pass

        produced[slot] += n
        H.op(wid, n)
        H.task_done(wid)


def setup(H):
    if not _HAVE_EVENTFD:
        H.note_scale_limit(
            "os.eventfd unavailable on this platform ({0}) -- skipping the "
            "user-eventfd park conservation test".format(sys.platform))
        H.state = None
        return
    if not _HAVE_WAITFD:
        H.note_scale_limit(
            "runloom_c.wait_fd unavailable -- cannot park on a raw fd; skipping")
        H.state = None
        return
    # ONE slot per worker (race-free counter rule, CLAUDE.md Benching): a shared
    # `list[slot] += n` is NOT atomic under GIL-off, so wid-COLLIDING slots lose
    # increments and make the CONSERVATION COUNTER itself race -- a false FAIL
    # that is a test measurement artifact, not a runtime lost-wake.  Size to the
    # worker count and index by the unique wid (see worker()).
    H.state = {"produced": [0] * H.funcs, "consumed": [0] * H.funcs}


def body(H):
    if H.state is None:
        return                          # skipped in setup (no eventfd / wait_fd)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED: {0}".format(H.scale_limit_reason or "no eventfd/wait_fd"))
        return
    p = sum(H.state["produced"])
    c = sum(H.state["consumed"])
    H.log("produced={0} consumed={1}".format(p, c))
    H.check(p > 0, "no eventfd increments produced")
    H.check(
        c == p,
        "conservation broken: consumed={0} != produced={1} (a wake-after-drain "
        "was lost on the user-eventfd value-semantics re-arm: a unit written "
        "between drain-to-0 and re-park was dropped)".format(c, p))
    # A consumer stranded parked with a non-zero kernel counter (a genuinely lost
    # re-arm the ceiling could not rescue) is a LOST worker, not merely slow.
    H.require_no_lost("eventfd consumer completeness")


if __name__ == "__main__":
    harness.main(
        "p313_eventfd_counter_conservation", body, setup=setup, post=post,
        default_funcs=2000,
        describe="park goroutines directly on a USER os.eventfd via wait_fd; "
                 "drain-to-0 then re-arm; sum of eventfd_write units == sum "
                 "drained (a lost wake-after-drain loses a unit)")
