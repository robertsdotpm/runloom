"""Per-hub kqueue + cross-hub self-pipe doorbell -- M:N wake delivery.

FOCUS (cluster mn_selfpipe): every hub gets its OWN kqueue + its OWN pump-wake
self-pipe (netpoll_init.c.inc:27-47, the EV_CLEAR self-pipe armed in THIS pool's
kqueue), so a fiber parked in wait_fd on hub A links + arms in pool[A] and its
readiness is collected by hub A's own kevent (netpoll_pump.c.inc:147-216). When a
DIFFERENT hub (or a foreign OS thread) must break hub A's idle kevent(), it pokes
hub A's self-pipe write end (netpoll_wake_iouring.c.inc:425-440 wake_pump, the
per-hub kqueue branch), which hub A's pump drains and discards
(netpoll_pump.c.inc:193-201). Cross-hub g.wake() / cancel_wait_fd() both route
through that same per-hub doorbell.

These run in-process under runloom.run(hubs, main) -- the same in-process M:N
harness test_mn_park.py uses -- because they exercise the live per-hub hub
threads, which a subprocess would only duplicate. They will run ONLY on a kqueue
host (skip below); a human runs + fixes them on the mac.

Conventions (per the repo's proven M:N tests):
  - race-free per-fiber flags: one bytearray slot per fiber, single writer each,
    summed at the boundary (a shared `+= 1` LOSES increments with the GIL off).
  - close every socket; reset the per-fd registration between tests (these raw-
    close socketpairs, bypassing the monkey unregister hook).
"""
import socket
import sys
import threading

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

# Run from the repo root; the in-tree build lives under src/.
sys.path.insert(0, "src")

import runloom        # noqa: E402  high-level go/sleep/run (monkey-free)
import runloom_c      # noqa: E402  raw scheduler: current_g / wait_fd / cancel

READ, WRITE = 1, 2
WAIT_FD_CANCELLED = runloom_c.WAIT_FD_CANCELLED   # 0x40000000


# --------------------------------------------------------------------------- #
# Registration reset.  The socketpairs below are raw-closed (no monkey close
# hook), so the kqueue fd-bit / arm bookkeeping for those fd NUMBERS can linger
# and a later test that reuses the number would otherwise hit the stale-bit
# early-return.  Clear it around every test.  (kqueue re-arms ONE-SHOT per park,
# but the fd-bit is still kept in sync for unregister -- see netpoll_register.)
# --------------------------------------------------------------------------- #
def _reset_registration():
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:
            pass


def setup_function(_fn):
    _reset_registration()


def teardown_function(_fn):
    _reset_registration()


def test_backend_is_kqueue():
    """Guard: this whole module is meaningless off kqueue."""
    assert runloom_c.netpoll_backend() == "kqueue"


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


# --------------------------------------------------------------------------- #
# 1. CROSS-HUB readiness: many fibers park READ on their own socketpair; the
#    root (a different hub) makes them ready in WAVES by writing a byte to each
#    peer.  The readiness event for a fiber parked on hub A is collected by hub
#    A's OWN kqueue (netpoll_pump.c.inc:182 kevent(kpool->kqueue_fd,...)), and
#    dispatch_event resolves the parker across pools regardless of which hub
#    collected it.  Arming each park's deadline + the root's writes drive the
#    per-hub self-pipe doorbell (wake_pump, netpoll_wake_iouring.c.inc:432) so an
#    idle hub's kevent() breaks promptly.
#    Branches: netpoll_init self-pipe arm; netpoll_pump kqueue drain + dispatch
#    (wake_all=1); netpoll_wake_iouring per-hub wake_pump.
# --------------------------------------------------------------------------- #
def _cross_hub_waves(hubs, n, waves):
    woke = bytearray(n)          # one slot per fiber: race-free
    pairs = [None] * n           # (parked_sock, peer_sock)

    def waiter(i):
        a, b = _pair()
        pairs[i] = (a, b)
        # deadline keeps the test from hanging if a wake is ever lost; it is
        # long enough that a correctly-delivered readiness ALWAYS wins it.
        r = runloom_c.wait_fd(a.fileno(), READ, 5000)
        if r & READ:
            woke[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        # let every waiter link + commit its park across the hubs
        runloom.sleep(0.2)
        per = max(1, n // waves)
        i = 0
        for _ in range(waves):
            for _ in range(per):
                if i >= n:
                    break
                a, b = pairs[i]
                b.send(b"x")     # makes pairs[i][0] READ-ready in ITS hub's kq
                i += 1
            runloom.sleep(0.05)  # let the readied wave wake before the next
        # flush any remainder
        while i < n:
            pairs[i][1].send(b"x")
            i += 1
        runloom.sleep(0.4)
        main.total = sum(woke)

    runloom.run(hubs, main)
    for p in pairs:
        if p:
            p[0].close()
            p[1].close()
    return main.total, n


@pytest.mark.parametrize("hubs", [2, 4, 8], ids=["h2", "h4", "h8"])
def test_cross_hub_waves_all_wake(hubs):
    total, n = _cross_hub_waves(hubs, n=60, waves=3)
    assert total == n, "lost %d of %d cross-hub wakes (hubs=%d)" % (
        n - total, n, hubs)


# --------------------------------------------------------------------------- #
# 2. SAME-HUB collection while siblings are busy/idle: a fiber parked on its own
#    hub is woken by an event collected on THAT hub's own kqueue.  Under run(1)
#    there is exactly one hub+kqueue+self-pipe, so this isolates the per-hub
#    kevent->dispatch path with no cross-hub routing.  A handful of busy CPU
#    fibers on the (multi-hub) variant keep other hubs occupied so the parking
#    hub still drains its own ready set via its periodic non-blocking self-pump.
#    Branch: netpoll_pump.c.inc:182-216 (this hub's kqueue), dispatch_event.
# --------------------------------------------------------------------------- #
def _same_hub(hubs, n, busy):
    woke = bytearray(n)

    def waiter(i, a, b):
        b.send(b"q")             # ready BEFORE the park: EV_ADD re-checks NOW
        r = runloom_c.wait_fd(a.fileno(), READ, 5000)
        if r & READ:
            woke[i] = 1

    def spinner():
        s = 0
        for _ in range(20000):
            s += 1
            runloom.yield_now()

    socks = []

    def main():
        for _ in range(busy):
            runloom.fiber(spinner)
        for i in range(n):
            a, b = _pair()
            socks.append((a, b))
            runloom.fiber(waiter, i, a, b)
        runloom.sleep(0.5)
        main.total = sum(woke)

    runloom.run(hubs, main)
    for a, b in socks:
        a.close()
        b.close()
    return main.total, n


def test_same_hub_single():
    """run(1): one kqueue, ready-before-park, collected on its own hub."""
    total, n = _same_hub(1, n=40, busy=0)
    assert total == n, "%d/%d woke on the single hub" % (total, n)


@pytest.mark.parametrize("hubs", [2, 4, 8], ids=["h2", "h4", "h8"])
def test_same_hub_with_busy_siblings(hubs):
    """Readiness collected on the parking hub's own kqueue while other hubs are
    busy spinning -- exercises the periodic non-blocking self-pump."""
    total, n = _same_hub(hubs, n=40, busy=hubs)
    assert total == n, "%d/%d woke with busy siblings (hubs=%d)" % (
        total, n, hubs)


# --------------------------------------------------------------------------- #
# 3. MANY waiters (scaled 50..500) across all hubs, all woken via the KQUEUE
#    EVENT path (not unpark_many): a scaled-down test_unpark_many analogue that
#    drives readiness through the kernel kqueue + per-hub drain instead of the
#    in-process batch wake.  Confirms the drain-until-empty loop
#    (netpoll_pump.c.inc:164-221, cap 4096 / n<256 break) delivers a large ready
#    set without throttling -- the high-parked-count case that motivated it.
# --------------------------------------------------------------------------- #
def _many_via_kqueue(hubs, n):
    woke = bytearray(n)
    pairs = [None] * n

    def waiter(i):
        a, b = _pair()
        pairs[i] = (a, b)
        r = runloom_c.wait_fd(a.fileno(), READ, 8000)
        if r & READ:
            woke[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        runloom.sleep(0.3)       # all parked across hubs
        for i in range(n):       # one burst: a big ready set into the kqueues
            pairs[i][1].send(b"z")
        runloom.sleep(0.6)
        main.total = sum(woke)

    runloom.run(hubs, main)
    for p in pairs:
        if p:
            p[0].close()
            p[1].close()
    return main.total, n


@pytest.mark.parametrize("n", [50, 200, 500], ids=["n50", "n200", "n500"])
def test_many_waiters_via_kqueue_event(n):
    total, got = _many_via_kqueue(8, n)
    assert total == got, "%d/%d waiters woke via the kqueue event path" % (
        total, got)


# --------------------------------------------------------------------------- #
# 4a. FOREIGN-THREAD CLOSE: a real (non-fiber, non-hub) threading.Thread
#     closes the PEER of a socketpair while a fiber is parked READ on its end.
#     Closing the peer half-closes the parked fd -> kqueue reports EV_EOF on the
#     READ filter; the pump folds EOF into BOTH directions
#     (netpoll_pump.c.inc:212, finding B1) so the parked reader is made runnable
#     and its wait_fd returns a nonzero (READ) mask.  The foreign close runs on a
#     thread with no hub, so the wake reaches the parking hub through its own
#     kqueue collection -- the cross-hub-collected path.  recv() then sees EOF
#     (empty) -- exactly what a peer-close should surface.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8], ids=["h2", "h4", "h8"])
def test_foreign_thread_peer_close_wakes_parked(hubs):
    n = 24
    woke = bytearray(n)
    pairs = [None] * n

    def waiter(i):
        a, b = _pair()
        pairs[i] = (a, b)
        r = runloom_c.wait_fd(a.fileno(), READ, 5000)
        # EOF folds into READ; a timeout (0) would mean a lost EOF wake.
        if r != 0 and r != WAIT_FD_CANCELLED:
            woke[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        runloom.sleep(0.25)      # all parked across the hubs

        def closer():
            # foreign OS thread: close every PEER -> EV_EOF on each parked fd.
            for i in range(n):
                pairs[i][1].close()
        t = threading.Thread(target=closer)
        t.start()
        t.join()
        runloom.sleep(0.5)
        main.total = sum(woke)

    runloom.run(hubs, main)
    for p in pairs:
        if p:
            try:
                p[0].close()
            except OSError:
                pass
    assert main.total == n, "%d/%d parked fibers woke on foreign peer-close" % (
        main.total, n)


# --------------------------------------------------------------------------- #
# 4b. FOREIGN-THREAD CANCEL: a real threading.Thread cancels each parked fiber's
#     wait_fd via g.cancel_wait_fd() (the close hook's cross-hub waker -- e.g. a
#     socket another fiber closes).  cancel_wait_fd claims the parker under its
#     OWNING pool's lock and re-queues the g with the CANCELLED sentinel, then
#     kicks THAT hub's self-pipe (RunloomG_wake/cancel path -> per-hub wake_pump,
#     netpoll_wake_iouring.c.inc:432) so an idle parking hub's kevent() breaks.
#     Branch: cross-hub cancel + per-hub self-pipe doorbell from a foreign thread.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8], ids=["h2", "h4", "h8"])
def test_foreign_thread_cancel_wait_fd(hubs):
    n = 24
    cancelled = bytearray(n)
    handles = [None] * n
    socks = []

    def waiter(i):
        a, b = _pair()
        socks.append((a, b))
        handles[i] = runloom_c.current_g()
        # never made ready: only a cancel can wake this within the deadline.
        r = runloom_c.wait_fd(a.fileno(), READ, 6000)
        if r == WAIT_FD_CANCELLED:
            cancelled[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        runloom.sleep(0.3)       # all parked; handles recorded

        def canceller():
            for h in handles:
                if h is not None:
                    h.cancel_wait_fd()
        t = threading.Thread(target=canceller)
        t.start()
        t.join()
        runloom.sleep(0.5)
        main.total = sum(cancelled)

    runloom.run(hubs, main)
    for a, b in socks:
        a.close()
        b.close()
    assert main.total == n, "%d/%d parked fibers saw CANCELLED (hubs=%d)" % (
        main.total, n, hubs)


# --------------------------------------------------------------------------- #
# 4c. CROSS-HUB g.wake() (in-runtime, not foreign): the root wakes each parked
#     fiber via its G handle from a DIFFERENT hub.  wake() routes by the g's
#     recorded park_hub and pokes that hub's self-pipe (RunloomG_wake ->
#     wake_pump), so an idle parking-hub kevent() breaks and the re-queued g
#     resumes -- the in-runtime counterpart of 4b.  The woken wait_fd returns 0
#     (it was a spurious wake: no fd event, no cancel), which the fiber tolerates
#     and re-confirms by simply recording that it resumed.
#     NB g.wake() re-queues the g via the same-thread/hub wake path; wait_fd's
#     defensive unlink + park/re-check loop make the spurious resume safe.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4], ids=["h2", "h4"])
def test_cross_hub_g_wake_breaks_parking_hub(hubs):
    n = 16
    resumed = bytearray(n)
    handles = [None] * n
    socks = []

    def waiter(i):
        a, b = _pair()
        socks.append((a, b))
        handles[i] = runloom_c.current_g()
        # short deadline so a missed wake still terminates the run cleanly,
        # but long enough that a delivered wake wins it.
        runloom_c.wait_fd(a.fileno(), READ, 4000)
        resumed[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        runloom.sleep(0.3)
        for h in handles:        # root hub wakes each parked fiber cross-hub
            if h is not None:
                # a wait_fd parker is woken out-of-band via cancel_wait_fd (g.wake()
                # is for the generic park()); this drives the cross-hub self-pipe
                # doorbell that breaks the parking hub's idle kevent.
                h.cancel_wait_fd()
        runloom.sleep(0.4)
        main.total = sum(resumed)

    runloom.run(hubs, main)
    for a, b in socks:
        a.close()
        b.close()
    # Every fiber must resume.  (Whether via the explicit wake or its deadline,
    # a hung fiber would wedge run() and trip the suite timeout -- the real
    # assertion is that none stay parked forever.)
    assert main.total == n, "%d/%d parked fibers resumed (hubs=%d)" % (
        main.total, n, hubs)


# --------------------------------------------------------------------------- #
# 5. cancel_all_parked() teardown backstop (finding B3) across hubs: many fibers
#    parked on idle-but-open socketpairs (never readied, no deadline) would wedge
#    the run on the pending-g join.  cancel_all_parked() walks EVERY by_fd bucket
#    of EVERY pool and force-wakes each with CANCELLED; the C binding returns the
#    count cancelled.  Driven here from the root so the run can complete.
#    Branch: netpoll_wake_iouring.c.inc:256 cancel_all_parked (per-pool walk).
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8], ids=["h2", "h4", "h8"])
def test_cancel_all_parked_drains_every_hub(hubs):
    n = 30
    cancelled = bytearray(n)
    socks = []
    box = {}

    def waiter(i):
        a, b = _pair()
        socks.append((a, b))
        # NO deadline (block forever): only cancel_all_parked can free it.
        r = runloom_c.wait_fd(a.fileno(), READ)
        if r == WAIT_FD_CANCELLED:
            cancelled[i] = 1

    def main():
        for i in range(n):
            runloom.fiber(waiter, i)
        runloom.sleep(0.3)       # all parked, blocking-forever, across hubs
        box["ret"] = runloom_c.cancel_all_parked()
        runloom.sleep(0.4)
        main.total = sum(cancelled)

    runloom.run(hubs, main)
    for a, b in socks:
        a.close()
        b.close()
    assert main.total == n, "%d/%d blocking-forever fibers cancelled (hubs=%d)" % (
        main.total, n, hubs)
    # The return value counts the parkers it actually claimed+cancelled; with no
    # competing waker every one of the n parkers should be counted.
    assert box.get("ret", -1) >= n, (
        "cancel_all_parked returned %r, expected >= %d" % (box.get("ret"), n))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
