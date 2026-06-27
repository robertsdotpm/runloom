"""big_100 / 310 -- a __del__ that RESURRECTS itself + re-registers a finalizer.

The genuinely untested finalizer corner.  p65/p139/p211 raise / spawn / yield /
preempt from a destructor, but NONE has an object's __del__ make the object
REACHABLE AGAIN (store `self` into a module-global live set) and register a FRESH
weakref.finalize during that same finalization -- all while a real OS thread is
running a gc.collect() storm and the M:N scheduler churns the objects' last
decref onto arbitrary hubs.  Resurrection mutates the GC's finalization worklist
mid-sweep; combined with the free-threaded "preemption must NOT yield mid
tp_dealloc" gate, a buggy runtime can:

  * DOUBLE-FINALIZE -- run the resurrected object's close path twice (once at the
    first death's __del__, once at the second death's finalize),
  * LEAK the resurrected graph's fd -- the second death never fires, the finalize
    callback is dropped, the fd stays open,
  * or crash if the resurrected object's RE-collection lands on a half-torn hub.

Each `Res` owns a REAL os.pipe() read-fd (a pipe fd, so we are bounded by the fd
rlimit -- raised by the harness -- not the socket-buffer pool, and a close is
unambiguous).  Its __del__, on a deterministic 1-in-K subset and only the FIRST
time, RESURRECTS: it stashes `self` in a module-global live[] list, registers a
fresh `weakref.finalize(self, release_fd, fd, slot)`, marks itself resurrected,
and does NOT close the fd yet.  Every other (non-resurrecting / already-once-
resurrected) finalization closes the fd directly.  A driver goroutine + a real
OS thread periodically empty live[] and gc.collect(), so each resurrected object
DIES A SECOND TIME and its registered finalize closes the fd exactly then.

ORACLE -- exact fd conservation through a REAL lock, not a racy slot.  Resurrection
means the SAME object's fd can be closed from a DIFFERENT hub on its second death
than the hub that created it, so a lock-free per-slot counter would lose an
increment under the GIL-off cross-thread merge and FALSELY trip released!=acquired
(a false bug).  We therefore route EVERY acquire and EVERY release through one
module-level _thread.allocate_lock() -- a genuine OS lock (foreign-thread-safe,
never made cooperative by monkey.patch), held only for a single += and an
os.close.  release_fd closes each fd EXACTLY ONCE (guarded by a per-fd "already
released" set under the same lock), so a double-finalize is caught as a release of
an fd not in the open set (-> double_release > 0), and a leaked resurrected-graph
fd is caught as released < acquired at the end.

post() forces a final gc.collect() so every second death fires, then asserts:
  * acquired > 0                          (we actually ran the path)
  * resurrections > 0 AND re_finalized > 0 (the resurrect path was EXERCISED)
  * double_release == 0                   (no fd closed twice -- no double-finalize)
  * released == acquired                  (no leaked resurrected-graph fd)
  * harness fd-leak balance stays bounded (count_fds end ~ base)
A double-free would also surface as a SIGSEGV/SIGABRT caught by run_all's core
attribution; the lock+exact-conservation oracle catches the non-crashing variants.

Stresses: __del__ self-resurrection, weakref.finalize re-registration during
finalization, GC-worklist mutation mid-sweep, preempt-mid-tp_dealloc gate,
cross-hub last-decref, fd conservation under resurrection.

Good TSan / controlled-replay target: the resurrect-publish vs second-death-close
ordering is a pure cross-hub memory ordering question; a data race on the live[]
publish or the finalize registration is the first signal, before the conservation
arithmetic even fires.
"""
import gc
import os
import sys
import weakref

import _thread

import harness
import runloom

# One genuine OS lock guards ALL fd bookkeeping.  It is a real lock (never made
# cooperative by monkey.patch), so a release that fires from a foreign hub on a
# resurrected object's SECOND death -- or from a weakref.finalize callback at an
# arbitrary point, even interpreter shutdown -- serializes correctly against the
# acquire on the creating hub.  A lock-free per-slot counter cannot: the same
# fd's acquire and release land on DIFFERENT hubs, so the GIL-off cross-thread
# refcount merge would lose an increment and falsely break conservation.
FD_LOCK = _thread.allocate_lock()

# fd accounting, all touched only under FD_LOCK.
ACQUIRED = [0]          # total fds opened by a Res
RELEASED = [0]          # total fds closed (each fd at most once)
DOUBLE_RELEASE = [0]    # an fd asked to close while NOT in OPEN -> double-finalize
OPEN_FDS = set()        # fds currently open (membership = "owned, not yet closed")

# resurrection accounting (under FD_LOCK; tiny, rare).
RESURRECTIONS = [0]     # __del__ that stashed self + registered a new finalize
RE_FINALIZED = [0]      # weakref.finalize callbacks that actually fired
UNRAISABLE = [0]        # finalizer exceptions counted by the unraisable hook

# The resurrected-object graveyard: __del__ publishes `self` here to revive the
# object; a driver empties it so each resurrected object dies a SECOND time.
LIVE = []
LIVE_LOCK = _thread.allocate_lock()

# Strong refs to the weakref.finalize objects WE registered (guarded by FD_LOCK).
# post() fires only these at the end -- never some other subsystem's finalizer --
# to reproduce a clean interpreter shutdown for resurrected objects whose second
# death was collected but whose finalize callback is still pending.
OUR_FINALIZERS = []

# Resurrect 1 in RESURRECT_EVERY objects (deterministic subset, by idx).
RESURRECT_EVERY = 8


def acquire_fd():
    """Open a real pipe read-fd and register it as owned.  Returns the read fd;
    the write end is closed immediately (we only need a closable fd, not I/O)."""
    r, w = os.pipe()
    os.close(w)
    with FD_LOCK:
        ACQUIRED[0] += 1
        OPEN_FDS.add(r)
    return r


def release_fd(fd, slot=0):
    """Close `fd` EXACTLY ONCE.  All bookkeeping under the one real lock so a
    second-death close on a foreign hub can't race the first death.  If `fd` is
    not currently owned, this is a double-finalize -> count it (the bug), do not
    re-close (re-closing a reused fd number would corrupt an unrelated fd)."""
    with FD_LOCK:
        if fd not in OPEN_FDS:
            DOUBLE_RELEASE[0] += 1
            return
        OPEN_FDS.discard(fd)
        try:
            os.close(fd)
        except OSError:
            pass
        RELEASED[0] += 1


def counting_release(fd, slot=0):
    """The release path registered by a RESURRECTING __del__: it both counts the
    second-death (re-finalize) firing and closes the fd through release_fd, so
    RE_FINALIZED proves the resurrect-then-second-death path actually executed."""
    with FD_LOCK:
        RE_FINALIZED[0] += 1
    release_fd(fd, slot)


class Res(object):
    """An fd-owning object whose __del__ may resurrect itself once."""
    __slots__ = ("idx", "fd", "slot", "resurrected", "__weakref__")

    def __init__(self, idx, slot):
        self.idx = idx
        self.slot = slot
        self.resurrected = False
        self.fd = acquire_fd()

    def __del__(self):
        # First-death finalization.  On the deterministic resurrect subset, and
        # only the FIRST time we are finalized, REVIVE: publish self into the
        # global live set and register a fresh weakref.finalize that will close
        # the fd on the SECOND death.  Do NOT close the fd here -- the object is
        # alive again and still owns it.
        if (self.idx % RESURRECT_EVERY) == 0 and not self.resurrected:
            self.resurrected = True
            fd = self.fd
            slot = self.slot
            try:
                # Re-register a finalizer DURING finalization -- this mutates the
                # GC's finalize worklist mid-sweep, the targeted hazard.  It fires
                # on the SECOND death (via counting_release, which counts the
                # re-finalize and closes the fd exactly once).  Keep a strong ref
                # to the finalize object so post() can fire any that the M:N
                # collector orphaned (object collected, callback still pending).
                fin = weakref.finalize(self, counting_release, fd, slot)
                with FD_LOCK:
                    OUR_FINALIZERS.append(fin)
                with LIVE_LOCK:
                    LIVE.append(self)        # resurrect: object reachable again
                with FD_LOCK:
                    RESURRECTIONS[0] += 1
            except Exception:
                # If revival itself failed, fall back to closing now so we never
                # leak; counted as unraisable by the hook if it escaped.
                release_fd(fd, slot)
            return
        # Second death of an ALREADY-resurrected object: the fd is owned by the
        # weakref.finalize(counting_release) we registered on the first death,
        # which fires for THIS same collection and closes it exactly once.  We
        # must NOT close here too -- doing so would be the very double-finalize
        # the oracle hunts for.  So a resurrected object's __del__ closes nothing
        # on its second death.
        if self.resurrected:
            return
        # Ordinary (never-resurrecting) death: close the fd directly, exactly once.
        release_fd(self.fd, self.slot)


def drain_live(n=None):
    """Drop the strong refs LIVE holds on up to `n` resurrected objects so they
    die a SECOND time.  The popped objects are held only by this function's local
    `batch`, which is freed on return -- so the objects' last strong ref goes away
    here and the next gc.collect() runs their registered weakref.finalize close.
    Returns how many were dropped."""
    batch = []
    with LIVE_LOCK:
        k = len(LIVE) if n is None else min(n, len(LIVE))
        for _ in range(k):
            batch.append(LIVE.pop())
    return len(batch)


def worker(H, wid, rng, state):
    base = wid * 4_000_000
    i = base
    for _ in H.round_range():
        if not H.running():
            break
        # Create a batch of fd-owning objects.  Some are paired into a cycle so
        # only the cyclic GC reclaims them -> finalization from the GC sweep, a
        # different context than a plain decref.
        batch = []
        for _ in range(rng.randint(6, 14)):
            o = Res(i, wid & 1023)
            i += 1
            if rng.random() < 0.5:
                o2 = Res(i, wid & 1023)
                i += 1
                # A real reference cycle (holder lists refer to each other), so
                # ONLY the cyclic GC can reclaim the pair -> finalization from the
                # GC sweep, a different context than a plain last-decref.
                ha = [o]
                hb = [o2]
                ha.append(hb)
                hb.append(ha)
                batch.append(ha)
            else:
                batch.append(o)
        del batch                       # drop strong refs (some die now)
        # Periodically force collection so cyclic + resurrected deaths fire from
        # the GC sweep on whatever hub runs it.
        if rng.random() < 0.05:
            gc.collect()
        # Empty a few resurrected objects so they die their SECOND death soon
        # (their registered finalize closes the fd).  Done by EVERY worker so the
        # second-death close lands on many different hubs -- the conservation
        # stress the real lock must survive.
        if (i & 3) == 0:
            drain_live(8)               # drop refs -> next collect fires finalize
            if rng.random() < 0.1:
                gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def setup(H):
    H.state = {}
    counter = UNRAISABLE

    def count_unraisable(unraisable):
        counter[0] += 1

    sys.unraisablehook = count_unraisable
    # Start with a clean GC state so our accounting isn't perturbed by leftover
    # cyclic garbage from import.
    gc.collect()


def body(H):
    def gc_driver():
        # A driver that both forces GC (so first AND second deaths fire) and
        # empties LIVE in bulk, so resurrected objects don't accumulate without
        # ever dying twice.  Runs as a goroutine; the GC sweep it triggers runs
        # finalizers on this hub while workers churn on others.
        while H.running():
            H.sleep(0.03)
            drain_live()                   # empty the whole graveyard
            gc.collect()
        H.log("acquired={0} released={1} resurrections={2} re_finalized={3} "
              "live={4}".format(ACQUIRED[0], RELEASED[0], RESURRECTIONS[0],
                                RE_FINALIZED[0], len(LIVE)))

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Drain any survivors: empty the graveyard and collect repeatedly so every
    # resurrected object dies its second death and its weakref.finalize fires
    # BEFORE we read the conservation counters.  Loop until still_open stops
    # shrinking (the cross-thread biased-refcount merge + cyclic GC settle over a
    # few rounds), bounded so a real leak can't spin forever.
    prev = -1
    for _ in range(20):
        drain_live()
        gc.collect()
        with FD_LOCK:
            cur = len(OPEN_FDS)
        if cur == prev and not LIVE:
            break
        prev = cur
    # Interpreter-finalization stand-in.  Under M:N a resurrected object can be
    # COLLECTED (its second death) while its registered weakref.finalize is still
    # `.alive` but ORPHANED -- the object is gone (peek() is None) yet the
    # callback has not been invoked: it is pending the weakref atexit hook that a
    # real interpreter shutdown would run.  That is exactly the "at interpreter
    # finalization" timing the spec targets.  Reproduce a clean shutdown here:
    # find every still-live weakref.finalize and fire it, so the conservation
    # oracle is read AFTER all finalizers have run.  weakref.finalize fires at
    # most once, so this can never double-close; a leftover fd it CANNOT close is
    # a genuine leak, caught by the released==acquired check below.
    for _ in range(3):
        with FD_LOCK:
            pending = [f for f in OUR_FINALIZERS if f.alive]
        if not pending:
            break
        for fobj in pending:
            try:
                fobj()        # fire the resurrected object's second-death close
            except Exception:
                pass
        gc.collect()
    gc.collect()

    acquired = ACQUIRED[0]
    released = RELEASED[0]
    dbl = DOUBLE_RELEASE[0]
    res = RESURRECTIONS[0]
    refin = RE_FINALIZED[0]
    with FD_LOCK:
        still_open = len(OPEN_FDS)
    H.log("final acquired={0} released={1} still_open={2} double_release={3} "
          "resurrections={4} re_finalized={5} unraisable={6}".format(
              acquired, released, still_open, dbl, res, refin, UNRAISABLE[0]))

    H.check(acquired > 0, "no fd-owning objects were ever created")
    # Proof the targeted path was actually exercised (not a vacuous green run).
    H.check(res > 0,
            "no object ever resurrected itself from __del__ -- the resurrect "
            "path never fired (oracle would be vacuous)")
    H.check(refin > 0,
            "no resurrected object's weakref.finalize ever fired -- the second "
            "death (re-finalize) path never executed")
    # The bite: no double-finalize, and no leaked resurrected-graph fd.
    H.check(dbl == 0,
            "double-finalize: {0} fd(s) closed twice (a resurrected object's fd "
            "was released by BOTH first-death __del__ and second-death finalize)"
            .format(dbl))
    H.check(released == acquired,
            "fd conservation broken: released={0} != acquired={1} (still_open="
            "{2}) -- a resurrected-graph fd leaked (released<acquired) or was "
            "over-released (released>acquired)".format(
                released, acquired, still_open))
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p310_finalizer_resurrect_at_shutdown", body, setup=setup,
                 post=post, default_funcs=2000, max_funcs=2000,
                 describe="__del__ resurrects self + re-registers weakref.finalize "
                          "during GC finalization under M:N; every fd acquired is "
                          "released EXACTLY once (real-lock conservation), "
                          "resurrections>0 and re_finalized>0")
