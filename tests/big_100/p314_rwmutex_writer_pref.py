"""big_100 / 314 -- sync.RWMutex writer-preference handoff under M:N.

runloom.sync.RWMutex is the ONE Go-style reader/writer lock in the library and
(grep-confirmed) is exercised by ZERO other big_100 programs.  It claims two
strong properties that nothing here has ever stressed:

  * MUTUAL EXCLUSION -- many readers OR exactly one writer; a reader's critical
    section never overlaps a writer's, and two writers never overlap.
  * WRITER-PREFERENCE -- the instant a writer queues (its cell is appended to
    `_wwait` under the internal guard), rlock's fast path is closed (`if not
    self._writer and not self._wwait`), so NO new reader may be admitted ahead
    of the waiting writer.  The strict starvation bound is therefore 0: a writer
    sees ZERO readers admitted between the moment it queues and the moment it is
    granted.

The hunted bug: the guard is a CoFMutex and the lock is transferred by HANDOFF
(unlock/runlock pop the next waiter, set its `granted` cell, then `wake()` it
across hubs).  Under M:N with the GIL off, a reader's `not self._wwait` check
runs on one hub while a writer's `self._wwait.append(cell)` runs on another, and
the handoff's "writer first" pop + wake is a cross-hub publish.  If that
ordering tears -- a reader reads a stale empty `_wwait`, or a handoff grants a
reader while a writer's grant is in flight -- then either a reader overtakes a
queued writer (starvation-bound breach) or, worse, a reader's critical section
overlaps the writer's (mutual-exclusion breach -> a lost guarded write).

ORACLE (two race-free detectors, both fire on the targeted bug):

  (1) MUTUAL EXCLUSION, breach==0.  A shared `in_write` flag is set/cleared by
      the writer at the EXACT grant/release boundary, and a `live_readers` count
      is bumped/dropped at each reader's admit/exit boundary -- all under the
      lock's OWN internal guard (we subclass RWMutex and instrument the admit/
      append/grant points inside `_mu`, the single serialization point that
      already orders admission vs. queueing -- so the observation is race-free by
      construction, no second lock that could itself reorder).  Every reader, on
      admit, asserts `in_write == 0`; the writer, on grant, asserts `live_readers
      == 0 and not other_writer`.  Any overlap -> breach += 1.  H.check(breach==0).

  (2) STRICT STARVATION BOUND, admitted_after_queue == 0 EXACTLY.  A monotonic
      `admitted` counter ticks once per reader admission, under `_mu`.  A writer,
      at the instant its cell is appended to `_wwait` (same `_mu` hold), snapshots
      `admitted`; at grant it computes delta = admitted_now - snapshot.  Writer-
      preference forbids ANY reader admission while `_wwait` is non-empty, so the
      delta must be 0 -- not a fuzzy SLACK window, exactly 0.  Max delta over all
      writers is the breach metric.  H.check(max_admitted_after_queue == 0).

Writers yield_now() WHILE holding the write lock to FORCE a cross-hub handoff and
widen the rlock/_wwait race; readers yield_now() inside their critical section to
keep many readers genuinely co-resident.  ~90% readers / ~10% writers all share
ONE lock, contenders capped (single-lock convoy) per the p214/p50 precedent.

Invariant (post): mutual-exclusion breach == 0 AND max admitted-after-queue == 0;
at least one writer was granted and readers were admitted (the oracle ran).

Stresses: RWMutex rlock/lock/runlock/unlock handoff, writer-preference fast-path
gating, cross-hub wake of a granted waiter, reader-overtakes-writer race,
reader/writer critical-section overlap.

Good TSan / controlled-M:N-replay target: the `_wwait`-check vs `_wwait`-append
and the handoff grant/wake are pure cross-hub memory-ordering races; a data-race
report on `granted`/`_wwait` is often the first signal before either value oracle
fires.
"""
import _thread
import random

import harness
import runloom
import runloom.sync as rsync
import runloom_c

# Real OS lock (not a goroutine primitive) for the rare aggregate-breach
# bookkeeping the instrumented lock does under its own _mu -- the critical
# sections are O(1) integer updates with no cooperative yield inside, so a real
# lock never parks a goroutine or stalls a hub.  (Same rationale as the harness's
# _exit_lock.)  We DO NOT yield while holding it.
_AGG = _thread.allocate_lock()


class ObservedRWMutex(rsync.RWMutex):
    """RWMutex with the admit / queue / grant points instrumented INSIDE the
    lock's own `_mu` guard.

    Re-implements rlock/lock byte-for-byte with the parent (same writer-
    preference fast-path gate, same handoff loop) and ADDS, under the same `_mu`
    hold that the parent already takes, the race-free observations the oracle
    needs:
      * on a reader admit (fast path OR handoff grant): live_readers += 1,
        admitted += 1, and record in_write at that instant (must be 0).
      * on a writer queue (cell appended to _wwait): snapshot admitted.
      * on a writer grant: set in_write, record live_readers + other-writer at
        that instant (must be 0,0), compute admitted-after-queue delta.
    Because every one of these happens while THIS object's `_mu` is held -- the
    single point that already serializes admission against queueing -- the
    observation cannot itself race the runtime it observes.
    """

    # Parent uses __slots__; add our observation slots (subclass slots compose).
    __slots__ = ("obs_in_write", "obs_live_readers", "obs_admitted",
                 "obs_writers_in_cs", "obs_mx_breach", "obs_max_after_queue",
                 "obs_writers_granted", "obs_readers_admitted")

    def __init__(self):
        super().__init__()
        self.obs_in_write = 0          # 1 while a writer holds the lock
        self.obs_live_readers = 0      # readers currently inside rlock CS
        self.obs_admitted = 0          # MONOTONIC reader-admission ticks
        self.obs_writers_in_cs = 0     # writers currently inside lock CS (must <=1)
        self.obs_mx_breach = 0         # mutual-exclusion violations seen
        self.obs_max_after_queue = 0   # max readers admitted while a writer queued
        self.obs_writers_granted = 0   # total write grants (oracle-ran metric)
        self.obs_readers_admitted = 0  # total read admits (oracle-ran metric)

    # -- reader side -------------------------------------------------------
    def rlock(self):
        rsync._resolve_from_fiber("RWMutex.rlock()")
        g = runloom_c.current_g()
        rsync._acquire(self._mu)
        if not self._writer and not self._wwait:        # writer-preference gate
            self._readers += 1
            # FAST-PATH admit: this is the ONLY admit that can overtake a queued
            # writer (it fires only when `_wwait` is empty at this instant).  It
            # bumps the monotonic OVERTAKE counter (obs_admitted) that the
            # starvation oracle snapshots.  Race-free: under _mu.
            self._on_reader_admitted_locked(fast_path=True)
            self._mu.unlock()
            return
        cell = [g, [False]]
        self._rwait.append(cell)
        self._mu.unlock()
        runloom_c.set_wait_reason(runloom_c.WR_LOCK)
        while True:
            runloom_c.park()
            rsync._acquire(self._mu)
            granted = cell[1][0]
            if granted:
                # HANDOFF grant: this reader queued (correctly, because a writer
                # was waiting) and is admitted now only after no writer remains --
                # it did NOT overtake anyone, so it does NOT tick the overtake
                # counter.  It still updates the mutual-exclusion bookkeeping
                # (live_readers / in_write assert), under _mu.
                self._on_reader_admitted_locked(fast_path=False)
                self._mu.unlock()
                return
            self._mu.unlock()

    def runlock(self):
        rsync._resolve_from_fiber("RWMutex.runlock()")
        rsync._acquire(self._mu)
        if self._readers <= 0:
            self._mu.unlock()
            raise RuntimeError("RWMutex.runlock(): read lock not held")
        self._readers -= 1
        self.obs_live_readers -= 1                       # under _mu: race-free
        if self._readers == 0 and self._wwait:
            cell = self._wwait.pop(0)
            self._writer = True
            cell[1][0] = True
            self._mu.unlock()
            cell[0].wake()
        else:
            self._mu.unlock()

    # -- writer side -------------------------------------------------------
    def lock(self):
        rsync._resolve_from_fiber("RWMutex.lock()")
        g = runloom_c.current_g()
        rsync._acquire(self._mu)
        if not self._writer and self._readers == 0:
            self._on_writer_granted_locked(snapshot=self.obs_admitted)
            self._writer = True
            self._mu.unlock()
            return
        cell = [g, [False]]
        self._wwait.append(cell)
        snap = self.obs_admitted                         # under _mu at the EXACT
        self._mu.unlock()                                # moment we queued
        runloom_c.set_wait_reason(runloom_c.WR_LOCK)
        while True:
            runloom_c.park()
            rsync._acquire(self._mu)
            granted = cell[1][0]
            if granted:
                self._on_writer_granted_locked(snapshot=snap)
                self._mu.unlock()
                return
            self._mu.unlock()

    def unlock(self):
        rsync._resolve_from_fiber("RWMutex.unlock()")
        rsync._acquire(self._mu)
        if not self._writer:
            self._mu.unlock()
            raise RuntimeError("RWMutex.unlock(): write lock not held")
        # Clear the write-CS observation while still holding _mu and BEFORE the
        # handoff sets the next waiter's granted flag, so a granted reader/writer
        # that wakes can never see our in_write still set.
        self.obs_in_write = 0
        self.obs_writers_in_cs -= 1
        if self._wwait:                                  # writer-preference first
            cell = self._wwait.pop(0)
            cell[1][0] = True
            self._mu.unlock()
            cell[0].wake()
        elif self._rwait:                                # else hand off all readers
            self._writer = False
            readers, self._rwait = self._rwait, []
            self._readers = len(readers)
            for cell in readers:
                cell[1][0] = True
            self._mu.unlock()
            for cell in readers:
                cell[0].wake()
        else:
            self._writer = False
            self._mu.unlock()

    # -- observations (ALL called with self._mu held) ----------------------
    def _on_reader_admitted_locked(self, fast_path):
        # A reader is entering its critical section.  Writer-preference + mutual
        # exclusion both say no writer may be in_write right now.
        if self.obs_in_write or self.obs_writers_in_cs:
            self._record_mx_breach()
        self.obs_live_readers += 1
        self.obs_readers_admitted += 1
        # The OVERTAKE counter ticks ONLY on a fast-path admit -- the only kind
        # that can pass a queued writer.  A handoff-grant admit (fast_path=False)
        # is a reader that waited its turn behind the writer, so it must NOT count
        # toward any writer's admitted-after-queue delta.
        if fast_path:
            self.obs_admitted += 1

    def _on_writer_granted_locked(self, snapshot):
        # A writer is entering its critical section.  No reader and no other
        # writer may be inside right now.
        if self.obs_live_readers != 0 or self.obs_writers_in_cs != 0 \
                or self.obs_in_write:
            self._record_mx_breach()
        self.obs_in_write = 1
        self.obs_writers_in_cs += 1
        self.obs_writers_granted += 1
        delta = self.obs_admitted - snapshot            # readers let in while queued
        if delta > self.obs_max_after_queue:
            self.obs_max_after_queue = delta

    def _record_mx_breach(self):
        with _AGG:
            self.obs_mx_breach += 1


# Per-critical-section work: a couple of hundred ticks plus a forced cross-hub
# yield, so writers genuinely co-reside with the reader stream and the handoff is
# a real cross-hub wake.
CS_SPINS = 64


def reader(H, wid, rw, rng):
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1
        with rw.rlocked():
            # Inside the read CS: assert no writer is concurrently in_write.  The
            # admit-time check already did the race-free assert under _mu; here we
            # additionally spin + yield to keep this reader resident and widen any
            # overlap window for the flag.
            if rw.obs_in_write:
                with _AGG:
                    rw.obs_mx_breach += 1
            acc = 0
            for k in range(CS_SPINS):
                acc ^= (k * 2654435761) & 0xFFFFFFFF
                if (k & 15) == 0:
                    runloom.yield_now()
        H.op(wid)
        H.task_done(wid)


def writer(H, wid, rw, rng):
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1
        with rw:                                         # rw.lock() / rw.unlock()
            # Inside the write CS: no reader may be live.  (Race-free version is
            # the grant-time assert under _mu; this is a belt-and-braces flag
            # read across the forced cross-hub yield below.)
            if rw.obs_live_readers != 0:
                with _AGG:
                    rw.obs_mx_breach += 1
            for k in range(CS_SPINS):
                if (k & 7) == 0:
                    runloom.yield_now()                  # force cross-hub handoff
            if rw.obs_live_readers != 0:
                with _AGG:
                    rw.obs_mx_breach += 1
        H.op(wid)
        H.task_done(wid)


def worker(H, wid, rng, state):
    rw = state["rw"]
    if wid < state["nwriters"]:
        writer(H, wid, rw, rng)
    else:
        reader(H, wid, rw, rng)


def setup(H):
    # Single shared lock (a real convoy), ~10% writers / ~90% readers, contender
    # count capped to keep the single-lock handoff a tight cross-hub stress.
    n = min(H.funcs, 2000)
    H.funcs = n
    nwriters = max(2, n // 10)
    H.state = {"rw": ObservedRWMutex(), "nwriters": nwriters}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rw = H.state["rw"]
    H.log("writers_granted={0} readers_admitted={1} mx_breach={2} "
          "max_after_queue={3} live_readers_end={4} in_write_end={5}".format(
              rw.obs_writers_granted, rw.obs_readers_admitted, rw.obs_mx_breach,
              rw.obs_max_after_queue, rw.obs_live_readers, rw.obs_in_write))
    # The oracle must actually have run.
    H.check(rw.obs_writers_granted > 0, "no writer was ever granted the lock")
    H.check(rw.obs_readers_admitted > 0, "no reader was ever admitted")
    # (1) MUTUAL EXCLUSION: a reader CS overlapped a writer CS, or two writers
    # overlapped -- a guarded write could be lost.
    H.check(rw.obs_mx_breach == 0,
            "RWMutex mutual exclusion BROKEN: {0} overlap breach(es) "
            "(reader admitted during a write CS, or two writers/readers+writer "
            "overlapped) -- the writer-preference handoff lost an ordering "
            "across hubs".format(rw.obs_mx_breach))
    # (2) STRICT STARVATION BOUND: writer-preference forbids ANY reader admission
    # while a writer is queued, so the bound is exactly 0.
    H.check(rw.obs_max_after_queue == 0,
            "RWMutex writer-preference VIOLATED: up to {0} reader(s) admitted "
            "AFTER a writer queued (strict bound is 0) -- a reader overtook a "
            "waiting writer via a stale `_wwait` check across hubs".format(
                rw.obs_max_after_queue))
    # Lock should be quiescent at teardown (no stranded holder).
    H.check(rw.obs_in_write == 0,
            "RWMutex left in_write set at teardown (writer never released)")


if __name__ == "__main__":
    harness.main("p314_rwmutex_writer_pref", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="shared sync.RWMutex, ~90% readers / ~10% writers; "
                          "mutual-exclusion breach==0 AND strict writer-"
                          "preference starvation bound (readers-admitted-after-"
                          "queue == 0) under cross-hub handoff")
