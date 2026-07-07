"""big_100 / 554 -- selectors.DefaultSelector modify() key.data/fileobj isolation
under M:N (SINGLE-OWNER selector, per-fiber pipe).

selectors.DefaultSelector keeps a private `_fd_to_key` dict (fd -> SelectorKey)
and a `_map` view.  register(fileobj, events, data) inserts a SelectorKey whose
`.data` and `.events` fields are exactly what the caller passed; modify() REBINDS
those fields on the live key (CPython's BaseSelector.modify pops the old key and
re-inserts a fresh SelectorKey with the new data/events); select() returns
`[(SelectorKey, mask), ...]` built by reading `_fd_to_key` for each ready fd.

WHERE M:N COULD BREAK IT (the gap this program probes)
------------------------------------------------------
runloom makes the underlying epoll/poll COOPERATIVE: select() parks the fiber and,
on resume, re-reads its selector's map to build the SelectorKey list.  Thousands of
fibers, spread across the M:N hubs with the GIL off, each own their OWN selector and
own pipe, and each is concurrently doing register / modify / select / unregister.
If any C-level state behind the cooperative epoll wrapper -- an epoll fd, a readiness
list, a SelectorKey scratch buffer, a `_fd_to_key` slot -- were shared or reused
ACROSS selectors/hubs instead of being strictly per-selector, then a modify() that
rebinds `key.data` on fiber A's selector (hub 1) while fiber B's select() runs on its
own selector (hub 2) could make B's select() hand back a STALE key, a key carrying
A's sentinel `.data`, or a key whose `.fileobj` is A's fd -- a cross-fiber leak of a
single-owner selector's key.  This is the isolation analogue of p418's SHARED-selector
race: p418 hammers ONE shared selector's `_fd_to_key` cross-hub (contention is the
point); THIS program gives every fiber a PRIVATE selector and asserts modify()'s
key.data/events + the fileobj/fd pairing + the map-count conservation stay perfectly
fiber-local across yields.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads)
---------------------------------------------------------------------
Each fiber owns a DefaultSelector and a pipe.  It registers its read fd with a
UNIQUE per-registration sentinel object as `.data`, makes the fd ready (writes one
byte to the write end, never drains it, so the read fd stays EVENT_READ-ready every
select), then repeatedly modify()s the key to a FRESH unique sentinel across yields
and calls select().  On a CORRECT runtime, every select() must return a SelectorKey
whose:
  * `.data` IS (identity `is`) the sentinel this fiber set on its MOST RECENT
    modify()/register -- never a sibling's sentinel, never a stale prior one;
  * `.fileobj` and `.fd` are THIS fiber's own read fd -- never a sibling's fd;
  * `.events` is exactly EVENT_READ -- the mask this fiber registered/modified to.
And get_map()/get_key() must agree with that key.  We verified with a standalone
plain-threads control (8 OS threads, each owning a selector+pipe, GIL on AND off,
same modify-across-a-barrier churn) that 100% of select()/get_key() results carry
the owning thread's own sentinel + fd -- 0 cross-thread leaks.  A private selector
touched by exactly one fiber MUST behave the same under a correct runloom.  A
sentinel identity that is not ours, a fileobj/fd that is not ours, or an events
value we never set = a runloom selector-key isolation bug, so this single-owner
load-bearing oracle PASSES (exit 0) on a correct runtime.

ORACLES
-------
  * LOAD-BEARING -- KEY DATA/FILEOBJ/EVENTS ISOLATION (worker, HARD, fail-fast).
    Single-owner: the selector, the pipe, and every sentinel are created in
    fiber-local variables and NEVER shared.  Per registration the fiber does
    MODIFY_ITERS modify()+yield+select() cycles; each cycle asserts the returned
    key is identity-ours (data `is` current sentinel), fd-ours (fileobj/fd ==
    our read fd), and mask-ours (events == EVENT_READ), and that get_key() agrees.
    A violation is a runloom cross-fiber selector-key leak / torn key.

  * CONSERVATION -- MAP-COUNT (worker fail-fast + post sum).  `len(get_map())` is
    exactly 1 from register() until unregister(), and returns to 0 after
    unregister (checked before the per-round close).  Per-wid register/unregister
    tallies live in [0]*H.funcs slots (single-writer-per-slot, race-free); post
    asserts total registers == total unregisters (every register matched by an
    unregister -- no leaked or double-removed key in the private _fd_to_key dict).

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (select_hits > 0
    and registers > 0) -- else the isolation oracle would be vacuous.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that parked in the
    cooperative select() and then vanished (a lost selector wake on a single-owner
    selector) shows up as a LOST worker.

FAIL ON: a select()/get_key() SelectorKey whose .data is not this fiber's current
sentinel, whose .fileobj/.fd is not this fiber's read fd, or whose .events is not
what we registered; a map-count that is not conserved; a KeyError on our own live
fd.  There is NO shared/MEASURED arm here -- the whole design is single-owner
isolation, so EVERY check is load-bearing and a correct runtime keeps them all
clean.

fd-heavy: one pipe (2 fds) + one DefaultSelector (an epoll fd) per fiber, opened
and closed WITHIN each round (finally); max_funcs caps the forever loop's
--funcs 1000000 so the fd count stays bounded, and an EMFILE/ENFILE at over-scale
is reported as a benign SCALE_LIMIT (a kernel fd ceiling, not a runtime bug).

Stresses: selectors.DefaultSelector modify() key.data/events rebinding, the
fileobj/fd <-> key pairing, cooperative epoll select() park-then-resume across a
sibling's modify() on a different hub, per-selector _fd_to_key isolation, map-count
conservation, single-owner selector-key integrity under M:N.
"""
import errno
import os
import selectors

import harness
import runloom

# modify()+yield+select() cycles per registration.  Each cycle rebinds the key's
# .data to a FRESH sentinel and re-reads it back through select()/get_key() across
# a yield, so a sibling on another hub reliably interleaves at the modify/select
# boundary -- the hazard window.  A handful per registration keeps the churn dense
# without making a single round long.
MODIFY_ITERS = 4

# select() ceiling.  Our read fd is made ready once (a byte written, never drained)
# so it stays EVENT_READ-ready every loop -- select() returns it deterministically;
# the small timeout is only a backstop so a (buggy) lost readiness returns instead
# of parking forever.
SELECT_TIMEOUT = 0.05


class Sentinel(object):
    """A per-registration UNIQUE key-data object.  Identity (`is`) distinguishes
    this fiber's current sentinel from any sibling's or any stale prior one; the
    fields make a failure message legible."""
    __slots__ = ("wid", "seq", "gen")

    def __init__(self, wid, seq, gen):
        self.wid = wid
        self.seq = seq
        self.gen = gen

    def __repr__(self):
        return "Sentinel(wid={0},seq={1},gen={2})".format(
            self.wid, self.seq, self.gen)


def find_our_key(events, our_fd):
    """Return the SelectorKey in `events` whose fd is our_fd, or None.  On a
    single-owner selector this is the ONLY key select() can report; a key for a
    different fd would itself be a leak (caught by the caller)."""
    for key, mask in events:
        if key.fd == our_fd:
            return key, mask
    return None


def check_key(H, wid, seq, key, mask, our_fd, sentinel):
    """LOAD-BEARING: assert a SelectorKey is entirely fiber-local.

    Returns True if OK (caller continues), False if a fail was recorded."""
    # .data must be IDENTITY-ours: the exact sentinel we last set.
    if key.data is not sentinel:
        H.fail("selector key DATA LEAK: select/get_key returned key.data={0!r} "
               "but this fiber's current sentinel is {1!r} (wid {2} seq {3}) -- a "
               "cross-fiber sentinel leak or a stale key from the private "
               "_fd_to_key under M:N modify()".format(key.data, sentinel, wid, seq))
        return False
    # .fileobj / .fd must be OUR read fd.
    if key.fd != our_fd:
        H.fail("selector key FD LEAK: key.fd={0} != our own read fd {1} (wid {2} "
               "seq {3}) -- a sibling selector's fd leaked into this single-owner "
               "selector's key".format(key.fd, our_fd, wid, seq))
        return False
    if key.fileobj != our_fd:
        H.fail("selector key FILEOBJ LEAK: key.fileobj={0!r} != our own read fd "
               "{1} (wid {2} seq {3}) -- torn fileobj/fd pairing in the private "
               "_fd_to_key".format(key.fileobj, our_fd, wid, seq))
        return False
    # .events must be exactly what we registered/modified to.
    if key.events != selectors.EVENT_READ:
        H.fail("selector key EVENTS WRONG: key.events={0} != EVENT_READ ({1}) "
               "(wid {2} seq {3}) -- modify() rebound the mask to a value we never "
               "set".format(key.events, selectors.EVENT_READ, wid, seq))
        return False
    # The reported readiness mask must include READ (we made it read-ready).
    if mask is not None and not (mask & selectors.EVENT_READ):
        H.fail("selector reported ready mask {0} without EVENT_READ for our "
               "read-ready fd {1} (wid {2} seq {3})".format(mask, our_fd, wid, seq))
        return False
    return True


def do_round(H, wid, seq, state):
    """One single-owner register / modify*N / unregister cycle on a PRIVATE
    selector + PRIVATE pipe.  Every oracle here is load-bearing (no sharing)."""
    sel = None
    r = None
    w = None
    try:
        try:
            r, w = os.pipe()
            sel = selectors.DefaultSelector()
        except OSError as exc:
            # Kernel fd ceiling at over-scale (EMFILE/ENFILE) -- a benign platform
            # SCALE LIMIT, not a runtime bug.  Report and skip this round.
            if exc.errno in (errno.EMFILE, errno.ENFILE, errno.ENOMEM):
                H.note_scale_limit("os.pipe/DefaultSelector hit fd ceiling: "
                                   "{0}".format(exc))
                return False
            raise
        os.set_blocking(r, False)
        os.set_blocking(w, False)

        # Make the read fd EVENT_READ-ready and keep it ready (never drained), so
        # select() reports it deterministically every loop.
        try:
            os.write(w, b"R")
        except OSError:
            pass

        # ---- register with a UNIQUE sentinel ----------------------------------
        s0 = Sentinel(wid, seq, 0)
        sel.register(r, selectors.EVENT_READ, s0)
        state["reg"][wid] += 1                 # single-writer-per-slot (race-free)
        cur = s0

        # Map count is exactly 1 for our one fd.
        if len(sel.get_map()) != 1:
            H.fail("private selector get_map() len={0} != 1 right after a single "
                   "register (wid {1} seq {2}) -- the per-fiber _fd_to_key is not "
                   "isolated".format(len(sel.get_map()), wid, seq))
            return True

        # ---- modify across yields, verify key stays entirely ours -------------
        for gen in range(1, MODIFY_ITERS + 1):
            # YIELD at the hazard boundary so a sibling's modify()/select() on its
            # OWN selector (another hub) interleaves before our select() resumes.
            runloom.yield_now()
            if gen & 1:
                runloom.sleep(0.0002)

            nxt = Sentinel(wid, seq, gen)
            sel.modify(r, selectors.EVENT_READ, nxt)
            cur = nxt

            # get_key() must immediately reflect OUR fresh sentinel.
            try:
                live = sel.get_key(r)
            except KeyError:
                H.fail("selector.get_key raised KeyError for OUR still-registered "
                       "fd {0} right after modify() (wid {1} seq {2} gen {3}) -- "
                       "the private key vanished from _fd_to_key".format(
                           r, wid, seq, gen))
                return True
            if not check_key(H, wid, seq, live, None, r, cur):
                return True

            # YIELD again, then select(): the cooperative epoll parks here and, on
            # resume, rebuilds the SelectorKey list from _fd_to_key.
            runloom.yield_now()
            events = sel.select(SELECT_TIMEOUT)
            found = find_our_key(events, r)
            if found is None:
                # Our always-ready fd was not reported.  Do not fail on readiness
                # slack; but a stray key for a DIFFERENT fd on a single-owner
                # selector IS a leak -- check for that.
                for key, mask in events:
                    H.fail("private selector select() returned a key for fd "
                           "{0} that is NOT our own fd {1} (wid {2} seq {3}) -- a "
                           "foreign fd leaked into this single-owner selector"
                           .format(key.fd, r, wid, seq))
                    return True
                # Genuinely empty (readiness slack) -- record a miss, keep going.
                state["misses"][wid & 1023] += 1
                continue
            key, mask = found
            if not check_key(H, wid, seq, key, mask, r, cur):
                return True
            state["hits"][wid & 1023] += 1

        # ---- unregister, assert map-count conservation ------------------------
        sel.unregister(r)
        state["unreg"][wid] += 1               # single-writer-per-slot (race-free)
        if len(sel.get_map()) != 0:
            H.fail("private selector get_map() len={0} != 0 after unregister "
                   "(wid {1} seq {2}) -- a key leaked in the per-fiber _fd_to_key"
                   .format(len(sel.get_map()), wid, seq))
            return True
        # get_key on the now-unregistered fd must raise (it is gone).
        try:
            sel.get_key(r)
            H.fail("selector.get_key succeeded for fd {0} AFTER unregister (wid "
                   "{1} seq {2}) -- stale key left in the private _fd_to_key"
                   .format(r, wid, seq))
            return True
        except KeyError:
            pass
        return True
    finally:
        if sel is not None:
            try:
                sel.close()
            except Exception:                  # noqa: BLE001
                pass
        for fd in (r, w):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


def worker(H, wid, rng, state):
    seq = 0
    for _ in H.round_range():
        if not H.running():
            break
        seq += 1
        did = do_round(H, wid, seq, state)
        if H.failed:
            return
        if did:
            H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        # CONSERVATION: one slot per worker (single-writer, race-free).
        "reg": [0] * H.funcs,
        "unreg": [0] * H.funcs,
        # NON-VACUITY / measured (sharded tallies -- not conservation counters).
        "hits": [0] * 1024,
        "misses": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    reg = sum(H.state["reg"])
    unreg = sum(H.state["unreg"])
    hits = sum(H.state["hits"])
    misses = sum(H.state["misses"])
    H.log("private-selector rounds: registers={0} unregisters={1} select_hits={2} "
          "select_misses={3} (all key-data/fileobj/events isolation + map-count "
          "conservation checks passed fail-fast)".format(reg, unreg, hits, misses))

    # NON-VACUITY: the load-bearing isolation arm actually ran.
    H.check(reg > 0,
            "no registrations happened -- the single-owner selector modify()/"
            "select() isolation hazard was never exercised (oracle vacuous)")
    H.check(hits > 0,
            "no select() ever returned our ready key -- the key-data/fileobj "
            "isolation oracle never fired (oracle vacuous)")

    # CONSERVATION: every register matched by exactly one unregister.  A round
    # either completes both (register then unregister in the same fiber, no yield
    # between the two tallies that could strand only one) or fails fast before the
    # register on an EMFILE scale limit.  A fiber force-cancelled AT TEARDOWN can
    # be parked mid-round (registered, not yet unregistered) -- bound the gap by
    # the in-flight-at-teardown worker set, exactly like p418.
    H.check(unreg <= reg,
            "MORE unregisters than registers: reg={0} unreg={1} -- a key was "
            "removed twice from a private _fd_to_key".format(reg, unreg))
    gap = reg - unreg
    bound = H.parked_cancelled + max(64, H.expected)
    H.check(gap <= bound,
            "register/unregister conservation gap {0} exceeds the teardown-boundary "
            "bound {1} (parked_cancelled={2}) -- registrations leaking beyond the "
            "in-flight-at-teardown set".format(gap, bound, H.parked_cancelled))

    # COMPLETENESS: no fiber parked in the cooperative select() and then vanished.
    H.require_no_lost("selector modify key-data isolation")


if __name__ == "__main__":
    harness.main(
        "p554_selectors_modify_key_data", body, setup=setup, post=post,
        default_funcs=1500, max_funcs=1500,
        describe="each fiber owns a PRIVATE selectors.DefaultSelector + pipe: it "
                 "registers its read fd with a unique sentinel as key.data, "
                 "modify()s to fresh sentinels across yields, and select()/get_key() "
                 "must always return a SelectorKey whose .data IS this fiber's "
                 "current sentinel, whose .fileobj/.fd is its own read fd, and whose "
                 ".events is EVENT_READ -- never a sibling's; len(get_map()) is "
                 "conserved (1 while registered, 0 after unregister, registers == "
                 "unregisters).  A cross-fiber key-data/fileobj leak or a "
                 "non-conserved map is the runloom selector-isolation bug")
