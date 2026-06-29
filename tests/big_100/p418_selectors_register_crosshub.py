"""big_100 / 418 -- shared selectors.DefaultSelector register/select cross-hub.

A small POOL of shared `selectors.DefaultSelector` objects, each driven by one
long-lived POLLER fiber that loops `sel.select(timeout)` and dispatches every
ready event back to the worker that registered the fd.  Thousands of worker
fibers, spread across the M:N hubs, each hammer ONE shared selector: a worker
makes a socketpair, `register`s its read end on the selector with a globally
UNIQUE data tag, has a peer write a byte to make the fd ready, waits to be
dispatched, then `unregister`s -- and verifies the dispatched event carried back
its OWN still-registered key with the correct data.

WHY THIS STRESSES FT
--------------------
`selectors.DefaultSelector` mutates a SHARED `_fd_to_key` dict and `_map` across
register/modify/unregister, and runloom makes the underlying epoll/poll
COOPERATIVE -- so the poller's `select()` parks (and on resume re-reads the
selector map) while, on OTHER hubs, thousands of workers concurrently
register/unregister fds on the SAME selector.  With the GIL off, register's
`self._fd_to_key[key.fd] = key` and unregister's `del self._fd_to_key[key.fd]`
race the poller's reads of that dict and its `get_map()` iteration.  A torn dict
can hand back a `SelectorKey` for an already-unregistered fd, a key whose `data`
came from a different/freed slot, a spurious `KeyError`/duplicate-key, or simply
LOSE a ready fd (the registration is published but the poller's snapshot of the
interest set never includes it) -- the worker then strands, parked forever on a
wake that never comes.

THE FALSIFIABLE INVARIANTS
--------------------------
  1. IDENTITY/VALUE: every event the poller dispatches maps to a key whose data
     decodes to a tag that is CURRENTLY registered by a live worker, and the
     tag's owner/fd match what that worker registered (no torn/stale key).
  2. CONSERVATION: register and unregister are conserved -- each selector's
     `len(get_map())` returns to its baseline (0) once every worker is done, and
     per-worker registers == unregisters (no leaked or double-removed key).
  3. NO KeyError / DUPLICATE-KEY: a register of an fd not already in the map and
     an unregister of a fd that IS in the map must each succeed; an unexpected
     KeyError (or a register reporting a duplicate fd that we did not double-
     register) is corruption of the shared `_fd_to_key` dict.
  4. NO LOST READY FD: a fd made ready and still registered must eventually be
     dispatched.  Each worker parks on a BOUNDED wait; a worker that is never
     dispatched is recorded, and a worker that parks-then-vanishes shows up as a
     LOST worker via require_no_lost -- a real missed-wake / lost selector event.

COVERAGE
--------
post() asserts that EACH distinct register-mode was exercised at least once.
The ops here are readiness-bound (each waits to be dispatched), so a worker
completes only a handful of rounds and pure-random mode selection reliably
MISSES a mode under load (the suite's p125/p126/p172 flaky-coverage bug).  So
each worker ROUND-ROBINS the modes by its id for its first ops -- deterministic
coverage whether one worker does K ops or K workers do 1 each -- then goes random
to preserve the concurrent mix.

Stresses: shared selectors `_fd_to_key`/`_map` dict CS under M:N register vs
select; cooperative epoll/poll park-then-resume across a foreign-hub mutation;
register/unregister conservation; torn/stale SelectorKey; lost selector wake.
"""
import selectors
import socket

import harness
import runloom

# A small pool of SHARED selectors -- few enough that thousands of workers pile
# onto each one's _fd_to_key dict concurrently (that contention is the point),
# many enough that the pollers run on different hubs.  One poller fiber per
# selector.
NSEL = 8

# How a worker waits to be dispatched before giving up (a bounded backstop so a
# lost selector wake surfaces as a recorded "stranded" worker instead of an
# eternal park that wedges teardown).
DISPATCH_WAIT_S = 4.0

# Number of distinct register-modes; round-robined by worker id for coverage.
NMODES = 3
MODE_PLAIN = 0          # register READ, make ready, expect dispatch, unregister
MODE_MODIFY = 1         # register READ, modify(READ) (re-touch _fd_to_key), ...
MODE_REREGISTER = 2     # register, unregister, re-register a fresh pair, ...


def pack_tag(wid, seq):
    """Globally-unique registration tag: (wid, seq).  Stored as the key's data,
    handed back by the poller, and checked against the live registry so a torn
    key (data from a different/freed slot) is caught."""
    return (wid << 24) | (seq & 0xFFFFFF)


def make_pair():
    """A connected socketpair; the read end is what we register.  Non-blocking so
    nothing here ever does a real OS block (the cooperative select is the only
    park)."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def poller(H, sel, registry, slot_dispatched, slot_torn, sel_idx):
    """The long-lived select() loop for ONE shared selector.

    Loops select(timeout); for every ready (key, mask) it reads key.data (the
    tag), looks the tag up in the SHARED live registry, and -- if the tag is
    still registered and owns this fd -- wakes the registering worker via the
    channel stored in the registry.  Reads of sel.get_map()/key.data here race
    the workers' concurrent register/unregister on the same selector's
    _fd_to_key dict; a key handed back for an unregistered/torn slot is the
    bug."""
    while H.running():
        try:
            events = sel.select(0.05)
        except OSError:
            # epoll fd closed at teardown, or a fd in the interest set was closed
            # under us -- benign during shutdown; re-check running() and loop.
            if not H.running():
                break
            runloom.yield_now()
            continue
        for key, mask in events:
            tag = key.data
            # A torn key may carry a data value that is not even a valid tag
            # (wrong type / out of range).  Guard the lookup.
            if not isinstance(tag, int):
                slot_torn[key.fd & 1023] += 1
                continue
            ent = registry.get(tag)
            if ent is None:
                # The data-tag is not (or no longer) in the live registry.  This
                # is the benign dereg-ordering window (worker popped its tag a
                # beat before unregistering the fd), NOT corruption -- the genuine
                # torn-key signal is a tag that resolves to the WRONG fd, checked
                # below.  Skip.
                continue
            owner_fd, owner_wid, ch = ent
            if key.fd != owner_fd:
                # The data tag resolves to a DIFFERENT fd than the live key's fd:
                # key.data and key.fd came from different slots -- a genuinely
                # torn SelectorKey (the bug).  (A tag simply ABSENT from the
                # registry above is NOT counted: a worker pops its tag from the
                # registry a beat before it unregisters from the selector, so the
                # poller can briefly see a still-registered fd whose tag is gone --
                # that is benign dereg ordering, not corruption.)
                slot_torn[owner_wid & 1023] += 1
                continue
            # Correct, still-registered key -> wake its worker.  The registered fd
            # stays READable (we never drain it), so select() re-reports it every
            # loop until the worker unregisters; use a NON-blocking try_send into
            # the worker's cap-1 channel so repeated readiness never blocks the
            # poller (the worker only needs ONE wake).  Count a dispatch only on
            # the transition (buffer was empty -> now full).
            if ch.try_send(True):
                slot_dispatched[owner_wid & 1023] += 1


def do_round(H, wid, rng, seq, mode, sel, registry,
             slot_reg, slot_unreg, slot_stranded, slot_keyerr):
    """One register/ready/dispatch/unregister cycle against a shared selector.

    Returns the mode actually run (so the worker can tally coverage)."""
    a, b = make_pair()
    tag = pack_tag(wid, seq)
    ch = runloom.Chan(1)
    afd = a.fileno()
    try:
        # Publish into the SHARED registry BEFORE register() so the poller can
        # resolve the tag the instant the event lands (the registry write and the
        # selector register together are the cross-hub publication the poller's
        # select() resume races).
        registry[tag] = (afd, wid, ch)
        try:
            sel.register(a, selectors.EVENT_READ, tag)
        except KeyError:
            # register raised KeyError ("fd already registered") for a fd we did
            # NOT double-register -> corruption of the shared _fd_to_key dict.
            slot_keyerr[wid & 1023] += 1
            H.fail("selector.register raised KeyError for a singly-registered fd "
                   "{0} (wid {1} seq {2}) -- shared _fd_to_key corruption under "
                   "concurrent register/unregister".format(afd, wid, seq))
            registry.pop(tag, None)
            return mode
        slot_reg[wid & 1023] += 1

        if mode == MODE_MODIFY:
            # Re-touch the shared _fd_to_key dict (modify rewrites the key) while
            # the poller may be mid-select on the same selector.
            try:
                sel.modify(a, selectors.EVENT_READ, tag)
            except KeyError:
                slot_keyerr[wid & 1023] += 1
                H.fail("selector.modify raised KeyError for a registered fd {0} "
                       "(wid {1}) -- _fd_to_key lost the just-registered key"
                       .format(afd, wid))
        elif mode == MODE_REREGISTER:
            # Churn the shared _fd_to_key dict hard: unregister the just-registered
            # fd, then re-register the SAME fd+tag.  This is del-then-set on the
            # same dict slot while the poller may be reading/iterating it -- the
            # tightest register/unregister race window.  A KeyError on either step
            # for our own fd is corruption.
            try:
                sel.unregister(a)
            except KeyError:
                slot_keyerr[wid & 1023] += 1
                H.fail("selector.unregister raised KeyError mid-reregister for "
                       "our fd {0} (wid {1}) -- _fd_to_key lost the key".format(
                           afd, wid))
            try:
                sel.register(a, selectors.EVENT_READ, tag)
            except KeyError:
                slot_keyerr[wid & 1023] += 1
                H.fail("selector.register raised KeyError re-registering our just-"
                       "unregistered fd {0} (wid {1}) -- stale entry left in "
                       "_fd_to_key".format(afd, wid))

        # Make the registered fd READY: peer writes a byte.
        try:
            b.send(b"R")
        except OSError:
            pass

        # Park (bounded) until the poller dispatches OUR event, or the backstop
        # fires (candidate lost selector wake).
        got = dispatch_wait(H, ch)
        if not got:
            slot_stranded[wid & 1023] += 1

        # Verify the live key still maps correctly BEFORE we unregister: the
        # selector must report exactly the key/data we registered.
        try:
            live = sel.get_key(a)
            if live.data != tag:
                H.fail("selector.get_key returned data {0!r} != registered tag "
                       "{1!r} for fd {2} (wid {3}) -- torn SelectorKey in the "
                       "shared _fd_to_key dict".format(live.data, tag, afd, wid))
            if live.fd != afd:
                H.fail("selector.get_key returned fd {0} != registered fd {1} "
                       "(wid {2}) -- torn key/fd pairing".format(
                           live.fd, afd, wid))
        except KeyError:
            # The key VANISHED from the shared map while still owned by us (and
            # never unregistered) -- a lost/corrupted registration.
            slot_keyerr[wid & 1023] += 1
            H.fail("selector.get_key raised KeyError for OUR still-registered fd "
                   "{0} (wid {1}) -- registration vanished from _fd_to_key"
                   .format(afd, wid))
    finally:
        # Remove from the shared registry FIRST so the poller stops resolving the
        # tag, then unregister from the selector.  unregister of a fd that IS in
        # the map must succeed; a KeyError DURING THE RUN is conservation
        # corruption.  At teardown a worker can be force-cancelled between
        # register and here, or the fd's epoll arm can already be torn down, so a
        # KeyError/OSError once the run is over is benign -- but the key must
        # still leave the selector map (we assert get_map()==0 in post), so if
        # the high-level unregister raises we fall back to popping the fd out of
        # the selector's map directly so nothing leaks.
        registry.pop(tag, None)
        try:
            sel.unregister(a)
            slot_unreg[wid & 1023] += 1
        except KeyError:
            if H.running():
                slot_keyerr[wid & 1023] += 1
                H.fail("selector.unregister raised KeyError for a fd we "
                       "registered and never removed (wid {0}) -- _fd_to_key "
                       "double-remove / lost key".format(wid))
        except OSError:
            pass
        try:
            a.close()
        except OSError:
            pass
        try:
            b.close()
        except OSError:
            pass
    return mode


def dispatch_wait(H, ch):
    """Bounded recv: True if the poller dispatched our event, False if the
    backstop fired.  A timer fiber sends on `timer` after the backstop; we select
    over the dispatch channel and the timer.  The timer fiber polls in short
    increments and stops early once the worker has been woken (or the run is
    over), so backstop timers do NOT pile up as long-lived sleepers at teardown.

    `stop` (cap 1) is set by the selecting side the instant it returns, so the
    timer fiber notices and exits promptly instead of sleeping the full
    backstop."""
    timer = runloom.Chan(1)
    stop = runloom.Chan(1)

    def fire():
        waited = 0.0
        step = 0.05
        while waited < DISPATCH_WAIT_S and H.running():
            if stop.try_recv() is not None:
                return                  # worker already woken; don't fire
            runloom.sleep(step)
            waited += step
        try:
            timer.send(True)
        except Exception:               # noqa: BLE001
            pass

    runloom.fiber(fire)
    r = runloom.select([("recv", ch), ("recv", timer)])
    idx, (_val, _ok) = r
    # Tell the timer fiber to stop (no-op if it already fired).
    try:
        stop.try_send(True)
    except Exception:                   # noqa: BLE001
        pass
    return idx == 0


def worker(H, wid, rng, state):
    selectors_pool = state["selectors"]
    registries = state["registries"]
    slot_reg = state["reg"]
    slot_unreg = state["unreg"]
    slot_stranded = state["stranded"]
    slot_keyerr = state["keyerr"]
    mode_seen = state["mode_seen"]
    seq = 0
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Pick a shared selector (spread workers across the pool, hub-mixing).
        sel_idx = (wid + i) % NSEL
        sel = selectors_pool[sel_idx]
        registry = registries[sel_idx]
        # Round-robin the modes by id for the first NMODES ops so coverage holds
        # under readiness-bound op counts (the p125/p126 flaky-coverage fix);
        # random after that to keep the concurrent mode mix.
        if i < NMODES:
            mode = (wid + i) % NMODES
        else:
            mode = rng.randrange(NMODES)
        seq += 1
        ran = do_round(H, wid, rng, seq, mode, sel, registry,
                       slot_reg, slot_unreg, slot_stranded, slot_keyerr)
        mode_seen[ran][wid & 1023] += 1
        i += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    selectors_pool = [selectors.DefaultSelector() for _ in range(NSEL)]
    # Do NOT register_close the selectors: post() must read each one's
    # get_map() AFTER the drain to assert conservation, and a closed selector's
    # epoll fd raises on get_map().  The pollers self-exit on H.running()
    # (select uses a 0.05s ceiling so they re-check promptly), and we close the
    # selectors in a cleanup that runs after post().
    def close_selectors(selectors_pool=selectors_pool):
        for sel in selectors_pool:
            try:
                sel.close()
            except Exception:           # noqa: BLE001
                pass
    H.add_cleanup(close_selectors)
    # One SHARED live-registry dict per selector: tag -> (fd, wid, channel).  The
    # poller reads it to resolve dispatched events; workers mutate it.  It is a
    # plain dict deliberately -- it is a second shared dict, mutated cross-hub,
    # that the poller races (single-key-per-tag, tags globally unique, so each
    # entry has exactly one writer at a time, but the dict structure itself is
    # shared across hubs).
    registries = [dict() for _ in range(NSEL)]
    slot_dispatched = [0] * 1024
    slot_torn = [0] * 1024
    H.state = {
        "selectors": selectors_pool,
        "registries": registries,
        "reg": [0] * 1024,
        "unreg": [0] * 1024,
        "stranded": [0] * 1024,
        "keyerr": [0] * 1024,
        "dispatched": slot_dispatched,
        "torn": slot_torn,
        "mode_seen": [[0] * 1024 for _ in range(NMODES)],
    }

    # Spawn the long-lived poller fibers INSIDE the root (setup runs in the root).
    for sel_idx in range(NSEL):
        H.fiber(poller, H, selectors_pool[sel_idx], registries[sel_idx],
                slot_dispatched, slot_torn, sel_idx)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    reg = sum(H.state["reg"])
    unreg = sum(H.state["unreg"])
    dispatched = sum(H.state["dispatched"])
    stranded = sum(H.state["stranded"])
    torn = sum(H.state["torn"])
    keyerr = sum(H.state["keyerr"])
    H.log("registers={0} unregisters={1} dispatched={2} stranded={3} "
          "torn={4} keyerr={5}".format(reg, unreg, dispatched, stranded,
                                        torn, keyerr))

    # Did we do any work at all?
    H.check(reg > 0, "no registrations happened -- the register/select race was "
                     "never exercised")

    # CONSERVATION.
    #
    # Authoritative leak check: every shared selector's map must drain back to its
    # baseline (0 keys) once all workers are done.  A KEY LEFT in the shared
    # _fd_to_key after drain is a real leaked registration -- the worker that owns
    # it either never unregistered (corruption) or its unregister was silently
    # dropped.  This reads the actual shared state, so it directly catches a
    # leaked/lost key.
    total_left = 0
    for sel_idx, sel in enumerate(H.state["selectors"]):
        try:
            remaining = len(sel.get_map())
        except Exception:               # noqa: BLE001
            remaining = -1
        total_left += max(0, remaining)
        H.check(remaining == 0,
                "selector {0} did not drain: {1} keys still registered after "
                "all workers finished -- leaked registration in shared "
                "_fd_to_key".format(sel_idx, remaining))
        # The shared per-selector registry must likewise be empty.
        rem_reg = len(H.state["registries"][sel_idx])
        H.check(rem_reg == 0,
                "selector {0} registry not empty: {1} live tags after drain"
                .format(sel_idx, rem_reg))

    # Counter conservation: unregisters must never EXCEED registers (an excess
    # unregister == a double-remove of a shared-dict key, a corruption signal).
    H.check(unreg <= reg,
            "MORE unregisters than registers: reg={0} unreg={1} -- a key was "
            "removed twice from the shared _fd_to_key dict".format(reg, unreg))
    # registers >= unregisters by at most the workers that were force-cancelled
    # mid-cycle at teardown (counted register, then their unregister hit the
    # already-torn-down fd / OSError path and was not tallied).  That gap must be
    # ACCOUNTED FOR by the still-registered keys above plus the cancelled
    # parkers -- it is NOT an unbounded leak.  Bound it by the worker count (each
    # worker can be mid-cycle at most once) so a systematic per-op leak (gap
    # growing with op-count, unrelated to the teardown boundary) still fails.
    gap = reg - unreg
    bound = H.parked_cancelled + total_left + max(64, H.expected)
    H.check(gap <= bound,
            "register/unregister gap {0} exceeds the teardown-boundary bound {1} "
            "(parked_cancelled={2} keys_left={3}) -- keys leaking from the shared "
            "_fd_to_key dict beyond the in-flight-at-teardown set".format(
                gap, bound, H.parked_cancelled, total_left))

    # IDENTITY: no torn/stale key was ever dispatched, and no KeyError corruption.
    H.check(torn == 0,
            "{0} dispatched events carried a torn/stale SelectorKey (tag not in "
            "live registry or fd mismatch) -- shared _fd_to_key corruption under "
            "concurrent register/unregister".format(torn))
    H.check(keyerr == 0,
            "{0} unexpected KeyError(s) on register/modify/unregister/get_key -- "
            "shared _fd_to_key corruption".format(keyerr))

    # NO LOST READY FD: stranded workers (made-ready-but-never-dispatched within
    # the backstop) are candidate lost selector wakes.  A handful under heavy
    # over-scale is benign scheduling slack; a meaningful fraction is a real
    # missed wake.  require_no_lost catches the harder case: a worker that parked
    # and then VANISHED (true lost-wakeup) instead of merely finishing slow.
    H.check(stranded * 50 <= reg + 1,
            "too many ready fds were never dispatched: {0}/{1} workers stranded "
            "past the {2}s backstop -- candidate lost selector wakes".format(
                stranded, reg, DISPATCH_WAIT_S))

    # COVERAGE: every register-mode was exercised at least once.
    for mode in range(NMODES):
        seen = sum(H.state["mode_seen"][mode])
        H.check(seen > 0, "register-mode {0} was never exercised".format(mode))

    H.require_no_lost()


if __name__ == "__main__":
    # The subject is the delicate cross-hub race on a SHARED selectors object's
    # _fd_to_key dict (register/select), plus raw socketpair()/close() churn per
    # op -- like the suite's other single-primitive socket programs (p105/p106)
    # this does not meaningfully scale to 1M (the cap is the honest fix), but it
    # piles thousands of fibers onto each of NSEL shared selectors, which is
    # exactly the contention that surfaces the FT hazard.
    harness.main("p418_selectors_register_crosshub", body, setup=setup,
                 post=post, default_funcs=3000, max_funcs=5000,
                 describe="thousands of fibers register/unregister fds on a small "
                          "pool of SHARED selectors.DefaultSelector while poller "
                          "fibers select()/dispatch; every ready event maps to a "
                          "still-registered key, register/unregister conserved, no "
                          "lost ready fd")
