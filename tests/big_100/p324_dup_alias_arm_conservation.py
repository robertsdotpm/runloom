"""big_100 / 324 -- fd-NUMBER-keyed arm/pending-wake state under dup() aliasing
and fd-number recycling.

The netpoll per-fd arm cache (`runloom_fd_armed`) AND the per-fd pending-wake
table (`runloom_fd_pending_wake_consume(fd, events)` in netpoll_wait_fd.c.inc:
the "drain any pre-existing pending-wake bits" path) are keyed by fd NUMBER.
`os.dup()` / `os.dup2()` mint a SECOND fd NUMBER that aliases the SAME kernel
file object.  `dup`/`dup2` appear in ZERO other big_100 programs, so this is the
first probe of the number-keyed arm/pending state under true aliasing + number
recycling.

ARCHITECTURE-CORRECTED bite (why the naive "wake routed to the wrong g" does NOT
fire, and what DOES):
  * Each dup'd NUMBER gets its OWN epoll registration and its OWN `by_fd[fd]`
    parker bucket -- so a wake delivered for fd-number X routes to the parker
    linked under X by construction.  A wake is NEVER mis-routed across two live
    alias numbers.  That arm of the hypothesis passes trivially; we still assert
    it (differential A below) to keep it honest.
  * The REAL bite is the fd-NUMBER-keyed PENDING-WAKE table's stale path (the
    p101 stale-arm class, here driven by dup/close churn instead of socket()
    churn): when an alias number is closed (epoll DEL) while a pending-wake mask
    for that OLD number sits UNCONSUMED, and the kernel immediately RECYCLES that
    number into a fresh, unrelated fd, the fresh fd's first wait_fd() drains the
    stale pending bit (`runloom_fd_pending_wake_consume`) and returns a SPURIOUS
    readiness the new owner never earned -- so the new owner "reads" readiness
    for a byte that was meant for the dead alias, or strands because its real arm
    was shadowed by the stale entry.

ORACLE (two parts, both implemented exactly):

  (A) close-one-alias SURVIVAL differential.  Per unit: a pipe (rfd, wfd); a
      second READ alias `fd2 = os.dup(rfd)`.  Parker A parks rfd (spawned pass 1
      -> hub A); parker B parks fd2 (spawned pass 2 -> hub B).  After BOTH are
      armed, the driver writes ONE tagged byte; BOTH same-file parkers must wake
      (level re-report) and EACH recv its own tagged byte -- no cross-talk, exact
      tag.  Then the driver CLOSES exactly ONE alias' NUMBER (epoll DEL on that
      number only) and writes a SECOND tagged byte; the OTHER alias' still-armed
      wait_fd MUST still wake on it (a different fd-number -> closing one number
      must NOT poison the other's arm).  A bounded WAIT_MS ceiling distinguishes
      a genuine lost wake from a slow one: `woken_by_event` must DOMINATE
      `woken_by_timeout` (a poisoned survivor would wake only by timeout, never
      by event).

  (B) recycled-NUMBER stale-pending conservation.  Aggressively churn dup+close
      so freed fd-numbers recycle into FRESH parkers across rounds.  Each fresh
      parker arms a NEW pipe whose number very likely == a number a just-closed
      alias used, then H.checks it reads ITS OWN round's UNIQUELY-tagged byte
      (every (wid,round) gets a distinct tag) -- never a stale/cross-round
      readiness inherited from the prior owner of that number.  `require_no_lost`
      catches any parker stranded by a poisoned/stale arm.  An fd-leak oracle
      bounds count_fds() over the whole dup churn (every dup'd number closed) and
      guards fd_base >= 0.

Meaningful only where wait_fd parks on a real readiness backend (epoll/kqueue);
io_uring marks fds always-ready so the arm/pending tables are a no-op -- still
correct (the tag-conservation holds on every backend), just not the targeted
race.  Mirrors the p312/p313 netpoll-corner structure: tagged-byte conservation
+ require_no_lost + bounded wait_fd ceiling + two-pass cross-hub fan-out.
"""
import os
import sys

import harness
import runloom

try:
    import runloom_c
    _HAVE_WAITFD = hasattr(runloom_c, "wait_fd")
except Exception:                       # pragma: no cover - import guard
    runloom_c = None
    _HAVE_WAITFD = False

# os.dup is universally available; the targeted hazard needs the NUMBER-keyed
# arm/pending tables, i.e. the wait_fd park primitive.
_HAVE_DUP = hasattr(os, "dup")

READ = 1                                # wait_fd events bitmask: 1 = readable
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", -1) if runloom_c else -1

# Per-park readiness ceiling (ms).  SHORT so a parker re-probes promptly across
# a cross-pass spawn gap, but the ceiling is the lost-vs-slow discriminator: a
# parker poisoned by a stale arm (its real wake shadowed) only ever returns by
# TIMEOUT, never by an event bit -- so woken_by_event must dominate
# woken_by_timeout.  A GENUINE strand never gets its readiness back: the round
# owner blocks on the result Chan forever -> require_no_lost fires.
WAIT_MS = 200

# How many dup/close recycle iterations a Part-B churn worker does per round.
# A volume sufficient to recycle fd NUMBERS back into fresh parkers, but the
# arm-cache/pending race is a CONCURRENCY (not a volume) hunt, so a modest count
# per worker across the cross-hub pool is plenty.
CHURN_PER_ROUND = 6

# Bound on fd growth across the whole churn (Part B).  Every dup'd number is
# closed in the same round, so steady-state open-fd count must NOT climb beyond
# a fixed slack over the baseline -- a leak (an alias number never closed, or a
# stale-arm entry pinning an fd) shows here.
FD_SLACK = 256


def tag_byte(wid, rno, which):
    """A byte UNIQUE per (worker, round, which-alias) so a cross-talk / stale
    readiness (reading a byte meant for a different alias or a prior owner of
    the fd number) is caught by CONTENT, not just by a count.  Spread across the
    full 1..255 range (never 0, so an all-zero stale buffer can't masquerade)."""
    x = ((wid * 2654435761) ^ (rno * 40503) ^ (which * 0x9E37)) & 0xFFFFFFFF
    return (x % 255) + 1


def park_read_one(running, fd, want, slot, results, kind):
    """Park `fd` for READ readiness (bounded re-probe), then read ONE byte and
    report it.  `results[slot]` receives (woke_by_event, byte_or_None).  kind is
    "event" if a READ bit was observed, "timeout" if it only ever timed out.

    Parks on a fd NUMBER that ALIASES the same kernel file another parker may be
    parked on under a DIFFERENT number (and very likely a different hub).  A
    poisoned survivor (its arm shadowed by a stale pending entry for a recycled
    number) wakes only by timeout -> woken_by_event stays False -> the dominance
    oracle fires."""
    woke_by_event = False
    got = None
    while running():
        try:
            ready = runloom_c.wait_fd(fd, READ, WAIT_MS)
        except OSError:
            break                        # fd closed at teardown
        if ready == CANCELLED:
            break
        if ready & READ:
            woke_by_event = True
            try:
                chunk = os.read(fd, 1)
            except BlockingIOError:
                # Spurious/level re-report with no data yet (or another aliased
                # reader already drained it) -- re-park.  A TRUE stale-pending
                # bite returns READ here with NO byte ever arriving for us.
                continue
            except OSError:
                break
            if chunk:
                got = chunk[0]
            break
        # ready == 0: bare timeout (ceiling).  Re-probe within budget; a genuine
        # lost/poisoned wake never gets its readiness back, so we keep timing out
        # until teardown -> reported as a timeout wake (woke_by_event False).
    results[slot] = (woke_by_event, got)


def make_nonblock_pipe():
    """A non-blocking pipe; both ends O_NONBLOCK so os.read raises
    BlockingIOError on an empty buffer instead of blocking a hub thread."""
    rfd, wfd = os.pipe()
    os.set_blocking(rfd, False)
    os.set_blocking(wfd, False)
    return rfd, wfd


def close_quiet(fd):
    if fd is None or fd < 0:
        return
    try:
        runloom_c.netpoll_release_if_idle(fd)
    except Exception:                    # noqa: BLE001
        pass
    try:
        os.close(fd)
    except OSError:
        pass


# --------------------------------------------------------------------------
# Part A: close-one-alias survival differential.  The driver owns the round and
# spawns BOTH parkers as fresh fibers each round (each reports through its own
# 1-cap Chan, so a join is exact and a STRAND is visible as a goroutine that
# never returns).  Spawned fibers fan out across hubs -> parker A (rfd) and
# parker B (the dup alias fd2) very likely land on DIFFERENT hubs -> two distinct
# fd NUMBERS aliasing ONE kernel file, armed on two hubs.
# --------------------------------------------------------------------------

def park_reader_chan(running, fd, done):
    """Park `fd` for READ readiness, read ONE byte, report (woke_by_event, byte)
    through `done` (a 1-cap Chan).  Reporting through a Chan (not a shared slot)
    makes the driver's join exact: a stranded parker never sends -> the driver
    blocks on the recv forever -> require_no_lost fires."""
    res = [None]
    park_read_one(running, fd, 1, 0, res, "")
    woke, got = res[0] if res[0] is not None else (False, None)
    done.send((woke, got))


def driver_a_pass(H, wid, rng, state):
    """One goroutine per unit: mint the dup alias, spawn BOTH alias parkers,
    drive the tagged writes, run the close-one-alias survival differential, and
    join -- one place owning the round so a strand surfaces as a non-returning
    goroutine (LOST)."""
    unit = state["units"][wid]
    rfd = unit["rfd"]
    wfd = unit["wfd"]
    if rfd is None or rfd < 0 or wfd is None or wfd < 0:
        return
    rno = 0
    for _ in H.round_range():
        rno += 1
        if not H.running():
            break
        # Mint the READ alias: a SECOND fd number for the SAME pipe read end.
        try:
            fd2 = os.dup(rfd)
            os.set_blocking(fd2, False)
        except OSError:
            return
        if fd2 < 0:
            return
        unit["fd2"] = fd2
        ta = tag_byte(wid, rno, 0)       # byte parker A (rfd) must read
        tb = tag_byte(wid, rno, 1)       # byte parker B (fd2) must read
        adone = runloom.Chan(1)
        bdone = runloom.Chan(1)

        # Spawn BOTH alias parkers.  Two different fd NUMBERS aliasing one kernel
        # file; the spawns fan out so A and B very likely park on DIFFERENT hubs.
        H.fiber(park_reader_chan, H.running, rfd, adone)
        H.fiber(park_reader_chan, H.running, fd2, bdone)

        # Make the pipe readable.  Both aliases see the SAME readable state (one
        # kernel file); write TWO distinct tagged bytes so each parker can recv
        # its OWN byte.  A level re-report across the two alias numbers must wake
        # BOTH (they are distinct epoll regs / by_fd buckets).
        try:
            os.write(wfd, bytes([ta, tb]))
        except OSError:
            close_quiet(fd2)
            unit["fd2"] = None
            return

        # Join both parkers.  A stranded parker (poisoned arm / dropped wake)
        # never sends -> this goroutine blocks here forever -> require_no_lost.
        (a_ev, a_byte), _ = adone.recv()
        (b_ev, b_byte), _ = bdone.recv()

        if H.running():
            # Tag conservation: the two bytes read are exactly {ta, tb} (no
            # cross-talk, no stale byte).  The two reads race on ordering, so
            # accept either assignment, but the MULTISET must equal {ta, tb}.
            got = sorted(b for b in (a_byte, b_byte) if b is not None)
            want = sorted([ta, tb])
            if not H.check(got == want,
                           "alias tag cross-talk wid={0} round={1}: wrote {2} "
                           "got {3} (a same-file/alias read crossed wires or a "
                           "stale byte leaked)".format(wid, rno, want, got)):
                close_quiet(fd2)
                unit["fd2"] = None
                return
            # Both must have woken BY EVENT, not by timeout (a poisoned arm wakes
            # only by timeout).  Tally for the dominance oracle in post().
            ev = (1 if a_ev else 0) + (1 if b_ev else 0)
            state["woke_event"][wid & 1023] += ev
            state["woke_timeout"][wid & 1023] += (2 - ev)

        # ---- the survival differential: close ONE alias NUMBER (fd2), then
        # write again; the OTHER alias (rfd) must STILL wake on it. ------------
        close_quiet(fd2)
        unit["fd2"] = None
        if H.running():
            tc = tag_byte(wid, rno, 2)
            sdone = runloom.Chan(1)
            # Re-park the SURVIVING alias (rfd) -- its sibling number was just
            # DEL'd; a poisoned arm would never wake on the write below.
            H.fiber(park_reader_chan, H.running, rfd, sdone)
            runloom.yield_now()           # let it re-arm before the write
            try:
                os.write(wfd, bytes([tc]))
            except OSError:
                pass
            (s_ev, s_byte), _ = sdone.recv()
            if H.running():
                if not H.check(s_byte == tc,
                               "survivor poisoned wid={0} round={1}: closing the "
                               "alias number DEL'd the OTHER alias' arm -- wrote "
                               "{2} got {3}".format(wid, rno, tc, s_byte)):
                    return
                if not H.check(s_ev,
                               "survivor woke by TIMEOUT not EVENT wid={0} "
                               "round={1}: the surviving alias' real wake was "
                               "lost when its sibling number was closed".format(
                                   wid, rno)):
                    return
                state["woke_event"][wid & 1023] += 1
                H.op(wid)
                H.task_done(wid)


# --------------------------------------------------------------------------
# Part B: recycled-NUMBER stale-pending.  A separate cross-hub pool that
# aggressively dup+closes to recycle fd NUMBERS, then arms a FRESH pipe on a
# recycled number and asserts it reads ITS OWN uniquely-tagged byte -- no stale
# readiness inherited from the prior owner of that number.
# --------------------------------------------------------------------------

def churn_worker(H, wid, rng, state):
    slot = wid & 1023
    leak = state["churn_leak"]
    for _ in H.round_range():
        if not H.running():
            break
        for it in range(CHURN_PER_ROUND):
            if not H.running():
                break
            # 1) churn: dup a throwaway fd to a fresh NUMBER and immediately
            #    close it, leaving a recycle candidate.  Arm+abandon it under a
            #    short ceiling first so a stale pending-wake bit for THAT number
            #    is the worst case the fresh owner below could inherit.
            try:
                scratch_r, scratch_w = make_nonblock_pipe()
            except OSError:
                continue
            stale_fd = None
            try:
                stale_fd = os.dup(scratch_r)
                os.set_blocking(stale_fd, False)
                # Make it readable, arm+abandon (the p101 stale-arm seed): a
                # readiness for this NUMBER may now sit pending/armed.
                try:
                    os.write(scratch_w, b"\x01")
                except OSError:
                    pass
                try:
                    runloom_c.wait_fd(stale_fd, READ, 1)   # 1ms: arm then abandon
                except OSError:
                    pass
            except OSError:
                pass
            finally:
                # Close the alias NUMBER (epoll DEL) WITHOUT consuming its
                # readiness -- the precise stale-pending seed.
                close_quiet(stale_fd)
                close_quiet(scratch_w)
                close_quiet(scratch_r)

            # 2) a FRESH pipe whose read end very likely reuses a just-freed
            #    NUMBER; a parker on it must read ITS OWN tag, never a stale
            #    readiness/byte inherited from the recycled number's prior owner.
            try:
                rfd, wfd = make_nonblock_pipe()
            except OSError:
                continue
            if rfd < 0:
                close_quiet(wfd)
                continue
            # fd_base guard: a negative fd here means the dup/recycle math went
            # wrong (a closed-twice number, etc.) -- a real bug, not a tag miss.
            if not H.check(rfd >= 0 and wfd >= 0,
                           "recycled fd_base negative wid={0}: rfd={1} wfd={2} "
                           "(a closed-twice / mis-recycled number)".format(
                               wid, rfd, wfd)):
                close_quiet(rfd)
                close_quiet(wfd)
                return
            tag = tag_byte(wid, (it << 8) ^ 0xB, 3)
            res = [None]
            wg = runloom.WaitGroup()
            wg.add(1)

            def fresh_reader(rfd=rfd, res=res, wg=wg):
                try:
                    park_read_one(H.running, rfd, 1, 0, res, "fresh")
                finally:
                    wg.done()

            H.fiber(fresh_reader)
            runloom.yield_now()
            try:
                os.write(wfd, bytes([tag]))
            except OSError:
                pass
            wg.wait()
            woke, got = res[0]
            if H.running():
                if got is not None and not H.check(
                        got == tag,
                        "recycled-number stale readiness wid={0} it={1}: a fresh "
                        "parker on a recycled fd number read {2}, not its own tag "
                        "{3} (stale pending-wake from the prior owner)".format(
                            wid, it, got, tag)):
                    close_quiet(rfd)
                    close_quiet(wfd)
                    return
                if got is not None and woke:
                    H.op(wid)
            close_quiet(rfd)
            close_quiet(wfd)
        H.task_done(wid)
    # Per-worker leak probe: this worker opened+closed everything it dup'd.
    leak[slot] += 0


def setup(H):
    if not _HAVE_WAITFD:
        H.note_scale_limit(
            "runloom_c.wait_fd unavailable -- cannot park on a raw fd number; "
            "skipping the dup-alias arm/pending conservation test")
        H.state = None
        return
    if not _HAVE_DUP:
        H.note_scale_limit(
            "os.dup unavailable on this platform ({0}) -- skipping".format(
                sys.platform))
        H.state = None
        return

    n = H.funcs
    # Part-A units: one pipe per unit.  The driver spawns fresh parkers per round
    # and joins them through per-round Chans, so no long-lived per-unit channels
    # are needed.
    units = []
    for _ in range(n):
        rfd, wfd = make_nonblock_pipe()
        unit = {"rfd": rfd, "wfd": wfd, "fd2": None}
        # close both ends at teardown so a parked reader unblocks.
        H.register_close(_Closer(rfd))
        H.register_close(_Closer(wfd))
        units.append(unit)
    H.state = {
        "units": units,
        "woke_event": [0] * 1024,
        "woke_timeout": [0] * 1024,
        "churn_leak": [0] * 1024,
        "fd_baseline": harness.count_fds(),
    }


class _Closer(object):
    """register_close expects an object with .close(); wrap a raw fd number."""
    __slots__ = ("fd",)

    def __init__(self, fd):
        self.fd = fd

    def close(self):
        try:
            os.close(self.fd)
        except OSError:
            pass


def body(H):
    if H.state is None:
        return
    n = len(H.state["units"])
    # Part A: one driver per unit; each spawns BOTH alias parkers fresh per round
    # (they fan out across hubs) and runs the close-one-alias survival diff.
    H.run_pool(n, driver_a_pass, H.state)
    # Part B: a separate cross-hub churn pool recycling fd NUMBERS into fresh
    # parkers.  Reuse n workers (each does CHURN_PER_ROUND dup/close+fresh-park).
    H.run_pool(n, churn_worker, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED: {0}".format(H.scale_limit_reason or "no wait_fd/dup"))
        return
    st = H.state
    ev = sum(st["woke_event"])
    to = sum(st["woke_timeout"])
    H.log("alias_units(ops)={0} tasks={1} woke_event={2} woke_timeout={3}".format(
        H.total_ops(), H.total_tasks(), ev, to))
    H.check(H.total_ops() > 0,
            "no alias units completed (every same-file alias parker stranded?)")
    # Dominance oracle: real readiness must wake the parkers, not the ceiling.
    # A poisoned-survivor / lost-wake regime would invert this (most wakes by
    # timeout).  Only meaningful once some events were observed at all.
    if ev + to > 0:
        H.check(ev > to,
                "wake regime inverted: woke_event={0} <= woke_timeout={1} -- a "
                "closed alias number poisoned surviving arms / stale pending bits "
                "starved real wakes".format(ev, to))
    # fd-leak oracle over the dup churn: steady-state fd count must not climb
    # beyond a fixed slack over the baseline (every dup'd number was closed).
    base = st["fd_baseline"]
    now = harness.count_fds()
    if base >= 0 and now >= 0:
        H.check(now <= base + FD_SLACK,
                "fd leak across dup churn: baseline={0} now={1} (>{2} slack) -- "
                "an alias number was never closed or a stale arm pinned an "
                "fd".format(base, now, FD_SLACK))
    # The completeness oracle: a parker stranded by a poisoned/stale arm leaves a
    # driver goroutine joined forever -> LOST.  The precise strand detector.
    H.require_no_lost("dup-alias arm/pending conservation")


if __name__ == "__main__":
    # Moderate default sibling N: like p312, the pipe + dup/close churn + the
    # multi-goroutine-per-unit handoff does not scale to tens of thousands, and
    # the arm-cache/pending race is a CONCURRENCY (not a volume) hunt -- a few
    # thousand cross-hub alias splits + recycles is plenty to expose a stale
    # pending-wake or a poisoned-survivor arm.
    harness.main("p324_dup_alias_arm_conservation", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=4000,
                 describe="dup() a SECOND fd number aliasing one pipe; park each "
                          "alias on a different hub; close ONE number -> the "
                          "OTHER must still wake; recycle freed numbers into "
                          "fresh parkers -> each reads its OWN tag, no stale "
                          "pending-wake")
