"""big_100 / 610 -- termios.tcgetattr/tcsetattr single-owner attribute identity under M:N.

The `termios` stdlib module's workhorses are termios.tcgetattr(fd) and
termios.tcsetattr(fd, when, attrs).  tcgetattr(fd) reads the kernel's `struct
termios` for a tty fd and builds a FRESH 7-element Python list
[iflag, oflag, cflag, lflag, ispeed, ospeed, cc] (cc is a fresh sub-list of the
control characters -- ints for VMIN/VTIME, length-1 bytes for the rest).
tcsetattr(fd, when, attrs) writes that list back into the kernel struct.  Each
tty fd here is the slave end of a private pseudo-terminal owned by exactly ONE
fiber -- nothing about the fd or its termios state is shared -- so on a correct
runtime two consecutive tcgetattr() calls on the same untouched fd MUST return
BIT-IDENTICAL lists, and a set-then-get round trips the fields we set exactly.
This is a pure closed-form identity, verifiable against plain OS threads.

WHERE M:N COULD BREAK IT (the gap this program probes).  tcgetattr() runs the C
`tcgetattr(2)` syscall and then ALLOCATES several Python objects (the outer list,
the cc sub-list, the per-control-char bytes/int objects) with the GIL OFF while a
sibling fiber on another hub is simultaneously doing the same in ITS own
tcgetattr()/tcsetattr().  If runloom's cooperative machinery torn the returned
list, leaked another fiber's freshly-built list into this fiber's call, or the
struct-to-list conversion raced object allocation, this fiber would observe an
attr list that DIFFERS from the one it just read on the same unchanged fd, or
whose fiber-unique fingerprint (VMIN/VTIME set from wid+round) is another fiber's
value.  We force a hub interleave with runloom.yield_now() between the two reads
so a sibling reliably runs in the window.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Two laws on a SINGLE-OWNER pty slave fd:
  (1) IDEMPOTENT SET/GET: after tcsetattr(fd, TCSANOW, attrs) with our fiber-unique
      VMIN/VTIME, the immediately-following tcgetattr(fd) reports those exact
      VMIN/VTIME values (the kernel stored what we asked; a well-defined closed
      form).
  (2) STABILITY ACROSS A YIELD: with NOTHING touching the fd, tcgetattr(fd) called
      before and after a yield returns BIT-IDENTICAL 7-element lists (deep ==).
Because the fd is private to one fiber and no other fiber ever touches it, there is
no shared-object semantics to muddy either law: a mismatch, a wrong fingerprint, or
a structurally malformed list can only be a runtime fault (a torn cooperative
tcgetattr build, a cross-fiber leak of another fiber's freshly-allocated list, or a
corrupted fd table).  There is deliberately NO shared-mutable arm.

ORACLES:
  * LOAD-BEARING -- ATTR IDENTITY (worker, HARD, fail-fast).  Each round the worker
    opens its OWN pty pair (os.openpty), reads baseline attrs, stamps a fiber-unique
    fingerprint into cc[VMIN]/cc[VTIME] (derived from wid + round), tcsetattr's it,
    reads it back (idempotence check), then loops: yield -> tcgetattr -> assert the
    list is deep-equal to the post-set baseline AND still carries this fiber's
    fingerprint.  Single-owner: the pair lives in a fiber-local variable and is
    never shared; no sibling ever touches it.

  * COMPLETENESS (post, HARD): require_no_lost -- catches any fiber that vanished
    mid-round (e.g. stranded inside a torn tcgetattr) and never returned.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (attr_checks > 0),
    else the get/set identity hazard was never exercised.

FAIL ON: a set field not read back (idempotence broken), an attr list that changes
across a yield on an untouched single-owner fd (stability broken), a fiber-unique
fingerprint replaced by another fiber's value (cross-fiber list leak), or a
structurally malformed attr list.  No documented Python semantics can be mislabeled
here: the pty fd and its termios state are strictly single-owner.

RESOURCE CAP: each concurrent worker holds a master+slave fd pair (2 pty devices)
for the duration of a round.  The kernel caps pseudo-terminals at
/proc/sys/kernel/pty/max (default 4096), so max_funcs is set to 512 (<=1024 pty
devices live) to keep the forever-loop's --funcs 1000000 well under the ceiling.

Stresses: termios.tcgetattr() struct-to-list conversion + Python object allocation
GIL-off under M:N, termios.tcsetattr() round-trip fidelity, the cc control-char
sub-list build (ints for VMIN/VTIME vs length-1 bytes for the rest), fd lifecycle
(openpty/close every round), and bit-exact attribute identity of a single-owner tty
across a hub-migrating yield.
"""
import os
import sys

# ---- availability guard (POSIX-only: ptys + the termios module) ------------
_POSIX = sys.platform.startswith(("linux", "darwin", "freebsd"))
if not _POSIX:
    print("SKIP: POSIX-only (termios + pty unavailable on {0})".format(
        sys.platform))
    sys.exit(0)

if not hasattr(os, "openpty"):
    print("SKIP: os.openpty unavailable on this platform")
    sys.exit(0)

try:
    import termios
except ImportError:
    print("SKIP: termios module unavailable on this platform")
    sys.exit(0)

import harness
import runloom

# Each concurrent worker holds master+slave (2 pty devices); the kernel caps
# ptys at /proc/sys/kernel/pty/max (default 4096).  Cap so <=1024 devices are
# ever live at once, well under the ceiling, and so the forever loop's
# --funcs 1000000 does not try to allocate a million ptys.
MAX_SESSIONS = 512

# Number of attr fields a tcgetattr() list carries:
# [iflag, oflag, cflag, lflag, ispeed, ospeed, cc].
NFIELDS = 7

# Yield-and-recheck iterations per session.  The stability hazard only shows
# under SUSTAINED churn -- many fibers reading/writing their own termios structs
# while parked across a yield so the scheduler reliably interleaves a sibling's
# tcgetattr build before this fiber resumes.  Bounded so each session returns.
INNER_CAP = 64


def attrs_equal(a, b):
    """Deep structural equality of two tcgetattr() lists.  The outer lists and
    the cc sub-list compare element-wise (ints and length-1 bytes both compare
    by value), so this is a true bit-for-bit identity of the termios snapshot."""
    if len(a) != len(b):
        return False
    # Fields 0..5 are plain ints; field 6 (cc) is a list.
    for i in range(6):
        if a[i] != b[i]:
            return False
    ca, cb = a[6], b[6]
    if len(ca) != len(cb):
        return False
    for i in range(len(ca)):
        if ca[i] != cb[i]:
            return False
    return True


def run_session(H, wid, rnd, rng, state):
    """One single-owner termios identity round.  Open a private pty pair, stamp a
    fiber-unique fingerprint into cc[VMIN]/cc[VTIME], set it, and assert set/get
    idempotence plus attr-list stability across yields."""
    master, slave = os.openpty()
    try:
        # Baseline read of the private fd's termios state.
        attrs = termios.tcgetattr(slave)
        if len(attrs) != NFIELDS or not isinstance(attrs[6], list):
            H.fail("tcgetattr returned a malformed attr list (len={0}, cc "
                   "type={1}) wid={2} round={3} -- a torn cooperative struct-to-"
                   "list build under M:N".format(
                       len(attrs), type(attrs[6]).__name__, wid, rnd))
            return False

        # Fiber-unique fingerprint: VMIN/VTIME are the two cc slots that termios
        # returns/accepts as plain ints (0..255), so they round-trip exactly and a
        # cross-fiber list leak would decode to the WRONG fiber's value.  Fold in
        # the round so the fingerprint also varies across rounds.  NOTE: CPython
        # only returns cc[VMIN]/cc[VTIME] as INTS in NON-canonical mode (in
        # canonical mode those array slots are the EOF/EOL chars and come back as
        # length-1 bytes), so clear ICANON first to make the int round-trip exact.
        attrs[3] &= ~termios.ICANON
        fp_min = (wid ^ (rnd * 7)) & 0xFF
        fp_time = ((wid >> 8) ^ (rnd * 131)) & 0xFF
        cc = list(attrs[6])                 # own copy; never share the sub-list
        cc[termios.VMIN] = fp_min
        cc[termios.VTIME] = fp_time
        attrs[6] = cc
        # TCSANOW applies immediately with no drain wait (no I/O to block on).
        termios.tcsetattr(slave, termios.TCSANOW, attrs)

        # Read back: the kernel must report exactly the fingerprint we set
        # (idempotent set/get -- a well-defined closed form on an untouched fd).
        post = termios.tcgetattr(slave)
        got_min = post[6][termios.VMIN]
        got_time = post[6][termios.VTIME]
        if got_min != fp_min or got_time != fp_time:
            H.fail("termios set/get NOT idempotent wid={0} round={1}: set "
                   "VMIN/VTIME=({2},{3}) but read back ({4},{5}) -- a torn "
                   "tcsetattr/tcgetattr or a cross-fiber list leak under M:N"
                   .format(wid, rnd, fp_min, fp_time, got_min, got_time))
            return False

        # STABILITY: with nothing touching the fd, repeated tcgetattr() across a
        # yield must return a BIT-IDENTICAL list (and keep this fiber's
        # fingerprint).  Any difference is a runtime fault, never Python
        # semantics (single-owner fd, no sibling touches it).
        idx = 0
        while H.running() and idx < INNER_CAP:
            runloom.yield_now()             # force a sibling to interleave here
            if idx & 1:
                runloom.sleep(0.0002)
            again = termios.tcgetattr(slave)
            if not attrs_equal(again, post):
                H.fail("termios attr list CHANGED across a yield on an untouched "
                       "single-owner fd wid={0} round={1} iter={2} -- a cross-"
                       "fiber tcgetattr list leak or a torn struct-to-list build "
                       "under M:N (before={3!r} after={4!r})".format(
                           wid, rnd, idx, post[:6] + [post[6][:8]],
                           again[:6] + [again[6][:8]]))
                return False
            if (again[6][termios.VMIN] != fp_min
                    or again[6][termios.VTIME] != fp_time):
                H.fail("termios fingerprint LOST across a yield wid={0} round={1} "
                       "iter={2}: expected VMIN/VTIME=({3},{4}) got ({5},{6}) -- a "
                       "cross-fiber leak of a sibling's termios list".format(
                           wid, rnd, idx, fp_min, fp_time,
                           again[6][termios.VMIN], again[6][termios.VTIME]))
                return False
            state["attr_checks"][wid & 1023] += 1
            H.op(wid)
            idx += 1
        return True
    finally:
        for fd in (slave, master):
            try:
                os.close(fd)
            except OSError:
                pass


def worker(H, wid, rng, state):
    rnd = 0
    for _ in H.round_range():
        if not H.running():
            break
        if not run_session(H, wid, rnd, rng, state):
            return                          # fail-fast: oracle already recorded
        if H.failed:
            return
        H.task_done(wid)
        rnd += 1


def setup(H):
    H.state = {
        "attr_checks": [0] * 1024,          # non-vacuity tally (sharded; report)
    }


def body(H):
    # Hard PTY resource cap: never spawn more concurrent pty-holding workers than
    # MAX_SESSIONS regardless of --funcs (2 pty devices/worker vs kernel pty/max).
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_SESSIONS)


def post(H):
    checks = sum(H.state["attr_checks"])
    H.log("termios attr identity: {0} single-owner tcgetattr stability + set/get "
          "idempotence checks (all bit-identical, fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the get/set identity hazard was actually exercised.
    H.check(checks > 0,
            "no termios attr checks completed -- the single-owner tcgetattr/"
            "tcsetattr identity hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber vanished mid-round (e.g. stranded inside a torn
    # tcgetattr) -- it would never return.
    H.require_no_lost("termios attr identity")


if __name__ == "__main__":
    harness.main(
        "p610_termios_tcattr_roundtrip", body, setup=setup, post=post,
        default_funcs=4000, max_funcs=MAX_SESSIONS,
        describe="termios.tcgetattr(fd) reads a tty's kernel struct termios and "
                 "builds a fresh 7-element Python list; tcsetattr(fd, when, attrs) "
                 "writes it back.  Each fiber owns a private pty slave fd, so two "
                 "tcgetattr() calls on the untouched fd MUST return bit-identical "
                 "lists and a set-then-get round-trips exactly.  LOAD-BEARING: each "
                 "fiber stamps a fiber-unique VMIN/VTIME fingerprint, sets it, and "
                 "across hub-migrating yields asserts the attr list stays deep-"
                 "equal and keeps its fingerprint.  A changed list, a lost "
                 "fingerprint (cross-fiber tcgetattr list leak), or a non-"
                 "idempotent set/get (torn cooperative build) is the runtime bug. "
                 "Single-owner fd, so no shared-object semantics can mislabel a "
                 "documented behavior as a fault")
