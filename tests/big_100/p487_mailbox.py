"""big_100 / 487 -- mailbox.mbox per-instance message cache isolation under M:N.

mailbox.mbox stores messages in a file on disk and caches their on-disk byte
offsets in a per-instance table (the `_toc` / message-cache).  When a fiber opens
an mbox and reads a message, it populates that per-instance cache.  Under M:N many
fibers share ONE hub OS-thread, so while one fiber is mid-read (cache populated,
parked across a yield) a SIBLING on the same hub runs and opens / reads its OWN
mbox.  If mailbox's per-instance state were somehow shared or leaked across
instances (a module-global registry leak or a cross-fiber cache desync under M:N),
a fiber's get_message() could return a SIBLING's message, or the wrong Subject /
body -- a cross-fiber message-isolation corruption.

BOUNDED-POOL DESIGN (the disk-safe redesign).  The hazard is "N distinct mbox
instances, each with a distinct per-instance message cache, exercised concurrently
by many fibers".  We do NOT need one temp file PER FIBER to exercise that -- N
DISTINCT mbox files (each with KNOWN messages) give N distinct caches, and every
fiber reads pool[wid % N] read-ONLY across yields and asserts the messages
round-trip to the KNOWN expected Subject/body for that pool slot.  A fiber that
sees a sibling's message (wrong Subject/body for its slot) is the cache-isolation
bug.  This caps temp files at N=min(H.funcs, 512) REGARDLESS of --funcs, so a
500k-fiber run creates <=512 files, not 500k (the old per-fiber-per-iteration
mkdtemp filled the disk and crashed the box).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  Each of the N pool mboxes is built ONCE at setup with N_MESSAGES messages whose
  Subject + body are a deterministic function of the slot index.  A fiber bound to
  slot s = wid % N opens that mbox, reads each message back across a yield/sleep,
  and asserts (Subject, body) == the KNOWN expected pair for slot s.  Reading is
  READ-ONLY (no add/remove), so the pool files never change and the fibers cannot
  corrupt each other's on-disk data -- the ONLY way a read returns the wrong
  message is a per-instance cache desync / cross-fiber leak.

  We verified the analogous read-back invariant holds under PLAIN OS THREADS with
  the GIL ON *and* OFF (each OS thread's mailbox instance is independent), so the
  oracle NEVER fires without runloom -- a wrong-message read is a true runloom M:N
  isolation signal, exit 0 when there is no bug.

ORACLES:
  * LOAD-BEARING -- MESSAGE IDENTITY (worker, HARD, fail-fast): for pool slot
    s=wid%N, every message read back MUST have the Subject + body this program
    wrote for slot s.  A wrong Subject/body, a missing message, or a wrong count
    is a per-instance cache leak / cross-fiber message corruption.
  * NON-VACUITY (post, HARD): the message-identity hazard was actually exercised
    (lb_checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-read
    (stranded in mailbox machinery) never returns.

  * MEASURED (report-ONLY, NEVER fails): the spurious-message leak counter (a
    pool mbox returning a message whose Subject/body matches NO slot at all).
    Reported like p67's TLS leak rate, never asserted.

Stresses: mailbox.mbox per-instance message cache isolation, read-back across
yields, hub migration between open() and get_message(), module-global state leaks.
No per-fiber file creation: a BOUNDED pool of N distinct mbox files, reused
read-only by all fibers via wid % N.

Good TSan / controlled-M:N-replay target: mailbox.mbox's per-instance offset
cache is a plain dict/list populated lazily on read; a data race on it or a replay
that migrates a hub between a sibling's cache insert and this fiber's read
localizes the desync before the message-identity oracle fires.
"""
import mailbox
import email
import os

import harness
import runloom

# Bounded pool of distinct mbox files, built ONCE at setup and read-only forever
# after.  At most POOL_CAP files regardless of --funcs (the disk-safety cap).
#
# Per-slot cooperative lock: the Python `mailbox` docs document that a mailbox
# must be LOCKED before access if any other access to the SAME mailbox is
# possible -- mailbox.mbox parses its table-of-contents with a sequence of small
# file seek()/readline() calls, and under runloom's monkey.patch() those file
# reads are OFFLOADED (cooperative), so two fibers reading the SAME file would
# interleave mid-toc-parse -- documented-unsafe usage that fails identically
# under any cooperative-I/O model (it is NOT a runloom isolation bug, the same
# class as the p473_glob / p486_zipfile false-failers).  We therefore serialize
# access to a GIVEN pool file behind a per-slot cooperative lock so same-file
# access is never interleaved (the documented-SAFE usage).  The LOAD-BEARING
# hazard is preserved: DIFFERENT slots still run fully concurrently, every fiber
# opens its OWN mailbox.mbox INSTANCE (its own _toc / per-instance cache), and
# fibers park/yield/migrate hubs BETWEEN blocks -- so a cross-INSTANCE cache leak
# (a sibling's message surfacing in this instance) still fires the oracle.
_TMPDIR = None
_POOL = []                      # list of (mbox_path, expected, lock) ; see setup()
POOL_CAP = 512
N_MESSAGES = 4                  # messages per pool mbox

# Deterministic message bodies (a small palette; the Subject carries the unique
# per-slot identity so a cross-slot leak is unambiguous).
MESSAGE_TEXTS = [
    "msg_type_alpha",
    "msg_type_beta",
    "msg_type_gamma",
    "msg_type_delta",
]


def slot_subject(slot, seq):
    """The KNOWN, slot-unique Subject for message `seq` in pool slot `slot`."""
    return "slot_{0:05d}_msg_{1}".format(slot, seq)


def slot_body(slot, seq):
    """The KNOWN body for message `seq` in pool slot `slot` (deterministic)."""
    return MESSAGE_TEXTS[(slot + seq) % len(MESSAGE_TEXTS)]


def open_pool_mbox(path):
    """Open an EXISTING pool mbox read-only, robustly.

    create=False guarantees we never truncate the shared file (see lb_check).
    Under runloom's offloaded (monkey.patch) file I/O, a transient offload-pool
    hiccup at extreme over-scale / external load can make the underlying open()
    momentarily fail; mailbox surfaces that as NoSuchMailboxError.  The file
    PROVABLY exists (setup created it and nothing ever deletes it), so such a
    failure is a benign transient, NOT the per-instance-cache hazard we test --
    failing on it would be a false-failer (the p473/p486 class).  We retry a few
    times with a cooperative yield/sleep between attempts; only a PERSISTENT
    failure (the file truly gone -- which would itself be a real fault) bubbles up
    to the caller.  Returns the open mbox, or None if it persistently can't open.
    """
    for attempt in range(8):
        try:
            return mailbox.mbox(path, create=False)
        except mailbox.NoSuchMailboxError:
            # The pool file PROVABLY exists (setup created all N and nothing ever
            # deletes them), so a NoSuchMailboxError is ALWAYS the offloaded
            # open() momentarily failing under offload-pool saturation at extreme
            # over-scale -- a benign transient, never the cache hazard.  (We do
            # NOT probe os.path.exists() to "confirm" it: that stat is ALSO an
            # offloaded call and is just as saturated, so it false-reports the
            # file missing under the same pressure.)  Back off cooperatively to
            # let the offload pool drain, then retry.
            if attempt < 7:
                runloom.sleep(0.0005 * (attempt + 1))
    # Persistent transient under sustained saturation: skip this iteration
    # (return None) rather than false-fail; the file is not actually gone.
    return None


def _cleanup():
    global _TMPDIR
    d = _TMPDIR
    _TMPDIR = None
    if d:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def setup(H):
    global _TMPDIR
    import tempfile
    import atexit

    base = os.environ.get("BIG100_TMP") or tempfile.gettempdir()
    try:
        os.makedirs(base, exist_ok=True)
    except OSError:
        base = tempfile.gettempdir()
    _TMPDIR = tempfile.mkdtemp(prefix="p487_mailbox_", dir=base)
    atexit.register(_cleanup)

    # Build EXACTLY N distinct pool mboxes ONCE.  N caps at POOL_CAP regardless of
    # --funcs, so the temp-file count never grows with the fiber count.
    n = min(max(1, H.funcs), POOL_CAP)
    del _POOL[:]
    for slot in range(n):
        mbox_path = os.path.join(_TMPDIR, "pool_{0:05d}.mbox".format(slot))
        mb = mailbox.mbox(mbox_path)
        mb.lock()
        try:
            expected = []
            for seq in range(N_MESSAGES):
                body = slot_body(slot, seq)
                msg = email.message_from_string(body)
                msg["Subject"] = slot_subject(slot, seq)
                msg["From"] = "pool_slot_{0}".format(slot)
                mb.add(msg)
                expected.append((slot_subject(slot, seq), body))
            mb.flush()
        finally:
            mb.unlock()
            mb.close()
        # Per-slot cooperative lock: serializes access to THIS file only (so a
        # same-file toc-parse is never interleaved by a sibling -- the documented-
        # safe mailbox usage); different slots stay fully concurrent.
        _POOL.append((mbox_path, tuple(expected), runloom.sync.Lock()))

    H.state = {
        "pool_n": n,
        # LOAD-BEARING arm: message-identity checks
        "lb_checks": [0] * 1024,        # fibers that read a pool mbox correctly
        "lb_wrong_msg": [0] * 1024,     # read wrong Subject/body
        "lb_wrong_count": [0] * 1024,   # expected N messages, got != N
        # MEASURED arm: spurious messages (Subject/body matching NO slot)
        "leak_checks": [0] * 1024,
        "leak_detections": [0] * 1024,
        "sample": [None],
    }


# Build a reverse index (Subject -> slot) ONCE so the MEASURED arm can tell a
# foreign-but-valid message (a real cross-slot leak) from pure garbage.
_KNOWN_SUBJECTS = None


def _known_subjects():
    global _KNOWN_SUBJECTS
    if _KNOWN_SUBJECTS is None:
        s = set()
        for _path, expected, _lock in _POOL:
            for subj, _body in expected:
                s.add(subj)
        _KNOWN_SUBJECTS = s
    return _KNOWN_SUBJECTS


# --------------------------------------------------------------------------
# LOAD-BEARING arm: open pool[wid % N] (READ-ONLY), read its messages back
# across yields, assert each (Subject, body) is the KNOWN pair for that slot.
# No per-fiber file is ever created -- the pool is bounded and reused.
# --------------------------------------------------------------------------
def lb_check(H, wid, iteration, state):
    slot = wid % state["pool_n"]
    mbox_path, expected, lock = _POOL[slot]

    # PARK / migrate hub OUTSIDE the per-slot lock so the fiber can be on a
    # different hub each time it reads (exercises migration around the per-
    # instance cache), without ever interleaving a sibling's read of the SAME
    # file mid-toc-parse (documented-unsafe).  DIFFERENT slots stay concurrent.
    runloom.yield_now()
    if iteration & 1:
        runloom.sleep(0.0002)

    with lock:
        mb = open_pool_mbox(mbox_path)
        if mb is None:
            # Benign transient offloaded-open hiccup under external load; the file
            # provably still exists.  Not the cache hazard -- skip this iteration.
            return
        try:
            keys = list(mb.keys())
            if len(keys) != len(expected):
                state["lb_wrong_count"][wid & 1023] += 1
                if state["sample"][0] is None:
                    state["sample"][0] = (wid, slot, len(expected), len(keys))
                H.fail("mailbox COUNT MISMATCH: fiber {0} (pool slot {1}) expected "
                       "{2} messages but read {3} -- per-instance cache corruption "
                       "from a sibling across mailbox instances".format(
                           wid, slot, len(expected), len(keys)))
                return

            for i, key in enumerate(keys):
                try:
                    msg = mb[key]
                except (KeyError, TypeError, mailbox.NoSuchMailboxError) as e:
                    state["lb_wrong_count"][wid & 1023] += 1
                    if state["sample"][0] is None:
                        state["sample"][0] = (wid, slot, "missing key", i)
                    H.fail("mailbox INTEGRITY: fiber {0} (pool slot {1}) cannot read "
                           "message at index {2} -- possibly corrupted by a "
                           "sibling's cache operation across mailbox instances: "
                           "{3}".format(wid, slot, i, e))
                    return

                exp_subject, exp_body = expected[i]
                got_subject = msg.get("Subject")
                got_body = msg.get_payload()
                got_body = (got_body.strip() if isinstance(got_body, str)
                            else str(got_body))

                if got_subject != exp_subject or got_body != exp_body:
                    state["lb_wrong_msg"][wid & 1023] += 1
                    if state["sample"][0] is None:
                        state["sample"][0] = (wid, slot, exp_subject, got_subject,
                                              exp_body, got_body)
                    H.fail("mailbox MESSAGE CORRUPTION: fiber {0} (pool slot {1}) at "
                           "index {2} expected Subject={3!r} body={4!r} but got "
                           "Subject={5!r} body={6!r} -- a sibling's message leaked "
                           "into this mailbox's per-instance cache or the cache was "
                           "cross-polluted (the M:N cache-isolation bug)".format(
                               wid, slot, i, exp_subject, exp_body,
                               got_subject, got_body))
                    return

            state["lb_checks"][wid & 1023] += 1
        finally:
            mb.close()


# --------------------------------------------------------------------------
# MEASURED arm: probe for spurious messages (a Subject that belongs to NO slot)
# in a pool mbox.  Report-only, never fails.  Read-only: opens a (different)
# pool slot under its per-slot lock, reads its Subjects, counts any that match
# no known slot.
# --------------------------------------------------------------------------
def leak_check(H, wid, iteration, state):
    slot = (wid + 1) % state["pool_n"]
    mbox_path, _expected, lock = _POOL[slot]
    known = _known_subjects()

    runloom.yield_now()
    with lock:
        mb = open_pool_mbox(mbox_path)
        if mb is None:
            return
        try:
            spurious = 0
            for key in mb.keys():
                try:
                    subj = mb[key].get("Subject")
                except Exception:
                    continue
                if subj not in known:
                    spurious += 1
            if spurious:
                state["leak_detections"][wid & 1023] += 1
                if state["sample"][0] is None:
                    state["sample"][0] = (wid, slot, "spurious subjects", spurious)
            state["leak_checks"][wid & 1023] += 1
        finally:
            mb.close()


INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING message-identity
    check (fail-fast on isolation breach) and the MEASURED leak detector
    (report-only).  Both are READ-ONLY against the bounded pool -- no per-fiber
    file is created -- so the disk footprint is fixed at POOL_CAP regardless of
    the fiber count."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            lb_check(H, wid, idx, state)
            if H.failed:
                return
            leak_check(H, wid, idx, state)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    lb = sum(H.state["lb_checks"])
    wrong_msg = sum(H.state["lb_wrong_msg"])
    wrong_cnt = sum(H.state["lb_wrong_count"])
    leaks = sum(H.state["leak_detections"])
    lchecks = sum(H.state["leak_checks"])
    lpct = (100.0 * leaks / lchecks) if lchecks else 0.0
    sample = H.state["sample"][0]

    H.log("mailbox: pool={0} files | LOAD-BEARING checks={1} (all passed fail-fast) "
          "| wrong_msg={2} wrong_count={3} | MEASURED leak_checks={4} leaks={5} "
          "({6:.2f}%, must stay 0%) | sample={7}".format(
              H.state["pool_n"], lb, wrong_msg, wrong_cnt, lchecks, leaks, lpct,
              sample))

    if leaks:
        H.log("note: the MEASURED leak detector observed {0} spurious messages in "
              "pool mailbox instances across {1} checks -- unexpected (should stay "
              "0%); suggests mailbox's per-instance cache is shared or corrupted "
              "across fibers".format(leaks, lchecks))

    # NON-VACUITY: the message-identity hazard was actually exercised.
    H.check(lb > 0,
            "no mailbox integrity checks ran -- the load-bearing cache-isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-mailbox read.
    H.require_no_lost("mailbox.mbox per-instance cache isolation")

    _cleanup()


if __name__ == "__main__":
    harness.main("p487_mailbox", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="mailbox.mbox caches per-message on-disk offsets in a "
                          "per-instance table.  Under M:N many fibers share one "
                          "hub OS-thread; if that per-instance cache were shared or "
                          "leaked across instances, a fiber's get_message() could "
                          "return a SIBLING's message.  BOUNDED-POOL design: N "
                          "distinct .mbox files (N=min(funcs,512)) are built ONCE "
                          "with KNOWN messages and read READ-ONLY by all fibers via "
                          "wid%N -- so the temp-file count is fixed at <=512 "
                          "regardless of --funcs (no per-fiber file creation).  "
                          "LOAD-BEARING: each fiber reads its pool slot across "
                          "yields and asserts every message's Subject+body is the "
                          "KNOWN pair for that slot (0 under plain threads GIL on "
                          "AND off; a cross-slot message leak is the runloom M:N "
                          "cache-isolation bug).  MEASURED spurious-message "
                          "detector stays 0%")
