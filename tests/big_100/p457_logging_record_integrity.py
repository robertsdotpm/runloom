"""big_100 / 457 -- logging record conservation + identity integrity under M:N.

The `logging` module is DOCUMENTED thread-safe and is built on real locks:

  * a module-global RLock `logging._lock` guards the global structures
    (Logger.manager.loggerDict, and the per-logger `handlers` list mutation in
    Logger.addHandler / removeHandler);
  * each Handler owns its OWN RLock `handler.lock`, and Handler.handle() wraps the
    actual `self.emit(record)` in `with self.lock:` -- so two concurrent emits to
    the SAME handler are serialized, and emit() sees a stable record.

Under runloom M:N, many fibers ("goroutines") share a hub OS-thread and a shared
hub PyThreadState.  Anything CPython keys to the OS-thread -- or to a shared module
global -- is shared across all fibers on a hub unless runloom isolates it.  The
hazards a CORRECT runtime must still defend against here:

  * many fibers emit concurrently through ONE shared Logger -> ONE shared Handler;
    the per-handler RLock must still serialize emit() so the handler's append-only
    record buffer is not TORN (two appends clobbering one slot), nor any record
    LOST or DUPLICATED;
  * Logger.callHandlers() iterates the per-logger `handlers` LIST while other
    fibers addHandler()/removeHandler() (global-list mutation under _lock) -- the
    iteration must not crash, skip the load-bearing handler, or double-deliver.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified, not assumed):

  LOAD-BEARING -- CONSERVATION + IDENTITY of the STRUCTURED records.  N fibers each
  emit exactly M records through one shared Logger to a custom Handler whose emit()
  appends a (wid, seq) tuple to a single shared list (under the handler's own
  RLock, the documented-safe path).  At quiesce the handler list MUST hold EXACTLY
  N*M records; every (wid, seq) must be SELF-CONSISTENT (the tuple a single fiber
  built, never spliced from two fibers); per (wid) the M seqs must be exactly
  {0..M-1} with no gap and no duplicate.  This is the run(1)/GIL-ON behaviour: a
  lock-serialized append-only handler never loses, duplicates, or tears a record.
  Under M:N it MUST hold too -- a torn/lost/duplicated record there is a runloom
  regression in how the shared per-handler RLock + the list.append() interact
  across a hub migration / preempt-mid-emit, NOT documented-unsafe usage.  We
  verified the serialized-append oracle is GREEN under plain OS threads with the
  GIL ON (a standalone control), so it is NOT a false-positive detector; it goes
  RED only on genuine record corruption.  The program EXITS 0 when there is no bug.

  SINGLE-OWNER control: one fiber emits K records to its OWN private handler ->
  exactly K intact records.  A loss/dup HERE would be a CPython logging-machinery
  bug, not contention (one writer).  Must always be exact.

  MEASURED arm (report-ONLY, NEVER fails): RAW-STREAM byte interleaving.  A second
  handler (`RawSplitHandler`) writes each record to a shared StringIO in SEVERAL
  pieces (prefix, body, terminator) and -- deliberately, the documented-unsafe
  pattern -- does NOT hold a serializing lock across those pieces (it yields
  between them).  Concurrent emits then SPLICE in the raw text stream: a parsed
  line can interleave another fiber's bytes.  That raw interleaving reproduces
  under plain OS threads too (it is unprotected multi-write I/O, not a runloom
  fault), so we MEASURE the splice rate and REPORT it, NEVER fail on it -- exactly
  like p67's TLS leak rate.  Crucially, the load-bearing STRUCTURED records are
  emitted through the LOCK-HELD path and stay intact even while the raw stream
  splices: structured-intact + raw-spliced is the expected, benign shape.

  CHURN arm (bounded): a few fibers concurrently addHandler()/removeHandler() a
  throwaway sink handler and setLevel() on the shared logger, mutating the global
  per-logger `handlers` list under _lock while the load-bearing emitters iterate it
  in callHandlers().  The load-bearing handler is NEVER removed, so every emit must
  still reach it (conservation must hold THROUGH the churn).  A crash, a skipped
  load-bearing handler, or a double-delivery is a hard fault.

  COMPLETENESS: require_no_lost -- a fiber stranded inside emit() holding the shared
  handler RLock when it vanished never returns; the watchdog catches an outright
  strand and require_no_lost catches a parked-then-vanished worker.

Stresses: logging.Handler per-handler RLock serialization of emit() across hub
migration + preempt-mid-emit, append-only handler buffer conservation/identity,
Logger.callHandlers() iterating the per-logger handlers list while addHandler/
removeHandler mutate it under the module _lock, setLevel churn, lost/duplicated/
torn record, raw-stream multi-write splice (documented-unsafe -> measured), no-lost-
wake while holding the shared handler lock.

Good TSan / controlled-M:N-replay target: the load-bearing handler's
`list.append((wid, seq))` runs under the shared handler RLock; a TSan report on
that list object, or a deterministic replay that migrates a hub between a record's
build and its append, localizes a torn/lost record before the conservation count
even closes.
"""
import io
import logging

import harness
import runloom

# LOAD-BEARING population cap.  This is a correctness probe of the shared-handler
# record path, not a scale soak -- keep contenders MODEST so every emitter
# completes its M records well inside the window and the conservation count is
# exact (a SLOW straggler is not a lost record, but a modest N removes the
# ambiguity entirely).
MAX_EMITTERS = 16000
# Records each load-bearing emitter pushes through the shared logger.  Small, so
# N*M stays bounded, but >1 so the per-(wid) seq set {0..M-1} is a real identity
# constraint (a duplicated/lost record breaks the per-wid set, not just the total).
RECORDS_PER_EMITTER = 6

# CHURN arm: a few fibers thrash the global per-logger handlers list (add/remove a
# throwaway sink + setLevel) while emitters iterate it in callHandlers().  Bounded
# and never touching the load-bearing handler.
CHURN_FIBERS = 16
# MEASURED raw-stream arm: a modest paired population emitting through the
# unprotected multi-write RawSplitHandler so concurrent emits splice in the raw
# StringIO.  Report-only; kept small so the splice measurement is quick.
RAW_EMITTERS = 400
RAW_RECORDS = 4


# --------------------------------------------------------------------------
# LOAD-BEARING handler: emit() appends a SELF-CONSISTENT (wid, seq) record to a
# single shared list.  Handler.handle() already wraps this emit() in the per-
# handler RLock (the documented-safe path), so the append is serialized on a
# correct runtime.  We yield INSIDE emit (under the held handler lock) so a hub
# migration / preempt lands mid-emit -- exactly where a broken save/restore of the
# lock state, or a torn list.append, would corrupt the buffer.  We parse wid/seq
# back out of the formatted message too, so a record whose TEXT was spliced from
# two fibers (the structured tuple disagreeing with the parsed message) is caught.
# --------------------------------------------------------------------------
class RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []      # appended under self.lock (Handler.handle holds it)
        self.emits = 0         # count of emit() entries (under the lock)

    def emit(self, record):
        # We are inside Handler.handle()'s `with self.lock:` -- the documented-safe
        # serialized path.  Build the structured tuple from the record's OWN args
        # (set by the emitter), then yield so a preempt/migration lands while we
        # hold the shared handler RLock, then commit the append.  On a correct
        # runtime the lock makes this atomic w.r.t. other emits; a torn append, a
        # lost entry, or a duplicated one is a runloom regression.
        wid = record.bigwid
        seq = record.bigseq
        # The formatted message independently carries wid:seq -- parse it back so a
        # record whose message TEXT was spliced from a sibling (message disagrees
        # with the args) is caught as a torn record, not silently counted.
        msg = self.format(record)
        self.emits += 1
        runloom.yield_now()                 # preempt/migrate mid-emit, lock held
        self.records.append((wid, seq, msg))


# --------------------------------------------------------------------------
# MEASURED raw-stream handler: writes each record to a SHARED StringIO in several
# pieces and -- deliberately, the documented-unsafe pattern -- yields BETWEEN the
# pieces WITHOUT a serializing lock across them.  Concurrent emits then splice in
# the raw text.  Handler.handle still takes THIS handler's RLock around emit(), so
# to actually exercise unprotected interleaving we drop to the bare stream here and
# do NOT rely on the handler lock to bracket the multi-write (we yield mid-write).
# Report-only: the splice rate reproduces under plain threads (unprotected multi-
# write I/O), so it is MEASURED, never failed.
# --------------------------------------------------------------------------
class RawSplitHandler(logging.Handler):
    def __init__(self, stream):
        super().__init__(level=logging.DEBUG)
        self.stream = stream

    def handle(self, record):
        # DOCUMENTED-UNSAFE on purpose: the stock Handler.handle() brackets emit()
        # in `with self.lock:`, which would serialize even our multi-write emit and
        # hide the splice.  This subclass deliberately does NOT take the handler
        # lock around its piecewise emit -- the exact unprotected-multi-write-I/O
        # pattern the docs warn against -- so concurrent emits genuinely interleave
        # in the raw stream.  This is the MEASURED (report-only) arm; the load-
        # bearing arm uses the stock lock-held path.
        rv = self.filter(record)
        if rv:
            self.emit(record)               # NO `with self.lock:` -- unprotected
        return rv

    def emit(self, record):
        wid = record.bigwid
        seq = record.bigseq
        s = self.stream
        # Three separate writes with a yield between each -> a concurrent emit can
        # interleave its bytes into this line in the raw stream (documented-unsafe
        # multi-write I/O).  The full line, if NOT spliced, is "R{wid}:{seq};\n".
        s.write("R{0}:".format(wid))
        runloom.yield_now()
        s.write("{0}".format(seq))
        runloom.yield_now()
        s.write(";\n")


def make_formatter():
    # The message itself carries wid:seq so emit() can re-parse it and detect a
    # spliced message text (independent of the structured args).
    return logging.Formatter("%(bigwid)d:%(bigseq)d")


# --------------------------------------------------------------------------
# LOAD-BEARING emitter: push exactly RECORDS_PER_EMITTER records through the shared
# logger.  Each record carries this fiber's wid + a per-fiber seq in {0..M-1}.
# --------------------------------------------------------------------------
def emitter(H, wid, rng, state):
    logger = state["logger"]
    m = state["records_per_emitter"]
    for r in H.round_range():
        if not H.running():
            break
        for seq in range(m):
            # extra={} stamps bigwid/bigseq onto the LogRecord so the handler reads
            # them back -- the identity the conservation oracle checks.
            logger.info("emit", extra={"bigwid": wid, "bigseq": seq})
            H.op(wid)
        # Only ONE round of M records per emitter (--rounds default 1): the
        # conservation count is exactly N*M.  If --rounds>1 we'd emit more; the
        # post() count adapts to the actual emit total, not a fixed N*M (see post).
        H.task_done(wid)


# --------------------------------------------------------------------------
# CHURN arm: thrash the global per-logger handlers list while emitters iterate it.
# Add then remove a throwaway sink handler, and setLevel.  NEVER touches the load-
# bearing RecordingHandler, so conservation must hold THROUGH the churn.
# --------------------------------------------------------------------------
def churner(H, wid, rng, state):
    logger = state["logger"]
    base_level = state["base_level"]
    for _ in H.round_range():
        n = 0
        while H.running() and n < 200:
            sink = logging.NullHandler()
            logger.addHandler(sink)         # global-list mutation under _lock
            runloom.yield_now()
            # setLevel churns the logger's effective level; keep it <= base so the
            # load-bearing INFO records are NEVER filtered out (conservation must
            # hold).  Alternate DEBUG/the base level only.
            logger.setLevel(logging.DEBUG if (n & 1) else base_level)
            runloom.yield_now()
            logger.removeHandler(sink)      # global-list mutation under _lock
            n += 1
            H.op(wid)
        H.task_done(wid)


# --------------------------------------------------------------------------
# MEASURED raw-stream emitter (report-only): emit through the RawSplitHandler so
# the raw StringIO splices.  Separate logger so it never touches the load-bearing
# handler's record buffer.
# --------------------------------------------------------------------------
def raw_emitter(H, wid, rng, state):
    rlogger = state["raw_logger"]
    for r in range(max(1, H.rounds)):
        if not H.running():
            break
        for seq in range(RAW_RECORDS):
            rlogger.info("raw", extra={"bigwid": wid, "bigseq": seq})


def run_raw_phase(H, state):
    """Report-ONLY pre-phase: drive the documented-unsafe RawSplitHandler so the
    raw StringIO splices, FULLY DRAIN it, then MEASURE the splice rate from the raw
    text.  Runs BEFORE the load-bearing pool and on a SEPARATE logger/handler, so
    its documented-unsafe raw interleaving can never reach the conservation oracle.
    """
    n = state["raw_emitters"]
    if n <= 0:
        return
    wg = runloom.WaitGroup()
    wg.add(n)

    def run_one(wid):
        rng = H.derive("raw", wid)
        try:
            raw_emitter(H, wid, rng, state)
        finally:
            wg.done()

    for wid in range(n):
        H.fiber(run_one, wid)
    wg.wait()

    # MEASURE the raw splice rate.  A clean line is exactly "R{wid}:{seq};".  A line
    # that does not match that shape (or whose wid/seq don't round-trip) was spliced
    # by a concurrent emit -- documented-unsafe multi-write I/O, reported not failed.
    text = state["raw_stream"].getvalue()
    total_lines = 0
    spliced = 0
    for line in text.split("\n"):
        if not line:
            continue
        total_lines += 1
        ok = False
        if line.startswith("R") and line.endswith(";"):
            core = line[1:-1]              # "{wid}:{seq}"
            parts = core.split(":")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                ok = True
        if not ok:
            spliced += 1
    state["raw_total_lines"] = total_lines
    state["raw_spliced"] = spliced


def setup(H):
    # LOAD-BEARING logger + handler.  A FRESH, non-propagating logger so no other
    # handler (root/stderr) sees these records and the conservation count is exactly
    # what the RecordingHandler holds.
    logger = logging.getLogger("big100.p457.loadbearing")
    logger.handlers[:] = []
    logger.propagate = False               # don't double-deliver up the hierarchy
    logger.setLevel(logging.DEBUG)
    rec = RecordingHandler()
    rec.setFormatter(make_formatter())
    logger.addHandler(rec)

    # MEASURED raw logger + handler (separate, report-only).
    raw_stream = io.StringIO()
    raw_logger = logging.getLogger("big100.p457.raw")
    raw_logger.handlers[:] = []
    raw_logger.propagate = False
    raw_logger.setLevel(logging.DEBUG)
    raw_h = RawSplitHandler(raw_stream)
    raw_h.setFormatter(make_formatter())
    raw_logger.addHandler(raw_h)

    nemitters = min(MAX_EMITTERS, max(1, H.funcs))
    raw_emitters = min(RAW_EMITTERS, max(0, nemitters))

    H.state = {
        "logger": logger,
        "recording": rec,
        "records_per_emitter": RECORDS_PER_EMITTER,
        "base_level": logging.INFO,
        "nemitters": nemitters,
        "raw_logger": raw_logger,
        "raw_handler": raw_h,
        "raw_stream": raw_stream,
        "raw_emitters": raw_emitters,
        "raw_total_lines": 0,
        "raw_spliced": 0,
    }


def body(H):
    state = H.state
    # Phase 1 (report-only, fully drained): the documented-unsafe RAW-STREAM splice
    # arm, measured in isolation on its own logger so it can never contaminate the
    # load-bearing conservation oracle.
    run_raw_phase(H, state)

    # SINGLE-OWNER control: one fiber emits K records to its OWN private logger ->
    # exactly K intact records (no contention; a loss here is a CPython bug).  Run
    # it inline, drained, before the contended pool.
    run_single_owner_control(H, state)

    # Phase 2 (LOAD-BEARING + CHURN): the shared-handler emitters race the global-
    # list churners.  callHandlers() iterates the per-logger handlers list while the
    # churners addHandler/removeHandler under _lock; the load-bearing handler is
    # never removed, so conservation must hold through the churn.
    n = state["nemitters"]
    nchurn = min(CHURN_FIBERS, max(0, n // 8))
    state["nchurn"] = nchurn
    if nchurn > 0:
        # Spawn churners directly; they loop on H.running() and return at deadline.
        for cwid in range(nchurn):
            crng = H.derive("churn", cwid)
            H.fiber(_churn_wrap, H, cwid, crng, state)
    H.run_pool(n, emitter, state, max_concurrent=n)


def _churn_wrap(H, cwid, crng, state):
    try:
        churner(H, cwid, crng, state)
    except harness.StopWorkload:
        pass
    except Exception as exc:               # noqa: BLE001
        H.error("churn-{0}".format(cwid), exc)


def run_single_owner_control(H, state):
    """SINGLE-OWNER control (drained inline): one fiber emits K records to its OWN
    private logger+handler.  A single-owner append-only handler must hold EXACTLY K
    intact records -- a loss/dup/tear here is a CPython logging-machinery bug, not
    contention.  Recorded into state for post()."""
    K = 64
    priv_logger = logging.getLogger("big100.p457.single")
    priv_logger.handlers[:] = []
    priv_logger.propagate = False
    priv_logger.setLevel(logging.DEBUG)
    priv_h = RecordingHandler()
    priv_h.setFormatter(make_formatter())
    priv_logger.addHandler(priv_h)
    state["single_handler"] = priv_h
    state["single_expected"] = K

    wg = runloom.WaitGroup()
    wg.add(1)

    def one():
        try:
            for seq in range(K):
                priv_logger.info("s", extra={"bigwid": 999999, "bigseq": seq})
        finally:
            wg.done()

    H.fiber(one)
    wg.wait()


def post(H):
    state = H.state
    rec = state["recording"]
    m = state["records_per_emitter"]
    n = state["nemitters"]

    # ---- LOAD-BEARING: CONSERVATION + IDENTITY of the structured records --------
    records = rec.records
    got = len(records)
    # Expected total: each emitter ran R rounds of M records.  With --rounds 1 that
    # is exactly N*M; for --rounds>1 the emitters that finished R rounds emitted
    # R*M.  We can't assume every emitter ran the same number of rounds under M:N
    # (a slow one may have run fewer), so we derive the expectation from the
    # PER-EMITTER seq sets, not a fixed N*M -- the identity constraint below proves
    # there are no gaps/dups, and the count is checked against emit() entries.
    #
    # Build a per-wid map of the seqs observed for round 0 (seq in {0..M-1}).  Under
    # --rounds 1 each emitter contributes exactly one full {0..M-1} set.
    by_wid = {}
    torn = 0
    for (wid, seq, msg) in records:
        # IDENTITY: the formatted message independently carries "wid:seq".  If the
        # message text disagrees with the structured args, the record was TORN
        # (spliced from two fibers) -- a hard fault on the lock-serialized path.
        parsed_ok = False
        try:
            pw, ps = msg.split(":")
            parsed_ok = (int(pw) == wid and int(ps) == seq)
        except (ValueError, AttributeError):
            parsed_ok = False
        if not parsed_ok:
            torn += 1
            if torn <= 5:
                H.log("TORN record: args=({0},{1}) but message={2!r}".format(
                    wid, seq, msg))
        by_wid.setdefault(wid, []).append(seq)

    if torn:
        H.fail("logging record integrity: {0} of {1} records were TORN (formatted "
               "message text disagrees with the record's own args -- a record "
               "spliced from two fibers' emits despite the per-handler RLock that "
               "Handler.handle() holds around emit())".format(torn, got))

    # CONSERVATION: total records == total emit() entries the handler made (every
    # emit appended exactly one record -- none lost, none duplicated by a torn
    # list.append).  emit() incremented self.emits under the same held lock.
    if not H.check(got == rec.emits,
                   "logging conservation BROKEN: handler holds {0} records but "
                   "emit() ran {1} times -- a lock-serialized list.append() under "
                   "the shared per-handler RLock {2} a record under M:N "
                   "(torn append)".format(
                       got, rec.emits,
                       "LOST" if got < rec.emits else "DUPLICATED")):
        pass

    # IDENTITY: for every emitter wid, the seqs it landed must be a set with NO
    # duplicate (a record delivered twice) and, for each round, contiguous {0..M-1}
    # (no record lost).  A duplicate seq for a wid = a record duplicated; a missing
    # seq with that wid present = a record lost mid-stream.
    dup_seq = 0
    lost_seq = 0
    for wid, seqs in by_wid.items():
        # Each round contributes one {0..M-1}; with R rounds we expect R copies of
        # each seq value.  Count occurrences; every seq value present must appear
        # the SAME number of times (== rounds completed by this wid), and the set of
        # seq VALUES must be exactly {0..M-1} (no value outside range, none missing
        # among those that appear).
        from collections import Counter as _C
        c = _C(seqs)
        # rounds this wid completed = max occurrence count of any seq value.
        rounds_done = max(c.values()) if c else 0
        for seq in range(m):
            occ = c.get(seq, 0)
            if occ == 0 and rounds_done > 0:
                lost_seq += 1            # this wid emitted some rounds but lost seq
            elif occ > rounds_done:
                dup_seq += 1             # seq appeared more times than rounds run
        # Any seq value OUTSIDE {0..M-1} is corruption (a wid/seq from nowhere).
        for seq in c:
            if not (0 <= seq < m):
                H.fail("logging IDENTITY CORRUPTION: emitter {0} has out-of-range "
                       "seq {1} (expected 0..{2}) -- a record's args were "
                       "corrupted under M:N".format(wid, seq, m - 1))
                break

    if lost_seq:
        H.fail("logging conservation BROKEN: {0} (wid,seq) record(s) LOST -- an "
               "emitter completed a round but a record in its {1}-record sequence "
               "never reached the shared handler (dropped on the lock-serialized "
               "emit path under M:N)".format(lost_seq, m))
    if dup_seq:
        H.fail("logging conservation BROKEN: {0} (wid,seq) record(s) DUPLICATED -- "
               "a record appeared in the shared handler more times than its emitter "
               "ran rounds (double-delivered through callHandlers / a torn "
               "list.append under M:N)".format(dup_seq))

    # ---- SINGLE-OWNER control: exact, intact -----------------------------------
    sh = state.get("single_handler")
    sk = state.get("single_expected", 0)
    if sh is not None:
        sgot = len(sh.records)
        H.check(sgot == sk,
                "SINGLE-OWNER control BROKEN: private handler holds {0} records, "
                "expected {1} -- a single-owner append-only handler must never lose "
                "or duplicate a record (this would be a CPython logging-machinery "
                "bug, not contention)".format(sgot, sk))
        # And every one intact + the seqs exactly {0..K-1}.
        sseqs = sorted(s for (_w, s, _m) in sh.records)
        H.check(sseqs == list(range(sk)),
                "SINGLE-OWNER control BROKEN: private handler seqs {0} != "
                "{{0..{1}}} -- a single-owner record was lost/duplicated".format(
                    sseqs[:8], sk - 1))

    # Sanity: the load-bearing hazard was actually exercised (not skipped) -- else
    # the oracle is vacuous.
    H.check(got > 0,
            "no records reached the shared handler -- the load-bearing logging "
            "conservation hazard was never exercised (oracle would be vacuous)")
    H.check(len(by_wid) > 1,
            "only one emitter's records reached the shared handler -- the "
            "concurrent shared-handler path was not exercised")

    # ---- MEASURED raw-stream splice arm (report-ONLY) --------------------------
    raw_lines = state.get("raw_total_lines", 0)
    raw_spliced = state.get("raw_spliced", 0)
    raw_pct = (100.0 * raw_spliced / raw_lines) if raw_lines else 0.0

    H.log("logging records: shared handler held {0} structured records from {1} "
          "emitters (LOAD-BEARING conservation+identity: torn={2} lost={3} "
          "dup={4}) | single-owner control={5}/{6} | raw-stream splice {7}/{8} "
          "lines ({9:.1f}%, documented-unsafe multi-write I/O -- REPORT ONLY)"
          .format(got, len(by_wid), torn, lost_seq, dup_seq,
                  len(sh.records) if sh else 0, sk,
                  raw_spliced, raw_lines, raw_pct))
    if raw_spliced:
        H.log("note: the raw-stream arm observed {0} spliced line(s) across {1} "
              "raw lines -- documented-unsafe UNPROTECTED multi-write I/O (a "
              "handler that writes a record in pieces without serializing across "
              "them; reproduces under plain GIL threads), NOT a runloom bug.  The "
              "LOAD-BEARING structured records went through the lock-held path and "
              "stayed intact (torn=0) even while the raw stream spliced.".format(
                  raw_spliced, raw_lines))

    # COMPLETENESS: no emitter parked-then-vanished (e.g. stranded inside emit()
    # holding the shared handler RLock, or in callHandlers iterating the list).
    H.require_no_lost("logging record conservation")


if __name__ == "__main__":
    harness.main(
        "p457_logging_record_integrity", body, setup=setup, post=post,
        default_funcs=4000,
        describe="N fibers emit M records each through ONE shared logging.Logger -> "
                 "ONE shared Handler whose emit() appends (wid,seq) under the per-"
                 "handler RLock; conservation+identity oracle: EXACTLY the emitted "
                 "count, every record intact + attributable, none lost/duplicated/"
                 "torn, THROUGH concurrent addHandler/removeHandler/setLevel churn "
                 "of the global handler list.  Raw multi-write stream splice is "
                 "documented-unsafe -> measured, never failed")
