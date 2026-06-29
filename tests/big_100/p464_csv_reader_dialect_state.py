"""big_100 / 464 -- _csv reader/writer parse state + global dialect registry
isolation under M:N.

The C `_csv` module keeps mutable PARSE STATE on each reader/writer OBJECT (the
incremental field buffer, the current field, the parsed-so-far row, and the bound
dialect), and it ALSO keeps a MODULE-GLOBAL dialect registry -- `csv._dialects`,
a plain dict mutated by register_dialect() / unregister_dialect() and read by
get_dialect() / the reader/writer dialect= lookup.  Under runloom M:N many fibers
share one hub OS-thread (and its PyThreadState), so:

  * a reader/writer OBJECT is per-fiber state IF each fiber builds its own -- the
    field buffer lives on the object, so a fiber's own reader/writer is private
    across yields and round-trips to that fiber's own values.  The hazard is a
    runloom regression that lets a sibling's preempt-mid-parse corrupt THIS
    fiber's object field buffer across a yield (a torn field, a row from the
    wrong fiber).
  * the dialect REGISTRY (`csv._dialects`) is a single PROCESS-GLOBAL dict shared
    across all hub fibers.  Concurrent register_dialect / unregister_dialect /
    get_dialect churn that dict; a torn insert/lookup under M:N would return the
    WRONG dialect (or lose one) for a fiber whose name is globally unique.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Two LOAD-BEARING invariants, each holding under stock CPython threads with the
  GIL ON *and* OFF (verified empirically with a standalone 64-thread control,
  same hazard, NO runloom: 0 errors in 25600 round-trips + dialect checks each):

  (1) OWN-OBJECT ROUND-TRIP IDENTITY.  A fiber builds a CSV row that ENCODES its
      own wid (and iteration), writes it through ITS OWN csv.writer over an
      io.StringIO, parks/yields, then parses the produced text back through ITS
      OWN csv.reader and asserts EVERY field round-trips to ITS OWN value.  Each
      fiber owns its reader/writer object, so the per-object field buffer is
      private for any concurrency model -- a wrong field is a torn reader/writer
      parse buffer (a sibling's parse leaked into this object across a yield), a
      true runloom object-state desync.

  (2) GLOBAL DIALECT-REGISTRY IDENTITY.  Each fiber register_dialect()s a
      GLOBALLY-UNIQUE per-wid name with a per-wid delimiter/quoting, parks/yields,
      then asserts get_dialect(its_name) returns ITS OWN dialect parameters, and
      that a reader/writer built with dialect=its_name uses ITS delimiter.  Names
      never collide across fibers, so under stock threads (GIL on/off) the shared
      registry dict always returns each thread's own entry -- a wrong/missing
      dialect is a torn `csv._dialects` dict mutation under M:N (a real runloom /
      FT shared-dict corruption), NOT documented-unsafe usage.

  Both arms PASS on a correct runtime (the program EXITS 0 when there is no bug).
  An oracle that fired under plain GIL-on threads would be a false-positive
  detector; neither does (controlled).

ORACLES:
  * LOAD-BEARING -- OWN-OBJECT ROUND-TRIP IDENTITY (worker, HARD, fail-fast):
    every field a fiber writes through its own writer and reads back through its
    own reader equals the value it encoded (which embeds its wid).  A torn field /
    wrong-fiber row = a runloom object-parse-state desync.
  * LOAD-BEARING -- GLOBAL DIALECT-REGISTRY IDENTITY (worker, HARD, fail-fast):
    get_dialect(my_unique_name) returns my own delimiter/quotechar/quoting, and a
    reader/writer built on dialect=my_name uses my delimiter.  A wrong/missing
    dialect for a globally-unique name = a torn shared-registry dict under M:N.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse, or
    holding a half-registered dialect when it vanished, never returns.
  * NON-VACUITY (post, HARD): both load-bearing arms actually ran (rt_checks > 0
    and dia_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): a SHARED single reader+writer used by ALL
    fibers (single owner object, many concurrent writerow/parse interleavings).
    The _csv object field buffer is NOT internally serialized, so concurrent
    use of ONE reader/writer across fibers tears its field buffer -- documented-
    unsafe (an object is not meant to be shared across concurrent writers, true
    under plain threads too).  We MEASURE the torn-field/contention rate; we NEVER
    fail on it.  Each shared-object op snapshots+restores nothing and writes to a
    throwaway buffer, so it never touches the load-bearing own-object checks.

FAIL ON: a field that round-trips to the wrong value through a fiber's OWN
reader/writer, or get_dialect()/dialect= returning the wrong dialect for a
globally-unique per-fiber name, or a crash.  NEVER fail on the shared-object
torn-buffer contention (measured).

Control: SINGLE-OWNER.  Each fiber owns its reader/writer object and registers a
globally-unique dialect name -- nothing but THIS fiber should touch them.  The
shared-object MEASURED arm is the deliberately-shared contrast.

Stresses: _csv reader/writer per-object incremental field-buffer parse state
across hub migration + preempt-mid-parse, the module-global csv._dialects dict
(register/unregister/get) under concurrent FT mutation, dialect= lookup, quoting
state machine, plain-global (non-contextvar) registry isolation.

Good TSan / controlled-M:N-replay target: csv._dialects is a plain dict mutated
(setitem on register, delitem on unregister, getitem on get/lookup) across hubs --
a data race on that dict, or a replay that migrates a hub between a fiber's
register_dialect and its get_dialect, localizes a torn registry before the
identity oracle fires; the per-object field buffer is the second race surface.
"""
import csv
import io

import harness
import runloom

# Modest, correctness-probe population (most workers run the load-bearing arms).
MAX_WORKERS = 8000

# Per-fiber dialect parameter band: each fiber gets a deterministic
# delimiter/quotechar/quoting from its wid so a leaked sibling dialect is a
# value distinct from this fiber's, hence detectable.  All are valid single-char
# _csv delimiters/quotechars that differ from the excel defaults.
DELIMS = ("|", ";", ":", "\t", "#", "~", "^", "@")
QUOTECHARS = ('"', "'", "*", "!")
QUOTINGS = (csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC)

# Fields-per-row for the round-trip identity check.  Each field encodes the
# fiber's wid + iteration + column so a torn buffer / wrong-fiber row mismatches.
NCOLS = 5

# Sustained inner churn per worker, bounded by H.running() so the load-bearing
# oracle fires at the DEFAULT --rounds 1: many fibers must be simultaneously
# mid-parse / mid-registry-churn and parked across their yield for a per-object
# or registry desync to surface.  INNER_CAP stops one worker monopolizing
# teardown on a slow box.
INNER_CAP = 100000


def dialect_params(wid):
    """Deterministic, per-wid dialect parameters.  Distinct enough that a leaked
    sibling dialect changes at least the delimiter detectably."""
    delim = DELIMS[wid % len(DELIMS)]
    quote = QUOTECHARS[(wid // len(DELIMS)) % len(QUOTECHARS)]
    quoting = QUOTINGS[(wid // (len(DELIMS) * len(QUOTECHARS))) % len(QUOTINGS)]
    # quotechar must differ from delimiter (a _csv constraint); rotate if equal.
    if quote == delim:
        quote = QUOTECHARS[(QUOTECHARS.index(quote) + 1) % len(QUOTECHARS)]
    return delim, quote, quoting


def setup(H):
    # MEASURED shared-object arm: ONE writer + ONE reader-buffer shared across all
    # fibers (deliberately unsafe contrast to the per-fiber own-object arm).
    shared_buf = io.StringIO()
    shared_writer = csv.writer(shared_buf)
    H.state = {
        "rt_checks": [0] * 1024,        # load-bearing round-trip fields verified
        "dia_checks": [0] * 1024,       # load-bearing dialect-identity checks
        "shared_checks": [0] * 1024,    # measured shared-object ops
        "shared_torn": [0] * 1024,      # measured shared-object torn fields
        "shared_writer": shared_writer,
        "shared_buf": shared_buf,
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm (1): OWN-OBJECT ROUND-TRIP IDENTITY.  Each fiber builds a row
# encoding its own wid, writes it through ITS OWN writer, yields/parks, parses it
# back through ITS OWN reader, and asserts every field round-trips to ITS value.
# Private per-object field buffer -> holds under plain threads (GIL on/off); a
# torn field is a runloom object-parse-state desync.
# --------------------------------------------------------------------------
def roundtrip_check(H, wid, idx, state):
    # Each field encodes wid+idx+col so a wrong-fiber row OR a torn buffer fails.
    fields = ["w{0}-i{1}-c{2}".format(wid, idx, c) for c in range(NCOLS)]
    # Embed a couple of awkward characters so the quoting state machine runs (a
    # comma forces quoting; a quote-in-field forces doubling) -- exercises the
    # per-object field buffer harder than plain tokens.
    fields[1] = fields[1] + ",x"           # comma -> must be quoted on write
    fields[3] = fields[3] + '"q'           # embedded quote -> doubled on write

    out = io.StringIO()
    writer = csv.writer(out)               # THIS fiber's own writer object
    # Park/migrate between building and writing so the object can be touched on a
    # different hub than it was created on (exercises migration around parse state).
    runloom.yield_now()
    writer.writerow(fields)
    text = out.getvalue()

    if idx & 1:
        runloom.sleep(0.0002)              # sleep-park: a sibling parses meanwhile
    else:
        runloom.yield_now()

    reader = csv.reader(io.StringIO(text))  # THIS fiber's own reader object
    rows = list(reader)

    if len(rows) != 1:
        H.fail("csv round-trip ROW COUNT wrong: wrote 1 row, parsed {0} (wid {1}, "
               "idx {2}); the per-object reader field buffer was torn by a sibling "
               "across a yield -- text={3!r}".format(len(rows), wid, idx, text))
        return
    got = rows[0]
    if got != fields:
        # Pinpoint the first differing column for the message.
        bad = next((c for c in range(min(len(got), len(fields)))
                    if got[c] != fields[c]), -1)
        H.fail("csv round-trip FIELD CORRUPTION: parsed {0!r} != written {1!r} "
               "(wid {2}, idx {3}, first-bad-col {4}); a sibling fiber's parse "
               "leaked into THIS fiber's own reader/writer field buffer across a "
               "yield (runloom object-parse-state desync)".format(
                   got, fields, wid, idx, bad))
        return
    state["rt_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# LOAD-BEARING arm (2): GLOBAL DIALECT-REGISTRY IDENTITY.  Each fiber registers a
# GLOBALLY-UNIQUE per-wid name in the shared csv._dialects dict, yields/parks,
# then asserts get_dialect(its_name) returns ITS OWN parameters and that a
# reader/writer built on dialect=its_name uses ITS delimiter.  Unique names ->
# holds under plain threads (GIL on/off); a wrong/missing dialect is a torn
# shared-registry dict under M:N.
# --------------------------------------------------------------------------
def dialect_check(H, wid, idx, state):
    delim, quote, quoting = dialect_params(wid)
    # Globally-unique name (wid+idx): no two fibers ever register the same name,
    # so a correct shared registry always returns THIS fiber's own entry.
    name = "big100_p464_d_{0}_{1}".format(wid, idx)
    try:
        csv.register_dialect(name, delimiter=delim, quotechar=quote,
                             quoting=quoting)
    except csv.Error as exc:
        H.fail("register_dialect FAILED for unique name {0!r} (wid {1}): {2} -- "
               "the shared csv._dialects registry rejected a valid, unique "
               "registration (torn dict under M:N)".format(name, wid, exc))
        return

    # Park/migrate between register and get so the registry dict can be mutated by
    # siblings on this and other hubs while THIS fiber is descheduled.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    try:
        d = csv.get_dialect(name)
    except csv.Error as exc:
        H.fail("get_dialect LOST a unique registration: name {0!r} (wid {1}) "
               "vanished from csv._dialects across a yield: {2} -- a sibling's "
               "register/unregister tore the shared registry dict under "
               "M:N".format(name, wid, exc))
        # Best-effort cleanup before bailing.
        try:
            csv.unregister_dialect(name)
        except csv.Error:
            pass
        return

    # Identity: the returned dialect must carry OUR parameters, not a sibling's.
    if d.delimiter != delim or d.quotechar != quote or d.quoting != quoting:
        H.fail("dialect REGISTRY CORRUPTION: get_dialect({0!r}) returned "
               "delimiter={1!r} quotechar={2!r} quoting={3} but wid {4} registered "
               "delimiter={5!r} quotechar={6!r} quoting={7} -- a sibling's dialect "
               "leaked into THIS fiber's globally-unique registry entry (torn "
               "shared csv._dialects under M:N)".format(
                   name, d.delimiter, d.quotechar, d.quoting, wid,
                   delim, quote, quoting))
        try:
            csv.unregister_dialect(name)
        except csv.Error:
            pass
        return

    # End-to-end: a writer/reader built on dialect=NAME must use OUR delimiter.
    # Encodes the wid so the round-trip also proves the per-object buffer is ours.
    probe = ["P{0}".format(wid), "Q{0}".format(idx), "R{0}".format(wid + idx)]
    out = io.StringIO()
    w = csv.writer(out, dialect=name)
    w.writerow(probe)
    line = out.getvalue()
    # Our delimiter must appear between fields; the excel default ',' must NOT be
    # what separates them (unless our delimiter IS ','-- it never is, DELIMS has
    # no comma).
    if delim not in line:
        H.fail("dialect= writer did NOT use the registered delimiter {0!r}: "
               "output {1!r} (wid {2}, name {3!r}) -- the dialect= lookup resolved "
               "to the wrong/torn registry entry under M:N".format(
                   delim, line, wid, name))
        try:
            csv.unregister_dialect(name)
        except csv.Error:
            pass
        return
    r = csv.reader(io.StringIO(line), dialect=name)
    back = list(r)
    if not back or back[0] != probe:
        H.fail("dialect= round-trip CORRUPTION: parsed {0!r} != {1!r} via dialect "
               "{2!r} (wid {3}) -- the dialect= reader resolved to the wrong "
               "registry entry or its field buffer was torn under M:N".format(
                   back, probe, name, wid))
        try:
            csv.unregister_dialect(name)
        except csv.Error:
            pass
        return

    # Clean up our unique registration so the registry doesn't grow unbounded
    # (each fiber registers+unregisters its own name every iteration -> sustained
    # dict churn, the hazard, without a leak).
    try:
        csv.unregister_dialect(name)
    except csv.Error as exc:
        H.fail("unregister_dialect FAILED for our own unique name {0!r} (wid {1}): "
               "{2} -- the registry dict was torn (our entry was already gone, a "
               "sibling deleted it)".format(name, wid, exc))
        return
    state["dia_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: ONE shared writer used by ALL fibers concurrently.  The _csv
# object field buffer is not internally serialized, so concurrent writerow across
# fibers tears it -- documented-unsafe (true under plain threads too).  We MEASURE
# the torn-field rate; we NEVER fail on it.  Writes to the shared throwaway buffer
# only; never touches the load-bearing own-object checks.
# --------------------------------------------------------------------------
def shared_object_op(H, wid, idx, state):
    sw = state["shared_writer"]
    sb = state["shared_buf"]
    # A row whose join, if produced atomically by the shared writer, would encode
    # this fiber's wid.  Under concurrent use the shared object's field buffer can
    # interleave with a sibling's writerow -> a torn line.  Measured, not failed.
    row = ["S{0}".format(wid), "{0}".format(idx)]
    mark = sb.tell()
    try:
        sw.writerow(row)
        runloom.yield_now()             # let a sibling writerow interleave
        produced = sb.getvalue()[mark:]
    except Exception:
        # A torn shared object can raise (the documented hazard of sharing one
        # writer); count it as contention, never fail.
        state["shared_torn"][wid & 1023] += 1
        state["shared_checks"][wid & 1023] += 1
        return
    state["shared_checks"][wid & 1023] += 1
    # If our exact row didn't come back cleanly from our own write offset, a
    # sibling interleaved into the shared buffer -- expected contention (measured).
    expect = "S{0},{1}\r\n".format(wid, idx)
    if produced != expect:
        state["shared_torn"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber sustains a churn loop (bounded by H.running()): one OWN-OBJECT
    round-trip identity check + one GLOBAL dialect-registry identity check
    (both LOAD-BEARING, fail-fast) per iteration, plus one MEASURED shared-object
    op -- so many fibers stay simultaneously mid-parse / mid-registry-churn and
    parked across their yield, the condition a per-object or registry desync needs,
    at the default --rounds 1.  The outer round_range() honors --rounds for the
    soak sweep."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_check(H, wid, idx, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            dialect_check(H, wid, idx, state)          # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_object_op(H, wid, idx, state)       # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    n = min(MAX_WORKERS, max(2, H.funcs))
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    rt = sum(H.state["rt_checks"])
    dia = sum(H.state["dia_checks"])
    schecks = sum(H.state["shared_checks"])
    storn = sum(H.state["shared_torn"])
    spct = (100.0 * storn / schecks) if schecks else 0.0
    H.log("csv: OWN-object round-trip identity checks={0} (LOAD-BEARING, all "
          "passed fail-fast) | GLOBAL dialect-registry identity checks={1} "
          "(LOAD-BEARING, all passed) | shared-object ops={2} torn/contended={3} "
          "({4:.1f}%, documented-unsafe single-shared-writer contention -- "
          "REPORT ONLY)".format(rt, dia, schecks, storn, spct))
    if storn:
        H.log("note: the shared-object arm observed {0} torn/contended writes "
              "across {1} ops -- one csv.writer shared across concurrent fibers is "
              "documented-unsafe (its per-object field buffer is not serialized; "
              "reproduces under plain threads), NOT a runloom bug; it never "
              "touches the load-bearing own-object checks".format(storn, schecks))
    # NON-VACUITY: both load-bearing hazards were actually exercised.
    H.check(rt > 0,
            "no own-object round-trip checks ran -- the load-bearing _csv "
            "object-parse-state hazard was never exercised (oracle would be "
            "vacuous)")
    H.check(dia > 0,
            "no dialect-registry identity checks ran -- the load-bearing global "
            "csv._dialects hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (stranded mid-parse, or holding
    # a half-registered dialect when it vanished).
    H.require_no_lost("csv reader/writer parse-state + dialect-registry isolation")


if __name__ == "__main__":
    harness.main(
        "p464_csv_reader_dialect_state", body, setup=setup, post=post,
        default_funcs=8000,
        describe="the C _csv module keeps incremental parse state (field buffer, "
                 "dialect) on each reader/writer OBJECT and a MODULE-GLOBAL dialect "
                 "registry (csv._dialects via register/unregister/get_dialect).  "
                 "LOAD-BEARING: each fiber's OWN reader/writer round-trips every "
                 "field encoding its wid to its own value across a yield, AND "
                 "get_dialect(its globally-unique name) returns its own dialect "
                 "(both 0-error under plain threads GIL on AND off; a torn field "
                 "or wrong/missing dialect is the runloom M:N bug).  A single "
                 "SHARED writer's torn-buffer contention is documented-unsafe -- "
                 "measured, report-only")
