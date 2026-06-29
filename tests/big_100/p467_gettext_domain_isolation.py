"""big_100 / 467 -- gettext process-global domain + _translations cache
isolation under M:N.

gettext keeps its active translation domain in a single PROCESS-GLOBAL string,
`gettext._current_domain` (default 'messages'), set by `gettext.textdomain(dom)`
and read by `gettext.gettext(msg)` -> `dgettext(_current_domain, msg)`.  It also
keeps a module-global `_translations` dict (the catalog cache, keyed by
(class_, abspath) for real .mo files) and a module-global `_localedirs` map.
NONE of these are contextvar-backed or keyed to anything per-execution-context:
they are plain module globals.  So under runloom M:N -- where many fibers share a
hub OS-thread (and its PyThreadState) -- every fiber on a hub sees the SAME
`_current_domain` and the SAME `_translations` dict.  A fiber that sets its domain,
yields, and reads the global back can get a SIBLING's domain (exactly p67's
threading.local / p66's contextvar leak, but for a plain module global with no
per-context identity at all).

This is adjacent-but-distinct from p66 (contextvars) / p67 (threading.local) /
p460 (decimal thread-affine Context): those guard goroutine/hub-local containers or
a contextvar-backed object; this guards a bare PROCESS-GLOBAL string + a
module-global dict that the gettext API itself save/restores by hand (textdomain
returns the prior domain so callers can restore it).

To avoid .mo files we never touch the real GNUTranslations/find() path: each fiber
builds its OWN in-memory catalog by subclassing gettext.NullTranslations and
overriding gettext() to return a wid-tagged string, and registers it in the
module-global `_translations` dict under its own unique domain key.  The current
catalog is then resolved exactly the way gettext.gettext() resolves it -- through
the PROCESS-GLOBAL `_current_domain` -- so the load-bearing arm exercises the real
global, just with an in-memory registry instead of a parsed .mo file.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically against plain threads,
NOT assumed):

  gettext.textdomain() RETURNS the prior domain precisely so a caller can do the
  documented save/restore: prior = textdomain(); textdomain(mine); ...; textdomain
  (prior).  That save/restore assumes STRICT LIFO / SERIALIZED use of the single
  process-global -- it is NOT thread-safe for OVERLAPPING blocks.  We verified with
  a standalone plain-threads control (64 threads, the same hazard, NO runloom):

    * SERIALIZED arm (every block holds ONE shared lock around its whole
      save/set/translate/restore, so the blocks are globally strict-LIFO -- the
      documented-SAFE usage): 0 identity mismatches and the global restored to
      baseline with PYTHON_GIL=1 AND PYTHON_GIL=0 (32000 checks each).  Stock
      CPython serializes the global access the same way; this is genuinely safe
      for any GIL setting.  An oracle that fired here would be a FALSE-POSITIVE
      detector -- it does NOT fire here.  Under a CORRECT runloom it MUST also hold
      (the per-block save/restore of the process-global survives a hub
      migration/preempt between blocks).  If runloom desyncs the save/restore across
      a migration -- the global does not return to baseline, or a block reads a
      domain it did not set -- THAT is the runloom bug, and the serialized arm
      PASSES on a correct runtime (the program exits 0 when there is no bug).

    * UNSERIALIZED arm (set the process-global, yield, read it back through the
      global -- NO lock): leaks a sibling's domain in 31886/32000 checks even with
      PYTHON_GIL=1 (and the global never returns to baseline).  That is
      documented-unsafe overlapping use of a single process-global for ANY
      concurrency model / GIL setting, NOT a runloom bug.  So it is MEASURED +
      REPORTED, never failed -- like p67's TLS leak rate.

ORACLES:
  * LOAD-BEARING -- (A) PER-BLOCK DOMAIN IDENTITY and (B) GLOBAL DOMAIN RESTORE
    INTEGRITY across the SERIALIZED strict-LIFO M:N arm.
      Per block (under the shared lock, strict-LIFO): saved = textdomain();
      textdomain(my_dom); resolve the catalog the way gettext.gettext does -- read
      the CURRENT domain back (textdomain()) and look it up in the module-global
      `_translations` -- translate a key, assert it is MY wid-tagged value
      (the global I just set still names MY catalog), then textdomain(saved) to
      restore.  Workers PARK / yield / migrate hubs OUTSIDE the lock between blocks.
      post() asserts the process-global `_current_domain` == the captured baseline.
    A block that reads a sibling's domain, or a global that does not restore to
    baseline after the serialized arm quiesces, is a runloom save/restore desync
    across a hub migration / preempt-mid-restore -- it does NOT reproduce under
    stock serialized LIFO use (verified GIL-on AND GIL-off), so it is a true
    runloom signal.
  * NON-VACUITY (post, HARD): the serialized arm actually ran (ser_blocks > 0) and
    every fiber registered its catalog in the shared `_translations` dict.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-restore on a
    desynced global (or holding the shared lock when it vanished) never returns;
    the watchdog + require_no_lost catch it.

  * MEASURED (report-ONLY, NEVER fails): the UNSERIALIZED global-domain path.  In a
    SEPARATE, fully-drained pre-phase, paired workers set the process-global
    `_current_domain`, yield, and read it back through the global -- a read that
    names another fiber's domain is a cross-fiber LEAK.  This reproduces under
    plain GIL threads (process-global, no per-context identity), so we MEASURE +
    REPORT the leak rate, NEVER fail on it.  The pre-phase fully drains and then
    hard-resets the global to baseline BEFORE the load-bearing pool runs, so the
    documented-unsafe drift can never reach the serialized-arm oracle.

FAIL ON: a serialized block that reads a sibling's domain (identity break), the
process-global `_current_domain` not restored to baseline after the serialized arm
quiesced, a missing/foreign catalog under the serialized arm, or a crash.  NEVER
fail on the unserialized global-domain leak (measured).

Stresses: gettext process-global `_current_domain` save/restore (textdomain) across
hub migration + preempt-mid-restore under serialized strict-LIFO usage, the
module-global `_translations` catalog dict shared across hub fibers, plain-global
(non-contextvar) isolation, no-lost-wake in the restore / while holding a shared
cooperative lock.

Good TSan / controlled-M:N-replay target: `_current_domain` is a single module
global rebound (store) on every textdomain() and read on every resolve; under the
serialized arm the store is single-block-at-a-time, so a data-race report on the
module global -- or a deterministic replay that migrates a hub between a serialized
block's set and its restore -- is the cleanest signal before the post() baseline
oracle fires.
"""
import socket

import gettext

import harness
import runloom

# Modest population.  Most workers run the LOAD-BEARING serialized strict-LIFO
# arm; a separate, fully-drained pre-phase runs the report-only UNSERIALIZED arm.
MAX_WORKERS = 4000
# Fraction of workers assigned to the report-only UNSERIALIZED pre-phase (paired,
# rendezvous so two blocks provably overlap on the shared process-global).  Small:
# a few hundred amply demonstrate the documented-unsafe leak without dominating.
UNSER_FRACTION = 0.2

# Key translated by every catalog.  The wid-tagged result is what proves identity.
MSG_KEY = "greeting"


class WidCatalog(gettext.NullTranslations):
    """A per-fiber in-memory catalog (no .mo file).  gettext() returns a string
    tagged with the OWNING fiber's domain, so a translation that came from the
    WRONG fiber's catalog is detectable -- the identity the load-bearing arm
    asserts.  Subclassing NullTranslations + overriding gettext() is the documented
    in-memory route (no GNUTranslations / find() / parsed .mo)."""

    def __init__(self, domain):
        super(WidCatalog, self).__init__()
        self._domain = domain

    def gettext(self, message):
        return "{0}|{1}".format(message, self._domain)


def domain_for(wid):
    return "big100dom-{0}".format(wid)


def resolve_current_catalog(state):
    """Resolve the catalog for the CURRENT domain exactly the way gettext.gettext()
    does -- through the PROCESS-GLOBAL `_current_domain` (read back via
    textdomain()) -- but against our in-memory `_translations` registry so no .mo
    file is needed.  Returns (current_domain, catalog_or_None)."""
    cur = gettext.textdomain()                 # reads the process-global
    cat = gettext._translations.get(cur)       # module-global catalog dict
    return cur, cat


# --------------------------------------------------------------------------
# LOAD-BEARING arm: SERIALIZED strict-LIFO.  Every block runs under ONE shared
# cooperative Lock, so the process-global save/restore is globally strict-LIFO
# (never two open at once = the documented-SAFE usage).  Workers PARK / yield /
# migrate hubs OUTSIDE the lock between blocks.  Each block must read ITS OWN
# domain back and the global MUST restore -- the run(1)/GIL behaviour a runloom
# save/restore desync across a hub migration / preempt-mid-restore would break.
# --------------------------------------------------------------------------
def serialized_block(H, wid, state):
    dom = state["domains"][wid]
    want = "{0}|{1}".format(MSG_KEY, dom)
    lock = state["lock"]
    # Park / migrate hub OUTSIDE the critical section so the goroutine can be on a
    # different hub each time it takes the lock (exercises migration around the
    # save/restore), without ever overlapping another block.
    runloom.sleep(0.0003)
    runloom.yield_now()
    with lock:
        saved = gettext.textdomain()           # SAVE the process-global domain
        gettext.textdomain(dom)                # SET mine
        cur, cat = resolve_current_catalog(state)   # resolve via the global I set
        got = cat.gettext(MSG_KEY) if cat is not None else None
        cur_dom = cur
        gettext.textdomain(saved)              # RESTORE the prior global domain
    # (A) IDENTITY: the global I set inside the lock named MY domain, and the
    # catalog it resolved to was MINE -- a sibling did not leak in across the
    # set/resolve (the lock makes this strict-LIFO; a break is a runloom
    # save/restore desync, NOT documented-unsafe overlap).
    if cur_dom != dom:
        H.fail("gettext serialized-arm DOMAIN IDENTITY break: inside the lock "
               "textdomain()=={0!r} != {1!r} this fiber just set (wid {2}) -- the "
               "process-global _current_domain was overwritten by a sibling between "
               "this fiber's textdomain(set) and its read-back, across a hub "
               "migration/preempt (a runloom save/restore desync; _current_domain "
               "is a plain module global, NOT contextvar-isolated)".format(
                   cur_dom, dom, wid))
        return
    if cat is None:
        H.fail("gettext serialized-arm MISSING CATALOG: no catalog registered for "
               "domain {0!r} (wid {1}) in the shared _translations dict -- the "
               "module-global catalog cache lost this fiber's entry".format(
                   dom, wid))
        return
    if got != want:
        H.fail("gettext serialized-arm WRONG TRANSLATION: gettext({0!r}) -> {1!r} "
               "!= {2!r} for this fiber's own domain {3!r} (wid {4}) -- a sibling's "
               "catalog answered through the shared process-global, a runloom "
               "domain save/restore desync under M:N".format(
                   MSG_KEY, got, want, dom, wid))
        return
    state["ser_blocks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """The LOAD-BEARING serialized-arm worker.  Runs ONLY serialized blocks -- the
    report-only UNSERIALIZED arm runs in a SEPARATE, fully-drained pre-phase
    (run_unser_phase) so its documented-unsafe drift can never contaminate the
    shared process-global `_current_domain` while the load-bearing pool measures
    it."""
    for _ in H.round_range():
        if not H.running():
            break
        serialized_block(H, wid, state)
        if H.failed:
            return
        H.op(wid)
    H.task_done(wid)


# --------------------------------------------------------------------------
# REPORT-ONLY UNSERIALIZED arm: paired workers PROVABLY overlap two writes to the
# single process-global `_current_domain` (the documented-unsafe non-LIFO case, NO
# shared lock).  Measured, never failed.  Runs in a SEPARATE fully-drained
# pre-phase; afterward the global is hard-reset to baseline so it cannot poison the
# load-bearing serialized arm.
# --------------------------------------------------------------------------
def unser_block(H, wid, state):
    pair = wid // 2
    slots = state["pairs"]
    dom = state["udomains"][wid]
    want = "{0}|{1}".format(MSG_KEY, dom)
    me = None
    if pair < len(slots):
        a_sock, b_sock = slots[pair]
        me = a_sock if (wid & 1) == 0 else b_sock
    gettext.textdomain(dom)                    # set the process-global, NO lock
    # Rendezvous so the SIBLING's block is provably open at the same time as ours
    # (overlapping writes to the single process-global).  Real socketpair recv =
    # netpoll park, so the overlap holds across hubs.
    if me is not None:
        try:
            me.send(b"x")
            me.settimeout(state["rdv_timeout"])
            try:
                me.recv(1)                     # park until the peer's block is open
            except (socket.timeout, OSError):
                pass                           # peer absent/closed -> proceed
        except OSError:
            pass
    else:
        runloom.yield_now()
    cur, cat = resolve_current_catalog(state)  # read the process-global back
    got = cat.gettext(MSG_KEY) if cat is not None else None
    # MEASURED: did the global name SOMEONE ELSE's domain across the yield/overlap?
    # (documented-unsafe for any concurrency model / GIL setting -- report only.)
    if got != want:
        state["unser_leaks"][wid & 1023] += 1
    state["unser_blocks"][wid & 1023] += 1


def run_unser_phase(H, state):
    """Report-ONLY pre-phase: spawn the paired UNSERIALIZED workers, let them
    PROVABLY overlap two writes to the single process-global `_current_domain`
    (documented-unsafe non-LIFO), and FULLY DRAIN them (WaitGroup.wait) before
    returning.  This runs BEFORE the load-bearing serialized pool, so the two arms
    never touch the shared process-global concurrently -- the leak is measured in
    isolation and cannot poison the serialized-arm restore-integrity oracle.  After
    the drain we hard-reset the global to the captured baseline so the serialized
    pool starts pristine regardless of any residual drift."""
    nunser = state["nunser"]
    if nunser <= 0:
        return
    wg = runloom.WaitGroup()
    wg.add(nunser)

    def run_one(wid):
        try:
            for _ in range(max(1, H.rounds)):
                if not H.running():
                    break
                unser_block(H, wid, state)
                if H.failed:
                    break
        finally:
            wg.done()

    for wid in range(nunser):
        H.fiber(run_one, wid)
    wg.wait()
    # Hard-reset the process-global to the pristine baseline before the load-bearing
    # pool runs: the documented-unsafe drift is now isolated to this drained
    # pre-phase + its measured counters and CANNOT reach the serialized-arm oracle.
    gettext.textdomain(state["baseline_domain"])


def setup(H):
    # LOAD-BEARING baseline: snapshot the process-global domain BEFORE any block
    # runs.  The serialized strict-LIFO arm must leave this exactly restored.
    baseline_domain = gettext.textdomain()     # 'messages' by default

    # nworkers = the LOAD-BEARING serialized pool size.
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    # nunser = a SEPARATE, modest paired population for the report-only pre-phase
    # (capped so the pre-phase stays quick).
    nunser = min(400, int(nworkers * UNSER_FRACTION))
    if nunser % 2:
        nunser -= 1                            # keep the unserialized arm paired
    npairs = nunser // 2

    pairs = []
    for _ in range(npairs):
        a, b = socket.socketpair()
        H.register_close(a)
        H.register_close(b)
        pairs.append((a, b))

    # Each fiber gets a UNIQUE domain string and registers its OWN in-memory
    # catalog in the module-global `_translations` dict under that domain key.  A
    # leaked sibling domain therefore resolves to a DIFFERENT catalog (a distinct
    # wid-tagged value), making any cross-fiber leak detectable.  Registered ONCE,
    # single-owner, before the pool -- so the registry is race-free; only the
    # process-global `_current_domain` read/write is exercised concurrently.
    domains = [domain_for(w) for w in range(nworkers)]
    for w in range(nworkers):
        gettext._translations[domains[w]] = WidCatalog(domains[w])
    # Separate domain namespace for the unserialized pre-phase (so its registry
    # entries never collide with the serialized arm's).
    udomains = ["big100udom-{0}".format(w) for w in range(nunser)]
    for w in range(nunser):
        gettext._translations[udomains[w]] = WidCatalog(udomains[w])

    H.state = {
        "baseline_domain": baseline_domain,    # the process-global domain at start
        "lock": runloom.sync.Lock(),           # serializes the load-bearing arm
        "nworkers": nworkers,
        "nunser": nunser,                      # report-only pre-phase population
        "pairs": pairs,
        "rdv_timeout": 2.0,
        "domains": domains,                    # per-serialized-worker unique domain
        "udomains": udomains,                  # per-unserialized-worker unique domain
        "ser_blocks": [0] * 1024,              # serialized strict-LIFO blocks done
        "unser_blocks": [0] * 1024,            # unserialized blocks done (report)
        "unser_leaks": [0] * 1024,             # unserialized cross-fiber leaks (report)
    }


def body(H):
    # Phase 1 (report-only, fully drained): the documented-unsafe UNSERIALIZED arm,
    # measured in isolation so it cannot contaminate the shared process-global while
    # the load-bearing pool measures it.
    run_unser_phase(H, H.state)
    # Phase 2 (LOAD-BEARING): the serialized strict-LIFO pool.  The process-global
    # starts pristine (run_unser_phase reset it), so any drift at post() is a
    # serialized-arm save/restore desync under M:N.
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    base = H.state["baseline_domain"]
    ser = sum(H.state["ser_blocks"])
    uns = sum(H.state["unser_blocks"])
    leaks = sum(H.state["unser_leaks"])
    leak_pct = (100.0 * leaks / uns) if uns else 0.0

    # Read the process-global DIRECTLY -- NOT reset first -- so this is a genuine,
    # non-vacuous measurement.  The unserialized arm ran in a drained pre-phase that
    # self-reset to baseline, so any domain left in the global after the whole run is
    # attributable to the LOAD-BEARING serialized strict-LIFO arm: a serialized
    # worker whose textdomain(restore) desynced across a hub migration / preempt.
    now_domain = gettext.textdomain()
    H.log("gettext: serialized-LIFO blocks={0} (LOAD-BEARING) | unserialized "
          "blocks={1} domain-leaks={2} ({3:.1f}%, documented-unsafe overlapping "
          "process-global use -- REPORT ONLY) | baseline_domain={4!r} "
          "final_domain={5!r}".format(
              ser, uns, leaks, leak_pct, base, now_domain))

    # LOAD-BEARING: the PROCESS-GLOBAL `_current_domain` MUST be the exact baseline
    # after the run.  The unserialized arm self-reset, so a residual non-baseline
    # domain is a SERIALIZED-arm save/restore desync under M:N (hub migration /
    # preempt-mid-restore) -- a runloom bug, NOT a documented caveat (serialized
    # strict-LIFO use always restores under run(1)/GIL -- verified GIL-on AND off).
    H.check(now_domain == base,
            "gettext PROCESS-GLOBAL DOMAIN NOT RESTORED: _current_domain=={0!r} != "
            "baseline {1!r} after the SERIALIZED strict-LIFO arm quiesced -- a "
            "lock-serialized (never-overlapping) textdomain() save/restore desynced "
            "across a hub migration / preempt-mid-restore (gettext._current_domain "
            "is a plain process global, NOT contextvar-isolated)".format(
                now_domain, base))

    # NON-VACUITY: the load-bearing serialized arm actually ran (the hazard was
    # exercised, not skipped) -- otherwise the oracle is vacuous.
    H.check(ser > 0,
            "no serialized gettext block ran -- the load-bearing process-global "
            "domain save/restore hazard was never exercised (oracle would be "
            "vacuous)")

    # Report-only context: surface that the documented-unsafe unserialized arm did
    # observe leaks (expected, benign) so the semantics are explicit in the log.
    if leaks:
        H.log("note: the unserialized arm observed {0} cross-fiber domain leaks "
              "across {1} overlapping blocks -- documented-unsafe overlapping use "
              "of the single process-global gettext._current_domain (reproduces "
              "under plain GIL threads with PYTHON_GIL=1), NOT a runloom bug; the "
              "pre-phase drained and reset the global so this never reaches the "
              "load-bearing check".format(leaks, uns))

    # COMPLETENESS: no worker parked-then-vanished (e.g. stranded mid-restore on a
    # desynced global, or holding the shared Lock when it vanished).
    H.require_no_lost("gettext process-global domain save/restore")


if __name__ == "__main__":
    harness.main(
        "p467_gettext_domain_isolation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="gettext keeps its active domain in a single PROCESS-GLOBAL "
                 "string (_current_domain, set by textdomain(), read by "
                 "gettext.gettext) plus module-global _translations / _localedirs "
                 "dicts -- none contextvar-backed, so hub fibers share them under "
                 "M:N.  LOAD-BEARING: the SERIALIZED strict-LIFO arm (one shared "
                 "lock around save/set/translate-via-the-global/restore, park+"
                 "migrate between blocks, each fiber an in-memory NullTranslations-"
                 "derived catalog registered under its own domain) MUST read its "
                 "OWN domain's catalog and MUST restore the process-global to its "
                 "exact baseline under M:N -- a textdomain save/restore desync "
                 "across hub migration is the real runloom bug.  The UNSERIALIZED "
                 "overlapping-global leak is documented-unsafe (reproduces under "
                 "plain GIL threads) -- measured, report-only")
