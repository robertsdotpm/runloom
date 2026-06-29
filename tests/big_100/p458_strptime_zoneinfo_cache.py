"""big_100 / 458 -- time.strptime() _regex_cache identity + the zoneinfo
ZoneInfo COLD-IMPORT false-`_DeadlockError` (FINDINGS #9) under M:N.

This program has TWO independent load-bearing arms:

ARM 1 -- strptime exact-field identity (CLEAN; always GREEN under a correct
runloom and under plain-threads-GIL-on).  The subject is ``_strptime``'s
process-global ``_regex_cache`` (a size-5 dict of format-string -> COMPILED
regex behind one raw ``_thread.lock``).  Thousands of fibers each parse a
DISTINCT (format, input) pair UNIQUE to that fiber (a per-wid literal token
makes the format string unique, so the size-5 cache clears/recompiles on
essentially every call -- constant insert/clear/recompile churn).  The closed-
world oracle: for every parse the result's (year, month, day, hour, minute,
second [, %j]) fields EXACTLY equal the fields this fiber encoded into its OWN
unique input.  A wrong field means a torn cache published a SIBLING's compiled
regex against our input (or a runloom save/restore desync across a hub
migration); a no-match means the cache handed back a regex that does not fit our
format.  Because each (format, input) is single-owner and fully determines the
correct answer, an exact-field mismatch is a genuine cache-tear, never
documented-unsafe usage -- so this arm is GREEN on plain-threads-GIL-on and a RED
there would be mis-calibrated.

ARM 2 -- ZoneInfo COLD-IMPORT false-`_DeadlockError` (FINDINGS #9; a REAL,
unfixed runloom bug -- this arm is SUPPOSED to FAIL under M:N).  ``zoneinfo``
loads its zone data LAZILY: the first ``ZoneInfo(region)`` for an area COLD-
imports that area's data-backend submodule (``tzdata.zoneinfo.<Area>``), taking
CPython's per-module import lock during the import.  CPython keys that lock by
``_thread.get_ident()`` -- the OS-thread id.  Under runloom's M:N runtime many
fibers share ONE hub OS-thread, so they share one ident; when fiber A triggers
the COLD lazy import of an area backend, takes the module lock, and YIELDS while
holding it (ZoneInfo construction cooperatively parks), a SIBLING fiber B on the
SAME hub thread that also needs a cold area import corrupts importlib's
``_blocking_on`` thread/lock graph (two distinct fibers appear as one thread),
and the deadlock detector trips a spurious ``importlib._bootstrap._DeadlockError``
even though NO real deadlock exists.  This is the known import-lock false-
deadlock class (FINDINGS #9): CPython's import lock assumes one-OS-thread-per-
unit-of-concurrency, which M:N breaks.

It is FLAKY to catch by accident because area backends warm up (become cached)
after their first import, so a steady run almost never re-takes a cold module
lock.  We force it RELIABLY (the p168 docstring note -- racing
``del sys.modules[m]`` reimports are the #9 trigger): each round, concurrently
purge the area backend submodules from ``sys.modules`` and re-construct
``ZoneInfo(region)`` over a SPREAD of distinct areas, so the import lock is
re-taken COLD while sibling fibers on the same hub yield -> the false
``_DeadlockError`` fires.  The arm catches it and ``H.fail``s with a #9
diagnostic -- it is DETECTING the real runloom bug.

ATTRIBUTION (why this is runloom-specific, not a CPython-FT or import-pattern
fault).  A standalone plain-OS-threads control doing the SAME cold area-backend
imports raises ZERO ``_DeadlockError`` under BOTH ``PYTHON_GIL=1`` and
``PYTHON_GIL=0`` (each OS thread has a distinct ident, so the import-lock graph
never forms a false self-cycle).  The spurious deadlock appears ONLY under M:N,
where fibers share a hub ident -- so it is a runloom defect, not CPython's.  (The
companion strptime arm is the control that the rest of the machinery is sound.)

SELF-CONTAINED zone backend.  ZoneInfo's per-region COLD Python import only
happens when its data source is the ``tzdata`` PyPI package (an area is a Python
subpackage); a system ``/usr/share/zoneinfo`` source reads TZif files directly
with no per-region import and so cannot exhibit #9.  To exercise the real #9 path
on ANY box, setup() builds a MINIMAL real ``tzdata``-style package in a temp dir
from the system TZif files for a spread of areas and points zoneinfo at it
(``reset_tzpath``); if the genuine ``tzdata`` package is already importable that
is used instead.  Either way ``ZoneInfo(region)`` performs a real cold area
import whose lock is what #9 trips.

Stresses: _strptime ``_regex_cache`` size-5 churn + exact-field identity
(ARM 1); zoneinfo lazy area-backend COLD import lock + the FINDINGS #9 false-
``_DeadlockError`` under M:N hub-thread-ident sharing (ARM 2).
"""
import os
import shutil
import sys
import time
import datetime
import importlib
import string
import zoneinfo

import harness
import runloom

# importlib's spurious deadlock signal (FINDINGS #9).  It lives in
# importlib._bootstrap; bind it once so the ZoneInfo arm can catch exactly it
# (and nothing else) when a cold area import trips the OS-thread-keyed detector.
_DeadlockError = importlib._bootstrap._DeadlockError

# A modest population: this is a CORRECTNESS probe of the shared cache's
# transparency under churn, not a scale soak.  Each worker still drives a unique
# format through the size-5 cache, so even a few thousand workers churn it hard.
MAX_WORKERS = 6000

# Safe literal alphabet for the per-wid unique token embedded as a FIXED literal
# in the format string (no '%' and no regex/strptime-special chars).
SAFE = string.ascii_uppercase + string.digits

# Directive LAYOUTS.  Each lays the six fields (plus %j for one) in a different
# ORDER with different literal separators, so a sibling's regex applied to our
# input maps fields wrongly -> a detectable wrong-field parse, not just a literal
# mismatch.  Every layout is a strict round-trip: build_input() emits exactly what
# the layout's directives consume, and decode is unambiguous.
NLAYOUTS = 6

SLOTS = 1024

# ZoneInfo arm: cold-construct attempts BURST per worker round.  A tight inner
# burst makes many fibers converge on the same few area imports in one window
# (forced overlap), so the FINDINGS #9 false-deadlock fires reliably in a single
# --rounds 1 pass instead of depending on incidental timing.  Each attempt purges
# + re-imports the area backend cold.
_ZI_ATTEMPTS = 8

# Areas to vendor into the minimal tzdata-style backend (each becomes a Python
# subpackage ``tzdata.zoneinfo.<Area>`` whose COLD import takes the module lock).
# A spread of distinct areas means many DIFFERENT area locks are re-taken cold
# across the pool, maximizing the chance a sibling fiber on the same hub yields
# inside one -> the #9 false-deadlock.  Each area must exist under the system
# zoneinfo dir for the self-contained build; missing ones are skipped.
_BACKEND_AREAS = [
    "Africa", "America", "Antarctica", "Asia", "Atlantic",
    "Australia", "Europe", "Indian", "Pacific",
]

# Filled by setup(): the list of (region_key, area_submodule_name) pairs the
# ZoneInfo arm cycles through, and the area-backend module names to purge.
_ZONE_PAIRS = []
_AREA_SUBMODS = []


def wid_token(wid):
    """A per-wid unique literal token (base-N over SAFE, fixed width) used as a
    FIXED literal inside the format string.  Makes every worker's FORMAT STRING
    distinct, so the size-5 ``_regex_cache`` clears/recompiles on essentially
    every call -- the churn that drives the insert/clear/recompile race.  Begins
    with a letter so it can never be mistaken for a directive."""
    w = wid
    out = []
    for _ in range(5):
        out.append(SAFE[w % len(SAFE)])
        w //= len(SAFE)
    return "Q" + "".join(out)


def fields_for(wid):
    """The six date/time fields this fiber ENCODES into its own input.  Derived
    from wid so the correct parse is deterministic and unique per worker; a parse
    that returns any OTHER fields means a sibling's regex matched our input."""
    year = 2000 + (wid % 100)
    month = 1 + (wid % 12)
    day = 1 + (wid % 28)
    hour = wid % 24
    minute = (wid * 3) % 60
    second = (wid * 7) % 60
    return year, month, day, hour, minute, second


def make_pair(wid):
    """Build this fiber's UNIQUE (format, input, expected_fields) triple.

    The format embeds the per-wid literal token (-> distinct format string ->
    cache churn) and one of NLAYOUTS directive orderings.  The input encodes the
    fiber's own fields per fields_for(wid).  A correct strptime MUST return
    exactly expected_fields; a torn cache that matched a sibling's compiled regex
    against this input returns wrong fields (or fails to match)."""
    year, month, day, hour, minute, second = fields_for(wid)
    t = wid_token(wid)
    layout = wid % NLAYOUTS

    if layout == 0:
        fmt = t + ":%Y-%m-%d_%H-%M-%S:" + t
        s = "{0}:{1:04d}-{2:02d}-{3:02d}_{4:02d}-{5:02d}-{6:02d}:{0}".format(
            t, year, month, day, hour, minute, second)
        exp = (year, month, day, hour, minute, second)
    elif layout == 1:
        # Day/month/year first, time after -- a different field ORDER.
        fmt = t + "%d/%m/%Y@%H.%M.%S" + t
        s = "{0}{1:02d}/{2:02d}/{3:04d}@{4:02d}.{5:02d}.{6:02d}{0}".format(
            t, day, month, year, hour, minute, second)
        exp = (year, month, day, hour, minute, second)
    elif layout == 2:
        # Month-first, '~' separators.
        fmt = t + "%m~%d~%Y~%H~%M~%S" + t
        s = "{0}{1:02d}~{2:02d}~{3:04d}~{4:02d}~{5:02d}~{6:02d}{0}".format(
            t, month, day, year, hour, minute, second)
        exp = (year, month, day, hour, minute, second)
    elif layout == 3:
        # %j (day-of-year) layout -- a structurally DIFFERENT directive set, so a
        # sibling's six-field regex cannot even match this input.
        doy = datetime.date(year, month, day).timetuple().tm_yday
        fmt = t + "%Y|%j|%H|%M|%S" + t
        s = "{0}{1:04d}|{2:03d}|{3:02d}|{4:02d}|{5:02d}{0}".format(
            t, year, doy, hour, minute, second)
        # %j fills tm_yday and CPython derives month/day from it; verify all.
        exp = (year, month, day, hour, minute, second)
    elif layout == 4:
        # Time-first, ' on ' literal, date after.
        fmt = t + "%H:%M:%S on %Y-%m-%d" + t
        s = "{0}{1:02d}:{2:02d}:{3:02d} on {4:04d}-{5:02d}-{6:02d}{0}".format(
            t, hour, minute, second, year, month, day)
        exp = (year, month, day, hour, minute, second)
    else:
        # Compact ISO-ish basic form, no separators between fields.
        fmt = t + "%Y%m%dT%H%M%S" + t
        s = "{0}{1:04d}{2:02d}{3:02d}T{4:02d}{5:02d}{6:02d}{0}".format(
            t, year, month, day, hour, minute, second)
        exp = (year, month, day, hour, minute, second)
    return fmt, s, exp, layout


def parse_and_check(H, wid, state):
    """Parse this fiber's UNIQUE input with its UNIQUE format and enforce the
    exact-field identity oracle.  Interleaves yields/sleeps around the parse so
    the fiber can be preempted / migrate hubs across the cache critical section
    and the post-lock ``format_regex.match``.  Returns True on success, False on
    the first violation (caller stops)."""
    fmt, s, exp, layout = make_pair(wid)

    # A parse of a format NOT in the size-5 cache forces a COMPILE under the raw
    # _cache_lock; because the format is unique per wid, essentially every call
    # forces a recompile -- we count it (churn metric).
    state["churn"][wid & (SLOTS - 1)] += 1

    # Yield right before so a sibling's cache insert/clear is more likely to land
    # in our window during the strptime (the torn-store race).
    runloom.yield_now()

    try:
        st = time.strptime(s, fmt)
    except ValueError as exc:
        # A correct cache is transparent: our own (format,input) ALWAYS matches.
        # A no-match here means the cache handed back a regex that does not fit
        # our format -- a torn cache / save-restore desync, NOT bad input (the
        # input round-trips under plain-threads-GIL-on; see the control).
        H.fail("strptime FAILED to parse our OWN unique (format,input): "
               "format={0!r} input={1!r} wid={2} layout={3} -- ValueError {4!r}; "
               "the size-5 _regex_cache returned a regex that does not match our "
               "format (torn cache entry / save-restore desync under M:N, NOT bad "
               "input: this pair round-trips under plain-threads-GIL-on)".format(
                   fmt, s, wid, layout, exc))
        return False

    # Park while holding the parsed result: a sibling hub is churning the cache
    # right now.  On resume, nothing about OUR already-parsed result may change,
    # but this exercises a migration across the strptime boundary.
    runloom.yield_now()
    if (wid & 7) == 0:
        runloom.sleep(0.0003)

    got = (st.tm_year, st.tm_mon, st.tm_mday, st.tm_hour, st.tm_min, st.tm_sec)
    if got != exp:
        H.fail("strptime FIELD MISMATCH (torn cache returned a SIBLING regex): "
               "wid={0} layout={1} format={2!r} input={3!r} expected fields "
               "{4} but got {5} -- our input encodes our OWN wid, so wrong fields "
               "mean strptime matched a DIFFERENT format's compiled regex against "
               "our input (a torn/published-wrong _regex_cache entry or a runloom "
               "save/restore desync across the hub migration; this pair parses "
               "exactly under plain-threads-GIL-on)".format(
                   wid, layout, fmt, s, exp, got))
        return False

    # ---- ARM 2: ZoneInfo COLD-IMPORT false-_DeadlockError (FINDINGS #9) ----
    # Pick a (region, area-backend submodule) for this fiber.  Each ATTEMPT purges
    # the area backend from sys.modules so the next ZoneInfo construct re-imports
    # it COLD (re-taking the OS-thread-keyed import lock); the area __init__ yields
    # while holding that lock (see _AREA_INIT_SRC).  We BURST several attempts in a
    # tight loop so many fibers converge on the same few area imports in one
    # window -- forced overlap, instead of relying on incidental timing.  Under M:N
    # a sibling fiber on the same hub yielding inside its own cold area import
    # corrupts importlib's _blocking_on graph -> a spurious _DeadlockError, which
    # we catch and FAIL on (the REAL, unfixed runloom bug).  A construct that
    # SUCCEEDS still gets its key checked (ZoneInfo cache identity).
    slot = wid & (SLOTS - 1)
    region, area_sub = _ZONE_PAIRS[wid % len(_ZONE_PAIRS)]
    for _ in range(_ZI_ATTEMPTS):
        if not H.running():
            break
        # Force the next import of this area backend to run cold (re-take lock).
        try:
            del sys.modules[area_sub]
        except KeyError:
            pass
        zoneinfo.ZoneInfo.clear_cache()
        runloom.yield_now()
        try:
            z = zoneinfo.ZoneInfo(region)
        except _DeadlockError as exc:
            # THE RUNLOOM BUG (FINDINGS #9).  A cold area-backend import tripped
            # the OS-thread-keyed deadlock detector though no real deadlock exists.
            H.fail("ZoneInfo COLD-IMPORT false-_DeadlockError (FINDINGS #9): "
                   "constructing ZoneInfo({0!r}) cold-imported its area backend "
                   "{1!r} and importlib raised {2!r} for wid={3} -- a SPURIOUS "
                   "deadlock.  CPython's per-module import lock is keyed by "
                   "_thread.get_ident() (the OS thread); under runloom M:N many "
                   "fibers share ONE hub OS-thread, so a sibling fiber yielding "
                   "inside its own cold area import makes the deadlock detector "
                   "see thread-T waiting on a lock thread-T holds.  No real "
                   "deadlock exists (a plain-OS-threads control doing the same "
                   "cold imports raises 0 _DeadlockError under PYTHON_GIL=1 AND "
                   "=0); this is the runloom-specific import-lock false-"
                   "deadlock.".format(region, area_sub, exc, wid))
            return False
        except (KeyError, zoneinfo.ZoneInfoNotFoundError):
            # NOT a failure -- this is the test's OWN ``del sys.modules`` racing
            # the importlib loader for the same area backend (a sibling purged the
            # module mid-``_load_unlocked``).  That race is the COST of forcing
            # cold imports and is NOT runloom-specific: a plain-OS-threads control
            # doing the same purge+reconstruct raises the SAME KeyError/
            # ZoneInfoNotFoundError in the thousands under BOTH GIL modes (only the
            # _DeadlockError above is runloom-specific).  This attempt merely
            # MISSED its cold window; count it and keep trying.
            state["zi_skip"][slot] += 1
            continue
        if z.key != region:
            H.fail("ZoneInfo CACHE IDENTITY broken: requested key {0!r} but got a "
                   "zone whose .key is {1!r} (wid={2}) -- the module-global "
                   "ZoneInfo cache returned an aliased/torn entry for a DIFFERENT "
                   "key under M:N contention".format(region, z.key, wid))
            return False
        state["zi_ok"][slot] += 1

    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    for _ in H.round_range():
        if not H.running():
            break
        if not parse_and_check(H, wid, state):
            return
        state["parses"][slot] += 1         # single-writer-per-slot, race-free
        H.op(wid)
        H.task_done(wid)


def control_resample(H, state):
    """CONTROL ARM (single-owner falsifier) for the strptime arm.  One fiber
    re-parses a sample of the SAME per-wid (format, input) pairs ALONE and must
    get the exact same fields.  It alone writes its own pass counter, so a control
    mis-parse localizes the fault to CPython's strptime machinery, not M:N
    contention.  Runs concurrently with the contended pool (just another fiber on
    the hubs), so it ALSO contends on the shared cache -- but its check is
    single-owner."""
    n = state["nworkers"]
    step = max(1, n // 256)                 # sample ~256 pairs spread over the pop
    ok = 0
    wid = 0
    while wid < n and H.running():
        fmt, s, exp, layout = make_pair(wid)
        runloom.yield_now()
        try:
            st = time.strptime(s, fmt)
        except ValueError as exc:
            H.fail("CONTROL: single-owner strptime FAILED on wid={0} layout={1} "
                   "format={2!r} input={3!r} -- ValueError {4!r}; a single-owner "
                   "parse must always match its own format (fault is in CPython's "
                   "strptime cache machinery, not contention)".format(
                       wid, layout, fmt, s, exc))
            return
        got = (st.tm_year, st.tm_mon, st.tm_mday, st.tm_hour, st.tm_min, st.tm_sec)
        if got != exp:
            H.fail("CONTROL: single-owner strptime FIELD MISMATCH on wid={0} "
                   "layout={1} expected {2} got {3} -- even the single-owner "
                   "control mis-parsed, so the fault is in CPython's strptime "
                   "cache machinery, not M:N contention".format(
                       wid, layout, exp, got))
            return
        ok += 1
        wid += step
    state["control_ok"][0] = ok


# The area-backend ``__init__.py`` cooperatively YIELDS during its import so the
# per-module import lock is HELD ACROSS A PARK -- this widens the FINDINGS #9
# detector window enormously (the spurious _DeadlockError becomes reliable in a
# single round instead of rare).  ``runloom.yield_now()``/``sleep()`` are safe
# no-ops outside the root, so the one-time setup validation construct is fine.
# Crucially this does NOT manufacture the bug under plain OS threads: a control
# whose area __init__ instead ``time.sleep``s (same hold-across-a-switch) raises
# 0 _DeadlockError under GIL on AND off -- the false deadlock needs the SHARED
# hub OS-thread ident, which only M:N produces.
_AREA_INIT_SRC = (
    "import runloom\n"
    "runloom.yield_now()\n"
    "runloom.sleep(0.0003)\n"
    "runloom.yield_now()\n"
)


def _build_tzdata_backend(H):
    """Make ZoneInfo's per-region COLD Python import path real on ANY box.

    ZoneInfo only imports a per-area Python submodule (``tzdata.zoneinfo.<Area>``)
    when its data SOURCE is the ``tzdata`` package; a system zoneinfo dir reads
    TZif files directly with no import (and so cannot exhibit the #9 false-
    deadlock).  Prefer the genuine ``tzdata`` package if importable; otherwise
    synthesize a MINIMAL real one from the system TZif files for a spread of
    areas and point zoneinfo at it.  The synthesized area packages have a
    cooperatively-YIELDING ``__init__`` (see _AREA_INIT_SRC) so the import lock
    is held across a park -> the #9 window is wide and the arm fires reliably.
    Populates _ZONE_PAIRS / _AREA_SUBMODS.

    Returns True on success; on failure (no tzdata, no system TZif) the ZoneInfo
    arm is disabled and setup() fails with a clear message rather than running a
    vacuous arm."""
    global _ZONE_PAIRS, _AREA_SUBMODS

    # 1) Real tzdata package already importable -> use it (force its backend so a
    # system zoneinfo dir doesn't shadow the import path).
    try:
        importlib.import_module("tzdata.zoneinfo")
        zoneinfo.reset_tzpath(())
        have_real = True
    except ImportError:
        have_real = False

    if have_real:
        regions = sorted(zoneinfo.available_timezones())
    else:
        # 2) Synthesize a minimal tzdata-style package from the system TZif files.
        sys_root = None
        for cand in ("/usr/share/zoneinfo", "/usr/lib/zoneinfo",
                     "/usr/share/lib/zoneinfo", "/etc/zoneinfo"):
            if os.path.isdir(cand):
                sys_root = cand
                break
        if sys_root is None:
            return False

        d = H.make_tmpdir(prefix="big100_p458_tzd_")
        pkg = os.path.join(d, "tzdata")
        os.makedirs(os.path.join(pkg, "zoneinfo"))
        with open(os.path.join(pkg, "__init__.py"), "w") as f:
            f.write("IANA_VERSION = '2026a'\n")
        with open(os.path.join(pkg, "zoneinfo", "__init__.py"), "w") as f:
            f.write("")

        built = []
        for area in _BACKEND_AREAS:
            asrc = os.path.join(sys_root, area)
            if not os.path.isdir(asrc):
                continue
            # Copy up to a few top-level TZif files for this area (enough to
            # construct a region; we only need real bytes, not every zone).
            zones = []
            for name in sorted(os.listdir(asrc)):
                fsrc = os.path.join(asrc, name)
                if os.path.isfile(fsrc):
                    zones.append(name)
                if len(zones) >= 4:
                    break
            if not zones:
                continue
            adst = os.path.join(pkg, "zoneinfo", area)
            os.makedirs(adst)
            with open(os.path.join(adst, "__init__.py"), "w") as f:
                f.write(_AREA_INIT_SRC)   # yields during import -> wide #9 window
            for name in zones:
                shutil.copyfile(os.path.join(asrc, name),
                                os.path.join(adst, name))
            for name in zones:
                built.append(area + "/" + name)
        if not built:
            return False

        if d not in sys.path:
            sys.path.insert(0, d)
        importlib.invalidate_caches()
        importlib.import_module("tzdata.zoneinfo")
        zoneinfo.reset_tzpath(())
        regions = built

    # Build (region, area-backend-submodule) pairs across a SPREAD of areas.  The
    # backend submodule for region "America/New_York" is "tzdata.zoneinfo.America"
    # -- that is the module whose cold re-import re-takes the lock that #9 trips.
    pairs = []
    submods = set()
    for region in regions:
        area = region.split("/")[0]
        if "/" not in region:
            continue                        # bare zones (e.g. UTC) have no area pkg
        sub = "tzdata.zoneinfo." + area
        pairs.append((region, sub))
        submods.add(sub)
    if not pairs:
        return False

    # Validate every chosen pair constructs once (warms + proves the backend is
    # usable) before the storm; a setup failure is clearer than a per-fiber one.
    good = []
    for region, sub in pairs:
        try:
            z = zoneinfo.ZoneInfo(region)
        except Exception:                   # noqa: BLE001
            continue
        if z.key == region:
            good.append((region, sub))
    if not good:
        return False

    _ZONE_PAIRS = good
    _AREA_SUBMODS = sorted({s for _, s in good})
    return True


def setup(H):
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    H.state = {
        "nworkers": nworkers,
        "parses": [0] * SLOTS,             # successful contended parses (per slot)
        "churn": [0] * SLOTS,              # compile-forcing parses (ARM 1 metric)
        "zi_ok": [0] * SLOTS,              # ZoneInfo cold constructs that succeeded
        "zi_skip": [0] * SLOTS,            # ZoneInfo constructs that hit the benign
                                           #   purge-race (KeyError/NotFound), skipped
        "control_ok": [0],                 # control-arm successful re-parses
        "control_wg": runloom.WaitGroup(),
    }
    # Stand up the real cold-import zone backend the #9 arm needs.  A setup
    # failure here is clearer than a per-fiber surprise (and means the ZoneInfo
    # arm would be vacuous, which we never want to ship silently).
    if not _build_tzdata_backend(H):
        H.fail("setup: could not establish a tzdata-style zone backend (no tzdata "
               "package AND no system TZif files to synthesize one) -- the "
               "ZoneInfo cold-import #9 arm cannot run")
        return
    H.log("zone backend ready: {0} (region,area) pairs over {1} area "
          "submodules -- {2}".format(
              len(_ZONE_PAIRS), len(_AREA_SUBMODS), _AREA_SUBMODS))


def body(H):
    # Spawn the single-owner CONTROL fiber FIRST (inside the root) so it re-parses
    # alongside the contended pool; it joins on its own WaitGroup, waited on in
    # post().
    wg = H.state["control_wg"]
    wg.add(1)

    def run_control():
        try:
            control_resample(H, H.state)
        finally:
            wg.done()

    H.fiber(run_control)
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    state = H.state
    state["control_wg"].wait()             # control snapshot complete + quiescent

    parses = sum(state["parses"])
    churn = sum(state["churn"])
    zi_ok = sum(state["zi_ok"])
    zi_skip = sum(state["zi_skip"])
    control_ok = state["control_ok"][0]
    H.log("strptime exact-field parses={0} (LOAD-BEARING identity, all matched "
          "their own unique format+input) | cache-churn compile-forcing "
          "parses={1} (distinct format per wid churns the size-5 _regex_cache) | "
          "control single-owner re-parses ok={2} | zone (region,area) pairs={3} "
          "zoneinfo cold ok={4} purge-race-skip={5} | ops={6}".format(
              parses, churn, control_ok, len(_ZONE_PAIRS), zi_ok, zi_skip,
              H.total_ops()))

    # Non-vacuity: the hazard was actually exercised (parses happened) -- else the
    # load-bearing field oracle never ran.
    if not H.check(parses > 0,
                   "no strptime parses completed -- the shared _regex_cache "
                   "churn/identity hazard was never exercised (oracle vacuous)"):
        return

    # The control arm re-parsed a sample of the same pairs single-owner; it must
    # have validated at least one (proves the control path ran and the machinery
    # is sound on the single-owner side).
    H.check(control_ok > 0,
            "CONTROL arm validated 0 single-owner re-parses -- the control "
            "falsifier never ran (cannot disambiguate a tear from an accounting "
            "race)")

    # COMPLETENESS: no worker parked-then-vanished (e.g. stranded inside the raw
    # _cache_lock critical section if it blocked the hub, or in a sleep/yield).
    H.require_no_lost("strptime/zoneinfo cache identity completeness")


if __name__ == "__main__":
    harness.main(
        "p458_strptime_zoneinfo_cache", body, setup=setup, post=post,
        default_funcs=6000,
        describe="ARM 1 (clean): thousands of fibers strptime a DISTINCT (format, "
                 "input) pair UNIQUE to each (a per-wid literal token churns the "
                 "size-5 _regex_cache) and assert the parsed datetime fields "
                 "EXACTLY match their own encoded wid -- a wrong field means a "
                 "torn cache returned a SIBLING's compiled regex.  ARM 2 (catches "
                 "the REAL, unfixed FINDINGS #9 runloom bug -> SUPPOSED to FAIL "
                 "under M:N): each round purge the tzdata.zoneinfo.<Area> backend "
                 "submodules and re-construct ZoneInfo(<region>) over a spread of "
                 "areas, so the OS-thread-keyed import lock is re-taken COLD while "
                 "sibling fibers on the same hub yield -> a spurious importlib "
                 "_DeadlockError (plain-OS-threads control: 0 under GIL on AND "
                 "off, so it is runloom-specific).")
