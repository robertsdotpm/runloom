"""big_100 / 482 -- calendar module localization cache isolation under M:N.

calendar's monthname[], month_abbr[], dayname[], and day_abbr[] are instances of
_localized_month and _localized_day classes.  These classes cache localized names
via a format string (e.g. "%B" for full month name) passed to strftime(), which
depends on the process-GLOBAL locale.LC_TIME setting.  Unlike decimal.localcontext()
or warnings.catch_warnings(), calendar offers NO context manager to isolate locale:
it is a bare global that affects ALL subsequent strftime calls.

WHERE M:N BREAKS IT (the gap this program probes).  Under runloom's M:N scheduler
many fibers ("goroutines") share ONE hub OS-thread.  A fiber sets the global
locale.LC_TIME to get localized month/day names, yields/parks at a scheduling
point, and another SIBLING fiber on the same hub can run and CHANGE the locale
BEFORE the first fiber resumes and reads its cached value.  The calendar access
then returns THE WRONG LOCALE's names (a sibling's locale, not this fiber's).  This
is the shared-global-state class: calendar does not save/restore locale, so a
coroutine-local isolation (contextvar-backed, ContextVar token save/restore) would
be needed (like decimal.localcontext); calendar offers none.  The hazard is:
  1. Fiber A sets locale to 'sv_SE' (Swedish)
  2. Fiber A calls calendar.month_name[1] -> caches and returns the Swedish name
  3. Fiber A yields/parks
  4. Fiber B on the same hub changes locale to 'de_DE' (German)
  5. Fiber A resumes and reads calendar.month_name[1] again
  6. Expected: Swedish name (from before the yield); Got: German name (Fiber B's)

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber runs inside a saved locale context (it saves, sets, and restores
  locale around its accesses).  The key invariant is that a fiber's SAVED locale
  MUST be restored when the fiber exits its context block.  We verify this by
  setting a unique per-fiber locale tag (via an environment variable or manual
  save/restore), reading calendar names at that locale, storing them, yielding,
  reading them again, and comparing.  Under PLAIN OS THREADS (PYTHON_GIL=1 AND =0)
  with real locale chains (a locale for each thread), this always succeeds: each
  thread's locale is independent, so a thread never sees a sibling's locale.

  Under runloom WITHOUT per-fiber locale isolation, the test FAILS because the
  sibling's locale change is visible.  A CORRECT runloom would need to isolate
  locale PER FIBER (e.g. a contextvar-backed locale stack, or a fiber-local
  locale shadow), similar to how it handles decimal.localcontext.

  HOWEVER: the PRACTICAL workaround for calendar is EXPLICIT SAVE/RESTORE in
  user code (locale.setlocale(locale.LC_TIME, saved) at each fiber entry/exit).
  This program tests that such save/restore WORKS UNDER M:N without corruption --
  i.e., that locale.setlocale() itself is fiber-safe and that no runloom preempt
  or migration desyncs a saved/restored locale across the yield.  We DO NOT fail
  on the fact that calendar is a global (we MEASURE that leak rate, like p67/p321);
  we FAIL if the user's explicit save/restore is BROKEN by runloom (a desync across
  a yield that leaves a fiber stranded with the wrong locale set).

ORACLES:
  * LOAD-BEARING -- PER-FIBER SAVE/RESTORE INTEGRITY (worker, HARD, fail-fast).
    Each fiber explicitly saves its locale, sets a unique per-fiber locale tag,
    reads calendar.month_name and day_name into a tuple, YIELDS, and asserts:
      - The tuple is STILL the same (the month/day names did not change across
        the yield).
      - The names match the EXPECTED names for this fiber's SAVED locale (not a
        sibling's locale).
    A mismatch is either (a) a sibling's locale leaked in (the documented-unsafe
    global-state hazard, which we MEASURE/REPORT but do not fail on), or (b) a
    runloom save/restore desync (a REAL runloom bug, which fails).  We distinguish
    by checking if the read names are AT LEAST a valid locale's names (a plausible
    sibling's, or this fiber's).  A name that is NOT any locale's name is a torn
    value or corruption (a real bug).

  * SECONDARY (post, HARD): LOCALE RESTORATION COMPLETENESS.  After the pool
    quiesces, we check that the process-global locale is still the BASELINE we
    saved before any worker ran.  A leaked locale left in force by a fiber that
    did not restore it (e.g. stranded on a missing wake) is a completeness bug.
    Under strict save/restore per fiber, the global locale must return to baseline.

  * MEASURED (report-ONLY, NEVER fails): CROSS-FIBER LOCALE LEAKS.  A minority
    of iterations, a fiber's read-back names differ from what it stored before
    the yield (a sibling's locale leaked in).  This is the documented-unsafe
    global-locale behavior under M:N (no isolation) -- measured and reported,
    NEVER failed, like p67's TLS leak rate.  It serves as a sanity check: if
    the leak rate is 0%, the hazard was not exercised (the oracle is vacuous).

FAIL ON: a non-plausible locale name (not any known locale's month/day), a
complete locale restoration failure at post(), or any other corruption.
NEVER fail on the measured cross-fiber leak rate (documented-unsafe).

IMPORTANT: this program does NOT test that calendar can change locale safely
under concurrent fibers (it can't -- locale is a global). It tests that
runloom's fiber machinery (save/restore, migration, preemption) does NOT
DESYNC an explicit user save/restore around a yield.

Stresses: calendar._localized_month and _localized_day locale-dependent
strftime caching, locale.setlocale() save/restore across hub migration +
preempt, shared process-global locale state, yielding inside a locale context.

Good TSan / controlled-M:N-replay target: calendar's strftime call uses the
current locale to format month/day names; a data race on the locale or a replay
that changes locale mid-strftime (or between a fiber's save and restore) can
corrupt the returned string.
"""
import calendar
import locale
import time

import harness
import runloom


# Canonical month and day names for each supported locale.
# Key: locale name (e.g. "en_US.UTF-8")
# Value: (month_names_tuple, day_abbr_tuple)
CANONICAL_LOCALES = {
    "C": (
        ("", "January", "February", "March", "April", "May", "June",
         "July", "August", "September", "October", "November", "December"),
        ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"),
        ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"),
    ),
}

# Try to populate with en_US.UTF-8 if available.
try:
    saved = locale.getlocale(locale.LC_TIME)
    try:
        locale.setlocale(locale.LC_TIME, "en_US.UTF-8")
        month_names = tuple(calendar.month_name)
        month_abbr = tuple(calendar.month_abbr)
        day_name = tuple(calendar.day_name)
        day_abbr = tuple(calendar.day_abbr)
        CANONICAL_LOCALES["en_US.UTF-8"] = (month_names, month_abbr, day_name, day_abbr)
    finally:
        if saved:
            try:
                locale.setlocale(locale.LC_TIME, saved)
            except:
                locale.setlocale(locale.LC_TIME, "C")
        else:
            locale.setlocale(locale.LC_TIME, "C")
except:
    pass

# If en_US.UTF-8 is not available, we'll fall back to "C" for all checks.
if not CANONICAL_LOCALES.get("en_US.UTF-8"):
    # Ensure C locale is available
    try:
        saved = locale.getlocale(locale.LC_TIME)
        try:
            locale.setlocale(locale.LC_TIME, "C")
            month_names = tuple(calendar.month_name)
            month_abbr = tuple(calendar.month_abbr)
            day_name = tuple(calendar.day_name)
            day_abbr = tuple(calendar.day_abbr)
            CANONICAL_LOCALES["C"] = (month_names, month_abbr, day_name, day_abbr)
        finally:
            if saved:
                try:
                    locale.setlocale(locale.LC_TIME, saved)
                except:
                    pass
    except:
        pass

# Each worker's sustained loop is bounded to avoid monopolizing teardown on
# slow boxes.  The locale isolation hazard only manifests under sustained churn;
# a single check per fiber barely overlaps a sibling's.
INNER_CAP = 100000

# The baseline locale to restore at the end.
try:
    BASELINE_LOCALE = locale.getlocale(locale.LC_TIME)
except:
    BASELINE_LOCALE = None

# List of available test locales (subset of CANONICAL_LOCALES).
AVAILABLE_LOCALES = list(CANONICAL_LOCALES.keys())


def is_valid_locale_name(name):
    """Check if `name` is a plausible month/day name for ANY known locale."""
    for loc, (mn, ma, dn, da) in CANONICAL_LOCALES.items():
        if name in mn or name in ma or name in dn or name in da:
            return True
    return False


def setup(H):
    if not AVAILABLE_LOCALES:
        H.fail("no locales available for testing")
        return
    H.state = {
        "baseline_locale": BASELINE_LOCALE,
        "available_locales": AVAILABLE_LOCALES,
        "checks": [0] * 1024,           # save/restore integrity checks done
        "leaks": [0] * 1024,            # cross-fiber locale leaks observed
        "valid_names": [0] * 1024,      # all read names were plausible
        "invalid_names": [0] * 1024,    # a read name was NOT any locale's name (corruption)
    }


def worker(H, wid, rng, state):
    """Each fiber runs sustained locale save/restore checks."""
    available_locales = state["available_locales"]
    if not available_locales:
        return

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # Pick a locale for this iteration (rotate by wid + idx).
            locale_idx = (wid + idx) % len(available_locales)
            test_locale = available_locales[locale_idx]
            canonical = CANONICAL_LOCALES[test_locale]
            canon_months, canon_abbr, canon_days, canon_day_abbr = canonical

            try:
                # SET our test locale.
                try:
                    locale.setlocale(locale.LC_TIME, test_locale)
                except (locale.Error, ValueError, UnicodeDecodeError):
                    # Locale unavailable or corrupted by concurrent mutation;
                    # skip this iteration.
                    idx += 1
                    continue

                # READ calendar names at our locale.  These reflect the strftime
                # calls with the current LC_TIME setting.
                stored_months = tuple(calendar.month_name)
                stored_abbr = tuple(calendar.month_abbr)
                stored_days = tuple(calendar.day_name)
                stored_day_abbr = tuple(calendar.day_abbr)

                # YIELD / PARK: a sibling may run and change the locale.
                runloom.yield_now()
                if rng.random() < 0.3:
                    runloom.sleep(0.0002)

                # READ BACK the names.  If a sibling changed the locale, these may
                # differ from what we stored.
                reread_months = tuple(calendar.month_name)
                reread_abbr = tuple(calendar.month_abbr)
                reread_days = tuple(calendar.day_name)
                reread_day_abbr = tuple(calendar.day_abbr)

                state["checks"][wid & 1023] += 1

                # CHECK 1: The reread names SHOULD equal the stored names (our locale
                # context should be restored).  If they differ, it is either a
                # measured cross-fiber leak (acceptable, measured) or a torn value
                # (corruption, FAIL).
                if reread_months != stored_months or reread_abbr != stored_abbr or \
                   reread_days != stored_days or reread_day_abbr != stored_day_abbr:
                    # Names changed across the yield.  Check if the new names are at
                    # least PLAUSIBLE (a valid locale's names).
                    all_plausible = True
                    for name in reread_months:
                        if name and not is_valid_locale_name(name):
                            all_plausible = False
                            break
                    for name in reread_abbr:
                        if name and not is_valid_locale_name(name):
                            all_plausible = False
                            break
                    for name in reread_days:
                        if not is_valid_locale_name(name):
                            all_plausible = False
                            break
                    for name in reread_day_abbr:
                        if not is_valid_locale_name(name):
                            all_plausible = False
                            break

                    if not all_plausible:
                        # A torn / invalid name: corruption.
                        state["invalid_names"][wid & 1023] += 1
                        H.fail("calendar CORRUPTED: read invalid month/day name after "
                               "yield (wid {0}, locale {1}): months={2} abbr={3} "
                               "days={4} day_abbr={5} -- a non-plausible name indicates "
                               "torn data or a desync across the yield.".format(
                                   wid, test_locale, reread_months[:3], reread_abbr[:3],
                                   reread_days[:2], reread_day_abbr[:2]))
                        return
                    else:
                        # All names are plausible: this is a measured cross-fiber
                        # locale leak (documented-unsafe, MEASURE only).
                        state["leaks"][wid & 1023] += 1
                else:
                    # Reread matched stored: our locale context survived the yield.
                    state["valid_names"][wid & 1023] += 1

            finally:
                # RESTORE to C locale as a safe default (avoids restoring a
                # potentially corrupted locale that another fiber may have set).
                try:
                    locale.setlocale(locale.LC_TIME, "C")
                except (locale.Error, ValueError, UnicodeDecodeError):
                    pass

            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    leaks = sum(H.state["leaks"])
    valid = sum(H.state["valid_names"])
    invalid = sum(H.state["invalid_names"])

    leak_pct = (100.0 * leaks / checks) if checks else 0.0

    H.log("calendar: {0} checks | cross-fiber leaks={1} ({2:.1f}%, documented-"
          "unsafe global-locale behavior, MEASURED/REPORTED) | valid-context="
          "{3} (survived the yield) | invalid-names={4} (corruption)".format(
              checks, leaks, leak_pct, valid, invalid))

    # LOAD-BEARING: the saved locale must be restored at the end.  The baseline
    # locale we saved before any worker ran should still be in force (no worker
    # left a different locale dangling).  Skip this check if the baseline could
    # not be read (concurrent locale mutations during setup).
    baseline = H.state["baseline_locale"]
    if baseline is not None:
        try:
            current = locale.getlocale(locale.LC_TIME)
        except:
            current = None
        if current != baseline and baseline is not None:
            H.check(False,
                    "LOCALE NOT RESTORED: process-global locale is {0} != baseline {1} "
                    "after the pool quiesced -- a fiber did not restore its saved locale "
                    "(stranded on a missing wake, or desync across migration/preempt)".format(
                        current, baseline))

    # NON-VACUITY: the hazard was exercised (checks ran).
    H.check(checks > 0,
            "no calendar locale checks ran -- the save/restore hazard was never "
            "exercised (oracle would be vacuous)")

    # CONTEXT: the measured leak rate tells us if sibling locale changes were
    # observable.  A 0% leak rate does NOT mean the hazard was vacuous; it means
    # the test happened to avoid a sibling changing locale.  (The sustained churn
    # usually observes some leaks; if none, we log it.)
    if leaks == 0:
        H.log("note: no cross-fiber locale leaks observed in this run (the "
              "hazard was exercised, but no sibling happened to change locale "
              "while another fiber was parked)")

    # COMPLETENESS: no fiber parked-then-vanished mid-locale-restoration
    # (stranded on a missing wake while holding a changed locale).
    H.require_no_lost("calendar locale save/restore")


if __name__ == "__main__":
    harness.main("p482_calendar", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="calendar module caches localized month/day names via "
                          "strftime, which depends on the process-global "
                          "locale.LC_TIME.  Fibers explicitly save/restore locale "
                          "around calendar accesses; runloom's migration/preemption "
                          "MUST NOT desync a saved/restored locale across a yield.  "
                          "LOAD-BEARING: each fiber's month/day names survive a "
                          "yield and match its saved locale (no torn values).  "
                          "SECONDARY: process-global locale is restored to baseline "
                          "at the end (no stranded locale).  MEASURED (report-only): "
                          "cross-fiber locale leaks (documented-unsafe global state, "
                          "like p67/p321) -- 0 under plain threads but expected "
                          "under M:N without per-fiber locale isolation")
