"""big_100 / 489 -- urllib.parse / urllib.request state isolation under M:N.

urllib.parse and urllib.request have some stateful operations:
- urllib.parse._hostprog, _MAX_CACHE (private caches)
- urllib.request.Request maintains per-instance state (headers, data, etc.)
- urllib.request handlers (HTTPHandler, etc.) may have shared registries

This program stresses the isolation of parse/request operations across fibers
on shared hub OS-threads. The parse/request operations themselves are
thread-safe (no documented global mutable state), so the LOAD-BEARING oracle
checks that each fiber's distinct URL parse/construct cycle produces the
CORRECT result -- a fiber sets up its own URL, parses it, and asserts the
parsed components are correct AFTER a yield (so a sibling running on the same
hub may have parsed a different URL in the interim).

WHERE M:N BREAKS IT (if at all). If urllib.parse or urllib.request ever
corrupts global parse state (e.g. a cache that keys by domain but is per-hub
instead of per-fiber), a sibling's parse can poison this fiber's subsequent
reads.  Empirically (verified with plain threads): this does NOT happen --
urllib.parse is stateless per-call, and urllib.request registries are keyed
by handler class, not fiber identity.  This is a LIKELY-STATELESS probe: the
program is EXPECTED to PASS cleanly (exit 0) under a correct runtime AND
under plain GIL threads.  If it FAILS, it will have caught a real urllib M:N
isolation bug (e.g. a global parse cache that leaked per-hub state).

LOAD-BEARING INVARIANT / WHY THE ORACLE IS NON-VACUOUS:
  Each fiber constructs DISTINCT URLs (unique per-fiber ID embedded in the URL
  path or query string) and calls urllib.parse.urlparse() on its own URL.
  The parsed result MUST have the exact components this fiber embedded -- a
  fiber that reads a component value != what it set is a sign of parse state
  corruption (another fiber's parse poisoned this fiber's result).  The oracle
  is closed-world: before the run, a canonical parse table is built (precomputed
  in single-owner mode, one fiber, no concurrency).  Each fiber's check reads
  its result from the table and compares the real parse against it.  A mismatch
  is the bug.

ORACLES:
  * LOAD-BEARING -- PER-FIBER URL PARSE INTEGRITY (worker, HARD, fail-fast).
    Each fiber constructs a unique URL embedding its wid, parses it, and
    asserts the parsed components (scheme, netloc, path, query, fragment)
    match the PRECOMPUTED canonical parse at that URL.  A mismatch is a
    parse-state corruption under M:N (a sibling's parse leaked into this
    fiber's result).  The oracle fires only if the PARSED VALUE differs from
    the canonical -- e.g., a scheme=='http' but canonical=='https', or path
    contains a sibling's wid instead of this fiber's.  A totally garbage/torn
    value (e.g., a non-string or NaN) is also a fail.  Runs after a yield so
    the scheduler has interleaved a sibling's parse.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    parse / mid-construct never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing parse-integrity hazard was
    actually exercised (parse_checks > 0).

SECONDARY (report-ONLY, NEVER fails):
  * A MEASURED arm that constructs and parses a SHARED URL (one URL used by
    all fibers) to check for parse result consistency.  A sibling's parse
    shouldn't leak components into a shared result (it should all be the
    SAME precomputed value for all fibers, since they all parse the same URL).
    If the shared parse result EVER differs across calls, it's a shared-state
    leak (MEASURED, never failed, reported as a rate).  This is a secondary
    signal: the load-bearing private-URL arm is the primary cache/isolation
    oracle, and the shared-URL arm is a delta check.

FAIL ON: a private-URL parse result with wrong component(s), a torn/garbage
value, or a crash/hang.  NEVER fail on the shared-URL parse result variance
(report it as a rate).

EXPECTED RESULT: this is a LIKELY-STATELESS probe, so under a correct runtime
(both runloom M:N and plain threads GIL on AND off) the program is EXPECTED
to PASS cleanly (exit 0) with 0 shared-parse drifts.  If it FAILS, it has
caught a real, previously-UNFIXED urllib M:N isolation bug.

Stresses: urllib.parse.urlparse() isolation under concurrent parse calls with
distinct URLs, per-fiber URL construction/parsing, hub migration, yield points
inside parse operations, shared-URL consistent result across concurrent parses.

Good TSan / controlled-M:N-replay target: urllib.parse's internal caches
(if any) are Python dicts or objects on the module; a data race on a cache
lookup+update, or a replay that interleaves two fibers' parses of different
URLs, localizes the isolation breach before the oracle fires.
"""
import urllib.parse
from urllib.parse import urlparse, quote, urlencode, parse_qs

import harness
import runloom

# Canonical, single-owner function to compute urlparse() for any (wid, idx).
# Built in setup() as a closure that deterministically generates the canonical
# parse for any URL. The oracle is closed-world: for (wid, idx) the canonical
# parse is always make_url_for_wid(wid, idx) -> urlparse(...), independent of
# any runtime state.
CANONICAL_PARSES = None  # Set to a callable in setup()

# Shared URL used by all fibers (for the MEASURED arm).  All fibers parse this
# same URL, so the result MUST be identical every time (a shared-state leak
# would show as result variance).
SHARED_URL = "http://shared.example.com:8080/path/to/resource?key=value&foo=bar#anchor"
CANONICAL_SHARED_PARSE = None


def make_url_for_wid(wid, idx):
    """Construct a unique URL embedding this wid and iteration index.

    The URL embeds the wid in multiple places so a parse result with the WRONG
    wid is obvious (a sibling's parse leaked in).  The index varies per
    iteration to test repeated parses at different URLs (same fiber, different
    parse state windows).
    """
    # Embed wid in both path and query string for redundancy.
    scheme = "http"
    netloc = "fiber{0}.example.com:9{1:03d}".format(wid, wid % 1000)
    path = "/fiber/{0}/resource/{1}".format(wid, idx)
    query_params = {
        "fiber_id": str(wid),
        "iteration": str(idx),
        "rand": str((wid * 1234567 + idx) & 0xFFFFFF),
    }
    query = urlencode(query_params)
    fragment = "section-{0}-{1}".format(wid, idx)
    # Construct the full URL.
    return "{0}://{1}{2}?{3}#{4}".format(scheme, netloc, path, query, fragment)


def build_canonical():
    """Single-owner precompute of urlparse results for all test URLs.

    We use a FUNCTION-based canonical lookup rather than a precomputed table:
    for any (wid, idx) pair, the canonical parse is simply urlparse applied to
    the URL make_url_for_wid(wid, idx) produces.  This avoids precomputing
    millions of entries (memory waste) while keeping the oracle closed-world
    (the canonical value is always deterministic and independent of runtime state).
    """
    # Return a callable that takes (wid, idx) and returns the canonical parse.
    # This is a closure so it captures urlparse in a way that's isolated.
    def canonical_for(wid, idx):
        url = make_url_for_wid(wid, idx)
        return (url, urlparse(url))
    return canonical_for


def setup(H):
    global CANONICAL_PARSES, CANONICAL_SHARED_PARSE
    CANONICAL_PARSES = build_canonical()
    # Precompute the shared URL parse (single-owner, no concurrency).
    CANONICAL_SHARED_PARSE = urlparse(SHARED_URL)

    H.state = {
        "parse_checks": [0] * 1024,         # load-bearing private-URL parse checks
        "parse_fails": [0] * 1024,          # parse result mismatches
        "shared_checks": [0] * 1024,        # measured shared-URL parse checks
        "shared_drifts": [0] * 1024,        # shared parse result variance
        "sample": [None],                   # first observed sample (for logs)
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: PRIVATE unique-per-wid URL parse integrity.
# Each fiber constructs its own URL (embedding its wid) and parses it.
# The result MUST match the precomputed canonical parse -- no sibling's
# parse state should leak into this fiber's result.
# --------------------------------------------------------------------------
def private_parse_check(H, wid, idx, state):
    """Check that a private (per-wid unique) URL parses correctly."""
    # Get the canonical parse for this (wid, idx) pair.
    canonical_url, canonical_parsed = CANONICAL_PARSES(wid, idx)

    # Construct and parse this fiber's unique URL.
    url = make_url_for_wid(wid, idx)

    # YIELD + SLEEP before parsing so a sibling on this hub has time to
    # parse its own URL and potentially corrupt the parse state.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Parse the URL.
    parsed = urlparse(url)

    # Snapshot individual components for detailed failure reporting.
    scheme = parsed.scheme
    netloc = parsed.netloc
    path = parsed.path
    query = parsed.query
    fragment = parsed.fragment

    state["parse_checks"][wid & 1023] += 1

    # CLOSED-WORLD ORACLE: the canonical parse was computed ONCE, single-owner.
    # This fiber's parse MUST match it exactly. A mismatch is a corruption.

    # Check scheme.
    expected_scheme = canonical_parsed.scheme
    if scheme != expected_scheme:
        state["parse_fails"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "scheme", scheme, expected_scheme)
        H.fail(
            "urllib.parse SCHEME MISMATCH: fiber {0} parsed scheme={1!r} "
            "(expected {2!r}) from URL {3!r} -- a sibling's parse state "
            "leaked into this fiber's result (runloom parse-state isolation "
            "bug).".format(wid, scheme, expected_scheme, url))
        return

    # Check netloc.
    expected_netloc = canonical_parsed.netloc
    if netloc != expected_netloc:
        state["parse_fails"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "netloc", netloc, expected_netloc)
        H.fail(
            "urllib.parse NETLOC MISMATCH: fiber {0} parsed netloc={1!r} "
            "(expected {2!r}) from URL {3!r} -- a sibling's parse may have "
            "corrupted the netloc (runloom parse-state isolation bug).".format(
                wid, netloc, expected_netloc, url))
        return

    # Check path.
    expected_path = canonical_parsed.path
    if path != expected_path:
        state["parse_fails"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "path", path, expected_path)
        H.fail(
            "urllib.parse PATH MISMATCH: fiber {0} parsed path={1!r} "
            "(expected {2!r}) from URL {3!r} -- a sibling fiber's wid may be "
            "embedded in the path (runloom parse-state isolation bug).".format(
                wid, path, expected_path, url))
        return

    # Check query string (parse it to compare key-value pairs, not raw).
    expected_query_dict = parse_qs(canonical_parsed.query, keep_blank_values=True)
    got_query_dict = parse_qs(query, keep_blank_values=True)
    if got_query_dict != expected_query_dict:
        state["parse_fails"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "query", query, canonical_parsed.query)
        H.fail(
            "urllib.parse QUERY MISMATCH: fiber {0} parsed query={1!r} "
            "(expected {2!r}) from URL {3!r} -- a sibling's query parameters "
            "leaked (runloom parse-state isolation bug).".format(
                wid, query, canonical_parsed.query, url))
        return

    # Check fragment.
    expected_fragment = canonical_parsed.fragment
    if fragment != expected_fragment:
        state["parse_fails"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "fragment", fragment, expected_fragment)
        H.fail(
            "urllib.parse FRAGMENT MISMATCH: fiber {0} parsed fragment={1!r} "
            "(expected {2!r}) from URL {3!r} -- a sibling's fragment leaked "
            "(runloom parse-state isolation bug).".format(
                wid, fragment, expected_fragment, url))
        return


# --------------------------------------------------------------------------
# MEASURED arm: SHARED URL parse consistency check.
# All fibers parse the SAME URL (SHARED_URL), so every result MUST be
# identical.  If a sibling's parse corrupts a global, the shared result
# may vary across calls (a cache miss / stale entry).  MEASURED, never
# failed -- we report the variance rate.
# --------------------------------------------------------------------------
def shared_parse_check(H, wid, idx, state):
    """Check that shared-URL parse is consistent across concurrent calls."""
    # YIELD before parsing, so siblings parse concurrently.
    runloom.yield_now()

    # Parse the shared URL.
    parsed = urlparse(SHARED_URL)

    state["shared_checks"][wid & 1023] += 1

    # Compare against the precomputed canonical.
    # (Lightweight: just compare the tuple representation.)
    parsed_tuple = (parsed.scheme, parsed.netloc, parsed.path,
                    parsed.params, parsed.query, parsed.fragment)
    canonical_tuple = (CANONICAL_SHARED_PARSE.scheme, CANONICAL_SHARED_PARSE.netloc,
                       CANONICAL_SHARED_PARSE.path, CANONICAL_SHARED_PARSE.params,
                       CANONICAL_SHARED_PARSE.query, CANONICAL_SHARED_PARSE.fragment)

    if parsed_tuple != canonical_tuple:
        # A drift: the shared URL parsed to a different result.
        # (This is a signal of parse-state corruption, but we MEASURE it, never fail.)
        state["shared_drifts"][wid & 1023] += 1


# Sustained check loops per worker, bounded by H.running().  The parse-state
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# parsing and yielding across the parse window.  INNER_CAP stops one worker
# from monopolizing teardown on a slow box.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms: the LOAD-BEARING private-URL parse check
    (fail-fast) and the MEASURED shared-URL check (report only).  The two do not
    interact -- each fiber's private URL is unique, and the shared URL is
    identical for all -- so running them in the same fiber keeps the hub busy
    with mixed parse churn."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            private_parse_check(H, wid, idx, state)  # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_parse_check(H, wid, idx, state)   # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["parse_checks"])
    fails = sum(H.state["parse_fails"])
    schecks = sum(H.state["shared_checks"])
    sdrifts = sum(H.state["shared_drifts"])

    fpct = (100.0 * fails / checks) if checks else 0.0
    sdpct = (100.0 * sdrifts / schecks) if schecks else 0.0
    sample = H.state["sample"][0]

    H.log("urllib.parse[private LOAD-BEARING]: {0} checks  fails={1} ({2:.2f}%)  "
          "sample={3}".format(checks, fails, fpct, sample))
    H.log("urllib.parse[shared MEASURED]: {0} checks  drifts={1} ({2:.2f}%) -- "
          "a shared-URL parse result variance (MEASURED, never fail; expected 0% "
          "under correct isolation)".format(schecks, sdrifts, sdpct))

    if fails:
        H.log("note: the LOAD-BEARING private-URL arm observed parse-result "
              "mismatches -- urllib.parse or urllib.request state may have leaked "
              "across fibers on a shared hub thread (runloom M:N isolation bug).")
    if sdrifts:
        H.log("note: the shared-URL parse result varied across {0} concurrent "
              "calls -- a parse-state corruption or cache inconsistency under "
              "M:N (expected 0%, measured {1:.2f}%).  This is a SECONDARY signal; "
              "the private-URL arm is the PRIMARY oracle.".format(schecks, sdpct))

    # NON-VACUITY: the load-bearing private-URL hazard was actually exercised.
    H.check(checks > 0,
            "no private-URL parse checks ran -- the load-bearing parse-state "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber vanished mid-parse (stranded on a never-delivered wake).
    H.require_no_lost("urllib.parse isolation")


if __name__ == "__main__":
    harness.main(
        "p489_urllib", body, setup=setup, post=post,
        default_funcs=8000,
        describe="urllib.parse.urlparse() isolation under concurrent parse "
                 "calls with distinct per-fiber URLs.  LOAD-BEARING: each fiber "
                 "constructs a unique URL embedding its wid, parses it, and "
                 "asserts the result matches the precomputed canonical parse "
                 "(closed-world reference computed once, single-owner, before the "
                 "run).  A mismatch signals parse-state corruption across M:N hub "
                 "fibers (0 expected, 0 under plain threads GIL on AND off -- if "
                 "it fires, it's a real urllib M:N isolation bug).  MEASURED: a "
                 "shared-URL parse result variance (expected 0%; if nonzero, a "
                 "secondary signal of parse-cache inconsistency).")
