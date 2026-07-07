"""big_100 / 581 -- nturl2path.pathname2url / url2pathname PURITY under M:N.

nturl2path exposes two PURE string functions used by urllib.request to convert
between NT (Windows) filesystem paths and 'file:' URLs:

  * pathname2url(p)   -- 'C:\\foo\\bar'      -> '///C:/foo/bar'
  * url2pathname(url) -- '///C:/foo/bar'     -> 'C:\\foo\\bar'

Both are referentially transparent: they take a string, do a fixed sequence of
slice/replace/quote/unquote steps (delegating the percent-encoding to
urllib.parse.quote/unquote and the root split to ntpath.splitroot), and return a
new string.  There is no module-level mutable state: the only globals they touch
are the cached `import urllib.parse` / `import ntpath` module objects in
sys.modules and the deprecation `warnings` machinery -- all read-only after first
call.  So for a FIXED input the output is a pure function of that input: the same
bytes, every time, on every hub, forever.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes tens
of thousands of goroutines over a handful of OS threads with the GIL OFF.  A pure
string function is the cleanest possible isolation oracle: its ONLY inputs are the
argument string (a single-owner fiber-local object) and read-only module state.
If the runtime ever (a) lets a sibling's computation clobber this fiber's
in-flight result string (a torn/leaked intermediate), (b) corrupts the shared
read-only urllib.parse / ntpath module objects mid-call, (c) resumes a yielded
fiber with a different frame's locals (a stack/frame desync across hub
migration), or (d) SIGSEGVs inside the C-level quote/unquote/splitroot loops
under concurrent access, then a fiber will observe an output that either differs
from the byte-identical serial reference or changes across a yield.  On a CORRECT
runtime the output is deterministic and equals the serially-precomputed value --
so the oracle PASSES (exit 0) when there is no bug.

WHY THE ORACLE IS LOAD-BEARING (verified serially + against plain threads):

  The GOLD reference is computed ONCE in setup(), single-threaded and quiescent,
  before any fiber runs: expected_p2u[i] = pathname2url(PATHS[i]) and
  expected_u2p[i] = url2pathname(URLS[i]).  These reference strings are the
  closed-form truth for each fixed input.  Each fiber then, on read-only shared
  corpus entries, recomputes the function and asserts the result is BIT-IDENTICAL
  to the gold reference AND stable across a yield.  Because the input is fixed and
  the function is pure, a correct runtime MUST reproduce the reference exactly;
  the corpus + reference tables are read-only (never mutated), and every result
  string is a single-owner fiber-local.  A standalone plain-threads control (8 OS
  threads hammering the same corpus, GIL on and off) reproduces the reference 100%
  of the time -- so under a correct runloom it must too.

ORACLES:
  * LOAD-BEARING A -- REFERENCE MATCH (worker, HARD, fail-fast).  Fiber picks a
    fixed corpus entry, computes pathname2url / url2pathname, YIELDS, recomputes,
    and asserts BOTH results are bit-identical to each other (determinism across a
    yield) AND equal to the serially-precomputed gold reference (no cross-fiber
    corruption of the pure output).  Single-owner: result strings are locals.

  * LOAD-BEARING B -- ROUND-TRIP IDENTITY (worker, HARD, fail-fast).  For the
    round-trip-clean corpus (well-formed DOS/UNC paths), the fiber asserts
    url2pathname(pathname2url(p)) == p across a yield -- the inverse-function law.
    Both directions run, and the recovered path must equal the original bytes.

  * LOAD-BEARING C -- FIBER-LOCAL DETERMINISM (worker, HARD, fail-fast).  Each
    fiber builds its OWN unique input path (wid + idx embedded, plus quote-forcing
    chars: spaces, '#', '%', '+', '='), computes pathname2url twice across a yield,
    and asserts the two results are bit-identical.  The input is a private local
    the fiber constructs, so a value change across the yield is a runtime desync,
    not a shared-object race.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    quote/unquote/splitroot C loop (SIGSEGV or hang mid-call) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0).

FAIL ON: a pathname2url / url2pathname result that differs from the byte-identical
serial reference, changes across a yield, breaks the round-trip identity, or a
SIGSEGV inside the conversion.  There is NO shared-mutable arm here: the functions
are pure and every arm is single-owner, so every failure is a genuine runtime bug
(torn result string, frame/locals desync across hub migration, read-only-module
corruption, or a crash in the C string machinery).

Stresses: pure-function determinism across hub migration + yield, urllib.parse
quote/unquote and ntpath.splitroot C paths under M:N, single-owner result-string
isolation, inverse-function round-trip law, no torn/leaked intermediate strings.
"""
import warnings

import harness
import runloom

# Silence the 3.19-removal DeprecationWarning at import; the module is stdlib and
# the deprecation is orthogonal to the concurrency law under test.
with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    import nturl2path


# ---- Corpus -------------------------------------------------------------------
# ROUND-TRIP-CLEAN NT paths: well-formed DOS-drive and UNC paths for which
# url2pathname(pathname2url(p)) == p holds exactly (verified serially).  These
# drive BOTH the reference-match arm and the round-trip identity arm.  Chars that
# force percent-encoding (space, '#', '%', '+', '=', '&') exercise the
# urllib.parse.quote C path; plain ASCII segments exercise the fast path.
RT_PATHS = (
    "C:\\foo\\bar\\spam.foo",
    "C:\\path with spaces\\file.txt",
    "D:\\a\\b\\c",
    "\\\\host\\share\\dir\\file",
    "C:\\weird#chars%here&there",
    "E:\\unicode\\segment\\name",
    "C:\\a+b=c",
    "relative\\path\\no\\drive",
    "F:\\100%done\\ok",
    "G:\\mixed 1#2%3&4=5+6\\end",
    "Z:\\single",
    "\\\\srv\\pub\\deep\\nested\\thing.dat",
)

# url2pathname inputs: file-URL forms (empty authority '///', pipe-drive 'C|',
# 'localhost' authority, percent-escapes) that exercise the url2pathname slicing
# and unquote paths.  These need not round-trip; they drive the reference-match
# arm only (expected computed serially).
URLS = (
    "///C|/foo/bar/spam.foo",
    "///C:/foo/bar/spam.foo",
    "//localhost/C:/x/y",
    "/C:/a%20b",
    "///D:/a%23b",
    "///E:/p%25q/r%2Bs",
    "//host/share/file",
    "///F:/name%20with%20spaces",
    "/D|/legacy/pipe/drive",
    "///G:/tail%26end",
)

# Quote-forcing character menu for the fiber-local determinism arm.
QUOTE_CHARS = (" ", "#", "%", "+", "=", "&", "@", "~")


def build_local_path(wid, idx):
    """Construct a UNIQUE fiber-local NT path embedding wid + idx plus a rotating
    quote-forcing char, so the fiber-local determinism arm exercises varied quote
    inputs while the input is a private local string this fiber built."""
    qc = QUOTE_CHARS[(wid + idx) % len(QUOTE_CHARS)]
    drive = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[wid % 26]
    return "{0}:\\w{1}\\seg{2}{3}part\\file.dat".format(drive, wid, idx, qc)


# ---- LOAD-BEARING A: reference match ------------------------------------------
def ref_check(H, wid, idx, state):
    """pathname2url / url2pathname must reproduce the serially-precomputed gold
    reference, bit-identical, and be stable across a yield."""
    paths = state["rt_paths"]
    urls = state["urls"]
    exp_p2u = state["exp_p2u"]
    exp_u2p = state["exp_u2p"]

    pi = idx % len(paths)
    ui = idx % len(urls)
    p = paths[pi]
    u = urls[ui]

    # Compute before the yield.
    got_p_before = nturl2path.pathname2url(p)
    got_u_before = nturl2path.url2pathname(u)

    # YIELD: a sibling on another hub may be mid-call in the same C quote/unquote
    # machinery; if the runtime tears this fiber's result or corrupts the shared
    # read-only module state, the post-yield recompute diverges.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    got_p_after = nturl2path.pathname2url(p)
    got_u_after = nturl2path.url2pathname(u)

    # Determinism across the yield.
    if got_p_before != got_p_after:
        H.fail("pathname2url NON-DETERMINISTIC across a yield: input {0!r} gave "
               "{1!r} then {2!r} (wid {3}) -- the pure-function result changed "
               "across hub migration".format(p, got_p_before, got_p_after, wid))
        return
    if got_u_before != got_u_after:
        H.fail("url2pathname NON-DETERMINISTIC across a yield: input {0!r} gave "
               "{1!r} then {2!r} (wid {3}) -- the pure-function result changed "
               "across hub migration".format(u, got_u_before, got_u_after, wid))
        return

    # Match the serial gold reference (closed-form truth for each fixed input).
    if got_p_after != exp_p2u[pi]:
        H.fail("pathname2url WRONG: input {0!r} gave {1!r}, expected {2!r} "
               "(serial reference, wid {3}) -- a cross-fiber corruption of the "
               "pure output or the read-only urllib/ntpath module state".format(
                   p, got_p_after, exp_p2u[pi], wid))
        return
    if got_u_after != exp_u2p[ui]:
        H.fail("url2pathname WRONG: input {0!r} gave {1!r}, expected {2!r} "
               "(serial reference, wid {3}) -- a cross-fiber corruption of the "
               "pure output or the read-only urllib/ntpath module state".format(
                   u, got_u_after, exp_u2p[ui], wid))
        return

    state["ref_checks"][wid] += 1


# ---- LOAD-BEARING B: round-trip identity --------------------------------------
def roundtrip_check(H, wid, idx, state):
    """url2pathname(pathname2url(p)) == p for the round-trip-clean corpus, across
    a yield -- the inverse-function law."""
    paths = state["rt_paths"]
    pi = idx % len(paths)
    p = paths[pi]

    url = nturl2path.pathname2url(p)
    runloom.yield_now()
    recovered = nturl2path.url2pathname(url)

    if recovered != p:
        H.fail("ROUND-TRIP broken: url2pathname(pathname2url({0!r})) == {1!r} != "
               "{0!r} (via url {2!r}, wid {3}) -- the inverse-function identity "
               "failed across a yield".format(p, recovered, url, wid))
        return

    state["rt_checks"][wid] += 1


# ---- LOAD-BEARING C: fiber-local determinism ----------------------------------
def local_check(H, wid, idx, state):
    """Each fiber builds its OWN unique input path and asserts pathname2url is
    deterministic across a yield -- a private-input purity check."""
    p = build_local_path(wid, idx)

    got_before = nturl2path.pathname2url(p)
    runloom.yield_now()
    got_after = nturl2path.pathname2url(p)

    if got_before != got_after:
        H.fail("pathname2url NON-DETERMINISTIC on fiber-local input {0!r}: {1!r} "
               "then {2!r} across a yield (wid {3}) -- a frame/locals desync or a "
               "torn result string under hub migration".format(
                   p, got_before, got_after, wid))
        return

    # The result must also round-trip for this well-formed DOS path (extra law).
    recovered = nturl2path.url2pathname(got_after)
    if recovered != p:
        H.fail("fiber-local ROUND-TRIP broken: recovered {0!r} != input {1!r} "
               "(url {2!r}, wid {3})".format(recovered, p, got_after, wid))
        return

    state["local_checks"][wid] += 1


INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs all three single-owner purity arms per inner iteration.  A
    sustained mix of many fibers computing + sleep-parked across their yields is
    what reliably interleaves a sibling's in-flight conversion before this fiber
    resumes; a single check barely overlaps and would not reproduce."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            ref_check(H, wid, idx, state)
            if H.failed:
                return
            roundtrip_check(H, wid, idx, state)
            if H.failed:
                return
            local_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # GOLD reference: computed ONCE, single-threaded and quiescent, before any
    # fiber runs.  These are the closed-form truth for each fixed corpus input.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        exp_p2u = tuple(nturl2path.pathname2url(p) for p in RT_PATHS)
        exp_u2p = tuple(nturl2path.url2pathname(u) for u in URLS)
        # Assert the round-trip-clean corpus really is clean (guards the corpus
        # itself, serially, so LOAD-BEARING B is a true identity law).
        for p, u in zip(RT_PATHS, exp_p2u):
            assert nturl2path.url2pathname(u) == p, (p, u)

    H.state = {
        "rt_paths": RT_PATHS,
        "urls": URLS,
        "exp_p2u": exp_p2u,
        "exp_u2p": exp_u2p,
        # Per-wid slots (single writer per slot; race-free conservation counters).
        "ref_checks": [0] * H.funcs,
        "rt_checks": [0] * H.funcs,
        "local_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    refc = sum(H.state["ref_checks"])
    rtc = sum(H.state["rt_checks"])
    locc = sum(H.state["local_checks"])

    H.log("nturl2path purity: reference-match={0} round-trip={1} fiber-local="
          "{2} (all single-owner, fail-fast; total ops={3})".format(
              refc, rtc, locc, H.total_ops()))

    # NON-VACUITY: every load-bearing arm actually ran.
    H.check(refc > 0,
            "no reference-match checks ran -- the pathname2url/url2pathname purity "
            "oracle was never exercised (vacuous)")
    H.check(rtc > 0,
            "no round-trip checks ran -- the inverse-function identity was never "
            "exercised (vacuous)")
    H.check(locc > 0,
            "no fiber-local determinism checks ran (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a conversion call.
    H.require_no_lost("nturl2path purity")


if __name__ == "__main__":
    harness.main(
        "p581_nturl2path_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="nturl2path.pathname2url / url2pathname are PURE string functions "
                 "(NT path <-> file URL) with no mutable module state.  Under M:N "
                 "with the GIL off, each fiber recomputes them on fixed read-only "
                 "corpus inputs and its OWN fiber-local inputs, across a yield, and "
                 "asserts the result is bit-identical to a serially-precomputed "
                 "gold reference, stable across the yield, and obeys the round-trip "
                 "identity url2pathname(pathname2url(p))==p.  Every arm is single-"
                 "owner (result strings are locals; corpus + reference are read-"
                 "only), so any divergence is a genuine runtime bug: a torn result "
                 "string, a frame/locals desync across hub migration, corruption of "
                 "the read-only urllib.parse/ntpath state, or a SIGSEGV in the C "
                 "quote/unquote/splitroot machinery")
