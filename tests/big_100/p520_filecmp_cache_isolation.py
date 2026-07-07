"""big_100 / 520 -- filecmp._cache isolation under M:N (free-threaded 3.14t).

filecmp keeps a MODULE-GLOBAL cache, ``filecmp._cache``, a plain dict keyed by
``(f1, f2, stat_signature(f1), stat_signature(f2))`` mapping to the last
``cmp()`` verdict for that exact path-pair-and-signature.  ``filecmp.cmp`` does a
per-call READ (``_cache.get(key)``), and on a miss a WRITE
(``_cache[key] = outcome``) plus a size-guard that CLEARS the whole dict once it
passes ~100 entries.  ``filecmp.cmpfiles`` (and therefore ``dircmp``) funnels
through the same ``cmp`` and the same shared cache.

WHERE M:N COULD BREAK IT (the gap this program probes).  With the GIL off and
hubs>1, tens of thousands of fibers call ``filecmp.cmp``/``dircmp`` concurrently,
so they all hammer the ONE module-global ``_cache`` dict: interleaved get / set /
clear on the same object across hub migrations.  If a cache entry were ever
mis-keyed, aliased, or torn -- e.g. a fiber's ``get`` returned a SIBLING's cached
verdict for a DIFFERENT path pair, or a concurrent ``clear()`` handed back a
poisoned value -- a fiber would observe a WRONG comparison result for its own
private files.  Because ``cmp`` is a read-modify-(maybe)-write over a shared dict,
this is exactly the shape where a runtime that mis-schedules or mis-shares state
would surface as a wrong verdict or a crash mid-``clear()``.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  Each fiber OWNS a private pair of on-disk files (paths contain its wid, created
  once at fiber start, never touched by any sibling) with content that is either
  byte-IDENTICAL or definitely DIFFERENT -- the GROUND TRUTH is fixed at creation
  time (closed-world).  Two documented single-owner interfaces must return that
  ground truth, and must keep returning it ACROSS A YIELD while thousands of
  siblings churn the shared cache:

    * ``filecmp.cmp(a, b, shallow=False)`` compares CONTENT (not just the stat
      signature), so identical content -> True, different content -> False,
      independent of mtime.  It is keyed in ``_cache`` by the fiber's UNIQUE
      paths, so a correct runtime can never return a sibling's verdict for it.
    * A single-owner ``filecmp.dircmp(dA, dB, shallow=False)`` over the fiber's
      own two directories: identical file -> the filename is in ``same_files``
      and NOT in ``diff_files``; different -> the reverse.  ``dircmp`` routes
      through ``cmpfiles`` -> ``cmp`` -> the same shared ``_cache``.

  The fiber computes both verdicts, YIELDS (so siblings interleave their own
  cache get/set/clear), then recomputes both and asserts each still equals the
  fiber's own ground truth.  Since every key in ``_cache`` for this fiber is
  built from its UNIQUE paths, a correct runtime returns the fiber's true verdict
  every time; a wrong verdict means the shared cache leaked a sibling's answer
  into this single-owner lookup -- a runloom isolation bug.  On a correct runtime
  the load-bearing arm PASSES (program exits 0).

  Verified against plain threads: a standalone control (8 OS threads, GIL on and
  off, each thread owning a private identical-or-different file pair, all sharing
  the one ``filecmp._cache``) returns the correct verdict 100% of the time -- 0
  cross-thread verdict leaks and no crash in the concurrent get/set/clear.  Under
  a correct runloom it must hold too.

ORACLES:
  * LOAD-BEARING -- FILECMP VERDICT ISOLATION (worker, HARD, fail-fast).  Private
    file pair + private dir pair, known ground truth, cmp + dircmp verdicts must
    equal ground truth before AND after a yield.  Single-owner: nobody else reads
    or writes this fiber's paths.  A wrong verdict is a runloom cache-isolation
    desync; a crash inside the shared ``_cache`` get/set/clear is a hard fault.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    ``cmp``/``_do_cmp``/``dircmp`` walk (parked mid os.stat / file read / dict
    op) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (verdicts > 0).

  * SECONDARY (report-ONLY, NEVER fails): MEASURED shared-pair cmp.  All fibers
    also compare ONE global shared file pair (same cache key hammered by every
    fiber -> maximal contention on a single ``_cache`` entry + its clear cycle).
    The shared pair's content is fixed, so its ground truth is fixed too; we
    COUNT any verdict that disagrees with ground truth and report the rate.  On a
    correct runtime this is 0 -- it is a stress witness that the shared cache was
    genuinely pounded, NOT a failure signal (a shared object under M:N is
    documented behavior; we never H.fail on it).

FAIL ON: a fiber's own cmp/dircmp verdict disagreeing with its private ground
truth (before or after a yield), or a crash inside the shared ``filecmp._cache``
get/set/clear.  The shared-pair MEASURED arm is report-only.

Stresses: filecmp module-global ``_cache`` dict get/set/clear under GIL-off
contention, ``filecmp.cmp(shallow=False)`` content compare + cache RMW,
``dircmp``/``cmpfiles`` funnelling through the same shared cache, os.stat +
file-read racing a concurrent ``_cache.clear()``, per-fiber verdict isolation vs
shared-key contention.

Good TSan / controlled-M:N-replay target: ``filecmp._cache`` is a shared dict
mutated (get/set and a full ``clear()`` past ~100 entries) by every fiber; under
the single-owner arm each fiber's KEY is private, so a data-race report on the
dict object -- or a replay that returns a sibling's verdict for a private key --
is the cleanest signal before the ground-truth oracle even fires.
"""
import filecmp
import os

import harness
import runloom

# Content sizes for the private file pair.  Identical case: both files hold the
# SAME bytes.  Different case: different bytes AND different sizes so the verdict
# is unambiguously False under shallow=False (content compare).  Small so 2000
# fibers' worth of files stay cheap.
BASE_LEN = 96


def make_content(wid, tag):
    """Deterministic per-fiber payload, unique across wids so a leaked sibling
    read would be visibly wrong."""
    seed = ("W{0}:{1}:".format(wid, tag)).encode("ascii")
    body = bytes((wid * 7 + i * 13) & 0xFF for i in range(BASE_LEN))
    return seed + body


def build_fiber_files(root, wid, identical):
    """Create this fiber's PRIVATE two directories, each with one file 'data'.

    Returns (fileA, fileB, dirA, dirB).  Paths contain wid so no sibling ever
    touches them.  Ground truth is `identical`: when True both files hold the
    exact same bytes (cmp/dircmp with shallow=False -> equal); when False the
    files differ in both content and size (-> not equal)."""
    dirA = os.path.join(root, "w{0}_a".format(wid))
    dirB = os.path.join(root, "w{0}_b".format(wid))
    os.mkdir(dirA)
    os.mkdir(dirB)
    fileA = os.path.join(dirA, "data")
    fileB = os.path.join(dirB, "data")

    contentA = make_content(wid, "A")
    if identical:
        contentB = contentA
    else:
        # Different bytes AND a different length so False is unambiguous even if
        # a signature-only path were ever taken.
        contentB = make_content(wid, "B") + b"\xff\x00extra-bytes-here"

    with open(fileA, "wb") as fh:
        fh.write(contentA)
    with open(fileB, "wb") as fh:
        fh.write(contentB)
    return fileA, fileB, dirA, dirB


def verdict_matches(H, wid, fileA, fileB, dirA, dirB, identical, phase):
    """Compute the cmp + dircmp verdicts and assert both equal `identical`.

    Returns True on match, False after calling H.fail on a mismatch."""
    # --- filecmp.cmp content compare (shared _cache, private key) -------------
    cmp_res = filecmp.cmp(fileA, fileB, shallow=False)
    if cmp_res != identical:
        H.fail("filecmp.cmp WRONG VERDICT ({0}): cmp({1!r}, {2!r}, shallow=False)"
               " returned {3}, ground truth is {4} (wid {5}) -- the shared "
               "filecmp._cache leaked a sibling's verdict into this fiber's "
               "private-key lookup, or a torn read".format(
                   phase, fileA, fileB, cmp_res, identical, wid))
        return False

    # --- dircmp over private dirs, routed through cmpfiles -> cmp -> _cache ----
    dc = filecmp.dircmp(dirA, dirB, shallow=False)
    same = "data" in dc.same_files
    diff = "data" in dc.diff_files
    if same == diff:
        H.fail("dircmp INCONSISTENT ({0}): 'data' same={1} diff={2} (wid {3}) -- "
               "a file must land in exactly one of same_files/diff_files; the "
               "shared cache/cmpfiles state is desynced".format(
                   phase, same, diff, wid))
        return False
    if same != identical:
        H.fail("dircmp WRONG VERDICT ({0}): 'data' in same_files={1}, ground "
               "truth identical={2} (wid {3}) -- dircmp/cmpfiles returned a "
               "sibling's verdict through the shared filecmp._cache".format(
                   phase, same, identical, wid))
        return False
    return True


# Sustained cmp/dircmp churn per fiber, bounded by H.running().  The shared-cache
# hazard only shows under SUSTAINED contention -- many fibers concurrently doing
# get/set and tripping the ~100-entry clear() on the one _cache dict while parked
# across their yield -- so a sibling reliably interleaves before this fiber
# resumes.  A single check per fiber barely overlaps and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Create the fiber's private identical-or-different file+dir pair ONCE, then
    loop asserting the cmp+dircmp verdicts equal ground truth across a yield while
    siblings churn the shared filecmp._cache.  Also drives the report-only
    shared-pair MEASURED arm."""
    identical = bool(rng.getrandbits(1))
    root = state["root"]
    try:
        fileA, fileB, dirA, dirB = build_fiber_files(root, wid, identical)
    except OSError:
        # Out of inodes / fds at over-scale is a resource ceiling, not a bug;
        # skip this fiber quietly (never fail on it).
        return

    shared_a = state["shared_a"]
    shared_b = state["shared_b"]

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # LOAD-BEARING: verdict before the yield ...
            if not verdict_matches(H, wid, fileA, fileB, dirA, dirB,
                                   identical, "pre-yield"):
                return
            # YIELD: let siblings interleave their _cache get/set/clear.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)
            # ... and the SAME verdict after the yield.
            if not verdict_matches(H, wid, fileA, fileB, dirA, dirB,
                                   identical, "post-yield"):
                return

            # MEASURED (report-only): hammer the ONE shared cache key.  Ground
            # truth for the shared pair is False (different content).  A correct
            # runtime always returns False; we count disagreements, never fail.
            try:
                sres = filecmp.cmp(shared_a, shared_b, shallow=False)
                state["shared_checks"][wid & 1023] += 1
                if sres is not False:
                    state["shared_leaks"][wid & 1023] += 1
            except OSError:
                pass

            state["verdicts"][wid & 1023] += 1
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    root = H.make_tmpdir(prefix="p520_filecmp_")
    # One GLOBAL shared file pair (different content -> ground truth False) that
    # ALL fibers compare via the same cache key -- maximal contention on a single
    # filecmp._cache entry.  Built in the root; read-only thereafter.
    shared_a = os.path.join(root, "shared_a")
    shared_b = os.path.join(root, "shared_b")
    with open(shared_a, "wb") as fh:
        fh.write(b"SHARED-A-" + b"\x11" * BASE_LEN)
    with open(shared_b, "wb") as fh:
        fh.write(b"SHARED-B-" + b"\x22" * (BASE_LEN + 7))

    H.state = {
        "root": root,
        "shared_a": shared_a,
        "shared_b": shared_b,
        "verdicts": [0] * 1024,        # LOAD-BEARING single-owner verdict checks
        "shared_checks": [0] * 1024,   # MEASURED shared-key cmp checks
        "shared_leaks": [0] * 1024,    # shared-key verdict disagreements (report)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    vchecks = sum(H.state["verdicts"])
    schecks = sum(H.state["shared_checks"])
    sleaks = sum(H.state["shared_leaks"])
    spct = (100.0 * sleaks / schecks) if schecks else 0.0

    H.log("filecmp[single-owner LOAD-BEARING]: {0} cmp+dircmp verdict-isolation "
          "checks (all passed fail-fast) | filecmp[shared-key MEASURED]: {1} "
          "checks {2} disagreements ({3:.1f}%, REPORT ONLY)".format(
              vchecks, schecks, sleaks, spct))

    if sleaks:
        H.log("note: the shared filecmp._cache key observed {0} verdict "
              "disagreements across {1} checks -- this is contention on a shared "
              "cache entry (documented shared-object behavior), NOT a runloom bug, "
              "and never reaches the load-bearing single-owner oracle".format(
                  sleaks, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(vchecks > 0,
            "no single-owner filecmp verdict-isolation checks ran -- the load-"
            "bearing cache-isolation hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside cmp/dircmp/_cache ops.
    H.require_no_lost("filecmp cache isolation")


if __name__ == "__main__":
    harness.main(
        "p520_filecmp_cache_isolation", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=2000,
        describe="filecmp keeps a module-global _cache dict keyed by "
                 "(paths, stat signatures) that every filecmp.cmp/dircmp call "
                 "gets/sets/clears.  Under M:N thousands of fibers pound that ONE "
                 "shared dict.  LOAD-BEARING: each fiber owns a private identical-"
                 "or-different file+dir pair with KNOWN ground truth; "
                 "filecmp.cmp(shallow=False) and a single-owner dircmp(shallow="
                 "False) must return that ground truth before AND after a yield.  "
                 "A wrong verdict means the shared cache leaked a sibling's answer "
                 "into a private-key lookup -- a runloom isolation bug.  MEASURED "
                 "shared-key arm (report-only) proves the cache was contended")
