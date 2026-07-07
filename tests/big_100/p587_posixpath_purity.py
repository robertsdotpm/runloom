"""big_100 / 587 -- posixpath pure-function purity + closed-form identity under M:N.

posixpath is a library of PURE functions over strings: split(), splitext(),
join(), normpath(), dirname(), basename(), isabs(), commonpath(), relpath()...
Each is a deterministic function of its arguments with NO process-global or
per-thread state (unlike os.getcwd()-touching abspath, which we deliberately
avoid).  Feed the same fiber-local string(s) in and you MUST get bit-identical
output out -- every time, on every hub, before and after any yield.

WHERE M:N COULD BREAK IT (the gap this program probes).  A pure string function
recomputed on the SAME fiber-local input must return the SAME result across a
yield.  If runloom torn a Python str object mid-flight, leaked a sibling fiber's
argument/return buffer across a hub migration, or corrupted an interned string
under GIL-off concurrency, the recomputed result would differ from the baseline
even though the input is single-owner and never shared.  posixpath is an ideal
probe because its outputs are exact, closed-form, and cheap to recompute: any
divergence is unambiguously a runtime memory/scheduling fault, never a Python
semantics quirk (there is no shared mutable state to blame).

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  For a pure function f and a single-owner input x, TWO independent laws hold on
  EVERY correct run and are checked fail-fast per fiber:

    (A) CLOSED-FORM IDENTITY (absolute, input-independent -- documented posixpath
        invariants that hold for ANY string):
          * splitext:   root, ext = splitext(p);  root + ext == p   (exact string)
          * normpath:   normpath(normpath(p)) == normpath(p)        (idempotent)
          * split:      split(p) == (dirname(p), basename(p))        (consistency)
          * isabs:      isabs(p) == p.startswith('/')                (definition)
        These are computed on the fiber-local input; a violation means posixpath
        produced a self-inconsistent result, i.e. a torn/corrupted computation.

    (B) DETERMINISM ACROSS A YIELD (purity): compute a full result BUNDLE
        (normpath, split, splitext, join-of-fiber-local-components, dirname,
        basename, isabs) BEFORE a yield, park the fiber so siblings run on other
        hubs, then recompute the SAME bundle from the SAME fiber-local inputs and
        assert it is BIT-IDENTICAL to the baseline.  A pure function of an
        unchanged single-owner input cannot legitimately change its answer; a
        change is a cross-fiber buffer leak, a torn str, or scheduler corruption.

  Verified with a plain-threads control (8 OS threads, GIL on AND off, each
  recomputing these bundles on its own inputs): 100% identical, 0 divergence.
  Under a correct runloom it must also hold, so this single-owner oracle PASSES
  (exit 0) when there is no bug.

  Single-owner: every input string / component list is built from the fiber's own
  wid + a fiber-local counter and lives in a fiber-local variable -- never shared,
  never mutated by anyone else.  posixpath itself keeps no state to alias.  There
  is therefore NO shared-mutable hazard to model here (unlike enum's shared
  _member_map_ in p490), so there is no report-only MEASURED arm: the purity
  oracle is the whole story and it must stay clean.

ORACLES:
  * LOAD-BEARING -- PURITY + CLOSED-FORM (worker, HARD, fail-fast): laws (A) and
    (B) above, per fiber, on fiber-local inputs, across a real yield.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-recompute
    never returns; the watchdog + require_no_lost catch it.

FAIL ON: a splitext root+ext that does not reconstruct the input, a non-idempotent
normpath, a split that disagrees with (dirname, basename), an isabs that disagrees
with startswith('/'), or ANY element of the recomputed bundle differing from the
baseline across a yield.  Each is a self-inconsistency or non-determinism of a
pure function on an unchanged single-owner input -- a runtime fault, never
documented Python behavior.

Stresses: posixpath pure-string functions (split/splitext/join/normpath/dirname/
basename/isabs) recomputed across hub migration + yield, str identity/value
stability under M:N, interned-separator handling, closed-form path-algebra
identities under GIL-off concurrency.
"""
import posixpath

import harness
import runloom

# Path-segment vocabulary.  A mix that pushes normpath/split through their real
# branches: current-dir '.', parent '..', empty '' (double-slash), dotfiles,
# multi-dot names (splitext edge), and ordinary names.  All ASCII, fiber-local.
SEGMENTS = (".", "..", "", "a", "bb", "ccc", "foo", "bar", "dir",
            ".hidden", "file.txt", "a.tar.gz", "name.", ".", "x", "y")

# How many path variants a single generate call produces (each a distinct
# fiber-local input for the closed-form + purity checks).
VARIANTS_PER_GEN = 8

# Sustained checks per worker, bounded by H.running().  The purity hazard (a torn
# str / cross-fiber buffer leak across a yield) only manifests under SUSTAINED
# churn -- many fibers recomputing path bundles while sleep-PARKED across their
# yield, so the scheduler reliably interleaves siblings before this fiber resumes.
INNER_CAP = 100000


def build_path(rng, wid, idx):
    """Build ONE fiber-local posix path string from the fiber's own wid+idx.

    The string is uniquely tied to this fiber (a wid-tagged leading segment) so it
    is provably single-owner and never coincides with a sibling's input.  Leading
    slash, interior segments, and trailing slash are all rng-driven so normpath /
    split traverse their real branches."""
    n = rng.randint(1, 6)
    segs = ["w{0}i{1}".format(wid, idx)]           # wid-tagged: single-owner marker
    for _ in range(n):
        segs.append(SEGMENTS[rng.randrange(len(SEGMENTS))])
    body = "/".join(segs)
    if rng.random() < 0.5:
        body = "/" + body                          # absolute
    if rng.random() < 0.4:
        body = body + "/"                          # trailing slash
    if rng.random() < 0.3:
        body = body.replace("/", "//", 1)          # double slash somewhere
    return body


def build_components(rng, wid, idx):
    """Build a fiber-local component list for a join() determinism probe."""
    n = rng.randint(2, 5)
    comps = ["c{0}_{1}".format(wid, idx)]
    for _ in range(n):
        comps.append(SEGMENTS[rng.randrange(len(SEGMENTS))])
    return comps


def compute_bundle(path, comps):
    """Compute the full posixpath result BUNDLE for a fiber-local (path, comps).

    Pure function of its arguments; returns a hashable tuple used for the
    across-a-yield determinism comparison.  No shared state is touched."""
    head, tail = posixpath.split(path)
    root, ext = posixpath.splitext(path)
    return (
        posixpath.normpath(path),
        head, tail,
        root, ext,
        posixpath.dirname(path),
        posixpath.basename(path),
        posixpath.isabs(path),
        posixpath.join(*comps),
    )


def check_closed_form(H, path, wid):
    """Absolute, input-independent posixpath identities (law A).  Any violation is
    a self-inconsistent pure-function result -- a torn/corrupted computation.
    Returns False (after H.fail) on violation."""
    root, ext = posixpath.splitext(path)
    if root + ext != path:
        H.fail("splitext identity BROKEN: splitext({0!r}) = ({1!r},{2!r}) but "
               "root+ext = {3!r} != input (wid {4}) -- posixpath produced a "
               "result that does not reconstruct its single-owner input".format(
                   path, root, ext, root + ext, wid))
        return False

    np1 = posixpath.normpath(path)
    np2 = posixpath.normpath(np1)
    if np2 != np1:
        H.fail("normpath NOT IDEMPOTENT: normpath({0!r})={1!r} but "
               "normpath(that)={2!r} (wid {3}) -- a pure idempotent function "
               "returned two different answers on an unchanged input".format(
                   path, np1, np2, wid))
        return False

    head, tail = posixpath.split(path)
    if head != posixpath.dirname(path) or tail != posixpath.basename(path):
        H.fail("split/dirname/basename DISAGREE for {0!r}: split=({1!r},{2!r}) "
               "vs (dirname={3!r}, basename={4!r}) (wid {5})".format(
                   path, head, tail, posixpath.dirname(path),
                   posixpath.basename(path), wid))
        return False

    if posixpath.isabs(path) != path.startswith("/"):
        H.fail("isabs DISAGREES with startswith('/') for {0!r}: isabs={1} "
               "startswith={2} (wid {3})".format(
                   path, posixpath.isabs(path), path.startswith("/"), wid))
        return False
    return True


def purity_check(H, wid, idx, rng, state):
    """One fiber-local purity + closed-form check across a real yield (law A + B).

    Build single-owner inputs, verify closed-form identities, snapshot the full
    result bundle, YIELD (siblings run on other hubs), then recompute the bundle
    from the SAME inputs and assert bit-identical.  A divergence is a runtime
    memory/scheduling fault, never documented Python behavior."""
    path = build_path(rng, wid, idx)
    comps = build_components(rng, wid, idx)

    # Law A -- closed-form identities on the single-owner input (pre-yield).
    if not check_closed_form(H, path, wid):
        return
    # Baseline bundle (law B snapshot).
    baseline = compute_bundle(path, comps)

    # YIELD: park so siblings recompute their own bundles on other hubs, exercising
    # the torn-str / cross-fiber-buffer-leak window before this fiber resumes.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Law A again post-yield (a corrupted input would break the closed form too).
    if not check_closed_form(H, path, wid):
        return
    # Law B -- recompute the SAME bundle from the SAME single-owner inputs.
    again = compute_bundle(path, comps)
    if again != baseline:
        # Find the first differing element for a precise message.
        labels = ("normpath", "split.head", "split.tail", "splitext.root",
                  "splitext.ext", "dirname", "basename", "isabs", "join")
        for i, lab in enumerate(labels):
            if again[i] != baseline[i]:
                H.fail("posixpath NON-DETERMINISM across a yield: for input "
                       "{0!r} the pure result {1} changed from {2!r} to {3!r} "
                       "(wid {4}) -- a pure function of an unchanged single-owner "
                       "input returned a different answer; a torn str / cross-"
                       "fiber buffer leak / scheduler corruption".format(
                           path, lab, baseline[i], again[i], wid))
                return
        # Shouldn't reach here (tuples differ but no element found), but be safe.
        H.fail("posixpath bundle differed across a yield for {0!r} (wid {1}) but "
               "no element localized -- torn tuple".format(path, wid))
        return

    state["checks"][wid & 1023] += 1           # non-vacuity tally (sharded; OK)


def worker(H, wid, rng, state):
    """Sustained purity + closed-form churn on fiber-local posixpath inputs."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,          # non-vacuity tally (sharded, report-only)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("posixpath purity[single-owner LOAD-BEARING]: {0} closed-form + "
          "across-a-yield determinism checks (all passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no posixpath purity checks ran -- the load-bearing closed-form + "
            "determinism oracle was never exercised (would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-recompute.
    H.require_no_lost("posixpath purity")


if __name__ == "__main__":
    harness.main(
        "p587_posixpath_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="posixpath is a library of PURE string functions (split/splitext/"
                 "join/normpath/dirname/basename/isabs) with no process-global or "
                 "per-thread state.  LOAD-BEARING (single-owner): each fiber builds "
                 "wid-tagged fiber-local paths, asserts closed-form identities "
                 "(splitext root+ext==p, normpath idempotent, split==(dirname,"
                 "basename), isabs==startswith('/')), snapshots a full result "
                 "bundle, yields across a hub migration, and recomputes the SAME "
                 "bundle from the SAME inputs -- it MUST be bit-identical.  A "
                 "self-inconsistent result or a bundle that changes across the "
                 "yield is a torn str / cross-fiber buffer leak / scheduler "
                 "corruption -- a runtime bug, never documented Python semantics")
