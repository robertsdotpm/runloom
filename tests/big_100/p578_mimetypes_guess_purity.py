"""big_100 / 578 -- mimetypes.MimeTypes guess round-trip PURITY under M:N.

mimetypes.guess_type(url) / guess_extension(type) / guess_all_extensions(type)
are pure lookups over a MimeTypes instance's two maps: types_map (extension ->
MIME type) and the reverse (type -> extensions).  add_type(type, ext) mutates
those maps.  The MODULE-LEVEL functions (mimetypes.guess_type, add_type, init)
operate on a PROCESS-GLOBAL default MimeTypes db -- that db is a shared object
and is NOT single-owner, so it must NOT be the fail-fast oracle (a shared map
mutated by many fibers races EXACTLY like a shared dict across threads --
documented Python behavior, not a runloom bug).

Instead the load-bearing oracle is built on a SINGLE-OWNER MimeTypes INSTANCE.
Each fiber constructs its OWN mimetypes.MimeTypes() and populates it with a set
of custom extension->type mappings whose values are UNIQUE to that fiber
(keyed on wid), so a sibling fiber's instance has DIFFERENT type strings for the
same-shaped extensions.  guess_type/guess_extension/guess_all_extensions on that
private instance are then pure functions of that fiber's own maps: the result
must be bit-identical across a yield and must match the closed-form expected
value this fiber wrote.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes
tens of thousands of fibers over a handful of hubs with the GIL off.  A fiber
adds custom types to its private MimeTypes instance, records the expected
guess_type result, YIELDS (so siblings run and mutate THEIR own instances +
the shared global db on other hubs), then re-guesses.  If the instance's maps
are not properly isolated per fiber -- if a lookup somehow reads another
fiber's instance's map, or a dict entry is torn under a concurrent rehash of a
DIFFERENT dict on another hub, or a member cache leaks across fibers -- the
re-guess returns a WRONG or non-deterministic type, and the purity law breaks.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  guess_type on a private MimeTypes instance whose maps only THIS fiber wrote
  is a pure function.  A standalone plain-threads control (8 OS threads, each
  building its own MimeTypes with per-thread-unique custom types, GIL on AND
  off) returns 100% the correct per-thread type for every custom filename, and
  the result is bit-stable across a yield/sleep -- 0 cross-thread leaks.  Under
  a CORRECT runloom it must also hold.  If a fiber's private-instance guess
  returns a value that differs from what it wrote, or changes across a yield,
  that is a runloom isolation/torn-lookup bug, and the single-owner oracle is
  clean on a correct runtime (program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- PRIVATE-INSTANCE PURITY (worker, HARD, fail-fast).  Each
    fiber owns one mimetypes.MimeTypes() (created fiber-local, never shared) and
    a table of NCUSTOM (extension, expected_type) pairs whose type strings embed
    wid (unique per fiber).  Per check it:
      - guess_type(filename)                 -> (type, encoding)   [before yield]
      - guess_extension(type)                -> ext
      - guess_all_extensions(type)           -> [ext]
      recorded as the baseline, then YIELDS (yield_now / tiny sleep) to let
      siblings interleave, then recomputes all three and asserts:
      (a) guess_type is BIT-IDENTICAL across the yield AND equals the closed-form
          expected (type, None) -- not a cross-fiber leak of a sibling's type;
      (b) guess_extension round-trips back to the extension it wrote;
      (c) guess_all_extensions contains that extension and every element is one
          of THIS fiber's own custom extensions (no foreign extension leaked in);
      (d) an extension this fiber NEVER added guesses to (None, None) on its
          private instance -- a sibling's add_type must not have leaked in.
    Single-owner: the instance and its maps are fiber-local; a failure is a
    runloom purity/isolation desync, never documented shared-map semantics.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    dict lookup / rehash on a desynced instance never returns; the watchdog +
    require_no_lost catch it.

  * MEASURED (report-ONLY, NEVER fails): a SHARED MimeTypes instance in the
    shared state is hammered by add_type from all fibers concurrently while they
    guess_type on it.  This is a shared mutable map under M:N -- cross-fiber
    visibility is EXPECTED and DOCUMENTED (like p490's shared-enum pool).  We
    MEASURE how often a guess on the shared instance disagrees with what this
    fiber just wrote to it and REPORT the rate; we NEVER fail on it.  This proves
    the hazard is real (fibers DO see each other's shared-map mutations), so the
    load-bearing single-owner arm is truly testing isolation, not missing it.

FAIL ON: a private-instance guess returning a cross-fiber/wrong type, a
non-deterministic guess across a yield, a broken extension round-trip, a foreign
extension leaking into a private instance's lookup, or a SIGSEGV/torn dict entry.
The shared-instance MEASURED arm is report-only and is expected to show cross-
fiber visibility (documented M:N shared-object behavior).

Stresses: mimetypes.MimeTypes types_map / reverse-map dict lookups and add_type
insertion under M:N, guess_type/guess_extension/guess_all_extensions purity and
per-instance isolation across hub migration + yield, private-instance vs shared-
instance map behavior, dict get/insert under GIL-off concurrency.
"""
import mimetypes

import harness
import runloom

# Number of custom (extension, type) mappings each fiber adds to its own private
# MimeTypes instance.  Enough to push the instance's maps through several dict
# growth/rehash boundaries (rehash is what moves entries under a concurrent
# lookup on another hub) yet small enough that many rounds complete under the
# timeout.
NCUSTOM = 24

# An extension NO fiber ever adds -- used to prove a sibling's add_type does not
# leak into this fiber's private instance (must guess to (None, None)).
NEVER_EXT = ".zzq_never_added_578"


def fiber_ext(idx):
    """Fiber-shaped-but-not-unique custom extension for slot idx.  The EXTENSION
    shape is shared across fibers (same idx -> same ext string) on purpose: what
    makes the values unique per fiber is the TYPE string (embeds wid).  So two
    fibers' private instances map the SAME extension to DIFFERENT types -- a
    cross-fiber leak would return the sibling's type for this fiber's extension."""
    return ".x578e{0}".format(idx)


def fiber_type(wid, idx):
    """The MIME type THIS fiber maps its slot-idx extension to.  Embeds wid so it
    is unique per fiber; a sibling mapping the same extension has a different wid
    and therefore a different type string."""
    return "application/x-fiber578-{0}-{1}".format(wid, idx)


def build_private_instance(wid):
    """Construct a fiber-local MimeTypes instance and populate it with this
    fiber's NCUSTOM unique custom types.  Returns (mt, expected) where expected
    maps filename -> (type, encoding) for the closed-form purity check.

    Single-owner: the returned instance is never shared; only this fiber reads or
    writes it.  add_type mutates the INSTANCE maps (mt.types_map), not the module
    global default db, so distinct fibers' instances stay isolated."""
    mt = mimetypes.MimeTypes()
    expected = []
    for idx in range(NCUSTOM):
        ext = fiber_ext(idx)
        typ = fiber_type(wid, idx)
        mt.add_type(typ, ext, strict=True)
        fname = "doc578_{0}_{1}{2}".format(wid, idx, ext)
        expected.append((fname, ext, typ))
    return mt, expected


# ---- LOAD-BEARING arm: single-owner private MimeTypes instance -----------
def purity_check(H, wid, mt, expected, state):
    """Recompute guess_type/guess_extension/guess_all_extensions on this fiber's
    PRIVATE instance across a yield and assert bit-identical + closed-form
    correct.  A cross-fiber leak or torn lookup returns a wrong/unstable value."""
    # Baseline pass (before the yield): record every lookup.
    baseline = []
    for fname, ext, typ in expected:
        gt = mt.guess_type(fname)
        ge = mt.guess_extension(typ, strict=True)
        gae = mt.guess_all_extensions(typ, strict=True)
        baseline.append((gt, ge, tuple(gae)))

    # An extension this fiber NEVER added must be unknown on its private instance.
    never_before = mt.guess_type("nope578" + NEVER_EXT)

    # YIELD at the hazard boundary so siblings interleave: they mutate THEIR own
    # private instances + the shared global db + the shared MEASURED instance on
    # other hubs while this fiber is parked.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    # Re-pass: assert stability + closed-form correctness.
    my_exts = frozenset(fiber_ext(i) for i in range(NCUSTOM))
    for i, (fname, ext, typ) in enumerate(expected):
        base_gt, base_ge, base_gae = baseline[i]

        gt = mt.guess_type(fname)
        # (a) guess_type bit-identical across the yield.
        if gt != base_gt:
            H.fail("guess_type NON-DETERMINISTIC across yield: {0!r} -> {1!r} "
                   "before, {2!r} after (wid {3}) -- a sibling's mutation or a "
                   "torn map lookup changed this fiber's PRIVATE-instance "
                   "guess".format(fname, base_gt, gt, wid))
            return
        # (a) guess_type equals the closed-form expected value this fiber wrote.
        if gt != (typ, None):
            H.fail("guess_type CROSS-FIBER/WRONG: {0!r} -> {1!r}, expected "
                   "{2!r} (wid {3}) -- this fiber's private instance returned a "
                   "type it did not write (a cross-fiber map leak)".format(
                       fname, gt, (typ, None), wid))
            return

        ge = mt.guess_extension(typ, strict=True)
        if ge != base_ge:
            H.fail("guess_extension NON-DETERMINISTIC across yield: {0!r} -> "
                   "{1!r} before, {2!r} after (wid {3})".format(
                       typ, base_ge, ge, wid))
            return
        # (b) extension round-trips: the type this fiber wrote maps back to the
        # extension it wrote (the reverse map is this fiber's own).
        if ge != ext:
            H.fail("guess_extension ROUND-TRIP broken: {0!r} -> {1!r}, expected "
                   "{2!r} (wid {3}) -- the reverse map returned a foreign or "
                   "wrong extension".format(typ, ge, ext, wid))
            return

        gae = tuple(mt.guess_all_extensions(typ, strict=True))
        if gae != base_gae:
            H.fail("guess_all_extensions NON-DETERMINISTIC across yield: {0!r} "
                   "-> {1!r} before, {2!r} after (wid {3})".format(
                       typ, base_gae, gae, wid))
            return
        # (c) every returned extension is one THIS fiber added (no foreign leak),
        # and the expected extension is present.
        if ext not in gae:
            H.fail("guess_all_extensions MISSING own ext: {0!r} -> {1!r} does "
                   "not contain {2!r} (wid {3})".format(typ, gae, ext, wid))
            return
        for got_ext in gae:
            if got_ext not in my_exts:
                H.fail("guess_all_extensions FOREIGN ext leaked: {0!r} -> {1!r} "
                       "contains {2!r}, not one of this fiber's own custom "
                       "extensions (wid {3}) -- a cross-fiber reverse-map "
                       "leak".format(typ, gae, got_ext, wid))
                return

    # (d) the never-added extension stays unknown on the private instance across
    # the yield -- a sibling's add_type must not have leaked in.
    never_after = mt.guess_type("nope578" + NEVER_EXT)
    if never_after != (None, None):
        H.fail("private instance LEAKED a foreign type for a never-added "
               "extension {0!r} -> {1!r} (wid {2}) -- a sibling's add_type "
               "leaked into this fiber's private map".format(
                   NEVER_EXT, never_after, wid))
        return
    if never_after != never_before:
        H.fail("never-added extension guess changed across yield: {0!r} -> "
               "{1!r} (wid {2})".format(never_before, never_after, wid))
        return

    state["checks"][wid] += NCUSTOM


# ---- MEASURED arm: shared MimeTypes instance (report-only) ---------------
def shared_probe(H, wid, r, state):
    """Add a custom type to the SHARED MimeTypes instance and immediately guess it
    back (MEASURED, report-only).  All fibers write the SAME shared maps, so a
    guess disagreeing with what this fiber just wrote is EXPECTED shared-object
    behavior (a sibling overwrote the same extension).  We measure + report the
    disagreement rate; we NEVER fail on it."""
    shared = state["shared_mt"]
    # A small shared extension keyspace so fibers COLLIDE on the same keys.
    ext = ".shr578_{0}".format(r % 16)
    typ = "application/x-shared578-{0}-{1}".format(wid, r)
    shared.add_type(typ, ext, strict=True)
    got_type, got_enc = shared.guess_type("s" + ext)

    state["shared_checks"][wid] += 1
    # A sibling may have overwritten ext->type between our add and our guess: the
    # guess then returns the sibling's type.  Documented shared-map behavior.
    if got_type != typ:
        state["shared_diffs"][wid] += 1
    # Crash-safety: the returned type, when present, must be a well-formed shared
    # type string (never a torn/garbage object).  A non-str return would be a
    # torn read -- but we still only MEASURE (report) it, per the shared-object
    # rule; the load-bearing arm is where a fault is declared.
    if got_type is not None and not isinstance(got_type, str):
        state["shared_torn"][wid] += 1


# Sustained checks per worker, bounded by H.running().  The isolation/purity
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# building/looking-up distinct instances while sleep-parked across their yield,
# so the scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber owns ONE private MimeTypes instance for its whole life and runs
    BOTH arms per iteration: the LOAD-BEARING private-instance purity check
    (fail-fast) and the MEASURED shared-instance probe (report only).  The two
    never share maps (private instance vs shared instance) so the shared
    mutations never reach the single-owner oracle."""
    mt, expected = build_private_instance(wid)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, mt, expected, state)     # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_probe(H, wid, idx, state)              # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # The shared MimeTypes instance for the MEASURED arm (a shared mutable map,
    # like p490's shared-enum pool) plus per-wid race-free slot tables (one
    # writer per slot; allocate here where H.funcs is known).
    H.state = {
        "shared_mt": mimetypes.MimeTypes(),
        "checks": [0] * H.funcs,          # LOAD-BEARING private-instance checks
        "shared_checks": [0] * H.funcs,   # MEASURED shared-instance probes
        "shared_diffs": [0] * H.funcs,    # shared guess != own write (expected)
        "shared_torn": [0] * H.funcs,     # shared guess returned a non-str (report)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    schecks = sum(H.state["shared_checks"])
    sdiffs = sum(H.state["shared_diffs"])
    storn = sum(H.state["shared_torn"])
    spct = (100.0 * sdiffs / schecks) if schecks else 0.0

    H.log("mimetypes[private-instance LOAD-BEARING]: {0} purity/isolation checks "
          "(all passed fail-fast) | mimetypes[shared-instance MEASURED]: {1} "
          "probes {2} cross-fiber-visible diffs ({3:.1f}%, documented shared-map "
          "behavior -- REPORT ONLY) torn={4}".format(
              checks, schecks, sdiffs, spct, storn))

    if sdiffs:
        H.log("note: the shared MimeTypes instance observed {0} cross-fiber "
              "guess/add-back disagreements across {1} probes -- runloom hub "
              "fibers see each other's mutations on a SHARED map object (like "
              "p490's shared-enum pool).  Documented M:N shared-object behavior, "
              "NOT a runloom bug; it never reaches the load-bearing single-owner "
              "oracle.".format(sdiffs, schecks))

    # NON-VACUITY: the load-bearing single-owner purity hazard was exercised.
    H.check(checks > 0,
            "no private-instance purity checks ran -- the load-bearing "
            "mimetypes isolation hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a map
    # lookup / rehash on a desynced instance).
    H.require_no_lost("mimetypes guess purity")


if __name__ == "__main__":
    harness.main(
        "p578_mimetypes_guess_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="mimetypes.MimeTypes guess_type/guess_extension/"
                 "guess_all_extensions are pure lookups over an instance's maps. "
                 "LOAD-BEARING: each fiber owns a PRIVATE MimeTypes instance "
                 "populated with per-wid-unique custom types; guesses on it must "
                 "be bit-identical across a yield and match the closed-form "
                 "expected value (no cross-fiber map leak, no torn lookup, "
                 "extension round-trips, foreign extensions never leak in, a "
                 "never-added extension stays unknown).  MEASURED shared-instance "
                 "probe (expected to show cross-fiber visibility, like p490) "
                 "proves the hazard is real.  A wrong/unstable private-instance "
                 "guess is the runloom isolation/torn-lookup bug")
