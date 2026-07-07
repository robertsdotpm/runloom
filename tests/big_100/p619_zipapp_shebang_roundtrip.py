"""big_100 / 619 -- zipapp archive shebang + content round-trip PURITY under M:N.

zipapp.create_archive() turns a source (a directory OR an existing archive
file-like object) into an application archive, optionally prepending a shebang
line "#!<interpreter>\n" via _write_file_prefix(), and zipapp.get_interpreter()
reads that shebang back.  Neither is documented as stateful: create_archive over
a FIXED source with a FIXED interpreter is a PURE function of its arguments, and
get_interpreter() is a pure read of a byte stream.  Every buffer it touches is
passed in by the caller (the source/target file-like objects) -- there is no
process-global scratch buffer that a sibling call could scribble on.

WHERE M:N BREAKS IT (the gap this program probes).  runloom runs tens of
thousands of goroutines across >1 hubs with the GIL off.  If zipapp / zipfile /
shutil.copyfileobj / pathlib held ANY shared mutable state (a module-level
scratch bytearray, a cached ZipInfo, a reused compression object, an offset
counter) that was not fiber/thread isolated, then two fibers concurrently
producing archives -- one with interpreter "/a/pyA", one with "/b/pyB" -- could
have their outputs cross-contaminate: fiber A's archive could come back carrying
fiber B's shebang, a torn zip body, or a byte that belongs to neither.  Each
fiber owns its OWN interpreter string and its OWN BytesIO source/target objects,
so a CORRECT runtime yields bit-identical, closed-form-exact output every time;
a cross-fiber leak of any shared internal state shows up as a byte mismatch.

WHICH ORACLE IS LOAD-BEARING, AND WHY (all single-owner / closed-form):

  * ARM A -- COPY-PATH CLOSED FORM (in-memory, bit-exact).  When the source is a
    file-like object, create_archive routes through _copy_archive, which writes
    the shebang then copies the source bytes VERBATIM (no zip re-encoding, so no
    offset adjustment).  Therefore, for a fixed base archive body B (a valid zip
    with NO shebang, computed once in setup) and a fiber-unique interpreter
    string I, the output is EXACTLY:  b'#!' + I.encode(fsenc) + b'\n' + B.
    This is a fully INDEPENDENT closed form (not create_archive compared against
    itself): a single wrong byte means an internal buffer leaked across fibers.
    The source and target are fiber-local BytesIO objects; B is immutable shared
    read-only bytes.  get_interpreter() on the produced bytes must return I.

  * ARM B -- DIRECTORY-BUILD PURITY + CONTENT ROUND-TRIP (shared read-only source
    dir).  The flagship zipapp path: build a real .pyz from a directory via the
    ZipFile-writing loop.  The zip body's central-directory offsets DO include
    the shebang length, so this arm does NOT use the copy-path closed form;
    instead it asserts (a) PURITY across a yield -- two builds with the same
    interpreter, straddling a runloom.yield_now() so a sibling reliably
    interleaves, are BIT-IDENTICAL; (b) get_interpreter() round-trips I; and
    (c) CONTENT round-trip -- the produced archive is a valid zip whose entry
    names are exactly the known UNIVERSE and whose __main__.py / data bytes equal
    the known source bytes.  The source directory is created ONCE in setup and
    only ever READ (pathlib.rglob + open); the per-fiber BytesIO target and
    interpreter are single-owner.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-copy or
    mid-zip-build (parked inside shutil.copyfileobj / ZipFile.write and never
    rewoken) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0),
    counted race-free in a per-wid slot (one writer per slot).

A FAIL here means a real runtime fault: a byte that does not match the closed
form (ARM A), a non-deterministic archive across a yield (ARM B purity), an
out-of-universe zip entry / wrong content (ARM B round-trip), a wrong shebang, or
a SIGSEGV inside the zip/copy machinery.  There is NO shared-mutable oracle here
-- every buffer under test is owned by exactly one fiber, so a failure cannot be
"documented shared-object semantics".

Resource note: ARM B's directory build opens the shared source files per call
(transient fd churn, no leak), so max_funcs is capped to keep the forever-loop's
--funcs 1000000 from over-committing fds under load.

Stresses: zipapp.create_archive copy-path (_copy_archive / _write_file_prefix /
shutil.copyfileobj) and directory-build path (pathlib.rglob + zipfile.ZipFile
write loop), zipapp.get_interpreter shebang parse, all under M:N with fiber-local
BytesIO buffers -- probing for a shared internal buffer leaking across fibers.
"""
import io
import os
import sys
import zipapp
import zipfile

import harness
import runloom

# Filesystem encoding zipapp uses for the shebang (utf-8 on Linux).  The closed
# form in ARM A encodes the interpreter with exactly this codec, matching
# zipapp.shebang_encoding.
FSENC = zipapp.shebang_encoding

# Known source-file contents.  ARM B asserts the produced archive round-trips
# these EXACTLY and that the entry-name set is exactly this UNIVERSE.
MAIN_BYTES = b"import sys\nsys.stdout.write('big100-zipapp\\n')\n"
DATA_BYTES = b"payload:big100:zipapp:0123456789abcdef\n"
UNIVERSE = frozenset(("__main__.py", "data.txt"))

# Directory builds per op are capped: each build opens the shared source files
# (transient fds).  Cap the forever-loop's --funcs so fd churn stays bounded.
MAX_FUNCS = 2000


def fiber_interp(wid, idx):
    """A fiber-unique interpreter string.  Distinct per (wid, idx) so a leak of a
    sibling's shebang into this fiber's archive is immediately visible."""
    return "/opt/big100/w{0}/i{1}/python3.14t".format(wid, idx)


def base_body(source_dir):
    """The canonical zip body with NO shebang, built from the shared source dir.
    Computed ONCE in setup; ARM A's closed form is b'#!'+I+b'\\n'+this."""
    buf = io.BytesIO()
    zipapp.create_archive(source_dir, target=buf, interpreter=None)
    return buf.getvalue()


# ---- ARM A: copy-path closed form (pure in-memory, bit-exact) -------------
def check_copy_path(H, wid, idx, state):
    """create_archive(file-like source, interpreter=I) must emit EXACTLY
    b'#!'+I+b'\\n'+BASE_BODY -- the copy path prepends the shebang and copies the
    source verbatim.  Fiber-local BytesIO source/target; closed-form expected is
    independent of create_archive.  A byte mismatch = a cross-fiber internal
    buffer leak."""
    interp = fiber_interp(wid, idx)
    base = state["base_body"]              # immutable shared read-only bytes
    expected = b"#!" + interp.encode(FSENC) + b"\n" + base

    src = io.BytesIO(base)                 # fiber-local source
    tgt = io.BytesIO()                     # fiber-local target
    zipapp.create_archive(src, target=tgt, interpreter=interp)

    # YIELD at the hazard boundary: siblings run their own create_archive between
    # our write and our verify, so a shared internal buffer would leak here.
    runloom.yield_now()

    out = tgt.getvalue()
    if out != expected:
        # Localize the first divergence for the report.
        n = min(len(out), len(expected))
        pos = next((i for i in range(n) if out[i] != expected[i]), n)
        H.fail("zipapp copy-path bytes DIVERGED from closed form at offset {0} "
               "(len got={1} want={2}) for interp {3!r} wid {4} -- create_archive "
               "leaked internal state across fibers (a sibling's bytes reached "
               "this fiber's target)".format(pos, len(out), len(expected),
                                              interp, wid))
        return False

    # Round-trip the shebang back out of the produced bytes.
    got_interp = zipapp.get_interpreter(io.BytesIO(out))
    if got_interp != interp:
        H.fail("zipapp.get_interpreter round-trip WRONG: got {0!r} expected {1!r} "
               "(wid {2}) -- shebang parse returned another fiber's interpreter "
               "or a torn line".format(got_interp, interp, wid))
        return False
    return True


# ---- ARM B: directory-build purity + content round-trip -------------------
def build_dir(source_dir, interp):
    """Build a .pyz from the shared source directory into a fiber-local BytesIO."""
    buf = io.BytesIO()
    zipapp.create_archive(source_dir, target=buf, interpreter=interp)
    return buf.getvalue()


def check_dir_build(H, wid, idx, state):
    """The flagship path.  PURITY: two builds with the same interpreter, straddling
    a yield, must be BIT-IDENTICAL.  CONTENT: the archive is a valid zip whose
    entry names are exactly UNIVERSE and whose bytes round-trip the known source.
    All buffers fiber-local; source dir read-only shared."""
    source_dir = state["source_dir"]
    interp = fiber_interp(wid, idx)

    out1 = build_dir(source_dir, interp)

    # YIELD: a sibling builds its own archive from the same shared dir here.
    runloom.yield_now()

    out2 = build_dir(source_dir, interp)

    # PURITY across the yield: a pure function of fixed args must be deterministic.
    if out1 != out2:
        n = min(len(out1), len(out2))
        pos = next((i for i in range(n) if out1[i] != out2[i]), n)
        H.fail("zipapp directory build NON-DETERMINISTIC across a yield at offset "
               "{0} (len1={1} len2={2}) for interp {3!r} wid {4} -- two identical "
               "builds diverged, a shared internal buffer leaked across fibers"
               .format(pos, len(out1), len(out2), interp, wid))
        return False

    # get_interpreter round-trip on the directory-built archive.
    got_interp = zipapp.get_interpreter(io.BytesIO(out1))
    if got_interp != interp:
        H.fail("zipapp.get_interpreter (dir build) WRONG: got {0!r} expected {1!r} "
               "(wid {2})".format(got_interp, interp, wid))
        return False

    # CONTENT round-trip: valid zip, exact universe of names, exact bytes.
    try:
        z = zipfile.ZipFile(io.BytesIO(out1))
    except zipfile.BadZipFile as exc:
        H.fail("zipapp directory build produced a CORRUPT zip for interp {0!r} "
               "wid {1}: {2} -- torn zip body under concurrent build".format(
                   interp, wid, exc))
        return False
    names = set(z.namelist())
    if names != UNIVERSE:
        H.fail("zipapp archive entry names {0!r} != universe {1!r} (wid {2}) -- an "
               "out-of-universe / dropped entry, a torn central directory under "
               "concurrent build".format(sorted(names), sorted(UNIVERSE), wid))
        return False
    if z.read("__main__.py") != MAIN_BYTES:
        H.fail("zipapp archive __main__.py bytes WRONG (wid {0}) -- content did "
               "not round-trip; a sibling's data leaked into this archive".format(
                   wid))
        return False
    if z.read("data.txt") != DATA_BYTES:
        H.fail("zipapp archive data.txt bytes WRONG (wid {0}) -- content did not "
               "round-trip".format(wid))
        return False
    return True


# Sustained checks per worker: the leak hazard only shows under sustained churn --
# many fibers building/copying archives while parked across a yield so the
# scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH single-owner arms per iteration: the copy-path closed
    form (ARM A) and the directory-build purity + round-trip (ARM B).  The two
    share no mutable state (fiber-local BytesIO + fiber-unique interpreter; the
    source dir/base body are read-only), so co-running them just keeps the hubs
    busy with mixed zipapp churn."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not check_copy_path(H, wid, idx, state):
                return
            if not check_dir_build(H, wid, idx, state):
                return
            state["checks"][wid] += 1      # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Shared READ-ONLY source directory, created ONCE.  Every fiber only reads it
    # (pathlib.rglob + open inside create_archive); nothing writes it after setup.
    d = H.make_tmpdir(prefix="p619_zipapp_")
    with open(os.path.join(d, "__main__.py"), "wb") as f:
        f.write(MAIN_BYTES)
    with open(os.path.join(d, "data.txt"), "wb") as f:
        f.write(DATA_BYTES)

    H.state = {
        "source_dir": d,
        "base_body": base_body(d),         # immutable canonical no-shebang body
        "checks": [0] * H.funcs,           # ONE slot per worker (race-free)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("zipapp single-owner round-trips: {0} (copy-path closed form + "
          "directory-build purity + content round-trip, all fail-fast); "
          "ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing arms actually ran.
    H.check(checks > 0,
            "no zipapp round-trip checks ran -- the copy/build hazard was never "
            "exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside the copy/zip machinery.
    H.require_no_lost("zipapp shebang/content round-trip")


if __name__ == "__main__":
    harness.main(
        "p619_zipapp_shebang_roundtrip", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=MAX_FUNCS,
        describe="zipapp.create_archive is a PURE function of its args (no shared "
                 "scratch buffer); under M:N, if zipapp/zipfile/shutil held any "
                 "unshared-mutable internal state, concurrent fibers building "
                 "archives with distinct interpreters could cross-contaminate. "
                 "ARM A: copy-path output must equal the closed form "
                 "b'#!'+interp+b'\\n'+base_body bit-for-bit; ARM B: directory "
                 "builds are deterministic across a yield and round-trip the exact "
                 "source content + shebang.  All buffers fiber-local -- a byte "
                 "mismatch is a cross-fiber internal-buffer leak")
