"""big_100 / 435 -- mmap.resize() mremap(MREMAP_MAYMOVE) vs a live memoryview export.

The subject is ``mmap.mmap`` (Modules/mmapmodule.c) and the field pair the resize
path rewrites IN PLACE under a sibling's live export.  mmap.resize() is called
NOWHERE in the suite -- p322/p327 only ever close / munmap / page-fault the
mapping, NEVER resize it -- so nothing here has driven the one mmap operation that
RELOCATES the backing pages and rewrites the object's base pointer.

The C state under attack (mmap_object, Modules/mmapmodule.c):

    typedef struct {
        ...
        char *      data;        # the mapping's BASE pointer
        Py_ssize_t  size;        # current mapped length
        Py_ssize_t  exports;     # count of live Py_buffer exports (getbuffer)
    } mmap_object;

  * memory_getbuf()  does  self->exports++          (a memoryview over the mmap)
  * memory_releasebuf() does self->exports--        (memoryview.release / GC)
  * mmap_resize_method() is a CHECK-then-mremap:

        if (self->exports > 0) {                     # the EXPORT GATE
            PyErr_SetString(PyExc_BufferError,
                "mmap can't resize with extant buffers exported.");
            return NULL;                             # refuse
        }
        ...
        newmap = mremap(self->data, self->size, new_size, MREMAP_MAYMOVE);
        self->data = newmap;                         # BASE POINTER REWRITTEN
        self->size = new_size;                       # and the length

MREMAP_MAYMOVE may RELOCATE the mapping to a brand-new virtual address; CPython
then stores that new base into self->data in place.  A live ``memoryview(mm)``
captured the OLD self->data into its Py_buffer.buf at getbuffer time.  So the
racing op pair is:

  * the export INCREMENT-on-getbuffer / DECREMENT-on-release of the view
    (self->exports++ / self->exports--), versus
  * the resize's CHECK-then-mremap, gated on self->exports, which on success
    frees/moves the old page range and rewrites self->data.

Two mutually-exclusive corruption modes, BOTH made falsifiable:

  * TORN-LOW / UAF: an export increment is lost or a release decrement is double-
    applied, self->exports reads 0 while the view is still live, the gate lets the
    mremap through, mremap MOVES the mapping and unmaps the old pages -- and the
    live view's captured Py_buffer.buf now dangles at a freed/relocated address.
    A read/write through it is a SIGBUS/SIGSEGV, or silent WRONG bytes (the old
    address was reused).  We catch it two ways: (a) while the view is live EVERY
    sibling resize MUST raise BufferError (a resize that SUCCEEDS under a live
    export is the gate torn open); (b) the holder stamps a known wid-byte through
    the writable view, parks, and on resume EVERY byte of the view must still read
    that stamp -- a slipped mremap (or a UAF read of relocated memory) changes
    them.

  * TORN-HIGH / LEAK: an export increment is double-applied or a release decrement
    is lost, self->exports never returns to 0, and the mapping is PERMANENTLY
    un-resizable.  We catch it directly: after the ONLY view is released, the very
    next mm.resize(size*2) MUST succeed (exports provably back to 0); a BufferError
    there is a leaked export.

CONSERVATION across the relocate: mremap with MREMAP_MAYMOVE must preserve the
mapped contents at the new address.  After the (now legal) resize, bytes [0, size)
MUST still read back the pre-resize stamped pattern -- a resize that loses or
corrupts the content (or a mremap that returned the wrong base) shows up as a
mismatch even though no view was live.

CONTROL ARM (single-owner, race-free -- the falsifier).  A second identical map is
built, a lone view taken, stamped, released, and resized by ONE fiber with NO
sibling touching it.  A single owner cannot race its own exports counter, so if the
CONTROL's post-self-release resize is refused, the lost decrement is provably in
mmap's OWN getbuffer/releasebuf bookkeeping, not M:N contention; if only the
CONTENDED arm leaks, it is the cross-hub race.  This isolates "mmap's export
counter is buggy" from "M:N dropped the increment/decrement".

CLOSED-WORLD oracle (per round, fail-fast + post).  The view's byte universe is a
finite per-(round, wid) stamp: every view byte == STAMP(wid) (a single recognizable
value); a torn/freed/relocated read yields a byte != STAMP(wid) -- out of universe.
Per-slot single-writer tallies count: rounds, refusals-while-live, resize-successes-
after-release, content-preserved confirmations, control successes.  post():
refusals>0 (the gate was real), resize_ok == refusals (CONSERVATION: every gated
round unpinned -- no leaked export), content preserved on every round, control
exercised, no lost worker.

NOTE on the mapping kind.  The TARGET invariant names an "anonymous" mmap, but on
this Linux build a MAP_PRIVATE anonymous mmap.resize() ftruncates the sentinel fd
-1 and corrupts the object (OSError Bad file descriptor), and a MAP_SHARED
anonymous mapping refuses to expand at all ("can't expand a shared anonymous
mapping on Linux").  Only a FILE-BACKED mmap exercises the real
mremap(MREMAP_MAYMOVE) base-pointer rewrite cleanly.  Each round therefore builds a
FRESH file-backed mmap over a fresh private tempfile (still fresh-per-round, single
live view) -- the exports/check-then-mremap race is identical; only the backing
store differs.

Round-robin the resize-trigger CASE by worker id in the first ops (grow-by-page /
grow-by-many-pages / shrink-then-regrow all reach mremap) so coverage holds under
the timeout -- the p125/p126 flaky-random-coverage fix -- then random.

Stresses: mmap.resize() export-gate (self->exports CHECK) vs mremap(MREMAP_MAYMOVE)
rewriting self->data/self->size, memoryview getbuffer/release increment/decrement of
self->exports, torn export count -> UAF/SIGBUS through the view's captured buf or a
permanently un-resizable leak, mremap content preservation across relocation,
cross-hub view-hold-vs-resize interleave.

Good TSan / controlled-replay target: the self->exports++ in memory_getbuf and the
self->exports-- in memory_releasebuf race the resize's read of self->exports -- a
TSan report on mmap_object->exports localizes the torn count before the stamp
assert or the BufferError gate even fires.
"""
import os
import sys
import tempfile
import mmap

# mmap.resize() is backed by mremap(MREMAP_MAYMOVE), which CPython only compiles in
# under HAVE_MREMAP (Linux).  On macOS / BSD every mm.resize() raises
# SystemError "mmap: resizing not available--no mremap()", so refuse_live /
# resize_ok / control_ok all stay 0 and post() reports a FALSE invariant violation
# for a platform that simply lacks the operation.  Skip cleanly off-Linux; this
# guard is INERT on linux (the program imports + runs exactly as before).
if sys.platform != "linux":
    print("SKIP: mmap.resize() needs mremap(MREMAP_MAYMOVE) -- Linux-only")
    sys.exit(0)

import harness
import runloom

# Page / allocation granularity on this box.  Every resize is granularity-multiple so
# mremap operates on whole pages (the kernel may relocate the whole range).  4096 on
# x86 Linux, 16384 on arm64 -- read it rather than hardcode (mmap.ALLOCATIONGRANULARITY
# is the platform page granularity).
PAGE = mmap.ALLOCATIONGRANULARITY

# Initial mapped length in BYTES (one page).  The view covers the whole map; the
# holder stamps every byte and reads every byte back across the park, so a relocate
# / UAF that touches any covered page is caught.  One page keeps each round cheap so
# many rounds complete under the timeout, while still being a real mremap.
INIT_BYTES = PAGE

# How many pages the backing file is pre-sized to (must be >= the largest resize so
# the file is big enough to back the grown mapping).  Grow targets stay within this.
FILE_PAGES = 8
FILE_BYTES = FILE_PAGES * PAGE

# Slots for race-free per-worker tallies (single writer per slot, summed in post()).
SLOTS = 1024

# The resize-trigger CASES.  Every one reaches mmap_resize_method -> ftruncate +
# mremap(MREMAP_MAYMOVE), the path that rewrites self->data/self->size.  post()
# requires each was hit, so the worker round-robins them by id in the first ops (NOT
# random -- pure random reliably misses a case at low op-count under load: the
# p125/p126/p172 flaky-coverage bug the suite already had to fix).
CASE_GROW_ONE = 0     # resize(size + PAGE)        -- grow by one page
CASE_GROW_MANY = 1    # resize(size * 2)           -- grow by many pages (more likely to MOVE)
CASE_SHRINK_REGROW = 2  # resize(PAGE) then resize(2*PAGE) -- shrink then regrow
NCASES = 3


def stamp_byte(wid):
    """The single recognizable byte stamped through the writable view this round.
    Every view byte must read back exactly this; a torn/freed/relocated read yields
    a different byte -- out of the (one-element) universe.  Kept off 0x00 so an
    accidental fresh/zeroed page does not coincidentally satisfy it."""
    return (0x40 + (wid & 0x3F)) & 0xFF


def fresh_map(d, rnd):
    """Build a FRESH file-backed mmap over a fresh private tempfile pre-sized to
    FILE_BYTES, mapping the first INIT_BYTES.  Returns (mm, fd, path).  The caller
    closes mm / fd and unlinks path.  File-backed so mmap.resize() exercises the
    real mremap(MREMAP_MAYMOVE) base-pointer rewrite (see the module note)."""
    path = os.path.join(d, "m{0}_{1}".format(os.getpid(), rnd))
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
    os.ftruncate(fd, FILE_BYTES)
    mm = mmap.mmap(fd, INIT_BYTES)
    return mm, fd, path


def drop_map(mm, fd, path):
    try:
        mm.close()
    except Exception:
        pass
    try:
        os.close(fd)
    except Exception:
        pass
    try:
        os.unlink(path)
    except Exception:
        pass


def resize_target(mm, case):
    """The new byte length for this round's resize CASE (page-multiple).  For the
    shrink-then-regrow case the caller drives two resizes; this returns the FINAL
    grown length so the post-resize content check covers the same span."""
    if case == CASE_GROW_ONE:
        return mm.size() + PAGE
    elif case == CASE_GROW_MANY:
        return mm.size() * 2
    else:  # CASE_SHRINK_REGROW
        return 2 * PAGE


def do_resize(mm, case):
    """Apply the round's resize CASE.  Returns True if the resize SUCCEEDED
    (BufferError NOT raised), False if it raised BufferError (the export gate
    refused -- a view was live).  Every path reaches mremap(MREMAP_MAYMOVE).  The
    shrink-then-regrow case does two resizes; a BufferError on EITHER means the gate
    refused, so we report False."""
    try:
        if case == CASE_GROW_ONE:
            mm.resize(mm.size() + PAGE)
        elif case == CASE_GROW_MANY:
            mm.resize(mm.size() * 2)
        else:  # CASE_SHRINK_REGROW
            mm.resize(PAGE)            # shrink first (mremap to a smaller range)
            mm.resize(2 * PAGE)        # then regrow (mremap, may MOVE)
        return True
    except BufferError:
        return False


def check_view_stamp(H, mv, wid, where):
    """Every byte of the live view MUST read back STAMP(wid).  A byte != STAMP is a
    TORN/FREED/RELOCATED read: the export count was torn to 0, the mremap moved the
    mapping and freed the old pages, and the view's captured buf now reads relocated
    or unmapped memory.  Returns False on the first violation."""
    s = stamp_byte(wid)
    for i in range(len(mv)):
        b = mv[i]
        if b != s:
            H.fail("view byte {0} == {1!r} != stamp {2!r} ({3}) -- a TORN/UAF read: "
                   "the export count was torn to 0, mmap.resize()'s mremap moved the "
                   "mapping and freed/relocated the pages, and the live view read "
                   "through its stale captured Py_buffer.buf".format(
                       i, b, s, where))
            return False
    return True


def check_map_content(H, mm, wid, span, where):
    """After the (legal) resize, bytes [0, span) of the (possibly relocated) mapping
    MUST still read the pre-resize stamp.  mremap(MREMAP_MAYMOVE) must preserve the
    mapped contents at the new base; a mismatch is a lost/corrupted relocate (the
    CONSERVATION-across-mremap law) even though no view was live.  Returns False on
    the first violation."""
    s = stamp_byte(wid)
    for i in range(span):
        b = mm[i]
        if b != s:
            H.fail("after resize, mapping byte {0} == {1!r} != pre-resize stamp "
                   "{2!r} ({3}) -- mremap(MREMAP_MAYMOVE) did not preserve the "
                   "content across the relocate (wrong base returned / pages not "
                   "carried over)".format(i, b, s, where))
            return False
    return True


def run_round_impl(H, wid, rnd, rng, case, slot, state):
    """One contended round.  Build a FRESH file-backed mmap, take a SINGLE writable
    memoryview over it, stamp every byte through the view, and spawn a sibling
    RESIZER on another hub that must be REFUSED while the view is live.  The sibling
    is synchronized into the holder's park window via a gate WaitGroup so its resize
    provably lands while the view is exported.

    Sequence (the export-count drama -- the handshake makes the live resize PROVABLY
    overlap a live export, otherwise the holder could release the view before the
    sibling's check-then-mremap read self->exports, and the gate refusal would be a
    benign reorder rather than a real overlap):
      1. mv = memoryview(mm)             -> self->exports 1
      2. stamp every byte through mv (STAMP(wid))
      3. trip gate; sibling tries resize while the view is live -> MUST be refused
         (BufferError); meanwhile we read+verify every view byte across a yield park
         (the UAF window if the gate tore open and mremap moved the pages)
      4. WAIT on done_live: the holder does NOT release until the sibling's live
         resize attempt has fully returned -- so that attempt provably saw exports>0
      5. mv.release()                    -> self->exports 0
      6. trip cont; the sibling's post-release resize MUST succeed exactly once
         (mremap), and the relocated mapping's bytes [0, INIT_BYTES) still read the
         stamp
    """
    tally = state["tally"]
    d = state["dir"]
    mm, fd, path = fresh_map(d, rnd)
    s = stamp_byte(wid)
    try:
        mv = memoryview(mm)               # self->exports -> 1
        # The view must be writable (file-backed RDWR mmap -> read/write buffer); a
        # read-only view would not exercise the write-through stamp.
        if mv.readonly:
            H.fail("memoryview(mm) over a read/write file-backed mmap is readonly -- "
                   "cannot stamp through it; the export/resize race is untestable")
            mv.release()
            return
        # Stamp every byte through the WRITABLE view (writes into the mapped pages).
        for i in range(len(mv)):
            mv[i] = s

        # gate: the holder trips it the instant before it parks (mid-verify) so the
        # sibling's resize-while-live attempt provably lands inside the park window.
        gate = runloom.WaitGroup()
        gate.add(1)
        # done_live: the SIBLING trips this once its live-resize attempt has fully
        # returned.  The holder WAITS on it before releasing the view, so the live
        # attempt provably observed self->exports > 0 (no benign release-before-check
        # reorder can masquerade as a gate refusal).
        done_live = runloom.WaitGroup()
        done_live.add(1)
        # cont: the holder trips this AFTER releasing the view so the sibling does
        # its post-release resize at a defined point (not before release).
        cont = runloom.WaitGroup()
        cont.add(1)
        wg = runloom.WaitGroup()
        wg.add(1)

        # Sibling result: live-refusal (must be a BufferError refusal -> False) and
        # post-release resize (must SUCCEED -> True).
        res = {"live_resized": None, "after_release_resized": None}

        def resizer():
            try:
                # Wait until the holder is parked mid-verify with the view LIVE.
                gate.wait()
                # Attempt the round's resize CASE while the view is exported.
                # self->exports == 1 -> MUST be refused (BufferError -> False).
                try:
                    res["live_resized"] = do_resize(mm, case)
                finally:
                    done_live.done()       # holder may release only AFTER this
                # Wait until the holder has released the view, then resize for real.
                cont.wait()
                res["after_release_resized"] = do_resize(mm, case)
            finally:
                wg.done()

        H.fiber(resizer)

        # Verify the stamp; trip the gate mid-verify and park so the sibling's
        # resize-while-live attempt overlaps the live view read (the UAF window if
        # the export count tore to 0 and mremap moved/freed the pages).
        if not check_view_stamp(H, mv, wid, "pre-park, view live"):
            gate.done()                   # don't strand the sibling
            done_live.wait()              # let the live attempt finish (view still live)
            cont.done()
            wg.wait()
            mv.release()
            return
        gate.done()                       # sibling may now attempt the live resize
        runloom.yield_now()               # park with the view LIVE -- resize lands here
        # Re-verify after the park: if the live resize had wrongly succeeded (gate
        # torn open), mremap moved the mapping and these reads are a UAF.
        if not check_view_stamp(H, mv, wid, "post-park, view live"):
            done_live.wait()
            cont.done()
            wg.wait()
            mv.release()
            return

        # Do NOT release the view until the sibling's live-resize attempt has fully
        # returned -- so that attempt provably observed self->exports > 0 (the gate
        # refusal is then a REAL overlap, never a release-before-check reorder).
        done_live.wait()
        # Release the ONLY view.  self->exports 1 -> 0.  The mapping is now unpinned.
        mv.release()
        cont.done()                       # sibling may now do its post-release resize

        # Join the sibling so its resize results are settled and mm is quiescent.
        wg.wait()
        if H.failed:
            return

        # ---- the export gate must have refused while live --------------------------
        live = res["live_resized"]
        after = res["after_release_resized"]
        if live is None or after is None:
            # Sibling didn't run both probes (shouldn't happen -- it always reaches
            # wg.done()); treat as not-exercised, no tally, no failure.
            return
        if live:
            H.fail("sibling mmap.resize() SUCCEEDED while a memoryview was LIVE "
                   "(self->exports should be 1) -- the export GATE was torn open: "
                   "the check-then-mremap read a stale exports==0, mremap moved/"
                   "freed the mapping under the live view's captured buf pointer "
                   "(UAF/SIGBUS risk)")
            return
        tally["refuse_live"][slot] += 1

        # ---- export count returned to 0: the post-release resize MUST have succeeded
        if not after:
            H.fail("after releasing the ONLY memoryview (self->exports must be 0) the "
                   "sibling mmap.resize() is STILL refused with BufferError -- a "
                   "release decrement was LOST: memoryview release did not return "
                   "self->exports to 0, the mapping is permanently un-resizable "
                   "(export LEAK)")
            return
        tally["resize_ok"][slot] += 1

        # ---- CONSERVATION across the mremap relocate -------------------------------
        # The mapping is now grown (>= 2*PAGE in every case).  Bytes [0, INIT_BYTES)
        # must still read the pre-resize stamp -- mremap(MREMAP_MAYMOVE) preserves
        # contents at the new base.
        if mm.size() < INIT_BYTES:
            H.fail("after resize the mapping shrank below the stamped span "
                   "(size={0} < {1}) -- the resize CASE did not end grown".format(
                       mm.size(), INIT_BYTES))
            return
        if not check_map_content(H, mm, wid, INIT_BYTES,
                                 "post-resize, relocated mapping"):
            return
        tally["content_ok"][slot] += 1
    finally:
        drop_map(mm, fd, path)


def control_round(H, wid, rnd, rng, case, slot, state):
    """SINGLE-OWNER CONTROL ARM.  An identical fresh map + lone view, stamped, read
    back, released, and resized by THIS fiber with NO sibling touching the mapping.
    A single owner cannot race its own exports counter, so the getbuffer/release
    increment/decrement bookkeeping is exercised race-free.  While the view is live a
    resize MUST be refused; after self-release the resize MUST succeed (count back to
    0) and the relocated content MUST be preserved.  If THIS leaks an export, the
    lost decrement is in mmap's machinery itself, NOT M:N contention -- the falsifier
    that distinguishes a primitive bug from a race."""
    tally = state["tally"]
    d = state["dir"]
    mm, fd, path = fresh_map(d, rnd)
    s = stamp_byte(wid)
    try:
        mv = memoryview(mm)
        if mv.readonly:
            H.fail("CONTROL: memoryview over RDWR file-backed mmap is readonly")
            mv.release()
            return
        for i in range(len(mv)):
            mv[i] = s
        if not check_view_stamp(H, mv, wid, "control, view live"):
            mv.release()
            return
        # While the lone view is live, a resize MUST be refused even with no sibling.
        if do_resize(mm, case):
            H.fail("CONTROL: mmap.resize() succeeded while a memoryview was live "
                   "with NO sibling -- getbuffer did not pin the mapping "
                   "(self->exports never reached 1); an mmap export-counter bug, "
                   "not contention")
            mv.release()
            return
        # Release the lone view; self->exports must be 0; resize must now succeed.
        mv.release()
        if not do_resize(mm, case):
            H.fail("CONTROL: mmap.resize() STILL refused after releasing the lone "
                   "view (no sibling) -- a release decrement was LOST in mmap's own "
                   "machinery; the single-owner map leaked an export, so the loss is "
                   "NOT contention")
            return
        # Content preserved across the relocate in the single-owner arm too.
        if mm.size() < INIT_BYTES:
            H.fail("CONTROL: mapping shrank below the stamped span after resize "
                   "(size={0} < {1})".format(mm.size(), INIT_BYTES))
            return
        if not check_map_content(H, mm, wid, INIT_BYTES, "control, post-resize"):
            return
        tally["control_ok"][slot] += 1
    finally:
        drop_map(mm, fd, path)


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    # `rnd` is an explicit per-worker round counter (H.round_range() yields None) and
    # also names each round's fresh tempfile so successive rounds don't collide.
    rnd = (wid * 0x9E3779B1) & 0xFFFFFF
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the resize-trigger CASE by worker id in the first ops so all
        # three mremap-reaching paths are covered even under the timeout (the
        # p125/p126 flaky-random-coverage fix); random after.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        # Most rounds run the CONTENDED arm (the race probe); every few rounds also
        # run the single-owner CONTROL arm (the falsifier).  Round-robin which rounds
        # get a control pass by (wid + i) so coverage is deterministic.
        do_control = ((wid + i) % 3 == 0)
        i += 1
        rnd = (rnd + 1) & 0xFFFFFF

        run_round_impl(H, wid, rnd, rng, case, slot, state)
        if H.failed:
            return
        if do_control:
            rnd = (rnd + 1) & 0xFFFFFF
            control_round(H, wid, rnd, rng, case, slot, state)
            if H.failed:
                return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All per-slot tallies allocated here, inside the root (single writer per slot ->
    # race-free; summed in post()).  No shared mmap lives at module scope; each round
    # builds its own fresh file-backed mapping over a fresh tempfile in this dir.
    d = H.make_tmpdir(prefix="big100_p435_")
    H.state = {
        "dir": d,
        "tally": {
            "refuse_live": [0] * SLOTS,    # resize refused while the view was live
            "resize_ok": [0] * SLOTS,      # resize succeeded after releasing the view
            "content_ok": [0] * SLOTS,     # content preserved across the mremap
            "control_ok": [0] * SLOTS,     # single-owner control rounds passed
        },
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    tally = H.state["tally"]
    refuse_live = sum(tally["refuse_live"])
    resize_ok = sum(tally["resize_ok"])
    content_ok = sum(tally["content_ok"])
    control_ok = sum(tally["control_ok"])
    H.log("refuse_live={0} resize_ok_after_release={1} content_preserved={2} "
          "control_ok={3} ops={4}".format(
              refuse_live, resize_ok, content_ok, control_ok, H.total_ops()))

    H.check(H.total_ops() > 0,
            "no rounds completed -- the resize-vs-live-view race window was never "
            "exercised")

    # The export GATE was real: while the view was live the mmap.resize() was
    # actually refused (so self->exports was genuinely > 0 and the test wasn't
    # vacuous).
    H.check(refuse_live > 0,
            "no resize was ever refused while a memoryview was live -- the export "
            "gate (self->exports check) was never exercised (the contended arm did "
            "no work)")

    # CONSERVATION: on every gated round the resize SUCCEEDED after the view was
    # released (self->exports returned to exactly 0 -- the getbuffer increment was
    # matched by the release decrement; no leak left the mapping pinned).
    H.check(resize_ok == refuse_live,
            "export-count conservation broken: {0} rounds refused the resize while "
            "the view was live but only {1} rounds could resize after releasing it "
            "-- {2} round(s) leaked an export (a release decrement was lost; the "
            "mapping stayed permanently un-resizable)".format(
                refuse_live, resize_ok, refuse_live - resize_ok))

    # Every successful resize preserved the pre-resize stamp across the mremap
    # relocate (the CONSERVATION-across-mremap law).  Every resized round must have
    # confirmed content.
    H.check(content_ok == resize_ok,
            "mremap content-preservation broken: {0} resizes succeeded but only {1} "
            "preserved the pre-resize stamped content across the relocate -- {2} "
            "round(s) lost/corrupted the mapping content (mremap returned a wrong "
            "base or did not carry the pages over)".format(
                resize_ok, content_ok, resize_ok - content_ok))

    # The single-owner CONTROL arm ran and never leaked (a leak HERE would be an
    # mmap export-counter bug, not contention).
    H.check(control_ok > 0,
            "the single-owner control arm never completed a round -- the falsifier "
            "that distinguishes an mmap export-counter bug from M:N contention was "
            "never exercised")

    H.require_no_lost()


if __name__ == "__main__":
    harness.main(
        "p435_mmap_resize_vs_live_view", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a fiber takes a writable memoryview over a fresh file-backed mmap, "
                 "stamps every byte and parks; a sibling on another hub calls "
                 "mm.resize() (mremap(MREMAP_MAYMOVE), which rewrites self->data/"
                 "self->size).  The self->exports gate MUST refuse the resize while "
                 "the view is live (a slipped mremap is a UAF on the view's captured "
                 "buf); after release the resize MUST succeed exactly once (no "
                 "leaked export) and the relocated mapping still reads the stamp.  "
                 "Closed-world: every view byte == STAMP(wid) across the park "
                 "(torn/UAF reads out-of-universe), resize refused while live, resize "
                 "+ content-preserved after release; single-owner control arm "
                 "falsifies mmap-counter loss vs contention")
