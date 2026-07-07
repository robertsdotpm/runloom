"""big_100 / 586 -- posix single-owner fd byte-conservation + stat identity under M:N.

The `posix` module is the raw POSIX syscall surface `os` is built on
(posix.open/read/write/pread/pwrite/fstat/memfd_create/ftruncate/close, plus
posix.stat_result).  It is PROCESS-GLOBAL -- there is no per-fiber posix state to
own -- so a naive "isolation" oracle here would be meaningless.  Following the
process-global playbook, we build the oracle instead on the SINGLE-OWNER OBJECTS
posix PRODUCES: an anonymous file descriptor from posix.memfd_create() and the
posix.stat_result objects from posix.fstat() on it.

WHY THIS PROBES AN M:N BUG.  Every fiber allocates its OWN anonymous in-memory fd
(memfd_create -- no filesystem, no shared path, no other fiber ever touches this
fd number while it is open), writes a KNOWN, wid-ENCODED payload into it via
posix.pwrite, then -- ACROSS A YIELD, during which siblings on other hubs are
frantically opening/closing their own memfds and reusing fd NUMBERS -- reads the
payload back with posix.pread and re-stats the fd.  The load-bearing laws:

  * BYTE CONSERVATION / fd ISOLATION.  posix.pread(fd, L, 0) must return EXACTLY
    the wid-encoded payload this fiber wrote (byte-identical, length L).  Because
    the payload encodes THIS fiber's wid and length, a read that returned another
    fiber's data -- an fd-number confusion where the runtime's netpoll/fd
    bookkeeping routed this fiber's pread to a sibling's fd, or a torn read under
    GIL-off fd churn -- would differ and is caught.  A single-owner fd's contents
    can NEVER legitimately change out from under one fiber; a mismatch is a real
    runtime fd-isolation bug.

  * STAT IDENTITY STABILITY.  posix.fstat(fd) before and after the yield must
    report the SAME st_ino / st_dev / st_size (the fd still names the same
    kernel object; nobody else holds it).  A changed inode/size across the yield
    means the fd was silently rebound to a different object -- a cross-fiber fd
    leak.  posix.write's return value must equal L (no short/dropped write on the
    single-owner fd).

Verified against plain threads: 8 OS threads each cycling their own memfd through
pwrite/fstat/pread/close (GIL on AND off) get 100% byte-identical round-trips and
stable stat identity -- 0 cross-thread fd bleed.  memfd/pread/pwrite on a regular
(seekable) in-memory file NEVER block, so no hub thread is ever parked in a
syscall; the fibers stay cooperative and the churn is pure fd-number allocation
pressure, which is exactly the M:N hazard we want.

ORACLES:
  * LOAD-BEARING (worker, HARD, fail-fast): single-owner memfd byte conservation
    + fstat identity stability across a yield (per fiber, per inner op).  A FAIL
    means a real runtime fd/stat corruption, never documented posix semantics.
  * NON-VACUITY (post, HARD): the round-trip arm actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- no fiber stranded mid-syscall.

Race-free counter: successful-check tally is [0]*H.funcs indexed by wid (one
writer per slot).  fd-heavy -> max_funcs caps the forever-loop's --funcs 1e6.

Stresses: posix.memfd_create fd allocation churn, posix.pwrite/pread on a
single-owner anonymous fd across hub migration + yield, posix.fstat / stat_result
construction under M:N, fd-number reuse across fibers, torn read / short write /
cross-fiber fd leak.
"""
import stat as statmod

import harness
import runloom

try:
    import posix
    HAVE_POSIX = True
except ImportError:                     # non-POSIX platform (e.g. Windows)
    HAVE_POSIX = False

# Cap concurrent fibers: each holds one anonymous fd per inner op (opened then
# closed within the op, but many fibers churn simultaneously).  fd-heavy program
# -> keep the forever loop's --funcs 1000000 bounded.
MAX_FUNCS = 2000

# Sustained per-fiber ops.  The fd-reuse hazard only manifests under SUSTAINED
# churn (many fibers simultaneously allocating/closing memfds while parked across
# their yield so a sibling reliably reuses a just-freed fd number before this
# fiber resumes its pread).  A single op per fiber barely overlaps a sibling's.
INNER_CAP = 100000

USE_MEMFD = HAVE_POSIX and hasattr(posix, "memfd_create")


def make_payload(wid):
    """A fiber-local, wid-ENCODED payload.  Length and content both depend on wid
    so a cross-fiber fd read (returning a sibling's bytes / a different length)
    differs from the expected payload and is caught.  Built ONCE per fiber and
    reused (single-owner constant)."""
    length = 256 + (wid % 384)            # 256..639 bytes; varies per wid
    base = (wid * 2654435761) & 0xFFFFFFFF
    return bytes(((base + i) & 0xFF) for i in range(length))


def open_owned_fd(wid, idx, state):
    """Allocate a SINGLE-OWNER anonymous fd for this fiber's op.

    Primary path: posix.memfd_create (anonymous, no filesystem, no shared path).
    Fallback (kernels/platforms without memfd): a fresh unique file in the shared
    tmpdir opened O_RDWR|O_CREAT|O_TRUNC -- still single-owner (unique name per
    (wid, idx)) and unlinked immediately so it never accumulates on disk."""
    if USE_MEMFD:
        return posix.memfd_create("p586_w{0}_{1}".format(wid, idx),
                                  getattr(posix, "MFD_CLOEXEC", 0))
    path = "{0}/w{1}_{2}".format(state["tmpdir"], wid, idx)
    fd = posix.open(path, posix.O_RDWR | posix.O_CREAT | posix.O_TRUNC, 0o600)
    try:
        posix.unlink(path)                # single-owner: gone from the namespace
    except OSError:
        pass
    return fd


def roundtrip_check(H, wid, idx, payload, state):
    """One single-owner memfd round-trip: write a wid-encoded payload, fstat, YIELD
    (siblings churn the fd space), then pread it back and re-fstat.  Byte
    conservation + stat identity must hold on this fiber's private fd."""
    L = len(payload)
    fd = open_owned_fd(wid, idx, state)
    try:
        posix.ftruncate(fd, 0)
        n = posix.pwrite(fd, payload, 0)
        if n != L:
            H.fail("posix.pwrite short write: wrote {0} of {1} bytes to this "
                   "fiber's OWN memfd (wid {2}) -- a dropped write on a single-"
                   "owner fd".format(n, L, wid))
            return False

        st = posix.fstat(fd)
        if st.st_size != L:
            H.fail("posix.fstat st_size={0} != {1} bytes just written to this "
                   "fiber's OWN fd (wid {2}) -- torn stat_result or lost "
                   "write".format(st.st_size, L, wid))
            return False

        # YIELD: siblings on other hubs open/close their own memfds and REUSE fd
        # numbers.  If the runtime confused fd bookkeeping across fibers, this
        # fiber's pread below would read a sibling's data or a torn buffer.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        got = posix.pread(fd, L, 0)
        if got != payload:
            # Diagnose length vs content so a cross-fiber read is obvious.
            if len(got) != L:
                H.fail("posix.pread returned {0} bytes, expected {1}, from this "
                       "fiber's OWN memfd (wid {2}) across a yield -- a short/torn "
                       "read or an fd-number confusion with a sibling".format(
                           len(got), L, wid))
            else:
                H.fail("posix.pread byte MISMATCH on this fiber's OWN memfd "
                       "(wid {0}, {1} bytes) across a yield -- the single-owner "
                       "fd's contents changed, a cross-fiber fd leak or torn "
                       "read".format(wid, L))
            return False

        st2 = posix.fstat(fd)
        if st2.st_size != L or st2.st_ino != st.st_ino or st2.st_dev != st.st_dev:
            H.fail("posix.fstat IDENTITY changed across a yield on this fiber's "
                   "OWN fd (wid {0}): (ino {1}->{2}, dev {3}->{4}, size {5}->{6}) "
                   "-- the fd was silently rebound to a different kernel object, "
                   "a cross-fiber fd leak".format(
                       wid, st.st_ino, st2.st_ino, st.st_dev, st2.st_dev,
                       st.st_size, st2.st_size))
            return False

        if not statmod.S_ISREG(st2.st_mode):
            H.fail("posix.fstat st_mode is not a regular file ({0:#o}) for this "
                   "fiber's OWN memfd (wid {1}) -- torn stat_result".format(
                       st2.st_mode, wid))
            return False
        return True
    finally:
        try:
            posix.close(fd)
        except OSError:
            pass


def worker(H, wid, rng, state):
    payload = make_payload(wid)           # fiber-local single-owner constant
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            ok = roundtrip_check(H, wid, idx, payload, state)
            if H.failed:
                return
            if ok:
                state["checks"][wid] += 1     # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    if not HAVE_POSIX:
        # posix is POSIX-only; on a non-POSIX platform there is nothing to probe.
        # Mark benign so the run doesn't false-FAIL.  (This suite targets Linux.)
        H.note_scale_limit("posix module unavailable on this platform")
    state = {"checks": [0] * H.funcs}     # ONE slot per worker (wid-indexed)
    if not USE_MEMFD:
        state["tmpdir"] = H.make_tmpdir(prefix="p586_")
    H.state = state


def body(H):
    if not HAVE_POSIX:
        return                            # nothing to run off POSIX
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if not HAVE_POSIX:
        H.log("posix unavailable on this platform -- benign skip (SCALE_LIMIT)")
        return
    checks = sum(H.state["checks"])
    H.log("posix single-owner memfd round-trips (byte-conservation + fstat "
          "identity, all passed fail-fast): {0}; ops={1} (memfd={2})".format(
              checks, H.total_ops(), USE_MEMFD))
    # NON-VACUITY: the load-bearing round-trip arm actually ran.
    H.check(checks > 0,
            "no posix memfd round-trips completed -- the single-owner fd "
            "byte-conservation / fstat-identity hazard was never exercised "
            "(oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-syscall.
    H.require_no_lost("posix memfd round-trip")


if __name__ == "__main__":
    harness.main(
        "p586_posix_memfd_roundtrip", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=MAX_FUNCS,
        describe="posix is process-global, so the oracle is built on the SINGLE-"
                 "OWNER objects it produces: an anonymous fd from "
                 "posix.memfd_create and stat_results from posix.fstat.  Each "
                 "fiber writes a wid-encoded payload to its OWN memfd, then across "
                 "a yield (while siblings churn/reuse fd numbers) preads it back "
                 "and re-fstats: byte conservation (pread == payload) + stat "
                 "identity (ino/dev/size stable) must hold on a single-owner fd. "
                 "A cross-fiber fd leak, torn read, short write, or torn "
                 "stat_result fails")
