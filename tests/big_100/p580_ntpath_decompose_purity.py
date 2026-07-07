"""big_100 / 580 -- ntpath path-decomposition purity across a yield under M:N.

ntpath is the Windows implementation of os.path -- pure str/bytes routines
(splitdrive, split, splitext, basename, dirname, normpath, join) that parse a
pathname into its structural pieces.  They are importable and fully functional
on any platform (ntpath is pure Python, no OS calls in these entry points) and
touch NO module-global mutable state -- they read only their argument.  So each
is mathematically PURE: the same input path must always decompose to the same
pieces, and the documented reconstruction identities must always hold:

    splitdrive(p)[0] + splitdrive(p)[1] == p          (drive + tail == path)
    splitext(p)[0]   + splitext(p)[1]   == p          (root  + ext  == path)
    split(p)         -> (head, tail)  with tail == basename(p),
                                            head == dirname(p)
    normpath(already-normal p)          == p          (idempotent on a path
                                                        with no '.'/'..' / redundant
                                                        separators)

WHERE M:N COULD BREAK IT (the gap this program probes).  Under the pygo runtime
a fiber can be preempted mid-call, migrated across hubs, or parked-and-resumed
at a cooperative yield.  These routines run C-level str partition/rindex/slice
loops (`p.rfind('\\')`, `p[:i]`, `p[i:]`) that allocate fresh transient objects.
If the runtime ever resumed a fiber with a stale frame (lost-wakeup class), let
a sibling's concurrent ntpath call scribble into this fiber's transient
partition/slice scratch (cross-fiber scratch leak), or tore a produced object,
then re-running the SAME pure calls on the SAME fiber-local path after a yield
would return DIFFERENT pieces.  Because the input path is a fiber-local constant
built from KNOWN components, and every routine is pure, ANY change across the
yield -- or any decomposition that disagrees with the closed form -- is a runtime
bug, never a documented Python semantic.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Single-owner closed-form PURITY law.  Each fiber builds its OWN path string
  (fiber-local, never shared) from KNOWN pieces: an optional drive ("C:" ... or
  ""), a leading root separator, zero or more nonempty directory segments, and a
  basename of the form NAME "." EXT.  The segment alphabet EXCLUDES the
  separators ('\\', '/'), the drive marker (':') and the extension marker ('.'),
  and NAME/EXT are nonempty with exactly one '.' between them -- so the
  decomposition of the assembled path is EXACTLY the pieces it was built from,
  by construction (verified against a 20k-case standalone control across drive /
  driveless / 0..4 directory segments -- every closed form held).  The fiber:
    * decomposes the path with the whole ntpath bundle and asserts every piece
      equals its closed form AND every reconstruction identity holds -- catches
      a piece that is WRONG the instant it is produced (a torn rfind/slice),
      which a pure recompute alone could miss if both computations tore identically;
    * YIELDS (yield_now / sleep) so siblings interleave their own ntpath churn,
      possibly on another hub;
    * recomputes the whole bundle and asserts every piece is UNCHANGED and still
      equals its closed form (the M:N purity hazard).

  str and bytes input shapes are round-robined so both the str and the
  bytes/bytearray branches of ntpath (which pick a different separator/colon/dot
  constant set) are exercised.  On a correct runtime every check passes
  deterministically (program exits 0).  A FAIL means an ntpath decomposition was
  wrong on production, or changed across a yield -- a real runtime
  purity/isolation bug.

  Note on why NO shared-mutable MEASURED arm: these ntpath routines are pure and
  read only their argument; there is no shared container they mutate, so there
  is no documented shared-object race to measure/report (unlike enum's
  _member_map_ or a shared Counter).  The single-owner recompute-across-yield arm
  IS the hazard test.

ORACLES:
  * LOAD-BEARING -- PURITY (worker, HARD, fail-fast): every piece equals its
    closed form + every reconstruction identity holds, on production and again
    unchanged across a yield, on fiber-local single-owner input.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-slice /
    inside rfind never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Stresses: ntpath.splitdrive / split / splitext / basename / dirname / normpath /
join partition/rfind/slice loops over str and bytes inputs, across a cooperative
yield + hub migration under M:N; purity of pure stdlib path routines under
preemption.
"""
import ntpath

import harness
import runloom

# Fiber-local segment alphabet.  DELIBERATELY excludes the path separators
# ('\\','/'), the drive-letter marker (':') and the extension marker ('.') so a
# segment can never introduce an unexpected split/drive/ext boundary -- this is
# what pins the closed-form decomposition to exactly the pieces we built from.
ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

# Drive letters used when the fiber builds a drive-qualified path.
DRIVE_LETTERS = "CDEFGHIJKLMNOPQRSTUVWXYZ"

# Input shape cases, round-robined by (wid + idx) so both the str and the bytes
# branches of ntpath are exercised regardless of how many rounds a worker does.
CASE_STR = 0      # str path   -> ntpath uses str sep/colon/dot constants
CASE_BYTES = 1    # bytes path -> ntpath uses bytes sep/colon/dot constants
NCASES = 2


def build_pieces(rng):
    """Build the KNOWN structural pieces of one fiber-local Windows path.

    Returns a dict of str pieces:
      drive     -- "" or "X:" (drive letter)
      dirs      -- list of nonempty directory segments (may be empty)
      base      -- basename  NAME "." EXT   (both nonempty, exactly one dot)
      path      -- the assembled absolute path  drive + "\\" + dirs.../base
      head      -- expected dirname / split()[0]
      rootext   -- expected splitext()[0]  (path without the ".EXT" suffix)
      ext       -- expected splitext()[1]  ("." + EXT)
      tail      -- expected splitdrive()[1] (path minus the drive)

    Every piece is closed-form by construction (the segment alphabet excludes
    every separator/marker), verified against a 20k-case standalone control."""
    def seg():
        return "".join(rng.choice(ALPHABET) for _ in range(rng.randint(1, 6)))

    drive = ("%s:" % rng.choice(DRIVE_LETTERS)) if rng.random() < 0.5 else ""
    ndir = rng.randint(0, 4)
    dirs = [seg() for _ in range(ndir)]
    name = seg()
    ext = seg()
    base = name + "." + ext

    # Absolute path: drive, a single leading backslash root, the directory
    # segments, then the basename -- no empty segments, no redundant separators.
    if dirs:
        tail = "\\" + "\\".join(dirs + [base])
        head = drive + "\\" + "\\".join(dirs)
    else:
        tail = "\\" + base
        head = drive + "\\"
    path = drive + tail
    rootext = path[: -len("." + ext)]     # path with the ".EXT" suffix removed

    return {
        "drive": drive,
        "dirs": dirs,
        "base": base,
        "path": path,
        "head": head,
        "rootext": rootext,
        "ext": "." + ext,
        "tail": tail,
    }


def as_case(pieces, case):
    """Project the str pieces onto the CASE's type (str or bytes).

    Returns (mod_ok, path, drive, tail, head, base, rootext, ext, join_args)
    all in the case's type.  ntpath dispatches on the argument type, so a bytes
    path drives the bytes-constant code path."""
    if case == CASE_STR:
        conv = lambda s: s
    else:
        conv = lambda s: s.encode("latin-1")
    drive = conv(pieces["drive"])
    tail = conv(pieces["tail"])
    head = conv(pieces["head"])
    base = conv(pieces["base"])
    rootext = conv(pieces["rootext"])
    ext = conv(pieces["ext"])
    path = conv(pieces["path"])
    # join reconstruction args: (drive + root sep, *dirs, base)
    sep = conv("\\")
    join_args = [conv(pieces["drive"]) + sep]
    for d in pieces["dirs"]:
        join_args.append(conv(d))
    join_args.append(base)
    return path, drive, tail, head, base, rootext, ext, join_args


def check_bundle(H, wid, case, label, path, drive, tail, head, base,
                 rootext, ext, join_args):
    """Run the whole ntpath decomposition bundle on `path` and assert every
    piece equals its closed form + every reconstruction identity holds.

    Returns True on all-pass, False (after H.fail) on any mismatch.  `label` is
    "production" or "post-yield" for the message."""
    # splitdrive: exact pieces + documented drive+tail==path identity.
    dv, tl = ntpath.splitdrive(path)
    if dv != drive or tl != tail:
        H.fail("ntpath.splitdrive WRONG ({0}): case={1} wid={2} path={3!r} got "
               "{4!r} expected {5!r} -- torn drive/tail split".format(
                   label, case, wid, path, (dv, tl), (drive, tail)))
        return False
    if dv + tl != path:
        H.fail("ntpath.splitdrive IDENTITY broken ({0}): case={1} wid={2} "
               "drive+tail={3!r} != path={4!r}".format(
                   label, case, wid, dv + tl, path))
        return False

    # split: (head, tail-basename); head==dirname, tail==basename.
    hd, tb = ntpath.split(path)
    if hd != head or tb != base:
        H.fail("ntpath.split WRONG ({0}): case={1} wid={2} path={3!r} got "
               "{4!r} expected {5!r} -- torn head/tail split".format(
                   label, case, wid, path, (hd, tb), (head, base)))
        return False

    # basename / dirname must agree with split (and with the closed form).
    bn = ntpath.basename(path)
    dn = ntpath.dirname(path)
    if bn != base:
        H.fail("ntpath.basename WRONG ({0}): case={1} wid={2} got {3!r} "
               "expected {4!r}".format(label, case, wid, bn, base))
        return False
    if dn != head:
        H.fail("ntpath.dirname WRONG ({0}): case={1} wid={2} got {3!r} "
               "expected {4!r}".format(label, case, wid, dn, head))
        return False

    # splitext: exact (root, ext) + documented root+ext==path identity.
    rt, ex = ntpath.splitext(path)
    if rt != rootext or ex != ext:
        H.fail("ntpath.splitext WRONG ({0}): case={1} wid={2} path={3!r} got "
               "{4!r} expected {5!r} -- torn root/ext split".format(
                   label, case, wid, path, (rt, ex), (rootext, ext)))
        return False
    if rt + ex != path:
        H.fail("ntpath.splitext IDENTITY broken ({0}): case={1} wid={2} "
               "root+ext={3!r} != path={4!r}".format(
                   label, case, wid, rt + ex, path))
        return False

    # normpath: our path is already fully normal (no '.'/'..'/redundant seps),
    # so normpath is the identity and is idempotent.
    np1 = ntpath.normpath(path)
    if np1 != path:
        H.fail("ntpath.normpath CHANGED an already-normal path ({0}): case={1} "
               "wid={2} got {3!r} expected {4!r}".format(
                   label, case, wid, np1, path))
        return False
    if ntpath.normpath(np1) != path:
        H.fail("ntpath.normpath NOT idempotent ({0}): case={1} wid={2} "
               "path={3!r}".format(label, case, wid, path))
        return False

    # join reconstruction: (drive+sep, *dirs, base) rebuilds the exact path.
    jp = ntpath.join(*join_args)
    if jp != path:
        H.fail("ntpath.join RECONSTRUCTION WRONG ({0}): case={1} wid={2} "
               "join({3!r})={4!r} expected {5!r}".format(
                   label, case, wid, join_args, jp, path))
        return False

    return True


def purity_check(H, wid, idx, state):
    """Single-owner ntpath decomposition purity check on fiber-local input.

    Build a path from KNOWN pieces, decompose it and assert every piece +
    identity is closed-form correct NOW, yield so siblings interleave, recompute
    the whole bundle and assert nothing changed and it still equals the closed
    form."""
    rng = H.derive("nt", wid, idx)
    case = (wid + idx) % NCASES
    pieces = build_pieces(rng)
    (path, drive, tail, head, base,
     rootext, ext, join_args) = as_case(pieces, case)

    # (1) production correctness: the whole bundle must be closed-form now.
    if not check_bundle(H, wid, case, "production", path, drive, tail, head,
                        base, rootext, ext, join_args):
        return

    # YIELD: let siblings run their own ntpath churn, possibly on another hub,
    # while this fiber is parked holding its path + expected pieces.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # (2) purity across the park: recompute the whole bundle; every piece must
    # still equal its closed form (nothing drifted while the fiber was parked).
    if not check_bundle(H, wid, case, "post-yield", path, drive, tail, head,
                        base, rootext, ext, join_args):
        return

    state["checks"][wid] += 1        # single-writer-per-slot, race-free (see p405)


# Sustained checks per worker, bounded by H.running().  The purity hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously computing/parking
# across their ntpath yield so the scheduler reliably interleaves a sibling's
# call before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE slot per worker (wid-indexed) -> single-writer, race-free non-vacuity
    # tally.  Allocated here where H.funcs is known (see HARD RULE 1 / p405).
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("ntpath decomposition purity checks (str/bytes, splitdrive/split/"
          "splitext/basename/dirname/normpath/join, all passed fail-fast): "
          "{0}; ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(checks > 0,
            "no ntpath purity checks ran -- the pure-function purity hazard was "
            "never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid ntpath decomposition.
    H.require_no_lost("ntpath decomposition purity")


if __name__ == "__main__":
    harness.main(
        "p580_ntpath_decompose_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="ntpath (Windows os.path) splitdrive/split/splitext/basename/"
                 "dirname/normpath/join are pure path-parsing routines (rfind + "
                 "slice, no shared mutable state).  LOAD-BEARING: each fiber "
                 "builds its OWN path (str or bytes) from KNOWN pieces (drive, "
                 "dirs, NAME.EXT) whose decomposition is closed-form by "
                 "construction; decomposes it and asserts every piece + every "
                 "reconstruction identity (drive+tail==path, root+ext==path, "
                 "join rebuilds path, normpath idempotent) holds, yields so "
                 "siblings interleave their own ntpath churn on other hubs, then "
                 "recomputes and asserts every piece is UNCHANGED and still "
                 "closed-form.  A piece wrong on production, or that changes "
                 "across the yield, is a runtime purity/isolation bug (lost-"
                 "wakeup stale frame, cross-fiber scratch leak, torn slice)")
