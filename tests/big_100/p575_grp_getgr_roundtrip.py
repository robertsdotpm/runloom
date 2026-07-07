"""big_100 / 575 -- grp.getgrnam / getgrgid struct_group purity + single-owner
stability under M:N.

The `grp` module exposes the system group database.  On 3.14t the two lookup
entry points -- grp.getgrnam(name) and grp.getgrgid(gid) -- are implemented over
the REENTRANT libc calls getgrnam_r / getgrgid_r (grpmodule.c: each call fills a
stack/heap buffer OWNED BY THAT CALL, never a shared static `struct group`), so
concurrent lookups do NOT tear against one another the way the non-reentrant
getgrnam()/getgrgid() would.  grp.getgrall() is the only entry that walks the
non-reentrant setgrent()/getgrent()/endgrent() cursor, and CPython guards THAT
with a C-level PyMutex (getgrall_mutex) -- so it is safe too, but it serializes
at the C level and is NOT used in the hot worker loop here (a hub OS thread that
blocks in C on that mutex would stall its co-resident fibers; we snapshot the DB
once, in setup, on the root).

Because the group database does NOT change during the run, every lookup has a
CLOSED-FORM expected answer computed once in setup().  That makes this a PURITY /
single-owner test, not a racy probe:

  * getgrnam(name) MUST return a struct_group whose fields bit-identically match
    the snapshot taken in setup (gr_name==name, gr_passwd, gr_gid, and gr_mem
    equal by VALUE -- gr_mem is a fresh list per call, so compare list==list, not
    identity);
  * gr_gid round-trips: getgrgid(getgrnam(name).gr_gid).gr_gid == that gid (we do
    NOT assert the names match on the gid round-trip because multiple group names
    may legitimately share one gid, in which case getgrgid returns the first --
    documented, not a bug);
  * SINGLE-OWNER STABILITY ACROSS A YIELD: the struct_group returned to a fiber is
    that fiber's own object.  The fiber reads all four fields into locals, YIELDS
    (so a sibling on another hub interleaves and does its own conflicting lookups),
    then re-reads the SAME object's fields and asserts they are unchanged -- and a
    FRESH getgrnam(name) taken after the yield must still equal the pre-yield
    snapshot.  A value that changed across the yield, or a fresh lookup that came
    back as another fiber's group, is a runloom object/frame-isolation desync or a
    cross-fiber leak of the reentrant lookup buffer.

WHERE M:N COULD BREAK IT (the gap probed).  getgrnam_r / getgrgid_r run inside
Py_BEGIN_ALLOW_THREADS on the hub's OS thread; the fiber parks across the
runloom.yield_now() at the hazard boundary and may resume on a DIFFERENT hub.  If
runloom leaked the reentrant call's buffer across fibers, corrupted a saved
register holding the struct_group pointer across the hub migration, or lost the
wakeup so the fiber never re-read, the fail-fast oracle catches it.  On a correct
runtime every law holds and the program exits 0.

WHY NO FAIL CAN BE DOCUMENTED-PYTHON BEHAVIOR.  The oracle object is single-owner
(each fiber's own struct_group, created in a fiber-local variable, never shared);
the expected values are an immutable process-global snapshot with a closed-form
answer; the reentrant getgr*_r calls are thread-safe by construction.  There is
no shared-mutable container in the fail-fast arm, so a violation can only be a
real runtime fault (torn field, cross-fiber value leak, lost wakeup, SIGSEGV).

ORACLES:
  * LOAD-BEARING (worker, HARD, fail-fast): per-fiber struct_group purity +
    round-trip + single-owner stability across a yield, all vs the immutable
    setup snapshot.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-lookup or
    across the yield never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Stresses: grp.getgrnam / getgrgid reentrant libc lookup on the hub OS thread
inside Py_BEGIN_ALLOW_THREADS, struct_group structseq field access across a yield
+ hub migration, per-fiber lookup-buffer isolation, closed-form DB purity.
"""
import grp

import harness
import runloom


def snapshot(name):
    """Canonical closed-form expected tuple for `name`, via getgrnam.

    gr_mem is materialised to a tuple so the stored expectation is immutable; the
    live comparison turns the freshly-returned list into a list and compares by
    value."""
    g = grp.getgrnam(name)
    return (g.gr_name, g.gr_passwd, g.gr_gid, tuple(g.gr_mem))


def check_entry(H, wid, name, expected):
    """One fiber-local lookup + purity + single-owner-across-yield check.

    `expected` == (name, passwd, gid, mem_tuple) from the immutable setup snapshot.
    Returns True if a check ran (for non-vacuity), False on early-out after fail."""
    exp_name, exp_pw, exp_gid, exp_mem = expected

    # ---- fresh lookup: must bit-identically match the closed-form snapshot ----
    g = grp.getgrnam(name)               # fiber-local, single-owner struct_group
    if g.gr_name != exp_name:
        H.fail("getgrnam({0!r}).gr_name == {1!r} != {2!r} -- wrong group name "
               "returned (cross-fiber leak of the reentrant lookup buffer under "
               "M:N)".format(name, g.gr_name, exp_name))
        return False
    if g.gr_passwd != exp_pw:
        H.fail("getgrnam({0!r}).gr_passwd == {1!r} != {2!r} -- torn/leaked "
               "passwd field".format(name, g.gr_passwd, exp_pw))
        return False
    if g.gr_gid != exp_gid:
        H.fail("getgrnam({0!r}).gr_gid == {1!r} != {2!r} -- torn/leaked gid "
               "field".format(name, g.gr_gid, exp_gid))
        return False
    if list(g.gr_mem) != list(exp_mem):
        H.fail("getgrnam({0!r}).gr_mem == {1!r} != {2!r} -- torn/leaked member "
               "list".format(name, list(g.gr_mem), list(exp_mem)))
        return False

    # Capture this fiber's own struct_group fields BEFORE the yield.
    pre_name = g.gr_name
    pre_pw = g.gr_passwd
    pre_gid = g.gr_gid
    pre_mem = list(g.gr_mem)

    # ---- hazard boundary: park + likely resume on a different hub while a
    # sibling does its own conflicting getgrnam/getgrgid on another hub ----
    runloom.yield_now()

    # Single-owner stability: the SAME object's fields must be unchanged.
    if g.gr_name != pre_name or g.gr_passwd != pre_pw or g.gr_gid != pre_gid \
            or list(g.gr_mem) != pre_mem:
        H.fail("struct_group for {0!r} MUTATED across a yield: "
               "before=({1!r},{2!r},{3!r},{4!r}) after=({5!r},{6!r},{7!r},{8!r}) "
               "-- single-owner object changed under hub migration (runloom "
               "isolation desync)".format(
                   name, pre_name, pre_pw, pre_gid, pre_mem,
                   g.gr_name, g.gr_passwd, g.gr_gid, list(g.gr_mem)))
        return False

    # A FRESH lookup after the yield must still be the closed-form snapshot (the
    # DB is immutable; a differing answer is a cross-fiber leak of another fiber's
    # concurrent lookup).
    g2 = grp.getgrnam(name)
    if g2.gr_name != exp_name or g2.gr_passwd != exp_pw or g2.gr_gid != exp_gid \
            or list(g2.gr_mem) != list(exp_mem):
        H.fail("post-yield getgrnam({0!r}) == ({1!r},{2!r},{3!r},{4!r}) != "
               "snapshot ({5!r},{6!r},{7!r},{8!r}) -- fresh lookup returned a "
               "different (leaked) group after hub migration".format(
                   name, g2.gr_name, g2.gr_passwd, g2.gr_gid, list(g2.gr_mem),
                   exp_name, exp_pw, exp_gid, list(exp_mem)))
        return False

    # ---- gid round-trip via getgrgid (reentrant getgrgid_r) ----
    # Do NOT assert names match: multiple names may share a gid, so getgrgid
    # returns THE (first) entry for that gid -- documented.  Assert only that the
    # gid itself round-trips and the returned object is self-consistent.
    g3 = grp.getgrgid(exp_gid)
    if g3.gr_gid != exp_gid:
        H.fail("getgrgid({0!r}).gr_gid == {1!r} != {0!r} -- gid did not "
               "round-trip through the reentrant lookup".format(
                   exp_gid, g3.gr_gid))
        return False
    if not isinstance(g3.gr_name, str) or not isinstance(g3.gr_mem, list):
        H.fail("getgrgid({0!r}) returned malformed struct_group "
               "name={1!r} mem={2!r} -- torn structseq under M:N".format(
                   exp_gid, g3.gr_name, g3.gr_mem))
        return False
    return True


def worker(H, wid, rng, state):
    names = state["names"]
    expected = state["expected"]
    nnames = len(names)
    for _ in H.round_range():
        if not H.running():
            break
        name = names[rng.randrange(nnames)]
        if not check_entry(H, wid, name, expected[name]):
            return                        # failed; stop this fiber cleanly
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Snapshot the (immutable-for-the-run) group DB ONCE on the root.  getgrall()
    # walks the non-reentrant cursor under the C PyMutex; keep it out of workers.
    all_groups = grp.getgrall()
    # Dedup names (a name is the unique key of the DB).  Build the closed-form
    # expected map via getgrnam so it matches exactly what workers will compare.
    seen = {}
    names = []
    for e in all_groups:
        nm = e.gr_name
        if nm in seen:
            continue
        seen[nm] = True
        names.append(nm)
    if not names:
        # Degenerate host with an empty group DB: nothing to test.  Leave names
        # empty; post()'s non-vacuity check will flag it rather than silently pass.
        H.state = {"names": [], "expected": {}}
        return
    expected = {}
    for nm in names:
        expected[nm] = snapshot(nm)
    H.state = {"names": names, "expected": expected}
    H.log("grp DB snapshot: {0} distinct group names".format(len(names)))


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("grp getgrnam/getgrgid purity checks: ops={0} over {1} group "
          "names".format(H.total_ops(), len(H.state["names"])))
    # Reaching post with no failure means every per-fiber purity + single-owner
    # + round-trip law held fail-fast; assert the arm actually ran.
    H.check(H.total_ops() > 0,
            "no grp purity checks completed -- the getgrnam/getgrgid lookup + "
            "cross-yield stability arm was never exercised (or an empty group DB)")
    H.require_no_lost("grp-getgr-roundtrip completeness")


if __name__ == "__main__":
    harness.main(
        "p575_grp_getgr_roundtrip", body, setup=setup, post=post,
        default_funcs=3000,
        describe="tens of thousands of fibers each do fiber-local grp.getgrnam / "
                 "getgrgid reentrant lookups and assert the returned struct_group "
                 "bit-identically matches an immutable setup snapshot, is stable "
                 "across a yield + hub migration (single-owner), and round-trips "
                 "its gid -- a torn field, cross-fiber leaked group, or a value "
                 "that changed across the yield fails")
