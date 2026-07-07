"""big_100 / 591 -- pwd getpwnam/getpwuid struct_passwd round-trip PURITY under M:N.

The `pwd` module is a PROCESS-GLOBAL wrapper over the system passwd database: it
exposes NO single-owner mutable handle -- getpwnam(), getpwuid(), and getpwall()
are pure lookups against a read-only, process-wide database.  So we build the
oracle NOT on a global hook but on the SINGLE-OWNER OBJECT the module PRODUCES:
each call returns a FRESH pwd.struct_passwd -- an immutable tuple subclass with
seven named fields (pw_name, pw_passwd, pw_uid, pw_gid, pw_gecos, pw_dir,
pw_shell).  A freshly-constructed tuple owned by one fiber is race-free by
construction; the load-bearing question is whether the C lookup path that FILLS
it is torn under M:N.

WHERE M:N COULD BREAK IT (the gap this program probes).  CPython's pwd module is
implemented in C (Modules/pwdmodule.c).  On the non-reentrant code path it calls
the libc getpwnam()/getpwuid() family, which classically return a pointer into a
SINGLE PROCESS-WIDE STATIC `struct passwd` buffer that is overwritten by the next
call from ANY thread.  CPython copies the fields out of that buffer into a Python
struct_passwd; if that copy is not serialized against a concurrent sibling call
that overwrites the same static buffer mid-copy, a fiber could observe a TORN
struct -- e.g. pw_name from entry A but pw_dir from entry B -- a cross-fiber leak
through libc's shared static storage.  Under a CORRECT runloom + a correctly
free-threading-audited pwd module (gh-116738 class of fix), every returned
struct must be a faithful, self-consistent copy of exactly ONE database entry.

CLOSED-WORLD GROUND TRUTH.  In setup() -- the quiescent single-threaded root --
we snapshot the ENTIRE passwd database once via pwd.getpwall() into a canonical,
read-only reference: a dict mapping pw_name -> the full 7-tuple, plus a dict
mapping pw_uid -> a set of names at that uid (a uid can be shared by several
names, so getpwuid(uid).pw_uid == uid is the law, NOT name identity).  This
snapshot is immutable for the run (the passwd DB does not change) and is only
READ by fibers, never written -- so it is a legitimate shared read-only oracle,
not a shared-mutable container.

WHICH ORACLE IS LOAD-BEARING, AND WHY (holds on plain threads):
  getpwnam(name) MUST return a struct_passwd whose seven fields equal EXACTLY the
  canonical snapshot tuple for `name` -- byte-for-byte, every field.  getpwuid(u)
  MUST return a struct whose pw_uid == u and whose pw_name is one of the names the
  snapshot records at uid u.  These are pure functions of the (unchanging)
  database, so on a correct runtime they are bit-identical every call, on every
  hub, before and after a yield.  A plain-threads control (many OS threads each
  hammering getpwnam/getpwuid for distinct names, GIL on AND off) returns the
  faithful entry 100% of the time.  If a fiber's returned struct disagrees with
  the canonical snapshot -- a field from a different entry, a uid mismatch, a name
  that maps to the wrong uid -- that is a torn cross-fiber read through the pwd
  module's C path, a real runtime bug.

ORACLES:
  * LOAD-BEARING -- STRUCT ROUND-TRIP PURITY (worker, HARD, fail-fast).  Each
    fiber owns a fiber-local rotation over a set of (name, uid) targets.  Per op:
      - s = pwd.getpwnam(name); capture its 7 fields into a fiber-local tuple.
      - YIELD (yield_now / tiny sleep) so a sibling reliably calls into the pwd
        C path (overwriting any shared libc static buffer) before we validate.
      - Re-read t = pwd.getpwnam(name); assert t == s (stable across the yield)
        AND t equals the canonical snapshot 7-tuple for `name` (faithful copy).
      - u = s.pw_uid; g = pwd.getpwuid(u); assert g.pw_uid == u and g.pw_name is
        in the snapshot's name-set for u (uid round-trip, tolerant of shared uids).
      - Assert struct self-consistency: tuple(s) has length 7 and s.pw_name ==
        s[0], s.pw_uid == s[2], etc. (the named-tuple view agrees with indices).
    Single-owner: s, t, g are fresh immutable tuples bound to fiber-locals; the
    only shared thing read is the immutable snapshot.  A mismatch is a torn/leaked
    pwd read, not documented Python semantics.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the pwd
    C call (parked-then-vanished) never returns; require_no_lost catches it.

FAIL ON: a getpwnam struct whose fields differ from the canonical snapshot, a
struct that changed across a yield, a getpwuid whose pw_uid != requested uid or
whose name is not recorded at that uid, or a named/indexed field disagreement.
There is NO shared-mutable arm here -- the database is read-only, so every
observation is load-bearing (a legitimate closed-world purity law).

Stresses: pwd.getpwnam / getpwuid C lookup path under GIL-off M:N churn, libc
getpwXXX shared-static-buffer copy-out serialization, struct_passwd named-tuple
field integrity across hub migration + yield, closed-world round-trip against a
quiescent-snapshot ground truth.

Good TSan / controlled-M:N-replay target: the field copy-out from libc's static
`struct passwd` into the Python struct_passwd is a read of shared process-wide
storage; a TSan report on that buffer, or a replay that copies a field mid-
overwrite by a sibling's getpwnam, localizes the tear before the tuple-equality
oracle fires.
"""
import pwd

import harness
import runloom


def build_snapshot():
    """Snapshot the ENTIRE passwd database once (called from the quiescent root).

    Returns:
      by_name: dict pw_name -> the full 7-tuple (canonical faithful copy).
      names:   tuple of all pw_name values (fiber target rotation source).
      uid_names: dict pw_uid -> frozenset of names at that uid (a uid may be
                 shared by several names; getpwuid returns the FIRST match, so the
                 law is pw_uid==u and pw_name in uid_names[u], not name identity).
    """
    by_name = {}
    uid_tmp = {}
    for e in pwd.getpwall():
        t = (e.pw_name, e.pw_passwd, e.pw_uid, e.pw_gid,
             e.pw_gecos, e.pw_dir, e.pw_shell)
        by_name[e.pw_name] = t
        uid_tmp.setdefault(e.pw_uid, set()).add(e.pw_name)
    uid_names = dict((u, frozenset(s)) for u, s in uid_tmp.items())
    names = tuple(by_name.keys())
    return by_name, names, uid_names


def struct_to_tuple(s):
    """The seven fields of a struct_passwd as a plain tuple, read via NAMED
    attributes (so the oracle also validates the named-tuple field mapping)."""
    return (s.pw_name, s.pw_passwd, s.pw_uid, s.pw_gid,
            s.pw_gecos, s.pw_dir, s.pw_shell)


def check_one(H, wid, name, state):
    """One round-trip purity check on a single fiber-local target name.

    Reads getpwnam(name) before and after a yield, compares both to each other and
    to the canonical snapshot, then round-trips the uid through getpwuid."""
    by_name = state["by_name"]
    uid_names = state["uid_names"]
    canon = by_name[name]

    # First read (single-owner fresh struct); capture named-field view.
    s = pwd.getpwnam(name)
    s_tuple = struct_to_tuple(s)

    # Named view must agree with the tuple/index view of the SAME object (the
    # struct_passwd named-tuple field mapping is itself under test).
    if s_tuple != tuple(s)[:7]:
        H.fail("struct_passwd named/indexed field mismatch for {0!r} (wid {1}): "
               "named={2!r} indexed={3!r} -- the named-tuple field mapping "
               "disagrees with positional access on a fresh pwd struct".format(
                   name, wid, s_tuple, tuple(s)[:7]))
        return

    # First read must faithfully equal the canonical snapshot entry.
    if s_tuple != canon:
        H.fail("getpwnam({0!r}) returned {1!r} but the quiescent-root snapshot "
               "recorded {2!r} (wid {3}) -- a torn/leaked pwd read: some field "
               "came from a DIFFERENT database entry (libc static-buffer overwrite "
               "by a concurrent sibling getpwnam mid copy-out)".format(
                   name, s_tuple, canon, wid))
        return

    # YIELD so a sibling reliably drives the pwd C path (and any shared libc
    # static buffer) before we re-validate this fiber's target.
    runloom.yield_now()
    if s.pw_uid & 1:
        runloom.sleep(0.0002)

    # Second read must be bit-identical to the first AND to the snapshot.
    t = pwd.getpwnam(name)
    t_tuple = struct_to_tuple(t)
    if t_tuple != s_tuple:
        H.fail("getpwnam({0!r}) CHANGED across a yield (wid {1}): before={2!r} "
               "after={3!r} -- a sibling's pwd call corrupted this fiber's lookup "
               "(shared static-buffer tear)".format(name, wid, s_tuple, t_tuple))
        return
    if t_tuple != canon:
        H.fail("getpwnam({0!r}) after yield {1!r} != snapshot {2!r} (wid {3}) -- "
               "torn cross-fiber pwd read".format(name, t_tuple, canon, wid))
        return

    # UID round-trip: getpwuid(u) must return uid u and a name recorded at u.
    u = s.pw_uid
    g = pwd.getpwuid(u)
    if g.pw_uid != u:
        H.fail("getpwuid({0}) returned pw_uid={1} != requested {0} (wid {2}, "
               "name {3!r}) -- torn uid lookup (static-buffer overwrite)".format(
                   u, g.pw_uid, wid, name))
        return
    valid = uid_names.get(u)
    if valid is None or g.pw_name not in valid:
        H.fail("getpwuid({0}) returned pw_name={1!r} not among the snapshot's "
               "names {2!r} at that uid (wid {3}) -- a leaked entry from a "
               "different uid under concurrent pwd churn".format(
                   u, g.pw_name, sorted(valid) if valid else None, wid))
        return
    # getpwuid's returned struct must itself faithfully match ITS name's snapshot.
    g_tuple = struct_to_tuple(g)
    if g_tuple != by_name[g.pw_name]:
        H.fail("getpwuid({0}) struct {1!r} != snapshot for its name {2!r} == "
               "{3!r} (wid {4}) -- torn getpwuid copy-out".format(
                   u, g_tuple, g.pw_name, by_name[g.pw_name], wid))
        return

    state["checks"][wid] += 1


# Sustained checks per worker.  The static-buffer tear only manifests under
# SUSTAINED overlap -- many fibers simultaneously inside the pwd C path while a
# sibling is sleep-PARKED across its yield, so the scheduler reliably interleaves
# another getpwnam before this fiber re-reads.  A single check per fiber barely
# overlaps and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber owns a fiber-local rotation over a shuffled subset of the DB
    names (fiber-local list; the shuffle is per-fiber so different fibers target
    different entries concurrently -- maximizing the chance that a sibling's
    getpwnam overwrites a shared static buffer while this fiber copies out)."""
    names = state["names"]
    # Fiber-local target list: a per-fiber shuffle so targets differ across fibers.
    local = list(names)
    rng.shuffle(local)
    n = len(local)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            name = local[idx % n]
            check_one(H, wid, name, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    by_name, names, uid_names = build_snapshot()
    if not names:
        # Degenerate environment with an empty passwd DB -- nothing to probe.
        # Leave checks empty; post()'s non-vacuity guard will flag it.
        pass
    H.state = {
        "by_name": by_name,        # read-only canonical snapshot (name -> 7-tuple)
        "names": names,            # all names (fiber target rotation)
        "uid_names": uid_names,    # uid -> frozenset(names) (shared-uid tolerant)
        "checks": [0] * H.funcs,   # LOAD-BEARING per-wid slot (single writer; race-free)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    ndb = len(H.state["names"])
    H.log("pwd round-trip[single-owner LOAD-BEARING]: {0} struct_passwd purity "
          "checks over {1} passwd entries (all passed fail-fast); ops={2}".format(
              checks, ndb, H.total_ops()))

    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(checks > 0,
            "no pwd round-trip checks ran -- the getpwnam/getpwuid tear hazard "
            "was never exercised (oracle would be vacuous; passwd DB may be empty)")

    # COMPLETENESS: no fiber parked-then-vanished inside the pwd C call.
    H.require_no_lost("pwd round-trip completeness")


if __name__ == "__main__":
    harness.main(
        "p591_pwd_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="pwd exposes no single-owner handle -- getpwnam/getpwuid are pure "
                 "lookups over a process-wide passwd DB whose C path may copy out "
                 "of a shared libc static struct.  LOAD-BEARING: snapshot the whole "
                 "DB once in the quiescent root, then each fiber round-trips "
                 "getpwnam(name)/getpwuid(uid) across a yield and asserts every "
                 "returned struct_passwd is bit-identical to the canonical snapshot "
                 "(faithful copy of exactly one entry, named==indexed fields, uid "
                 "round-trip).  A field from a different entry, a struct that "
                 "changes across a yield, or a uid/name mismatch is a torn cross-"
                 "fiber read through the pwd C path -- a runtime bug")
