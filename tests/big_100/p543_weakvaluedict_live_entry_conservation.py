"""big_100 / 543 -- weakref.WeakValueDictionary live-entry CONSERVATION under M:N.

weakref.WeakValueDictionary (WVD) does not hold a strong reference to its values;
each stored value is wrapped in a KeyedRef (a weakref.ref subclass that remembers
its dict key).  When a value's LAST strong reference is dropped, the value is
deallocated and its KeyedRef's GC callback fires -- and that callback SPLICES the
WVD's backing dict, deleting the entry (or, if the WVD is mid-iteration, deferring
the delete onto a _pending_removals list that a later mutation flushes).

WHERE M:N CAN BREAK IT (the corner this program probes).  Under free-threaded
CPython 3.14t the deallocation that fires a KeyedRef callback runs on whichever
hub drops the value's LAST reference -- NOT necessarily the hub that created the
WVD or inserted the entry.  And gc.collect() is PROCESS-GLOBAL: a collection
triggered by a fiber on hub A walks EVERY container object in the process,
including a WVD that a fiber on hub B is concurrently inserting into or reading.
The hazards, if the runtime's GC / weakref-callback integration is not M:N-safe:

  * a KeyedRef callback firing on one hub splices the backing dict while the owner
    reads it on another hub -> a still-STRONGLY-referenced entry is dropped (the
    value is provably alive -- we hold a strong ref -- yet vanishes from the WVD),
  * a dead value's callback is stranded (never splices) -> a dropped key lingers,
    mapping to a freed slot (UAF / torn value read), or
  * a cross-hub gc.collect() traverses the WVD's backing dict mid-mutation ->
    torn entry / SIGSEGV in the collector.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner closed world).

  Each fiber owns its OWN WeakValueDictionary and its OWN strong-reference table
  -- NEVER shared with any sibling.  A WVD is a container; a SHARED one raced by
  many fibers would drop/keep entries exactly like a shared dict raced across OS
  threads (documented Python behavior, NOT a runtime bug), so sharing is banned
  from the fail-fast oracle.  Because this fiber is the ONLY holder of strong refs
  to its values, the set of LIVE entries in its WVD is a CLOSED-WORLD function of
  which strong refs it still holds -- a deterministic conservation law that a
  correct runtime MUST satisfy no matter what siblings do on other hubs:

    1. Insert K values keeping a strong ref to each; record the per-key expected
       (wid, kid) identity.
    2. YIELD (siblings churn their own WVDs + call gc.collect() cross-hub).
    3. Assert ALL K keys are present and each maps to a value with THIS fiber's
       (wid, kid) -- no entry dropped while strongly held, no cross-fiber value.
    4. Drop a KNOWN subset of strong refs, then gc.collect().
    5. YIELD again.
    6. Assert EXACTLY the dropped keys vanished and EVERY retained key still maps
       to the correct (wid, kid) value; and len(wvd) == number of strong refs
       still held (live-count == retained-strong-refs conservation).

  On a correct runtime this PASSES (program exits 0): the retained values have
  live strong refs, so no global collection can reap them; the dropped values have
  NO refs, so refcounting + the KeyedRef callback remove exactly them.  A FAIL
  means a retained-but-strongly-held entry was spliced away, a dropped entry
  lingered, a value's identity/payload changed across a yield (cross-fiber leak of
  single-owner state), or the collector tore the backing dict -- all real runtime
  bugs, none of them documented Python semantics.

ORACLES:
  * LOAD-BEARING -- WVD live-entry CONSERVATION (worker, HARD, fail-fast).  The
    single-owner closed-world law above.  Every value is (wid, kid)-stamped so a
    cross-fiber leak (reading a sibling's value object) is caught by payload, and
    the retained/dropped partition is caught by presence.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (wvd_rounds > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-splice
    inside a KeyedRef callback or mid-gc never returns; the watchdog catches it.

Distinct from p330 (resurrect-identity of a __del__-revived object) and p77
(callback storm rate): this is a CONSERVATION law on a SINGLE-OWNER WVD that
deliberately exercises the KeyedRef dict-splice across a cross-hub gc.collect().

Stresses: WeakValueDictionary KeyedRef creation + callback dict-splice,
_pending_removals flushing, len()/__contains__/get() over the backing dict racing
a cross-hub process-global gc.collect(), refcount-driven value dealloc on a
foreign hub, live-entry vs strong-ref conservation.

Good TSan / controlled-M:N-replay target: the KeyedRef callback's `del
self.data[key]` on one hub racing a `self.data[key]` read on another, or the
collector walking `self.data` mid-insert, is the cleanest data-race signal before
the conservation sum even closes.
"""
import gc
import weakref

import harness
import runloom


# Values stored in the WVD.  Must be weakly referenceable, so either a plain class
# (has __weakref__ automatically) or __slots__ that INCLUDES __weakref__.  We stamp
# each with (wid, kid) so a cross-fiber leak -- reading a sibling's value object --
# is caught by payload, not just by presence.
class Val(object):
    __slots__ = ("wid", "kid", "__weakref__")

    def __init__(self, wid, kid):
        self.wid = wid
        self.kid = kid


# Entries per fiber per round.  Big enough to push the WVD's backing dict through
# several growth/rehash boundaries (a rehash is what moves entries under a racing
# cross-hub collection), small enough that many rounds complete under the timeout.
K = 24


def wvd_round(H, wid, rng, state):
    """One single-owner closed-world conservation round on a fiber-private WVD."""
    wvd = weakref.WeakValueDictionary()
    strong = {}                            # kid -> Val, THIS fiber's only strong refs

    # (1) Insert K stamped values, keeping a strong ref to each.
    for kid in range(K):
        v = Val(wid, kid)
        wvd[kid] = v
        strong[kid] = v
    del v                                  # else the loop var keeps a strong ref to
                                           # the LAST value, defeating its drop below

    # (2) YIELD: siblings churn their own WVDs and call gc.collect() on other hubs.
    runloom.yield_now()

    # (3) All K keys must be present and map to THIS fiber's (wid, kid) value.
    for kid in range(K):
        got = wvd.get(kid)
        if got is None:
            H.fail("WVD dropped a STRONGLY-HELD entry: key {0} vanished from the "
                   "fiber-private WVD across a yield although wid {1} still holds a "
                   "strong ref -- a KeyedRef callback spliced a live entry, or a "
                   "cross-hub gc reaped a referenced value".format(kid, wid))
            return
        if got.wid != wid or got.kid != kid:
            H.fail("WVD cross-fiber value LEAK: key {0} in wid {1}'s private WVD "
                   "maps to a value stamped ({2},{3}) -- this fiber's single-owner "
                   "value was overwritten by or returned a sibling's".format(
                       kid, wid, got.wid, got.kid))
            return
        got = None                         # release the transient strong ref

    # (4) Drop a KNOWN subset of strong refs, then force a collection.  The dropped
    # values have NO remaining strong ref (refcount -> 0), so their KeyedRef
    # callbacks must splice them out; the retained values are still strongly held,
    # so no collection -- local or cross-hub -- may reap them.
    ndrop = K // 2
    dropped = set(rng.sample(range(K), ndrop))
    for kid in dropped:
        del strong[kid]
    gc.collect()

    # (5) YIELD again: overlap this splice with siblings' inserts/collections.
    runloom.yield_now()

    # (6) EXACTLY the dropped keys vanished; EVERY retained key still maps correctly.
    for kid in range(K):
        got = wvd.get(kid)
        if kid in dropped:
            if got is not None:
                H.fail("WVD failed to drop a DEAD entry: key {0} (wid {1}) still "
                       "present after its last strong ref was dropped + gc.collect() "
                       "-- a KeyedRef callback was stranded, leaving a dead value "
                       "mapped (dangling/UAF)".format(kid, wid))
                return
        else:
            if got is None:
                H.fail("WVD dropped a RETAINED entry: key {0} vanished after "
                       "dropping an UNRELATED subset + gc.collect() although wid "
                       "{1} still holds its strong ref -- the splice removed the "
                       "wrong entry, or a cross-hub gc reaped a live value".format(
                           kid, wid))
                return
            if got.wid != wid or got.kid != kid:
                H.fail("WVD retained-entry CORRUPTION: key {0} (wid {1}) maps to a "
                       "value stamped ({2},{3}) after the collect -- the dict-splice "
                       "or a cross-hub gc corrupted this fiber's live entry".format(
                           kid, wid, got.wid, got.kid))
                return
            got = None

    # live-count == retained-strong-refs conservation.  len(wvd) counts live
    # entries (backing-dict size minus pending removals); it MUST equal the number
    # of strong refs this fiber still holds.
    live = len(wvd)
    retained = len(strong)
    if live != retained:
        H.fail("WVD live-count conservation broken: len(wvd)={0} but this fiber "
               "holds {1} strong refs (wid {2}) -- {3} entr{4} were {5} relative "
               "to the closed-world strong-ref set (splice under cross-hub gc "
               "lost or stranded a value)".format(
                   live, retained, wid, abs(live - retained),
                   "y" if abs(live - retained) == 1 else "ies",
                   "dropped" if live < retained else "stranded"))
        return

    state["rounds"][wid] += 1              # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        wvd_round(H, wid, rng, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # One rounds-completed slot per worker (wid-indexed => single writer, race-free;
    # feeds the non-vacuity law).  Allocated here where H.funcs is known.
    H.state = {
        "rounds": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rounds = sum(H.state["rounds"])
    H.log("WVD single-owner conservation rounds completed: {0} (every per-round "
          "closed-world live-entry + retained-payload + live-count check passed "
          "fail-fast); ops={1}".format(rounds, H.total_ops()))

    # NON-VACUITY: the load-bearing conservation arm actually exercised the
    # KeyedRef splice under cross-hub gc.  (Reaching post with no failure already
    # proves every per-round law held.)
    H.check(rounds > 0,
            "no WVD conservation rounds completed -- the KeyedRef splice / cross-"
            "hub gc.collect() window was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a KeyedRef
    # callback splice or inside gc).
    H.require_no_lost("weakvaluedict live-entry conservation")


if __name__ == "__main__":
    harness.main(
        "p543_weakvaluedict_live_entry_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="each fiber owns its OWN weakref.WeakValueDictionary + strong-ref "
                 "table (single-owner, never shared).  Closed-world conservation "
                 "law: insert K stamped values keeping strong refs, assert all "
                 "present+correct across a yield, drop a KNOWN subset + gc.collect() "
                 "(process-global, cross-hub), then assert EXACTLY the dropped keys "
                 "vanished, every retained key still maps to its (wid,kid) value, "
                 "and len(wvd)==retained strong refs.  A strongly-held entry "
                 "spliced away, a dead entry stranded, a cross-fiber value leak, or "
                 "a torn backing dict under cross-hub gc fails.  Distinct from p330 "
                 "(resurrect identity) and p77 (callback storm)")
