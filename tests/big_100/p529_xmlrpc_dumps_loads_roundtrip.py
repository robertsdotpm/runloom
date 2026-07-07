"""big_100 / 529 -- xmlrpc.client dumps/loads round-trip conservation under M:N.

xmlrpc.client is a PURE, OFFLINE string transform for the serialization core:
`dumps(params, methodname=name)` walks a Marshaller over the param tuple and
emits an XML-RPC methodCall document (a str/bytes), and `loads(xml)` drives a
fresh SAX parser + Unmarshaller that ACCUMULATES parse state in per-instance
attributes -- `_stack` (the value stack it pushes decoded values onto),
`_marks` (array/struct boundary marks), `_value`/`_type` (the element currently
being decoded), `_methodname`.  No network, no sockets: dumps and loads are
deterministic functions of their string input.

WHERE M:N COULD BREAK IT (the hazard this program probes).  Each `loads()` call
constructs its OWN parser + Unmarshaller (getparser() news them up per call),
so the Unmarshaller's `_stack`/`_marks`/`_value` accumulation is, by design,
private to that one call on that one fiber.  runloom gives every fiber its own
Python frame stack and its own C stack; a correctly isolated runtime keeps each
fiber's Unmarshaller instance state entirely local.  The hazard: if instance
state leaked across hubs -- e.g. a sibling fiber mid-`loads()` on another hub
pushed a decoded value onto THIS fiber's Unmarshaller `_stack`, or the parser's
character-data buffer bled between the two in-flight decodes across a yield --
then a fiber's `loads()` would return a value tuple that is NOT the round-trip
of the string it dumped: an extra element, a torn element, a value carrying a
sibling's wid.  That is a cross-fiber leak of single-owner parser state.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world CONSERVATION law):

  Each fiber OWNS a param tuple whose every element embeds its own (wid, idx):
  an int, a str, a bool, a double, an xmlrpc.client.Binary, and a DateTime, plus
  a wid-embedded methodname.  The fiber serializes it with `dumps(...)` to a
  private XML string (single-owner: nobody else can see or touch that string),
  YIELDS (so a sibling's dumps/loads reliably interleaves on the same or another
  hub), then `loads()` the string back and asserts the recovered (params, name)
  is ELEMENT-FOR-ELEMENT EQUAL to what it dumped -- Binary compares by wrapped
  bytes, DateTime by wrapped value string, the scalars by value.  Every unit put
  in must come back out, unchanged, exactly once: a CONSERVATION law over the
  serialize->yield->deserialize round-trip.  On a correct runtime this holds
  100% (verified: all six types round-trip and compare equal offline), so the
  program EXITS 0 when there is no bug.

  The conservation is closed-world and falsifiable per fiber:
    * len(recovered_params) == len(original_params)  (no extra/dropped element
      from a leaked sibling push onto _stack, no truncated tuple);
    * recovered_name == original wid-embedded methodname (the _methodname slot
      was not overwritten by a sibling's parse);
    * each recovered element == the corresponding original element AND carries
      THIS fiber's wid (a sibling's value would carry a different wid -- a
      cross-fiber leak of Unmarshaller state).
  A per-wid race-free counter (one slot per fiber, single-writer) tallies
  successful round-trips; post() asserts the global sum is non-zero (non-vacuity)
  and that no fiber vanished mid-parse (completeness).

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).  Single-
    owner dumps->yield->loads equality per the closed-world law above.  The XML
    string is fiber-private; a mismatch means Unmarshaller/parser instance state
    leaked across fibers (or a torn value), which is a runloom isolation bug.
  * NON-VACUITY (post, HARD): the round-trip arm actually ran (roundtrips > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside loads()
    (parked mid-parse on a desynced Unmarshaller) never returns; watchdog catches.

There is deliberately NO shared-mutable arm: xmlrpc dumps/loads have no shared
aggregate object here (each call news its own Marshaller/Unmarshaller), so the
hazard is purely per-call instance-state isolation, which the single-owner
oracle tests directly.  A FAIL means a real cross-fiber leak of parser state or
a torn serialized value -- never documented Python semantics.

Stresses: xmlrpc.client Marshaller.dumps over a mixed-type param tuple, the
per-call getparser()/Unmarshaller _stack/_marks/_value accumulation across a
yield between dumps and loads, Binary/DateTime wrapper round-trip equality,
per-fiber serializer/deserializer instance isolation under M:N with hubs>1.

Good TSan / controlled-M:N-replay target: the Unmarshaller `_stack.append` and
the parser character-data buffer are per-instance mutable state; a data-race
report on one fiber's _stack while a sibling parses, or a replay that returns a
tuple with a sibling's wid, localizes the leak before the equality oracle fires.
"""
import xmlrpc.client as xmlrpc

import harness
import runloom

# Per-fiber value band.  Every element of a fiber's param tuple embeds its wid so
# a value that leaked from a sibling carries a DIFFERENT wid and is caught by the
# equality + wid-check oracle.  BASE spaces wids far apart so int/double values
# never collide across fibers.
BASE = 1000000
# Fixed epoch anchor for the DateTime element; wid+idx are folded in so the
# formatted value string differs per fiber/iteration.  strftime is deterministic
# for a given input within a run.
EPOCH_ANCHOR = 1735689600            # 2025-01-01T00:00:00 UTC-ish anchor

# Sustained round-trips per worker, bounded by H.running().  The instance-state
# isolation hazard only manifests under SUSTAINED churn: many fibers each with a
# dumps->yield->loads in flight, so a sibling's parse reliably overlaps this
# fiber's between its dumps and its loads.  One round-trip per fiber barely
# overlaps a sibling's and does not reproduce a leak.
INNER_CAP = 100000


def build_params(wid, idx):
    """Build this fiber's SINGLE-OWNER param tuple, every element embedding
    (wid, idx).  Returns (params_tuple, methodname).  All six types round-trip
    exactly through dumps/loads offline (int, str, bool, double, Binary,
    DateTime); a recovered element that is unequal -- or carries another wid --
    is a cross-fiber leak of Unmarshaller state."""
    n = wid * BASE + idx
    i_val = n                                    # <int>
    s_val = "w{0}_i{1}".format(wid, idx)         # <string>
    b_val = bool((wid + idx) & 1)                # <boolean>
    d_val = float(n) + 0.5                        # <double> (repr round-trips exact)
    bin_val = xmlrpc.Binary(
        "payload-{0}-{1}".format(wid, idx).encode("ascii"))   # <base64> -> Binary
    dt_val = xmlrpc.DateTime(EPOCH_ANCHOR + (wid % 100000) * 60 + (idx % 60))  # <dateTime>
    params = (i_val, s_val, b_val, d_val, bin_val, dt_val)
    methodname = "m{0}_{1}".format(wid, idx)
    return params, methodname


def elems_equal(a, b):
    """Element equality that also pins wid-bearing identity.  Binary compares by
    .data, DateTime by .value, scalars by ==; every path is a by-VALUE compare so
    a torn or leaked element (different bytes / different value string / different
    number) is unequal."""
    if isinstance(a, xmlrpc.Binary):
        return isinstance(b, xmlrpc.Binary) and a.data == b.data
    if isinstance(a, xmlrpc.DateTime):
        # DateTime.__eq__ compares wrapped value strings; guard the type too.
        return isinstance(b, xmlrpc.DateTime) and a.value == b.value
    return type(a) is type(b) and a == b


def roundtrip_check(H, wid, idx, state):
    """Single-owner dumps->yield->loads conservation check.

    dumps() the fiber's private param tuple to a private XML string, YIELD (a
    sibling's dumps/loads interleaves here), then loads() it back and assert the
    recovered (params, name) is element-for-element equal to the original.  A
    mismatch is a cross-fiber leak of per-call Marshaller/Unmarshaller state."""
    params, methodname = build_params(wid, idx)

    # Serialize (single-owner: xml is a private str nobody else references).
    xml = xmlrpc.dumps(params, methodname=methodname)

    # YIELD: a sibling fiber's dumps/loads runs here, possibly on another hub.
    # If parser/Unmarshaller instance state leaked across fibers, the loads()
    # below would recover a corrupted tuple.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Deserialize (getparser() news up a fresh parser + Unmarshaller per call).
    recovered, recovered_name = xmlrpc.loads(xml)

    # ---- closed-world round-trip conservation law -----------------------------
    # (1) methodname conserved (the _methodname slot was not overwritten).
    if recovered_name != methodname:
        H.fail("xmlrpc round-trip methodname CORRUPTED: dumped {0!r} but loads() "
               "returned {1!r} (wid {2}, idx {3}) -- the Unmarshaller _methodname "
               "slot was overwritten across a yield (cross-fiber parser leak)".format(
                   methodname, recovered_name, wid, idx))
        return

    # (2) arity conserved: no extra element pushed onto _stack by a sibling, none
    #     dropped/truncated.
    if len(recovered) != len(params):
        H.fail("xmlrpc round-trip ARITY CHANGED: dumped {0} params but loads() "
               "returned {1} (wid {2}, idx {3}) -- a sibling's decoded value was "
               "pushed onto this fiber's Unmarshaller _stack, or the tuple was "
               "truncated (cross-fiber parser-state leak)".format(
                   len(params), len(recovered), wid, idx))
        return

    # (3) each element conserved, by value, carrying THIS fiber's wid.  A leaked
    #     sibling value would compare unequal (different wid embedded).
    for pos, (orig, got) in enumerate(zip(params, recovered)):
        if not elems_equal(orig, got):
            H.fail("xmlrpc round-trip ELEMENT CORRUPTED at position {0}: dumped "
                   "{1!r} but loads() returned {2!r} (wid {3}, idx {4}) -- the "
                   "recovered element is not the round-trip of the dumped one; a "
                   "sibling's Unmarshaller state leaked into this fiber's parse or "
                   "the value was torn".format(pos, orig, got, wid, idx))
            return

    state["roundtrips"][wid] += 1        # ONE slot per worker (race-free; wid-indexed)


def worker(H, wid, rng, state):
    """Each fiber runs a SUSTAINED stream of single-owner dumps->yield->loads
    conservation checks (fail-fast).  The XML strings and param tuples are all
    fiber-private, so nothing is shared across fibers -- the only way the oracle
    fails is a runloom leak of per-call parser/Unmarshaller instance state."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE race-free slot per worker (single-writer-per-slot; wid-indexed) tallies
    # successful round-trips.  Allocated here where H.funcs is known.
    H.state = {
        "roundtrips": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("xmlrpc dumps/loads round-trips conserved this run: {0} (every single-"
          "owner element-for-element conservation check passed fail-fast); "
          "ops={1}".format(rts, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip conservation arm actually ran.
    H.check(rts > 0,
            "no xmlrpc round-trip conservation checks completed -- the single-"
            "owner dumps->yield->loads isolation hazard was never exercised "
            "(oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside loads()
    # on a desynced Unmarshaller mid-parse).
    H.require_no_lost("xmlrpc dumps/loads round-trip conservation")


if __name__ == "__main__":
    harness.main(
        "p529_xmlrpc_dumps_loads_roundtrip", body, setup=setup, post=post,
        default_funcs=6000,
        describe="xmlrpc.client dumps/loads are pure offline string transforms; "
                 "loads() news a per-call parser + Unmarshaller that accumulates "
                 "parse state in _stack/_marks/_value.  LOAD-BEARING: each fiber "
                 "owns a wid-embedded param tuple (int/str/bool/double/Binary/"
                 "DateTime), dumps() it to a private XML string, yields, then "
                 "loads() it back and asserts element-for-element equality -- a "
                 "closed-world conservation law over serialize->yield->deserialize. "
                 "A recovered tuple with wrong arity, a corrupted methodname, or "
                 "an element carrying a sibling's wid is a cross-fiber leak of "
                 "Unmarshaller instance state (a runloom isolation bug)")
