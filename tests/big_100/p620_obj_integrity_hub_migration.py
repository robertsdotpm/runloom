"""big_100 / 620 -- object integrity across a runloom hub migration.

A live Python object is created on one hub (producer), sent through a runloom
channel, and resumed/read on a DIFFERENT hub (consumer).  The consumer proves
the object survived the cross-hub transfer intact:
  * FIELD CHECKSUM equals the value independently re-derived from (chan, seq) --
    no field was corrupted in transit.
  * SELF-HASH the producer stamped INTO the object equals a fresh recompute on
    the consumer hub -- internal consistency after the move.
  * REFCOUNT SANITY: sys.getrefcount(obj) on the consumer hub is a small
    positive int in a plausible band (a cross-hub refcount UAF/corruption blows
    this out; freed-then-read raises).  getrefcount is biased/deferred
    cross-thread on 3.14t, so this is a BAND check, not an equality (p229).
Conservation: every object sent is received & verified exactly once
(produced == verified).  Coverage: a nonzero fraction of deliveries actually
crossed hubs (producer OS-thread != consumer OS-thread) -- else the test is
vacuous.

Stresses: FT object safety -- one live PyObject's memory touched from two hub
OS threads across a channel handoff, under M:N with the GIL off.  This is the
most probable free-threading-specific SILENT corruption class, which the crash/
conservation-counter oracles elsewhere in big_100 cannot see.
"""
import sys
import _thread                      # real OS-thread id == hub id (never patched)

import harness
import runloom

HUB_TID = _thread.get_ident        # current hub's OS thread id

SEND_COUNT = 64
BIG_CAP = 8
RC_LO, RC_HI = 1, 4096             # sane getrefcount band for a fresh payload


class Payload(object):
    __slots__ = ("chan", "seq", "a", "b", "c", "blob",
                 "stamp", "src_tid", "rc_before")

    def __init__(self, chan, seq):
        self.chan = chan
        self.seq = seq
        h = (chan * 2654435761 ^ (seq * 40503)) & 0xFFFFFFFF
        self.a = h
        self.b = (h * 1000003 + seq) & 0xFFFFFFFFFFFF
        self.c = (chan ^ seq ^ h) & 0xFFFFFFFF
        self.blob = bytes(((h >> (8 * (i & 3))) & 0xFF) for i in range(64))
        self.stamp = None
        self.src_tid = None
        self.rc_before = None


def field_checksum(p):
    """Pure fold over DATA fields only (excludes self.stamp)."""
    cs = 0
    for v in (p.chan, p.seq, p.a, p.b, p.c):
        cs = (cs * 1000003 + (v & 0xFFFFFFFFFFFF)) & 0xFFFFFFFFFFFF
    for byte in p.blob:
        cs = (cs * 1000003 + byte) & 0xFFFFFFFFFFFF
    return cs


def expected_checksum(chan, seq):
    """Ground truth: the checksum a correct Payload(chan, seq) MUST have,
    derived WITHOUT trusting the received object."""
    return field_checksum(Payload(chan, seq))


def setup(H):
    n = max(1, H.funcs // 2)                       # one producer+consumer / chan
    chans = [runloom.Chan(1 if (i & 1) == 0 else BIG_CAP) for i in range(n)]
    for ch in chans:
        H.register_close(ch)
    H.state = {
        "chans": chans,
        "nchan": n,
        "produced": [0] * n,                       # producer[i] only
        "verified": [0] * n,                       # consumer[i] only
        "mismatch": [0] * n,                       # consumer[i] only
        "rc_bad": [0] * n,                         # consumer[i] only
        "crosshub": [0] * n,                       # consumer[i] only
        "getrc_ok": [1],                           # single flag
    }


def producer(H, wid, rng, state):
    ch = state["chans"][wid]                        # wids [0, nchan)
    sent = 0
    for seq in range(SEND_COUNT):
        if not H.running():
            break
        p = Payload(wid, seq)
        p.stamp = field_checksum(p)
        p.src_tid = HUB_TID()
        try:
            p.rc_before = sys.getrefcount(p)
        except (AttributeError, TypeError):
            p.rc_before = None
        runloom.yield_now()                         # migrate holding a live obj
        try:
            ch.send(p)
        except Exception:
            break                                   # closed at teardown
        sent += 1
        H.op(wid)
    try:
        ch.close()
    except Exception:
        pass
    state["produced"][wid] = sent


def consumer(H, wid, rng, state):
    cidx = wid - state["nchan"]                     # wids [nchan, 2*nchan)
    ch = state["chans"][cidx]
    got = bad = rcbad = xhub = 0
    exp_seq = 0
    while True:
        try:
            p, ok = ch.recv()
        except Exception:
            break
        if not ok:
            break                                   # closed and drained
        runloom.yield_now()                         # resume/migrate holding it
        # ground truth from (channel, FIFO seq) -- independent of the object
        want = expected_checksum(cidx, exp_seq)
        cs = field_checksum(p)
        if (p.chan != cidx or p.seq != exp_seq or cs != want
                or p.stamp != cs):
            bad += 1
        if HUB_TID() != p.src_tid:
            xhub += 1
        try:
            rc = sys.getrefcount(p)
            if not (RC_LO <= rc <= RC_HI):
                rcbad += 1
        except (AttributeError, TypeError):
            state["getrc_ok"][0] = 0
        except Exception:
            rcbad += 1                               # corrupted -> raised
        exp_seq += 1
        got += 1
        H.op(wid)
    state["verified"][cidx] = got
    state["mismatch"][cidx] = bad
    state["rc_bad"][cidx] = rcbad
    state["crosshub"][cidx] = xhub
    if got:
        H.task_done(wid)


def body(H):
    n = H.state["nchan"]

    def both(H, wid, rng, state):
        if wid < state["nchan"]:
            producer(H, wid, rng, state)
        else:
            consumer(H, wid, rng, state)

    H.run_pool(2 * n, both, H.state)


def post(H):
    st = H.state
    produced = sum(st["produced"])
    verified = sum(st["verified"])
    mismatch = sum(st["mismatch"])
    rc_bad = sum(st["rc_bad"])
    crosshub = sum(st["crosshub"])
    H.check(mismatch == 0,
            "OBJECT CORRUPTION across hub migration: {0} field/hash mismatch(es)"
            .format(mismatch))
    H.check(produced == verified,
            "value LOSS/EXCESS: produced={0} verified={1}".format(
                produced, verified))
    if st["getrc_ok"][0]:
        H.check(rc_bad == 0,
                "REFCOUNT corruption: {0} objects had an out-of-band refcount "
                "on the consumer hub".format(rc_bad))
    H.check(crosshub > 0,
            "coverage: 0/{0} deliveries crossed hubs -- oracle never exercised "
            "cross-hub migration (raise --hubs or --funcs)".format(verified))
    H.log("produced={0} verified={1} mismatch={2} rc_bad={3} crosshub={4}/{1}"
          .format(produced, verified, mismatch, rc_bad, crosshub))


if __name__ == "__main__":
    # Cap scale: each func is a live Payload (64B blob) held in flight through a
    # channel, so RSS scales with funcs (~800MB/100k).  ~18% of deliveries cross
    # hubs, so 200k already gives tens of thousands of real cross-hub integrity
    # checks -- the oracle's value is correctness, not raw goroutine count.
    harness.main("p620_obj_integrity_hub_migration", body, setup=setup,
                 post=post, default_funcs=4000, max_funcs=200000,
                 describe="live Python object survives a cross-hub channel "
                          "handoff intact (field checksum + self-hash + "
                          "refcount sanity), produced==verified")
