"""big_100 / 225 -- cross-hub wake-eventfd coalescing fan-in storm.

WAKE_DEDUP (RUNLOOM_WAKE_DEDUP, default ON) coalesces the redundant per-hub
wake-eventfd writes that a burst of concurrent cross-hub wakes would otherwise
generate: a waker writes the kick eventfd only on the 0->1 transition of the
hub's wake_pending flag, and the pump clears-then-rechecks it (a Dekker-shaped
clear-then-recheck) when it drains the eventfd.  Spin-proven no-lost-kick
(verify/spin/netpoll_pump_kick.pml), but a regression there would strand an idle
hub: a coalesced-but-DROPPED kick leaves the parked consumer asleep forever.

This program intentionally manufactures that race.  One consumer goroutine
blocks on an UNBUFFERED channel recv (capacity 0 -> a full park each round).
Each round a barrier releases N producer goroutines, spread across all the hubs,
to kick that single idle consumer as close to simultaneously as possible (each
sends one tagged token on its own producer->consumer channel).  The consumer
must wake, drain exactly the N tokens it was sent, verify a per-round monotone
sequence (no missed round, no duplicated resume), bump the round, and re-park.
A coalesced-but-dropped kick = the consumer never wakes = no forward progress =
watchdog HANG (exit 3).  A spurious double-resume = the consumer observes a
token with no matching pending send = a sequence/accounting break (exit 1).

The two modes (RUNLOOM_WAKE_DEDUP=1 coalescing vs =0 every-write) must BOTH
pass; this module re-execs itself once per mode (a tiny sub-mode driver) when
the env var is unset, so a single invocation asserts =0 vs =1 parity.  The race
window is widened with RUNLOOM_WAKE_SKEW / RUNLOOM_DELAY if those are set.

The feature is netpoll-internal and present on every backend (epoll eventfd /
kqueue+select self-pipe), so there is no hard skip -- it runs everywhere; the
harness already enforces --hubs>=2, which is the only guard needed (a fan-in
across hubs is meaningless with one hub).

Stresses: Stresses: cross-hub pump-wake eventfd coalescing (WAKE_DEDUP) Dekker
clear-then-recheck under a fan-in storm -- N wakers on hubs A.. simultaneously
kick ONE parker idle on hub B; assert no lost wake (coalesced != dropped) and no
spurious double-resume; =0 vs =1 parity.
"""
import os
import sys

# RUNLOOM_WAKE_DEDUP is read ONCE per process (getenv cached at first park), so
# the =0/=1 parity cannot be exercised in one scheduler.  When the operator has
# NOT pinned a mode, re-exec ourselves once per mode as child processes and
# require BOTH to pass -- that is the "=0 vs =1 parity" assertion.  Each child
# sets the env BEFORE importing runloom (mandatory: mn_init/getenv caches it).
# A sentinel guards against a re-exec loop.  This must happen before any runloom
# import below.
_SUBMODE = os.environ.get("RUNLOOM_WAKE_DEDUP")
if _SUBMODE is None and os.environ.get("BIG100_DEDUP_PARITY_CHILD") != "1":
    import subprocess
    rc = 0
    for mode in ("1", "0"):
        env = dict(os.environ)
        env["RUNLOOM_WAKE_DEDUP"] = mode
        env["BIG100_DEDUP_PARITY_CHILD"] = "1"
        sys.stderr.write(
            "[p225_wake_dedup_fanin_storm] === sub-mode "
            "RUNLOOM_WAKE_DEDUP={0} ===\n".format(mode))
        sys.stderr.flush()
        cp = subprocess.run([sys.executable] + sys.argv, env=env)
        if cp.returncode != 0:
            sys.stderr.write(
                "[p225_wake_dedup_fanin_storm] sub-mode DEDUP={0} FAILED "
                "(exit {1}) -> =0/=1 parity broken\n".format(
                    mode, cp.returncode))
            rc = cp.returncode
    sys.exit(rc)

# A single mode is now pinned in the env; default to ON if somehow still unset.
os.environ.setdefault("RUNLOOM_WAKE_DEDUP", "1")

import harness        # noqa: E402
import runloom        # noqa: E402

# Producers per fan-in round: a meaty burst of simultaneous cross-hub kicks at
# the one idle consumer.  Bounded so memory/channel count stay flat at scale.
PRODUCERS = 64
# Channel send chunking note: each producer owns its own capacity-0 channel to
# the consumer, so every send is a real rendezvous (park on the empty side,
# wake on the other) -- maximal wake-eventfd traffic into the consumer's hub.


def setup(H):
    # One consumer; PRODUCERS producer->consumer rendezvous channels; one
    # release barrier channel per round so the burst is near-simultaneous.
    feeds = [runloom.Chan(0) for _ in range(PRODUCERS)]
    barrier = runloom.Chan(0)        # broadcast-by-N: producers each recv once
    for ch in feeds:
        H.register_close(ch)
    H.register_close(barrier)
    H.state = {
        "feeds": feeds,
        "barrier": barrier,
        # consumer-observed totals, single-writer (the consumer goroutine):
        "rounds_done": [0],
        "tokens_seen": [0],
        # spurious-resume / cross-talk guard: per (round,producer) one-bit seen
        # map is too big; instead the consumer tallies tokens per round and
        # asserts == PRODUCERS, and checks each token's round field matches.
        "dedup": os.environ.get("RUNLOOM_WAKE_DEDUP", "1"),
    }


def producer(H, wid, rng, state):
    """One producer per feed channel.  Each round: wait on the barrier (so the
    whole pack releases together), then kick the consumer with one token."""
    feeds = state["feeds"]
    barrier = state["barrier"]
    if wid >= PRODUCERS:
        return                        # only PRODUCERS feeds exist
    ch = feeds[wid]
    rnd = 0
    while H.running():
        # Block until the consumer's round-coordinator releases this round's
        # burst.  recv returns (val, ok); ok False == barrier closed at
        # teardown -> exit cleanly.
        val, ok = barrier.recv()
        if not ok:
            return
        rnd = val                     # the round number the consumer expects
        # Tag = (round, producer-id) so the consumer can verify it received
        # exactly this round's burst with no stale/duplicate token.
        try:
            ch.send((rnd, wid))
        except Exception:
            return                    # channel closed at teardown
        H.op(wid)


def consumer(H, wid, rng, state):
    """The single idle parker.  Each round it releases the producer pack via
    the barrier, then drains exactly PRODUCERS tokens (parking between kicks),
    verifying the fan-in burst arrived intact -- no lost wake, no spurious
    resume."""
    feeds = state["feeds"]
    barrier = state["barrier"]
    rounds_done = state["rounds_done"]
    tokens_seen = state["tokens_seen"]
    rnd = 0
    try:
        consume_rounds(H, wid, state, feeds, barrier, rounds_done, tokens_seen)
    finally:
        # The consumer DRIVES the rounds: once it is done (rounds exhausted,
        # deadline, or a failure) the producers must stop too.  They are parked
        # in barrier.recv() / their ch.send(), not polling H.running(), so close
        # both ends here to unpark them with ok=False -> they exit cleanly and
        # the run can drain (else they strand the join -> watchdog HANG).
        for ch in feeds:
            try:
                ch.close()
            except Exception:
                pass
        try:
            barrier.close()
        except Exception:
            pass


def consume_rounds(H, wid, state, feeds, barrier, rounds_done, tokens_seen):
    rnd = 0
    for _ in H.round_range():
        if not H.running():
            break
        rnd += 1
        # Release the whole producer pack for this round.  Each barrier.send is
        # a rendezvous with one waiting producer; doing all PRODUCERS sends
        # back-to-back arms the simultaneous fan-in of kicks at our feed
        # channels (which we then park on, one by one, below).
        released = 0
        for _ in range(PRODUCERS):
            if not H.running():
                break
            try:
                barrier.send(rnd)
            except Exception:
                return
            released += 1
        if released == 0:
            break

        # Now drain exactly `released` tokens.  Each feed recv PARKS the
        # consumer (capacity-0 channel) until that producer's wake lands -- the
        # exact path WAKE_DEDUP coalesces.  A coalesced-but-dropped kick means
        # one of these recvs never returns -> watchdog HANG (the bug we hunt).
        seen_mask = 0
        got = 0
        for _ in range(released):
            if not H.running():
                return
            ch = feeds[got]           # producers fill feeds[0..PRODUCERS) in id
            # ...but a producer may be slow; we don't know which feed fires
            # first, so recv on each feed in id order: every producer sends
            # exactly once per round, so each feed yields exactly one token.
            val, ok = ch.recv()
            if not ok:
                return
            r, pid = val
            # INVARIANT: no spurious / stale resume -- the token must belong to
            # THIS round and to the producer that owns this feed.  A double
            # resume or a coalesced kick that mis-delivered would break one of
            # these.
            if not H.check(r == rnd,
                           "stale/spurious resume: round {0} got token from "
                           "round {1} (pid {2}) -> dropped or duplicated "
                           "wake".format(rnd, r, pid)):
                return
            if not H.check(pid == got,
                           "feed cross-talk: feed {0} delivered producer {1}'s "
                           "token (round {2})".format(got, pid, r)):
                return
            bit = 1 << pid
            if not H.check((seen_mask & bit) == 0,
                           "DUPLICATE token: producer {0} delivered twice in "
                           "round {1} -> spurious double-resume".format(
                               pid, rnd)):
                return
            seen_mask |= bit
            got += 1
            H.op(wid)

        # INVARIANT: every released producer's kick was observed exactly once.
        # A coalesced-but-DROPPED kick would have stalled a recv above (caught
        # by the watchdog) rather than reach here short, but assert the count
        # too for a clean accounting oracle.
        if not H.check(got == released,
                       "lost wake: round {0} expected {1} tokens, drained {2} "
                       "-> a coalesced kick was dropped".format(
                           rnd, released, got)):
            return
        rounds_done[0] += 1
        tokens_seen[0] += got
        H.task_done(wid)


def worker(H, wid, rng, state):
    # wid 0 is the single consumer (the idle parker under fan-in); 1..PRODUCERS
    # are the producers.  Anything beyond is a no-op (we only need PRODUCERS+1).
    if wid == 0:
        consumer(H, wid, rng, state)
    else:
        producer(H, wid - 1, rng, state)


def body(H):
    # Exactly one consumer + PRODUCERS producers.  H.funcs is ignored on
    # purpose: this is a single-parker fan-in, not a per-goroutine pool, so the
    # shape is fixed regardless of the sweep's --funcs.
    H.run_pool(PRODUCERS + 1, worker, H.state)


def post(H):
    st = H.state
    rounds = st["rounds_done"][0]
    tokens = st["tokens_seen"][0]
    H.log("dedup={0} rounds={1} tokens={2} (expected {3} tokens/round)".format(
        st["dedup"], rounds, tokens, PRODUCERS))
    # Conservation: every completed round drained exactly PRODUCERS tokens.
    if rounds > 0:
        H.check(tokens == rounds * PRODUCERS,
                "token conservation: {0} tokens != rounds*{1} = {2} -> a wake "
                "was lost or duplicated".format(
                    tokens, PRODUCERS, rounds * PRODUCERS))


if __name__ == "__main__":
    harness.main("p225_wake_dedup_fanin_storm", body, setup=setup, post=post,
                 default_funcs=PRODUCERS + 1,
                 describe="fan-in storm at one idle hub: N cross-hub wakes kick "
                          "a single parker; assert no lost/dropped/duplicated "
                          "wake under RUNLOOM_WAKE_DEDUP=1 and =0")
