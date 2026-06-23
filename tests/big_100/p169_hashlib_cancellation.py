"""big_100 / 169 -- hashlib incremental hashing under cancellation.

Goroutines hash large buffers (>256 KiB so runloom's size-gated auto-offload of
hashlib kicks in) in chunks via hashlib.  Each hash runs inside a cancellable
scope (a context with a short jittered timeout); SOME are cancelled mid-update.
A cancelled hash is simply DISCARDED; a hash that runs to completion must equal
the precomputed reference digest of its exact input.  This stresses the
offload-pool + cooperative cancellation interaction (FINDINGS #4 family):
cancellation must not deliver a sibling's digest to the wrong owner, corrupt the
shared offload pool, or wedge.

Stresses: size-gated hashlib auto-offload, cancellation mid-offload, offload
pool integrity, digest correctness.
"""
import hashlib

import harness
import runloom
import cancelutil

CHUNK = 64 * 1024
NCHUNKS = 8                     # 8 * 64KiB = 512 KiB total -> auto-offload range


def setup(H):
    # A small set of distinct payloads (keyed by a per-goroutine byte) plus the
    # precomputed reference digest for each.  Each goroutine derives its own
    # payload from wid so a completed digest is checkable against a known answer.
    H.completed = [0] * H.funcs
    H.cancelled = [0] * H.funcs
    H.state = {}


def make_payload(wid):
    # Deterministic, distinct per goroutine, >= 512 KiB.
    seed = bytes(((wid + i) & 0xFF) for i in range(64))
    block = (seed * (CHUNK // len(seed) + 1))[:CHUNK]
    return [bytes((b ^ ((wid + c) & 0xFF)) for b in block) for c in range(NCHUNKS)]


def worker(H, wid, rng, state):
    completed = H.completed
    cancelled = H.cancelled
    rnd = 0
    for _ in H.round_range():
        chunks = make_payload(wid + (rnd << 20))
        # Reference digest of the EXACT input (computed in one shot).
        ref = hashlib.sha256()
        for c in chunks:
            ref.update(c)
        ref_digest = ref.hexdigest()

        # Decide up front whether this round will be cancelled mid-update.
        will_cancel = (rng.random() < 0.4)
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())

        if will_cancel:
            # Cancel after a tiny delay so it lands somewhere mid-update.  A
            # fraction cancel essentially immediately (delay 0) so the very first
            # inter-chunk ctx.err() check catches it; the rest race the offload.
            delay = 0.0 if (rng.random() < 0.5) else rng.uniform(0.0001, 0.002)
            runloom.fiber(cancelutil.delayed_cancel, cancel, delay)

        h = hashlib.sha256()
        aborted = False
        try:
            for c in chunks:
                if ctx.err() is not None:
                    aborted = True
                    break
                # The actual hashing -- >256KiB cumulative triggers the size-gated
                # auto-offload; the goroutine parks on the offload pool here.
                h.update(c)
                # Yield so a concurrent cancel + other goroutines' offloads
                # interleave on the pool.
                runloom.yield_now()
        finally:
            if not will_cancel:
                cancel()       # release the context regardless

        rnd += 1
        if aborted:
            cancelled[wid] += 1
            H.op(wid)
            continue

        # Ran to completion -> the digest MUST match its own reference.  A wrong
        # digest here would mean a sibling's offload result was delivered to this
        # owner, or the offloaded hash state was corrupted.
        if not H.check(h.hexdigest() == ref_digest,
                       "digest mismatch wid={0} rnd={1}: got {2} want {3}".format(
                           wid, rnd, h.hexdigest()[:16], ref_digest[:16])):
            return
        completed[wid] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    comp = sum(H.completed)
    canc = sum(H.cancelled)
    H.check(comp > 0, "no hash ever completed (all cancelled?)")
    H.log("completed={0} cancelled={1}".format(comp, canc))


if __name__ == "__main__":
    harness.main("p169_hashlib_cancellation", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="large hashlib hashes via auto-offload, some cancelled "
                          "mid-update; completed digests exact")
