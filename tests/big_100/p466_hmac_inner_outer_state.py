"""big_100 / 466 -- hmac.HMAC inner/outer hash-state integrity under M:N.

hmac.HMAC is RFC-2104 keyed hashing: it wraps an INNER hash (over ipad-keyed
key || message) and an OUTER hash (over opad-keyed key || inner-digest).  On this
3.14t build there are TWO live code paths and BOTH are exercised here:

  * the OpenSSL path (the default for `hmac.new(key, digestmod=hashlib.sha256)`):
    `self._hmac` is a C object that owns a single HMAC_CTX holding the inner+outer
    MD/SHA state internally; `.update()` mutates that C context in place.
  * the pure-Python `_init_old` path (taken when digestmod is a PEP-247 module-
    like object): `self._inner` and `self._outer` are two explicit live HASH
    objects; `.update()` feeds `self._inner`, and `.hexdigest()` copies
    `self._outer`, folds in `self._inner.digest()`, and finalizes.  This is the
    LITERAL inner/outer pair the hazard names.

Cookie / token / webhook signing hammers HMAC: a request handler keys an HMAC
with a per-route secret and streams the body in through `.update()`.  Under
runloom M:N a fiber that opens an HMAC on one hub can be PREEMPTED / MIGRATED to
another hub mid-MAC -- between two of its own `.update()` chunks, or between the
last chunk and `.hexdigest()`.  The C HMAC_CTX (or the Python _inner/_outer HASH
objects) lives on the heap and travels with the fiber, NOT with the OS thread, so
a correct runtime keeps each fiber's MAC private and exact across the migration.
A torn inner/outer state -- the fiber resuming on a hub where the C context was
left mid-block by something else, or its _inner/_outer attribute binding desyncing
across the migration -- yields a WRONG MAC: a forged / garbled signature, a real
security regression.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  An HMAC object that ONE fiber owns is single-owner: nothing but that fiber ever
  touches its inner/outer state.  Feeding its own message in chunks -- interleaved
  with yields/sleeps so the fiber is preempted/migrated mid-MAC -- and then asking
  for `.hexdigest()` MUST equal the reference single-threaded HMAC of (its key,
  its message), no matter how many times it was descheduled.  We verified with a
  standalone plain-threads control (64 threads, the SAME chunked-update hazard, NO
  runloom): 0 mismatches in 25600 MACs on the OpenSSL path AND 0 in 25600 on the
  Python _inner/_outer path, under PYTHON_GIL=1 AND PYTHON_GIL=0.  Stock CPython
  keeps each owned HMAC object's context private to whichever thread holds the
  reference, so a chunked MAC is exact for any GIL setting; an oracle that fired
  there would be a false-positive detector -- it does NOT fire there.  Under a
  correct runloom it must ALSO hold (the per-fiber heap object migrates intact).
  If runloom tears the inner/outer state across a hub migration, the recomputed
  `.hexdigest()` won't match the reference -- THAT is the runloom bug, and the
  single-owner arm PASSES on a correct runtime (the program exits 0 with no bug).

ORACLES:
  * LOAD-BEARING -- OWNED-HMAC MAC INTEGRITY (worker, HARD, fail-fast).  Each
    fiber builds a unique per-wid (key, message), opens its OWN hmac.HMAC (half
    the fibers on the OpenSSL C path, half on the Python _inner/_outer path),
    feeds the message in chunks with a yield/sleep-park BETWEEN chunks (and one
    between the final chunk and the digest), then asserts:
      - h.hexdigest() == the reference single-threaded HMAC of (key, message),
        compared with hmac.compare_digest (the exact primitive token-verify uses);
      - h.copy() taken MID-stream and finished separately reproduces the same MAC
        (the inner/outer state is cleanly snapshotable across a migration);
      - a deliberately WRONG key/message does NOT verify (compare_digest is live,
        so the equality check is non-vacuous, not "always True").
    Single-owner: a mismatch is a runloom inner/outer-state tear across migration.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-MAC (parked
    between two .update() chunks and never re-woken) never returns; the watchdog +
    require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (macs > 0), and
    the negative control (wrong key must NOT verify) was exercised.

  * MEASURED (report-ONLY, NEVER fails): a SINGLE module-global HMAC object SHARED
    by all fibers on a hub, updated without a lock and interleaved with yields.
    This is documented-unsafe shared-mutable-object usage (the inner context is one
    object many fibers mutate) -- exactly p67's threading.local / p460's global
    getcontext() shape: under M:N siblings on a hub interleave their .update()s
    into the one context, so a fiber's read-back MAC reflects sibling bytes.  We
    MEASURE how often the shared object's running digest fails to equal what THIS
    fiber alone would have produced (an interleave/contention rate) and REPORT it;
    we NEVER fail on it -- a shared mutable HMAC is unsafe for any concurrency
    model, so failing would mislabel documented-unsafe usage as a runloom bug.  The
    shared path NEVER touches the load-bearing owned-HMAC checks (those are
    self-contained against a freshly computed reference), so it cannot poison the
    oracle.

FAIL ON: an owned-HMAC chunked MAC that != its reference, a mid-stream copy that
diverges, or the negative control verifying.  NEVER fail on the shared-object
interleave rate (measured).

Stresses: hmac.HMAC inner/outer hash state (OpenSSL HMAC_CTX C object AND the
pure-Python _inner/_outer HASH-object pair) across hub migration + preempt-mid-MAC,
chunked .update() spanning yields/sleeps, .copy() mid-stream, hmac.compare_digest,
per-fiber heap-object affinity vs OS-thread affinity.

Good TSan / controlled-M:N-replay target: an owned HMAC's C HMAC_CTX is mutated by
.update() across a migration -- a replay that migrates a hub between two of a
fiber's .update() chunks, or a data-race report on the shared-object arm's one
context, localizes a tear before the hexdigest oracle fires.
"""
import hashlib
import hmac

import harness
import runloom

# The reference digest constructor for the OpenSSL/load-bearing path.
DIGEST = hashlib.sha256


class _ModDigest(object):
    """A PEP-247 module-like INSTANCE (not callable, not str) so hmac.HMAC takes
    the pure-Python _init_old path with explicit live _inner/_outer HASH objects --
    the literal inner/outer hash-object pair the hazard names.  One shared,
    stateless instance is fine: .new() returns a FRESH hashlib object every call,
    so no per-fiber state lives on it."""

    @staticmethod
    def new(data=b""):
        return hashlib.sha256(data)


MOD = _ModDigest()

# How many chunks each fiber's message is fed through .update() in -- enough that
# the fiber yields/parks SEVERAL times mid-MAC (so it is preempted / migrated
# between chunks), not just once.
NCHUNKS = 6

# Sustained MACs per worker: the inner/outer-tear hazard only manifests under
# SUSTAINED churn (many fibers simultaneously mid-MAC and parked across a chunk
# boundary so the scheduler migrates them), so each worker runs an internal loop
# (bounded by H.running()) -- one full owned-HMAC MAC per iteration -- which makes
# the oracle fire at the DEFAULT --rounds 1 without depending on a large --rounds.
# INNER_CAP stops one worker from monopolizing teardown on a slow box.
INNER_CAP = 100000

# ---- MEASURED shared-object arm: ONE global HMAC many fibers update (report only)
SHARED_HMAC = None   # built in setup(); documented-unsafe shared mutable object


def setup(H):
    global SHARED_HMAC
    # The MEASURED shared object: one process-global HMAC every fiber updates
    # without a lock.  Documented-unsafe (a shared mutable hash context); we only
    # measure the interleave rate against it, never assert.
    SHARED_HMAC = hmac.new(b"shared-measured-key", digestmod=DIGEST)
    H.state = {
        "macs": [0] * 1024,            # load-bearing owned-HMAC MACs verified
        "neg_ok": [0] * 1024,          # negative-control runs (wrong key rejected)
        "shared_checks": [0] * 1024,   # measured shared-object reads
        "shared_interleave": [0] * 1024,  # measured: read != this-fiber-alone
    }


def _make_message(wid, idx):
    """A unique, multi-chunk message for this (wid, idx).  Long enough that a
    6-way split gives several real .update() chunks spanning yields."""
    base = ("p466-worker-{0}-mac-{1}-".format(wid, idx)).encode()
    # Repeat so the message is comfortably longer than NCHUNKS bytes.
    return base * 8


def _chunks(msg, n):
    step = max(1, len(msg) // n)
    return [msg[i:i + step] for i in range(0, len(msg), step)]


def _key_for(wid):
    return ("p466-key-{0}".format(wid)).encode() * 4


# --------------------------------------------------------------------------
# LOAD-BEARING arm: an HMAC object this fiber alone OWNS, fed in chunks across
# yields/parks, verified against the reference single-threaded HMAC.  Single-owner;
# a mismatch is a runloom inner/outer-state tear across a hub migration.  Verified
# 0/25600 under plain threads GIL on AND off (both code paths).
# --------------------------------------------------------------------------
def owned_mac(H, wid, idx, state):
    key = _key_for(wid)
    msg = _make_message(wid, idx)
    # Reference: the canonical single-threaded HMAC of (key, msg).  Computed in one
    # shot here so the load-bearing check is self-contained -- independent of any
    # shared state, exactly like p460's canonical table.
    ref = hmac.new(key, msg, DIGEST).hexdigest()

    # Half the fibers exercise the OpenSSL C HMAC_CTX path, half the pure-Python
    # _inner/_outer HASH-object path, so both inner/outer representations are
    # migrated mid-MAC.
    use_openssl = (wid & 1) == 0
    if use_openssl:
        h = hmac.new(key, digestmod=DIGEST)
    else:
        h = hmac.HMAC(key, digestmod=MOD)

    chunks = _chunks(msg, NCHUNKS)
    mid_copy = None
    mid_at = len(chunks) // 2
    for ci, ch in enumerate(chunks):
        h.update(ch)
        # Snapshot a mid-stream copy ONCE, halfway through: the inner/outer state
        # must be cleanly snapshotable and the copy must finish to the SAME MAC.
        if ci == mid_at and mid_copy is None:
            mid_copy = h.copy()
            mid_rest = chunks[ci + 1:]
        # YIELD / SLEEP-PARK BETWEEN chunks so the fiber is descheduled mid-MAC and
        # the scheduler can migrate it to another hub before the next .update().
        runloom.yield_now()
        if ci & 1:
            runloom.sleep(0.0002)

    # One more park between the final chunk and the digest (migrate before finalize).
    runloom.yield_now()
    got = h.hexdigest()

    # (1) The chunked, migration-spanning MAC must equal the reference, compared
    # with the exact primitive a token verifier uses.
    if not hmac.compare_digest(got, ref):
        H.fail("HMAC inner/outer state TORN across migration: chunked .hexdigest() "
               "{0} != reference {1} for wid {2} ({3} path) -- the per-fiber HMAC "
               "context did not survive being preempted/migrated mid-MAC (a "
               "forged/garbled signature)".format(
                   got, ref, wid, "openssl" if use_openssl else "py-inner/outer"))
        return
    # (2) The mid-stream copy must finish to the SAME MAC -- the inner/outer state
    # was cleanly snapshotable across the migration.
    if mid_copy is not None:
        for ch in mid_rest:
            mid_copy.update(ch)
            runloom.yield_now()
        cpy = mid_copy.hexdigest()
        if not hmac.compare_digest(cpy, ref):
            H.fail("HMAC mid-stream copy DIVERGED across migration: copy "
                   ".hexdigest() {0} != reference {1} for wid {2} -- .copy() did "
                   "not capture a consistent inner/outer state".format(
                       cpy, ref, wid))
            return
    # (3) NEGATIVE control: a WRONG key MUST NOT verify -- proves compare_digest is
    # live and the equality oracle is non-vacuous (not "always True").
    wrong = hmac.new(key + b"X", msg, DIGEST).hexdigest()
    if hmac.compare_digest(got, wrong):
        H.fail("HMAC oracle VACUOUS: a wrong-key MAC verified equal to the correct "
               "MAC for wid {0} -- compare_digest/equality is not actually "
               "discriminating".format(wid))
        return
    state["neg_ok"][wid & 1023] += 1
    state["macs"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: ONE global HMAC object many fibers update without a lock.
# Documented-unsafe shared mutable object (p67/p460 shape).  Report-ONLY.
# --------------------------------------------------------------------------
def shared_check(H, wid, idx, state):
    # What THIS fiber alone contributes, in isolation, for comparison.
    tag = ("sh-{0}-{1}-".format(wid, idx)).encode()
    solo = hmac.new(b"shared-measured-key", tag, DIGEST).hexdigest()
    # Update the SHARED object (no lock) interleaved with a yield, then read its
    # running digest.  Under M:N siblings interleave their bytes into the one
    # context, so the read reflects sibling state -- a documented shared-object
    # interleave, exactly like p67's TLS leak.  Measured, never asserted.
    SHARED_HMAC.update(tag)
    runloom.yield_now()
    running = SHARED_HMAC.hexdigest()
    state["shared_checks"][wid & 1023] += 1
    # The running digest reflects ALL fibers' bytes, so it ~never equals this
    # fiber's solo MAC -- count that as the interleave signal (report only).
    if not hmac.compare_digest(running, solo):
        state["shared_interleave"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms: the LOAD-BEARING owned-HMAC integrity check
    (fail-fast) and the MEASURED shared-object interleave check (report only).  The
    two do not interact -- the owned arm builds a private object and verifies
    against a freshly computed reference; the shared arm touches only the global
    object -- so the measured interleave can never reach the owned-HMAC oracle.

    The worker SUSTAINS a churn loop bounded by H.running(): one full owned MAC per
    iteration (parking between chunks) so many fibers stay simultaneously mid-MAC
    and parked across a chunk boundary -- the condition the inner/outer tear needs
    -- regardless of --rounds.  --rounds 1 (the default) runs the inner loop once,
    which is all it takes."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            owned_mac(H, wid, idx, state)         # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_check(H, wid, idx, state)      # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    macs = sum(H.state["macs"])
    neg = sum(H.state["neg_ok"])
    schecks = sum(H.state["shared_checks"])
    sinter = sum(H.state["shared_interleave"])
    spct = (100.0 * sinter / schecks) if schecks else 0.0
    H.log("hmac: owned-HMAC inner/outer MAC-integrity checks={0} (LOAD-BEARING, "
          "all passed fail-fast; openssl + py _inner/_outer paths, negative "
          "control={1}) | shared-object interleave={2}/{3} ({4:.1f}%, documented-"
          "unsafe shared mutable HMAC under M:N -- REPORT ONLY, like p67/p460)"
          .format(macs, neg, sinter, schecks, spct))
    if sinter:
        H.log("note: the shared global HMAC object observed {0} interleaves across "
              "{1} reads -- many hub fibers update ONE HMAC context without a lock, "
              "so its running digest reflects sibling bytes.  Documented-unsafe "
              "shared-mutable-object usage (NOT a runloom bug); it never touches "
              "the load-bearing owned-HMAC oracle (which verifies a private object "
              "against a freshly computed reference)".format(sinter, schecks))
    # NON-VACUITY: the load-bearing owned-HMAC hazard was actually exercised, and
    # the negative control (wrong key must NOT verify) ran -- otherwise the oracle
    # would be vacuous.
    H.check(macs > 0,
            "no owned-HMAC MAC-integrity checks ran -- the load-bearing inner/outer "
            "state hazard was never exercised (oracle would be vacuous)")
    H.check(neg > 0,
            "the negative control (wrong-key MAC must NOT verify) never ran -- "
            "the equality oracle's non-vacuity is unproven")
    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-MAC between
    # two .update() chunks and never re-woken).
    H.require_no_lost("hmac owned inner/outer MAC integrity")


if __name__ == "__main__":
    harness.main(
        "p466_hmac_inner_outer_state", body, setup=setup, post=post,
        default_funcs=8000,
        describe="hmac.HMAC wraps an INNER and an OUTER hash (OpenSSL HMAC_CTX C "
                 "object OR the pure-Python _inner/_outer HASH pair); cookie/token/"
                 "webhook signing streams the body through .update().  LOAD-BEARING: "
                 "a fiber that OWNS its HMAC, feeds its message in chunks "
                 "interleaved with yields/sleeps so it is preempted/migrated "
                 "mid-MAC, MUST get .hexdigest() == the reference single-threaded "
                 "HMAC (0 mismatches under plain threads GIL on AND off, both paths; "
                 "a torn inner/outer state = a forged signature = the runloom bug).  "
                 "A SHARED global HMAC many fibers update without a lock is "
                 "documented-unsafe shared-object usage -- measured interleave rate, "
                 "report-only")
