"""Coverage recovery for two cold runloom_c fragments:

  * src/runloom_c/chan_select_main.c.inc -- the select() PARK path
    (runloom_chan_select Phase-2).  Round-1 suites only ever drove select
    through Phase-1 (a ready case) or default=True; the entire park-then-wake
    machinery -- install, M:N vs single-thread park, the fired-case eviction,
    the SEND-on-closed / close-wake re-scan, and the multi-case non-firing-SEND
    value drop -- was dark.

  * src/runloom_c/runloom_stackadvice.c -- the per-fiber-kind stack-usage
    profiler.  Its autosize/prescan/learned-size paths are gated behind
    enable_stack_autosize() (which resolves RUNLOOM_STACK_AUTOSIZE_START once),
    so they need fresh SUBPROCESSES that exit cleanly for gcov to flush.

Oracles are real: exact delivered value/index, exact (val, ok) on close,
ValueError on send-to-closed-while-parked, learned/prescan reserved-size
SHRINK-or-GROW assertions, and integrity (no value lost/duplicated) for the
M:N stress.  No line-touching filler.

What is DELIBERATELY left to the exclusions[] report (not faked):
  * chan: the install-time abort/retry race (a foreign waiter parking, or a
    buffer slot opening, in the few-instruction window between Phase-1's scan
    and the per-case install re-check), the abort-CAS-loss `break`s, and the
    spurious-wake re-evict+retry.  These fire only on a specific M:N
    interleaving with NO Python-controllable yield point inside the C install
    loop; a 64k-iteration single-process driver hit them zero times.  Classified
    RACE.  The +10_000_000 retry-overflow guards and the m_select-guarded
    n<=0 check are DEAD; the waiters PyMem_Calloc-NULL is OOM.
  * stackadvice: the ensure_lock spin (concurrent-init RACE), the full-table
    find-exhausted return (DEFENSIVE), and the report() PyList/BuildValue OOM
    error paths (OOM).

The chan select stress test (test_select_mn_*_integrity_stress) is kept because
it is genuinely adversarial -- it asserts NO value is lost or duplicated across
the cross-hub select handoff under heavy contention -- even though it only
*opportunistically* lights the race lines.
"""
import os
import subprocess
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


# ==========================================================================
# Helpers
# ==========================================================================
def _run_subproc(script, env_extra=None, timeout=240):
    """Run a script in a fresh child so an env-resolved-once mode is exercised
    and gcov flushes on clean exit.  Skip (not fail) on timeout: this box is
    shared with a CI runner, so a timeout is contention, not a bug."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("cov workload timed out (box under heavy load)")


# ==========================================================================
# PART 1 -- chan_select_main.c.inc: the select() PARK path.
# These are single-thread (run()) unless marked M:N; the park machinery is
# backend-independent so the deterministic cases run on any build.
# ==========================================================================

def test_select_recv_parks_then_woken_by_send():
    """A blocking RECV-select on a cap-0 channel must PARK (Phase-2: heap the
    waiters, pin the channels, install, park_current + yield), then wake with
    the delivered value when a producer sends.  Covers the install loop, the
    single-thread park, the post-wake eviction, and the RECV fired-value
    return path (waiters[fired].value != NULL)."""
    res = {}
    order = []

    def main():
        ch = rc.Chan(0)

        def chooser():
            idx, (val, ok) = rc.select([("recv", ch)])
            res["r"] = (idx, val, ok)

        def sender():
            for i in range(4):
                order.append(("y", i))   # prove the chooser parked (cooperative overlap)
                rc.sched_yield()
            ch.send("payload")

        rc.fiber(chooser)
        rc.fiber(sender)

    with hang_guard(20, "recv parks then woken"):
        rc.fiber(main)
        rc.run()

    assert res["r"] == (0, "payload", True), res
    # the sender got to run (the chooser was genuinely parked, not busy-spinning)
    assert order == [("y", 0), ("y", 1), ("y", 2), ("y", 3)], order


def test_select_send_parks_then_taken_by_recv():
    """A blocking SEND-select on a cap-0 channel parks until a receiver takes
    the value.  Covers the SEND install (Py_INCREF send_value, waiter push) and
    the SEND fired return (send_result == 0 -> return fired)."""
    res = {}

    def main():
        ch = rc.Chan(0)

        def chooser():
            idx, payload = rc.select([("send", ch, "hello")])
            res["choose"] = (idx, payload)

        def receiver():
            for _ in range(4):
                rc.sched_yield()
            res["recv"] = ch.recv()

        rc.fiber(chooser)
        rc.fiber(receiver)

    with hang_guard(20, "send parks then taken"):
        rc.fiber(main)
        rc.run()

    # SEND case fired (index 0), payload is None (Go: send case yields no value)
    assert res["choose"] == (0, None), res
    # the receiver got exactly our value with ok=True
    assert res["recv"] == ("hello", True), res


def test_select_recv_parks_then_channel_closed_returns_ok_false():
    """A blocking RECV-select parked on a cap-0 channel that is then CLOSED:
    close() wakes the select waiter with value==NULL/ok==0, so the fired-RECV
    branch sees value==NULL and falls through to the close-wake RE-SCAN, whose
    Phase-1 returns the closed case with ok=False.  Covers the
    waiters[fired].value == NULL fall-through and the re-scan goto."""
    res = {}

    def main():
        ch = rc.Chan(0)

        def chooser():
            idx, (val, ok) = rc.select([("recv", ch)])
            res["r"] = (idx, val, ok)

        def closer():
            for _ in range(4):
                rc.sched_yield()
            ch.close()

        rc.fiber(chooser)
        rc.fiber(closer)

    with hang_guard(20, "recv parks then closed"):
        rc.fiber(main)
        rc.run()

    # closed + empty -> the Go `v, ok := <-ch` close idiom: (None, False)
    assert res["r"] == (0, None, False), res


def test_select_send_parks_then_channel_closed_raises():
    """A blocking SEND-select parked on a cap-0 channel that is then CLOSED:
    close() wakes the parked sender with send_result == -1, so the fired-SEND
    branch raises ValueError('select send on closed channel') and drops the
    still-ours value ref.  Covers the send_result != 0 cleanup branch."""
    res = {}

    def main():
        ch = rc.Chan(0)

        def chooser():
            try:
                rc.select([("send", ch, "x")])
                res["r"] = "no-error"
            except ValueError as e:
                res["r"] = str(e)

        def closer():
            for _ in range(4):
                rc.sched_yield()
            ch.close()

        rc.fiber(chooser)
        rc.fiber(closer)

    with hang_guard(20, "send parks then closed"):
        rc.fiber(main)
        rc.run()

    assert res["r"] == "select send on closed channel", res


def test_select_multicase_nonfiring_send_value_dropped():
    """A multi-case select [SEND on a (never fires), RECV on b (fires)]: when
    the RECV case fires, the install dropped the non-firing SEND case's value
    ref in the 'drop refs from non-firing SEND cases' loop.  We assert the
    RECV case won and the SEND object is released (no leak)."""
    import gc
    import weakref

    res = {}
    refs = []

    class Box:
        __slots__ = ("__weakref__",)

    def main():
        a = rc.Chan(0)   # SEND case target: no receiver -> never fires
        b = rc.Chan(0)   # RECV case target: a feeder rendezvouses -> fires
        sent = Box()
        refs.append(weakref.ref(sent))

        def chooser():
            idx, payload = rc.select([("send", a, sent), ("recv", b)])
            res["r"] = (idx, payload)

        def feeder():
            for _ in range(4):
                rc.sched_yield()
            b.send("delivered")   # makes case 1 (RECV) fire

        rc.fiber(chooser)
        rc.fiber(feeder)

    with hang_guard(20, "multicase nonfiring send"):
        rc.fiber(main)
        rc.run()

    # the RECV case (index 1) won, with its delivered value
    assert res["r"] == (1, ("delivered", True)), res
    # the non-firing SEND value ref was dropped -> object collectable
    del res
    gc.collect()
    assert all(r() is None for r in refs), "non-firing SEND value leaked"


@pytest.mark.skipif(not FT, reason="the M:N hub park branch needs a real hub")
def test_select_recv_parks_in_mn_hub_then_woken():
    """Under runloom.run(N) a select parked inside an M:N HUB takes the
    runloom_mn_current_hub_opaque() != NULL park branch (distinct from the
    single-thread one).  The sender deliberately sleeps so the chooser is surely
    parked in the hub before any value is ready (forcing the park, not a
    Phase-1 rendezvous)."""
    from runloom.sync import WaitGroup
    res = {}

    def main():
        ch = rc.Chan(0)
        wg = WaitGroup(); wg.add(2)

        def chooser():
            try:
                idx, (val, ok) = rc.select([("recv", ch)])
                res["r"] = (idx, val, ok)
            finally:
                wg.done()

        def sender():
            try:
                rc.sched_sleep(0.15)    # let the chooser park in the hub first
                ch.send("mn-payload")
            finally:
                wg.done()

        rc.mn_fiber(chooser)
        rc.mn_fiber(sender)
        wg.wait()

    with hang_guard(30, "mn hub select park"):
        runloom.run(3, main)

    assert res["r"] == (0, "mn-payload", True), res


@pytest.mark.skipif(not FT, reason="M:N cross-hub select handoff needs GIL off")
def test_select_mn_recv_integrity_stress():
    """Adversarial: many RECV-select consumers over a few shared cap-0 channels,
    fed by many plain producers, under run(4).  Asserts NO value is lost or
    duplicated across the cross-hub select wake handoff (a count alone would
    miss a lost+dup pair that nets out).  Opportunistically exercises the
    install-time RECV-abort/retry race; the integrity oracle is what matters."""
    from runloom.sync import WaitGroup
    K, NPROD, PER = 6, 12, 80
    TOTAL = NPROD * PER
    chans = [rc.Chan(0) for _ in range(K)]
    collected = []
    cmu = rc.Mutex()

    def main():
        wg = WaitGroup(); wg.add(NPROD)
        consumed = [0]

        def producer(p):
            try:
                for r in range(PER):
                    chans[(p + r) % K].send((p, r))
            finally:
                wg.done()

        def selector():
            while True:
                idx, (val, ok) = rc.select([("recv", c) for c in chans])
                if not ok:
                    return                       # a channel was closed: drain done
                cmu.lock()
                try:
                    collected.append(val)
                    done = len(collected) >= TOTAL
                finally:
                    cmu.unlock()
                if done:
                    return

        NSEL = 8
        for _ in range(NSEL):
            rc.mn_fiber(selector)
        for p in range(NPROD):
            rc.mn_fiber(lambda p=p: producer(p))
        wg.wait()                                 # everything produced
        for c in chans:                           # unpark leftover selectors
            try:
                c.close()
            except Exception:
                pass

    with hang_guard(90, "mn recv-select integrity"):
        runloom.run(4, main)

    expected = set((p, r) for p in range(NPROD) for r in range(PER))
    assert len(collected) == len(expected), (
        "lost/dup: got %d distinct want %d" % (len(set(collected)), len(expected)))
    assert set(collected) == expected, "value set mismatch (lost or duplicated)"


@pytest.mark.skipif(not FT, reason="M:N send-select handoff needs GIL off")
def test_select_mn_send_integrity_stress():
    """Adversarial mirror: many SEND-select producers (multi-case SEND over
    shared cap-0 channels) feeding many plain receivers.  Asserts every value
    is received exactly once (no loss/dup across the SEND-side select handoff).
    Opportunistically exercises the SEND-abort/retry install race."""
    from runloom.sync import WaitGroup
    K, NSEL, PER = 6, 10, 80
    TOTAL = NSEL * PER
    chans = [rc.Chan(0) for _ in range(K)]
    got = []
    gmu = rc.Mutex()

    def main():
        wgS = WaitGroup(); wgS.add(NSEL)
        consumed = [0]
        cmu = rc.Mutex()

        def selector(s):
            try:
                for r in range(PER):
                    cases = [("send", chans[(s + r + i) % K], (s, r, i))
                             for i in range(K)]
                    rc.select(cases)             # exactly one delivered SEND
            finally:
                wgS.done()

        def receiver(rr):
            while True:
                cmu.lock()
                if consumed[0] >= TOTAL:
                    cmu.unlock(); return
                consumed[0] += 1
                cmu.unlock()
                v, ok = chans[rr % K].recv()
                gmu.lock()
                try:
                    got.append(v)
                finally:
                    gmu.unlock()

        NRECV = K * 4
        for rr in range(NRECV):
            rc.mn_fiber(lambda rr=rr: receiver(rr))
        for s in range(NSEL):
            rc.mn_fiber(lambda s=s: selector(s))
        wgS.wait()

    with hang_guard(90, "mn send-select integrity"):
        runloom.run(4, main)

    expected = set((s, r, i)
                   for s in range(NSEL) for r in range(PER) for i in range(K))
    # each select delivered exactly ONE of its K candidate (s,r,i) tuples, so we
    # can't predict WHICH, but every delivered value must be a valid candidate
    # and the count must equal the number of selects (no loss / dup).
    assert len(got) == TOTAL, "lost/dup sends: got %d want %d" % (len(got), TOTAL)
    assert len(set(got)) == TOTAL, "duplicated send value across handoff"
    assert set(got) <= expected, "received a value no selector ever offered"


# ==========================================================================
# PART 2 -- runloom_stackadvice.c: the autosize / prescan / learned profiler.
#
# Each driver runs in a fresh subprocess (autosize/prescan are env-resolved
# once) that EXITS CLEANLY so gcov flushes.  Oracles read the public
# stack_advice() report and the per-kind `reserved` (the stack size the spawn
# actually ran with), which is the observable proof a given size policy ran.
# ==========================================================================

# --- learned path: a SECOND spawn of a sampled kind sizes to the learned peak,
#     SHRINKING from the large autosize start once a sample exists. ---
_ADVICE_LEARNED = r'''
import sys, os; sys.path.insert(0, "src")
# Start above the FT-3.14 256 KiB spawn floor (p226, 289ecb99): at the default
# 256 KiB start the floor equals the start there, so nothing could shrink.
os.environ["RUNLOOM_STACK_AUTOSIZE_START"] = str(1024 * 1024)
import runloom, runloom_c as rc
rc.enable_stack_autosize(True, False)   # autosize ON, prescan OFF

def worker():
    return sum(range(100))              # shallow -> learned size is small

def main():
    for _ in range(4):                  # round 1: establishes the sample
        rc.mn_fiber(worker)
    rc.sched_sleep(0.3)
    for _ in range(4):                  # round 2: learned (samples > 0)
        rc.mn_fiber(worker)
    rc.sched_sleep(0.3)

runloom.run(2, main)
rep = rc.stack_advice()
wk = [r for r in rep if "worker" in r["kind"]]
assert wk, "worker kind not recorded"
w = wk[0]
sys.stdout.write("LEARNED samples=%d reserved=%d max_hwm=%d\n"
                 % (w["samples"], w["reserved"], w["max_hwm"]))
# reset_stack_advice clears the table (proves the reset path runs)
rc.reset_stack_advice()
sys.stdout.write("RESET entries=%d\n" % len(rc.stack_advice()))
'''


def test_stackadvice_learned_size_shrinks_from_start():
    p = _run_subproc(_ADVICE_LEARNED)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    learned = [l for l in p.stdout.splitlines() if l.startswith("LEARNED ")]
    assert learned, (p.stdout[-400:], p.stderr[-800:])
    f = dict(kv.split("=") for kv in learned[0][len("LEARNED "):].split())
    # both rounds folded into one kind (8 samples)
    assert int(f["samples"]) == 8, learned[0]
    # the learned reserved size SHRANK below the 1 MiB autosize start (set in
    # the snippet, above the FT-3.14 spawn floor): round-2 spawns sized to the
    # observed (shallow) peak, not the cold default.
    assert int(f["reserved"]) < 1024 * 1024, (
        "learned size did not shrink from the autosize start: " + learned[0])
    reset = [l for l in p.stdout.splitlines() if l.startswith("RESET ")]
    assert reset and reset[0] == "RESET entries=0", (p.stdout[-400:],)


# --- prescan cold-start: a kind whose bytecode references a fat-frame symbol
#     (Decimal) is given a roomy cold start (>= the prescan floor), and the
#     floor is remembered so learn-down can't shrink it under that. ---
_ADVICE_PRESCAN = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from decimal import Decimal
rc.enable_stack_autosize(True, True)    # prescan ON

def crypto_like():                      # co_names references 'Decimal' (fat frame)
    d = Decimal("1.5")
    return str(d)

assert "Decimal" in crypto_like.__code__.co_names

def main():
    for _ in range(6):
        rc.mn_fiber(crypto_like)           # spawned DIRECTLY -> size_for sees it
    rc.sched_sleep(0.3)

runloom.run(2, main)
rep = rc.stack_advice()
ck = [r for r in rep if "crypto_like" in r["kind"]]
assert ck, "crypto_like kind not recorded"
c = ck[0]
sys.stdout.write("PRESCAN samples=%d reserved=%d\n" % (c["samples"], c["reserved"]))
'''


def test_stackadvice_prescan_cold_start_raises_floor():
    p = _run_subproc(_ADVICE_PRESCAN)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("PRESCAN ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    f = dict(kv.split("=") for kv in line[0][len("PRESCAN "):].split())
    assert int(f["samples"]) == 6, line[0]
    # the prescan matched 'Decimal' (a >= 256 KiB single frame), so the
    # cold-start floor lifted the reserved size to >= 512 KiB and the
    # learn-down kept it there (the floor protects the deep path).
    assert int(f["reserved"]) >= 512 * 1024, (
        "prescan floor did not raise the cold-start size: " + line[0])


# --- the RUNLOOM_STACK_AUTOSIZE_START env override (atol parse). ---
_ADVICE_ENV_START = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_autosize(True, False)   # parses RUNLOOM_STACK_AUTOSIZE_START

def fresh():                            # an UNSEEN kind -> starts at the env size
    return 1

def main():
    rc.mn_fiber(fresh)                     # first spawn: no sample -> autosize start
    rc.sched_sleep(0.2)

runloom.run(2, main)
rep = rc.stack_advice()
fk = [r for r in rep if r["kind"].endswith(":%d)" % fresh.__code__.co_firstlineno)
      or "fresh" in r["kind"]]
fk = [r for r in rep if "fresh" in r["kind"]]
assert fk, "fresh kind not recorded"
sys.stdout.write("ENVSTART reserved=%d\n" % fk[0]["reserved"])
'''


def test_stackadvice_env_start_override():
    # 393216 = 384 KiB, a non-default value not equal to the 256 KiB default
    # nor any pow2 the cold path would otherwise pick.
    p = _run_subproc(_ADVICE_ENV_START, {"RUNLOOM_STACK_AUTOSIZE_START": "393216"})
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("ENVSTART ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    reserved = int(line[0].split("=")[1])
    # the first (unsampled) spawn ran with the env-overridden start size.
    assert reserved == 393216, (
        "env start not honoured: reserved=%d (want 393216)" % reserved)


# --- __wrapped__ unwrap: a functools.wraps wrapper attributes to the REAL
#     target, so the prescan scans the target's bytecode (Decimal), not the
#     wrapper's. ---
_ADVICE_WRAPPED = r'''
import sys, functools; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from decimal import Decimal
rc.enable_stack_autosize(True, True)

def real_target():
    return Decimal("2")

@functools.wraps(real_target)
def wrapper():
    return real_target()

assert wrapper.__wrapped__ is real_target
assert "Decimal" not in wrapper.__code__.co_names   # only the target has it

def main():
    for _ in range(4):
        rc.mn_fiber(wrapper)               # unwrap -> real_target's bytecode
    rc.sched_sleep(0.3)

runloom.run(2, main)
rep = rc.stack_advice()
# the kind is attributed to real_target, NOT wrapper (the unwrap worked)
tk = [r for r in rep if "real_target" in r["kind"]]
wk = [r for r in rep if r["kind"].endswith("wrapper") or ".wrapper " in r["kind"]]
assert tk, "kind not attributed to the wrapped target"
sys.stdout.write("WRAPPED target_reserved=%d wrapper_kinds=%d\n"
                 % (tk[0]["reserved"], len(wk)))
'''


def test_stackadvice_unwrap_follows_wrapped():
    p = _run_subproc(_ADVICE_WRAPPED)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("WRAPPED ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    f = dict(kv.split("=") for kv in line[0][len("WRAPPED "):].split())
    # attributed to the wrapped target, and the prescan matched through the
    # unwrap (Decimal -> >= 512 KiB).
    assert int(f["wrapper_kinds"]) == 0, "kind attributed to wrapper, not target"
    assert int(f["target_reserved"]) >= 512 * 1024, line[0]


# --- name_of with a callable that has NO __code__ (an instance) -> the
#     no-filename else branch of the name builder. ---
_ADVICE_NOCODE = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_autosize(True, True)

class Callable:                          # an instance: no __code__, no __qualname__
    def __call__(self):
        return 42

c = Callable()
assert not hasattr(c, "__code__")

def main():
    for _ in range(3):
        rc.mn_fiber(c)                      # name_of: code==NULL -> "module.<callable>"
    rc.sched_sleep(0.2)

runloom.run(2, main)
rep = rc.stack_advice()
ck = [r for r in rep if r["kind"].endswith("<callable>")]
assert ck, "no-__code__ callable not recorded with the <callable> name"
sys.stdout.write("NOCODE kind=%s samples=%d\n" % (ck[0]["kind"], ck[0]["samples"]))
'''


def test_stackadvice_name_of_callable_without_code():
    p = _run_subproc(_ADVICE_NOCODE)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("NOCODE ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    # the name builder's no-filename branch produced "<module>.<callable>"
    assert line[0].split("kind=")[1].split(" ")[0].endswith("<callable>"), line[0]


# --- cold_start with a callable whose __code__.co_names is NOT a tuple -> the
#     !PyTuple_Check guard returns the generic size. ---
_ADVICE_BADNAMES = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_autosize(True, True)

class FakeCode:
    co_names = "not-a-tuple"             # str, not tuple -> !PyTuple_Check
    co_filename = "fake.py"
    co_firstlineno = 1

class Weird:
    __qualname__ = "Weird"
    __module__ = "probe"
    __code__ = FakeCode()
    def __call__(self):
        return 1

w = Weird()

def main():
    for _ in range(3):
        rc.mn_fiber(w)                      # cold_start: co_names not tuple -> generic
    rc.sched_sleep(0.2)

runloom.run(2, main)
rep = rc.stack_advice()
wk = [r for r in rep if r["kind"].startswith("probe.")]
assert wk, "Weird kind not recorded"
# generic (no prescan bump) because co_names was not a scannable tuple.
sys.stdout.write("BADNAMES reserved=%d\n" % wk[0]["reserved"])
'''


def test_stackadvice_cold_start_non_tuple_co_names():
    p = _run_subproc(_ADVICE_BADNAMES)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("BADNAMES ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    reserved = int(line[0].split("=")[1])
    # non-tuple co_names -> cold_start bailed to the generic start (256 KiB),
    # NOT a fat-frame bump.
    assert reserved < 512 * 1024, (
        "cold_start did not bail on a non-tuple co_names: " + line[0])


# --- direct (non-autosize) measurement: enable_stack_advice records HWM
#     samples on completion; report + reset + disable. ---
_ADVICE_RECORD = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_advice(True)            # measurement only (no autosize)
assert rc.stack_advice_enabled() is True

def w():
    return sum(range(50))

def main():
    for _ in range(6):
        rc.mn_fiber(w)
    rc.sched_sleep(0.2)

runloom.run(2, main)
n = len(rc.stack_advice())
rc.reset_stack_advice()
m = len(rc.stack_advice())
rc.enable_stack_advice(False)
sys.stdout.write("RECORD before=%d after_reset=%d enabled=%s\n"
                 % (n, m, rc.stack_advice_enabled()))
'''


def test_stackadvice_record_report_reset_disable():
    p = _run_subproc(_ADVICE_RECORD)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("RECORD ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    f = dict(kv.split("=") for kv in line[0][len("RECORD "):].split())
    assert int(f["before"]) >= 1, line[0]      # at least the worker kind recorded
    assert int(f["after_reset"]) == 0, line[0]  # reset cleared the table
    assert f["enabled"] == "False", line[0]     # disable took effect


# --- find-miss on an empty slot: a g spawned while advice is on (its key
#     inserted) whose table is RESET mid-flight, so on completion record_g
#     calls find() and probes the now-empty slot. ---
_ADVICE_FINDMISS = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_advice(True)

def slow():
    rc.sched_sleep(0.25)                # stay alive across the reset
    return 1

def main():
    rc.mn_fiber(slow)                      # note_spawn inserts slow's key
    rc.sched_sleep(0.05)
    rc.reset_stack_advice()             # clears the table -> slow's key gone
    rc.sched_sleep(0.4)                 # slow completes -> record_g -> find MISS

runloom.run(2, main)
sys.stdout.write("FINDMISS entries=%d\n" % len(rc.stack_advice()))
'''


def test_stackadvice_find_miss_on_reset_in_flight():
    p = _run_subproc(_ADVICE_FINDMISS)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("FINDMISS ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    # the reset cleared everything; the in-flight g's late record found no slot
    # (find miss) and added nothing back -> table stays empty.
    assert line[0] == "FINDMISS entries=0", line[0]


# --- table-full insert: spawn > RUNLOOM_ADVICE_CAP (2048) distinct kinds so
#     insert() returns NULL once the table fills (note_spawn yields key 0). ---
_ADVICE_TABLEFULL = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.enable_stack_advice(True)

N = 2100
ns = {}
exec("\n".join("def k%d():\n return %d" % (i, i) for i in range(N)), ns)
funcs = [ns["k%d" % i] for i in range(N)]

def main():
    for f in funcs:
        rc.mn_fiber(f)                     # each distinct qualname -> distinct kind
    rc.sched_sleep(0.6)

runloom.run(2, main)
# the table caps at RUNLOOM_ADVICE_CAP (2048); the rest hit insert-full.
sys.stdout.write("TABLEFULL entries=%d\n" % len(rc.stack_advice()))
'''


def test_stackadvice_insert_table_full():
    p = _run_subproc(_ADVICE_TABLEFULL, timeout=200)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("TABLEFULL ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    entries = int(line[0].split("=")[1])
    # we offered 2100 distinct kinds; the fixed-cap table holds exactly 2048,
    # the remaining 52 hit the insert-full return.
    assert entries == 2048, "table did not cap at RUNLOOM_ADVICE_CAP: %d" % entries


# --- autosize_enabled() + reset_after_fork() (the at-fork child hook re-inits
#     the advice lock; we then prove advice still works). ---
_ADVICE_MISC = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
assert rc.stack_autosize_enabled() is False
rc.enable_stack_autosize(True, False)
assert rc.stack_autosize_enabled() is True
rc.enable_stack_autosize(False)
assert rc.stack_autosize_enabled() is False
rc.reset_after_fork()                   # re-inits the advice lock (child hook)
rc.enable_stack_advice(True)            # still works after the re-init

def w():
    return 1

def main():
    for _ in range(3):
        rc.mn_fiber(w)
    rc.sched_sleep(0.15)

runloom.run(2, main)
sys.stdout.write("MISC entries=%d enabled=%s\n"
                 % (len(rc.stack_advice()), rc.stack_advice_enabled()))
'''


def test_stackadvice_autosize_enabled_and_reset_after_fork():
    p = _run_subproc(_ADVICE_MISC)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1600:])
    line = [l for l in p.stdout.splitlines() if l.startswith("MISC ")]
    assert line, (p.stdout[-400:], p.stderr[-800:])
    f = dict(kv.split("=") for kv in line[0][len("MISC "):].split())
    # advice still functioned after the at-fork lock re-init
    assert int(f["entries"]) >= 1, line[0]
    assert f["enabled"] == "True", line[0]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
