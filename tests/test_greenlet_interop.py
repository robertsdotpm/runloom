"""Pillar E part 1 -- greenlet interop torture (generalizes big_100/p76_greenlet.py).

Nest greenlet C-stack ``switch()`` chains INSIDE runloom goroutines: two stackful
context switchers -- greenlet's and runloom's -- coexisting on the same OS thread.

p76's finding (FINDINGS BUG #8): interleaving the two switchers historically
CRASHED -- either cooperatively (yielding to the runloom scheduler while control
sits on a greenlet's C-stack) or preemptively (a preemptive goroutine switch
landing in the middle of a greenlet switch).  p76 therefore ran with preemption
DISABLED (RUNLOOM_PREEMPT=0) AND drove each greenlet tree to completion
ATOMICALLY (no runloom scheduling point between two greenlet switches).

This session's measured result on CPython 3.14.4t + current runloom (the probes
that back these assertions, re-run below as tests): the coexistence is now
ROBUST.  The atomic p76 pattern passes WITH preemption ON -- the improvement over
p76 -- and even the historically-worst case, cooperatively yielding to the
runloom scheduler from WITHIN a switched-in greenlet, now completes cleanly
(measured: 128 goroutines x 8 hubs x 20 iterations, preemption on AND off, 8 runs
each, zero crashes).  We assert both the safe/atomic ordering and the now-passing
interleaved ordering, and note the improvement rather than silently relying on
RUNLOOM_PREEMPT=0.

Isolation strategy:
  * SINGLE-THREAD (run(1)) greenlet nesting runs IN-PROCESS via the raw
    runloom_c.fiber/run scheduler.  Raw rc.run() does NOT trigger runloom.run()'s
    PYTHON_TLBC=0 self-re-exec, and the 3.14t TLBC SIGSEGV is a MANY-HUB defect,
    so single-thread in-process is safe and fast.
  * M:N (run(n>1)) greenlet torture runs in a PYTHON_TLBC=0 SUBPROCESS (this same
    file is the entry point -- see the ``__main__`` dispatch).  That is the
    codebase idiom for launching M:N runloom (tools/lincheck, tools/soak all
    preset PYTHON_TLBC=0): it avoids run()'s self-re-exec mid-pytest AND isolates
    any greenlet/M:N crash as a captured non-zero child exit instead of a
    suite-killing SIGSEGV in the pytest process.
"""
import os
import subprocess
import sys

import pytest

REPO_SRC = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)

import runloom            # noqa: E402
import runloom_c as rc    # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import hang_guard, needs_free_threading   # noqa: E402

try:
    import greenlet
    HAVE_GREENLET = True
except ImportError:                                       # pragma: no cover
    HAVE_GREENLET = False

FT = needs_free_threading()

pytestmark = pytest.mark.skipif(
    not HAVE_GREENLET, reason="greenlet not installed on this interpreter")


# ---------------------------------------------------------------------------
# greenlet workloads -- shared by the in-process single-thread tests AND the
# M:N subprocess scenarios below.  Kept dependency-free so the subprocess entry
# point can call them without pytest.
# ---------------------------------------------------------------------------
def deep_chain(depth):
    """A chain of `depth` nested greenlets, each switching into the next with an
    incremented value and threading a running sum back up on return.  Returns
    (levels_entered_in_order, final_value).  For this construction the leaf is
    ``depth`` and final == 2*depth - 1."""
    order = []

    def make(level):
        def body(x):
            order.append(level)
            if level < depth:
                child = greenlet.greenlet(make(level + 1))
                r = child.switch(x + 1)
                return r + 1
            return x + 1
        return body

    root = greenlet.greenlet(make(1))
    final = root.switch(0)
    return order, final


def ping_pong(n):
    """Classic greenlet ping-pong: a worker greenlet hands `n` values back to the
    current greenlet one switch at a time.  Returns (produced, received)."""
    produced = []
    received = []

    def gbody():
        for i in range(n):
            produced.append(i)
            main.switch(i)

    main = greenlet.getcurrent()
    g = greenlet.greenlet(gbody)
    r = g.switch()
    while not g.dead:
        if r is not None:
            received.append(r)
        r = g.switch()
    return produced, received


def run_single_thread(fn):
    """Drive `fn` to completion on runloom's single-thread (run(1)) scheduler via
    the raw C runner (no PYTHON_TLBC=0 re-exec)."""
    box = {}

    def main():
        box["r"] = fn()

    rc.fiber(main)
    rc.run()
    return box.get("r")


# ---------------------------------------------------------------------------
# IN-PROCESS single-thread tests: greenlet stacks nested inside a run(1) fiber.
# ---------------------------------------------------------------------------
def test_single_deep_switch_chain():
    """A deep greenlet switch chain inside one runloom goroutine: correct entry
    order and the correct value threaded back up, no crash."""
    depth = 80

    def body():
        return deep_chain(depth)

    with hang_guard(20, "greenlet deep chain in fiber"):
        order, final = run_single_thread(body)
    assert order == list(range(1, depth + 1)), order
    assert final == 2 * depth - 1, final


def test_single_ping_pong_ordering_and_values():
    """greenlet ping-pong nested in a goroutine hands values in order (the exact
    p76 invariant: produced == received == range(n))."""
    def body():
        return ping_pong(64)

    with hang_guard(20, "greenlet ping-pong in fiber"):
        produced, received = run_single_thread(body)
    assert produced == list(range(64)) == received, (produced, received)


def test_single_raise_across_switch():
    """A greenlet that raises across a switch() inside a goroutine: the exception
    propagates to the switch() caller in the goroutine, cleanly, the greenlet is
    dead afterwards, and the scheduler is not corrupted."""
    def body():
        def boom(x):
            raise ValueError("across-switch-%d" % x)
        g = greenlet.greenlet(boom)
        caught = None
        try:
            g.switch(7)
        except ValueError as e:
            caught = str(e)
        return caught, g.dead

    with hang_guard(20, "greenlet raise across switch"):
        caught, dead = run_single_thread(body)
    assert caught == "across-switch-7", caught
    assert dead is True


def test_single_interleave_greenlet_with_yield_and_chan():
    """greenlet trees interleaved with runloom.yield_now() and channel ops at
    goroutine-stack-safe points: run an atomic greenlet tree, then yield / send on
    a channel, alternating.  Two goroutines (producer + consumer) under run(1)."""
    ROUNDS, PER = 4, 5

    def body():
        ch = runloom.Chan(0)
        got = []

        def producer():
            for _ in range(ROUNDS):
                produced, received = ping_pong(PER)   # atomic greenlet tree
                assert produced == received == list(range(PER))
                runloom.yield_now()                    # safe point: own stack
                for v in produced:
                    ch.send(v)                         # chan op, own stack
            ch.close()

        def consumer():
            while True:
                v, ok = ch.recv()
                if not ok:
                    break
                got.append(v)

        rc.fiber(producer)
        rc.fiber(consumer)
        return got

    with hang_guard(25, "greenlet + yield/chan interleave"):
        got = run_single_thread(body)
    assert got == list(range(PER)) * ROUNDS, got


# ---------------------------------------------------------------------------
# M:N subprocess scenarios.  Each drives a greenlet torture under runloom.run(
# hubs, main) with the GIL off and prints a single ``RESULT <name> ok=<bool>``
# line.  Launched by the pytest tests below in a PYTHON_TLBC=0 child.
# ---------------------------------------------------------------------------
def scenario_mn_atomic_trees(hubs=4, nfibers=64, rounds=4):
    """p76 pattern generalized: many goroutines, each drives several INDEPENDENT
    greenlet ping-pong trees to completion ATOMICALLY, yielding only at the safe
    boundary between trees."""
    results = [None] * nfibers
    errs = []
    lock = rc.Mutex()

    def worker(wid):
        try:
            for rnd in range(rounds):
                n = 3 + ((wid + rnd) % 10)
                produced, received = ping_pong(n)
                if produced != list(range(n)) or received != list(range(n)):
                    raise AssertionError("wid=%d bad tree %r %r" % (wid, produced, received))
                runloom.yield_now()
            results[wid] = "done"
        except BaseException as e:                    # noqa: BLE001
            lock.lock(); errs.append(repr(e)); lock.unlock()

    def main():
        for wid in range(nfibers):
            runloom.fiber(lambda wid=wid: worker(wid))

    runloom.run(hubs, main)
    done = sum(1 for r in results if r == "done")
    ok = (done == nfibers and not errs)
    print("RESULT mn_atomic_trees ok=%s done=%d/%d errs=%d %s"
          % (ok, done, nfibers, len(errs), errs[:3]))
    return ok


def scenario_mn_chan_interleave(hubs=4, nproducers=16, rounds=3, per=5):
    """greenlet trees interleaved with channel send/recv across hubs: each
    producer drives an atomic greenlet tree then streams its values on a shared
    channel; one consumer drains the expected total."""
    ch = runloom.Chan(0)
    total = [0]
    errs = []
    lock = rc.Mutex()
    target = nproducers * rounds * per

    def producer(wid):
        try:
            for _ in range(rounds):
                produced, received = ping_pong(per)
                if produced != received or produced != list(range(per)):
                    raise AssertionError("wid=%d bad tree" % wid)
                for v in produced:
                    ch.send(v)
        except BaseException as e:                    # noqa: BLE001
            lock.lock(); errs.append(repr(e)); lock.unlock()

    def consumer():
        got = 0
        while got < target:
            v, ok = ch.recv()
            if not ok:
                break
            got += 1
        lock.lock(); total[0] += got; lock.unlock()

    def main():
        runloom.fiber(consumer)
        for wid in range(nproducers):
            runloom.fiber(lambda wid=wid: producer(wid))

    runloom.run(hubs, main)
    ok = (total[0] == target and not errs)
    print("RESULT mn_chan_interleave ok=%s got=%d/%d errs=%d %s"
          % (ok, total[0], target, len(errs), errs[:3]))
    return ok


def scenario_mn_raise(hubs=4, nfibers=48):
    """A greenlet raising across a switch inside each of many parallel goroutines:
    every exception must propagate to its goroutine cleanly, no crash."""
    caught = [0]
    errs = []
    lock = rc.Mutex()

    def worker(wid):
        def boom(x):
            raise ValueError("boom-%d" % wid)
        g = greenlet.greenlet(boom)
        try:
            g.switch(wid)
        except ValueError as e:
            if str(e) == "boom-%d" % wid and g.dead:
                lock.lock(); caught[0] += 1; lock.unlock()
                return
        lock.lock(); errs.append("wid=%d mishandled" % wid); lock.unlock()

    def main():
        for wid in range(nfibers):
            runloom.fiber(lambda wid=wid: worker(wid))

    runloom.run(hubs, main)
    ok = (caught[0] == nfibers and not errs)
    print("RESULT mn_raise ok=%s caught=%d/%d errs=%d %s"
          % (ok, caught[0], nfibers, len(errs), errs[:3]))
    return ok


def scenario_mn_yield_inside(hubs=8, nfibers=128, iters=20):
    """The historically-worst case (FINDINGS BUG #8): cooperatively yield to the
    runloom scheduler from WITHIN a switched-in greenlet -- control is on the
    greenlet's C-stack, not the goroutine's own stack -- interleaved with
    greenlet switches, across many hubs.  Asserts it now completes cleanly."""
    done = [0]
    errs = []
    lock = rc.Mutex()

    def worker(wid):
        def gbody(x):
            for i in range(iters):
                runloom.yield_now()      # yield to the scheduler from inside the greenlet
                main.switch(i)
        main = greenlet.getcurrent()
        g = greenlet.greenlet(gbody)
        try:
            r = g.switch(0)
            while not g.dead:
                r = g.switch()
        except BaseException as e:                    # noqa: BLE001
            lock.lock(); errs.append(repr(e)); lock.unlock()
            return
        lock.lock(); done[0] += 1; lock.unlock()

    def main():
        for wid in range(nfibers):
            runloom.fiber(lambda wid=wid: worker(wid))

    runloom.run(hubs, main)
    ok = (done[0] == nfibers and not errs)
    print("RESULT mn_yield_inside ok=%s done=%d/%d errs=%d %s"
          % (ok, done[0], nfibers, len(errs), errs[:3]))
    return ok


SCENARIOS = {
    "mn_atomic_trees": scenario_mn_atomic_trees,
    "mn_chan_interleave": scenario_mn_chan_interleave,
    "mn_raise": scenario_mn_raise,
    "mn_yield_inside": scenario_mn_yield_inside,
}


def run_mn_scenario(name, preempt=True, timeout=90):
    """Launch a scenario in a PYTHON_TLBC=0 subprocess (this file as entry point).
    Returns (returncode, stdout+stderr).  preempt=False sets RUNLOOM_PREEMPT=0."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHON_TLBC"] = "0"          # preset -> no runloom self-re-exec
    env["PYTHONPATH"] = REPO_SRC + os.pathsep + env.get("PYTHONPATH", "")
    env["RUNLOOM_PREEMPT"] = "1" if preempt else "0"
    proc = subprocess.run(
        [sys.executable, os.path.abspath(__file__), name],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=timeout)
    return proc.returncode, proc.stdout.decode("utf-8", "replace")


def assert_scenario(name, preempt):
    rc_code, out = run_mn_scenario(name, preempt=preempt)
    token = "RESULT %s ok=True" % name
    assert rc_code == 0, (
        "M:N greenlet scenario %s (preempt=%s) child exited %d (crash/signal if "
        "negative):\n%s" % (name, preempt, rc_code, out))
    assert token in out, (
        "M:N greenlet scenario %s (preempt=%s) did not report ok=True:\n%s"
        % (name, preempt, out))


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_independent_greenlet_trees_preempt_on():
    """Many goroutines each running independent greenlet trees, atomically -- WITH
    preemption ON.  This is the improvement over p76 (which required
    RUNLOOM_PREEMPT=0): the atomic ordering is now robust under preemption."""
    assert_scenario("mn_atomic_trees", preempt=True)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_independent_greenlet_trees_preempt_off():
    """Same torture with the p76-safe RUNLOOM_PREEMPT=0 ordering -- the original
    guarantee still holds."""
    assert_scenario("mn_atomic_trees", preempt=False)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_greenlet_trees_interleaved_with_chan():
    """greenlet switches interleaved with channel send/recv across hubs."""
    assert_scenario("mn_chan_interleave", preempt=True)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_greenlet_raise_across_switch():
    """A greenlet raising across a switch inside each of many parallel goroutines."""
    assert_scenario("mn_raise", preempt=True)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_yield_from_inside_greenlet_preempt_on():
    """FINDINGS BUG #8 case: cooperatively yielding to the runloom scheduler from
    inside a switched-in greenlet, interleaved with greenlet switches, across many
    hubs, WITH preemption ON.  Historically crashed; asserted here to now complete
    cleanly (subprocess-isolated so any regression is a captured child crash)."""
    assert_scenario("mn_yield_inside", preempt=True)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_yield_from_inside_greenlet_preempt_off():
    """Same BUG #8 case with RUNLOOM_PREEMPT=0."""
    assert_scenario("mn_yield_inside", preempt=False)


# ---------------------------------------------------------------------------
# Subprocess entry point: ``python test_greenlet_interop.py <scenario>``.
# Exits 0 iff the scenario reports ok=True; non-zero on any assertion/exception.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    fn = SCENARIOS.get(name)
    if fn is None:
        sys.stderr.write("unknown scenario %r; choices: %s\n"
                         % (name, ", ".join(sorted(SCENARIOS))))
        raise SystemExit(2)
    try:
        ok = fn()
    except BaseException as exc:                       # noqa: BLE001
        print("RESULT %s ok=False EXC=%r" % (name, exc))
        raise SystemExit(1)
    raise SystemExit(0 if ok else 1)
