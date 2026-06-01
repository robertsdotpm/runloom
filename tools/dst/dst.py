"""Deterministic Simulation Testing (DST) for pygo's channel / scheduler core.

The single-thread cooperative scheduler (pygo_core.go + run) is deterministic:
for a fixed set of goroutines making fixed yield decisions, the run-queue
order is fixed, so the whole execution is reproducible.  This harness drives
REAL pygo channels/select on that scheduler while a seeded decision oracle
chooses WHERE each goroutine yields (pygo_core.sched_yield) -- so a different
seed explores a different interleaving, and the SAME seed reproduces an
execution exactly.  A failing run therefore reduces to a single integer seed
(the property the cross-file leaked-parker flake never had).

Two pluggable scheduling strategies:
  * UniformYield(p)  -- yield at each decision point with probability p
                        (classic randomized interleaving; many preemptions).
  * PCTBounded(d)    -- PCT-style: pick d-1 preemption step indices up front
                        from the calibrated horizon and yield ONLY there.
                        Few, well-placed preemptions find deep bugs that a
                        sea of random yields misses (Burckhardt et al.,
                        ASPLOS'10).  This is the bounded-preemption adaptation
                        of PCT to cooperative yield insertion; full
                        priority-scheduler PCT needs scheduler control (the
                        documented PYGO_SIM C-hook extension).

Scope honesty: this is deterministic for the SINGLE-THREAD cooperative
scheduler + channel/select logic.  Controlled interleaving of the multi-OS-
thread M:N path (which would reproduce the OS-thread flake class) requires a
C-level scheduler hook and is the next step (see VALIDATION.md).

Usage:
  tools/dst/dst.py determinism            # prove same seed -> same execution
  tools/dst/dst.py sweep [N]              # UniformYield over seeds 1..N
  tools/dst/dst.py pct [N] [depth]        # PCTBounded over seeds 1..N
  tools/dst/dst.py repro <scenario> <seed> [pct|uniform] [depth]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import random

import pygo_core


# ---- scheduling strategies (the decision oracle) -------------------------

class UniformYield(object):
    name = "uniform"

    def __init__(self, p=0.5):
        self.p = p

    def reset(self, rng, horizon):
        pass

    def should_yield(self, rng, step, gid):
        return rng.random() < self.p


class PCTBounded(object):
    name = "pct"

    def __init__(self, depth=3):
        self.depth = depth
        self.points = set()

    def reset(self, rng, horizon):
        k = max(0, min(self.depth - 1, horizon))
        self.points = set(rng.sample(range(1, horizon + 1), k)) if horizon > 0 else set()

    def should_yield(self, rng, step, gid):
        return step in self.points


class NoYield(object):
    name = "none"

    def reset(self, rng, horizon):
        pass

    def should_yield(self, rng, step, gid):
        return False


# ---- the simulation context threaded through a scenario ------------------

class Sim(object):
    def __init__(self, seed, strategy):
        self.rng = random.Random(seed)
        self.strategy = strategy
        self.events = []
        self.step = 0

    def point(self, gid, label):
        """A cooperative decision point inside a goroutine."""
        self.step += 1
        if self.strategy.should_yield(self.rng, self.step, gid):
            pygo_core.sched_yield()

    def record(self, ev):
        self.events.append(ev)

    def signature(self):
        return hash(tuple(self.events))


# ---- scenarios: real pygo goroutines with invariants ---------------------
# Each returns a list of "sent" facts; the harness checks conservation +
# self_check after pygo_core.run() drains.

def scenario_unbuffered_handoff(sim):
    ch = pygo_core.Chan()           # unbuffered: every send rendezvous-paired
    n = 6
    received = []

    def sender():
        for i in range(n):
            sim.point(0, "pre-send")
            ch.send(i)
            sim.point(0, "post-send")
        ch.close()

    def receiver():
        while True:
            sim.point(1, "pre-recv")
            v, ok = ch.recv()
            if not ok:
                break
            received.append(v)

    pygo_core.go(sender)
    pygo_core.go(receiver)
    pygo_core.run()
    sim.record(("recv_order", tuple(received)))
    # unbuffered single sender/receiver: strict FIFO handoff, nothing lost
    assert received == list(range(n)), "handoff lost/reordered: {0}".format(received)


def scenario_buffered_mpmc(sim):
    cap = 3
    ch = pygo_core.Chan(cap)
    nprod = 3
    per = 4
    sent = [p * 100 + i for p in range(nprod) for i in range(per)]
    total = nprod * per
    received = []
    done = pygo_core.Chan()

    def producer(p):
        for i in range(per):
            sim.point(p, "pre-send")
            ch.send(p * 100 + i)
        done.send(p)

    def closer():
        for _ in range(nprod):
            done.recv()
        ch.close()

    def consumer(c):
        while True:
            sim.point(10 + c, "pre-recv")
            v, ok = ch.recv()
            if not ok:
                break
            received.append(v)

    for p in range(nprod):
        pygo_core.go(lambda p=p: producer(p))
    pygo_core.go(closer)
    for c in range(2):
        pygo_core.go(lambda c=c: consumer(c))
    pygo_core.run()
    sim.record(("recv_set", tuple(sorted(received))))
    # conservation: every produced value received exactly once (any order)
    assert sorted(received) == sorted(sent), \
        "conservation broke: got {0} want {1}".format(len(received), total)
    assert len(received) == len(set(received)), "duplicate delivery"


def scenario_select_race(sim):
    a = pygo_core.Chan()
    b = pygo_core.Chan()
    got = []
    n = 4

    def sender(ch, base):
        for i in range(n):
            sim.point(99, "pre-send")
            ch.send(base + i)
        ch.close()

    def selector():
        open_a = True
        open_b = True
        while open_a or open_b:
            sim.point(0, "pre-select")
            # only select over channels still open: a closed recv-case fires
            # ok=False every time and would starve the other channel.
            cases = []
            if open_a:
                cases.append(("recv", a))
            if open_b:
                cases.append(("recv", b))
            # cases are (op, chan[, value]); r[0]=fired index, r[1]=(val, ok)
            r = pygo_core.select(cases)
            chan = cases[r[0]][1]
            val, ok = r[1]
            if ok:
                got.append(val)
            elif chan is a:
                open_a = False
            else:
                open_b = False

    pygo_core.go(lambda: sender(a, 0))
    pygo_core.go(lambda: sender(b, 1000))
    pygo_core.go(selector)
    pygo_core.run()
    sim.record(("select_set", tuple(sorted(got))))
    want = sorted([i for i in range(n)] + [1000 + i for i in range(n)])
    assert sorted(got) == want, "select lost/dup: got {0}".format(sorted(got))


SCENARIOS = {
    "unbuffered_handoff": scenario_unbuffered_handoff,
    "buffered_mpmc": scenario_buffered_mpmc,
    "select_race": scenario_select_race,
}


# ---- harness -------------------------------------------------------------

def scenario_BUG_strict_order(sim):
    """Negative control (selftest only): asserts strict FIFO arrival order on
    a buffered channel with TWO consumers -- which legitimately reorders.  So
    some interleavings violate it.  The harness must catch that and reproduce
    it from the seed; that is the proof its invariant checks have teeth."""
    ch = pygo_core.Chan(2)
    n = 6
    received = []

    def prod():
        for i in range(n):
            sim.point(0, "s")
            ch.send(i)
        ch.close()

    def cons(c):
        while True:
            sim.point(10 + c, "r")
            v, ok = ch.recv()
            if not ok:
                break
            received.append(v)

    pygo_core.go(prod)
    pygo_core.go(lambda: cons(0))
    pygo_core.go(lambda: cons(1))
    pygo_core.run()
    sim.record(("order", tuple(received)))
    assert received == list(range(n)), "non-FIFO arrival: {0}".format(received)


def calibrate(scenario):
    """Run once with no yields to count decision points (the PCT horizon).
    Tolerant of invariant failures -- it only needs the step count."""
    sim = Sim(0, NoYield())
    try:
        scenario(sim)
    except Exception:
        pass
    return sim.step


def run_once(scenario, seed, strategy, horizon):
    sim = Sim(seed, strategy)
    strategy.reset(random.Random(seed ^ 0x5DEECE66D), horizon)
    err = None
    try:
        scenario(sim)
        if pygo_core._self_check(0) != 0:
            err = "self_check != 0"
    except Exception as exc:  # invariant violation or crash
        err = "{0}: {1}".format(type(exc).__name__, exc)
    return sim.signature(), err


def cmd_determinism():
    print("[dst] determinism: same seed must reproduce the same execution")
    bad = 0
    for sname, scen in SCENARIOS.items():
        horizon = calibrate(scen)
        for seed in range(1, 26):
            s1, e1 = run_once(scen, seed, UniformYield(0.5), horizon)
            s2, e2 = run_once(scen, seed, UniformYield(0.5), horizon)
            if s1 != s2 or e1 != e2:
                bad += 1
                print("  NONDETERMINISTIC {0} seed={1}: sig {2}!={3} err {4!r}!={5!r}".format(
                    sname, seed, s1, s2, e1, e2))
        print("  {0:<20} horizon={1:<4d} 25 seeds reproducible".format(sname, horizon))
    if bad:
        print("  >>> {0} nondeterministic runs -- DST seed->repro contract BROKEN".format(bad))
        return 1
    print("  >>> seed -> execution is deterministic; failures reduce to a seed")
    return 0


def cmd_sweep(strategy_factory, label, nseeds, depth=None):
    print("[dst] {0} sweep: seeds 1..{1}{2}".format(
        label, nseeds, " depth={0}".format(depth) if depth else ""))
    findings = []
    for sname, scen in SCENARIOS.items():
        horizon = calibrate(scen)
        fails = 0
        for seed in range(1, nseeds + 1):
            strat = strategy_factory()
            _, err = run_once(scen, seed, strat, horizon)
            if err:
                fails += 1
                findings.append((sname, label, seed, depth, err))
                if fails <= 3:
                    print("  FAIL {0} seed={1}: {2}".format(sname, seed, err))
        print("  {0:<20} {1} seeds, {2} failures".format(sname, nseeds, fails))
    if findings:
        print("  >>> {0} invariant violations (reproduce with: dst.py repro <scenario> <seed> {1})".format(
            len(findings), label))
        return 1
    print("  >>> no invariant violations across all scenarios/seeds")
    return 0


def cmd_repro(scenario_name, seed, label, depth):
    scen = SCENARIOS[scenario_name]
    horizon = calibrate(scen)
    strat = PCTBounded(depth) if label == "pct" else UniformYield(0.5)
    sig, err = run_once(scen, seed, strat, horizon)
    print("[dst] repro {0} seed={1} strategy={2}: sig={3} err={4!r}".format(
        scenario_name, seed, label, sig, err))
    return 1 if err else 0


def cmd_selftest():
    print("[dst] selftest: the harness must DETECT a broken invariant + reproduce by seed")
    scen = scenario_BUG_strict_order
    horizon = calibrate(scen)
    failing = []
    for seed in range(1, 201):
        _, err = run_once(scen, seed, UniformYield(0.5), horizon)
        if err:
            failing.append(seed)
    if not failing:
        print("  >>> SELFTEST INCONCLUSIVE: no interleaving violated the bad invariant")
        return 1
    seed = failing[0]
    s1, e1 = run_once(scen, seed, UniformYield(0.5), horizon)
    s2, e2 = run_once(scen, seed, UniformYield(0.5), horizon)
    print("  detected {0} failing seeds (e.g. {1}); first failure: {2}".format(
        len(failing), failing[:8], e1))
    print("  repro seed={0}: run1 sig={1} run2 sig={2} -> {3}".format(
        seed, s1, s2, "IDENTICAL" if (s1 == s2 and e1 == e2) else "DIVERGED"))
    if s1 == s2 and e1 == e2:
        print("  >>> teeth confirmed: the harness catches the bug AND the seed reproduces it exactly")
        return 0
    print("  >>> FAIL: the failing seed did not reproduce -- determinism contract broken")
    return 1


def main():
    args = sys.argv[1:]
    mode = args[0] if args else "determinism"
    if mode == "determinism":
        return cmd_determinism()
    if mode == "selftest":
        return cmd_selftest()
    if mode == "sweep":
        n = int(args[1]) if len(args) > 1 else 500
        return cmd_sweep(lambda: UniformYield(0.5), "uniform", n)
    if mode == "pct":
        n = int(args[1]) if len(args) > 1 else 500
        d = int(args[2]) if len(args) > 2 else 3
        return cmd_sweep(lambda: PCTBounded(d), "pct", n, d)
    if mode == "repro":
        return cmd_repro(args[1], int(args[2]),
                         args[3] if len(args) > 3 else "uniform",
                         int(args[4]) if len(args) > 4 else 3)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
