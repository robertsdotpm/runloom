"""Deterministic Simulation Testing (DST) for runloom's channel / scheduler core.

The single-thread cooperative scheduler (runloom_c.fiber + run) is deterministic:
for a fixed set of goroutines making fixed yield decisions, the run-queue
order is fixed, so the whole execution is reproducible.  This harness drives
REAL runloom channels/select on that scheduler while a seeded decision oracle
chooses WHERE each goroutine yields (runloom_c.sched_yield) -- so a different
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
                        documented RUNLOOM_SIM C-hook extension).

Scope honesty: this is deterministic for the SINGLE-THREAD cooperative
scheduler + channel/select logic.  Controlled interleaving of the multi-OS-
thread M:N path (which would reproduce the OS-thread flake class) requires a
C-level scheduler hook and is the next step (see docs/dev/VALIDATION.md).

A third mode, Antithesis-style branch-seeds (ForcedAt), replays BOTH sides of a
decision from one seed: it wraps a base strategy but overrides the yield decision
at one step, calling the base first so the rng stream stays aligned -- the two
branches differ only at that step.  Exploring both branches at every decision
(2*horizon runs) reaches executions the seed's own run pinned one way, WITHOUT
os.fork (forking mid runloom_c.run() on an fcontext stack would hang/SEGV): the
seed is the snapshot, replay is the fork.  On the strict-FIFO control this reaches
the bug from seeds whose own run is clean (see `branchsweep`).

Usage:
  tools/dst/dst.py determinism            # prove same seed -> same execution
  tools/dst/dst.py sweep [N]              # UniformYield over seeds 1..N
  tools/dst/dst.py pct [N] [depth]        # PCTBounded over seeds 1..N
  tools/dst/dst.py repro <scenario> <seed> [pct|uniform] [depth]
  tools/dst/dst.py branch <scenario> <seed> [depth]   # both branches of each decision
  tools/dst/dst.py branchsweep [N] [depth]            # branch-seed teeth on the control
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # for simnet
# Deterministic TIME (the second DST pillar): sched_sleep + loop.time() read the
# logical clock, not the wall clock.  Set before runloom_c reads it.  The
# channel/timer scenarios don't sched_sleep, so they are unaffected; the sim-net
# scenarios use it for deterministic delivery timing.
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")

import random

import runloom_c


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


class ForcedAt(object):
    """Antithesis-style branch-seed: wrap a base strategy but OVERRIDE the yield
    decision at exactly one step to a fixed value.  It still calls the base's
    should_yield first, so the shared rng stream advances identically -- the two
    forced branches (yield / no-yield at step k) from one seed therefore differ
    ONLY at k and its downstream consequences.  Exploring both branches at every
    k costs 2*horizon runs and covers the decisions a single seed's run pinned
    one way -- without os.fork (forking mid-runloom_c.run() on an fcontext stack
    under free-threaded CPython would hang/SEGV); the seed IS the snapshot, replay
    IS the fork."""

    def __init__(self, base, k, forced):
        self.base = base
        self.k = k
        self.forced = forced
        self.name = "forced@%d=%s" % (k, "Y" if forced else "N")

    def reset(self, rng, horizon):
        self.base.reset(rng, horizon)

    def should_yield(self, rng, step, gid):
        d = self.base.should_yield(rng, step, gid)   # keep the rng stream aligned
        return self.forced if step == self.k else d


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
            runloom_c.sched_yield()

    def record(self, ev):
        self.events.append(ev)

    def signature(self):
        return hash(tuple(self.events))


# ---- scenarios: real runloom goroutines with invariants ---------------------
# Each returns a list of "sent" facts; the harness checks conservation +
# self_check after runloom_c.run() drains.

def scenario_unbuffered_handoff(sim):
    ch = runloom_c.Chan()           # unbuffered: every send rendezvous-paired
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

    runloom_c.fiber(sender)
    runloom_c.fiber(receiver)
    runloom_c.run()
    sim.record(("recv_order", tuple(received)))
    # unbuffered single sender/receiver: strict FIFO handoff, nothing lost
    assert received == list(range(n)), "handoff lost/reordered: {0}".format(received)


def scenario_buffered_mpmc(sim):
    cap = 3
    ch = runloom_c.Chan(cap)
    nprod = 3
    per = 4
    sent = [p * 100 + i for p in range(nprod) for i in range(per)]
    total = nprod * per
    received = []
    done = runloom_c.Chan()

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
        runloom_c.fiber(lambda p=p: producer(p))
    runloom_c.fiber(closer)
    for c in range(2):
        runloom_c.fiber(lambda c=c: consumer(c))
    runloom_c.run()
    sim.record(("recv_set", tuple(sorted(received))))
    # conservation: every produced value received exactly once (any order)
    assert sorted(received) == sorted(sent), \
        "conservation broke: got {0} want {1}".format(len(received), total)
    assert len(received) == len(set(received)), "duplicate delivery"


def scenario_select_race(sim):
    a = runloom_c.Chan()
    b = runloom_c.Chan()
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
            r = runloom_c.select(cases)
            chan = cases[r[0]][1]
            val, ok = r[1]
            if ok:
                got.append(val)
            elif chan is a:
                open_a = False
            else:
                open_b = False

    runloom_c.fiber(lambda: sender(a, 0))
    runloom_c.fiber(lambda: sender(b, 1000))
    runloom_c.fiber(selector)
    runloom_c.run()
    sim.record(("select_set", tuple(sorted(got))))
    want = sorted([i for i in range(n)] + [1000 + i for i in range(n)])
    assert sorted(got) == want, "select lost/dup: got {0}".format(sorted(got))


def scenario_sim_echo(sim):
    """Deterministic SIMULATED-NETWORK scenario (the third DST pillar, Slice 0):
    a request/response echo over sim sockets whose delivery timing, fragmentation,
    and short-writes are drawn from sim.rng.  Reliable delivery here (loss/reset
    off) so it terminates by fixed byte count; the byte trace IS the signature, so
    same seed => identical trace.  See tools/dst/simnet.py."""
    import simnet
    net = simnet.SimNet(sim.rng, record=sim.record,
                        cfg={"P_LOSS": 0.0, "P_RESET": 0.0, "P_CONNECT_FAIL": 0.0})
    addr = ("srv", 7)
    n = 6
    srv = net.socket()
    srv.bind(addr)
    srv.listen()                                    # register listener before run()

    def server():
        conn, _ = srv.accept()
        got = b""
        while len(got) < n:
            data = conn.recv(n - len(got))
            if not data:
                break
            got += data
        conn.sendall(got)                           # echo exactly what arrived
        conn.close()

    def client():
        c = net.socket()
        c.connect(addr)
        for i in range(n):
            c.sendall(bytes([i]))
        back = b""
        while len(back) < n:
            data = c.recv(n - len(back))
            if not data:
                break
            back += data
        sim.record(("echo", tuple(back)))
        c.close()

    runloom_c.fiber(server)
    runloom_c.fiber(client)
    runloom_c.run()
    srv.close()


def scenario_sim_lostwake(sim):
    """Negative control for the INSTANT hang oracle: the server accepts but never
    replies (a planted lost-wake), so the client parks on recv forever.  Under the
    logical clock nothing rides wall time, so this is a genuine structural deadlock
    -- caught in microseconds, not by a wall-clock timeout.  run_once must surface
    it (a raised deadlock, or a parked fiber the caller checks)."""
    import simnet
    net = simnet.SimNet(sim.rng, record=sim.record,
                        cfg={"P_LOSS": 0.0, "P_RESET": 0.0, "P_CONNECT_FAIL": 0.0})
    addr = ("srv", 8)
    srv = net.socket()
    srv.bind(addr)
    srv.listen()

    def server():
        conn, _ = srv.accept()
        runloom_c.sched_yield()                     # BUG: never sends, never closes

    def client():
        c = net.socket()
        c.connect(addr)
        data = c.recv(1)                            # parks forever -> deadlock
        sim.record(("got", data))

    runloom_c.fiber(server)
    runloom_c.fiber(client)
    runloom_c.run()
    srv.close()


SCENARIOS = {
    "unbuffered_handoff": scenario_unbuffered_handoff,
    "buffered_mpmc": scenario_buffered_mpmc,
    "select_race": scenario_select_race,
    "sim_echo": scenario_sim_echo,
}


# ---- harness -------------------------------------------------------------

def scenario_BUG_strict_order(sim):
    """Negative control (selftest only): asserts strict FIFO arrival order on
    a buffered channel with TWO consumers -- which legitimately reorders.  So
    some interleavings violate it.  The harness must catch that and reproduce
    it from the seed; that is the proof its invariant checks have teeth."""
    ch = runloom_c.Chan(2)
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

    runloom_c.fiber(prod)
    runloom_c.fiber(lambda: cons(0))
    runloom_c.fiber(lambda: cons(1))
    runloom_c.run()
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
    # Instant lost-wake oracle: under the logical clock nothing rides wall time, so
    # a run that ends with an unwakeable parked fiber is a genuine deadlock, counted
    # by runloom in microseconds -- no wall-clock timeout.  Deadlock mode stays WARN
    # (1) so the scheduler still recovers from a TRANSIENT all-parked moment by
    # advancing the logical clock (a pending sim-delivery timer); only an
    # unrecoverable lost wake bumps the counter.
    runloom_c.set_deadlock_mode(1)
    dl0 = runloom_c.count_deadlocked()
    err = None
    try:
        scenario(sim)
        dl = runloom_c.count_deadlocked() - dl0
        if dl > 0:
            err = "DEADLOCK ({0} unwakeable fiber(s) -- lost wake)".format(dl)
        elif runloom_c._self_check(0) != 0:
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


def _all_scenarios():
    d = dict(SCENARIOS)
    d["BUG_strict_order"] = scenario_BUG_strict_order   # the negative control
    return d


def _base_strategy(depth):
    return PCTBounded(depth) if depth else UniformYield(0.5)


def cmd_branch(scenario_name, seed, depth):
    """From ONE seed, replay both branches (yield / no-yield) of every decision
    point and report the distinct executions + bugs the branching reaches beyond
    the seed's own single run."""
    scen = _all_scenarios()[scenario_name]
    horizon = calibrate(scen)
    base_sig, base_err = run_once(scen, seed, _base_strategy(depth), horizon)
    print("[dst] branch {0} seed={1} horizon={2}: base run {3}".format(
        scenario_name, seed, horizon, "BUG(%s)" % base_err if base_err else "ok"))
    execs = {base_sig: base_err}          # distinct executions the seed can reach
    live = 0
    revealed = []
    for k in range(1, horizon + 1):
        sig_k = {}
        for forced in (True, False):
            sig, err = run_once(scen, seed, ForcedAt(_base_strategy(depth), k, forced), horizon)
            sig_k[forced] = sig
            execs.setdefault(sig, err)
            if err and not base_err:
                revealed.append((k, forced, err))
        if sig_k[True] != sig_k[False]:
            live += 1
    print("  {0} live decisions of {1}; branching reaches {2} distinct executions "
          "(the seed's own run was 1)".format(live, horizon, len(execs)))
    if revealed:
        k, forced, err = revealed[0]
        print("  >>> branching REVEALED a bug the seed missed: force step {0} -> "
              "{1}: {2}  ({3} such branches)".format(
                  k, "yield" if forced else "no-yield", err, len(revealed)))
        return 1 if scenario_name != "BUG_strict_order" else 0
    print("  >>> no new bug from branching this seed")
    return 0


def cmd_branchsweep(nseeds, depth):
    """Teeth check: on the BUG control, count seeds whose OWN run is clean but
    whose branch-replay reveals the bug -- the coverage a single seed misses."""
    scen = scenario_BUG_strict_order
    horizon = calibrate(scen)
    base_bug = branch_bug = both = clean = 0
    first = None
    for seed in range(1, nseeds + 1):
        _, base_err = run_once(scen, seed, _base_strategy(depth), horizon)
        hit = False
        for k in range(1, horizon + 1):
            for forced in (True, False):
                _, err = run_once(scen, seed, ForcedAt(_base_strategy(depth), k, forced), horizon)
                if err:
                    hit = True
        if base_err:
            base_bug += 1
        if base_err and hit:
            both += 1
        elif hit and not base_err:
            branch_bug += 1
            if first is None:
                first = seed
        elif not base_err and not hit:
            clean += 1
    print("[dst] branchsweep BUG_strict_order: {0} seeds, horizon={1}".format(nseeds, horizon))
    print("  base run hits bug:                 {0}".format(base_bug))
    print("  base CLEAN but branch reveals bug: {0}  (branch-seeds' added coverage)".format(branch_bug))
    print("  base clean AND no branch bug:      {0}".format(clean))
    if branch_bug > 0:
        print("  >>> teeth: branch-replay from a clean seed (e.g. {0}) reaches the "
              "bug the seed's own run misses".format(first))
        return 0
    print("  >>> INCONCLUSIVE: every buggy interleaving was already hit by a base run")
    return 0


def main():
    args = sys.argv[1:]
    mode = args[0] if args else "determinism"
    if mode == "branch":
        return cmd_branch(args[1], int(args[2]),
                          int(args[3]) if len(args) > 3 else 0)
    if mode == "branchsweep":
        n = int(args[1]) if len(args) > 1 else 100
        d = int(args[2]) if len(args) > 2 else 0
        return cmd_branchsweep(n, d)
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
