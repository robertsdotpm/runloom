"""Deterministic simulated-network DST -- Slice 0 (tools/dst/simnet.py + the
sim_echo / sim_lostwake scenarios in tools/dst/dst.py).

The third DST pillar: over the already-deterministic single-thread scheduler +
logical clock, a sim network whose delivery timing, fragmentation, and faults are
drawn from ONE seeded rng makes a whole network run a pure function of its seed.
This pins: (a) same seed gives an identical byte trace and run-twice is identical;
(b) different seeds give varied traces (the fault/timing model has coverage);
(c) a planted lost-wake is caught INSTANTLY by the count_deadlocked structural
oracle -- not a wall-clock timeout; (d) a clean run never trips that oracle.

Honest scope (documented in simnet.py): this models protocol LOGIC, not kernel/wire
quirks (Nagle, TIME_WAIT, NAT, CGNAT, simul-open RST) -- it will not catch the
NAT-traversal bug class; it is a determinism amplifier for runloom's internal
scheduler-to-I/O plumbing (lost wakes, park/commit races, deadlocks).
"""
import os
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))
import dst  # noqa: E402  (sets RUNLOOM_LOGICAL_CLOCK, imports runloom_c + simnet)
import simnet  # noqa: E402


def _run(scenario, seed):
    horizon = dst.calibrate(scenario)
    return dst.run_once(scenario, seed, dst.UniformYield(0.5), horizon)


class TestSimEchoDeterminism(unittest.TestCase):
    def test_same_seed_identical_and_run_twice_identical(self):
        for seed in range(1, 13):
            s1, e1 = _run(dst.scenario_sim_echo, seed)
            s2, e2 = _run(dst.scenario_sim_echo, seed)
            self.assertEqual((s1, e1), (s2, e2),
                             "sim_echo seed %d not reproducible" % seed)
            self.assertIsNone(e1, "clean echo seed %d flagged: %r" % (seed, e1))

    def test_seeds_produce_varied_traces(self):
        sigs = set(_run(dst.scenario_sim_echo, s)[0] for s in range(1, 25))
        # delivery timing + short-write + partial-read draws give real variety
        self.assertGreater(len(sigs), 5, "sim network produced too little variety")


class TestInstantHangOracle(unittest.TestCase):
    def test_planted_lost_wake_is_caught(self):
        sig, err = _run(dst.scenario_sim_lostwake, 1)
        self.assertIsNotNone(err, "planted lost-wake not caught")
        self.assertIn("DEADLOCK", err)

    def test_lost_wake_oracle_has_no_false_positives(self):
        # A clean echo must never trip the deadlock counter (the delivery fibers
        # sleeping on the logical clock are transient, not lost wakes).
        for seed in range(1, 20):
            _, err = _run(dst.scenario_sim_echo, seed)
            self.assertIsNone(err, "clean echo seed %d false-flagged: %r" % (seed, err))


class TestSimNetModel(unittest.TestCase):
    def test_faults_are_a_pure_function_of_the_rng(self):
        import random
        a = simnet._Faults(random.Random(42))
        b = simnet._Faults(random.Random(42))
        seqa = [(a.connect_fails(), a.on_send(10), a.on_recv(10, 8), a.delivery())
                for _ in range(50)]
        seqb = [(b.connect_fails(), b.on_send(10), b.on_recv(10, 8), b.delivery())
                for _ in range(50)]
        self.assertEqual(seqa, seqb, "fault draws not a pure function of the seed")

    def test_connect_refused_without_listener(self):
        import random
        net = simnet.SimNet(random.Random(1),
                            cfg={"P_CONNECT_FAIL": 0.0})
        c = net.socket()
        with self.assertRaises(simnet.SimError):
            c.connect(("nobody", 9))     # no listener registered


if __name__ == "__main__":
    unittest.main()
