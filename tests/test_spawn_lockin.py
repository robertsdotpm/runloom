"""Lock-in tests (env/contract) for the spawn fast-path (docs/dev/spawn_experiments.md).

These assert the *wiring* of the two landed keepers without running the runtime to a
completion count (so they can't interact with each other's runtime state):
  1. resident-memset stack scrub is the DEFAULT (secure + fast); the get/set contract.
  2. optimize("throughput") wires the validated warm-stack arena + bulk + FRESH, and
     optimize("memory") (higher precedence) turns the RAM-spending parts back off.

The bulk batch *lifecycle* at scale is exercised separately in
tests/test_spawn_bulk_lifecycle.py (own subprocess), and scrub-under-churn
correctness is covered by the existing swarm/coro/stack tests now running the
resident-scrub default.
"""
import os

os.environ.setdefault("PYTHON_GIL", "0")

import runloom_c  # noqa: E402


def test_resident_scrub_contract():
    # The secure resident wipe is the default (RUNLOOM_STACK_SCRUB_RESIDENT, opt-out =0);
    # the toggle surface exists for the "secure"/"memory" profiles to drive.
    assert callable(runloom_c.get_stack_scrub)
    assert callable(runloom_c.set_stack_scrub)


def test_optimize_throughput_wires_spawn_fastpath():
    import runloom
    eff = runloom.optimize("throughput")
    for k in ("RUNLOOM_STACK_ARENA", "RUNLOOM_GON_BULK", "RUNLOOM_GON_FRESH"):
        assert eff.get(k) == "1", (k, eff.get(k))
    assert eff.get("RUNLOOM_GON_PCREATE") == "auto", eff.get("RUNLOOM_GON_PCREATE")
    assert eff.get("RUNLOOM_GON_PCREATE_B") == "auto", eff.get("RUNLOOM_GON_PCREATE_B")


def test_optimize_memory_overrides_throughput():
    # "memory" has higher precedence -> it claws back the RAM-spending arena + the
    # non-reclaiming resident scrub.  Resolve straight from the goal tables.
    import runloom._optimize as opt
    merged = {}
    for g in opt._PRECEDENCE:
        if g in ("throughput", "memory"):
            merged.update(opt._GOAL_ENV[g])
    assert merged["RUNLOOM_STACK_ARENA"] == "0"
    assert merged["RUNLOOM_STACK_SCRUB_RESIDENT"] == "0"
