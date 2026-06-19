"""big_100 / 100 -- deterministic replay stress harness.

The capstone.  A bank of accounts is mutated by transfers under a lock; the
total must never change.  Each worker is driven by a per-worker RNG derived
deterministically from --seed, and records its recent operations in a ring
buffer.  If the invariant ever breaks, the offending worker dumps its operation
log and the exact command to replay -- so a rare race becomes reproducible by
re-running with the same --seed.

Stresses: debuggability, rare-race reproduction, invariant tracking with a
recorded, replayable operation history.
"""
import collections
import sys

import harness
import runloom

NACCOUNTS = 512
START = 1000
TOTAL = NACCOUNTS * START

def setup(H):
    H.state = {"acct": [START] * NACCOUNTS, "lock": runloom.sync.Lock(),
               "broke": [False]}


def worker(H, wid, rng, state):
    acct = state["acct"]
    lock = state["lock"]
    oplog = collections.deque(maxlen=32)
    while H.running():
        a = rng.randrange(NACCOUNTS)
        b = rng.randrange(NACCOUNTS)
        amt = rng.randint(1, 50)
        op = rng.randrange(3)               # 0 transfer, 1 swap, 2 noop-check
        oplog.append((op, a, b, amt))
        if op == 0 and a != b:
            with lock:
                if acct[a] >= amt:
                    acct[a] -= amt
                    if rng.random() < 0.3:
                        runloom.yield_now()  # widen the race window
                    acct[b] += amt
        elif op == 1 and a != b:
            with lock:
                acct[a], acct[b] = acct[b], acct[a]
        else:
            with lock:
                total = sum(acct)
            if total != TOTAL and not state["broke"][0]:
                state["broke"][0] = True
                dump_replay(H, wid, oplog, total)
                H.fail("invariant broken: total={0} != {1}".format(
                    total, TOTAL))
                return
        H.op(wid)
        H.task_done(wid)


def dump_replay(H, wid, oplog, total):
    sys.stderr.write(
        "\n=== REPLAY INFO (invariant violated) ===\n"
        "  seed       : {0}\n"
        "  worker     : {1}\n"
        "  observed   : total={2} expected={3}\n"
        "  recent ops : {4}\n"
        "  replay     : re-run with --seed {0} --hubs {5} --funcs {6}\n"
        "=========================================\n".format(
            H.seed, wid, total, TOTAL, list(oplog), H.hubs, H.funcs))
    sys.stderr.flush()


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        acct = H.state["acct"]
        lock = H.state["lock"]
        while H.running():
            H.sleep(0.25)
            with lock:
                total = sum(acct)
            if not H.check(total == TOTAL,
                           "auditor: total {0} != {1}".format(total, TOTAL)):
                return

    H.fiber(auditor)


def post(H):
    H.check(sum(H.state["acct"]) == TOTAL,
            "final total {0} != {1}".format(sum(H.state["acct"]), TOTAL))
    H.log("final_total={0} (seed {1} reproduces this run's per-worker ops)"
          .format(sum(H.state["acct"]), H.seed))


if __name__ == "__main__":
    harness.main("p100_replay_stress", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="seeded bank workload with recorded, replayable ops")
