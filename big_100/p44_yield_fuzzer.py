"""big_100 / 44 -- random yield fuzzer.

A bank of accounts whose total balance must never change.  Many goroutines do
transfers (debit one account, credit another) under a lock, with random
cooperative yields sprinkled between every step -- including INSIDE the locked
critical section -- to widen race windows.  An auditor periodically sums all
balances and asserts the total is unchanged.

Stresses: race windows around shared state, lock correctness across yields.
"""
import harness
import runloom

NACCOUNTS = 256
START = 1000
TOTAL = NACCOUNTS * START


def setup(H):
    H.state = {"acct": [START] * NACCOUNTS, "lock": runloom.sync.Lock()}


def maybe_yield(rng):
    if rng.random() < 0.5:
        runloom.yield_now()


def worker(H, wid, rng, state):
    acct = state["acct"]
    lock = state["lock"]
    while H.running():
        a = rng.randrange(NACCOUNTS)
        b = rng.randrange(NACCOUNTS)
        if a == b:
            continue
        amount = rng.randint(1, 50)
        maybe_yield(rng)
        with lock:
            # The lock must keep this transfer atomic even across the yield in
            # the middle -- if it doesn't, the auditor will see a broken total.
            if acct[a] >= amount:
                acct[a] -= amount
                maybe_yield(rng)
                acct[b] += amount
        H.op(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        acct = H.state["acct"]
        lock = H.state["lock"]
        while H.running():
            H.sleep(0.5)
            with lock:
                total = sum(acct)
            if not H.check(total == TOTAL,
                           "balance invariant broken: total={0} != {1}".format(
                               total, TOTAL)):
                return

    H.go(auditor)


def post(H):
    H.check(sum(H.state["acct"]) == TOTAL,
            "final balance total {0} != {1}".format(sum(H.state["acct"]), TOTAL))


if __name__ == "__main__":
    harness.main("p44_yield_fuzzer", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="locked transfers + random yields keep total invariant")
