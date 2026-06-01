"""Free-threading stress, in the spirit of CPython's Lib/test/test_free_threading,
with pygo goroutines layered in.

CPython's free-threading tests hammer shared list/dict/GC from many OS threads
with the GIL off and assert no loss / no corruption / no crash.  pygo's whole
correctness story is 3.13t, and its goroutines run *on* those GIL-free hub
threads -- so the meaningful version of those tests has goroutines doing the
hammering: shared-container mutation across hubs, a read-modify-write guarded by
a channel-mutex, and -- most pointed for pygo -- gc.collect() stop-the-world
firing while goroutines are live on every hub (the exact shape behind the
io_uring STW deadlock and the Group-B handoff work).

Each workload runs in a fresh subprocess with PYTHON_GIL=0 so the hubs really
run in parallel.  Running python -c with only pygo_core/gc/threading imported
also dodges this venv's GIL-re-enabling C extensions (a stray _brotli import
flips the GIL back on), which a subprocess cleanly avoids.
"""
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_mn(code, timeout=60):
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import pygo_core\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYGO_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return 124, out, err + "\n[timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


def assert_pass(code, timeout=60):
    rc, out, err = run_mn(code, timeout=timeout)
    assert rc == 0 and "PASS" in out, (
        "rc={0}\n--- stdout ---\n{1}\n--- stderr ---\n{2}".format(rc, out, err))
    return out


# ---------------------------------------------------------------------------
# test_free_threading/test_list.py: concurrent list.append loses nothing and
# corrupts nothing under the free-threaded interpreter -- here driven by many
# goroutines across hubs rather than OS threads.
# ---------------------------------------------------------------------------
def test_concurrent_list_append_no_loss():
    """N goroutines each append K distinct ints to ONE shared list, in
    parallel across 4 hubs (GIL off).  list.append is atomic on 3.13t, so the
    final list must contain every item exactly once -- no torn writes, no lost
    appends, no crash."""
    assert_pass(r"""
NHUB, NG, K = 4, 64, 500
shared = []
done = pygo_core.Chan(NG)
def mk(g):
    def w():
        base = g * K
        for i in range(K):
            shared.append(base + i)
        done.send(1)
    return w
pygo_core.mn_init(NHUB)
for g in range(NG):
    pygo_core.mn_go(mk(g))
pygo_core.mn_run()
fin = 0
for _ in range(NG):
    if done.try_recv() is None: break
    fin += 1
pygo_core.mn_fini()
assert fin == NG, (fin, NG)
assert len(shared) == NG * K, ("lost/extra appends", len(shared), NG * K)
assert set(shared) == set(range(NG * K)), "wrong/duplicated/corrupted items"
assert pygo_core._self_check(0) == 0
print("PASS", len(shared))
""", timeout=40)


# ---------------------------------------------------------------------------
# test_free_threading/test_dict.py: concurrent dict insertion of distinct keys
# (the resize-under-concurrent-writers hazard) keeps every key/value.
# ---------------------------------------------------------------------------
def test_concurrent_dict_distinct_keys():
    """N goroutines each insert K distinct keys into ONE shared dict in
    parallel; the dict must end up with every key mapping to its value
    (free-threaded dict insert + resize under concurrent writers)."""
    assert_pass(r"""
NHUB, NG, K = 4, 48, 400
shared = {}
done = pygo_core.Chan(NG)
def mk(g):
    def w():
        for i in range(K):
            key = g * K + i
            shared[key] = key * 3 + 1
        done.send(1)
    return w
pygo_core.mn_init(NHUB)
for g in range(NG):
    pygo_core.mn_go(mk(g))
pygo_core.mn_run()
fin = 0
for _ in range(NG):
    if done.try_recv() is None: break
    fin += 1
pygo_core.mn_fini()
assert fin == NG, (fin, NG)
assert len(shared) == NG * K, ("lost keys", len(shared), NG * K)
assert all(shared[k] == k * 3 + 1 for k in range(NG * K)), "wrong/torn values"
assert pygo_core._self_check(0) == 0
print("PASS", len(shared))
""", timeout=40)


# ---------------------------------------------------------------------------
# Read-modify-write under contention: an UNguarded shared += would lose updates
# under real parallelism; a channel used as a mutex (single token) must make it
# exact.  Also proves the channel-mutex itself is correct under cross-hub
# contention.
# ---------------------------------------------------------------------------
def test_channel_mutex_guarded_counter_exact():
    """Many goroutines each do `box[0] += 1` (a non-atomic read-modify-write)
    K times, serialised by a single-token channel mutex.  With true parallel
    hubs the count is exact only if the mutex actually excludes -- a lost
    update would make the total < NG*K."""
    assert_pass(r"""
NHUB, NG, K = 4, 32, 300
box = [0]
mu = pygo_core.Chan(1)
mu.send(0)                 # one token == unlocked
done = pygo_core.Chan(NG)
def worker():
    for _ in range(K):
        mu.recv()          # acquire
        box[0] += 1        # critical section (non-atomic RMW)
        mu.send(0)         # release
    done.send(1)
pygo_core.mn_init(NHUB)
for _ in range(NG):
    pygo_core.mn_go(worker)
pygo_core.mn_run()
fin = 0
for _ in range(NG):
    if done.try_recv() is None: break
    fin += 1
pygo_core.mn_fini()
assert fin == NG, (fin, NG)
assert box[0] == NG * K, ("lost updates -> mutex did not exclude", box[0], NG * K)
assert pygo_core._self_check(0) == 0
print("PASS", box[0])
""", timeout=40)


# ---------------------------------------------------------------------------
# THE pygo-pointed one: gc.collect() stop-the-world while goroutines are live
# on every hub, all churning cyclic garbage.  This is the STW-vs-running-hubs
# shape behind the io_uring STW deadlock and the Group-B handoff; it must run
# to completion with no crash, no hang, and a clean self-check.
# ---------------------------------------------------------------------------
def test_gc_stw_under_goroutine_churn():
    """Workers churn reference cycles while a dedicated goroutine repeatedly
    forces a full gc.collect() (stop-the-world).  A STW that can't complete
    because a hub is wedged in a syscall, or a handoff that races re-attach,
    would hang (timeout) or crash; correct behavior finishes clean."""
    assert_pass(r"""
import gc
NHUB, NWORK, ROUNDS = 4, 48, 200
done = pygo_core.Chan(NWORK + 1)
stop = [False]
def worker():
    for _ in range(ROUNDS):
        # build a reference cycle, then drop it -> work for the collector
        a = {}; b = {}
        a['b'] = b; b['a'] = a
        a['self'] = a
        del a, b
        pygo_core.sched_yield_classic()
    done.send(1)
def collector():
    n = 0
    while not stop[0]:
        gc.collect()           # full STW collection
        n += 1
        pygo_core.sched_yield_classic()
    done.send(('gc', n))
pygo_core.mn_init(NHUB)
pygo_core.mn_go(collector)
for _ in range(NWORK):
    pygo_core.mn_go(worker)
def stopper():
    for _ in range(NWORK):
        done.recv()            # all workers finished
    stop[0] = True
    done.recv()                # collector's final tally
pygo_core.mn_go(stopper)
pygo_core.mn_run()
pygo_core.mn_fini()
assert pygo_core._self_check(0) == 0
print("PASS")
""", timeout=50)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
