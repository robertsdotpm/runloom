"""Pillar E part 2 -- gevent greentest-style smoke runner.

gevent's own ``greentest`` suite is NOT vendored here (and must not be -- it is a
large third-party tree).  Instead this runner proves gevent COEXISTS with and
RUNS on this box/interpreter by exercising a curated handful of gevent's public
APIs the way greentest does -- cooperative spawn/join, cooperative sleep
interleaving, a lock round-trip (with a real concurrency bound), a queue
round-trip, an Event handoff, a Pool map -- plus a runloom-coexistence check
(import runloom alongside gevent and drive a runloom fiber right after a gevent
greenlet, proving the two stackful runtimes do not clash in one process).

Honesty contract:
  * gevent NOT importable  -> print WHY and exit 0 (SKIP).  gevent needs a source
    build and MAY be absent on some interpreters; that is a clean skip, and the
    box is then simply NOT covered beyond this scaffold.
  * gevent importable      -> run the smoke checks; exit 0 iff all pass, exit 1 on
    a genuine gevent-API failure (a real problem worth a red).

This session's status (CPython 3.14.4t free-threaded, PYTHON_GIL=0): gevent
26.5.0 built a clean cp314t wheel and installed with greenlet 3.5.3; every check
below passes.  See the module-level RESULT lines when run.

Usage:
  PYTHON_GIL=0 PYTHONPATH=src \\
    $HOME/.pyenv/versions/3.14.4t/bin/python3 tools/conformance/run_gevent_greentest.py
  ... run_gevent_greentest.py --list            # show the checks
  ... run_gevent_greentest.py spawn_join queue  # run a chosen subset
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_SRC = os.path.join(os.path.dirname(os.path.dirname(HERE)), "src")
if REPO_SRC not in sys.path:
    sys.path.insert(0, REPO_SRC)


def try_import_gevent():
    """Return (gevent_module, None) or (None, reason_string)."""
    try:
        import gevent
        return gevent, None
    except Exception as exc:  # noqa: BLE001 -- ImportError or a build/ABI error
        return None, "%s: %s" % (type(exc).__name__, exc)


# ---------------------------------------------------------------------------
# The checks.  Each takes the imported gevent module and returns a short detail
# string on success or raises on failure.
# ---------------------------------------------------------------------------
def check_spawn_join(gevent):
    """gevent.spawn + joinall + Greenlet.value -- the core greenlet primitive."""
    from gevent import spawn, sleep

    def work(x):
        sleep(0)                       # a cooperative switch inside the greenlet
        return x * x

    gs = [spawn(work, i) for i in range(16)]
    gevent.joinall(gs, timeout=10)
    vals = [g.value for g in gs]
    expect = [i * i for i in range(16)]
    if vals != expect:
        raise AssertionError("spawn/join values %r != %r" % (vals, expect))
    return "16 greenlets joined, values correct"


def check_sleep_interleave(gevent):
    """gevent.sleep yields the hub -- two greenlets with different sleeps must
    interleave (b's short sleep wakes before a's long one)."""
    from gevent import spawn, sleep
    order = []

    def a():
        order.append("a1"); sleep(0.02); order.append("a2")

    def b():
        order.append("b1"); sleep(0.005); order.append("b2")

    gevent.joinall([spawn(a), spawn(b)], timeout=10)
    if order != ["a1", "b1", "b2", "a2"]:
        raise AssertionError("cooperative sleep did not interleave: %r" % (order,))
    return "cooperative interleave order %r" % (order,)


def check_lock_roundtrip(gevent):
    """gevent.lock.BoundedSemaphore bounds concurrency and round-trips
    acquire/release across greenlets."""
    from gevent import spawn, sleep
    from gevent.lock import BoundedSemaphore

    LIMIT, N = 2, 12
    sem = BoundedSemaphore(LIMIT)
    cur = [0]
    peak = [0]
    done = [0]

    def crit():
        sem.acquire()
        cur[0] += 1
        if cur[0] > peak[0]:
            peak[0] = cur[0]
        sleep(0.001)
        cur[0] -= 1
        sem.release()
        done[0] += 1

    gevent.joinall([spawn(crit) for _ in range(N)], timeout=10)
    if done[0] != N:
        raise AssertionError("only %d/%d critical sections ran" % (done[0], N))
    if peak[0] > LIMIT:
        raise AssertionError("semaphore admitted %d > limit %d" % (peak[0], LIMIT))
    return "%d sections, peak concurrency %d <= %d" % (done[0], peak[0], LIMIT)


def check_queue_roundtrip(gevent):
    """gevent.queue.Queue producer/consumer round-trip preserves order."""
    from gevent import spawn
    from gevent.queue import Queue

    q = Queue()
    out = []
    N = 50

    def producer():
        for i in range(N):
            q.put(i)
        q.put(StopIteration)

    def consumer():
        while True:
            v = q.get()
            if v is StopIteration:
                break
            out.append(v)

    gevent.joinall([spawn(producer), spawn(consumer)], timeout=10)
    if out != list(range(N)):
        raise AssertionError("queue round-trip lost/reordered items: %r" % (out[:8],))
    return "%d items through the queue in order" % (len(out),)


def check_event_handoff(gevent):
    """gevent.event.Event: a waiter parks until a setter fires it."""
    from gevent import spawn, sleep
    from gevent.event import Event

    ev = Event()
    seen = []

    def waiter():
        ev.wait(timeout=10)
        seen.append("woke")

    def setter():
        sleep(0.005)
        seen.append("set")
        ev.set()

    gevent.joinall([spawn(waiter), spawn(setter)], timeout=10)
    if seen != ["set", "woke"]:
        raise AssertionError("event handoff ordering wrong: %r" % (seen,))
    return "event handoff order %r" % (seen,)


def check_pool_map(gevent):
    """gevent.pool.Pool.map fans work out cooperatively and gathers in order."""
    from gevent.pool import Pool

    pool = Pool(4)
    result = pool.map(lambda x: x + 100, range(20))
    expect = [x + 100 for x in range(20)]
    if list(result) != expect:
        raise AssertionError("pool.map mismatch: %r" % (list(result)[:8],))
    return "pool.map of 20 items, gathered in order"


def check_runloom_coexistence(gevent):
    """Prove gevent and runloom (both stackful, both greenlet/fcontext-based)
    coexist in ONE process: run a gevent greenlet, then immediately drive a
    runloom single-thread fiber, and confirm both produced their result."""
    from gevent import spawn
    try:
        import runloom_c as rc
    except Exception as exc:  # noqa: BLE001
        raise AssertionError("runloom_c not importable for coexistence: %r" % (exc,))

    g = spawn(lambda: 6 * 7)
    g.join(timeout=10)
    if g.value != 42:
        raise AssertionError("gevent side wrong: %r" % (g.value,))

    box = {}

    def main():
        box["r"] = sum(range(10))

    rc.fiber(main)
    rc.run()
    if box.get("r") != 45:
        raise AssertionError("runloom side wrong: %r" % (box.get("r"),))
    return "gevent(42) + runloom(45) both ran in one process"


CHECKS = [
    ("spawn_join", check_spawn_join),
    ("sleep_interleave", check_sleep_interleave),
    ("lock_roundtrip", check_lock_roundtrip),
    ("queue_roundtrip", check_queue_roundtrip),
    ("event_handoff", check_event_handoff),
    ("pool_map", check_pool_map),
    ("runloom_coexistence", check_runloom_coexistence),
]


def main(argv):
    args = list(argv)
    verbose = False
    if "-v" in args:
        verbose = True
        args.remove("-v")
    if "--list" in args:
        for name, _ in CHECKS:
            print(name)
        return 0

    gevent, reason = try_import_gevent()
    if gevent is None:
        sys.stdout.write(
            "SKIP: gevent is not importable on this interpreter (%s).\n"
            "      gevent needs a source/wheel build and may be absent; the box "
            "is NOT covered beyond this scaffold.\n"
            "      Install with: PYTHON_GIL=0 %s -m pip install gevent\n"
            % (reason, sys.executable))
        return 0

    sys.stdout.write(
        "gevent %s / greenlet %s on %s (gil_enabled=%s)\n" % (
            getattr(gevent, "__version__", "?"),
            greenlet_version(),
            sys.version.split()[0],
            getattr(sys, "_is_gil_enabled", lambda: "n/a")()))

    selected = ([c for c in CHECKS if c[0] in set(args)] if args else CHECKS)
    if not selected:
        sys.stdout.write("no matching checks for %r; choices: %s\n"
                         % (args, ", ".join(n for n, _ in CHECKS)))
        return 2

    failures = 0
    for name, fn in selected:
        t0 = time.monotonic()
        try:
            detail = fn(gevent)
            dt = time.monotonic() - t0
            print("RESULT %-22s PASS  (%.3fs)  %s" % (name, dt, detail))
        except Exception as exc:  # noqa: BLE001 -- a real gevent-API failure is a red
            dt = time.monotonic() - t0
            failures += 1
            print("RESULT %-22s FAIL  (%.3fs)  %r" % (name, dt, exc))
            if verbose:
                import traceback
                traceback.print_exc()

    total = len(selected)
    print("\nSUMMARY gevent smoke: %d/%d passed, %d failed"
          % (total - failures, total, failures))
    return 1 if failures else 0


def greenlet_version():
    try:
        import greenlet
        return greenlet.__version__
    except Exception:  # noqa: BLE001
        return "?"


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
