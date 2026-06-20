"""@runloom.hot + auto per-core handler scaling.

The contention these fix is SHARED CLOSURE CELLS: one closure (e.g.
``handler = make_app(config)``) run by many fibers across many cores makes the
cores fight over the captured slots.  @hot gives each core its own cells holding
the same values -- distinct cells, SHARED code (the code was never the problem).
A module-level def captures nothing and already scales, so @hot is a no-op there.
Runnable standalone or under pytest.
"""
import os
import threading

import runloom


def test_hot_splits_cells_per_thread_and_shares_code():
    captured = {"n": 7}                       # a read-only capture

    @runloom.hot
    def work(x):
        return x * captured["n"]              # reads the captured dict

    N = 8
    results = {}
    barrier = threading.Barrier(N)

    def run_one(i):
        barrier.wait()                        # maximise real concurrency
        results[i] = work(i)

    threads = [threading.Thread(target=run_one, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(results[i] == i * 7 for i in range(N)), results        # correct
    copies = work._runloom_copies
    assert len(copies) == N, copies                                   # one per thread
    fns = list(copies.values())
    assert len({id(f) for f in fns}) == N                             # distinct fns
    assert len({id(f.__closure__[0]) for f in fns}) == N              # distinct CELLS
    assert len({id(f.__closure__[0].cell_contents) for f in fns}) == 1  # SAME values
    assert all(f.__code__ is work.__wrapped__.__code__ for f in fns)    # SHARED code


def test_hot_noop_on_module_level_def():
    def plain(x):                             # captures nothing -> already scales
        return x + 1
    assert runloom.hot(plain) is plain        # returned unchanged, a true no-op


def test_hot_noop_on_rebound_capture():
    total = 0

    def acc():
        nonlocal total                        # REBINDS a capture -> unsafe to split
        total += 1
    assert runloom.hot(acc) is acc            # left shared, not split


def test_hot_is_noop_when_disabled():
    os.environ["RUNLOOM_HOT_HANDLERS"] = "0"
    try:
        cfg = {"k": 1}

        @runloom.hot
        def work():
            return cfg["k"]

        work(); work()
        assert work._runloom_copies == {}     # disabled -> never made a copy
    finally:
        os.environ.pop("RUNLOOM_HOT_HANDLERS", None)


def test_hot_noop_on_non_function():
    class C:
        def __call__(self):
            return 7
    c = C()
    assert runloom.hot(c) is c                # passthrough, no crash


def test_hot_under_mn_scheduler():
    out = bytearray(64)                       # captured, mutated in place (safe)

    @runloom.hot
    def w(i):
        out[i] = (i * 3) & 0xff               # distinct slot -> race-free

    def root():
        for i in range(64):
            runloom.fiber(w, i)

    runloom.run(4, root)
    assert all(out[i] == ((i * 3) & 0xff) for i in range(64)), bytes(out)


def test_auto_promotes_busy_closure():
    from runloom import _hot
    a = _hot._AutoHot()
    a.after, a.budget = 4, 2
    cfg = object()

    def handler():
        return cfg                            # captures cfg -> a closure

    for _ in range(3):
        assert a.resolve(handler) is handler  # below threshold: shared
    promoted = a.resolve(handler)             # at threshold: promoted
    assert promoted is not handler
    assert getattr(promoted, "__runloom_hot__", False) is True
    assert a.resolve(handler) is promoted     # sticky thereafter
    assert a.stats()["promoted"] == 1


def test_auto_skips_module_level_def():
    from runloom import _hot
    a = _hot._AutoHot()
    a.after = 1

    def plain():                              # no capture -> already scales
        return 1

    for _ in range(5):
        assert a.resolve(plain) is plain      # never promoted, never even counted
    assert a.stats()["promoted"] == 0


def test_auto_budget_caps_and_warns():
    import warnings
    from runloom import _hot
    a = _hot._AutoHot()
    a.after, a.budget = 1, 1
    c1, c2 = object(), object()

    def h1():
        return c1

    def h2():
        return c2

    assert a.resolve(h1).__runloom_hot__      # first fits the budget
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert a.resolve(h2) is h2            # over budget: stays shared
    assert any("budget" in str(x.message) for x in w), [str(x.message) for x in w]
    assert a.stats()["left_shared_over_budget"] == 1


if __name__ == "__main__":
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print("PASS", _name)
    print("all hot tests passed")
