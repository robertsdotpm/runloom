"""Linearizability battery -- the generative (abstract) generalization of big_100.

Three layers:
  1. TestCheckerTeeth  -- pure-Python unit checks that the WGL checker (tools/
     lincheck/linz/checker.py) ACCEPTS legal histories and REJECTS illegal ones
     (phantom delivery, FIFO reorder, double-lock, over-capacity semaphore,
     reader/writer overlap, early wait).  No build needed; proves the oracle has
     teeth so a green battery means something.
  2. TestLiveBattery   -- record real concurrent histories on the M:N scheduler
     (a few seeds x every primitive) and assert each linearizes.  Free-threaded.
  3. TestDifferentialGo -- record a channel history and check it with BOTH the
     Python checker and the independent Go Porcupine binary; assert they AGREE on
     a clean history and on a corrupted one.  Two unrelated checkers agreeing is
     far stronger evidence than either alone.

House style: %/.format, prints kept, no f-strings.
"""
import json
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LINZ = os.path.join(REPO, "tools", "lincheck", "linz")
sys.path.insert(0, LINZ)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # tests/ (adv_util)

import checker   # noqa: E402
import specs     # noqa: E402

from adv_util import needs_free_threading  # noqa: E402

FT = pytest.mark.skipif(not needs_free_threading(),
                        reason="the M:N scheduler is only real on free-threaded builds")


def op(proc, inp, out, call, ret):
    return checker.Op(proc, inp, out, call, ret)


# ----------------------------------------------------------- 1. checker teeth

class TestCheckerTeeth:
    def test_mutex_sequential_ok(self):
        m = specs.Mutex()
        h = [op(0, ("lock",), ("ok",), 0, 1), op(0, ("unlock",), ("ok",), 2, 3),
             op(1, ("lock",), ("ok",), 4, 5), op(1, ("unlock",), ("ok",), 6, 7)]
        assert checker.check(m, h).linearizable

    def test_mutex_double_lock_rejected(self):
        m = specs.Mutex()
        h = [op(0, ("lock",), ("ok",), 0, 5), op(1, ("lock",), ("ok",), 1, 4)]
        assert not checker.check(m, h).linearizable

    def test_mutex_unlock_free_rejected(self):
        m = specs.Mutex()
        assert not checker.check(m, [op(0, ("unlock",), ("ok",), 0, 1)]).linearizable

    def test_chan_fifo_ok(self):
        c = specs.ChanFIFO()
        h = [op(0, ("send", 1), ("ok",), 0, 2), op(1, ("send", 2), ("ok",), 1, 3),
             op(2, ("recv",), ("ok", 1), 4, 5), op(2, ("recv",), ("ok", 2), 6, 7)]
        assert checker.check(c, h).linearizable

    def test_chan_phantom_delivery_rejected(self):
        c = specs.ChanFIFO()
        h = [op(0, ("send", 1), ("ok",), 0, 1), op(1, ("recv",), ("ok", 999), 2, 3)]
        assert not checker.check(c, h).linearizable

    def test_chan_fifo_reorder_rejected(self):
        c = specs.ChanFIFO()
        h = [op(0, ("send", 1), ("ok",), 0, 1), op(0, ("send", 2), ("ok",), 2, 3),
             op(1, ("recv",), ("ok", 2), 4, 5), op(1, ("recv",), ("ok", 1), 6, 7)]
        assert not checker.check(c, h).linearizable

    def test_chan_concurrent_sends_may_reorder(self):
        # overlapping sends CAN linearize in either order
        c = specs.ChanFIFO()
        h = [op(0, ("send", 1), ("ok",), 0, 3), op(1, ("send", 2), ("ok",), 1, 2),
             op(2, ("recv",), ("ok", 2), 4, 5), op(2, ("recv",), ("ok", 1), 6, 7)]
        assert checker.check(c, h).linearizable

    def test_semaphore_over_capacity_rejected(self):
        s = specs.Semaphore(2)
        h = [op(0, ("acquire", 1), ("ok",), 0, 9),
             op(1, ("acquire", 1), ("ok",), 1, 8),
             op(2, ("acquire", 1), ("ok",), 2, 7)]
        assert not checker.check(s, h).linearizable

    def test_semaphore_over_release_rejected(self):
        # BOUNDED semaphore: a release that pushes held permits past capacity is
        # an over-release the real primitive raises on -> must NOT linearize.
        s = specs.Semaphore(2)
        # grant 3 permits on a cap-2 sem via an over-release
        assert not checker.check(s, [op(0, ("release", 3), ("ok",), 0, 1),
                                     op(1, ("acquire", 3), ("ok",), 2, 3)]).linearizable
        # 4 permits held simultaneously on cap-2
        assert not checker.check(s, [op(0, ("release", 2), ("ok",), 0, 1),
                                     op(1, ("acquire", 2), ("ok",), 2, 7),
                                     op(2, ("acquire", 2), ("ok",), 3, 6)]).linearizable
        # bare release with nothing held
        assert not checker.check(s, [op(0, ("release", 1), ("ok",), 0, 1)]).linearizable

    def test_waitgroup_early_wait_rejected(self):
        wg = specs.WaitGroup()
        h = [op(0, ("add", 2), ("ok",), 0, 1), op(1, ("wait",), ("ok",), 2, 3)]
        assert not checker.check(wg, h).linearizable

    def test_event_wait_before_set_rejected(self):
        e = specs.Event()
        assert not checker.check(e, [op(0, ("wait",), ("ok",), 0, 1)]).linearizable

    def test_rwmutex_reader_writer_overlap_rejected(self):
        rw = specs.RWMutex()
        h = [op(0, ("rlock",), ("ok",), 0, 5), op(1, ("wlock",), ("ok",), 1, 4)]
        assert not checker.check(rw, h).linearizable

    def test_rwmutex_two_readers_ok(self):
        rw = specs.RWMutex()
        h = [op(0, ("rlock",), ("ok",), 0, 5), op(1, ("rlock",), ("ok",), 1, 4),
             op(1, ("runlock",), ("ok",), 6, 7), op(0, ("runlock",), ("ok",), 8, 9)]
        assert checker.check(rw, h).linearizable


# ----------------------------------------------------------- 2. live battery

@FT
class TestLiveBattery:
    """A bounded seed sweep of the real recorder -- every history must linearize.
    (The full generative sweep is tools/lincheck/linz/battery.py + its forever
    runner; here we keep it small so the suite stays fast.)"""

    def run_primitive(self, primitive):
        import battery
        fatal = []
        for seed in range(4):
            status, detail = battery.check_seed(primitive, seed, True,
                                                None, None, None,
                                                checker.DEFAULT_BUDGET, False)
            if status in battery.FATAL:
                fatal.append((seed, status, detail))
        assert not fatal, "%s: %s" % (primitive, fatal)

    def test_chan(self):
        self.run_primitive("chan")

    def test_mutex(self):
        self.run_primitive("mutex")

    def test_rwmutex(self):
        self.run_primitive("rwmutex")

    def test_semaphore(self):
        self.run_primitive("semaphore")

    def test_waitgroup(self):
        self.run_primitive("waitgroup")

    def test_event(self):
        self.run_primitive("event")


# ----------------------------------------------------- 3. differential vs Go

@FT
class TestDifferentialGo:
    """Record one channel history and check it with BOTH the Python WGL checker
    and the independent Go Porcupine binary; they must agree -- on the clean
    history (LINEARIZABLE) and on a phantom-delivery corruption (NOT)."""

    def record_chan(self, tmp_path):
        rec = os.path.join(REPO, "tools", "lincheck", "record_history.py")
        out = str(tmp_path / "hist.json")
        env = dict(os.environ, PYTHON_GIL="0",
                   PYTHONPATH=os.path.join(REPO, "src"))
        # record_history.py <out> <nhubs> <nprod> <nper> <cap>
        subprocess.check_call([sys.executable, rec, out, "3", "3", "6", "2"],
                              env=env, cwd=REPO)
        with open(out) as fh:
            return json.load(fh), out

    def py_verdict(self, hist):
        spec = specs.ChanFIFO()
        ops = checker.ops_from_events(hist["events"], spec)
        return checker.check(spec, ops).linearizable

    def go_verdict(self, path):
        exe = os.path.join(REPO, "tools", "lincheck", "porcupine", "lincheck")
        rc = subprocess.call([exe, path], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        # exit 0 = linearizable, 1 = not
        return rc == 0

    def test_python_and_go_agree(self, tmp_path):
        exe = os.path.join(REPO, "tools", "lincheck", "porcupine", "lincheck")
        if not os.path.exists(exe):
            pytest.skip("Go porcupine binary not built")
        hist, path = self.record_chan(tmp_path)
        # clean: both LINEARIZABLE
        assert self.py_verdict(hist) is True
        assert self.go_verdict(path) is True
        # corrupt one delivered recv to a never-sent value: both NOT
        for e in hist["events"]:
            if e["op"] == "recv" and e["result"] == "ok":
                e["value"] = 999999
                break
        bad = str(tmp_path / "bad.json")
        with open(bad, "w") as fh:
            json.dump(hist, fh)
        assert self.py_verdict(hist) is False
        assert self.go_verdict(bad) is False
