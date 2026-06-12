"""Cooperative select.select, and a guard on stdlib C-frame footprint.

Background: a fiber runs on a small fixed C stack (default 32 KB, with a
PROT_NONE guard page).  CPython's `select_select_impl` declares three
`pylist[FD_SETSIZE + 1]` arrays -- ~51 KB in a single C frame, the only stdlib
leaf that overflows 32 KB -- so calling it inline in a fiber SEGV'd.  The
fix is NOT a bigger stack: `select.select` is reimplemented cooperatively on a
transient epoll (register the fds, park on the epoll's own fd via netpoll, map
results back), so the fat frame is never allocated on the fiber stack and
the fiber parks like any other socket waiter -- no pool thread, scales.

Two things are tested here:
  * TestCooperativeSelect -- select in a fiber doesn't crash, returns the
    right ready sets, and (the point) stays cooperative: a sibling fiber
    keeps running while one parks in select.
  * TestStdlibFrameFootprint -- a regression guard that measures the C-stack
    high-water mark of the deepest-known stdlib leaves and asserts they fit the
    default fiber stack, so a NEW fat-framed C function (a future stdlib
    addition) that would silently re-arm the SEGV is caught.  select is the
    one allowlisted exception precisely because it's handled cooperatively and
    never runs inline.

SEGV-prone cases run in a child interpreter and assert a clean exit (rc 0);
rc -11 (SIGSEGV) = the regression is back.
"""
import os
import re as _re
import subprocess
import sys
import unittest

import runloom_c

import os as _hwm_os
import pytest as _hwm_pytest
# Stack high-water-mark is precise only on a POSIX guard-page backend
# (fcontext-asm / ucontext) with 4 KB pages.  Windows Fibers have no guard page,
# and macOS 16 KB pages make the mincore-based HWM over-report (it reports the
# whole stack resident), so these HWM/advice/sizing tests can't measure precisely
# there -- skip them (the diagnostic itself just over-reserves, which is safe).
_RELIABLE_HWM = (_hwm_os.name == "posix"
                 and runloom_c.backend() in ("fcontext-asm", "ucontext")
                 and _hwm_os.sysconf("SC_PAGESIZE") == 4096)
pytestmark = _hwm_pytest.mark.skipif(
    not _RELIABLE_HWM,
    reason="stack HWM is reliable only on a POSIX guard-page backend with 4 KB pages")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_child(code, timeout=60):
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import runloom, runloom_c\n"
        "runloom.monkey.patch()\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired:
        return 124, "", "[timed out]"
    return p.returncode, p.stdout, p.stderr


def assert_pass(code, **kw):
    rc, out, err = run_child(code, **kw)
    sig = "  (SIGSEGV -- the stack-overflow regression is back)" if rc == -11 else ""
    assert rc == 0 and "PASS" in out, (
        "rc={0}{1}\n--- stdout ---\n{2}\n--- stderr ---\n{3}".format(
            rc, sig, out, err))
    return out


class TestCooperativeSelect(unittest.TestCase):
    def test_no_segv_empty_select_m1(self):
        # The original crash: select([],[],[],0) inline overflowed 32 KB.
        assert_pass(r"""
import select
def w():
    for _ in range(50):
        select.select([], [], [], 0)
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_no_segv_select_mn(self):
        assert_pass(r"""
import select, socket
pairs = [socket.socketpair() for _ in range(3)]
def w():
    for _ in range(30):
        select.select([a for a, b in pairs], [], [], 0)
        runloom.sleep(0.0001)
runloom.mn_init(2)
for _ in range(6):
    runloom_c.mn_go(w)
runloom.mn_run(); runloom.mn_fini()
print("PASS")
""")

    def test_returns_readable(self):
        assert_pass(r"""
import select, socket
a, b = socket.socketpair()
def w():
    b.sendall(b"x")
    r, wl, x = select.select([a], [], [], 1.0)
    assert r == [a], (r, wl, x)
    assert a.recv(1) == b"x"
print("READY")  # marker before
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_returns_writable(self):
        assert_pass(r"""
import select, socket
a, b = socket.socketpair()
def w():
    r, wl, x = select.select([], [a], [], 1.0)
    assert wl == [a], (r, wl, x)
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_timeout_returns_empty(self):
        # A fd that never becomes readable: select must time out cleanly.
        assert_pass(r"""
import select, socket, time
a, b = socket.socketpair()
def w():
    t0 = time.monotonic()
    r, wl, x = select.select([a], [], [], 0.2)
    dt = time.monotonic() - t0
    assert (r, wl, x) == ([], [], []), (r, wl, x)
    assert dt >= 0.15, dt
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_stays_cooperative_sibling_runs(self):
        # THE point: while one fiber parks in select, a sibling keeps
        # running on the same hub.  If select wedged the hub (blocking inline
        # or busy-poll), the canary would barely tick.  M:1 (one thread) is the
        # strictest check.
        out = assert_pass(r"""
import select, socket, runloom_c
a, b = socket.socketpair()
ticks = [0]
def canary():
    for _ in range(40):
        runloom_c.sched_sleep(0.01)
        ticks[0] += 1
def waiter():
    r, wl, x = select.select([a], [], [], 0.3)   # never readable -> parks 0.3s
    assert (r, wl, x) == ([], [], []), (r, wl, x)
runloom_c.go(canary)
runloom_c.go(waiter)
runloom_c.run()
assert ticks[0] >= 10, ticks[0]   # cooperative: canary ran while waiter parked
print("PASS ticks=%d" % ticks[0])
""")
        m = _re.search(r"ticks=(\d+)", out)
        self.assertIsNotNone(m)
        self.assertGreaterEqual(int(m.group(1)), 10)


class TestStdlibFrameFootprint(unittest.TestCase):
    """Measure the C-stack high-water mark of the deepest-known stdlib leaves
    and assert they fit the default fiber stack.  Catches a NEW fat-framed
    C function before it can re-arm the guard-page SEGV."""

    # Raw (unpatched) C-stack high-water marks, free-threaded 3.13t:
    #   select.select        50.9 KB  -- the FD_SETSIZE arrays (handled: cooperative)
    #   first ssl use        ~40   KB  -- OpenSSL one-time init (handled: main-thread warm)
    #   json (nested)         6.3 KB
    #   getaddrinfo / re      ~2.7 KB
    # Two fat frames exist (select, first-ssl); both have mitigations asserted
    # by their own tests below.  Everything else must fit the default stack.
    LEAVES = {
        "getaddrinfo": "import socket; socket.getaddrinfo('127.0.0.1', 80)",
        "json":        "import json; json.loads(json.dumps({'a':[1,2,{'b':3}]*50}))",
        "re":          "import re; re.match(r'(a|b)*c', 'ab'*40 + 'c')",
    }

    def _measure_hwm(self, op_src):
        # Measure the RAW stdlib leaf (NO monkey.patch): the guard is about the
        # C-frame footprint of the unpatched function -- that's what determines
        # whether it needs a cooperative path.  A roomy 2 MB stack so the fat
        # frame can't crash the measurement.
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "import runloom_c\n"
            "def worker():\n"
            "    %s\n"
            "runloom_c.go(worker, stack_size=2*1024*1024)\n"
            "runloom_c.run()\n"
            "print('HWM', runloom_c.stats().get('stack_hwm', 0))\n"
            % (os.path.join(REPO, "src"), op_src)
        )
        env = dict(os.environ, PYTHON_GIL="0", RUNLOOM_GIL="0")
        p = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                           timeout=60, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, text=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        m = _re.search(r"HWM (\d+)", p.stdout)
        self.assertIsNotNone(m, p.stdout)
        return int(m.group(1))

    def test_leaf_frames_fit_default_stack(self):
        default = runloom_c.get_stack_size()
        # Leave headroom for the Python/user frames stacked above the leaf.
        budget = int(default * 0.6)
        for name, src in self.LEAVES.items():
            hwm = self._measure_hwm(src)
            self.assertLess(
                hwm, budget,
                "{0} uses {1} B of C stack (> {2} B budget of the {3} B default "
                "fiber stack); it needs a cooperative path or an allowlist "
                "entry, like select.select".format(name, hwm, budget, default))

    def test_select_is_the_known_fat_frame(self):
        # select.select's raw frame is the fattest stdlib single frame (~51 KB:
        # three pylist[FD_SETSIZE+1] arrays).  At the 512 KB default it FITS, so
        # the cooperative path (monkey/polling.py) is no longer needed to avoid
        # an overflow -- it stays because it makes select PARK on netpoll instead
        # of blocking the hub.  Still assert it's the known-fat frame so a CPython
        # change is noticed; and that it now fits the default.
        hwm = self._measure_hwm("import select; select.select([], [], [], 0)")
        default = runloom_c.get_stack_size()
        self.assertGreater(hwm, 32 * 1024,
            "select's frame ({0} B) is no longer fat; re-check the measurement"
            .format(hwm))
        self.assertLess(hwm, default,
            "select's frame ({0} B) exceeds the {1} B default -- it would need a "
            "bigger default or stay an overflow risk".format(hwm, default))

    def test_first_ssl_use_is_fat(self):
        # The OTHER fat frame: the first _ssl use drives a ~40 KB OpenSSL init.
        # At the 512 KB default it fits, so ssl-warming-on-main-thread is no
        # longer needed to avoid an overflow -- it stays as a cheap one-time init
        # prepay.  Documented/measured here so it isn't forgotten.
        hwm = self._measure_hwm(
            "import ssl; ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)")
        default = runloom_c.get_stack_size()
        self.assertGreater(hwm, 32 * 1024,
            "first ssl use ({0} B) is no longer fat; re-check".format(hwm))
        self.assertLess(hwm, default,
            "first ssl use ({0} B) exceeds the {1} B default".format(hwm, default))

    def test_ssl_warmed_on_main_thread_so_fiber_is_safe(self):
        # Mitigation: runloom.monkey imports ssl on the main thread and
        # _patch_ssl forces OpenSSL init there, off any fiber stack.  So a
        # fiber that is the first to create an SSLContext must NOT crash.
        # (Guard against a future refactor that lazy-imports ssl -> re-arms the
        # 40 KB init on a 32 KB fiber stack.)
        assert_pass(r"""
import ssl
def w():
    ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)   # first context, on a fiber
runloom_c.go(w); runloom_c.run()
print("PASS")
""")


class TestDeepRecursionSafety(unittest.TestCase):
    """Deeply-nested input to C-recursive stdlib ops must not SEGV a fiber.

    Two mechanisms keep it safe:
      * json/pickle/marshal/copy.deepcopy (~60-80 B of C stack per level)
        degrade to a clean RecursionError -- CPython's recursion counter fires
        (~150 levels ~ 12 KB) well within the 32 KB default stack.
      * ast/compile (~1.5 KB per level, which WOULD SEGV past ~18 deep before
        the counter fires) are auto-offloaded to the backend pool's full-size
        thread stack when called inside a fiber (the `compile` patch).
    eval(str)/exec(str) compile internally in C (not via builtins.compile) and
    are the documented residual -- use offload()/a roomier g-stack.
    """

    def test_json_bomb_is_clean_recursionerror(self):
        assert_pass(r"""
import json
def w():
    try:
        json.loads("[" * 5000 + "]" * 5000)
        ok = "no-error"
    except RecursionError:
        ok = "clean"
    assert ok == "clean", ok
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_pickle_deep_is_clean(self):
        assert_pass(r"""
import pickle
def w():
    # build the nesting in-fiber (pure-Python loop -> datastack, safe);
    # pickle.dumps then C-recurses and must hit a clean RecursionError, not SEGV.
    x = []; cur = x
    for _ in range(5000):
        n = []; cur.append(n); cur = n
    try:
        pickle.loads(pickle.dumps(x))
        ok = "no-error"
    except RecursionError:
        ok = "clean"
    assert ok == "clean", ok
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_compile_deep_offloaded_no_segv(self):
        # compile of 100-deep nested source SEGVs inline (~1.5 KB/level) but is
        # auto-offloaded to the pool's 8 MB stack by the `compile` patch.
        assert_pass(r"""
SRC = "(" * 100 + "1" + ")" * 100
def w():
    code = compile(SRC, "<s>", "eval")   # auto-offloaded inside a fiber
    assert eval(code) == 1
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_ast_parse_deep_offloaded_no_segv(self):
        # ast.parse routes through builtins.compile, so it's covered too.
        assert_pass(r"""
import ast
SRC = "(" * 100 + "1" + ")" * 100
def w():
    tree = ast.parse(SRC)
    assert type(tree).__name__ == "Module"
runloom_c.go(w); runloom_c.run()
print("PASS")
""")

    def test_compile_passthrough_off_fiber(self):
        # Off any fiber, compile must be the plain builtin (no offload).
        assert_pass(r"""
assert eval(compile("6*7", "<s>", "eval")) == 42
print("PASS")
""")


if __name__ == "__main__":
    unittest.main()
