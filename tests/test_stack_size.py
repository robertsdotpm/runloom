"""Goroutine C-stack sizing: the default floor must clear the stdlib's
deepest single C frame so a plain stdlib call in a goroutine never overflows.

The motivating crash: CPython's select.select() declares three
`pylist[FD_SETSIZE + 1]` arrays and burns ~51 KB of C stack in ONE frame
(every other stdlib leaf is <10 KB).  The old 32 KB default sat inside that
frame's footprint, so the very first select.select() in a goroutine ran its
-fstack-clash-protection probe straight into the coro's PROT_NONE guard page
-> SIGSEGV, on EVERY scheduler (M:1 `go`, M:N `mn_go`).  The fix raises the
default to 128 KB and makes calibration ratchet UP only, never below the floor.

A stack overflow takes the whole process down, so the SEGV-prone cases run in
a child interpreter and assert a clean exit: rc 0 = safe, rc -11 (SIGSEGV) =
the regression is back.
"""
import os
import subprocess
import sys

import runloom_c

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Measured high-water marks (runloom_coro_scan_hwm, free-threaded 3.13t):
#   select.select [],[],[],0  -> 50.9 KB     (the worst; FD_SETSIZE arrays)
#   ssl handshake             ->  8.0 KB
#   json (nested)             ->  6.3 KB
#   getaddrinfo / re          ->  ~2.7 KB
SELECT_HWM = 52168          # bytes, the empirically deepest stdlib C frame
SELECT_SAFE_FLOOR = 56 * 1024   # smallest stack size at which select succeeds


def run_child(code, timeout=60, extra_env=None):
    """Run `code` in a fresh free-threaded child; return (rc, out, err).
    A guard-page stack overflow shows up as rc == -11 (SIGSEGV)."""
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import runloom, runloom_c\n"
        "runloom.monkey.patch()\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    if extra_env:
        env.update(extra_env)
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired:
        return 124, "", "[timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


def assert_ok(code, **kw):
    rc, out, err = run_child(code, **kw)
    signal = ("  (SIGSEGV -- stack overflow: the regression is back)"
              if rc == -11 else "")
    assert rc == 0 and "PASS" in out, (
        "rc={0}{1}\n--- stdout ---\n{2}\n--- stderr ---\n{3}".format(
            rc, signal, out, err))
    return out


# ---------------------------------------------------------------------------
# The floor itself -- a cheap, in-process guard against anyone lowering the
# default back into select's footprint.
# ---------------------------------------------------------------------------
def test_default_stack_clears_select_frame():
    default = runloom_c.get_stack_size()
    assert default >= SELECT_SAFE_FLOOR, (
        "default g-stack {0} B < select-safe floor {1} B -- select.select() "
        "will SEGV in a goroutine".format(default, SELECT_SAFE_FLOOR))
    # The shipped default is 128 KB; assert it explicitly so a silent change
    # is caught (raise this if a deeper stdlib frame is ever measured).
    assert default >= 128 * 1024


def test_set_get_stack_size_roundtrip():
    orig = runloom_c.get_stack_size()
    try:
        runloom_c.set_stack_size(256 * 1024)
        assert runloom_c.get_stack_size() == 256 * 1024
    finally:
        runloom_c.set_stack_size(orig)


# ---------------------------------------------------------------------------
# The crash itself: select.select() in a goroutine on every scheduler.
# ---------------------------------------------------------------------------
def test_select_in_goroutine_no_segv_m1():
    assert_ok(r"""
import select
def w():
    for _ in range(50):
        select.select([], [], [], 0)
runloom_c.go(w)
runloom_c.run()
print("PASS")
""")


def test_select_in_goroutine_no_segv_mn():
    assert_ok(r"""
import select
def w():
    for _ in range(50):
        select.select([], [], [], 0)
        runloom.sleep(0.0001)
runloom.mn_init(2)
for _ in range(4):
    runloom_c.mn_go(w)
runloom.mn_run(); runloom.mn_fini()
print("PASS")
""")


def test_select_with_fds_in_goroutine_no_segv():
    # A select with real fds takes the same fat frame; confirm it too is safe.
    assert_ok(r"""
import select, socket
pairs = [socket.socketpair() for _ in range(3)]
def w():
    select.select([a for a, b in pairs], [], [], 0)
runloom.mn_init(2)
runloom_c.mn_go(w)
runloom.mn_run(); runloom.mn_fini()
print("PASS")
""")


# ---------------------------------------------------------------------------
# Calibration must ratchet UP from the floor, never below it: a program whose
# first RUNLOOM_CAL_TARGET (1000) goroutines are all shallow used to freeze the
# default down to the MIN floor, re-arming the SEGV on a later select.
# ---------------------------------------------------------------------------
def test_calibration_never_shrinks_below_floor():
    assert_ok(r"""
import select
def shallow():
    pass
# Drive >1000 trivial completions on M:1 so calibration freezes.
for _ in range(1100):
    runloom_c.go(shallow)
runloom_c.run()
st = runloom_c.stats()
assert st.get("stack_calibrated"), st
floor = runloom_c.get_stack_size()
assert floor >= 128 * 1024, ("calibration shrank below floor: %d" % floor)
# And select must still be safe on the post-calibration default.
def w():
    select.select([], [], [], 0)
runloom_c.go(w)
runloom_c.run()
print("PASS")
""")


# ---------------------------------------------------------------------------
# RUNLOOM_STACK_SIZE env override (non-freezing): lets the high-goroutine-count
# memory model trade the floor down, and a smaller-but-still-safe value works.
# ---------------------------------------------------------------------------
def test_env_stack_size_override_down():
    out = assert_ok(r"""
import select
sz = runloom_c.get_stack_size()
assert sz == 64 * 1024, sz
# 64 KB still clears select (HWM ~51 KB), so a select g is safe.
def w():
    select.select([], [], [], 0)
runloom_c.go(w)
runloom_c.run()
print("PASS size=%d" % sz)
""", extra_env={"RUNLOOM_STACK_SIZE": str(64 * 1024)})
    assert "size=65536" in out


def test_env_stack_size_override_up():
    assert_ok(r"""
sz = runloom_c.get_stack_size()
assert sz == 512 * 1024, sz
print("PASS")
""", extra_env={"RUNLOOM_STACK_SIZE": str(512 * 1024)})
