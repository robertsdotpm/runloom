"""Misc-contracts coverage: three under-covered runtime guarantees.

  (1) immortalize(x) -- must be an IDENTITY function (returns the SAME object),
      the object must stay fully usable afterwards, and immortalizing an
      already-immortal singleton (None/True/42/an interned str) must be a pure
      no-op that leaves the runtime self-check clean.  The existing coverage for
      this lives in big_100/p229 which is skipped under the GIL; this is a plain
      top-level test so it runs on the free-threaded interpreter.

  (2) set_stack_scrub(True) actually WIPES a recycled fiber stack.  A single
      fiber (A) writes a 0xAB sentinel onto its own C stack at a fixed frame
      offset and returns; the scheduler recycles that coro/stack (LIFO pool);
      a second fiber (B) -- proven to reuse the SAME stack by an identical
      sentinel address -- reads that same offset.  With scrub ON the bytes must
      read back all-zero (the security wipe); with scrub OFF the sentinel must
      still be visible (proving the wipe, not some other effect, is what zeroed
      it).  The sentinel is poked/peeked with a tiny ctypes helper compiled at
      test time, because the fiber C stack is not otherwise reachable from
      Python.

  (3) DatagramProtocol.error_received fires on an ICMP port-unreachable.  A
      connected UDP endpoint (remote_addr = a local port with no listener) that
      sendto()s must, on a platform that delivers the ICMP error back on the
      connected socket, surface it through error_received(exc).  Skips cleanly
      where the platform does not deliver it.
"""
import asyncio
import os
import socket
import subprocess
import sys
import sysconfig
import tempfile

import pytest

import runloom
import runloom_c as rc
import runloom.aio as aio
from adv_util import hang_guard


# ==========================================================================
# (1) immortalize -- identity + usability + no-op on already-immortal
# ==========================================================================
def test_immortalize_returns_same_object_and_stays_usable():
    x = [1, 2, 3]
    r = rc.immortalize(x)
    assert r is x, "immortalize(x) must return the SAME object, not a copy"
    # The object must remain fully usable after its refcount is frozen.
    x.append(4)
    x[0] = 99
    assert x == [99, 2, 3, 4]
    assert rc.immortalize(x) is x            # idempotent
    assert rc._self_check(0) == 0


def test_immortalize_of_custom_instance_is_identity():
    class Box(object):
        def __init__(self):
            self.v = 7
    b = Box()
    assert rc.immortalize(b) is b
    b.v = 8                                  # still mutable / usable
    assert b.v == 8
    assert rc._self_check(0) == 0


def test_immortalize_singletons_are_noops():
    # Already-immortal singletons: immortalizing them is a pure identity no-op
    # and must not perturb any runtime invariant.
    for obj in (None, True, False, 42, sys.intern("cov_misc_intern_probe")):
        assert rc.immortalize(obj) is obj, "immortalize must be identity for %r" % (obj,)
    # Interned string stays interned + equal after the freeze.
    s = sys.intern("cov_misc_intern_probe")
    assert rc.immortalize(s) is s
    assert s == "cov_misc_intern_probe"
    assert rc._self_check(0) == 0


# ==========================================================================
# (2) set_stack_scrub -- the actual WIPE of a recycled fiber stack
# ==========================================================================
# The stack HWM / scrub machinery is only introspectable on a POSIX guard-page
# backend (fcontext-asm / ucontext); Windows Fibers have no reachable stack and
# the madvise/mincore scrub is POSIX-only.  Gate exactly like test_stack_advice.
_SCRUB_TESTABLE = (os.name == "posix"
                   and rc.backend() in ("fcontext-asm", "ucontext"))

# A tiny helper whose local C-stack buffer we poke a sentinel into (fiber A) and
# peek back (fiber B).  ONE function with a `fill` flag so both fibers use an
# identical frame -> the buffer lands at the SAME absolute stack address when the
# recycled stack is reused, which we assert to prove reuse actually happened.
_SCRUB_HELPER_C = r"""
#include <stdint.h>
uintptr_t rl_stack_op(int fill, int nbytes, int *count_out)
{
    volatile unsigned char buf[8192];
    int i, c = 0;
    if (nbytes > 8192) nbytes = 8192;
    /* Count the sentinel that is ALREADY present (whatever the recycled stack
     * holds) BEFORE writing anything, so fiber B observes the leftover state. */
    for (i = 0; i < nbytes; i++)
        if (buf[i] == (unsigned char)0xAB) c++;
    *count_out = c;
    if (fill)
        for (i = 0; i < nbytes; i++) buf[i] = (unsigned char)0xAB;
    return (uintptr_t)buf;
}
"""


def _build_scrub_helper():
    """Compile the poke/peek helper to a .so; return a ctypes handle or None."""
    cc = sysconfig.get_config_var("CC") or os.environ.get("CC") or "cc"
    cc = cc.split()[0]                        # strip any trailing flags
    tmp = tempfile.mkdtemp(prefix="rl_scrub_")
    src = os.path.join(tmp, "rl_scrub_probe.c")
    so = os.path.join(tmp, "rl_scrub_probe.so")
    with open(src, "w") as f:
        f.write(_SCRUB_HELPER_C)
    try:
        # -O0: keep the frame layout deterministic so the buffer address is
        # reproducible across the two fibers.
        subprocess.check_call([cc, "-O0", "-fPIC", "-shared", "-o", so, src],
                              stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except (OSError, subprocess.CalledProcessError):
        return None
    import ctypes
    lib = ctypes.CDLL(so)
    lib.rl_stack_op.restype = ctypes.c_size_t
    lib.rl_stack_op.argtypes = [ctypes.c_int, ctypes.c_int,
                                ctypes.POINTER(ctypes.c_int)]
    return lib


# Use an unusual, page-aligned stack size so this size-class coro pool is not
# shared with any other fiber in the process -> deterministic LIFO reuse of the
# exact stack fiber A ran on.
_SCRUB_STACK = 148 * 1024        # 37 pages
_SENTINEL_N = 4096               # bytes of 0xAB at the deep end of the buffer


def _scrub_probe(lib, scrub):
    """Run fiber A (fill+return) then fiber B (peek) with scrub on/off.

    Returns (addr_a, addr_b, leftover_count): how many of the first _SENTINEL_N
    bytes fiber B still sees as the 0xAB sentinel on the recycled stack.
    """
    import ctypes
    state = {}

    def fiber_a():
        c = ctypes.c_int(-1)
        addr = lib.rl_stack_op(1, _SENTINEL_N, ctypes.byref(c))
        state["addr_a"] = int(addr)

    def fiber_b():
        c = ctypes.c_int(-1)
        addr = lib.rl_stack_op(0, _SENTINEL_N, ctypes.byref(c))
        state["addr_b"] = int(addr)
        state["count"] = int(c.value)

    old = rc.get_stack_scrub()
    try:
        rc.set_stack_scrub(scrub)
        rc.fiber(fiber_a, _SCRUB_STACK)
        rc.run()                              # A completes -> coro recycled (scrubbed iff on)
        rc.fiber(fiber_b, _SCRUB_STACK)
        rc.run()                              # B reuses the recycled coro/stack
    finally:
        rc.set_stack_scrub(old)
    return state["addr_a"], state["addr_b"], state["count"]


@pytest.mark.skipif(not _SCRUB_TESTABLE,
                    reason="stack scrub is observable only on a POSIX fcontext/ucontext backend")
def test_stack_scrub_wipes_recycled_stack():
    lib = _build_scrub_helper()
    if lib is None:
        pytest.skip("no working C compiler to build the stack-poke helper")

    with hang_guard(30, "stack scrub wipe probe"):
        # scrub OFF: fiber B must reuse the SAME stack and still see the sentinel.
        a_off, b_off, count_off = _scrub_probe(lib, False)
        # scrub ON: same reuse, but every sentinel byte must have been wiped.
        a_on, b_on, count_on = _scrub_probe(lib, True)

    # Reuse is the precondition for the wipe check; if the recycled stack was not
    # reused (a different stack / an unexpected allocation intervened) we cannot
    # observe the wipe -- skip rather than mis-report.
    if a_off != b_off or a_on != b_on:
        pytest.skip("recycled stack was not reused (addr %x!=%x / %x!=%x); "
                    "cannot observe the wipe" % (a_off, b_off, a_on, b_on))

    # scrub OFF: the sentinel fiber A wrote survives on the recycled stack.
    assert count_off >= _SENTINEL_N - 64, (
        "scrub OFF should leave the sentinel visible on the recycled stack, "
        "saw %d/%d bytes" % (count_off, _SENTINEL_N))
    # scrub ON: every sentinel byte must be zero -- the actual security wipe.
    assert count_on == 0, (
        "scrub ON must WIPE the recycled stack, but %d/%d sentinel bytes "
        "survived recycle" % (count_on, _SENTINEL_N))
    assert rc._self_check(0) == 0


# ==========================================================================
# (3) DatagramProtocol.error_received on an ICMP port-unreachable
# ==========================================================================
def _free_udp_port():
    """Bind+release a loopback UDP port so nothing is listening on it."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def test_datagram_error_received_on_icmp_unreachable():
    dead_port = _free_udp_port()
    got = {}

    async def body():
        loop = asyncio.get_event_loop()

        class Proto(asyncio.DatagramProtocol):
            def error_received(self, exc):
                got["exc"] = exc
            def datagram_received(self, data, addr):
                got["data"] = (data, addr)

        # Connected UDP endpoint aimed at a local port with no listener.  The
        # kernel returns an ICMP port-unreachable, which a CONNECTED UDP socket
        # surfaces as ECONNREFUSED on its next recv -> error_received(exc).
        tr, pr = await loop.create_datagram_endpoint(
            Proto, remote_addr=("127.0.0.1", dead_port))
        try:
            # Several sends across a short window: the ICMP error is asynchronous.
            for _ in range(6):
                tr.sendto(b"probe")
                await asyncio.sleep(0.05)
                if "exc" in got:
                    break
        finally:
            tr.close()
            await asyncio.sleep(0)

    with hang_guard(20, "datagram error_received"):
        aio.run(body())

    if "exc" not in got:
        pytest.skip("platform did not deliver an ICMP port-unreachable back on "
                    "the connected UDP socket")
    exc = got["exc"]
    assert isinstance(exc, OSError), (
        "error_received must be passed an OSError-like exception, got %r" % (exc,))
    # It should specifically reflect the unreachable destination.
    assert isinstance(exc, ConnectionRefusedError) or exc.errno is not None, (
        "expected a connection-refused / errno-bearing OSError, got %r" % (exc,))
    assert "data" not in got, "no datagram should have been received"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
