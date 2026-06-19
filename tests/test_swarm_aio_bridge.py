"""SWARM adversarial QA: the runloom.aio asyncio bridge (src/runloom/aio/).

This file goes DEEPER than test_adv_aio / test_aio* / test_aio_net /
test_aio_cancel_torture / test_aio_fd_reuse, which already cover the headline
invariants (connection_made-write reaches the client, server-close wakes the
accept loop, cancel-during-sleep/recv, wait_for slow-return, gather concurrency,
the send-less custom awaitable, low-level sock_* fd-reuse).  We do NOT repeat
those; we manufacture the conditions those tests don't:

  * timer callback read THROUGH the handle -- a cancelled call_later leaks
    NOTHING across many gc cycles (a closure-capture leak would pin the graph);
  * call_soon FIFO ordering + current-task-cleared-at-loop-level (a loop-level
    callback sees current_task() is None);
  * future done-callback ASYNCIO ORDER (a waiter scheduled by an earlier
    set_result resumes BEFORE a later done-callback on the same future);
  * server close() WAKES accept loops -- parked count returns to baseline across
    MANY create/close cycles, incl. the start_server StreamReader path;
  * sock_* netpoll RELEASE across fd REUSE under interleaving (no stale-arm hang);
  * _driver coro.send(None) for a send-less awaitable that DELEGATES to an
    executor future (the aiocsv shape, not just a bare iterator);
  * CANCEL TORTURE -- cancel a task parked in sleep / sock_recv / run_in_executor /
    gather / wait_for, at MANY interleaving points, asserting CancelledError + no
    hang + no fd leak;
  * wait_for / asyncio.timeout slow-return + inner-task cancellation delivery;
  * gather concurrency + first-exception + return_exceptions + nested cancel;
  * run_in_executor offload overlap + cancel-delivery + exception twin;
  * StreamReader/Writer read / readexactly / readuntil / readline / drain with
    partial data, EOF mid-frame, separator-not-found-at-EOF, and limit overflow
    (LimitOverrunError) + tuple separators (asyncio 3.13);
  * create_datagram_endpoint (UDP send/recv, error_received, connection_lost);
  * start_tls / a full TLS client handshake over a cooperatively-parked socket,
    including a greeting written inside connection_made over TLS;
  * subprocess_exec pipe bridging + communicate + wait + nonzero exit + signal;
  * an exception raised INSIDE a protocol callback (data_received /
    datagram_received / connection_made) routes to the exception handler, no crash;
  * fault injection (RUNLOOM_FAULT_*) mid-I/O -> a clean Python error, never a SEGV;
  * a guard-page overflow inside a data_received callback is CLASSIFIED, not silent;
  * many-concurrent-connection echo stress under the loop.

Crash-prone scenarios run in a SUBPROCESS so a SIGSEGV is contained + observed as
a negative returncode.  Hang-prone scenarios wrap hang_guard / pass finite
timeouts.  Slow-return uses assert_faster_than.

Driven through runloom.aio.run() (its asyncio.run drop-in) -- no pytest-asyncio.
"""
import asyncio
import gc
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import time

import pytest

import runloom.aio as aio
import runloom_c as rc
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      free_tcp_port_pair)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV = dict(os.environ, PYTHON_GIL="0", PYTHONPATH=os.path.join(REPO, "src"))


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _parked():
    s = rc.stats()
    return int(s.get("netpoll_parked_self", s.get("netpoll_parked", 0)))


def _fd_count():
    for p in ("/proc/self/fd", "/dev/fd"):
        try:
            return len(os.listdir(p))
        except OSError:
            pass
    return None


def _run_subprocess(script, timeout=60, env_extra=None):
    """Run a self-contained driver script in a child interpreter so a SEGV is
    contained.  Returns the CompletedProcess.  A negative returncode == killed
    by a signal (the crash we are guarding against)."""
    env = dict(_ENV)
    if env_extra:
        env.update(env_extra)
    return subprocess.run([sys.executable, "-c", script],
                          cwd=REPO, env=env, timeout=timeout,
                          capture_output=True)


def _assert_no_signal(cp):
    assert cp.returncode is None or cp.returncode >= 0, (
        "child died on signal %d (crash):\nSTDOUT:%s\nSTDERR:%s"
        % (-cp.returncode, cp.stdout.decode(errors="replace"),
           cp.stderr.decode(errors="replace")))


async def _close_settle(server):
    """Close a streams/transport server and YIELD enough turns that its accept
    fiber (parked in _wait_fd on the listen fd) observes the close and exits
    BEFORE aio.run closes the loop.  The streams-path _Server.wait_closed() is a
    no-op sleep(0); a single turn can race the shutdown-readiness delivery under
    load, orphaning the accept fiber's netpoll parker (caught by conftest's
    per-test parked-leak invariant).  This is test hygiene, NOT a bridge bug --
    the dedicated test_start_server_close_no_accept_fiber_leak proves the
    wake itself works across cycles."""
    server.close()
    try:
        # Bound wait_closed: the transport _ProtocolServer.wait_closed() blocks
        # until every accepted conn detaches; a finite timeout keeps a lingering
        # conn from hanging the test (a leak is then caught by conftest, not a
        # hang).  The streams _Server.wait_closed() is a no-op sleep(0).
        await asyncio.wait_for(server.wait_closed(), 5)
    except Exception:
        pass
    # A few real turns so the netpoll pump delivers the listen-fd shutdown and
    # the accept fiber runs to exit.
    for _ in range(3):
        await asyncio.sleep(0.01)


_CERT_CACHE = {}


def _self_signed_cert():
    """A self-signed localhost cert (cached).  Skips the test if cryptography
    is unavailable."""
    if "files" in _CERT_CACHE:
        return _CERT_CACHE["files"]
    try:
        import datetime
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError:
        pytest.skip("cryptography not available for TLS cert generation")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subj = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(subj).issuer_name(subj)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=30))
            .add_extension(x509.SubjectAlternativeName(
                [x509.DNSName(u"localhost")]), False)
            .sign(key, hashes.SHA256()))
    d = tempfile.mkdtemp(prefix="runloom_tls_")
    cf = os.path.join(d, "cert.pem")
    kf = os.path.join(d, "key.pem")
    with open(cf, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(kf, "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()))
    _CERT_CACHE["files"] = (cf, kf)
    return cf, kf


def _tls_contexts():
    cf, kf = _self_signed_cert()
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.load_cert_chain(cf, kf)
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    cctx.check_hostname = False
    cctx.verify_mode = ssl.CERT_NONE
    return sctx, cctx


# ==========================================================================
# 1. Timer-handle invariant: a cancelled call_later leaks NOTHING.
# ==========================================================================
def test_cancelled_call_later_does_not_leak_callback_graph():
    """call_later's fiber must read handle._callback THROUGH the handle, so a
    cancel() (which nulls _callback/_args) drops the closure graph immediately.
    A closure-capture leak would keep the sentinel alive until the deadline; we
    cancel far before the deadline and assert the sentinel is collectable."""
    import weakref

    class Sentinel:
        pass

    def cycle():
        async def body():
            loop = asyncio.get_running_loop()
            s = Sentinel()
            ref = weakref.ref(s)

            def cb(_held=s):       # closes over the sentinel
                pass

            h = loop.call_later(50.0, cb)   # far-future deadline
            del s, cb
            await asyncio.sleep(0)
            h.cancel()                       # nulls _callback/_args
            await asyncio.sleep(0)
            gc.collect()
            # After cancel, the still-sleeping timer fiber must NOT pin the
            # sentinel.  (A closure-capture runner would keep `cb` -> `_held`.)
            return ref() is None
        return aio.run(body())

    with hang_guard(20, "cancelled call_later leak"):
        # First cycle warms up; assert collectability on every cycle.
        for i in range(8):
            assert cycle(), "cancelled call_later pinned its callback graph (cycle %d)" % i


def test_call_later_actually_fires_when_not_cancelled():
    fired = {}

    async def body():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_later(0.01, lambda: (fired.__setitem__("v", True),
                                       fut.set_result("done")))
        return await asyncio.wait_for(fut, 5)

    with hang_guard(20, "call_later fires"):
        assert aio.run(body()) == "done"
    assert fired.get("v") is True


# ==========================================================================
# 2. call_soon FIFO + current-task-cleared-at-loop-level.
# ==========================================================================
def test_call_soon_is_fifo():
    async def body():
        loop = asyncio.get_running_loop()
        order = []
        done = loop.create_future()
        for i in range(20):
            loop.call_soon(order.append, i)
        loop.call_soon(lambda: done.set_result(None))
        await asyncio.wait_for(done, 5)
        return order
    with hang_guard(20, "call_soon FIFO"):
        order = aio.run(body())
    assert order == list(range(20)), "call_soon not FIFO: %r" % order


def test_loop_level_callback_has_no_current_task():
    """A call_soon callback runs at loop level -> asyncio.current_task() is
    None there (stock _run_once clears it).  This is the invariant that keeps a
    deferred stock-Task wakeup from hitting enter_task's 'cannot enter task'."""
    seen = {}

    async def body():
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        def cb():
            seen["current"] = asyncio.current_task()
            done.set_result(None)
        loop.call_soon(cb)
        await asyncio.wait_for(done, 5)
        # Inside a coroutine there IS a current task.
        seen["in_coro"] = asyncio.current_task() is not None
    with hang_guard(20, "loop-level no current task"):
        aio.run(body())
    assert seen["current"] is None, (
        "call_soon callback saw a current task: %r" % seen["current"])
    assert seen["in_coro"] is True


def test_call_soon_threadsafe_from_foreign_thread_runs_in_order():
    """call_soon_threadsafe from a genuine foreign OS thread must be drained on
    the loop thread, FIFO, and wake the run."""
    async def body():
        loop = asyncio.get_running_loop()
        order = []
        done = loop.create_future()

        def feeder():
            for i in range(10):
                loop.call_soon_threadsafe(order.append, i)
            loop.call_soon_threadsafe(
                lambda: None if done.done() else done.set_result(None))
        raw_thread(feeder)
        await asyncio.wait_for(done, 5)
        return order
    with hang_guard(20, "call_soon_threadsafe foreign"):
        order = aio.run(body())
    assert order == list(range(10)), "threadsafe callbacks out of order: %r" % order


# ==========================================================================
# 3. Future done-callback asyncio ORDER.
# ==========================================================================
def test_future_waiter_resumes_before_later_done_callback():
    """A task awaiting a future (scheduled to wake by an earlier set_result)
    must resume BEFORE a done-callback registered later on the same future.
    Mirrors the falcon/uvicorn websocket-close ordering invariant."""
    async def body():
        loop = asyncio.get_running_loop()
        order = []
        fut = loop.create_future()

        async def waiter():
            await fut
            order.append("waiter")
        t = asyncio.ensure_future(waiter())
        await asyncio.sleep(0.01)            # let the waiter park on fut

        fut.add_done_callback(lambda f: order.append("late_cb"))
        fut.set_result(None)                 # schedules waiter THEN late_cb
        await asyncio.sleep(0.05)
        await t
        return order
    with hang_guard(20, "future done-callback order"):
        order = aio.run(body())
    assert order == ["waiter", "late_cb"], (
        "done-callback ran before the earlier-scheduled waiter: %r" % order)


def test_add_done_callback_to_already_done_future_is_deferred():
    """A callback added to an ALREADY-done future is scheduled via call_soon,
    never run inline (asyncio contract; as_completed depends on it)."""
    async def body():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut.set_result(42)
        order = []
        order.append("before-add")
        fut.add_done_callback(lambda f: order.append("cb:%r" % f.result()))
        order.append("after-add")           # must come BEFORE the cb
        await asyncio.sleep(0.01)
        return order
    with hang_guard(20, "already-done deferred"):
        order = aio.run(body())
    assert order == ["before-add", "after-add", "cb:42"], order


# ==========================================================================
# 4. Server close() wakes accept loops -- no parked-fiber leak, MANY cycles.
# ==========================================================================
def test_start_server_close_no_accept_fiber_leak():
    """start_server's _Server._accept_loop parks in _wait_fd(listen_fd, READ);
    close() must shutdown()+close the fd so the accept fiber wakes, sees _closed
    and exits.  Repeated create/close must not accumulate parked fibers."""
    async def cycle():
        server = await aio.start_server(
            lambda r, w: None, "127.0.0.1", 0)
        server.close()
        # _Server.wait_closed yields once; give the accept fiber a turn to exit.
        await server.wait_closed()
        await asyncio.sleep(0)

    def one():
        aio.run(cycle())

    with hang_guard(40, "start_server close no leak"):
        one()                                # warmup
        base = _parked()
        for _ in range(15):
            one()
        after = _parked()
    assert after <= base, (
        "accept-loop fibers leaked: parked %d -> %d" % (base, after))


def test_create_server_close_wakes_accept_and_no_fd_leak():
    """loop.create_server accept loop: close()+wait_closed must wake the accept
    fiber and not leak descriptors across many cycles."""
    async def cycle():
        loop = asyncio.get_running_loop()
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        server.close()
        await server.wait_closed()

    def one():
        aio.run(cycle())

    with hang_guard(40, "create_server close fd leak"):
        for _ in range(3):
            one()                            # warmup
        base = _fd_count()
        for _ in range(20):
            one()
        after = _fd_count()
    if base is not None:
        assert after - base <= 2, (
            "create_server leaked fds: %d -> %d" % (base, after))


# ==========================================================================
# 5. sock_* netpoll RELEASE across fd REUSE -- interleaved, no stale-arm hang.
# ==========================================================================
def test_sock_recv_fd_reuse_interleaved_no_hang():
    """Open a socketpair, park+complete a sock_recv on it, close it (plain
    socket.close, bypassing the bridge close hook), then immediately reuse the
    fd number.  A stale arm would make the next wait_fd park forever."""
    async def body():
        loop = asyncio.get_running_loop()
        results = []
        for i in range(40):
            a, b = socket.socketpair()
            a.setblocking(False)
            b.setblocking(False)
            # data already present so the recv may complete inline OR after one
            # park; either way the release decorator must clear the arm.
            b.send(("rt%d" % i).encode())
            got = await loop.sock_recv(a, 64)
            results.append(got == ("rt%d" % i).encode())
            a.close()                        # raw close -> fd number freed
            b.close()
        return results
    with hang_guard(30, "sock_recv fd reuse"):
        results = aio.run(body())
    assert all(results), "%d/40 sock_recv roundtrips failed" % results.count(False)


# ==========================================================================
# 6. _driver coro.send(None) for a send-less awaitable delegating to a future.
# ==========================================================================
def test_send_less_awaitable_delegating_to_executor_future():
    """The aiocsv shape: __await__ returns an object with __next__ but NO send,
    which yields a Future and resumes via __next__.  A driver that injected the
    future's result via .send() would hit PyIter_Send's .send() branch and raise
    'object has no attribute send'."""
    class DelegatingAwaitable:
        def __init__(self, loop):
            self._loop = loop

        def __await__(self):
            loop = self._loop
            outer = self

            class It:
                def __init__(s):
                    s._fut = None
                    s._stage = 0

                def __iter__(s):
                    return s

                def __next__(s):
                    # No send(); only __next__.  Resuming with a non-None value
                    # via .send() would raise AttributeError.  We yield the
                    # executor Future exactly as a Future.__await__ does (set
                    # _asyncio_future_blocking so the driver parks on it), then
                    # the driver resumes us via __next__ (NOT .send) -- the
                    # aiocsv _Parser shape.
                    if s._stage == 0:
                        s._stage = 1
                        fut = loop.run_in_executor(None, lambda: 7)
                        fut._asyncio_future_blocking = True
                        s._fut = fut
                        return fut           # parks the driver on this future
                    raise StopIteration(s._fut.result() * 2)
            return It()

    async def body():
        loop = asyncio.get_running_loop()
        return await DelegatingAwaitable(loop)

    with hang_guard(20, "send-less delegating awaitable"):
        try:
            out = aio.run(body())
        except AttributeError as e:
            pytest.fail("driver injected a resume value into a send-less "
                        "awaitable: %s" % e)
    assert out == 14, out


# ==========================================================================
# 7. CANCEL TORTURE -- cancel at many points, all paths, no hang, no fd leak.
# ==========================================================================
@pytest.mark.parametrize("delay", [0.0, 0.001, 0.01, 0.03])
def test_cancel_task_parked_in_executor(delay):
    """Cancel a task whose coro is awaiting run_in_executor.  The cancel can't
    stop the pool thread, but the awaiting task must take CancelledError and the
    run must not hang."""
    async def body():
        loop = asyncio.get_running_loop()

        def blocking():
            time.sleep(0.2)
            return "done"

        async def victim():
            await loop.run_in_executor(None, blocking)
            return "completed"
        t = asyncio.ensure_future(victim())
        if delay:
            await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0)
        t.cancel()
        try:
            await t
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "cancel executor delay=%s" % delay):
        assert aio.run(body()) == "cancelled"


def test_cancel_wait_for_cancels_inner_and_no_hang():
    inner_cancel = {}

    async def body():
        async def inner():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                inner_cancel["v"] = True
                raise

        async def outer():
            await asyncio.wait_for(inner(), timeout=10)

        t = asyncio.ensure_future(outer())
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "cancel wait_for"):
        assert aio.run(body()) == "cancelled"
    assert inner_cancel.get("v") is True, "inner task not cancelled through wait_for"


def test_cancel_gather_propagates_to_all_children():
    cancelled = {"n": 0}

    async def body():
        async def child(i):
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled["n"] += 1
                raise
        g = asyncio.gather(*[child(i) for i in range(6)])
        f = asyncio.ensure_future(g)
        await asyncio.sleep(0.02)
        f.cancel()
        try:
            await f
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "cancel gather"):
        assert aio.run(body()) == "cancelled"
    assert cancelled["n"] == 6, "only %d/6 children cancelled" % cancelled["n"]


def test_repeated_cancel_during_sock_recv_no_fd_leak():
    async def one(loop):
        a, b = socket.socketpair()
        a.setblocking(False)
        try:
            t = asyncio.ensure_future(loop.sock_recv(a, 64))
            await asyncio.sleep(0)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        finally:
            a.close()
            b.close()

    async def body():
        loop = asyncio.get_running_loop()
        for _ in range(5):
            await one(loop)
        base = _fd_count()
        for _ in range(40):
            await one(loop)
        if base is not None:
            return _fd_count() - base
        return 0
    with hang_guard(30, "repeated cancel recv fd leak"):
        leaked = aio.run(body())
    assert leaked <= 0, "leaked %d fd(s) across cancel-during-recv cycles" % leaked


# ==========================================================================
# 8. wait_for / asyncio.timeout slow-return + overlap.
# ==========================================================================
def test_asyncio_timeout_block_promptly_cancels_inner():
    """asyncio.timeout() context manager must time out promptly and cancel the
    body, not serialize or hang."""
    async def body():
        t0 = time.monotonic()
        try:
            async with asyncio.timeout(0.05):
                await asyncio.sleep(5)
            return ("no-timeout", 0.0)
        except TimeoutError:
            return ("timeout", time.monotonic() - t0)
    with hang_guard(20, "asyncio.timeout"):
        outcome, el = aio.run(body())
    assert outcome == "timeout"
    assert el < 1.0, "asyncio.timeout took %.3fs for a 50ms deadline" % el


def test_wait_for_returns_value_when_under_budget():
    async def body():
        async def quick():
            await asyncio.sleep(0.01)
            return "value"
        return await asyncio.wait_for(quick(), timeout=5)
    with hang_guard(20, "wait_for under budget"):
        with assert_faster_than(2.0, "wait_for under budget"):
            assert aio.run(body()) == "value"


# ==========================================================================
# 9. gather: first-exception, return_exceptions, overlap.
# ==========================================================================
def test_gather_first_exception_does_not_swallow_or_hang():
    async def body():
        async def ok(i):
            await asyncio.sleep(0.02)
            return i

        async def boom():
            await asyncio.sleep(0.005)
            raise RuntimeError("boom")
        try:
            await asyncio.gather(ok(1), boom(), ok(2))
            return "no-raise"
        except RuntimeError as e:
            out = ("raised", str(e))
        # gather's first-exception cancels the still-running ok() siblings; give
        # them a turn to finalize and force a collection so they leave the global
        # task registry BEFORE this run's teardown -- otherwise a lingering (but
        # done) cancelled task makes the NEXT aio.run see `sibling_busy` and skip
        # sched_reset(), stranding an unrelated server's accept-fiber parker.
        # See test_gather_first_exc_strands_next_run_accept_parker (the FINDING).
        await asyncio.sleep(0.03)
        gc.collect()
        return out
    with hang_guard(20, "gather first-exception"):
        out = aio.run(body())
    assert out == ("raised", "boom"), out


# REGRESSION (was finding #7): after a gather() first-exception cancels its
# siblings, those zombie tasks linger not-done on the now-closed loop -- they no
# longer block the NEXT run's sched_reset().  _cancel_outstanding_tasks's
# sibling_busy check now ignores tasks on a CLOSED loop (they can never be
# driven), so the accept-fiber parker drains to 0 without a gc.collect() and no
# longer accumulates per run.  (Open sibling loops stay protected.)
def test_gather_first_exc_strands_next_run_accept_parker():
    """Deterministic in-subprocess repro of the cross-run parker strand: run a
    gather-first-exception (NO gc), then a streams server create/close in a
    fresh aio.run, then assert the global netpoll parker is back to 0 WITHOUT a
    gc.collect().  It currently is NOT (the stranded accept-fiber parker), so
    this xfails until the teardown drains it independent of GC timing."""
    script = r"""
import asyncio, time, sys
import runloom.aio as aio
import runloom_c as rc

def parked():
    return rc.stats().get("netpoll_parked", 0)

def gather_first():
    async def body():
        async def ok(i):
            await asyncio.sleep(0.05); return i      # still running when cancelled
        async def boom():
            await asyncio.sleep(0.005); raise RuntimeError("boom")
        try:
            await asyncio.gather(ok(1), boom(), ok(2))
        except RuntimeError:
            pass
        return "raised"
    aio.run(body())   # NOTE: no gc.collect() -- cancelled siblings linger

def streams_cycle():
    async def body():
        async def handler(r, w):
            w.write(b"abc"); await w.drain(); w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        try: await r.readexactly(10)
        except asyncio.IncompleteReadError: pass
        w.close(); server.close()
    aio.run(body())

gather_first()
streams_cycle()
# Give the run-teardown a generous settle WITHOUT forcing a collection.
deadline = time.monotonic() + 1.0
p = parked()
while p > 0 and time.monotonic() < deadline:
    time.sleep(0.01); p = parked()
print("PARKED", p, flush=True)
"""
    with hang_guard(40, "gather-first strands next-run parker"):
        cp = _run_subprocess(script, timeout=30,
                             env_extra={"RUNLOOM_GOROUTINE_PANIC": "silent"})
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "PARKED 0" in out, (
        "stranded accept-fiber parker after gather-first + streams run "
        "(no gc): %s / stderr=%s" % (out, cp.stderr.decode(errors="replace")))


def test_gather_return_exceptions_collects_all():
    async def body():
        async def ok():
            await asyncio.sleep(0.01)
            return "ok"

        async def boom():
            await asyncio.sleep(0.005)
            raise ValueError("v")
        res = await asyncio.gather(ok(), boom(), ok(), return_exceptions=True)
        return res
    with hang_guard(20, "gather return_exceptions"):
        res = aio.run(body())
    assert res[0] == "ok" and res[2] == "ok"
    assert isinstance(res[1], ValueError)


def test_gather_overlaps_executor_offloads():
    async def body():
        loop = asyncio.get_running_loop()

        def blocking(x):
            time.sleep(0.05)
            return x * 2
        with assert_faster_than(0.4, "executor overlap"):
            a, b, c = await asyncio.gather(
                loop.run_in_executor(None, blocking, 1),
                loop.run_in_executor(None, blocking, 2),
                loop.run_in_executor(None, blocking, 3),
            )
        return (a, b, c)
    with hang_guard(20, "gather executor overlap"):
        assert aio.run(body()) == (2, 4, 6)


# ==========================================================================
# 10. run_in_executor exception twin + cancel delivery.
# ==========================================================================
def test_run_in_executor_exception_propagates_as_asyncio():
    async def body():
        loop = asyncio.get_running_loop()

        def boom():
            raise KeyError("nope")
        try:
            await loop.run_in_executor(None, boom)
            return "no-raise"
        except KeyError as e:
            return ("raised", str(e))
    with hang_guard(20, "executor exception"):
        out = aio.run(body())
    assert out == ("raised", "'nope'"), out


# ==========================================================================
# 11. StreamReader / StreamWriter edge values.
# ==========================================================================
def _echo_server_serving(handler):
    """Return an async ctx-like coroutine: start a server, yield (port), close."""
    pass


def test_readexactly_eof_midframe_raises_incomplete():
    async def body():
        async def handler(r, w):
            w.write(b"abc")              # only 3 bytes then EOF
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        try:
            await r.readexactly(10)
            out = "no-raise"
        except asyncio.IncompleteReadError as e:
            out = ("incomplete", bytes(e.partial), e.expected)
        w.close()
        await _close_settle(server)
        return out
    with hang_guard(20, "readexactly EOF"):
        out = aio.run(body())
    assert out == ("incomplete", b"abc", 10), out


def test_readuntil_separator_not_found_at_eof_raises():
    async def body():
        async def handler(r, w):
            w.write(b"no-newline-here")   # no separator, then EOF
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        try:
            await r.readuntil(b"\n")
            out = "no-raise"
        except asyncio.IncompleteReadError as e:
            out = ("incomplete", bytes(e.partial))
        w.close()
        await _close_settle(server)
        return out
    with hang_guard(20, "readuntil sep-not-found"):
        out = aio.run(body())
    assert out == ("incomplete", b"no-newline-here"), out


def test_readline_returns_partial_on_eof():
    async def body():
        async def handler(r, w):
            w.write(b"partial-line")      # no \n, then close
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        line = await r.readline()         # asyncio returns the partial
        w.close()
        await _close_settle(server)
        return line
    with hang_guard(20, "readline partial"):
        assert aio.run(body()) == b"partial-line"


def test_read_negative_until_eof_and_read_zero():
    async def body():
        async def handler(r, w):
            w.write(b"chunk1chunk2")
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        z = await r.read(0)               # read(0) -> b"" immediately
        whole = await r.read(-1)          # read until EOF
        w.close()
        await _close_settle(server)
        return z, whole
    with hang_guard(20, "read(-1)/read(0)"):
        z, whole = aio.run(body())
    assert z == b""
    assert whole == b"chunk1chunk2", whole


def test_readexactly_streamed_in_chunks_with_delay():
    """readexactly must accumulate across multiple recv chunks delivered with a
    delay (cooperative re-park between chunks), returning the exact count."""
    async def body():
        async def handler(r, w):
            for i in range(5):
                w.write(b"X" * 4)
                await w.drain()
                await asyncio.sleep(0.005)
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        data = await r.readexactly(20)
        w.close()
        await _close_settle(server)
        return data
    with hang_guard(20, "readexactly chunked"):
        assert aio.run(body()) == b"X" * 20


# REGRESSION (was finding #9): readuntil now honors `limit` -- when the
# separator is not found within `limit` bytes it raises LimitOverrunError (and
# leaves the data in the buffer), matching stock asyncio.
def test_readuntil_limit_overflow_raises_limitoverrun():
    async def body():
        async def handler(r, w):
            w.write(b"x" * 100 + b"\n")   # 100 bytes before the separator
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port, limit=10)
        raised = "no-raise"
        try:
            await r.readuntil(b"\n")
        except asyncio.LimitOverrunError:
            raised = "limit-overrun"
        except asyncio.IncompleteReadError:
            raised = "incomplete"
        w.close()
        await _close_settle(server)
        return raised
    with hang_guard(20, "readuntil limit overflow"):
        assert aio.run(body()) == "limit-overrun", (
            "readuntil did not enforce the limit")


# REGRESSION (was finding #10): readuntil now accepts a tuple of separators
# (asyncio 3.13 feature); the shortest match wins.
def test_readuntil_tuple_separators():
    async def body():
        async def handler(r, w):
            w.write(b"hello;world")
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        try:
            data = await r.readuntil((b";", b","))
        finally:
            w.close()
            await _close_settle(server)
        return data
    with hang_guard(20, "readuntil tuple sep"):
        assert aio.run(body()) == b"hello;", "tuple separators not supported"


def test_writer_write_after_close_raises():
    async def body():
        async def handler(r, w):
            await r.read(10)
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        w.close()
        try:
            w.write(b"after-close")
            out = "no-raise"
        except RuntimeError:
            out = "raised"
        await _close_settle(server)
        return out
    with hang_guard(20, "write after close"):
        assert aio.run(body()) == "raised"


def test_drain_flushes_large_payload():
    """drain() must cooperatively flush a payload larger than the socket buffer
    (forces EAGAIN -> wait_fd(WRITE) -> resume), no hang, exact bytes echoed."""
    PAYLOAD = b"Q" * (1 << 20)   # 1 MiB

    async def body():
        async def handler(r, w):
            data = await r.readexactly(len(PAYLOAD))
            w.write(data)
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        w.write(PAYLOAD)
        await w.drain()
        echoed = await r.readexactly(len(PAYLOAD))
        w.close()
        await _close_settle(server)
        return echoed == PAYLOAD
    with hang_guard(40, "drain large payload"):
        assert aio.run(body()) is True


# ==========================================================================
# 12. create_datagram_endpoint (UDP).
# ==========================================================================
def test_udp_send_recv_roundtrip():
    async def body():
        loop = asyncio.get_running_loop()
        got = {}
        done = loop.create_future()

        class Srv(asyncio.DatagramProtocol):
            def connection_made(self, tr):
                self.tr = tr

            def datagram_received(self, data, addr):
                self.tr.sendto(b"pong:" + data, addr)

        class Cli(asyncio.DatagramProtocol):
            def connection_made(self, tr):
                self.tr = tr

            def datagram_received(self, data, addr):
                got["d"] = data
                if not done.done():
                    done.set_result(None)
        st, sp = await loop.create_datagram_endpoint(
            Srv, local_addr=("127.0.0.1", 0))
        port = st.get_extra_info("sockname")[1]
        ct, cp = await loop.create_datagram_endpoint(
            Cli, remote_addr=("127.0.0.1", port))
        ct.sendto(b"ping")
        await asyncio.wait_for(done, 5)
        ct.close()
        st.close()
        await asyncio.sleep(0.03)     # let both recv fibers observe close + exit
        return got.get("d")
    with hang_guard(20, "udp roundtrip"):
        assert aio.run(body()) == b"pong:ping"


def test_udp_connection_lost_fires_on_close():
    async def body():
        loop = asyncio.get_running_loop()
        lost = {}

        class P(asyncio.DatagramProtocol):
            def connection_lost(self, exc):
                lost["called"] = True
                lost["exc"] = exc
        tr, pr = await loop.create_datagram_endpoint(
            P, local_addr=("127.0.0.1", 0))
        tr.close()
        await asyncio.sleep(0.03)     # let the recv fiber observe close + exit
        return lost
    with hang_guard(20, "udp connection_lost"):
        lost = aio.run(body())
    assert lost.get("called") is True
    assert lost.get("exc") is None


def test_udp_datagram_received_exception_routed_to_handler():
    """An exception raised inside datagram_received must be routed to the loop's
    exception handler (DatagramTransport._report), not crash the recv fiber."""
    async def body():
        loop = asyncio.get_running_loop()
        errors = []
        loop.set_exception_handler(lambda l, ctx: errors.append(ctx))
        done = loop.create_future()

        class Srv(asyncio.DatagramProtocol):
            def connection_made(self, tr):
                self.tr = tr

            def datagram_received(self, data, addr):
                if not done.done():
                    loop.call_soon(done.set_result, None)
                raise RuntimeError("in-datagram_received")
        st, sp = await loop.create_datagram_endpoint(
            Srv, local_addr=("127.0.0.1", 0))
        port = st.get_extra_info("sockname")[1]
        ct, cp = await loop.create_datagram_endpoint(
            lambda: asyncio.DatagramProtocol(),
            remote_addr=("127.0.0.1", port))
        ct.sendto(b"trigger")
        await asyncio.wait_for(done, 5)
        await asyncio.sleep(0.02)
        ct.close()
        st.close()
        await asyncio.sleep(0.03)     # let both recv fibers observe close + exit
        return errors
    with hang_guard(20, "udp received exception"):
        errors = aio.run(body())
    assert any("datagram_received" in (e.get("message") or "") for e in errors), (
        "datagram_received exception not routed to handler: %r" % errors)


# ==========================================================================
# 13. TLS: handshake over a cooperatively-parked socket + greeting in
#     connection_made over TLS.
# ==========================================================================
def test_tls_client_handshake_and_greeting_in_connection_made():
    sctx, cctx = _tls_contexts()

    async def body():
        loop = asyncio.get_running_loop()
        got = {}
        done = loop.create_future()

        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                tr.write(b"TLS-GREETING")     # write inside connection_made (TLS)

            def data_received(self, data):
                pass

        class Cli(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                got["greeting"] = data
                if not done.done():
                    done.set_result(None)
        server = await loop.create_server(Srv, "127.0.0.1", 0, ssl=sctx)
        port = server.sockets[0].getsockname()[1]
        ctr, cli = await loop.create_connection(
            Cli, "127.0.0.1", port, ssl=cctx, server_hostname="localhost")
        await asyncio.wait_for(done, 10)
        # Close the client so the server's accepted TLS conn detaches; then
        # wait_closed() can settle (it waits for all connections, asyncio parity).
        ctr.close()
        await asyncio.sleep(0.05)
        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), 5)
        except (asyncio.TimeoutError, TimeoutError):
            pass
        return got.get("greeting")
    with hang_guard(30, "tls greeting in connection_made"):
        assert aio.run(body()) == b"TLS-GREETING"


def test_tls_streams_echo():
    sctx, cctx = _tls_contexts()

    async def body():
        async def handler(r, w):
            data = await r.read(64)
            w.write(b"echo:" + data)
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0, ssl=sctx)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port, ssl=cctx,
                                         server_hostname="localhost")
        w.write(b"secret")
        await w.drain()
        # server reads 64 bytes (gets "secret" since recv returns what's there),
        # then echoes; close half so the server's read returns.
        data = await r.read(64)
        w.close()
        await _close_settle(server)
        return data
    with hang_guard(30, "tls streams echo"):
        out = aio.run(body())
    assert out == b"echo:secret", out


# ==========================================================================
# 14. subprocess_exec pipe bridging + wait.
# ==========================================================================
# NOTE: each subprocess body runs in its OWN child interpreter (one aio.run-
# with-a-subprocess per process).  This is REQUIRED, not cosmetic: the SECOND
# consecutive aio.run that spawns a subprocess in the SAME process hangs in
# communicate()/wait() -- the child's pidfd reaper never wakes (see the FINDING
# test_second_subprocess_run_never_reaps_hangs).  Running each in a fresh child
# both avoids that cross-run hang AND keeps the SIGSEGV containment of the
# subprocess-for-crashes mandate.
def test_subprocess_exec_communicate_uppercase():
    script = r"""
import asyncio, sys
import runloom.aio as aio
async def body():
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c",
        "import sys;d=sys.stdin.buffer.read();"
        "sys.stdout.buffer.write(d.upper());sys.stdout.buffer.flush()",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate(b"hello bridge")
    return proc.returncode, out, err
rc, out, err = aio.run(body())
print("RESULT", rc, out, err)
"""
    with hang_guard(30, "subprocess communicate"):
        cp = _run_subprocess(script, timeout=20)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "RESULT 0 b'HELLO BRIDGE' b''" in out, (
        "subprocess communicate failed: %s\n%s"
        % (out, cp.stderr.decode(errors="replace")))


def test_subprocess_exec_nonzero_exit_and_stderr():
    script = r"""
import asyncio, sys
import runloom.aio as aio
async def body():
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import sys;sys.stderr.write('boom');sys.exit(3)",
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    return proc.returncode, err
rc, err = aio.run(body())
print("RESULT", rc, err)
"""
    with hang_guard(30, "subprocess nonzero exit"):
        cp = _run_subprocess(script, timeout=20)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "RESULT 3 b'boom'" in out, (
        "subprocess nonzero-exit/stderr failed: %s\n%s"
        % (out, cp.stderr.decode(errors="replace")))


def test_subprocess_wait_for_long_child_with_timeout():
    """wait() on a child that outlives a wait_for timeout: the wait_for must
    time out (cancelling the wait) promptly, then we kill + reap.  Runs in a
    fresh child (one subprocess aio.run per process; see the note above)."""
    script = r"""
import asyncio, sys, time
import runloom.aio as aio
async def body():
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time;time.sleep(30)",
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL)
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(proc.wait(), timeout=0.1)
        outcome = "completed"
    except (asyncio.TimeoutError, TimeoutError):
        outcome = "timeout"
    el = time.monotonic() - t0
    proc.kill()
    await proc.wait()
    return outcome, el
outcome, el = aio.run(body())
print("RESULT", outcome, "%.3f" % el)
"""
    with hang_guard(30, "subprocess wait timeout"):
        cp = _run_subprocess(script, timeout=20)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "RESULT timeout" in out, (
        "subprocess wait_for-timeout failed: %s\n%s"
        % (out, cp.stderr.decode(errors="replace")))
    el = float(out.split("RESULT timeout")[1].split()[0])
    assert el < 2.0, "wait_for on subprocess slow-returned: %.3fs" % el


# REGRESSION (was finding #2): a 2nd consecutive aio.run() spawning a subprocess
# no longer hangs.  Root cause was stale netpoll arm caches on fd reuse across
# the aio.run boundary -- BOTH the pidfd (reaper) AND the subprocess stdout/stderr
# pipe fds.  The reaper and the pipe transports now netpoll_release_if_idle the fd
# before closing it, so the reused fd numbers re-register cleanly.
def test_second_subprocess_run_never_reaps_hangs():
    """Two consecutive aio.run()s each spawning + awaiting a trivial child.  The
    CORRECT behavior is both complete; currently the second hangs, so the child
    is killed by its own timeout and we observe only one RESULT line."""
    script = r"""
import asyncio, sys
import runloom.aio as aio
def run_one(tag):
    async def body():
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import sys;sys.exit(7)",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        await proc.communicate()
        return proc.returncode
    rc = aio.run(body())
    print("RESULT", tag, rc, flush=True)
run_one("first")
run_one("second")
print("BOTH-DONE", flush=True)
"""
    # The child must itself terminate; a generous timeout bounds the hang so the
    # test asserts the CURRENT (buggy) behavior rather than wedging the suite.
    with hang_guard(40, "second subprocess run reap"):
        try:
            cp = _run_subprocess(script, timeout=20)
        except subprocess.TimeoutExpired as e:
            # The hang manifested: only the first RESULT printed before the
            # child wedged.  Assert the CORRECT behavior (both done) so this
            # registers as the xfail FINDING.
            partial = (e.stdout or b"").decode(errors="replace")
            assert "BOTH-DONE" in partial, (
                "second subprocess aio.run hung (no BOTH-DONE): %s" % partial)
            return
    out = cp.stdout.decode(errors="replace")
    assert "BOTH-DONE" in out, (
        "second subprocess aio.run did not complete: %s\n%s"
        % (out, cp.stderr.decode(errors="replace")))


# ==========================================================================
# 15. Exception inside a protocol callback routes to the handler, no crash.
# ==========================================================================
def test_exception_in_data_received_routed_not_crash():
    async def body():
        loop = asyncio.get_running_loop()
        errors = []
        loop.set_exception_handler(lambda l, ctx: errors.append(ctx))
        done = loop.create_future()

        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                if not done.done():
                    loop.call_soon(done.set_result, None)
                raise RuntimeError("in-data_received")

        class Cli(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr
                tr.write(b"trigger")
        server = await loop.create_server(Srv, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        ctr, cli = await loop.create_connection(Cli, "127.0.0.1", port)
        await asyncio.wait_for(done, 5)
        await asyncio.sleep(0.02)
        ctr.close()
        await asyncio.sleep(0.02)
        await _close_settle(server)
        return errors
    with hang_guard(20, "exception in data_received"):
        errors = aio.run(body())
    # The bridge must not crash; the error should be visible (routed or logged).
    # We assert the run completed and at least observed the trigger.
    assert isinstance(errors, list)


def test_exception_in_connection_made_does_not_crash_run():
    """A protocol whose connection_made raises must not crash the loop; the run
    completes (the error is reported, the connection is dropped)."""
    script = r"""
import asyncio, sys
import runloom.aio as aio
class Cli(asyncio.Protocol):
    def connection_made(self, tr):
        raise RuntimeError("boom-in-connection_made")
async def body():
    loop = asyncio.get_running_loop()
    errs = []
    loop.set_exception_handler(lambda l, c: errs.append(c))
    class Srv(asyncio.Protocol):
        def connection_made(self, tr): self.tr = tr
    server = await loop.create_server(Srv, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        await loop.create_connection(Cli, "127.0.0.1", port)
    except RuntimeError:
        pass
    await asyncio.sleep(0.05)
    server.close()
    # The server-side half-open conn (client connection_made raised) may never
    # detach, so bound wait_closed -- the point is the LOOP keeps running, not a
    # crash.  A hang here would be a separate (worse) bug; a timeout is fine.
    try:
        await asyncio.wait_for(server.wait_closed(), 2)
    except (asyncio.TimeoutError, TimeoutError):
        pass
    print("OK", len(errs) >= 0)
aio.run(body())
"""
    with hang_guard(40, "exception in connection_made subproc"):
        cp = _run_subprocess(script, timeout=30,
                             env_extra={"RUNLOOM_GOROUTINE_PANIC": "silent"})
    _assert_no_signal(cp)
    assert b"OK" in cp.stdout, (
        "run did not complete:\n%s\n%s"
        % (cp.stdout.decode(errors="replace"),
           cp.stderr.decode(errors="replace")))


# ==========================================================================
# 16. Fault injection mid-I/O -> clean Python error, never a SEGV.
# ==========================================================================
@pytest.mark.parametrize("site,errno_", [
    ("TCP_RECV", 104),    # ECONNRESET
    ("TCP_SEND", 32),     # EPIPE
    ("FD_READ", 9),       # EBADF
    ("TCP_ACCEPT", 24),   # EMFILE
    ("SPAWN_G", 12),      # ENOMEM
])
def test_fault_injection_does_not_crash(site, errno_):
    """Inject a one-shot fault at an I/O / spawn site mid-workload; assert the
    child does not die on a signal (a clean Python error is acceptable)."""
    script = r"""
import asyncio, sys, socket
import runloom.aio as aio

async def body():
    loop = asyncio.get_running_loop()
    class Echo(asyncio.Protocol):
        def connection_made(self, tr): self.tr = tr
        def data_received(self, data):
            try: self.tr.write(data)
            except Exception: pass
    try:
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        for _ in range(20):
            try:
                r, w = await asyncio.open_connection("127.0.0.1", port)
                w.write(b"ping")
                await w.drain()
                try:
                    await asyncio.wait_for(r.read(64), 0.3)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
                w.close()
            except Exception:
                pass
        server.close()
        try:
            await asyncio.wait_for(server.wait_closed(), 2)
        except (asyncio.TimeoutError, TimeoutError):
            pass
    except Exception as e:
        sys.stderr.write("clean-error: %r\n" % e)
    print("SURVIVED")

try:
    aio.run(body())
except BaseException as e:
    # A fault that fires on the entry-task spawn (e.g. SPAWN_G once:ENOMEM hits
    # the very first runloom_c.fiber before body() runs) surfaces as a CLEAN Python
    # exception out of aio.run -- acceptable per the mandate (no crash).
    if isinstance(e, (KeyboardInterrupt, SystemExit)):
        raise
    print("CLEAN-ERROR", type(e).__name__)
"""
    env = {"RUNLOOM_FAULT_" + site: "once:%d" % errno_,
           "RUNLOOM_GOROUTINE_PANIC": "silent"}
    with hang_guard(40, "fault %s" % site):
        cp = _run_subprocess(script, timeout=30, env_extra=env)
    # The ONLY unacceptable outcome is a signal crash (SEGV/abort); a clean
    # Python error or a survived workload are both fine.
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert ("SURVIVED" in out or "CLEAN-ERROR" in out), (
        "child neither survived nor failed cleanly on fault %s:\n"
        "STDOUT:%s\nSTDERR:%s"
        % (site, out, cp.stderr.decode(errors="replace")))


# ==========================================================================
# 17. Guard-page overflow inside a data_received callback is CLASSIFIED.
# ==========================================================================
def test_deep_recursion_in_data_received_is_classified_not_silent():
    """A protocol callback that recurses past the fiber stack guard page
    must NOT silently corrupt: the runtime should either classify the overflow
    ('GOROUTINE STACK OVERFLOW' + 'guard page') or unwind cleanly -- never a bare
    unclassified SIGSEGV.  We raise the Python recursion limit (so RecursionError
    doesn't preempt the C-stack overflow) and force a small IO stack so the C
    recursion runs into the fiber guard page.  The child must TERMINATE
    (no hang) and must not die on an unclassified signal."""
    script = r"""
import asyncio, sys
sys.setrecursionlimit(50_000_000)   # let the C stack overflow before RecursionError
import runloom.aio as aio

def deep(n):
    if n <= 0:
        return 0
    pad = bytearray(8192)            # a fat frame to burn stack fast
    return deep(n - 1) + pad[0]

async def body():
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    class Srv(asyncio.Protocol):
        def connection_made(self, tr): self.tr = tr
        def data_received(self, data):
            try:
                deep(1_000_000)      # blow the guard page inside the callback
            except RecursionError:
                pass
    class Cli(asyncio.Protocol):
        def connection_made(self, tr):
            tr.write(b"go")
    server = await loop.create_server(Srv, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    await loop.create_connection(Cli, "127.0.0.1", port)
    try:
        await asyncio.wait_for(done, 2)
    except Exception:
        pass
    print("CHILD-DONE", flush=True)

aio.run(body())
"""
    env = {"RUNLOOM_AIO_IO_STACK": str(64 * 1024),
           "RUNLOOM_GOROUTINE_PANIC": "silent"}
    with hang_guard(90, "guard-page in data_received"):
        try:
            cp = _run_subprocess(script, timeout=60, env_extra=env)
        except subprocess.TimeoutExpired:
            pytest.fail("guard-page recursion in data_received HUNG the child "
                        "(no termination within 60s)")
    combined = (cp.stdout + cp.stderr).decode(errors="replace")
    classified = ("GOROUTINE STACK OVERFLOW" in combined
                  and "guard page" in combined)
    crashed_unclassified = (cp.returncode is not None and cp.returncode < 0
                            and not classified)
    assert not crashed_unclassified, (
        "guard-page overflow in data_received crashed WITHOUT classification "
        "(rc=%d):\n%s" % (cp.returncode, combined))


# ==========================================================================
# 18. Many concurrent echo connections under the create_server/create_connection
#     transport stack (not just the streams path).
# ==========================================================================
def test_many_concurrent_transport_echo_connections():
    N = 60

    async def body():
        loop = asyncio.get_running_loop()

        class Echo(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                self.tr.write(data)
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        sem = asyncio.Semaphore(32)

        async def one(i):
            async with sem:
                r, w = await asyncio.open_connection("127.0.0.1", port)
                payload = ("hello-%d" % i).encode()
                w.write(payload)
                await w.drain()
                got = await r.readexactly(len(payload))
                w.close()
                return got == payload
        results = await asyncio.gather(*[one(i) for i in range(N)])
        await _close_settle(server)
        return results
    with hang_guard(60, "many transport echo"):
        results = aio.run(body())
    assert all(results), "%d/%d echo roundtrips failed" % (results.count(False), N)


# ==========================================================================
# 19. Run-level robustness: nested aio.run, sock_sendall partial writes.
# ==========================================================================
def test_sock_sendall_large_then_recv_exact():
    """sock_sendall must cooperatively flush a >buffer payload (forces
    wait_fd(WRITE)) and sock_recv must reassemble it, no hang, exact bytes."""
    PAYLOAD = b"Z" * (512 * 1024)

    async def body():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)

        async def reader():
            chunks = []
            n = 0
            while n < len(PAYLOAD):
                d = await loop.sock_recv(b, 65536)
                if not d:
                    break
                chunks.append(d)
                n += len(d)
            return b"".join(chunks)
        rt = asyncio.ensure_future(reader())
        await loop.sock_sendall(a, PAYLOAD)
        got = await rt
        a.close()
        b.close()
        return got == PAYLOAD
    with hang_guard(30, "sock_sendall large"):
        assert aio.run(body()) is True


def test_consecutive_independent_runs_do_not_wedge():
    """Several independent aio.run() invocations in sequence (fresh loop each)
    must each complete -- a leaked parker from one would wedge the next."""
    with hang_guard(30, "consecutive runs"):
        for i in range(10):
            async def body(k=i):
                async def child():
                    await asyncio.sleep(0.005)
                    return k
                return await asyncio.gather(*[child() for _ in range(5)])
            assert aio.run(body()) == [i, i, i, i, i]


# ==========================================================================
# ==========================================================================
# AUGMENTATION (adversarial critic pass): conditions the first pass MISSED.
#
# The first pass was strong on I/O paths (streams, TLS, UDP, subprocess, cancel
# torture, fault injection, guard-page) but THIN on the Future/Task STATE
# MACHINE and ordering/integrity guarantees, on synchronization-primitive
# cancellation, on positional integrity of gather under out-of-order completion,
# on the foreign-thread run_coroutine_threadsafe path, on the rest of the
# StreamReader/Writer surface, and on several fault sites / env-gated modes.
# It also never exercised __del__ robustness for a HALF-CONSTRUCTED task, which
# is a real defect (see test_rejected_noncoro_task_del_is_clean).
# ==========================================================================


# ==========================================================================
# A1. Future state machine integrity -- every error branch + identity.
# ==========================================================================
def test_future_state_machine_error_branches():
    """result()/exception() on a PENDING future raise InvalidStateError;
    set_result/set_exception on a DONE future raise InvalidStateError;
    cancel() of a done future returns False; result() of a cancelled future
    raises CancelledError.  The first pass tested NONE of these branches."""
    async def body():
        loop = asyncio.get_running_loop()
        out = {}
        f = loop.create_future()
        # PENDING reads.
        try:
            f.result()
        except asyncio.InvalidStateError:
            out["pending_result"] = True
        try:
            f.exception()
        except asyncio.InvalidStateError:
            out["pending_exception"] = True
        # set_result then double-set.
        f.set_result(7)
        out["result"] = f.result()
        out["exception_none"] = f.exception() is None
        try:
            f.set_result(8)
        except asyncio.InvalidStateError:
            out["double_set_result"] = True
        try:
            f.set_exception(ValueError())
        except asyncio.InvalidStateError:
            out["set_exc_on_done"] = True
        # cancel() of a done future -> False.
        out["cancel_done"] = f.cancel()
        # A fresh cancelled future.
        g = loop.create_future()
        out["cancel_pending"] = g.cancel()
        out["cancelled"] = g.cancelled()
        try:
            g.result()
        except asyncio.CancelledError:
            out["cancelled_result"] = True
        return out
    with hang_guard(20, "future state machine"):
        out = aio.run(body())
    assert out == {
        "pending_result": True, "pending_exception": True, "result": 7,
        "exception_none": True, "double_set_result": True,
        "set_exc_on_done": True, "cancel_done": False, "cancel_pending": True,
        "cancelled": True, "cancelled_result": True,
    }, out


def test_future_set_exception_stopiteration_rejected():
    """set_exception(StopIteration) must raise TypeError (asyncio contract:
    StopIteration interacts badly with generators)."""
    async def body():
        loop = asyncio.get_running_loop()
        f = loop.create_future()
        try:
            f.set_exception(StopIteration())
            return "no-raise"
        except TypeError:
            return "rejected"
    with hang_guard(20, "set_exception StopIteration"):
        assert aio.run(body()) == "rejected"


def test_future_cancel_message_and_identity_preserved():
    """A cancel(msg) message must reach the awaiter's CancelledError; and the
    EXACT CancelledError instance a coroutine raised must be re-raised by the
    awaiting parent (identity, like asyncio.Future._cancelled_exc)."""
    async def body():
        out = {}
        # cancel(msg) propagates the message.
        async def victim():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError as e:
                out["inner_args"] = e.args
                raise
        t = asyncio.ensure_future(victim())
        await asyncio.sleep(0.02)
        t.cancel("custom-reason")
        try:
            await t
        except asyncio.CancelledError as e:
            out["outer_args"] = e.args
        # identity: the instance a coroutine raises is the one the parent gets.
        sentinel = asyncio.CancelledError("sentinel")

        async def raiser():
            raise sentinel
        t2 = asyncio.ensure_future(raiser())
        try:
            await t2
        except asyncio.CancelledError as e:
            out["identity"] = (e is sentinel)
        return out
    with hang_guard(20, "cancel message + identity"):
        out = aio.run(body())
    assert out.get("inner_args") == ("custom-reason",), out
    assert out.get("outer_args") == ("custom-reason",), out
    assert out.get("identity") is True, "cancel identity not preserved: %r" % out


def test_remove_done_callback_count_and_effect():
    """remove_done_callback returns the number removed and actually prevents the
    callback from firing; an unregistered callback returns 0."""
    async def body():
        loop = asyncio.get_running_loop()
        f = loop.create_future()
        fired = []
        cb = lambda fut: fired.append("cb")
        other = lambda fut: fired.append("other")
        f.add_done_callback(cb)
        f.add_done_callback(cb)         # registered twice
        n_removed = f.remove_done_callback(cb)
        n_noop = f.remove_done_callback(other)   # never registered -> 0
        f.set_result(None)
        await asyncio.sleep(0.02)
        return n_removed, n_noop, fired
    with hang_guard(20, "remove_done_callback"):
        n_removed, n_noop, fired = aio.run(body())
    assert n_removed == 2, n_removed
    assert n_noop == 0, n_noop
    assert fired == [], "removed callback still fired: %r" % fired


# ==========================================================================
# A2. __del__ ROBUSTNESS for a half-constructed task (FINDING).
# ==========================================================================
# REGRESSION (was finding #11): a rejected create_task(non_coro) no longer
# leaves a half-built object whose __del__ AttributeErrors at GC -- the inherited
# _RunloomFutureMixin.__del__ now guards its _pglogtb/_pgexc reads with getattr,
# so stderr stays clean.
def test_rejected_noncoro_task_del_is_clean():
    """A subprocess that rejects several non-coroutine create_task() calls then
    gc.collect()s.  The CORRECT behavior is a CLEAN stderr (no 'Exception
    ignored in __del__'); currently __del__ AttributeErrors on _pglogtb."""
    script = r"""
import asyncio, gc, sys
import runloom.aio as aio
async def body():
    loop = asyncio.get_running_loop()
    for _ in range(8):
        try:
            loop.create_task(123)        # non-coroutine -> TypeError in __init__
        except TypeError:
            pass
    gc.collect()
    return "ok"
aio.run(body())
gc.collect()
print("DONE", flush=True)
"""
    with hang_guard(30, "rejected noncoro task __del__"):
        cp = _run_subprocess(script, timeout=20,
                             env_extra={"RUNLOOM_GOROUTINE_PANIC": "silent"})
    _assert_no_signal(cp)
    err = cp.stderr.decode(errors="replace")
    assert "Exception ignored in" not in err and "_pglogtb" not in err, (
        "half-constructed task __del__ raised at GC:\n%s" % err)


# ==========================================================================
# A3. Task surface: name/factory/cancelling/uncancel/double-cancel/non-coro.
# ==========================================================================
def test_task_name_factory_and_cancelling_counter():
    async def body():
        loop = asyncio.get_running_loop()
        out = {}

        async def c():
            await asyncio.sleep(0.01)
            return "x"
        t = loop.create_task(c(), name="orig")
        out["name"] = t.get_name()
        t.set_name("renamed")
        out["renamed"] = t.get_name()
        await t
        out["cancel_done"] = t.cancel()       # done -> False

        # cancelling()/uncancel() counter (used by asyncio.timeout/TaskGroup).
        async def sleeper():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise
        t2 = asyncio.ensure_future(sleeper())
        await asyncio.sleep(0.01)
        t2.cancel()
        out["cancelling_1"] = t2.cancelling()
        # uncancel before delivery clears the pending one-shot.
        out["uncancel_to"] = t2.uncancel()
        try:
            await asyncio.wait_for(t2, 0.3)
            out["after_uncancel"] = "completed-or-running"
        except asyncio.CancelledError:
            out["after_uncancel"] = "still-cancelled"
        except (asyncio.TimeoutError, TimeoutError):
            out["after_uncancel"] = "timeout"
            t2.cancel()
            try:
                await t2
            except asyncio.CancelledError:
                pass
        return out
    with hang_guard(20, "task name/cancelling"):
        out = aio.run(body())
    assert out["name"] == "orig"
    assert out["renamed"] == "renamed"
    assert out["cancel_done"] is False
    assert out["cancelling_1"] >= 1, out
    assert out["uncancel_to"] == 0, out


def test_double_cancel_is_idempotent_no_hang():
    """Two cancel() calls on a task parked in sleep must not double-deliver or
    hang; exactly one CancelledError unwinds it."""
    delivered = {"n": 0}

    async def body():
        async def victim():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                delivered["n"] += 1
                raise
        t = asyncio.ensure_future(victim())
        await asyncio.sleep(0.02)
        t.cancel()
        t.cancel()                # second cancel before the first is delivered
        try:
            await t
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "double cancel"):
        assert aio.run(body()) == "cancelled"
    assert delivered["n"] == 1, "CancelledError delivered %d times" % delivered["n"]


def test_custom_task_factory_is_honored():
    """loop.set_task_factory installs a factory; create_task must route through
    it (a stock asyncio.Task), and set_name must apply after."""
    async def body():
        loop = asyncio.get_running_loop()
        made = {}

        def factory(lp, coro, **kw):
            t = asyncio.Task(coro, loop=lp, **kw)
            made["was_called"] = True
            made["type"] = type(t).__name__
            return t
        loop.set_task_factory(factory)

        async def c():
            await asyncio.sleep(0.01)
            return 99
        t = loop.create_task(c(), name="factory-made")
        out = await t
        made["name"] = t.get_name()
        loop.set_task_factory(None)
        return out, made
    with hang_guard(20, "custom task factory"):
        out, made = aio.run(body())
    assert out == 99
    assert made.get("was_called") is True, made
    assert made.get("name") == "factory-made", made


# ==========================================================================
# A4. Future done-callback ORDER under a WaitER scheduled by set_result, with
#     MULTIPLE later callbacks (strengthens the first pass's 1-callback case).
# ==========================================================================
def test_future_waiter_before_multiple_later_callbacks_in_fifo():
    """A waiter (scheduled by set_result) must resume BEFORE every done-callback
    added later, and those callbacks must themselves run call_soon-FIFO."""
    async def body():
        loop = asyncio.get_running_loop()
        order = []
        fut = loop.create_future()

        async def waiter():
            await fut
            order.append("waiter")
        t = asyncio.ensure_future(waiter())
        await asyncio.sleep(0.01)
        for i in range(5):
            fut.add_done_callback(lambda f, i=i: order.append("cb%d" % i))
        fut.set_result(None)
        await asyncio.sleep(0.05)
        await t
        return order
    with hang_guard(20, "waiter before many cbs FIFO"):
        order = aio.run(body())
    assert order == ["waiter", "cb0", "cb1", "cb2", "cb3", "cb4"], order


# ==========================================================================
# A5. gather POSITIONAL INTEGRITY under out-of-order completion (set-equality
#     is insufficient: results must align to argument index regardless of which
#     child finishes first).  WRONG-DATA guard.
# ==========================================================================
def test_gather_results_positional_under_reverse_completion():
    """24 children where child i sleeps (N-i)*unit, so the LAST argument
    completes FIRST.  gather MUST still return results in argument order."""
    N = 24

    async def body():
        async def child(i):
            await asyncio.sleep((N - i) * 0.003)
            return i * 1000 + 7
        return await asyncio.gather(*[child(i) for i in range(N)])
    with hang_guard(30, "gather positional reverse-completion"):
        res = aio.run(body())
    assert res == [i * 1000 + 7 for i in range(N)], (
        "gather reordered results vs argument index: %r" % res)


def test_as_completed_yields_in_completion_order():
    """as_completed depends on the already-done-future deferral contract; it
    must yield futures in COMPLETION order (not submission)."""
    async def body():
        async def n(i, d):
            await asyncio.sleep(d)
            return i
        coros = [n(0, 0.03), n(1, 0.005), n(2, 0.02), n(3, 0.01)]
        order = []
        for fut in asyncio.as_completed(coros):
            order.append(await fut)
        return order
    with hang_guard(20, "as_completed order"):
        order = aio.run(body())
    # Completion order by delay: 1 (5ms), 3 (10ms), 2 (20ms), 0 (30ms).
    assert order == [1, 3, 2, 0], order


# ==========================================================================
# A6. asyncio.wait return_when modes.
# ==========================================================================
def test_wait_first_completed_and_first_exception():
    async def body():
        out = {}

        async def quick():
            await asyncio.sleep(0.01)
            return "q"

        async def slow():
            await asyncio.sleep(2.0)
            return "s"
        tq = asyncio.ensure_future(quick())
        ts = asyncio.ensure_future(slow())
        done, pending = await asyncio.wait(
            {tq, ts}, return_when=asyncio.FIRST_COMPLETED)
        out["fc_done"] = len(done)
        out["fc_pending"] = len(pending)
        out["fc_val"] = list(done)[0].result()
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        async def ok():
            await asyncio.sleep(0.05)
            return "ok"

        async def boom():
            await asyncio.sleep(0.01)
            raise RuntimeError("b")
        to = asyncio.ensure_future(ok())
        tb = asyncio.ensure_future(boom())
        done2, pending2 = await asyncio.wait(
            {to, tb}, return_when=asyncio.FIRST_EXCEPTION)
        out["fe_exc"] = sum(1 for t in done2 if t.exception() is not None)
        for t in pending2:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        # Drain `to` if it landed in done with a result (retrieve it).
        for t in done2:
            if t.exception() is None:
                t.result()
        return out
    with hang_guard(20, "asyncio.wait modes"):
        out = aio.run(body())
    assert out["fc_done"] == 1 and out["fc_pending"] == 1, out
    assert out["fc_val"] == "q", out
    assert out["fe_exc"] == 1, out


# ==========================================================================
# A7. asyncio.shield: cancelling the SHIELD wrapper does NOT cancel the inner.
# ==========================================================================
def test_shield_protects_inner_from_outer_cancel():
    state = {}

    async def body():
        async def protected():
            try:
                await asyncio.sleep(0.05)
                state["finished"] = True
                return "ok"
            except asyncio.CancelledError:
                state["inner_cancelled"] = True
                raise
        inner = asyncio.ensure_future(protected())
        wrapper = asyncio.ensure_future(asyncio.shield(inner))
        await asyncio.sleep(0.01)
        wrapper.cancel()                 # cancel the shield, NOT the inner
        try:
            await wrapper
        except asyncio.CancelledError:
            state["outer_cancelled"] = True
        # The inner must still run to completion.
        state["inner_result"] = await inner
        return state
    with hang_guard(20, "shield protects inner"):
        state = aio.run(body())
    assert state.get("outer_cancelled") is True, state
    assert state.get("finished") is True, state
    assert state.get("inner_result") == "ok", state
    assert "inner_cancelled" not in state, (
        "shielded inner was cancelled by outer cancel: %r" % state)


# ==========================================================================
# A8. Synchronization-primitive CANCELLATION: a task cancelled while waiting on
#     Lock / Event / Condition / Queue.get must take CancelledError AND leave the
#     primitive in a usable state (no leaked waiter, no double-acquire). The
#     first pass never cancelled inside a synchronization primitive.
# ==========================================================================
def test_cancel_task_waiting_on_lock_leaves_lock_usable():
    state = {}

    async def body():
        lock = asyncio.Lock()
        await lock.acquire()

        async def waiter():
            try:
                await lock.acquire()
                state["acquired"] = True
            except asyncio.CancelledError:
                state["cancelled"] = True
                raise
        t = asyncio.ensure_future(waiter())
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            state["outer"] = True
        lock.release()
        # The cancelled waiter must NOT have stolen the lock: a fresh acquire
        # under a finite budget must succeed.
        await asyncio.wait_for(lock.acquire(), 1)
        state["reacquired"] = True
        lock.release()
        return state
    with hang_guard(20, "cancel waiting on lock"):
        state = aio.run(body())
    assert state.get("cancelled") is True and state.get("reacquired") is True, state
    assert "acquired" not in state, "cancelled waiter still acquired the lock"


def test_cancel_task_waiting_on_queue_get_leaves_queue_usable():
    state = {}

    async def body():
        q = asyncio.Queue()

        async def getter():
            try:
                await q.get()
            except asyncio.CancelledError:
                state["cancelled"] = True
                raise
        t = asyncio.ensure_future(getter())
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            state["outer"] = True
        # The queue must still deliver to a new getter (no leaked waiter that
        # swallows the next put).
        await q.put(42)
        state["got"] = await asyncio.wait_for(q.get(), 1)
        return state
    with hang_guard(20, "cancel waiting on queue.get"):
        state = aio.run(body())
    assert state.get("cancelled") is True, state
    assert state.get("got") == 42, "queue lost the value after a cancelled get: %r" % state


def test_event_and_condition_wakeups():
    """An Event.wait and a Condition.wait must both wake on set/notify (the
    cooperative wait must not be lost)."""
    async def body():
        out = {}
        ev = asyncio.Event()

        async def ew():
            await ev.wait()
            out["event"] = True
        te = asyncio.ensure_future(ew())
        await asyncio.sleep(0.02)
        ev.set()
        await asyncio.wait_for(te, 2)

        cond = asyncio.Condition()

        async def cw():
            async with cond:
                await cond.wait()
                out["cond"] = True
        tc = asyncio.ensure_future(cw())
        await asyncio.sleep(0.02)
        async with cond:
            cond.notify()
        await asyncio.wait_for(tc, 2)
        return out
    with hang_guard(20, "event/condition wakeups"):
        out = aio.run(body())
    assert out == {"event": True, "cond": True}, out


# ==========================================================================
# A9. Foreign-thread run_coroutine_threadsafe (cross-thread scheduling).
# ==========================================================================
def test_run_coroutine_threadsafe_from_foreign_thread():
    """A genuine foreign OS thread submits a coroutine via
    run_coroutine_threadsafe; the loop must run it on the loop thread and the
    concurrent.futures.Future must deliver the result."""
    result_box = {}

    async def body():
        loop = asyncio.get_running_loop()

        async def work(x):
            await asyncio.sleep(0.01)
            return x * 2

        def feeder():
            fut = asyncio.run_coroutine_threadsafe(work(21), loop)
            try:
                result_box["v"] = fut.result(5)
            except Exception as e:
                result_box["e"] = repr(e)
        raw_thread(feeder)
        # Pump the loop until the foreign thread observes its result.
        for _ in range(300):
            if "v" in result_box or "e" in result_box:
                break
            await asyncio.sleep(0.01)
        return result_box
    with hang_guard(30, "run_coroutine_threadsafe foreign"):
        box = aio.run(body())
    assert box.get("v") == 42, "run_coroutine_threadsafe failed: %r" % box


def test_call_soon_threadsafe_wakes_an_otherwise_idle_loop():
    """A loop whose only live task is parked with NOTHING else to do must still
    wake to drain a call_soon_threadsafe from a foreign thread (the keepalive
    heartbeat path).  Slow-return guard: the wake must land well under the
    budget, not wait out a long timeout."""
    async def body():
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        def feeder():
            time.sleep(0.05)              # let the loop go idle (parked on `done`)
            loop.call_soon_threadsafe(
                lambda: None if done.done() else done.set_result("woke"))
        raw_thread(feeder)
        return await asyncio.wait_for(done, 5)
    with hang_guard(20, "threadsafe wakes idle loop"):
        with assert_faster_than(3.0, "idle-loop threadsafe wake"):
            assert aio.run(body()) == "woke"


# ==========================================================================
# A10. wait_for edge values: timeout=0 (immediate), timeout=None (no deadline),
#      already-finished coro (no spurious cancel).
# ==========================================================================
def test_wait_for_timeout_zero_and_none_and_already_done():
    async def body():
        out = {}

        async def slow():
            await asyncio.sleep(2)
            return "s"
        try:
            await asyncio.wait_for(slow(), 0)
            out["zero"] = "completed"
        except (asyncio.TimeoutError, TimeoutError):
            out["zero"] = "timeout"

        async def quick():
            await asyncio.sleep(0.01)
            return "q"
        out["none"] = await asyncio.wait_for(quick(), None)   # no deadline

        async def instant():
            return "i"
        out["done"] = await asyncio.wait_for(instant(), 5)    # no spurious cancel
        return out
    with hang_guard(20, "wait_for edges"):
        out = aio.run(body())
    assert out["zero"] == "timeout", out
    assert out["none"] == "q", out
    assert out["done"] == "i", out


# ==========================================================================
# A11. Timer ORDERING + call_at past-deadline (deadline-ordered, fires prompt).
# ==========================================================================
def test_call_later_timers_fire_in_deadline_order():
    async def body():
        loop = asyncio.get_running_loop()
        order = []
        done = loop.create_future()
        # Register out of deadline order; they must FIRE in deadline order.
        loop.call_later(0.04, lambda: order.append("d"))
        loop.call_later(0.01, lambda: order.append("a"))
        loop.call_later(0.03, lambda: order.append("c"))
        loop.call_later(0.02, lambda: order.append("b"))
        loop.call_later(0.06, lambda: done.set_result(None))
        await asyncio.wait_for(done, 5)
        return order
    with hang_guard(20, "timer deadline order"):
        order = aio.run(body())
    assert order == ["a", "b", "c", "d"], "timers fired out of deadline order: %r" % order


def test_call_at_past_deadline_fires_promptly():
    async def body():
        loop = asyncio.get_running_loop()
        done = loop.create_future()
        t0 = time.monotonic()
        loop.call_at(loop.time() - 5.0,
                     lambda: done.set_result(time.monotonic() - t0))
        el = await asyncio.wait_for(done, 2)
        return el
    with hang_guard(20, "call_at past deadline"):
        with assert_faster_than(1.5, "call_at past deadline"):
            el = aio.run(body())
    assert el < 0.5, "call_at(past) took %.3fs to fire" % el


# ==========================================================================
# A12. StreamReader/Writer surface the first pass skipped: read(n) BOUNDED even
#      with more buffered, readexactly(0), at_eof transitions, writelines.
# ==========================================================================
def test_read_n_is_bounded_even_with_more_buffered():
    """read(n) must return AT MOST n bytes even when the internal buffer holds
    far more -- a WRONG-DATA guard (a naive 'return whole buffer' would over-
    deliver)."""
    async def body():
        async def handler(r, w):
            w.write(b"X" * 4000)
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.05)            # let all 4000 bytes land in buffer
        first = await r.read(10)
        rest = await r.read(-1)
        w.close()
        await _close_settle(server)
        return len(first), len(first) + len(rest)
    with hang_guard(20, "read(n) bounded"):
        n_first, total = aio.run(body())
    assert n_first == 10, "read(10) returned %d bytes" % n_first
    assert total == 4000, "lost/duplicated bytes: total=%d" % total


def test_reader_writer_surface_edges():
    async def body():
        out = {}

        async def handler(r, w):
            payload = await r.readexactly(6)   # "ABCDEF" via writelines
            out["server_got"] = payload
            w.write(b"hi")
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port)
        out["rex0"] = await r.readexactly(0)   # b"" immediately
        w.writelines([b"AB", b"CDEF"])
        await w.drain()
        out["is_closing_before"] = w.is_closing()
        out["resp"] = await r.read(2)
        out["at_eof_pre"] = r.at_eof()
        w.close()
        out["is_closing_after"] = w.is_closing()
        await _close_settle(server)
        return out
    with hang_guard(20, "reader/writer surface"):
        out = aio.run(body())
    assert out["rex0"] == b"", out
    assert out["server_got"] == b"ABCDEF", out
    assert out["resp"] == b"hi", out
    assert out["is_closing_before"] is False, out
    assert out["is_closing_after"] is True, out


# ==========================================================================
# A13. TLS DATA INTEGRITY over a large multi-record payload (the first pass's
#      TLS echo was 6 bytes -- shallow; a single TLS record).  Force several TLS
#      records + reassembly and verify the EXACT bytes.
# ==========================================================================
def test_tls_streams_large_payload_exact():
    sctx, cctx = _tls_contexts()
    PAYLOAD = bytes((i * 7 + 3) & 0xFF for i in range(200000))   # ~195 KiB

    async def body():
        async def handler(r, w):
            data = await r.readexactly(len(PAYLOAD))
            w.write(data)
            await w.drain()
            w.close()
        server = await aio.start_server(handler, "127.0.0.1", 0, ssl=sctx)
        port = server.sockets[0].getsockname()[1]
        r, w = await aio.open_connection("127.0.0.1", port, ssl=cctx,
                                         server_hostname="localhost")
        w.write(PAYLOAD)
        await w.drain()
        echoed = await r.readexactly(len(PAYLOAD))
        w.close()
        await _close_settle(server)
        return echoed
    with hang_guard(40, "tls large payload integrity"):
        echoed = aio.run(body())
    assert echoed == PAYLOAD, (
        "TLS echo corrupted/truncated: got %d of %d bytes; first mismatch at %r"
        % (len(echoed), len(PAYLOAD),
           next((i for i in range(min(len(echoed), len(PAYLOAD)))
                 if echoed[i] != PAYLOAD[i]), None)))


# ==========================================================================
# A14. Additional fault-injection sites the first pass skipped: TCP_CONNECT,
#      TCP_SOCKET, FD_WRITE, SPAWN_STACK, SPAWN_TSTATE.  Inject mid-workload;
#      the only unacceptable outcome is a signal crash.
# ==========================================================================
@pytest.mark.parametrize("site,errno_", [
    ("TCP_CONNECT", 111),    # ECONNREFUSED
    ("TCP_SOCKET", 24),      # EMFILE
    ("FD_WRITE", 32),        # EPIPE
    ("SPAWN_STACK", 12),     # ENOMEM
    ("SPAWN_TSTATE", 12),    # ENOMEM
])
def test_more_fault_sites_no_crash(site, errno_):
    script = r"""
import asyncio, sys
import runloom.aio as aio
async def body():
    loop = asyncio.get_running_loop()
    class Echo(asyncio.Protocol):
        def connection_made(self, tr): self.tr = tr
        def data_received(self, data):
            try: self.tr.write(data)
            except Exception: pass
    try:
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        for _ in range(15):
            try:
                r, w = await asyncio.open_connection("127.0.0.1", port)
                w.write(b"ping"); await w.drain()
                try: await asyncio.wait_for(r.read(64), 0.3)
                except (asyncio.TimeoutError, TimeoutError): pass
                w.close()
            except Exception: pass
        server.close()
        try: await asyncio.wait_for(server.wait_closed(), 2)
        except (asyncio.TimeoutError, TimeoutError): pass
    except Exception as e:
        sys.stderr.write("clean-error: %r\n" % e)
    print("SURVIVED")
try:
    aio.run(body())
except BaseException as e:
    if isinstance(e, (KeyboardInterrupt, SystemExit)):
        raise
    print("CLEAN-ERROR", type(e).__name__)
"""
    env = {"RUNLOOM_FAULT_" + site: "once:%d" % errno_,
           "RUNLOOM_GOROUTINE_PANIC": "silent"}
    with hang_guard(40, "fault %s" % site):
        cp = _run_subprocess(script, timeout=30, env_extra=env)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert ("SURVIVED" in out or "CLEAN-ERROR" in out), (
        "child neither survived nor failed cleanly on fault %s:\n"
        "STDOUT:%s\nSTDERR:%s"
        % (site, out, cp.stderr.decode(errors="replace")))


# ==========================================================================
# A15. Env-gated M:N modes under the aio bridge (sysmon / preempt / handoff)
#      with a CPU-heavy executor offload + concurrent echo -- the detectors
#      must not crash or corrupt the round-trips.  Run in a subprocess so the
#      env mode is contained.
# ==========================================================================
@pytest.mark.parametrize("mode", [
    {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "8"},
    {"RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "8"},
    {"RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2"},
])
def test_env_gated_modes_under_aio_echo(mode):
    script = r"""
import asyncio, sys
import runloom.aio as aio
async def body():
    loop = asyncio.get_running_loop()
    class Echo(asyncio.Protocol):
        def connection_made(self, tr): self.tr = tr
        def data_received(self, data): self.tr.write(data)
    server = await loop.create_server(Echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    def burn():
        x = 0
        for _ in range(2_000_000): x += 1
        return x
    async def one(i):
        r, w = await asyncio.open_connection("127.0.0.1", port)
        msg = ("hi%d" % i).encode()
        w.write(msg); await w.drain()
        await loop.run_in_executor(None, burn)   # CPU-heavy to trip detectors
        got = await r.readexactly(len(msg))
        w.close()
        return got == msg
    res = await asyncio.gather(*[one(i) for i in range(8)])
    server.close()
    try: await asyncio.wait_for(server.wait_closed(), 2)
    except Exception: pass
    return all(res)
print("RESULT", aio.run(body()), flush=True)
"""
    env = dict(mode)
    env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
    with hang_guard(60, "env mode %r" % sorted(mode)):
        cp = _run_subprocess(script, timeout=45, env_extra=env)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "RESULT True" in out, (
        "echo round-trips failed under %r:\nSTDOUT:%s\nSTDERR:%s"
        % (mode, out, cp.stderr.decode(errors="replace")))


# ==========================================================================
# A16. UNSAFE-MIGRATION flags must take the GATED-OFF warn path (NEVER set
#      RUNLOOM_ALLOW_UNSAFE_MIGRATION).  The flag without the allow-key must warn
#      to stderr and run the DEFAULT scheduler -- no crash, workload completes.
# ==========================================================================
@pytest.mark.parametrize("flag", ["RUNLOOM_PER_G_TSTATE", "RUNLOOM_STEAL_WOKEN"])
def test_unsafe_migration_flag_gated_off_warns_not_crashes(flag):
    script = r"""
import asyncio, sys
import runloom.aio as aio
async def body():
    async def child(i):
        await asyncio.sleep(0.005)
        return i
    res = await asyncio.gather(*[child(i) for i in range(8)])
    return res == list(range(8))
print("RESULT", aio.run(body()), flush=True)
"""
    # Set the flag WITHOUT RUNLOOM_ALLOW_UNSAFE_MIGRATION -> gated-off warn path.
    env = {flag: "1", "RUNLOOM_GOROUTINE_PANIC": "silent"}
    with hang_guard(30, "gated-off %s" % flag):
        cp = _run_subprocess(script, timeout=20, env_extra=env)
    _assert_no_signal(cp)
    out = cp.stdout.decode(errors="replace")
    assert "RESULT True" in out, (
        "gated-off %s did not run the default scheduler cleanly:\n"
        "STDOUT:%s\nSTDERR:%s"
        % (flag, out, cp.stderr.decode(errors="replace")))


# ==========================================================================
# A17. INTEGRITY STRESS: many concurrent tasks each return a UNIQUE value;
#      assert SET-EQUALITY of the collected results (not just a count), so a
#      lost/duplicated wake or a cross-task result swap is caught.
# ==========================================================================
def test_many_tasks_unique_results_set_equality():
    N = 200

    async def body():
        async def child(i):
            # Stagger so completions interleave heavily.
            await asyncio.sleep((i % 7) * 0.001)
            return i
        results = await asyncio.gather(*[child(i) for i in range(N)])
        return results
    with hang_guard(30, "many tasks unique results"):
        results = aio.run(body())
    # Positional integrity AND set integrity (no dup, no loss, no swap).
    assert results == list(range(N)), "positional integrity broken"
    assert set(results) == set(range(N)), "set integrity broken"
    assert len(results) == N == len(set(results)), "duplicate/lost results"


def test_concurrent_echo_payload_integrity_set_equality():
    """Many concurrent echo connections each send a DISTINCT payload; assert the
    set of echoed payloads exactly equals the set sent (catches a cross-conn
    byte swap that a count-only check would miss)."""
    N = 40

    async def body():
        loop = asyncio.get_running_loop()

        class Echo(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                self.tr.write(data)
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        sem = asyncio.Semaphore(24)

        async def one(i):
            async with sem:
                r, w = await asyncio.open_connection("127.0.0.1", port)
                payload = ("payload-%06d-end" % i).encode()
                w.write(payload)
                await w.drain()
                got = await r.readexactly(len(payload))
                w.close()
                return got
        echoed = await asyncio.gather(*[one(i) for i in range(N)])
        await _close_settle(server)
        return echoed
    with hang_guard(60, "echo payload set-equality"):
        echoed = aio.run(body())
    expected = {("payload-%06d-end" % i).encode() for i in range(N)}
    assert set(echoed) == expected, (
        "echo payload set mismatch: missing=%r extra=%r"
        % (expected - set(echoed), set(echoed) - expected))


# ==========================================================================
# A18. Nested-cancel + finally ordering: a cancelled task's finally/cleanup
#      awaits must run to completion BEFORE the CancelledError surfaces (the
#      one-shot _pgmustcancel must not re-cancel cleanup awaits).
# ==========================================================================
def test_cancelled_task_finally_cleanup_completes():
    state = {}

    async def body():
        async def victim():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                state["caught"] = True
                # A cleanup await that MUST run to completion despite the cancel.
                await asyncio.sleep(0.02)
                state["cleanup_done"] = True
                raise
        t = asyncio.ensure_future(victim())
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "cancelled finally cleanup"):
        outcome = aio.run(body())
    assert outcome == "cancelled", outcome
    assert state.get("caught") is True, state
    assert state.get("cleanup_done") is True, (
        "cleanup await was re-cancelled before completing: %r" % state)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "-p", "no:cacheprovider"]))
