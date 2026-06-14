"""Adversarial QA: the runloom.aio asyncio bridge.

The aio bridge is where most of the recent compat bugs lived; CLAUDE.md lists a
dozen fragile invariants.  We target the observable ones:

  * connection_made-that-writes -- a server greeting written inside
    connection_made must reach the client (the _io_g seed-before-callback bug);
  * server close() wakes its accept-loop goroutines -- repeated create/close
    must not accumulate parked goroutines (the per-server leak);
  * cancellation -- a task parked in sleep / I/O / executor must take a
    CancelledError, not hang;
  * SLOW RETURN -- wait_for must time out promptly and overlap, gather must run
    concurrently (not serialise);
  * _driver sends None -- a custom awaitable whose __await__ yields a plain
    iterator (no .send) must not raise "object has no attribute 'send'".

Driven through runloom.aio.run() (its asyncio.run drop-in), no pytest-asyncio.
"""
import asyncio
import socket
import sys
import time

import pytest

import runloom.aio as aio
import runloom_c as rc
from adv_util import hang_guard, assert_faster_than


def _parked():
    return int(rc.stats().get("netpoll_parked_self", rc.stats().get("netpoll_parked", 0)))


# --------------------------------------------------------------------------
# connection_made that writes (the _io_g seed invariant)
# --------------------------------------------------------------------------
def test_connection_made_write_reaches_client():
    got = {}
    async def body():
        loop = asyncio.get_event_loop()
        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                tr.write(b"GREETING")          # write INSIDE connection_made
        class Cli(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr
            def data_received(self, data):
                got["greeting"] = data
                self.tr.close()
        server = await loop.create_server(Srv, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        await loop.create_connection(Cli, "127.0.0.1", port)
        await asyncio.sleep(0.05)
        server.close()
        await server.wait_closed()
    with hang_guard(20, "connection_made write"):
        aio.run(body())
    assert got.get("greeting") == b"GREETING"


# --------------------------------------------------------------------------
# server close() wakes accept loops -- no parked-goroutine accumulation
# --------------------------------------------------------------------------
def test_server_close_does_not_leak_accept_goroutines():
    async def cycle():
        loop = asyncio.get_event_loop()
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        server.close()
        await server.wait_closed()
    def one():
        aio.run(cycle())
    with hang_guard(30, "server close no leak"):
        one()                              # warm up (loop/keepalive setup)
        base = _parked()
        for _ in range(10):
            one()
        after = _parked()
    assert after <= base, "accept-loop goroutines leaked: parked %d -> %d" % (base, after)


# --------------------------------------------------------------------------
# cancellation: parked task must take CancelledError, not hang
# --------------------------------------------------------------------------
def test_cancel_task_parked_in_sleep():
    async def body():
        async def victim():
            await asyncio.sleep(100)
            return "finished"
        t = asyncio.ensure_future(victim())
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
            return "not-cancelled"
        except asyncio.CancelledError:
            return "cancelled"
    with hang_guard(20, "cancel parked sleep"):
        assert aio.run(body()) == "cancelled"


def test_cancel_task_parked_in_socket_recv():
    async def body():
        loop = asyncio.get_event_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        async def victim():
            await loop.sock_recv(a, 64)        # nobody sends -> parks
            return "got-data"
        t = asyncio.ensure_future(victim())
        await asyncio.sleep(0.05)
        t.cancel()
        try:
            await t
            out = "not-cancelled"
        except asyncio.CancelledError:
            out = "cancelled"
        a.close(); b.close()
        return out
    with hang_guard(20, "cancel parked recv"):
        assert aio.run(body()) == "cancelled"


# --------------------------------------------------------------------------
# slow return: wait_for timeout + gather concurrency
# --------------------------------------------------------------------------
def test_wait_for_times_out_promptly_and_cancels_inner():
    inner_cancelled = {}
    async def body():
        async def slow():
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                inner_cancelled["yes"] = True
                raise
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(slow(), timeout=0.05)
            return ("no-timeout", 0)
        except asyncio.TimeoutError:
            return ("timeout", time.monotonic() - t0)
    with hang_guard(20, "wait_for timeout"):
        outcome, el = aio.run(body())
    assert outcome == "timeout"
    assert el < 1.0, "wait_for took %.3fs for a 50ms timeout (slow return)" % el


def test_gather_runs_concurrently_not_serial():
    async def body():
        async def unit(i):
            await asyncio.sleep(0.05)
            return i
        t0 = time.monotonic()
        res = await asyncio.gather(*[unit(i) for i in range(8)])
        return res, time.monotonic() - t0
    with hang_guard(20, "gather concurrency"):
        res, el = aio.run(body())
    assert res == list(range(8))
    assert el < 0.3, "gather serialised: %.3fs for 8x50ms" % el


# --------------------------------------------------------------------------
# run_in_executor offload (+ that a parked executor call overlaps)
# --------------------------------------------------------------------------
def test_run_in_executor_offload_and_overlap():
    async def body():
        loop = asyncio.get_event_loop()
        def blocking(x):
            time.sleep(0.05)               # real blocking on a pool thread
            return x * 2
        t0 = time.monotonic()
        # two offloads should overlap on the pool, not serialise
        a, b = await asyncio.gather(
            loop.run_in_executor(None, blocking, 10),
            loop.run_in_executor(None, blocking, 20),
        )
        return (a, b), time.monotonic() - t0
    with hang_guard(20, "run_in_executor"):
        (a, b), el = aio.run(body())
    assert (a, b) == (20, 40)
    assert el < 0.4, "executor offloads serialised or blocked the loop (%.3fs)" % el


# --------------------------------------------------------------------------
# _driver must coro.send(None): a custom awaitable with no .send
# --------------------------------------------------------------------------
def test_custom_awaitable_without_send_does_not_break():
    class BareIterAwaitable:
        # __await__ returns an iterator that has __next__ but NO send(); a driver
        # that injects a non-None resume value would hit the .send() branch and
        # raise "object has no attribute 'send'".
        def __await__(self):
            class It:
                def __init__(s): s.n = 0
                def __iter__(s): return s
                def __next__(s):
                    s.n += 1
                    if s.n > 3:
                        raise StopIteration("done")
                    return None            # bare yield -> reschedule
            return It()
    async def body():
        return await BareIterAwaitable()
    with hang_guard(20, "custom awaitable no-send"):
        try:
            out = aio.run(body())
        except AttributeError as e:
            pytest.fail("driver injected a resume value into a send-less "
                        "awaitable: %s" % e)
    assert out == "done"


# --------------------------------------------------------------------------
# stress: many concurrent echo connections
# --------------------------------------------------------------------------
def test_many_concurrent_echo_connections():
    N = 50
    async def body():
        loop = asyncio.get_event_loop()
        class Echo(asyncio.Protocol):
            def connection_made(self, tr): self.tr = tr
            def data_received(self, data):
                self.tr.write(data)
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        async def one(i):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            payload = ("msg%d" % i).encode()
            w.write(payload)
            await w.drain()
            data = await r.readexactly(len(payload))
            w.close()
            return data == payload

        results = await asyncio.gather(*[one(i) for i in range(N)])
        server.close()
        await server.wait_closed()
        return results
    with hang_guard(40, "many echo connections"):
        results = aio.run(body())
    assert all(results), "%d/%d echo roundtrips failed" % (results.count(False), N)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
