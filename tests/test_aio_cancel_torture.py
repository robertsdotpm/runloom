"""Cancellation / teardown torture for the pygo aio layer.

pygo's history is dense with cancel/teardown bugs: cancel couldn't interrupt a
task parked in wait_fd (sock_recv/accept/connect), loop.close() hung, and
exception refcycles leaked on teardown.  This cancels tasks blocked on
cooperative I/O at staggered points and asserts cancellation is PROMPT (a hang
is caught by run_isolated's per-file timeout), CancelledError propagates, the
loop closes cleanly, and repeated cancel cycles leak no descriptors.
"""
import asyncio
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pygo.aio as aio


def _run(coro_fn):
    loop = aio.PygoEventLoop()
    try:
        return loop.run_until_complete(coro_fn(loop))
    finally:
        loop.close()


def _fd_count():
    return len(os.listdir("/proc/self/fd"))


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def test_cancel_parked_in_sock_recv():
    """A task parked in sock_recv (the classic 'cancel can't interrupt wait_fd'
    bug) must cancel promptly with CancelledError."""
    async def main(loop):
        a, b = _pair()
        try:
            task = loop.create_task(loop.sock_recv(a, 100))
            await asyncio.sleep(0.02)        # let it park in wait_fd
            task.cancel()
            try:
                await task
                raise AssertionError("expected CancelledError")
            except asyncio.CancelledError:
                pass
        finally:
            a.close()
            b.close()
    _run(main)


def test_cancel_during_sleep():
    async def main(loop):
        task = loop.create_task(asyncio.sleep(30))
        await asyncio.sleep(0.02)
        task.cancel()
        try:
            await task
            raise AssertionError("expected CancelledError")
        except asyncio.CancelledError:
            pass
    _run(main)


def test_cancel_gather_of_blocked_tasks():
    """Cancel a gather() of several tasks all parked in sock_recv."""
    async def main(loop):
        pairs = [_pair() for _ in range(8)]
        try:
            g = asyncio.gather(*[loop.sock_recv(a, 10) for a, _ in pairs])
            await asyncio.sleep(0.02)
            g.cancel()
            try:
                await g
                raise AssertionError("expected CancelledError")
            except asyncio.CancelledError:
                pass
        finally:
            for a, b in pairs:
                a.close()
                b.close()
    _run(main)


def test_repeated_cancel_no_fd_leak():
    """Many cancel-during-sock_recv cycles must not leak descriptors."""
    async def one(loop):
        a, b = _pair()
        try:
            task = loop.create_task(loop.sock_recv(a, 10))
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            a.close()
            b.close()

    async def main(loop):
        for _ in range(5):           # warmup
            await one(loop)
        base = _fd_count()
        for _ in range(40):
            await one(loop)
        leaked = _fd_count() - base
        assert leaked <= 0, "leaked {0} fd(s) across cancel cycles".format(leaked)
    _run(main)
