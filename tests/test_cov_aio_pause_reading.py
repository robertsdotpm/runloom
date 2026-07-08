"""Coverage: transport.pause_reading() / resume_reading() read-side backpressure.

A server Protocol calls ``pause_reading()`` from its first ``data_received``.
While paused the transport must NOT deliver any further ``data_received`` --
the extra bytes the client pushes have to wait in the kernel receive buffer.
Only once ``resume_reading()`` is scheduled do the remaining bytes flow, and
they must arrive in the original order (concatenation identity).

Driven through runloom.aio.run() (its asyncio.run drop-in), no pytest-asyncio,
matching tests/test_adv_aio.py / tests/test_aio_net.py conventions.
"""
import asyncio
import sys
import time

import pytest

import runloom.aio as aio
from adv_util import hang_guard


async def _wait_until(pred, timeout=5.0):
    """Poll `pred` cooperatively until true or timeout; returns pred()'s truth."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if pred():
            return True
        await asyncio.sleep(0.005)
    return bool(pred())


def _make_server_protocol(state):
    class Srv(asyncio.Protocol):
        def connection_made(self, tr):
            self.tr = tr
            state["srv_tr"] = tr

        def data_received(self, data):
            state["chunks"].append(bytes(data))
            state["calls"] += 1
            if not state["paused"]:
                # Pause on the FIRST data_received.  Every later byte the client
                # sends must now be withheld until resume_reading().
                state["paused"] = True
                self.tr.pause_reading()
                state["paused_event"].set()
    return Srv


# --------------------------------------------------------------------------
# core: pause holds off further data_received; resume delivers the rest in order
# --------------------------------------------------------------------------
def test_pause_reading_holds_then_resume_delivers_in_order():
    first = b"FIRST-CHUNK"
    rest = [b"beta", b"gamma", b"delta", b"epsilon"]
    total = first + b"".join(rest)

    outcome = {}

    async def body():
        loop = asyncio.get_event_loop()
        state = {
            "chunks": [],
            "calls": 0,
            "paused": False,
            "srv_tr": None,
            "paused_event": asyncio.Event(),
        }
        server = await loop.create_server(
            _make_server_protocol(state), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        r, w = await asyncio.open_connection("127.0.0.1", port)

        # 1) Client sends the first chunk; server pauses on receiving it.
        w.write(first)
        await w.drain()
        got_pause = await _wait_until(
            lambda: state["paused_event"].is_set(), timeout=5.0)
        assert got_pause, "server never received the first chunk / never paused"

        # 2) With the transport paused, the client pushes MORE data.
        for c in rest:
            w.write(c)
        await w.drain()

        # 3) Backpressure window: the paused transport must deliver NOTHING new.
        #    Give the io fiber ample chance to (wrongly) read + dispatch.
        await asyncio.sleep(0.3)
        outcome["during_pause"] = b"".join(state["chunks"])
        outcome["calls_during_pause"] = state["calls"]

        # 4) Schedule resume_reading (via the loop, per the "is scheduled" spec).
        loop.call_soon(state["srv_tr"].resume_reading)

        # 5) After resume, the withheld bytes must all arrive.
        arrived = await _wait_until(
            lambda: b"".join(state["chunks"]) == total, timeout=5.0)
        outcome["after_resume"] = b"".join(state["chunks"])
        outcome["arrived"] = arrived

        w.close()
        server.close()
        await server.wait_closed()

    with hang_guard(30, "pause_reading backpressure"):
        aio.run(body())

    # CORRECTNESS: while paused only the first chunk was delivered.
    assert outcome["during_pause"] == first, (
        "pause_reading() leaked bytes: expected only %r while paused, got %r"
        % (first, outcome["during_pause"]))
    # CORRECTNESS: after resume every withheld byte arrived, in original order.
    assert outcome["arrived"], (
        "resume_reading() did not deliver the withheld bytes: got %r, want %r"
        % (outcome["after_resume"], total))
    assert outcome["after_resume"] == total


# --------------------------------------------------------------------------
# is_reading() reflects the pause state
# --------------------------------------------------------------------------
def test_is_reading_reflects_pause_state():
    outcome = {}

    async def body():
        loop = asyncio.get_event_loop()
        state = {
            "chunks": [],
            "calls": 0,
            "paused": False,
            "srv_tr": None,
            "paused_event": asyncio.Event(),
        }
        server = await loop.create_server(
            _make_server_protocol(state), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"x")
        await w.drain()
        assert await _wait_until(
            lambda: state["paused_event"].is_set(), timeout=5.0)

        tr = state["srv_tr"]
        outcome["reading_while_paused"] = tr.is_reading()
        tr.resume_reading()
        outcome["reading_after_resume"] = tr.is_reading()

        w.close()
        server.close()
        await server.wait_closed()

    with hang_guard(30, "is_reading pause state"):
        aio.run(body())

    assert outcome["reading_while_paused"] is False, \
        "is_reading() must be False while paused"
    assert outcome["reading_after_resume"] is True, \
        "is_reading() must be True after resume_reading()"


# --------------------------------------------------------------------------
# a larger withheld payload also stays buffered, then arrives whole & in order
# --------------------------------------------------------------------------
def test_pause_reading_withholds_large_payload():
    first = b"HELLO"
    # ~256 KiB pushed while paused -- large enough to span many recv()s, so a
    # broken pause that read even one recv would show up as a partial leak.
    bulk = (b"".join((b"%08d" % i) for i in range(32768)))  # 256 KiB, ordered
    total = first + bulk
    outcome = {}

    async def body():
        loop = asyncio.get_event_loop()
        state = {
            "chunks": [],
            "calls": 0,
            "paused": False,
            "srv_tr": None,
            "paused_event": asyncio.Event(),
        }
        server = await loop.create_server(
            _make_server_protocol(state), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(first)
        await w.drain()
        assert await _wait_until(
            lambda: state["paused_event"].is_set(), timeout=5.0)

        w.write(bulk)
        # NB: don't fully drain -- a 256 KiB write may block on the socket
        # buffer precisely because the paused server isn't draining it, which
        # is the backpressure we want.  Kick the write off and move on.
        drain_task = asyncio.ensure_future(w.drain())

        await asyncio.sleep(0.3)
        outcome["during_pause"] = b"".join(state["chunks"])

        loop.call_soon(state["srv_tr"].resume_reading)
        arrived = await _wait_until(
            lambda: b"".join(state["chunks"]) == total, timeout=10.0)
        outcome["arrived"] = arrived
        outcome["after_len"] = sum(len(c) for c in state["chunks"])

        try:
            await asyncio.wait_for(drain_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        w.close()
        server.close()
        await server.wait_closed()

    with hang_guard(60, "pause_reading large payload"):
        aio.run(body())

    assert outcome["during_pause"] == first, (
        "pause_reading() leaked %d bytes of a large withheld payload"
        % (len(outcome["during_pause"]) - len(first)))
    assert outcome["arrived"], (
        "resume_reading() did not deliver the full withheld payload "
        "(got %d/%d bytes)" % (outcome["after_len"], len(total)))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
