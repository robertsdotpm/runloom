"""Coverage: aio transport/stream LIFECYCLE edge cases.

Four gaps in the runloom.aio asyncio bridge that the existing suites don't
exercise as a lifecycle whole:

  (1) write_eof / can_write_eof HALF-CLOSE -- a client that writes a request
      then write_eof() must let the server's reader hit EOF, reply, and the
      client must still read that reply.  can_write_eof() is True and
      connection_lost must NOT fire until the client explicitly close()s
      (the peer keeps its write side open via eof_received()->True).
  (2) transport.abort() with a large queued payload -- connection_lost must
      fire (exc None, per asyncio's forced _call_connection_lost), the payload
      is NOT fully delivered, teardown is clean, and repeated cycles leak no
      file descriptors.
  (3) refused-port connect -- open_connection AND loop.create_connection to a
      closed port raise ConnectionRefusedError, and the loop stays usable: a
      subsequent connect to a live server succeeds.
  (4) AF_UNIX -- asyncio.start_unix_server + asyncio.open_unix_connection over
      a tempfile path do an echo round-trip; server.close()/wait_closed() must
      not leave the accept goroutine parked (the per-server accept-fiber leak).

Driven through runloom.aio.run() (its asyncio.run drop-in), no pytest-asyncio.
"""
import asyncio
import os
import socket
import sys
import tempfile

import pytest

import runloom.aio as aio
import runloom_c as rc
from adv_util import hang_guard


def _parked():
    return int(rc.stats().get("netpoll_parked_self",
                              rc.stats().get("netpoll_parked", 0)))


def _fdcount():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


# --------------------------------------------------------------------------
# (1) write_eof / can_write_eof half-close
# --------------------------------------------------------------------------
def test_write_eof_half_close_reply_then_explicit_close():
    state = {}
    srv = {}

    async def body():
        loop = asyncio.get_event_loop()

        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                srv["tr"] = tr
                self.buf = b""

            def data_received(self, data):
                self.buf += data

            def eof_received(self):
                # Client half-closed its write side.  Reply, and RETURN True to
                # keep our own write side open (half-open) so the client can
                # read the reply and its connection_lost stays unfired.
                srv["tr"].write(b"REPLY:" + self.buf)
                return True

        class Cli(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                state["reply"] = state.get("reply", b"") + data

            def connection_lost(self, exc):
                state["lost"] = True

        server = await loop.create_server(Srv, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        tr, proto = await loop.create_connection(Cli, "127.0.0.1", port)

        # can_write_eof() must be True for a plain socket transport.
        state["can_write_eof"] = tr.can_write_eof()

        tr.write(b"REQUEST")
        tr.write_eof()

        # Wait for the reply to arrive over the still-open read side.
        for _ in range(200):
            if state.get("reply") == b"REPLY:REQUEST":
                break
            await asyncio.sleep(0.01)

        state["lost_before_close"] = state.get("lost", False)

        # Explicit close now delivers connection_lost.
        tr.close()
        for _ in range(200):
            if state.get("lost"):
                break
            await asyncio.sleep(0.01)
        state["lost_after_close"] = state.get("lost", False)

        # Clean up the still-open server side + the listener.
        st = srv.get("tr")
        if st is not None:
            st.close()
        server.close()
        await server.wait_closed()

    with hang_guard(20, "write_eof half-close"):
        aio.run(body())

    assert state.get("can_write_eof") is True, "can_write_eof() should be True"
    assert state.get("reply") == b"REPLY:REQUEST", \
        "client did not read the post-EOF reply: %r" % state.get("reply")
    assert state.get("lost_before_close") is False, \
        "connection_lost fired BEFORE the explicit close (half-close broken)"
    assert state.get("lost_after_close") is True, \
        "connection_lost never fired after explicit close()"


# --------------------------------------------------------------------------
# (2) transport.abort()
# --------------------------------------------------------------------------
def test_abort_fires_connection_lost_no_fd_leak():
    PAYLOAD = b"x" * (4 * 1024 * 1024)   # big enough to not fully drain instantly

    async def one_cycle(record):
        loop = asyncio.get_event_loop()
        seen = {"n": 0}

        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr
                # Pause reading so the client's buffer can't fully flush before
                # abort() discards it -> the payload is genuinely truncated.
                tr.pause_reading()

            def data_received(self, data):
                seen["n"] += len(data)

        class Cli(asyncio.Protocol):
            def connection_made(self, tr):
                self.tr = tr

            def connection_lost(self, exc):
                record["lost"] = True
                record["exc"] = exc

        server = await loop.create_server(Srv, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        tr, proto = await loop.create_connection(Cli, "127.0.0.1", port)

        tr.write(PAYLOAD)
        tr.abort()

        for _ in range(200):
            if record.get("lost"):
                break
            await asyncio.sleep(0.01)

        record["delivered"] = seen["n"]
        # The server-side connection was pause_reading()'d so it never observed
        # the client's RST and is still in _conns; close_clients() tears it down
        # (-> connection_lost -> _detach) so wait_closed() can complete instead
        # of blocking on the lingering connection.
        server.close()
        server.close_clients()
        await server.wait_closed()

    def run_cycle():
        rec = {}
        aio.run(one_cycle(rec))
        return rec

    with hang_guard(30, "abort connection_lost + fd leak"):
        rec = run_cycle()                # warm up (loop/netpoll fds settle)
        assert rec.get("lost") is True, "connection_lost did not fire after abort()"
        # asyncio's abort() forces connection_lost with exc None (or a
        # ConnectionError on some stacks) -- never a normal completion.
        exc = rec.get("exc")
        assert exc is None or isinstance(exc, ConnectionError), \
            "abort() connection_lost exc must be None/ConnectionError, got %r" % exc
        assert rec.get("delivered", 0) < len(PAYLOAD), \
            "abort() delivered the WHOLE payload (%d) -- buffer not dropped" \
            % rec.get("delivered")

        base = _fdcount()
        for _ in range(8):
            r = run_cycle()
            assert r.get("lost") is True, "connection_lost missed on a repeat abort"
        after = _fdcount()

    assert after <= base + 2, \
        "fd leak across abort cycles: %d -> %d" % (base, after)


# --------------------------------------------------------------------------
# (3) refused-port connect leaves the loop usable
# --------------------------------------------------------------------------
def _closed_port():
    # Bind+listen to grab a port, then close it: connects now get RST/ECONNREFUSED.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def test_refused_port_open_connection_then_loop_still_usable():
    async def body():
        results = {}
        dead = _closed_port()

        # (a) streams open_connection to a refused port -> ConnectionRefusedError
        try:
            await aio.open_connection("127.0.0.1", dead)
            results["open"] = "no-raise"
        except ConnectionRefusedError:
            results["open"] = "refused"
        except OSError as e:
            results["open"] = ("oserror", type(e).__name__)

        # (b) loop.create_connection to a refused port -> ConnectionRefusedError
        loop = asyncio.get_event_loop()
        try:
            await loop.create_connection(asyncio.Protocol, "127.0.0.1", dead)
            results["create"] = "no-raise"
        except ConnectionRefusedError:
            results["create"] = "refused"
        except OSError as e:
            results["create"] = ("oserror", type(e).__name__)

        # (c) the loop must still work -- a real echo round-trip afterwards.
        async def handler(reader, writer):
            data = await reader.read(64)
            writer.write(data)
            await writer.drain()
            writer.close()

        server = await aio.start_server(handler, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()[:2]
        r, w = await aio.open_connection(host, port)
        w.write(b"still-alive")
        await w.drain()
        results["echo"] = await r.read(64)
        w.close()
        server.close()
        return results

    with hang_guard(20, "refused port then reuse"):
        results = aio.run(body())

    assert results.get("open") == "refused", \
        "open_connection to refused port: expected ConnectionRefusedError, got %r" \
        % (results.get("open"),)
    assert results.get("create") == "refused", \
        "create_connection to refused port: expected ConnectionRefusedError, got %r" \
        % (results.get("create"),)
    assert results.get("echo") == b"still-alive", \
        "loop unusable after a refused connect: echo=%r" % results.get("echo")


# --------------------------------------------------------------------------
# (4) AF_UNIX echo + no leaked accept fiber
# --------------------------------------------------------------------------
@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"),
                    reason="AF_UNIX not available on this platform")
def test_unix_server_echo_and_no_accept_fiber_leak():
    # Short path (AF_UNIX sun_path caps at ~108 bytes); the scratchpad path is
    # long, so bind under /tmp directly.
    path = tempfile.mktemp(prefix="rl_unix_", suffix=".sock", dir="/tmp")

    async def cycle():
        async def handle(reader, writer):
            data = await reader.readline()
            writer.write(b"echo:" + data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_unix_server(handle, path)
        try:
            reader, writer = await asyncio.open_unix_connection(path)
            writer.write(b"ping\n")
            await writer.drain()
            resp = await reader.readline()
            writer.close()
            return resp
        finally:
            server.close()
            await server.wait_closed()

    try:
        with hang_guard(20, "unix echo + accept leak"):
            resp = aio.run(cycle())            # round-trip + warm up
            assert resp == b"echo:ping\n", "unix echo round-trip failed: %r" % resp
            base = _parked()
            for _ in range(6):
                aio.run(cycle())
            after = _parked()
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass

    assert after <= base, \
        "unix server accept fiber leaked: parked %d -> %d" % (base, after)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
