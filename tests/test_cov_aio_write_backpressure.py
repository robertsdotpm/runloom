"""Adversarial QA: aio write-side flow control (backpressure).

The stream transport carries asyncio's write-buffer flow-control surface:

  * protocol.pause_writing() fires when the queued write buffer crosses the
    HIGH watermark (a peer that never reads must apply backpressure instead of
    growing memory without bound);
  * protocol.resume_writing() fires once the peer drains the buffer back to the
    LOW watermark;
  * transport.set_write_buffer_limits()/get_write_buffer_limits() configure the
    watermarks (and re-evaluate pause/resume immediately);
  * transport.get_write_buffer_size() reports the queued bytes accurately.

We drive real sockets: a client transport (via loop.create_connection) writing
to a raw peer socket we accept ourselves and deliberately DON'T read from.  We
write far more than any kernel socket buffer can absorb, so the transport's
_write_buf grows past the high watermark and pause_writing() fires; then we
drain the peer and watch resume_writing() fire.

Driven through runloom.aio.run() (its asyncio.run drop-in), no pytest-asyncio.
"""
import asyncio
import socket
import sys

import pytest

import runloom.aio as aio
from adv_util import hang_guard


# A write large enough to overflow even a generously auto-tuned loopback socket
# buffer, so bytes are GUARANTEED to queue in the transport's _write_buf while
# the peer refuses to read.  (Draining it over loopback is still sub-second.)
BLOB = b"x" * (8 * 1024 * 1024)


class _RecordingProtocol(asyncio.Protocol):
    """Records pause_writing / resume_writing into a shared list."""
    def __init__(self, events):
        self.events = events
        self.tr = None

    def connection_made(self, tr):
        self.tr = tr

    def pause_writing(self):
        self.events.append("pause")

    def resume_writing(self):
        self.events.append("resume")


async def _connect_to_deaf_peer(loop, events):
    """Return (transport, protocol, peer_sock, listen_sock).

    `peer_sock` is a raw accepted socket that NOBODY reads from, so writing to
    `transport` backs up in the kernel + the transport's write buffer.
    """
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(1)
    lsock.setblocking(False)
    host, port = lsock.getsockname()[:2]

    accept = asyncio.ensure_future(loop.sock_accept(lsock))
    transport, proto = await loop.create_connection(
        lambda: _RecordingProtocol(events), host, port)
    peer_sock, _ = await accept
    peer_sock.setblocking(False)
    return transport, proto, peer_sock, lsock


# --------------------------------------------------------------------------
# The requested scenario: write past high_water to a deaf peer -> pause_writing;
# drain the peer -> resume_writing.
# --------------------------------------------------------------------------
def test_pause_writing_fires_then_resume_after_peer_drains():
    events = []
    result = {}

    async def body():
        loop = asyncio.get_event_loop()
        transport, proto, peer_sock, lsock = await _connect_to_deaf_peer(
            loop, events)

        # Realistic asyncio-default watermarks.
        transport.set_write_buffer_limits(high=64 * 1024, low=16 * 1024)

        # Write far more than the deaf peer + kernel can absorb.  The plaintext
        # fast path sends what fits, then buffers the (large) remainder -> the
        # queued buffer blows past high_water -> pause_writing() (fired
        # synchronously inside write()).
        transport.write(BLOB)
        for _ in range(50):
            if "pause" in events and transport.get_write_buffer_size() > 0:
                break
            await asyncio.sleep(0.01)
        result["paused_size"] = transport.get_write_buffer_size()
        result["limits"] = transport.get_write_buffer_limits()

        # Now drain the peer: read everything.  Freeing kernel space wakes the
        # transport's io fiber to send buffered bytes; once _write_buf falls to
        # low_water, resume_writing() fires.
        read_total = 0
        while "resume" not in events and read_total < len(BLOB):
            try:
                chunk = await loop.sock_recv(peer_sock, 262144)
            except OSError:
                break
            if not chunk:
                break
            read_total += len(chunk)
        # A final scheduler turn for a resume queued right at the boundary.
        for _ in range(50):
            if "resume" in events:
                break
            await asyncio.sleep(0.01)
        result["read_total"] = read_total
        result["final_size"] = transport.get_write_buffer_size()

        transport.close()
        peer_sock.close()
        lsock.close()

    with hang_guard(30, "pause/resume backpressure"):
        aio.run(body())

    assert "pause" in events, (
        "pause_writing never fired despite an 8 MB write to a peer that never "
        "reads; events=%r size=%r" % (events, result.get("paused_size")))
    assert result["paused_size"] > 0, (
        "get_write_buffer_size() reported 0 while paused (limits=%r)"
        % (result.get("limits"),))
    assert "resume" in events, (
        "resume_writing never fired after the peer drained %d bytes; "
        "events=%r final_size=%r"
        % (result.get("read_total", -1), events, result.get("final_size")))
    # pause must precede resume (correct flow-control ordering).
    assert events.index("pause") < events.index("resume"), (
        "resume_writing fired before pause_writing: %r" % events)


# --------------------------------------------------------------------------
# set_write_buffer_limits() re-evaluates pause/resume immediately -- a
# deterministic, drain-independent check of the same surface.
# --------------------------------------------------------------------------
def test_set_write_buffer_limits_reevaluates_pause_and_resume():
    events = []
    seen = {}

    async def body():
        loop = asyncio.get_event_loop()
        transport, proto, peer_sock, lsock = await _connect_to_deaf_peer(
            loop, events)

        # Watermarks well above anything we'll queue -> the initial write does
        # NOT auto-pause; we then drive pause/resume purely via the limits.
        transport.set_write_buffer_limits(high=64 * 1024 * 1024,
                                          low=16 * 1024 * 1024)
        transport.write(BLOB)
        # Let the fast path + io fiber settle; the deaf peer keeps bytes queued.
        for _ in range(50):
            if transport.get_write_buffer_size() > 0:
                break
            await asyncio.sleep(0.01)
        buffered = transport.get_write_buffer_size()
        seen["buffered"] = buffered
        seen["paused_before"] = "pause" in events

        if buffered > 0:
            # Lower high watermark BELOW the queued size -> pause_writing() must
            # fire synchronously inside set_write_buffer_limits().
            transport.set_write_buffer_limits(high=max(1, buffered // 2),
                                              low=max(0, buffered // 4))
            seen["paused_after_lower"] = "pause" in events

            cur = transport.get_write_buffer_size()
            # Raise low watermark ABOVE the queued size -> resume_writing().
            transport.set_write_buffer_limits(high=cur * 4 + 1,
                                              low=cur * 2 + 1)
            seen["resumed_after_raise"] = "resume" in events

        # Buffer is still full and the peer never read: abort() discards it and
        # tears down without a drain-forever hang.
        transport.abort()
        peer_sock.close()
        lsock.close()

    with hang_guard(30, "set_write_buffer_limits pause/resume"):
        aio.run(body())

    assert seen.get("buffered", 0) > 0, (
        "peer-never-reads write did not queue any bytes; can't exercise "
        "watermark re-evaluation (buffered=%r)" % seen.get("buffered"))
    assert seen.get("paused_before") is False, (
        "pause_writing fired before we lowered the limit: %r" % events)
    assert seen.get("paused_after_lower") is True, (
        "lowering high watermark below the buffer did not fire pause_writing: "
        "%r" % events)
    assert seen.get("resumed_after_raise") is True, (
        "raising low watermark above the buffer did not fire resume_writing: "
        "%r" % events)


# --------------------------------------------------------------------------
# get/set_write_buffer_limits contract: defaults, query round-trip, validation.
# --------------------------------------------------------------------------
def test_write_buffer_limits_query_and_validation():
    out = {}

    async def body():
        loop = asyncio.get_event_loop()
        transport, proto, peer_sock, lsock = await _connect_to_deaf_peer(
            loop, [])

        # Explicit values: get_write_buffer_limits returns (low, high).
        transport.set_write_buffer_limits(high=100, low=25)
        out["explicit"] = transport.get_write_buffer_limits()

        # high derived from low when omitted (asyncio: high = 4*low).
        transport.set_write_buffer_limits(low=20)
        out["from_low"] = transport.get_write_buffer_limits()

        # low derived from high when omitted (asyncio: low = high // 4).
        transport.set_write_buffer_limits(high=400)
        out["from_high"] = transport.get_write_buffer_limits()

        # high < low must be rejected.
        try:
            transport.set_write_buffer_limits(high=10, low=100)
            out["bad_raised"] = False
        except ValueError:
            out["bad_raised"] = True

        # get_write_buffer_size on an empty buffer is a non-negative int.
        out["empty_size"] = transport.get_write_buffer_size()

        transport.close()
        peer_sock.close()
        lsock.close()

    with hang_guard(20, "write buffer limits contract"):
        aio.run(body())

    assert out["explicit"] == (25, 100), (
        "get_write_buffer_limits should return (low, high); got %r"
        % (out["explicit"],))
    assert out["from_low"] == (20, 80), (
        "high should default to 4*low; got %r" % (out["from_low"],))
    assert out["from_high"] == (100, 400), (
        "low should default to high//4; got %r" % (out["from_high"],))
    assert out["bad_raised"] is True, "high < low was not rejected"
    assert out["empty_size"] == 0, (
        "fresh transport reported non-empty write buffer: %r"
        % (out["empty_size"],))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
