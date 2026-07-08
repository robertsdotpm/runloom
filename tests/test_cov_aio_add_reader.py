"""Coverage: loop.add_reader / add_writer / remove_reader / remove_writer.

These low-level selector-style callbacks had zero direct tests.  asyncio's
contract: add_reader(fd, cb, *args) fires cb() (the callback does the recv)
whenever fd is readable; remove_reader(fd) stops it.  add_writer/remove_writer
mirror that for writability.  runloom implements them level-triggered via a
single per-fd io-runner fiber (src/runloom/aio/loop_io.py::_pg_io_runner) that
parks on the union interest mask and re-arms after each dispatch.

We drive everything through runloom.aio.run() (its asyncio.run drop-in) with a
hang_guard so a lost-wake regression fails as a timeout, not a wedged suite.
"""
import asyncio
import socket
import sys

import pytest

import runloom.aio as aio
from adv_util import hang_guard


def _nb_pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


# --------------------------------------------------------------------------
# add_reader fires + receives data; remove_reader stops further fires.
# --------------------------------------------------------------------------
def test_add_reader_fires_and_remove_stops():
    async def body():
        loop = asyncio.get_event_loop()
        a, b = _nb_pair()
        received = []

        def on_readable():
            try:
                data = a.recv(1024)
            except BlockingIOError:
                return           # spurious level-triggered re-arm; no data yet
            if data:
                received.append(data)

        h = loop.add_reader(a, on_readable)
        assert isinstance(h, asyncio.Handle)

        b.send(b"ping")
        await asyncio.sleep(0.05)          # let the io-runner service the fd
        first = list(received)

        removed = loop.remove_reader(a)
        await asyncio.sleep(0.02)          # let the runner observe removal + exit

        b.send(b"pong")                    # data arrives but nobody is watching
        await asyncio.sleep(0.05)
        after = list(received)

        a.close(); b.close()
        return first, removed, after

    with hang_guard(20, "add_reader fires/remove stops"):
        first, removed, after = aio.run(body())

    assert first == [b"ping"], \
        "add_reader callback did not fire/receive data: %r" % (first,)
    assert removed is True, "remove_reader returned %r for a registered fd" % (removed,)
    assert after == [b"ping"], \
        "reader callback fired after remove_reader (got %r)" % (after,)


# --------------------------------------------------------------------------
# add_writer fires on a writable fd; remove_writer stops further fires.
# --------------------------------------------------------------------------
def test_add_writer_fires_and_remove_stops():
    async def body():
        loop = asyncio.get_event_loop()
        a, b = _nb_pair()
        count = [0]

        def on_writable():
            count[0] += 1

        h = loop.add_writer(a, on_writable)
        assert isinstance(h, asyncio.Handle)

        await asyncio.sleep(0.05)          # fresh socket is writable -> cb fires
        fired = count[0]

        removed = loop.remove_writer(a)
        await asyncio.sleep(0.02)          # let the runner observe removal + exit
        snapshot = count[0]

        await asyncio.sleep(0.05)          # confirm no further fires
        final = count[0]

        a.close(); b.close()
        return fired, removed, snapshot, final

    with hang_guard(20, "add_writer fires/remove stops"):
        fired, removed, snapshot, final = aio.run(body())

    assert fired >= 1, "add_writer callback never fired on a writable socket"
    assert removed is True, "remove_writer returned %r for a registered fd" % (removed,)
    assert final == snapshot, \
        "writer callback fired after remove_writer (%d -> %d)" % (snapshot, final)


# --------------------------------------------------------------------------
# reader + writer on the SAME fd share one runner (union interest mask): both
# fire, and removing one leaves the other live.
# --------------------------------------------------------------------------
def test_reader_and_writer_same_fd_union_mask():
    async def body():
        loop = asyncio.get_event_loop()
        a, b = _nb_pair()
        got = {"r": [], "w": 0}

        def on_readable():
            try:
                data = a.recv(1024)
            except BlockingIOError:
                return
            if data:
                got["r"].append(data)

        def on_writable():
            got["w"] += 1

        loop.add_reader(a, on_readable)
        loop.add_writer(a, on_writable)

        b.send(b"hey")
        await asyncio.sleep(0.05)
        r_after_both = list(got["r"])
        w_fired = got["w"]

        # Drop only the writer; the reader must keep working.
        loop.remove_writer(a)
        await asyncio.sleep(0.02)
        w_snapshot = got["w"]

        b.send(b"more")
        await asyncio.sleep(0.05)
        r_final = list(got["r"])
        w_final = got["w"]

        loop.remove_reader(a)
        a.close(); b.close()
        return r_after_both, w_fired, w_snapshot, r_final, w_final

    with hang_guard(20, "reader+writer same fd"):
        r_after_both, w_fired, w_snapshot, r_final, w_final = aio.run(body())

    assert r_after_both == [b"hey"], "reader on shared fd missed data: %r" % (r_after_both,)
    assert w_fired >= 1, "writer on shared fd never fired"
    assert r_final == [b"hey", b"more"], \
        "reader stopped after removing the writer (got %r)" % (r_final,)
    assert w_final == w_snapshot, \
        "writer kept firing after remove_writer (%d -> %d)" % (w_snapshot, w_final)


# --------------------------------------------------------------------------
# remove on an fd with nothing registered returns False (asyncio contract).
# --------------------------------------------------------------------------
def test_remove_unregistered_returns_false():
    async def body():
        loop = asyncio.get_event_loop()
        a, b = _nb_pair()
        r = loop.remove_reader(a)
        w = loop.remove_writer(a)
        a.close(); b.close()
        return r, w

    with hang_guard(20, "remove unregistered"):
        r, w = aio.run(body())

    assert r is False, "remove_reader on an unregistered fd returned %r" % (r,)
    assert w is False, "remove_writer on an unregistered fd returned %r" % (w,)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
