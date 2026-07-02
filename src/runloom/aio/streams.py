"""StreamReader / StreamWriter: the asyncio streams API surface."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ====================================================================
# Policy + convenience entry points
# ====================================================================
# ====================================================================
# Network: open_connection / start_server with StreamReader/Writer.
#
# We bypass asyncio's Transport/Protocol stack entirely.  Each connection
# is a runloom fiber doing cooperative socket I/O via wait_fd.  The
# StreamReader/Writer classes we hand to user code present the standard
# asyncio API surface (read / readline / readuntil / readexactly /
# write / drain / close) so existing async TCP code Just Works.
# ====================================================================
class StreamReader(object):
    """asyncio.StreamReader-compatible reader backed by cooperative
    socket recv.  Implements: read, readline, readuntil, readexactly,
    at_eof, feed_eof.  Buffers internally so readline / readuntil
    don't have to issue per-byte recvs."""

    def __init__(self, sock, *, limit=2**16, loop=None):
        self._sock = sock
        self._buf  = bytearray()
        self._eof  = False
        self._limit = limit
        self._loop = loop

    def at_eof(self):
        return self._eof and not self._buf

    def feed_eof(self):
        self._eof = True

    def _fill(self):
        """Block (cooperatively) until at least one chunk arrives, or
        the peer closes.  Returns True if data was read, False at EOF."""
        if self._eof:
            return False
        while True:
            try:
                chunk = self._sock.recv(self._limit)
            except (BlockingIOError, InterruptedError):
                _wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK, _errno.EINTR):
                    _wait_fd(self._sock.fileno(), 1)
                    continue
                raise
            if not chunk:
                self._eof = True
                return False
            self._buf.extend(chunk)
            return True

    async def read(self, n=-1):
        """Read up to n bytes (-1 = until EOF)."""
        if n == 0:
            return b""
        if n < 0:
            # Read until EOF.
            while not self._eof:
                self._fill()
            data, self._buf = bytes(self._buf), bytearray()
            return data

        # n > 0: ensure we have at least one byte, then return up to n.
        while not self._buf and not self._eof:
            self._fill()
        if not self._buf:
            return b""
        take = min(n, len(self._buf))
        data = bytes(self._buf[:take])
        del self._buf[:take]
        return data

    async def readexactly(self, n):
        """Read exactly n bytes, or raise asyncio.IncompleteReadError."""
        while len(self._buf) < n:
            if not self._fill():
                # EOF -- partial data.
                partial = bytes(self._buf)
                self._buf.clear()
                raise asyncio.IncompleteReadError(partial, n)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    async def readuntil(self, separator=b"\n"):
        """Read until separator (inclusive), or raise
        asyncio.IncompleteReadError on EOF.

        Honors the configured stream `limit`: if the separator is not found
        within `limit` bytes, asyncio.LimitOverrunError is raised and the data
        is LEFT in the buffer (so it can be read again) -- matching stock
        asyncio.  `separator` may also be a tuple of separators (asyncio 3.13);
        the shortest match wins.
        """
        # Mirror stock asyncio.StreamReader.readuntil: sorted-by-length so the
        # shortest separator wins on a tie; tuple support; LimitOverrunError.
        if isinstance(separator, tuple):
            separator = sorted(separator, key=len)
        else:
            separator = [separator]
        if not separator:
            raise ValueError("Separator should contain at least one element")
        min_seplen = len(separator[0])
        max_seplen = len(separator[-1])
        if min_seplen == 0:
            raise ValueError("Separator should be at least one-byte string")

        # `offset` is the count of leading buffer bytes known to contain no
        # occurrence of any separator (so we don't rescan them each pass).
        offset = 0
        match_start = match_end = None
        while True:
            buflen = len(self._buf)
            if buflen - offset >= min_seplen:
                match_start = match_end = None
                for sep in separator:
                    isep = self._buf.find(sep, offset)
                    if isep != -1:
                        end = isep + len(sep)
                        if match_end is None or end < match_end:
                            match_end = end
                            match_start = isep
                if match_end is not None:
                    break
                offset = max(0, buflen + 1 - max_seplen)
                if offset > self._limit:
                    raise asyncio.LimitOverrunError(
                        "Separator is not found, and chunk exceed the limit",
                        offset)
            # Inspect the buffer BEFORE acting on EOF: the final chunk may have
            # completed the separator.
            if self._eof:
                partial = bytes(self._buf)
                self._buf.clear()
                raise asyncio.IncompleteReadError(partial, None)
            self._fill()

        if match_start > self._limit:
            raise asyncio.LimitOverrunError(
                "Separator is found, but chunk is longer than limit",
                match_start)
        data = bytes(self._buf[:match_end])
        del self._buf[:match_end]
        return data

    async def readline(self):
        sep = b"\n"
        seplen = len(sep)
        try:
            return await self.readuntil(sep)
        except asyncio.IncompleteReadError as e:
            return e.partial
        except asyncio.LimitOverrunError as e:
            # Match stock asyncio: drop the over-limit line (consuming the
            # separator if present) and raise ValueError.
            if self._buf.startswith(sep, e.consumed):
                del self._buf[:e.consumed + seplen]
            else:
                self._buf.clear()
            raise ValueError(e.args[0])

    def __aiter__(self):
        # Match stock asyncio.StreamReader: `async for line in reader:` reads
        # lines via readline() and stops at EOF (readline returns b"").
        return self

    async def __anext__(self):
        val = await self.readline()
        if val == b"":
            raise StopAsyncIteration
        return val


class StreamWriter(object):
    """asyncio.StreamWriter-compatible writer backed by cooperative
    socket sendall.  Implements: write, writelines, drain, close,
    wait_closed, get_extra_info."""

    def __init__(self, sock, reader=None, *, loop=None):
        self._sock = sock
        self._reader = reader
        self._loop = loop
        self._closed = False
        # Buffer for write/drain semantics.  asyncio's StreamWriter
        # buffers on writes and flushes on drain; we send immediately
        # (cooperative blocking) so drain is a no-op but kept for API
        # parity.
        self._buf = bytearray()

    def write(self, data):
        if self._closed:
            raise RuntimeError("write on closed StreamWriter")
        self._buf.extend(data)
        # Try a non-blocking flush so small writes don't accumulate
        # in pathological cases.  If the socket would block, the next
        # drain() will handle it.
        self._try_flush()

    def writelines(self, lines):
        for line in lines:
            self._buf.extend(line)
        self._try_flush()

    def _try_flush(self):
        """Best-effort non-blocking flush.  Leaves residue in _buf."""
        while self._buf:
            try:
                n = self._sock.send(self._buf)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as e:
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    return
                raise
            if n <= 0:
                return
            del self._buf[:n]

    def _flush_blocking(self):
        """Block (cooperatively) until all buffered data is on the wire."""
        while self._buf:
            try:
                n = self._sock.send(self._buf)
                if n > 0:
                    del self._buf[:n]
                    continue
            except (BlockingIOError, InterruptedError):
                pass
            except OSError as e:
                if e.errno not in (_errno.EAGAIN, _errno.EWOULDBLOCK, _errno.EINTR):
                    raise
            _wait_fd(self._sock.fileno(), 2)

    async def drain(self):
        """Block (cooperatively) until all buffered data is on the wire."""
        self._flush_blocking()

    def close(self):
        if self._closed:
            return
        self._closed = True
        # asyncio's transport.close() flushes any buffered write data before
        # sending FIN; mirror that so a write() whose data didn't fit the kernel
        # send buffer (residue left in _buf by _try_flush) isn't silently
        # truncated by close().  We run in a fiber, so _flush_blocking parks
        # cooperatively just like drain().
        try:
            self._flush_blocking()
        except OSError:
            pass
        finally:
            try:
                self._sock.shutdown(_socket.SHUT_RDWR)
            except OSError:
                pass
            _close_sock(self._sock)

    def is_closing(self):
        return self._closed

    async def wait_closed(self):
        # Our close is synchronous; nothing to wait on.  Yield once so
        # callers using `await writer.wait_closed()` don't see surprise
        # tight loops.
        await asyncio.sleep(0)

    def get_extra_info(self, name, default=None):
        if name == "peername":
            try:
                return self._sock.getpeername()
            except OSError:
                return default
        if name == "sockname":
            try:
                return self._sock.getsockname()
            except OSError:
                return default
        if name == "socket":
            return self._sock
        obj = getattr(self._sock, "ssl_object", None)
        if name == "ssl_object":
            return obj if obj is not None else default
        if name == "peercert":
            return obj.getpeercert() if obj is not None else default
        if name == "cipher":
            return obj.cipher() if obj is not None else default
        if name == "sslcontext":
            return obj.context if obj is not None else default
        return default

    @property
    def transport(self):
        # asyncio code commonly does writer.transport.get_extra_info(...);
        # forward to ourselves for compat.
        return self


# ====================================================================
# 3.14 free-threaded fix for the STDLIB asyncio.streams.StreamReader.read
#
# create_subprocess_exec() builds the stdlib StreamReader (via
# SubprocessStreamProtocol), not the bridge reader above, and its
# Process.communicate() does `await gather(_read_stream(1), _read_stream(2))`.
# The stdlib read() (byte-identical across 3.13 and 3.14) does:
#     data = bytes(memoryview(self._buffer)[:n])
#     del self._buffer[:n]
# Under 3.14 free-threading the temporary `memoryview(self._buffer)[:n]` slice's
# deallocation is DEFERRED (deferred refcounting / delayed free), so its buffer
# EXPORT over the bytearray is still live when the very next statement resizes
# it -> "BufferError: Existing exports of data: object cannot be re-sized" at
# streams.py:734.  3.13t (immediate reclaim) is unaffected.  The minimal,
# lowest-risk fix is to drop the lingering memoryview temporary: a plain
# bytearray slice (`self._buffer[:n]`) yields a fresh bytes object and holds no
# export over the buffer.  Same return semantics (at most n bytes,
# _maybe_resume_transport()).  Installed only on >=3.14, so 3.13t is untouched.
def _pg_install_stdlib_streamreader_read_314():
    if sys.version_info < (3, 14):
        return
    import asyncio.streams as _streams

    _Reader = _streams.StreamReader
    if getattr(_Reader.read, "__runloom_ft314__", False):
        return      # already installed

    async def read(self, n=-1):
        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b''

        if n < 0:
            blocks = []
            while True:
                block = await self.read(self._limit)
                if not block:
                    break
                blocks.append(block)
            return b''.join(blocks)

        if not self._buffer and not self._eof:
            await self._wait_for_data('read')

        # bytes(self._buffer[:n]) -- no lingering memoryview export (see above).
        data = bytes(self._buffer[:n])
        del self._buffer[:n]

        self._maybe_resume_transport()
        return data

    read.__doc__ = _Reader.read.__doc__
    read.__runloom_ft314__ = True
    _Reader.read = read


_pg_install_stdlib_streamreader_read_314()
