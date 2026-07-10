"""_ReadPipeTransport / _WritePipeTransport (connect_read/write_pipe)."""
import stat as _stat

from ._base import *  # noqa: F401,F403  (shared foundation)

class _ReadPipeTransport(asyncio.ReadTransport):
    """connect_read_pipe transport: a fiber parks on the pipe fd via wait_fd
    (cooperative, no OS thread) and feeds protocol.data_received; EOF ->
    eof_received + connection_lost."""

    def __repr__(self):
        # Mirror asyncio's _UnixReadPipeTransport.__repr__ (the runloom loop has
        # no _selector, so the open pipe reports 'open').
        info = [self.__class__.__name__]
        if self._pipe is None:
            info.append("closed")
        elif self._closing:
            info.append("closing")
        info.append("fd={0}".format(self._fd))
        info.append("open" if self._pipe is not None else "closed")
        return "<{0}>".format(" ".join(info))

    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._fd = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._paused = False
        self._read_g = None
        try:
            _os.set_blocking(self._fd, False)
        except OSError:
            pass
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        # Roomy stack: _read_loop runs protocol.data_received synchronously, and
        # that can recurse deep into C (e.g. asyncssh encrypts a channel write
        # via chacha20/OpenSSL right inside data_received) -- the default 128 KB
        # g-stack overflows and SEGVs.  Same rationale as the socket io_loop /
        # _IO_STACK; the pipe transport just took a different spawn path.
        self._read_g = _fiber_io(self._read_loop)

    def _read_loop(self):
        # Cooperative replacement for the old reader thread: non-blocking os.read
        # + wait_fd(READ) on the raw pipe fd, on the loop thread, so data_received
        # / eof fire inline (no call_soon_threadsafe).  Exits on pause (respawned
        # by resume_reading), close, or EOF.
        fd = self._fd
        eof = False
        while True:
            if self._closing or self._paused:
                self._read_g = None
                return
            try:
                data = _os.read(fd, 32768)
            except (BlockingIOError, InterruptedError):
                try:
                    _wait_fd(fd, _WAIT_READ)
                except asyncio.CancelledError:
                    continue          # interest changed (pause/close): re-check
                except Exception:
                    eof = True
                    break
                continue
            except OSError:
                eof = True            # peer reset etc. -> treat as EOF
                break
            if not data:
                eof = True            # clean EOF
                break
            self._deliver(data)
            # Hand the scheduler to a woken consumer before reading again.
            runloom_c.sched_yield_classic()
        self._read_g = None
        if eof and not self._closing:
            self._eof()

    def _deliver(self, data):
        if not self._closing:
            try:
                self._protocol.data_received(data)
            except Exception as e:
                self._report(e, "data_received")

    def _eof(self):
        # A read pipe is unidirectional, so EOF is terminal -- there is nothing
        # left to read and no write side to keep half-open.  Like CPython's
        # _UnixReadPipeTransport._read_ready, call eof_received() for the
        # protocol's sake (it feeds EOF to a StreamReader) but IGNORE its return
        # and ALWAYS close: honouring a True return (StreamReaderProtocol over a
        # pipe returns True) left self._pipe open forever, so an abandoned pipe
        # transport leaked its FileIO -> a stray "unclosed file" ResourceWarning
        # at the next gc (test_streams::test_unclosed_resource_warnings counts
        # ResourceWarnings and saw 2 instead of 1).  Defer the close one turn so
        # a pending reader.read() drains the buffered data before connection_lost.
        try:
            self._protocol.eof_received()
        except Exception as e:
            self._report(e, "eof_received")
        self._loop.call_soon(self._close, None)

    def _close(self, exc):
        if self._closing:
            return
        self._closing = True
        g = self._read_g
        if g is not None:
            try:
                g.cancel_wait_fd()   # wake the parked read fiber so it exits
            except Exception:
                pass
        # Clear the netpoll arm cache for this fd before closing it: a plain
        # pipe.close() leaves the single-thread per-fd LEVEL arm sticky, so when
        # the OS reuses this fd NUMBER (the next subprocess / aio.run), the new
        # transport's wait_fd skips EPOLL_CTL_ADD and parks forever.  Safe here:
        # the read fiber was just cancelled, so nothing is validly parked on it.
        try:
            runloom_c.netpoll_release_if_idle(self._fd)
        except (OSError, ValueError, AttributeError):
            pass
        try:
            self._pipe.close()
        except Exception:
            pass
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")

    def pause_reading(self):
        self._paused = True
        g = self._read_g
        if g is not None:
            try:
                g.cancel_wait_fd()   # wake it so it observes _paused and exits
            except Exception:
                pass

    def resume_reading(self):
        if not self._paused:
            return
        self._paused = False
        if self._read_g is None and not self._closing:
            self._read_g = _fiber_io(self._read_loop)

    def close(self):
        self._close(None)

    def is_closing(self):
        return self._closing

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_extra_info(self, name, default=None):
        return self._pipe if name == "pipe" else default

    def _report(self, exc, where):
        self._loop.call_exception_handler(
            {"message": "Read pipe " + where + " raised", "exception": exc})


class _WritePipeTransport(asyncio.WriteTransport):
    """connect_write_pipe transport: a fiber drains the write buffer to the
    pipe fd via wait_fd (cooperative, no OS thread); connection_lost fires on
    close/EOF/error.  Implements the asyncio watermark flow-control contract so
    StreamWriter.drain() blocks until the backlog flushes or the pipe breaks."""

    def __repr__(self):
        # Mirror asyncio's _UnixWritePipeTransport.__repr__ (no _selector -> open).
        info = [self.__class__.__name__]
        if self._pipe is None:
            info.append("closed")
        elif self._closing:
            info.append("closing")
        info.append("fd={0}".format(self._fd))
        info.append("open" if self._pipe is not None else "closed")
        return "<{0}>".format(" ".join(info))

    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._fd = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._eof_requested = False
        self._conn_lost_fired = False
        self._buf = bytearray()
        self._high_water = 64 * 1024
        self._low_water = 16 * 1024
        self._protocol_paused = False
        self._drain_g = None
        # Only a SOCKET write-pipe gets a persistent READ park to detect the peer
        # closing its end (POLLHUP) -> connection_lost, mirroring asyncio's
        # _add_reader on the write fd.  A FIFO / anonymous pipe / PTY (subprocess
        # stdin is a FIFO) keeps its exact idle-exit behaviour -- watch_hup False.
        self._watch_hup = False
        try:
            _os.set_blocking(self._fd, False)
            self._watch_hup = _stat.S_ISSOCK(_os.fstat(self._fd).st_mode)
        except OSError:
            pass
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        if self._watch_hup and not self._closing:
            self._kick()   # keep a fiber parked to observe the peer HUP

    def _kick(self):
        # Wake/spawn the drain fiber after write()/write_eof changed the
        # buffer.  write() and the drain both run on the loop thread, so there is
        # no cross-thread queue -- just one bytearray + one fiber.
        if self._drain_g is None:
            if not self._closing:
                # Roomy stack: the drain side runs protocol.resume_writing (user
                # callback), kept consistent with the read loop / _IO_STACK.
                self._drain_g = _fiber_io(self._drain_loop)
        else:
            try:
                self._drain_g.cancel_wait_fd()   # wake it if parked on WRITE
            except Exception:
                pass

    def _drain_loop(self):
        fd = self._fd
        while True:
            # Drain everything currently buffered.
            while self._buf:
                chunk = bytes(self._buf[:262144])
                try:
                    n = _os.write(fd, chunk)
                except (BlockingIOError, InterruptedError):
                    break                        # not writable now; park below
                except OSError as e:             # BrokenPipe etc.
                    self._drain_g = None
                    self._finish(e)
                    return
                if n:
                    del self._buf[:n]
                    self._maybe_resume()
            if not self._buf and self._eof_requested:
                self._drain_g = None
                self._finish(None)
                return
            # Park on the union of WRITE (bytes pending) and READ (HUP watch for a
            # socket write-pipe).  For a non-socket pipe watch_hup is False, so the
            # READ bit is never set and this behaves exactly like the old loop:
            # park on WRITE while buffered, else exit idle.
            mask = (_WAIT_WRITE if self._buf else 0) \
                 | (_WAIT_READ if (self._watch_hup and not self._closing) else 0)
            if mask == 0:
                self._drain_g = None
                return                           # idle; respawn on next write()
            try:
                ready = _wait_fd(fd, mask)
            except asyncio.CancelledError:
                continue                         # new write()/close: re-check
            except Exception as e:
                self._drain_g = None
                self._finish(e)
                return
            if (ready & _WAIT_READ) and self._watch_hup:
                # Peer closed its read end: pending bytes -> broken pipe, else a
                # clean disconnect.  (Like asyncio's _read_ready, we treat any
                # read-readiness on the write fd as the peer going away.)
                self._drain_g = None
                self._finish(BrokenPipeError() if self._buf else None)
                return

    def _maybe_resume(self):
        if self._protocol_paused and len(self._buf) <= self._low_water:
            self._protocol_paused = False
            try:
                self._protocol.resume_writing()
            except Exception as e:
                self._report(e, "resume_writing")

    def _finish(self, exc):
        # Clear the netpoll arm cache before close (see _ReadPipeTransport._close)
        # so a reused fd number re-registers cleanly instead of skipping the
        # EPOLL_CTL_ADD on the stale arm and parking forever.
        try:
            runloom_c.netpoll_release_if_idle(self._fd)
        except (OSError, ValueError, AttributeError):
            pass
        try:
            self._pipe.close()
        except Exception:
            pass
        if self._conn_lost_fired:
            return
        self._conn_lost_fired = True
        self._closing = True
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")

    def write(self, data):
        if self._closing or self._eof_requested:
            return
        data = bytes(data)
        if not data:
            return
        if not self._buf:
            # Eager synchronous write when nothing is queued (mirrors stock
            # unix_events._UnixWritePipeTransport.write).  On this single-thread
            # loop a subsequent BLOCKING os.read on the pipe's read end (as the
            # stdlib pipe/PTY tests do) would otherwise self-deadlock: the drain
            # fiber can't run to write the byte until the read returns.  Safe re
            # ordering -- the buffer is empty, so no drain fiber holds pending
            # bytes; a real EPIPE is swallowed to n=0 and re-surfaced by the drain
            # fiber's OSError->_finish path (not fired reentrantly inside write()).
            try:
                n = _os.write(self._fd, data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except OSError:
                n = 0
            if n >= len(data):
                return
            if n:
                data = data[n:]
        self._buf += data
        if (not self._protocol_paused) and len(self._buf) > self._high_water:
            self._protocol_paused = True
            try:
                self._protocol.pause_writing()
            except Exception as e:
                self._report(e, "pause_writing")
        self._kick()

    def writelines(self, list_of_data):
        self.write(b"".join(list_of_data))

    def get_write_buffer_size(self):
        return len(self._buf)

    def get_write_buffer_limits(self):
        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            high = 64 * 1024 if low is None else 4 * low
        if low is None:
            low = high // 4
        self._high_water = high
        self._low_water = low

    def write_eof(self):
        if self._eof_requested:
            return
        self._eof_requested = True
        # Drain whatever is queued, then finish.  If nothing is queued and no
        # drain fiber is running, finish inline now.
        if self._drain_g is None and not self._buf:
            self._finish(None)
        else:
            self._kick()

    def can_write_eof(self):
        return True

    def close(self):
        self.write_eof()

    def abort(self):
        self.write_eof()

    def is_closing(self):
        return self._closing or self._eof_requested

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_extra_info(self, name, default=None):
        return self._pipe if name == "pipe" else default

    def _report(self, exc, where):
        self._loop.call_exception_handler(
            {"message": "Write pipe " + where + " raised", "exception": exc})
