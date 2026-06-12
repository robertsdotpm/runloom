"""_StreamTransport: the recv/send I/O loop (read path + write drain)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class _StreamIOMixin(object):
    def _run_cb(self, fn, *args):
        # Run a protocol callback inside this connection's contextvars Context
        # (so contextvars set before the connection -- e.g. uvicorn's request
        # context -- reach any task it spawns).  A Context cannot be entered
        # re-entrantly, and our callbacks fire synchronously: data_received may
        # call transport.close() (-> connection_lost) while still inside its own
        # _context.run.  Stock asyncio sidesteps this by scheduling each
        # callback in its own loop iteration; we instead detect the nested case
        # and call directly -- we are already executing inside self._context, so
        # the contextvars are identical.  Goroutines are cooperative and these
        # callbacks never await, so the flag needs no lock.
        if self._in_context:
            return fn(*args)
        self._in_context = True
        try:
            return self._context.run(fn, *args)
        finally:
            self._in_context = False

    def _io_loop(self):
        # The ONE fiber for this fd.  Parks on the union of the directions
        # we currently need (READ unless paused / read-EOF'd, WRITE while the
        # write buffer is non-empty) and services whichever is ready.  Merging
        # recv and drain into one fiber is mandatory on runloom's netpoll: a
        # second fiber parking the other direction on the same fd clobbers
        # this one's arm (one-shot per fd) and strands it.
        sock = self._sock
        while not self._stopping:
            # TLS bidirectional half-close: once the peer's close_notify has
            # been read (read EOF) AND our write buffer is fully drained, answer
            # with OUR close_notify -- the asyncio/sslproto behaviour.  Without
            # it a peer doing a clean ssl.SSLSocket.unwrap() blocks forever
            # waiting for our close_notify (test_remote_shutdown trailing-data:
            # its server reads our data, then ends its read loop only on our
            # close_notify).  Fire once, here in the io fiber (send may park).
            if (self._read_eof and not self._write_buf and not self._closed
                    and not self._sock_is_plain and not self._tls_shutdown_sent):
                self._send_tls_close_notify()
            mask = 0
            if not self._paused and not self._read_eof and not self._closed:
                mask |= 1
            if self._write_buf and not self._write_paused:
                mask |= 2
            if mask == 0:
                # Paused/EOF'd for reading and nothing queued to write: nobody
                # needs the fd right now.  Exit; resume_reading()/write() will
                # respawn us via _kick_io.  (Incoming bytes wait in the kernel
                # buffer = correct read backpressure.)
                self._io_g = None
                return
            if (mask & 1) and not self._sock_is_plain:
                # TLS read-ahead: the SSL layer may hold decrypted bytes OR whole
                # undecrypted records buffered (read together with the handshake
                # flight / a prior record).  The socket isn't readable for that,
                # so _wait_fd(READ) would never fire -- drain it before parking.
                # Stop draining when pending() stops dropping (a partial record
                # that needs more socket bytes).
                drained = False
                while not self._stopping:
                    try:
                        before = self._sock.pending()
                    except Exception:
                        before = 0
                    if not before:
                        break
                    if not self._recv_step():
                        return
                    drained = True
                    try:
                        if self._sock.pending() >= before:
                            break          # no progress: partial record, park
                    except Exception:
                        break
                if drained:
                    runloom_c.sched_yield_classic()
                    # RE-LOOP, don't fall through: _recv_step delivered data,
                    # which can wake a peer coroutine that queues writes during
                    # the yield above (a streaming write loop, or a test's
                    # _test__append_write_backlog).  `mask` was computed at the
                    # top of THIS iteration -- stale now -- so parking on it
                    # would park READ-only and strand the new write buffer.
                    # Re-evaluating the mask picks up WRITE.
                    continue
            try:
                fd = sock.fileno()
            except Exception:
                self._io_g = None
                return
            try:
                ready = _wait_fd(fd, mask)
            except asyncio.CancelledError:
                # Interest changed via _kick_io (write()/resume_reading()/
                # close()): re-loop to recompute the mask (or exit on _stopping).
                continue
            except Exception:
                if self._stopping:
                    return
                continue
            if self._stopping:
                return
            # Drain queued writes first so output flushes promptly, then read.
            # Re-check flags between steps: a drain error or a data_received
            # callback may close() the transport.
            if (ready & 2) and self._write_buf and not self._stopping:
                self._drain_step()
            if (ready & 1) and not self._paused and not self._read_eof \
                    and not self._stopping:
                if not self._recv_step():
                    return
            # Hand the scheduler to any fiber a data_received just woke (a
            # protocol coroutine awaiting this read) BEFORE we recv() again.
            # Without this yield the loop can drain the whole response AND the
            # EOF/close in one burst, firing connection_lost (-> protocol state
            # CLOSED) before the woken coro ran its post-read step -- breaking
            # ordering-sensitive protocols (e.g. websockets' client handshake
            # asserts CONNECTING in connection_open()).
            runloom_c.sched_yield_classic()
        self._io_g = None

    def _recv_step(self):
        # One NON-BLOCKING recv + dispatch.  Returns True to keep looping, False
        # to stop the io fiber (transport closed).  Must not park: a
        # plaintext socket.recv raises BlockingIOError when dry; a _TLSSock's
        # parking recv() would stall the write drain on this same fiber, so
        # use its single-shot recv_nb instead.
        #
        # BufferedProtocol path: ask the protocol for a buffer and recv straight
        # into it (get_buffer -> recv_into -> buffer_updated), asyncio's
        # zero-copy read contract, instead of recv() -> data_received().  Checked
        # dynamically so a set_protocol() swap is always honoured.
        proto = self._protocol
        if isinstance(proto, asyncio.BufferedProtocol):
            return self._recv_step_buffered(proto)
        sock = self._sock
        try:
            recv_nb = getattr(sock, "recv_nb", None)
            data = recv_nb(65536) if recv_nb is not None else sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return True            # nothing ready / spurious readiness; re-park
        except OSError as e:
            if self._stopping: return False
            if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                return True
            # Route through close() so connection_lost(e) fires exactly once
            # (the guard) rather than racing close()'s own call.
            self.close(e)
            return False
        if not data:
            return self._handle_read_eof()
        try:
            self._run_cb(self._protocol.data_received, data)
        except Exception as e:
            # asyncio treats an exception out of data_received() as fatal: close
            # the transport and deliver connection_lost(exc).  Without this a
            # protocol that faults mid-read never gets connection_lost, so any
            # await on closure (websockets recv() -> shield(connection_lost_
            # waiter)) hangs forever.  close()'s guard keeps it single-fire.
            self._report(e, "data_received")
            self.close(e)
            return False
        return True

    def _handle_read_eof(self):
        # EOF: peer half-closed its write side; recv() now returns b'' forever,
        # so stop READING (a `continue` here would busy-spin at 100% CPU).  Keep
        # the transport (and this fiber, for our own writes) only if the
        # protocol asked (eof_received() -> True).  Shared by the data_received
        # and BufferedProtocol read paths.
        try:
            keep = self._run_cb(self._protocol.eof_received)
        except Exception as e:
            self._report(e, "eof_received")
            keep = False
        if not keep:
            self.close()
            # A TLS half-close: the peer sent close_notify (our read side is now
            # EOF) but we may still owe it queued output -- the classic
            # remote-shutdown-with-trailing-data case
            # (test_remote_shutdown_receives_trailing_data, where the peer reads
            # 4MB AFTER sending close_notify).  close() deferred teardown to
            # flush that backlog (_close_when_drained); KEEP this io fiber
            # alive to drain it -- _closed now masks READ off so we only pump
            # WRITE, and _drain_step fires the teardown (and our close_notify)
            # once empty.  With nothing queued, close() tore down already ->
            # stop the fiber.
            return self._close_when_drained
        self._read_eof = True   # mask drops READ; loop stays for writes
        return True

    def _recv_step_buffered(self, proto):
        # BufferedProtocol read: get_buffer(-1) -> recv_into(buf) ->
        # buffer_updated(nbytes).  Mirrors asyncio _SelectorSocketTransport's
        # _read_ready__get_buffer.  Must not park (same constraint as
        # _recv_step): plain sockets recv_into non-blocking and zero-copy; a
        # _TLSSock has no non-blocking recv_into, so use its single-shot recv_nb
        # and copy into the protocol's buffer.
        sock = self._sock
        try:
            buf = self._run_cb(proto.get_buffer, -1)
            if not len(buf):
                raise RuntimeError("get_buffer() returned an empty buffer")
        except Exception as e:
            self._report(e, "get_buffer")
            self.close(e)
            return False
        try:
            recv_nb = getattr(sock, "recv_nb", None)
            if recv_nb is None:
                nbytes = sock.recv_into(buf)          # plain: zero-copy
            else:
                data = recv_nb(len(memoryview(buf)))  # TLS: single-shot + copy
                if data:
                    memoryview(buf)[:len(data)] = data
                nbytes = len(data)
        except (BlockingIOError, InterruptedError):
            return True
        except OSError as e:
            if self._stopping:
                return False
            if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                return True
            self.close(e)
            return False
        if not nbytes:
            return self._handle_read_eof()
        try:
            self._run_cb(proto.buffer_updated, nbytes)
        except Exception as e:
            self._report(e, "buffer_updated")
            self.close(e)
            return False
        return True

    def _drain_step(self):
        # Send as much of the write buffer as the socket accepts now.  Snapshot
        # a bounded chunk: a _TLSSock send() can park (releasing its CoLock), so
        # a concurrent write() may append meanwhile -- snapshotting keeps the
        # bytes we send stable, and del[:n] still removes exactly the consumed
        # prefix (appends stay at the tail for the next pass).
        sock = self._sock
        chunk = bytes(self._write_buf[:262144])
        try:
            n = sock.send(chunk)
        except (BlockingIOError, InterruptedError):
            return                 # not actually writable; re-park
        except OSError as e:
            # Peer dropped mid-drain.  If we were draining for a graceful close,
            # close() already ran (so it'd early-return) -- fire the teardown
            # directly; otherwise route through close(e).
            if self._close_when_drained:
                self._close_when_drained = False
                self._stopping = True
                self._deliver_connection_lost(e, self._close_deliver_cl)
            else:
                self.close(e)
            return
        if n:
            del self._write_buf[:n]
            self._maybe_resume_writing()
        if not self._write_buf:
            if self._eof_pending and not self._eof_written and not self._closed:
                # Buffer drained: honour the deferred write_eof half-close.
                self._eof_pending = False
                self._eof_written = True
                try:
                    sock.shutdown(_socket.SHUT_WR)
                except OSError:
                    pass
            if self._close_when_drained and not self._stopping:
                # Graceful close's queued output is flushed: send our TLS
                # close_notify (so a peer in unwrap() completes), then tear down.
                if self._close_exc is None:
                    self._send_tls_close_notify()
                self._close_when_drained = False
                self._stopping = True
                self._deliver_connection_lost(self._close_exc,
                                              self._close_deliver_cl)

    def _kick_io(self):
        # Re-evaluate the io fiber's interest mask after write()/resume_
        # reading() changed it: wake it if parked, or respawn it if it had
        # exited (mask was 0).
        if self._stopping:
            return
        g = self._io_g
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        else:
            self._io_g = _go_io(self._io_loop)

    def _maybe_pause_writing(self):
        if not self._protocol_paused and len(self._write_buf) > self._high_water:
            self._protocol_paused = True
            try:
                self._run_cb(self._protocol.pause_writing)
            except Exception as e:
                self._report(e, "pause_writing")

    def _maybe_resume_writing(self):
        if self._protocol_paused and len(self._write_buf) <= self._low_water:
            self._protocol_paused = False
            try:
                self._run_cb(self._protocol.resume_writing)
            except Exception as e:
                self._report(e, "resume_writing")

    def write(self, data):
        if self._eof_written or self._eof_pending:
            # Mirror stock asyncio's selector transport so callers (e.g.
            # websockets' broadcast) see the failure they expect, with the
            # same message they assert on.
            raise RuntimeError("Cannot call write() after write_eof()")
        if self._closed:
            return
        if not data:
            return
        if not self._write_buf and self._sock_is_plain:
            # Fast path: nothing queued and a plaintext non-blocking socket whose
            # send() never parks -- send straight from the caller's fiber.
            # (A _TLSSock send() can park EPOLLOUT and clobber the recv arm, so
            # TLS skips this and always buffers + lets the io fiber drain.)
            try:
                n = self._sock.send(data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except OSError as e:
                # close() delivers connection_lost(e) exactly once -- calling it
                # here too double-fires it (websockets' connection_lost sets a
                # one-shot Future -> InvalidStateError "Future already done").
                self.close(e)
                return
            if n >= len(data):
                return                          # fully sent, no buffer needed
            data = memoryview(data)[n:]         # buffer the remainder, in order
        # Queue (preserving order) and hand off to the single io fiber,
        # which drains EPOLLOUT on the SAME union-mask park as the recv side.
        self._write_buf += bytes(data)
        self._maybe_pause_writing()
        self._kick_io()

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def _send_tls_close_notify(self):
        # Graceful TLS close: emit OUR close_notify so a peer doing a clean
        # ssl.SSLSocket.unwrap() -- which sends its close_notify then BLOCKS
        # for ours -- completes instead of seeing a bare TCP FIN (which it
        # surfaces as SSLEOFError UNEXPECTED_EOF_WHILE_READING and treats as a
        # protocol violation).  asyncio's SSLProtocol sends close_notify on
        # close whether or not the peer sent theirs first; runloom's stream-EOF
        # path (_handle_read_eof -> eof_received()==False -> close()) used to
        # close the raw socket with no close_notify, so a server doing the
        # symmetric unwrap() (test_ssl::test_shutdown_cleanly) aborted -- and
        # under its sequential threaded server that abort cascaded a FIN to
        # every still-handshaking client.  Idempotent (the io-loop half-close
        # block shares _tls_shutdown_sent) and a no-op on plaintext sockets.
        if self._sock_is_plain or self._tls_shutdown_sent:
            return
        snc = getattr(self._sock, "send_close_notify", None)
        if snc is None:
            return
        self._tls_shutdown_sent = True
        snc()
