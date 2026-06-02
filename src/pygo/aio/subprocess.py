"""Subprocess pipe protocols + _SubprocessTransport."""
from ._base import *  # noqa: F401,F403  (shared foundation)

class _WriteSubprocessPipeProto(asyncio.BaseProtocol):
    """Bridge protocol for a child's stdin pipe.  connect_write_pipe drives the
    real _WritePipeTransport; this forwards its lifecycle to the owning
    _SubprocessTransport (mirrors CPython asyncio.base_subprocess.
    WriteSubprocessPipeProto)."""
    def __init__(self, proc, fd):
        self.proc = proc
        self.fd = fd
        self.pipe = None
        self.disconnected = False

    def connection_made(self, transport):
        self.pipe = transport

    def connection_lost(self, exc):
        self.disconnected = True
        self.proc._pipe_connection_lost(self.fd, exc)
        self.proc = None

    def pause_writing(self):
        self.proc._protocol.pause_writing()

    def resume_writing(self):
        self.proc._protocol.resume_writing()


class _ReadSubprocessPipeProto(_WriteSubprocessPipeProto, asyncio.Protocol):
    """Bridge protocol for a child's stdout/stderr pipe; adds data forwarding."""
    def data_received(self, data):
        self.proc._pipe_data_received(self.fd, data)


class _SubprocessTransport(asyncio.SubprocessTransport):
    """Subprocess transport that, like CPython's BaseSubprocessTransport, builds
    its per-fd pipe transports by AWAITING loop.connect_write_pipe /
    connect_read_pipe (with the bridge protocols above) rather than constructing
    them inline.  Routing through those loop methods is what makes start-time
    cancellation propagate and lets flow-control / pause_reading be observed on
    the returned pipe transport (test_subprocess's pipe-cancel + pause_reading
    tests mock exactly those loop methods)."""
    def __init__(self, loop, protocol, args, *, shell,
                 stdin, stdout, stderr, **kwargs):
        self._loop = loop
        self._protocol = protocol
        self._closed = False
        self._finished = False
        self._returncode = None
        self._exit_waiters = []
        self._pipes = {}            # fd -> bridge protocol (.pipe = transport)
        self._pipes_connected = False
        self._extra = {}
        # _pending_calls holds pipe_data_received / process_exited that land
        # before the protocol's connection_made has run; flushed by _connect_pipes.
        self._pending_calls = []
        # bufsize=0: unbuffered, so reads see data promptly and our stdin writer
        # controls flushing.  Spawn the child, then reap it on a wait thread.
        self._proc = _subprocess.Popen(
            args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr,
            bufsize=0, **kwargs)
        self._pid = self._proc.pid
        self._extra["subprocess"] = self._proc
        # Placeholder ONLY for fds that actually have a pipe: Popen leaves
        # proc.std* None for DEVNULL / an inherited fd / a passed file, and those
        # must NOT seed a _pipes entry -- _connect_pipes connects exactly the same
        # set (gated on proc.std* is not None), so a None placeholder here would
        # never be filled and _try_finish's all-disconnected gate (hence wait())
        # would hang (test_devnull_input).
        if self._proc.stdin is not None:
            self._pipes[0] = None
        if self._proc.stdout is not None:
            self._pipes[1] = None
        if self._proc.stderr is not None:
            self._pipes[2] = None
        self._start_reaper()

    def _start_reaper(self):
        # Reap the child COOPERATIVELY via its pidfd (Linux 5.3+ / py3.9+): a
        # goroutine parks on the pidfd -- which becomes readable exactly when the
        # child exits -- instead of burning a dedicated OS thread blocked in
        # Popen.wait().  This is the asyncio-bridge equivalent of pygo.monkey's
        # cooperative os.waitpid.  Falls back to the wait thread where pidfd_open
        # is missing or fails (older kernels, non-Linux).
        pidfd = None
        opener = getattr(_os, "pidfd_open", None)
        if opener is not None:
            try:
                pidfd = opener(self._pid)
            except OSError:
                pidfd = None
        if pidfd is None:
            _threading.Thread(target=self._wait_thread,
                              name="pygo-subproc-wait", daemon=True).start()
            return

        def reaper():
            try:
                _wait_fd(pidfd, _WAIT_READ)   # readable once the child exits
            except BaseException:
                pass
            finally:
                try:
                    _os.close(pidfd)
                except OSError:
                    pass
            # The child has exited: Popen.wait() reaps it immediately (no block),
            # and we are on the loop thread (this goroutine), so deliver inline.
            try:
                rc = self._proc.wait()
            except Exception:
                rc = self._proc.poll()
                if rc is None:
                    rc = -1
            self._process_exited(rc)

        pygo_core.go(reaper)

    async def _connect_pipes(self):
        # Mirror asyncio.base_subprocess._connect_pipes: await the loop's pipe
        # connectors so a cancellation there (or any failure) propagates to the
        # create_subprocess_* caller, then fire connection_made and flush the
        # callbacks that arrived while connecting.
        loop = self._loop
        proc = self._proc
        if proc.stdin is not None:
            _, proto = await loop.connect_write_pipe(
                lambda: _WriteSubprocessPipeProto(self, 0), proc.stdin)
            self._pipes[0] = proto
        if proc.stdout is not None:
            _, proto = await loop.connect_read_pipe(
                lambda: _ReadSubprocessPipeProto(self, 1), proc.stdout)
            self._pipes[1] = proto
        if proc.stderr is not None:
            _, proto = await loop.connect_read_pipe(
                lambda: _ReadSubprocessPipeProto(self, 2), proc.stderr)
            self._pipes[2] = proto
        # connection_made must run BEFORE create_subprocess_*'s Process.__init__
        # reads protocol.stdout/stderr (SubprocessStreamProtocol sets those in
        # connection_made).  Stock asyncio relies on a waiter + FIFO call_soon to
        # order it; pygo awaits _connect_pipes directly and it completes without
        # suspending (connect_*_pipe never awaits the loop), so a call_soon here
        # would run AFTER Process.__init__ -> stdout=None -> stdin never closed ->
        # deadlock.  Call it inline instead; pipe_data_received/process_exited
        # that arrived mid-connect are still deferred (call_soon) so they land
        # after connection_made.
        try:
            self._protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        for cb, data in self._pending_calls:
            loop.call_soon(cb, *data)
        self._pending_calls = None
        self._pipes_connected = True
        self._try_finish()

    def _call(self, cb, *data):
        # Before connection_made: queue; after: dispatch on the loop.
        if self._pending_calls is not None:
            self._pending_calls.append((cb, data))
        else:
            self._loop.call_soon(cb, *data)

    def _pipe_data_received(self, fd, data):
        self._call(self._protocol.pipe_data_received, fd, data)

    def _pipe_connection_lost(self, fd, exc):
        self._call(self._protocol.pipe_connection_lost, fd, exc)
        self._try_finish()

    def _wait_thread(self):
        rc = self._proc.wait()
        self._loop.call_soon_threadsafe(self._process_exited, rc)

    def _process_exited(self, rc):
        if self._returncode is not None:
            return
        self._returncode = rc
        self._call(self._protocol.process_exited)
        # The child is gone, so its stdin read end is closed.  Stock asyncio sees
        # that as POLLHUP and disconnects the write pipe; pygo's stdin transport
        # is a blocking-thread _WritePipeTransport that can't observe the hangup
        # on its own, so close it here -- otherwise it never disconnects and
        # _try_finish's all-pipes-disconnected gate (hence wait()) hangs for any
        # process whose stdin was left open (e.g. one just awaiting exit).
        # Output pipes are left alone: they EOF naturally and may still hold
        # buffered data to deliver.
        stdin = self._pipes.get(0)
        if stdin is not None and stdin.pipe is not None and not stdin.disconnected:
            try:
                stdin.pipe.close()
            except Exception:
                pass
        self._try_finish()

    def _try_finish(self):
        # connection_lost fires once the process has exited AND every connected
        # pipe has disconnected -- mirror asyncio.base_subprocess._try_finish.
        if self._returncode is None or self._finished:
            return
        if not self._pipes_connected:
            # _connect_pipes never completed (failed / cancelled): wake wait()ers
            # so they don't hang, but don't deliver connection_made/lost.
            for fut in self._exit_waiters:
                if not fut.done():
                    fut.set_result(self._returncode)
            self._exit_waiters = []
            return
        if all(p is not None and p.disconnected for p in self._pipes.values()):
            self._finished = True
            self._closed = True
            self._call(self._call_connection_lost, None)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")
        finally:
            for fut in self._exit_waiters:
                if not fut.done():
                    fut.set_result(self._returncode)
            self._exit_waiters = []

    # ---- asyncio.SubprocessTransport interface ----
    def get_pid(self):
        return self._pid

    def get_returncode(self):
        return self._returncode

    def get_pipe_transport(self, fd):
        proto = self._pipes.get(fd)
        return proto.pipe if proto is not None else None

    def _wait(self):
        # asyncio.subprocess.Process.wait() awaits this.
        fut = self._loop.create_future()
        if self._returncode is not None:
            fut.set_result(self._returncode)
        else:
            self._exit_waiters.append(fut)
        return fut

    def send_signal(self, signal):
        self._proc.send_signal(signal)

    def terminate(self):
        self._proc.terminate()

    def kill(self):
        self._proc.kill()

    def is_closing(self):
        return self._closed

    def close(self):
        # Best-effort: close every connected pipe transport, then kill a child
        # that is genuinely still running.  Only kill if poll() confirms it is
        # alive -- self._returncode is set ASYNCHRONOUSLY by the wait thread, so
        # a child that already exited may not have been notified yet, and close()
        # must not kill a finished process (test_close_dont_kill_finished).
        if self._closed:
            return
        self._closed = True
        for proto in self._pipes.values():
            if proto is not None and proto.pipe is not None:
                try:
                    proto.pipe.close()
                except Exception:
                    pass
        if self._returncode is None and self._proc.poll() is None:
            try:
                self._proc.kill()
            except (ProcessLookupError, OSError):
                pass

    def get_protocol(self):
        return self._protocol

    def get_extra_info(self, name, default=None):
        return self._extra.get(name, default)

    def _report(self, exc, where):
        self._loop.call_exception_handler({
            "message": "Subprocess " + where + " raised",
            "exception": exc,
        })
