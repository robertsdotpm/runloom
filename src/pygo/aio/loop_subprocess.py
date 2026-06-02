"""PygoEventLoop: subprocess_exec/shell, connect_read/write_pipe, and the
executor (run_in_executor)."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .futures import PygoFuture  # noqa: F401
from .handles import _pg_convert_future_exc  # noqa: F401
from .pipes import _ReadPipeTransport, _WritePipeTransport  # noqa: F401
from .subprocess import _SubprocessTransport  # noqa: F401

class _LoopSubprocessMixin(object):
    async def _make_subprocess(self, protocol, args, *, shell,
                               stdin, stdout, stderr, **kwargs):
        # Spawn the child, then connect its pipes by AWAITING connect_*_pipe.  If
        # that connect raises (e.g. cancellation) the child is already running,
        # so kill + reap it before propagating -- mirror asyncio's
        # _make_subprocess_transport so create_subprocess_* never leaks a child.
        transport = _SubprocessTransport(
            self, protocol, args, shell=shell,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        try:
            await transport._connect_pipes()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        return transport

    async def subprocess_exec(self, protocol_factory, program, *args,
                              stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                              stderr=_subprocess.PIPE, **kwargs):
        _reject_subprocess_text_mode(kwargs)
        protocol = protocol_factory()
        transport = await self._make_subprocess(
            protocol, [program] + list(args), shell=False,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        return transport, protocol

    async def subprocess_shell(self, protocol_factory, cmd,
                               stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                               stderr=_subprocess.PIPE, **kwargs):
        _reject_subprocess_text_mode(kwargs)
        protocol = protocol_factory()
        transport = await self._make_subprocess(
            protocol, cmd, shell=True,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        return transport, protocol

    # ---- pipe transports (thread-backed, like subprocess) ----
    # connect_read_pipe / connect_write_pipe wrap an arbitrary readable/writable
    # pipe or file object in a transport driving a standard Protocol.  Used by
    # aioconsole + libs doing async stdio.  Same thread-bridge as subprocess.
    async def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        transport = _ReadPipeTransport(self, pipe, protocol)
        return transport, protocol

    async def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        transport = _WritePipeTransport(self, pipe, protocol)
        return transport, protocol


    def run_in_executor(self, executor, func, *args):
        """Run func(*args) on a thread pool.  Returns a PygoFuture
        that resolves when the thread completes.  We hand out a real
        threadpool via concurrent.futures."""
        import concurrent.futures as _cf
        if executor is None:
            # Lazy-init default pool.
            if self._default_executor is None:
                self._default_executor = _cf.ThreadPoolExecutor(max_workers=8)
            executor = self._default_executor
        fut = PygoFuture(loop=self)
        cf_fut = executor.submit(func, *args)
        def _on_thread_done(_cf_fut):
            # Marshal the thread's result back into our PygoFuture.
            # call_soon_threadsafe wakes the loop.
            def _set():
                if cf_fut.cancelled():
                    fut.cancel()
                elif cf_fut.exception() is not None:
                    # A concurrent.futures exception crossing into asyncio-land
                    # must become its asyncio twin: concurrent.futures.Cancelled
                    # Error subclasses Exception, but asyncio.CancelledError
                    # subclasses BaseException -- distinct classes, so the raw
                    # concurrent kind slips past `except asyncio.CancelledError`.
                    # Stock wrap_future/_chain_future runs this same conversion.
                    fut.set_exception(_pg_convert_future_exc(cf_fut.exception()))
                else:
                    fut.set_result(cf_fut.result())
            try:
                self.call_soon_threadsafe(_set)
            except RuntimeError:
                # Loop closed before the pool thread finished -- nothing to
                # resolve into; drop the result (matches stock asyncio, whose
                # wrap_future done-callback no-ops once the loop is closed).
                pass
        cf_fut.add_done_callback(_on_thread_done)
        return fut

    def set_default_executor(self, executor):
        """asyncio.AbstractEventLoop.set_default_executor.  Used by
        run_in_executor(None, ...).  Libraries (aiomisc) inject their own
        thread pool through this; the base class raises NotImplementedError."""
        self._default_executor = executor

    # ---- Unix signals (loop.add_signal_handler) ----
    # The base class raises NotImplementedError; servers (uvicorn, hypercorn,
    # aiohttp) install SIGINT/SIGTERM handlers for graceful shutdown, so without
    # this they can't run under pygo.  signal.signal must be called from the
    # main thread (asyncio has the same constraint); the handler itself runs on
    # the main thread, and we marshal the user callback onto the loop thread via
    # call_soon_threadsafe so it runs cooperatively like asyncio's wakeup-fd path.
