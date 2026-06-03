"""pygo.aio -- async/await on the pygo scheduler.

Approach: each asyncio.Task gets its own pygo goroutine.  The goroutine
drives `coro.send()` itself; when the coro yields a pending Future,
the goroutine parks via a 1-buffered channel and resumes when the
Future's done_callback fires.  Cooperative switching between tasks is
a stack swap (~80 ns).

Measured perf characteristics (Python 3.12 on Linux, see
bench/bench_aio_io.py):
  * Multi-await chains (n=100 k=100 awaits each): ~1.9x faster
  * Deep recursive awaits (n=100 d=20): ~1.7x faster
  * Simple fan-out (10k tasks one sleep each): ~5x SLOWER

The wins come from amortizing PygoTask setup cost across many awaits.
The losses come from PygoTask creation + Chan alloc being heavier
than asyncio's tight C-deque dispatcher for one-await fan-outs.

For workloads dominated by per-task setup (asyncio-style microservice
request handlers), stick with vanilla asyncio.  For workloads with
significant per-task work (multi-await pipelines, recursive coroutine
trees, mixed monkey-patched sync I/O), the bridge wins.

The much-larger speedup our architecture allows (3-10x) requires
bypassing the asyncio.Future protocol entirely -- a separate project.

Compatibility:
  * asyncio.Future, asyncio.gather, asyncio.wait_for, asyncio.shield: work.
  * asyncio.sleep, asyncio.Lock, asyncio.Event, asyncio.Queue: work.
  * loop.add_reader / add_writer: work (level-triggered like asyncio's
    default selector loop, just driven by pygo's netpoll).
  * asyncio.start_server / open_connection (Transport+Protocol stack):
    NOT in this MVP -- for I/O, prefer `pygo.monkey.patch()` and write
    blocking-style socket code inside an `async def`.  Stack-switching
    means it just works.

Use:
    import pygo.aio as aio
    aio.install()                        # one-shot policy install
    asyncio.run(main())                  # routed through pygo

    # or directly:
    import pygo.aio as aio
    aio.run(main())                      # equivalent of asyncio.run

A user can also opt into the bridge per-call:
    loop = aio.PygoEventLoop()
    loop.run_until_complete(main())
"""

from ._base import *  # noqa: F401,F403  (asyncio + shared foundation, re-exported)

# Public API surface (kept importable as pygo.aio.<name>).
from .loop import PygoEventLoop
from .tasks import PygoTask
from .futures import PygoFuture
from .streams import StreamReader, StreamWriter
from .streams_api import open_connection, start_server
from .transport_datagram import DatagramTransport

# Section modules, so any internal name read as pygo.aio.<name> resolves live
# through __getattr__ below (this module used to be one flat file).
from . import (_base, tls_sock, tls_bio, tls_wrap, handles, futures, tasks,
               loop_core, loop_schedule, loop_io, loop_net, loop_subprocess,
               loop_signals, loop_run, loop, streams, streams_api,
               transport_stream_io, transport_stream, transport_server,
               transport_datagram, subprocess, pipes)

_SECTIONS = (_base, tls_sock, tls_bio, tls_wrap, handles, futures, tasks,
             loop_core, loop_schedule, loop_io, loop_net, loop_subprocess,
             loop_signals, loop_run, loop, streams, streams_api,
             transport_stream_io, transport_stream, transport_server,
             transport_datagram, subprocess, pipes)


def __getattr__(name):
    """Resolve a section-internal name (pygo.aio._wait_fd, _PG_ALL_TASKS, ...)
    live against the submodules -- preserving the old flat module's read
    surface.  PEP 562 function form on purpose; see pygo.monkey for why a
    __class__-swapped module subclass crashes inside a goroutine."""
    for section in _SECTIONS:
        try:
            return getattr(section, name)
        except AttributeError:
            continue
    raise AttributeError("module %r has no attribute %r" % (__name__, name))


# ====================================================================
# Policy + convenience entry points (install / run)
# ====================================================================
class PygoEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    def __init__(self):
        self._loop = None
        self._set_called = False
        self._child_watcher = None

    def get_event_loop(self):
        # Mirror CPython's BaseDefaultEventLoopPolicy exactly: lazily create a
        # loop ONLY on the main thread and ONLY if set_event_loop() was never
        # called; once a loop has been explicitly set (even to None, as the
        # test suites do via test_utils.set_event_loop -> set_event_loop(None)
        # to force loops to be passed explicitly), or off the main thread, a
        # missing loop is an ERROR -- raise instead of silently fabricating one.
        # The old unconditional auto-create masked that contract and broke
        # test_streams::test_streamreader*_constructor_without_loop.
        if (self._loop is None
                and not self._set_called
                and _threading.current_thread() is _threading.main_thread()):
            stacklevel = 2
            try:
                f = sys._getframe(1)
            except AttributeError:
                pass
            else:
                while f:
                    module = f.f_globals.get("__name__")
                    if module == "asyncio" or (
                            module and module.startswith("asyncio.")):
                        f = f.f_back
                        stacklevel += 1
                    else:
                        break
            _warnings.warn(
                "There is no current event loop",
                DeprecationWarning, stacklevel=stacklevel)
            self.set_event_loop(self.new_event_loop())
        if self._loop is None:
            raise RuntimeError(
                "There is no current event loop in thread %r."
                % _threading.current_thread().name)
        return self._loop

    def set_event_loop(self, loop):
        self._set_called = True
        self._loop = loop

    def new_event_loop(self):
        return PygoEventLoop()

    # Child-watcher accessors (deprecated asyncio API still asked for on Unix).
    # pygo drives subprocesses with its own per-process _wait_thread, NOT an
    # asyncio child watcher, so any watcher set here is INERT -- pygo never
    # calls add_child_handler on it.  But we must still store and hand back the
    # exact object set, or callers that do the set/get/attach_loop(None)/close
    # lifecycle (e.g. CPython's test_subprocess watcher mixins) crash on a None.
    def get_child_watcher(self):
        _warnings._deprecated(
            "get_child_watcher",
            "{name!r} is deprecated as of Python 3.12 and will be "
            "removed in Python {remove}.", remove=(3, 14))
        return self._child_watcher

    def set_child_watcher(self, watcher):
        self._child_watcher = watcher
        _warnings._deprecated(
            "set_child_watcher",
            "{name!r} is deprecated as of Python 3.12 and will be "
            "removed in Python {remove}.", remove=(3, 14))


def install():
    """Install PygoEventLoopPolicy globally.  After this, every
    `asyncio.run(...)` / `asyncio.new_event_loop()` returns a pygo
    loop instead of the stdlib selector / proactor loop."""
    asyncio.set_event_loop_policy(PygoEventLoopPolicy())


def run(coro, *, debug=False):
    """Drop-in for `asyncio.run`.  Creates a fresh PygoEventLoop,
    runs `coro` to completion, returns the result.  Caller doesn't
    need to call install() first."""
    loop = PygoEventLoop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
