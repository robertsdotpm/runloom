"""PygoEventLoop: add/remove_signal_handler + the self-pipe wakeup."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .handles import _Handle  # noqa: F401

class _LoopSignalMixin(object):
    def add_signal_handler(self, sig, callback, *args):
        import signal as _signal
        if _threading.current_thread() is not _threading.main_thread():
            raise ValueError("add_signal_handler() can only be called from the "
                             "main thread")
        self._check_closed()
        handle = _Handle(callback, args, self)
        if not hasattr(self, "_signal_handlers"):
            self._signal_handlers = {}
        # Dispatch via signal.set_wakeup_fd + a self-pipe, exactly like
        # asyncio's Unix loop -- NOT via our own signal.signal() callback.
        # Servers (uvicorn, hypercorn) install their OWN
        # signal.signal(sig, handle_exit) for graceful shutdown, which would
        # clobber a Python-level handler we set and silently drop the user's
        # callback.  CPython, however, still writes the signum to the wakeup fd
        # for whatever Python handler is current, so the loop-side dispatch off
        # that pipe survives a server overriding signal.signal().
        self._setup_signal_wakeup()
        try:
            # A handler must be installed for CPython to write to the wakeup fd;
            # a no-op suffices (real work is loop-side).  siginterrupt(False)
            # so the wakeup doesn't EINTR a syscall on the main thread.
            _signal.signal(sig, _signal_wakeup_noop)
            try:
                _signal.siginterrupt(sig, False)
            except (OSError, ValueError):
                pass
        except (ValueError, OSError, RuntimeError) as e:
            raise RuntimeError(str(e))
        self._signal_handlers[sig] = handle

    def _setup_signal_wakeup(self):
        if getattr(self, "_signal_wakeup_setup", False):
            return
        import signal as _signal
        self._signal_rsock, self._signal_wsock = _socket.socketpair()
        self._signal_rsock.setblocking(False)
        self._signal_wsock.setblocking(False)
        try:
            self._signal_old_wakeup_fd = _signal.set_wakeup_fd(
                self._signal_wsock.fileno(), warn_on_full_buffer=False)
        except TypeError:    # pre-3.7 signature; shouldn't happen on 3.12+
            self._signal_old_wakeup_fd = _signal.set_wakeup_fd(
                self._signal_wsock.fileno())
        # Drain the pipe on the loop and dispatch each pending signum's handler.
        self.add_reader(self._signal_rsock.fileno(), self._read_signal_wakeup)
        self._signal_wakeup_setup = True

    def _read_signal_wakeup(self):
        # This runs in the per-fd I/O goroutine that watches the signal
        # self-pipe.  set_wakeup_fd writes EVERY caught signum to the pipe, so
        # this goroutine wakes and runs Python on the loop thread whenever any
        # signal fires -- which means CPython delivers a pending Python signal
        # handler (e.g. pytest-timeout's SIGALRM raising, or a user SIGUSR1
        # handler) at a bytecode boundary INSIDE this body.  Everything below is
        # exception-safe on its own (recv errors handled; call_soon guarded), so
        # any BaseException reaching the outer handler is an async signal-handler
        # raise -> route it out of run_forever() via the fatal path, exactly as
        # the keepalive does (otherwise this goroutine swallows it and an idle
        # loop hangs -- the reason aiosmtpd's main() under pytest-timeout hung).
        try:
            try:
                data = self._signal_rsock.recv(4096)
            except (BlockingIOError, InterruptedError, OSError):
                return
            if not data:
                return
            handlers = getattr(self, "_signal_handlers", None)
            if not handlers:
                return
            for signum in data:
                handle = handlers.get(signum)
                if handle is not None and not handle._cancelled:
                    # Run the user callback on the loop in the Handle's captured
                    # context (matches asyncio's _handle_signal -> _add_callback).
                    try:
                        self.call_soon(handle._callback, *handle._args,
                                       context=handle._context)
                    except RuntimeError:
                        pass
        except BaseException as e:
            self._pg_signal_fatal(e)

    def remove_signal_handler(self, sig):
        import signal as _signal
        handlers = getattr(self, "_signal_handlers", None)
        if not handlers or sig not in handlers:
            return False
        handlers.pop(sig)._cancelled = True
        try:
            if sig == _signal.SIGINT:
                _signal.signal(sig, _signal.default_int_handler)
            else:
                _signal.signal(sig, _signal.SIG_DFL)
        except (ValueError, OSError):
            pass
        if not handlers:
            self._teardown_signal_wakeup()
        return True

    def _teardown_signal_wakeup(self):
        if not getattr(self, "_signal_wakeup_setup", False):
            return
        import signal as _signal
        try:
            self.remove_reader(self._signal_rsock.fileno())
        except Exception:
            pass
        try:
            _signal.set_wakeup_fd(self._signal_old_wakeup_fd
                                  if self._signal_old_wakeup_fd is not None
                                  else -1)
        except (ValueError, OSError):
            pass
        for s in (getattr(self, "_signal_rsock", None),
                  getattr(self, "_signal_wsock", None)):
            try:
                if s is not None:
                    s.close()
            except OSError:
                pass
        self._signal_rsock = None
        self._signal_wsock = None
        self._signal_wakeup_setup = False

    # ---- run loop ----
    # ---- per-thread run machinery (Phase C: one sched per OS thread) ----
