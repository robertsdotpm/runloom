"""Type stubs for runloom."""
from collections.abc import Callable
from typing import Any, TypeVar

# Core primitives re-exported from the C extension so `import runloom` suffices.
from runloom_c import (
    Chan as Chan,
    select as select,
    mn_init as mn_init,
    mn_go as mn_go,
    mn_run as mn_run,
    mn_fini as mn_fini,
    netpoll_backend as netpoll_backend,
)

_T = TypeVar("_T")

__version__: str

def blocking(fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Offload a blocking/CPU-bound call to a worker pool; park until done."""
    ...

class Goroutine:
    """Opaque handle returned by go().  Has no public methods today --
    join / cancel arrive via the runloom.context module."""

    name: str
    coro: Any  # runloom_c.Coro

def go(
    callable_: Callable[..., _T],
    *args: Any,
    **kwargs: Any,
) -> Goroutine:
    """Spawn a goroutine.  Returns a Goroutine handle.  Same semantics
    as `go fn(a, b)` in Go: schedules fn(*args, **kwargs) to run
    cooperatively, returns immediately."""
    ...

def yield_() -> None:
    """Cooperative yield.  Equivalent to runtime.Gosched()."""
    ...

def sleep(seconds: float) -> None:
    """Sleep without blocking the OS thread.  Other goroutines run."""
    ...

def run(main_fn: Callable[[], Any] | None = ...) -> int:
    """Drive the scheduler until idle.

    If main_fn is given it's spawned first, so:
        runloom.run(my_main)
    is the moral equivalent of Go's `func main()`.

    Returns the number of goroutines that completed."""
    ...

def current() -> Goroutine | None:
    """Return the currently-running Goroutine handle, or None when
    called from outside any goroutine."""
    ...

def backend() -> str:
    """Coroutine backend name: 'fibers' | 'fcontext-asm' | 'ucontext'."""
    ...
