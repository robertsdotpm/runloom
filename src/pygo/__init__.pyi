"""Type stubs for pygo."""
from collections.abc import Callable
from typing import Any, TypeVar

_T = TypeVar("_T")

class Goroutine:
    """Opaque handle returned by go().  Has no public methods today --
    join / cancel arrive via the pygo.context module."""

    name: str
    coro: Any  # pygo_core.Coro

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
        pygo.run(my_main)
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
