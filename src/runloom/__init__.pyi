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
    mn_hub_count as mn_hub_count,
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
) -> Goroutine | None:
    """Spawn a goroutine.  Same semantics as `go fn(a, b)` in Go:
    schedules fn(*args, **kwargs) to run cooperatively, returns immediately.

    Returns a Goroutine handle on the single-thread scheduler (run(1, ...));
    inside an M:N run (run(n > 1, ...)) it spawns onto a hub via mn_go, which
    is fire-and-forget in v1, so it returns None."""
    ...

def yield_() -> None:
    """Cooperative yield.  Equivalent to runtime.Gosched()."""
    ...

def sleep(seconds: float) -> None:
    """Sleep without blocking the OS thread.  Other goroutines run."""
    ...

def run(n: int, main_fn: Callable[[], Any] | None = ...) -> int:
    """THE entry point: run the scheduler on n OS-thread hubs until idle.

        run(1, main)   single-thread (M:1).
        run(n, main)   M:N across n hubs, GIL off -> real multi-core
                       parallelism.  Requires a free-threaded build (3.13t,
                       PYTHON_GIL=0); n > 1 with the GIL on raises.
        run(n)         main_fn omitted -> drain already-go()'d goroutines.

    n is required and explicit: M:N is a different correctness model (Python
    runs in parallel, so shared state can race), opted into by typing the
    number.  main_fn, when given, is the root goroutine and may go() more.
    Collapses the raw mn_init/mn_go/mn_run/mn_fini envelope.  Returns the
    number of goroutines completed."""
    ...

def current() -> Goroutine | None:
    """Return the currently-running Goroutine handle, or None when
    called from outside any goroutine."""
    ...

def backend() -> str:
    """Coroutine backend name: 'fibers' | 'fcontext-asm' | 'ucontext'."""
    ...
