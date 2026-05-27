"""Type stubs for the pygo_core C extension."""
from collections.abc import Callable
from typing import Any, Literal, overload

# ---- Coroutine handle (raw, no scheduler) -----------------------------

class Coro:
    """A raw stackful coroutine.  Most users want go()/run() instead."""
    done: bool
    def __init__(self, fn: Callable[..., Any], stack_size: int = ...) -> None: ...
    def resume(self) -> Any: ...

# ---- Goroutine handle (scheduler-aware) -------------------------------

class G:
    """Opaque goroutine handle returned by go() / mn_go()."""
    ...

# ---- Channel ---------------------------------------------------------

class Chan:
    """Go-style channel.  Buffered if capacity > 0, unbuffered otherwise."""
    def __init__(self, capacity: int = ...) -> None: ...
    def send(self, value: Any) -> None: ...
    def recv(self) -> tuple[Any, bool]: ...
    def try_send(self, value: Any) -> bool: ...
    def try_recv(self) -> tuple[Any, bool] | None: ...
    def close(self) -> None: ...
    def __iter__(self) -> Chan: ...
    def __next__(self) -> Any: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...

# ---- Single-thread scheduler -----------------------------------------

def go(callable_: Callable[[], Any]) -> G:
    """Spawn a goroutine on the single-thread C scheduler.  Returns handle."""
    ...

def go_noyield(callable_: Callable[[], Any]) -> G:
    """Spawn a goroutine the caller PROMISES runs to completion without
    yielding.  Skips per-g snap/load dance.  150-400 ns/g faster.
    Undefined behaviour if the callable yields."""
    ...

def run() -> int:
    """Drive the scheduler until all goroutines complete.  Returns count."""
    ...

def yield_() -> None:
    """Yield from inside a raw Coro (no-op outside one)."""
    ...

def sched_yield_classic() -> None:
    """Yield the current goroutine.  Slower form for benchmarking."""
    ...

def sched_sleep(seconds: float) -> None:
    """Sleep the current goroutine N seconds.  Scheduler-aware."""
    ...

# ---- Backend introspection -------------------------------------------

def backend() -> Literal["fibers", "fcontext-asm", "ucontext"]:
    """Coroutine stack-switch backend."""
    ...

def netpoll_backend() -> Literal["epoll", "kqueue", "iocp-afd", "wsapoll", "select"]:
    """Active netpoll backend selected at first init."""
    ...

# ---- netpoll -----------------------------------------------------------

def wait_fd(fd: int, events: int, timeout_ms: int = ...) -> int:
    """Park the current goroutine until fd is ready.  events bitmask:
    1=read, 2=write.  Returns the readiness mask."""
    ...

def select(
    cases: list[tuple[str, Chan] | tuple[str, Chan, Any]],
    default: bool = ...,
) -> tuple[int, Any]:
    """Wait on multiple channels.  Each case is ('recv', ch) or
    ('send', ch, value).  Returns (index, (value, ok)) for recv or
    (index, None) for send.  With default=True returns -1 if no case
    is immediately ready."""
    ...

# ---- C-level socket fast path ----------------------------------------

def tcp_recv(fd: int, buffer: bytearray | memoryview, n: int) -> int:
    """recv into buffer; returns bytes received.  Cooperative blocking."""
    ...

def tcp_send(fd: int, data: bytes | bytearray | memoryview) -> int:
    """sendall; returns bytes_sent.  Cooperative blocking."""
    ...

# ---- Per-thread + warmup ---------------------------------------------

def thread_init() -> None:
    """Idempotent per-thread setup."""
    ...

def thread_fini() -> None:
    """Per-thread teardown."""
    ...

def warmup(n: int, stack_size: int = ...) -> int:
    """Pre-allocate n stacks of stack_size bytes for the per-thread
    stack pool.  Returns actual count.  Eliminates first-spawn mmap
    latency on server workloads."""
    ...

# ---- M:N scheduler (3.13t) -------------------------------------------

def mn_init(n: int = ...) -> int:
    """Start N hub threads (default: nproc).  Returns count."""
    ...

def mn_go(callable_: Callable[[], Any]) -> G:
    """Spawn on a round-robin hub.  v1: run-to-completion only."""
    ...

def mn_run() -> int:
    """Wait for all M:N goroutines to complete.  Returns total."""
    ...

def mn_fini() -> None:
    """Tear down the hub pool."""
    ...

# ---- Preemption (3.13t) ----------------------------------------------

def preempt_init(quantum_us: int = ...) -> None:
    """Start the time-sliced preemption timer.  3.13t only."""
    ...

def preempt_fini() -> None:
    """Stop the preemption timer if running."""
    ...
