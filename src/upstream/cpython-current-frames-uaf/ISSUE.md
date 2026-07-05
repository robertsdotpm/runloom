# Free-threaded build: reading a `sys._current_frames()` frame is a use-after-free

**Affects:** free-threaded build only (`--disable-gil`; `python3.13t`, `PYTHON_GIL=0`). The default (GIL) build is safe — the GIL serializes it.

`sys._current_frames()` returns `PyFrameObject`s whose `f_frame` points at **another running thread's live `_PyInterpreterFrame`**. Reading any attribute (`f_lineno`, `f_lasti`, `f_globals`, …) dereferences that frame with **no synchronization** while the owning thread concurrently materializes/pops it (`take_ownership`, `Python/frame.c`). So **pure-Python code can segfault the interpreter** (use-after-free).

This is documented as unsafe in the free-threading HOWTO, but it is a *memory-safety crash reachable from pure Python*. It is the gap left by **gh-117300** (PR #117301, closed): that change stop-the-world's only the *dict snapshot*, not the subsequent frame-attribute reads — which is why the HOWTO's "may crash" warning *post-dates* the fix. ThreadSanitizer on a free-threaded build flags the race in `frame_getlineno` ↔ `take_ownership` / `_PyFrame_GetCode`.

## Related

- **gh-117300** / PR #117301 (closed, @colesbury) — the parent: STW'd the snapshot only; this is the remaining read-path race.
- **DataDog dd-trace-py #13567** — the same UAF in the wild: a production profiler takes a fatal SIGSEGV dereferencing a stale frame from `sys._current_frames()` / `_PyThread_CurrentFrames`.
- **gh-106883** — a related (different) hazard: deadlock when using `sys._current_frames` under threads.

cc @colesbury (free-threaded frames / gh-117300).

## Repro (segfaults within seconds on a free-threaded build)

```python
import sys, threading

def recurse(n):
    return recurse(n - 1) if n else 0

stop = False
def worker():
    while not stop:
        recurse(60)          # constantly pushes/pops _PyInterpreterFrames

t = threading.Thread(target=worker); t.start()
try:
    while True:
        for fr in sys._current_frames().values():
            fr.f_lineno      # reads the worker's LIVE frame -> use-after-free
finally:
    stop = True; t.join()
```

## Proposed minimal fix — `frame_read_sync.patch`

Serialize the frame reader against `take_ownership` with the frame object's own critical section (`Py_BEGIN_CRITICAL_SECTION(f)`). The reader then sees either the pre-materialize live frame or the post-materialize embedded copy — never a torn `f_frame` nor a freed `_PyInterpreterFrame`. The other attribute readers (`frame_getlasti`, `frame_getglobals`, `frame_getbuiltins`, …) need the same wrap; the patch shows the pattern on `f_lineno` (the repro path).

> **Status: proposed.** Generated against CPython 3.13.13; applies cleanly. NOT yet run against the CPython test suite / TSan oracle — treat as a starting point.
