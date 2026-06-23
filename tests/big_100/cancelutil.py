"""Cooperative cancellation helpers for the big_100 cancellation projects.

Under M:N (`run(n>1)`) `go()` returns no handle, so there is no forced
`g.cancel()`.  runloom's idiom is cooperative cancellation: a `context` whose
`done` channel closes on cancel, which goroutines watch with `select`.  These
helpers wrap that pattern (plus timeouts via `runloom.time.After`).
"""
import runloom
import runloom.context as _ctx
import runloom.time as _time

Background = _ctx.Background
WithCancel = _ctx.WithCancel
WithTimeout = _ctx.WithTimeout
CANCELED = _ctx.CANCELED
DEADLINE_EXCEEDED = _ctx.DEADLINE_EXCEEDED


def cancellable_sleep(ctx, seconds):
    """Sleep up to `seconds`; return True if it elapsed, False if cancelled."""
    timer = _time.After(seconds)
    idx, _payload = runloom.select([("recv", ctx.done), ("recv", timer)])
    return idx == 1


def cancellable_recv(ctx, ch, timeout=None):
    """Recv from ch, but bail if ctx is cancelled (or timeout elapses).

    Returns (value, ok) on a real receive, None if cancelled/timed-out."""
    cases = [("recv", ctx.done), ("recv", ch)]
    if timeout is not None:
        cases.append(("recv", _time.After(timeout)))
    idx, payload = runloom.select(cases)
    if idx == 0:
        return None                 # cancelled
    if timeout is not None and idx == 2:
        return None                 # timed out
    return payload                  # (value, ok)


def delayed_cancel(cancel, delay):
    """Goroutine body: wait `delay`, then cancel."""
    runloom.sleep(delay)
    cancel()
