# Channels

runloom channels match Go's semantics:

- **Buffered or unbuffered.**  `Chan(N)` for capacity N; `Chan(0)` for
  rendezvous.
- **Senders block** when the buffer is full (or always, if unbuffered).
- **Receivers block** when the buffer is empty (and the channel is open).
- **Closing** the channel wakes every parked sender and receiver.  Sending
  on a closed channel raises `ValueError`; receiving drains the buffer
  then returns `(None, False)`.

```python
import runloom

ch = runloom.Chan(5)        # capacity 5
```

## Send and receive

```python
ch.send(value)                # blocks if buffer full
value, ok = ch.recv()         # blocks if buffer empty
```

`recv` returns a tuple -- `value` is the payload, `ok` is `True` if it
came from a real send and `False` if the channel was closed and empty:

```python
def producer():
    for i in range(3):
        ch.send(i)
    ch.close()

def consumer():
    while True:
        v, ok = ch.recv()
        if not ok:
            print("closed")
            return
        print("got", v)

runloom.go(producer)
runloom.go(consumer)
runloom.run_single()
```

Output:

```
got 0
got 1
got 2
closed
```

## `for v in ch`

A channel is iterable; iteration stops when the channel closes:

```python
def consumer():
    for v in ch:           # yields v on success, stops on close
        print(v)
```

Equivalent to the explicit loop above but slightly faster and reads
better.

## Non-blocking send / recv

`try_send(value)` returns `True` if the value was accepted, `False`
if the buffer was full.  `try_recv()` returns `None` if nothing is
ready, or `(value, ok)` if a value was available.

```python
ch = runloom.Chan(1)

ch.try_send("first")          # True  -- buffer was empty
ch.try_send("second")         # False -- buffer is full

ch.try_recv()                 # ("first", True)
ch.try_recv()                 # None  -- buffer empty
```

These never park the goroutine -- useful for polling, watchdog probes,
or "drain whatever's available without waiting."

## Unbuffered (rendezvous)

`Chan(0)` has no buffer.  Every send pairs with a recv; both park
until the other side is ready.

```python
ch = runloom.Chan(0)

def producer():
    ch.send("hi")             # blocks here until consumer arrives
    print("sent")

def consumer():
    print("got:", ch.recv())  # blocks here until producer sends

runloom.go(producer)
runloom.go(consumer)
runloom.run_single()
```

Output:

```
got: ('hi', True)
sent
```

Unbuffered channels are how Go expresses synchronisation as well as
data flow -- pair them with a goroutine and they become the equivalent
of a mailbox/actor.

## `select`

`runloom.select(cases, default=False)` waits on multiple channels
at once.  Each case is a tuple:

- `("recv", ch)` -- wait until `ch` has data, then receive.
- `("send", ch, value)` -- wait until `ch` can accept, then send.

Returns `(case_index, payload)`.  For a `recv` case, `payload` is the
`(value, ok)` tuple.  For a `send` case, `payload` is `None`.

```python
import runloom

a, b = runloom.Chan(1), runloom.Chan(1)

def producer():
    a.send("A")

def consumer():
    idx, payload = runloom.select([
        ("recv", a),
        ("recv", b),
    ])
    if idx == 0:
        print("a got:", payload)
    else:
        print("b got:", payload)

runloom.go(producer)
runloom.go(consumer)
runloom.run_single()
```

Output:

```
a got: ('A', True)
```

### Default case (non-blocking select)

Pass `default=True` to return immediately if no case is ready.  In that
event `select` returns `-1` (not a tuple -- the caller's signal that
the default branch fired).

```python
r = runloom.select([("recv", a), ("recv", b)], default=True)
if r == -1:
    print("nobody ready right now")
else:
    idx, payload = r
    ...
```

### Mixing send and recv

```python
idx, _ = runloom.select([
    ("send", out_ch, computed_value),
    ("recv", cancel_ch),
])
if idx == 1:
    return                    # cancelled
```

This is the canonical "send unless cancelled" pattern.

## Closing semantics in detail

```python
ch = runloom.Chan(2)
ch.try_send("a")
ch.try_send("b")
ch.close()
```

After this:

- `ch.send(...)` raises `ValueError("send on closed channel")`.
- `ch.recv()` returns `("a", True)`, then `("b", True)`, then
  `(None, False)` from then on.
- `ch.close()` a second time raises `ValueError("close on closed channel")`.

Closing is a one-way operation; once closed a channel cannot be
re-opened (matches Go's semantics -- you'd create a new channel).

## When to use channels vs. plain data structures

Channels are best when you need *synchronisation*, not just storage.
Use a channel when:

- Multiple goroutines produce; one or many consume.  Channel's send/
  recv pairing replaces locks.
- A goroutine should wait until something is ready (work item, event,
  cancellation signal).
- You want backpressure -- a slow consumer naturally slows the producer
  via channel-full blocking.

Use a `list` or `dict` (no synchronisation needed) when you're on one
goroutine and just want a queue.  Channels add overhead per
send/recv that's wasted if you never actually cross goroutine
boundaries.

## Performance

Linux 3.12, x86_64, single thread:

| Operation | Cost |
| --- | --- |
| Buffered send + recv (same goroutine) | ~90 ns |
| Unbuffered ping-pong (two goroutines) | ~560 ns / round-trip |
| `select` over 2 ready cases | ~120 ns |

Unbuffered ping-pong is within 7% of Go 1.22's `BenchmarkPingPong` on
the same hardware.

## Pitfalls

### Sending after close raises

```python
ch.close()
ch.send("x")          # ValueError: send on closed channel
```

Either coordinate so producers stop sending before `close()`, or wrap
`send` in `try`/`except ValueError`.

### `recv` on closed-and-empty returns `(None, False)`

Not an exception -- easy to miss.  Always check `ok`:

```python
v, ok = ch.recv()
if not ok:
    break          # channel closed, no more data
```

`for v in ch` handles this automatically.

### Don't share a closed-channel object across `run()` calls

`runloom.run_single()` returns when all goroutines are done.  If you keep a
closed channel as a module-level singleton across multiple `run()`
calls, you'll see `close on closed channel` errors on the second
iteration.  Create channels inside the entry point.
