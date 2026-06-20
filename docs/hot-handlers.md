# Hot handlers — `@runloom.hot`

`@runloom.hot` makes a **shared closure** handler scale cleanly across all your
cores. It is a no-op for handlers that don't need it, so it's safe to leave on.

## When you need it

A plain module-level handler already scales — there's nothing shared for the
cores to fight over:

```python
def handle(conn):          # scales flat across every core
    ...
```

The one shape that *doesn't* scale on its own is a **shared closure**: one
handler that *captures* something and is reused for every connection.

```python
config = load_config()

def handle(conn):
    serve(conn, config)    # captures `config`
server.serve(handle)       # the SAME closure runs on every core
```

When many cores run that one closure flat-out, they all hammer the same captured
variables (the closure's *cells*) and start colliding — so adding cores stops
helping. (Measured at 44 cores: a shared-closure handler ran ~160× slower than an
equivalent that didn't share its captures.)

## The fix

```python
@runloom.hot
def handle(conn):
    serve(conn, config)
```

Each core gets its own private copy of the captured variables (pointing at the
same values), so they stop colliding. Same behaviour — just no cross-core
contention.

## Rules (why it's safe to leave on everywhere)

- **No-op on a module-level `def` that captures nothing** — it already scales.
- **No-op if the handler *rebinds* a capture** (`nonlocal x; x = ...`) — per-core
  copies could drift, so runloom leaves it shared. *Reading* a capture, or
  mutating a captured object **in place** (`cfg.x = 1`, `d[k] = v`,
  `buf.append(...)`), is fully supported — every copy points at the same object.
- **No-op on anything that isn't a plain Python function** (a builtin, a class
  instance, an already-wrapped C callable) — returned unchanged.
- **No-op at runtime under `optimize("memory")`** — spends the memory back.

## Cost

One copy of the **captured variables** per core — **not per fiber**. A million
fibers over one `@hot` handler still cost one copy per core. The copies share the
same captured *values* (only the per-core cell wrappers differ), so it is cheap;
RSS is bounded by your core count, not your fiber count.

## Automatic mode (no decorator)

`runloom.optimize("throughput")` turns on **auto** hot-handlers: runloom watches
which closures get spawned a lot and gives the busiest few the `@hot` treatment
automatically, under a hard budget so it can never clone its way through your RAM.
It emits a warning if the budget is hit (no silent truncation).
`optimize("memory")` turns both the decorator and auto mode off.

Rarely-needed knobs:

| env var | meaning | default |
|---|---|--:|
| `RUNLOOM_HOT_HANDLERS` | master on/off for `@hot` | on |
| `RUNLOOM_HOT_AUTO` | auto-promotion on/off (set by `optimize`) | off |
| `RUNLOOM_HOT_AUTO_AFTER` | spawns of a closure before it's promoted | 64 |
| `RUNLOOM_HOT_AUTO_BUDGET` | max distinct handlers to clone | 32 |

## Stacking with other decorators

Put `@runloom.hot` **closest to your `def`** (the innermost decorator) so it sees
your real closure, not another decorator's wrapper.

## Fastest path first

If a handler is hot enough to want this, *compiling* it (a Cython `cdef`
handler) beats it outright — that removes the interpreter cost entirely, not just
the cross-core contention. `@runloom.hot` is the zero-rewrite option for when you
won't compile.

## Why it works (one line)

The contention is the **closure's cells** — the captured-variable slots — shared
across cores under free-threading; `@hot` gives each core its own cells holding
the same values. It is **not** the code object: a single shared *code* object
scales fine. See [`benchmark/SCHEDULER_SCALING_FINDINGS.md`](../benchmark/SCHEDULER_SCALING_FINDINGS.md)
for the 7-variant ablation that proves it's the cells, and `src/runloom/_hot.py`
for the implementation.
