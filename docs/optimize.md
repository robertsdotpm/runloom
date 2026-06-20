# Tuning: `runloom.optimize()`

runloom is correct and fast **with zero configuration** — call nothing and the
runtime tunes itself (best netpoll backend, stall-recovery on free-threaded
builds, calibrating stacks, an auto-sized stack pool, io_uring that engages as
connections climb, …). You should never need to learn a tuning flag.

When you *do* want to lean one way, there is **one function**, and you ask for it
by **the trade-off you're making** — not by memorizing knobs:

```python
import runloom

runloom.optimize()                          # auto — the default; nothing to set
runloom.optimize("throughput")              # max req/s
runloom.optimize("memory")                  # tight RSS
runloom.optimize("latency")                 # sharp tail
runloom.optimize("secure")                  # hardened
runloom.optimize("throughput", "latency")   # compose — pass the trades you want
runloom.optimize("memory", max_fibers=200_000)
```

Call it **before `runloom.run()`** — the settings are read as the runtime starts.

## The four trades

Each name says what you **buy** and what you **spend** — that's the whole mental
model:

| goal | buys you | spends |
|---|---|---|
| **`"throughput"`** | max req/s — io_uring engages early, bigger offload pool, bulk spawn (the stack pool already self-sizes) | a little RAM |
| **`"memory"`** | tightest RSS — eager page reclaim, and idle parked-fiber stack pages handed back now | some throughput (more reclaim syscalls) |
| **`"latency"`** | sharp tail — tighter stall detection so a wedged hub recovers faster | a little CPU (extra watchdog wakeups) |
| **`"secure"`** | hardened — recycled stacks are wiped before reuse (no leftover TLS keys / request bodies) | a little speed |

`max_fibers=N` is the one genuine number with no sane automatic value: a hard
backpressure ceiling on concurrent fibers.

These trades are deliberately **safe** — none flips an experimental lever or a
setting that can OOM-kill a RAM-tight host. The sharpest expert tricks (e.g.
`RUNLOOM_STACK_MADV=off` for zero reclaim syscalls *at the cost of no
pressure-relief*) stay raw env vars with their own warnings; a friendly name
should never hide a footgun.

> **The stack pool sizes itself.** Out of the box (any preset, or none) the depot
> auto-caps to ~1.5× your live-fiber high-water-mark — clamped by `vm.max_map_count`
> *and* RAM so it can't ENOMEM or balloon — so completions pool instead of churning,
> with no number to set. `RUNLOOM_STACK_DEPOT_CAP` still forces a static cap if you
> insist. (See [resource-limits](resource-limits.md) for raising `vm.max_map_count`
> past ~30K concurrent fibers on a stock host.)

## Composing

Goals compose — pass several and they merge. On any knob where two goals
disagree, the higher-precedence one wins:

```
secure  >  memory  >  latency  >  throughput
```

So `optimize("throughput", "memory")` gives you throughput's io_uring/bulk-spawn
*and* memory's eager reclaim. Where two goals ever set the same knob, the
higher-precedence one wins. It returns the dict of **effective** settings (an
explicit shell env var shows through, since it overrides optimize()).

## Power users

The trades are just a friendly layer over the runtime's `RUNLOOM_*` env vars (see
[Resource limits & internals](resource-limits.md)). An **explicit env var still
wins** over `optimize()` — so if you export `RUNLOOM_STACK_MADV=free` yourself,
that sticks. You never *need* the raw vars; they're the escape hatch under the
hood.

## Examples

```python
# RAM-constrained container: just make it lean.
runloom.optimize("memory")

# Latency-critical RPC tier on a dedicated host, multi-tenant secure.
runloom.optimize("throughput", "latency", "secure")

# Hard fan-out ceiling on a shared box.
runloom.optimize(max_fibers=200_000)
```

## Hot handlers: scaling a shared handler across cores

> Full reference: **[Hot handlers](hot-handlers.md)** (`@runloom.hot`, auto mode,
> the rules, and why it works). Short version below.

A plain module-level handler already scales across every core — there's nothing
shared for the cores to fight over:

```python
def handle(conn):          # scales flat to as many cores as you have
    ...
```

The one shape that *doesn't* scale on its own is a **shared closure** — a single
handler that *captures* something and is reused for every connection:

```python
config = load_config()

def handle(conn):
    serve(conn, config)    # captures `config`
server.serve(handle)       # the SAME closure runs on every core
```

When many cores run that one closure flat out, they all hammer the same captured
slots and start colliding, so adding cores stops helping. Mark it `@runloom.hot`
and each core gets its own private copy of the captured slots (pointing at the
same values), so they stop colliding:

```python
@runloom.hot
def handle(conn):
    serve(conn, config)
```

- It's a **no-op** on a handler that captures nothing (already scales) — safe to
  leave on.
- It costs one copy of the captured slots **per core**, not per fiber (a million
  fibers over one handler still cost one copy per core).
- It stays correct: it only kicks in when the handler *reads* its captures. If it
  *rebinds* one (`nonlocal x; x = ...`), runloom leaves it shared.
- Stacking decorators? Put `@runloom.hot` closest to your `def`.

`optimize("throughput")` turns this on automatically for the busiest closures (no
decorator, under a memory budget — it tells you if the budget is hit);
`optimize("memory")` turns it all off to reclaim the RAM.

**Fastest path first:** if a handler is hot enough to want this, *compiling* it
(a Cython `cdef` handler) beats it outright — that removes the interpreter cost
entirely, not just the cross-core contention. `@runloom.hot` is the zero-rewrite
option for when you won't compile.
