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
| **`"throughput"`** | max req/s — io_uring engages early, bigger offload pool, bulk spawn, an 8× stack pool (fewer unmaps) | RAM (the bigger pool) |
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

> **`"throughput"` at very high fiber counts:** the 8× pool holds ~16K memory
> mappings, safe under the stock `vm.max_map_count` (65530). Past ~30K concurrent
> fibers, raise `vm.max_map_count` (see [resource-limits](resource-limits.md)) —
> or wait for the auto-sized pool (it sizes itself to your live-fiber high-water).

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
