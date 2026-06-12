# 11 — Crash reporting and introspection

Ground truth: `runloom_crash.{h,c}`, `runloom_introspect.{h,c}`,
`runloom_introspect_frames.c.inc`, `runloom_iframe.{h,c}`, `runloom_diag.{h,c}`,
`mn_sched_hubinfo.c.inc`, `inspect.py`, `docs/debugging.md`.

## The problem

A custom scheduler that hides a million fibers on a few OS threads is opaque
when something hangs or crashes: a stack trace shows the hub thread, not the
fiber. Go answers this with `kill -QUIT` fiber dumps + `runtime.Stack`;
asyncio has `all_tasks()`. runloom had neither. This subsystem is the answer, and
its hard constraint is **it must work when the process is wedged or faulting** —
i.e. without taking locks that might be held and without touching Python (which
may be mid-collection).

## The fiber registry (and its zero-hot-path-cost design)

A global intrusive doubly-linked list of every live `runloom_g` *struct*. The
clever part (the same field-ordering contract as spec 02): a g is linked **once**
when its struct is first OS-allocated and unlinked only when returned to the OS; a
**slab-recycled g stays linked** (the dump skips `FREED` entries). Because the slab
reuse path memsets a g only up to `offsetof(runloom_g_t, state)`
([runloom_sched_core.c.inc:329](../src/runloom_c/runloom_sched_core.c.inc#L329))
and the `reg_prev/reg_next` links sit *after* the `state`/`id` block, recycling
preserves them. So link/unlink touch the registry lock
only on the cold slab-miss/overflow paths — **the hot spawn/complete path pays
nothing** (no lock, no shared atomic). The goid (`runloom_next_goid`) is
**block-allocated**: each thread grabs a block of 1024 ids
(`RUNLOOM_GOID_BLOCK`) from a global atomic and hands them out locally, so the
shared cacheline is touched once per 1024 spawns and ids stay compact + roughly
monotonic ([runloom_introspect.c:57-71](../src/runloom_c/runloom_introspect.c#L57)).
(The header comment's "a per-thread counter ORed with a per-thread base" describes
an older scheme; the code block-batches.)

## Three dump paths, by safety level

Each is matched to how broken the process is:

1. **Structural dump, async-signal-safe-ish** (`runloom_dump_fibers_fd`). id /
   state / what-it's-blocked-on (fd, channel, sleep deadline) / owner thread / age
   / refcount / stack size, written with **only `snprintf` + `write(2)`**. It
   **try-locks** the registry — on contention it prints a note and the parker
   pool's own dump rather than deadlocking. **Touches no Python**, so it is safe
   from a signal handler and when the interpreter is wedged. This is the SIGQUIT
   handler (`install_dump_signal`) and the crash-handler path.
2. **Rich snapshot** (`runloom_fiber_snapshot`, Python context only). Every
   field is **plain data copied under the registry lock** — deliberately *no
   owned-object pointers* (callable/coro/parker), because those are freed by
   fiber teardown which does *not* take the registry lock, so dereferencing
   them in the dump would be a UAF. "Blocked on" detail rides POD fields the g
   maintains itself (`park_fd`) or values not pointers (`wake_at`).
3. **Reconstructed Python stack** (`runloom_fiber_frames_by_id`). Walks the
   suspended interpreter-frame chain (`runloom_iframe_walk`, deepest-first, skipping
   C-trampoline shims). The frame walk is unsafe in general (frames mutate as code
   runs), so the caller must **claim** the fiber first: under M:N via the
   sweeper handshake (`PARKED→SWEEPING`, so it can neither resume nor tear down
   mid-walk), under single-thread by owning the thread. Withheld under M:N for a g
   that a hub could resume at any instant (no safe way to freeze it) — the
   structural fields still tell the story.

## The crash handler — classifying a fault by the guard pages

`runloom_crash.c` turns a SIGSEGV/SIGBUS (optionally SIGILL/FPE/ABRT) into a
structured dump instead of a silent core. The key trick: **map the faulting
address onto the per-fiber guard pages** (spec 01) to classify it —

- fault **in a guard page** → "GOROUTINE STACK OVERFLOW," naming the fiber and
  its stack size;
- fault **inside a usable fiber stack** → a wild pointer / UAF on that g;
- otherwise → a non-fiber fault.

Then it dumps the live-fiber registry (path 1 above), optionally a native C
backtrace (`execinfo`) and the Python traceback (by chaining out to `faulthandler`),
and can **wait for a debugger** or `fork+exec gdb` before chaining to the default
disposition (so a core is still produced and the exit code stays correct).

Two robustness details that are easy to miss and load-bearing:

- **Survives a blown fiber stack:** every runloom OS thread installs its own
  `sigaltstack` (`runloom_crash_thread_arm`, wired into `coro_thread_init` and the
  blockpool workers), so the handler runs even when the fault *is* the stack
  overflow (the normal stack is gone).
- **Freezes the watchdogs first.** The faulting hub, on becoming crash owner, calls
  `runloom_sched_freeze_for_crash` (async-signal-safe: just stores to the sysmon /
  handoff stop flags + the preempt flag). Otherwise the handoff rescue would adopt
  the faulting hub's fibers and *steal the faulting g away* before the handler's
  chain-out re-faults and cores — leaving a limping process and no core.

On Windows the rich POSIX path isn't available; a Vectored Exception Handler does
the fiber dump and continues the search (the OS still produces the crash).

## Hub introspection (`runloom.inspect.hubs()` / `mn_hub_snapshot`)

The hub-level companion to the fiber dump — "what is each hub doing right
now": id, attach-state (detached/attached/suspended), the running goid, dwell-ms
(how long the current resume has run — a large dwell + detached = a wedged hub),
pending count, whether sysmon requested a preempt. For a **DETACHED-wedged** hub it
best-effort fills `blocked_at` with the running fiber's top Python frame (the
blocking call site, e.g. `cursor.execute (db.py:88)`), read under a handoff-rescue
lockout. The Python layer (`inspect.py`) labels each hub (`WEDGED/io`, `WEDGED/cpu`,
`idle`, `running`) and prints a `py-spy dump --pid <PID>` command for the full
C+Python stack of every thread — the always-safe out-of-process fallback.

## Other introspection surfaces (`inspect.py`)

- **`fibers()` / `stack(gid)` / `dump()`** — the Python-formatted dumps.
- **Park-age tracking** (opt-in, `enable_timestamps`): a g's `state_since_ns` is
  stamped on each park, so the dump reports "parked 45.2s" — and **`leaked()` /
  `watch_leaks()`** surface fibers parked too long (an orphaned accept loop, a
  never-awaited task, a stuck timer).
- **Deadlock mode** (`set_deadlock_mode`, off/warn/raise): the single-thread drain
  detecting "all fibers asleep" (spec 02).
- **Max-fibers admission gate** (`set_max_fibers`): backpressure — over the
  cap, `go`/spawn raises so an unbounded spawn loop can't OOM; zero hot-path cost
  when unset.

## The diag ring (`runloom_diag.h`, opt-in `RUNLOOM_DEBUG`)

A per-OS-thread lock-free event ring (~30 ns/event) recording lifecycle events
(parker link/unlink/wake, g transitions, snap save/load, handoff adopt, world
yield, coro acquire/release). Off in release (a predicted-not-taken branch).
Dumped on demand, and a bounded **flight-recorder** dump is safe from the crash
handler. Plus `runloom_self_check` (walk every parker list / per-fd bucket / the
parked counter and assert invariants — Floyd cycle checks, count match), cheap
enough to run between bench iterations. These are the tools that root-caused the
hard bugs (the sentinel-pointer crash, the lost wakes); the determinism delay
injection (`RUNLOOM_DELAY`) and the gilstate/baton trace conformance (spec 09) hang
off the same module.

## Invariants

1. **The structural dump touches no Python and try-locks the registry** — safe
   from a signal handler / when wedged; never blocks.
2. **The rich snapshot copies only POD under the registry lock** — never derefs an
   owned object teardown could free without that lock.
3. **A frame walk requires claiming the fiber** (sweeper handshake under M:N).
4. **The crash handler classifies faults by the guard pages**, runs on a
   `sigaltstack` (survives stack overflow), and **freezes the watchdogs first** so
   the faulting g isn't stolen before the core.
5. **Registry link/unlink stay off the hot path** (field-ordering contract,
   spec 02) — the dump must never add cost to spawn/complete.
