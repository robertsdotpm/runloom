# 17 — How the asyncio bridge was derived (empirically, not from the spec)

Ground truth: `top_100.txt` (the validated-projects list — 222 projects; the
filename is legacy), `tests/test_asyncio_conformance.py` /
`test_asyncio_server_conformance.py` / `test_asyncio_bufferedproto_conformance.py`
(CPython's own asyncio tests run verbatim against the loop), the **aio bridge
invariants** block in `CLAUDE.md`, the regression guards under `runloom_compat/`,
the soak-loop campaigns, and spec 13 (the bridge itself).

## The thesis

> The `runloom.aio` bridge was **not** designed top-down from asyncio's
> documentation. It was **derived empirically**: build a minimal
> `RunloomEventLoop`, then run the **actual test suites of name-brand asyncio
> projects** under it, watch what breaks, root-cause each failure, and either fix
> the bridge or prove the test over-specifies asyncio. The bridge's correctness is
> the fixed point of that loop.

This is the single most important thing to understand about the bridge, and it is
why the bridge is *trustworthy* despite re-implementing one of the hairiest
contracts in the stdlib. asyncio's real contract is not what its docs say — it is
**what the ecosystem's code actually depends on**, and the only way to discover
that is to run the ecosystem.

## Why a from-spec implementation would have failed

A bridge written purely from the asyncio docs would pass the easy 95% — `gather`,
`sleep`, `Lock`, basic streams — and then **silently break the frameworks on the
subtle 5%**: the exact ordering of a future's done-callbacks, the current-task
slot across a socket park, the lifetime of a cancelled timer's captured closure,
the stack depth a TLS handshake reaches inside `data_received`. None of those are
in the docs; all of them are load-bearing for real frameworks; each one is a
*specific crash or hang* in a *specific project's* test. You cannot find them by
reading — only by running aiohttp, uvicorn, asyncssh, aiocoap, anyio, … and
watching their own assertions go red.

So the bridge's invariant list (spec 13, `CLAUDE.md`) reads like a catalog of
forensics, because that is exactly what it is.

## The corpus (`top_100.txt`)

**222 name-brand asyncio projects** whose own test suites (or end-to-end
exercises) run green under `RunloomEventLoop` on free-threaded CPython 3.13t —
"green == matches stock asyncio on the same interpreter," with every residual
failure traced to an environment/version/test-framework issue that reproduces on
stock asyncio too, not to runloom. Categories include:

- **web frameworks / ASGI**: aiohttp, starlette, uvicorn, hypercorn, tornado,
  sanic, fastapi, django-channels, falcon, emmett, …
- **protocol stacks**: asyncssh, websockets, aiomqtt, aiocoap, aiosmtpd, …
- **DB / cache drivers**: asyncpg-style, aiomysql, aioredis-style, aiosqlite, …
- **structured-concurrency / glue**: anyio, aiojobs, aiomisc, async-lru, aiocsv, …

> The headline number from the list: across these projects — **tens of thousands
> of third-party tests** — exactly **one new runloom bug** surfaced in the final
> sweep, and it was found, fixed, and merged (the future done-callback ordering /
> websocket-close fix, spec 13 §1). That low count is the *output* of the
> derivation, not a starting condition: the bulk of the bridge's invariants were
> paid for during its iterative construction (the soak loops below); by the time
> the 222-project sweep ran, the bridge was mature enough that only one more
> defect remained to find.

## The conformance layer: CPython's own asyncio tests, verbatim

Above the third-party suites sits a stricter check: **run CPython's own
`Lib/test/test_asyncio` bodies against `RunloomEventLoop`**, unmodified, by
swapping the loop in via the suite's `create_event_loop` hook
(`tests/test_asyncio_conformance.py`). This runs the canonical
`BaseSockTestsMixin` (`sock_recv`/`sock_sendall`/`sock_connect`/`sock_accept`/
`sock_recvfrom`/backpressure) — exactly the loop's own primitives — and CPython's
own assertions turn red on any regression. The first run was **10/13**; the 3
failures were real bridge gaps (the loop wasn't enforcing asyncio's "socket must
be non-blocking" debug precondition; `sock_accept` returned a blocking socket),
since fixed → **13/13 verbatim**. Companions cover `BaseTestBufferedProtocol`
(the `get_buffer()`/`buffer_updated()` path) and the server side. This is the
"the bridge passes asyncio's *own* test of itself" layer, distinct from "the
bridge passes aiohttp's test of aiohttp."

## The catalog of fixes (each = one framework's failing test)

The `CLAUDE.md` "aio bridge invariants" are the record. Each entry has the same
shape — *a project's test failed, here is the root cause, here is the fix, here is
the regression guard*:

| Symptom (the failing project) | Root cause | Fix | Guard |
|---|---|---|---|
| asyncssh `data_received` SEGV | TLS/SSH kex recurses deep into C on a small g-stack | spawn callback fibers on a 512 KB `_IO_STACK` | — |
| aiohttp `web.AppKey` `UnboundLocalError` | `AppKey` walks `f_back` to a `<module>` frame the fiber root severs | run the driver under a real `<module>`-named frame | `aiohttp_*` |
| aiocoap strict gc-leak teardown fails | a cancelled timer's fiber captured the callback in its closure | timer reads the callback **through the handle** (cancel nulls it) | `timer_leak.py` |
| aiohttp connector `_wait_for_close` deadlock | a deferred stock-Task wakeup hit a stale current-task slot | clear `_CURRENT_TASKS[loop]` for loop-level callbacks (`_pg_run_loop_cb`) | `aiohttp_leak_probe.py` |
| falcon/uvicorn websocket close 1012≠1001; aiojobs | library done-callbacks fired synchronously inverted asyncio order | **defer** done-callbacks via `call_soon`; only `_wake_unpark`/`_runloom_fire_sync` sync | `ws_close_order_repro.py` |
| asyncssh channel-open-vs-close crash | a same-thread future wake routed through the batch wake_list landed out of FIFO | same-thread `wake_safe` pushes straight to the ready ring | `call_soon_fifo.py` |
| aiocsv `_Parser` "no attribute 'send'" | the driver injected `future.result()` into a custom awaitable-iterator | driver always `coro.send(None)` | `aiocsv_repro.py` |
| aiocoap CSM / SMTP banner dropped connection | a write inside `connection_made` reached `_kick_io` before `_io_g` was seeded | seed `self._io_g = None` before `connection_made` | `tls_connection_made_write.py` |
| aiosmtpd/anyio teardown hang | `task.cancel()` of a g parked in a C `wait_fd` had no await-point | `cancel_wait_fd()` (netpoll cancel sentinel, spec 06) | — |
| long-lived server accept-loop leak | `server.close()` left accept fibers parked on the listen fd | `close()` `cancel_wait_fd()`s them | `fiber_leak_char.py` |

(The C-side cancel sentinel, the `current_task()` save/restore across `_wait_fd`,
and the module-root frame are also in this set — see spec 13. The point is the
*provenance*: every one is a named project's red test.)

## The soak loops (where the bulk was found)

The construction phase ran **autonomous soak loops**: drive a name-brand project's
test suite under `RunloomEventLoop` on 3.13t, and for every failure, fix the
bridge bug (Python `runloom.aio` or the underlying C primitive) and continue — a
ratchet that only stops when a suite is fully green. Several of the deepest C
fixes came from these (the `current_task()`-across-socket-I/O corruption that
broke `asyncio.timeout`/`wait_for`; the `run_forever` signal-interrupt hang; the
`cancel` that couldn't interrupt a `wait_fd` park — all root-caused because some
project's teardown deadlocked or some test's timeout never fired). The tracker
listed the top projects and swept them one at a time.

## The triage discipline (the load-bearing judgment)

Not every red test is a runloom bug — and treating them all as bugs would have
warped the bridge toward bug-for-bug emulation of asyncio internals. The
discipline (stated in `docs/asyncio.md`) is one question per failure:

> **Does the test assert an *observable behavioral guarantee*, or assume an
> *implementation mechanism*?**

- *Asserts behavior* ("this callback must not see state X before `close()` runs")
  → a genuine fidelity gap; fix the bridge. These produced the invariant catalog.
- *Assumes a mechanism* (mocks `loop.time()` to fast-forward a timer; relies on an
  exact `sleep` duration; runs several loops per OS thread) → the test
  over-specifies asyncio's *internals*, which runloom deliberately implements
  differently (spec 13's three documented semantic diffs). Adapt the test (use
  real time), don't contort the bridge.

This is why spec 13 has a *"known semantic differences"* section at all: those are
the cases where the derivation deliberately *stopped* — runloom matches asyncio's
**observable** contract, not its callback-queue/timer-heap *mechanism*, and the
handful of tests that pin on the mechanism are documented rather than chased.

## Why this belongs in the archive

A re-implementer who builds `runloom.aio` from spec 13 alone will get a bridge
that looks right and breaks aiohttp. The *method* — stand up a minimal loop, point
it at the real ecosystem's test suites + CPython's own `test_asyncio`, fix every
behavioral failure, document every mechanism-assumption — is how you converge on
the same ~12 invariants without re-suffering each crash blind. **The corpus and
the conformance harness are part of the spec**: they are the executable definition
of "the bridge is correct."

## Invariants (about the method)

1. **The bridge's contract is the ecosystem's behavior, not asyncio's docs.**
   Derive it by running real suites, not by reading.
2. **Run CPython's own `test_asyncio` verbatim** against the loop — the canonical
   conformance check, distinct from third-party suites.
3. **Every behavioral fix ships a regression guard** (`runloom_compat/*.py`) named
   for the project that motivated it.
4. **Triage each failure: behavior (fix the bridge) vs mechanism (adapt the
   test).** The latter become the documented semantic diffs, not bug-for-bug
   emulation.
5. **The 222-project / one-bug result is an output, not an input** — the
   invariants were paid for during construction; the sweep confirms convergence.
