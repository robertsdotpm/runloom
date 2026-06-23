# Stage 3 — weak memory (iRC11 / gpfsl): DONE

Stages 1–2 (`OneShotWake.v`, `WakeQueue.v`) are sequentially-consistent Iris.
This is the genuine weak-memory tier: runloom's commit-publish release/acquire
pattern proved correct under the **RC11** relaxed-memory model in a separation
logic, with **iRC11** (the relaxed-memory logic built on Iris, shipped as
`gpfsl`).

**Status: machine-checked.** `rc11/CommitPublish.v` compiles under gpfsl
(`verify/iris/rc11/run_rc11.sh` → PASS). It is a real proof, not an `Admitted`
stub.

## The target property: the netpoll commit publish

`runloom_pump_*` (the claimer) publishes the readiness and then releases
`pool->lock`; the aborting parker re-acquires `pool->lock` before reading it:

```
claimer:   *ready_out = mask;            // publish the readiness (data)
           <release the commit>          // commit-CAS / unlock pool->lock
parker:    <acquire the commit>          // re-read under acquire
           r = *ready_out;               // MUST observe mask, never stale 0
```

This is the **message-passing (MP)** shape: data written before a release-store
is visible to a thread that reads the released location with an acquire.

## The proof: `rc11/CommitPublish.v`

Models the program in iRC11's HeapLang (RC11 operational semantics, explicit
`<-ʳᵉˡ` / `!ᵃᶜ`): the claimer writes `ready := 42` then does a **release** write
of `commit`; the parker spins on an **acquire** read of `commit`, then reads
`ready`. The spec
```
{{{ True }}} commit_publish {{{ v, RET #v; ⌜v = 42⌝ }}}
```
proves the parker reads **42 (the published value), never the stale initial 0**,
under RC11 — i.e. the release/acquire pair is sufficient for the publish to be
visible. The proof uses a GPS single-writer boolean protocol whose `true`
state ships an escrow that transfers ownership of `ready ↦ 42`, so the
acquiring reader obtains it (a faithful adaptation of gpfsl's own
`gpfsl-examples/mp` proof, specialized to the runloom framing, with the unique
token inlined so the file is self-contained).

## Corroboration at the fence level (no gpfsl needed)

The *fence placement* for the same property is independently checked under RC11
and runs in `verify/run_verify.sh`:

- **herd7 litmus** (`verify/litmus/`): `commit_cas_then_publish` shows the
  CAS-acquire alone admits a stale read (**Sometimes**); `commit_lock_publish`
  shows the `pool->lock` round-trip forbids it (**Never**).
- **GenMC** (`verify/genmc/netpoll_claim.c`): the whole claim protocol as real
  C under RC11, every execution; `-DBUG_NO_LOCK` reintroduces the race and is
  caught.

So the property now has three independent weak-memory witnesses: the herd7
axiomatic litmus, the GenMC exhaustive real-C model, and this iRC11
separation-logic program proof.

## Build (isolated switch — gpfsl pins iris-dev)

```sh
opam switch create runloom-weakmem --packages=ocaml-system
opam repo add coq-released https://coq.inria.fr/opam/released
opam repo add iris-dev git+https://gitlab.mpi-sws.org/iris/opam.git
opam install -y coq-gpfsl
```
`run_rc11.sh` auto-selects the `runloom-weakmem` switch (override with
`RUNLOOM_WEAKMEM_SWITCH`) and uses `rocq compile` (Rocq 9.2) or `coqc`. It is kept
in its own switch so it never disturbs the released-Iris build of Stages 1–2.

## What remains genuinely open

A full iRC11 proof of the *entire* Chase-Lev deque under RC11 is research-scale
and has **no adaptable reference**: gpfsl's `gpfsl-examples/chase_lev` ships
`code.v` only (the implementation, ~97 lines) — there is no spec or proof file
to specialize. Deriving the deque's linearizability/safety under RC11 in iRC11
from scratch is the content of the Lê–Pop–Cohen–Nardelli line of work, i.e. a
research paper, not an adaptation. It is therefore left as the explicit
ceiling rather than faked with `Admitted`.

What IS established for the deque, at the levels that are soundly checkable
here: the unbounded conservation / no-double-consume invariant in Coq
(`verify/coq/Deque.v`), the real C with its `__atomic` orders in CBMC
(`verify/cbmc`), and the related fences under RC11 in herd7 + GenMC. The
commit-publish core — the load-bearing fence the litmus tests isolate — is
fully proven in iRC11 here (`rc11/CommitPublish.v`).
