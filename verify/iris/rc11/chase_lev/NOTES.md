# Chase-Lev under iRC11 — invariants, fence map, and the steal/take race

**Status:** experiment. One iRC11 fragment fully machine-checked
(`StealClaim.v`, axiom-free); a GenMC/RC11 oracle of the full algorithm + resize
(7/0); and the central **steal/take race invariant** written down for porting.
The invariant — not a QED — is the deliverable, per the brief.

This report follows the requested checkpoint shape at the end (§7).

---

## 1. Reframing: linearizability → logical atomicity (a result, not a dodge)

The original goal said "prove linearizability." **Under RC11 that is the wrong
notion and I stopped when I hit the reason**, as instructed:

Classic linearizability presupposes a single global real-time total order of
operation invocations/responses that the abstract history must respect. RC11 has
no such order: it has per-location modification order `mo`, a *partial*
happens-before `hb`, reads-from `rf`, `sb`. There is no global clock to linearize
against. Concretely, **`take`'s linearization point is future-dependent**: when
`take` and a `steal` both target the last element, *which* one logically happened
first is decided by which CAS on `top` wins, and that is observable only after the
CAS resolves — `take`'s LP can fall **inside** a concurrent `steal`'s CAS.

So the framing switches to **logical atomicity + contextual refinement**:

- Each operation gets an Iris/iRC11 **atomic triple** `⟨ S ⟩ op ⟨ S' ; r ⟩` whose
  precondition/postcondition name an abstract deque state `S` living in an
  invariant; the op commits its effect at a single *logical* LP (the atomic
  update fires exactly once). The *ordering* of LPs is induced by the proof's
  protocols/`hb`, **not** a presupposed global clock.
- The deque **contextually refines** a coarse-grained atomic spec (§2) in which
  push/pop/steal each run atomically on an abstract ADT.

This is the reportable result: *the Chase-Lev correctness statement under RC11 is
logical atomicity against a view-relative abstract state, because the
empty-observations and the take/steal LP are not assignable in a single global
order.*

---

## 2. The atomic spec — and what it forbids

Abstract state: a sequence `S` of *element ids* (bottom = owner end, top = thief
end), plus a ghost **fate** map `st : id → {InDeque, Taken, Stolen}`.

- `push(x)`  `⟨ S ⟩ → ⟨ S ++ [id_x] ⟩`, mints fresh `id_x` with `st = InDeque`.
- `take()`   `⟨ S ⟩ → ⟨ S' ⟩`: if `S` nonempty, `S' = drop-last S`,
  `RET val(last S)`, flip `st(last) : InDeque→Taken`; else `RET EMPTY`.
- `steal()`  `⟨ S ⟩ → ⟨ S' ⟩`: if `S` nonempty, `S' = drop-head S`,
  `RET val(head S)`, flip `st(head) : InDeque→Stolen`; else `RET EMPTY/ABORT`.

The two things that matter become invariants on `st`:

- **(a) No LOSS.** `st(id)` leaves `InDeque` *only* via a take/steal that returns
  `val(id)`. Equivalently: `returned(id) ⟺ st(id) ≠ InDeque`. No element silently
  disappears.
- **(b) No DUPLICATION.** The `InDeque → Taken/Stolen` flip is **one-shot** per
  id (guarded by an exclusive *claim token*, §4): `∀ id, returned(id) ≤ 1`. No
  element is returned by two consumers (two steals, or a steal and a take).

### Non-vacuity (a broken deque violates it) — machine-checked

| broken impl | spec clause violated | GenMC evidence |
|---|---|---|
| `take` skips the top-CAS on the last element | (b) duplication | `chase_lev.c -DBUG_NO_CAS` → `got[1]==2` |
| drop the SC fences (≥2 elements) | (b) duplication | `chase_lev2.c -DBUG_NO_FENCE` → `got[2]==2` |
| publish the resized array with relaxed order | data-race / (b) | `chase_lev_resize.c -DBUG_RLX_ARR` → race on `bufB[0]` |

So the spec is **not vacuous**: obviously-broken implementations fail it.

---

## 3. Memory orderings — assumed vs. real (no SC strengthening)

Pinned to `gpfsl-examples/chase_lev/code.v` and Lê et al. PPoPP'13:

| op | location | access used (= real) |
|---|---|---|
| push | `bot` | relaxed load, **release** store |
| push | `buf[b]` | non-atomic write |
| steal | `top` | relaxed load · **SC fence** · relaxed CAS |
| steal | `bot` | **acquire** load |
| steal | `buf[t]` | non-atomic read |
| take | `bot` | relaxed load, relaxed store, relaxed store |
| take | `top` | **SC fence** · **acquire** load · relaxed CAS |
| resize | `arr` | **release** publish / **acquire** observe |

**No access is strengthened to SC** beyond the algorithm's own two SC fences. No
strengthening compromise to report.

### Fence-necessity map (the primary output) — all machine-checked by GenMC

- **top CAS — NECESSARY.** Drop it → duplication at one element
  (`chase_lev.c -DBUG_NO_CAS`).
- **SC fence — configuration-dependent.**
  - *Redundant at 1 contended element*: the top-CAS alone arbitrates take vs
    steal (`chase_lev.c -DBUG_NO_FENCE` is **clean** — a finding).
  - *NECESSARY at ≥2 elements*: it stops `take` reading a stale `top`, entering
    the no-CAS branch (`t < b`), and returning `buf[b]` — a slot a thief already
    stole → duplication (`chase_lev2.c -DBUG_NO_FENCE` → `got[2]==2`).
- **`arr` release/acquire — NECESSARY for resize.** Relaxed → the owner's
  copy-write into the new buffer races a thief's read of it
  (`chase_lev_resize.c -DBUG_RLX_ARR`: `Wna bufB[0]` ∥ `Rna bufB[0]`).
- **`bot` release(push)/acquire(steal) — NECESSARY (message passing).** This is
  the pair that carries the buffer write to the thief; it is the synchronisation
  that `StealClaim.v` proves in iRC11 (§5).

---

## 4. The steal/take race invariant — the deliverable

Ghost setup (the device proven to work in `StealClaim.v`):

- `top` value `T` is a monotone counter; per index `i` there is a one-shot
  **exclusive** token `claim(i)`. The element at `i` is owned by the holder of
  `claim(i)`; an **escrow** `[es claim(i) ⇝ buf[i]↦x_i ∗ P x_i]` (persistent,
  seated in `bot`'s release interpretation) lets the holder redeem the slot.
- `claim(i)` is *seated in `top`'s CAS protocol at value i*: whoever CASes
  `top : i→i+1` extracts `claim(i)`. This makes the steal claim unique
  (mechanised, `StealClaim.v`).

**INV_race** (the invariant I need; the one to port):
there exist `T` (real top), `B` (bot), and a ghost **owner lower-bound** `Tc`
such that

1. `top`'s protocol value is `T`, and `T ≤ B` is **not** assumed globally
   (weak memory!);
2. the owner holds `⊛_{i ∈ [Tc, B)} claim(i)` — exclusive ownership of the
   bottom segment it has not ceded;
3. every `claim(i)` for `i ∈ [T, Tc)` has been ceded into `top`'s CAS protocol
   (available to a steal winner);
4. **`Tc ≤ T`, and `Tc` is exactly the `top` value the owner's last
   *SC-fence-ordered* read established as a lower bound.** After take's
   `bot:=b ; fence_sc ; t:=top.acquire`, the SC fence gives `t ≤ T`, so the
   owner *knows* it still holds `claim(i)` for `i ∈ [t, B)` — in particular
   `claim(b)` whenever `t ≤ b`, which is what licenses the **no-CAS** read of
   `buf[b]`.

> ### ⚠ PROOF-INSTANT vs RUNTIME — the authoritative runtime form of INV_race
>
> Clause 2's whole-segment ownership `⊛_{i∈[Tc,B)} claim(i)` (equivalently
> "`∀ i∈[t,B): owner owns i` at the fenced read") is a **linearization-INSTANT
> invariant**: it holds at the LP, and is what the *proof* threads. It is
> **NOT a runtime-checkable predicate** and must never be asserted as a runtime
> monitor. Reason: between the owner's fenced `top`-read and any later
> observation, thieves *legitimately* claim indices in `[t, b-1]` (the owner only
> takes `b`), so the whole-segment assertion fails on **correct** code. CBMC
> confirms this — the whole-segment assert FAILS on the real `cldeque.c` while
> the loop-unwind assertion and every per-claim assertion pass
> (`verify/cbmc/cldeque_disjoint.c`, diagnosed 2026-06-01).
>
> **The authoritative RUNTIME form of INV_race (use this; it is what
> `cldeque_disjoint.c` checks and what any CBMC/Spin port must use):**
> 1. **Per-claim disjointness + TAKEN-once.** At every claim of index `i`
>    (thief steal CAS, owner pop CAS, or owner no-CAS take): `owner_of[i] ==
>    OWNER` immediately before, then set `TAKEN`; and `taken[i]` ends `≤ 1`.
>    This catches *any* double-claim (two thieves, or thief+owner).
> 2. **No-contention boundary check.** At the fenced read, `if (t < b) assert
>    owner_of[b] == OWNER` — *only* index `b`, not the segment. In the
>    no-contention branch `top` can never reach `b` while the owner holds
>    `bottom = b`, so `b` stays owner-owned; this is the clause-(4) consequence
>    that **depends on the SC ordering** (a stale `bottom` would let a thief
>    reach `b`).
>
> Slogan: *the whole segment is the proof's device; index `b` is the monitor's
> checkable shadow.*

**The single fact that closes the race is (4):** the SC fence turns the owner's
post-decrement read of `top` into a *sound lower bound* on the real top, making
the owner's retained-claim set `[t,B)` **disjoint** from the thieves' claimable
set `[·,t)`. Drop the fence and (4) fails: `t` may undercount the real top, the
two sets overlap at index `b`, both the owner and a thief hold `claim(b)` →
duplication. *This is precisely the mechanised `chase_lev2 -DBUG_NO_FENCE`
counterexample.*

### Prophecy or helping?

- `take` last-element branch (`t==b`, CAS): LP is take's own CAS — **no prophecy**.
- `take` no-CAS branch (`t<b`): LP is the SC-fenced `top.acquire` read, which
  certifies `claim(b)` is still the owner's — LP in take's own code, **no
  prophecy**.
- The genuinely future/external part is a **losing or empty `steal`**: its
  return-EMPTY/ABORT observation linearises relative to *its own view*, and under
  RC11 two concurrent steals can validly observe "empty"/"nonempty" in
  incomparable views — there is no global instant to pin it to.

→ **Resolution: helping + view-relative abstract state, NOT prophecy.** The
future-dependence lives only in the *return-EMPTY* observations, which are
**no-ops** on the abstract state, so they need a view-relative empty-predicate,
not a resolved future value. Every *value-returning* LP is pinned by a one-shot
`top`-CAS token (take's own CAS, or the SC-fence-certified bottom claim). The
take-last-element/steal interaction uses **helping**: the CAS winner commits the
loser's abstract no-op.

---

## 5. What is mechanised, what is dodged

- **Mechanised (`StealClaim.v`, axiom-free iRC11/RC11):** a faithful slice of
  `cl_new`/`cl_push`/`cl_steal` — owner pushes one element and **release**-
  publishes `bot`; **two thieves** each run the real `cl_steal` (relaxed `top`
  load, **acquire** `bot` load, **relaxed** `top` CAS); each returns `NONE` or
  exactly `42`. Proves §2(b) for **steal/steal + push** under RC11: no
  double-steal, no stale/uninitialised read. The §4 token-seating device
  (`claim(i)` in `top`'s CAS protocol, escrow in `bot`'s release interp) is the
  load-bearing part and it type-checks.
- **Dodged in iRC11 (honest):**
  - `take` and the **SC fence** — needs a *two-location seq_cst-fence
    ghost-transfer rule* gpfsl does not export (§6). `StealClaim.v` omits take.
  - **resize** in iRC11 — modelled only in GenMC (§ below); the separation-logic
    proof of grow/copy/switch is multi-week and not attempted.
  - Unbounded index range / FIFO order — `StealClaim.v` is the single-index
    fragment; INV_race (§4) is stated for all indices but proven only at one.
- **Resize: MODELLED (GenMC), not proved.** `chase_lev_resize.c` models
  allocate-bigger + copy-live `[top,bot)` + **release**-switch the array pointer,
  **concurrent** with thieves reading old/new arrays. Covered: the copy/read race
  and exactly-once across the switch (clean under RC11). **Dodged:** reclamation
  (the simplified deque leaks the old array, as gpfsl's does) and repeated growth
  (one grow A→B).

---

## 6. Stuck obligation (file:line)

The precise wall for the take/steal proof in iRC11:

- **Need:** a rule that turns the *pair* of SC fences (owner:
  `bot.relaxed:=b ; fence_sc ; top.acquire`; thief:
  `top.relaxed ; fence_sc ; bot.acquire`) into ghost ownership: namely INV_race
  (4), `t ≤ T_real`, as a separation-logic entailment that transfers `claim`
  tokens. gpfsl's GPS protocols are **per-location** release/acquire; the SC fence
  relates **two** locations (`bot` and `top`) jointly, and gpfsl/iRC11 does not
  export a cross-location `seq_cst`-fence transfer lemma.
- **Where it would land:** analogous to `StealClaim.v:210` (the `GPS_iSP_Read`
  acquire on `bot`); the take proof's `top.acquire` step would need this lemma
  instead of a plain GPS read. That step does not exist in `StealClaim.v` because
  take is omitted.
- **Classification:** missing library ingredient, not a bug. This is the
  "research-grade" gap; it is *not* closed by strengthening to SC (the algorithm
  already uses SC *fences*; what is missing is the *logic rule* for them).

---

## 7. Checkpoint report (the requested shape)

- **Goal as currently stated:** logical atomicity (atomic triples) + contextual
  refinement of the §2 atomic spec, under RC11 — *not* classic linearizability
  (reframed, §1, with the global-total-order reason written down).
- **Spec (what it forbids):** §2 — (a) loss and (b) duplication, as one-shot
  `st`/`claim` invariants; shown non-vacuous by three broken impls (§2 table).
- **Access modes assumed vs real:** identical to the real code (§3 table); fence
  map machine-checked (§3). **No SC strengthening** beyond the algorithm's own
  two SC fences.
- **Resize:** MODELLED in GenMC (grow/copy/switch concurrent with thieves, clean;
  relaxed-pointer control finds the race). NOT proved in iRC11. Reclamation
  dodged (documented).
- **Stuck obligations (file:line):** the two-location seq_cst-fence
  ghost-transfer rule, landing at the take proof's `top.acquire`
  (cf. `StealClaim.v:210`); §6.
- **SC/strengthening compromises:** none.

### Definition of done — the portable invariant (DONE: `verify/cbmc/cldeque_disjoint.c`)

INV_race ports to a CBMC monitor on the **real C deque** (the zero-cost
`PYGO_CLDEQUE_VERIFY` ghost hooks in `cldeque.c`):

- ghost `owner_of[i] ∈ {OWNER, TAKEN}`; `push` → OWNER; each claim → TAKEN.
- per-claim disjointness + TAKEN-once: `pygo_cl_claim(i)` asserts
  `owner_of[i]==OWNER` then sets TAKEN and asserts `taken[i]==1`. Catches ANY
  double-claim (two thieves, or thief+owner). **CBMC: verified on `cldeque.c`.**
- boundary disjointness at the fenced read: `if (t<b) assert owner_of[b]==OWNER`
  — in the no-contention branch the owner provably still owns `b`; this is the
  part that **depends on the SC ordering** (a stale `bottom` would let a thief
  reach `b`). **CBMC: verified.** Teeth: a `-DBUG_SELFTEST` double-claim trips
  the monitor (it is not vacuous), and CBMC's `--unwinding-assertions` rules out
  vacuity-by-truncation.

**Finding (proof-instant vs runtime-checkable).** The full §4 predicate
"`∀ i∈[t,B): owner owns i` at the fenced read" is a *linearization-instant*
invariant — true at the LP, but **NOT a valid runtime monitor**: between the
owner's fenced `top`-read and any later observation, thieves *legitimately*
claim indices in `[t, b-1]` (the owner only takes `b`). CBMC confirms this — the
whole-segment assertion FAILS on the correct code (and the failure is the
segment assertion alone, with the loop-unwind assertion and the per-claim
assertions all passing). The runtime-robust port is therefore the **boundary**
form above (index `b` only) + per-claim TAKEN-once. This refines §4: the segment
ownership is the *proof's* device; the *monitor's* checkable shadow is the
boundary claim. The GenMC harnesses already encode TAKEN-once (`got[v]`); this
CBMC monitor adds the SC-ordering-dependent boundary check on the real code.

---

## 8. Gate 2 — production diff & bug-check (the finding most wanted)

Diffed the GenMC models against the shipped `src/pygo_core/cldeque.c` line by
line, and drove the **real cldeque.c** under GenMC verbatim
(`verify/genmc/chase_lev_real.c`, CAP=4, 2-elt pop + 2 steals): **No errors, 29
executions.** Verdict: **no live bug — production does NOT skimp.**

| step | production `cldeque.c` | min model | verdict |
|---|---|---|---|
| push load bottom | relaxed (`:19`) | relaxed | match |
| push load top | **acquire** (`:20`, cap check) | n/a | prod stronger; guards circular reuse |
| push write buf | non-atomic (`:22`) | non-atomic | match |
| push store bottom | release (`:23`) | release | match |
| pop store bottom=b | **SEQ_CST** (`:33`) | relaxed + SC fence | **equivalent StoreLoad** |
| pop load top | **SEQ_CST** (`:34`) | acquire (post-fence) | **equivalent** |
| pop read buf | non-atomic (`:40`) | non-atomic | match |
| pop CAS top (last) | SEQ_CST (`:48`) | relaxed | prod stronger |
| steal load top | **acquire** (`:62`) | relaxed | prod stronger |
| steal fence | **SEQ_CST** (`:65`) | SEQ_CST | match |
| steal load bottom | acquire (`:66`) | acquire | match |
| steal read buf | non-atomic (`:68`) | non-atomic | match |
| steal CAS top | SEQ_CST (`:71`) | relaxed | prod stronger |

**Findings:**
1. **Every ordering the fence map flags as NECESSARY is present in production.**
   The ≥2-element StoreLoad ordering in `pop` is provided by the **SC store of
   bottom + SC load of top** (`cldeque.c:33-34`) — the SC-atomics formulation,
   equivalent to the model's relaxed-store + SC-fence + acquire-load. The
   `steal` load-load ordering is an **explicit `SEQ_CST` fence** (`:65`). So the
   `-DBUG_NO_FENCE` duplication is **not** reachable in the shipped code.
2. **Resize finding is N/A to pygo.** `cldeque.c` is fixed-cap circular
   (`PYGO_CLDEQUE_CAP`, `& MASK`; `push` returns -1 when full). It never grows
   (header: *"growable if needed later"*). The rel/acq-on-array-pointer race
   (`chase_lev_resize.c -DBUG_RLX_ARR`) applies only to the growable variant — a
   **live constraint to honour IF/when growth is added**, not a current bug.
3. **Three safe over-synchronizations** (steal top-load acquire, both CASes
   SEQ_CST) — see `verify/genmc/PERF_OBSERVATIONS.md`. Correct; perf-phase only.
4. **Model abstraction gap, closed by the real-code run.** The hand models are
   non-circular; production is circular with slot reuse + a `push` cap-check
   acquire (`:20`). The verbatim-cldeque.c run exercises both and is clean.

## 9. Frontier — the missing cross-location SC-fence rule (standalone problem)

*Banked for upstream, not to build now.* The take/steal iRC11 QED needs one
ingredient gpfsl does not export:

- **What gpfsl exports:** per-location persistent protocols (`surface_iSP`,
  `surface_iPP`) with release/acquire reads/writes/CAS, escrows, and the GPS
  invariant machinery. Reads/writes synchronise *one* location.
- **What is absent:** a rule relating a **`seq_cst` fence pair across two
  locations**. Concretely: owner does `bot.store(b, rlx); fence_sc;
  t := top.load(acq)` and a thief does `t' := top.load(rlx); fence_sc;
  b' := bot.load(acq)`. RC11's `[SC] ; fence_sc ; ...` gives a `psc`-based
  ordering forcing `t ≤ T_real` (and dually `b' ≥ B` or the thief sees the
  decrement). There is no gpfsl lemma turning that `psc` ordering into
  separation-logic ghost ownership transfer (the `claim` tokens of §4).
- **Where it would seat:** the take proof's `top.load(acq)` step, analogous to
  `StealClaim.v:210` (the `GPS_iSP_Read` acquire on `bot`). `StealClaim.v` omits
  `take` precisely because this step has no rule.
- **Shape of the wanted lemma (informal):**
  `GPS_iSP_Writer bot (val b) ∗ GPS_Reader top t  ⊢  {SC-fence} ⟹
   GPS_Reader top t' ∗ ⌜t ≤ t'⌝` where `t'` is a lower bound certified across the
  fence — i.e. an SC-fence variant of `GPS_iSP_SWWriter_latest` spanning the
  *other* protocol. This is iRC11 metatheory (gpfsl/orc11 `psc` reasoning), a
  candidate to raise with the gpfsl authors, **not** pygo work.

## Files

- `StealClaim.v` — iRC11/RC11 proof, steal/steal + push, axiom-free
  (`Closed under the global context`). Build: `./build.sh`.
- `../../genmc/chase_lev.c` — single-element take/steal race (+ `-DBUG_NO_CAS`,
  `-DBUG_NO_FENCE`).
- `../../genmc/chase_lev2.c` — two-element fence-necessity (+ `-DBUG_NO_FENCE`).
- `../../genmc/chase_lev_resize.c` — grow/copy/switch (+ `-DBUG_RLX_ARR`).
- `../../genmc/chase_lev_real.c` — drives the REAL `src/pygo_core/cldeque.c`
  verbatim under GenMC (Gate 2). Clean, 29 executions.
- `../../genmc/run_chase_lev.sh` — runs all 8 GenMC checks (8/0).
- `../../genmc/PERF_OBSERVATIONS.md` — over-synchronizations / fence-removal
  candidates (optimization-phase only; do not act).
- `../../cbmc/cldeque_disjoint.c` — INV_race segment-disjointness + TAKEN-once
  monitor on the real `cldeque.c` (CBMC).
