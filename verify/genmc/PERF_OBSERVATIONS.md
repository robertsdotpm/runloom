# cldeque — perf observations (DO NOT ACT until the optimization phase)

Surfaced while diffing the production `src/pygo_core/cldeque.c` access modes
against the GenMC/RC11 fence map. These are **safe over-synchronizations**: the
code is correct; these are spots where a *weaker* order would still verify, i.e.
candidate hot-path fence removals. Logged per [[optimize_after_working]] —
**do not touch the fences until the optimization phase is explicitly opened.**

All claims below are backed by the GenMC oracle (`run_chase_lev.sh`).

1. **`steal` CAS is `SEQ_CST`; relaxed suffices.**
   `cldeque.c:71-73` uses `__ATOMIC_SEQ_CST` for the success order of the top
   CAS. gpfsl's reference (`gpfsl-examples/chase_lev/code.v`) uses a **relaxed**
   CAS here, with the comment *"Relaxed CAS is good enough because we already
   synchronized using the acquire read of b above."* The preceding SC fence +
   acquire-load of `bottom` already provide the needed ordering. Hot path on
   every steal.

2. **`pop` last-element CAS is `SEQ_CST`; relaxed (fail) / release-ish (success)
   is the theoretical floor.** `cldeque.c:48-50`. Only reached on the
   single-element race, so lower value than (1).

3. **`steal` top-load is `ACQUIRE`; relaxed + the existing SC fence suffices.**
   `cldeque.c:62`. The reference uses a relaxed top-load (the SC fence does the
   ordering). On x86/TSO this is free; on ARM it is a real `ldar` per steal.

4. **`push` top-load is `ACQUIRE`** (`cldeque.c:20`) for the capacity check.
   This one is load-bearing for the **circular buffer**: it stops `push` from
   overwriting a slot whose element a thief has not yet consumed. Do **not**
   weaken without re-checking slot-reuse safety (the GenMC models abstract the
   circular buffer away; the real-code run `chase_lev_real.c` exercises it).

5. **SC fence is redundant at one contended element** (the user's observation):
   when only the last element is contended, the top-CAS alone arbitrates
   take vs steal — GenMC `chase_lev.c -DBUG_NO_FENCE` is *clean*. **BUT** the
   fence is NECESSARY at ≥2 elements (`chase_lev2.c -DBUG_NO_FENCE` →
   duplication). So this is **not** a safe removal in general; it would only be
   sound under a proof that the deque is never accessed with ≥2 elements while a
   pop races a steal — which is not an invariant pygo maintains. Logged as an
   observation; **the fence stays.**

### Net
The minimum-order Chase-Lev (per Lê et al. / gpfsl) is: push{relaxed,acq,rel},
steal{relaxed-top, SCfence, acq-bottom, relaxed-CAS}, pop{relaxed, SCfence (or
SC store+SC load), acq-top, relaxed-CAS}. Production matches the *ordering*
strength everywhere it matters and is *stronger* in (1)-(3). Revisit (1) and (3)
first in the optimization phase (biggest hot-path wins, especially on ARM).
