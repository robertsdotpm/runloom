---------------------------- MODULE RunloomGilstate ----------------------------
(***************************************************************************)
(* TLA+ model of M4 -- CPython's per-OS-thread GILState-TSS binding -- and  *)
(* the teardown bug it cost us (contract C6, found by the --with-pydebug    *)
(* oracle: Python/pystate.c:345 unbind_gilstate_tstate aborts).  Companion  *)
(* to RunloomCPythonSTW.tla (M1+M2): M4 is a teardown-lifecycle concern,     *)
(* orthogonal to the running/stop-the-world phase, so it is its own focused  *)
(* module (the repo keeps one concern per spec).                            *)
(*                                                                         *)
(* Interface state (read from Python/pystate.c):                           *)
(*   gilslot[t]   -- the tstate in OS-thread t's GILState-TSS slot, or Null  *)
(*   boundGil[ts] -- tstate ts's _status.bound_gilstate flag               *)
(*                                                                         *)
(* The two real transitions:                                               *)
(*   _PyThreadState_NewBound (pystate.c:1611): a thread that creates a       *)
(*     tstate binds it as THAT thread's gilstate tstate iff the thread's    *)
(*     slot is empty -- always true for a fresh hub OS thread.              *)
(*   tstate_delete_common -> unbind_gilstate_tstate (pystate.c:338,345):     *)
(*     clears the CALLING thread's slot and asserts the slot held the tstate *)
(*     being deleted.                                                       *)
(*                                                                         *)
(* Each OS thread owns exactly one tstate (a hub's, or the main thread's),   *)
(* so a tstate is identified with its owning thread.  THE BUG: a hub tstate, *)
(* created on -- and thus gilstate-bound to -- the hub thread, is deleted    *)
(* from the MAIN thread, so unbind clears the MAIN thread's slot, wiping the *)
(* main tstate's binding and tripping the assert.  CONSTANT DeleteOnOwner =  *)
(* TRUE is the fix (commit c28e5ca: the hub deletes its own tstate on its    *)
(* own thread); FALSE is the original bug (mn_fini deletes hub tstates from  *)
(* the main thread).                                                        *)
(***************************************************************************)

CONSTANTS Hubs,          \* set of hub OS-thread ids
          DeleteOnOwner  \* TRUE = the fix; FALSE = delete from the main thread (bug)

Main    == "main"
Threads == Hubs \cup {Main}
Tstates == Threads            \* tstate t is owned by / runs on thread t
Null    == "null"

VARIABLES
    created,   \* [Tstates -> BOOLEAN] : tstate has been created (once)
    live,      \* [Tstates -> BOOLEAN] : tstate exists (created, not yet deleted)
    boundGil,  \* [Tstates -> BOOLEAN] : _status.bound_gilstate
    gilslot,   \* [Threads -> Tstates \cup {Null}] : the thread's GILState-TSS slot
    assertOK   \* history: no unbind ever hit the wrong-thread assert (pystate.c:345)

vars == <<created, live, boundGil, gilslot, assertOK>>

TypeOK ==
    /\ created  \in [Tstates -> BOOLEAN]
    /\ live     \in [Tstates -> BOOLEAN]
    /\ boundGil \in [Tstates -> BOOLEAN]
    /\ gilslot  \in [Threads -> Tstates \cup {Null}]
    /\ assertOK \in BOOLEAN

Init ==
    \* Py_Initialize: the main tstate is live and gilstate-bound to the main thread.
    /\ created  = [t \in Tstates |-> t = Main]
    /\ live     = [t \in Tstates |-> t = Main]
    /\ boundGil = [t \in Tstates |-> t = Main]
    /\ gilslot  = [t \in Threads |-> IF t = Main THEN Main ELSE Null]
    /\ assertOK = TRUE

\* _PyThreadState_NewBound on thread h: create hub h's tstate; bind it as h's
\* gilstate tstate iff h's slot is empty (it is -- a fresh hub OS thread, so the
\* tstate ALWAYS becomes gilstate-bound to that thread).
Create(h) ==
    /\ h \in Hubs
    /\ ~created[h]
    /\ created' = [created EXCEPT ![h] = TRUE]
    /\ live'    = [live    EXCEPT ![h] = TRUE]
    /\ IF gilslot[h] = Null
         THEN /\ gilslot'  = [gilslot  EXCEPT ![h] = h]
              /\ boundGil' = [boundGil EXCEPT ![h] = TRUE]
         ELSE UNCHANGED <<gilslot, boundGil>>
    /\ UNCHANGED assertOK

\* tstate_delete_common with deleter = thread d.  If the tstate is gilstate-bound,
\* unbind_gilstate_tstate clears the DELETER's slot and asserts that slot held the
\* tstate -- so deleting from the wrong thread BOTH trips the assert (assertOK) AND
\* clears the wrong slot (the release-build corruption).
DoDelete(h, d) ==
    /\ live[h]
    /\ live' = [live EXCEPT ![h] = FALSE]
    /\ IF boundGil[h]
         THEN /\ assertOK' = (assertOK /\ gilslot[d] = h)   \* pystate.c:345 assert
              /\ gilslot'  = [gilslot  EXCEPT ![d] = Null]   \* clears the DELETER's slot
              /\ boundGil' = [boundGil EXCEPT ![h] = FALSE]
         ELSE UNCHANGED <<assertOK, gilslot, boundGil>>
    /\ UNCHANGED created

\* The fix (c28e5ca): a hub deletes its OWN tstate on its OWN thread (d = h).
DeleteOwner(h) == DeleteOnOwner /\ h \in Hubs /\ DoDelete(h, h)

\* The bug: mn_fini deletes a hub's tstate from the MAIN thread (d = Main).
DeleteFromWrongThread(h) == ~DeleteOnOwner /\ h \in Hubs /\ DoDelete(h, Main)

Done == \A h \in Hubs : created[h] /\ ~live[h]

Next ==
    \/ \E h \in Hubs : \/ Create(h)
                       \/ DeleteOwner(h)
                       \/ DeleteFromWrongThread(h)
    \/ (Done /\ UNCHANGED vars)        \* terminal self-loop (no false deadlock)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* SAFETY (the pydebug assert): unbind never fires from a thread whose slot does
\* not hold the tstate being deleted -- i.e. pystate.c:345 never aborts.
GilstateContract == assertOK

\* STRONGER (the release-build consequence the assert hides): every live,
\* gilstate-bound tstate is in ITS OWN thread's slot.  The wrong-thread delete
\* clears the main thread's slot while the main tstate is still live and bound
\* -> violated even with asserts compiled out.
GilBindingConsistent ==
    \A t \in Tstates : (live[t] /\ boundGil[t]) => gilslot[t] = t
=============================================================================
