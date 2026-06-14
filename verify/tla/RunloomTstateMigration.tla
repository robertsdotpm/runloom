------------------------- MODULE RunloomTstateMigration -------------------------
(***************************************************************************)
(* TLA+ model of Tier-1 #2 -- the per-g PyThreadState / mimalloc-heap        *)
(* MIGRATION hazard (RUNLOOM_PER_G_TSTATE; the crash this session found and    *)
(* gated off in commit 70e6ddb).  Companion to RunloomGilstate.tla (per-      *)
(* thread tstate binding) and RunloomCPythonSTW.tla (attach/STW): this module  *)
(* adds the one atom those lack -- mimalloc's per-PAGE owner thread -- and      *)
(* proves the abandon/adopt handshake at the park/resume boundary is           *)
(* NECESSARY for a tstate to migrate hubs without corrupting its heap.         *)
(*                                                                            *)
(* Ground truth (docs/dev/STEAL_WOKEN_CLEANUP.md; gold-standard TSan):         *)
(* under per-g-tstate a fiber's PyThreadState carries its OWN mimalloc heap.    *)
(* Free-threaded CPython binds that heap's pages to ONE OS thread -- mimalloc   *)
(* keys page ownership on _Py_ThreadId (MI_PRIM_THREAD_ID), and the local-free  *)
(* fast path + _mi_page_retire assume the OPERATING thread == the page owner.   *)
(* runloom migrates the tstate hub->hub (attach on hub A, alloc, park/detach,   *)
(* re-attach + alloc/retire on hub B) WITHOUT the mimalloc segment abandon/     *)
(* adopt handshake CPython runs at real thread create/exit -- so a page         *)
(* allocated on A is later retired on B, owner != operating thread -> the heap  *)
(* bookkeeping drifts and teardown SEGVs in _mi_page_retire.                    *)
(*                                                                            *)
(* Abstraction + HONEST SCOPE: pageOwner is the mimalloc owner-thread atom;     *)
(* a "page op" stands for a local-free / page-retire that takes the owner-only  *)
(* fast path.  CONSTANT Handshake = TRUE installs the fix -- abandon-on-detach  *)
(* (the g's pages -> the Abandoned pool) and adopt-on-attach (Abandoned pages   *)
(* -> the attaching thread) BEFORE any alloc/retire on the new hub.  This model *)
(* proves the handshake is NECESSARY (without it, a cross-thread page op is     *)
(* reachable) and that its PLACEMENT (abandon precedes detach; adopt precedes   *)
(* any page op) is SUFFICIENT at the ownership abstraction.  It does NOT prove  *)
(* sufficiency against mimalloc's internal ordering -- pair with the runtime    *)
(* (page, _Py_ThreadId()) assert oracle (LIFECYCLE_INVARIANTS.md, deep         *)
(* surfaces) before trusting a candidate C handshake.                          *)
(***************************************************************************)

CONSTANTS Hubs,        \* set of hub OS-thread ids the per-g tstate can run on
          Pages,       \* set of mimalloc pages the g's heap can hold
          Handshake    \* TRUE = abandon-on-detach + adopt-on-attach (fix); FALSE = bug

None      == "none"        \* the tstate is parked / attached to no thread
Abandoned == "abandoned"   \* page is in the interp abandoned pool (owned by no thread)
Unalloc   == "unalloc"     \* page not yet allocated
Owners    == Hubs \cup {Abandoned, Unalloc}

VARIABLES
    attachedHub,  \* Hubs \cup {None} : hub the per-g tstate is currently bound to
    pageOwner,    \* [Pages -> Owners] : mimalloc per-page owner thread (xthread_id)
    corrupt       \* history: a page op ever ran on a thread that is not the page owner

vars == <<attachedHub, pageOwner, corrupt>>

TypeOK ==
    /\ attachedHub \in Hubs \cup {None}
    /\ pageOwner \in [Pages -> Owners]
    /\ corrupt \in BOOLEAN

Init ==
    /\ attachedHub = None                       \* g spawned, not yet resumed
    /\ pageOwner = [p \in Pages |-> Unalloc]     \* heap empty
    /\ corrupt = FALSE

\* Resume the g on hub h (PyEval_RestoreThread(g->tstate) on hub h's thread).
\* With the handshake, ADOPT: every abandoned page is re-owned by h BEFORE any
\* alloc/retire runs on h (mimalloc reclaims abandoned segments under the new
\* _Py_ThreadId).  Without it, owners are left as-is (the bug).
Attach(h) ==
    /\ attachedHub = None
    /\ h \in Hubs
    /\ attachedHub' = h
    /\ IF Handshake
         THEN pageOwner' = [p \in Pages |->
                              IF pageOwner[p] = Abandoned THEN h ELSE pageOwner[p]]
         ELSE pageOwner' = pageOwner
    /\ UNCHANGED corrupt

\* Allocate a page while running on the current hub: its owner is that hub
\* (mimalloc stamps the new page/segment with the operating _Py_ThreadId).
Alloc(p) ==
    /\ attachedHub # None
    /\ pageOwner[p] = Unalloc
    /\ pageOwner' = [pageOwner EXCEPT ![p] = attachedHub]
    /\ UNCHANGED <<attachedHub, corrupt>>

\* A page op (local-free / _mi_page_retire) on a live page, running on the
\* currently attached hub.  THE SAFETY EVENT: it must run on the page's owner;
\* if the owner is a different thread, the owner-only fast path corrupts.
PageOp(p) ==
    /\ attachedHub # None
    /\ pageOwner[p] \notin {Unalloc, Abandoned}
    /\ corrupt' = (corrupt \/ pageOwner[p] # attachedHub)
    /\ UNCHANGED <<attachedHub, pageOwner>>

\* Park the g (PyEval_SaveThread).  With the handshake, ABANDON: the g's pages
\* (owned by the detaching hub) go to the abandoned pool BEFORE the tstate can be
\* picked up by another hub.  Without it, the pages stay stamped with the old hub.
Detach ==
    /\ attachedHub # None
    /\ IF Handshake
         THEN pageOwner' = [p \in Pages |->
                              IF pageOwner[p] = attachedHub THEN Abandoned ELSE pageOwner[p]]
         ELSE pageOwner' = pageOwner
    /\ attachedHub' = None
    /\ UNCHANGED corrupt

Next ==
    \/ \E h \in Hubs : Attach(h)
    \/ \E p \in Pages : Alloc(p)
    \/ \E p \in Pages : PageOp(p)
    \/ Detach

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* SAFETY: no mimalloc page op ever runs on a thread that is not the page's
\* owner.  Holds under Handshake=TRUE (abandon/adopt keeps owner == operating
\* thread); the Handshake=FALSE control reaches a cross-thread op (alloc on hub A,
\* migrate, retire on hub B) -> the heap-corruption precondition.
NoCrossThreadPageOp == ~corrupt

\* STRONGER + earlier-tripping (the invariant the handshake maintains): while the
\* g is attached to a hub, NO page is still stamped with a DIFFERENT hub -- so the
\* owner-only fast path is always legal.  Holds under Handshake=TRUE (abandon-on-
\* detach + adopt-on-attach); the bug reaches a page owned by hub A while the
\* tstate is attached to hub B (a foreign-owner heap), which is the corruption
\* precondition even before the next page op runs.
NoForeignOwnerWhileAttached ==
    (attachedHub # None) =>
        \A p \in Pages : pageOwner[p] \notin (Hubs \ {attachedHub})
=============================================================================
