/*
 * tstate_attach_detach.pml -- Promela model of the per-g PyThreadState
 * attach/detach BALANCE in the RUNLOOM_PER_G_TSTATE resume block
 * (mn_sched_hub_main.c.inc, the `if (runloom_get_per_g_tstate_mode())` branch,
 * ~lines 614-735).
 *
 * Life-cycle invariant (NOT a memory-ordering property -- a control-flow
 * ownership balance across the ~6 exit paths of one hub-loop iteration):
 *
 *   The hub thread owns exactly ONE attached tstate at every loop top, and it
 *   is the HUB's own tstate.  A resume runs Python ONLY with the G's tstate
 *   attached.  Every path back to the loop top -- the from_runq claim-fail
 *   skip, the dead-g skip, and the done / parked-offqueue / yield completions
 *   -- must leave (current == HUB, depth == 1).  A mis-ordered early `continue`
 *   placed AFTER RestoreThread(g) but before the Save(g)+Restore(hub) pair
 *   would leave the G tstate attached (or none) at the next loop top: the next
 *   iteration's SaveThread then detaches the WRONG tstate, and a per-g tstate
 *   is left live on a thread that will free it elsewhere -- the BUG-#2 /
 *   "cross-hub tstate" UAF class.  This is the structural seam the whole per-g
 *   mode rests on, and (unlike the mimalloc heap handshake) it is checkable now.
 *
 * The C structure being modelled (verbatim shape):
 *
 *   while (...) {                          // loop top: current==HUB, depth==1
 *     ... drain subs / timers ...
 *     g = pick();
 *     if (from_runq && !CAS(QUEUED->RUNNING)) { decref; continue; }   // pre-attach skip
 *     if (g->coro==NULL || g->done)        { ...; continue; }         // pre-attach skip
 *     hub_ts = PyEval_SaveThread();        // HUB -> none
 *     PyEval_RestoreThread(g->tstate);     // none -> G
 *     resume(g);                           // runs Python: requires current==G, depth==1
 *     PyEval_SaveThread();                 // G -> none
 *     PyEval_RestoreThread(hub_ts);        // none -> HUB
 *     ... done / parked_offqueue / yield handling (all post-restore) ...
 *   }                                      // back to loop top: current==HUB, depth==1
 *
 * Proven (all interleavings of the nondeterministic per-iteration outcome):
 *   BALANCE -- at every loop top current==HUB && depth==1 (assert).
 *   SAFE-RESUME -- resume executes only at current==G && depth==1 (assert).
 *
 * Negative control (-DBUG_EARLY_CONTINUE_AFTER_ATTACH, must FAIL): a skip path
 * taken AFTER RestoreThread(g) but before the Save(g)+Restore(hub) pair -> the
 * G tstate is still attached at the next loop top -> the loop-top BALANCE assert
 * fires (and a real run would Save the wrong tstate / strand a live per-g tstate).
 *
 * Single hub thread: the attach/detach is one thread's control flow, so this is
 * a bounded SEQUENTIAL reachability proof; Spin explores the nondeterministic
 * exit choice each iteration exhaustively.
 */

#define NONE 0
#define HUB  1
#define G    2

#define NITERS 4          /* hub-loop iterations to model */

mtype = { OUT_SKIP_CLAIM, OUT_SKIP_DEAD, OUT_DONE, OUT_PARKED, OUT_YIELD };

int cur   = HUB;          /* which tstate is attached (NONE/HUB/G) */
int depth = 1;            /* attach depth: must be exactly 1 with a tstate bound */

/* PyEval_SaveThread(): detach the current tstate (-> NONE, depth 0). */
inline save_thread() {
    assert(cur != NONE && depth == 1);    /* never Save with nothing/double attached */
    cur = NONE; depth = 0;
}
/* PyEval_RestoreThread(ts): attach ts (-> ts, depth 1). */
inline restore_thread(ts) {
    assert(cur == NONE && depth == 0);    /* never Restore over a live attach */
    cur = ts; depth = 1;
}

active proctype hub()
{
    int i = 0;
    int outcome;

    do
    :: (i < NITERS) ->
        i++;

        /* ---- loop top: the BALANCE invariant ---- */
        assert(cur == HUB && depth == 1);

        /* pick this iteration's outcome nondeterministically */
        if
        :: outcome = OUT_SKIP_CLAIM
        :: outcome = OUT_SKIP_DEAD
        :: outcome = OUT_DONE
        :: outcome = OUT_PARKED
        :: outcome = OUT_YIELD
        fi;

        /* pre-attach skips (from_runq claim-fail; dead/done g): NO Save/Restore
         * ran, so attach state is untouched -> straight back to loop top. */
        if
        :: (outcome == OUT_SKIP_CLAIM) -> goto iter_end
        :: (outcome == OUT_SKIP_DEAD)  -> goto iter_end
        :: else -> skip
        fi;

        /* the resume slice: swap hub tstate out, g tstate in */
        save_thread();                 /* HUB -> NONE  (hub_ts = PyEval_SaveThread) */
        restore_thread(G);             /* NONE -> G    (PyEval_RestoreThread(g->tstate)) */

#ifdef BUG_EARLY_CONTINUE_AFTER_ATTACH
        /* INJECTED BUG: a mis-ordered early continue after attaching G but
         * before detaching it -> G stays attached at the next loop top. */
        if
        :: goto iter_end              /* the bad skip */
        :: skip                       /* or fall through normally */
        fi;
#endif

        /* run Python: only legal with the g's own tstate attached, depth 1 */
        assert(cur == G && depth == 1);

        save_thread();                 /* G -> NONE   (PyEval_SaveThread) */
        restore_thread(HUB);           /* NONE -> HUB (PyEval_RestoreThread(hub_ts)) */

        /* post-restore handling (done / parked_offqueue / yield) does NOT touch
         * the attach state -- it is already back to HUB.  Model as no-ops. */
        if
        :: (outcome == OUT_DONE)   -> skip
        :: (outcome == OUT_PARKED) -> skip
        :: (outcome == OUT_YIELD)  -> skip
        :: else -> skip
        fi;

iter_end:
        skip
    :: (i >= NITERS) -> break
    od;

    /* quiescence: the hub exits its loop holding its own tstate, balanced. */
    assert(cur == HUB && depth == 1);
}
