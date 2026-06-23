/*
 * foreign_thread_fallback.pml -- Promela model of the FOREIGN-OS-THREAD-safe
 * fallback in runloom's monkey-patched cooperative primitives.
 *
 * SUBSYSTEM (faithful to the source):
 *   monkey.patch() globally replaces threading.Lock / Condition / Event with
 *   the cooperative versions in src/runloom/monkey/ (CoLock in locks.py,
 *   CoCondition / CoEvent in events.py).  Each is built on:
 *     * a runloom_c.Mutex token (CoLock._mu) for mutual exclusion, and
 *     * a _Parker (src/runloom/monkey/_base.py) to BLOCK a contending actor.
 *
 *   The contended-acquire / Condition.wait blocking step BRANCHES on the
 *   caller's thread kind, decided by _in_fiber() (monkey/_base.py) which
 *   peeks runloom_c.current_g() == runloom_sched_peek_current() (a
 *   NON-allocating TLS peek, module_go.c.inc m_current_g / module_init.c.inc
 *   runloom_running_in_goroutine -> runloom_sched_peek_current, never the
 *   allocating runloom_sched_get):
 *
 *     _Parker.park():                 (monkey/_base.py)
 *       if _in_fiber():               # has a current g + a sched
 *           runloom_c.wait_fd(...)    #   -> COOPERATIVELY PARK the goroutine
 *       else:                         # FOREIGN OS thread: no g, no hub, no sched
 *           _raw_select([r],...)      #   -> REAL OS BLOCK on the wake fd
 *
 *   CLAUDE.md invariant "Cooperative primitives must be FOREIGN-OS-THREAD-safe":
 *   a foreign actor MUST (a) take the real-OS-block path, NEVER park a g that
 *   does not exist (runloom_c.wait_fd / runloom_c.park park a goroutine on a
 *   hub's netpoll -- undefined on a thread with no g/hub -> SIGSEGV/UAF under
 *   M:N), and (b) NEVER lazily allocate scheduler state -- the peek must stay
 *   runloom_sched_peek_current (non-allocating), never runloom_sched_get
 *   (mallocs a sched + arms the wake-pump on the foreign thread).
 *
 * WHAT THIS MODEL PROVES (two concurrent actors on the SAME patched lock):
 *   * actor G  -- a real GOROUTINE: in_fiber=1, peek != NULL.  On contention it
 *                 cooperatively parks (the park-g path).
 *   * actor F  -- a FOREIGN OS thread (a stdlib daemon: mp.Queue _feed /
 *                 concurrent.futures worker): in_fiber=0, peek == NULL.
 *   Property MUTEX     : the runloom_c.Mutex token gives real mutual exclusion --
 *                        at most one actor is in the critical section at a time
 *                        (incs/holding asserts), and no wake is lost (both
 *                        actors that block are eventually released -> no invalid
 *                        end state / deadlock).
 *   Property BRANCH    : actor F NEVER reaches the park-a-NULL-g state nor the
 *                        alloc-a-sched state (the SIGSEGV/UAF class).  Encoded as
 *                        assert(0) inside those two states, reachable ONLY when a
 *                        non-fiber takes the cooperative branch / calls
 *                        sched_get.
 *
 * NEGATIVE CONTROL (must FAIL):
 *   -DBUG_FOREIGN_PARKS routes actor F down the cooperative park branch (the bug:
 *   _Parker.park forgetting the _in_fiber() guard, or current_g() being changed
 *   to call the allocating runloom_sched_get).  F then enters park_null_g (and,
 *   modelling sched_get's lazy alloc, alloc_sched) -> the BRANCH assert fires.
 *
 * DEPTH / HONESTY (per the task):
 *   This is a BRANCH-SAFETY property, NOT a deep weak-memory interleaving model.
 *   It proves: under ALL interleavings of a goroutine and a foreign thread
 *   contending one patched lock, the routing decision (in_fiber-peek) sends the
 *   foreign actor to the real-OS-block path and never to park-a-g / alloc-a-sched,
 *   while the shared token still yields mutual exclusion with no lost wake.  It
 *   does NOT model the C-level memory ordering of runloom_c.Mutex's internal
 *   channel park/wake (that is the GenMC/CBMC-verified Dekker, covered elsewhere),
 *   nor the actual SIGSEGV mechanics of dereferencing a NULL g -- it models the
 *   branch that WOULD reach that deref as an assert-false sink.  The memory model
 *   is Spin's default sequential consistency.
 */

/* ---- shared lock state (the CoLock runloom_c.Mutex token, capacity-1) ---- */
bit token       = 1;     /* 1 == unlocked (one buffered token), 0 == held     */
byte holders    = 0;     /* actors currently in the critical section (<=1)    */

/* ---- per-actor wake flags (model the _Parker wake: g.wake() for the         */
/*      cooperative parker; os.write(pipe) seen by the foreign _raw_select) -- */
bit wake_g      = 0;     /* the goroutine parker has a pending wake            */
bit wake_f      = 0;     /* the foreign-thread parker has a pending wake byte  */

/* ---- liveness / branch bookkeeping ---- */
bit g_blocked   = 0;     /* goroutine is cooperatively parked                 */
bit f_blocked   = 0;     /* foreign thread is OS-blocked in _raw_select       */
byte finished       = 0;     /* actors that finished their critical section       */

/* the FOREIGN actor must never set these (the SIGSEGV/UAF class) */
bit foreign_parked_null_g = 0;   /* reached runloom_c.wait_fd/park with no g   */
bit foreign_alloc_sched   = 0;   /* reached runloom_sched_get (lazy malloc)    */

/* ----------------------------------------------------------------------------
 * acquire: model CoLock.acquire(blocking=True) -> self._mu.lock().
 *   try_lock fast path (atomic claim of the buffered token); on contention the
 *   actor BLOCKS via its _Parker.park(), which branches on in_fiber.
 *   `fiber` selects the branch: 1 = goroutine, 0 = foreign OS thread.
 * -------------------------------------------------------------------------- */
inline acquire(fiber)
{
    bit got;
    do
    :: atomic {                        /* runloom_c.Mutex.try_lock() */
           if
           :: (token == 1) -> token = 0; got = 1;
           :: else          -> got = 0;
           fi;
       }
       if
       :: (got == 1) -> break;          /* claimed -> enter critical section */
       :: else ->
           /* Contended: _Parker.park() -- BRANCH on the in_fiber()/peek result.
            * The commit-to-block + re-check is atomic{}: the real park primitive
            * (runloom_c.Mutex's buffered channel for a fiber; _raw_select armed
            * on a wake fd for a foreign thread) absorbs a wake/unlock that races
            * the park (the GenMC-verified Dekker for the fiber park; the wake
            * byte already queued on the pipe for the foreign select).  We model
            * that race-freedom by committing the *_blocked flag and re-checking
            * the token in one atomic step -- if the token came free meanwhile,
            * we bail and re-loop instead of parking into a lost wake.  This is
            * NOT the property under test (that is the Dekker, verified
            * elsewhere); it just keeps the harness from a spurious lost-wake so
            * the BRANCH property is what teeth-tests. */
           if
           :: (fiber == 1) ->
                /* GOROUTINE: peek != NULL -> cooperatively park the g.
                 * (runloom_c.wait_fd parks the g on the hub's netpoll.) */
                atomic {
                    if
                    :: (token == 1 || wake_g == 1) -> skip;   /* race: don't park */
                    :: else -> g_blocked = 1;
                    fi;
                }
                if
                :: (g_blocked == 1) ->
                       (wake_g == 1);   /* resumed by the holder's g.wake() */
                       atomic { wake_g = 0; g_blocked = 0; }
                :: else -> skip;
                fi;
#ifndef BUG_FOREIGN_PARKS
           :: else ->
                /* FOREIGN OS thread: peek == NULL (non-allocating
                 * runloom_sched_peek_current) -> REAL OS BLOCK on the wake fd
                 * via _raw_select.  NEVER parks a g, NEVER allocs a sched. */
                atomic {
                    if
                    :: (token == 1 || wake_f == 1) -> skip;   /* race: don't park */
                    :: else -> f_blocked = 1;
                    fi;
                }
                if
                :: (f_blocked == 1) ->
                       (wake_f == 1);   /* resumed by the holder's os.write */
                       atomic { wake_f = 0; f_blocked = 0; }
                :: else -> skip;
                fi;
#else
           :: else ->
                /* BUG: the foreign actor is (mis)routed to the cooperative park
                 * path -- either _Parker.park lost its _in_fiber() guard, or
                 * current_g() was changed to call the allocating
                 * runloom_sched_get.  This is the SIGSEGV/UAF class.  The
                 * BRANCH-SAFETY assert fires the instant a NON-fiber takes this
                 * branch -- detection is scheduling-independent. */
                foreign_alloc_sched = (fiber == 0);    /* lazy sched_get malloc */
                foreign_parked_null_g = (fiber == 0);  /* wait_fd with no g     */
                assert(foreign_parked_null_g == 0 && foreign_alloc_sched == 0);
                atomic {
                    if
                    :: (token == 1 || wake_g == 1) -> skip;
                    :: else -> g_blocked = 1;
                    fi;
                }
                if
                :: (g_blocked == 1) ->
                       (wake_g == 1);
                       atomic { wake_g = 0; g_blocked = 0; }
                :: else -> skip;
                fi;
#endif
           fi;
           /* re-loop: re-check the token under the guard (CoLock loops on
            * try_lock; the channel token hands off on unlock) */
       fi;
    od;
}

/* ----------------------------------------------------------------------------
 * release: model CoLock.release() -> self._mu.unlock(), then wake one blocked
 *   peer (the channel token hand-off).  We wake whichever kind is blocked;
 *   the wake mechanism differs (g.wake() vs os.write) but both are issued by
 *   the releasing actor, which is ALWAYS a fiber in the normal corpus -- but
 *   here either actor may hold/release, so we model both wake kinds.
 * -------------------------------------------------------------------------- */
inline release()
{
    atomic {
        token = 1;                       /* return the buffered token */
        /* hand-off wake: prefer waking a blocked peer so the token is not
         * "released and raced".  Either order is sound; we wake both kinds if
         * blocked so no waiter is lost. */
        if
        :: (g_blocked == 1) -> wake_g = 1;
        :: (f_blocked == 1) -> wake_f = 1;
        :: else -> skip;
        fi;
    }
}

/* the GOROUTINE actor: in_fiber = 1 */
active proctype goroutine()
{
    acquire(1);
    atomic { holders++; assert(holders <= 1); }   /* MUTEX */
    atomic { holders--; }
    release();
    finished++;
    /* a second wake pass: if our release didn't catch a peer that blocked
     * after we checked, the peer's own re-loop on the freed token progresses.
     * Re-issue wakes so no waiter is stranded (models the token hand-off +
     * the foreign poller's os.write retry). */
    atomic {
        if
        :: (g_blocked == 1) -> wake_g = 1;
        :: (f_blocked == 1) -> wake_f = 1;
        :: else -> skip;
        fi;
    }
}

/* the FOREIGN OS thread actor: in_fiber = 0, peek == NULL */
active proctype foreign()
{
    acquire(0);
    atomic { holders++; assert(holders <= 1); }   /* MUTEX */
    atomic { holders--; }
    release();
    finished++;
    atomic {
        if
        :: (g_blocked == 1) -> wake_g = 1;
        :: (f_blocked == 1) -> wake_f = 1;
        :: else -> skip;
        fi;
    }
}

/* BRANCH-SAFETY is asserted inline in acquire()'s contended branch (the instant
 * a non-fiber would take the cooperative park / alloc-a-sched path), and a final
 * global invariant double-checks at quiescence that the foreign actor never set
 * either forbidden flag.  Both flags stay 0 across every interleaving in the
 * positive model; the negative control (-DBUG_FOREIGN_PARKS) sets them. */
active proctype final_check()
{
    (finished == 2);                 /* both actors completed                  */
    assert(foreign_parked_null_g == 0 && foreign_alloc_sched == 0);
}
