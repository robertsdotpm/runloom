/*
 * cldeque.pml -- Promela model of the Chase-Lev work-stealing deque.
 *
 * Models src/runloom_c/cldeque.c:  one OWNER thread that push/pops at the
 * bottom, plus N THIEF threads that steal from the top.  Spin explores
 * every interleaving of the individual atomic memory operations under a
 * sequentially-consistent memory model.
 *
 * What is proven (over the bounded model: CAP=4, NITEMS=3, THIEVES=2):
 *
 *   1. NO DUPLICATION   -- no work-item is ever returned by two consumers
 *      (asserted at every successful pop/steal via the `claimed[]` bitset).
 *   2. NO LOSS          -- at quiescence every pushed item is either still
 *      logically in the deque ([top,bottom)) or has been consumed exactly
 *      once:   consumed + (bottom - top) == pushes_done.
 *   3. NO PHANTOM ITEM  -- a consumer never returns an empty (0) slot.
 *   4. NO DEADLOCK      -- the model has no invalid end states (every proc
 *      runs to its end label).
 *
 * The interesting races this covers:
 *   - pop-last-element  vs  steal        (both CAS `top`)
 *   - steal             vs  steal        (two thieves CAS `top`)
 *   - push (publishing an item)          vs  steal reading `bottom`
 *
 * Run:  spin -a cldeque.pml && cc -O2 -o pan pan.c && ./pan -m100000
 *
 * Faithfulness note: each line that touches a shared variable (top,
 * bottom, buf[]) is a separate Promela statement, so Spin interleaves
 * other threads between them -- exactly the granularity of the
 * __atomic_* ops in cldeque.c.  The compare-exchange is an atomic{}
 * block (read-compare-write is one indivisible op, as in the C CAS).
 * Because pushes (NITEMS) <= CAP there is no index wraparound, so a slot
 * is written exactly once -- which is also true of the real owner during
 * any window in which an item is steal-visible.
 */

#define CAP      4
#define NITEMS   3
#define THIEVES  2
#define TOTAL    (THIEVES + 1)   /* owner + thieves */

int  top    = 0;
int  bottom = 0;
int  buf[CAP];

bit  claimed[NITEMS + 1];        /* claimed[k]=1 once item k consumed (1-indexed) */
int  consumed = 0;               /* number of items consumed (popped or stolen)   */
int  pushes_done = 0;            /* items actually pushed by the owner             */
int  nfin = 0;                   /* processes that have reached their end          */

/* Record a successful consume of `item`; assert no duplication / phantom. */
inline consume(item) {
    atomic {
        assert(item != 0);                 /* never return an empty slot   */
        assert(claimed[item] == 0);        /* never returned twice         */
        claimed[item] = 1;
        consumed = consumed + 1;
    }
}

/* ---- OWNER: push at bottom, pop at bottom -------------------------- */
proctype owner()
{
    int b, t, item, casok;
    int prog = 0;        /* drives a representative push/pop schedule */

    do
    /* push next item while items remain */
    :: (prog == 0 || prog == 1 || prog == 3) ->
        /* push(prog_item) */
        b = bottom;                        /* relaxed load bottom */
        t = top;                           /* acquire load top    */
        if
        :: (b - t >= CAP) -> skip;         /* full: drop (won't happen here) */
        :: else ->
            item = pushes_done + 1;
            buf[b] = item;                 /* store buf slot (publish)        */
            bottom = b + 1;                /* release store bottom            */
            pushes_done = pushes_done + 1;
        fi;
        prog++;

    /* interleave pops */
    :: (prog == 2 || prog == 4 || prog == 5 || prog == 6) ->
        /* pop() */
        b = bottom - 1;                    /* local */
        bottom = b;                        /* SEQ_CST store bottom */
        t = top;                           /* SEQ_CST load  top    */
        if
        :: (t > b) ->                      /* deque empty */
            bottom = t;                    /* reset bottom = top   */
            /* returns EMPTY */
        :: else ->
            item = buf[b];                 /* read candidate item  */
            if
            :: (t < b) ->                  /* >1 element: no contention */
                consume(item);
            :: else ->                     /* t == b: last element, race */
                atomic {                   /* CAS top: t -> t+1 */
                    if
                    :: (top == t) -> top = t + 1; casok = 1;
                    :: else -> casok = 0;
                    fi;
                }
                if
                :: (casok == 1) ->
                    bottom = t + 1;
                    consume(item);
                :: else ->
                    bottom = t + 1;
                    /* lost race: thief got it, returns EMPTY */
                fi;
            fi;
        fi;
        prog++;

    :: (prog == 7) -> break;
    od;

end_owner:
    atomic {
        nfin = nfin + 1;
        if
        :: (nfin == TOTAL) ->
            assert(consumed + (bottom - top) == pushes_done);
        :: else -> skip;
        fi;
    }
}

/* ---- THIEF: steal from top ---------------------------------------- */
proctype thief()
{
    int t, b, item, casok;
    int tries = 0;

    do
    :: (tries < 6) ->
        tries++;
        t = top;                           /* acquire load top    */
        /* SEQ_CST fence is implicit between the two loads here:    */
        b = bottom;                        /* acquire load bottom  */
        if
        :: (t >= b) -> skip;               /* empty / lost -> NULL */
        :: else ->
            item = buf[t];                 /* read candidate item  */
            atomic {                       /* CAS top: t -> t+1    */
                if
                :: (top == t) -> top = t + 1; casok = 1;
                :: else -> casok = 0;
                fi;
            }
            if
            :: (casok == 1) -> consume(item);
            :: else -> skip;               /* lost race -> NULL    */
            fi;
        fi;
    :: (tries >= 6) -> break;
    od;

end_thief:
    atomic {
        nfin = nfin + 1;
        if
        :: (nfin == TOTAL) ->
            assert(consumed + (bottom - top) == pushes_done);
        :: else -> skip;
        fi;
    }
}

init {
    atomic {
        run owner();
        run thief();
        run thief();
    }
}
