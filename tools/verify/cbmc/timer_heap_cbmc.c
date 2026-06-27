/* SOURCE-ANCHOR: runloom_dh_sift_up runloom_dh_sift_down runloom_dh_insert runloom_dh_remove runloom_dh_peek_deadline runloom_dh_swap  (guards this hand-model vs src drift; tools/verify/model_source_drift.py) */
/*
 * timer_heap_cbmc.c -- CBMC bounded proof of the netpoll per-pool DEADLINE
 * MIN-HEAP (the sleep/timer heap), the data structure that orders parked
 * goroutines by their wake deadline so the pump knows the earliest timeout.
 *
 * FAITHFUL SLICE of the heap mechanics in src/runloom_c/netpoll_parkers.c.inc:
 *   runloom_dh_swap        -- swap two slots, fix BOTH moved nodes' heap_index
 *   runloom_dh_sift_up     -- bubble a node toward the root while < parent
 *   runloom_dh_sift_down   -- bubble a node toward leaves while > best child
 *   runloom_dh_insert      -- append at tail, set heap_index, sift up
 *   runloom_dh_remove      -- arbitrary remove BY heap_index (the prime target):
 *                             overwrite slot i with the tail, fix its index,
 *                             then sift up AND down (could go either way)
 *   runloom_dh_peek_deadline -- the min (root) deadline, -1 if empty
 *
 * The real ops run single-owner under pool->lock (no concurrency), so this is a
 * SEQUENTIAL proof.  The real structure stores `runloom_parked_t *dh_arr[]` and
 * each parker keeps `p->heap_index`.  Here a node's IDENTITY is its slot in the
 * fixed `key[]`/`hidx[]` arrays (a small int id) instead of a heap pointer, and
 * `arr[slot]` holds that id -- a behaviour-preserving renaming of the pointer
 * model (CBMC reasons about plain integer arrays, not pointer aliasing, so the
 * bounded check stays tractable).  Concretely:
 *     real:  pool->dh_arr[slot] = p;   p->heap_index = slot;
 *     here:  arr[slot] = id;           hidx[id]      = slot;
 * key[id] is the node's deadline_ns; hidx[id] is its heap_index back-pointer
 * (-1 when not in the heap).  Every line of the swap/sift/insert/remove logic is
 * transcribed verbatim under this renaming.
 *
 * What is proven, after ANY sequence of insert / pop-min / arbitrary-remove:
 *   (1) MIN-HEAP PROPERTY -- every parent's deadline <= each child's.
 *   (2) PEEK CORRECTNESS  -- dh_peek_deadline returns the true array minimum.
 *   (3) INDEX CONSISTENCY -- for every live slot i, hidx[arr[i]] == i (the
 *       back-pointer round-trips), so arbitrary-remove-by-heap_index always
 *       lands on the right node.
 *   (4) BOUNDS            -- 0 <= dh_size <= cap, every access in [0,size).
 *
 * NEGATIVE CONTROL (-DBUG_NO_INDEX_UPDATE): the swap forgets to update the
 * heap_index back-pointer on the node that moved into slot j (in a sift, the
 * node moving DOWN the tree), leaving a stale back-pointer.  A later
 * arbitrary-remove then reads that stale index and unlinks/overwrites the WRONG
 * slot -> index-consistency (or the heap property) breaks.  CBMC must report
 * VERIFICATION FAILED, proving the index-consistency assertion has teeth on
 * exactly the back-pointer the real code is careful to maintain.
 *
 * Run via verify/run_verify.sh (cbmc), or directly:
 *   cbmc timer_heap_cbmc.c --unwind 8 --unwinding-assertions
 *   cbmc timer_heap_cbmc.c --unwind 8 --unwinding-assertions -DBUG_NO_INDEX_UPDATE  (expect FAILED)
 */

/* Small bounds keep the bounded check tractable while still exercising
 * multi-level sift (CAP=5 -> depth 3: root, 2 children, up to 2 grandchildren)
 * and arbitrary interior removes.  STEPS sequences enough ops to build then
 * tear down a multi-level heap. */
#ifndef CAP
#  define CAP 5
#endif
#ifndef STEPS
#  define STEPS 6
#endif

typedef long long ll;

/* The heap state.  arr[] holds node ids (the renamed `dh_arr` of pointers);
 * dh_size/dh_cap are the real fields.  Per-node state lives in the parallel
 * key[]/hidx[] arrays indexed by node id (the renamed deadline_ns/heap_index). */
static int arr[CAP];      /* arr[slot] = node id occupying that slot          */
static ll  key[CAP];      /* key[id]   = node's deadline_ns                    */
static int hidx[CAP];     /* hidx[id]  = node's heap_index back-pointer; -1 off */
static int dh_size;
static int dh_cap;

int nondet_int(void);
ll  nondet_ll(void);

/* ---- faithful transcription of netpoll_parkers.c.inc ------------------ */

static void runloom_dh_swap(int i, int j)
{
    int t = arr[i];           /* node_t *t = pool->dh_arr[i];          */
    arr[i] = arr[j];          /* pool->dh_arr[i] = pool->dh_arr[j];    */
    arr[j] = t;               /* pool->dh_arr[j] = t;                  */
    hidx[arr[i]] = i;         /* pool->dh_arr[i]->heap_index = i;      */
#ifndef BUG_NO_INDEX_UPDATE
    hidx[arr[j]] = j;         /* pool->dh_arr[j]->heap_index = j;      */
#else
    /* BUG: the node that moved into slot j keeps its OLD heap_index.  In a
     * sift this is the node moving DOWN the tree; its back-pointer now lies. */
#endif
}

static void runloom_dh_sift_up(int i)
{
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (key[arr[i]] >= key[arr[parent]])
            break;
        runloom_dh_swap(i, parent);
        i = parent;
    }
}

static void runloom_dh_sift_down(int i)
{
    int n = dh_size;
    while (1) {
        int l = 2 * i + 1, r = 2 * i + 2, best = i;
        if (l < n && key[arr[l]] < key[arr[best]])
            best = l;
        if (r < n && key[arr[r]] < key[arr[best]])
            best = r;
        if (best == i) break;
        runloom_dh_swap(i, best);
        i = best;
    }
}

static void runloom_dh_insert(int id)
{
    if (key[id] < 0 || hidx[id] >= 0) return;
    if (dh_size >= dh_cap) {
        /* real code grows; here CAP is fixed -> insert dropped, heap stays
         * consistent (same contract: "heap stays consistent; insert dropped"). */
        return;
    }
    arr[dh_size] = id;
    hidx[id] = dh_size;
    dh_size++;
    runloom_dh_sift_up(hidx[id]);
}

static void runloom_dh_remove(int id)
{
    int i = hidx[id];
    if (i < 0 || i >= dh_size) return;
    hidx[id] = -1;
    dh_size--;
    if (i == dh_size) return;          /* removed the tail */
    arr[i] = arr[dh_size];
    hidx[arr[i]] = i;
    /* Could be either direction; try both. */
    runloom_dh_sift_up(i);
    runloom_dh_sift_down(i);
}

static ll runloom_dh_peek_deadline(void)
{
    if (dh_size == 0) return -1;
    return key[arr[0]];
}

/* pop-min: the sched/timer "fire the earliest sleeper" op.  The real pump
 * expires the root via runloom_dh_remove(dh_arr[0]); model it the same way. */
static int runloom_dh_pop_min(void)
{
    int root;
    if (dh_size == 0) return -1;
    root = arr[0];
    runloom_dh_remove(root);           /* same arbitrary-remove path, i==0 */
    return root;
}

/* ---- invariant checks ------------------------------------------------- */

static void check_invariants(void)
{
    int i;

    /* (4) BOUNDS: size always within [0, cap]. */
    __CPROVER_assert(dh_size >= 0 && dh_size <= dh_cap,
        "timer-heap: size out of [0,cap]");

    for (i = 0; i < dh_size; i++) {
        int l = 2 * i + 1, r = 2 * i + 2;

        /* slot index in range (defends array accesses below). */
        __CPROVER_assert(i >= 0 && i < dh_cap,
            "timer-heap: live slot index out of array bounds");

        /* (3) INDEX CONSISTENCY: the back-pointer round-trips to this slot. */
        __CPROVER_assert(hidx[arr[i]] == i,
            "timer-heap: heap_index back-pointer != actual slot");

        /* (1) MIN-HEAP PROPERTY: parent <= each present child. */
        if (l < dh_size)
            __CPROVER_assert(key[arr[i]] <= key[arr[l]],
                "timer-heap: min-heap property violated (left child < parent)");
        if (r < dh_size)
            __CPROVER_assert(key[arr[i]] <= key[arr[r]],
                "timer-heap: min-heap property violated (right child < parent)");
    }
}

static void check_peek_is_true_min(void)
{
    /* (2) PEEK CORRECTNESS: dh_peek_deadline == the minimum over all live
     * deadlines (and -1 iff empty). */
    ll peek = runloom_dh_peek_deadline();
    if (dh_size == 0) {
        __CPROVER_assert(peek == -1, "timer-heap: peek of empty heap != -1");
    } else {
        int i;
        for (i = 0; i < dh_size; i++)
            __CPROVER_assert(peek <= key[arr[i]],
                "timer-heap: peek is not the true minimum");
    }
}

int main(void)
{
    int s, used = 0;

    dh_size = 0;
    dh_cap  = CAP;
    for (s = 0; s < CAP; s++) {
        arr[s]  = 0;
        hidx[s] = -1;
        /* symbolic, non-negative deadline (negative is filtered upstream and
         * never enters the heap).  The key field is `long long` exactly as in
         * runloom_parked_t, but the heap ONLY compares keys (<, <=, >=) -- never
         * arithmetic -- so the proof over the full 64-bit domain reduces, with no
         * loss of generality, to a totally-ordered range with enough values AND
         * repeats to realise every pairwise order relation among CAP nodes
         * (<, =, > for each pair).  [0, CAP] gives that while keeping SAT small. */
        key[s] = nondet_ll();
        __CPROVER_assume(key[s] >= 0);
        __CPROVER_assume(key[s] <= CAP);
    }

    for (s = 0; s < STEPS; s++) {
        int op = nondet_int();
        __CPROVER_assume(op >= 0 && op <= 2);

        if (op == 0) {
            /* INSERT a fresh symbolic-deadline node (if any left). */
            if (used < CAP) {
                runloom_dh_insert(used);
                used++;
            }
        } else if (op == 1) {
            /* POP-MIN (fire the earliest sleeper). */
            (void)runloom_dh_pop_min();
        } else {
            /* ARBITRARY REMOVE by a nondeterministic slot: pick a live slot,
             * take the node living there, and remove it via its OWN back-pointer
             * (hidx) -- exactly how the real unlink works (a caller holding the
             * parker calls runloom_dh_remove(p), which reads p->heap_index).
             * This is the op the negative control corrupts. */
            if (dh_size > 0) {
                int k = nondet_int();
                __CPROVER_assume(k >= 0 && k < dh_size);
                runloom_dh_remove(arr[k]);
            }
        }

        /* invariants must hold after EVERY op. */
        check_invariants();
        check_peek_is_true_min();
    }

    return 0;
}
