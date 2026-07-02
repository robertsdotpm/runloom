/* Part of chan.c -- #included into that translation unit, not compiled
 * standalone.  See chan.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* ---- select() ---------------------------------------------------- */

/* Per-thread PRNG driving Go's uniform-pseudo-random select order (below).
 * xorshift64, same generator the PCT scheduler uses (runloom_sched_datastack).
 * Lazily seeded from the address of the thread-local itself, so distinct
 * threads get distinct streams without a shared, contended seed. */
static RUNLOOM_TLS uint64_t runloom_select_rng = 0;

static uint64_t runloom_select_rand(void)
{
    uint64_t x = runloom_select_rng;
    if (x == 0) {                        /* lazy per-thread seed */
        /* &runloom_select_rng is a per-thread address (distinct TLS blocks),
         * so distinct threads diverge; a splitmix64 finalizer diffuses the
         * low address bits.  Never 0 (the xorshift fixpoint). */
        x = (uint64_t)(uintptr_t)&runloom_select_rng + 0x9E3779B97F4A7C15ULL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        x ^= x >> 31;
        if (x == 0) x = 0x9E3779B97F4A7C15ULL;
    }
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    runloom_select_rng = x;
    return x;
}

static int runloom_select_gcd(int a, int b)
{
    while (b != 0) { int t = a % b; a = b; b = t; }
    return a;
}

/* Phase 1: caller holds NO locks.  For each case (visited in a random
 * order, see below), lock the channel, check if the op is immediately
 * fireable.  If yes, perform the op under lock (like send/recv would),
 * unlock, return the case index.  Else unlock and continue.
 * Returns -1 if no case is immediately ready.
 *
 * Go's select spec: when several cases can proceed, "a single one that can
 * proceed is chosen via a uniform pseudo-random selection" -- precisely so a
 * persistently-ready earlier case can never starve later ones (e.g. the docs'
 * canonical send-unless-cancelled pattern must still observe the cancel).
 * A strict caller-order scan fired the first ready case every time, starving
 * the rest.  We don't materialise a full Fisher-Yates pollorder (that needs a
 * per-call allocation); instead we begin at a random offset and step by a
 * stride coprime with n, so all n cases are visited exactly once and the FIRST
 * fireable one wins.  Same randomOrder trick the work-stealer uses
 * (mn_sched_hub_main.c.inc); for the canonical 2-case pattern it is exactly
 * 50/50. */
static int select_try_each(runloom_select_case_t *cases, int n)
{
    int k, i, start = 0, stride = 1;
    if (n > 1) {
        start = (int)(runloom_select_rand() % (uint64_t)n);
        if (n > 2) {
            /* A stride in [1, n-1] coprime with n visits the whole residue
             * set 0..n-1 exactly once; s==1 is coprime with every n, so the
             * search always terminates. */
            stride = 1 + (int)(runloom_select_rand() % (uint64_t)(n - 1));
            while (runloom_select_gcd(stride, n) != 1)
                stride = (stride % (n - 1)) + 1;
        }
    }
    i = start;
    for (k = 0; k < n; k++) {
        runloom_select_case_t *c = &cases[i];
        runloom_chan_t *ch = c->ch;
        RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        if (c->op == RUNLOOM_SELECT_SEND) {
            runloom_chan_waiter_t *rx;
            if (ch->closed) {
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                PyErr_SetString(PyExc_ValueError, "select send on closed channel");
                return -2;
            }
            rx = waiter_pop_claimable(&ch->receivers, &ch->receivers_tail);
            if (rx != NULL) {
                Py_INCREF(c->send_value);
                rx->value = c->send_value;
                rx->ok = 1;
                wake_waiter(rx);
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                return i;
            }
            if (ch->cap > 0 && ch->len < ch->cap) {
                Py_INCREF(c->send_value);
                buf_push(ch, c->send_value);
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                return i;
            }
        } else {  /* RECV */
            runloom_chan_waiter_t *tx;
            if (ch->len > 0) {
                c->recv_value = buf_pop(ch);
                c->recv_ok = 1;
                tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
                if (tx != NULL) {
                    buf_push(ch, tx->value);
                    tx->value = NULL;
                    tx->send_result = 0;
                    wake_waiter(tx);
                }
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                return i;
            }
            tx = waiter_pop_claimable(&ch->senders, &ch->senders_tail);
            if (tx != NULL) {
                c->recv_value = tx->value;
                tx->value = NULL;
                tx->send_result = 0;
                wake_waiter(tx);
                c->recv_ok = 1;
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                return i;
            }
            if (ch->closed) {
                Py_INCREF(Py_None);
                c->recv_value = Py_None;
                c->recv_ok = 0;
                RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
                return i;
            }
        }
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        i += stride;              /* next case in the randomized visit order */
        if (i >= n) i -= n;       /* stride < n, so a single wrap suffices */
    }
    return -1;
}

/* Walk all OTHER channels under their locks, splice out the stale
 * tombstone waiter we left behind during the park phase.  Without
 * this, the next operation on those channels would see a phantom
 * waiter, fail to claim it (CAS lose), pop the next -- so it's
 * eventually-cleaned, but a slow-trickle accumulation of stale
 * pointers into freed stack frames is asking for use-after-free.
 * Better to evict eagerly. */
static void select_evict_self(runloom_select_case_t *cases, int n,
                              runloom_chan_waiter_t *waiters,
                              int fired)
{
    int i;
    for (i = 0; i < n; i++) {
        runloom_chan_t *ch;
        runloom_chan_waiter_t *target;
        runloom_chan_waiter_t **head;
        runloom_chan_waiter_t **tail;
        runloom_chan_waiter_t **pp;

        if (i == fired) continue;            /* the firing channel already popped us */
        ch = cases[i].ch;
        target = &waiters[i];
        head = (cases[i].op == RUNLOOM_SELECT_SEND) ? &ch->senders : &ch->receivers;
        tail = (cases[i].op == RUNLOOM_SELECT_SEND) ? &ch->senders_tail : &ch->receivers_tail;

        RUNLOOM_RLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
        pp = head;
        while (*pp != NULL) {
            if (*pp == target) {
                *pp = target->next;
                if (*pp == NULL) *tail = target;  /* fix tail if we removed the last */
                if (*pp == NULL) *tail = NULL;
                target->next = NULL;
                break;
            }
            pp = &(*pp)->next;
        }
        /* Tail fix-up: if we just removed the tail entry, walk to find
         * the new tail.  Cheap because queues are short. */
        if (*head != NULL) {
            runloom_chan_waiter_t *w = *head;
            while (w->next != NULL) w = w->next;
            *tail = w;
        } else {
            *tail = NULL;
        }
        RUNLOOM_RUNLOCK(&ch->lock, RUNLOOM_RANK_CHAN);
    }
}
