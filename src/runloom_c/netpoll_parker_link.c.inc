/* Part of netpoll.c -- #included into that translation unit, not compiled
 * standalone.  See netpoll.c for the includes/typedefs/file-scope state
 * these definitions depend on. */


/* Link p into both pool->head and pool's per-fd bucket.  Caller
 * holds pool->lock.
 *
 * Stack-pooling note: runloom_parked_t lives on the calling fiber's
 * coroutine stack (see runloom_netpoll_wait_fd).  Stacks are returned to
 * a per-hub TLS pool when a g completes (runloom_stack_release) and
 * re-issued to the next g spawned on that hub.  The new g's wait_fd
 * places its parker at the SAME stack offset, so the parker address
 * is byte-identical to a previous occupant's.  All four list-link
 * fields are freshly zeroed before this call (in runloom_netpoll_wait_fd),
 * but pool->head and pool->by_fd[fd] can still reference this address
 * from a prior life if any unlink path for that previous occupant
 * missed (a residual M:N + free-threaded race that is not yet fully
 * isolated upstream).
 *
 * Detach any stale self-reference here before pushing.  Otherwise
 * p->next = pool->head sets p->next = p (1-cycle in the global
 * list), and head = pool->by_fd[p->fd] = p sets p->next_by_fd = p /
 * p->prev_by_fd = p (self-cycle in the bucket).  Either form wedges
 * the pump's list walks indefinitely. */
static void runloom_parker_link(runloom_parker_pool_t *pool, runloom_parked_t *p)
{
    /* Stale-reference clears.  See header comment.  Cheap (two
     * compare-and-conditional-store); only fires when stack reuse hits
     * a parker address that an unlink missed. */
    if (pool->head == p) {
        pool->head = NULL;
        RUNLOOM_EVT(RUNLOOM_EVT_PARKER_GHOST, p, NULL, (long long)p->fd);
    }
    if (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap &&
        pool->by_fd[p->fd] == p) {
        pool->by_fd[p->fd] = NULL;
        RUNLOOM_EVT(RUNLOOM_EVT_PARKER_GHOST, p, (void *)(uintptr_t)1, (long long)p->fd);
    }
#ifdef RUNLOOM_PARKER_DEBUG
    /* Diagnostic: announce ghost references so we can pinpoint the
     * upstream missed-unlink path. */
    if (p->slot != NULL || p->next != NULL) {
        fprintf(stderr,
                "[runloom] parker has nonnull slot/next at link entry: "
                "parker=%p fd=%d g=%p slot=%p next=%p\n",
                (void *)p, p->fd, (void *)p->g,
                (void *)p->slot, (void *)p->next);
    }
#endif
    /* Global list: push at head, slot-pointer trick. */
    p->next = pool->head;
    if (p->next != NULL) p->next->slot = &p->next;
    pool->head = p;
    p->slot = &pool->head;

    /* Per-fd bucket: push at head, doubly-linked.  If the realloc
     * failed we still keep the parker on the global list (slow path
     * walks the global list to find it); subsequent epoll events on
     * this fd just won't find it through the fast path. */
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
    if (p->fd >= 0 && runloom_parker_fd_index_ensure(pool, p->fd) == 0) {
        runloom_parked_t *head = pool->by_fd[p->fd];
        p->next_by_fd = head;
        if (head != NULL) head->prev_by_fd = p;
        pool->by_fd[p->fd] = p;
    }
    /* Deadline heap: insert if this parker has a finite deadline.
     * Pump now reads min-deadline in O(1) instead of walking the
     * global list. */
    runloom_dh_insert(pool, p);
    __atomic_add_fetch(&pool->total, 1, __ATOMIC_RELEASE);
    /* Per-owner-sched parked count for the single-thread drain (non-hub
     * parkers only; M:N hubs have their own drain).  g->owner is set once at
     * spawn and never changes, so this is balanced by the unlink decrement. */
    if (p->hub == NULL && p->g != NULL && p->g->owner != NULL)
        __atomic_add_fetch(&p->g->owner->netpoll_parked, 1, __ATOMIC_RELEASE);
    pool->link_count++;       /* arrival counter for the sweep churn throttle */
    RUNLOOM_EVT(RUNLOOM_EVT_PARKER_LINK, p, p->g, (long long)p->fd);
}

/* Unlink p from both lists.  Caller holds pool->lock.  Returns 1
 * if p was actually removed from either list (caller "owns" the wake);
 * 0 if p was already fully unlinked (no-op, e.g. pump and pending-bits
 * path both raced to clean us up).  The counter is decremented exactly
 * once per actual removal.
 *
 * The counter previously lived at the call sites and had to be paired
 * by hand with each unlink invocation.  That broke under M:N: pump
 * could fire wake_g + set pending bits for the same fd if multiple
 * events arrived between wait_fd's link and pump's drain, and the
 * pending-bits path then called unlink+decrement on an already-cleaned
 * parker -- double-decrementing the counter and leaving the idle path
 * (which gates pump on parked_total > 0) sleeping while a real parker
 * sat in the list. */
static int runloom_parker_unlink(runloom_parker_pool_t *pool, runloom_parked_t *p)
{
    int touched = 0;
#ifdef RUNLOOM_PARKER_DEBUG
    if (p->prev_by_fd == p || p->next_by_fd == p) {
        fprintf(stderr,
                "[runloom] UNLINK on self-looped bucket entry parker=%p fd=%d "
                "g=%p hub=%p prev_by_fd=%p next_by_fd=%p bucket=%p\n",
                (void *)p, p->fd, (void *)p->g, p->hub,
                (void *)p->prev_by_fd, (void *)p->next_by_fd,
                (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap)
                    ? (void *)pool->by_fd[p->fd] : NULL);
        p->prev_by_fd = NULL;
        p->next_by_fd = NULL;
    }
#endif
    if (p->slot != NULL) {
        *p->slot = p->next;
        if (p->next != NULL) p->next->slot = p->slot;
        p->slot = NULL;
        p->next = NULL;
        touched = 1;
    }
    /* Bucket cleanup.  Normally either prev_by_fd is set (we're in the
     * middle/tail of the chain) OR bucket[fd] == p (we're the head) --
     * never both.  We check both unconditionally so that if a prior
     * link or stale-reference cleanup left the structure in an
     * inconsistent state (e.g., bucket points to us but we also have a
     * predecessor from a different chain), we still leave nothing
     * behind that the pump's bucket walk could trip on. */
    if (p->prev_by_fd != NULL) {
        p->prev_by_fd->next_by_fd = p->next_by_fd;
        touched = 1;
    }
    if (p->fd >= 0 && (size_t)p->fd < pool->by_fd_cap &&
        pool->by_fd[p->fd] == p) {
        pool->by_fd[p->fd] = p->next_by_fd;
        touched = 1;
    }
    if (p->next_by_fd != NULL) p->next_by_fd->prev_by_fd = p->prev_by_fd;
    p->prev_by_fd = NULL;
    p->next_by_fd = NULL;
    /* Heap remove is independent of list/bucket touch -- a parker
     * can be in the heap even if its other linkages were already
     * cleaned by a partial unlink. */
    if (p->heap_index >= 0) {
        runloom_dh_remove(pool, p);
        touched = 1;
    }
    if (touched) {
        __atomic_sub_fetch(&pool->total, 1, __ATOMIC_RELEASE);
        /* Mirror the per-owner-sched parked count bumped at link. */
        if (p->hub == NULL && p->g != NULL && p->g->owner != NULL)
            __atomic_sub_fetch(&p->g->owner->netpoll_parked, 1, __ATOMIC_RELEASE);
        /* Clear the per-g back-pointer so g completion's force-unlink
         * (in mn_sched.c hub_main) doesn't see a stale reference. */
        if (p->g != NULL && p->g->netpoll_parker == p) {
            p->g->netpoll_parker = NULL;
        }
        RUNLOOM_EVT(RUNLOOM_EVT_PARKER_UNLINK, p, p->g, (long long)p->fd);
    }
    return touched;
}

static void runloom_parker_pool_lock_ensure_inited(runloom_parker_pool_t *pool);

/* ---- self-check inspection hook ----
 *
 * Called by runloom_self_check() in runloom_diag.c.  Walks pool->head
 * (Floyd cycle detection) and every per-fd bucket, fills in the stats
 * struct.  Takes pool->lock.
 *
 * The stats struct layout is declared in runloom_diag.c as a friend
 * (extern struct, no shared header) -- this keeps runloom_diag.h free of
 * netpoll-internal types. */
struct runloom_self_check_stats;
extern void runloom_self_check_stats_set(struct runloom_self_check_stats *out,
                                      int global_list_count,
                                      int global_list_cycle,
                                      int parked_total_atomic,
                                      int bucket_count_total,
                                      int bucket_self_loops,
                                      int bucket_unreachable);
