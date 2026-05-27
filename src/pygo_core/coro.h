/* coro.h -- portable stackful coroutines.
 *
 * Three properties matter:
 *   1. Each pygo_coro owns its own C stack (heap-allocated, fixed at create).
 *   2. pygo_coro_resume / pygo_coro_yield transfer control with no syscall.
 *   3. The current coroutine pointer is thread-local, so multiple OS
 *      threads each run their own scheduler independently.
 *
 * Backend is chosen at compile time via plat.h:
 *   - Windows: ConvertThreadToFiber + CreateFiber + SwitchToFiber.
 *              Available since Windows 95.  Fastest + simplest on Win.
 *   - POSIX:   getcontext / makecontext / swapcontext.  POSIX.1-2001.
 *              Still works on every Unix we care about despite being
 *              deprecated by POSIX.1-2008.
 *
 * The public API is the same on both.
 */
#ifndef PYGO_CORO_H
#define PYGO_CORO_H

#include "compat.h"

typedef struct pygo_coro pygo_coro_t;

typedef void (*pygo_entry_fn)(void *user);

/* Lifecycle: returns NULL on alloc failure (errno set, or
 * GetLastError on Windows). */
pygo_coro_t *pygo_coro_new(size_t stack_size,
                           pygo_entry_fn entry,
                           void *user);

void pygo_coro_destroy(pygo_coro_t *c);

/* Switch into the coroutine.  Must be called from the same OS thread on
 * which pygo_coro_new was called.  Returns when the coroutine yields or
 * returns.  Calling resume on a done coroutine is undefined. */
void pygo_coro_resume(pygo_coro_t *c);

/* Yield from inside a coroutine.  Returns control to whatever called
 * pygo_coro_resume on us; on next resume, execution continues just
 * past the pygo_coro_yield call.  Calling yield from outside any
 * coroutine is undefined. */
void pygo_coro_yield(void);

/* Predicates. */
int pygo_coro_done(const pygo_coro_t *c);

/* Backend identifier ("fibers", "ucontext"); useful for tests. */
const char *pygo_coro_backend(void);

/* Per-thread setup / teardown.  Must be called once per OS thread
 * before any coro on that thread.  Idempotent. */
int pygo_coro_thread_init(void);
void pygo_coro_thread_fini(void);

#endif /* PYGO_CORO_H */
