"""A recycled g must not carry a stale `pass_index` across slab reuse.

`runloom_g_slab_alloc` re-uses completed g structs from a per-thread freelist.
The reuse scrub `memset`s only up to `offsetof(runloom_g_t, state)`, then the
load-bearing `state` byte is re-initialised atomically -- but `arena`, `batch`
and `pass_index` live AFTER `state` and BEFORE the introspection block, so they
were NOT cleared by that single memset (the header documented them as cleared;
the code did not -- a doc/code contradiction).

`fiber_n(fn, n, indexed=True)` sets `pass_index = 1` on each (slab-allocated, on the
default non-bulk path) g so `g_entry` calls `fn(i)`.  When such a g completes and
is recycled for a plain `go(fn2)` (which goes through the `py_index < 0` branch
and never touches `pass_index`), the stale `pass_index = 1` made `g_entry` call
`fn2(stale c_arg)` instead of `fn2()`.  Observed effect: a recycled fiber is
mis-invoked with a spurious positional arg AND can fail to reach its WaitGroup
`done()`, wedging the program (the without-fix form of this test hangs).

Fix: a second memset clears `[offsetof(arena), offsetof(id))` after the atomic
state store, restoring the documented "everything before the introspection block
is cleared" contract (and defending any future field added in that gap).
"""
import runloom
import runloom_c


def _round(nhubs, k):
    """Phase 1: k indexed fiber_n fibers (pass_index=1 on slab gs).  Drain them so
    they return to the slab freelist.  Phase 2: k plain fiber() fibers that MUST be
    called with no positional arg -- any arg means pass_index leaked.  Returns
    the list of leaked arg-tuples (empty == clean)."""
    from runloom.sync import WaitGroup

    leaked = []

    def main():
        wg1 = WaitGroup()
        wg1.add(k)

        def idx(i):
            wg1.done()

        runloom_c.fiber_n(idx, k, 0, True)   # (fn, n, stack_size, indexed=True)
        wg1.wait()                        # phase 1 fully drains -> gs recycled

        wg2 = WaitGroup()
        wg2.add(k)

        def plain(*args):
            if args:                      # leaked: called as plain(stale index)
                leaked.append(args)
            wg2.done()

        for _ in range(k):
            go = runloom.go
            go(plain)                     # reuse the recycled slab gs
        wg2.wait()

    runloom.run(nhubs, main)
    return leaked


def test_pass_index_not_leaked_across_recycle():
    # Several rounds across hub counts; k large enough that phase-2 plain gs
    # reuse phase-1's recycled slots.  A stale pass_index either shows up as a
    # spurious arg (caught by the assert) or wedges the run (caught by timeout).
    for i in range(4):
        nhubs = (i % 3) + 1
        if nhubs == 1:
            nhubs = 2                     # fiber_n needs the M:N runtime (n > 1)
        leaked = _round(nhubs, 256)
        assert leaked == [], (i, nhubs, leaked[:8])
