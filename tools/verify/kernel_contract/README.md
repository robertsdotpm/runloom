# The kernel-interface trust surface

`syscall_returns.json` is every syscall/libc call runloom depends on, with its
**documented return set** (Linux man pages).  It exists because of one idea: you
can't prove software that talks to a kernel you can't audit -- but you *can*
shrink what you trust about the kernel from "it behaves correctly" (unbounded,
un-modelable) to "its returns are a subset of this finite list" (auditable).

Two things consume it, and both become *kernel-independent* once they do:

1. **Demonic-oracle proofs** (`../cbmc/*_demonic_cbmc.c`).  Model each syscall as
   a demon that returns *anything in its set* and prove the runloom invariant
   (no lost wake / no leak / no UB) holds for ALL of them.  Correct under a demon
   ⇒ correct under any real kernel whose returns are a subset.  The first one,
   `netpoll_arm_demonic_cbmc.c`, proves the arm/re-arm path is lost-wake-free for
   every `epoll_ctl` return, and its negative controls reproduce the real
   2026-07-02 migration bug + isolate the one return the fix leans on a recovery
   for.  Run: `../cbmc/run_demonic.sh`.

2. **Compile-time exhaustive handling** (Idea 1, not yet built).  Encode each
   `class` as a Result enum; `-Wswitch-enum -Werror` + `warn_unused_result` then
   make it a *compile error* to consume the result without a branch for every
   documented return.  Same table, feeding the type system instead of the prover.

`class`: **transient** = kernel says retry (must loop/repark, never propagate);
**hard** = propagate to the fiber as an error; **fatal** = a bug in OUR args
(EBADF/EINVAL -- should be unreachable, assert); **degraded** = fall back
(io_uring absent -> epoll).  The `note`s flag the footguns (short read/write is a
SUCCESS value; `mmap` fails to `MAP_FAILED` not NULL; EINTR-on-close still closed
the fd; `connect` EINPROGRESS is the async success path, not an error).
