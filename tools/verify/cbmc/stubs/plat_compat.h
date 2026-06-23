/* plat_compat.h -- CBMC stub.
 *
 * The real src/runloom_c/plat_compat.h pulls in the MSVC _Interlocked*
 * atomic shims (plat_atomic.h) and Win/POSIX threading glue.  Under CBMC
 * we compile in gcc mode, so cldeque.c uses the genuine __atomic_*
 * builtins directly and needs nothing from this header.  This stub just
 * satisfies the `#include "plat_compat.h"` line so we verify the REAL
 * cldeque.c source (compiled unmodified) rather than a hand-copy.
 */
#ifndef RUNLOOM_PLAT_COMPAT_STUB_H
#define RUNLOOM_PLAT_COMPAT_STUB_H
#endif
