/* compat.h -- portable typedef + stdint shims for pre-C99 / old MSVC.
 *
 * MSVC pre-2010 had no <stdint.h>; old Watcom too.  Provide a minimal
 * subset of the types we need (uint8/16/32/64, intptr, size_t).  Modern
 * compilers get the real header.
 */
#ifndef PYGO_COMPAT_H
#define PYGO_COMPAT_H

#include "plat.h"

#include <stddef.h> /* size_t, NULL */

#if defined(PYGO_CC_MSVC) && _MSC_VER < 1600
   /* MSVC < 2010: no <stdint.h>. */
   typedef signed char        int8_t;
   typedef unsigned char      uint8_t;
   typedef short              int16_t;
   typedef unsigned short     uint16_t;
   typedef int                int32_t;
   typedef unsigned int       uint32_t;
   typedef __int64            int64_t;
   typedef unsigned __int64   uint64_t;
#  if defined(_WIN64)
     typedef __int64          intptr_t;
     typedef unsigned __int64 uintptr_t;
#  else
     typedef int              intptr_t;
     typedef unsigned int     uintptr_t;
#  endif
#else
#  include <stdint.h>
#endif

#if defined(PYGO_CC_MSVC) && _MSC_VER < 1800
   /* MSVC < 2013 lacks proper <stdbool.h>. */
#  ifndef __cplusplus
     typedef int _Bool;
#    define bool _Bool
#    define true 1
#    define false 0
#  endif
#else
#  include <stdbool.h>
#endif

/* snprintf shim for ancient MSVC (pre-2015). */
#if defined(PYGO_CC_MSVC) && _MSC_VER < 1900
#  include <stdio.h>
#  define snprintf _snprintf
#endif

#endif /* PYGO_COMPAT_H */
