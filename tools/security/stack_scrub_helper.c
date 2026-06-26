/* Deterministic pooled-stack hygiene probe. write_sentinel() fills ~32 KB of
 * the goroutine stack and records 16 exact addresses spread across it.
 * read_recorded() (run in a later goroutine that reused the stack) reads those
 * same virtual addresses and counts how many still hold the sentinel -- a leak.
 * Built -O0 so nothing is optimised away. */
#include <stdint.h>

/* POSIX .so exports every non-static symbol; a Windows .dll exports nothing
 * unless told to, so ctypes.CDLL would not find these.  EXPORT is empty on
 * POSIX (identical .so) and __declspec(dllexport) on Windows. */
#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

volatile uint64_t g_sink;
enum { NWORDS = 4096, NREC = 16 };          /* 32 KB buffer, 16 probe points */
static volatile uint64_t * volatile g_addrs[NREC];

EXPORT void write_sentinel(uint64_t val) {
    volatile uint64_t buf[NWORDS];
    int i;
    for (i = 0; i < NWORDS; i++) buf[i] = val;
    for (i = 0; i < NREC; i++) g_addrs[i] = &buf[(NWORDS / NREC) * i];
    for (i = 0; i < NWORDS; i++) g_sink += buf[i];
}

/* Count recorded addresses (now in the reused stack) that still == val. */
EXPORT int read_recorded(uint64_t val) {
    int i, hits = 0;
    for (i = 0; i < NREC; i++)
        if (g_addrs[i] && *g_addrs[i] == val) hits++;
    return hits;
}

/* Diagnostic: return the raw 8 bytes now at a mid-buffer recorded address. */
EXPORT uint64_t read_raw(void) { return g_addrs[8] ? *g_addrs[8] : 0; }
