"""Cross-hub PyObject refcount race (S3).

Free-threaded CPython makes refcounts on *shared* objects atomic; this checks
that runloom's C scheduler paths don't bypass that. Many goroutines spread across
hubs hammer one shared object's refcount concurrently (bind/unbind a local in
a tight loop = incref/decref). A non-atomic refcount race would crash, or
leave the object over-freed (segfault) or leaked (refcount drift).

Run standalone (functional), and under the whole-ext TSan harness
(tools/run_sanitizers_ext.sh) to catch a genuine data race even if this run
happens not to corrupt.
"""
import gc
import sys

sys.path.insert(0, "src")
import runloom_c

SHARED = None          # module global -> goroutines read it without a closure
                       # cell so the only refs are accountable.
N_HUBS = 8
N_GOROUTINES = 64
ITERS = 50_000


def worker():
    s = SHARED
    acc = 0
    for _ in range(ITERS):
        t = s            # incref SHARED
        acc += len(t)
        del t            # decref SHARED
    return acc


def main():
    global SHARED
    SHARED = bytearray(64)          # mutable heap object, plain refcounting
    gc.collect()
    base = sys.getrefcount(SHARED)

    runloom_c.mn_init(N_HUBS)
    for _ in range(N_GOROUTINES):
        runloom_c.mn_go(worker)
    runloom_c.mn_run()
    runloom_c.mn_fini()

    gc.collect()
    final = sys.getrefcount(SHARED)
    total_incdec = N_GOROUTINES * ITERS
    print("hubs=%d goroutines=%d  inc/dec pairs=%d on one shared object"
          % (N_HUBS, N_GOROUTINES, total_incdec))
    print("refcount base=%d  final=%d" % (base, final))
    if final != base:
        print("FAIL: refcount drifted by %d -- non-atomic refcount race"
              % (final - base))
        return 1
    print("OK: shared-object refcount stable under cross-hub concurrent access")
    return 0


if __name__ == "__main__":
    sys.exit(main())
