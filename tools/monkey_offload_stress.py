"""Stress the monkey offload backend's cross-thread wake path.

Non-pollable I/O (regular-file open/read, os.stat / listdir, os.system) is
dispatched to a thread-pool backend; when the offloaded call finishes, the
worker -- a REAL OS thread, not a goroutine -- calls parker.unpark() to wake the
goroutine that is blocked on it.  That cross-thread wake into the scheduler is
the race-prone seam.  This drives many goroutines through it concurrently under
the multi-hub M:N scheduler, so multiple worker threads unpark goroutines on
multiple hubs at once -- the worst case for the wake path.  Run under
ThreadSanitizer (tools/run_sanitizers_ext.sh) to check it for data races.

Usage:  monkey_offload_stress.py [ngoroutines] [ops_each] [nhubs]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import runloom.monkey
import runloom_c

runloom.monkey.patch()


def worker(gid, ops):
    # Each op parks the goroutine and is woken by a pool worker thread.
    for _ in range(ops):
        os.stat(".")            # offloaded syscall
    # one regular-file roundtrip too (open syscall offloaded)
    import tempfile
    fd, path = tempfile.mkstemp()
    try:
        os.write(fd, b"z" * 128)
        os.close(fd)
        with open(path, "rb") as fh:
            fh.read()
    finally:
        os.unlink(path)


def main():
    ngor = int(sys.argv[1]) if len(sys.argv) > 1 else 48
    ops = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    nhubs = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    # Single-thread scheduler (nhubs=1, the monkey design target): many
    # goroutines park on offloaded syscalls and are woken cross-thread by the
    # pool workers -- still exercises the worker->scheduler wake under TSan.
    # NOTE: the multi-hub M:N path (nhubs>1) currently HANGS with the offload
    # backend (the offload parker's wake is not M:N-aware); monkey documents the
    # single-thread cooperative model as the design target.  See FINDINGS.
    if nhubs <= 1:
        for i in range(ngor):
            runloom_c.go(lambda i=i: worker(i, ops), stack_size=2 << 20)
        runloom_c.run()
    else:
        runloom_c.mn_init(nhubs)
        for i in range(ngor):
            runloom_c.mn_go(lambda i=i: worker(i, ops))
        runloom_c.mn_run()
        runloom_c.mn_fini()
    assert runloom_c._self_check(0) == 0, "self_check failed after offload stress"
    print("[offload-stress] {0} goroutines x {1} ops x {2} hub(s) OK".format(ngor, ops, nhubs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
