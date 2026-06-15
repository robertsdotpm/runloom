"""A diverse M:N workload reused by the coverage tests (tests/test_cov_*.py).

Exercises, under runloom.run(N): cross-hub channels (wake_g / hub_submit),
sleeps (timer/sleep heap), CPU spins (preempt/sysmon), blocking offload
(blockpool), socket + file I/O (netpoll + io_uring), sched_yield (fastpath),
go_n bulk spawn, and introspection.  Importing as a module gives `workload()`;
running as a script runs it under run(args.hubs) and exits 0 on success.

It is deliberately SHORT and self-terminating so it can run as a subprocess
with an env-gated scheduler mode set, to drive that mode's C paths for gcov.
"""
import os
import socket
import sys
import tempfile

sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup


def workload(producers=4, consumers=4, per=80):
    wg = WaitGroup()
    ch = rc.Chan(16)
    total = producers * per
    sink = [0]
    sink_mu = rc.Mutex()

    wg.add(producers)
    def producer(pid):
        try:
            for j in range(per):
                if j % 7 == 0:
                    runloom.sleep(0.0003)          # sleep heap / timer drain
                ch.send(pid * per + j)
        finally:
            wg.done()

    def consumer():
        n = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            sink_mu.lock(); sink[0] += 1; sink_mu.unlock()
            n += 1

    # CPU spin: env-tunable so the sysmon/preempt tests can make a fiber occupy
    # its hub long enough (> RUNLOOM_SYSMON_MS / RUNLOOM_PREEMPT_MS) to trip the
    # detector, while the default-path tests keep it cheap.
    _cpu_iters = int(os.environ.get("RUNLOOM_COV_CPU", "200000"))
    def cpu():
        x = 0
        for i in range(_cpu_iters):
            x += i
        return x

    def yielder():
        for _ in range(50):
            rc.sched_yield()                      # yield fastpath / bound

    def offloader():
        import time as _t
        rc.blocking(lambda: (_t.sleep(0.002), 1)[1])   # blockpool offload

    def io_pair():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        def w():
            rc.sched_yield()
            rc.tcp_send(b.fileno(), b"ping")        # cooperative
        rc.mn_go(w)                                  # MUST be mn_go under M:N
        buf = bytearray(4)
        rc.tcp_recv(a.fileno(), buf, 4)             # cooperative park
        rc.netpoll_unregister(a.fileno()); a.close()
        rc.netpoll_unregister(b.fileno()); b.close()

    def file_io():
        fd, path = tempfile.mkstemp()
        rc.file_write(fd, b"coverage", 0)
        buf = bytearray(8); rc.file_read(fd, buf, 8, 0)
        os.close(fd); os.unlink(path)

    def main():
        for c in range(consumers):
            rc.mn_go(consumer)
        for p in range(producers):
            rc.mn_go(lambda p=p: producer(p))
        for _ in range(3):
            rc.mn_go(cpu)
            rc.mn_go(yielder)
            rc.mn_go(offloader)
            rc.mn_go(io_pair)
            rc.mn_go(file_io)
        wg.wait()
        ch.close()
        # introspection while hubs are still alive
        rc.mn_hub_states(); rc.fibers(); rc.stats()
    return main, sink, total


def run(hubs=4):
    main, sink, total = workload()
    runloom.run(hubs, main)
    assert sink[0] == total, "lost %d/%d" % (total - sink[0], total)
    return sink[0]


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=4)
    a = ap.parse_args()
    n = run(a.hubs)
    sys.stdout.write("WORKLOAD_OK %d\n" % n)
