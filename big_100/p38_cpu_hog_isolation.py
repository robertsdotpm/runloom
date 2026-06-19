"""big_100 / 38 -- CPU hog isolation.

Half the goroutines run tight CPU loops that never yield; the other half do
short cooperative sleeps (standing in for I/O).  With preemption on (the M:N
default) the I/O goroutines must keep making steady progress even though the
CPU hogs are trying to monopolise the hubs.

Stresses: preemption / fairness between non-yielding CPU work and I/O.
"""
import harness
import runloom


def cpu_hog(H, wid, rng, state):
    x = 0
    while H.running():
        # A chunk of non-yielding CPU work.  Preemption must still let the I/O
        # goroutines run between (or during) these.
        for i in range(200000):
            x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        state["hog_ticks"][wid & 1023] += 1
        H.op(wid)
    state["sink"][0] = x


def io_worker(H, wid, rng, state):
    while H.running():
        runloom.sleep(0.005)
        state["io_ticks"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"io_ticks": [0] * 1024, "hog_ticks": [0] * 1024, "sink": [0]}


def body(H):
    hogs = H.funcs // 2
    ios = H.funcs - hogs
    H.run_pool(hogs, cpu_hog, H.state)
    H.run_pool(ios, io_worker, H.state)

    def auditor():
        H.sleep(3.0)
        last = sum(H.state["io_ticks"])
        while H.running():
            H.sleep(2.0)
            now = sum(H.state["io_ticks"])
            progress = now - last
            # The I/O goroutines must keep advancing despite the CPU hogs.
            if not H.check(progress > 0,
                           "I/O goroutines starved by CPU hogs (no progress "
                           "in 2s)"):
                return
            last = now
        H.log("io_ticks={0} hog_ticks={1}".format(
            sum(H.state["io_ticks"]), sum(H.state["hog_ticks"])))

    H.fiber(auditor)


if __name__ == "__main__":
    harness.main("p38_cpu_hog_isolation", body, setup=setup, default_funcs=2000,
                 describe="CPU hogs must not starve cooperative I/O goroutines")
