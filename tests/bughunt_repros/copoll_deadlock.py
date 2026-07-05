"""CoPoll.poll(None) is offloaded to the blocking pool, sharded by the
submitting OS thread id.  Two fibers on the SAME hub -> same shard -> a
second offload queues BEHIND the infinite poll on the single shard worker.
If the second fiber's offload gates the event the poll waits for -> deadlock.
Stock threading: both blocking calls run on their own threads, finishes fast."""
import select, sys, os, time

def scenario(spawn, join_all):
    r, w = os.pipe()
    state = []
    def poller():
        p = select.poll()
        p.register(r, select.POLLIN)
        ev = p.poll(None)                    # infinite poll, offloaded
        state.append("poll done: %r" % ev)
    def writer():
        time.sleep(0.1)                      # let poller park first
        import runloom
        runloom.monkey.offload(time.sleep, 0.05)   # any offloaded blocking call
        os.write(w, b"x")                    # unblocks the poll
        state.append("writer done")
    t1 = spawn(poller); t2 = spawn(writer)
    join_all(t1, t2)
    print("completed:", state, flush=True)

if sys.argv[1] == "stock":
    import threading
    def spawn(fn):
        t = threading.Thread(target=fn); t.start(); return t
    scenario(spawn, lambda *ts: [t.join(8) for t in ts])
else:
    import runloom
    def main():
        done = []
        def wrap(fn):
            def g():
                fn(); done.append(1)
            return g
        def spawn(fn):
            runloom.fiber(wrap(fn)); return None
        # crude join: fibers run to completion under runloom.run
        r, w = os.pipe()
        state = []
        def poller():
            p = select.poll()
            p.register(r, select.POLLIN)
            ev = p.poll(None)
            print("poll done:", ev, flush=True)
        def writer():
            runloom.sleep(0.2)
            runloom.monkey.offload(time.sleep, 0.05)
            os.write(w, b"x")
            print("writer done", flush=True)
        runloom.fiber(poller)
        runloom.fiber(writer)
    runloom.monkey.patch()
    runloom.run(1, main)   # ONE hub: both fibers submit from the same OS thread
    print("ALL DONE", flush=True)
