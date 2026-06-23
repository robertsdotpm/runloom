"""Direct cross-hub migration proof for the alloc-home patch.

Each fiber records the OS thread (= hub) it runs on BEFORE parking on a channel
and AFTER being woken; a different thread id == it migrated to another hub.

  PRODUCTION (patched CPython, the flag is all you need):
      RUNLOOM_MIGRATION=1 PYTHON_GIL=0 PYTHONPATH=src <patched-python> migration_crosshub_proof.py
      -> ~50/60 MIGRATED, 0 crash

  STOCK CPython (no patch): RUNLOOM_MIGRATION=1 warns + falls back -> 0 migrated, no crash.
  Default mode (no flag): woken fibers pin to origin -> 0 migrated.
"""
import runloom_c, threading
N = 60
def body():
    ch  = runloom_c.Chan(0)        # unbuffered -> recv PARKS until fed
    res = runloom_c.Chan(N)
    def waiter(i):
        def r():
            t0 = threading.get_ident()   # OS thread (= hub) BEFORE parking
            ch.recv()                    # PARK here
            t1 = threading.get_ident()   # OS thread AFTER waking
            res.send((t0, t1))
        return r
    def feeder():
        for _ in range(N): ch.send(1)    # wake the waiters one by one
    runloom_c.mn_init(8)
    for i in range(N): runloom_c.mn_fiber(waiter(i))
    runloom_c.mn_fiber(feeder)
    runloom_c.mn_run()
    migrated = same = 0; hubs = set()
    for _ in range(N):
        g = res.try_recv()
        if g is None: break
        (t0, t1), ok = g
        hubs.add(t0); hubs.add(t1)
        if t0 != t1: migrated += 1
        else: same += 1
    runloom_c.mn_fini()
    print("fibers=%d  MIGRATED(woke on a DIFFERENT hub)=%d  same-hub=%d  distinct hub-threads=%d"
          % (migrated+same, migrated, same, len(hubs)))
body()
