"""Adversarial verify: foreign (non-runloom) threads hammer g.stack() while
M:N pools run and tear down repeatedly. Claim: lazy sched_get on the foreign
thread races netpoll pool state -> SIGSEGV, plus per-thread sched leak.
Also checks the claimed semantic misreport: a RUNNING m:n fiber reported
'parked'."""
import threading
import time
import sys
import runloom_c as rc

handles = []          # G handles published by fibers (read by foreign threads)
handles_lock = threading.Lock()
stop = threading.Event()
states_seen = set()
running_flag_states = []   # states observed while a fiber advertises "I am running"
run_marker = {}            # id -> True while fiber body is actively executing


def monitor():
    # foreign OS thread, never runs runloom; polls g.stack() in a tight loop
    while not stop.is_set():
        with handles_lock:
            hs = list(handles)
        for i, h in enumerate(hs):
            try:
                d = h.stack()
            except Exception as e:
                print("stack() raised:", e)
                continue
            states_seen.add(d["state"])
            if run_marker.get(i):
                # fiber claims to be mid-busy-loop (running on a hub)
                running_flag_states.append(d["state"])


MONITORS = 8
threads = [threading.Thread(target=monitor, daemon=True) for _ in range(MONITORS)]
for t in threads:
    t.start()

deadline = time.time() + 12
cycle = 0
while time.time() < deadline:
    cycle += 1
    local = []

    def make_fiber(idx):
        def body():
            g = rc.current_g()
            with handles_lock:
                while len(handles) <= idx:
                    handles.append(g)
                handles[idx] = g
            # park once so snap becomes valid at least transiently
            rc.sched_yield()
            run_marker[idx] = True
            t0 = time.time()
            while time.time() - t0 < 0.02:
                pass          # busy: genuinely RUNNING on a hub
            run_marker[idx] = False
            rc.sched_yield()
        return body

    rc.mn_init(4)
    for i in range(8):
        rc.mn_fiber(make_fiber(i))
    rc.mn_run()
    rc.mn_fini()

stop.set()
for t in threads:
    t.join(timeout=2)

print("cycles:", cycle)
print("states seen:", sorted(states_seen))
from collections import Counter
print("states while fiber advertised RUNNING:", Counter(running_flag_states))
print("OK: no crash")
