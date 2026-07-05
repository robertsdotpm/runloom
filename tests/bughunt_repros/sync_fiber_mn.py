"""runloom.sync.fiber() calls runloom_c.fiber() directly with NO M:N dispatch
(unlike runloom.fiber / runloom.time._spawn / runloom.context._spawn /
sync.gather, which all route through mn_fiber when hubs are live).
Under run(n>1) a sync.fiber() spawn lands on the calling hub's single-thread
scheduler queue, skipping the M:N pending accounting mn_run() joins on."""
import time as wall
import runloom
import runloom.sync as gsync

ran = {"v": False}

def work():
    ran["v"] = True

def main():
    gsync.fiber(work)
    runloom.sleep(0.5)

t0 = wall.monotonic()
runloom.run(2, main)
print("run(2) done in %.2fs; sync.fiber target ran? %s (expected True)"
      % (wall.monotonic() - t0, ran["v"]))
if not ran["v"]:
    print("BUG: fiber spawned via runloom.sync.fiber never ran under M:N")
