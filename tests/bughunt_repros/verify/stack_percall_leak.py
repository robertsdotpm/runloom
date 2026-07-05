"""Is the RSS growth a PER-CALL leak in RunloomG_stack (PyDict_SetItemString
with an unconsumed PyUnicode_FromString ref), not a per-thread sched leak?
Single thread, one fiber handle, N stack() calls."""
import runloom_c as rc


def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])


h = None

def main():
    global h
    h = rc.current_g()

rc.fiber(main)
rc.run()

# warm up
for _ in range(100_000):
    h.stack()
r0 = rss_kb()
N = 2_000_000
for _ in range(N):
    h.stack()
r1 = rss_kb()
print("RSS delta over %d stack() calls on ONE thread: %d kB (%.1f bytes/call)"
      % (N, r1 - r0, (r1 - r0) * 1024.0 / N))
