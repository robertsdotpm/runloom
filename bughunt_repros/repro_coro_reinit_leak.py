"""Coro.__init__ (tp_init) called again on an existing object overwrites
self->coro and self->callable without releasing the old ones: leaks a
whole coroutine stack mapping (>=128 KiB VA + pages) per call."""
import runloom_c

def f(): pass

def vmsize_kb():
    with open("/proc/self/status") as fp:
        for line in fp:
            if line.startswith("VmSize:"):
                return int(line.split()[1])

c = runloom_c.Coro(f)
before = vmsize_kb()
for _ in range(1000):
    c.__init__(f)          # tp_init re-invocation, pure Python
after = vmsize_kb()
print("VmSize growth after 1000 re-inits: %.1f MiB" % ((after - before) / 1024.0))
