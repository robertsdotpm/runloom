import runloom_c

def f(): pass

def vmsize_kb():
    with open('/proc/self/status') as fp:
        for line in fp:
            if line.startswith('VmSize:'):
                return int(line.split()[1])

c = runloom_c.Coro(f)
before = vmsize_kb()
for _ in range(1000):
    c.__init__(f)
after = vmsize_kb()
print('VmSize growth after 1000 re-inits: %.1f MiB' % ((after - before) / 1024.0))
