import sys, runloom_c

def f(): pass

c = runloom_c.Coro(f)
r0 = sys.getrefcount(f)
for _ in range(100):
    c.__init__(f)
r1 = sys.getrefcount(f)
print('callable refcount growth after 100 re-inits:', r1 - r0)

# result leak: run coro to completion so self->result holds a ref, then re-init
class Obj: pass
o = Obj()
def g():
    return o
c2 = runloom_c.Coro(g)
c2.resume()
before = sys.getrefcount(o)
c2.__init__(g)
after = sys.getrefcount(o)
print('result ref leaked on re-init (delta should be -1 if freed, 0 if leaked):', after - before)
