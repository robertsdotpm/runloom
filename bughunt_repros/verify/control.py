import gc, weakref, runloom_c as rc
class Req: pass
# control 1: plain cycle through list
r = Req(); lst = [r]; r.reply = lst
w = weakref.ref(r)
del r, lst
gc.collect()
print('control list cycle collected:', w() is None)
# control 2: chan without cycle -- object freed when chan freed?
ch = rc.Chan(1)
r2 = Req()
ch.send(r2)
w2 = weakref.ref(r2)
del r2
print('after del r2, alive (expected, buffered):', w2() is not None)
del ch
gc.collect()
print('after del ch, collected (dealloc drains buffer?):', w2() is None)
# GC flags
print('Chan GC-tracked type?', bool(rc.Chan.__flags__ & (1<<14)))  # Py_TPFLAGS_HAVE_GC
