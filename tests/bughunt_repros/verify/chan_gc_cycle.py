import gc, weakref, runloom_c as rc
class Req: pass
ch = rc.Chan(1)
r = Req(); r.reply = ch      # r -> ch
ch.send(r)                   # ch C-buffer -> r (invisible to GC)
w = weakref.ref(r)
del ch, r
for _ in range(5): gc.collect()
print('collected:', w() is None)
