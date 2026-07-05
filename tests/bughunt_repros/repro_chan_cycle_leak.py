"""RunloomChan has no tp_traverse/tp_clear (Py_TPFLAGS_DEFAULT, not GC-tracked),
but its C-side ring buffer owns strong refs to arbitrary PyObjects.  Any
reference cycle passing through a channel buffer is invisible to the cyclic GC
and leaks forever.  Real-world shape: a request object carrying its reply
channel, still sitting in that channel's buffer when everything is dropped."""
import gc
import weakref

class Req:
    pass

def control_no_chan():
    # same cycle through a Python list: collected fine
    r = Req()
    r.lst = [r]
    w = weakref.ref(r)
    del r
    gc.collect()
    return w() is None

def through_chan():
    import runloom_c as rc
    ch = rc.Chan(1)
    r = Req()
    r.reply = ch          # r -> ch
    ch.send(r)            # ch buffer -> r  (C-side strong ref, invisible to GC)
    w = weakref.ref(r)
    del ch, r
    for _ in range(5):
        gc.collect()
    return w() is None

print("control cycle (list) collected:", control_no_chan())
collected = through_chan()
print("cycle through Chan buffer collected:", collected)
if not collected:
    print("LEAK CONFIRMED: cycle through Chan buffer is uncollectable")
