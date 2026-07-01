import runloom_c as rc
ch = rc.Chan(1)
ch.send("x")
print("len before reinit:", len(ch))
ch.__init__(1)              # tp_init again: old runloom_chan_t leaked (with "x" ref)
print("len after reinit:", len(ch))   # 0 -> new struct; old one + buffered "x" leaked
m = rc.Mutex()
m.lock()
m.__init__()                # old mutex chan leaked; lock state reset silently
print("mutex locked after reinit:", m.locked())
