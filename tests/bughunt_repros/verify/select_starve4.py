import runloom_c as rc

N = 100000
out = rc.Chan(N + 10)   # send case is ready on every call (buffer space)
cancel = rc.Chan(1)
result = {}

def producer():
    i = 0
    while i < N:
        idx, _ = rc.select([('send', out, i), ('recv', cancel)])
        if idx == 1:
            result['cancel_seen_at'] = i
            return
        i += 1
    result['cancel_seen_at'] = None

cancel.send('STOP')     # cancellation pending BEFORE the loop starts
rc.fiber(producer)
rc.run()
print("cancel seen at iteration:", result['cancel_seen_at'],
      "(None => never, across", N, "selects; Go picks a ready case uniformly at random)")
