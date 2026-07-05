import runloom_c as rc

out = rc.Chan(0)      # unbuffered, consumer blocks in recv -> waiting receiver
cancel = rc.Chan(1)
result = {}
N = 100000

def producer():
    i = 0
    while True:
        idx, _ = rc.select([('send', out, i), ('recv', cancel)])
        if idx == 1:
            result['cancel_seen_at'] = i
            return
        i += 1
        if i >= N:
            result['cancel_seen_at'] = None
            return

def consumer():
    n = 0
    while True:
        v, ok = out.recv()
        n += 1
        if v is None or v >= N - 1:
            return

cancel.send('STOP')   # cancellation pending the whole time
rc.fiber(consumer)
rc.fiber(producer)
rc.run()
print("cancel seen at iteration:", result.get('cancel_seen_at', 'MISSING'),
      "(None => starved for", N, "sends despite pending cancel; Go would see it ~immediately)")
