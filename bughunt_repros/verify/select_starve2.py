import runloom_c as rc

out = rc.Chan(1)
cancel = rc.Chan(1)
result = {}

def producer():
    i = 0
    while True:
        idx, _ = rc.select([('send', out, i), ('recv', cancel)])
        if idx == 1:
            result['cancel_seen_at'] = i
            return
        i += 1
        if i >= 100000:
            result['cancel_seen_at'] = None
            return

def consumer():
    while 'cancel_seen_at' not in result:
        r = rc.select([('recv', out)], default=True)
        if r == -1:
            rc.yield_()

cancel.send('STOP')   # pending cancellation before producer starts
rc.fiber(producer)
rc.fiber(consumer)
rc.run()
print("cancel seen at iteration:", result.get('cancel_seen_at', 'MISSING'))
