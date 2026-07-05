# The docs/channels.md "canonical send unless cancelled" pattern.
import runloom_c as rc

def main():
    out = rc.Chan(1)          # buffered: send is ready whenever there's space
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
            if i >= 200000:
                result['cancel_seen_at'] = None  # starved
                return

    def consumer():
        while 'cancel_seen_at' not in result:
            out.recv()   # keep out drainable -> send case always eventually ready

    cancel.send('STOP')      # cancellation is PENDING before producer even starts
    rc.fiber(producer)
    rc.fiber(consumer)
    while 'cancel_seen_at' not in result:
        rc.yield_()
    print("cancel seen at iteration:", result['cancel_seen_at'])

rc.fiber(main); rc.run()
