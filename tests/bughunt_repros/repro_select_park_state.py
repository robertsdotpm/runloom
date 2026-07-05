"""A fiber blocked in select() should introspect as parked (park:select /
chan), like a fiber blocked in ch.recv() does.  runloom_chan_select's phase-2
park never calls runloom_g_state_set(PARKED_CHAN) / set_wait_reason, so the
fiber dump reports it as 'running' and the deadlock detector does not count it
as blocked."""
import runloom
import runloom_c as rc
import io, sys

ch = rc.Chan(0)
ch2 = rc.Chan(0)
states = {}

def main():
    def blocked_in_recv():
        ch2.recv()
    def blocked_in_select():
        rc.select([("recv", ch)])
    rc.mn_fiber(blocked_in_recv)
    rc.mn_fiber(blocked_in_select)
    runloom.sleep(0.2)
    for f in runloom.fibers():
        states[f["id"]] = (f.get("state"), f.get("wait"), f.get("wait_reason"))
    print("fiber states while one parked in recv and one in select:")
    for k, v in states.items():
        print("  g%s -> %r" % (k, v))
    # unblock both so run() can end
    ch.send(1)
    ch2.send(1)

runloom.run(2, main)
