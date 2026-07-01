"""Two fibers, each looping select([("send", ch, v), ("recv", ch)]) on ONE
unbuffered channel.  Every rendezvous consumes one round from each party, so
both always finish together -- Go equivalent never deadlocks.  Hunt for
lost-rendezvous hang."""
import sys
import runloom
import runloom_c as rc

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 300

ch = rc.Chan(0)
done = [0, 0]
progress = [0, 0]

def main():
    def party(pid):
        for i in range(ROUNDS):
            progress[pid] = i
            idx, res = rc.select([("send", ch, (pid, i)), ("recv", ch)])
            if idx == 1:
                v, ok = res
                assert ok and v[0] != pid, "self-paired or closed: %r" % (v,)
        done[pid] = 1
    rc.mn_fiber(lambda: party(0))
    rc.mn_fiber(lambda: party(1))

runloom.run(HUBS, main)
print("done flags:", done, "progress:", progress)
assert done == [1, 1]
print("OK")
