"""Go semantics: 'If one or more of the communications can proceed, a single
one that can proceed is chosen via a uniform pseudo-random selection.'
runloom's select_try_each scans cases in caller order -> case 0 always wins
when multiple cases are ready, so a busy case 0 starves case 1 forever."""
import runloom_c as rc
from collections import Counter

def main():
    a = rc.Chan(1000)
    b = rc.Chan(1000)
    for i in range(1000):
        a.send(("a", i))
        b.send(("b", i))
    picks = Counter()
    for _ in range(1000):
        idx, (val, ok) = rc.select([("recv", a), ("recv", b)])
        picks[idx] += 1
    print("both channels ready 1000x, picks:", dict(picks))
    if picks.get(1, 0) == 0:
        print("STARVATION: case 1 never selected while case 0 ready (Go: ~50/50)")

rc.fiber(main)
rc.run()
