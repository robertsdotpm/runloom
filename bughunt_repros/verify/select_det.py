import runloom_c as rc
from collections import Counter
def main():
    a, b = rc.Chan(1000), rc.Chan(1000)
    for i in range(1000): a.send(i); b.send(i)
    picks = Counter()
    for _ in range(1000):
        idx, _ = rc.select([('recv', a), ('recv', b)])
        picks[idx] += 1
    print(dict(picks))
rc.fiber(main); rc.run()
