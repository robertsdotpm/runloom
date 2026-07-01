"""Two OS threads call run(2, work) concurrently, many rounds.
Expect: one wins, other raises cleanly. Watch for double-init, lost work, crash, hang."""
import threading, sys
import runloom

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 30

for rnd in range(ROUNDS):
    ran = [0, 0]
    errs = []
    barrier = threading.Barrier(2)

    def t(i):
        def work():
            for _ in range(50):
                runloom.yield_now()
            ran[i] = 1
        barrier.wait()
        try:
            runloom.run(2, work)
        except Exception as e:
            errs.append((i, type(e).__name__))

    ths = [threading.Thread(target=t, args=(i,)) for i in range(2)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    ok_threads = [i for i in range(2) if ran[i]]
    # every thread that did NOT error must have had its work run
    for i in range(2):
        if not any(e[0] == i for e in errs):
            assert ran[i] == 1, "round %d: thread %d run() returned but work never ran! errs=%r ran=%r" % (rnd, i, errs, ran)
print("concurrent mn: %d rounds OK" % ROUNDS)
