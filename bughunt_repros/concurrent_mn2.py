"""Two OS threads racing run(2, work): instrumented, faulthandler dump on hang."""
import threading, sys, faulthandler
import runloom

faulthandler.dump_traceback_later(20, exit=True)
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
    print("round", rnd, "ran=", ran, "errs=", errs, flush=True)
    for i in range(2):
        if not any(e[0] == i for e in errs):
            assert ran[i] == 1, "round %d: thread %d run() returned but work never ran! errs=%r ran=%r" % (rnd, i, errs, ran)
print("concurrent mn: %d rounds OK" % ROUNDS)
